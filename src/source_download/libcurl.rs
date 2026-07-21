use super::{
    DownloadError, DownloadResult, HTTP_MAX_BODY_BYTES, HTTP_MAX_HEADER_BYTES, HttpHeader,
    HttpHeaderKind, HttpResponse, HttpStreamResponse, StreamedBodySummary, StreamingBodySink,
    RESPONSE_HEADER_CONTENT_LENGTH, RESPONSE_HEADER_SET_COOKIE, download_error_with_source,
    enforce_http_body_length, validated_http_content_length,
    http_client::HttpMethod,
};
use alloc::{borrow::Cow, string::String, vec::Vec};
use core::{
    ffi::{CStr, c_char, c_long, c_uint, c_void},
    marker::{PhantomData, PhantomPinned},
    mem::{self, align_of, offset_of, size_of},
    ptr::{NonNull, null_mut},
    slice,
    str,
};
use std::{io::Write as IoWrite, sync::LazyLock};
mod sys;
macro_rules! curl_setopt {
    ($handle:expr, $option:expr, $value:expr) => {{
        // SAFETY: call sites pair each option with a wrapper using its documented libcurl ABI type.
        let code = unsafe { sys::curl_easy_setopt($handle.as_ptr(), $option, $value) };
        if code == CURLE_OK {
            Ok(())
        } else {
            Err(curl_error("curl_easy_setopt", code).into())
        }
    }};
}
const CURLE_OK: CurlCode = 0;
const CURL_ERROR_SIZE: usize = 256;
const CURL_GLOBAL_DEFAULT: c_long = 3;
const CURLINFO_RESPONSE_CODE: CurlInfo = 0x20_0002;
const CURLINFO_SCHEME: CurlInfo = 0x10_0031;
const CURL_MIN_PROTOCOLS_STR_VERSION: c_uint = 0x07_55_00;
const CURLVERSION_NOW: CurlVersion = 11;
const CURLOPT_CONNECTTIMEOUT_MS: CurlOption = 156;
const CURLOPT_ERRORBUFFER: CurlOption = 10_010;
const CURLOPT_FOLLOWLOCATION: CurlOption = 52;
const CURLOPT_HEADERDATA: CurlOption = 10_029;
const CURLOPT_HEADERFUNCTION: CurlOption = 20_079;
const CURLOPT_HTTPHEADER: CurlOption = 10_023;
const CURLOPT_HTTPGET: CurlOption = 80;
const CURLOPT_MAXFILESIZE_LARGE: CurlOption = 30_117;
const CURLOPT_NOSIGNAL: CurlOption = 99;
const CURLOPT_POST: CurlOption = 47;
const CURLOPT_POSTFIELDS: CurlOption = 10_015;
const CURLOPT_POSTFIELDSIZE: CurlOption = 60;
const CURLOPT_PROTOCOLS_STR: CurlOption = 10_318;
const CURLOPT_SSLVERSION: CurlOption = 32;
const CURLOPT_TIMEOUT_MS: CurlOption = 155;
const CURLOPT_URL: CurlOption = 10_002;
const CURLOPT_WRITEDATA: CurlOption = 10_001;
const CURLOPT_WRITEFUNCTION: CurlOption = 20_011;
const CURL_SSLVERSION_MAX_DEFAULT: c_long = 1 << 16;
const CURL_SSLVERSION_TLSV1_2: c_long = 6;
const HTTPS_SCHEME_PREFIX: &str = "https://";
const HTTPS_PROTOCOL: &CStr = c"https";
static CURL_INIT: LazyLock<CurlCode> = LazyLock::new(|| {
    // SAFETY: LazyLock runs this initializer once before any easy handles are used.
    unsafe { sys::curl_global_init(CURL_GLOBAL_DEFAULT) }
});
static CURL_PROTOCOLS_STR_UNSUPPORTED_VERSION: LazyLock<Option<Cow<'static, str>>> =
    LazyLock::new(|| {
        // SAFETY: callers force this after curl_global_init has completed, and libcurl returns a
        // process-wide immutable version info pointer.
        NonNull::new(unsafe { sys::curl_version_info(CURLVERSION_NOW) }).map_or(
            Some(Cow::Borrowed("unknown")),
            |version_info| {
                // SAFETY: version_info is non-null and points to libcurl's version info.
                let version_info_ref = unsafe { version_info.as_ref() };
                if version_info_ref.version_num >= CURL_MIN_PROTOCOLS_STR_VERSION {
                    None
                } else {
                    let version_ptr = version_info_ref.version;
                    let version = if version_ptr.is_null() {
                        Cow::Borrowed("unknown")
                    } else {
                        // SAFETY: libcurl documents version as an ASCII NUL-terminated string.
                        Cow::Owned(
                            unsafe { CStr::from_ptr(version_ptr) }
                                .to_string_lossy()
                                .into_owned(),
                        )
                    };
                    Some(version)
                }
            },
        )
    });
#[repr(C)]
struct Curl {
    _data: (),
    _marker: PhantomData<(*mut u8, PhantomPinned)>,
}
type CurlCode = c_uint;
type CurlInfo = c_uint;
type CurlOffT = i64;
type CurlOption = c_uint;
type CurlVersion = c_uint;
#[repr(C)]
struct CurlVersionInfoData {
    age: CurlVersion,
    version: *const c_char,
    version_num: c_uint,
}
const _: () = assert!(
    size_of::<CurlVersionInfoData>() == size_of::<[*const c_char; 3]>()
        && align_of::<CurlVersionInfoData>() == align_of::<*const c_char>()
        && offset_of!(CurlVersionInfoData, age) == 0
        && offset_of!(CurlVersionInfoData, version) == size_of::<*const c_char>()
        && offset_of!(CurlVersionInfoData, version_num) == size_of::<[*const c_char; 2]>(),
    "libcurl version info prefix ABI mismatch"
);
#[derive(Default)]
pub(super) struct Client {
    easy_handle: Option<EasyHandle>,
    header_buffer: Vec<u8>,
    header_build_buffer: Vec<u8>,
    url_buffer: Vec<u8>,
}
#[repr(C)]
struct CurlSlist {
    _data: (),
    _marker: PhantomData<(*mut u8, PhantomPinned)>,
}
struct EasyHandle(NonNull<Curl>);
struct HeaderList(Option<NonNull<CurlSlist>>);
struct BoundedResponseBuffer {
    bytes: Vec<u8>,
    error: Option<DownloadError>,
    label: &'static str,
    limit: usize,
}
enum CurlWriteTarget<'target, 'writer> {
    Buffer(&'target mut BoundedResponseBuffer),
    Stream(&'target mut StreamingBodySink<'writer>),
}
impl CurlWriteTarget<'_, '_> {
    const fn take_error(&mut self) -> Option<DownloadError> {
        match *self {
            Self::Buffer(ref mut buffer) => buffer.error.take(),
            Self::Stream(ref mut stream) => stream.error.take(),
        }
    }
}
impl Drop for EasyHandle {
    fn drop(&mut self) {
        // SAFETY: self.0 is an easy handle returned by libcurl and is closed exactly once here.
        unsafe {
            sys::curl_easy_cleanup(self.0.as_ptr());
        }
    }
}
impl Drop for HeaderList {
    fn drop(&mut self) {
        if let Some(list) = self.0 {
            // SAFETY: list is a curl_slist allocated by libcurl and is freed exactly once here.
            unsafe {
                sys::curl_slist_free_all(list.as_ptr());
            }
        }
    }
}
impl EasyHandle {
    const fn as_ptr(&self) -> *mut Curl {
        self.0.as_ptr()
    }
    fn ensure_https_scheme(&self) -> DownloadResult<()> {
        let mut scheme = null_mut::<c_char>();
        // SAFETY: scheme is a valid output pointer for CURLINFO_SCHEME.
        let status_code =
            unsafe { sys::curl_easy_getinfo(self.as_ptr(), CURLINFO_SCHEME, &raw mut scheme) };
        if status_code != CURLE_OK {
            return Err(curl_error("curl_easy_getinfo scheme", status_code).into());
        }
        let Some(scheme_ptr) = NonNull::new(scheme) else {
            return Err("curl 최종 scheme이 비어 있습니다.".into());
        };
        // SAFETY: libcurl returns a NUL-terminated scheme string owned by the easy handle.
        let scheme_bytes = unsafe { CStr::from_ptr(scheme_ptr.as_ptr()) }.to_bytes();
        if scheme_bytes.eq_ignore_ascii_case(b"https") {
            Ok(())
        } else {
            Err(format!(
                "curl 최종 scheme이 HTTPS가 아닙니다: {}",
                String::from_utf8_lossy(scheme_bytes)
            )
            .into())
        }
    }
    fn perform(&self) -> CurlCode {
        // SAFETY: self.0 is a configured easy handle and callback data live until this call returns.
        unsafe { sys::curl_easy_perform(self.as_ptr()) }
    }
    fn reset(&self) {
        // SAFETY: self.0 is a valid easy handle; reset only clears options and keeps connection cache.
        unsafe {
            sys::curl_easy_reset(self.as_ptr());
        }
    }
    fn response_code(&self) -> DownloadResult<c_long> {
        let mut raw_status = c_long::default();
        // SAFETY: raw_status is a valid output pointer for CURLINFO_RESPONSE_CODE.
        let status_code = unsafe {
            sys::curl_easy_getinfo(self.as_ptr(), CURLINFO_RESPONSE_CODE, &raw mut raw_status)
        };
        if status_code == CURLE_OK {
            Ok(raw_status)
        } else {
            Err(curl_error("curl_easy_getinfo response_code", status_code).into())
        }
    }
    fn setopt_callback(
        &self,
        option: CurlOption,
        value: unsafe extern "C" fn(*mut c_char, usize, usize, *mut c_void) -> usize,
    ) -> DownloadResult<()> {
        curl_setopt!(self, option, value)
    }
    fn setopt_long(&self, option: CurlOption, value: c_long) -> DownloadResult<()> {
        curl_setopt!(self, option, value)
    }
    fn setopt_off_t(&self, option: CurlOption, value: CurlOffT) -> DownloadResult<()> {
        curl_setopt!(self, option, value)
    }
    fn setopt_ptr<T>(&self, option: CurlOption, value: *const T) -> DownloadResult<()> {
        curl_setopt!(self, option, value)
    }
    fn setopt_str(&self, option: CurlOption, value: *const c_char) -> DownloadResult<()> {
        curl_setopt!(self, option, value)
    }
}
impl HeaderList {
    fn append(&mut self, header: &CStr) -> DownloadResult<()> {
        // SAFETY: header is a valid NUL-terminated string and self.0 is null or a libcurl list.
        let updated_ptr = unsafe { sys::curl_slist_append(self.as_ptr(), header.as_ptr()) };
        let Some(updated) = NonNull::new(updated_ptr) else {
            return Err("curl_slist_append 실패".into());
        };
        self.0 = Some(updated);
        Ok(())
    }
    const fn as_ptr(&self) -> *mut CurlSlist {
        let Some(list) = self.0 else {
            return null_mut();
        };
        list.as_ptr()
    }
}
impl Client {
    fn perform_request(
        &mut self,
        method: HttpMethod<'_>,
        host: &str,
        path: &str,
        request_headers: &[(&str, &str)],
        mut body_target: CurlWriteTarget<'_, '_>,
        header_buffer: &mut BoundedResponseBuffer,
    ) -> DownloadResult<u32> {
        let mut url_buffer = mem::take(&mut self.url_buffer);
        let mut header_build_buffer = mem::take(&mut self.header_build_buffer);
        let mut error_buffer = [c_char::default(); CURL_ERROR_SIZE];
        let result = (|| {
            let mut header_list = HeaderList(None);
            for &(name, value) in request_headers {
                let header = nul_terminated_buffer(
                    &mut header_build_buffer,
                    &[name.as_bytes(), b": ", value.as_bytes()],
                    "HTTP header",
                )?;
                header_list.append(header)?;
            }
            let url = nul_terminated_buffer(
                &mut url_buffer,
                &[HTTPS_SCHEME_PREFIX.as_bytes(), host.as_bytes(), path.as_bytes()],
                "URL",
            )?;
            let (perform_code, response_code) = self.with_reusable_handle(|handle| {
                handle.setopt_str(CURLOPT_URL, url.as_ptr())?;
                handle.setopt_ptr(CURLOPT_HTTPHEADER, header_list.as_ptr())?;
                handle.setopt_ptr(CURLOPT_ERRORBUFFER, error_buffer.as_mut_ptr())?;
                handle.setopt_callback(CURLOPT_WRITEFUNCTION, write_callback)?;
                handle.setopt_callback(CURLOPT_HEADERFUNCTION, write_callback)?;
                for (option, value) in [
                    (CURLOPT_CONNECTTIMEOUT_MS, 30_000),
                    (CURLOPT_TIMEOUT_MS, 60_000),
                    (CURLOPT_FOLLOWLOCATION, 0),
                    (CURLOPT_NOSIGNAL, 1),
                    (
                        CURLOPT_SSLVERSION,
                        CURL_SSLVERSION_TLSV1_2 | CURL_SSLVERSION_MAX_DEFAULT,
                    ),
                ] {
                    handle.setopt_long(option, value)?;
                }
                handle.setopt_str(CURLOPT_PROTOCOLS_STR, HTTPS_PROTOCOL.as_ptr())?;
                let max_file_size = CurlOffT::try_from(HTTP_MAX_BODY_BYTES).map_err(|source| {
                    download_error_with_source("HTTP 본문 한도 변환 실패", source)
                })?;
                handle.setopt_off_t(CURLOPT_MAXFILESIZE_LARGE, max_file_size)?;
                match method {
                    HttpMethod::Get => handle.setopt_long(CURLOPT_HTTPGET, 1)?,
                    HttpMethod::Post(body_bytes) => {
                        handle.setopt_long(CURLOPT_POST, 1)?;
                        let body_len = c_long::try_from(body_bytes.len()).map_err(|source| {
                            download_error_with_source("요청 본문 길이 변환 실패", source)
                        })?;
                        handle.setopt_long(CURLOPT_POSTFIELDSIZE, body_len)?;
                        if !body_bytes.is_empty() {
                            handle.setopt_ptr(
                                CURLOPT_POSTFIELDS,
                                body_bytes.as_ptr().cast::<c_char>(),
                            )?;
                        }
                    }
                }
                let mut header_target = CurlWriteTarget::Buffer(header_buffer);
                let body_data = (&raw mut body_target).cast::<c_void>();
                let header_data = (&raw mut header_target).cast::<c_void>();
                handle.setopt_ptr(CURLOPT_WRITEDATA, body_data)?;
                handle.setopt_ptr(CURLOPT_HEADERDATA, header_data)?;
                let perform_code = handle.perform();
                if perform_code == CURLE_OK {
                    handle.ensure_https_scheme()?;
                    Ok((perform_code, Some(handle.response_code()?)))
                } else {
                    Ok((perform_code, None))
                }
            })?;
            if let Some(callback_error) = body_target
                .take_error()
                .or_else(|| header_buffer.error.take())
            {
                self.easy_handle = None;
                return Err(callback_error);
            }
            if perform_code != CURLE_OK {
                let bytes = error_buffer.map(|ch| ch.to_le_bytes()[0]);
                let perform_error = if let Ok(message_cstr) = CStr::from_bytes_until_nul(&bytes)
                    && !message_cstr.to_bytes().is_empty()
                {
                    let message = message_cstr.to_string_lossy();
                    format!("curl_easy_perform 실패: {message} ({perform_code})")
                } else {
                    curl_error("curl_easy_perform", perform_code)
                };
                self.easy_handle = None;
                return Err(perform_error.into());
            }
            let raw_status = response_code.ok_or("curl response code가 비어 있습니다.")?;
            u32::try_from(raw_status)
                .map_err(|source| download_error_with_source("HTTP 상태 코드 변환 실패", source))
        })();
        self.header_build_buffer = header_build_buffer;
        self.url_buffer = url_buffer;
        result
    }
    pub(super) fn request(
        &mut self,
        method: HttpMethod<'_>,
        host: &str,
        path: &str,
        request_headers: &[(&str, &str)],
    ) -> DownloadResult<HttpResponse> {
        let header_bytes = mem::take(&mut self.header_buffer);
        let mut body_buffer =
            BoundedResponseBuffer::from_bytes("본문", HTTP_MAX_BODY_BYTES, Vec::new());
        let mut header_buffer =
            BoundedResponseBuffer::from_bytes("헤더", HTTP_MAX_HEADER_BYTES, header_bytes);
        let result = (|| {
            let status = self.perform_request(
                method,
                host,
                path,
                request_headers,
                CurlWriteTarget::Buffer(&mut body_buffer),
                &mut header_buffer,
            )?;
            let headers = parsed_headers_from_bytes(&header_buffer.bytes)?;
            let content_length = validated_http_content_length(&headers, HTTP_MAX_BODY_BYTES)?;
            enforce_http_body_length(body_buffer.bytes.len(), content_length)?;
            let body = mem::take(&mut body_buffer.bytes);
            Ok(HttpResponse {
                body,
                headers,
                status,
            })
        })();
        self.header_buffer = header_buffer.into_reusable_bytes();
        result
    }
    pub(super) fn request_to_writer(
        &mut self,
        method: HttpMethod<'_>,
        host: &str,
        path: &str,
        request_headers: &[(&str, &str)],
        writer: &mut dyn IoWrite,
    ) -> DownloadResult<HttpStreamResponse> {
        let mut body_sink = StreamingBodySink {
            error: None,
            limit: HTTP_MAX_BODY_BYTES,
            summary: StreamedBodySummary {
                bytes_seen: 0,
                preview: Vec::new(),
            },
            writer,
        };
        let header_bytes = mem::take(&mut self.header_buffer);
        let mut header_buffer =
            BoundedResponseBuffer::from_bytes("헤더", HTTP_MAX_HEADER_BYTES, header_bytes);
        let result = (|| {
            let status = self.perform_request(
                method,
                host,
                path,
                request_headers,
                CurlWriteTarget::Stream(&mut body_sink),
                &mut header_buffer,
            )?;
            let headers = parsed_headers_from_bytes(&header_buffer.bytes)?;
            let content_length = validated_http_content_length(&headers, HTTP_MAX_BODY_BYTES)?;
            enforce_http_body_length(body_sink.summary.bytes_seen, content_length)?;
            Ok(HttpStreamResponse {
                body: body_sink.summary,
                headers,
                status,
            })
        })();
        self.header_buffer = header_buffer.into_reusable_bytes();
        result
    }
    fn with_reusable_handle<R>(
        &mut self,
        action: impl FnOnce(&EasyHandle) -> DownloadResult<R>,
    ) -> DownloadResult<R> {
        let init_code = *CURL_INIT;
        if init_code != CURLE_OK {
            return Err(curl_error("curl_global_init", init_code).into());
        }
        let handle = match &mut self.easy_handle {
            &mut Some(ref mut handle) => handle,
            empty @ &mut None => {
                if let Some(version) = CURL_PROTOCOLS_STR_UNSUPPORTED_VERSION.as_ref() {
                    return Err(format!(
                        "libcurl {version}은 HTTPS protocol 제한 최신 API를 지원하지 않습니다. libcurl 7.85.0 이상이 필요합니다."
                    )
                    .into());
                }
                // SAFETY: curl_easy_init has no preconditions after global init.
                let raw_handle_ptr = unsafe { sys::curl_easy_init() };
                let Some(raw_handle) = NonNull::new(raw_handle_ptr) else {
                    return Err("curl_easy_init 실패".into());
                };
                empty.insert(EasyHandle(raw_handle))
            }
        };
        handle.reset();
        action(handle)
    }
}
impl BoundedResponseBuffer {
    fn append(&mut self, bytes: &[u8]) -> bool {
        let Some(next_len) = self.bytes.len().checked_add(bytes.len()) else {
            self.error = Some(format!(
                "HTTP 응답 {} 크기 계산 실패",
                self.label
            )
            .into());
            return false;
        };
        if next_len > self.limit {
            self.error = Some(format!(
                "HTTP 응답 {} 크기가 허용 한도({} bytes)를 초과했습니다.",
                self.label, self.limit
            )
            .into());
            return false;
        }
        if self.bytes.capacity() < next_len
            && let Err(source) = self.bytes.try_reserve(bytes.len())
        {
            self.error = Some(download_error_with_source(
                format!("HTTP 응답 {} 메모리 확보 실패", self.label),
                source,
            ));
            return false;
        }
        self.bytes.extend_from_slice(bytes);
        true
    }
    fn from_bytes(label: &'static str, limit: usize, mut bytes: Vec<u8>) -> Self {
        bytes.clear();
        Self {
            bytes,
            error: None,
            label,
            limit,
        }
    }
    fn into_reusable_bytes(mut self) -> Vec<u8> {
        self.bytes.clear();
        self.bytes
    }
}
fn nul_terminated_buffer<'buffer>(
    out: &'buffer mut Vec<u8>,
    parts: &[&[u8]],
    label: &str,
) -> DownloadResult<&'buffer CStr> {
    let capacity = parts
        .iter()
        .try_fold(1_usize, |capacity, part| capacity.checked_add(part.len()))
        .ok_or_else(|| format!("{label} 용량 계산 실패"))?;
    out.clear();
    if out.capacity() < capacity {
        out.try_reserve_exact(capacity).map_err(|source| {
            download_error_with_source(format!("{label} 메모리 확보 실패"), source)
        })?;
    }
    for part in parts {
        out.extend_from_slice(part);
    }
    out.push(0);
    CStr::from_bytes_with_nul(out).map_err(|source| {
        download_error_with_source(format!("{label}에 NUL 문자가 포함되어 있습니다"), source)
    })
}
fn parsed_headers_from_bytes(header_bytes: &[u8]) -> DownloadResult<Vec<HttpHeader>> {
    let separator: &[u8] = if header_bytes
        .array_windows::<4>()
        .any(|window| window == b"\r\n\r\n")
    {
        b"\r\n\r\n"
    } else {
        b"\n\n"
    };
    let mut selected_block = None;
    let mut rest = header_bytes;
    loop {
        let (block, next) = match rest.windows(separator.len()).position(|window| window == separator)
        {
            Some(index) => {
                let block = rest
                    .get(..index)
                    .ok_or("HTTP header block 범위 계산 실패")?;
                let next_start = index
                    .checked_add(separator.len())
                    .ok_or("HTTP header block cursor 계산 실패")?;
                let next = rest
                    .get(next_start..)
                    .ok_or("HTTP header block cursor 범위 계산 실패")?;
                (block, next)
            }
            None => (rest, &[][..]),
        };
        if block
            .split(|byte| *byte == b'\n')
            .next()
            .is_some_and(|line| line.starts_with(b"HTTP/"))
        {
            selected_block = Some(block);
        }
        if next.is_empty() {
            break;
        }
        rest = next;
    }
    let Some(header_block) = selected_block else {
        return Ok(Vec::new());
    };
    let mut headers = Vec::new();
    for raw_line in header_block.split(|byte| *byte == b'\n').skip(1) {
        let line = raw_line.strip_suffix(b"\r").unwrap_or(raw_line).trim_ascii();
        let Some(colon) = line.iter().position(|byte| *byte == b':') else {
            continue;
        };
        let (raw_name, tail) = line.split_at(colon);
        let Some((_, raw_value_tail)) = tail.split_first() else {
            continue;
        };
        let name = raw_name.trim_ascii();
        let raw_value = raw_value_tail.trim_ascii();
        let kind = if name.eq_ignore_ascii_case(RESPONSE_HEADER_CONTENT_LENGTH) {
            HttpHeaderKind::ContentLength
        } else if name.eq_ignore_ascii_case(RESPONSE_HEADER_SET_COOKIE) {
            HttpHeaderKind::SetCookie
        } else {
            continue;
        };
        if headers.len() == headers.capacity() {
            headers.try_reserve(1).map_err(|source| {
                download_error_with_source("HTTP header 목록 메모리 확보 실패", source)
            })?;
        }
        let value = str::from_utf8(raw_value)
            .map_err(|source| download_error_with_source("HTTP header 값 UTF-8 변환 실패", source))?;
        let mut header_value = String::new();
        header_value
            .try_reserve_exact(value.len())
            .map_err(|source| download_error_with_source("HTTP header 값 메모리 확보 실패", source))?;
        header_value.push_str(value);
        headers.push(HttpHeader {
            kind,
            value: header_value,
        });
    }
    Ok(headers)
}
fn curl_error(context: &str, code: CurlCode) -> String {
    // SAFETY: curl_easy_strerror returns either null or a static NUL-terminated message for code.
    let raw_ptr = unsafe { sys::curl_easy_strerror(code) };
    let message = if raw_ptr.is_null() {
        Cow::Borrowed("unknown curl error")
    } else {
        // SAFETY: libcurl guarantees a valid NUL-terminated string for non-null strerror results.
        unsafe { CStr::from_ptr(raw_ptr) }.to_string_lossy()
    };
    format!("{context} 실패: {message} ({code})")
}
unsafe extern "C" fn write_callback(
    ptr: *mut c_char,
    size: usize,
    nmemb: usize,
    userdata: *mut c_void,
) -> usize {
    let Some(len) = size.checked_mul(nmemb) else {
        return 0;
    };
    if len == 0 {
        return 0;
    }
    let Some(payload_head) = NonNull::new(ptr.cast::<u8>()) else {
        return 0;
    };
    let Some(mut target_ptr) = NonNull::new(userdata.cast::<CurlWriteTarget<'_, '_>>()) else {
        return 0;
    };
    // SAFETY: len is non-zero, payload_head is non-null, and libcurl passes a readable buffer with
    // len bytes for this callback.
    let bytes = unsafe { slice::from_raw_parts(payload_head.as_ptr(), len) };
    // SAFETY: userdata is the write target pointer configured before curl_easy_perform.
    let target = unsafe { target_ptr.as_mut() };
    let appended = match *target {
        CurlWriteTarget::Buffer(ref mut buffer) => buffer.append(bytes),
        CurlWriteTarget::Stream(ref mut stream) => match stream.append(bytes) {
            Ok(()) => true,
            Err(error) => {
                stream.error = Some(error);
                false
            }
        },
    };
    if !appended {
        return 0;
    }
    len
}
