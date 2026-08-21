use super::{
    DownloadResult, HTTP_MAX_BODY_BYTES, HTTP_MAX_HEADER_BYTES, HttpResponse, RequestHeaders,
    ResponseHeaders, checked_http_buffer_len, download_error_with_source,
};
use crate::diagnostic::AppError as DownloadError;
use alloc::{borrow::Cow, string::String, vec::Vec};
use core::{
    ffi::{CStr, c_char, c_long, c_uint, c_void},
    marker::{PhantomData, PhantomPinned},
    mem,
    ptr::{NonNull, null_mut},
    slice, str,
};
use std::sync::LazyLock;
mod sys;
macro_rules! curl_setopt {
    ($handle:expr, $option:expr, $value:expr) => {{
        // SAFETY: call sites pair each option with a wrapper using its documented libcurl ABI type.
        let code = unsafe { sys::curl_easy_setopt($handle.as_ptr(), $option, $value) };
        (code == CURLE_OK).ok_or_else(|| curl_error("curl_easy_setopt", code).into())
    }};
}
const CURLE_OK: CurlCode = 0;
const CURL_ERROR_SIZE: usize = 256;
const CURL_GLOBAL_DEFAULT: c_long = 3;
const CURLINFO_RESPONSE_CODE: CurlInfo = 0x20_0002;
const CURLINFO_SCHEME: CurlInfo = 0x10_0031;
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
const RESPONSE_HEADER_CONTENT_LENGTH: &[u8] = b"Content-Length";
const RESPONSE_HEADER_SET_COOKIE: &[u8] = b"Set-Cookie";
static CURL_INIT: LazyLock<CurlCode> = LazyLock::new(|| {
    // SAFETY: LazyLock runs this initializer once before any easy handles are used.
    unsafe { sys::curl_global_init(CURL_GLOBAL_DEFAULT) }
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
#[derive(Default)]
pub(super) struct Client {
    easy_handle: Option<EasyHandle>,
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
struct ResponseBody {
    bytes: Vec<u8>,
    error: Option<DownloadError>,
}
#[derive(Default)]
struct CapturedHeaderBlock {
    error: Option<DownloadError>,
    headers: ResponseHeaders,
}
struct CurlHeaderCapture {
    bytes_seen: usize,
    completed_block: Option<CapturedHeaderBlock>,
    current_block: Option<CapturedHeaderBlock>,
    error: Option<DownloadError>,
}
enum CurlWriteTarget<'target> {
    Body(&'target mut ResponseBody),
    Header(&'target mut CurlHeaderCapture),
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
        (status_code == CURLE_OK)
            .ok_or_else(|| curl_error("curl_easy_getinfo scheme", status_code))?;
        let Some(scheme_ptr) = NonNull::new(scheme) else {
            return Err("curl 최종 scheme이 비어 있습니다.".into());
        };
        // SAFETY: libcurl returns a NUL-terminated scheme string owned by the easy handle.
        let scheme_bytes = unsafe { CStr::from_ptr(scheme_ptr.as_ptr()) }.to_bytes();
        scheme_bytes.eq_ignore_ascii_case(b"https").ok_or_else(|| {
            format!(
                "curl 최종 scheme이 HTTPS가 아닙니다: {}",
                String::from_utf8_lossy(scheme_bytes)
            )
            .into()
        })
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
        (status_code == CURLE_OK)
            .ok_or_else(|| curl_error("curl_easy_getinfo response_code", status_code))?;
        Ok(raw_status)
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
    fn execute_request(
        &mut self,
        request_body: Option<&[u8]>,
        host: &str,
        path: &str,
        request_headers: &RequestHeaders<'_>,
        body_buffer: &mut ResponseBody,
        header_capture: &mut CurlHeaderCapture,
    ) -> DownloadResult<u32> {
        let mut url_buffer = mem::take(&mut self.url_buffer);
        let mut header_build_buffer = mem::take(&mut self.header_build_buffer);
        let mut error_buffer = [c_char::default(); CURL_ERROR_SIZE];
        let result = (|| {
            let mut header_list = HeaderList(None);
            for (name, value) in request_headers.iter() {
                let header = nul_terminated_buffer(
                    &mut header_build_buffer,
                    &[name.as_bytes(), b": ", value.as_bytes()],
                    "HTTP header",
                )?;
                header_list.append(header)?;
            }
            let url = nul_terminated_buffer(
                &mut url_buffer,
                &[
                    HTTPS_SCHEME_PREFIX.as_bytes(),
                    host.as_bytes(),
                    path.as_bytes(),
                ],
                "URL",
            )?;
            let init_code = *CURL_INIT;
            (init_code == CURLE_OK).ok_or_else(|| curl_error("curl_global_init", init_code))?;
            let handle = match &mut self.easy_handle {
                &mut Some(ref mut handle) => handle,
                empty @ &mut None => {
                    // SAFETY: curl_easy_init has no preconditions after global init.
                    let raw_handle_ptr = unsafe { sys::curl_easy_init() };
                    let Some(raw_handle) = NonNull::new(raw_handle_ptr) else {
                        return Err("curl_easy_init 실패".into());
                    };
                    empty.insert(EasyHandle(raw_handle))
                }
            };
            handle.reset();
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
            let max_file_size = CurlOffT::try_from(HTTP_MAX_BODY_BYTES)
                .map_err(|source| download_error_with_source("HTTP 본문 한도 변환 실패", source))?;
            handle.setopt_off_t(CURLOPT_MAXFILESIZE_LARGE, max_file_size)?;
            if let Some(body_bytes) = request_body {
                handle.setopt_long(CURLOPT_POST, 1)?;
                let body_len = c_long::try_from(body_bytes.len()).map_err(|source| {
                    download_error_with_source("요청 본문 길이 변환 실패", source)
                })?;
                handle.setopt_long(CURLOPT_POSTFIELDSIZE, body_len)?;
                if !body_bytes.is_empty() {
                    handle.setopt_ptr(CURLOPT_POSTFIELDS, body_bytes.as_ptr().cast::<c_char>())?;
                }
            } else {
                handle.setopt_long(CURLOPT_HTTPGET, 1)?;
            }
            let perform_code = {
                let mut body_target = CurlWriteTarget::Body(&mut *body_buffer);
                let mut header_target = CurlWriteTarget::Header(&mut *header_capture);
                let body_data = (&raw mut body_target).cast::<c_void>();
                let header_data = (&raw mut header_target).cast::<c_void>();
                handle.setopt_ptr(CURLOPT_WRITEDATA, body_data)?;
                handle.setopt_ptr(CURLOPT_HEADERDATA, header_data)?;
                handle.perform()
            };
            if let Some(callback_error) = body_buffer
                .error
                .take()
                .or_else(|| header_capture.error.take())
            {
                self.easy_handle = None;
                return Err(callback_error);
            }
            if perform_code != CURLE_OK {
                let bytes = error_buffer.map(|ch| ch.to_le_bytes()[0]);
                let perform_error = if let Ok(message_cstr) = CStr::from_bytes_until_nul(&bytes)
                    && !message_cstr.is_empty()
                {
                    let message = message_cstr.to_string_lossy();
                    format!("curl_easy_perform 실패: {message} ({perform_code})")
                } else {
                    curl_error("curl_easy_perform", perform_code)
                };
                self.easy_handle = None;
                return Err(perform_error.into());
            }
            handle.ensure_https_scheme()?;
            let raw_status = handle.response_code()?;
            u32::try_from(raw_status)
                .map_err(|source| download_error_with_source("HTTP 상태 코드 변환 실패", source))
        })();
        self.header_build_buffer = header_build_buffer;
        self.url_buffer = url_buffer;
        result
    }
    pub(super) fn request(
        &mut self,
        request_body: Option<&[u8]>,
        host: &str,
        path: &str,
        request_headers: RequestHeaders<'_>,
    ) -> DownloadResult<HttpResponse> {
        let mut body_buffer = ResponseBody {
            bytes: Vec::new(),
            error: None,
        };
        let mut header_capture = CurlHeaderCapture {
            bytes_seen: 0,
            completed_block: None,
            current_block: None,
            error: None,
        };
        let status = self.execute_request(
            request_body,
            host,
            path,
            &request_headers,
            &mut body_buffer,
            &mut header_capture,
        )?;
        let final_block = header_capture
            .completed_block
            .ok_or("완료된 HTTP 응답 header block을 찾지 못했습니다.")?;
        if let Some(header_error) = final_block.error {
            return Err(header_error);
        }
        let headers = final_block.headers;
        if let Some(expected_len) = headers.content_length
            && body_buffer.bytes.len() != expected_len
        {
            return Err(format!(
                "HTTP 응답 본문 길이가 Content-Length와 다릅니다: expected={expected_len}, actual={}",
                body_buffer.bytes.len()
            )
            .into());
        }
        Ok(HttpResponse {
            body: body_buffer.bytes,
            headers,
            status,
        })
    }
}
impl ResponseBody {
    fn append(&mut self, bytes: &[u8]) -> bool {
        let next_len = match checked_http_buffer_len(
            "본문",
            self.bytes.len(),
            bytes.len(),
            HTTP_MAX_BODY_BYTES,
        ) {
            Ok(next_len) => next_len,
            Err(error) => {
                self.error = Some(error);
                return false;
            }
        };
        if self.bytes.capacity() < next_len
            && let Err(source) = self.bytes.try_reserve(bytes.len())
        {
            self.error = Some(download_error_with_source(
                "HTTP 응답 본문 메모리 확보 실패",
                source,
            ));
            return false;
        }
        self.bytes.extend_from_slice(bytes);
        true
    }
}
impl CurlHeaderCapture {
    fn append(&mut self, bytes: &[u8]) -> bool {
        let next_len = match checked_http_buffer_len(
            "헤더",
            self.bytes_seen,
            bytes.len(),
            HTTP_MAX_HEADER_BYTES,
        ) {
            Ok(next_len) => next_len,
            Err(error) => {
                self.error = Some(error);
                return false;
            }
        };
        self.bytes_seen = next_len;
        let Some(line) = bytes.strip_suffix(b"\n") else {
            self.error = Some("libcurl이 종결되지 않은 HTTP header line을 전달했습니다.".into());
            return false;
        };
        self.capture_line(line);
        true
    }
    fn capture_line(&mut self, bytes: &[u8]) {
        let line = bytes.strip_suffix(b"\r").unwrap_or(bytes);
        if line.starts_with(b"HTTP/") {
            self.current_block = Some(CapturedHeaderBlock::default());
            return;
        }
        if line.is_empty() {
            if let Some(block) = self.current_block.take() {
                self.completed_block = Some(block);
            }
            return;
        }
        let Some(current_block) = self.current_block.as_mut() else {
            return;
        };
        if current_block.error.is_some() {
            return;
        }
        let trimmed_line = line.trim_ascii();
        let Some(colon) = trimmed_line.iter().position(|byte| *byte == b':') else {
            return;
        };
        let (raw_name, tail) = trimmed_line.split_at(colon);
        let Some((_, raw_value_tail)) = tail.split_first() else {
            return;
        };
        let name = raw_name.trim_ascii();
        let is_content_length = name.eq_ignore_ascii_case(RESPONSE_HEADER_CONTENT_LENGTH);
        if !(is_content_length || name.eq_ignore_ascii_case(RESPONSE_HEADER_SET_COOKIE)) {
            return;
        }
        let raw_value = raw_value_tail.trim_ascii();
        let value = match str::from_utf8(raw_value) {
            Ok(value) => value,
            Err(source) => {
                current_block.error = Some(download_error_with_source(
                    "HTTP header 값 UTF-8 변환 실패",
                    source,
                ));
                return;
            }
        };
        let capture_result = if is_content_length {
            current_block.headers.parse_content_length(value)
        } else {
            current_block.headers.push_set_cookie(value)
        };
        if let Err(source) = capture_result {
            current_block.error = Some(source);
        }
    }
}
fn nul_terminated_buffer<'buffer>(
    out: &'buffer mut Vec<u8>,
    parts: &[&[u8]],
    label: &str,
) -> DownloadResult<&'buffer CStr> {
    let capacity = parts
        .iter()
        .fold(1_usize, |capacity, part| capacity.strict_add(part.len()));
    out.clear();
    if out.capacity() < capacity {
        out.try_reserve_exact(capacity).map_err(|source| {
            download_error_with_source("libcurl 문자열 메모리 확보 실패", source)
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
    let Some(mut target_ptr) = NonNull::new(userdata.cast::<CurlWriteTarget<'_>>()) else {
        return 0;
    };
    // SAFETY: len is non-zero, payload_head is non-null, and libcurl passes a readable buffer with
    // len bytes for this callback.
    let bytes = unsafe { slice::from_raw_parts(payload_head.as_ptr(), len) };
    // SAFETY: userdata is the CurlWriteTarget pointer configured before curl_easy_perform.
    let target = unsafe { target_ptr.as_mut() };
    let accepted = match *target {
        CurlWriteTarget::Body(ref mut buffer) => (*buffer).append(bytes),
        CurlWriteTarget::Header(ref mut capture) => (*capture).append(bytes),
    };
    if !accepted {
        return 0;
    }
    len
}
