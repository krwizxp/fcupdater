use super::{
    DownloadError, DownloadResult, HTTP_MAX_BODY_BYTES, HTTP_MAX_HEADER_BYTES, HttpHeader,
    HttpHeaderKind, HttpResponse, HttpStreamResponse, StreamedBodySummary, StreamingBodySink,
    RESPONSE_HEADER_CONTENT_LENGTH, RESPONSE_HEADER_SET_COOKIE, download_error_with_source,
    enforce_http_body_length, validated_http_content_length,
    http_client::HttpMethod,
};
use alloc::{borrow::Cow, string::String, vec::Vec};
use core::{
    ffi::{CStr, c_char, c_int, c_long, c_uint, c_void},
    mem::{self, align_of, offset_of, size_of},
    ptr::{NonNull, null, null_mut},
    slice,
    str,
};
use std::{io::Write as IoWrite, sync::LazyLock};
mod sys {
    use super::{
        Curl, CurlCode, CurlInfo, CurlOption, CurlSlist, CurlVersion, CurlVersionInfoData, c_char,
        c_long,
    };
    #[link(name = "curl")]
    unsafe extern "C" {
        pub fn curl_easy_cleanup(curl: *mut Curl);
        pub fn curl_easy_getinfo(curl: *mut Curl, info: CurlInfo, ...) -> CurlCode;
        pub fn curl_easy_init() -> *mut Curl;
        pub fn curl_easy_perform(curl: *mut Curl) -> CurlCode;
        pub fn curl_easy_reset(curl: *mut Curl);
        pub fn curl_easy_setopt(curl: *mut Curl, option: CurlOption, ...) -> CurlCode;
        pub fn curl_easy_strerror(code: CurlCode) -> *const c_char;
        pub fn curl_global_init(flags: c_long) -> CurlCode;
        pub fn curl_version_info(age: CurlVersion) -> *const CurlVersionInfoData;
        pub fn curl_slist_append(
            list: *mut CurlSlist,
            string: *const c_char,
        ) -> *mut CurlSlist;
        pub fn curl_slist_free_all(list: *mut CurlSlist);
    }
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
const CURLOPT_REDIR_PROTOCOLS_STR: CurlOption = 10_319;
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
        NonNull::new(unsafe { sys::curl_version_info(CURLVERSION_NOW).cast_mut() }).map_or(
            Some(Cow::Borrowed("unknown")),
            |version_info| {
                // SAFETY: version_info is non-null and points to libcurl's version info.
                let version_info_ref = unsafe { version_info.as_ref() };
                if version_info_ref.version_num >= CURL_MIN_PROTOCOLS_STR_VERSION {
                    None
                } else {
                    Some(NonNull::new(version_info_ref.version.cast_mut()).map_or_else(
                        || Cow::Borrowed("unknown"),
                        |version_ptr| {
                            // SAFETY: libcurl documents version as an ASCII NUL-terminated string.
                            Cow::Owned(
                                unsafe { CStr::from_ptr(version_ptr.as_ptr()) }
                                    .to_string_lossy()
                                    .into_owned(),
                            )
                        },
                    ))
                }
            },
        )
    });
type Curl = c_void;
type CurlCode = c_int;
type CurlInfo = c_int;
type CurlOffT = i64;
type CurlOption = c_int;
type CurlVersion = c_int;
type CurlWriteCallback = unsafe extern "C" fn(*mut c_char, usize, usize, *mut c_void) -> usize;
#[repr(C)]
struct CurlVersionInfoData {
    age: CurlVersion,
    version: *const c_char,
    version_num: c_uint,
}
cfg_select! {
    target_pointer_width = "64" => {
        const _: () = assert!(
            size_of::<CurlVersionInfoData>() == 24,
            "libcurl version info prefix x64 size mismatch"
        );
        const _: () = assert!(
            align_of::<CurlVersionInfoData>() == 8,
            "libcurl version info prefix x64 align mismatch"
        );
        const _: () = assert!(
            offset_of!(CurlVersionInfoData, age) == 0,
            "libcurl version info prefix x64 age offset mismatch"
        );
        const _: () = assert!(
            offset_of!(CurlVersionInfoData, version) == 8,
            "libcurl version info prefix x64 version offset mismatch"
        );
        const _: () = assert!(
            offset_of!(CurlVersionInfoData, version_num) == 16,
            "libcurl version info prefix x64 version number offset mismatch"
        );
    }
    target_pointer_width = "32" => {
        const _: () = assert!(
            size_of::<CurlVersionInfoData>() == 12,
            "libcurl version info prefix x86 size mismatch"
        );
        const _: () = assert!(
            align_of::<CurlVersionInfoData>() == 4,
            "libcurl version info prefix x86 align mismatch"
        );
        const _: () = assert!(
            offset_of!(CurlVersionInfoData, age) == 0,
            "libcurl version info prefix x86 age offset mismatch"
        );
        const _: () = assert!(
            offset_of!(CurlVersionInfoData, version) == 4,
            "libcurl version info prefix x86 version offset mismatch"
        );
        const _: () = assert!(
            offset_of!(CurlVersionInfoData, version_num) == 8,
            "libcurl version info prefix x86 version number offset mismatch"
        );
    }
    _ => {}
}
pub(super) struct Client {
    easy_handle: Option<EasyHandle>,
    header_buffer: Vec<u8>,
    header_build_buffer: Vec<u8>,
    scheme_prefix: &'static str,
    url_buffer: Vec<u8>,
}
#[repr(C)]
struct CurlSlist {
    data: *mut c_char,
    next: *mut Self,
}
cfg_select! {
    target_pointer_width = "64" => {
        const _: () = assert!(size_of::<CurlSlist>() == 16, "libcurl slist x64 size mismatch");
        const _: () = assert!(align_of::<CurlSlist>() == 8, "libcurl slist x64 align mismatch");
        const _: () = assert!(offset_of!(CurlSlist, data) == 0, "libcurl slist x64 data offset mismatch");
        const _: () = assert!(offset_of!(CurlSlist, next) == 8, "libcurl slist x64 next offset mismatch");
    }
    target_pointer_width = "32" => {
        const _: () = assert!(size_of::<CurlSlist>() == 8, "libcurl slist x86 size mismatch");
        const _: () = assert!(align_of::<CurlSlist>() == 4, "libcurl slist x86 align mismatch");
        const _: () = assert!(offset_of!(CurlSlist, data) == 0, "libcurl slist x86 data offset mismatch");
        const _: () = assert!(offset_of!(CurlSlist, next) == 4, "libcurl slist x86 next offset mismatch");
    }
    _ => {}
}
struct EasyHandle(NonNull<Curl>);
struct HeaderList(Option<NonNull<CurlSlist>>);
struct BoundedResponseBuffer {
    bytes: Vec<u8>,
    error: Option<DownloadError>,
    label: &'static str,
    limit: usize,
}
struct CurlPerformResult {
    code: CurlCode,
    response_code: Option<c_long>,
}
struct ResponseBuffers {
    body: BoundedResponseBuffer,
    headers: BoundedResponseBuffer,
}
enum CurlWriteTarget<'target, 'writer> {
    Buffer(&'target mut BoundedResponseBuffer),
    Stream(&'target mut StreamingBodySink<'writer>),
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
        let mut scheme = null::<c_char>();
        // SAFETY: scheme is a valid output pointer for CURLINFO_SCHEME.
        let status_code =
            unsafe { sys::curl_easy_getinfo(self.as_ptr(), CURLINFO_SCHEME, &raw mut scheme) };
        if status_code != CURLE_OK {
            return Err(curl_error("curl_easy_getinfo scheme", status_code).into());
        }
        let Some(scheme_ptr) = NonNull::new(scheme.cast_mut()) else {
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
    fn setopt_callback(&self, option: CurlOption, value: CurlWriteCallback) -> DownloadResult<()> {
        // SAFETY: value is a libcurl-compatible callback function pointer.
        let code = unsafe { sys::curl_easy_setopt(self.as_ptr(), option, value) };
        if code == CURLE_OK {
            Ok(())
        } else {
            Err(curl_error("curl_easy_setopt", code).into())
        }
    }
    fn setopt_long(&self, option: CurlOption, value: c_long) -> DownloadResult<()> {
        // SAFETY: value is a scalar option value for the given CurlOption.
        let code = unsafe { sys::curl_easy_setopt(self.as_ptr(), option, value) };
        if code == CURLE_OK {
            Ok(())
        } else {
            Err(curl_error("curl_easy_setopt", code).into())
        }
    }
    fn setopt_off_t(&self, option: CurlOption, value: CurlOffT) -> DownloadResult<()> {
        // SAFETY: value is a curl_off_t scalar option value for the given CurlOption.
        let code = unsafe { sys::curl_easy_setopt(self.as_ptr(), option, value) };
        if code == CURLE_OK {
            Ok(())
        } else {
            Err(curl_error("curl_easy_setopt", code).into())
        }
    }
    fn setopt_ptr<T>(&self, option: CurlOption, value: *const T) -> DownloadResult<()> {
        // SAFETY: value is a pointer option that remains valid for the transfer duration.
        let code = unsafe { sys::curl_easy_setopt(self.as_ptr(), option, value) };
        if code == CURLE_OK {
            Ok(())
        } else {
            Err(curl_error("curl_easy_setopt", code).into())
        }
    }
    fn setopt_str(&self, option: CurlOption, value: *const c_char) -> DownloadResult<()> {
        // SAFETY: value is a valid NUL-terminated string that outlives the setopt call.
        let code = unsafe { sys::curl_easy_setopt(self.as_ptr(), option, value) };
        if code == CURLE_OK {
            Ok(())
        } else {
            Err(curl_error("curl_easy_setopt", code).into())
        }
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
    fn from_headers(
        request_headers: &[(&str, &str)],
        header_bytes: &mut Vec<u8>,
    ) -> DownloadResult<Self> {
        let mut header_list = Self(None);
        for &(name, value) in request_headers {
            let header_capacity = name
                .len()
                .checked_add(": ".len())
                .and_then(|capacity| capacity.checked_add(value.len()))
                .and_then(|capacity| capacity.checked_add(1))
                .ok_or("HTTP header 용량 계산 실패")?;
            header_bytes.clear();
            if header_bytes.capacity() < header_capacity {
                header_bytes.try_reserve_exact(header_capacity).map_err(|source| {
                    download_error_with_source("HTTP header 메모리 확보 실패", source)
                })?;
            }
            header_bytes.extend_from_slice(name.as_bytes());
            header_bytes.extend_from_slice(b": ");
            header_bytes.extend_from_slice(value.as_bytes());
            header_bytes.push(0);
            let header_c = CStr::from_bytes_with_nul(header_bytes)
                .map_err(|source| download_error_with_source("HTTP header C string 해석 실패", source))?;
            header_list.append(header_c)?;
        }
        Ok(header_list)
    }
}
impl Client {
    fn clear_reusable_handle(&mut self) {
        self.easy_handle = None;
    }
    fn configure_request(
        handle: &EasyHandle,
        url: &CStr,
        header_list: &HeaderList,
        error_buffer: &mut [c_char; CURL_ERROR_SIZE],
        method: HttpMethod<'_>,
    ) -> DownloadResult<()> {
        struct CurlLongOption {
            option: CurlOption,
            value: c_long,
        }
        handle.setopt_str(CURLOPT_URL, url.as_ptr())?;
        handle.setopt_ptr(CURLOPT_HTTPHEADER, header_list.as_ptr())?;
        handle.setopt_ptr(CURLOPT_ERRORBUFFER, error_buffer.as_mut_ptr())?;
        handle.setopt_callback(CURLOPT_WRITEFUNCTION, write_callback)?;
        handle.setopt_callback(CURLOPT_HEADERFUNCTION, write_callback)?;
        for setting in [
            CurlLongOption {
                option: CURLOPT_CONNECTTIMEOUT_MS,
                value: 30_000,
            },
            CurlLongOption {
                option: CURLOPT_TIMEOUT_MS,
                value: 60_000,
            },
            CurlLongOption {
                option: CURLOPT_NOSIGNAL,
                value: 1,
            },
            CurlLongOption {
                option: CURLOPT_SSLVERSION,
                value: CURL_SSLVERSION_TLSV1_2 | CURL_SSLVERSION_MAX_DEFAULT,
            },
        ] {
            handle.setopt_long(setting.option, setting.value)?;
        }
        handle.setopt_str(CURLOPT_PROTOCOLS_STR, HTTPS_PROTOCOL.as_ptr())?;
        handle.setopt_str(CURLOPT_REDIR_PROTOCOLS_STR, HTTPS_PROTOCOL.as_ptr())?;
        let max_file_size = CurlOffT::try_from(HTTP_MAX_BODY_BYTES)
            .map_err(|source| download_error_with_source("HTTP 본문 한도 변환 실패", source))?;
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
                    let post_fields = body_bytes.as_ptr().cast::<c_char>();
                    handle.setopt_ptr(CURLOPT_POSTFIELDS, post_fields)?;
                }
            }
        }
        Ok(())
    }
    pub(super) fn request(
        &mut self,
        method: HttpMethod<'_>,
        host: &str,
        path: &str,
        request_headers: &[(&str, &str)],
    ) -> DownloadResult<HttpResponse> {
        let mut url_buffer = mem::take(&mut self.url_buffer);
        let mut header_build_buffer = mem::take(&mut self.header_build_buffer);
        let mut error_buffer = [c_char::default(); CURL_ERROR_SIZE];
        let header_bytes = mem::take(&mut self.header_buffer);
        let mut response_buffers = ResponseBuffers {
            body: BoundedResponseBuffer::from_bytes("본문", HTTP_MAX_BODY_BYTES, Vec::new()),
            headers: BoundedResponseBuffer::from_bytes(
                "헤더",
                HTTP_MAX_HEADER_BYTES,
                header_bytes,
            ),
        };
        let result = (|| {
            let header_list = HeaderList::from_headers(request_headers, &mut header_build_buffer)?;
            let url = request_url(&mut url_buffer, self.scheme_prefix, host, path)?;
            let perform = self.with_reusable_handle(|handle| {
                Self::configure_request(
                    handle,
                    url,
                    &header_list,
                    &mut error_buffer,
                    method,
                )?;
                let perform_code = {
                    let mut body_target = CurlWriteTarget::Buffer(&mut response_buffers.body);
                    let mut header_target = CurlWriteTarget::Buffer(&mut response_buffers.headers);
                    let body_data = (&raw mut body_target).cast::<c_void>();
                    let header_data = (&raw mut header_target).cast::<c_void>();
                    handle.setopt_ptr(CURLOPT_WRITEDATA, body_data)?;
                    handle.setopt_ptr(CURLOPT_HEADERDATA, header_data)?;
                    handle.perform()
                };
                if perform_code == CURLE_OK {
                    Ok(CurlPerformResult {
                        code: perform_code,
                        response_code: {
                            handle.ensure_https_scheme()?;
                            Some(handle.response_code()?)
                        },
                    })
                } else {
                    Ok(CurlPerformResult {
                        code: perform_code,
                        response_code: None,
                    })
                }
            })?;
            if let Some(callback_error) = response_buffers
                .body
                .error
                .take()
                .or_else(|| response_buffers.headers.error.take())
            {
                self.clear_reusable_handle();
                return Err(callback_error);
            }
            if perform.code != CURLE_OK {
                let perform_error = curl_perform_error(error_buffer, perform.code);
                self.clear_reusable_handle();
                return Err(perform_error.into());
            }
            let raw_status = perform
                .response_code
                .ok_or("curl response code가 비어 있습니다.")?;
            let status = u32::try_from(raw_status)
                .map_err(|source| download_error_with_source("HTTP 상태 코드 변환 실패", source))?;
            let headers = parsed_headers_from_bytes(&response_buffers.headers.bytes)?;
            let content_length = validated_http_content_length(&headers, HTTP_MAX_BODY_BYTES)?;
            enforce_http_body_length(response_buffers.body.bytes.len(), content_length)?;
            let body = mem::take(&mut response_buffers.body.bytes);
            Ok(HttpResponse {
                body,
                headers,
                status,
            })
        })();
        self.header_buffer = response_buffers.headers.into_reusable_bytes();
        self.header_build_buffer = header_build_buffer;
        self.url_buffer = url_buffer;
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
        let mut url_buffer = mem::take(&mut self.url_buffer);
        let mut header_build_buffer = mem::take(&mut self.header_build_buffer);
        let mut error_buffer = [c_char::default(); CURL_ERROR_SIZE];
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
            let header_list = HeaderList::from_headers(request_headers, &mut header_build_buffer)?;
            let url = request_url(&mut url_buffer, self.scheme_prefix, host, path)?;
            let perform = self.with_reusable_handle(|handle| {
                Self::configure_request(
                    handle,
                    url,
                    &header_list,
                    &mut error_buffer,
                    method,
                )?;
                let perform_code = {
                    let mut body_target = CurlWriteTarget::Stream(&mut body_sink);
                    let mut header_target = CurlWriteTarget::Buffer(&mut header_buffer);
                    let body_data = (&raw mut body_target).cast::<c_void>();
                    let header_data = (&raw mut header_target).cast::<c_void>();
                    handle.setopt_ptr(CURLOPT_WRITEDATA, body_data)?;
                    handle.setopt_ptr(CURLOPT_HEADERDATA, header_data)?;
                    handle.perform()
                };
                if perform_code == CURLE_OK {
                    Ok(CurlPerformResult {
                        code: perform_code,
                        response_code: {
                            handle.ensure_https_scheme()?;
                            Some(handle.response_code()?)
                        },
                    })
                } else {
                    Ok(CurlPerformResult {
                        code: perform_code,
                        response_code: None,
                    })
                }
            })?;
            if let Some(callback_error) =
                body_sink.error.take().or_else(|| header_buffer.error.take())
            {
                self.clear_reusable_handle();
                return Err(callback_error);
            }
            if perform.code != CURLE_OK {
                let perform_error = curl_perform_error(error_buffer, perform.code);
                self.clear_reusable_handle();
                return Err(perform_error.into());
            }
            let raw_status = perform
                .response_code
                .ok_or("curl response code가 비어 있습니다.")?;
            let status = u32::try_from(raw_status)
                .map_err(|source| download_error_with_source("HTTP 상태 코드 변환 실패", source))?;
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
        self.header_build_buffer = header_build_buffer;
        self.url_buffer = url_buffer;
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
        if self.easy_handle.is_none() {
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
            self.easy_handle = Some(EasyHandle(raw_handle));
        }
        let handle = self
            .easy_handle
            .as_ref()
            .ok_or("curl easy handle cache 상태 오류")?;
        handle.reset();
        action(handle)
    }
}
impl Default for Client {
    fn default() -> Self {
        Self {
            easy_handle: None,
            header_buffer: Vec::new(),
            header_build_buffer: Vec::new(),
            scheme_prefix: HTTPS_SCHEME_PREFIX,
            url_buffer: Vec::new(),
        }
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
fn request_url<'url>(
    out: &'url mut Vec<u8>,
    scheme_prefix: &str,
    host: &str,
    path: &str,
) -> DownloadResult<&'url CStr> {
    let url_capacity = scheme_prefix
        .len()
        .checked_add(host.len())
        .and_then(|capacity| capacity.checked_add(path.len()))
        .and_then(|capacity| capacity.checked_add(1))
        .ok_or("URL 용량 계산 실패")?;
    out.clear();
    if out.capacity() < url_capacity {
        out.try_reserve_exact(url_capacity)
            .map_err(|source| download_error_with_source("URL 메모리 확보 실패", source))?;
    }
    out.extend_from_slice(scheme_prefix.as_bytes());
    out.extend_from_slice(host.as_bytes());
    out.extend_from_slice(path.as_bytes());
    out.push(0);
    CStr::from_bytes_with_nul(out)
        .map_err(|source| download_error_with_source("URL에 NUL 문자가 포함되어 있습니다", source))
}
fn parsed_headers_from_bytes(header_bytes: &[u8]) -> DownloadResult<Vec<HttpHeader>> {
    let separator: &[u8] = if find_subslice(header_bytes, b"\r\n\r\n").is_some() {
        b"\r\n\r\n"
    } else {
        b"\n\n"
    };
    let mut selected_block = None;
    let mut rest = header_bytes;
    loop {
        let (block, next) = match find_subslice(rest, separator) {
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
fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() {
        return None;
    }
    haystack.windows(needle.len()).position(|window| window == needle)
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
fn curl_perform_error(
    error_buffer: [c_char; CURL_ERROR_SIZE],
    code: CurlCode,
) -> Cow<'static, str> {
    let bytes = error_buffer.map(|ch| ch.to_le_bytes()[0]);
    Cow::Owned(
        if let Ok(message_cstr) = CStr::from_bytes_until_nul(&bytes)
            && !message_cstr.to_bytes().is_empty()
        {
            let message = message_cstr.to_string_lossy();
            format!("curl_easy_perform 실패: {message} ({code})")
        } else {
            curl_error("curl_easy_perform", code)
        },
    )
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
