use super::{
    DownloadError, DownloadResult, HTTP_MAX_BODY_BYTES, HTTP_MAX_HEADER_BYTES, HttpHeader,
    HttpResponse, HttpStreamResponse, StreamedBodySummary, StreamingBodySink,
    download_error_with_source, enforce_http_content_length_limit,
};
use alloc::{borrow::Cow, ffi::CString, string::String};
use core::{
    ffi::{CStr, c_char, c_int, c_long, c_void},
    ptr::{NonNull, null_mut},
    str,
};
use std::{io::Write as IoWrite, sync::LazyLock};
mod sys {
    use super::{Curl, CurlCode, CurlInfo, CurlOption, CurlSlist, c_char, c_long};
    #[link(name = "curl")]
    unsafe extern "C" {
        pub(super) fn curl_easy_cleanup(curl: *mut Curl);
        pub(super) fn curl_easy_getinfo(curl: *mut Curl, info: CurlInfo, ...) -> CurlCode;
        pub(super) fn curl_easy_init() -> *mut Curl;
        pub(super) fn curl_easy_perform(curl: *mut Curl) -> CurlCode;
        pub(super) fn curl_easy_reset(curl: *mut Curl);
        pub(super) fn curl_easy_setopt(curl: *mut Curl, option: CurlOption, ...) -> CurlCode;
        pub(super) fn curl_easy_strerror(code: CurlCode) -> *const c_char;
        pub(super) fn curl_global_init(flags: c_long) -> CurlCode;
        pub(super) fn curl_slist_append(
            list: *mut CurlSlist,
            string: *const c_char,
        ) -> *mut CurlSlist;
        pub(super) fn curl_slist_free_all(list: *mut CurlSlist);
    }
}
const CURLE_OK: CurlCode = 0;
const CURL_ERROR_SIZE: usize = 256;
const CURL_GLOBAL_DEFAULT: c_long = 3;
const CURLINFO_RESPONSE_CODE: CurlInfo = 0x20_0002;
const CURLOPT_CONNECTTIMEOUT: CurlOption = 78;
const CURLOPT_CUSTOMREQUEST: CurlOption = 10_036;
const CURLOPT_ERRORBUFFER: CurlOption = 10_010;
const CURLOPT_HEADERDATA: CurlOption = 10_029;
const CURLOPT_HEADERFUNCTION: CurlOption = 20_079;
const CURLOPT_HTTPHEADER: CurlOption = 10_023;
const CURLOPT_MAXFILESIZE_LARGE: CurlOption = 30_117;
const CURLOPT_NOSIGNAL: CurlOption = 99;
const CURLOPT_POST: CurlOption = 47;
const CURLOPT_POSTFIELDS: CurlOption = 10_015;
const CURLOPT_POSTFIELDSIZE: CurlOption = 60;
const CURLOPT_TIMEOUT: CurlOption = 13;
const CURLOPT_URL: CurlOption = 10_002;
const CURLOPT_WRITEDATA: CurlOption = 10_001;
const CURLOPT_WRITEFUNCTION: CurlOption = 20_011;
const HTTPS_SCHEME_PREFIX: &str = "https://";
static CURL_INIT: LazyLock<CurlCode> = LazyLock::new(|| {
    // SAFETY: LazyLock runs this initializer once before any easy handles are used.
    unsafe { sys::curl_global_init(CURL_GLOBAL_DEFAULT) }
});
type Curl = c_void;
type CurlCode = c_int;
type CurlInfo = c_int;
type CurlOption = c_int;
type CurlWriteCallback = unsafe extern "C" fn(*mut c_char, usize, usize, *mut c_void) -> usize;
pub(super) struct Client {
    easy_handle: Option<EasyHandle>,
    scheme_prefix: &'static str,
}
#[repr(C)]
struct CurlSlist {
    data: *mut c_char,
    next: *mut Self,
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
    fn from_headers(request_headers: &[(&str, &str)]) -> DownloadResult<Self> {
        let mut header_list = Self(None);
        for &(name, value) in request_headers {
            let header_capacity = name
                .len()
                .checked_add(": ".len())
                .and_then(|capacity| capacity.checked_add(value.len()))
                .ok_or("HTTP header 용량 계산 실패")?;
            let mut header = String::new();
            header
                .try_reserve(header_capacity)
                .map_err(|source| download_error_with_source("HTTP header 메모리 확보 실패", source))?;
            header.push_str(name);
            header.push_str(": ");
            header.push_str(value);
            let header_c = cstring("HTTP header", &header)?;
            header_list.append(header_c.as_c_str())?;
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
        custom_method: Option<&CStr>,
        body: Option<&[u8]>,
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
                option: CURLOPT_CONNECTTIMEOUT,
                value: 30,
            },
            CurlLongOption {
                option: CURLOPT_MAXFILESIZE_LARGE,
                value: c_long::try_from(HTTP_MAX_BODY_BYTES)
                    .map_err(|source| download_error_with_source("HTTP 본문 한도 변환 실패", source))?,
            },
            CurlLongOption {
                option: CURLOPT_TIMEOUT,
                value: 60,
            },
            CurlLongOption {
                option: CURLOPT_NOSIGNAL,
                value: 1,
            },
        ] {
            handle.setopt_long(setting.option, setting.value)?;
        }
        if let Some(body_bytes) = body {
            handle.setopt_long(CURLOPT_POST, 1)?;
            let post_fields = body_bytes.as_ptr().cast::<c_char>();
            handle.setopt_ptr(CURLOPT_POSTFIELDS, post_fields)?;
            let body_len = c_long::try_from(body_bytes.len())
                .map_err(|source| download_error_with_source("요청 본문 길이 변환 실패", source))?;
            handle.setopt_long(CURLOPT_POSTFIELDSIZE, body_len)?;
        }
        if let Some(custom_method_c) = custom_method {
            handle.setopt_str(CURLOPT_CUSTOMREQUEST, custom_method_c.as_ptr())?;
        }
        Ok(())
    }
    pub(super) fn request(
        &mut self,
        method: &str,
        host: &str,
        path: &str,
        body: Option<&[u8]>,
        request_headers: &[(&str, &str)],
    ) -> DownloadResult<HttpResponse> {
        let url_capacity = self
            .scheme_prefix
            .len()
            .checked_add(host.len())
            .and_then(|capacity| capacity.checked_add(path.len()))
            .ok_or("URL 용량 계산 실패")?;
        let mut raw_url = String::new();
        raw_url
            .try_reserve(url_capacity)
            .map_err(|source| download_error_with_source("URL 메모리 확보 실패", source))?;
        raw_url.push_str(self.scheme_prefix);
        raw_url.push_str(host);
        raw_url.push_str(path);
        let url = cstring("URL", &raw_url)?;
        let header_list = HeaderList::from_headers(request_headers)?;
        let custom_method = (method != "GET" && method != "POST")
            .then(|| cstring("HTTP method", method))
            .transpose()?;
        let mut error_buffer = [c_char::default(); CURL_ERROR_SIZE];
        let mut response_buffers = ResponseBuffers {
            body: BoundedResponseBuffer::new("본문", HTTP_MAX_BODY_BYTES),
            headers: BoundedResponseBuffer::new("헤더", HTTP_MAX_HEADER_BYTES),
        };
        let perform = self.with_reusable_handle(|handle| {
            Self::configure_request(
                handle,
                url.as_c_str(),
                &header_list,
                &mut error_buffer,
                custom_method.as_deref(),
                body,
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
                    response_code: Some(handle.response_code()?),
                })
            } else {
                Ok(CurlPerformResult {
                    code: perform_code,
                    response_code: None,
                })
            }
        })?;
        if let Some(callback_error) = response_buffers.take_error() {
            self.clear_reusable_handle();
            return Err(callback_error);
        }
        if perform.code != CURLE_OK {
            let bytes = error_buffer.map(|ch| ch.to_le_bytes()[0]);
            let perform_error: Cow<'static, str> =
                Cow::Owned(if let Ok(message_cstr) = CStr::from_bytes_until_nul(&bytes)
                && !message_cstr.to_bytes().is_empty()
            {
                let message = message_cstr.to_string_lossy();
                format!("curl_easy_perform 실패: {message} ({})", perform.code)
            } else {
                curl_error("curl_easy_perform", perform.code)
            });
            self.clear_reusable_handle();
            return Err(perform_error.into());
        }
        let raw_status = perform
            .response_code
            .ok_or("curl response code가 비어 있습니다.")?;
        let status = u32::try_from(raw_status)
            .map_err(|source| download_error_with_source("HTTP 상태 코드 변환 실패", source))?;
        let headers = response_buffers.parsed_headers()?;
        enforce_http_content_length_limit(&headers, HTTP_MAX_BODY_BYTES)
            ?;
        Ok(HttpResponse {
            body: response_buffers.body.bytes,
            headers,
            status,
        })
    }
    pub(super) fn request_to_writer(
        &mut self,
        method: &str,
        host: &str,
        path: &str,
        body: Option<&[u8]>,
        request_headers: &[(&str, &str)],
        writer: &mut dyn IoWrite,
    ) -> DownloadResult<HttpStreamResponse> {
        let url_capacity = self
            .scheme_prefix
            .len()
            .checked_add(host.len())
            .and_then(|capacity| capacity.checked_add(path.len()))
            .ok_or("URL 용량 계산 실패")?;
        let mut raw_url = String::new();
        raw_url
            .try_reserve(url_capacity)
            .map_err(|source| download_error_with_source("URL 메모리 확보 실패", source))?;
        raw_url.push_str(self.scheme_prefix);
        raw_url.push_str(host);
        raw_url.push_str(path);
        let url = cstring("URL", &raw_url)?;
        let header_list = HeaderList::from_headers(request_headers)?;
        let custom_method = (method != "GET" && method != "POST")
            .then(|| cstring("HTTP method", method))
            .transpose()?;
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
        let mut header_buffer = BoundedResponseBuffer::new("헤더", HTTP_MAX_HEADER_BYTES);
        let perform = self.with_reusable_handle(|handle| {
            Self::configure_request(
                handle,
                url.as_c_str(),
                &header_list,
                &mut error_buffer,
                custom_method.as_deref(),
                body,
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
                    response_code: Some(handle.response_code()?),
                })
            } else {
                Ok(CurlPerformResult {
                    code: perform_code,
                    response_code: None,
                })
            }
        })?;
        if let Some(callback_error) = body_sink
            .error
            .take()
            .or_else(|| header_buffer.error.take())
        {
            self.clear_reusable_handle();
            return Err(callback_error);
        }
        if perform.code != CURLE_OK {
            let bytes = error_buffer.map(|ch| ch.to_le_bytes()[0]);
            let perform_error: Cow<'static, str> =
                Cow::Owned(if let Ok(message_cstr) = CStr::from_bytes_until_nul(&bytes)
                && !message_cstr.to_bytes().is_empty()
            {
                let message = message_cstr.to_string_lossy();
                format!("curl_easy_perform 실패: {message} ({})", perform.code)
            } else {
                curl_error("curl_easy_perform", perform.code)
            });
            self.clear_reusable_handle();
            return Err(perform_error.into());
        }
        let raw_status = perform
            .response_code
            .ok_or("curl response code가 비어 있습니다.")?;
        let status = u32::try_from(raw_status)
            .map_err(|source| download_error_with_source("HTTP 상태 코드 변환 실패", source))?;
        let headers = parsed_headers_from_bytes(&header_buffer.bytes)?;
        enforce_http_content_length_limit(&headers, HTTP_MAX_BODY_BYTES)
            ?;
        Ok(HttpStreamResponse {
            body: body_sink.summary,
            headers,
            status,
        })
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
            scheme_prefix: HTTPS_SCHEME_PREFIX,
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
        if let Err(source) = self.bytes.try_reserve(bytes.len()) {
            self.error = Some(download_error_with_source(
                format!("HTTP 응답 {} 메모리 확보 실패", self.label),
                source,
            ));
            return false;
        }
        self.bytes.extend_from_slice(bytes);
        true
    }
    const fn new(label: &'static str, limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            error: None,
            label,
            limit,
        }
    }
}
impl ResponseBuffers {
    fn parsed_headers(&self) -> DownloadResult<Vec<HttpHeader>> {
        parsed_headers_from_bytes(&self.headers.bytes)
    }
    fn take_error(&mut self) -> Option<DownloadError> {
        self.body.error.take().or_else(|| self.headers.error.take())
    }
}
fn parsed_headers_from_bytes(header_bytes: &[u8]) -> DownloadResult<Vec<HttpHeader>> {
    let text = str::from_utf8(header_bytes)
        .map_err(|source| download_error_with_source("HTTP header UTF-8 변환 실패", source))?;
    let separator = if text.contains("\r\n\r\n") {
        "\r\n\r\n"
    } else {
        "\n\n"
    };
    let Some(selected) = text
        .rsplit(separator)
        .find(|block| !block.trim_ascii().is_empty())
    else {
        return Ok(Vec::new());
    };
    let header_items = || {
        selected
            .lines()
            .filter(|line| !line.starts_with("HTTP/"))
            .filter_map(|line| line.split_once(':'))
    };
    let header_count = header_items().count();
    let mut headers = Vec::new();
    headers
        .try_reserve(header_count)
        .map_err(|source| download_error_with_source("HTTP header 목록 메모리 확보 실패", source))?;
    for (raw_name, raw_value) in header_items() {
        let name = raw_name.trim_ascii();
        let value = raw_value.trim_ascii();
        let mut header_name = String::new();
        header_name
            .try_reserve(name.len())
            .map_err(|source| download_error_with_source("HTTP header 이름 메모리 확보 실패", source))?;
        header_name.push_str(name);
        let mut header_value = String::new();
        header_value
            .try_reserve(value.len())
            .map_err(|source| download_error_with_source("HTTP header 값 메모리 확보 실패", source))?;
        header_value.push_str(value);
        headers.push(HttpHeader {
            name: header_name,
            value: header_value,
        });
    }
    Ok(headers)
}
fn cstring(label: &str, value: &str) -> DownloadResult<CString> {
    CString::new(value)
        .map_err(|source| download_error_with_source(format!("{label}에 NUL 문자가 포함되어 있습니다"), source))
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
    let payload_ptr = NonNull::slice_from_raw_parts(payload_head, len);
    // SAFETY: libcurl passes a readable buffer with len bytes for this callback.
    let bytes = unsafe { payload_ptr.as_ref() };
    // SAFETY: userdata is the write target pointer configured before curl_easy_perform.
    let target = unsafe { target_ptr.as_mut() };
    let appended = match *target {
        CurlWriteTarget::Buffer(ref mut buffer) => buffer.append(bytes),
        CurlWriteTarget::Stream(ref mut stream) => stream.append(bytes),
    };
    if !appended {
        return 0;
    }
    len
}
