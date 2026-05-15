use super::{
    HTTP_MAX_BODY_BYTES, HTTP_MAX_HEADER_BYTES, enforce_http_content_length_limit,
    http_client::HttpResponse,
};
use alloc::{ffi::CString, string::String};
use core::{
    ffi::{CStr, c_char, c_int, c_long, c_void},
    ptr::{NonNull, null_mut},
};
use std::sync::OnceLock;
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
pub(super) const CLIENT: Client = Client {
    scheme_prefix: HTTPS_SCHEME_PREFIX,
};
type Curl = c_void;
type CurlCode = c_int;
type CurlInfo = c_int;
type CurlOption = c_int;
type CurlWriteCallback = unsafe extern "C" fn(*mut c_char, usize, usize, *mut c_void) -> usize;
pub(super) struct Client {
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
    error: Option<String>,
    label: &'static str,
    limit: usize,
}
struct ResponseBuffers {
    body: BoundedResponseBuffer,
    headers: BoundedResponseBuffer,
}
#[link(name = "curl")]
unsafe extern "C" {
    fn curl_easy_cleanup(curl: *mut Curl);
    fn curl_easy_getinfo(curl: *mut Curl, info: CurlInfo, ...) -> CurlCode;
    fn curl_easy_init() -> *mut Curl;
    fn curl_easy_perform(curl: *mut Curl) -> CurlCode;
    fn curl_easy_setopt(curl: *mut Curl, option: CurlOption, ...) -> CurlCode;
    fn curl_easy_strerror(code: CurlCode) -> *const c_char;
    fn curl_global_init(flags: c_long) -> CurlCode;
    fn curl_slist_append(list: *mut CurlSlist, string: *const c_char) -> *mut CurlSlist;
    fn curl_slist_free_all(list: *mut CurlSlist);
}
impl Drop for EasyHandle {
    fn drop(&mut self) {
        // SAFETY: self.0 is an easy handle returned by libcurl and is closed exactly once here.
        unsafe {
            curl_easy_cleanup(self.0.as_ptr());
        }
    }
}
impl Drop for HeaderList {
    fn drop(&mut self) {
        if let Some(list) = self.0 {
            // SAFETY: list is a curl_slist allocated by libcurl and is freed exactly once here.
            unsafe {
                curl_slist_free_all(list.as_ptr());
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
        unsafe { curl_easy_perform(self.as_ptr()) }
    }
    fn response_code(&self) -> Result<c_long, String> {
        let mut raw_status = c_long::default();
        // SAFETY: raw_status is a valid output pointer for CURLINFO_RESPONSE_CODE.
        let status_code = unsafe {
            curl_easy_getinfo(self.as_ptr(), CURLINFO_RESPONSE_CODE, &raw mut raw_status)
        };
        if status_code == CURLE_OK {
            Ok(raw_status)
        } else {
            Err(curl_error("curl_easy_getinfo response_code", status_code))
        }
    }
    fn setopt_callback(&self, option: CurlOption, value: CurlWriteCallback) -> Result<(), String> {
        // SAFETY: value is a libcurl-compatible callback function pointer.
        let code = unsafe { curl_easy_setopt(self.as_ptr(), option, value) };
        if code == CURLE_OK {
            Ok(())
        } else {
            Err(curl_error("curl_easy_setopt", code))
        }
    }
    fn setopt_long(&self, option: CurlOption, value: c_long) -> Result<(), String> {
        // SAFETY: value is a scalar option value for the given CurlOption.
        let code = unsafe { curl_easy_setopt(self.as_ptr(), option, value) };
        if code == CURLE_OK {
            Ok(())
        } else {
            Err(curl_error("curl_easy_setopt", code))
        }
    }
    fn setopt_ptr<T>(&self, option: CurlOption, value: *const T) -> Result<(), String> {
        // SAFETY: value is a pointer option that remains valid for the transfer duration.
        let code = unsafe { curl_easy_setopt(self.as_ptr(), option, value) };
        if code == CURLE_OK {
            Ok(())
        } else {
            Err(curl_error("curl_easy_setopt", code))
        }
    }
    fn setopt_str(&self, option: CurlOption, value: *const c_char) -> Result<(), String> {
        // SAFETY: value is a valid NUL-terminated string that outlives the setopt call.
        let code = unsafe { curl_easy_setopt(self.as_ptr(), option, value) };
        if code == CURLE_OK {
            Ok(())
        } else {
            Err(curl_error("curl_easy_setopt", code))
        }
    }
}
impl HeaderList {
    fn append(&mut self, header: &CStr) -> Result<(), String> {
        // SAFETY: header is a valid NUL-terminated string and self.0 is null or a libcurl list.
        let updated_ptr = unsafe { curl_slist_append(self.as_ptr(), header.as_ptr()) };
        let Some(updated) = NonNull::new(updated_ptr) else {
            return Err("curl_slist_append 실패".to_owned());
        };
        self.0 = Some(updated);
        Ok(())
    }
    fn as_ptr(&self) -> *mut CurlSlist {
        match self.0 {
            Some(list) => list.as_ptr(),
            None => null_mut(),
        }
    }
}
impl Client {
    pub(super) fn request(
        &self,
        method: &str,
        host: &str,
        path: &str,
        body: Option<&[u8]>,
        request_headers: &[(&str, &str)],
    ) -> Result<HttpResponse, String> {
        static INIT: OnceLock<CurlCode> = OnceLock::new();
        // SAFETY: curl_global_init may be called once here before any easy handles are used.
        let init_code = *INIT.get_or_init(|| unsafe { curl_global_init(CURL_GLOBAL_DEFAULT) });
        if init_code != CURLE_OK {
            return Err(curl_error("curl_global_init", init_code));
        }
        // SAFETY: curl_easy_init has no preconditions.
        let raw_handle_ptr = unsafe { curl_easy_init() };
        let Some(raw_handle) = NonNull::new(raw_handle_ptr) else {
            return Err("curl_easy_init 실패".to_owned());
        };
        let handle = EasyHandle(raw_handle);
        let url_capacity = self
            .scheme_prefix
            .len()
            .saturating_add(host.len())
            .saturating_add(path.len());
        let mut raw_url = String::new();
        raw_url
            .try_reserve(url_capacity)
            .map_err(|source| format!("URL 메모리 확보 실패: {source}"))?;
        raw_url.push_str(self.scheme_prefix);
        raw_url.push_str(host);
        raw_url.push_str(path);
        let url = cstring("URL", &raw_url)?;
        let mut header_list = HeaderList(None);
        for &(name, value) in request_headers {
            let header_capacity = name
                .len()
                .saturating_add(": ".len())
                .saturating_add(value.len());
            let mut header = String::new();
            header
                .try_reserve(header_capacity)
                .map_err(|source| format!("HTTP header 메모리 확보 실패: {source}"))?;
            header.push_str(name);
            header.push_str(": ");
            header.push_str(value);
            let header_c = cstring("HTTP header", &header)?;
            header_list.append(header_c.as_c_str())?;
        }
        let mut error_buffer = [c_char::default(); CURL_ERROR_SIZE];
        let mut response_buffers = ResponseBuffers {
            body: BoundedResponseBuffer::new("본문", HTTP_MAX_BODY_BYTES),
            headers: BoundedResponseBuffer::new("헤더", HTTP_MAX_HEADER_BYTES),
        };
        handle.setopt_str(CURLOPT_URL, url.as_ptr())?;
        handle.setopt_ptr(CURLOPT_HTTPHEADER, header_list.as_ptr())?;
        handle.setopt_ptr(CURLOPT_ERRORBUFFER, error_buffer.as_mut_ptr())?;
        handle.setopt_callback(CURLOPT_WRITEFUNCTION, write_vec_callback)?;
        handle.setopt_callback(CURLOPT_HEADERFUNCTION, write_vec_callback)?;
        for (option, value) in [
            (CURLOPT_CONNECTTIMEOUT, 30),
            (
                CURLOPT_MAXFILESIZE_LARGE,
                c_long::try_from(HTTP_MAX_BODY_BYTES)
                    .map_err(|source| format!("HTTP 본문 한도 변환 실패: {source}"))?,
            ),
            (CURLOPT_TIMEOUT, 60),
            (CURLOPT_NOSIGNAL, 1),
        ] {
            handle.setopt_long(option, value)?;
        }
        let body_data = (&raw mut response_buffers.body).cast::<c_void>();
        let header_data = (&raw mut response_buffers.headers).cast::<c_void>();
        handle.setopt_ptr(CURLOPT_WRITEDATA, body_data)?;
        handle.setopt_ptr(CURLOPT_HEADERDATA, header_data)?;
        if let Some(body_bytes) = body {
            handle.setopt_long(CURLOPT_POST, 1)?;
            let post_fields = body_bytes.as_ptr().cast::<c_char>();
            handle.setopt_ptr(CURLOPT_POSTFIELDS, post_fields)?;
            let body_len = c_long::try_from(body_bytes.len())
                .map_err(|source| format!("요청 본문 길이 변환 실패: {source}"))?;
            handle.setopt_long(CURLOPT_POSTFIELDSIZE, body_len)?;
        }
        if method != "GET" && method != "POST" {
            let custom_method = cstring("HTTP method", method)?;
            handle.setopt_str(CURLOPT_CUSTOMREQUEST, custom_method.as_ptr())?;
        }
        let perform_code = handle.perform();
        if let Some(callback_error) = response_buffers.take_error() {
            return Err(callback_error);
        }
        if perform_code != CURLE_OK {
            let bytes = error_buffer.map(|ch| ch.to_le_bytes()[0]);
            if let Ok(message_cstr) = CStr::from_bytes_until_nul(&bytes)
                && !message_cstr.to_bytes().is_empty()
            {
                let message = message_cstr.to_string_lossy();
                return Err(format!(
                    "curl_easy_perform 실패: {message} ({perform_code})"
                ));
            }
            return Err(curl_error("curl_easy_perform", perform_code));
        }
        let raw_status = handle.response_code()?;
        let status = u32::try_from(raw_status)
            .map_err(|source| format!("HTTP 상태 코드 변환 실패: {source}"))?;
        let headers = response_buffers.parsed_headers()?;
        enforce_http_content_length_limit(&headers, HTTP_MAX_BODY_BYTES)?;
        Ok(HttpResponse {
            body: response_buffers.body.bytes,
            headers,
            status,
        })
    }
}
impl BoundedResponseBuffer {
    fn append(&mut self, bytes: &[u8]) -> bool {
        let Some(next_len) = self.bytes.len().checked_add(bytes.len()) else {
            self.error = Some(format!("HTTP 응답 {} 크기 계산 실패", self.label));
            return false;
        };
        if next_len > self.limit {
            self.error = Some(format!(
                "HTTP 응답 {} 크기가 허용 한도({} bytes)를 초과했습니다.",
                self.label, self.limit
            ));
            return false;
        }
        if let Err(source) = self.bytes.try_reserve(bytes.len()) {
            self.error = Some(format!(
                "HTTP 응답 {} 메모리 확보 실패: {source}",
                self.label
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
    fn parsed_headers(&self) -> Result<Vec<(String, String)>, String> {
        let text = String::from_utf8_lossy(&self.headers.bytes);
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
        let header_count = selected
            .lines()
            .filter(|line| !line.starts_with("HTTP/"))
            .filter(|line| line.contains(':'))
            .count();
        let mut headers = Vec::new();
        headers
            .try_reserve(header_count)
            .map_err(|source| format!("HTTP header 목록 메모리 확보 실패: {source}"))?;
        for line in selected.lines() {
            if line.starts_with("HTTP/") {
                continue;
            }
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            let name = name.trim_ascii();
            let value = value.trim_ascii();
            let mut header_name = String::new();
            header_name
                .try_reserve(name.len())
                .map_err(|source| format!("HTTP header 이름 메모리 확보 실패: {source}"))?;
            header_name.push_str(name);
            let mut header_value = String::new();
            header_value
                .try_reserve(value.len())
                .map_err(|source| format!("HTTP header 값 메모리 확보 실패: {source}"))?;
            header_value.push_str(value);
            headers.push((header_name, header_value));
        }
        Ok(headers)
    }
    fn take_error(&mut self) -> Option<String> {
        self.body.error.take().or_else(|| self.headers.error.take())
    }
}
fn cstring(label: &str, value: &str) -> Result<CString, String> {
    CString::new(value)
        .map_err(|source| format!("{label}에 NUL 문자가 포함되어 있습니다: {source}"))
}
fn curl_error(context: &str, code: CurlCode) -> String {
    // SAFETY: curl_easy_strerror returns either null or a static NUL-terminated message for code.
    let raw_ptr = unsafe { curl_easy_strerror(code) };
    let message = if raw_ptr.is_null() {
        "unknown curl error".to_owned()
    } else {
        // SAFETY: libcurl guarantees a valid NUL-terminated string for non-null strerror results.
        unsafe { CStr::from_ptr(raw_ptr) }
            .to_string_lossy()
            .into_owned()
    };
    format!("{context} 실패: {message} ({code})")
}
unsafe extern "C" fn write_vec_callback(
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
    // SAFETY: libcurl invokes this callback with a readable payload pointer and
    // the userdata pointer configured for the active response buffer.
    let Some((bytes, target)) = (unsafe { callback_payload(ptr, len, userdata) }) else {
        return 0;
    };
    if !target.append(bytes) {
        return 0;
    }
    len
}
unsafe fn callback_payload<'a>(
    ptr: *mut c_char,
    len: usize,
    userdata: *mut c_void,
) -> Option<(&'a [u8], &'a mut BoundedResponseBuffer)> {
    let bytes_ptr = NonNull::new(ptr.cast::<u8>())?;
    let mut target_ptr = NonNull::new(userdata.cast::<BoundedResponseBuffer>())?;
    let bytes_ptr = NonNull::slice_from_raw_parts(bytes_ptr, len);
    // SAFETY: libcurl passes a valid buffer with len bytes for the duration of this callback.
    let bytes = unsafe { bytes_ptr.as_ref() };
    // SAFETY: userdata is the target buffer pointer set before curl_easy_perform.
    let target = unsafe { target_ptr.as_mut() };
    Some((bytes, target))
}
