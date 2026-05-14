use crate::{
    Result, err, err_with_source, has_basic_region_suffix, is_metropolitan_token,
    is_province_token, normalize_address_key, path_source_message, prefixed_message,
    source_sync::SourceRecord, strip_basic_region_suffix,
};
use alloc::{borrow::Cow, string::String, vec::Vec};
use core::{result::Result as StdResult, time::Duration};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::LazyLock,
    thread::sleep,
    time::{SystemTime, UNIX_EPOCH},
};
cfg_select! {
    any(target_os = "linux", target_os = "macos") => {
        mod libcurl {
    use super::{
        HTTP_MAX_BODY_BYTES, HTTP_MAX_HEADER_BYTES, HttpResponse, enforce_http_content_length_limit,
    };
    use alloc::{ffi::CString, string::String};
    use core::{
        ffi::{CStr, c_char, c_int, c_long, c_void},
        ptr::{NonNull, null_mut},
        slice,
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
            let status_code =
                unsafe { curl_easy_getinfo(self.as_ptr(), CURLINFO_RESPONSE_CODE, &raw mut raw_status) };
            if status_code == CURLE_OK {
                Ok(raw_status)
            } else {
                Err(curl_error("curl_easy_getinfo response_code", status_code))
            }
        }
        fn setopt_callback(
            &self,
            option: CurlOption,
            value: CurlWriteCallback,
        ) -> Result<(), String> {
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
                return Err(String::from("curl_slist_append 실패"));
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
                return Err(String::from("curl_easy_init 실패"));
            };
            let handle = EasyHandle(raw_handle);
            let raw_url = [self.scheme_prefix, host, path].concat();
            let url = cstring("URL", &raw_url)?;
            let mut header_list = HeaderList(None);
            for &(name, value) in request_headers {
                let header = format!("{name}: {value}");
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
            let headers = response_buffers.parsed_headers();
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
        fn parsed_headers(&self) -> Vec<(String, String)> {
            let text = String::from_utf8_lossy(&self.headers.bytes);
            let normalized = text.replace("\r\n", "\n");
            let Some(selected) = normalized
                .rsplit("\n\n")
                .find(|block| !block.trim_ascii().is_empty())
            else {
                return Vec::new();
            };
            selected
                .lines()
                .filter(|line| !line.starts_with("HTTP/"))
                .filter_map(|line| line.split_once(':'))
                .map(|(name, value)| (name.trim_ascii().to_owned(), value.trim_ascii().to_owned()))
                .collect::<Vec<_>>()
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
            String::from("unknown curl error")
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
        let Some(bytes_ptr) = NonNull::new(ptr.cast::<u8>()) else {
            return 0;
        };
        let Some(mut target_ptr) = NonNull::new(userdata.cast::<BoundedResponseBuffer>()) else {
            return 0;
        };
        // SAFETY: libcurl passes a valid buffer with len bytes for the duration of this callback.
        let bytes = unsafe { slice::from_raw_parts(bytes_ptr.as_ptr(), len) };
        // SAFETY: userdata is the BoundedResponseBuffer pointer set before curl_easy_perform.
        let target = unsafe { target_ptr.as_mut() };
        if !target.append(bytes) {
            return 0;
        }
        len
    }
        }
    }
    windows => {
        mod winhttp {
    use super::{
        HTTP_MAX_BODY_BYTES, HTTP_MAX_HEADER_BYTES, HttpResponse, checked_http_buffer_len,
        enforce_http_content_length_limit,
    };
    use alloc::{string::String, vec::Vec};
    use core::{
        ffi::c_void,
        ptr::{NonNull, null, null_mut},
    };
    use std::{ffi::OsStr, os::windows::ffi::OsStrExt as _};
    const ERROR_INSUFFICIENT_BUFFER: u32 = 122;
    const INTERNET_DEFAULT_HTTPS_PORT: u16 = 443;
    const WINHTTP_ACCESS_TYPE_DEFAULT_PROXY: u32 = 0;
    const WINHTTP_FLAG_SECURE: u32 = 0x0080_0000;
    const WINHTTP_QUERY_FLAG_NUMBER: u32 = 0x2000_0000;
    const WINHTTP_QUERY_RAW_HEADERS_CRLF: u32 = 22;
    const WINHTTP_QUERY_STATUS_CODE: u32 = 19;
    const WINHTTP_CONNECT_TIMEOUT_MS: i32 = 30_000;
    const WINHTTP_RECEIVE_TIMEOUT_MS: i32 = 60_000;
    const WINHTTP_RESOLVE_TIMEOUT_MS: i32 = 30_000;
    const WINHTTP_SEND_TIMEOUT_MS: i32 = 60_000;
    pub(super) const CLIENT: Client = Client {
        get_last_error: GetLastError,
    };
    type HInternet = *mut c_void;
    pub(super) struct Client {
        get_last_error: unsafe extern "system" fn() -> u32,
    }
    #[link(name = "winhttp")]
    unsafe extern "system" {
        fn WinHttpCloseHandle(h_internet: HInternet) -> i32;
        fn WinHttpConnect(
            h_session: HInternet,
            server_name: *const u16,
            server_port: u16,
            reserved: u32,
        ) -> HInternet;
        fn WinHttpOpen(
            user_agent: *const u16,
            access_type: u32,
            proxy_name: *const u16,
            proxy_bypass: *const u16,
            flags: u32,
        ) -> HInternet;
        fn WinHttpOpenRequest(
            h_connect: HInternet,
            verb: *const u16,
            object_name: *const u16,
            version: *const u16,
            referrer: *const u16,
            accept_types: *const *const u16,
            flags: u32,
        ) -> HInternet;
        fn WinHttpQueryDataAvailable(h_request: HInternet, bytes_available: *mut u32) -> i32;
        fn WinHttpQueryHeaders(
            h_request: HInternet,
            info_level: u32,
            name: *const u16,
            buffer: *mut c_void,
            buffer_length: *mut u32,
            index: *mut u32,
        ) -> i32;
        fn WinHttpReadData(
            h_request: HInternet,
            buffer: *mut c_void,
            bytes_to_read: u32,
            bytes_read: *mut u32,
        ) -> i32;
        fn WinHttpReceiveResponse(h_request: HInternet, reserved: *mut c_void) -> i32;
        fn WinHttpSendRequest(
            h_request: HInternet,
            headers: *const u16,
            headers_length: u32,
            optional: *const c_void,
            optional_length: u32,
            total_length: u32,
            context: usize,
        ) -> i32;
        fn WinHttpSetTimeouts(
            h_internet: HInternet,
            resolve_timeout: i32,
            connect_timeout: i32,
            send_timeout: i32,
            receive_timeout: i32,
        ) -> i32;
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetLastError() -> u32;
    }
    struct Handle(NonNull<c_void>);
    impl Drop for Handle {
        fn drop(&mut self) {
            // SAFETY: self.0 is a WinHTTP handle returned by WinHTTP and is closed exactly once here.
            unsafe {
                WinHttpCloseHandle(self.as_ptr());
            }
        }
    }
    impl Handle {
        const fn as_ptr(&self) -> HInternet {
            self.0.as_ptr()
        }
    }
    impl Client {
        fn connect(&self, session: &Handle, host: &[u16]) -> Result<Handle, String> {
            // SAFETY: host is NUL-terminated and session is a valid session handle.
            let raw_connect = unsafe {
                WinHttpConnect(session.as_ptr(), host.as_ptr(), INTERNET_DEFAULT_HTTPS_PORT, 0)
            };
            self.non_null_handle(raw_connect, "WinHttpConnect")
        }
        fn last_error_code(&self) -> u32 {
            // SAFETY: GetLastError has no preconditions.
            unsafe { (self.get_last_error)() }
        }
        fn last_error_message(&self, context: &str) -> String {
            let code = self.last_error_code();
            format!("{context} 실패: Windows error {code}")
        }
        fn non_null_handle(&self, handle: HInternet, context: &str) -> Result<Handle, String> {
            NonNull::new(handle)
                .map(Handle)
                .ok_or_else(|| self.last_error_message(context))
        }
        fn open_request(
            &self,
            connect: &Handle,
            method: &[u16],
            path: &[u16],
        ) -> Result<Handle, String> {
            // SAFETY: method and path are NUL-terminated and connect is valid.
            let raw_request = unsafe {
                WinHttpOpenRequest(
                    connect.as_ptr(),
                    method.as_ptr(),
                    path.as_ptr(),
                    null(),
                    null(),
                    null(),
                    WINHTTP_FLAG_SECURE,
                )
            };
            self.non_null_handle(raw_request, "WinHttpOpenRequest")
        }
        fn open_session(&self, user_agent: &[u16]) -> Result<Handle, String> {
            // SAFETY: user_agent is NUL-terminated and optional proxy pointers are intentionally null.
            let raw_session = unsafe {
                WinHttpOpen(
                    user_agent.as_ptr(),
                    WINHTTP_ACCESS_TYPE_DEFAULT_PROXY,
                    null(),
                    null(),
                    0,
                )
            };
            let session = self.non_null_handle(raw_session, "WinHttpOpen")?;
            // SAFETY: session is a valid WinHTTP session handle.
            unsafe {
                WinHttpSetTimeouts(
                    session.as_ptr(),
                    WINHTTP_RESOLVE_TIMEOUT_MS,
                    WINHTTP_CONNECT_TIMEOUT_MS,
                    WINHTTP_SEND_TIMEOUT_MS,
                    WINHTTP_RECEIVE_TIMEOUT_MS,
                )
            };
            Ok(session)
        }
        fn query_data_available(&self, request: &Handle) -> Result<u32, String> {
            let mut available = 0_u32;
            // SAFETY: available is a valid output buffer and request is a valid request handle.
            let available_ok =
                unsafe { WinHttpQueryDataAvailable(request.as_ptr(), &raw mut available) };
            if available_ok == 0_i32 {
                Err(self.last_error_message("WinHttpQueryDataAvailable"))
            } else {
                Ok(available)
            }
        }
        fn query_headers(&self, request: &Handle) -> Result<Vec<(String, String)>, String> {
            let mut bytes = 0_u32;
            let mut index = 0_u32;
            // SAFETY: request is valid; this first call probes the required buffer size.
            let probe_ok = unsafe {
                WinHttpQueryHeaders(
                    request.as_ptr(),
                    WINHTTP_QUERY_RAW_HEADERS_CRLF,
                    null(),
                    null_mut(),
                    &raw mut bytes,
                    &raw mut index,
                )
            };
            if probe_ok != 0_i32 {
                return Ok(Vec::new());
            }
            let last_error = self.last_error_code();
            if last_error != ERROR_INSUFFICIENT_BUFFER {
                return Err(self.last_error_message("WinHttpQueryHeaders"));
            }
            let header_bytes = usize::try_from(bytes)
                .map_err(|source| format!("응답 헤더 길이 변환 실패: {source}"))?;
            checked_http_buffer_len("헤더", 0, header_bytes, HTTP_MAX_HEADER_BYTES)?;
            let units = header_bytes
                .checked_div(2)
                .ok_or_else(|| String::from("응답 헤더 길이 계산 실패"))?;
            let mut buffer = vec![0_u16; units];
            index = 0;
            // SAFETY: buffer has the size requested by WinHTTP and request is valid.
            let fetch_ok = unsafe {
                WinHttpQueryHeaders(
                    request.as_ptr(),
                    WINHTTP_QUERY_RAW_HEADERS_CRLF,
                    null(),
                    buffer.as_mut_ptr().cast::<c_void>(),
                    &raw mut bytes,
                    &raw mut index,
                )
            };
            if fetch_ok == 0_i32 {
                return Err(self.last_error_message("WinHttpQueryHeaders"));
            }
            while buffer.pop_if(|value| *value == 0).is_some() {}
            let raw = String::from_utf16_lossy(&buffer);
            let mut parsed = Vec::new();
            for line in raw.lines().skip(1) {
                let Some((name, value)) = line.split_once(':') else {
                    continue;
                };
                parsed.push((name.trim_ascii().to_owned(), value.trim_ascii().to_owned()));
            }
            Ok(parsed)
        }
        fn query_status(&self, request: &Handle) -> Result<u32, String> {
            let mut status = 0_u32;
            let mut bytes = u32::try_from(size_of::<u32>())
                .map_err(|source| format!("상태 코드 버퍼 길이 변환 실패: {source}"))?;
            // SAFETY: status and bytes are valid output buffers for the numeric status query.
            let ok = unsafe {
                WinHttpQueryHeaders(
                    request.as_ptr(),
                    WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER,
                    null(),
                    (&raw mut status).cast::<c_void>(),
                    &raw mut bytes,
                    null_mut(),
                )
            };
            if ok == 0_i32 {
                Err(self.last_error_message("WinHttpQueryHeaders status"))
            } else {
                Ok(status)
            }
        }
        fn read_body(&self, request: &Handle) -> Result<Vec<u8>, String> {
            let mut body = Vec::new();
            loop {
                let available = self.query_data_available(request)?;
                if available == 0 {
                    break;
                }
                let chunk_len = usize::try_from(available)
                    .map_err(|source| format!("응답 chunk 길이 변환 실패: {source}"))?;
                let old_len = body.len();
                let buffered_len =
                    checked_http_buffer_len("본문", old_len, chunk_len, HTTP_MAX_BODY_BYTES)?;
                body.try_reserve(chunk_len)
                    .map_err(|source| format!("응답 본문 메모리 확보 실패: {source}"))?;
                body.resize(buffered_len, 0);
                let chunk = body
                    .get_mut(old_len..buffered_len)
                    .ok_or_else(|| String::from("응답 본문 chunk 범위 계산 실패"))?;
                let read = self.read_chunk(request, chunk, available)?;
                let read_len = usize::try_from(read)
                    .map_err(|source| format!("응답 read 길이 변환 실패: {source}"))?;
                let actual_len =
                    checked_http_buffer_len("본문", old_len, read_len, HTTP_MAX_BODY_BYTES)?;
                body.truncate(actual_len);
                if read == 0 {
                    break;
                }
            }
            Ok(body)
        }
        fn read_chunk(
            &self,
            request: &Handle,
            chunk: &mut [u8],
            available: u32,
        ) -> Result<u32, String> {
            let mut read = 0_u32;
            // SAFETY: chunk is a valid writable buffer and read is a valid output buffer.
            let read_ok = unsafe {
                WinHttpReadData(
                    request.as_ptr(),
                    chunk.as_mut_ptr().cast::<c_void>(),
                    available,
                    &raw mut read,
                )
            };
            if read_ok == 0_i32 {
                Err(self.last_error_message("WinHttpReadData"))
            } else {
                Ok(read)
            }
        }
        fn receive_response(&self, request: &Handle) -> Result<(), String> {
            // SAFETY: request is a valid request handle and no reserved pointer is required.
            let received = unsafe { WinHttpReceiveResponse(request.as_ptr(), null_mut()) };
            if received == 0_i32 {
                Err(self.last_error_message("WinHttpReceiveResponse"))
            } else {
                Ok(())
            }
        }
        pub(super) fn request(
            &self,
            method: &str,
            host: &str,
            path: &str,
            request_body: Option<&[u8]>,
            headers: &[(&str, &str)],
        ) -> Result<HttpResponse, String> {
            let user_agent = wide(super::USER_AGENT);
            let host_wide = wide(host);
            let method_wide = wide(method);
            let path_wide = wide(path);
            let session = self.open_session(&user_agent)?;
            let connect = self.connect(&session, &host_wide)?;
            let request = self.open_request(&connect, &method_wide, &path_wide)?;
            let header_capacity = headers.iter().fold(0_usize, |acc, &(name, value)| {
                acc.saturating_add(name.len())
                    .saturating_add(value.len())
                    .saturating_add(4)
            });
            let mut headers_text = String::new();
            headers_text
                .try_reserve(header_capacity)
                .map_err(|source| format!("요청 헤더 메모리 확보 실패: {source}"))?;
            for header in headers {
                let name = header.0;
                let value = header.1;
                headers_text.push_str(name);
                headers_text.push_str(": ");
                headers_text.push_str(value);
                headers_text.push_str("\r\n");
            }
            let headers_wide = wide(&headers_text);
            let body_slice = match request_body {
                Some(body) => body,
                None => &[],
            };
            let body_len = u32::try_from(body_slice.len())
                .map_err(|source| format!("요청 본문 길이 변환 실패: {source}"))?;
            self.send_request(&request, &headers_wide, body_slice, body_len)?;
            self.receive_response(&request)?;
            let status = self.query_status(&request)?;
            let response_headers = self.query_headers(&request)?;
            enforce_http_content_length_limit(&response_headers, HTTP_MAX_BODY_BYTES)?;
            let response_body = self.read_body(&request)?;
            Ok(HttpResponse {
                body: response_body,
                headers: response_headers,
                status,
            })
        }
        fn send_request(
            &self,
            request: &Handle,
            headers: &[u16],
            body: &[u8],
            body_len: u32,
        ) -> Result<(), String> {
            let header_len = u32::try_from(headers.len().saturating_sub(1))
                .map_err(|source| format!("요청 헤더 길이 변환 실패: {source}"))?;
            let body_ptr = if body.is_empty() {
                null()
            } else {
                body.as_ptr().cast::<c_void>()
            };
            // SAFETY: request is valid, headers are NUL-terminated, and body_ptr is null or points to body.
            let sent = unsafe {
                WinHttpSendRequest(
                    request.as_ptr(),
                    headers.as_ptr(),
                    header_len,
                    body_ptr,
                    body_len,
                    body_len,
                    0,
                )
            };
            if sent == 0_i32 {
                Err(self.last_error_message("WinHttpSendRequest"))
            } else {
                Ok(())
            }
        }
    }
    fn wide(value: &str) -> Vec<u16> {
        OsStr::new(value).encode_wide().chain([0]).collect::<Vec<_>>()
    }
        }
    }
    _ => {}
}
const HTTP_MAX_BODY_BYTES: usize = 64 * 1024 * 1024;
const HTTP_MAX_HEADER_BYTES: usize = 1024 * 1024;
const OPINET_HOST: &str = "www.opinet.co.kr";
const NETFUNNEL_HOST: &str = "nfl.opinet.co.kr";
const OPDOWNLOAD_PATH: &str = "/user/opdown/opDownload.do";
const OPDOWNLOAD_URL: &str = "https://www.opinet.co.kr/user/opdown/opDownload.do";
const OPDOWNLOAD_LAYOUT_PATH: &str = "/user/main/main_move_price.do";
const OPDOWNLOAD_EXCEL_PATH: &str = "/user/main/main_download_excel.do";
const OIL_PRICE_DOWNLOAD_TAR_URL: &str = "/user/opdown/oil_price_download";
const OPINET_KEY: &str = "tNNJ/zjnjSUqxRLpgiO/at1/w4SoJGbzzDOFVmlgEO0=";
const NETFUNNEL_SERVICE_ID: &str = "service_1";
const NETFUNNEL_ENTRY_ACTION_ID: &str = "B1";
const NETFUNNEL_DOWNLOAD_ACTION_ID: &str = "B7";
const CURRENT_PRICE_PAGE_DIV: &str = "PAGE_DIV_2";
const GAS_STATION_LPG_CODE: &str = "A";
const GAS_STATION_API_GBN: &str = "A";
const DEFAULT_REGION_LABEL: &str = "선택하세요.";
const USER_AGENT: &str = concat!("fcupdater/", env!("CARGO_PKG_VERSION"));
const NETFUNNEL_POLL_LIMIT: usize = 20;
pub const AUTO_SOURCE_MARKER: &str = "__fcupdater_auto__";
const TASKS: [Task; 11] = [
    Task {
        sido: "대전광역시",
        sigungu: "대덕구",
    },
    Task {
        sido: "대전광역시",
        sigungu: "동구",
    },
    Task {
        sido: "대전광역시",
        sigungu: "서구",
    },
    Task {
        sido: "대전광역시",
        sigungu: "유성구",
    },
    Task {
        sido: "대전광역시",
        sigungu: "중구",
    },
    Task {
        sido: "세종특별자치시",
        sigungu: "세종시",
    },
    Task {
        sido: "충청북도",
        sigungu: "청주시",
    },
    Task {
        sido: "충청남도",
        sigungu: "공주시",
    },
    Task {
        sido: "충청남도",
        sigungu: "보령시",
    },
    Task {
        sido: "충청남도",
        sigungu: "아산시",
    },
    Task {
        sido: "충청남도",
        sigungu: "천안시",
    },
];
#[derive(Debug, Clone, Copy)]
struct Task {
    sido: &'static str,
    sigungu: &'static str,
}
struct TaskMatcher {
    sido_key: String,
    task_keys: Vec<String>,
}
#[derive(Debug)]
struct HttpResponse {
    body: Vec<u8>,
    headers: Vec<(String, String)>,
    status: u32,
}
struct HttpClient {
    cookies: Vec<Cookie>,
}
#[derive(Clone)]
struct Cookie {
    name: String,
    value: String,
}
pub struct SourceDownloadOps;
pub trait SourceDownloadApi {
    fn filter_target_region_records(&self, records: Vec<SourceRecord>)
    -> Result<Vec<SourceRecord>>;
    fn refresh_sources(
        &self,
        dir: &Path,
        prefix: &str,
        _out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>>;
}
impl SourceDownloadApi for SourceDownloadOps {
    fn filter_target_region_records(
        &self,
        records: Vec<SourceRecord>,
    ) -> Result<Vec<SourceRecord>> {
        self.filter_target_region_records_impl(records)
    }
    fn refresh_sources(
        &self,
        dir: &Path,
        prefix: &str,
        out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>> {
        self.refresh_sources_impl(dir, prefix, out)
    }
}
trait SourceDownloadOpsExt {
    fn filter_target_region_records_impl(
        &self,
        records: Vec<SourceRecord>,
    ) -> Result<Vec<SourceRecord>>;
    fn refresh_sources_impl(
        &self,
        dir: &Path,
        prefix: &str,
        out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>>;
}
trait SourceDownloadWorkflowExt {
    fn auto_source_name(&self, prefix: &str, extension: &str) -> String;
    fn cleanup_auto_source_files(&self, dir: &Path, prefix: &str) -> StdResult<usize, String>;
    fn download_nationwide_source(&self, dir: &Path, prefix: &str) -> Result<Vec<PathBuf>>;
    fn download_nationwide_source_http(
        &self,
        dir: &Path,
        prefix: &str,
    ) -> StdResult<PathBuf, String>;
    fn record_matches_any_task(&self, record: &SourceRecord, matchers: &[TaskMatcher]) -> bool;
    fn region_has_explicit_sigungu(&self, region: &str) -> bool;
    fn task_match_keys(&self, task: &Task) -> Vec<String>;
    fn task_matchers(&self) -> &'static [TaskMatcher];
}
trait HttpClientExt {
    fn add_cookie(&mut self, name: &str, value: &str) -> StdResult<(), String>;
    fn cookie_header(&self) -> Option<String>;
    fn extract_netfunnel_key(result: &str) -> StdResult<String, String>;
    fn fetch_netfunnel_ticket(&mut self, action_id: &str) -> StdResult<String, String>;
    fn get_text(
        &mut self,
        host: &str,
        path: &str,
        referer: Option<&str>,
    ) -> StdResult<String, String>;
    fn percent_encoded(bytes: &[u8]) -> String;
    fn post_form(
        &mut self,
        host: &str,
        path: &str,
        form: &[(&str, &str)],
        referer: Option<&str>,
        ajax: bool,
    ) -> StdResult<HttpResponse, String>;
    fn push_percent_encoded(out: &mut String, bytes: &[u8]);
    fn request(
        &mut self,
        method: &str,
        host: &str,
        path: &str,
        body: Option<&[u8]>,
        headers: &[(&str, &str)],
    ) -> StdResult<HttpResponse, String>;
    fn request_netfunnel(
        &mut self,
        action_id: &str,
        key: Option<&str>,
        ttl: Option<u32>,
    ) -> StdResult<String, String>;
    fn store_response_cookies(&mut self, response: &HttpResponse) -> StdResult<(), String>;
}
impl SourceDownloadOpsExt for SourceDownloadOps {
    fn filter_target_region_records_impl(
        &self,
        records: Vec<SourceRecord>,
    ) -> Result<Vec<SourceRecord>> {
        let matchers = self.task_matchers();
        let mut filtered = Vec::new();
        filtered
            .try_reserve_exact(records.len())
            .map_err(|source| {
                let record_count = records.len();
                err_with_source(
                    format!("필터링 소스 레코드 목록 메모리 확보 실패: {record_count} records"),
                    source,
                )
            })?;
        for record in records {
            if self.record_matches_any_task(&record, matchers) {
                filtered.push(record);
            }
        }
        Ok(filtered)
    }
    fn refresh_sources_impl(
        &self,
        dir: &Path,
        prefix: &str,
        out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>> {
        fs::create_dir_all(dir).map_err(|source_err| {
            err(path_source_message("소스 폴더 생성 실패", dir, source_err))
        })?;
        let canonical_dir = dir.canonicalize().map_err(|source_err| {
            err(path_source_message(
                "소스 폴더 경로 확인 실패",
                dir,
                source_err,
            ))
        })?;
        let removed = self
            .cleanup_auto_source_files(&canonical_dir, prefix)
            .map_err(|error_text| {
                err(prefixed_message("기존 자동 소스 정리 실패: ", error_text))
            })?;
        if removed > 0 {
            let _write_result = writeln!(out, "이전 임시 소스 파일 {removed}개 정리");
        }
        self.download_nationwide_source(&canonical_dir, prefix)
    }
}
impl SourceDownloadWorkflowExt for SourceDownloadOps {
    fn auto_source_name(&self, prefix: &str, extension: &str) -> String {
        format!("{prefix}{AUTO_SOURCE_MARKER}_opdownload_current_price.{extension}")
    }
    fn cleanup_auto_source_files(&self, dir: &Path, prefix: &str) -> StdResult<usize, String> {
        let mut removed = 0_usize;
        let prefix_fold = prefix.to_lowercase();
        let entries =
            fs::read_dir(dir).map_err(|error| path_source_message("폴더 읽기 실패", dir, error))?;
        for entry_result in entries {
            let dir_entry = entry_result
                .map_err(|error| prefixed_message("디렉터리 항목 읽기 실패: ", error))?;
            let path = dir_entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let folded = file_name.to_lowercase();
            if !(folded.starts_with(&prefix_fold) && folded.contains(AUTO_SOURCE_MARKER)) {
                continue;
            }
            fs::remove_file(&path)
                .map_err(|error| path_source_message("자동 소스 파일 삭제 실패", &path, error))?;
            removed = removed.saturating_add(1);
        }
        Ok(removed)
    }
    fn download_nationwide_source(&self, dir: &Path, prefix: &str) -> Result<Vec<PathBuf>> {
        let downloaded =
            self.download_nationwide_source_http(dir, prefix)
                .map_err(|error_text| {
                    err(prefixed_message("Opinet 자동 다운로드 실패: ", error_text))
                })?;
        Ok(vec![downloaded])
    }
    fn download_nationwide_source_http(
        &self,
        dir: &Path,
        prefix: &str,
    ) -> StdResult<PathBuf, String> {
        let mut client = HttpClient {
            cookies: Vec::new(),
        };
        let _gate_html = client.get_text(OPINET_HOST, OPDOWNLOAD_PATH, None)?;
        let entry_key = client.fetch_netfunnel_ticket(NETFUNNEL_ENTRY_ACTION_ID)?;
        let _entry_page = client.post_form(
            OPINET_HOST,
            OPDOWNLOAD_PATH,
            &[
                ("netfunnel_key", entry_key.as_str()),
                ("opinet_key", OPINET_KEY),
            ],
            Some(OPDOWNLOAD_URL),
            false,
        )?;
        let _layout = client.post_form(
            OPINET_HOST,
            OPDOWNLOAD_LAYOUT_PATH,
            &[("tarUrl", OIL_PRICE_DOWNLOAD_TAR_URL)],
            Some(OPDOWNLOAD_URL),
            true,
        )?;
        let download_key = client.fetch_netfunnel_ticket(NETFUNNEL_DOWNLOAD_ACTION_ID)?;
        let response = client.post_form(
            OPINET_HOST,
            OPDOWNLOAD_EXCEL_PATH,
            &[
                ("LPG_CD", GAS_STATION_LPG_CODE),
                ("DATE_DIV_CD", ""),
                ("PAGE_DIV", CURRENT_PRICE_PAGE_DIV),
                ("SIDO_NM", DEFAULT_REGION_LABEL),
                ("SIGUN_NM", DEFAULT_REGION_LABEL),
                ("API_GBN", GAS_STATION_API_GBN),
                ("netfunnel_key", download_key.as_str()),
            ],
            Some(OPDOWNLOAD_URL),
            false,
        )?;
        let has_excel_signature = response
            .body
            .starts_with(&[0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1])
            || response.body.starts_with(b"PK\x03\x04");
        if !has_excel_signature {
            let preview = lossy_prefix(&response.body, 512);
            return Err(prefixed_message(
                "다운로드 응답이 Excel 파일이 아닙니다: ",
                preview,
            ));
        }
        let mut extension = "xls";
        for header in &response.headers {
            let name = &header.0;
            let value = &header.1;
            if !name.eq_ignore_ascii_case("content-disposition") {
                continue;
            }
            if contains_ascii_ignore_case(value, b".xlsx") {
                extension = "xlsx";
                break;
            }
            if contains_ascii_ignore_case(value, b".xls") {
                break;
            }
        }
        let target = dir.join(self.auto_source_name(prefix, extension));
        let temp = dir.join(self.auto_source_name(prefix, "tmp"));
        fs::write(&temp, &response.body)
            .map_err(|error| path_source_message("다운로드 파일 쓰기 실패", &temp, error))?;
        match fs::rename(&temp, &target) {
            Ok(()) => {}
            Err(error) => {
                let _cleanup_result = fs::remove_file(&temp);
                return Err(path_source_message(
                    "다운로드 파일 이름 변경 실패",
                    &target,
                    error,
                ));
            }
        }
        Ok(target)
    }
    fn record_matches_any_task(&self, record: &SourceRecord, matchers: &[TaskMatcher]) -> bool {
        let region_key = normalize_address_key(&record.region);
        let region_has_explicit_sigungu =
            !region_key.is_empty() && self.region_has_explicit_sigungu(&record.region);
        let mut combined_key: Option<String> = None;
        matchers.iter().any(|matcher| {
            let matches_task = |value: &str| {
                matcher
                    .task_keys
                    .iter()
                    .any(|task_key| value.contains(task_key))
            };
            if !region_key.is_empty() {
                if !region_key.contains(&matcher.sido_key) {
                    return false;
                }
                if matches_task(&region_key) {
                    return true;
                }
                if region_has_explicit_sigungu {
                    return false;
                }
            }
            let combined = combined_key.get_or_insert_with(|| {
                let combined_source = format!("{} {}", record.region, record.address);
                normalize_address_key(&combined_source)
            });
            combined.contains(&matcher.sido_key) && matches_task(combined)
        })
    }
    fn region_has_explicit_sigungu(&self, region: &str) -> bool {
        let mut tokens = region.split_whitespace();
        let Some(first_token) = tokens.next() else {
            return false;
        };
        has_basic_region_suffix(first_token)
            || ((is_province_token(first_token) || is_metropolitan_token(first_token))
                && tokens.next().is_some_and(has_basic_region_suffix))
    }
    fn task_match_keys(&self, task: &Task) -> Vec<String> {
        let mut keys = Vec::with_capacity(4);
        let mut push_alias_key = |alias: &str| {
            let alias_key = normalize_address_key(alias);
            if !alias_key.is_empty() && !keys.contains(&alias_key) {
                keys.push(alias_key);
            }
            if let Some(stripped_alias) = strip_basic_region_suffix(alias) {
                let stripped = normalize_address_key(stripped_alias);
                if !stripped.is_empty() && !keys.contains(&stripped) {
                    keys.push(stripped);
                }
            }
        };
        push_alias_key(task.sigungu);
        if task.sigungu == "세종시" {
            push_alias_key("세종특별자치시");
        }
        keys
    }
    fn task_matchers(&self) -> &'static [TaskMatcher] {
        static TASK_MATCHERS: LazyLock<Vec<TaskMatcher>> = LazyLock::new(|| {
            let ops = SourceDownloadOps;
            TASKS
                .iter()
                .map(|task| TaskMatcher {
                    sido_key: normalize_address_key(task.sido),
                    task_keys: ops.task_match_keys(task),
                })
                .collect::<Vec<_>>()
        });
        TASK_MATCHERS.as_slice()
    }
}
impl HttpClientExt for HttpClient {
    fn add_cookie(&mut self, name: &str, value: &str) -> StdResult<(), String> {
        if let Some(cookie) = self.cookies.iter_mut().find(|cookie| cookie.name == name) {
            cookie.value.clear();
            cookie
                .value
                .try_reserve(value.len())
                .map_err(|source| prefixed_message("Cookie 값 메모리 확보 실패: ", source))?;
            cookie.value.push_str(value);
            return Ok(());
        }
        let mut cookie = Cookie {
            name: String::new(),
            value: String::new(),
        };
        cookie
            .name
            .try_reserve(name.len())
            .map_err(|source| prefixed_message("Cookie 이름 메모리 확보 실패: ", source))?;
        cookie
            .value
            .try_reserve(value.len())
            .map_err(|source| prefixed_message("Cookie 값 메모리 확보 실패: ", source))?;
        cookie.name.push_str(name);
        cookie.value.push_str(value);
        self.cookies.push(cookie);
        Ok(())
    }
    fn cookie_header(&self) -> Option<String> {
        if self.cookies.is_empty() {
            return None;
        }
        let pairs = self
            .cookies
            .iter()
            .map(|cookie| format!("{}={}", cookie.name, cookie.value))
            .collect::<Vec<_>>();
        Some(pairs.join("; "))
    }
    fn extract_netfunnel_key(result: &str) -> StdResult<String, String> {
        let Some((_, tail)) = result.split_once("key=") else {
            return Err(prefixed_message("NetFunnel key 없음: ", result));
        };
        let value = split_head_or_all(tail, '&');
        if value.is_empty() {
            return Err(prefixed_message("NetFunnel key 비어 있음: ", result));
        }
        Ok(value.to_owned())
    }
    fn fetch_netfunnel_ticket(&mut self, action_id: &str) -> StdResult<String, String> {
        let mut current_key: Option<String> = None;
        for _ in 0..NETFUNNEL_POLL_LIMIT {
            let result = self.request_netfunnel(action_id, current_key.as_deref(), None)?;
            self.add_cookie("NetFunnel_ID", &result)?;
            let mut parts = result.split(':');
            let _opcode = parts.next();
            let Some(code_text) = parts.next() else {
                return Err(prefixed_message("NetFunnel 코드 없음: ", result));
            };
            let code = code_text
                .parse::<u32>()
                .map_err(|source| prefixed_message("NetFunnel 코드 파싱 실패: ", source))?;
            if matches!(code, 200 | 300 | 303) {
                return Self::extract_netfunnel_key(&result);
            }
            if matches!(code, 201 | 202 | 302) {
                current_key = Some(Self::extract_netfunnel_key(&result)?);
                let wait_secs = result
                    .split_once("ttl=")
                    .map(|(_, tail)| split_head_or_all(tail, '&'))
                    .and_then(|ttl_text| ttl_text.parse::<u32>().ok())
                    .unwrap_or(1)
                    .clamp(1, 30);
                sleep(Duration::from_secs(u64::from(wait_secs)));
                continue;
            }
            return Err(prefixed_message("NetFunnel 응답 오류: ", result));
        }
        Err(String::from("NetFunnel 대기 횟수를 초과했습니다."))
    }
    fn get_text(
        &mut self,
        host: &str,
        path: &str,
        referer: Option<&str>,
    ) -> StdResult<String, String> {
        let mut headers = Vec::with_capacity(3);
        headers.push((
            "Accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        ));
        if let Some(referer_value) = referer {
            headers.push(("Referer", referer_value));
        }
        let response = self.request("GET", host, path, None, &headers)?;
        String::from_utf8(response.body)
            .map_err(|source| prefixed_message("HTTP 응답 UTF-8 변환 실패: ", source))
    }
    fn percent_encoded(bytes: &[u8]) -> String {
        let mut out = String::with_capacity(bytes.len().saturating_mul(3));
        Self::push_percent_encoded(&mut out, bytes);
        out
    }
    fn post_form(
        &mut self,
        host: &str,
        path: &str,
        form: &[(&str, &str)],
        referer: Option<&str>,
        ajax: bool,
    ) -> StdResult<HttpResponse, String> {
        let body = form
            .iter()
            .map(|&(name, value)| {
                format!(
                    "{}={}",
                    Self::percent_encoded(name.as_bytes()),
                    Self::percent_encoded(value.as_bytes())
                )
            })
            .collect::<Vec<_>>()
            .join("&");
        let mut headers = Vec::with_capacity(6);
        headers.push((
            "Content-Type",
            "application/x-www-form-urlencoded; charset=UTF-8",
        ));
        headers.push(("Accept", "text/html, */*; q=0.01"));
        if ajax {
            headers.push(("X-Requested-With", "XMLHttpRequest"));
        }
        if let Some(referer_value) = referer {
            headers.push(("Referer", referer_value));
        }
        self.request("POST", host, path, Some(body.as_bytes()), &headers)
    }
    fn push_percent_encoded(out: &mut String, bytes: &[u8]) {
        for byte in bytes {
            match *byte {
                unreserved
                    if unreserved.is_ascii_alphanumeric()
                        || matches!(unreserved, b'-' | b'_' | b'.' | b'~') =>
                {
                    out.push(char::from(unreserved));
                }
                b' ' => out.push('+'),
                other => {
                    let high = other >> 4_u8;
                    let low = other & 0x0F;
                    out.push('%');
                    out.push(match high {
                        0..=9 => char::from(b'0'.saturating_add(high)),
                        10..=15 => char::from(b'A'.saturating_add(high.saturating_sub(10))),
                        _ => '?',
                    });
                    out.push(match low {
                        0..=9 => char::from(b'0'.saturating_add(low)),
                        10..=15 => char::from(b'A'.saturating_add(low.saturating_sub(10))),
                        _ => '?',
                    });
                }
            }
        }
    }
    fn request(
        &mut self,
        method: &str,
        host: &str,
        path: &str,
        body: Option<&[u8]>,
        headers: &[(&str, &str)],
    ) -> StdResult<HttpResponse, String> {
        let mut merged_headers = Vec::with_capacity(headers.len().saturating_add(3));
        merged_headers.push(("User-Agent", USER_AGENT));
        merged_headers.push(("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.5,en;q=0.3"));
        for header in headers {
            merged_headers.push(*header);
        }
        let cookie_header = self.cookie_header();
        if let Some(cookie_text) = cookie_header.as_deref() {
            merged_headers.push(("Cookie", cookie_text));
        }
        let response = {
            cfg_select! {
                windows => {
                    winhttp::CLIENT.request(method, host, path, body, &merged_headers)
                }
                any(target_os = "linux", target_os = "macos") => {
                    libcurl::CLIENT.request(method, host, path, body, &merged_headers)
                }
                _ => {
                    let _ = (method, host, path, body, headers, &merged_headers);
                    Err(String::from(
                        "외부 TLS 크레이트 없이 HTTPS 다운로드를 수행하려면 Windows WinHTTP 또는 Linux/macOS libcurl이 필요합니다.",
                    ))
                }
            }
        }?;
        enforce_http_content_length_limit(&response.headers, HTTP_MAX_BODY_BYTES)?;
        self.store_response_cookies(&response)?;
        if !(200..300).contains(&response.status) {
            let body_preview = lossy_prefix(&response.body, 512);
            let status = response.status;
            return Err(format!("HTTP {status}: {body_preview}"));
        }
        Ok(response)
    }
    fn request_netfunnel(
        &mut self,
        action_id: &str,
        key: Option<&str>,
        ttl: Option<u32>,
    ) -> StdResult<String, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .map_err(|source| prefixed_message("현재 시간 조회 실패: ", source))?;
        let opcode = if key.is_some() { "5002" } else { "5101" };
        let key_fragment = key.map_or_else(String::new, |key_value| {
            format!("&key={}", Self::percent_encoded(key_value.as_bytes()))
        });
        let ttl_fragment = ttl.map_or_else(String::new, |ttl_value| format!("&ttl={ttl_value}"));
        let path = format!(
            "/ts.wseq?opcode={opcode}{key_fragment}&nfid=0&prefix=NetFunnel.gRtype%3D{opcode}%3B{ttl_fragment}&sid={NETFUNNEL_SERVICE_ID}&aid={action_id}&js=yes&{timestamp}"
        );
        let response = self.request(
            "GET",
            NETFUNNEL_HOST,
            &path,
            None,
            &[("Accept", "application/javascript,*/*;q=0.8")],
        )?;
        let text = String::from_utf8(response.body)
            .map_err(|source| prefixed_message("NetFunnel 응답 UTF-8 변환 실패: ", source))?;
        let result = text
            .split_once("result='")
            .and_then(|(_, rest)| rest.split_once('\''))
            .map(|(value, _)| value);
        result
            .map(str::to_owned)
            .ok_or_else(|| prefixed_message("NetFunnel result 파싱 실패: ", text))
    }
    fn store_response_cookies(&mut self, response: &HttpResponse) -> StdResult<(), String> {
        for header in &response.headers {
            let name = &header.0;
            let value = &header.1;
            if !name.eq_ignore_ascii_case("set-cookie") {
                continue;
            }
            let cookie_pair = split_head_or_all(value, ';');
            let Some((cookie_name, cookie_value)) = cookie_pair.split_once('=') else {
                continue;
            };
            self.add_cookie(cookie_name.trim_ascii(), cookie_value.trim_ascii())?;
        }
        Ok(())
    }
}
fn lossy_prefix(bytes: &[u8], max_len: usize) -> Cow<'_, str> {
    let prefix_len = bytes.len().min(max_len);
    let Some((prefix, _)) = bytes.split_at_checked(prefix_len) else {
        return String::from_utf8_lossy(bytes);
    };
    String::from_utf8_lossy(prefix)
}
fn split_head_or_all(value: &str, separator: char) -> &str {
    match value.split_once(separator) {
        Some((head, _)) => head,
        None => value,
    }
}
#[cfg(windows)]
fn checked_http_buffer_len(
    label: &str,
    current_len: usize,
    additional_len: usize,
    limit: usize,
) -> StdResult<usize, String> {
    let next_len = current_len
        .checked_add(additional_len)
        .ok_or_else(|| format!("HTTP 응답 {label} 크기 계산 실패"))?;
    if next_len > limit {
        Err(format!(
            "HTTP 응답 {label} 크기가 허용 한도({limit} bytes)를 초과했습니다."
        ))
    } else {
        Ok(next_len)
    }
}
fn contains_ascii_ignore_case<const N: usize>(text: &str, needle: &[u8; N]) -> bool {
    text.as_bytes()
        .array_windows::<N>()
        .any(|window| window.eq_ignore_ascii_case(needle))
}
fn enforce_http_content_length_limit(
    headers: &[(String, String)],
    limit: usize,
) -> StdResult<(), String> {
    for header in headers {
        let name = &header.0;
        let value = &header.1;
        if !name.eq_ignore_ascii_case("Content-Length") {
            continue;
        }
        let parsed = value
            .trim_ascii()
            .parse::<usize>()
            .map_err(|source| format!("HTTP Content-Length 해석 실패: {source}"))?;
        if parsed > limit {
            return Err(format!(
                "HTTP Content-Length가 허용 한도({limit} bytes)를 초과했습니다."
            ));
        }
    }
    Ok(())
}
