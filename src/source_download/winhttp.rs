use super::{
    HTTP_MAX_BODY_BYTES, HTTP_MAX_HEADER_BYTES, checked_http_buffer_len,
    enforce_http_content_length_limit,
    http_client::{HttpResponse, HttpStreamResponse},
    StreamedBodySummary, StreamingBodySink,
};
use alloc::{string::String, vec::Vec};
use core::{
    ffi::c_void,
    ptr::{NonNull, null, null_mut},
};
use std::ffi::OsStr;
use std::io::Write as IoWrite;
use std::os::windows::ffi::OsStrExt as WindowsOsStrExt;
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
            WinHttpConnect(
                session.as_ptr(),
                host.as_ptr(),
                INTERNET_DEFAULT_HTTPS_PORT,
                0,
            )
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
            );
        }
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
            .ok_or_else(|| "응답 헤더 길이 계산 실패".to_owned())?;
        let mut buffer = Vec::new();
        buffer
            .try_reserve(units)
            .map_err(|source| format!("응답 헤더 버퍼 메모리 확보 실패: {source}"))?;
        buffer.resize(units, 0_u16);
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
        parsed
            .try_reserve(raw.lines().count().saturating_sub(1))
            .map_err(|source| format!("응답 header 목록 메모리 확보 실패: {source}"))?;
        for line in raw.lines().skip(1) {
            let Some((raw_name, raw_value)) = line.split_once(':') else {
                continue;
            };
            let name = raw_name.trim_ascii();
            let value = raw_value.trim_ascii();
            let mut header_name = String::new();
            header_name
                .try_reserve(name.len())
                .map_err(|source| format!("응답 header 이름 메모리 확보 실패: {source}"))?;
            header_name.push_str(name);
            let mut header_value = String::new();
            header_value
                .try_reserve(value.len())
                .map_err(|source| format!("응답 header 값 메모리 확보 실패: {source}"))?;
            header_value.push_str(value);
            parsed.push((header_name, header_value));
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
                .ok_or_else(|| "응답 본문 chunk 범위 계산 실패".to_owned())?;
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
    fn read_body_to_writer(
        &self,
        request: &Handle,
        writer: &mut dyn IoWrite,
    ) -> Result<StreamedBodySummary, String> {
        let mut sink = StreamingBodySink {
            error: None,
            limit: HTTP_MAX_BODY_BYTES,
            summary: StreamedBodySummary {
                bytes_seen: 0,
                prefix: Vec::new(),
                preview: Vec::new(),
            },
            writer,
        };
        let mut chunk_buffer = Vec::new();
        loop {
            let available = self.query_data_available(request)?;
            if available == 0 {
                break;
            }
            let chunk_len = usize::try_from(available)
                .map_err(|source| format!("응답 chunk 길이 변환 실패: {source}"))?;
            checked_http_buffer_len("본문", 0, chunk_len, HTTP_MAX_BODY_BYTES)?;
            chunk_buffer.clear();
            chunk_buffer
                .try_reserve(chunk_len)
                .map_err(|source| format!("응답 본문 chunk 메모리 확보 실패: {source}"))?;
            chunk_buffer.resize(chunk_len, 0);
            let read = self.read_chunk(request, &mut chunk_buffer, available)?;
            let read_len = usize::try_from(read)
                .map_err(|source| format!("응답 read 길이 변환 실패: {source}"))?;
            checked_http_buffer_len("본문", 0, read_len, HTTP_MAX_BODY_BYTES)?;
            chunk_buffer.truncate(read_len);
            if !sink.append(&chunk_buffer) {
                return Err(sink
                    .error
                    .take()
                    .unwrap_or_else(|| "응답 본문 파일 쓰기 실패".to_owned()));
            }
            if read == 0 {
                break;
            }
        }
        Ok(sink.summary)
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
        let user_agent = wide(super::USER_AGENT)?;
        let host_wide = wide(host)?;
        let method_wide = wide(method)?;
        let path_wide = wide(path)?;
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
        let headers_wide = wide(&headers_text)?;
        let body_slice = request_body.unwrap_or_default();
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
    pub(super) fn request_to_writer(
        &self,
        method: &str,
        host: &str,
        path: &str,
        request_body: Option<&[u8]>,
        headers: &[(&str, &str)],
        writer: &mut dyn IoWrite,
    ) -> Result<HttpStreamResponse, String> {
        let user_agent = wide(super::USER_AGENT)?;
        let host_wide = wide(host)?;
        let method_wide = wide(method)?;
        let path_wide = wide(path)?;
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
        let headers_wide = wide(&headers_text)?;
        let body_slice = request_body.unwrap_or_default();
        let body_len = u32::try_from(body_slice.len())
            .map_err(|source| format!("요청 본문 길이 변환 실패: {source}"))?;
        self.send_request(&request, &headers_wide, body_slice, body_len)?;
        self.receive_response(&request)?;
        let status = self.query_status(&request)?;
        let response_headers = self.query_headers(&request)?;
        enforce_http_content_length_limit(&response_headers, HTTP_MAX_BODY_BYTES)?;
        let response_body = self.read_body_to_writer(&request, writer)?;
        Ok(HttpStreamResponse {
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
fn wide(value: &str) -> Result<Vec<u16>, String> {
    let capacity = value
        .len()
        .checked_add(1)
        .ok_or_else(|| "wide 문자열 용량 계산 실패".to_owned())?;
    let mut out = Vec::new();
    out.try_reserve(capacity)
        .map_err(|source| format!("wide 문자열 메모리 확보 실패: {source}"))?;
    out.extend(<OsStr as WindowsOsStrExt>::encode_wide(OsStr::new(value)));
    out.push(0);
    Ok(out)
}
