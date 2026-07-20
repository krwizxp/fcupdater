use super::{
    DownloadResult, HTTP_MAX_BODY_BYTES, HTTP_MAX_HEADER_BYTES, HttpHeader, HttpHeaderKind,
    HttpResponse, HttpStreamResponse, RESPONSE_HEADER_CONTENT_LENGTH, RESPONSE_HEADER_SET_COOKIE,
    StreamedBodySummary, StreamingBodySink,
    checked_http_buffer_len, download_error_with_source, enforce_http_body_length,
    validated_http_content_length,
    http_client::HttpMethod,
};
use alloc::{string::String, vec::Vec};
use core::{
    array::from_fn,
    ffi::c_void,
    mem,
    ptr::{NonNull, null, null_mut},
    result::Result as CoreResult,
    time::Duration,
};
use std::{
    ffi::OsStr,
    io::Write as IoWrite,
    os::windows::ffi::OsStrExt as WindowsOsStrExt,
    time::Instant,
};
mod sys;
const DWORD_BYTE_SIZE: u32 = 4;
const ERROR_INSUFFICIENT_BUFFER: u32 = 122;
const HTTP_MAX_HEADER_BYTES_DWORD: u32 = 256 * 1024;
const INTERNET_DEFAULT_HTTPS_PORT: u16 = 443;
const WINHTTP_ACCESS_TYPE_AUTOMATIC_PROXY: u32 = 4;
const WINHTTP_FLAG_SECURE: u32 = 0x0080_0000;
const WINHTTP_OPTION_DISABLE_FEATURE: u32 = 63;
const WINHTTP_OPTION_ENABLE_FEATURE: u32 = 79;
const WINHTTP_OPTION_SECURE_PROTOCOLS: u32 = 84;
const WINHTTP_OPTION_MAX_RESPONSE_HEADER_SIZE: u32 = 91;
const WINHTTP_OPTION_DISABLE_SECURE_PROTOCOL_FALLBACK: u32 = 144;
const WINHTTP_OPTION_IPV6_FAST_FALLBACK: u32 = 140;
const WINHTTP_OPTION_DISABLE_GLOBAL_POOLING: u32 = 195;
const WINHTTP_SESSION_OPTIONS: [(u32, &str, Option<u32>); 3] = [
    (
        WINHTTP_OPTION_DISABLE_SECURE_PROTOCOL_FALLBACK,
        "WinHttpSetOption DISABLE_SECURE_PROTOCOL_FALLBACK",
        None,
    ),
    (
        WINHTTP_OPTION_DISABLE_GLOBAL_POOLING,
        "WinHttpSetOption DISABLE_GLOBAL_POOLING",
        Some(ERROR_WINHTTP_INVALID_OPTION),
    ),
    (
        WINHTTP_OPTION_IPV6_FAST_FALLBACK,
        "WinHttpSetOption IPV6_FAST_FALLBACK",
        None,
    ),
];
const WINHTTP_FLAG_SECURE_PROTOCOL_TLS1_2: u32 = 0x0000_0800;
const WINHTTP_FLAG_SECURE_PROTOCOL_TLS1_3: u32 = 0x0000_2000;
const WINHTTP_SECURE_PROTOCOLS_MIN_TLS_1_2: u32 =
    WINHTTP_FLAG_SECURE_PROTOCOL_TLS1_2 | WINHTTP_FLAG_SECURE_PROTOCOL_TLS1_3;
const WINHTTP_DISABLE_COOKIES: u32 = 0x0000_0001;
const WINHTTP_DISABLE_REDIRECTS: u32 = 0x0000_0002;
const WINHTTP_ENABLE_SSL_REVOCATION: u32 = 0x0000_0001;
const ERROR_INVALID_PARAMETER: u32 = 87;
const ERROR_WINHTTP_INVALID_OPTION: u32 = 12_009;
const WINHTTP_QUERY_FLAG_NUMBER: u32 = 0x2000_0000;
const WINHTTP_QUERY_RAW_HEADERS_CRLF: u32 = 22;
const WINHTTP_QUERY_STATUS_CODE: u32 = 19;
const WINHTTP_CONNECT_TIMEOUT_MS: i32 = 30_000;
const WINHTTP_CONNECT_CACHE_LIMIT: usize = 4;
const WINHTTP_RECEIVE_TIMEOUT_MS: i32 = 60_000;
const WINHTTP_RESOLVE_TIMEOUT_MS: i32 = 30_000;
const WINHTTP_SEND_TIMEOUT_MS: i32 = 60_000;
const WINHTTP_TOTAL_TIMEOUT: Duration = Duration::from_mins(1);
const WINHTTP_READ_BUFFER_BYTES: usize = 64 * 1024;
const HEADER_SEPARATOR_WIDE: [u16; 2] = [0x3A, 0x20];
const HEADER_TERMINATOR_WIDE: [u16; 2] = [0x0D, 0x0A];
const METHOD_GET_WIDE: [u16; 4] = [0x47, 0x45, 0x54, 0];
const METHOD_POST_WIDE: [u16; 5] = [0x50, 0x4F, 0x53, 0x54, 0];
type HInternet = *mut c_void;
#[derive(Default)]
pub(super) struct Client {
    header_buffer: Vec<u16>,
    read_buffer: Vec<u8>,
    session_cache: Option<SessionCache>,
}
struct Handle(NonNull<c_void>);
struct CachedConnect {
    handle: Handle,
    host: String,
}
struct SessionCache {
    connects: [Option<CachedConnect>; WINHTTP_CONNECT_CACHE_LIMIT],
    session: Handle,
}
impl Drop for Handle {
    fn drop(&mut self) {
        // SAFETY: self.0 is a WinHTTP handle returned by WinHTTP and is closed exactly once here.
        unsafe {
            sys::WinHttpCloseHandle(self.as_ptr());
        }
    }
}
impl Handle {
    const fn as_ptr(&self) -> HInternet {
        self.0.as_ptr()
    }
}
impl Client {
    fn cached_connect_ptr(
        &mut self,
        host: &str,
        host_wide: &[u16],
    ) -> DownloadResult<HInternet> {
        if self.session_cache.is_none() {
            let user_agent = wide(super::USER_AGENT)?;
            // SAFETY: user_agent is NUL-terminated and optional proxy pointers are intentionally null.
            let raw_session = unsafe {
                sys::WinHttpOpen(
                    user_agent.as_ptr(),
                    WINHTTP_ACCESS_TYPE_AUTOMATIC_PROXY,
                    null(),
                    null(),
                    0,
                )
            };
            let session = Self::non_null_handle(raw_session, "WinHttpOpen")?;
            // SAFETY: session is a valid WinHTTP session handle.
            let timeout_ok = unsafe {
                sys::WinHttpSetTimeouts(
                    session.as_ptr(),
                    WINHTTP_RESOLVE_TIMEOUT_MS,
                    WINHTTP_CONNECT_TIMEOUT_MS,
                    WINHTTP_SEND_TIMEOUT_MS,
                    WINHTTP_RECEIVE_TIMEOUT_MS,
                )
            };
            Self::check_winhttp(timeout_ok, "WinHttpSetTimeouts")?;
            if let Err(code) = Self::try_set_dword_option(
                &session,
                WINHTTP_OPTION_SECURE_PROTOCOLS,
                WINHTTP_SECURE_PROTOCOLS_MIN_TLS_1_2,
            ) {
                if matches!(code, ERROR_INVALID_PARAMETER | ERROR_WINHTTP_INVALID_OPTION) {
                    Self::set_dword_option(
                        &session,
                        WINHTTP_OPTION_SECURE_PROTOCOLS,
                        WINHTTP_FLAG_SECURE_PROTOCOL_TLS1_2,
                        "WinHttpSetOption SECURE_PROTOCOLS",
                    )?;
                } else {
                    return Err(Self::windows_error_message(
                        "WinHttpSetOption SECURE_PROTOCOLS",
                        code,
                    )
                    .into());
                }
            }
            for (option, context, ignored_error) in WINHTTP_SESSION_OPTIONS {
                if let Err(code) = Self::try_set_dword_option(&session, option, 1)
                    && ignored_error != Some(code)
                {
                    return Err(Self::windows_error_message(context, code).into());
                }
            }
            self.session_cache = Some(SessionCache {
                connects: from_fn(|_| None),
                session,
            });
        }
        let cache = self
            .session_cache
            .as_mut()
            .ok_or("WinHTTP session cache 상태 오류")?;
        if let Some(entry) = cache
            .connects
            .iter()
            .filter_map(Option::as_ref)
            .find(|entry| entry.host == host)
        {
            return Ok(entry.handle.as_ptr());
        }
        // SAFETY: host_wide is NUL-terminated and cache.session is a valid session handle.
        let raw_connect = unsafe {
            sys::WinHttpConnect(
                cache.session.as_ptr(),
                host_wide.as_ptr(),
                INTERNET_DEFAULT_HTTPS_PORT,
                0,
            )
        };
        let handle = NonNull::new(raw_connect)
            .map(Handle)
            .ok_or_else(|| Self::last_error_message("WinHttpConnect"))?;
        let mut host_key = String::new();
        host_key.try_reserve_exact(host.len()).map_err(|source| {
                download_error_with_source("WinHTTP connect host key 메모리 확보 실패", source)
        })?;
        host_key.push_str(host);
        let connect = handle.as_ptr();
        let entry = CachedConnect {
            handle,
            host: host_key,
        };
        if let Some(slot) = cache.connects.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(entry);
        } else {
            cache.connects.rotate_left(1);
            let [_, _, _, slot] = cache.connects.each_mut();
            *slot = Some(entry);
        }
        Ok(connect)
    }
    fn check_winhttp(ok: i32, context: &str) -> DownloadResult<()> {
        if ok == 0_i32 {
            Err(Self::last_error_message(context).into())
        } else {
            Ok(())
        }
    }
    fn last_error_code() -> u32 {
        // SAFETY: GetLastError has no preconditions.
        unsafe { sys::GetLastError() }
    }
    fn last_error_message(context: &str) -> String {
        let code = Self::last_error_code();
        Self::windows_error_message(context, code)
    }
    fn non_null_handle(handle: HInternet, context: &str) -> DownloadResult<Handle> {
        Ok(NonNull::new(handle)
            .map(Handle)
            .ok_or_else(|| Self::last_error_message(context))?)
    }
    fn open_request(
        connect: HInternet,
        method: HttpMethod<'_>,
        path: &[u16],
    ) -> DownloadResult<Handle> {
        let method_wide: &[u16] = if matches!(method, HttpMethod::Post(_)) {
            &METHOD_POST_WIDE
        } else {
            &METHOD_GET_WIDE
        };
        // SAFETY: method and path are NUL-terminated and connect is valid.
        let raw_request = unsafe {
            sys::WinHttpOpenRequest(
                connect,
                method_wide.as_ptr(),
                path.as_ptr(),
                null(),
                null(),
                null(),
                WINHTTP_FLAG_SECURE,
            )
        };
        let request = Self::non_null_handle(raw_request, "WinHttpOpenRequest")?;
        Self::set_dword_option(
            &request,
            WINHTTP_OPTION_ENABLE_FEATURE,
            WINHTTP_ENABLE_SSL_REVOCATION,
            "WinHttpSetOption ENABLE_FEATURE",
        )?;
        Self::set_dword_option(
            &request,
            WINHTTP_OPTION_DISABLE_FEATURE,
            WINHTTP_DISABLE_COOKIES | WINHTTP_DISABLE_REDIRECTS,
            "WinHttpSetOption DISABLE_FEATURE",
        )?;
        Self::set_dword_option(
            &request,
            WINHTTP_OPTION_MAX_RESPONSE_HEADER_SIZE,
            HTTP_MAX_HEADER_BYTES_DWORD,
            "WinHttpSetOption MAX_RESPONSE_HEADER_SIZE",
        )?;
        Ok(request)
    }
    fn query_headers(
        request: &Handle,
        buffer: &mut Vec<u16>,
    ) -> DownloadResult<Vec<HttpHeader>> {
        let mut bytes = 0_u32;
        let mut index = 0_u32;
        // SAFETY: request is valid; this first call probes the required buffer size.
        let probe_ok = unsafe {
            sys::WinHttpQueryHeaders(
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
        let last_error = Self::last_error_code();
        if last_error != ERROR_INSUFFICIENT_BUFFER {
            return Err(Self::windows_error_message("WinHttpQueryHeaders", last_error).into());
        }
        let header_bytes = usize::try_from(bytes)
            .map_err(|source| download_error_with_source("응답 헤더 길이 변환 실패", source))?;
        checked_http_buffer_len("헤더", 0, header_bytes, HTTP_MAX_HEADER_BYTES)?;
        if !header_bytes.is_multiple_of(2) {
            return Err("응답 헤더 UTF-16 버퍼 길이가 2바이트 단위가 아닙니다.".into());
        }
        let units = header_bytes.div_euclid(2);
        buffer.clear();
        if buffer.capacity() < units {
            buffer.try_reserve_exact(units).map_err(|source| {
                download_error_with_source("응답 헤더 버퍼 메모리 확보 실패", source)
            })?;
        }
        buffer.resize(units, 0_u16);
        index = 0;
        // SAFETY: buffer has the size requested by WinHTTP and request is valid.
        let fetch_ok = unsafe {
            sys::WinHttpQueryHeaders(
                request.as_ptr(),
                WINHTTP_QUERY_RAW_HEADERS_CRLF,
                null(),
                buffer.as_mut_ptr().cast::<c_void>(),
                &raw mut bytes,
                &raw mut index,
            )
        };
        Self::check_winhttp(fetch_ok, "WinHttpQueryHeaders")?;
        while buffer.pop_if(|value| *value == 0).is_some() {}
        let mut parsed = Vec::new();
        for raw_line in buffer.split(|unit| *unit == u16::from(b'\n')).skip(1) {
            let trimmed_line = trim_ascii_utf16(
                raw_line
                    .strip_suffix(&[u16::from(b'\r')])
                    .unwrap_or(raw_line),
            );
            let Some(colon) = trimmed_line
                .iter()
                .position(|unit| *unit == u16::from(b':'))
            else {
                continue;
            };
            let (raw_name, tail) = trimmed_line.split_at(colon);
            let Some((_, raw_value)) = tail.split_first() else {
                continue;
            };
            let name = trim_ascii_utf16(raw_name);
            let value = trim_ascii_utf16(raw_value);
            let kind = if header_name_eq_ignore_ascii_case(name, RESPONSE_HEADER_CONTENT_LENGTH) {
                HttpHeaderKind::ContentLength
            } else if header_name_eq_ignore_ascii_case(name, RESPONSE_HEADER_SET_COOKIE) {
                HttpHeaderKind::SetCookie
            } else {
                continue;
            };
            if parsed.len() == parsed.capacity() {
                parsed.try_reserve(1).map_err(|source| {
                    download_error_with_source("응답 header 목록 메모리 확보 실패", source)
                })?;
            }
            let header_value = String::from_utf16(value).map_err(|source| {
                download_error_with_source("응답 header 값 UTF-16 변환 실패", source)
            })?;
            parsed.push(HttpHeader {
                kind,
                value: header_value,
            });
        }
        Ok(parsed)
    }
    fn query_status(request: &Handle) -> DownloadResult<u32> {
        let mut status = 0_u32;
        let mut bytes = DWORD_BYTE_SIZE;
        // SAFETY: status and bytes are valid output buffers for the numeric status query.
        let ok = unsafe {
            sys::WinHttpQueryHeaders(
                request.as_ptr(),
                WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER,
                null(),
                (&raw mut status).cast::<c_void>(),
                &raw mut bytes,
                null_mut(),
            )
        };
        Self::check_winhttp(ok, "WinHttpQueryHeaders status")?;
        Ok(status)
    }
    fn read_body(
        &mut self,
        request: &Handle,
        expected_len: Option<usize>,
        started: Instant,
    ) -> DownloadResult<Vec<u8>> {
        let mut body = Vec::new();
        if let Some(capacity) = expected_len {
            body.try_reserve_exact(capacity).map_err(|source| {
                download_error_with_source("응답 본문 메모리 선확보 실패", source)
            })?;
        }
        let mut chunk_buffer = mem::take(&mut self.read_buffer);
        let result = (|| {
            ensure_read_buffer(&mut chunk_buffer)?;
            loop {
                ensure_within_total_timeout(started)?;
                let read = Self::read_chunk(request, &mut chunk_buffer)?;
                let read_len = usize::try_from(read).map_err(|source| {
                    download_error_with_source("응답 read 길이 변환 실패", source)
                })?;
                if read_len == 0 {
                    break;
                }
                let read_chunk = chunk_buffer
                    .get(..read_len)
                    .ok_or("응답 본문 chunk 범위 계산 실패")?;
                let next_len = checked_http_buffer_len(
                    "본문",
                    body.len(),
                    read_chunk.len(),
                    HTTP_MAX_BODY_BYTES,
                )?;
                if body.capacity() < next_len {
                    body.try_reserve(read_chunk.len()).map_err(|source| {
                        download_error_with_source("응답 본문 메모리 확보 실패", source)
                    })?;
                }
                body.extend_from_slice(read_chunk);
            }
            Ok(body)
        })();
        self.read_buffer = chunk_buffer;
        result
    }
    fn read_body_to_writer(
        &mut self,
        request: &Handle,
        writer: &mut dyn IoWrite,
        started: Instant,
    ) -> DownloadResult<StreamedBodySummary> {
        let mut sink = StreamingBodySink {
            limit: HTTP_MAX_BODY_BYTES,
            summary: StreamedBodySummary {
                bytes_seen: 0,
                preview: Vec::new(),
            },
            writer,
        };
        let mut chunk_buffer = mem::take(&mut self.read_buffer);
        let result = (|| {
            ensure_read_buffer(&mut chunk_buffer)?;
            loop {
                ensure_within_total_timeout(started)?;
                let read = Self::read_chunk(request, &mut chunk_buffer)?;
                let read_len = usize::try_from(read).map_err(|source| {
                    download_error_with_source("응답 read 길이 변환 실패", source)
                })?;
                if read_len == 0 {
                    break;
                }
                let read_chunk = chunk_buffer
                    .get(..read_len)
                    .ok_or("응답 본문 chunk 범위 계산 실패")?;
                sink.append(read_chunk)?;
            }
            Ok(sink.summary)
        })();
        self.read_buffer = chunk_buffer;
        result
    }
    fn read_chunk(
        request: &Handle,
        chunk: &mut [u8],
    ) -> DownloadResult<u32> {
        let mut read = 0_u32;
        let bytes_to_read = u32::try_from(chunk.len())
            .map_err(|source| download_error_with_source("응답 read 버퍼 길이 변환 실패", source))?;
        // SAFETY: chunk is a valid writable buffer and read is a valid output buffer.
        let read_ok = unsafe {
            sys::WinHttpReadData(
                request.as_ptr(),
                chunk.as_mut_ptr().cast::<c_void>(),
                bytes_to_read,
                &raw mut read,
            )
        };
        Self::check_winhttp(read_ok, "WinHttpReadData")?;
        Ok(read)
    }
    fn receive_response(request: &Handle) -> DownloadResult<()> {
        // SAFETY: request is a valid request handle and no reserved pointer is required.
        let received = unsafe { sys::WinHttpReceiveResponse(request.as_ptr(), null_mut()) };
        Self::check_winhttp(received, "WinHttpReceiveResponse")
    }
    pub(super) fn request(
        &mut self,
        method: HttpMethod<'_>,
        host: &str,
        path: &str,
        headers: &[(&str, &str)],
    ) -> DownloadResult<HttpResponse> {
        let host_wide = wide(host)?;
        let path_wide = wide(path)?;
        let mut headers_wide = mem::take(&mut self.header_buffer);
        let response = (|| {
            Self::request_headers_wide(&mut headers_wide, headers)?;
            let body_slice = match method {
                HttpMethod::Get => &[],
                HttpMethod::Post(body) => body,
            };
            let body_len = u32::try_from(body_slice.len()).map_err(|source| {
                download_error_with_source("요청 본문 길이 변환 실패", source)
            })?;
            let started = Instant::now();
            let connect = self.cached_connect_ptr(host, &host_wide)?;
            (|| {
                let request = Self::open_request(connect, method, &path_wide)?;
                Self::send_request(&request, &headers_wide, body_slice, body_len)?;
                Self::receive_response(&request)?;
                let status = Self::query_status(&request)?;
                let response_headers = Self::query_headers(&request, &mut headers_wide)?;
                let content_length =
                    validated_http_content_length(&response_headers, HTTP_MAX_BODY_BYTES)?;
                let response_body = self.read_body(&request, content_length, started)?;
                enforce_http_body_length(response_body.len(), content_length)?;
                Ok(HttpResponse {
                    body: response_body,
                    headers: response_headers,
                    status,
                })
            })()
            .inspect_err(|_| self.session_cache = None)
        })();
        self.header_buffer = headers_wide;
        response
    }
    fn request_headers_wide(out: &mut Vec<u16>, headers: &[(&str, &str)]) -> DownloadResult<()> {
        let header_capacity = headers
            .iter()
            .try_fold(0_usize, |acc, &(name, value)| {
                acc.checked_add(name.encode_utf16().count())?
                    .checked_add(value.encode_utf16().count())?
                    .checked_add(4)
            })
            .and_then(|capacity| capacity.checked_add(1))
            .ok_or("요청 헤더 용량 계산 실패")?;
        out.clear();
        if out.capacity() < header_capacity {
            out.try_reserve_exact(header_capacity).map_err(|source| {
                download_error_with_source("요청 헤더 메모리 확보 실패", source)
            })?;
        }
        for &(name, value) in headers {
            out.extend(name.encode_utf16());
            out.extend_from_slice(&HEADER_SEPARATOR_WIDE);
            out.extend(value.encode_utf16());
            out.extend_from_slice(&HEADER_TERMINATOR_WIDE);
        }
        out.push(0);
        Ok(())
    }
    pub(super) fn request_to_writer(
        &mut self,
        method: HttpMethod<'_>,
        host: &str,
        path: &str,
        headers: &[(&str, &str)],
        writer: &mut dyn IoWrite,
    ) -> DownloadResult<HttpStreamResponse> {
        let host_wide = wide(host)?;
        let path_wide = wide(path)?;
        let mut headers_wide = mem::take(&mut self.header_buffer);
        let response = (|| {
            Self::request_headers_wide(&mut headers_wide, headers)?;
            let body_slice = match method {
                HttpMethod::Get => &[],
                HttpMethod::Post(body) => body,
            };
            let body_len = u32::try_from(body_slice.len()).map_err(|source| {
                download_error_with_source("요청 본문 길이 변환 실패", source)
            })?;
            let started = Instant::now();
            let connect = self.cached_connect_ptr(host, &host_wide)?;
            (|| {
                let request = Self::open_request(connect, method, &path_wide)?;
                Self::send_request(&request, &headers_wide, body_slice, body_len)?;
                Self::receive_response(&request)?;
                let status = Self::query_status(&request)?;
                let response_headers = Self::query_headers(&request, &mut headers_wide)?;
                let content_length =
                    validated_http_content_length(&response_headers, HTTP_MAX_BODY_BYTES)?;
                let response_body = self.read_body_to_writer(&request, writer, started)?;
                enforce_http_body_length(response_body.bytes_seen, content_length)?;
                Ok(HttpStreamResponse {
                    body: response_body,
                    headers: response_headers,
                    status,
                })
            })()
            .inspect_err(|_| self.session_cache = None)
        })();
        self.header_buffer = headers_wide;
        response
    }
    fn send_request(
        request: &Handle,
        headers: &[u16],
        body: &[u8],
        body_len: u32,
    ) -> DownloadResult<()> {
        let header_units = headers
            .len()
            .checked_sub(1)
            .ok_or("요청 헤더 NUL terminator가 없습니다.")?;
        let header_len = u32::try_from(header_units)
            .map_err(|source| download_error_with_source("요청 헤더 길이 변환 실패", source))?;
        let body_ptr = if body.is_empty() {
            null()
        } else {
            body.as_ptr().cast::<c_void>()
        };
        // SAFETY: request is valid, headers are NUL-terminated, and body_ptr is null or points to body.
        let sent = unsafe {
            sys::WinHttpSendRequest(
                request.as_ptr(),
                headers.as_ptr(),
                header_len,
                body_ptr,
                body_len,
                body_len,
                0,
            )
        };
        Self::check_winhttp(sent, "WinHttpSendRequest")
    }
    fn set_dword_option(
        handle: &Handle,
        option: u32,
        value: u32,
        context: &str,
    ) -> DownloadResult<()> {
        Self::try_set_dword_option(handle, option, value)
            .map_err(|code| Self::windows_error_message(context, code).into())
    }
    fn try_set_dword_option(
        handle: &Handle,
        option: u32,
        value: u32,
    ) -> CoreResult<(), u32> {
        // SAFETY: handle is valid and value points to a DWORD option value for this call.
        let ok = unsafe {
            sys::WinHttpSetOption(
                handle.as_ptr(),
                option,
                (&raw const value).cast::<c_void>(),
                DWORD_BYTE_SIZE,
            )
        };
        if ok == 0_i32 {
            Err(Self::last_error_code())
        } else {
            Ok(())
        }
    }
    fn windows_error_message(context: &str, code: u32) -> String {
        format!("{context} 실패: Windows error {code}")
    }
}
fn ensure_read_buffer(buffer: &mut Vec<u8>) -> DownloadResult<()> {
    if buffer.capacity() < WINHTTP_READ_BUFFER_BYTES {
        buffer
            .try_reserve_exact(WINHTTP_READ_BUFFER_BYTES)
            .map_err(|source| {
                download_error_with_source("응답 read 버퍼 메모리 확보 실패", source)
            })?;
    }
    buffer.resize(WINHTTP_READ_BUFFER_BYTES, 0);
    Ok(())
}
fn ensure_within_total_timeout(started: Instant) -> DownloadResult<()> {
    if started.elapsed() >= WINHTTP_TOTAL_TIMEOUT {
        Err("HTTP 전체 전송 제한 시간(60초)을 초과했습니다.".into())
    } else {
        Ok(())
    }
}
const fn trim_ascii_utf16(mut value: &[u16]) -> &[u16] {
    while let Some((first, rest)) = value.split_first()
        && is_ascii_whitespace_utf16(*first)
    {
        value = rest;
    }
    while let Some((last, rest)) = value.split_last()
        && is_ascii_whitespace_utf16(*last)
    {
        value = rest;
    }
    value
}
const fn is_ascii_whitespace_utf16(value: u16) -> bool {
    matches!(value, 0x09 | 0x0A | 0x0C | 0x0D | 0x20)
}
fn header_name_eq_ignore_ascii_case(name: &[u16], expected: &[u8]) -> bool {
    name.len() == expected.len()
        && name.iter().zip(expected).all(|(&unit, &byte)| {
            u8::try_from(unit).is_ok_and(|unit_byte| unit_byte.eq_ignore_ascii_case(&byte))
        })
}
fn wide(value: &str) -> DownloadResult<Vec<u16>> {
    let capacity = value
        .len()
        .checked_add(1)
        .ok_or("wide 문자열 용량 계산 실패")?;
    let mut out = Vec::new();
    out.try_reserve_exact(capacity)
        .map_err(|source| download_error_with_source("wide 문자열 메모리 확보 실패", source))?;
    out.extend(<OsStr as WindowsOsStrExt>::encode_wide(OsStr::new(value)));
    out.push(0);
    Ok(out)
}
