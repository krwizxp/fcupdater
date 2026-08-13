use super::{
    DownloadResult, HTTP_MAX_BODY_BYTES, HTTP_MAX_HEADER_BYTES, HttpResponse, RequestHeaders,
    ResponseHeaders, checked_http_buffer_len, download_error_with_source,
};
use crate::diagnostic::{try_string_with_capacity, try_vec_with_capacity};
use alloc::{string::String, vec::Vec};
use core::{
    array::from_fn,
    ffi::c_void,
    ptr::{NonNull, null, null_mut},
    result::Result as CoreResult,
    time::Duration,
};
use std::{
    ffi::OsStr,
    os::windows::ffi::OsStrExt as WindowsOsStrExt,
    time::Instant,
};
mod sys;
const DWORD_BYTE_SIZE: u32 = 4;
const ERROR_INSUFFICIENT_BUFFER: u32 = 122;
const ERROR_WINHTTP_HEADER_NOT_FOUND: u32 = 12_150;
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
const WINHTTP_QUERY_CONTENT_LENGTH: u32 = 5;
const WINHTTP_QUERY_SET_COOKIE: u32 = 43;
const WINHTTP_QUERY_STATUS_CODE: u32 = 19;
const WINHTTP_CONNECT_TIMEOUT_MS: i32 = 30_000;
const WINHTTP_CONNECT_CACHE_LIMIT: usize = 2;
const WINHTTP_RECEIVE_TIMEOUT_MS: i32 = 60_000;
const WINHTTP_RESOLVE_TIMEOUT_MS: i32 = 30_000;
const WINHTTP_SEND_TIMEOUT_MS: i32 = 60_000;
const WINHTTP_TOTAL_TIMEOUT: Duration = Duration::from_mins(1);
const WINHTTP_READ_BUFFER_BYTES: usize = 64 * 1024;
const HEADER_SEPARATOR_WIDE: [u16; 2] = [0x3A, 0x20];
const HEADER_TERMINATOR_WIDE: [u16; 2] = [0x0D, 0x0A];
const METHOD_GET_WIDE: [u16; 4] = [0x47, 0x45, 0x54, 0];
const METHOD_POST_WIDE: [u16; 5] = [0x50, 0x4F, 0x53, 0x54, 0];
enum WinHttpHandle {}
type HInternet = *mut WinHttpHandle;
#[derive(Default)]
pub(super) struct Client {
    header_buffer: Vec<u16>,
    read_buffer: Vec<u8>,
    session_cache: Option<SessionCache>,
}
struct Handle(NonNull<WinHttpHandle>);
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
    fn begin_request(
        &mut self,
        host: &str,
        path: &str,
        headers: &RequestHeaders<'_>,
        method: &[u16],
        body: &[u8],
    ) -> DownloadResult<(Handle, u32, Instant)> {
        let path_wide = wide(path)?;
        let header_capacity = headers
            .iter()
            .try_fold(0_usize, |acc, (name, value)| {
                acc.checked_add(name.len())?
                    .checked_add(value.len())?
                    .checked_add(4)
            })
            .and_then(|capacity| capacity.checked_add(1))
            .ok_or("요청 헤더 용량 계산 실패")?;
        self.header_buffer.clear();
        if self.header_buffer.capacity() < header_capacity {
            self.header_buffer
                .try_reserve_exact(header_capacity)
                .map_err(|source| {
                    download_error_with_source("요청 헤더 메모리 확보 실패", source)
                })?;
        }
        for (name, value) in headers.iter() {
            self.header_buffer.extend(name.encode_utf16());
            self.header_buffer.extend_from_slice(&HEADER_SEPARATOR_WIDE);
            self.header_buffer.extend(value.encode_utf16());
            self.header_buffer
                .extend_from_slice(&HEADER_TERMINATOR_WIDE);
        }
        let header_len = u32::try_from(self.header_buffer.len()).map_err(|source| {
            download_error_with_source("요청 헤더 길이 변환 실패", source)
        })?;
        self.header_buffer.push(0);
        let started = Instant::now();
        let connect = self.cached_connect(host)?;
        (|| {
            // SAFETY: method and path are NUL-terminated and connect is valid.
            let raw_request = unsafe {
                sys::WinHttpOpenRequest(
                    connect.as_ptr(),
                    method.as_ptr(),
                    path_wide.as_ptr(),
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
            let body_len = u32::try_from(body.len()).map_err(|source| {
                download_error_with_source("요청 본문 길이 변환 실패", source)
            })?;
            let body_ptr = if body.is_empty() {
                null()
            } else {
                body.as_ptr().cast::<c_void>()
            };
            // SAFETY: request is valid, self.header_buffer is NUL-terminated, and body_ptr is null or points to body.
            let sent = unsafe {
                sys::WinHttpSendRequest(
                    request.as_ptr(),
                    self.header_buffer.as_ptr(),
                    header_len,
                    body_ptr,
                    body_len,
                    body_len,
                    0,
                )
            };
            Self::check_winhttp(sent, "WinHttpSendRequest")?;
            // SAFETY: request is a valid request handle and no reserved pointer is required.
            let received = unsafe { sys::WinHttpReceiveResponse(request.as_ptr(), null_mut()) };
            Self::check_winhttp(received, "WinHttpReceiveResponse")?;
            let mut status = 0_u32;
            let mut bytes = DWORD_BYTE_SIZE;
            // SAFETY: status and bytes are valid output buffers for the numeric status query.
            let queried = unsafe {
                sys::WinHttpQueryHeaders(
                    request.as_ptr(),
                    WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER,
                    null(),
                    (&raw mut status).cast::<c_void>(),
                    &raw mut bytes,
                    null_mut(),
                )
            };
            Self::check_winhttp(queried, "WinHttpQueryHeaders status")?;
            Ok((request, status, started))
        })()
        .inspect_err(|_| self.session_cache = None)
    }
    fn cached_connect(&mut self, host: &str) -> DownloadResult<NonNull<WinHttpHandle>> {
        let cache = if let Some(ref mut cache) = self.session_cache {
            cache
        } else {
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
            self.session_cache.insert(SessionCache {
                connects: from_fn(|_| None),
                session,
            })
        };
        if let Some(entry) = cache
            .connects
            .iter()
            .filter_map(Option::as_ref)
            .find(|entry| entry.host == host)
        {
            return Ok(entry.handle.0);
        }
        let host_wide = wide(host)?;
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
        let mut host_key = try_string_with_capacity(
            host.len(),
            "WinHTTP connect host key 메모리 확보 실패",
        )?;
        host_key.push_str(host);
        let connect = handle.0;
        let entry = CachedConnect {
            handle,
            host: host_key,
        };
        if let Some(slot) = cache.connects.iter_mut().find(|slot| slot.is_none()) {
            *slot = Some(entry);
        } else {
            cache.connects.rotate_left(1);
            let [_, slot] = cache.connects.each_mut();
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
    fn complete_request(
        &mut self,
        request: &Handle,
        status: u32,
        started: Instant,
    ) -> DownloadResult<HttpResponse> {
        (|| {
            let mut headers = ResponseHeaders::default();
            let mut content_length_index = 0_u32;
            loop {
                let previous_index = content_length_index;
                let mut value = 0_u32;
                let mut bytes = DWORD_BYTE_SIZE;
                // SAFETY: request is valid and value is a DWORD output buffer.
                let queried = unsafe {
                    sys::WinHttpQueryHeaders(
                        request.as_ptr(),
                        WINHTTP_QUERY_CONTENT_LENGTH | WINHTTP_QUERY_FLAG_NUMBER,
                        null(),
                        (&raw mut value).cast::<c_void>(),
                        &raw mut bytes,
                        &raw mut content_length_index,
                    )
                };
                if queried == 0_i32 {
                    let code = Self::last_error_code();
                    if code == ERROR_WINHTTP_HEADER_NOT_FOUND {
                        break;
                    }
                    return Err(Self::windows_error_message(
                        "WinHttpQueryHeaders Content-Length",
                        code,
                    )
                    .into());
                }
                if content_length_index <= previous_index {
                    return Err(
                        "WinHTTP Content-Length header index가 진행되지 않았습니다.".into(),
                    );
                }
                headers.set_content_length(usize::try_from(value).map_err(|source| {
                    download_error_with_source("Content-Length 변환 실패", source)
                })?)?;
            }
            let mut cookie_index = 0_u32;
            let mut header_bytes_seen = 0_usize;
            loop {
                let current_index = cookie_index;
                let mut bytes = 0_u32;
                // SAFETY: request is valid; this call probes the indexed header value size.
                let probe = unsafe {
                    sys::WinHttpQueryHeaders(
                        request.as_ptr(),
                        WINHTTP_QUERY_SET_COOKIE,
                        null(),
                        null_mut(),
                        &raw mut bytes,
                        &raw mut cookie_index,
                    )
                };
                if probe != 0_i32 {
                    return Err(
                        "WinHTTP Set-Cookie 크기 조회가 예기치 않게 성공했습니다.".into(),
                    );
                }
                let code = Self::last_error_code();
                if code == ERROR_WINHTTP_HEADER_NOT_FOUND {
                    break;
                }
                if code != ERROR_INSUFFICIENT_BUFFER {
                    return Err(Self::windows_error_message(
                        "WinHttpQueryHeaders Set-Cookie 크기",
                        code,
                    )
                    .into());
                }
                let header_bytes = usize::try_from(bytes).map_err(|source| {
                    download_error_with_source("Set-Cookie 길이 변환 실패", source)
                })?;
                header_bytes_seen = checked_http_buffer_len(
                    "헤더",
                    header_bytes_seen,
                    header_bytes,
                    HTTP_MAX_HEADER_BYTES,
                )?;
                if !header_bytes.is_multiple_of(2) {
                    return Err("Set-Cookie UTF-16 길이가 2바이트 단위가 아닙니다.".into());
                }
                let units = header_bytes.div_euclid(2);
                self.header_buffer.clear();
                if self.header_buffer.capacity() < units {
                    self.header_buffer.try_reserve_exact(units).map_err(|source| {
                        download_error_with_source("Set-Cookie 메모리 확보 실패", source)
                    })?;
                }
                self.header_buffer.resize(units, 0_u16);
                cookie_index = current_index;
                // SAFETY: buffer has the probed size and request is valid.
                let fetched = unsafe {
                    sys::WinHttpQueryHeaders(
                        request.as_ptr(),
                        WINHTTP_QUERY_SET_COOKIE,
                        null(),
                        self.header_buffer.as_mut_ptr().cast::<c_void>(),
                        &raw mut bytes,
                        &raw mut cookie_index,
                    )
                };
                Self::check_winhttp(fetched, "WinHttpQueryHeaders Set-Cookie")?;
                if cookie_index <= current_index {
                    return Err("WinHTTP Set-Cookie header index가 진행되지 않았습니다.".into());
                }
                while self.header_buffer.pop_if(|unit| *unit == 0).is_some() {}
                let value = String::from_utf16(&self.header_buffer).map_err(|source| {
                    download_error_with_source("Set-Cookie UTF-16 변환 실패", source)
                })?;
                headers.push_set_cookie(value.trim_ascii())?;
            }
            let body = self.read_body(request, headers.content_length, started)?;
            if let Some(expected_len) = headers.content_length
                && body.len() != expected_len
            {
                return Err(format!(
                    "HTTP 응답 본문 길이가 Content-Length와 다릅니다: expected={expected_len}, actual={}",
                    body.len()
                )
                .into());
            }
            Ok(HttpResponse {
                body,
                headers,
                status,
            })
        })()
        .inspect_err(|_| self.session_cache = None)
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
    fn read_body(
        &mut self,
        request: &Handle,
        expected_len: Option<usize>,
        started: Instant,
    ) -> DownloadResult<Vec<u8>> {
        let mut body = match expected_len {
            Some(capacity) => try_vec_with_capacity(capacity, "응답 본문 메모리 선확보 실패")?,
            None => Vec::new(),
        };
        if self.read_buffer.capacity() < WINHTTP_READ_BUFFER_BYTES {
            self.read_buffer
                    .try_reserve_exact(WINHTTP_READ_BUFFER_BYTES)
                    .map_err(|source| {
                        download_error_with_source("응답 read 버퍼 메모리 확보 실패", source)
                    })?;
        }
        self.read_buffer.resize(WINHTTP_READ_BUFFER_BYTES, 0);
        let bytes_to_read = u32::try_from(self.read_buffer.len()).map_err(|source| {
                download_error_with_source("응답 read 버퍼 길이 변환 실패", source)
            })?;
        loop {
            if started.elapsed() >= WINHTTP_TOTAL_TIMEOUT {
                return Err("HTTP 전체 전송 제한 시간(60초)을 초과했습니다.".into());
            }
            let mut read = 0_u32;
            // SAFETY: request is valid, self.read_buffer is writable, and read is an output buffer.
            let read_ok = unsafe {
                sys::WinHttpReadData(
                    request.as_ptr(),
                    self.read_buffer.as_mut_ptr().cast::<c_void>(),
                    bytes_to_read,
                    &raw mut read,
                )
            };
            Self::check_winhttp(read_ok, "WinHttpReadData")?;
            let read_len = usize::try_from(read).map_err(|source| {
                download_error_with_source("응답 read 길이 변환 실패", source)
            })?;
            if read_len == 0 {
                break;
            }
            let read_chunk = self
                .read_buffer
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
    }
    pub(super) fn request(
        &mut self,
        request_body: Option<&[u8]>,
        host: &str,
        path: &str,
        headers: RequestHeaders<'_>,
    ) -> DownloadResult<HttpResponse> {
        let (body, method): (&[u8], &[u16]) = request_body.map_or_else(
            || (&[][..], METHOD_GET_WIDE.as_slice()),
            |body| (body, METHOD_POST_WIDE.as_slice()),
        );
        let (request, status, started) = self.begin_request(host, path, &headers, method, body)?;
        self.complete_request(&request, status, started)
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
fn wide(value: &str) -> DownloadResult<Vec<u16>> {
    let capacity = value
        .len()
        .checked_add(1)
        .ok_or("wide 문자열 용량 계산 실패")?;
    let mut out = try_vec_with_capacity(capacity, "wide 문자열 메모리 확보 실패")?;
    out.extend(<OsStr as WindowsOsStrExt>::encode_wide(OsStr::new(value)));
    out.push(0);
    Ok(out)
}
