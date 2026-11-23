use super::{
    DownloadResult, HTTP_MAX_BODY_BYTES, HTTP_MAX_HEADER_BYTES, HttpResponse, RequestHeaders,
    ResponseHeaders, checked_http_buffer_len, download_error_with_source,
};
use crate::diagnostic::{
    AppError as DownloadError, ensure, fmt_err, map_reserve, try_vec_with_capacity,
};
use alloc::{string::String, vec::Vec};
use core::{
    array::from_fn,
    ffi::c_void,
    ptr::{NonNull, null, null_mut},
    result::Result as CoreResult,
    time::Duration,
};
use std::time::Instant;
mod sys;
const ERROR_WINHTTP_HEADER_NOT_FOUND: u32 = 12_150;
const ERROR_WINHTTP_INVALID_OPTION: u32 = 12_009;
const WINHTTP_FLAG_SECURE_PROTOCOL_TLS1_2: u32 = 0x0000_0800;
const WINHTTP_OPTION_SECURE_PROTOCOLS: u32 = 84;
const WINHTTP_READ_BUFFER_BYTES: usize = 64 * 1024;
const WINHTTP_SESSION_OPTIONS: [(u32, &str, Option<u32>); 3] = [
    (
        144,
        "WinHttpSetOption DISABLE_SECURE_PROTOCOL_FALLBACK",
        None,
    ),
    (
        195,
        "WinHttpSetOption DISABLE_GLOBAL_POOLING",
        Some(ERROR_WINHTTP_INVALID_OPTION),
    ),
    (140, "WinHttpSetOption IPV6_FAST_FALLBACK", None),
];
enum WinHttpHandle {}
type HInternet = *mut WinHttpHandle;
#[derive(Default)]
pub(super) struct Client {
    header_buffer: Vec<u16>,
    read_buffer: Vec<u8>,
    session_cache: Option<SessionCache>,
}
struct Handle(NonNull<WinHttpHandle>);
struct SessionCache {
    connects: [Option<(Handle, &'static str)>; 2],
    session: Handle,
}
impl Drop for Handle {
    fn drop(&mut self) {
        // SAFETY: self.0 is a WinHTTP handle returned by WinHTTP and is closed exactly once here.
        unsafe {
            sys::WinHttpCloseHandle(self.0.as_ptr());
        }
    }
}
impl Client {
    fn cached_connect(&mut self, host: &'static str) -> DownloadResult<NonNull<WinHttpHandle>> {
        let cache = if let Some(ref mut cache) = self.session_cache {
            cache
        } else {
            let user_agent = wide(super::USER_AGENT)?;
            // SAFETY: user_agent is NUL-terminated and optional proxy pointers are intentionally null.
            let raw_session =
                unsafe { sys::WinHttpOpen(user_agent.as_ptr(), 4, null(), null(), 0) };
            let session = Self::non_null_handle(raw_session, "WinHttpOpen")?;
            // SAFETY: session is a valid WinHTTP session handle.
            let timeout_ok = unsafe {
                sys::WinHttpSetTimeouts(session.0.as_ptr(), 30_000, 30_000, 60_000, 60_000)
            };
            Self::check_winhttp(timeout_ok, "WinHttpSetTimeouts")?;
            if let Err(code) = Self::try_set_dword_option(
                &session,
                WINHTTP_OPTION_SECURE_PROTOCOLS,
                WINHTTP_FLAG_SECURE_PROTOCOL_TLS1_2 | 0x0000_2000,
            ) {
                if matches!(code, 87 | ERROR_WINHTTP_INVALID_OPTION) {
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
                    ));
                }
            }
            for (option, context, ignored_error) in WINHTTP_SESSION_OPTIONS {
                if let Err(code) = Self::try_set_dword_option(&session, option, 1)
                    && ignored_error != Some(code)
                {
                    return Err(Self::windows_error_message(context, code));
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
            .flatten()
            .find(|entry| entry.1 == host)
        {
            return Ok(entry.0.0);
        }
        let host_wide = wide(host)?;
        // SAFETY: host_wide is NUL-terminated and cache.session is a valid session handle.
        let raw_connect =
            unsafe { sys::WinHttpConnect(cache.session.0.as_ptr(), host_wide.as_ptr(), 443, 0) };
        let handle = Self::non_null_handle(raw_connect, "WinHttpConnect")?;
        let connect = handle.0;
        let [first, second] = cache.connects.each_mut();
        if first.is_none() {
            *first = Some((handle, host));
        } else {
            if second.is_some() {
                *first = second.take();
            }
            *second = Some((handle, host));
        }
        Ok(connect)
    }
    fn check_winhttp(ok: i32, context: &str) -> DownloadResult<()> {
        (ok != 0_i32).ok_or_else(|| Self::last_error_message(context))
    }
    fn last_error_code() -> u32 {
        // SAFETY: GetLastError has no preconditions.
        unsafe { sys::GetLastError() }
    }
    #[cold]
    #[inline(never)]
    fn last_error_message(context: &str) -> DownloadError {
        Self::windows_error_message(context, Self::last_error_code())
    }
    fn non_null_handle(handle: HInternet, context: &str) -> DownloadResult<Handle> {
        NonNull::new(handle)
            .map(Handle)
            .ok_or_else(|| Self::last_error_message(context))
    }
    fn query_dword_header(
        request: &Handle,
        info_level: u32,
        index: *mut u32,
    ) -> CoreResult<u32, u32> {
        let (mut value, mut bytes) = (0_u32, 4_u32);
        // SAFETY: request is valid and value/bytes are valid DWORD output buffers.
        let queried = unsafe {
            sys::WinHttpQueryHeaders(
                request.0.as_ptr(),
                info_level | 0x2000_0000,
                null(),
                (&raw mut value).cast::<c_void>(),
                &raw mut bytes,
                index,
            )
        };
        if queried == 0_i32 {
            Err(Self::last_error_code())
        } else {
            Ok(value)
        }
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
            map_reserve(
                self.read_buffer
                    .try_reserve_exact(WINHTTP_READ_BUFFER_BYTES),
                "응답 read 버퍼 메모리 확보 실패",
            )?;
        }
        self.read_buffer.resize(WINHTTP_READ_BUFFER_BYTES, 0);
        loop {
            ensure(
                started.elapsed() < Duration::from_mins(1),
                "HTTP 전체 전송 제한 시간(60초)을 초과했습니다.",
            )?;
            let mut read = 0_u32;
            // SAFETY: request is valid, self.read_buffer is writable, and read is an output buffer.
            let read_ok = unsafe {
                sys::WinHttpReadData(
                    request.0.as_ptr(),
                    self.read_buffer.as_mut_ptr().cast::<c_void>(),
                    64_u32.strict_mul(1024),
                    &raw mut read,
                )
            };
            Self::check_winhttp(read_ok, "WinHttpReadData")?;
            let read_len = read as usize;
            if read_len == 0 {
                break;
            }
            let read_chunk = self
                .read_buffer
                .get(..read_len)
                .ok_or("응답 본문 chunk 범위 계산 실패")?;
            checked_http_buffer_len("본문", body.len(), read_chunk.len(), HTTP_MAX_BODY_BYTES)?;
            map_reserve(
                body.try_reserve(read_chunk.len()),
                "응답 본문 메모리 확보 실패",
            )?;
            body.extend_from_slice(read_chunk);
        }
        Ok(body)
    }
    pub(super) fn request(
        &mut self,
        request_body: Option<&[u8]>,
        host: &'static str,
        path: &str,
        request_headers: RequestHeaders<'_>,
    ) -> DownloadResult<HttpResponse> {
        let (body, method): (&[u8], &[u16]) = request_body.map_or_else(
            || (&[][..], [0x47_u16, 0x45, 0x54, 0].as_slice()),
            |payload| (payload, [0x50_u16, 0x4F, 0x53, 0x54, 0].as_slice()),
        );
        let path_wide = wide(path)?;
        let header_capacity = request_headers.iter().fold(1_usize, |cap, (name, value)| {
            cap.strict_add(name.len())
                .strict_add(value.len())
                .strict_add(4)
        });
        self.header_buffer.clear();
        map_reserve(
            self.header_buffer.try_reserve_exact(header_capacity),
            "요청 헤더 메모리 확보 실패",
        )?;
        for (name, value) in request_headers.iter() {
            self.header_buffer.extend(name.encode_utf16());
            self.header_buffer.extend_from_slice(&[0x3A, 0x20]);
            self.header_buffer.extend(value.encode_utf16());
            self.header_buffer.extend_from_slice(&[0x0D, 0x0A]);
        }
        let header_len = u32::try_from(self.header_buffer.len())
            .map_err(|source| download_error_with_source("요청 헤더 길이 변환 실패", source))?;
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
                    0x0080_0000,
                )
            };
            let request = Self::non_null_handle(raw_request, "WinHttpOpenRequest")?;
            for (option, value, context) in [
                (79, 0x0000_0001, "WinHttpSetOption ENABLE_FEATURE"),
                (
                    63,
                    0x0000_0001 | 0x0000_0002,
                    "WinHttpSetOption DISABLE_FEATURE",
                ),
                (91, 256 * 1024, "WinHttpSetOption MAX_RESPONSE_HEADER_SIZE"),
            ] {
                Self::set_dword_option(&request, option, value, context)?;
            }
            let body_len = u32::try_from(body.len())
                .map_err(|source| download_error_with_source("요청 본문 길이 변환 실패", source))?;
            let body_ptr = if body.is_empty() {
                null()
            } else {
                body.as_ptr().cast::<c_void>()
            };
            // SAFETY: request is valid, self.header_buffer is NUL-terminated, and body_ptr is null or points to body.
            let sent = unsafe {
                sys::WinHttpSendRequest(
                    request.0.as_ptr(),
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
            let received = unsafe { sys::WinHttpReceiveResponse(request.0.as_ptr(), null_mut()) };
            Self::check_winhttp(received, "WinHttpReceiveResponse")?;
            let status = Self::query_dword_header(&request, 19, null_mut())
                .map_err(|code| Self::windows_error_message("WinHttpQueryHeaders status", code))?;
            let mut headers = ResponseHeaders::default();
            let mut content_length_index = 0_u32;
            loop {
                let previous_index = content_length_index;
                let value =
                    match Self::query_dword_header(&request, 5, &raw mut content_length_index) {
                        Ok(value) => value,
                        Err(ERROR_WINHTTP_HEADER_NOT_FOUND) => break,
                        Err(code) => {
                            return Err(Self::windows_error_message(
                                "WinHttpQueryHeaders Content-Length",
                                code,
                            ));
                        }
                    };
                ensure(
                    content_length_index > previous_index,
                    "WinHTTP Content-Length header index가 진행되지 않았습니다.",
                )?;
                headers.set_content_length(value as usize)?;
            }
            let (mut cookie_index, mut header_bytes_seen) = (0_u32, 0_usize);
            loop {
                let current_index = cookie_index;
                let mut bytes = 0_u32;
                // SAFETY: request is valid; this call probes the indexed header value size.
                let probe = unsafe {
                    sys::WinHttpQueryHeaders(
                        request.0.as_ptr(),
                        43,
                        null(),
                        null_mut(),
                        &raw mut bytes,
                        &raw mut cookie_index,
                    )
                };
                ensure(
                    probe == 0_i32,
                    "WinHTTP Set-Cookie 크기 조회가 예기치 않게 성공했습니다.",
                )?;
                let code = Self::last_error_code();
                if code == ERROR_WINHTTP_HEADER_NOT_FOUND {
                    break;
                }
                (code == 122).ok_or_else(|| {
                    Self::windows_error_message("WinHttpQueryHeaders Set-Cookie 크기", code)
                })?;
                let header_bytes = bytes as usize;
                header_bytes_seen = checked_http_buffer_len(
                    "헤더",
                    header_bytes_seen,
                    header_bytes,
                    HTTP_MAX_HEADER_BYTES,
                )?;
                ensure(
                    header_bytes.is_multiple_of(2),
                    "Set-Cookie UTF-16 길이가 2바이트 단위가 아닙니다.",
                )?;
                let units = header_bytes.div_euclid(2);
                self.header_buffer.clear();
                map_reserve(
                    self.header_buffer.try_reserve(units),
                    "Set-Cookie 메모리 확보 실패",
                )?;
                self.header_buffer.resize(units, 0_u16);
                cookie_index = current_index;
                // SAFETY: buffer has the probed size and request is valid.
                let fetched = unsafe {
                    sys::WinHttpQueryHeaders(
                        request.0.as_ptr(),
                        43,
                        null(),
                        self.header_buffer.as_mut_ptr().cast::<c_void>(),
                        &raw mut bytes,
                        &raw mut cookie_index,
                    )
                };
                Self::check_winhttp(fetched, "WinHttpQueryHeaders Set-Cookie")?;
                ensure(
                    cookie_index > current_index,
                    "WinHTTP Set-Cookie header index가 진행되지 않았습니다.",
                )?;
                while self.header_buffer.pop_if(|unit| *unit == 0).is_some() {}
                let value = String::from_utf16(&self.header_buffer).map_err(|source| {
                    download_error_with_source("Set-Cookie UTF-16 변환 실패", source)
                })?;
                headers.push_set_cookie(value.trim_ascii())?;
            }
            let response_body = self.read_body(&request, headers.content_length, started)?;
            Ok(HttpResponse {
                body: response_body,
                headers,
                status,
            })
        })()
        .inspect_err(|_| self.session_cache = None)
    }
    fn set_dword_option(
        handle: &Handle,
        option: u32,
        value: u32,
        context: &str,
    ) -> DownloadResult<()> {
        Self::try_set_dword_option(handle, option, value)
            .map_err(|code| Self::windows_error_message(context, code))
    }
    fn try_set_dword_option(handle: &Handle, option: u32, value: u32) -> CoreResult<(), u32> {
        // SAFETY: handle is valid and value points to a DWORD option value for this call.
        let ok = unsafe {
            sys::WinHttpSetOption(
                handle.0.as_ptr(),
                option,
                (&raw const value).cast::<c_void>(),
                4,
            )
        };
        (ok != 0_i32).ok_or_else(Self::last_error_code)
    }
    #[cold]
    #[inline(never)]
    fn windows_error_message(context: &str, code: u32) -> DownloadError {
        fmt_err!("{context} 실패: Windows error {code}")
    }
}
fn wide(value: &str) -> DownloadResult<Vec<u16>> {
    let mut out = try_vec_with_capacity(value.len().strict_add(1), "wide 문자열 메모리 확보 실패")?;
    out.extend(value.encode_utf16());
    out.push(0);
    Ok(out)
}
