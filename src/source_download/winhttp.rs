use super::{
    DownloadResult, HTTP_MAX_BODY_BYTES, HTTP_MAX_HEADER_BYTES, HttpResponse, RequestHeaders,
    ResponseHeaders, checked_http_buffer_len, download_error_with_source,
};
use crate::diagnostic::{AppError as DownloadError, ensure, fmt_err, reserve, vec_cap};
use alloc::{string::String, vec::Vec};
use core::{
    array::from_fn,
    ffi::c_void,
    ptr::{NonNull, null, null_mut},
    result::Result as CoreResult,
    time::Duration,
};
use std::time::Instant;
#[link(name = "winhttp")]
unsafe extern "system" {
    fn WinHttpCloseHandle(hi: HInt) -> i32;
    fn WinHttpConnect(hs: HInt, srv: *const u16, port: u16, res: u32) -> HInt;
    fn WinHttpOpen(ua: *const u16, acc: u32, prx: *const u16, byp: *const u16, flg: u32) -> HInt;
    fn WinHttpOpenRequest(
        hc: HInt,
        v: *const u16,
        obj: *const u16,
        ver: *const u16,
        rf: *const u16,
        ac: *const *const u16,
        flg: u32,
    ) -> HInt;
    fn WinHttpQueryHeaders(
        hr: HInt,
        lvl: u32,
        nm: *const u16,
        buf: *mut c_void,
        len: *mut u32,
        idx: *mut u32,
    ) -> i32;
    fn WinHttpReadData(hr: HInt, buf: *mut c_void, to_read: u32, read: *mut u32) -> i32;
    fn WinHttpReceiveResponse(hr: HInt, res: *mut c_void) -> i32;
    fn WinHttpSendRequest(
        hr: HInt,
        hdr: *const u16,
        hl: u32,
        opt: *const c_void,
        ol: u32,
        tl: u32,
        ctx: usize,
    ) -> i32;
    fn WinHttpSetOption(hi: HInt, opt: u32, buf: *const c_void, len: u32) -> i32;
    fn WinHttpSetTimeouts(hi: HInt, res: i32, conn: i32, send: i32, recv: i32) -> i32;
}
#[link(name = "kernel32")]
unsafe extern "system" {
    fn GetLastError() -> u32;
}
const ERR_HDR_NOT_FOUND: u32 = 12_150;
const ERR_INVALID_OPT: u32 = 12_009;
const FLAG_TLS1_2: u32 = 0x0000_0800;
const OPT_SECURE_PROTOS: u32 = 84;
const READ_BUF_BYTES: usize = 64 * 1024;
const SESSION_OPTS: [(u32, &str, Option<u32>); 3] = [
    (
        144,
        "WinHttpSetOption DISABLE_SECURE_PROTOCOL_FALLBACK",
        None,
    ),
    (
        195,
        "WinHttpSetOption DISABLE_GLOBAL_POOLING",
        Some(ERR_INVALID_OPT),
    ),
    (140, "WinHttpSetOption IPV6_FAST_FALLBACK", None),
];
enum WinHttpHandle {}
type HInt = *mut WinHttpHandle;
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
            WinHttpCloseHandle(self.0.as_ptr());
        }
    }
}
impl Client {
    fn cached_connect(&mut self, host: &'static str) -> DownloadResult<NonNull<WinHttpHandle>> {
        let cache = if let Some(ref mut cache) = self.session_cache {
            cache
        } else {
            let ua = wide(super::USER_AGENT)?;
            // SAFETY: ua is NUL-terminated and optional proxy pointers are intentionally null.
            let session = Self::non_null_handle(
                unsafe { WinHttpOpen(ua.as_ptr(), 4, null(), null(), 0) },
                "WinHttpOpen",
            )?;
            // SAFETY: session is a valid WinHTTP session handle.
            let t_ok =
                unsafe { WinHttpSetTimeouts(session.0.as_ptr(), 30_000, 30_000, 60_000, 60_000) };
            Self::check_winhttp(t_ok, "WinHttpSetTimeouts")?;
            if let Err(code) =
                Self::try_set_dword_option(&session, OPT_SECURE_PROTOS, FLAG_TLS1_2 | 0x0000_2000)
            {
                if matches!(code, 87 | ERR_INVALID_OPT) {
                    Self::set_dword_option(
                        &session,
                        OPT_SECURE_PROTOS,
                        FLAG_TLS1_2,
                        "WinHttpSetOption SECURE_PROTOCOLS",
                    )?;
                } else {
                    return Err(Self::win_err("WinHttpSetOption SECURE_PROTOCOLS", code));
                }
            }
            for (opt, ctx, ignored) in SESSION_OPTS {
                if let Err(code) = Self::try_set_dword_option(&session, opt, 1)
                    && ignored != Some(code)
                {
                    return Err(Self::win_err(ctx, code));
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
        let host_w = wide(host)?;
        // SAFETY: host_w is NUL-terminated and cache.session is a valid session handle.
        let handle = Self::non_null_handle(
            unsafe { WinHttpConnect(cache.session.0.as_ptr(), host_w.as_ptr(), 443, 0) },
            "WinHttpConnect",
        )?;
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
    fn check_winhttp(ok: i32, ctx: &str) -> DownloadResult<()> {
        (ok != 0_i32).ok_or_else(|| Self::last_err_msg(ctx))
    }
    fn last_err_code() -> u32 {
        // SAFETY: GetLastError has no preconditions.
        unsafe { GetLastError() }
    }
    #[cold]
    #[inline(never)]
    fn last_err_msg(ctx: &str) -> DownloadError {
        Self::win_err(ctx, Self::last_err_code())
    }
    fn non_null_handle(handle: HInt, ctx: &str) -> DownloadResult<Handle> {
        NonNull::new(handle)
            .map(Handle)
            .ok_or_else(|| Self::last_err_msg(ctx))
    }
    fn query_dword_header(request: &Handle, level: u32, index: *mut u32) -> CoreResult<u32, u32> {
        let (mut val, mut bytes) = (0_u32, 4_u32);
        // SAFETY: request is valid and val/bytes are valid DWORD output buffers.
        let ok = unsafe {
            WinHttpQueryHeaders(
                request.0.as_ptr(),
                level | 0x2000_0000,
                null(),
                (&raw mut val).cast::<c_void>(),
                &raw mut bytes,
                index,
            )
        };
        if ok == 0_i32 {
            Err(Self::last_err_code())
        } else {
            Ok(val)
        }
    }
    fn read_body(
        &mut self,
        request: &Handle,
        expected_len: Option<usize>,
        started: Instant,
    ) -> DownloadResult<Vec<u8>> {
        let mut body = match expected_len {
            Some(cap) => vec_cap(cap, "응답 본문")?,
            None => Vec::new(),
        };
        if self.read_buffer.capacity() < READ_BUF_BYTES {
            reserve(
                self.read_buffer.try_reserve_exact(READ_BUF_BYTES),
                "read 버퍼",
            )?;
        }
        self.read_buffer.resize(READ_BUF_BYTES, 0);
        loop {
            ensure(
                started.elapsed() < Duration::from_mins(1),
                "HTTP 전송 제한 시간(60초) 초과",
            )?;
            let mut read = 0_u32;
            // SAFETY: request is valid, self.read_buffer is writable, and read is an output buffer.
            let ok = unsafe {
                WinHttpReadData(
                    request.0.as_ptr(),
                    self.read_buffer.as_mut_ptr().cast::<c_void>(),
                    64_u32.strict_mul(1024),
                    &raw mut read,
                )
            };
            Self::check_winhttp(ok, "WinHttpReadData")?;
            let read_len = read as usize;
            if read_len == 0 {
                break;
            }
            let chunk = self
                .read_buffer
                .get(..read_len)
                .ok_or("응답 본문 chunk 범위 오류")?;
            checked_http_buffer_len("본문", body.len(), chunk.len(), HTTP_MAX_BODY_BYTES)?;
            reserve(body.try_reserve(chunk.len()), "응답 본문")?;
            body.extend_from_slice(chunk);
        }
        Ok(body)
    }
    pub(super) fn request(
        &mut self,
        req_body: Option<&[u8]>,
        host: &'static str,
        path: &str,
        req_hdrs: RequestHeaders<'_>,
    ) -> DownloadResult<HttpResponse> {
        let (body, method): (&[u8], &[u16]) = req_body.map_or_else(
            || (&[][..], [0x47_u16, 0x45, 0x54, 0].as_slice()),
            |payload| (payload, [0x50_u16, 0x4F, 0x53, 0x54, 0].as_slice()),
        );
        let path_w = wide(path)?;
        let hdr_cap = req_hdrs.iter().fold(1_usize, |cap, (nm, val)| {
            cap.strict_add(nm.len()).strict_add(val.len()).strict_add(4)
        });
        self.header_buffer.clear();
        reserve(self.header_buffer.try_reserve_exact(hdr_cap), "요청 헤더")?;
        for (name, val) in req_hdrs.iter() {
            self.header_buffer.extend(name.encode_utf16());
            self.header_buffer.extend_from_slice(&[0x3A, 0x20]);
            self.header_buffer.extend(val.encode_utf16());
            self.header_buffer.extend_from_slice(&[0x0D, 0x0A]);
        }
        let hdr_len = u32::try_from(self.header_buffer.len())
            .map_err(|src| download_error_with_source("요청 헤더 길이 변환 실패", src))?;
        self.header_buffer.push(0);
        let started = Instant::now();
        let connect = self.cached_connect(host)?;
        (|| {
            // SAFETY: method and path_w are NUL-terminated and connect is valid.
            let raw_req = unsafe {
                WinHttpOpenRequest(
                    connect.as_ptr(),
                    method.as_ptr(),
                    path_w.as_ptr(),
                    null(),
                    null(),
                    null(),
                    0x0080_0000,
                )
            };
            let request = Self::non_null_handle(raw_req, "WinHttpOpenRequest")?;
            for (opt, val, ctx) in [
                (79, 0x0000_0001, "WinHttpSetOption ENABLE_FEATURE"),
                (
                    63,
                    0x0000_0001 | 0x0000_0002,
                    "WinHttpSetOption DISABLE_FEATURE",
                ),
                (91, 256 * 1024, "WinHttpSetOption MAX_RESPONSE_HEADER_SIZE"),
            ] {
                Self::set_dword_option(&request, opt, val, ctx)?;
            }
            let body_len = u32::try_from(body.len())
                .map_err(|src| download_error_with_source("요청 본문 길이 변환 실패", src))?;
            let body_ptr = if body.is_empty() {
                null()
            } else {
                body.as_ptr().cast::<c_void>()
            };
            // SAFETY: request is valid, self.header_buffer is NUL-terminated, and body_ptr is null or points to body.
            let sent = unsafe {
                WinHttpSendRequest(
                    request.0.as_ptr(),
                    self.header_buffer.as_ptr(),
                    hdr_len,
                    body_ptr,
                    body_len,
                    body_len,
                    0,
                )
            };
            Self::check_winhttp(sent, "WinHttpSendRequest")?;
            // SAFETY: request is a valid request handle and no reserved pointer is required.
            let r_ok = unsafe { WinHttpReceiveResponse(request.0.as_ptr(), null_mut()) };
            Self::check_winhttp(r_ok, "WinHttpReceiveResponse")?;
            let status = Self::query_dword_header(&request, 19, null_mut())
                .map_err(|code| Self::win_err("WinHttpQueryHeaders status", code))?;
            let (mut headers, mut cl_idx) = (ResponseHeaders::default(), 0_u32);
            loop {
                let prev = cl_idx;
                let val = match Self::query_dword_header(&request, 5, &raw mut cl_idx) {
                    Ok(val) => val,
                    Err(ERR_HDR_NOT_FOUND) => break,
                    Err(code) => {
                        return Err(Self::win_err("WinHttpQueryHeaders Content-Length", code));
                    }
                };
                ensure(cl_idx > prev, "WinHTTP Content-Length header index 미진행")?;
                headers.set_content_length(val as usize)?;
            }
            let (mut cookie_idx, mut hdr_seen) = (0_u32, 0_usize);
            loop {
                let (cur_idx, mut bytes) = (cookie_idx, 0_u32);
                // SAFETY: request is valid; this call probes the indexed header value size.
                let probe = unsafe {
                    WinHttpQueryHeaders(
                        request.0.as_ptr(),
                        43,
                        null(),
                        null_mut(),
                        &raw mut bytes,
                        &raw mut cookie_idx,
                    )
                };
                ensure(
                    probe == 0_i32,
                    "WinHTTP Set-Cookie 크기 조회 예기치 않은 성공",
                )?;
                let code = Self::last_err_code();
                if code == ERR_HDR_NOT_FOUND {
                    break;
                }
                (code == 122)
                    .ok_or_else(|| Self::win_err("WinHttpQueryHeaders Set-Cookie 크기", code))?;
                let hdr_bytes = bytes as usize;
                hdr_seen =
                    checked_http_buffer_len("헤더", hdr_seen, hdr_bytes, HTTP_MAX_HEADER_BYTES)?;
                ensure(hdr_bytes.is_multiple_of(2), "Set-Cookie UTF-16 길이 오류")?;
                let units = hdr_bytes.div_euclid(2);
                self.header_buffer.clear();
                reserve(self.header_buffer.try_reserve(units), "Set-Cookie")?;
                self.header_buffer.resize(units, 0_u16);
                cookie_idx = cur_idx;
                // SAFETY: buffer has the probed size and request is valid.
                let fetched = unsafe {
                    WinHttpQueryHeaders(
                        request.0.as_ptr(),
                        43,
                        null(),
                        self.header_buffer.as_mut_ptr().cast::<c_void>(),
                        &raw mut bytes,
                        &raw mut cookie_idx,
                    )
                };
                Self::check_winhttp(fetched, "WinHttpQueryHeaders Set-Cookie")?;
                ensure(
                    cookie_idx > cur_idx,
                    "WinHTTP Set-Cookie header index 미진행",
                )?;
                while self.header_buffer.pop_if(|unit| *unit == 0).is_some() {}
                let val = String::from_utf16(&self.header_buffer).map_err(|src| {
                    download_error_with_source("Set-Cookie UTF-16 변환 실패", src)
                })?;
                headers.push_set_cookie(val.trim_ascii())?;
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
    fn set_dword_option(handle: &Handle, opt: u32, val: u32, ctx: &str) -> DownloadResult<()> {
        Self::try_set_dword_option(handle, opt, val).map_err(|code| Self::win_err(ctx, code))
    }
    fn try_set_dword_option(handle: &Handle, opt: u32, val: u32) -> CoreResult<(), u32> {
        // SAFETY: handle is valid and val points to a DWORD option value for this call.
        let ok = unsafe {
            WinHttpSetOption(handle.0.as_ptr(), opt, (&raw const val).cast::<c_void>(), 4)
        };
        (ok != 0_i32).ok_or_else(Self::last_err_code)
    }
    #[cold]
    #[inline(never)]
    fn win_err(ctx: &str, code: u32) -> DownloadError {
        fmt_err!("{ctx} 실패: Windows error {code}")
    }
}
fn wide(value: &str) -> DownloadResult<Vec<u16>> {
    let mut out = vec_cap(value.len().strict_add(1), "wide 문자열")?;
    out.extend(value.encode_utf16());
    out.push(0);
    Ok(out)
}
