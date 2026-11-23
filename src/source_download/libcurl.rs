use super::{
    DownloadResult, HTTP_MAX_BODY_BYTES, HTTP_MAX_HEADER_BYTES, HttpResponse, RequestHeaders,
    ResponseHeaders, checked_http_buffer_len, download_error_with_source,
};
use crate::diagnostic::{AppError as DownloadError, fmt_err, fmt_src, or_abort, reserve};
use alloc::{borrow::Cow, string::String, vec::Vec};
use core::{
    ffi::{CStr, c_char, c_long, c_uint, c_void},
    mem::{self, DropGuard},
    ptr::{NonNull, null_mut},
    slice, str,
};
use std::sync::LazyLock;
#[link(name = "curl")]
unsafe extern "C" {
    fn curl_easy_cleanup(curl: *mut Curl);
    fn curl_easy_getinfo(curl: *mut Curl, info: CurlInfo, ...) -> CurlCode;
    fn curl_easy_init() -> *mut Curl;
    fn curl_easy_perform(curl: *mut Curl) -> CurlCode;
    fn curl_easy_reset(curl: *mut Curl);
    fn curl_easy_setopt(curl: *mut Curl, option: CurlOption, ...) -> CurlCode;
    fn curl_easy_strerror(code: CurlCode) -> *const c_char;
    fn curl_global_init(flags: c_long) -> CurlCode;
    fn curl_slist_append(list: *mut CurlSlist, string: *const c_char) -> *mut CurlSlist;
    fn curl_slist_free_all(list: *mut CurlSlist);
}
macro_rules! curl_setopt {
    ($h:expr, $opt:expr, $val:expr) => {{
        // SAFETY: call sites pair each option with its documented libcurl ABI type.
        let code = unsafe { curl_easy_setopt($h.0.as_ptr(), $opt, $val) };
        (code == CURLE_OK).ok_or_else(|| curl_error("curl_easy_setopt", code))
    }};
}
const CURLE_OK: CurlCode = 0;
static CURL_INIT: LazyLock<CurlCode> = LazyLock::new(|| {
    // SAFETY: LazyLock runs this initializer once before any easy handles are used.
    unsafe { curl_global_init(3) }
});
enum Curl {}
type CurlCb = unsafe extern "C" fn(*mut c_char, usize, usize, *mut c_void) -> usize;
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
enum CurlSlist {}
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
impl Client {
    pub(super) fn request(
        &mut self,
        req_body: Option<&[u8]>,
        host: &'static str,
        path: &str,
        req_hdrs: RequestHeaders<'_>,
    ) -> DownloadResult<HttpResponse> {
        let mut body_buf = ResponseBody {
            bytes: Vec::new(),
            error: None,
        };
        let mut hdr_cap = CurlHeaderCapture {
            bytes_seen: 0,
            completed_block: None,
            current_block: None,
            error: None,
        };
        let (mut url_buf, mut hdr_build, mut err_buf) = (
            mem::take(&mut self.url_buffer),
            mem::take(&mut self.header_build_buffer),
            [0_u8; 256],
        );
        let result = (|| {
            let mut hdr_list = HeaderList(None);
            for (name, val) in req_hdrs.iter() {
                let hdr = nul_buf(
                    &mut hdr_build,
                    &[name.as_bytes(), b": ", val.as_bytes()],
                    "HTTP header",
                )?;
                let cur_ptr = hdr_list.0.map_or(null_mut(), NonNull::as_ptr);
                // SAFETY: hdr is a valid NUL-terminated string and cur_ptr is null or a libcurl list.
                let next_ptr = unsafe { curl_slist_append(cur_ptr, hdr.as_ptr()) };
                hdr_list.0 = Some(NonNull::new(next_ptr).ok_or("curl_slist_append 실패")?);
            }
            let url = nul_buf(
                &mut url_buf,
                &[b"https://", host.as_bytes(), path.as_bytes()],
                "URL",
            )?;
            let init_code = *CURL_INIT;
            (init_code == CURLE_OK).ok_or_else(|| curl_error("curl_global_init", init_code))?;
            let handle = match &mut self.easy_handle {
                &mut Some(ref mut handle) => handle,
                empty @ &mut None => {
                    // SAFETY: curl_easy_init has no preconditions after global init.
                    let raw = unsafe { curl_easy_init() };
                    empty.insert(EasyHandle(NonNull::new(raw).ok_or("curl_easy_init 실패")?))
                }
            };
            let (perform_code, response_info) = {
                let (mut body_tgt, mut hdr_tgt) = (
                    CurlWriteTarget::Body(&mut body_buf),
                    CurlWriteTarget::Header(&mut hdr_cap),
                );
                let _options = DropGuard::new(&*handle, |reset_handle| {
                    // SAFETY: reset_handle is valid; reset clears options while keeping connection cache.
                    unsafe {
                        curl_easy_reset(reset_handle.0.as_ptr());
                    }
                });
                let list_ptr = hdr_list.0.map_or(null_mut(), NonNull::as_ptr);
                for (opt, ptr) in [
                    (10_002, url.as_ptr().cast::<c_void>()),
                    (10_023, list_ptr.cast::<c_void>()),
                    (10_010, err_buf.as_mut_ptr().cast::<c_void>()),
                    (10_318, c"https".as_ptr().cast::<c_void>()),
                    (10_001, (&raw mut body_tgt).cast::<c_void>()),
                    (10_029, (&raw mut hdr_tgt).cast::<c_void>()),
                ] {
                    curl_setopt!(handle, opt, ptr)?;
                }
                let (write_cb, header_cb): (CurlCb, CurlCb) = (write_callback, write_callback);
                curl_setopt!(handle, 20_011, write_cb)?;
                curl_setopt!(handle, 20_079, header_cb)?;
                for (opt, val) in [
                    (156, 30_000_i64),
                    (155, 60_000_i64),
                    (52, 0_i64),
                    (99, 1_i64),
                    (32, 6_i64 | (1_i64 << 16_u32)),
                ] {
                    let opt_val: c_long = val;
                    curl_setopt!(handle, opt, opt_val)?;
                }
                let max_body: CurlOffT = 32_i64.strict_mul(1024).strict_mul(1024);
                curl_setopt!(handle, 30_117, max_body)?;
                if let Some(body_bytes) = req_body {
                    curl_setopt!(handle, 47, 1_i64)?;
                    let body_len = c_long::try_from(body_bytes.len()).map_err(|src| {
                        download_error_with_source("요청 본문 길이 변환 실패", src)
                    })?;
                    curl_setopt!(handle, 60, body_len)?;
                    if !body_bytes.is_empty() {
                        curl_setopt!(handle, 10_015, body_bytes.as_ptr().cast::<c_void>())?;
                    }
                } else {
                    curl_setopt!(handle, 80, 1_i64)?;
                }
                // SAFETY: handle is configured and callback data live until this call returns.
                let code = unsafe { curl_easy_perform(handle.0.as_ptr()) };
                let info = (code == CURLE_OK).then(|| -> DownloadResult<c_long> {
                    let mut scheme = null_mut::<c_char>();
                    // SAFETY: scheme is a valid output pointer for 0x10_0031.
                    let sc =
                        unsafe { curl_easy_getinfo(handle.0.as_ptr(), 0x10_0031, &raw mut scheme) };
                    (sc == CURLE_OK).ok_or_else(|| curl_error("curl_easy_getinfo scheme", sc))?;
                    let scheme_ptr = NonNull::new(scheme).ok_or("curl 최종 scheme 비어 있음")?;
                    // SAFETY: libcurl returns a NUL-terminated scheme string owned by the easy handle.
                    let scheme_bytes = unsafe { CStr::from_ptr(scheme_ptr.as_ptr()) }.to_bytes();
                    scheme_bytes.eq_ignore_ascii_case(b"https").ok_or_else(|| {
                        fmt_err!(
                            "curl 최종 scheme 비HTTPS: {}",
                            String::from_utf8_lossy(scheme_bytes)
                        )
                    })?;
                    let mut raw_status = c_long::default();
                    // SAFETY: raw_status is a valid output pointer for 0x20_0002.
                    let rc = unsafe {
                        curl_easy_getinfo(handle.0.as_ptr(), 0x20_0002, &raw mut raw_status)
                    };
                    (rc == CURLE_OK)
                        .ok_or_else(|| curl_error("curl_easy_getinfo response_code", rc))?;
                    Ok(raw_status)
                });
                (code, info)
            };
            if let Some(cb_err) = body_buf.error.take().or_else(|| hdr_cap.error.take()) {
                self.easy_handle = None;
                return Err(cb_err);
            }
            if perform_code != CURLE_OK {
                self.easy_handle = None;
                return Err(
                    if let Ok(msg) = CStr::from_bytes_until_nul(&err_buf)
                        && !msg.is_empty()
                    {
                        fmt_err!(
                            "curl_easy_perform 실패: {} ({perform_code})",
                            msg.to_string_lossy()
                        )
                    } else {
                        curl_error("curl_easy_perform", perform_code)
                    },
                );
            }
            u32::try_from(or_abort(response_info)?)
                .map_err(|src| download_error_with_source("HTTP 상태 코드 변환 실패", src))
        })();
        (self.header_build_buffer, self.url_buffer) = (hdr_build, url_buf);
        let status = result?;
        let final_block = hdr_cap
            .completed_block
            .ok_or("완료된 HTTP header block 없음")?;
        if let Some(hdr_err) = final_block.error {
            return Err(hdr_err);
        }
        Ok(HttpResponse {
            body: body_buf.bytes,
            headers: final_block.headers,
            status,
        })
    }
}
impl ResponseBody {
    fn append(&mut self, bytes: &[u8]) -> bool {
        if let Err(err_val) =
            checked_http_buffer_len("본문", self.bytes.len(), bytes.len(), HTTP_MAX_BODY_BYTES)
                .and_then(|_| reserve(self.bytes.try_reserve(bytes.len()), "HTTP 응답 본문"))
        {
            self.error = Some(err_val);
            return false;
        }
        self.bytes.extend_from_slice(bytes);
        true
    }
}
impl CurlHeaderCapture {
    fn append(&mut self, bytes: &[u8]) -> bool {
        match checked_http_buffer_len("헤더", self.bytes_seen, bytes.len(), HTTP_MAX_HEADER_BYTES)
        {
            Ok(next) => self.bytes_seen = next,
            Err(err_val) => {
                self.error = Some(err_val);
                return false;
            }
        }
        let Some(raw_line) = bytes.strip_suffix(b"\n") else {
            self.error = Some("libcurl 종결되지 않은 HTTP header line".into());
            return false;
        };
        let line = raw_line.trim_suffix(b"\r");
        if line.starts_with(b"HTTP/") {
            self.current_block = Some(CapturedHeaderBlock::default());
            return true;
        }
        if line.is_empty() {
            if let Some(block) = self.current_block.take() {
                self.completed_block = Some(block);
            }
            return true;
        }
        if let Some(block) = self.current_block.as_mut()
            && block.error.is_none()
            && let trimmed = line.trim_ascii()
            && let Some(colon) = trimmed.iter().position(|&ch| ch == b':')
        {
            let (raw_name, tail) = trimmed.split_at(colon);
            let name = raw_name.trim_ascii();
            let is_cl = name.eq_ignore_ascii_case(b"Content-Length");
            if is_cl || name.eq_ignore_ascii_case(b"Set-Cookie") {
                let raw_val = or_abort(tail.get(1..)).trim_ascii();
                let res = str::from_utf8(raw_val)
                    .map_err(|src| download_error_with_source("HTTP header UTF-8 실패", src))
                    .and_then(|val| {
                        if is_cl {
                            block.headers.parse_content_length(val)
                        } else {
                            block.headers.push_set_cookie(val)
                        }
                    });
                if let Err(src) = res {
                    block.error = Some(src);
                }
            }
        }
        true
    }
}
#[cold]
#[inline(never)]
fn curl_error(context: &str, code: CurlCode) -> DownloadError {
    // SAFETY: curl_easy_strerror returns either null or a static NUL-terminated message for code.
    let raw_ptr = unsafe { curl_easy_strerror(code) };
    let msg = if raw_ptr.is_null() {
        Cow::Borrowed("unknown curl error")
    } else {
        // SAFETY: libcurl guarantees a valid NUL-terminated string for non-null strerror results.
        unsafe { CStr::from_ptr(raw_ptr) }.to_string_lossy()
    };
    fmt_err!("{context} 실패: {msg} ({code})")
}
fn nul_buf<'buffer>(
    out: &'buffer mut Vec<u8>,
    parts: &[&[u8]],
    label: &str,
) -> DownloadResult<&'buffer CStr> {
    let cap = parts
        .iter()
        .fold(1_usize, |sum, part| sum.strict_add(part.len()));
    out.clear();
    reserve(out.try_reserve_exact(cap), "libcurl 문자열")?;
    for part in parts {
        out.extend_from_slice(part);
    }
    out.push(0);
    CStr::from_bytes_with_nul(out).map_err(|src| fmt_src!(src, "{label}에 NUL 문자 포함"))
}
unsafe extern "C" fn write_callback(
    ptr: *mut c_char,
    size: usize,
    nmemb: usize,
    userdata: *mut c_void,
) -> usize {
    let Some(len) = size.checked_mul(nmemb).filter(|&cnt| cnt != 0) else {
        return 0;
    };
    let (Some(head), Some(mut target_ptr)) = (
        NonNull::new(ptr.cast::<u8>()),
        NonNull::new(userdata.cast::<CurlWriteTarget<'_>>()),
    ) else {
        return 0;
    };
    // SAFETY: len is non-zero, head is non-null, and libcurl passes a readable buffer with len bytes.
    let bytes = unsafe { slice::from_raw_parts(head.as_ptr(), len) };
    // SAFETY: userdata is the CurlWriteTarget pointer configured before curl_easy_perform.
    let accepted = match *unsafe { target_ptr.as_mut() } {
        CurlWriteTarget::Body(ref mut buffer) => (*buffer).append(bytes),
        CurlWriteTarget::Header(ref mut capture) => (*capture).append(bytes),
    };
    if accepted { len } else { 0 }
}
