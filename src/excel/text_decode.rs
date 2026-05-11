use crate::{Result, err};
#[cfg(not(windows))]
use alloc::collections::VecDeque;
#[cfg(not(windows))]
use core::ffi::{c_char, c_int, c_void};
use core::ptr::null_mut;
#[cfg(not(windows))]
use core::sync::atomic::{AtomicBool, Ordering};
use std::env;
#[cfg(not(windows))]
use std::{
    collections::HashMap,
    io::{Write as _, stderr},
    sync::{LazyLock, Mutex},
};
#[cfg(not(windows))]
const CP949_CACHE_MAX_BYTES: usize = 8 * 1024 * 1024;
#[cfg(not(windows))]
const ICONV_FROM_CP949: &[u8] = b"CP949\0";
#[cfg(not(windows))]
const ICONV_TO_UTF8: &[u8] = b"UTF-8\0";
const WINDOWS_1252_EXTENDED_CHARS: [char; 32] = [
    '€', '�', '‚', 'ƒ', '„', '…', '†', '‡', 'ˆ', '‰', 'Š', '‹', 'Œ', '�', 'Ž', '�', '�', '‘', '’',
    '“', '”', '•', '–', '—', '˜', '™', 'š', '›', 'œ', '�', 'ž', 'Ÿ',
];
#[cfg(not(windows))]
static WARNED_NON_WINDOWS_CP949_FALLBACK: AtomicBool = AtomicBool::new(false);
#[cfg(not(windows))]
static CP949_ICONV_AVAILABLE: LazyLock<bool> = LazyLock::new(|| IconvDecoder::open().is_some());
#[cfg(not(windows))]
static CP949_DECODE_CACHE: LazyLock<Mutex<Cp949DecodeCache>> =
    LazyLock::new(|| Mutex::new(Cp949DecodeCache::default()));
#[cfg(not(windows))]
#[derive(Default)]
struct Cp949DecodeCache {
    map: HashMap<Vec<u8>, String>,
    order: VecDeque<Vec<u8>>,
    total_bytes: usize,
}
#[cfg(not(windows))]
struct NonWindowsCp949;
#[cfg(not(windows))]
trait NonWindowsCp949Ext {
    fn cache_get(&self, bytes: &[u8]) -> Option<String>;
    fn cache_put(&self, bytes: &[u8], decoded: &str);
    fn decode(&self, bytes: &[u8]) -> Option<String>;
    fn decode_with_iconv(&self, bytes: &[u8]) -> Option<String>;
    fn warn_once(&self, code_page: u16);
}
#[cfg(not(windows))]
type IconvDescriptor = *mut c_void;
#[cfg(not(windows))]
struct IconvDecoder {
    descriptor: IconvDescriptor,
}
#[cfg(not(windows))]
#[cfg_attr(any(target_os = "macos", target_os = "ios"), link(name = "iconv"))]
unsafe extern "C" {
    fn iconv_open(tocode: *const c_char, fromcode: *const c_char) -> IconvDescriptor;
    fn iconv(
        cd: IconvDescriptor,
        inbuf: *mut *mut c_char,
        inbytesleft: *mut usize,
        outbuf: *mut *mut c_char,
        outbytesleft: *mut usize,
    ) -> usize;
    fn iconv_close(cd: IconvDescriptor) -> c_int;
}
#[cfg(not(windows))]
impl IconvDecoder {
    fn decode(&mut self, bytes: &[u8]) -> Option<String> {
        let initial_len = bytes.len().checked_mul(3)?.checked_add(16)?.max(16);
        let mut output = vec![0_u8; initial_len];
        let mut input = bytes.as_ptr().cast::<c_char>().cast_mut();
        let mut input_left = bytes.len();
        let mut output_len = 0_usize;
        while input_left > 0 {
            if output_len == output.len() {
                grow_iconv_output(&mut output)?;
            }
            // SAFETY: `output_len` is kept within `output.len()` by the loop guard and updates below.
            let mut out = unsafe { output.as_mut_ptr().add(output_len) }.cast::<c_char>();
            let mut out_left = output.len().saturating_sub(output_len);
            let out_capacity = out_left;
            // SAFETY: `input` points into `bytes`, `out` points into `output`, and the byte counters describe the remaining valid ranges.
            let converted = unsafe {
                iconv(
                    self.descriptor,
                    &raw mut input,
                    &raw mut input_left,
                    &raw mut out,
                    &raw mut out_left,
                )
            };
            output_len = output_len.saturating_add(out_capacity.saturating_sub(out_left));
            if converted != usize::MAX {
                continue;
            }
            if out_left < 4 {
                grow_iconv_output(&mut output)?;
                continue;
            }
            return None;
        }
        self.flush(&mut output, &mut output_len)?;
        output.truncate(output_len);
        String::from_utf8(output).ok()
    }
    fn flush(&mut self, output: &mut Vec<u8>, output_len: &mut usize) -> Option<()> {
        loop {
            if *output_len == output.len() {
                grow_iconv_output(output)?;
            }
            let mut input = null_mut::<c_char>();
            let mut input_left = 0_usize;
            // SAFETY: `*output_len` is kept within `output.len()` by the loop guard and updates below.
            let mut out = unsafe { output.as_mut_ptr().add(*output_len) }.cast::<c_char>();
            let mut out_left = output.len().saturating_sub(*output_len);
            let out_capacity = out_left;
            // SAFETY: A null input pointer asks iconv to flush/reset shift state, and `out` describes the remaining output buffer.
            let converted = unsafe {
                iconv(
                    self.descriptor,
                    &raw mut input,
                    &raw mut input_left,
                    &raw mut out,
                    &raw mut out_left,
                )
            };
            *output_len = (*output_len).saturating_add(out_capacity.saturating_sub(out_left));
            if converted != usize::MAX {
                return Some(());
            }
            if out_left < 4 {
                grow_iconv_output(output)?;
                continue;
            }
            return None;
        }
    }
    fn open() -> Option<Self> {
        // SAFETY: Both encoding names are static NUL-terminated C strings.
        let descriptor = unsafe {
            iconv_open(
                ICONV_TO_UTF8.as_ptr().cast::<c_char>(),
                ICONV_FROM_CP949.as_ptr().cast::<c_char>(),
            )
        };
        if descriptor.addr() == usize::MAX {
            None
        } else {
            Some(Self { descriptor })
        }
    }
}
#[cfg(not(windows))]
impl Drop for IconvDecoder {
    fn drop(&mut self) {
        // SAFETY: `descriptor` was returned by `iconv_open` and is owned by this decoder.
        let _: c_int = unsafe { iconv_close(self.descriptor) };
    }
}
#[cfg(not(windows))]
impl NonWindowsCp949Ext for NonWindowsCp949 {
    fn cache_get(&self, bytes: &[u8]) -> Option<String> {
        let guard = CP949_DECODE_CACHE.lock().ok()?;
        guard.map.get(bytes).cloned()
    }
    fn cache_put(&self, bytes: &[u8], decoded: &str) {
        if let Ok(mut guard) = CP949_DECODE_CACHE.lock() {
            let key = bytes.to_vec();
            let entry_size = bytes.len().saturating_add(decoded.len());
            if entry_size > CP949_CACHE_MAX_BYTES {
                guard.map.clear();
                guard.order.clear();
                guard.total_bytes = 0;
                return;
            }
            if let Some(prev) = guard.map.remove(&key) {
                guard.total_bytes = guard
                    .total_bytes
                    .saturating_sub(bytes.len().saturating_add(prev.len()));
                guard
                    .order
                    .retain(|cache_key| cache_key.as_slice() != bytes);
            }
            while guard.total_bytes.saturating_add(entry_size) > CP949_CACHE_MAX_BYTES {
                let Some(evict_key) = guard.order.pop_front() else {
                    break;
                };
                if let Some(evicted) = guard.map.remove(&evict_key) {
                    guard.total_bytes = guard
                        .total_bytes
                        .saturating_sub(evict_key.len().saturating_add(evicted.len()));
                }
            }
            if guard.total_bytes.saturating_add(entry_size) > CP949_CACHE_MAX_BYTES {
                guard.map.clear();
                guard.order.clear();
                guard.total_bytes = 0;
            }
            guard.total_bytes = guard.total_bytes.saturating_add(entry_size);
            guard.order.push_back(key.clone());
            guard.map.insert(key, decoded.to_owned());
        }
    }
    fn decode(&self, bytes: &[u8]) -> Option<String> {
        if bytes.is_empty() {
            return Some(String::new());
        }
        if bytes.is_ascii() {
            return Some(String::from_utf8_lossy(bytes).into_owned());
        }
        if let Some(cached) = self.cache_get(bytes) {
            return Some(cached);
        }
        self.decode_with_iconv(bytes)
            .inspect(|text| self.cache_put(bytes, text))
    }
    fn decode_with_iconv(&self, bytes: &[u8]) -> Option<String> {
        if !*CP949_ICONV_AVAILABLE {
            return None;
        }
        IconvDecoder::open()?.decode(bytes)
    }
    fn warn_once(&self, code_page: u16) {
        if WARNED_NON_WINDOWS_CP949_FALLBACK
            .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            let mut err = stderr().lock();
            match writeln!(
                err,
                "주의: 비Windows 환경에서 code page {code_page}는 완전 디코딩이 불가하여 비ASCII 문자를 대체문자(�)로 처리합니다."
            ) {
                Ok(()) | Err(_) => {}
            }
        }
    }
}
#[cfg(not(windows))]
fn grow_iconv_output(output: &mut Vec<u8>) -> Option<()> {
    let next_len = output.len().checked_mul(2)?;
    output.resize(next_len, 0);
    Some(())
}
fn decode_bytes_to_string(bytes: &[u8], mut map_byte: impl FnMut(u8) -> char) -> String {
    let mut out = String::with_capacity(bytes.len());
    for byte in bytes {
        out.push(map_byte(*byte));
    }
    out
}
pub fn decode_single_byte_text(bytes: &[u8], code_page: Option<u16>) -> Result<String> {
    let decoded_text = cfg_select! {
        windows => {{
            let code_page_u32 = u32::from(code_page.unwrap_or(949));
            if bytes.is_empty() {
                Some(String::new())
            } else {
                let src_len = i32::try_from(bytes.len()).ok();
                src_len.and_then(|src_len_i32| {
                    // SAFETY: `bytes.as_ptr()` is valid for `src_len_i32` bytes and a null destination requests only the required UTF-16 output length.
                    let required = unsafe {
                        super::windows_api::MultiByteToWideChar(
                            code_page_u32,
                            super::windows_api::MB_ERR_INVALID_CHARS,
                            bytes.as_ptr(),
                            src_len_i32,
                            null_mut(),
                            0,
                        )
                    };
                    if required <= 0_i32 {
                        return None;
                    }
                    let required_usize = usize::try_from(required).ok()?;
                    let mut wide = vec![0_u16; required_usize];
                    // SAFETY: `wide` is allocated for `required` UTF-16 code units and both buffers remain valid for the duration of the conversion call.
                    let written = unsafe {
                        super::windows_api::MultiByteToWideChar(
                            code_page_u32,
                            0,
                            bytes.as_ptr(),
                            src_len_i32,
                            wide.as_mut_ptr(),
                            required,
                        )
                    };
                    if written <= 0_i32 {
                        return None;
                    }
                    let written_usize = usize::try_from(written).ok()?;
                    Some(String::from_utf16_lossy(wide.get(..written_usize)?))
                })
            }
        }}
        _ => None
    };
    if let Some(decoded) = decoded_text {
        return Ok(decoded);
    }
    match code_page {
        Some(65001) => Ok(String::from_utf8_lossy(bytes).into_owned()),
        Some(949 | 1361 | 51949) => {
            let cp949_decoded =
                cfg_select! { windows => { None } _ => { NonWindowsCp949.decode(bytes) } };
            if let Some(decoded_cp949_text) = cp949_decoded {
                return Ok(decoded_cp949_text);
            }
            if env::var("FCUPDATER_CP949_STRICT").is_ok_and(|value| {
                let trimmed = value.trim();
                trimmed == "1"
                    || trimmed.eq_ignore_ascii_case("true")
                    || trimmed.eq_ignore_ascii_case("yes")
                    || trimmed.eq_ignore_ascii_case("on")
            }) {
                return Err(err(format!(
                    "code page {} 디코딩에 실패했습니다. (FCUPDATER_CP949_STRICT=1)",
                    code_page.unwrap_or(949)
                )));
            }
            cfg_select! { windows => {} _ => { NonWindowsCp949.warn_once(code_page.unwrap_or(0)); } };
            Ok(decode_bytes_to_string(bytes, |byte| {
                if byte.is_ascii() {
                    char::from(byte)
                } else {
                    '�'
                }
            }))
        }
        Some(1252 | 28591) => Ok(decode_bytes_to_string(bytes, |byte| {
            if (0x80..=0x9F).contains(&byte) {
                WINDOWS_1252_EXTENDED_CHARS
                    .get(usize::from(byte).saturating_sub(0x80))
                    .copied()
                    .unwrap_or('�')
            } else {
                char::from(byte)
            }
        })),
        _ => Ok(decode_bytes_to_string(bytes, char::from)),
    }
}
#[cfg(test)]
mod tests {
    #[cfg(not(windows))]
    use super::{CP949_ICONV_AVAILABLE, NonWindowsCp949, NonWindowsCp949Ext as _};
    #[cfg(not(windows))]
    #[test]
    fn decodes_cp949_with_native_iconv_when_available() {
        if !*CP949_ICONV_AVAILABLE {
            return;
        }
        assert_eq!(
            NonWindowsCp949.decode_with_iconv(&[0xB0, 0xA1]).as_deref(),
            Some("가")
        );
    }
}
