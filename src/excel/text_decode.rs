use crate::{Result, err};
#[cfg(not(windows))]
use core::fmt::Arguments;
#[cfg(windows)]
use core::ptr::null_mut;
use std::env;
#[cfg(not(windows))]
use std::{
    collections::{HashMap, VecDeque},
    io::{ErrorKind, Write},
    process::{Child, Command, Output, Stdio},
    sync::{
        LazyLock, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread::sleep,
    time::{Duration, Instant},
};
#[cfg(not(windows))]
const CP949_CACHE_MAX_BYTES: usize = 8 * 1024 * 1024;
#[cfg(not(windows))]
static WARNED_NON_WINDOWS_CP949_FALLBACK: AtomicBool = AtomicBool::new(false);
#[cfg(not(windows))]
static CP949_ICONV_AVAILABLE: LazyLock<bool> =
    LazyLock::new(|| command_available("iconv", &["--version"]));
#[cfg(not(windows))]
static CP949_PYTHON3_AVAILABLE: LazyLock<bool> =
    LazyLock::new(|| command_available("python3", &["-V"]));
#[cfg(not(windows))]
static CP949_PYTHON_AVAILABLE: LazyLock<bool> =
    LazyLock::new(|| command_available("python", &["-V"]));
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
const WINDOWS_1252_EXTENDED_CHARS: [char; 32] = [
    '€', '�', '‚', 'ƒ', '„', '…', '†', '‡', 'ˆ', '‰', 'Š', '‹', 'Œ', '�', 'Ž', '�', '�', '‘', '’',
    '“', '”', '•', '–', '—', '˜', '™', 'š', '›', 'œ', '�', 'ž', 'Ÿ',
];
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
                cfg_select! { windows => { None } _ => { decode_cp949_non_windows(bytes) } };
            if let Some(decoded_cp949_text) = cp949_decoded {
                return Ok(decoded_cp949_text);
            }
            if env::var("FCUPDATER_CP949_STRICT")
                .ok()
                .is_some_and(|value| {
                    let trimmed = value.trim();
                    trimmed == "1"
                        || trimmed.eq_ignore_ascii_case("true")
                        || trimmed.eq_ignore_ascii_case("yes")
                        || trimmed.eq_ignore_ascii_case("on")
                })
            {
                return Err(err(format!(
                    "code page {} 디코딩에 실패했습니다. (FCUPDATER_CP949_STRICT=1)",
                    code_page.unwrap_or(949)
                )));
            }
            cfg_select! { windows => {} _ => { warn_non_windows_cp949_once(code_page.unwrap_or(0)); } };
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
#[cfg(not(windows))]
fn decode_cp949_non_windows(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        return Some(String::new());
    }
    if bytes.iter().all(|b| b.is_ascii()) {
        return Some(String::from_utf8_lossy(bytes).into_owned());
    }
    if let Some(cached) = cp949_cache_get(bytes) {
        return Some(cached);
    }
    decode_cp949_with_iconv(bytes)
        .or_else(|| decode_cp949_with_python(bytes))
        .inspect(|text| cp949_cache_put(bytes, text))
}
#[cfg(not(windows))]
fn cp949_cache_get(bytes: &[u8]) -> Option<String> {
    let guard = CP949_DECODE_CACHE.lock().ok()?;
    guard.map.get(bytes).cloned()
}
#[cfg(not(windows))]
fn cp949_cache_put(bytes: &[u8], decoded: &str) {
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
            guard.order.retain(|k| k.as_slice() != bytes);
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
#[cfg(not(windows))]
fn decode_cp949_with_iconv(bytes: &[u8]) -> Option<String> {
    if !*CP949_ICONV_AVAILABLE {
        return None;
    }
    run_decoder_command("iconv", &["-f", "CP949", "-t", "UTF-8"], bytes)
}
#[cfg(not(windows))]
fn decode_cp949_with_python(bytes: &[u8]) -> Option<String> {
    let script = "import sys;data=sys.stdin.buffer.read();sys.stdout.buffer.write(data.decode('cp949','strict').encode('utf-8'))";
    if *CP949_PYTHON3_AVAILABLE
        && let Some(decoded) = run_decoder_command("python3", &["-c", script], bytes)
    {
        return Some(decoded);
    }
    if !*CP949_PYTHON_AVAILABLE {
        return None;
    }
    run_decoder_command("python", &["-c", script], bytes)
}
#[cfg(not(windows))]
fn run_decoder_command(program: &str, args: &[&str], input: &[u8]) -> Option<String> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;
    if let Some(mut stdin) = child.stdin.take()
        && stdin.write_all(input).is_err()
    {
        report_decoder_cleanup_issue(program, stop_decoder_child(&mut child));
        return None;
    }
    let output = wait_decoder_with_optional_timeout(program, child, decoder_timeout())?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}
#[cfg(not(windows))]
fn decoder_timeout() -> Option<Duration> {
    env::var("FCUPDATER_DECODER_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
}
#[cfg(not(windows))]
fn wait_decoder_with_optional_timeout(
    program: &str,
    mut child: Child,
    timeout: Option<Duration>,
) -> Option<Output> {
    let Some(limit) = timeout else {
        return child.wait_with_output().ok();
    };
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return child.wait_with_output().ok(),
            Ok(None) => {
                if start.elapsed() >= limit {
                    report_decoder_cleanup_issue(program, stop_decoder_child(&mut child));
                    return None;
                }
                sleep(Duration::from_millis(25));
            }
            Err(_) => {
                report_decoder_cleanup_issue(program, stop_decoder_child(&mut child));
                return None;
            }
        }
    }
}
#[cfg(not(windows))]
fn command_available(program: &str, args: &[&str]) -> bool {
    Command::new(program)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}
#[cfg(not(windows))]
fn warn_non_windows_cp949_once(code_page: u16) {
    if WARNED_NON_WINDOWS_CP949_FALLBACK
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        write_nonfatal_stderr_line(format_args!(
            "주의: 비Windows 환경에서 code page {code_page}는 완전 디코딩이 불가하여 비ASCII 문자를 대체문자(�)로 처리합니다."
        ));
    }
}
#[cfg(not(windows))]
fn report_decoder_cleanup_issue(program: &str, cleanup_diagnostic: Option<String>) {
    let Some(cleanup) = cleanup_diagnostic else {
        return;
    };
    write_nonfatal_stderr_line(format_args!(
        "주의: {program} 디코더 프로세스 정리 중 문제가 발생했습니다: {cleanup}"
    ));
}
#[cfg(not(windows))]
fn stop_decoder_child(child: &mut Child) -> Option<String> {
    let mut diagnostic = String::with_capacity(96);
    match child.try_wait() {
        Ok(Some(_)) => return None,
        Ok(None) => {}
        Err(source_err) => {
            diagnostic.push_str("상태 확인 실패: ");
            match write!(&mut diagnostic, "{source_err}") {
                Ok(()) | Err(_) => {}
            }
        }
    }
    match child.kill() {
        Ok(()) => {}
        Err(source_err) if source_err.kind() == ErrorKind::InvalidInput => {}
        Err(source_err) => {
            if !diagnostic.is_empty() {
                diagnostic.push_str(" / ");
            }
            diagnostic.push_str("종료 실패: ");
            match write!(&mut diagnostic, "{source_err}") {
                Ok(()) | Err(_) => {}
            }
        }
    }
    match child.wait() {
        Ok(_) => {}
        Err(source_err) => {
            if !diagnostic.is_empty() {
                diagnostic.push_str(" / ");
            }
            diagnostic.push_str("대기 실패: ");
            match write!(&mut diagnostic, "{source_err}") {
                Ok(()) | Err(_) => {}
            }
        }
    }
    if diagnostic.is_empty() {
        None
    } else {
        Some(diagnostic)
    }
}
#[cfg(not(windows))]
fn write_nonfatal_stderr_line(args: Arguments<'_>) {
    let mut err = std::io::stderr().lock();
    match writeln!(err, "{args}") {
        Ok(()) | Err(_) => {}
    }
}
