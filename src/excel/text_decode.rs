use crate::{Result, err};
#[cfg(not(windows))]
use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    process::{Command, Stdio},
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
#[cfg(not(windows))]
const CP949_CACHE_MAX_BYTES: usize = 8 * 1024 * 1024;
#[cfg(not(windows))]
static WARNED_NON_WINDOWS_CP949_FALLBACK: AtomicBool = AtomicBool::new(false);
#[cfg(not(windows))]
static CP949_ICONV_AVAILABLE: OnceLock<bool> = OnceLock::new();
#[cfg(not(windows))]
static CP949_PYTHON3_AVAILABLE: OnceLock<bool> = OnceLock::new();
#[cfg(not(windows))]
static CP949_PYTHON_AVAILABLE: OnceLock<bool> = OnceLock::new();
#[cfg(not(windows))]
static CP949_DECODE_CACHE: OnceLock<Mutex<Cp949DecodeCache>> = OnceLock::new();
#[cfg(not(windows))]
#[derive(Default)]
struct Cp949DecodeCache {
    map: HashMap<Vec<u8>, String>,
    order: VecDeque<Vec<u8>>,
    total_bytes: usize,
}
pub fn decode_single_byte_text(bytes: &[u8], code_page: Option<u16>) -> Result<String> {
    #[cfg(windows)]
    {
        if let Some(decoded) =
            super::windows_api::decode_code_page(bytes, u32::from(code_page.unwrap_or(949)))
        {
            return Ok(decoded);
        }
    }
    match code_page {
        Some(65001) => Ok(String::from_utf8_lossy(bytes).into_owned()),
        Some(949 | 1361 | 51949) => {
            #[cfg(not(windows))]
            if let Some(decoded) = decode_cp949_non_windows(bytes) {
                return Ok(decoded);
            }
            if cp949_strict_mode() {
                let cp = code_page.unwrap_or(949);
                return Err(err(format!(
                    "code page {cp} 디코딩에 실패했습니다. (FCUPDATER_CP949_STRICT=1)"
                )));
            }
            Ok(decode_ascii_with_replacement(bytes, code_page))
        }
        Some(1252 | 28591) => Ok(decode_windows_1252(bytes)),
        _ => Ok(decode_latin1(bytes)),
    }
}
fn decode_ascii_with_replacement(bytes: &[u8], code_page: Option<u16>) -> String {
    #[cfg(windows)]
    let _ = code_page;
    #[cfg(not(windows))]
    warn_non_windows_cp949_once(code_page.unwrap_or(0));
    bytes
        .iter()
        .map(|b| {
            if b.is_ascii() {
                char::from(*b)
            } else {
                '\u{FFFD}'
            }
        })
        .collect()
}
fn decode_latin1(bytes: &[u8]) -> String {
    bytes.iter().map(|b| char::from(*b)).collect()
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
    let decoded = decode_cp949_with_iconv(bytes).or_else(|| decode_cp949_with_python(bytes));
    if let Some(text) = &decoded {
        cp949_cache_put(bytes, text);
    }
    decoded
}
fn cp949_strict_mode() -> bool {
    std::env::var("FCUPDATER_CP949_STRICT")
        .ok()
        .is_some_and(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}
#[cfg(not(windows))]
fn cp949_cache_get(bytes: &[u8]) -> Option<String> {
    let cache = CP949_DECODE_CACHE.get_or_init(|| Mutex::new(Cp949DecodeCache::default()));
    let guard = cache.lock().ok()?;
    guard.map.get(bytes).cloned()
}
#[cfg(not(windows))]
fn cp949_cache_put(bytes: &[u8], decoded: &str) {
    let cache = CP949_DECODE_CACHE.get_or_init(|| Mutex::new(Cp949DecodeCache::default()));
    if let Ok(mut guard) = cache.lock() {
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
        guard.map.insert(key, decoded.to_string());
    }
}
#[cfg(not(windows))]
fn decode_cp949_with_iconv(bytes: &[u8]) -> Option<String> {
    let available =
        *CP949_ICONV_AVAILABLE.get_or_init(|| command_available("iconv", &["--version"]));
    if !available {
        return None;
    }
    run_decoder_command("iconv", &["-f", "CP949", "-t", "UTF-8"], bytes)
}
#[cfg(not(windows))]
fn decode_cp949_with_python(bytes: &[u8]) -> Option<String> {
    let script = "import sys;data=sys.stdin.buffer.read();sys.stdout.buffer.write(data.decode('cp949','strict').encode('utf-8'))";
    let python3_available =
        *CP949_PYTHON3_AVAILABLE.get_or_init(|| command_available("python3", &["-V"]));
    if python3_available
        && let Some(decoded) = run_decoder_command("python3", &["-c", script], bytes)
    {
        return Some(decoded);
    }
    let python_available =
        *CP949_PYTHON_AVAILABLE.get_or_init(|| command_available("python", &["-V"]));
    if !python_available {
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
        let _ = child.kill();
        let _ = child.wait();
        return None;
    }
    let output = wait_decoder_with_optional_timeout(child, decoder_timeout())?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout).ok()
}
#[cfg(not(windows))]
fn decoder_timeout() -> Option<Duration> {
    std::env::var("FCUPDATER_DECODER_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
}
#[cfg(not(windows))]
fn wait_decoder_with_optional_timeout(
    mut child: std::process::Child,
    timeout: Option<Duration>,
) -> Option<std::process::Output> {
    let Some(limit) = timeout else {
        return child.wait_with_output().ok();
    };
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => return child.wait_with_output().ok(),
            Ok(None) => {
                if start.elapsed() >= limit {
                    let _ = child.kill();
                    let _ = child.wait();
                    return None;
                }
                std::thread::sleep(Duration::from_millis(25));
            }
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
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
        eprintln!(
            "[주의] 비Windows 환경에서 code page {code_page}는 완전 디코딩이 불가하여 비ASCII 문자를 대체문자(�)로 처리합니다."
        );
    }
}
fn decode_windows_1252(bytes: &[u8]) -> String {
    bytes.iter().map(|b| decode_windows_1252_byte(*b)).collect()
}
fn decode_windows_1252_byte(byte: u8) -> char {
    match byte {
        0x80 => '\u{20AC}',
        0x81 | 0x8D | 0x8F | 0x90 | 0x9D => '\u{FFFD}',
        0x82 => '\u{201A}',
        0x83 => '\u{0192}',
        0x84 => '\u{201E}',
        0x85 => '\u{2026}',
        0x86 => '\u{2020}',
        0x87 => '\u{2021}',
        0x88 => '\u{02C6}',
        0x89 => '\u{2030}',
        0x8A => '\u{0160}',
        0x8B => '\u{2039}',
        0x8C => '\u{0152}',
        0x8E => '\u{017D}',
        0x91 => '\u{2018}',
        0x92 => '\u{2019}',
        0x93 => '\u{201C}',
        0x94 => '\u{201D}',
        0x95 => '\u{2022}',
        0x96 => '\u{2013}',
        0x97 => '\u{2014}',
        0x98 => '\u{02DC}',
        0x99 => '\u{2122}',
        0x9A => '\u{0161}',
        0x9B => '\u{203A}',
        0x9C => '\u{0153}',
        0x9E => '\u{017E}',
        0x9F => '\u{0178}',
        _ => char::from(byte),
    }
}
