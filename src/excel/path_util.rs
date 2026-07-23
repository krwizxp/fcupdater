use crate::diagnostic::{Result, err, err_with_source, prefixed_message};
use core::fmt::Display;
use std::path::{Component, Path};
pub(super) fn reject_windows_special_component<D>(component: &str, context: &D) -> Result<()>
where
    D: Display,
{
    if component.contains(':') {
        return Err(err(prefixed_message(
            "Windows ADS 경로 component는 허용되지 않습니다: ",
            context,
        )));
    }
    if component.ends_with([' ', '.']) {
        return Err(err(prefixed_message(
            "Windows에서 공백 또는 점으로 끝나는 경로 component는 허용되지 않습니다: ",
            context,
        )));
    }
    let normalized = component.trim_end_matches([' ', '.']);
    let stem = match normalized.split_once('.') {
        Some((first, _)) => first,
        None => normalized,
    };
    let is_numbered_device =
        if let Some((name_prefix, suffix)) = stem.as_bytes().split_first_chunk::<3>() {
            matches!(suffix, [b'1'..=b'9'] | [0xc2, 0xb9 | 0xb2 | 0xb3])
                && (name_prefix.eq_ignore_ascii_case(b"COM")
                    || name_prefix.eq_ignore_ascii_case(b"LPT"))
        } else {
            false
        };
    if stem.eq_ignore_ascii_case("CON")
        || stem.eq_ignore_ascii_case("PRN")
        || stem.eq_ignore_ascii_case("AUX")
        || stem.eq_ignore_ascii_case("NUL")
        || stem.eq_ignore_ascii_case("CONIN$")
        || stem.eq_ignore_ascii_case("CONOUT$")
        || is_numbered_device
    {
        return Err(err(prefixed_message(
            "Windows 예약 파일명은 허용되지 않습니다: ",
            context,
        )));
    }
    Ok(())
}
pub(super) fn path_to_slashes<D>(path: &Path, context: D) -> Result<String>
where
    D: Display,
{
    let mut out = String::new();
    out.try_reserve_exact(path.as_os_str().len())
        .map_err(|source| err_with_source("상대 경로 메모리 확보 실패", source))?;
    for component in path.components() {
        let Component::Normal(part) = component else {
            return Err(err(prefixed_message(
                "상대 경로에 허용되지 않은 component가 있습니다: ",
                &context,
            )));
        };
        let Some(text) = part.to_str() else {
            return Err(err(prefixed_message(
                "상대 경로 component가 UTF-8이 아닙니다: ",
                &context,
            )));
        };
        if !out.is_empty() {
            out.push('/');
        }
        out.push_str(text);
    }
    Ok(out)
}
