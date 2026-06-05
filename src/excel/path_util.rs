use crate::diagnostic::{Result, err, err_with_source, prefixed_message};
use core::fmt::Display;
use std::path::{Component, Path, PathBuf};
pub(super) fn path_from_slashes(path: &str) -> PathBuf {
    let mut out = PathBuf::new();
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        out.push(segment);
    }
    out
}
pub(super) fn path_to_slashes(path: &Path, context: impl Display) -> Result<String> {
    let mut out = String::new();
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
        let separator_len = usize::from(!out.is_empty());
        let reserve_len = separator_len
            .checked_add(text.len())
            .ok_or_else(|| err("상대 경로 길이 계산 중 overflow가 발생했습니다."))?;
        out.try_reserve(reserve_len)
            .map_err(|source| err_with_source("상대 경로 메모리 확보 실패", source))?;
        if !out.is_empty() {
            out.push('/');
        }
        out.push_str(text);
    }
    Ok(out)
}
