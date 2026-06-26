use crate::diagnostic::{Result, err, err_with_source, prefixed_message};
use core::fmt::Display;
use std::path::{Component, Path};
pub(super) fn path_to_slashes<D>(path: &Path, context: D) -> Result<String>
where
    D: Display,
{
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
