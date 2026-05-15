use crate::{Result, err, err_with_source, numeric::round_f64_to_i32};
pub fn canon_header(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if !ch.is_whitespace() {
            out.push(ch);
        }
    }
    out
}
pub fn same_trimmed(left: &str, right: &str) -> bool {
    left.trim() == right.trim()
}
pub fn parse_i32_str(text: &str) -> Option<i32> {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "-" {
        return None;
    }
    let normalized_storage;
    let normalized = if trimmed.contains(',') {
        normalized_storage = trimmed.replace(',', "");
        normalized_storage.as_str()
    } else {
        trimmed
    };
    normalized.parse::<f64>().ok().and_then(round_f64_to_i32)
}
pub fn usize_to_u32(value: usize, context: &str) -> Result<u32> {
    u32::try_from(value).map_err(|source| {
        let out = format!("{context} 값이 너무 큽니다. (value={value})");
        err_with_source(out, source)
    })
}
pub fn shift_row(row: u32, increase: u32, decrease: u32) -> u32 {
    if increase > 0 {
        row.saturating_add(increase)
    } else {
        row.saturating_sub(decrease).max(1)
    }
}
pub fn add_row_offset(base_row: u32, offset: usize, context: &str) -> Result<u32> {
    let offset_u32 = usize_to_u32(offset, context)?;
    base_row.checked_add(offset_u32).ok_or_else(|| {
        err(format!(
            "{context} 계산 중 overflow가 발생했습니다. ({base_row} + {offset_u32})"
        ))
    })
}
