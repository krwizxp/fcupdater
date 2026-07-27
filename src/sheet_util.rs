use crate::diagnostic::{Result, err, err_with_source};
pub(super) fn usize_to_u32(value: usize, context: &str) -> Result<u32> {
    u32::try_from(value).map_err(|source| {
        let out = format!("{context} 값이 너무 큽니다. (value={value})");
        err_with_source(out, source)
    })
}
pub(super) fn add_row_offset(base_row: u32, offset: usize, context: &str) -> Result<u32> {
    let offset_u32 = usize_to_u32(offset, context)?;
    base_row.checked_add(offset_u32).ok_or_else(|| {
        err(format!(
            "{context} 계산 중 overflow가 발생했습니다. ({base_row} + {offset_u32})"
        ))
    })
}
