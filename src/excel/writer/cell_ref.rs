use super::CellReference;
use crate::diagnostic::{Result, err, err_with_source};
use core::str;
const COL_NAME_BUF_LEN: usize = 8;
const _: () = assert!(COL_NAME_BUF_LEN >= 7, "COL_NAME_BUF_LEN too small");
pub(super) const MAX_A1_COL: u32 = 0x4000;
pub(super) const MAX_A1_ROW: u32 = 0x0010_0000;
impl CellReference {
    pub(super) const fn with_row(self, row: u32) -> Self {
        Self { row, ..self }
    }
}
fn col_name_text(mut col: u32, buffer: &mut [u8; COL_NAME_BUF_LEN]) -> Result<&str> {
    if !(1..=MAX_A1_COL).contains(&col) {
        return Err(err(format!("Excel column 범위를 벗어났습니다: {col}")));
    }
    let mut index = buffer.len();
    while col > 0 {
        let base = col
            .checked_sub(1)
            .ok_or_else(|| err("Excel column 변환 중 underflow가 발생했습니다."))?;
        let rem = u8::try_from(base.rem_euclid(26))
            .map_err(|source| err_with_source("Excel column 나머지 변환 실패", source))?;
        let letter = b'A'
            .checked_add(rem)
            .ok_or_else(|| err("Excel column 문자 계산 실패"))?;
        let next_index = index
            .checked_sub(1)
            .ok_or_else(|| err("Excel column buffer index 계산 실패"))?;
        index = next_index;
        let slot = buffer
            .get_mut(index)
            .ok_or_else(|| err("Excel column buffer 범위가 손상되었습니다."))?;
        *slot = letter;
        col = base.div_euclid(26);
    }
    let bytes = buffer
        .get(index..)
        .ok_or_else(|| err("Excel column 결과 범위가 손상되었습니다."))?;
    str::from_utf8(bytes).map_err(|source| err_with_source("Excel column UTF-8 변환 실패", source))
}
pub(super) fn parse_range_token(token: &str) -> (&str, &str) {
    token.split_once(':').unwrap_or((token, token))
}
const fn advance_index(index: &mut usize, step: usize) {
    *index = index.wrapping_add(step);
}
pub(super) fn parse_ref_with_locks(reference: &str) -> Option<CellReference> {
    let bytes = reference.as_bytes();
    let mut index = 0_usize;
    let mut col_locked = false;
    if bytes.get(index) == Some(&b'$') {
        col_locked = true;
        advance_index(&mut index, 1);
    }
    let col_start = index;
    while bytes.get(index).is_some_and(u8::is_ascii_alphabetic) {
        advance_index(&mut index, 1);
    }
    if index == col_start {
        return None;
    }
    let col_chars = bytes.get(col_start..index)?;
    if col_chars.len() > 3 {
        return None;
    }
    let mut col = 0_u32;
    for &ch in col_chars {
        let letter = u32::from(ch.to_ascii_uppercase())
            .checked_sub(u32::from('A'))?
            .checked_add(1)?;
        col = col.checked_mul(26)?.checked_add(letter)?;
    }
    if !(1..=MAX_A1_COL).contains(&col) {
        return None;
    }
    let mut row_locked = false;
    if bytes.get(index) == Some(&b'$') {
        row_locked = true;
        advance_index(&mut index, 1);
    }
    let row_start = index;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        advance_index(&mut index, 1);
    }
    if index == row_start {
        return None;
    }
    let mut row = 0_u32;
    for &ch in bytes.get(row_start..index)? {
        let digit = u32::from(ch).checked_sub(u32::from(b'0'))?;
        row = row.checked_mul(10)?.checked_add(digit)?;
    }
    if !(1..=MAX_A1_ROW).contains(&row) {
        return None;
    }
    (index == reference.len()).then_some(CellReference {
        col,
        col_locked,
        row,
        row_locked,
    })
}
pub(super) fn ref_with_locks(reference: CellReference) -> Result<String> {
    let mut col_buffer = [0_u8; COL_NAME_BUF_LEN];
    let col_name = col_name_text(reference.col, &mut col_buffer)?;
    if !(1..=MAX_A1_ROW).contains(&reference.row) {
        return Err(err(format!(
            "Excel row 범위를 벗어났습니다: {}",
            reference.row
        )));
    }
    let col_prefix = if reference.col_locked { "$" } else { "" };
    let row_prefix = if reference.row_locked { "$" } else { "" };
    Ok(format!(
        "{col_prefix}{col_name}{row_prefix}{}",
        reference.row
    ))
}
pub(super) fn with_unlocked_ref_parts<R>(
    col: u32,
    row: u32,
    use_parts: impl FnOnce(&str, u32) -> R,
) -> Result<R> {
    let mut col_buffer = [0_u8; COL_NAME_BUF_LEN];
    let col_name = col_name_text(col, &mut col_buffer)?;
    if !(1..=MAX_A1_ROW).contains(&row) {
        return Err(err(format!("Excel row 범위를 벗어났습니다: {row}")));
    }
    Ok(use_parts(col_name, row))
}
