use super::CellReference;
use crate::diagnostic::{Result, err, err_with_source};
use core::str;
const COL_NAME_BUF_LEN: usize = 8;
const _: () = assert!(COL_NAME_BUF_LEN >= 7, "COL_NAME_BUF_LEN too small");
pub(super) const MAX_A1_COL: u32 = 0x4000;
pub(super) const MAX_A1_ROW: u32 = 0x0010_0000;
pub(super) fn parse_ref_with_locks(reference: &str) -> Option<CellReference> {
    let bytes = reference.as_bytes();
    let mut index = 0_usize;
    let mut col_locked = false;
    if bytes.get(index) == Some(&b'$') {
        col_locked = true;
        index = index.strict_add(1);
    }
    let col_start = index;
    while bytes.get(index).is_some_and(u8::is_ascii_alphabetic) {
        index = index.strict_add(1);
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
        index = index.strict_add(1);
    }
    let row_start = index;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        index = index.strict_add(1);
    }
    if index == row_start {
        return None;
    }
    let row = reference.get(row_start..index)?.parse::<u32>().ok()?;
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
pub(super) fn with_unlocked_ref_parts<R>(
    mut col: u32,
    row: u32,
    use_parts: impl FnOnce(&str, u32) -> R,
) -> Result<R> {
    if !(1..=MAX_A1_COL).contains(&col) {
        return Err(err(format!("Excel column 범위를 벗어났습니다: {col}")));
    }
    let mut col_buffer = [0_u8; COL_NAME_BUF_LEN];
    let mut index = col_buffer.len();
    while col > 0 {
        let base = col.strict_sub(1);
        let [rem, ..] = base.rem_euclid(26).to_le_bytes();
        index = index.strict_sub(1);
        *col_buffer
            .get_mut(index)
            .ok_or_else(|| err("Excel column buffer 범위가 손상되었습니다."))? =
            b'A'.strict_add(rem);
        col = base.div_euclid(26);
    }
    let col_name = str::from_utf8(
        col_buffer
            .get(index..)
            .ok_or_else(|| err("Excel column 결과 범위가 손상되었습니다."))?,
    )
    .map_err(|source| err_with_source("Excel column UTF-8 변환 실패", source))?;
    if !(1..=MAX_A1_ROW).contains(&row) {
        return Err(err(format!("Excel row 범위를 벗어났습니다: {row}")));
    }
    Ok(use_parts(col_name, row))
}
