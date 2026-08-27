use super::CellReference;
use crate::diagnostic::{Result, err};
use std::process;
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
    let mut col = 0_u32;
    while let Some(&ch) = bytes.get(index)
        && ch.is_ascii_alphabetic()
    {
        if index.strict_sub(col_start) >= 3 {
            return None;
        }
        let letter = u32::from(ch.to_ascii_uppercase())
            .checked_sub(u32::from('A'))?
            .checked_add(1)?;
        col = col.checked_mul(26)?.checked_add(letter)?;
        index = index.strict_add(1);
    }
    if index == col_start {
        return None;
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
    let mut row = 0_u32;
    while let Some(&ch) = bytes.get(index)
        && ch.is_ascii_digit()
    {
        row = row
            .strict_mul(10)
            .strict_add(u32::from(ch.strict_sub(b'0')));
        if row > MAX_A1_ROW {
            return None;
        }
        index = index.strict_add(1);
    }
    if index == row_start || row == 0 {
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
    (1..=MAX_A1_COL)
        .contains(&col)
        .ok_or_else(|| err(format!("Excel column 범위를 벗어났습니다: {col}")))?;
    let mut col_buffer = [0_u8; COL_NAME_BUF_LEN];
    let mut index = col_buffer.len();
    while col > 0 {
        let base = col.strict_sub(1);
        let [rem, ..] = base.rem_euclid(26).to_le_bytes();
        index = index.strict_sub(1);
        *col_buffer
            .get_mut(index)
            .unwrap_or_else(|| process::abort()) = b'A'.strict_add(rem);
        col = base.div_euclid(26);
    }
    let col_name = str::from_utf8(col_buffer.get(index..).unwrap_or_else(|| process::abort()))
        .unwrap_or_else(|_| process::abort());
    (1..=MAX_A1_ROW)
        .contains(&row)
        .ok_or_else(|| err(format!("Excel row 범위를 벗어났습니다: {row}")))?;
    Ok(use_parts(col_name, row))
}
