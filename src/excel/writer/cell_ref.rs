use super::CellReference;
use crate::diagnostic::{Result, err_fmt};
use std::process;
const MAX_A1_COLUMN_LETTERS: usize = 3;
pub(super) const SINGLE_LETTER_COLUMNS: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
const MAX_A1_COL: u32 = 0x4000;
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
        if index.strict_sub(col_start) >= MAX_A1_COLUMN_LETTERS {
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
    let row = bytes.get(index..)?.iter().try_fold(0_u32, |row, &ch| {
        if !ch.is_ascii_digit() {
            return None;
        }
        let value = row
            .strict_mul(10)
            .strict_add(u32::from(ch.strict_sub(b'0')));
        (value <= MAX_A1_ROW).then_some(value)
    })?;
    (row != 0).then_some(CellReference {
        col,
        col_locked,
        row,
        row_locked,
    })
}
pub(super) fn col_name_into(
    mut col: u32,
    col_buffer: &mut [u8; MAX_A1_COLUMN_LETTERS],
) -> Result<&str> {
    (1..=MAX_A1_COL)
        .contains(&col)
        .ok_or_else(|| err_fmt(format_args!("Excel column 범위를 벗어났습니다: {col}")))?;
    let single_index = col.strict_sub(1) as usize;
    if let Some(single) = SINGLE_LETTER_COLUMNS.get(single_index..single_index.strict_add(1)) {
        return Ok(single);
    }
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
    Ok(
        str::from_utf8(col_buffer.get(index..).unwrap_or_else(|| process::abort()))
            .unwrap_or_else(|_| process::abort()),
    )
}
