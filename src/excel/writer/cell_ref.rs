use crate::diagnostic::{Result, fmt_err, or_abort};
pub(super) const MAX_A1_ROW: u32 = 0x0010_0000;
pub(super) const SINGLE_LETTER_COLUMNS: &str = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
pub(super) fn col_name_into(mut col: u32, col_buffer: &mut [u8; 3]) -> Result<&str> {
    (1..=0x4000_u32)
        .contains(&col)
        .ok_or_else(|| fmt_err!("Excel column 범위를 벗어났습니다: {col}"))?;
    let single_index = col.strict_sub(1) as usize;
    if let Some(single) = SINGLE_LETTER_COLUMNS.get(single_index..single_index.strict_add(1)) {
        return Ok(single);
    }
    let mut index = col_buffer.len();
    while col > 0 {
        let base = col.strict_sub(1);
        index = index.strict_sub(1);
        *or_abort(col_buffer.get_mut(index)) =
            b'A'.strict_add(base.rem_euclid(26).to_le_bytes()[0]);
        col = base.div_euclid(26);
    }
    Ok(or_abort(
        str::from_utf8(or_abort(col_buffer.get(index..))).ok(),
    ))
}
pub(super) fn parse_ref_with_locks(reference: &str) -> Option<(u32, u32, bool)> {
    let bytes = reference.as_bytes();
    let mut index = usize::from(bytes.first() == Some(&b'$'));
    let col_locked = index != 0;
    let (col_start, mut col) = (index, 0_u32);
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
    if index == col_start || !(1..=0x4000_u32).contains(&col) {
        return None;
    }
    let row_locked = bytes.get(index) == Some(&b'$');
    index = index.strict_add(usize::from(row_locked));
    let row = bytes.get(index..)?.iter().try_fold(0_u32, |row, &ch| {
        if !ch.is_ascii_digit() {
            return None;
        }
        let value = row
            .strict_mul(10)
            .strict_add(u32::from(ch.strict_sub(b'0')));
        (value <= MAX_A1_ROW).then_some(value)
    })?;
    (row != 0).then_some((col, row, !col_locked && !row_locked))
}
