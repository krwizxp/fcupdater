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
fn parse_ref_prefix(reference: &str) -> Option<(CellReference, usize)> {
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
    Some((
        CellReference {
            col,
            col_locked,
            row,
            row_locked,
        },
        index,
    ))
}
pub(super) fn parse_ref_with_locks(reference: &str) -> Option<CellReference> {
    let (parsed, end) = parse_ref_prefix(reference)?;
    (end == reference.len()).then_some(parsed)
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
pub(super) fn shift_formula(
    formula: &str,
    col_delta: i32,
    row_delta: i32,
) -> Result<Option<String>> {
    let mut copy_start = 0_usize;
    let mut output = None::<String>;
    let mut in_string = false;
    let mut chars = formula.char_indices().peekable();
    while let Some((index, character)) = chars.next() {
        if character == '"' {
            if in_string
                && chars
                    .next_if(|&(_, next_character)| next_character == '"')
                    .is_some()
            {
                continue;
            }
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        if character == '\'' {
            let mut quoted = chars.clone();
            let mut after_sheet = None;
            while let Some((_, quoted_character)) = quoted.next() {
                if quoted_character != '\'' {
                    continue;
                }
                if quoted.next_if(|&(_, escaped)| escaped == '\'').is_some() {
                    continue;
                }
                if quoted.next_if(|&(_, separator)| separator == '!').is_some() {
                    after_sheet = Some(quoted);
                }
                break;
            }
            if let Some(rest) = after_sheet {
                chars = rest;
                continue;
            }
        }
        if character == '[' {
            let mut depth = 1_usize;
            while let Some((_, bracket_character)) = chars.next() {
                if bracket_character == '\''
                    && chars
                        .next_if(|&(_, escaped)| matches!(escaped, '[' | ']' | '#' | '\'' | '@'))
                        .is_some()
                {
                    continue;
                }
                match bracket_character {
                    '[' => depth = depth.saturating_add(1),
                    ']' if depth == 1 => break,
                    ']' => depth = depth.saturating_sub(1),
                    _ => {}
                }
            }
            continue;
        }
        if !matches!(character, '$' | 'A'..='Z' | 'a'..='z') {
            continue;
        }
        let Some(candidate) = formula.get(index..) else {
            continue;
        };
        let Some((reference, reference_len)) = parse_ref_prefix(candidate) else {
            continue;
        };
        let reference_end = index
            .checked_add(reference_len)
            .ok_or_else(|| err("formula reference 끝 계산 실패"))?;
        let bytes = formula.as_bytes();
        let previous = index
            .checked_sub(1)
            .and_then(|position| bytes.get(position));
        let next = bytes.get(reference_end);
        if previous.is_some_and(|byte| is_reference_neighbor(*byte))
            || next.is_some_and(|byte| {
                is_reference_neighbor(*byte) || matches!(*byte, b'!' | b'\'' | b'(' | b'[')
            })
        {
            continue;
        }
        let shifted_col = if reference.col_locked {
            reference.col
        } else {
            shift_reference_index(reference.col, col_delta, MAX_A1_COL)?
        };
        let shifted_row = if reference.row_locked {
            reference.row
        } else {
            shift_reference_index(reference.row, row_delta, MAX_A1_ROW)?
        };
        if (shifted_col, shifted_row) == (reference.col, reference.row) {
            continue;
        }
        let replacement = ref_with_locks(CellReference {
            col: shifted_col,
            row: shifted_row,
            ..reference
        })?;
        let out = output.get_or_insert_with(String::new);
        if out.capacity() == 0 {
            out.try_reserve(formula.len())
                .map_err(|source| err_with_source("formula rewrite 메모리 확보 실패", source))?;
        }
        out.push_str(
            formula
                .get(copy_start..index)
                .ok_or_else(|| err("formula rewrite 복사 범위가 손상되었습니다."))?,
        );
        out.push_str(&replacement);
        copy_start = reference_end;
        while chars
            .next_if(|&(next_index, _)| next_index < copy_start)
            .is_some()
        {}
    }
    if let Some(out) = output.as_mut() {
        out.push_str(
            formula
                .get(copy_start..)
                .ok_or_else(|| err("formula rewrite 나머지 범위가 손상되었습니다."))?,
        );
    }
    Ok(output)
}
fn shift_reference_index(value: u32, delta: i32, max: u32) -> Result<u32> {
    let shifted = value
        .checked_add_signed(delta)
        .filter(|shifted| (1..=max).contains(shifted));
    shifted.ok_or_else(|| {
        err(format!(
            "shared formula 상대참조 이동 범위를 벗어났습니다: {value} + {delta}"
        ))
    })
}
const fn is_reference_neighbor(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.')
}
