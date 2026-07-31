use super::CellReference;
use crate::diagnostic::{Result, err, err_with_source};
use core::{fmt::Write as _, str};
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
fn parse_ref_prefix(reference: &str) -> Option<(CellReference, usize)> {
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
    let mut output = String::new();
    push_ref_with_locks(&mut output, reference)?;
    Ok(output)
}
fn push_ref_with_locks(output: &mut String, reference: CellReference) -> Result<()> {
    let mut col_buffer = [0_u8; COL_NAME_BUF_LEN];
    let col_name = col_name_text(reference.col, &mut col_buffer)?;
    if !(1..=MAX_A1_ROW).contains(&reference.row) {
        return Err(err(format!(
            "Excel row 범위를 벗어났습니다: {}",
            reference.row
        )));
    }
    output
        .try_reserve_exact(17)
        .map_err(|source| err_with_source("Excel cell reference 메모리 확보 실패", source))?;
    if reference.col_locked {
        output.push('$');
    }
    output.push_str(col_name);
    if reference.row_locked {
        output.push('$');
    }
    write!(output, "{}", reference.row)
        .map_err(|source| err_with_source("Excel cell reference 출력 실패", source))
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
    let bytes = formula.as_bytes();
    let mut copy_start = 0_usize;
    let mut index = 0_usize;
    let mut output = None::<String>;
    while let Some(&byte) = bytes.get(index) {
        if byte == b'"' {
            index = index.strict_add(1);
            while let Some(&string_byte) = bytes.get(index) {
                index = index.strict_add(1);
                if string_byte != b'"' {
                    continue;
                }
                if bytes.get(index) == Some(&b'"') {
                    index = index.strict_add(1);
                    continue;
                }
                break;
            }
            continue;
        }
        if byte == b'\'' {
            let mut quoted_index = index.strict_add(1);
            let mut after_sheet = None;
            while let Some(&quoted_byte) = bytes.get(quoted_index) {
                quoted_index = quoted_index.strict_add(1);
                if quoted_byte != b'\'' {
                    continue;
                }
                if bytes.get(quoted_index) == Some(&b'\'') {
                    quoted_index = quoted_index.strict_add(1);
                    continue;
                }
                if bytes.get(quoted_index) == Some(&b'!') {
                    after_sheet = Some(quoted_index.strict_add(1));
                }
                break;
            }
            if let Some(after_sheet_index) = after_sheet {
                index = after_sheet_index;
                continue;
            }
        }
        if byte == b'[' {
            let mut depth = 1_usize;
            index = index.strict_add(1);
            while let Some(&bracket_byte) = bytes.get(index) {
                index = index.strict_add(1);
                if bracket_byte == b'\''
                    && bytes.get(index).is_some_and(|escaped| {
                        matches!(*escaped, b'[' | b']' | b'#' | b'\'' | b'@')
                    })
                {
                    index = index.strict_add(1);
                    continue;
                }
                match bracket_byte {
                    b'[' => depth = depth.strict_add(1),
                    b']' if depth == 1 => break,
                    b']' => depth = depth.strict_sub(1),
                    _ => {}
                }
            }
            continue;
        }
        if !matches!(byte, b'$' | b'A'..=b'Z' | b'a'..=b'z') {
            index = index.strict_add(1);
            continue;
        }
        let reference_start = index;
        let Some(candidate) = formula.get(reference_start..) else {
            index = index.strict_add(1);
            continue;
        };
        let Some((reference, reference_len)) = parse_ref_prefix(candidate) else {
            index = index.strict_add(1);
            continue;
        };
        let reference_end = reference_start
            .checked_add(reference_len)
            .ok_or_else(|| err("formula reference 끝 계산 실패"))?;
        let previous = reference_start
            .checked_sub(1)
            .and_then(|position| bytes.get(position));
        let next = bytes.get(reference_end);
        if previous.is_some_and(|neighbor| is_reference_neighbor(*neighbor))
            || next.is_some_and(|neighbor| {
                is_reference_neighbor(*neighbor) || matches!(*neighbor, b'!' | b'\'' | b'(' | b'[')
            })
        {
            index = index.strict_add(1);
            continue;
        }
        index = reference_end;
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
        let out = output.get_or_insert_with(String::new);
        if out.capacity() == 0 {
            out.try_reserve(formula.len())
                .map_err(|source| err_with_source("formula rewrite 메모리 확보 실패", source))?;
        }
        out.push_str(
            formula
                .get(copy_start..reference_start)
                .ok_or_else(|| err("formula rewrite 복사 범위가 손상되었습니다."))?,
        );
        push_ref_with_locks(
            out,
            CellReference {
                col: shifted_col,
                row: shifted_row,
                ..reference
            },
        )?;
        copy_start = reference_end;
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
