use super::{CellReference, FormulaCellReference, FormulaRewrite, RangeTokenParts};
use crate::diagnostic::{Result, err, err_with_source};
use core::str;
const COL_NAME_BUF_LEN: usize = 8;
const _: () = assert!(COL_NAME_BUF_LEN >= 7, "COL_NAME_BUF_LEN too small");
pub(super) const MAX_A1_COL: u32 = 0x4000;
pub(super) const MAX_A1_ROW: u32 = 0x0010_0000;
pub(crate) struct AbsoluteColumnRangeRewriter {
    column: u32,
    first_row: u32,
    last_row: u32,
}
impl AbsoluteColumnRangeRewriter {
    pub(crate) fn rewrite(&self, formula: &str) -> Result<Option<String>> {
        rewrite_formula_cell_refs(formula, |candidate_formula, start| {
            let bytes = candidate_formula.as_bytes();
            if bytes.get(start) != Some(&b'$') {
                return Ok(None);
            }
            let Some(first_ref) = parse_formula_cell_ref(candidate_formula, start) else {
                return Ok(None);
            };
            if bytes.get(first_ref.end_index) != Some(&b':') {
                return Ok(None);
            }
            let second_start = first_ref.end_index.wrapping_add(1);
            let Some(second_ref) = parse_formula_cell_ref(candidate_formula, second_start) else {
                return Ok(None);
            };
            let first = first_ref.reference;
            let second = second_ref.reference;
            if first.col != self.column
                || second.col != self.column
                || !first.col_locked
                || !first.row_locked
                || !second.row_locked
            {
                return Ok(None);
            }
            if first.row == self.first_row && second.row == self.last_row {
                return Ok(None);
            }
            let updated_first = ref_with_locks(CellReference {
                col: self.column,
                col_locked: true,
                row: self.first_row,
                row_locked: true,
            })?;
            let updated_second = ref_with_locks(CellReference {
                col: self.column,
                col_locked: true,
                row: self.last_row,
                row_locked: true,
            })?;
            let capacity = updated_first
                .len()
                .wrapping_add(1)
                .wrapping_add(updated_second.len());
            let mut replacement = String::new();
            replacement.try_reserve_exact(capacity).map_err(|source| {
                err_with_source("formula 절대 열 범위 치환 메모리 확보 실패", source)
            })?;
            replacement.push_str(&updated_first);
            replacement.push(':');
            replacement.push_str(&updated_second);
            Ok(Some(FormulaRewrite {
                end_index: second_ref.end_index,
                replacement,
            }))
        })
    }
}
impl From<(u32, u32, u32)> for AbsoluteColumnRangeRewriter {
    fn from((column, first_row, last_row): (u32, u32, u32)) -> Self {
        Self {
            column,
            first_row,
            last_row,
        }
    }
}
impl CellReference {
    pub(super) const fn unlocked(col: u32, row: u32) -> Self {
        Self {
            col,
            col_locked: false,
            row,
            row_locked: false,
        }
    }
    const fn with_locks(self, col_locked: bool, row_locked: bool) -> Self {
        Self {
            col_locked,
            row_locked,
            ..self
        }
    }
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
pub(super) fn parse_range_token(token: &str) -> RangeTokenParts<'_> {
    match token.split_once(':') {
        Some((start_ref, end_ref)) => RangeTokenParts { end_ref, start_ref },
        None => RangeTokenParts {
            end_ref: token,
            start_ref: token,
        },
    }
}
const fn advance_index(index: &mut usize, step: usize) {
    *index = index.wrapping_add(step);
}
pub(in crate::excel) fn parse_ref_with_locks(reference: &str) -> Option<CellReference> {
    let (parsed, end) = parse_ref_prefix(reference)?;
    (end == reference.len()).then_some(parsed)
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
        CellReference::unlocked(col, row).with_locks(col_locked, row_locked),
        index,
    ))
}
pub(super) fn parse_formula_cell_ref(formula: &str, start: usize) -> Option<FormulaCellReference> {
    let tail = formula.get(start..)?;
    let (reference, consumed) = parse_ref_prefix(tail)?;
    let end_index = start.checked_add(consumed)?;
    let bytes = formula.as_bytes();
    let previous = start
        .checked_sub(1)
        .and_then(|index| bytes.get(index))
        .copied();
    if previous.is_some_and(is_ref_neighbor_identifier) {
        return None;
    }
    let next = bytes.get(end_index).copied();
    if next.is_some_and(|ch| {
        is_ref_neighbor_identifier(ch) || matches!(ch, b'!' | b'\'' | b'(' | b'[')
    }) {
        return None;
    }
    Some(FormulaCellReference {
        end_index,
        reference,
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
pub(super) fn rewrite_formula_cell_refs<F>(
    formula: &str,
    mut try_rewrite_cell_ref: F,
) -> Result<Option<String>>
where
    F: FnMut(&str, usize) -> Result<Option<FormulaRewrite>>,
{
    let mut copy_start = 0_usize;
    let mut out: Option<String> = None;
    let mut in_string = false;
    let mut chars = formula.char_indices().peekable();
    while let Some((i, ch)) = chars.next() {
        if ch == '"' {
            if in_string && chars.next_if(|&(_, next_char)| next_char == '"').is_some() {
                continue;
            }
            in_string = !in_string;
            continue;
        }
        if in_string {
            continue;
        }
        if ch == '\'' {
            let mut quoted_chars = chars.clone();
            let mut quoted_sheet_end = None;
            while let Some((_, quoted_char)) = quoted_chars.next() {
                if quoted_char == '\'' {
                    if quoted_chars
                        .next_if(|&(_, next_char)| next_char == '\'')
                        .is_some()
                    {
                        continue;
                    }
                    if quoted_chars
                        .next_if(|&(_, next_char)| next_char == '!')
                        .is_some()
                    {
                        quoted_sheet_end = Some(quoted_chars);
                    }
                    break;
                }
            }
            if let Some(rest) = quoted_sheet_end {
                chars = rest;
                continue;
            }
        }
        if ch == '[' {
            let mut depth = 1_usize;
            while let Some((_, bracket_char)) = chars.next() {
                if bracket_char == '\''
                    && chars
                        .next_if(|&(_, escaped)| matches!(escaped, '[' | ']' | '#' | '\'' | '@'))
                        .is_some()
                {
                    continue;
                }
                match bracket_char {
                    '[' => depth = depth.wrapping_add(1),
                    ']' => {
                        if depth == 1 {
                            break;
                        }
                        depth = depth.wrapping_sub(1);
                    }
                    _ => {}
                }
            }
            continue;
        }
        if (ch == '$' || ch.is_ascii_alphabetic())
            && let Some(rewrite) = try_rewrite_cell_ref(formula, i)?
        {
            let output = out.get_or_insert_default();
            if output.capacity() == 0 {
                output.try_reserve(formula.len()).map_err(|source| {
                    err_with_source("formula rewrite buffer 메모리 확보 실패", source)
                })?;
            }
            output.push_str(
                formula
                    .get(copy_start..i)
                    .ok_or_else(|| err("formula rewrite 복사 범위가 손상되었습니다."))?,
            );
            output.push_str(&rewrite.replacement);
            copy_start = rewrite.end_index;
            while chars.next_if(|&(index, _)| index < copy_start).is_some() {}
        }
    }
    if let Some(output) = out.as_mut() {
        let tail = formula
            .get(copy_start..)
            .ok_or_else(|| err("formula rewrite 나머지 범위가 손상되었습니다."))?;
        output.push_str(tail);
    }
    Ok(out)
}
const fn is_ref_neighbor_identifier(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'.'
}
