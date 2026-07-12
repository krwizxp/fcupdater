use super::{CellReference, RangeTokenParts, RewrittenCellReference};
use crate::{
    decimal::U128DecimalDigits,
    diagnostic::{AppError, Result, err, err_with_source},
};
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
pub(super) struct FormulaRewrite {
    end_index: usize,
    replacement: String,
}
struct FormulaBracket {
    end_index: usize,
}
struct LockPrefix<'reference> {
    locked: bool,
    rest: &'reference str,
}
impl AbsoluteColumnRangeRewriter {
    pub(crate) fn rewrite(&self, formula: &str) -> Result<Option<String>> {
        rewrite_formula_cell_refs(formula, |candidate_formula, start| {
            let bytes = candidate_formula.as_bytes();
            if bytes.get(start) != Some(&b'$') {
                return Ok(None);
            }
            let previous = start.checked_sub(1).and_then(|index| bytes.get(index));
            if previous.is_some_and(|byte| is_ref_neighbor_identifier(*byte)) {
                return Ok(None);
            }
            let Some(tail) = candidate_formula.get(start..) else {
                return Ok(None);
            };
            let Some(colon_relative) = tail.find(':') else {
                return Ok(None);
            };
            let Some(first_end) = start.checked_add(colon_relative) else {
                return Ok(None);
            };
            let Some(first_text) = candidate_formula.get(start..first_end) else {
                return Ok(None);
            };
            let Some(first) = parse_ref_with_locks(first_text) else {
                return Ok(None);
            };
            let Some(second_start) = first_end.checked_add(1) else {
                return Ok(None);
            };
            let Some(second_tail) = candidate_formula.get(second_start..) else {
                return Ok(None);
            };
            let second_len = second_tail
                .bytes()
                .take_while(|byte| byte.is_ascii_alphanumeric() || *byte == b'$')
                .count();
            let Some(second_end) = second_start.checked_add(second_len) else {
                return Ok(None);
            };
            let Some(second_text) = candidate_formula.get(second_start..second_end) else {
                return Ok(None);
            };
            let Some(second) = parse_ref_with_locks(second_text) else {
                return Ok(None);
            };
            if bytes
                .get(second_end)
                .is_some_and(|byte| is_ref_neighbor_identifier(*byte))
                || first.col != self.column
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
                .checked_add(1)
                .and_then(|value| value.checked_add(updated_second.len()))
                .ok_or_else(|| err("formula 절대 열 범위 치환 용량 계산 실패"))?;
            let mut replacement = String::new();
            replacement.try_reserve_exact(capacity).map_err(|source| {
                err_with_source("formula 절대 열 범위 치환 메모리 확보 실패", source)
            })?;
            replacement.push_str(&updated_first);
            replacement.push(':');
            replacement.push_str(&updated_second);
            Ok(Some(FormulaRewrite {
                end_index: second_end,
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
impl TryFrom<(&str, usize)> for FormulaBracket {
    type Error = AppError;
    fn try_from((formula, start): (&str, usize)) -> Result<Self> {
        let mut depth = 0_usize;
        let mut index = start;
        while let Some(tail) = formula.get(index..) {
            let Some(ch) = tail.chars().next() else {
                break;
            };
            let ch_len = ch.len_utf8();
            if ch == '\'' {
                let escaped_index = index.checked_add(ch_len).ok_or_else(|| {
                    err("formula structured reference cursor 계산에 실패했습니다.")
                })?;
                if formula
                    .get(escaped_index..)
                    .and_then(|escaped_tail| escaped_tail.chars().next())
                    .is_some_and(|escaped| matches!(escaped, '[' | ']' | '#' | '\'' | '@'))
                {
                    index = escaped_index.checked_add(1).ok_or_else(|| {
                        err("formula structured reference cursor 계산에 실패했습니다.")
                    })?;
                    continue;
                }
            }
            match ch {
                '[' => {
                    depth = depth.checked_add(1).ok_or_else(|| {
                        err("formula structured reference bracket 깊이 계산에 실패했습니다.")
                    })?;
                }
                ']' => {
                    depth = depth.checked_sub(1).ok_or_else(|| {
                        err("formula structured reference bracket 깊이가 손상되었습니다.")
                    })?;
                    if depth == 0 {
                        index = index.checked_add(ch_len).ok_or_else(|| {
                            err("formula structured reference 종료 계산에 실패했습니다.")
                        })?;
                        break;
                    }
                }
                _ => {}
            }
            advance_index(
                &mut index,
                ch_len,
                "formula structured reference cursor 계산에 실패했습니다.",
            )?;
        }
        formula
            .get(start..index)
            .ok_or_else(|| err("formula bracket 표현식 범위가 손상되었습니다."))?;
        Ok(Self { end_index: index })
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
pub(crate) fn col_to_name(col: u32) -> Result<String> {
    let mut buffer = [0_u8; COL_NAME_BUF_LEN];
    let text = col_name_text(col, &mut buffer)?;
    let mut out = String::new();
    out.try_reserve_exact(text.len())
        .map_err(|source| err_with_source("Excel column 이름 메모리 확보 실패", source))?;
    out.push_str(text);
    Ok(out)
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
fn advance_index(index: &mut usize, step: usize, context: &'static str) -> Result<()> {
    *index = index.checked_add(step).ok_or_else(|| err(context))?;
    Ok(())
}
pub(super) fn parse_ref_with_locks(reference: &str) -> Option<CellReference> {
    let col_prefix = strip_ref_lock_prefix(reference);
    let col_end = col_prefix
        .rest
        .bytes()
        .position(|byte| !byte.is_ascii_alphabetic())
        .unwrap_or(col_prefix.rest.len());
    if col_end == 0 {
        return None;
    }
    let (col_s, after_col) = col_prefix.rest.split_at_checked(col_end)?;
    let row_prefix = strip_ref_lock_prefix(after_col);
    let row_part = row_prefix.rest;
    let row_end = row_part
        .bytes()
        .position(|byte| !byte.is_ascii_digit())
        .unwrap_or(row_part.len());
    if row_end == 0 || row_end != row_part.len() {
        return None;
    }
    let mut col = 0_u32;
    for byte in col_s.bytes() {
        let upper = byte.to_ascii_uppercase();
        let one_based = upper.checked_sub(b'A')?.checked_add(1)?;
        col = col.checked_mul(26)?.checked_add(u32::from(one_based))?;
    }
    if !(1..=MAX_A1_COL).contains(&col) {
        return None;
    }
    let mut row = 0_u32;
    for byte in row_part.bytes() {
        let digit = u32::from(byte.wrapping_sub(b'0'));
        row = row.checked_mul(10)?.checked_add(digit)?;
    }
    if !(1..=MAX_A1_ROW).contains(&row) {
        return None;
    }
    Some(CellReference::unlocked(col, row).with_locks(col_prefix.locked, row_prefix.locked))
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
    let row_digits = U128DecimalDigits::new(u128::from(reference.row))
        .ok_or_else(|| err("Excel row decimal 변환 실패"))?;
    let row_text = row_digits
        .as_str()
        .ok_or_else(|| err("Excel row decimal 문자열 변환 실패"))?;
    let Some(capacity) = col_prefix
        .len()
        .checked_add(col_name.len())
        .and_then(|value| value.checked_add(row_prefix.len()))
        .and_then(|value| value.checked_add(row_text.len()))
    else {
        return Err(err("Excel cell reference 용량 계산 실패"));
    };
    let mut out = String::new();
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source("Excel cell reference 메모리 확보 실패", source))?;
    out.push_str(col_prefix);
    out.push_str(col_name);
    out.push_str(row_prefix);
    out.push_str(row_text);
    Ok(out)
}
pub(super) fn with_unlocked_ref_text<R>(
    col: u32,
    row: u32,
    use_text: impl FnOnce(&str, &str) -> R,
) -> Result<R> {
    let mut col_buffer = [0_u8; COL_NAME_BUF_LEN];
    let col_name = col_name_text(col, &mut col_buffer)?;
    if !(1..=MAX_A1_ROW).contains(&row) {
        return Err(err(format!("Excel row 범위를 벗어났습니다: {row}")));
    }
    let row_digits = U128DecimalDigits::new(u128::from(row))
        .ok_or_else(|| err("Excel row decimal 변환 실패"))?;
    let row_text = row_digits
        .as_str()
        .ok_or_else(|| err("Excel row decimal 문자열 변환 실패"))?;
    Ok(use_text(col_name, row_text))
}
pub(super) fn rewrite_formula_cell_refs<F>(
    formula: &str,
    mut try_rewrite_cell_ref: F,
) -> Result<Option<String>>
where
    F: FnMut(&str, usize) -> Result<Option<FormulaRewrite>>,
{
    let mut i = 0_usize;
    let mut copy_start = 0_usize;
    let mut out: Option<String> = None;
    let mut in_string = false;
    while let Some(tail) = formula.get(i..) {
        let Some(ch) = tail.chars().next() else {
            break;
        };
        let ch_len = ch.len_utf8();
        if ch == '"' {
            if in_string {
                let escaped_quote_idx = i.checked_add(ch_len).ok_or_else(|| err("quote 오류"))?;
                if formula.as_bytes().get(escaped_quote_idx) == Some(&b'"') {
                    let escaped_quote_end = escaped_quote_idx
                        .checked_add(1)
                        .ok_or_else(|| err("quote 오류"))?;
                    i = escaped_quote_end;
                    continue;
                }
                in_string = false;
            } else {
                in_string = true;
            }
            advance_index(&mut i, ch_len, "formula 문자열 cursor 계산에 실패했습니다.")?;
            continue;
        }
        if in_string {
            advance_index(&mut i, ch_len, "formula 문자열 cursor 계산에 실패했습니다.")?;
            continue;
        }
        if ch == '\'' {
            let mut quoted_end = None;
            let mut quoted_index = i
                .checked_add(ch_len)
                .ok_or_else(|| err("formula quoted sheet cursor 계산에 실패했습니다."))?;
            while let Some(quoted_tail) = formula.get(quoted_index..) {
                let Some(quoted_char) = quoted_tail.chars().next() else {
                    break;
                };
                if quoted_char == '\'' {
                    let next_idx = quoted_index
                        .checked_add(quoted_char.len_utf8())
                        .ok_or_else(|| err("formula quoted sheet cursor 계산에 실패했습니다."))?;
                    if formula.as_bytes().get(next_idx) == Some(&b'\'') {
                        quoted_index = next_idx.checked_add(1).ok_or_else(|| {
                            err("formula quoted sheet escaped quote cursor 계산에 실패했습니다.")
                        })?;
                        continue;
                    }
                    if formula.as_bytes().get(next_idx) == Some(&b'!') {
                        quoted_end = Some(next_idx.checked_add(1).ok_or_else(|| {
                            err("formula quoted sheet 종료 cursor 계산에 실패했습니다.")
                        })?);
                    }
                    break;
                }
                advance_index(
                    &mut quoted_index,
                    quoted_char.len_utf8(),
                    "formula quoted sheet cursor 계산에 실패했습니다.",
                )?;
            }
            if let Some(next_idx) = quoted_end {
                i = next_idx;
                continue;
            }
        }
        if ch == '[' {
            let bracket = FormulaBracket::try_from((formula, i))?;
            i = bracket.end_index;
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
            i = rewrite.end_index;
            copy_start = i;
            continue;
        }
        advance_index(&mut i, ch_len, "formula cursor 계산에 실패했습니다.")?;
    }
    if let Some(output) = out.as_mut() {
        let tail = formula
            .get(copy_start..)
            .ok_or_else(|| err("formula rewrite 나머지 범위가 손상되었습니다."))?;
        output.push_str(tail);
    }
    Ok(out)
}
pub(super) fn shift_formula_index(value: u32, delta: i32, max: u32) -> Result<u32> {
    let Some(shifted) = value.checked_add_signed(delta) else {
        return Err(err(format!(
            "shared formula 상대참조 이동 범위를 벗어났습니다. ({value} + {delta}, max={max})"
        )));
    };
    if shifted == 0 || shifted > max {
        return Err(err(format!(
            "shared formula 상대참조 이동 범위를 벗어났습니다. ({value} + {delta}, max={max})"
        )));
    }
    Ok(shifted)
}
pub(super) fn try_parse_and_rewrite_cell_ref<F>(
    formula: &str,
    start: usize,
    mut rewrite_ref: F,
) -> Result<Option<FormulaRewrite>>
where
    F: FnMut(u32, u32, bool, bool) -> Result<RewrittenCellReference>,
{
    let mut index = start;
    let mut col_lock = false;
    let bytes = formula.as_bytes();
    if bytes.get(index) == Some(&b'$') {
        col_lock = true;
        advance_index(&mut index, 1, "formula 열 lock cursor 오류")?;
    }
    let col_start = index;
    while bytes.get(index).is_some_and(u8::is_ascii_alphabetic) {
        advance_index(&mut index, 1, "formula column cursor 계산에 실패했습니다.")?;
    }
    if index == col_start {
        return Ok(None);
    }
    let col_chars = bytes
        .get(col_start..index)
        .ok_or_else(|| err("formula column reference 범위가 손상되었습니다."))?;
    if col_chars.len() > 3 {
        return Ok(None);
    }
    let mut base_col = 0_u32;
    for &ch in col_chars {
        let upper = ch.to_ascii_uppercase();
        let Some(letter) = u32::from(upper)
            .checked_sub(u32::from('A'))
            .and_then(|value| value.checked_add(1))
        else {
            return Ok(None);
        };
        let Some(next_col) = base_col
            .checked_mul(26)
            .and_then(|value| value.checked_add(letter))
        else {
            return Ok(None);
        };
        base_col = next_col;
    }
    if !(1..=MAX_A1_COL).contains(&base_col) {
        return Ok(None);
    }
    let mut row_lock = false;
    if bytes.get(index) == Some(&b'$') {
        row_lock = true;
        advance_index(
            &mut index,
            1,
            "formula row lock cursor 계산에 실패했습니다.",
        )?;
    }
    let row_start = index;
    while bytes.get(index).is_some_and(u8::is_ascii_digit) {
        advance_index(&mut index, 1, "formula row cursor 계산에 실패했습니다.")?;
    }
    if index == row_start {
        return Ok(None);
    }
    let previous = start.checked_sub(1).and_then(|idx| bytes.get(idx)).copied();
    if previous.is_some_and(is_ref_neighbor_identifier) {
        return Ok(None);
    }
    let next = bytes.get(index).copied();
    if next.is_some_and(|ch| {
        is_ref_neighbor_identifier(ch) || matches!(ch, b'!' | b'\'' | b'(' | b'[')
    }) {
        return Ok(None);
    }
    let row_chars = bytes
        .get(row_start..index)
        .ok_or_else(|| err("formula row reference 범위가 손상되었습니다."))?;
    let mut base_row = 0_u32;
    for &ch in row_chars {
        let Some(digit) = u32::from(ch).checked_sub(u32::from(b'0')) else {
            return Ok(None);
        };
        let Some(next_row) = base_row
            .checked_mul(10)
            .and_then(|value| value.checked_add(digit))
        else {
            return Ok(None);
        };
        base_row = next_row;
    }
    if !(1..=MAX_A1_ROW).contains(&base_row) {
        return Ok(None);
    }
    let parsed = CellReference::unlocked(base_col, base_row).with_locks(col_lock, row_lock);
    let rewritten = rewrite_ref(base_col, base_row, col_lock, row_lock)?;
    if rewritten.col == base_col && rewritten.row == base_row {
        return Ok(None);
    }
    let replaced = ref_with_locks(CellReference {
        col: rewritten.col,
        row: rewritten.row,
        ..parsed
    })?;
    Ok(Some(FormulaRewrite {
        end_index: index,
        replacement: replaced,
    }))
}
const fn is_ref_neighbor_identifier(ch: u8) -> bool {
    ch.is_ascii_alphanumeric() || ch == b'_' || ch == b'.'
}
fn strip_ref_lock_prefix(reference: &str) -> LockPrefix<'_> {
    reference.strip_prefix('$').map_or(
        LockPrefix {
            locked: false,
            rest: reference,
        },
        |tail| LockPrefix {
            locked: true,
            rest: tail,
        },
    )
}
