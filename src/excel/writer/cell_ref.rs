use super::{CellReference, RangeTokenParts, RewrittenCellReference};
use crate::diagnostic::{Result, err, err_with_source};
const COL_NAME_BUF_LEN: usize = 8;
const COL_NAME_CHARS: [char; 26] = [
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
];
const _: () = assert!(COL_NAME_BUF_LEN >= 7, "COL_NAME_BUF_LEN too small");
pub(super) const MAX_A1_COL: u32 = 0x4000;
pub(super) const MAX_A1_ROW: u32 = 0x0010_0000;
pub(super) struct FormulaRewrite {
    end_index: usize,
    replacement: String,
}
struct LockPrefix<'reference> {
    locked: bool,
    rest: &'reference str,
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
    pub(super) const fn with_row(self, row: u32) -> Self {
        Self { row, ..self }
    }
}
pub(super) fn col_to_name(mut col: u32) -> Result<String> {
    if !(1..=MAX_A1_COL).contains(&col) {
        return Err(err(format!("Excel column 범위를 벗어났습니다: {col}")));
    }
    let mut rev = ['\0'; COL_NAME_BUF_LEN];
    let mut index = rev.len();
    while col > 0 {
        let base = col
            .checked_sub(1)
            .ok_or_else(|| err("Excel column 변환 중 underflow가 발생했습니다."))?;
        let rem = u8::try_from(base.rem_euclid(26))
            .map_err(|source| err_with_source("Excel column 나머지 변환 실패", source))?;
        let letter = COL_NAME_CHARS
            .get(usize::from(rem))
            .copied()
            .ok_or_else(|| err("Excel column 문자 범위가 손상되었습니다."))?;
        let next_index = index
            .checked_sub(1)
            .ok_or_else(|| err("Excel column buffer index 계산 실패"))?;
        index = next_index;
        let slot = rev
            .get_mut(index)
            .ok_or_else(|| err("Excel column buffer 범위가 손상되었습니다."))?;
        *slot = letter;
        col = base.div_euclid(26);
    }
    rev.get(index..)
        .map(|chars| chars.iter().collect())
        .ok_or_else(|| err("Excel column 결과 범위가 손상되었습니다."))
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
fn advance_index(index: &mut usize, context: &'static str) -> Result<()> {
    *index = index.checked_add(1).ok_or_else(|| err(context))?;
    Ok(())
}
pub(super) fn parse_ref_with_locks(reference: &str) -> Option<CellReference> {
    let col_prefix = strip_ref_lock_prefix(reference);
    let col_end = col_prefix
        .rest
        .find(|ch: char| !ch.is_ascii_alphabetic())
        .unwrap_or(col_prefix.rest.len());
    if col_end == 0 {
        return None;
    }
    let (col_s, after_col) = col_prefix.rest.split_at_checked(col_end)?;
    let row_prefix = strip_ref_lock_prefix(after_col);
    let row_part = row_prefix.rest;
    let row_end = row_part
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(row_part.len());
    if row_end == 0 || row_end != row_part.len() {
        return None;
    }
    let mut col = 0_u32;
    for ch in col_s.chars() {
        if !ch.is_ascii_alphabetic() {
            return None;
        }
        let upper = u8::try_from(u32::from(ch.to_ascii_uppercase())).ok()?;
        let one_based = upper.checked_sub(b'A')?.checked_add(1)?;
        col = col.checked_mul(26)?.checked_add(u32::from(one_based))?;
    }
    if !(1..=MAX_A1_COL).contains(&col) {
        return None;
    }
    let row = row_part.parse::<u32>().ok()?;
    Some(CellReference {
        col,
        col_locked: col_prefix.locked,
        row,
        row_locked: row_prefix.locked,
    })
}
pub(super) fn ref_with_locks(reference: CellReference) -> Result<String> {
    let col_name = col_to_name(reference.col)?;
    let col_prefix = if reference.col_locked { "$" } else { "" };
    let row_prefix = if reference.row_locked { "$" } else { "" };
    let row_text = reference.row.to_string();
    let Some(capacity) = col_prefix
        .len()
        .checked_add(col_name.len())
        .and_then(|value| value.checked_add(row_prefix.len()))
        .and_then(|value| value.checked_add(row_text.len()))
    else {
        return Err(err("Excel cell reference 용량 계산 실패"));
    };
    let mut out = String::new();
    out.try_reserve(capacity)
        .map_err(|source| err_with_source("Excel cell reference 메모리 확보 실패", source))?;
    out.push_str(col_prefix);
    out.push_str(&col_name);
    out.push_str(row_prefix);
    out.push_str(&row_text);
    Ok(out)
}
pub(super) fn rewrite_formula_cell_refs<F>(
    formula: &str,
    mut try_rewrite_cell_ref: F,
) -> Result<String>
where
    F: FnMut(&[char], usize) -> Result<Option<FormulaRewrite>>,
{
    let mut chars: Vec<char> = Vec::new();
    chars.try_reserve_exact(formula.len()).map_err(|source| {
        let formula_len = formula.len();
        err_with_source(
            format!("formula 문자 목록 메모리 확보 실패: {formula_len} chars"),
            source,
        )
    })?;
    chars.extend(formula.chars());
    let mut i = 0_usize;
    let capacity = formula.len();
    let mut out = String::new();
    out.try_reserve(capacity)
        .map_err(|source| err_with_source("formula rewrite buffer 메모리 확보 실패", source))?;
    let mut in_string = false;
    while let Some(&ch) = chars.get(i) {
        if ch == '"' {
            out.push(ch);
            if in_string {
                let escaped_quote_idx = i
                    .checked_add(1)
                    .ok_or_else(|| err("formula 문자열 quote index 계산에 실패했습니다."))?;
                if chars.get(escaped_quote_idx) == Some(&'"') {
                    out.push('"');
                    i = i
                        .checked_add(2)
                        .ok_or_else(|| err("formula 문자열 cursor 계산에 실패했습니다."))?;
                    continue;
                }
                in_string = false;
            } else {
                in_string = true;
            }
            advance_index(&mut i, "formula 문자열 cursor 계산에 실패했습니다.")?;
            continue;
        }
        if in_string {
            out.push(ch);
            advance_index(&mut i, "formula 문자열 cursor 계산에 실패했습니다.")?;
            continue;
        }
        if ch == '\'' {
            let mut quoted_end = None;
            let mut quoted_index = i
                .checked_add(1)
                .ok_or_else(|| err("formula quoted sheet cursor 계산에 실패했습니다."))?;
            while let Some(&quoted_char) = chars.get(quoted_index) {
                if quoted_char == '\'' {
                    let next_idx = quoted_index
                        .checked_add(1)
                        .ok_or_else(|| err("formula quoted sheet cursor 계산에 실패했습니다."))?;
                    if chars.get(next_idx) == Some(&'\'') {
                        quoted_index = quoted_index.checked_add(2).ok_or_else(|| {
                            err("formula quoted sheet escaped quote cursor 계산에 실패했습니다.")
                        })?;
                        continue;
                    }
                    if chars.get(next_idx) == Some(&'!') {
                        quoted_end = Some(quoted_index.checked_add(2).ok_or_else(|| {
                            err("formula quoted sheet 종료 cursor 계산에 실패했습니다.")
                        })?);
                    }
                    break;
                }
                advance_index(
                    &mut quoted_index,
                    "formula quoted sheet cursor 계산에 실패했습니다.",
                )?;
            }
            if let Some(next_idx) = quoted_end {
                let quoted = chars
                    .get(i..next_idx)
                    .ok_or_else(|| err("formula quoted sheet 범위가 손상되었습니다."))?;
                out.extend(quoted.iter().copied());
                i = next_idx;
                continue;
            }
        }
        if (ch == '$' || ch.is_ascii_alphabetic())
            && let Some(rewrite) = try_rewrite_cell_ref(&chars, i)?
        {
            out.push_str(&rewrite.replacement);
            i = rewrite.end_index;
            continue;
        }
        out.push(ch);
        advance_index(&mut i, "formula cursor 계산에 실패했습니다.")?;
    }
    Ok(out)
}
pub(super) fn shift_formula_index(value: u32, delta: i64, max: u32) -> Result<u32> {
    let shifted = i64::from(value).checked_add(delta).ok_or_else(|| {
        err(format!(
            "shared formula index 계산 overflow: {value} + {delta}"
        ))
    })?;
    if !(1..=i64::from(max)).contains(&shifted) {
        return Err(err(format!(
            "shared formula 상대참조 이동 범위를 벗어났습니다. ({value} + {delta}, max={max})"
        )));
    }
    u32::try_from(shifted)
        .map_err(|source| err_with_source("shared formula index 변환 실패", source))
}
pub(super) fn try_parse_and_rewrite_cell_ref<F>(
    chars: &[char],
    start: usize,
    mut rewrite_ref: F,
) -> Result<Option<FormulaRewrite>>
where
    F: FnMut(u32, u32, bool, bool) -> Result<RewrittenCellReference>,
{
    let mut index = start;
    let mut col_lock = false;
    if chars.get(index) == Some(&'$') {
        col_lock = true;
        advance_index(
            &mut index,
            "formula column lock cursor 계산에 실패했습니다.",
        )?;
    }
    let col_start = index;
    while chars.get(index).is_some_and(char::is_ascii_alphabetic) {
        advance_index(&mut index, "formula column cursor 계산에 실패했습니다.")?;
    }
    if index == col_start {
        return Ok(None);
    }
    let col_chars = chars
        .get(col_start..index)
        .ok_or_else(|| err("formula column reference 범위가 손상되었습니다."))?;
    if col_chars.len() > 3 {
        return Ok(None);
    }
    let mut base_col = 0_u32;
    for ch in col_chars {
        let upper = ch.to_ascii_uppercase();
        if !upper.is_ascii_alphabetic() {
            return Ok(None);
        }
        let letter_value = u32::from(upper)
            .checked_sub(u32::from('A'))
            .and_then(|value| value.checked_add(1));
        let Some(letter) = letter_value else {
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
    if chars.get(index) == Some(&'$') {
        row_lock = true;
        advance_index(&mut index, "formula row lock cursor 계산에 실패했습니다.")?;
    }
    let row_start = index;
    while chars.get(index).is_some_and(char::is_ascii_digit) {
        advance_index(&mut index, "formula row cursor 계산에 실패했습니다.")?;
    }
    if index == row_start {
        return Ok(None);
    }
    let previous = start.checked_sub(1).and_then(|idx| chars.get(idx)).copied();
    if previous.is_some_and(is_ref_neighbor_identifier) {
        return Ok(None);
    }
    let next = chars.get(index).copied();
    if next.is_some_and(|ch| is_ref_neighbor_identifier(ch) || matches!(ch, '!' | '\'' | '(' | '['))
    {
        return Ok(None);
    }
    let row_chars = chars
        .get(row_start..index)
        .ok_or_else(|| err("formula row reference 범위가 손상되었습니다."))?;
    let mut base_row = 0_u32;
    for ch in row_chars {
        let Some(digit) = u32::from(*ch).checked_sub(u32::from('0')) else {
            return Ok(None);
        };
        if digit > 9 {
            return Ok(None);
        }
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
    let rewritten = rewrite_ref(base_col, base_row, col_lock, row_lock)?;
    let replaced = ref_with_locks(CellReference {
        col: rewritten.col,
        col_locked: col_lock,
        row: rewritten.row,
        row_locked: row_lock,
    })?;
    Ok(Some(FormulaRewrite {
        end_index: index,
        replacement: replaced,
    }))
}
const fn is_ref_neighbor_identifier(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'
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
