use crate::{Result, err, err_with_source};
const COL_NAME_BUF_LEN: usize = 8;
const COL_NAME_CHARS: [char; 26] = [
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
];
const _: () = assert!(COL_NAME_BUF_LEN >= 7, "COL_NAME_BUF_LEN too small");
pub(super) const MAX_A1_COL: u32 = 0x4000;
pub(super) const MAX_A1_ROW: u32 = 0x0010_0000;
pub(super) fn col_to_name(mut col: u32) -> String {
    if col == 0 {
        return "A".into();
    }
    let mut rev = ['\0'; COL_NAME_BUF_LEN];
    let mut index = rev.len();
    while col > 0 {
        let base = col.saturating_sub(1);
        let Ok(rem) = u8::try_from(base.rem_euclid(26)) else {
            return String::new();
        };
        let Some(letter) = COL_NAME_CHARS.get(usize::from(rem)).copied() else {
            return String::new();
        };
        let Some(next_index) = index.checked_sub(1) else {
            return String::new();
        };
        index = next_index;
        let Some(slot) = rev.get_mut(index) else {
            return String::new();
        };
        *slot = letter;
        col = base.div_euclid(26);
    }
    rev.get(index..)
        .map(|chars| chars.iter().collect())
        .unwrap_or_default()
}
pub(super) fn parse_range_token(token: &str) -> (&str, &str) {
    token.split_once(':').unwrap_or((token, token))
}
pub(super) fn parse_ref_with_locks(reference: &str) -> Option<(u32, u32, bool, bool)> {
    let (col_lock, after_col_lock) = strip_ref_lock_prefix(reference);
    let col_end = after_col_lock
        .find(|ch: char| !ch.is_ascii_alphabetic())
        .unwrap_or(after_col_lock.len());
    if col_end == 0 {
        return None;
    }
    let (col_s, after_col) = after_col_lock.split_at_checked(col_end)?;
    let (row_lock, row_part) = strip_ref_lock_prefix(after_col);
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
        col = col
            .checked_mul(26)?
            .checked_add(u32::from(upper.saturating_sub(b'A')).saturating_add(1))?;
    }
    if !(1..=MAX_A1_COL).contains(&col) {
        return None;
    }
    let row = row_part.parse::<u32>().ok()?;
    Some((col, row, col_lock, row_lock))
}
pub(super) fn ref_with_locks(col: u32, row: u32, col_lock: bool, row_lock: bool) -> String {
    let col_name = col_to_name(col);
    let col_prefix = if col_lock { "$" } else { "" };
    let row_prefix = if row_lock { "$" } else { "" };
    let row_text = row.to_string();
    let capacity = col_prefix
        .len()
        .saturating_add(col_name.len())
        .saturating_add(row_prefix.len())
        .saturating_add(row_text.len());
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("{col_prefix}{col_name}{row_prefix}{row_text}");
    }
    out.push_str(col_prefix);
    out.push_str(&col_name);
    out.push_str(row_prefix);
    out.push_str(&row_text);
    out
}
pub(super) fn rewrite_formula_cell_refs<F>(
    formula: &str,
    mut try_rewrite_cell_ref: F,
) -> Result<String>
where
    F: FnMut(&[char], usize) -> Result<Option<(usize, String)>>,
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
            i = i.saturating_add(1);
            continue;
        }
        if in_string {
            out.push(ch);
            i = i.saturating_add(1);
            continue;
        }
        if ch == '\'' {
            let mut quoted_end = None;
            let mut quoted_index = i.saturating_add(1);
            while let Some(&quoted_char) = chars.get(quoted_index) {
                if quoted_char == '\'' {
                    let next_idx = quoted_index.saturating_add(1);
                    if chars.get(next_idx) == Some(&'\'') {
                        quoted_index = quoted_index.saturating_add(2);
                        continue;
                    }
                    if chars.get(next_idx) == Some(&'!') {
                        quoted_end = Some(quoted_index.saturating_add(2));
                    }
                    break;
                }
                quoted_index = quoted_index.saturating_add(1);
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
            && let Some((end_idx, replaced)) = try_rewrite_cell_ref(&chars, i)?
        {
            out.push_str(&replaced);
            i = end_idx;
            continue;
        }
        out.push(ch);
        i = i.saturating_add(1);
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
        .map_err(|source| err(format!("shared formula index 변환 실패: {source}")))
}
pub(super) fn try_parse_and_rewrite_cell_ref<F>(
    chars: &[char],
    start: usize,
    mut rewrite_ref: F,
) -> Result<Option<(usize, String)>>
where
    F: FnMut(u32, u32, bool, bool) -> Result<(u32, u32)>,
{
    let mut index = start;
    let mut col_lock = false;
    if chars.get(index) == Some(&'$') {
        col_lock = true;
        index = index.saturating_add(1);
    }
    let col_start = index;
    while chars.get(index).is_some_and(char::is_ascii_alphabetic) {
        index = index.saturating_add(1);
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
        index = index.saturating_add(1);
    }
    let row_start = index;
    while chars.get(index).is_some_and(char::is_ascii_digit) {
        index = index.saturating_add(1);
    }
    if index == row_start {
        return Ok(None);
    }
    let previous = start
        .checked_sub(1)
        .and_then(|previous_index| chars.get(previous_index))
        .copied();
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
    let (new_col, new_row) = rewrite_ref(base_col, base_row, col_lock, row_lock)?;
    let replaced = ref_with_locks(new_col, new_row, col_lock, row_lock);
    Ok(Some((index, replaced)))
}
const fn is_ref_neighbor_identifier(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'
}
fn strip_ref_lock_prefix(reference: &str) -> (bool, &str) {
    reference
        .strip_prefix('$')
        .map_or((false, reference), |tail| (true, tail))
}
