use super::{DECIMAL_SCALE, DECIMAL_SCALE_SQUARED, ScaledDecimal, ScaledSortKey};
use crate::diagnostic::{AppError, Result, err, err_with_source};
use alloc::string::String;
pub(super) fn format_fuel_price_text(label: &str, total: ScaledSortKey) -> Result<String> {
    let half_scale = ScaledSortKey(DECIMAL_SCALE_SQUARED.as_i128().div_euclid(2));
    let rounded = total
        .checked_add(half_scale)
        .ok_or_else(|| err("연료비 반올림 계산 중 overflow가 발생했습니다."))?
        .as_i128()
        .div_euclid(DECIMAL_SCALE_SQUARED.as_i128());
    let raw = rounded.to_string();
    let (sign, digits) = split_negative_prefix(raw.as_str(), "", "-");
    let groups = digits
        .len()
        .checked_sub(1)
        .map_or(0, |remaining| remaining.div_euclid(3));
    let amount_capacity = sign
        .len()
        .checked_add(digits.len())
        .and_then(|value| value.checked_add(groups))
        .ok_or_else(|| err("연료비 금액 문자열 용량 계산 실패"))?;
    let mut amount = String::new();
    amount
        .try_reserve(amount_capacity)
        .map_err(|source| err_with_source("연료비 표시 문자열 메모리 확보 실패", source))?;
    amount.push_str(sign);
    for (index, ch) in digits.chars().enumerate() {
        if index != 0
            && digits
                .len()
                .checked_sub(index)
                .is_some_and(|remaining| remaining.is_multiple_of(3))
        {
            amount.push(',');
        }
        amount.push(ch);
    }
    let output_capacity = label
        .len()
        .checked_add(" ".len())
        .and_then(|value| value.checked_add(amount.len()))
        .and_then(|value| value.checked_add("원".len()))
        .ok_or_else(|| err("연료비 표시 문자열 용량 계산 실패"))?;
    let mut out = String::new();
    out.try_reserve(output_capacity)
        .map_err(|source| err_with_source("연료비 표시 문자열 메모리 확보 실패", source))?;
    out.push_str(label);
    out.push(' ');
    out.push_str(&amount);
    out.push('원');
    Ok(out)
}
pub(super) fn split_negative_prefix<T>(value: &str, positive: T, negative: T) -> (T, &str) {
    value
        .strip_prefix('-')
        .map_or((positive, value), |rest| (negative, rest))
}
pub(super) fn format_scaled_value(value: i128, scale: i128) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let abs = value.unsigned_abs();
    let scale_abs = scale.unsigned_abs();
    if scale_abs == 0 {
        return value.to_string();
    }
    let whole = abs.div_euclid(scale_abs);
    let frac = abs.rem_euclid(scale_abs);
    let whole_text = whole.to_string();
    if frac == 0 {
        return format!("{sign}{whole_text}");
    }
    let mut frac_text = frac.to_string();
    let width = usize::try_from(scale_abs.ilog10()).unwrap_or_default();
    while frac_text.len() < width {
        frac_text.insert(0, '0');
    }
    let trimmed_frac_len = frac_text.trim_end_matches('0').len();
    frac_text.truncate(trimmed_frac_len);
    format!("{sign}{whole_text}.{frac_text}")
}
pub(super) fn format_unit_price_text(total: ScaledSortKey, qty: ScaledDecimal) -> Option<String> {
    if qty.is_zero() {
        return None;
    }
    let denominator_raw = qty.as_i128().checked_mul(DECIMAL_SCALE.as_i128())?;
    let sign = if total.as_i128() < 0 { "-" } else { "" };
    let abs = total.as_i128().unsigned_abs();
    let denominator = denominator_raw.unsigned_abs();
    let whole = abs.div_euclid(denominator);
    let mut remainder = abs.rem_euclid(denominator);
    let whole_text = whole.to_string();
    if remainder == 0 {
        return Some(format!("{sign}{whole_text}"));
    }
    let mut frac_text = String::new();
    frac_text.try_reserve(15).ok()?;
    while frac_text.len() < 15 && remainder != 0 {
        remainder = remainder.checked_mul(10)?;
        let digit = remainder.div_euclid(denominator);
        let digit_u8 = u8::try_from(digit).ok()?;
        frac_text.push(char::from(b'0'.checked_add(digit_u8)?));
        remainder = remainder.rem_euclid(denominator);
    }
    let trimmed_frac_len = frac_text.trim_end_matches('0').len();
    frac_text.truncate(trimmed_frac_len);
    Some(format!("{sign}{whole_text}.{frac_text}"))
}
pub(super) fn missing_sort_target_row_error(row_num: u32) -> AppError {
    err(format!("정렬 대상 행을 찾지 못했습니다: {row_num}"))
}
