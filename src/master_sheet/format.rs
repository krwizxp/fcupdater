use super::{DECIMAL_SCALE, DECIMAL_SCALE_SQUARED, ScaledDecimal, ScaledSortKey};
use crate::err;
use alloc::string::String;
use core::error::Error;
pub(super) fn format_fuel_price_text(label: &str, total: ScaledSortKey) -> String {
    let rounded = total
        .checked_add(DECIMAL_SCALE_SQUARED.div_euclid(2))
        .unwrap_or(total)
        .div_euclid(DECIMAL_SCALE_SQUARED);
    let raw = rounded.to_string();
    let (sign, digits) = split_negative_prefix(raw.as_str(), "", "-");
    let groups = digits.len().saturating_sub(1).div_euclid(3);
    let mut amount = String::with_capacity(
        sign.len()
            .saturating_add(digits.len())
            .saturating_add(groups),
    );
    amount.push_str(sign);
    for (index, ch) in digits.chars().enumerate() {
        if index != 0 && digits.len().saturating_sub(index).is_multiple_of(3) {
            amount.push(',');
        }
        amount.push(ch);
    }
    format!("{label} {amount}원")
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
    if qty == 0 {
        return None;
    }
    let denominator_raw = i128::from(qty).checked_mul(i128::from(DECIMAL_SCALE))?;
    let sign = if total < 0 { "-" } else { "" };
    let abs = total.unsigned_abs();
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
        frac_text.push(char::from(b'0'.saturating_add(digit_u8)));
        remainder = remainder.rem_euclid(denominator);
    }
    let trimmed_frac_len = frac_text.trim_end_matches('0').len();
    frac_text.truncate(trimmed_frac_len);
    Some(format!("{sign}{whole_text}.{frac_text}"))
}
pub(super) fn missing_sort_target_row_error(row_num: u32) -> Box<dyn Error + Send + Sync> {
    err(format!("정렬 대상 행을 찾지 못했습니다: {row_num}"))
}
