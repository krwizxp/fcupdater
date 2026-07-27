use super::{DECIMAL_SCALE, ScaledDecimal, ScaledSortKey};
use crate::diagnostic::{Result, append_fmt, err};
const UNIT_PRICE_MAX_FRAC_DIGITS: usize = 15;
pub(super) fn format_scaled_value_into(text: &mut String, value: i128, scale: i128) {
    text.clear();
    let sign = if value != 0 && (value < 0) != (scale < 0) {
        "-"
    } else {
        ""
    };
    let abs = value.unsigned_abs();
    let scale_abs = scale.unsigned_abs();
    if scale_abs == 0 {
        append_fmt(text, format_args!("{sign}{abs}"));
        return;
    }
    let whole = abs.div_euclid(scale_abs);
    let frac = abs.rem_euclid(scale_abs);
    if frac == 0 {
        append_fmt(text, format_args!("{sign}{whole}"));
        return;
    }
    let width = usize::from(scale_abs.ilog10().to_le_bytes()[0]);
    append_fmt(text, format_args!("{sign}{whole}.{frac:0width$}"));
    text.truncate(text.trim_end_matches('0').len());
}
pub(super) fn format_unit_price_text_into(
    text: &mut String,
    total: ScaledSortKey,
    qty: ScaledDecimal,
) -> Result<bool> {
    text.clear();
    if qty == ScaledDecimal::ZERO {
        return Ok(false);
    }
    let denominator_raw = qty
        .as_i128()
        .checked_mul(DECIMAL_SCALE.as_i128())
        .ok_or_else(|| err("단가 분모 계산 중 overflow가 발생했습니다."))?;
    let numerator = total.as_i128();
    let sign = if numerator != 0 && (numerator < 0) != (denominator_raw < 0) {
        "-"
    } else {
        ""
    };
    let abs = numerator.unsigned_abs();
    let denominator = denominator_raw.unsigned_abs();
    let whole = abs.div_euclid(denominator);
    let mut remainder = abs.rem_euclid(denominator);
    append_fmt(text, format_args!("{sign}{whole}"));
    if remainder == 0 {
        return Ok(true);
    }
    let integer_end = text.len();
    text.push('.');
    for _ in 0..UNIT_PRICE_MAX_FRAC_DIGITS {
        if remainder == 0 {
            break;
        }
        remainder = remainder
            .checked_mul(10)
            .ok_or_else(|| err("단가 소수부 계산 중 overflow가 발생했습니다."))?;
        let digit = remainder.div_euclid(denominator).to_le_bytes()[0];
        text.push(char::from(b'0'.strict_add(digit)));
        remainder = remainder.rem_euclid(denominator);
    }
    text.truncate(text.trim_end_matches('0').len());
    if text.ends_with('.') {
        text.truncate(integer_end);
        if whole == 0 && text.starts_with('-') {
            text.replace_range(..1, "");
        }
    }
    Ok(true)
}
