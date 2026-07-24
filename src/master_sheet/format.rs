use super::{DECIMAL_SCALE, ScaledDecimal, ScaledSortKey};
use crate::diagnostic::{Result, err};
const UNIT_PRICE_MAX_FRAC_DIGITS: usize = 15;
pub(super) fn format_scaled_value(value: i128, scale: i128) -> String {
    let sign = if value != 0 && (value < 0) != (scale < 0) {
        "-"
    } else {
        ""
    };
    let abs = value.unsigned_abs();
    let scale_abs = scale.unsigned_abs();
    if scale_abs == 0 {
        return format!("{sign}{abs}");
    }
    let whole = abs.div_euclid(scale_abs);
    let frac = abs.rem_euclid(scale_abs);
    if frac == 0 {
        return format!("{sign}{whole}");
    }
    let width = usize::from(scale_abs.ilog10().to_le_bytes()[0]);
    let mut text = format!("{sign}{whole}.{frac:0width$}");
    text.truncate(text.trim_end_matches('0').len());
    text
}
pub(super) fn format_unit_price_text(
    total: ScaledSortKey,
    qty: ScaledDecimal,
) -> Result<Option<String>> {
    if qty == ScaledDecimal::ZERO {
        return Ok(None);
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
    if remainder == 0 {
        return Ok(Some(format!("{sign}{whole}")));
    }
    let mut text = format!("{sign}{whole}");
    let integer_end = text.len();
    text.push('.');
    for _ in 0..UNIT_PRICE_MAX_FRAC_DIGITS {
        if remainder == 0 {
            break;
        }
        remainder = remainder.wrapping_mul(10);
        let digit = remainder.div_euclid(denominator).to_le_bytes()[0];
        text.push(char::from(b'0'.wrapping_add(digit)));
        remainder = remainder.rem_euclid(denominator);
    }
    text.truncate(text.trim_end_matches('0').len());
    if text.ends_with('.') {
        text.truncate(integer_end);
        if whole == 0 && text.starts_with('-') {
            text.replace_range(..1, "");
        }
    }
    Ok(Some(text))
}
