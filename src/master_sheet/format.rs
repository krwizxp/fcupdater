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
    let frac_text = format!("{frac:0width$}");
    format!("{sign}{whole}.{}", frac_text.trim_end_matches('0'))
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
    let mut fraction = String::new();
    while fraction.len() < UNIT_PRICE_MAX_FRAC_DIGITS && remainder != 0 {
        remainder = remainder.wrapping_mul(10);
        let digit = remainder.div_euclid(denominator).to_le_bytes()[0];
        fraction.push(char::from(b'0'.wrapping_add(digit)));
        remainder = remainder.rem_euclid(denominator);
    }
    let trimmed_fraction = fraction.trim_end_matches('0');
    let visible_sign = if trimmed_fraction.is_empty() && whole == 0 {
        ""
    } else {
        sign
    };
    if trimmed_fraction.is_empty() {
        Ok(Some(format!("{visible_sign}{whole}")))
    } else {
        Ok(Some(format!("{visible_sign}{whole}.{trimmed_fraction}")))
    }
}
