use super::{DECIMAL_SCALE, ScaledDecimal, ScaledSortKey};
use crate::diagnostic::{Result, append_fmt, err};
use crate::excel::writer::format_excel_ratio_into;
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
    format_excel_ratio_into(text, total.as_i128(), denominator_raw)?;
    Ok(true)
}
