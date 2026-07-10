use super::{DECIMAL_SCALE, DECIMAL_SCALE_SQUARED, ScaledDecimal, ScaledSortKey};
use crate::{
    decimal::U128DecimalDigits,
    diagnostic::{AppError, Result, err, err_with_source},
};
use core::str;
const UNIT_PRICE_MAX_FRAC_DIGITS: usize = 15;
fn decimal_digits(value: u128) -> Result<U128DecimalDigits> {
    U128DecimalDigits::new(value).ok_or_else(|| err("decimal digit buffer 상태가 손상되었습니다."))
}
fn decimal_digit_bytes(digits: &U128DecimalDigits) -> Result<&[u8]> {
    digits
        .as_bytes()
        .ok_or_else(|| err("decimal digit buffer 상태가 손상되었습니다."))
}
fn push_decimal_digits(digits: &U128DecimalDigits, out: &mut String) -> Result<()> {
    digits
        .push_to(out)
        .ok_or_else(|| err("decimal digit UTF-8 변환 실패"))
}
pub(super) fn format_fuel_price_text(label: &str, total: ScaledSortKey) -> Result<String> {
    let half_scale = ScaledSortKey(DECIMAL_SCALE_SQUARED.as_i128().div_euclid(2));
    let rounded = total
        .checked_add(half_scale)
        .ok_or_else(|| err("연료비 반올림 계산 중 overflow가 발생했습니다."))?
        .as_i128()
        .div_euclid(DECIMAL_SCALE_SQUARED.as_i128());
    let sign = if rounded < 0 { "-" } else { "" };
    let digits = decimal_digits(rounded.unsigned_abs())?;
    let digit_bytes = decimal_digit_bytes(&digits)?;
    let groups = digit_bytes.len().saturating_sub(1).div_euclid(3);
    let amount_capacity = sign
        .len()
        .checked_add(digit_bytes.len())
        .and_then(|value| value.checked_add(groups))
        .ok_or_else(|| err("연료비 금액 문자열 용량 계산 실패"))?;
    let output_capacity = label
        .len()
        .checked_add(" ".len())
        .and_then(|value| value.checked_add(amount_capacity))
        .and_then(|value| value.checked_add("원".len()))
        .ok_or_else(|| err("연료비 표시 문자열 용량 계산 실패"))?;
    let mut out = String::new();
    out.try_reserve_exact(output_capacity)
        .map_err(|source| err_with_source("연료비 표시 문자열 메모리 확보 실패", source))?;
    out.push_str(label);
    out.push(' ');
    out.push_str(sign);
    for (index, &byte) in digit_bytes.iter().enumerate() {
        if index != 0
            && digit_bytes
                .len()
                .checked_sub(index)
                .is_some_and(|remaining| remaining.is_multiple_of(3))
        {
            out.push(',');
        }
        out.push(char::from(byte));
    }
    out.push('원');
    Ok(out)
}
pub(super) fn format_scaled_value(value: i128, scale: i128) -> Result<String> {
    let sign = if value != 0 && (value < 0) != (scale < 0) {
        "-"
    } else {
        ""
    };
    let abs = value.unsigned_abs();
    let scale_abs = scale.unsigned_abs();
    if scale_abs == 0 {
        let integer_digits = decimal_digits(abs)?;
        let mut out = String::new();
        out.try_reserve_exact(
            sign.len()
                .checked_add(decimal_digit_bytes(&integer_digits)?.len())
                .ok_or_else(|| err("정수 표시 문자열 용량 계산 실패"))?,
        )
        .map_err(|source| err_with_source("정수 표시 문자열 메모리 확보 실패", source))?;
        out.push_str(sign);
        push_decimal_digits(&integer_digits, &mut out)?;
        return Ok(out);
    }
    let whole = abs.div_euclid(scale_abs);
    let frac = abs.rem_euclid(scale_abs);
    let whole_digits = decimal_digits(whole)?;
    let whole_bytes = decimal_digit_bytes(&whole_digits)?;
    if frac == 0 {
        let mut out = String::new();
        let capacity = sign
            .len()
            .checked_add(whole_bytes.len())
            .ok_or_else(|| err("소수 표시 문자열 용량 계산 실패"))?;
        out.try_reserve_exact(capacity)
            .map_err(|source| err_with_source("소수 표시 문자열 메모리 확보 실패", source))?;
        out.push_str(sign);
        push_decimal_digits(&whole_digits, &mut out)?;
        return Ok(out);
    }
    let frac_digits = decimal_digits(frac)?;
    let frac_bytes = decimal_digit_bytes(&frac_digits)?;
    let width = usize::try_from(scale_abs.ilog10())
        .map_err(|source| err_with_source("소수 표시 폭 변환 실패", source))?;
    let zero_padding = width
        .checked_sub(frac_bytes.len())
        .ok_or_else(|| err("소수 표시 0 padding 계산 실패"))?;
    let trailing_zeros = frac_bytes
        .iter()
        .rev()
        .take_while(|&&byte| byte == b'0')
        .count();
    let padded_len = zero_padding
        .checked_add(frac_bytes.len())
        .ok_or_else(|| err("소수 표시 길이 계산 실패"))?;
    let trimmed_padded_len = padded_len
        .checked_sub(trailing_zeros)
        .ok_or_else(|| err("소수 표시 trailing zero 계산 실패"))?;
    let kept_padding = zero_padding.min(trimmed_padded_len);
    let kept_digits = trimmed_padded_len
        .checked_sub(zero_padding)
        .ok_or_else(|| err("소수 표시 digit 길이 계산 실패"))?;
    let kept_frac_bytes = frac_bytes
        .get(..kept_digits)
        .ok_or_else(|| err("소수 표시 digit 범위 오류"))?;
    let mut out = String::new();
    let capacity = sign
        .len()
        .checked_add(whole_bytes.len())
        .and_then(|len| len.checked_add(1))
        .and_then(|len| len.checked_add(kept_padding))
        .and_then(|len| len.checked_add(kept_frac_bytes.len()))
        .ok_or_else(|| err("소수 표시 문자열 용량 계산 실패"))?;
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source("소수 표시 문자열 메모리 확보 실패", source))?;
    out.push_str(sign);
    push_decimal_digits(&whole_digits, &mut out)?;
    out.push('.');
    for _ in 0..kept_padding {
        out.push('0');
    }
    let kept_frac_text = str::from_utf8(kept_frac_bytes)
        .map_err(|source| err_with_source("소수 표시 digit UTF-8 변환 실패", source))?;
    out.push_str(kept_frac_text);
    Ok(out)
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
    let whole_digits = decimal_digits(whole)?;
    let whole_bytes = decimal_digit_bytes(&whole_digits)?;
    if remainder == 0 {
        let mut out = String::new();
        let capacity = sign
            .len()
            .checked_add(whole_bytes.len())
            .ok_or_else(|| err("단가 표시 문자열 용량 계산 실패"))?;
        out.try_reserve_exact(capacity)
            .map_err(|source| err_with_source("단가 표시 문자열 메모리 확보 실패", source))?;
        out.push_str(sign);
        push_decimal_digits(&whole_digits, &mut out)?;
        return Ok(Some(out));
    }
    let mut frac_bytes = [0_u8; UNIT_PRICE_MAX_FRAC_DIGITS];
    let mut frac_len = 0_usize;
    while frac_len < UNIT_PRICE_MAX_FRAC_DIGITS && remainder != 0 {
        remainder = remainder
            .checked_mul(10)
            .ok_or_else(|| err("단가 소수부 계산 중 overflow가 발생했습니다."))?;
        let digit = remainder.div_euclid(denominator);
        let digit_u8 = u8::try_from(digit)
            .map_err(|source| err_with_source("단가 소수부 digit 변환 실패", source))?;
        let digit_byte = b'0'
            .checked_add(digit_u8)
            .ok_or_else(|| err("단가 소수부 digit 계산 중 overflow가 발생했습니다."))?;
        let Some(slot) = frac_bytes.get_mut(frac_len) else {
            return Err(err("단가 소수부 buffer 범위 오류"));
        };
        *slot = digit_byte;
        frac_len = frac_len
            .checked_add(1)
            .ok_or_else(|| err("단가 소수부 길이 계산 실패"))?;
        remainder = remainder.rem_euclid(denominator);
    }
    let frac_slice = frac_bytes
        .get(..frac_len)
        .ok_or_else(|| err("단가 소수부 범위 오류"))?;
    let trimmed_frac_len = frac_slice
        .iter()
        .rposition(|byte| *byte != b'0')
        .map_or(0, |index| index.saturating_add(1));
    let trimmed_frac = frac_slice
        .get(..trimmed_frac_len)
        .ok_or_else(|| err("단가 소수부 trim 범위 오류"))?;
    let visible_sign = if trimmed_frac.is_empty() && whole == 0 {
        ""
    } else {
        sign
    };
    let decimal_separator_len = usize::from(!trimmed_frac.is_empty());
    let capacity = visible_sign
        .len()
        .checked_add(whole_bytes.len())
        .and_then(|value| value.checked_add(decimal_separator_len))
        .and_then(|value| value.checked_add(trimmed_frac.len()))
        .ok_or_else(|| err("단가 표시 문자열 용량 계산 실패"))?;
    let mut out = String::new();
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source("단가 표시 문자열 메모리 확보 실패", source))?;
    out.push_str(visible_sign);
    push_decimal_digits(&whole_digits, &mut out)?;
    if !trimmed_frac.is_empty() {
        out.push('.');
        let trimmed_frac_text = str::from_utf8(trimmed_frac)
            .map_err(|source| err_with_source("단가 소수부 UTF-8 변환 실패", source))?;
        out.push_str(trimmed_frac_text);
    }
    Ok(Some(out))
}
pub(super) fn missing_sort_target_row_error(row_num: u32) -> AppError {
    err(format!("정렬 대상 행을 찾지 못했습니다: {row_num}"))
}
