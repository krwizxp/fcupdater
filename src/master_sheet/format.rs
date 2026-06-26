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
pub(super) fn format_scaled_value(value: i128, scale: i128) -> Result<String> {
    let sign = if value < 0 { "-" } else { "" };
    let abs = value.unsigned_abs();
    let scale_abs = scale.unsigned_abs();
    if scale_abs == 0 {
        return Ok(value.to_string());
    }
    let whole = abs.div_euclid(scale_abs);
    let frac = abs.rem_euclid(scale_abs);
    let whole_text = whole.to_string();
    if frac == 0 {
        if sign.is_empty() {
            return Ok(whole_text);
        }
        let mut out = String::new();
        let capacity = sign
            .len()
            .checked_add(whole_text.len())
            .ok_or_else(|| err("소수 표시 문자열 용량 계산 실패"))?;
        out.try_reserve(capacity)
            .map_err(|source| err_with_source("소수 표시 문자열 메모리 확보 실패", source))?;
        out.push_str(sign);
        out.push_str(&whole_text);
        return Ok(out);
    }
    let mut frac_text = frac.to_string();
    let width = usize::try_from(scale_abs.ilog10())
        .map_err(|source| err_with_source("소수 표시 폭 변환 실패", source))?;
    let zero_padding = width
        .checked_sub(frac_text.len())
        .ok_or_else(|| err("소수 표시 0 padding 계산 실패"))?;
    let trailing_zeros = frac_text
        .as_bytes()
        .iter()
        .rev()
        .take_while(|&&byte| byte == b'0')
        .count();
    let padded_len = zero_padding
        .checked_add(frac_text.len())
        .ok_or_else(|| err("소수 표시 길이 계산 실패"))?;
    let trimmed_padded_len = padded_len
        .checked_sub(trailing_zeros)
        .ok_or_else(|| err("소수 표시 trailing zero 계산 실패"))?;
    let kept_padding = zero_padding.min(trimmed_padded_len);
    let kept_digits = trimmed_padded_len
        .checked_sub(zero_padding)
        .ok_or_else(|| err("소수 표시 digit 길이 계산 실패"))?;
    frac_text.truncate(kept_digits);
    let mut out = String::new();
    let capacity = sign
        .len()
        .checked_add(whole_text.len())
        .and_then(|len| len.checked_add(1))
        .and_then(|len| len.checked_add(kept_padding))
        .and_then(|len| len.checked_add(frac_text.len()))
        .ok_or_else(|| err("소수 표시 문자열 용량 계산 실패"))?;
    out.try_reserve(capacity)
        .map_err(|source| err_with_source("소수 표시 문자열 메모리 확보 실패", source))?;
    out.push_str(sign);
    out.push_str(&whole_text);
    out.push('.');
    for _ in 0..kept_padding {
        out.push('0');
    }
    out.push_str(&frac_text);
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
    let sign = if total.as_i128() < 0 { "-" } else { "" };
    let abs = total.as_i128().unsigned_abs();
    let denominator = denominator_raw.unsigned_abs();
    let whole = abs.div_euclid(denominator);
    let mut remainder = abs.rem_euclid(denominator);
    let whole_text = whole.to_string();
    if remainder == 0 {
        if sign.is_empty() {
            return Ok(Some(whole_text));
        }
        let mut out = String::new();
        let capacity = sign
            .len()
            .checked_add(whole_text.len())
            .ok_or_else(|| err("단가 표시 문자열 용량 계산 실패"))?;
        out.try_reserve(capacity)
            .map_err(|source| err_with_source("단가 표시 문자열 메모리 확보 실패", source))?;
        out.push_str(sign);
        out.push_str(&whole_text);
        return Ok(Some(out));
    }
    let mut frac_text = String::new();
    frac_text
        .try_reserve(15)
        .map_err(|source| err_with_source("단가 소수부 메모리 확보 실패", source))?;
    while frac_text.len() < 15 && remainder != 0 {
        remainder = remainder
            .checked_mul(10)
            .ok_or_else(|| err("단가 소수부 계산 중 overflow가 발생했습니다."))?;
        let digit = remainder.div_euclid(denominator);
        let digit_u8 = u8::try_from(digit)
            .map_err(|source| err_with_source("단가 소수부 digit 변환 실패", source))?;
        let digit_byte = b'0'
            .checked_add(digit_u8)
            .ok_or_else(|| err("단가 소수부 digit 계산 중 overflow가 발생했습니다."))?;
        frac_text.push(char::from(digit_byte));
        remainder = remainder.rem_euclid(denominator);
    }
    let trimmed_frac_len = frac_text.trim_end_matches('0').len();
    frac_text.truncate(trimmed_frac_len);
    let capacity = sign
        .len()
        .checked_add(whole_text.len())
        .and_then(|value| value.checked_add(1))
        .and_then(|value| value.checked_add(frac_text.len()))
        .ok_or_else(|| err("단가 표시 문자열 용량 계산 실패"))?;
    let mut out = String::new();
    out.try_reserve(capacity)
        .map_err(|source| err_with_source("단가 표시 문자열 메모리 확보 실패", source))?;
    out.push_str(sign);
    out.push_str(&whole_text);
    out.push('.');
    out.push_str(&frac_text);
    Ok(Some(out))
}
pub(super) fn missing_sort_target_row_error(row_num: u32) -> AppError {
    err(format!("정렬 대상 행을 찾지 못했습니다: {row_num}"))
}
