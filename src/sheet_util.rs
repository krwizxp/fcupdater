use crate::diagnostic::{Result, err, err_with_source};
use core::cmp::Ordering;
#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) enum DecimalParseMode {
    LooseRoundedI32,
    StrictFuelPrice,
}
pub(super) fn add_row_offset(base_row: u32, offset: usize, context: &str) -> Result<u32> {
    let offset_u32 = usize_to_u32(offset, context)?;
    base_row.checked_add(offset_u32).ok_or_else(|| {
        err(format!(
            "{context} 계산 중 overflow가 발생했습니다. ({base_row} + {offset_u32})"
        ))
    })
}
pub(super) fn parse_rounded_decimal(trimmed: &str, mode: DecimalParseMode) -> Option<i64> {
    let strict_fuel = mode == DecimalParseMode::StrictFuelPrice;
    let (negative, digits) = trimmed.strip_prefix('-').map_or_else(
        || (false, trimmed.trim_prefix('+')),
        |unsigned| (true, unsigned),
    );
    if digits.is_empty() {
        return None;
    }
    let (mantissa, exponent) =
        if let Some((mantissa, exponent_text)) = digits.split_once(['e', 'E']) {
            if strict_fuel || mantissa.contains(',') {
                return None;
            }
            (mantissa, exponent_text.parse::<i64>().ok()?)
        } else {
            (digits, 0_i64)
        };
    let integer_digits = mantissa
        .bytes()
        .take_while(|&byte| byte != b'.')
        .filter(u8::is_ascii_digit)
        .count();
    let decimal_position = i64::try_from(integer_digits).ok()?.checked_add(exponent)?;
    let mut whole = 0_i64;
    let mut position = 0_i64;
    let mut round_away_from_zero = false;
    let mut saw_digit = false;
    let mut seen_decimal = false;
    let mut seen_fraction = false;
    let mut group_len = 0_u32;
    let mut has_group = false;
    for byte in mantissa.bytes() {
        match byte {
            b',' if !seen_decimal => {
                if strict_fuel
                    && (group_len == 0
                        || has_group && group_len != 3
                        || !has_group && group_len > 3)
                {
                    return None;
                }
                has_group = true;
                group_len = 0;
            }
            b'.' if !seen_decimal => {
                if strict_fuel && (group_len == 0 || has_group && group_len != 3) {
                    return None;
                }
                seen_decimal = true;
            }
            b'0'..=b'9' => {
                saw_digit = true;
                if seen_decimal {
                    seen_fraction = true;
                } else {
                    group_len = group_len.strict_add(1);
                }
                let digit = i64::from(byte.strict_sub(b'0'));
                match position.cmp(&decimal_position) {
                    Ordering::Less => {
                        whole = whole.checked_mul(10)?.checked_add(digit)?;
                    }
                    Ordering::Equal => round_away_from_zero = digit >= 5_i64,
                    Ordering::Greater => {}
                }
                position = position.strict_add(1);
            }
            _ => return None,
        }
    }
    if !saw_digit
        || strict_fuel && has_group && group_len != 3
        || strict_fuel && seen_decimal && !seen_fraction
    {
        return None;
    }
    if whole != 0 && decimal_position > position {
        let power = u32::try_from(decimal_position.strict_sub(position)).ok()?;
        whole = 10_i64.checked_pow(power)?.checked_mul(whole)?;
    }
    let magnitude = whole.checked_add(i64::from(round_away_from_zero))?;
    if negative {
        magnitude.checked_neg()
    } else {
        Some(magnitude)
    }
}
pub(super) fn usize_to_u32(value: usize, context: &str) -> Result<u32> {
    u32::try_from(value).map_err(|source| {
        err_with_source(
            format!("{context} 값이 너무 큽니다. (value={value})"),
            source,
        )
    })
}
