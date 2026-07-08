use crate::diagnostic::{Result, err, err_with_source};
pub fn parse_i32_str(text: &str) -> Option<i32> {
    let trimmed = text.trim();
    if trimmed.is_empty() {
        return None;
    }
    let (negative, digits) = trimmed.strip_prefix('-').map_or_else(
        || {
            trimmed
                .strip_prefix('+')
                .map_or((false, trimmed), |digits| (false, digits))
        },
        |digits| (true, digits),
    );
    if digits.is_empty() {
        return None;
    }
    let mut whole = 0_i64;
    let mut round_away_from_zero = false;
    let mut saw_digit = false;
    let mut saw_fraction_digit = false;
    let mut seen_decimal = false;
    for byte in digits.bytes() {
        match byte {
            b',' if !seen_decimal => {}
            b'.' if !seen_decimal => {
                seen_decimal = true;
            }
            b'0'..=b'9' => {
                saw_digit = true;
                let digit = i64::from(byte.wrapping_sub(b'0'));
                if seen_decimal {
                    if !saw_fraction_digit {
                        round_away_from_zero = digit >= 5_i64;
                        saw_fraction_digit = true;
                    }
                } else {
                    whole = whole.checked_mul(10)?.checked_add(digit)?;
                }
            }
            _ => return None,
        }
    }
    if !saw_digit {
        return None;
    }
    let magnitude = if round_away_from_zero {
        whole.checked_add(1)?
    } else {
        whole
    };
    let signed = if negative {
        magnitude.checked_neg()?
    } else {
        magnitude
    };
    i32::try_from(signed).ok()
}
pub fn usize_to_u32(value: usize, context: &str) -> Result<u32> {
    u32::try_from(value).map_err(|source| {
        let out = format!("{context} 값이 너무 큽니다. (value={value})");
        err_with_source(out, source)
    })
}
pub fn add_row_offset(base_row: u32, offset: usize, context: &str) -> Result<u32> {
    let offset_u32 = usize_to_u32(offset, context)?;
    base_row.checked_add(offset_u32).ok_or_else(|| {
        err(format!(
            "{context} 계산 중 overflow가 발생했습니다. ({base_row} + {offset_u32})"
        ))
    })
}
