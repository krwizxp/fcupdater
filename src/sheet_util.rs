use crate::{Result, err, err_with_source};
const EXPONENT_BIAS: i32 = 1_023;
const EXPONENT_MASK: u64 = 0x07ff;
const FRACTION_BITS: u32 = 52;
const FRACTION_MASK: u64 = (1_u64 << FRACTION_BITS) - 1;
const I32_MIN_F64: f64 = -2_147_483_648.0;
const I32_MAX_F64: f64 = 2_147_483_647.0;
const SIGNIFICAND_HIDDEN_BIT: u64 = 1_u64 << FRACTION_BITS;
pub fn canon_header(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if !ch.is_whitespace() {
            out.push(ch);
        }
    }
    out
}
pub fn same_trimmed(left: &str, right: &str) -> bool {
    left.trim() == right.trim()
}
pub fn parse_i32_str(text: &str) -> Option<i32> {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "-" {
        return None;
    }
    let normalized_storage;
    let normalized = if trimmed.contains(',') {
        normalized_storage = trimmed.replace(',', "");
        normalized_storage.as_str()
    } else {
        trimmed
    };
    let value = normalized.parse::<f64>().ok()?;
    if !value.is_finite() {
        return None;
    }
    let rounded = value.round();
    if !(I32_MIN_F64..=I32_MAX_F64).contains(&rounded) {
        return None;
    }
    let bits = rounded.to_bits();
    let negative = (bits >> 63_u32) != 0_u64;
    let exponent_bits = u16::try_from((bits >> FRACTION_BITS) & EXPONENT_MASK).ok()?;
    if exponent_bits == 0 {
        return Some(0);
    }
    let exponent = i32::from(exponent_bits).checked_sub(EXPONENT_BIAS)?;
    if exponent < 0_i32 {
        return Some(0);
    }
    let significand = SIGNIFICAND_HIDDEN_BIT | (bits & FRACTION_MASK);
    let fraction_bits_i32 = i32::try_from(FRACTION_BITS).ok()?;
    let magnitude = if exponent >= fraction_bits_i32 {
        let shift = u32::try_from(exponent.checked_sub(fraction_bits_i32)?).ok()?;
        significand.checked_shl(shift)?
    } else {
        let shift = u32::try_from(fraction_bits_i32.checked_sub(exponent)?).ok()?;
        significand.checked_shr(shift)?
    };
    let signed = if negative {
        i64::try_from(magnitude).ok()?.checked_neg()?
    } else {
        i64::try_from(magnitude).ok()?
    };
    i32::try_from(signed).ok()
}
pub fn usize_to_u32(value: usize, context: &str) -> Result<u32> {
    u32::try_from(value).map_err(|source| {
        let out = format!("{context} 값이 너무 큽니다. (value={value})");
        err_with_source(out, source)
    })
}
pub fn shift_row(row: u32, increase: u32, decrease: u32) -> u32 {
    if increase > 0 {
        row.saturating_add(increase)
    } else {
        row.saturating_sub(decrease).max(1)
    }
}
pub fn add_row_offset(base_row: u32, offset: usize, context: &str) -> Result<u32> {
    let offset_u32 = usize_to_u32(offset, context)?;
    base_row.checked_add(offset_u32).ok_or_else(|| {
        err(format!(
            "{context} 계산 중 overflow가 발생했습니다. ({base_row} + {offset_u32})"
        ))
    })
}
