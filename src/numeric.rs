pub fn round_f64_to_i32(value: f64) -> Option<i32> {
    const EXPONENT_BIAS: i32 = 1_023;
    const EXPONENT_MASK: u64 = 0x07ff;
    const FRACTION_BITS: u32 = 52;
    const FRACTION_MASK: u64 = (1_u64 << FRACTION_BITS) - 1;
    const I32_MIN_F64: f64 = -2_147_483_648.0;
    const I32_MAX_F64: f64 = 2_147_483_647.0;
    const SIGNIFICAND_HIDDEN_BIT: u64 = 1_u64 << FRACTION_BITS;
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
