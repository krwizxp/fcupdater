pub fn round_f64_to_i32(value: f64) -> Option<i32> {
    const I32_MIN_F64: f64 = -2_147_483_648.0;
    const I32_MAX_F64: f64 = 2_147_483_647.0;
    if !value.is_finite() {
        return None;
    }
    let rounded = value.round();
    if !(I32_MIN_F64..=I32_MAX_F64).contains(&rounded) {
        return None;
    }
    format!("{rounded:.0}").parse::<i32>().ok()
}
