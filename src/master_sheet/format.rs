use core::fmt::NumBuffer;
pub(super) fn format_scaled_value_into(text: &mut String, value: i128, scale: i128) {
    text.clear();
    let abs = value.unsigned_abs();
    let scale_abs = scale.unsigned_abs();
    if value < 0 {
        text.push('-');
    }
    let mut buffer = NumBuffer::new();
    let whole = abs.div_euclid(scale_abs);
    text.push_str(whole.format_into(&mut buffer));
    let frac = abs.rem_euclid(scale_abs);
    if frac == 0 {
        return;
    }
    text.push('.');
    let width = scale_abs.ilog10() as usize;
    let frac_text = frac.format_into(&mut buffer);
    for _ in frac_text.len()..width {
        text.push('0');
    }
    text.push_str(frac_text);
    text.truncate(text.trim_end_matches('0').len());
}
