use crate::decimal::U128DecimalDigits;
use core::time::Duration;
pub(crate) const STALE_TEMP_ENTRY_AGE: Duration = Duration::from_hours(24);
pub(crate) const TEMP_ENTRY_NAME_CAPACITY: usize = 128;
pub(crate) const TEMP_ENTRY_RESERVATION_ATTEMPTS: u32 = 1024;
pub(crate) fn temp_entry_age_nanos(file_name: &str, prefix: &str, now_nanos: u128) -> Option<u128> {
    let suffix = file_name.strip_prefix(prefix)?;
    let mut fragments = suffix.split('_');
    let (Some(pid), Some(created_at), Some(sequence), None) = (
        fragments.next(),
        fragments.next(),
        fragments.next(),
        fragments.next(),
    ) else {
        return None;
    };
    pid.parse::<u32>().ok()?;
    let created_at_nanos = created_at.parse::<u128>().ok()?;
    sequence.parse::<u32>().ok()?;
    now_nanos.checked_sub(created_at_nanos)
}
pub(crate) fn write_temp_entry_name(
    out: &mut String,
    prefix: &str,
    pid: u32,
    nanos: u128,
    sequence: u32,
) -> Option<()> {
    out.clear();
    out.push_str(prefix);
    U128DecimalDigits::new(u128::from(pid))?.push_to(out)?;
    out.push('_');
    U128DecimalDigits::new(nanos)?.push_to(out)?;
    out.push('_');
    U128DecimalDigits::new(u128::from(sequence))?.push_to(out)
}
