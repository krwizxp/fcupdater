use core::time::Duration;
const DAYS_PER_100_YEARS_I64: i64 = 36_524;
const DAYS_PER_400_YEARS_I64: i64 = 146_097;
const DAYS_PER_4_YEARS_I64: i64 = 1_460;
const DAYS_PER_COMMON_YEAR_I64: i64 = 365;
const DAYS_UNTIL_UNIX_EPOCH_I64: i64 = 719_468;
pub const KST_OFFSET: Duration = Duration::from_hours(9);
const LEAP_YEAR_CENTURY_DIVISOR_I32: i32 = 100;
const LEAP_YEAR_DIVISOR_I32: i32 = 4;
const LEAP_YEAR_ERA_DIVISOR_I32: i32 = 400;
const MARCH_BASE_MONTH_OFFSET_I64: i64 = 3;
const MARCH_MONTH_THRESHOLD: u32 = 2;
const MONTH_TERM_DIVISOR_I64: i64 = 5;
const MONTH_TERM_MULTIPLIER_I64: i64 = 153;
const MONTH_TERM_OFFSET_I64: i64 = 2;
const PRE_MARCH_MONTH_OFFSET_I64: i64 = 9;
pub const SECS_PER_DAY_U64: u64 = 86_400;
pub struct KstDateCalculator {
    pub day_index: i32,
}
impl KstDateCalculator {
    pub fn civil_from_days(&self) -> Option<(i32, u32, u32)> {
        let shifted_days = i64::from(self.day_index).checked_add(DAYS_UNTIL_UNIX_EPOCH_I64)?;
        let era = shifted_days.div_euclid(DAYS_PER_400_YEARS_I64);
        let doe = shifted_days.rem_euclid(DAYS_PER_400_YEARS_I64);
        let yoe_after_first = doe.checked_sub(doe.checked_div(DAYS_PER_4_YEARS_I64)?)?;
        let yoe_after_second =
            yoe_after_first.checked_add(doe.checked_div(DAYS_PER_100_YEARS_I64)?)?;
        let yoe_numerator =
            yoe_after_second.checked_sub(doe.checked_div(DAYS_PER_400_YEARS_I64 - 1_i64)?)?;
        let yoe = yoe_numerator.checked_div(DAYS_PER_COMMON_YEAR_I64)?;
        let y = yoe.checked_add(era.checked_mul(i64::from(LEAP_YEAR_ERA_DIVISOR_I32))?)?;
        let year_days = DAYS_PER_COMMON_YEAR_I64.checked_mul(yoe)?;
        let leap_days = yoe.checked_div(i64::from(LEAP_YEAR_DIVISOR_I32))?;
        let skipped_centuries = yoe.checked_div(i64::from(LEAP_YEAR_CENTURY_DIVISOR_I32))?;
        let doy = doe.checked_sub(
            year_days
                .checked_add(leap_days)?
                .checked_sub(skipped_centuries)?,
        )?;
        let mp = MONTH_TERM_DIVISOR_I64
            .checked_mul(doy)?
            .checked_add(MONTH_TERM_OFFSET_I64)?
            .checked_div(MONTH_TERM_MULTIPLIER_I64)?;
        let month_term = MONTH_TERM_MULTIPLIER_I64
            .checked_mul(mp)?
            .checked_add(MONTH_TERM_OFFSET_I64)?
            .checked_div(MONTH_TERM_DIVISOR_I64)?;
        let day = u32::try_from(doy.checked_sub(month_term)?.checked_add(1_i64)?).ok()?;
        let month_i64 = if mp < 10_i64 {
            mp.checked_add(MARCH_BASE_MONTH_OFFSET_I64)?
        } else {
            mp.checked_sub(PRE_MARCH_MONTH_OFFSET_I64)?
        };
        let month = u32::try_from(month_i64).ok()?;
        let year_adjust = i64::from(month <= MARCH_MONTH_THRESHOLD);
        let year = i32::try_from(y.checked_add(year_adjust)?).ok()?;
        Some((year, month, day))
    }
}
