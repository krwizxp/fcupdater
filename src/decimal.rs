use core::str;
pub(crate) const U128_DECIMAL_MAX_LEN: usize = 39;
pub(crate) struct U128DecimalDigits {
    bytes: [u8; U128_DECIMAL_MAX_LEN],
    start: usize,
}
impl U128DecimalDigits {
    pub(crate) fn as_bytes(&self) -> Option<&[u8]> {
        self.bytes.get(self.start..)
    }
    pub(crate) fn as_str(&self) -> Option<&str> {
        str::from_utf8(self.as_bytes()?).ok()
    }
    pub(crate) fn new(mut value: u128) -> Option<Self> {
        const DIGITS: &[u8; 10] = b"0123456789";
        let mut bytes = [0_u8; U128_DECIMAL_MAX_LEN];
        let mut start = bytes.len();
        loop {
            let digit = usize::try_from(value.rem_euclid(10)).ok()?;
            start = start.checked_sub(1)?;
            *bytes.get_mut(start)? = *DIGITS.get(digit)?;
            value = value.div_euclid(10);
            if value == 0 {
                break;
            }
        }
        Some(Self { bytes, start })
    }
    pub(crate) fn push_to(&self, out: &mut String) -> Option<()> {
        out.push_str(self.as_str()?);
        Some(())
    }
}
