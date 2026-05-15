#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct NaturalKey(Vec<NaturalPart>);
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum NaturalPart {
    Number {
        digits_len: usize,
        normalized: String,
        raw_len: usize,
    },
    Text(String),
}
impl From<&str> for NaturalKey {
    fn from(text: &str) -> Self {
        let mut out = Vec::with_capacity(text.len().saturating_div(2).saturating_add(1));
        let mut buf = String::with_capacity(text.len());
        let mut digit_mode: Option<bool> = None;
        for ch in text.chars() {
            let is_digit = ch.is_ascii_digit();
            match digit_mode {
                None => {
                    digit_mode = Some(is_digit);
                    buf.push(ch);
                }
                Some(mode) if mode == is_digit => buf.push(ch),
                Some(mode) => {
                    push_part(&mut out, &buf, mode);
                    buf.clear();
                    digit_mode = Some(is_digit);
                    buf.push(ch);
                }
            }
        }
        if let Some(mode) = digit_mode {
            push_part(&mut out, &buf, mode);
        }
        Self(out)
    }
}
fn push_part(parts_out: &mut Vec<NaturalPart>, raw: &str, part_is_digit: bool) {
    if part_is_digit {
        let trimmed = raw.trim_start_matches('0');
        let normalized: String = if trimmed.is_empty() {
            "0".into()
        } else {
            trimmed.to_owned()
        };
        parts_out.push(NaturalPart::Number {
            digits_len: normalized.len(),
            normalized,
            raw_len: raw.len(),
        });
    } else {
        parts_out.push(NaturalPart::Text(raw.to_owned()));
    }
}
