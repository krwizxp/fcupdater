use super::cp949_table;
use crate::{Result, err};

const WINDOWS_1252_EXTENDED_CHARS: [char; 32] = [
    '€', '�', '‚', 'ƒ', '„', '…', '†', '‡', 'ˆ', '‰', 'Š', '‹', 'Œ', '�', 'Ž', '�', '�', '‘', '’',
    '“', '”', '•', '–', '—', '˜', '™', 'š', '›', 'œ', '�', 'ž', 'Ÿ',
];

struct Cp949Decoder;

trait Cp949DecoderExt {
    fn decode(&self, bytes: &[u8]) -> Option<String>;
}

impl Cp949DecoderExt for Cp949Decoder {
    fn decode(&self, bytes: &[u8]) -> Option<String> {
        let mut out = String::with_capacity(bytes.len());
        let mut index = 0_usize;
        while index < bytes.len() {
            let byte = *bytes.get(index)?;
            if byte.is_ascii() {
                out.push(char::from(byte));
                index = index.checked_add(1)?;
                continue;
            }
            let trail_index = index.checked_add(1)?;
            let trail = *bytes.get(trail_index)?;
            let encoded = (u16::from(byte) << 8_u8) | u16::from(trail);
            let table_index = cp949_table::CP949_DECODE
                .binary_search_by_key(&encoded, |entry| entry.0)
                .ok()?;
            let &(_, scalar) = cp949_table::CP949_DECODE.get(table_index)?;
            out.push(char::from_u32(scalar)?);
            index = index.checked_add(2)?;
        }
        Some(out)
    }
}

fn decode_bytes_to_string(bytes: &[u8], mut map_byte: impl FnMut(u8) -> char) -> String {
    let mut out = String::with_capacity(bytes.len());
    for byte in bytes {
        out.push(map_byte(*byte));
    }
    out
}

pub fn decode_single_byte_text(bytes: &[u8], code_page: Option<u16>) -> Result<String> {
    match code_page {
        Some(65001) => Ok(String::from_utf8_lossy(bytes).into_owned()),
        Some(949 | 1361 | 51949) => Cp949Decoder.decode(bytes).ok_or_else(|| {
            err(format!(
                "code page {} 디코딩에 실패했습니다.",
                code_page.unwrap_or(949)
            ))
        }),
        Some(1252 | 28591) => Ok(decode_bytes_to_string(bytes, |byte| {
            if (0x80..=0x9f).contains(&byte) {
                WINDOWS_1252_EXTENDED_CHARS
                    .get(usize::from(byte).saturating_sub(0x80))
                    .copied()
                    .unwrap_or('�')
            } else {
                char::from(byte)
            }
        })),
        _ => Ok(decode_bytes_to_string(bytes, char::from)),
    }
}

#[cfg(test)]
mod tests {
    use super::decode_single_byte_text;
    use crate::{Result, err};

    #[test]
    fn decodes_cp949_table() -> Result<()> {
        let first = decode_single_byte_text(&[0xb0, 0xa1], Some(949))?;
        if first != "가" {
            return Err(err(format!("CP949 디코딩 결과 불일치: {first}")));
        }
        let second = decode_single_byte_text(&[b'A', 0xb3, 0xaa], Some(949))?;
        if second != "A나" {
            return Err(err(format!("CP949 디코딩 결과 불일치: {second}")));
        }
        Ok(())
    }
}
