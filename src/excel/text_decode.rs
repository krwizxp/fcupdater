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
    bytes.iter().copied().map(&mut map_byte).collect()
}
pub(super) fn decode_single_byte_text(bytes: &[u8], code_page: Option<u16>) -> Result<String> {
    match code_page {
        Some(65001) => Ok(String::from_utf8_lossy(bytes).into_owned()),
        Some(selected_code_page @ (949 | 1361 | 51949)) => {
            Cp949Decoder.decode(bytes).ok_or_else(|| {
                err(format!(
                    "code page {selected_code_page} 디코딩에 실패했습니다."
                ))
            })
        }
        Some(1252 | 28591) => Ok(decode_bytes_to_string(bytes, |byte| {
            if (0x80..=0x9f).contains(&byte) {
                let index = usize::from(byte).saturating_sub(0x80);
                let Some(&mapped) = WINDOWS_1252_EXTENDED_CHARS.get(index) else {
                    return '�';
                };
                mapped
            } else {
                char::from(byte)
            }
        })),
        _ => Ok(decode_bytes_to_string(bytes, char::from)),
    }
}
