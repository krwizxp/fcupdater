use super::cp949_table;
use crate::{Result, err, err_with_source};
const WINDOWS_1252_EXTENDED_CHARS: [char; 32] = [
    '€', '�', '‚', 'ƒ', '„', '…', '†', '‡', 'ˆ', '‰', 'Š', '‹', 'Œ', '�', 'Ž', '�', '�', '‘', '’',
    '“', '”', '•', '–', '—', '˜', '™', 'š', '›', 'œ', '�', 'ž', 'Ÿ',
];
struct Cp949Decoder;
trait Cp949DecoderExt {
    fn decode(&self, bytes: &[u8]) -> Result<Option<String>>;
}
impl Cp949DecoderExt for Cp949Decoder {
    fn decode(&self, bytes: &[u8]) -> Result<Option<String>> {
        let mut out = String::new();
        out.try_reserve(bytes.len())
            .map_err(|source| err_with_source("CP949 문자열 메모리 확보 실패", source))?;
        let mut index = 0_usize;
        while index < bytes.len() {
            let Some(&byte) = bytes.get(index) else {
                return Ok(None);
            };
            if byte.is_ascii() {
                out.push(char::from(byte));
                let Some(next_index) = index.checked_add(1) else {
                    return Ok(None);
                };
                index = next_index;
                continue;
            }
            let Some(trail_index) = index.checked_add(1) else {
                return Ok(None);
            };
            let Some(&trail) = bytes.get(trail_index) else {
                return Ok(None);
            };
            let encoded = u16::from_be_bytes([byte, trail]);
            let Some(key_range_index) = cp949_table::CP949_DECODE_KEY_RANGES
                .partition_point(|range| range.start() <= encoded)
                .checked_sub(1)
            else {
                return Ok(None);
            };
            let Some(key_range) = cp949_table::CP949_DECODE_KEY_RANGES.get(key_range_index) else {
                return Ok(None);
            };
            let key_start = key_range.start();
            let char_start = usize::from(key_range.char_start());
            let key_len = key_range_index
                .checked_add(1)
                .and_then(|next_index| cp949_table::CP949_DECODE_KEY_RANGES.get(next_index))
                .map_or(cp949_table::CP949_DECODE_CHAR_COUNT, |next_range| {
                    usize::from(next_range.char_start())
                })
                .saturating_sub(char_start);
            let key_offset = usize::from(encoded.saturating_sub(key_start));
            if key_offset >= key_len {
                return Ok(None);
            }
            let Some(char_index) = key_offset.checked_add(char_start) else {
                return Ok(None);
            };
            let Some(value_range_index) = cp949_table::CP949_DECODE_VALUE_RANGES
                .partition_point(|range| usize::from(range.start()) <= char_index)
                .checked_sub(1)
            else {
                return Ok(None);
            };
            let Some(value_range) = cp949_table::CP949_DECODE_VALUE_RANGES.get(value_range_index)
            else {
                return Ok(None);
            };
            let value_start = usize::from(value_range.start());
            let value_end_exclusive = value_range_index
                .checked_add(1)
                .and_then(|next_index| cp949_table::CP949_DECODE_VALUE_RANGES.get(next_index))
                .map_or(cp949_table::CP949_DECODE_CHAR_COUNT, |next_range| {
                    usize::from(next_range.start())
                });
            if char_index >= value_end_exclusive {
                return Ok(None);
            }
            let Some(char_offset) = char_index.checked_sub(value_start) else {
                return Ok(None);
            };
            let Ok(scalar_offset) = u32::try_from(char_offset) else {
                return Ok(None);
            };
            let Some(scalar) = u32::from(value_range.scalar_start()).checked_add(scalar_offset)
            else {
                return Ok(None);
            };
            let Some(ch) = char::from_u32(scalar) else {
                return Ok(None);
            };
            out.push(ch);
            let Some(next_index) = index.checked_add(2) else {
                return Ok(None);
            };
            index = next_index;
        }
        Ok(Some(out))
    }
}
fn decode_bytes_to_string(bytes: &[u8], mut map_byte: impl FnMut(u8) -> char) -> Result<String> {
    let mut out = String::new();
    out.try_reserve(bytes.len())
        .map_err(|source| err_with_source("single-byte 문자열 메모리 확보 실패", source))?;
    for byte in bytes {
        out.push(map_byte(*byte));
    }
    Ok(out)
}
pub(super) fn decode_single_byte_text(bytes: &[u8], code_page: Option<u16>) -> Result<String> {
    match code_page {
        Some(65001) => Ok(String::from_utf8_lossy(bytes).into_owned()),
        Some(selected_code_page @ (949 | 1361 | 51949)) => {
            Cp949Decoder.decode(bytes)?.ok_or_else(|| {
                err(format!(
                    "code page {selected_code_page} 디코딩에 실패했습니다."
                ))
            })
        }
        Some(1252 | 28591) => decode_bytes_to_string(bytes, |byte| {
            if (0x80..=0x9f).contains(&byte) {
                let index = usize::from(byte).saturating_sub(0x80);
                let Some(&mapped) = WINDOWS_1252_EXTENDED_CHARS.get(index) else {
                    return '�';
                };
                mapped
            } else {
                char::from(byte)
            }
        }),
        _ => decode_bytes_to_string(bytes, char::from),
    }
}
