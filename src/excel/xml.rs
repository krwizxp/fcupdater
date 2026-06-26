use alloc::borrow::Cow;
use core::{iter, range::Range};

use crate::diagnostic::{Result, err, err_with_source};
pub(super) struct XmlTag<'xml> {
    pub end: usize,
    pub is_start: bool,
    pub local_name: &'xml str,
    pub name: &'xml str,
    pub raw: &'xml str,
    pub self_closing: bool,
    pub start: usize,
}
pub(super) struct XmlScanner<'xml> {
    cursor: usize,
    xml: &'xml str,
}
impl XmlScanner<'_> {
    pub(super) fn skip_to(&mut self, cursor: usize) {
        self.cursor = cursor.min(self.xml.len());
    }
}
impl<'xml> XmlScanner<'xml> {
    fn find_tag_matching<F>(&mut self, predicate: F) -> Option<XmlTag<'xml>>
    where
        F: FnMut(&XmlTag<'xml>) -> bool,
    {
        iter::from_fn(|| self.next_tag()).find(predicate)
    }
    const fn from(xml: &'xml str, cursor: usize) -> Self {
        Self { cursor, xml }
    }
    pub(super) const fn new(xml: &'xml str) -> Self {
        Self { cursor: 0, xml }
    }
    pub(super) fn next_start_named(&mut self, tag_name: &str) -> Option<XmlTag<'xml>> {
        let wanted = local_tag_name(tag_name);
        self.find_tag_matching(|tag| tag.is_start && tag.local_name == wanted)
    }
    fn next_tag(&mut self) -> Option<XmlTag<'xml>> {
        while let Some(rel) = self.xml.get(self.cursor..)?.find('<') {
            let start = checked_offset_add(self.cursor, rel)?;
            let end = find_tag_end(self.xml, start)?;
            self.cursor = checked_offset_add(end, 1)?;
            let inner_start = checked_offset_add(start, 1)?;
            let mut name_start = inner_start;
            let bytes = self.xml.as_bytes();
            let first = *bytes.get(name_start)?;
            let is_start = if first == b'/' {
                name_start = checked_offset_add(name_start, 1)?;
                false
            } else if matches!(first, b'!' | b'?') {
                continue;
            } else {
                true
            };
            while bytes.get(name_start).is_some_and(u8::is_ascii_whitespace) {
                name_start = checked_offset_add(name_start, 1)?;
            }
            let mut name_end = name_start;
            while bytes
                .get(name_end)
                .is_some_and(|byte| !byte.is_ascii_whitespace() && !matches!(*byte, b'/' | b'>'))
            {
                name_end = checked_offset_add(name_end, 1)?;
            }
            let name = self.xml.get(name_start..name_end)?;
            if name.is_empty() {
                continue;
            }
            let raw = self.xml.get(Range {
                start,
                end: checked_offset_add(end, 1)?,
            })?;
            let mut self_close_cursor = end;
            let mut self_closing = false;
            while self_close_cursor > start {
                let previous = self_close_cursor.checked_sub(1)?;
                let byte = *bytes.get(previous)?;
                if byte.is_ascii_whitespace() {
                    self_close_cursor = previous;
                    continue;
                }
                self_closing = byte == b'/';
                break;
            }
            return Some(XmlTag {
                end,
                is_start,
                local_name: local_tag_name(name),
                name,
                raw,
                self_closing,
                start,
            });
        }
        None
    }
}
const fn checked_offset_add(base: usize, add: usize) -> Option<usize> {
    base.checked_add(add)
}
pub(super) fn extract_attr<'tag>(
    tag: &'tag str,
    attr_name: &str,
) -> Result<Option<Cow<'tag, str>>> {
    if attr_name.is_empty() {
        return Ok(None);
    }
    let bytes = tag.as_bytes();
    let Some(tag_start) = tag.find('<') else {
        return Err(err("XML 태그 시작 문자를 찾지 못했습니다."));
    };
    let mut cursor = checked_offset_add(tag_start, 1)
        .ok_or_else(|| err("XML 태그 속성 cursor 계산에 실패했습니다."))?;
    while bytes
        .get(cursor)
        .is_some_and(|byte| !byte.is_ascii_whitespace() && *byte != b'/' && *byte != b'>')
    {
        cursor = checked_offset_add(cursor, 1)
            .ok_or_else(|| err("XML 태그 이름 cursor 계산에 실패했습니다."))?;
    }
    loop {
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor = checked_offset_add(cursor, 1)
                .ok_or_else(|| err("XML 속성 공백 cursor 계산에 실패했습니다."))?;
        }
        match bytes.get(cursor).copied() {
            Some(b'/' | b'>') | None => return Ok(None),
            Some(_) => {}
        }
        let name_start = cursor;
        while bytes.get(cursor).is_some_and(|byte| {
            !byte.is_ascii_whitespace() && *byte != b'=' && *byte != b'/' && *byte != b'>'
        }) {
            cursor = checked_offset_add(cursor, 1)
                .ok_or_else(|| err("XML 속성 이름 cursor 계산에 실패했습니다."))?;
        }
        let name_end = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor = checked_offset_add(cursor, 1)
                .ok_or_else(|| err("XML 속성 이름 뒤 공백 cursor 계산에 실패했습니다."))?;
        }
        if bytes.get(cursor) != Some(&b'=') {
            return Err(err("XML 속성의 '=' 문자를 찾지 못했습니다."));
        }
        cursor = checked_offset_add(cursor, 1)
            .ok_or_else(|| err("XML 속성 값 cursor 계산에 실패했습니다."))?;
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor = checked_offset_add(cursor, 1)
                .ok_or_else(|| err("XML 속성 값 앞 공백 cursor 계산에 실패했습니다."))?;
        }
        let Some(&quote) = bytes.get(cursor) else {
            return Err(err("XML 속성 값 quote 문자를 찾지 못했습니다."));
        };
        if quote != b'"' && quote != b'\'' {
            return Err(err("XML 속성 값 quote 문자가 올바르지 않습니다."));
        }
        let value_start = checked_offset_add(cursor, 1)
            .ok_or_else(|| err("XML 속성 값 시작 위치 계산에 실패했습니다."))?;
        let Some(value_tail) = tag.get(value_start..) else {
            return Err(err("XML 속성 값 범위가 손상되었습니다."));
        };
        let Some(value_end_rel) = value_tail.find(char::from(quote)) else {
            return Err(err("XML 속성 값 종료 quote를 찾지 못했습니다."));
        };
        let value_end = checked_offset_add(value_start, value_end_rel)
            .ok_or_else(|| err("XML 속성 값 종료 위치 계산에 실패했습니다."))?;
        let Some(name) = tag.get(name_start..name_end) else {
            return Err(err("XML 속성 이름 범위가 손상되었습니다."));
        };
        if name == attr_name {
            let Some(value) = tag.get(value_start..value_end) else {
                return Err(err("XML 속성 값 범위가 손상되었습니다."));
            };
            return decode_xml_entities(value).map(Some);
        }
        cursor = checked_offset_add(value_end, 1)
            .ok_or_else(|| err("XML 다음 속성 cursor 계산에 실패했습니다."))?;
    }
}
pub(super) fn find_start_tag(xml: &str, tag_name: &str, from: usize) -> Option<usize> {
    XmlScanner::from(xml, from)
        .next_start_named(tag_name)
        .map(|tag| tag.start)
}
pub(super) fn find_end_tag(xml: &str, tag_name: &str, from: usize) -> Option<usize> {
    let wanted = local_tag_name(tag_name);
    XmlScanner::from(xml, from)
        .find_tag_matching(|tag| !tag.is_start && tag.local_name == wanted)
        .map(|tag| tag.start)
}
pub(super) fn find_tag_end(xml: &str, tag_start: usize) -> Option<usize> {
    let bytes = xml.as_bytes();
    let mut cursor = tag_start;
    let mut quote = None;
    while let Some(&byte) = bytes.get(cursor) {
        match quote {
            Some(active_quote) if byte == active_quote => quote = None,
            None if matches!(byte, b'"' | b'\'') => quote = Some(byte),
            None if byte == b'>' => return Some(cursor),
            Some(_) | None => {}
        }
        cursor = checked_offset_add(cursor, 1)?;
    }
    None
}
pub(super) fn extract_first_tag_text<'xml>(
    xml: &'xml str,
    tag_name: &str,
) -> Result<Option<&'xml str>> {
    let mut scanner = XmlScanner::new(xml);
    let Some(tag) = scanner.next_start_named(tag_name) else {
        return Ok(None);
    };
    if tag.self_closing {
        return Ok(Some(""));
    }
    let open_end = tag.end;
    let body_start = checked_offset_add(open_end, 1)
        .ok_or_else(|| err(format!("XML <{tag_name}> 본문 시작 계산에 실패했습니다.")))?;
    let body_end = find_end_tag(xml, tag_name, body_start)
        .ok_or_else(|| err(format!("XML </{tag_name}> 종료 태그를 찾지 못했습니다.")))?;
    let body_span = Range {
        start: body_start,
        end: body_end,
    };
    xml.get(body_span)
        .ok_or_else(|| err(format!("XML <{tag_name}> 본문 범위가 손상되었습니다.")))
        .map(Some)
}
pub(super) fn extract_all_tag_text<'xml>(
    xml: &'xml str,
    tag_name: &str,
) -> Result<Option<Cow<'xml, str>>> {
    let mut scanner = XmlScanner::new(xml);
    let mut first_text: Option<Cow<'xml, str>> = None;
    let mut out: Option<String> = None;
    let mut saw_text_tag = false;
    while let Some(tag) = scanner.next_start_named(tag_name) {
        saw_text_tag = true;
        if tag.self_closing {
            continue;
        }
        let open_end = tag.end;
        let body_start = checked_offset_add(open_end, 1)
            .ok_or_else(|| err(format!("XML <{tag_name}> 본문 시작 계산에 실패했습니다.")))?;
        let body_end = find_end_tag(xml, tag_name, body_start)
            .ok_or_else(|| err(format!("XML </{tag_name}> 종료 태그를 찾지 못했습니다.")))?;
        let body_span = Range {
            start: body_start,
            end: body_end,
        };
        let body = xml
            .get(body_span)
            .ok_or_else(|| err(format!("XML <{tag_name}> 본문 범위가 손상되었습니다.")))?;
        let decoded = decode_xml_entities(body)?;
        if !decoded.is_empty() {
            if let Some(out_text) = out.as_mut() {
                out_text.push_str(decoded.as_ref());
            } else if let Some(previous) = first_text.take() {
                let capacity = previous
                    .len()
                    .checked_add(decoded.len())
                    .ok_or_else(|| err(format!("XML <{tag_name}> text 용량 계산 실패")))?;
                let mut out_text = String::new();
                out_text.try_reserve(capacity).map_err(|source| {
                    err_with_source(format!("XML <{tag_name}> text 메모리 확보 실패"), source)
                })?;
                out_text.push_str(previous.as_ref());
                out_text.push_str(decoded.as_ref());
                out = Some(out_text);
            } else {
                first_text = Some(decoded);
            }
        }
        let close_end = find_tag_end(xml, body_end)
            .ok_or_else(|| err(format!("XML </{tag_name}> 태그가 손상되었습니다.")))?;
        let next_cursor = checked_offset_add(close_end, 1)
            .ok_or_else(|| err(format!("XML 다음 <{tag_name}> cursor 계산에 실패했습니다.")))?;
        scanner.skip_to(next_cursor);
    }
    Ok(out
        .map(Cow::Owned)
        .or(first_text)
        .or_else(|| saw_text_tag.then_some(Cow::Borrowed(""))))
}
pub(super) fn decode_xml_entities(text: &str) -> Result<Cow<'_, str>> {
    let mut out: Option<String> = None;
    let mut i = 0_usize;
    while i < text.len() {
        let rest = text
            .get(i..)
            .ok_or_else(|| err("XML entity decode cursor 범위가 손상되었습니다."))?;
        if rest.starts_with("]]>") {
            return Err(err(
                "XML text에 허용되지 않는 ']]>' 시퀀스가 포함되어 있습니다.",
            ));
        }
        let Some(ch) = rest.chars().next() else {
            return Err(err("XML entity decode 문자를 읽지 못했습니다."));
        };
        if !is_valid_xml_char(ch) {
            return Err(err(format!(
                "XML text: XML 1.0에서 허용되지 않는 문자가 포함되어 있습니다: U+{:04X}",
                u32::from(ch)
            )));
        }
        if ch == '<' {
            return Err(err("XML text에 raw '<' 문자가 포함되어 있습니다."));
        }
        if let Some(after_amp) = rest.strip_prefix('&') {
            if out.is_none() {
                let mut out_text = String::new();
                out_text.try_reserve(text.len()).map_err(|source| {
                    err_with_source("XML entity decode 메모리 확보 실패", source)
                })?;
                let prefix = text
                    .get(..i)
                    .ok_or_else(|| err("XML entity decode prefix 범위가 손상되었습니다."))?;
                out_text.push_str(prefix);
                out = Some(out_text);
            }
            let Some((entity, _after_semi)) = after_amp.split_once(';') else {
                return Err(err("XML entity 종료 세미콜론을 찾지 못했습니다."));
            };
            if entity.is_empty() {
                return Err(err("XML entity 이름이 비어 있습니다."));
            }
            let decoded = match entity {
                "lt" => Some('<'),
                "gt" => Some('>'),
                "quot" => Some('"'),
                "apos" => Some('\''),
                "amp" => Some('&'),
                _ => {
                    let Some(body) = entity.strip_prefix('#') else {
                        return Err(err(format!("지원하지 않는 XML entity입니다: &{entity};")));
                    };
                    let value = if let Some(hex) = body.strip_prefix(['x', 'X']) {
                        u32::from_str_radix(hex, 16).map_err(|source| {
                            err_with_source("XML numeric hex entity 해석 실패", source)
                        })?
                    } else {
                        body.parse::<u32>().map_err(|source| {
                            err_with_source("XML numeric entity 해석 실패", source)
                        })?
                    };
                    char::from_u32(value)
                }
            };
            let Some(decoded_char) = decoded else {
                return Err(err(format!(
                    "XML numeric entity가 유효한 Unicode scalar value가 아닙니다: &{entity};"
                )));
            };
            if !is_valid_xml_char(decoded_char) {
                return Err(err(format!(
                    "XML numeric entity가 XML 1.0 유효 문자 범위를 벗어났습니다: &{entity};"
                )));
            }
            let Some(out_text) = out.as_mut() else {
                return Err(err("XML entity decode output 상태가 손상되었습니다."));
            };
            out_text.push(decoded_char);
            let consumed = checked_offset_add(entity.len(), 2)
                .ok_or_else(|| err("XML entity 소비 길이 계산에 실패했습니다."))?;
            i = checked_offset_add(i, consumed)
                .ok_or_else(|| err("XML entity 다음 cursor 계산에 실패했습니다."))?;
            continue;
        }
        if let Some(out_text) = out.as_mut() {
            out_text.push(ch);
        }
        i = checked_offset_add(i, ch.len_utf8())
            .ok_or_else(|| err("XML entity decode cursor 계산에 실패했습니다."))?;
    }
    Ok(out.map_or(Cow::Borrowed(text), Cow::Owned))
}
pub(super) fn ensure_valid_xml_text(text: &str, context: &str) -> Result<()> {
    for ch in text.chars() {
        if !is_valid_xml_char(ch) {
            return Err(err(format!(
                "{context}: XML 1.0에서 허용되지 않는 문자가 포함되어 있습니다: U+{:04X}",
                u32::from(ch)
            )));
        }
    }
    Ok(())
}
fn is_valid_xml_char(ch: char) -> bool {
    matches!(
        u32::from(ch),
        0x09 | 0x0A | 0x0D | 0x20..=0xD7FF | 0xE000..=0xFFFD | 0x0001_0000..=0x0010_FFFF
    )
}
fn local_tag_name(name: &str) -> &str {
    match name.rsplit_once(':') {
        Some((_, local)) => local,
        None => name,
    }
}
