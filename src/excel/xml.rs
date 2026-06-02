use alloc::borrow::Cow;
use core::{
    iter,
    range::{Range, RangeInclusive},
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum XmlTagKind {
    End,
    Start,
}
#[derive(Clone, Copy)]
pub(in crate::excel) struct XmlTag<'xml> {
    kind: XmlTagKind,
    name: &'xml str,
    self_closing: bool,
    span: RangeInclusive<usize>,
    tag: &'xml str,
}
pub(in crate::excel) struct XmlScanner<'xml> {
    cursor: usize,
    xml: &'xml str,
}
impl XmlScanner<'_> {
    pub(in crate::excel) fn skip_to(&mut self, cursor: usize) {
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
    pub(in crate::excel) const fn new(xml: &'xml str) -> Self {
        Self { cursor: 0, xml }
    }
    pub(in crate::excel) fn next_start_named(&mut self, tag_name: &str) -> Option<XmlTag<'xml>> {
        let wanted = local_tag_name(tag_name);
        self.find_tag_matching(|tag| tag.is_start() && tag.local_name() == wanted)
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
            let kind = if first == b'/' {
                name_start = checked_offset_add(name_start, 1)?;
                XmlTagKind::End
            } else if matches!(first, b'!' | b'?') {
                continue;
            } else {
                XmlTagKind::Start
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
            let span = RangeInclusive { start, last: end };
            let tag = self.xml.get(span)?;
            let mut self_close_cursor = end;
            let mut self_closing = false;
            while self_close_cursor > start {
                let previous = self_close_cursor.saturating_sub(1);
                let Some(&byte) = bytes.get(previous) else {
                    break;
                };
                if byte.is_ascii_whitespace() {
                    self_close_cursor = previous;
                    continue;
                }
                self_closing = byte == b'/';
                break;
            }
            return Some(XmlTag {
                kind,
                name,
                self_closing,
                span,
                tag,
            });
        }
        None
    }
}
impl<'xml> XmlTag<'xml> {
    pub(in crate::excel) const fn end(&self) -> usize {
        self.span.last
    }
    pub(in crate::excel) const fn is_self_closing(&self) -> bool {
        self.self_closing
    }
    pub(in crate::excel) const fn is_start(&self) -> bool {
        matches!(self.kind, XmlTagKind::Start)
    }
    pub(in crate::excel) fn local_name(&self) -> &str {
        local_tag_name(self.name)
    }
    const fn start(&self) -> usize {
        self.span.start
    }
    pub(in crate::excel) const fn tag(&self) -> &'xml str {
        self.tag
    }
}
const fn checked_offset_add(base: usize, add: usize) -> Option<usize> {
    base.checked_add(add)
}
pub(in crate::excel) fn extract_attr<'tag>(
    tag: &'tag str,
    attr_name: &str,
) -> Option<Cow<'tag, str>> {
    if attr_name.is_empty() {
        return None;
    }
    let bytes = tag.as_bytes();
    let mut cursor = checked_offset_add(tag.find('<')?, 1)?;
    while bytes
        .get(cursor)
        .is_some_and(|byte| !byte.is_ascii_whitespace() && *byte != b'/' && *byte != b'>')
    {
        cursor = checked_offset_add(cursor, 1)?;
    }
    loop {
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor = checked_offset_add(cursor, 1)?;
        }
        match bytes.get(cursor).copied() {
            Some(b'/' | b'>') | None => return None,
            Some(_) => {}
        }
        let name_start = cursor;
        while bytes.get(cursor).is_some_and(|byte| {
            !byte.is_ascii_whitespace() && *byte != b'=' && *byte != b'/' && *byte != b'>'
        }) {
            cursor = checked_offset_add(cursor, 1)?;
        }
        let name_end = cursor;
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor = checked_offset_add(cursor, 1)?;
        }
        if bytes.get(cursor) != Some(&b'=') {
            return None;
        }
        cursor = checked_offset_add(cursor, 1)?;
        while bytes.get(cursor).is_some_and(u8::is_ascii_whitespace) {
            cursor = checked_offset_add(cursor, 1)?;
        }
        let quote = *bytes.get(cursor)?;
        if quote != b'"' && quote != b'\'' {
            return None;
        }
        let value_start = checked_offset_add(cursor, 1)?;
        let value_end_rel = tag.get(value_start..)?.find(char::from(quote))?;
        let value_end = checked_offset_add(value_start, value_end_rel)?;
        if tag.get(name_start..name_end)? == attr_name {
            return Some(decode_xml_entities(tag.get(value_start..value_end)?));
        }
        cursor = checked_offset_add(value_end, 1)?;
    }
}
pub(in crate::excel) fn find_start_tag(xml: &str, tag_name: &str, from: usize) -> Option<usize> {
    XmlScanner::from(xml, from)
        .next_start_named(tag_name)
        .map(|tag| tag.start())
}
pub(in crate::excel) fn find_end_tag(xml: &str, tag_name: &str, from: usize) -> Option<usize> {
    let wanted = local_tag_name(tag_name);
    XmlScanner::from(xml, from)
        .find_tag_matching(|tag| !tag.is_start() && tag.local_name() == wanted)
        .map(|tag| tag.start())
}
pub(in crate::excel) fn find_tag_end(xml: &str, tag_start: usize) -> Option<usize> {
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
pub(in crate::excel) fn extract_first_tag_text<'xml>(
    xml: &'xml str,
    tag_name: &str,
) -> Option<&'xml str> {
    let open_start = find_start_tag(xml, tag_name, 0)?;
    let open_end = find_tag_end(xml, open_start)?;
    let body_start = checked_offset_add(open_end, 1)?;
    let body_end = find_end_tag(xml, tag_name, body_start)?;
    let body_span = Range {
        start: body_start,
        end: body_end,
    };
    xml.get(body_span)
}
pub(in crate::excel) fn extract_all_tag_text(xml: &str, tag_name: &str) -> Option<String> {
    let mut cursor = 0_usize;
    let capacity = xml.len().min(128);
    let mut out = String::new();
    out.try_reserve(capacity).ok()?;
    while let Some(open_start) = find_start_tag(xml, tag_name, cursor) {
        let open_end = find_tag_end(xml, open_start)?;
        let body_start = checked_offset_add(open_end, 1)?;
        let body_end = find_end_tag(xml, tag_name, body_start)?;
        let body_span = Range {
            start: body_start,
            end: body_end,
        };
        let body = xml.get(body_span)?;
        match decode_xml_entities(body) {
            Cow::Borrowed(text) => out.push_str(text),
            Cow::Owned(text) => out.push_str(&text),
        }
        let close_tag_len = checked_offset_add(tag_name.len(), 3)?;
        cursor = checked_offset_add(body_end, close_tag_len)?;
    }
    (!out.is_empty()).then_some(out)
}
pub(in crate::excel) fn decode_xml_entities(text: &str) -> Cow<'_, str> {
    if !text.contains('&') {
        return Cow::Borrowed(text);
    }
    let capacity = text.len();
    let mut out = String::with_capacity(capacity);
    let mut i = 0_usize;
    while i < text.len() {
        let Some(rest) = text.get(i..) else {
            break;
        };
        if let Some(after_amp) = rest.strip_prefix('&')
            && let Some((entity, _after_semi)) = after_amp.split_once(';')
            && !entity.is_empty()
        {
            let decoded = match entity {
                "lt" => Some('<'),
                "gt" => Some('>'),
                "quot" => Some('"'),
                "apos" => Some('\''),
                "amp" => Some('&'),
                _ => entity.strip_prefix('#').and_then(|body| {
                    let value = if let Some(hex) = body.strip_prefix(['x', 'X']) {
                        u32::from_str_radix(hex, 16).ok()?
                    } else {
                        body.parse::<u32>().ok()?
                    };
                    char::from_u32(value)
                }),
            };
            if let Some(decoded_char) = decoded {
                out.push(decoded_char);
                let Some(consumed) = entity.len().checked_add(2) else {
                    break;
                };
                let Some(next_i) = checked_offset_add(i, consumed) else {
                    break;
                };
                i = next_i;
                continue;
            }
        }
        let mut chars = rest.chars();
        if let Some(ch) = chars.next() {
            out.push(ch);
            let Some(next_i) = checked_offset_add(i, ch.len_utf8()) else {
                break;
            };
            i = next_i;
        } else {
            break;
        }
    }
    Cow::Owned(out)
}
fn local_tag_name(name: &str) -> &str {
    match name.rsplit_once(':') {
        Some((_, local)) => local,
        None => name,
    }
}
