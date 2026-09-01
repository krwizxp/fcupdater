use crate::diagnostic::{Result, err, err_with_source, try_string_with_capacity};
use alloc::borrow::Cow;
use core::{iter, num::IntErrorKind, range::Range};
pub(super) const MAX_XML_NESTING_DEPTH: usize = 64;
pub(super) struct XmlTag<'xml> {
    pub end: usize,
    pub is_start: bool,
    pub local_name: &'xml str,
    pub name: &'xml str,
    pub raw: &'xml str,
    pub self_closing: bool,
    pub start: usize,
}
pub(super) struct XmlElement<'xml> {
    pub body: &'xml str,
    pub body_span: Range<usize>,
    pub opening: XmlTag<'xml>,
    pub span: Range<usize>,
}
pub(super) struct XmlScanner<'xml> {
    cursor: usize,
    xml: &'xml str,
}
pub(super) struct XmlAttrScanner<'tag> {
    cursor: usize,
    tag: &'tag str,
}
impl<'xml> XmlScanner<'xml> {
    pub(super) const fn cursor(&self) -> usize {
        self.cursor
    }
    fn element_from_opening(
        &mut self,
        opening: XmlTag<'xml>,
        ancestor_depth: usize,
    ) -> Result<XmlElement<'xml>> {
        if ancestor_depth >= MAX_XML_NESTING_DEPTH {
            return Err(err("XML 중첩 깊이가 너무 큽니다."));
        }
        let tag_name = opening.name;
        let body_start = opening.end.strict_add(1);
        let (body_end, end) = if opening.self_closing {
            (body_start, body_start)
        } else {
            let mut scanner = Self {
                cursor: body_start,
                xml: self.xml,
            };
            let mut ancestors = [tag_name; MAX_XML_NESTING_DEPTH];
            let mut depth = 1_usize;
            let closing = loop {
                let tag = scanner.next_tag().ok_or_else(|| {
                    err(format!("XML </{tag_name}> 종료 태그를 찾지 못했습니다."))
                })?;
                if tag.is_start {
                    if !tag.self_closing {
                        if ancestor_depth.strict_add(depth) >= MAX_XML_NESTING_DEPTH {
                            return Err(err("XML 중첩 깊이가 너무 큽니다."));
                        }
                        *ancestors
                            .get_mut(depth)
                            .ok_or_else(|| err("XML 중첩 깊이가 너무 큽니다."))? = tag.name;
                        depth = depth.strict_add(1);
                    }
                    continue;
                }
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| err("XML 종료 태그 순서가 올바르지 않습니다."))?;
                let open = ancestors
                    .get(depth)
                    .copied()
                    .ok_or_else(|| err("XML 중첩 깊이가 손상되었습니다."))?;
                if open != tag.name {
                    return Err(err(format!(
                        "XML 태그 쌍이 일치하지 않습니다: {open} / {}",
                        tag.name
                    )));
                }
                if depth == 0 {
                    break tag;
                }
            };
            let end = closing.end.strict_add(1);
            (closing.start, end)
        };
        let body_span = Range {
            start: body_start,
            end: body_end,
        };
        let body = self
            .xml
            .get(body_span)
            .ok_or_else(|| err(format!("XML <{tag_name}> 본문 범위가 손상되었습니다.")))?;
        let span = Range {
            start: opening.start,
            end,
        };
        self.skip_to(end);
        Ok(XmlElement {
            body,
            body_span,
            opening,
            span,
        })
    }
    fn find_tag_end(&self, tag_start: usize) -> Option<usize> {
        let tail = self.xml.get(tag_start..)?;
        if tail.starts_with("<!--") {
            return find_delimited_markup_end(self.xml, tag_start, 4, "-->");
        }
        if tail.starts_with("<![CDATA[") {
            return find_delimited_markup_end(self.xml, tag_start, 9, "]]>");
        }
        if tail.starts_with("<?") {
            return find_delimited_markup_end(self.xml, tag_start, 2, "?>");
        }
        if !tail.starts_with('<') {
            return None;
        }
        let is_declaration = tail.starts_with("<!");
        let bytes = self.xml.as_bytes();
        let mut cursor = if is_declaration {
            tag_start.strict_add(2)
        } else {
            tag_start
        };
        let mut in_comment = false;
        let mut quote = None;
        let mut subset_depth = 0_usize;
        while let Some(&byte) = bytes.get(cursor) {
            if in_comment {
                if bytes
                    .get(cursor..)
                    .is_some_and(|remaining| remaining.starts_with(b"-->"))
                {
                    in_comment = false;
                    cursor = cursor.strict_add(3);
                } else {
                    cursor = cursor.strict_add(1);
                }
                continue;
            }
            if is_declaration
                && quote.is_none()
                && bytes
                    .get(cursor..)
                    .is_some_and(|remaining| remaining.starts_with(b"<!--"))
            {
                in_comment = true;
                cursor = cursor.strict_add(4);
                continue;
            }
            match quote {
                Some(active_quote) if byte == active_quote => quote = None,
                None if matches!(byte, b'"' | b'\'') => quote = Some(byte),
                None if is_declaration && byte == b'[' => {
                    subset_depth = subset_depth.strict_add(1);
                }
                None if is_declaration && byte == b']' && subset_depth != 0 => {
                    subset_depth = subset_depth.strict_sub(1);
                }
                None if byte == b'>' && subset_depth == 0 => return Some(cursor),
                Some(_) | None => {}
            }
            cursor = cursor.strict_add(1);
        }
        None
    }
    pub(super) const fn new(xml: &'xml str) -> Self {
        Self { cursor: 0, xml }
    }
    pub(super) fn next_direct_element(
        &mut self,
        context: &str,
    ) -> Result<Option<XmlElement<'xml>>> {
        self.next_direct_element_before(None, context)
    }
    fn next_direct_element_before(
        &mut self,
        closing_name: Option<&str>,
        context: &str,
    ) -> Result<Option<XmlElement<'xml>>> {
        let content_start = self.cursor;
        let opening = self.next_tag();
        let content_end = opening.as_ref().map_or(self.xml.len(), |tag| tag.start);
        self.validate_direct_gap(content_start, content_end, context)?;
        let Some(opening_tag) = opening else {
            return closing_name.map_or(Ok(None), |_| {
                Err(err(format!("{context}의 XML root 종료 태그가 없습니다.")))
            });
        };
        if !opening_tag.is_start {
            if closing_name == Some(opening_tag.name)
                && self
                    .xml
                    .get(self.cursor..)
                    .is_some_and(|trailing| xml_misc_only(trailing, false))
            {
                return Ok(None);
            }
            return Err(err(format!(
                "{context}에 직접 자식이 아닌 종료 태그가 있습니다: {}",
                opening_tag.name
            )));
        }
        self.element_from_opening(opening_tag, 0).map(Some)
    }
    pub(super) fn next_direct_element_named_until(
        &mut self,
        tag_name: &str,
        closing_name: &str,
        ancestor_depth: usize,
        context: &str,
    ) -> Result<Option<XmlElement<'xml>>> {
        let Some(opening) =
            self.next_direct_opening_named_until(tag_name, closing_name, context)?
        else {
            return Ok(None);
        };
        self.element_from_opening(opening, ancestor_depth).map(Some)
    }
    pub(super) fn next_direct_element_until(
        &mut self,
        closing_name: &str,
        context: &str,
    ) -> Result<Option<XmlElement<'xml>>> {
        self.next_direct_element_before(Some(closing_name), context)
    }
    pub(super) fn next_direct_opening_named_until(
        &mut self,
        tag_name: &str,
        closing_name: &str,
        context: &str,
    ) -> Result<Option<XmlTag<'xml>>> {
        let content_start = self.cursor;
        let Some(opening) = self.next_tag() else {
            return Err(err(format!(
                "XML </{closing_name}> 종료 태그를 찾지 못했습니다."
            )));
        };
        self.validate_direct_gap(content_start, opening.start, context)?;
        if !opening.is_start {
            if opening.name == closing_name {
                return Ok(None);
            }
            return Err(err(format!(
                "XML 태그 쌍이 일치하지 않습니다: {closing_name} / {}",
                opening.name
            )));
        }
        if opening.name != tag_name {
            return Err(err(format!(
                "{context}에 직접 자식 {tag_name} 외 요소가 있습니다: {}",
                opening.name
            )));
        }
        Ok(Some(opening))
    }
    pub(super) fn next_element_named(
        &mut self,
        tag_name: &str,
    ) -> Result<Option<XmlElement<'xml>>> {
        let Some(opening) = self.next_start_named(tag_name) else {
            return Ok(None);
        };
        self.element_from_opening(opening, 0).map(Some)
    }
    pub(super) fn next_start_named(&mut self, tag_name: &str) -> Option<XmlTag<'xml>> {
        let wanted = local_tag_name(tag_name);
        iter::from_fn(|| self.next_tag()).find(|tag| tag.is_start && tag.local_name == wanted)
    }
    pub(super) fn next_tag(&mut self) -> Option<XmlTag<'xml>> {
        while let Some(rel) = self.xml.get(self.cursor..)?.find('<') {
            let start = self.cursor.strict_add(rel);
            let end = self.find_tag_end(start)?;
            self.cursor = end.strict_add(1);
            let inner_start = start.strict_add(1);
            let mut name_start = inner_start;
            let bytes = self.xml.as_bytes();
            let first = *bytes.get(name_start)?;
            let is_start = if first == b'/' {
                name_start = name_start.strict_add(1);
                false
            } else if first == b'?' || first == b'!' && self.xml.get(start..)?.starts_with("<!--") {
                continue;
            } else if first == b'!' {
                return None;
            } else {
                true
            };
            if bytes
                .get(name_start)
                .is_some_and(|&byte| is_xml_whitespace(byte))
            {
                return None;
            }
            let mut name_end = name_start;
            while bytes
                .get(name_end)
                .is_some_and(|&byte| !is_xml_whitespace(byte) && !matches!(byte, b'/' | b'>'))
            {
                name_end = name_end.strict_add(1);
            }
            let name = self.xml.get(name_start..name_end)?;
            if name.is_empty() {
                return None;
            }
            if !is_start
                && self
                    .xml
                    .get(name_end..end)?
                    .bytes()
                    .any(|byte| !is_xml_whitespace(byte))
            {
                return None;
            }
            let raw = self.xml.get(Range {
                start,
                end: end.strict_add(1),
            })?;
            let self_closing = bytes.get(end.strict_sub(1)) == Some(&b'/');
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
    pub(super) fn skip_to(&mut self, cursor: usize) {
        self.cursor = cursor.min(self.xml.len());
    }
    fn validate_direct_gap(&self, start: usize, end: usize, context: &str) -> Result<()> {
        let between = self
            .xml
            .get(start..end)
            .ok_or_else(|| err(format!("{context}의 XML child 범위가 손상되었습니다.")))?;
        if xml_misc_only(between, false) {
            Ok(())
        } else {
            Err(err(format!(
                "{context}의 XML 요소 사이 내용이 올바르지 않습니다."
            )))
        }
    }
}
impl<'tag> XmlAttrScanner<'tag> {
    pub(super) fn new(tag: &'tag str) -> Result<Self> {
        let bytes = tag.as_bytes();
        let Some(tag_start) = tag.find('<') else {
            return Err(err("XML 태그 시작 문자를 찾지 못했습니다."));
        };
        let mut cursor = tag_start.strict_add(1);
        while bytes
            .get(cursor)
            .is_some_and(|&byte| !is_xml_whitespace(byte) && byte != b'/' && byte != b'>')
        {
            cursor = cursor.strict_add(1);
        }
        Ok(Self { cursor, tag })
    }
    pub(super) fn next(&mut self) -> Result<Option<(&'tag str, Cow<'tag, str>)>> {
        let bytes = self.tag.as_bytes();
        let separator_start = self.cursor;
        skip_xml_whitespace(bytes, &mut self.cursor);
        match bytes.get(self.cursor).copied() {
            Some(b'>') if self.cursor.strict_add(1) == bytes.len() => return Ok(None),
            Some(b'/') if self.tag.get(self.cursor..) == Some("/>") => return Ok(None),
            Some(b'/' | b'?' | b'>') | None => {
                return Err(err("XML 태그 종료 형식이 올바르지 않습니다."));
            }
            Some(_) if self.cursor == separator_start => {
                return Err(err("XML 속성 사이에 공백이 없습니다."));
            }
            Some(_) => {}
        }
        let name_start = self.cursor;
        while bytes.get(self.cursor).is_some_and(|byte| {
            !is_xml_whitespace(*byte)
                && *byte != b'='
                && *byte != b'/'
                && *byte != b'?'
                && *byte != b'>'
        }) {
            self.cursor = self.cursor.strict_add(1);
        }
        let name_end = self.cursor;
        if name_start == name_end {
            return Err(err("XML 속성 이름이 비어 있습니다."));
        }
        skip_xml_whitespace(bytes, &mut self.cursor);
        if bytes.get(self.cursor) != Some(&b'=') {
            return Err(err("XML 속성의 '=' 문자를 찾지 못했습니다."));
        }
        self.cursor = self.cursor.strict_add(1);
        skip_xml_whitespace(bytes, &mut self.cursor);
        let Some(&quote) = bytes.get(self.cursor) else {
            return Err(err("XML 속성 값 quote 문자를 찾지 못했습니다."));
        };
        if quote != b'"' && quote != b'\'' {
            return Err(err("XML 속성 값 quote 문자가 올바르지 않습니다."));
        }
        let value_start = self.cursor.strict_add(1);
        let Some(value_tail) = self.tag.get(value_start..) else {
            return Err(err("XML 속성 값 범위가 손상되었습니다."));
        };
        let Some(value_end_rel) = value_tail.find(char::from(quote)) else {
            return Err(err("XML 속성 값 종료 quote를 찾지 못했습니다."));
        };
        let value_end = value_start.strict_add(value_end_rel);
        let name = self
            .tag
            .get(name_start..name_end)
            .ok_or_else(|| err("XML 속성 이름 범위가 손상되었습니다."))?;
        let value = self
            .tag
            .get(value_start..value_end)
            .ok_or_else(|| err("XML 속성 값 범위가 손상되었습니다."))?;
        self.cursor = value_end.strict_add(1);
        Ok(Some((name, decode_xml_entities(value)?)))
    }
}
const fn is_xml_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}
fn skip_xml_whitespace(bytes: &[u8], cursor: &mut usize) {
    while bytes
        .get(*cursor)
        .is_some_and(|&byte| is_xml_whitespace(byte))
    {
        *cursor = cursor.strict_add(1);
    }
}
fn find_delimited_markup_end(
    xml: &str,
    tag_start: usize,
    opener_len: usize,
    terminator: &str,
) -> Option<usize> {
    let search_start = tag_start.strict_add(opener_len);
    let relative_end = xml.get(search_start..)?.find(terminator)?;
    if terminator == "-->"
        && xml
            .get(search_start..search_start.strict_add(relative_end))
            .is_some_and(|body| body.contains("--") || body.ends_with('-'))
    {
        return None;
    }
    Some(
        search_start
            .strict_add(relative_end)
            .strict_add(terminator.len())
            .strict_sub(1),
    )
}
pub(super) fn xml_misc_only(mut xml: &str, allow_bom: bool) -> bool {
    if allow_bom && let Some(without_bom) = xml.strip_prefix('\u{feff}') {
        xml = without_bom;
    }
    loop {
        xml = xml.trim_start_matches([' ', '\t', '\n', '\r']);
        if xml.is_empty() {
            return true;
        }
        let (opener_len, terminator) = if xml.starts_with("<!--") {
            (4, "-->")
        } else if xml.starts_with("<?") {
            (2, "?>")
        } else {
            return false;
        };
        let Some(next) =
            find_delimited_markup_end(xml, 0, opener_len, terminator).map(|end| end.strict_add(1))
        else {
            return false;
        };
        let Some(remaining) = xml.get(next..) else {
            return false;
        };
        xml = remaining;
    }
}
pub(super) fn extract_first_tag_text<'xml>(
    xml: &'xml str,
    tag_name: &str,
) -> Result<Option<&'xml str>> {
    let mut scanner = XmlScanner::new(xml);
    scanner
        .next_element_named(tag_name)
        .map(|maybe_element| maybe_element.map(|element| element.body))
}
pub(super) fn decode_xml_entities(text: &str) -> Result<Cow<'_, str>> {
    let mut out: Option<String> = None;
    let mut cursor = 0_usize;
    while cursor < text.len() {
        let tail = text
            .get(cursor..)
            .ok_or_else(|| err("XML entity decode cursor 범위가 손상되었습니다."))?;
        let mut amp = None;
        for (relative, ch) in tail.char_indices() {
            if !is_valid_xml_char(ch) {
                return Err(err(format!(
                    "XML text: XML 1.0에서 허용되지 않는 문자가 포함되어 있습니다: U+{:04X}",
                    u32::from(ch)
                )));
            }
            if ch == '<' {
                return Err(err("XML text에 raw '<' 문자가 포함되어 있습니다."));
            }
            if ch == ']'
                && tail
                    .get(relative..)
                    .is_some_and(|remaining| remaining.starts_with("]]>"))
            {
                return Err(err(
                    "XML text에 허용되지 않는 ']]>' 시퀀스가 포함되어 있습니다.",
                ));
            }
            if ch == '&' {
                amp = Some(relative);
                break;
            }
        }
        let Some(relative_amp) = amp else {
            if let Some(out_text) = out.as_mut() {
                out_text.push_str(tail);
            }
            break;
        };
        let amp_index = cursor.strict_add(relative_amp);
        let after_amp = tail
            .get(relative_amp.strict_add(1)..)
            .ok_or_else(|| err("XML entity decode 범위가 손상되었습니다."))?;
        let Some((entity, _)) = after_amp.split_once(';') else {
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
                    parse_numeric_entity(
                        hex,
                        16,
                        "XML numeric hex entity가 16진수 형식이 아닙니다.",
                        "XML numeric hex entity 해석 실패",
                    )?
                } else {
                    parse_numeric_entity(
                        body,
                        10,
                        "XML numeric entity가 10진수 형식이 아닙니다.",
                        "XML numeric entity 해석 실패",
                    )?
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
        let out_text = if let Some(out_text) = out.as_mut() {
            out_text
        } else {
            let out_text =
                try_string_with_capacity(text.len(), "XML entity decode 메모리 확보 실패")?;
            out.insert(out_text)
        };
        out_text.push_str(
            text.get(cursor..amp_index)
                .ok_or_else(|| err("XML entity decode prefix 범위가 손상되었습니다."))?,
        );
        out_text.push(decoded_char);
        cursor = amp_index.strict_add(entity.len()).strict_add(2);
    }
    Ok(out.map_or(Cow::Borrowed(text), Cow::Owned))
}
fn parse_numeric_entity(
    value: &str,
    radix: u32,
    format_error: &'static str,
    parse_context: &'static str,
) -> Result<u32> {
    if value.starts_with('+') {
        return Err(err(format_error));
    }
    u32::from_str_radix(value, radix).map_err(|source| {
        if matches!(
            source.kind(),
            IntErrorKind::Empty | IntErrorKind::InvalidDigit
        ) {
            err(format_error)
        } else {
            err_with_source(parse_context, source)
        }
    })
}
pub(super) fn is_valid_xml_char(ch: char) -> bool {
    matches!(
        u32::from(ch),
        0x09 | 0x0A | 0x0D | 0x20..=0xD7FF | 0xE000..=0xFFFD | 0x0001_0000..=0x0010_FFFF
    )
}
fn local_tag_name(name: &str) -> &str {
    name.rsplit_once(':').map_or(name, |(_, local)| local)
}
