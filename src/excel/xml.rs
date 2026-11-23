use crate::diagnostic::{
    Result, bail, bail_fmt, ensure, ensure_fmt, err, err_with_source, fmt_err, or_abort,
    try_string_with_capacity,
};
use alloc::borrow::Cow;
use core::{iter, num::IntErrorKind, range::Range};
pub(super) const MAX_XML_NESTING_DEPTH: usize = 64;
#[derive(Clone, Copy)]
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
        root_name: &str,
    ) -> Result<XmlElement<'xml>> {
        ensure(
            ancestor_depth < MAX_XML_NESTING_DEPTH,
            "XML 중첩 깊이가 너무 큽니다.",
        )?;
        let body_start = opening.end.strict_add(1);
        let (body_end, end) = if opening.self_closing {
            (body_start, body_start)
        } else {
            let mut scanner = Self {
                cursor: body_start,
                xml: self.xml,
            };
            let (mut shallow, mut depth) = ([opening.name; 4], 1_usize);
            let closing = loop {
                let tag = scanner
                    .next_tag()
                    .ok_or_else(|| fmt_err!("XML </{root_name}> 종료 태그를 찾지 못했습니다."))?;
                if tag.is_start {
                    if !tag.self_closing {
                        let next_depth = ancestor_depth.strict_add(depth);
                        ensure(
                            next_depth < MAX_XML_NESTING_DEPTH,
                            "XML 중첩 깊이가 너무 큽니다.",
                        )?;
                        if let Some(slot) = shallow.get_mut(depth) {
                            *slot = tag.name;
                            depth = depth.strict_add(1);
                        } else {
                            scanner.element_from_opening(tag, next_depth, root_name)?;
                        }
                    }
                    continue;
                }
                depth = depth.strict_sub(1);
                let open = or_abort(shallow.get(depth).copied());
                ensure_fmt!(
                    open == tag.name,
                    "XML 태그 쌍이 일치하지 않습니다: {open} / {}",
                    tag.name
                );
                if depth == 0 {
                    break tag;
                }
            };
            (closing.start, closing.end.strict_add(1))
        };
        let body_span = Range {
            start: body_start,
            end: body_end,
        };
        let span = Range {
            start: opening.start,
            end,
        };
        self.skip_to(end);
        Ok(XmlElement {
            body: or_abort(self.xml.get(body_span)),
            body_span,
            opening,
            span,
        })
    }
    fn find_tag_end(&self, tag_start: usize) -> Option<usize> {
        let bytes = self.xml.as_bytes();
        let next_byte = *bytes.get(tag_start.strict_add(1))?;
        if next_byte == b'/' {
            return self
                .xml
                .get(tag_start.strict_add(2)..)?
                .find('>')
                .map(|offset| tag_start.strict_add(2).strict_add(offset));
        }
        if matches!(next_byte, b'!' | b'?') {
            let tail = self.xml.get(tag_start..)?;
            return if tail.starts_with("<!--") {
                find_delimited_markup_end(self.xml, tag_start, 4, "-->")
            } else if next_byte == b'?' {
                find_delimited_markup_end(self.xml, tag_start, 2, "?>")
            } else {
                None
            };
        }
        let mut cursor = tag_start.strict_add(1);
        loop {
            let tail = bytes.get(cursor..)?;
            let relative = tail
                .iter()
                .position(|&byte| matches!(byte, b'>' | b'"' | b'\''))?;
            let delimiter = *tail.get(relative)?;
            cursor = cursor.strict_add(relative);
            if delimiter == b'>' {
                return Some(cursor);
            }
            let quoted_start = cursor.strict_add(1);
            let quote_end = bytes
                .get(quoted_start..)?
                .iter()
                .position(|&byte| byte == delimiter)?;
            cursor = quoted_start.strict_add(quote_end).strict_add(1);
        }
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
        self.validate_direct_gap(
            content_start,
            opening.as_ref().map_or(self.xml.len(), |tag| tag.start),
            context,
        )?;
        let Some(opening_tag) = opening else {
            return closing_name.map_or(Ok(None), |_| {
                Err(fmt_err!("{context}의 XML root 종료 태그가 없습니다."))
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
            bail_fmt!(
                "{context}에 직접 자식이 아닌 종료 태그가 있습니다: {}",
                opening_tag.name
            );
        }
        self.element_from_opening(opening_tag, 0, opening_tag.name)
            .map(Some)
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
        self.element_from_opening(opening, ancestor_depth, opening.name)
            .map(Some)
    }
    pub(super) fn next_direct_element_until(
        &mut self,
        root: &XmlTag<'_>,
        context: &str,
    ) -> Result<Option<XmlElement<'xml>>> {
        if root.self_closing {
            self.validate_direct_gap(self.cursor, self.xml.len(), context)?;
            return Ok(None);
        }
        self.next_direct_element_before(Some(root.name), context)
    }
    pub(super) fn next_direct_opening_named_until(
        &mut self,
        tag_name: &str,
        closing_name: &str,
        context: &str,
    ) -> Result<Option<XmlTag<'xml>>> {
        let content_start = self.cursor;
        let opening = self
            .next_tag()
            .ok_or_else(|| fmt_err!("XML </{closing_name}> 종료 태그를 찾지 못했습니다."))?;
        self.validate_direct_gap(content_start, opening.start, context)?;
        if !opening.is_start {
            if opening.name == closing_name {
                return Ok(None);
            }
            bail_fmt!(
                "XML 태그 쌍이 일치하지 않습니다: {closing_name} / {}",
                opening.name
            );
        }
        ensure_fmt!(
            opening.name == tag_name,
            "{context}에 직접 자식 {tag_name} 외 요소가 있습니다: {}",
            opening.name
        );
        Ok(Some(opening))
    }
    pub(super) fn next_element_named(
        &mut self,
        tag_name: &str,
    ) -> Result<Option<XmlElement<'xml>>> {
        let Some(opening) = self.next_start_named(tag_name) else {
            return Ok(None);
        };
        self.element_from_opening(opening, 0, opening.name)
            .map(Some)
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
            let mut name_start = start.strict_add(1);
            let bytes = self.xml.as_bytes();
            let is_start = match *bytes.get(name_start)? {
                b'/' => {
                    name_start = name_start.strict_add(1);
                    false
                }
                b'?' | b'!' => continue,
                _ => true,
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
            if name.is_empty()
                || (!is_start
                    && self
                        .xml
                        .get(name_end..end)?
                        .bytes()
                        .any(|byte| !is_xml_whitespace(byte)))
            {
                return None;
            }
            return Some(XmlTag {
                end,
                is_start,
                local_name: local_tag_name(name),
                name,
                raw: self.xml.get(start..end.strict_add(1))?,
                self_closing: bytes.get(end.strict_sub(1)) == Some(&b'/'),
                start,
            });
        }
        None
    }
    pub(super) fn skip_to(&mut self, cursor: usize) {
        self.cursor = cursor.min(self.xml.len());
    }
    fn validate_direct_gap(&self, start: usize, end: usize, context: &str) -> Result<()> {
        ensure_fmt!(
            xml_misc_only(or_abort(self.xml.get(start..end)), false),
            "{context}의 XML 요소 사이 내용이 올바르지 않습니다."
        );
        Ok(())
    }
}
impl<'tag> XmlAttrScanner<'tag> {
    pub(super) const fn from_tag(tag: &XmlTag<'tag>) -> Self {
        Self {
            cursor: tag.name.len().strict_add(1),
            tag: tag.raw,
        }
    }
    pub(super) fn new(tag: &'tag str) -> Result<Self> {
        let bytes = tag.as_bytes();
        let mut cursor = tag
            .find('<')
            .ok_or("XML 태그 시작 문자를 찾지 못했습니다.")?
            .strict_add(1);
        while bytes
            .get(cursor)
            .is_some_and(|&byte| !is_xml_whitespace(byte) && !matches!(byte, b'/' | b'>'))
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
            Some(b'/' | b'?' | b'>') | None => bail!("XML 태그 종료 형식이 올바르지 않습니다."),
            Some(_) if self.cursor == separator_start => {
                bail!("XML 속성 사이에 공백이 없습니다.");
            }
            Some(_) => {}
        }
        let name_start = self.cursor;
        while bytes.get(self.cursor).is_some_and(|&byte| {
            !is_xml_whitespace(byte) && !matches!(byte, b'=' | b'/' | b'?' | b'>')
        }) {
            self.cursor = self.cursor.strict_add(1);
        }
        let name_end = self.cursor;
        ensure(name_start != name_end, "XML 속성 이름이 비어 있습니다.")?;
        skip_xml_whitespace(bytes, &mut self.cursor);
        ensure(
            bytes.get(self.cursor) == Some(&b'='),
            "XML 속성의 '=' 문자를 찾지 못했습니다.",
        )?;
        self.cursor = self.cursor.strict_add(1);
        skip_xml_whitespace(bytes, &mut self.cursor);
        let &quote = bytes
            .get(self.cursor)
            .ok_or("XML 속성 값 quote 문자를 찾지 못했습니다.")?;
        ensure(
            matches!(quote, b'"' | b'\''),
            "XML 속성 값 quote 문자가 올바르지 않습니다.",
        )?;
        let value_start = self.cursor.strict_add(1);
        let value_end = value_start.strict_add(
            or_abort(self.tag.get(value_start..))
                .find(char::from(quote))
                .ok_or("XML 속성 값 종료 quote를 찾지 못했습니다.")?,
        );
        self.cursor = value_end.strict_add(1);
        Ok(Some((
            or_abort(self.tag.get(name_start..name_end)),
            decode_xml_entities(or_abort(self.tag.get(value_start..value_end)))?,
        )))
    }
}
pub(super) fn decode_xml_entities(text: &str) -> Result<Cow<'_, str>> {
    let (mut out, mut cursor): (Option<String>, usize) = (None, 0);
    while cursor < text.len() {
        let tail = or_abort(text.get(cursor..));
        let mut amp = None;
        let tail_bytes = tail.as_bytes();
        for (relative, &byte) in tail_bytes.iter().enumerate() {
            match byte {
                0x00..=0x1F => ensure_fmt!(
                    matches!(byte, b'\t' | b'\n' | b'\r'),
                    "XML text: XML 1.0에서 허용되지 않는 문자가 포함되어 있습니다: U+{byte:04X}"
                ),
                b'&' => {
                    amp = Some(relative);
                    break;
                }
                b'<' => bail!("XML text에 raw '<' 문자가 포함되어 있습니다."),
                b']' if tail_bytes
                    .get(relative.strict_add(1)..relative.strict_add(3))
                    .is_some_and(|next| next == b"]>") =>
                {
                    bail!("XML text에 허용되지 않는 ']]>' 시퀀스가 포함되어 있습니다.");
                }
                0xEF => {
                    if let Some(&[0xBF, third @ (0xBE | 0xBF)]) =
                        tail_bytes.get(relative.strict_add(1)..relative.strict_add(3))
                    {
                        let code = if third == 0xBE {
                            0xFFFE_u32
                        } else {
                            0xFFFF_u32
                        };
                        bail_fmt!(
                            "XML text: XML 1.0에서 허용되지 않는 문자가 포함되어 있습니다: U+{code:04X}"
                        );
                    }
                }
                _ => {}
            }
        }
        let Some(relative_amp) = amp else {
            if let Some(out_text) = out.as_mut() {
                out_text.push_str(tail);
            }
            break;
        };
        let amp_index = cursor.strict_add(relative_amp);
        let (entity, _) = or_abort(tail.get(relative_amp.strict_add(1)..))
            .split_once(';')
            .ok_or("XML entity 종료 세미콜론을 찾지 못했습니다.")?;
        ensure(!entity.is_empty(), "XML entity 이름이 비어 있습니다.")?;
        let decoded = match entity {
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            "amp" => Some('&'),
            _ => {
                let body = entity
                    .strip_prefix('#')
                    .ok_or_else(|| fmt_err!("지원하지 않는 XML entity입니다: &{entity};"))?;
                let (digits, radix, fmt_msg, parse_msg) = body.strip_prefix('x').map_or(
                    (
                        body,
                        10_u32,
                        "XML numeric entity가 10진수 형식이 아닙니다.",
                        "XML numeric entity 해석 실패",
                    ),
                    |hex| {
                        (
                            hex,
                            16_u32,
                            "XML numeric hex entity가 16진수 형식이 아닙니다.",
                            "XML numeric hex entity 해석 실패",
                        )
                    },
                );
                ensure(!digits.starts_with('+'), fmt_msg)?;
                let value = u32::from_str_radix(digits, radix).map_err(|source| {
                    if matches!(
                        source.kind(),
                        IntErrorKind::Empty | IntErrorKind::InvalidDigit
                    ) {
                        err(fmt_msg)
                    } else {
                        err_with_source(parse_msg, source)
                    }
                })?;
                char::from_u32(value)
            }
        };
        let decoded_char = decoded.ok_or_else(|| {
            fmt_err!("XML numeric entity가 유효한 Unicode scalar value가 아닙니다: &{entity};")
        })?;
        ensure_fmt!(
            (decoded_char >= ' ' && !matches!(decoded_char, '\u{fffe}' | '\u{ffff}'))
                || matches!(decoded_char, '\t' | '\n' | '\r'),
            "XML numeric entity가 XML 1.0 유효 문자 범위를 벗어났습니다: &{entity};"
        );
        let out_text = match out.as_mut() {
            Some(existing) => existing,
            None => out.insert(try_string_with_capacity(
                text.len(),
                "XML entity decode 메모리 확보 실패",
            )?),
        };
        out_text.push_str(or_abort(text.get(cursor..amp_index)));
        out_text.push(decoded_char);
        cursor = amp_index.strict_add(entity.len()).strict_add(2);
    }
    Ok(out.map_or(Cow::Borrowed(text), Cow::Owned))
}
pub(super) fn extract_first_tag_text<'xml>(
    xml: &'xml str,
    tag_name: &str,
) -> Result<Option<&'xml str>> {
    XmlScanner::new(xml)
        .next_element_named(tag_name)
        .map(|maybe_element| maybe_element.map(|element| element.body))
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
const fn is_xml_whitespace(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}
fn local_tag_name(name: &str) -> &str {
    name.rsplit_once(':').map_or(name, |(_, local)| local)
}
fn skip_xml_whitespace(bytes: &[u8], cursor: &mut usize) {
    while bytes
        .get(*cursor)
        .is_some_and(|&byte| is_xml_whitespace(byte))
    {
        *cursor = cursor.strict_add(1);
    }
}
pub(super) fn xml_misc_only(mut xml: &str, allow_bom: bool) -> bool {
    if allow_bom {
        xml = xml.trim_prefix('\u{feff}');
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
        xml = or_abort(xml.get(next..));
    }
}
