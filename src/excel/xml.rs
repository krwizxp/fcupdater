use crate::diagnostic::{
    Result, bail, bail_fmt, ensure, ensure_fmt, err, err_src, fmt_err, or_abort, str_cap,
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
        anc_depth: usize,
        root_name: &str,
    ) -> Result<XmlElement<'xml>> {
        ensure(anc_depth < MAX_XML_NESTING_DEPTH, "XML 중첩 깊이 초과")?;
        let body_start = opening.end.strict_add(1);
        let (body_end, end) = if opening.self_closing {
            (body_start, body_start)
        } else {
            let mut sc = Self {
                cursor: body_start,
                xml: self.xml,
            };
            let (mut shallow, mut depth) = ([opening.name; 4], 1_usize);
            let closing = loop {
                let tag = sc
                    .next_tag()
                    .ok_or_else(|| fmt_err!("XML </{root_name}> 종료 태그 없음"))?;
                if tag.is_start {
                    if !tag.self_closing {
                        let next_depth = anc_depth.strict_add(depth);
                        ensure(next_depth < MAX_XML_NESTING_DEPTH, "XML 중첩 깊이 초과")?;
                        if let Some(slot) = shallow.get_mut(depth) {
                            *slot = tag.name;
                            depth = depth.strict_add(1);
                        } else {
                            sc.element_from_opening(tag, next_depth, root_name)?;
                        }
                    }
                    continue;
                }
                depth = depth.strict_sub(1);
                let open = or_abort(shallow.get(depth).copied());
                ensure_fmt!(open == tag.name, "XML 태그 불일치: {open} / {}", tag.name);
                if depth == 0 {
                    break tag;
                }
            };
            (closing.start, closing.end.strict_add(1))
        };
        let (body_span, span) = (
            Range {
                start: body_start,
                end: body_end,
            },
            Range {
                start: opening.start,
                end,
            },
        );
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
        let next = *bytes.get(tag_start.strict_add(1))?;
        if next == b'/' {
            return self
                .xml
                .get(tag_start.strict_add(2)..)?
                .find('>')
                .map(|off| tag_start.strict_add(2).strict_add(off));
        }
        if matches!(next, b'!' | b'?') {
            return if self.xml.get(tag_start..)?.starts_with("<!--") {
                find_markup_end(self.xml, tag_start, 4, "-->")
            } else if next == b'?' {
                find_markup_end(self.xml, tag_start, 2, "?>")
            } else {
                None
            };
        }
        let mut cur = tag_start.strict_add(1);
        loop {
            let tail = bytes.get(cur..)?;
            let rel = tail
                .iter()
                .position(|&ch| matches!(ch, b'>' | b'"' | b'\''))?;
            let delim = *tail.get(rel)?;
            cur = cur.strict_add(rel);
            if delim == b'>' {
                return Some(cur);
            }
            let q_start = cur.strict_add(1);
            cur = q_start
                .strict_add(bytes.get(q_start..)?.iter().position(|&ch| ch == delim)?)
                .strict_add(1);
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
        close_name: Option<&str>,
        ctx: &str,
    ) -> Result<Option<XmlElement<'xml>>> {
        let start = self.cursor;
        let opening = self.next_tag();
        self.validate_direct_gap(
            start,
            opening.as_ref().map_or(self.xml.len(), |tg| tg.start),
            ctx,
        )?;
        let Some(tag) = opening else {
            return close_name.map_or(Ok(None), |_| Err(fmt_err!("{ctx} XML root 종료 태그 없음")));
        };
        if !tag.is_start {
            if close_name == Some(tag.name)
                && self
                    .xml
                    .get(self.cursor..)
                    .is_some_and(|tail| xml_misc_only(tail, false))
            {
                return Ok(None);
            }
            bail_fmt!("{ctx} 직접 자식이 아닌 종료 태그: {}", tag.name);
        }
        self.element_from_opening(tag, 0, tag.name).map(Some)
    }
    pub(super) fn next_direct_element_named_until(
        &mut self,
        tag_name: &str,
        close_name: &str,
        anc_depth: usize,
        ctx: &str,
    ) -> Result<Option<XmlElement<'xml>>> {
        let Some(opening) = self.next_direct_opening_named_until(tag_name, close_name, ctx)? else {
            return Ok(None);
        };
        self.element_from_opening(opening, anc_depth, opening.name)
            .map(Some)
    }
    pub(super) fn next_direct_element_until(
        &mut self,
        root: &XmlTag<'_>,
        ctx: &str,
    ) -> Result<Option<XmlElement<'xml>>> {
        if root.self_closing {
            self.validate_direct_gap(self.cursor, self.xml.len(), ctx)?;
            return Ok(None);
        }
        self.next_direct_element_before(Some(root.name), ctx)
    }
    pub(super) fn next_direct_opening_named_until(
        &mut self,
        tag_name: &str,
        close_name: &str,
        ctx: &str,
    ) -> Result<Option<XmlTag<'xml>>> {
        let start = self.cursor;
        let opening = self
            .next_tag()
            .ok_or_else(|| fmt_err!("XML </{close_name}> 종료 태그 없음"))?;
        self.validate_direct_gap(start, opening.start, ctx)?;
        if !opening.is_start {
            if opening.name == close_name {
                return Ok(None);
            }
            bail_fmt!("XML 태그 불일치: {close_name} / {}", opening.name);
        }
        ensure_fmt!(
            opening.name == tag_name,
            "{ctx} 예상 외 자식 요소: {}",
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
            if bytes.get(name_start).is_some_and(|&ch| is_xml_ws(ch)) {
                return None;
            }
            let mut name_end = name_start;
            while bytes
                .get(name_end)
                .is_some_and(|&ch| !is_xml_ws(ch) && !matches!(ch, b'/' | b'>'))
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
                        .any(|ch| !is_xml_ws(ch)))
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
    fn validate_direct_gap(&self, start: usize, end: usize, ctx: &str) -> Result<()> {
        ensure_fmt!(
            xml_misc_only(or_abort(self.xml.get(start..end)), false),
            "{ctx} XML 요소 사이 내용 오류"
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
        let mut cur = tag
            .find('<')
            .ok_or("XML 태그 시작 문자 없음")?
            .strict_add(1);
        while bytes
            .get(cur)
            .is_some_and(|&ch| !is_xml_ws(ch) && !matches!(ch, b'/' | b'>'))
        {
            cur = cur.strict_add(1);
        }
        Ok(Self { cursor: cur, tag })
    }
    #[inline]
    pub(super) fn next(&mut self) -> Result<Option<(&'tag str, Cow<'tag, str>)>> {
        let bytes = self.tag.as_bytes();
        let sep_start = self.cursor;
        skip_xml_ws(bytes, &mut self.cursor);
        match bytes.get(self.cursor).copied() {
            Some(b'>') if self.cursor.strict_add(1) == bytes.len() => return Ok(None),
            Some(b'/') if self.tag.get(self.cursor..) == Some("/>") => return Ok(None),
            Some(b'/' | b'?' | b'>') | None => bail!("XML 태그 종료 형식 오류"),
            Some(_) if self.cursor == sep_start => bail!("XML 속성 사이 공백 없음"),
            Some(_) => {}
        }
        let name_start = self.cursor;
        while bytes
            .get(self.cursor)
            .is_some_and(|&ch| !is_xml_ws(ch) && !matches!(ch, b'=' | b'/' | b'?' | b'>'))
        {
            self.cursor = self.cursor.strict_add(1);
        }
        let name_end = self.cursor;
        ensure(name_start != name_end, "XML 속성 이름 비어 있음")?;
        skip_xml_ws(bytes, &mut self.cursor);
        ensure(bytes.get(self.cursor) == Some(&b'='), "XML 속성 '=' 없음")?;
        self.cursor = self.cursor.strict_add(1);
        skip_xml_ws(bytes, &mut self.cursor);
        let &quote = bytes.get(self.cursor).ok_or("XML 속성 값 quote 없음")?;
        ensure(matches!(quote, b'"' | b'\''), "XML 속성 값 quote 오류")?;
        let val_start = self.cursor.strict_add(1);
        let val_end = val_start.strict_add(
            or_abort(self.tag.get(val_start..))
                .find(char::from(quote))
                .ok_or("XML 속성 종료 quote 없음")?,
        );
        self.cursor = val_end.strict_add(1);
        Ok(Some((
            or_abort(self.tag.get(name_start..name_end)),
            decode_xml_entities(or_abort(self.tag.get(val_start..val_end)))?,
        )))
    }
    #[inline]
    pub(super) fn parse_into(
        tag: &'tag str,
        names: &[&str],
        values: &mut [Option<Cow<'tag, str>>],
        ctx: &str,
    ) -> Result<()> {
        let mut attrs = Self::new(tag)?;
        while let Some((name, val)) = attrs.next()? {
            let Some((_, slot)) = names
                .iter()
                .zip(values.iter_mut())
                .find(|&(cand, _)| *cand == name)
            else {
                bail_fmt!("{ctx} 알 수 없는 {name} 속성");
            };
            ensure_fmt!(slot.replace(val).is_none(), "{ctx} {name} 속성 중복");
        }
        Ok(())
    }
}
#[inline]
pub(super) fn decode_xml_entities(text: &str) -> Result<Cow<'_, str>> {
    if text
        .bytes()
        .all(|byte| (0x20..=0x7E).contains(&byte) && !matches!(byte, b'&' | b'<' | b']'))
    {
        return Ok(Cow::Borrowed(text));
    }
    let (mut out, mut cur): (Option<String>, usize) = (None, 0);
    while cur < text.len() {
        let tail = or_abort(text.get(cur..));
        let (mut amp, bytes) = (None, tail.as_bytes());
        for (rel, &byte) in bytes.iter().enumerate() {
            match byte {
                0x00..=0x1F => ensure_fmt!(
                    matches!(byte, b'\t' | b'\n' | b'\r'),
                    "XML 1.0 미허용 문자: U+{byte:04X}"
                ),
                b'&' => {
                    amp = Some(rel);
                    break;
                }
                b'<' => bail!("XML text에 raw '<' 포함"),
                b']' if bytes.get(rel.strict_add(1)..rel.strict_add(3)) == Some(b"]>") => {
                    bail!("XML text에 ']]>' 시퀀스 포함")
                }
                0xEF => {
                    if let Some(&[0xBF, third @ (0xBE | 0xBF)]) =
                        bytes.get(rel.strict_add(1)..rel.strict_add(3))
                    {
                        let code = if third == 0xBE {
                            0xFFFE_u32
                        } else {
                            0xFFFF_u32
                        };
                        bail_fmt!("XML 1.0 미허용 문자: U+{code:04X}");
                    }
                }
                _ => {}
            }
        }
        let Some(rel_amp) = amp else {
            if let Some(buf) = out.as_mut() {
                buf.push_str(tail);
            }
            break;
        };
        let amp_idx = cur.strict_add(rel_amp);
        let (entity, _) = or_abort(tail.get(rel_amp.strict_add(1)..))
            .split_once(';')
            .ok_or("XML entity 종료 세미콜론 없음")?;
        ensure(!entity.is_empty(), "XML entity 이름 비어 있음")?;
        let decoded = match entity {
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            "amp" => Some('&'),
            _ => {
                let body = entity
                    .strip_prefix('#')
                    .ok_or_else(|| fmt_err!("미지원 XML entity: &{entity};"))?;
                let (digits, radix, fmt_msg, parse_msg) = body.strip_prefix('x').map_or(
                    (
                        body,
                        10_u32,
                        "XML numeric entity 10진수 오류",
                        "XML numeric entity 해석 실패",
                    ),
                    |hex| {
                        (
                            hex,
                            16_u32,
                            "XML numeric hex entity 16진수 오류",
                            "XML hex entity 해석 실패",
                        )
                    },
                );
                ensure(!digits.starts_with('+'), fmt_msg)?;
                let val = u32::from_str_radix(digits, radix).map_err(|src| {
                    if matches!(src.kind(), IntErrorKind::Empty | IntErrorKind::InvalidDigit) {
                        err(fmt_msg)
                    } else {
                        err_src(parse_msg, src)
                    }
                })?;
                char::from_u32(val)
            }
        };
        let ch =
            decoded.ok_or_else(|| fmt_err!("XML numeric entity 잘못된 Unicode: &{entity};"))?;
        ensure_fmt!(
            (ch >= ' ' && !matches!(ch, '\u{fffe}' | '\u{ffff}'))
                || matches!(ch, '\t' | '\n' | '\r'),
            "XML numeric entity 범위 오류: &{entity};"
        );
        let buf = match out.as_mut() {
            Some(existing) => existing,
            None => out.insert(str_cap(text.len(), "XML entity decode")?),
        };
        buf.push_str(or_abort(text.get(cur..amp_idx)));
        buf.push(ch);
        cur = amp_idx.strict_add(entity.len()).strict_add(2);
    }
    Ok(out.map_or(Cow::Borrowed(text), Cow::Owned))
}
pub(super) fn extract_first_tag_text<'xml>(
    xml: &'xml str,
    tag_name: &str,
) -> Result<Option<&'xml str>> {
    XmlScanner::new(xml)
        .next_element_named(tag_name)
        .map(|el| el.map(|item| item.body))
}
fn find_markup_end(xml: &str, start: usize, opener_len: usize, term: &str) -> Option<usize> {
    let s_start = start.strict_add(opener_len);
    let rel_end = xml.get(s_start..)?.find(term)?;
    if term == "-->"
        && xml
            .get(s_start..s_start.strict_add(rel_end))
            .is_some_and(|body| body.contains("--") || body.ends_with('-'))
    {
        return None;
    }
    Some(
        s_start
            .strict_add(rel_end)
            .strict_add(term.len())
            .strict_sub(1),
    )
}
const fn is_xml_ws(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r')
}
fn local_tag_name(name: &str) -> &str {
    name.rsplit_once(':').map_or(name, |(_, local)| local)
}
fn skip_xml_ws(bytes: &[u8], cursor: &mut usize) {
    while bytes.get(*cursor).is_some_and(|&ch| is_xml_ws(ch)) {
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
        let (op_len, term) = if xml.starts_with("<!--") {
            (4, "-->")
        } else if xml.starts_with("<?") {
            (2, "?>")
        } else {
            return false;
        };
        let Some(next) = find_markup_end(xml, 0, op_len, term).map(|end| end.strict_add(1)) else {
            return false;
        };
        xml = or_abort(xml.get(next..));
    }
}
