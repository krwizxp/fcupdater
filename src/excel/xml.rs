const fn checked_offset_add(base: usize, add: usize) -> Option<usize> {
    base.checked_add(add)
}
pub(in crate::excel) fn extract_attr(tag: &str, attr_name: &str) -> Option<String> {
    let capacity = attr_name.len().saturating_add(1);
    let mut pattern = String::with_capacity(capacity);
    pattern.push_str(attr_name);
    pattern.push('=');
    let bytes = tag.as_bytes();
    let mut cursor = 0_usize;
    while let Some(rel) = tag.get(cursor..)?.find(&pattern) {
        let idx = checked_offset_add(cursor, rel)?;
        if idx > 0 {
            let prev = *bytes.get(idx.checked_sub(1)?)?;
            if !prev.is_ascii_whitespace() && prev != b'<' {
                cursor = checked_offset_add(idx, pattern.len())?;
                continue;
            }
        }
        let quote_idx = checked_offset_add(idx, pattern.len())?;
        let quote = *bytes.get(quote_idx)?;
        if quote != b'"' && quote != b'\'' {
            cursor = quote_idx;
            continue;
        }
        let value_start = checked_offset_add(quote_idx, 1)?;
        let value_end_rel = tag.get(value_start..)?.find(char::from(quote))?;
        let value_end = checked_offset_add(value_start, value_end_rel)?;
        return Some(decode_xml_entities(tag.get(value_start..value_end)?));
    }
    None
}
pub(in crate::excel) fn find_start_tag(xml: &str, tag_name: &str, from: usize) -> Option<usize> {
    let mut cursor = from.min(xml.len());
    let wanted = local_tag_name(tag_name);
    while let Some(rel) = xml.get(cursor..)?.find('<') {
        let start = checked_offset_add(cursor, rel)?;
        let rest = xml.get(checked_offset_add(start, 1)?..)?;
        if rest.starts_with('/') || rest.starts_with('!') || rest.starts_with('?') {
            cursor = checked_offset_add(start, 1)?;
            continue;
        }
        let name_end_rel = rest
            .find(|ch: char| ch.is_ascii_whitespace() || ch == '/' || ch == '>')
            .unwrap_or(rest.len());
        let raw_name = rest.get(..name_end_rel)?;
        if !raw_name.is_empty() && local_tag_name(raw_name) == wanted {
            return Some(start);
        }
        cursor = checked_offset_add(start, 1)?;
    }
    None
}
pub(in crate::excel) fn find_end_tag(xml: &str, tag_name: &str, from: usize) -> Option<usize> {
    let mut cursor = from.min(xml.len());
    let wanted = local_tag_name(tag_name);
    while let Some(rel) = xml.get(cursor..)?.find("</") {
        let start = checked_offset_add(cursor, rel)?;
        let rest = xml.get(checked_offset_add(start, 2)?..)?;
        let name_end_rel = rest
            .find(|ch: char| ch.is_ascii_whitespace() || ch == '>')
            .unwrap_or(rest.len());
        let raw_name = rest.get(..name_end_rel)?;
        if !raw_name.is_empty() && local_tag_name(raw_name) == wanted {
            return Some(start);
        }
        cursor = checked_offset_add(start, 2)?;
    }
    None
}
pub(in crate::excel) fn find_tag_end(xml: &str, tag_start: usize) -> Option<usize> {
    let rel = xml.get(tag_start..)?.find('>')?;
    checked_offset_add(tag_start, rel)
}
pub(in crate::excel) fn extract_first_tag_text(xml: &str, tag_name: &str) -> Option<String> {
    let open_start = find_start_tag(xml, tag_name, 0)?;
    let open_end = find_tag_end(xml, open_start)?;
    let body_start = checked_offset_add(open_end, 1)?;
    let body_end = find_end_tag(xml, tag_name, body_start)?;
    let text = xml.get(body_start..body_end)?;
    let capacity = text.len();
    let mut out = String::with_capacity(capacity);
    out.push_str(text);
    Some(out)
}
pub(in crate::excel) fn extract_all_tag_text(xml: &str, tag_name: &str) -> Option<String> {
    let mut cursor = 0_usize;
    let capacity = xml.len().min(128);
    let mut out = String::with_capacity(capacity);
    while let Some(open_start) = find_start_tag(xml, tag_name, cursor) {
        let open_end = find_tag_end(xml, open_start)?;
        let body_start = checked_offset_add(open_end, 1)?;
        let body_end = find_end_tag(xml, tag_name, body_start)?;
        out.push_str(xml.get(body_start..body_end)?);
        let close_tag_len = checked_offset_add(tag_name.len(), 3)?;
        cursor = checked_offset_add(body_end, close_tag_len)?;
    }
    if out.is_empty() { None } else { Some(out) }
}
pub(in crate::excel) fn decode_xml_entities(text: &str) -> String {
    if !text.contains('&') {
        return text.to_owned();
    }
    let capacity = text.len();
    let mut out = String::with_capacity(capacity);
    let mut i = 0_usize;
    while i < text.len() {
        let Some(rest) = text.get(i..) else {
            break;
        };
        if rest.starts_with('&')
            && let Some(end_rel) = rest.find(';')
        {
            let Some(end) = checked_offset_add(i, end_rel) else {
                break;
            };
            let Some(entity_start) = checked_offset_add(i, 1) else {
                break;
            };
            if end > entity_start
                && let Some(entity) = text.get(entity_start..end)
            {
                let decoded = match entity {
                    "lt" => Some('<'),
                    "gt" => Some('>'),
                    "quot" => Some('"'),
                    "apos" => Some('\''),
                    "amp" => Some('&'),
                    _ => entity
                        .strip_prefix("#x")
                        .or_else(|| entity.strip_prefix("#X"))
                        .map_or_else(
                            || {
                                entity
                                    .strip_prefix('#')
                                    .and_then(|dec| dec.parse::<u32>().ok())
                                    .and_then(char::from_u32)
                            },
                            |hex| u32::from_str_radix(hex, 16).ok().and_then(char::from_u32),
                        ),
                };
                if let Some(decoded_char) = decoded {
                    out.push(decoded_char);
                    let Some(next_i) = checked_offset_add(end, 1) else {
                        break;
                    };
                    i = next_i;
                    continue;
                }
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
    out
}
fn local_tag_name(name: &str) -> &str {
    name.rsplit(':').next().unwrap_or(name)
}
