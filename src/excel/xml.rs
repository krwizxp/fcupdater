pub(super) fn extract_attr(tag: &str, attr_name: &str) -> Option<String> {
    let pattern = format!("{attr_name}=");
    let bytes = tag.as_bytes();
    let mut cursor = 0usize;
    while let Some(rel) = tag[cursor..].find(&pattern) {
        let idx = cursor + rel;
        if idx > 0 {
            let prev = *bytes.get(idx - 1)?;
            if !prev.is_ascii_whitespace() && prev != b'<' {
                cursor = idx + pattern.len();
                continue;
            }
        }
        let quote_idx = idx + pattern.len();
        let quote = *bytes.get(quote_idx)?;
        if quote != b'"' && quote != b'\'' {
            cursor = quote_idx;
            continue;
        }
        let value_start = quote_idx + 1;
        let value_end_rel = tag[value_start..].find(char::from(quote))?;
        let value_end = value_start + value_end_rel;
        return Some(decode_xml_entities(&tag[value_start..value_end]));
    }
    None
}
pub(super) fn find_start_tag(xml: &str, tag_name: &str, from: usize) -> Option<usize> {
    let mut cursor = from.min(xml.len());
    let wanted = local_tag_name(tag_name);
    while let Some(rel) = xml[cursor..].find('<') {
        let start = cursor + rel;
        let rest = xml.get(start + 1..)?;
        if rest.starts_with('/') || rest.starts_with('!') || rest.starts_with('?') {
            cursor = start + 1;
            continue;
        }
        let name_end_rel = rest
            .find(|ch: char| ch.is_ascii_whitespace() || ch == '/' || ch == '>')
            .unwrap_or(rest.len());
        let raw_name = rest.get(..name_end_rel)?;
        if !raw_name.is_empty() && local_tag_name(raw_name) == wanted {
            return Some(start);
        }
        cursor = start + 1;
    }
    None
}
pub(super) fn find_end_tag(xml: &str, tag_name: &str, from: usize) -> Option<usize> {
    let mut cursor = from.min(xml.len());
    let wanted = local_tag_name(tag_name);
    while let Some(rel) = xml[cursor..].find("</") {
        let start = cursor + rel;
        let rest = xml.get(start + 2..)?;
        let name_end_rel = rest
            .find(|ch: char| ch.is_ascii_whitespace() || ch == '>')
            .unwrap_or(rest.len());
        let raw_name = rest.get(..name_end_rel)?;
        if !raw_name.is_empty() && local_tag_name(raw_name) == wanted {
            return Some(start);
        }
        cursor = start + 2;
    }
    None
}
pub(super) fn find_tag_end(xml: &str, tag_start: usize) -> Option<usize> {
    xml.get(tag_start..)
        .and_then(|v| v.find('>'))
        .map(|rel| tag_start + rel)
}
pub(super) fn extract_first_tag_text(xml: &str, tag_name: &str) -> Option<String> {
    let open_pattern = format!("<{tag_name}");
    let open_start = xml.find(&open_pattern)?;
    let open_end = open_start + xml[open_start..].find('>')?;
    let body_start = open_end + 1;
    let close_pattern = format!("</{tag_name}>");
    let close_rel = xml[body_start..].find(&close_pattern)?;
    let body_end = body_start + close_rel;
    Some(xml[body_start..body_end].to_string())
}
pub(super) fn extract_all_tag_text(xml: &str, tag_name: &str) -> Option<String> {
    let open_pattern = format!("<{tag_name}");
    let close_pattern = format!("</{tag_name}>");
    let mut cursor = 0usize;
    let mut out = String::new();
    while let Some(open_rel) = xml[cursor..].find(&open_pattern) {
        let open_start = cursor + open_rel;
        let open_end = open_start + xml[open_start..].find('>')?;
        let body_start = open_end + 1;
        let close_rel = xml[body_start..].find(&close_pattern)?;
        let body_end = body_start + close_rel;
        out.push_str(&xml[body_start..body_end]);
        cursor = body_end + close_pattern.len();
    }
    if out.is_empty() { None } else { Some(out) }
}
pub(super) fn decode_xml_entities(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut i = 0usize;
    while i < s.len() {
        let rest = &s[i..];
        if rest.starts_with('&')
            && let Some(end_rel) = rest.find(';')
        {
            let end = i + end_rel;
            if end > i + 1
                && let Some(decoded) = decode_single_entity(&s[i + 1..end])
            {
                out.push(decoded);
                i = end + 1;
                continue;
            }
        }
        let mut chars = rest.chars();
        if let Some(ch) = chars.next() {
            out.push(ch);
            i += ch.len_utf8();
        } else {
            break;
        }
    }
    out
}
fn decode_single_entity(entity: &str) -> Option<char> {
    match entity {
        "lt" => Some('<'),
        "gt" => Some('>'),
        "quot" => Some('"'),
        "apos" => Some('\''),
        "amp" => Some('&'),
        _ => decode_numeric_entity(entity),
    }
}
fn decode_numeric_entity(entity: &str) -> Option<char> {
    if let Some(hex) = entity
        .strip_prefix("#x")
        .or_else(|| entity.strip_prefix("#X"))
    {
        return u32::from_str_radix(hex, 16).ok().and_then(char::from_u32);
    }
    if let Some(dec) = entity.strip_prefix('#') {
        return dec.parse::<u32>().ok().and_then(char::from_u32);
    }
    None
}
fn local_tag_name(name: &str) -> &str {
    name.rsplit(':').next().unwrap_or(name)
}
