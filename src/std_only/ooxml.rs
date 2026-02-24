use super::xlsx_container::XlsxContainer;
use crate::{Result, err};
use std::{
    collections::HashMap,
    path::{Component, Path, PathBuf},
};
#[derive(Debug, Clone, Default)]
pub struct SheetCatalog {
    pub sheet_name_to_path: HashMap<String, String>,
    pub sheet_order: Vec<String>,
}
pub fn load_sheet_catalog(container: &XlsxContainer) -> Result<SheetCatalog> {
    let workbook_xml = container.read_text("xl/workbook.xml")?;
    let rels_xml = container.read_text("xl/_rels/workbook.xml.rels")?;
    let rid_to_target = parse_relationship_targets(&rels_xml);
    let mut sheet_name_to_path = HashMap::new();
    let mut sheet_order = Vec::new();
    for tag in iter_start_tags(&workbook_xml, "sheet") {
        let Some(name) = extract_attr(tag, "name") else {
            continue;
        };
        let Some(rid) = extract_attr(tag, "r:id") else {
            continue;
        };
        let Some(target) = rid_to_target.get(&rid) else {
            continue;
        };
        let resolved = resolve_ooxml_target("xl/workbook.xml", target);
        sheet_order.push(name.clone());
        sheet_name_to_path.insert(name, resolved);
    }
    if sheet_name_to_path.is_empty() {
        return Err(err("workbook에서 시트 정보를 찾지 못했습니다."));
    }
    Ok(SheetCatalog {
        sheet_name_to_path,
        sheet_order,
    })
}
pub fn load_sheet_xml(
    container: &XlsxContainer,
    catalog: &SheetCatalog,
    sheet_name: &str,
) -> Result<String> {
    let Some(path) = catalog.sheet_name_to_path.get(sheet_name) else {
        return Err(err(format!("시트를 찾지 못했습니다: {sheet_name}")));
    };
    container.read_text(path)
}
pub fn load_shared_strings(container: &XlsxContainer) -> Result<Vec<String>> {
    let path = container
        .unpack_dir()
        .join(path_from_slashes("xl/sharedStrings.xml"));
    if !path.exists() {
        return Ok(vec![]);
    }
    let xml = container.read_text("xl/sharedStrings.xml")?;
    Ok(parse_shared_strings_xml(&xml))
}
pub fn get_cell_display_value(
    sheet_xml: &str,
    shared_strings: &[String],
    cell_ref: &str,
) -> Option<String> {
    let (start_tag, body) = find_cell_block(sheet_xml, cell_ref)?;
    let cell_type = extract_attr(start_tag, "t");
    if matches!(cell_type.as_deref(), Some("inlineStr")) {
        return extract_all_tag_text(body, "t").map(|v| decode_xml_entities(&v));
    }
    let raw_value = extract_first_tag_text(body, "v")?;
    let decoded = decode_xml_entities(&raw_value);
    if matches!(cell_type.as_deref(), Some("s")) {
        let idx = decoded.parse::<usize>().ok()?;
        return shared_strings.get(idx).cloned();
    }
    Some(decoded)
}
fn parse_relationship_targets(rels_xml: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for tag in iter_start_tags(rels_xml, "Relationship") {
        let Some(id) = extract_attr(tag, "Id") else {
            continue;
        };
        let Some(target) = extract_attr(tag, "Target") else {
            continue;
        };
        map.insert(id, target);
    }
    map
}
fn parse_shared_strings_xml(xml: &str) -> Vec<String> {
    let mut out = vec![];
    let mut cursor = 0usize;
    while let Some(si_start_rel) = xml[cursor..].find("<si") {
        let si_start = cursor + si_start_rel;
        let Some(si_tag_end_rel) = xml[si_start..].find('>') else {
            break;
        };
        let body_start = si_start + si_tag_end_rel + 1;
        let Some(si_end_rel) = xml[body_start..].find("</si>") else {
            break;
        };
        let si_body_end = body_start + si_end_rel;
        let si_body = &xml[body_start..si_body_end];
        let text = extract_all_tag_text(si_body, "t")
            .map(|v| decode_xml_entities(&v))
            .unwrap_or_default();
        out.push(text);
        cursor = si_body_end + "</si>".len();
    }
    out
}
fn find_cell_block<'a>(sheet_xml: &'a str, cell_ref: &str) -> Option<(&'a str, &'a str)> {
    let pattern = format!("r=\"{cell_ref}\"");
    if let Some(found_rel) = sheet_xml.find(&pattern) {
        let found = found_rel;
        let open_start = sheet_xml[..found].rfind("<c")?;
        let open_end = open_start + sheet_xml[open_start..].find('>')?;
        let start_tag = &sheet_xml[open_start..=open_end];
        if start_tag.ends_with("/>") {
            return Some((start_tag, ""));
        }
        let body_start = open_end + 1;
        let close_rel = sheet_xml[body_start..].find("</c>")?;
        let body_end = body_start + close_rel;
        let body = &sheet_xml[body_start..body_end];
        return Some((start_tag, body));
    }
    None
}
fn extract_attr(tag: &str, attr_name: &str) -> Option<String> {
    let pattern = format!("{attr_name}=");
    let mut cursor = 0usize;
    while let Some(rel) = tag[cursor..].find(&pattern) {
        let idx = cursor + rel;
        if idx > 0
            && !tag.as_bytes()[idx - 1].is_ascii_whitespace()
            && tag.as_bytes()[idx - 1] != b'<'
        {
            cursor = idx + pattern.len();
            continue;
        }
        let quote_idx = idx + pattern.len();
        let quote = *tag.as_bytes().get(quote_idx)?;
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
fn extract_first_tag_text(xml: &str, tag_name: &str) -> Option<String> {
    let open_pattern = format!("<{tag_name}");
    let start = xml.find(&open_pattern)?;
    let open_end = start + xml[start..].find('>')?;
    let content_start = open_end + 1;
    let close_pattern = format!("</{tag_name}>");
    let close_rel = xml[content_start..].find(&close_pattern)?;
    let close = content_start + close_rel;
    Some(xml[content_start..close].to_string())
}
fn extract_all_tag_text(xml: &str, tag_name: &str) -> Option<String> {
    let open_pattern = format!("<{tag_name}");
    let close_pattern = format!("</{tag_name}>");
    let mut cursor = 0usize;
    let mut out = String::new();
    while let Some(start_rel) = xml[cursor..].find(&open_pattern) {
        let start = cursor + start_rel;
        let open_end = start + xml[start..].find('>')?;
        let content_start = open_end + 1;
        let close_rel = xml[content_start..].find(&close_pattern)?;
        let close = content_start + close_rel;
        out.push_str(&xml[content_start..close]);
        cursor = close + close_pattern.len();
    }
    if out.is_empty() { None } else { Some(out) }
}
fn decode_xml_entities(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}
fn iter_start_tags<'a>(xml: &'a str, tag_name: &str) -> Vec<&'a str> {
    let pattern = format!("<{tag_name}");
    let mut out = vec![];
    let mut cursor = 0usize;
    while let Some(rel) = xml[cursor..].find(&pattern) {
        let start = cursor + rel;
        let after = xml[start + pattern.len()..].chars().next();
        if !matches!(after, Some(' ' | '\t' | '\n' | '\r' | '/' | '>')) {
            cursor = start + pattern.len();
            continue;
        }
        let Some(end_rel) = xml[start..].find('>') else {
            break;
        };
        let end = start + end_rel;
        out.push(&xml[start..=end]);
        cursor = end + 1;
    }
    out
}
fn resolve_ooxml_target(base_file: &str, target: &str) -> String {
    if target.starts_with('/') {
        return target.trim_start_matches('/').to_string();
    }
    let mut base = PathBuf::from(base_file);
    base.pop();
    let combined = base.join(path_from_slashes(target));
    normalize_path(&combined)
}
fn normalize_path(path: &Path) -> String {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(s) => normalized.push(s),
            Component::RootDir | Component::Prefix(_) => {}
        }
    }
    normalized
        .components()
        .filter_map(|c| match c {
            Component::Normal(v) => Some(v.to_string_lossy().to_string()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}
fn path_from_slashes(s: &str) -> PathBuf {
    let mut out = PathBuf::new();
    for segment in s.split('/') {
        if segment.is_empty() {
            continue;
        }
        out.push(segment);
    }
    out
}
