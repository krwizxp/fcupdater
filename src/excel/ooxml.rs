use super::{
    path_util::path_from_slashes,
    xlsx_container::XlsxContainer,
    xml::{
        decode_xml_entities, extract_all_tag_text, extract_attr, find_end_tag, find_start_tag,
        find_tag_end,
    },
};
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
    while let Some(si_start) = find_start_tag(xml, "si", cursor) {
        let Some(si_tag_end) = find_tag_end(xml, si_start) else {
            break;
        };
        let body_start = si_tag_end + 1;
        let Some(si_end) = find_end_tag(xml, "si", body_start) else {
            break;
        };
        let si_body = &xml[body_start..si_end];
        let text = extract_all_tag_text(si_body, "t")
            .map(|v| decode_xml_entities(&v))
            .unwrap_or_default();
        out.push(text);
        cursor = si_end + "</si>".len();
    }
    out
}
fn iter_start_tags<'a>(xml: &'a str, tag_name: &str) -> Vec<&'a str> {
    let mut out = vec![];
    let mut cursor = 0usize;
    while let Some(start) = find_start_tag(xml, tag_name, cursor) {
        let Some(end) = find_tag_end(xml, start) else {
            break;
        };
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
            Component::ParentDir => {
                normalized.pop();
            }
            Component::Normal(s) => normalized.push(s),
            Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
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
