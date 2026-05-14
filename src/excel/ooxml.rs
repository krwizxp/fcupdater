use super::{
    path_util::path_from_slashes,
    xlsx_container::XlsxContainer,
    xml::{
        decode_xml_entities, extract_all_tag_text, extract_attr, find_end_tag, find_start_tag,
        find_tag_end,
    },
};
use crate::{Result, err, err_with_source};
use std::{
    collections::HashMap,
    path::{Component, PathBuf},
};
#[derive(Debug, Clone, Default)]
pub struct SheetCatalog {
    pub sheet_name_to_path: HashMap<String, String>,
    pub sheet_order: Vec<String>,
}
pub fn load_sheet_catalog(container: &XlsxContainer) -> Result<SheetCatalog> {
    let workbook_xml = container.read_text("xl/workbook.xml")?;
    let rels_xml = container.read_text("xl/_rels/workbook.xml.rels")?;
    let relationship_count = rels_xml.matches("<Relationship").count();
    let mut rid_to_target: HashMap<String, String> = HashMap::new();
    rid_to_target
        .try_reserve(relationship_count)
        .map_err(|source| {
            err_with_source(
                format!("workbook 관계 맵 메모리 확보 실패: {relationship_count} entries"),
                source,
            )
        })?;
    for tag in iter_start_tags(&rels_xml, "Relationship")? {
        let Some(id) = extract_attr(tag, "Id") else {
            continue;
        };
        let Some(target) = extract_attr(tag, "Target") else {
            continue;
        };
        rid_to_target.insert(id, target);
    }
    let sheet_count = workbook_xml.matches("<sheet").count();
    let mut sheet_name_to_path: HashMap<String, String> = HashMap::new();
    sheet_name_to_path
        .try_reserve(sheet_count)
        .map_err(|source| {
            err_with_source(
                format!("시트 경로 맵 메모리 확보 실패: {sheet_count} entries"),
                source,
            )
        })?;
    let mut sheet_order: Vec<String> = Vec::new();
    sheet_order
        .try_reserve_exact(sheet_count)
        .map_err(|source| {
            err_with_source(
                format!("시트 순서 목록 메모리 확보 실패: {sheet_count} sheets"),
                source,
            )
        })?;
    for tag in iter_start_tags(&workbook_xml, "sheet")? {
        let Some(name) = extract_attr(tag, "name") else {
            continue;
        };
        let Some(rid) = extract_attr(tag, "r:id") else {
            continue;
        };
        let Some(target) = rid_to_target.get(&rid) else {
            continue;
        };
        let resolved = if target.starts_with('/') {
            target.trim_start_matches('/').to_owned()
        } else {
            let mut base: PathBuf = "xl/workbook.xml".into();
            base.pop();
            let combined = base.join(path_from_slashes(target));
            let mut normalized = PathBuf::default();
            for component in combined.components() {
                match component {
                    Component::ParentDir => {
                        normalized.pop();
                    }
                    Component::Normal(path_segment) => normalized.push(path_segment),
                    Component::CurDir | Component::RootDir | Component::Prefix(_) => {}
                }
            }
            normalized
                .components()
                .filter_map(|component| {
                    if let Component::Normal(path_segment) = component {
                        Some(path_segment.to_string_lossy())
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
                .join("/")
        };
        sheet_name_to_path.insert(name.clone(), resolved);
        sheet_order.push(name);
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
    if !(path.try_exists().map_err(|source_err| {
        err(format!(
            "sharedStrings.xml 경로 확인 실패: {} ({source_err})",
            path.display()
        ))
    }))? {
        return Ok(Vec::new());
    }
    let xml = container.read_text("xl/sharedStrings.xml")?;
    let shared_string_count = xml.matches("<si").count();
    let mut out: Vec<String> = Vec::new();
    out.try_reserve_exact(shared_string_count)
        .map_err(|source| {
            err_with_source(
                format!("sharedStrings 메모리 확보 실패: {shared_string_count} entries"),
                source,
            )
        })?;
    let mut cursor = 0_usize;
    while let Some(si_start) = find_start_tag(&xml, "si", cursor) {
        let Some(si_tag_end) = find_tag_end(&xml, si_start) else {
            break;
        };
        let Some(body_start) = si_tag_end.checked_add(1) else {
            break;
        };
        let Some(si_end) = find_end_tag(&xml, "si", body_start) else {
            break;
        };
        let Some(si_body) = xml.get(body_start..si_end) else {
            break;
        };
        let text = extract_all_tag_text(si_body, "t")
            .map(|text_value| decode_xml_entities(&text_value))
            .unwrap_or_default();
        out.push(text);
        let Some(next_cursor) = si_end.checked_add("</si>".len()) else {
            break;
        };
        cursor = next_cursor;
    }
    Ok(out)
}
fn iter_start_tags<'xml>(xml: &'xml str, tag_name: &str) -> Result<Vec<&'xml str>> {
    let tag_count = xml.matches(tag_name).count();
    let mut out: Vec<&'xml str> = Vec::new();
    out.try_reserve_exact(tag_count).map_err(|source| {
        err_with_source(
            format!("OOXML 태그 목록 메모리 확보 실패: {tag_count} entries"),
            source,
        )
    })?;
    let mut cursor = 0_usize;
    while let Some(start) = find_start_tag(xml, tag_name, cursor) {
        let Some(end) = find_tag_end(xml, start) else {
            break;
        };
        let Some(tag) = xml.get(start..=end) else {
            break;
        };
        out.push(tag);
        let Some(next_cursor) = end.checked_add(1) else {
            break;
        };
        cursor = next_cursor;
    }
    Ok(out)
}
