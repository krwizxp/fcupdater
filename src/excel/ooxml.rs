use super::{
    path_util::path_from_slashes,
    xlsx_container::XlsxContainer,
    xml::{
        XmlScanner, extract_all_tag_text, extract_attr, find_end_tag, find_start_tag, find_tag_end,
    },
};
use crate::{Result, err, err_with_source};
use core::{iter, range::Range};
use std::{
    collections::HashMap,
    path::{Component, PathBuf},
};
pub(super) type SheetCatalog = Vec<SheetInfo>;
pub(in crate::excel) struct SheetInfo {
    pub name: String,
    pub path: String,
}
pub(super) struct XlsxOoxml<'container> {
    pub container: &'container XlsxContainer,
}
impl XlsxOoxml<'_> {
    pub(super) fn load_shared_strings(&self) -> Result<Vec<String>> {
        let path = self
            .container
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
        let xml = self.container.read_text("xl/sharedStrings.xml")?;
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
            let si_body_span = Range {
                start: body_start,
                end: si_end,
            };
            let Some(si_body) = xml.get(si_body_span) else {
                break;
            };
            let text = extract_all_tag_text(si_body, "t").unwrap_or_default();
            out.push(text);
            let Some(next_cursor) = si_end.checked_add("</si>".len()) else {
                break;
            };
            cursor = next_cursor;
        }
        Ok(out)
    }
    pub(super) fn load_sheet_catalog(&self) -> Result<SheetCatalog> {
        let workbook_xml = self.container.read_text("xl/workbook.xml")?;
        let rels_xml = self.container.read_text("xl/_rels/workbook.xml.rels")?;
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
        rid_to_target.extend(
            iter_start_tags(&rels_xml, "Relationship")
                .filter_map(|tag| Some((extract_attr(tag, "Id")?, extract_attr(tag, "Target")?))),
        );
        let sheet_count = workbook_xml.matches("<sheet").count();
        let mut sheets: SheetCatalog = Vec::new();
        sheets.try_reserve_exact(sheet_count).map_err(|source| {
            err_with_source(
                format!("시트 순서 목록 메모리 확보 실패: {sheet_count} sheets"),
                source,
            )
        })?;
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
                let mut resolved_path = String::new();
                for component in normalized.components() {
                    let Component::Normal(path_segment) = component else {
                        continue;
                    };
                    let segment = path_segment.to_string_lossy();
                    let separator_len = usize::from(!resolved_path.is_empty());
                    resolved_path
                        .try_reserve(separator_len.saturating_add(segment.len()))
                        .map_err(|source| err_with_source("시트 경로 메모리 확보 실패", source))?;
                    if !resolved_path.is_empty() {
                        resolved_path.push('/');
                    }
                    resolved_path.push_str(&segment);
                }
                resolved_path
            };
            sheets.push(SheetInfo {
                name,
                path: resolved,
            });
        }
        if sheets.is_empty() {
            return Err(err("workbook에서 시트 정보를 찾지 못했습니다."));
        }
        Ok(sheets)
    }
}
fn iter_start_tags<'xml, 'tag>(
    xml: &'xml str,
    tag_name: &'tag str,
) -> impl Iterator<Item = &'xml str> + use<'xml, 'tag>
where
    'xml: 'tag,
{
    let mut scanner = XmlScanner::new(xml);
    iter::from_fn(move || scanner.next_start_named(tag_name).map(|tag| tag.tag()))
}
