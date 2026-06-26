use crate::diagnostic::{Result, err, err_with_source};
use core::range::Range;
use std::path::Path;
mod path_util;
pub mod source_reader;
pub mod writer;
pub mod xlsx_container;
mod xml;
mod zip_archive;
pub enum SaveVerification {
    Skip,
    Verify,
}
struct ZipArchiveBuilder<'path> {
    archive_path: &'path Path,
    root: &'path Path,
}
struct ZipArchiveExtractor<'path> {
    archive_path: &'path Path,
    unpack_dir: &'path Path,
}
struct SheetInfo {
    name: String,
    path: String,
}
fn workbook_defined_name_content_span(
    workbook_xml: &str,
    defined_name: &str,
    sheet_index: usize,
    quoted_sheet: &str,
    plain_sheet: &str,
) -> Result<Range<usize>> {
    let mut matched_span = None;
    let mut scanner = xml::XmlScanner::new(workbook_xml);
    while let Some(tag) = scanner.next_start_named("definedName") {
        let open_tag = tag.raw;
        let content_start = tag
            .end
            .checked_add(1)
            .ok_or_else(|| err("workbook.xml definedName 본문 시작 계산에 실패했습니다."))?;
        let content_end = if tag.self_closing {
            content_start
        } else {
            xml::find_end_tag(workbook_xml, tag.name, content_start)
                .ok_or_else(|| err("workbook.xml의 </definedName> 태그를 찾지 못했습니다."))?
        };
        let content = workbook_xml
            .get(content_start..content_end)
            .ok_or_else(|| err("workbook.xml의 definedName 본문 범위가 손상되었습니다."))?;
        let decoded_content = xml::decode_xml_entities(content)?;
        let decoded_reference = decoded_content.trim();
        if xml::extract_attr(open_tag, "name")?.as_deref() == Some(defined_name) {
            let local_sheet_id = xml::extract_attr(open_tag, "localSheetId")?
                .map(|value| {
                    value.parse::<usize>().map_err(|source| {
                        err_with_source("workbook.xml localSheetId 해석 실패", source)
                    })
                })
                .transpose()?;
            let references_sheet = decoded_reference.starts_with(quoted_sheet)
                || decoded_reference.starts_with(plain_sheet);
            if references_sheet && local_sheet_id != Some(sheet_index) {
                return Err(err(
                    "유류비 _FilterDatabase의 localSheetId가 유류비 시트 index와 다릅니다.",
                ));
            }
            if local_sheet_id == Some(sheet_index) && !references_sheet {
                return Err(err(
                    "유류비 localSheetId의 _FilterDatabase가 유류비 시트를 참조하지 않습니다.",
                ));
            }
            if local_sheet_id == Some(sheet_index)
                && references_sheet
                && matched_span
                    .replace(Range {
                        start: content_start,
                        end: content_end,
                    })
                    .is_some()
            {
                return Err(err("유류비 _FilterDatabase가 중복되어 있습니다."));
            }
        }
        if !tag.self_closing {
            let close_end = xml::find_tag_end(workbook_xml, content_end)
                .ok_or_else(|| err("workbook.xml의 </definedName> 태그가 손상되었습니다."))?;
            let next_cursor = close_end
                .checked_add(1)
                .ok_or_else(|| err("workbook.xml 다음 definedName 위치 계산에 실패했습니다."))?;
            scanner.skip_to(next_cursor);
        }
    }
    matched_span.ok_or_else(|| err("유류비 _FilterDatabase definedName을 찾지 못했습니다."))
}
fn workbook_sheet_index_by_name(workbook_xml: &str, sheet_name: &str) -> Result<usize> {
    let mut sheet_index = None;
    let mut sheet_order = 0_usize;
    let mut scanner = xml::XmlScanner::new(workbook_xml);
    while let Some(tag) = scanner.next_start_named("sheet") {
        if xml::extract_attr(tag.raw, "name")?.as_deref() == Some(sheet_name)
            && sheet_index.replace(sheet_order).is_some()
        {
            return Err(err("workbook.xml에 대상 시트가 중복되어 있습니다."));
        }
        sheet_order = sheet_order
            .checked_add(1)
            .ok_or_else(|| err("workbook.xml sheet 순서 계산 중 overflow가 발생했습니다."))?;
    }
    sheet_index.ok_or_else(|| err("workbook.xml에서 대상 시트를 찾지 못했습니다."))
}
