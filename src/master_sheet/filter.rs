use super::FilterDatabaseDefinedNameUpdater;
use crate::diagnostic::{Result, err};
use crate::excel;
use core::range::Range;
impl FilterDatabaseDefinedNameUpdater<'_> {
    pub(super) fn update(&mut self) -> Result<()> {
        let end_col = excel::writer::col_to_name(self.data_end_col)?;
        let replacement = format!(
            "유류비!$A${}:{end_col}${}",
            self.data_rows.start, self.data_rows.last
        );
        let marker = "_xlnm._FilterDatabase";
        let marker_attr_double = format!("name=\"{marker}\"");
        let marker_attr_single = format!("name='{marker}'");
        let sheet_ref_plain = "유류비!";
        let sheet_ref_quoted = "'유류비'!";
        let mut cursor = 0_usize;
        while let Some(open_rel) = self
            .workbook_xml
            .get(cursor..)
            .and_then(|tail| tail.find("<definedName"))
        {
            let Some(open_pos) = cursor.checked_add(open_rel) else {
                return Err(err(
                    "workbook.xml의 definedName 태그 시작 위치 계산에 실패했습니다.",
                ));
            };
            let Some(open_end_rel) = self
                .workbook_xml
                .get(open_pos..)
                .and_then(|tail| tail.find('>'))
            else {
                return Err(err("workbook.xml의 definedName 태그가 손상되었습니다."));
            };
            let Some(open_end) = open_pos.checked_add(open_end_rel) else {
                return Err(err(
                    "workbook.xml의 definedName 태그 끝 위치 계산에 실패했습니다.",
                ));
            };
            let Some(open_tag) = self.workbook_xml.get(open_pos..=open_end) else {
                return Err(err(
                    "workbook.xml의 definedName 태그 범위가 손상되었습니다.",
                ));
            };
            if !open_tag.contains(&marker_attr_double) && !open_tag.contains(&marker_attr_single) {
                let Some(next_cursor) = open_end.checked_add(1) else {
                    return Err(err(
                        "workbook.xml의 다음 definedName 위치 계산에 실패했습니다.",
                    ));
                };
                cursor = next_cursor;
                continue;
            }
            let Some(content_start) = open_end.checked_add(1) else {
                return Err(err(
                    "workbook.xml의 definedName 본문 시작 계산에 실패했습니다.",
                ));
            };
            let Some(close_rel) = self
                .workbook_xml
                .get(content_start..)
                .and_then(|tail| tail.find("</definedName>"))
            else {
                return Err(err("workbook.xml의 </definedName> 태그를 찾지 못했습니다."));
            };
            let Some(content_end) = content_start.checked_add(close_rel) else {
                return Err(err(
                    "workbook.xml의 definedName 본문 끝 계산에 실패했습니다.",
                ));
            };
            let content_span = Range {
                start: content_start,
                end: content_end,
            };
            let Some(content) = self.workbook_xml.get(content_span) else {
                return Err(err(
                    "workbook.xml의 definedName 본문 범위가 손상되었습니다.",
                ));
            };
            if content.contains(sheet_ref_plain) || content.contains(sheet_ref_quoted) {
                self.workbook_xml.replace_range(content_span, &replacement);
                return Ok(());
            }
            let Some(next_cursor) = content_end.checked_add("</definedName>".len()) else {
                return Err(err(
                    "workbook.xml의 다음 definedName 위치 계산에 실패했습니다.",
                ));
            };
            cursor = next_cursor;
        }
        Ok(())
    }
}
