use crate::excel;
pub(super) struct FilterDatabaseDefinedNameUpdater<'xml> {
    pub data_end_col: u32,
    pub data_end_row: u32,
    pub data_start_row: u32,
    pub workbook_xml: &'xml mut String,
}
impl FilterDatabaseDefinedNameUpdater<'_> {
    pub(super) fn update(&mut self) {
        let end_col = excel::writer::col_to_name(self.data_end_col.max(1));
        let replacement = format!(
            "유류비!$A${}:{end_col}${}",
            self.data_start_row, self.data_end_row
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
            let open_pos = cursor.saturating_add(open_rel);
            let Some(open_end_rel) = self
                .workbook_xml
                .get(open_pos..)
                .and_then(|tail| tail.find('>'))
            else {
                break;
            };
            let open_end = open_pos.saturating_add(open_end_rel);
            let Some(open_tag) = self.workbook_xml.get(open_pos..=open_end) else {
                break;
            };
            if !open_tag.contains(&marker_attr_double) && !open_tag.contains(&marker_attr_single) {
                cursor = open_end.saturating_add(1);
                continue;
            }
            let content_start = open_end.saturating_add(1);
            let Some(close_rel) = self
                .workbook_xml
                .get(content_start..)
                .and_then(|tail| tail.find("</definedName>"))
            else {
                break;
            };
            let content_end = content_start.saturating_add(close_rel);
            let Some(content) = self.workbook_xml.get(content_start..content_end) else {
                break;
            };
            if content.contains(sheet_ref_plain) || content.contains(sheet_ref_quoted) {
                self.workbook_xml
                    .replace_range(content_start..content_end, &replacement);
                return;
            }
            cursor = content_end.saturating_add("</definedName>".len());
        }
    }
}
