use crate::excel::writer::col_to_name;
pub fn update_filter_database_defined_name(
    workbook_xml: &mut String,
    data_start_row: u32,
    data_end_row: u32,
    data_end_col: u32,
) {
    let end_col = col_to_name(data_end_col.max(1));
    let replacement = format!("유류비!$A${data_start_row}:${end_col}${data_end_row}");
    let Some((content_start, content_end)) =
        find_filter_database_defined_name_content_range(workbook_xml, "유류비")
    else {
        return;
    };
    workbook_xml.replace_range(content_start..content_end, &replacement);
}
fn find_filter_database_defined_name_content_range(
    workbook_xml: &str,
    sheet_name: &str,
) -> Option<(usize, usize)> {
    let marker = "_xlnm._FilterDatabase";
    let marker_attr_double = format!("name=\"{marker}\"");
    let marker_attr_single = format!("name='{marker}'");
    let sheet_ref_plain = format!("{sheet_name}!");
    let sheet_ref_quoted = format!("'{sheet_name}'!");
    let mut cursor = 0_usize;
    while let Some(open_rel) = workbook_xml.get(cursor..)?.find("<definedName") {
        let open_pos = cursor + open_rel;
        let Some(open_end_rel) = workbook_xml.get(open_pos..)?.find('>') else {
            break;
        };
        let open_end = open_pos + open_end_rel;
        let Some(open_tag) = workbook_xml.get(open_pos..=open_end) else {
            break;
        };
        if !open_tag.contains(&marker_attr_double) && !open_tag.contains(&marker_attr_single) {
            cursor = open_end + 1;
            continue;
        }
        let content_start = open_end + 1;
        let Some(close_rel) = workbook_xml.get(content_start..)?.find("</definedName>") else {
            break;
        };
        let content_end = content_start + close_rel;
        let Some(content) = workbook_xml.get(content_start..content_end) else {
            break;
        };
        if content.contains(&sheet_ref_plain) || content.contains(&sheet_ref_quoted) {
            return Some((content_start, content_end));
        }
        cursor = content_end + "</definedName>".len();
    }
    None
}
