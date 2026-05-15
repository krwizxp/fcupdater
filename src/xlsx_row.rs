use crate::{
    Result, err, err_with_source, excel::source_reader::checked_xml_offset_add, prefixed_message,
};
pub fn parse_next_sheet_row(
    sheet_data: &str,
    cursor: usize,
) -> Result<Option<(usize, Option<&str>, usize)>> {
    let Some(row_open_rel) = sheet_data.get(cursor..).and_then(|tail| tail.find("<row")) else {
        return Ok(None);
    };
    let row_open = checked_xml_offset_add(cursor, row_open_rel, "xlsx row 시작")?;
    let Some(row_tag_end_rel) = sheet_data.get(row_open..).and_then(|tail| tail.find('>')) else {
        return Err(err(xlsx_row_offset_message(
            "xlsx row 시작 태그가 손상되었습니다. (offset=",
            row_open,
        )));
    };
    let row_tag_end = checked_xml_offset_add(row_open, row_tag_end_rel, "xlsx row 태그 끝")?;
    let row_tag = sheet_data.get(row_open..=row_tag_end).ok_or_else(|| {
        err(xlsx_row_offset_message(
            "xlsx row 태그 범위가 손상되었습니다. (offset=",
            row_open,
        ))
    })?;
    let row_num_u32 = row_tag
        .split_once("r=\"")
        .and_then(|(_, tail)| tail.split_once('"'))
        .map(|(value, _)| value)
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or_default();
    let row_num = usize::try_from(row_num_u32).map_err(|source| {
        err_with_source(
            prefixed_message("xlsx 행 번호 변환 실패: ", row_num_u32),
            source,
        )
    })?;
    if row_tag.ends_with("/>") {
        let next_cursor = checked_xml_offset_add(row_tag_end, 1, "xlsx row cursor 전진")?;
        return Ok(Some((row_num, None, next_cursor)));
    }
    let row_body_start = checked_xml_offset_add(row_tag_end, 1, "xlsx row 본문 시작")?;
    let Some(row_close_rel) = sheet_data
        .get(row_body_start..)
        .and_then(|tail| tail.find("</row>"))
    else {
        return Err(err(xlsx_row_number_message(
            "xlsx row 종료 태그를 찾지 못했습니다. (row=",
            row_num_u32,
        )));
    };
    let row_body_end = checked_xml_offset_add(row_body_start, row_close_rel, "xlsx row 본문 끝")?;
    let row_body = sheet_data
        .get(row_body_start..row_body_end)
        .ok_or_else(|| {
            err(xlsx_row_number_message(
                "xlsx row 본문 범위가 손상되었습니다. (row=",
                row_num_u32,
            ))
        })?;
    let next_cursor = checked_xml_offset_add(row_body_end, "</row>".len(), "xlsx row cursor 전진")?;
    Ok(Some((row_num, Some(row_body), next_cursor)))
}
fn xlsx_row_offset_message(prefix: &str, offset: usize) -> String {
    let offset_text = offset.to_string();
    let capacity = prefix
        .len()
        .saturating_add(offset_text.len())
        .saturating_add(1);
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("{prefix}{offset_text})");
    }
    out.push_str(prefix);
    out.push_str(&offset_text);
    out.push(')');
    out
}
fn xlsx_row_number_message(prefix: &str, row_num: u32) -> String {
    let row_text = row_num.to_string();
    let capacity = prefix
        .len()
        .saturating_add(row_text.len())
        .saturating_add(1);
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("{prefix}{row_text})");
    }
    out.push_str(prefix);
    out.push_str(&row_text);
    out.push(')');
    out
}
