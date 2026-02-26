use super::{
    ooxml::{load_shared_strings, load_sheet_catalog, load_sheet_xml},
    xlsx_container::XlsxContainer,
    xml::{
        decode_xml_entities, extract_all_tag_text, extract_attr, extract_first_tag_text,
        find_end_tag, find_start_tag, find_tag_end,
    },
};
use crate::source_sync::SourceRecord;
use crate::{Result, canon_header, err, numeric::round_f64_to_i32, parse_i32_str};
use std::{collections::BTreeMap, path::Path};
#[path = "source_reader_biff.rs"]
mod source_reader_biff;
#[derive(Debug, Clone, PartialEq)]
enum CellValue {
    Empty,
    Text(String),
    Number(f64),
}
#[derive(Debug, Clone, Copy)]
struct SourceHeaderIndices {
    region: Option<usize>,
    name: usize,
    address: usize,
    brand: Option<usize>,
    phone: Option<usize>,
    self_yn: Option<usize>,
    premium: Option<usize>,
    gasoline: Option<usize>,
    diesel: Option<usize>,
}
const MAX_XLSX_ROW: u32 = 200_000;
const MAX_XLSX_COL: usize = 1_024;
const DEFAULT_SOURCE_HEADER_SCAN_ROWS: usize = 200;
impl CellValue {
    fn as_string(&self) -> String {
        match self {
            Self::Empty => String::new(),
            Self::Text(v) => v.trim().to_string(),
            Self::Number(v) => format_number(*v),
        }
    }
    fn as_i32(&self) -> Option<i32> {
        match self {
            Self::Empty => None,
            Self::Number(v) => round_f64_to_i32(*v),
            Self::Text(v) => parse_i32_str(v),
        }
    }
}
pub fn read_source_file(path: &Path) -> Result<Vec<SourceRecord>> {
    let ext = path
        .extension()
        .and_then(|v| v.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    match ext.as_str() {
        "xlsx" => read_xlsx_source(path),
        "xls" => read_xls_source(path),
        _ => Err(err(format!(
            "지원하지 않는 소스 확장자입니다: {}",
            path.display()
        ))),
    }
}
fn read_xlsx_source(path: &Path) -> Result<Vec<SourceRecord>> {
    let container = XlsxContainer::open_for_update(path)?;
    let catalog = load_sheet_catalog(&container)?;
    let shared_strings = load_shared_strings(&container)?;
    if catalog.sheet_order.is_empty() {
        return Err(err("xlsx에 시트가 없습니다."));
    }
    let mut all = Vec::new();
    let mut last_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;
    for sheet_name in &catalog.sheet_order {
        let sheet_xml = load_sheet_xml(&container, &catalog, sheet_name)?;
        match parse_sheet_source_records(&sheet_xml, &shared_strings) {
            Ok(records) if !records.is_empty() => all.extend(records),
            Ok(_) => {}
            Err(e) => last_err = Some(e),
        }
    }
    if !all.is_empty() {
        return Ok(all);
    }
    if let Some(e) = last_err {
        return Err(err(format!(
            "xlsx 시트에서 유효한 소스 데이터를 찾지 못했습니다. ({e})"
        )));
    }
    Err(err("xlsx 시트에서 유효한 소스 데이터를 찾지 못했습니다."))
}
fn parse_sheet_source_records(
    sheet_xml: &str,
    shared_strings: &[String],
) -> Result<Vec<SourceRecord>> {
    match build_source_records_from_sheet_xml_streaming(sheet_xml, shared_strings) {
        Ok(records) => Ok(records),
        Err(stream_err) => {
            let rows = parse_xlsx_rows(sheet_xml, shared_strings).map_err(|legacy_parse_err| {
                err(format!(
                    "스트리밍 파싱 실패: {stream_err}; 레거시 파싱 실패: {legacy_parse_err}"
                ))
            })?;
            build_source_records_from_rows(&rows).map_err(|legacy_err| {
                err(format!(
                    "스트리밍 파싱 실패: {stream_err}; 레거시 파싱 실패: {legacy_err}"
                ))
            })
        }
    }
}
fn build_source_records_from_sheet_xml_streaming(
    sheet_xml: &str,
    shared_strings: &[String],
) -> Result<Vec<SourceRecord>> {
    let sheet_data = sheet_data_body(sheet_xml)?;
    let mut out = Vec::new();
    let mut cursor = 0usize;
    let mut next_row_num = 1usize;
    let mut scanned_rows = 0usize;
    let header_scan_rows = source_header_scan_rows();
    let mut header_indices: Option<SourceHeaderIndices> = None;
    while let Some(row_open_rel) = sheet_data[cursor..].find("<row") {
        let row_open = cursor + row_open_rel;
        let Some(row_tag_end_rel) = sheet_data[row_open..].find('>') else {
            return Err(err(format!(
                "xlsx row 시작 태그가 손상되었습니다. (offset={row_open})"
            )));
        };
        let row_tag_end = row_open + row_tag_end_rel;
        let row_tag = &sheet_data[row_open..=row_tag_end];
        let row_num_u32 = parse_row_number(row_tag)
            .unwrap_or_else(|| u32::try_from(next_row_num).unwrap_or(MAX_XLSX_ROW + 1));
        if row_num_u32 > MAX_XLSX_ROW {
            return Err(err(format!(
                "xlsx 행 인덱스가 비정상적으로 큽니다: {row_num_u32} (최대 {MAX_XLSX_ROW})"
            )));
        }
        if row_num_u32 == 0 {
            cursor = row_tag_end + 1;
            continue;
        }
        let row_num = usize::try_from(row_num_u32)
            .map_err(|_| err(format!("xlsx 행 인덱스 변환 실패: {row_num_u32}")))?;
        let row_cells = if row_tag.ends_with("/>") {
            Vec::new()
        } else {
            let row_body_start = row_tag_end + 1;
            let Some(row_close_rel) = sheet_data[row_body_start..].find("</row>") else {
                return Err(err(format!(
                    "xlsx row 종료 태그를 찾지 못했습니다. (row={row_num_u32})"
                )));
            };
            let row_body_end = row_body_start + row_close_rel;
            let row_body = &sheet_data[row_body_start..row_body_end];
            cursor = row_body_end + "</row>".len();
            parse_xlsx_row_cells(row_body, row_num, shared_strings)?
        };
        next_row_num = row_num.saturating_add(1);
        if row_tag.ends_with("/>") {
            cursor = row_tag_end + 1;
        }
        if header_indices.is_none() && scanned_rows < header_scan_rows {
            header_indices = parse_source_header_indices(&row_cells);
            scanned_rows = scanned_rows.saturating_add(1);
            if header_indices.is_some() {
                continue;
            }
            continue;
        }
        if let Some(indices) = header_indices
            && let Some(record) = build_source_record_from_row(&row_cells, indices)
        {
            out.push(record);
        }
        scanned_rows = scanned_rows.saturating_add(1);
    }
    if header_indices.is_none() {
        return Err(err("헤더 행을 찾지 못했습니다"));
    }
    Ok(out)
}
fn sheet_data_body(sheet_xml: &str) -> Result<&str> {
    let Some(sheet_data_open) = find_start_tag(sheet_xml, "sheetData", 0) else {
        return Err(err("xlsx worksheet XML에 <sheetData>가 없습니다."));
    };
    let Some(sheet_data_open_end) = find_tag_end(sheet_xml, sheet_data_open) else {
        return Err(err(
            "xlsx worksheet XML의 <sheetData> 시작 태그가 손상되었습니다.",
        ));
    };
    let sheet_data_body_start = sheet_data_open_end + 1;
    let Some(sheet_data_close) = find_end_tag(sheet_xml, "sheetData", sheet_data_body_start) else {
        return Err(err("xlsx worksheet XML에 </sheetData>가 없습니다."));
    };
    Ok(&sheet_xml[sheet_data_body_start..sheet_data_close])
}
fn parse_xlsx_rows(
    sheet_xml: &str,
    shared_strings: &[String],
) -> Result<Vec<(usize, Vec<CellValue>)>> {
    let sheet_data = sheet_data_body(sheet_xml)?;
    let mut rows_map: BTreeMap<usize, Vec<CellValue>> = BTreeMap::new();
    let mut cursor = 0usize;
    let mut next_row_num = 1usize;
    while let Some(row_open_rel) = sheet_data[cursor..].find("<row") {
        let row_open = cursor + row_open_rel;
        let Some(row_tag_end_rel) = sheet_data[row_open..].find('>') else {
            return Err(err(format!(
                "xlsx row 시작 태그가 손상되었습니다. (offset={row_open})"
            )));
        };
        let row_tag_end = row_open + row_tag_end_rel;
        let row_tag = &sheet_data[row_open..=row_tag_end];
        let row_num_u32 = parse_row_number(row_tag)
            .unwrap_or_else(|| u32::try_from(next_row_num).unwrap_or(MAX_XLSX_ROW + 1));
        if row_num_u32 > MAX_XLSX_ROW {
            return Err(err(format!(
                "xlsx 행 인덱스가 비정상적으로 큽니다: {row_num_u32} (최대 {MAX_XLSX_ROW})"
            )));
        }
        if row_num_u32 == 0 {
            cursor = row_tag_end + 1;
            continue;
        }
        let row_num = usize::try_from(row_num_u32)
            .map_err(|_| err(format!("xlsx 행 인덱스 변환 실패: {row_num_u32}")))?;
        if row_tag.ends_with("/>") {
            rows_map.insert(row_num, Vec::new());
            next_row_num = row_num.saturating_add(1);
            cursor = row_tag_end + 1;
            continue;
        }
        let row_body_start = row_tag_end + 1;
        let Some(row_close_rel) = sheet_data[row_body_start..].find("</row>") else {
            return Err(err(format!(
                "xlsx row 종료 태그를 찾지 못했습니다. (row={row_num_u32})"
            )));
        };
        let row_body_end = row_body_start + row_close_rel;
        let row_body = &sheet_data[row_body_start..row_body_end];
        let row_cells = parse_xlsx_row_cells(row_body, row_num, shared_strings)?;
        rows_map.insert(row_num, row_cells);
        next_row_num = row_num.saturating_add(1);
        cursor = row_body_end + "</row>".len();
    }
    Ok(rows_map.into_iter().collect())
}
fn parse_row_number(row_tag: &str) -> Option<u32> {
    let value = extract_attr(row_tag, "r")?;
    value.parse::<u32>().ok()
}
fn parse_xlsx_row_cells(
    row_xml: &str,
    row_num: usize,
    shared_strings: &[String],
) -> Result<Vec<CellValue>> {
    let mut row_cells: Vec<CellValue> = Vec::new();
    let mut cursor = 0usize;
    let mut next_col = 0usize;
    while let Some(cell_open_rel) = row_xml[cursor..].find("<c") {
        let cell_open = cursor + cell_open_rel;
        let Some(cell_tag_end_rel) = row_xml[cell_open..].find('>') else {
            return Err(err(format!(
                "xlsx 셀 시작 태그가 손상되었습니다. (row={row_num}, offset={cell_open})"
            )));
        };
        let cell_tag_end = cell_open + cell_tag_end_rel;
        let cell_tag = &row_xml[cell_open..=cell_tag_end];
        let col_index = extract_attr(cell_tag, "r")
            .as_deref()
            .and_then(cell_ref_to_col_index)
            .unwrap_or(next_col);
        if col_index >= MAX_XLSX_COL {
            return Err(err(format!(
                "xlsx 열 인덱스가 비정상적으로 큽니다: {}",
                col_index + 1
            )));
        }
        if row_cells.len() <= col_index {
            row_cells.resize(col_index + 1, CellValue::Empty);
        }
        if cell_tag.ends_with("/>") {
            if let Some(cell) = row_cells.get_mut(col_index) {
                *cell = CellValue::Empty;
            }
            next_col = col_index + 1;
            cursor = cell_tag_end + 1;
            continue;
        }
        let cell_body_start = cell_tag_end + 1;
        let Some(cell_close_rel) = row_xml[cell_body_start..].find("</c>") else {
            return Err(err(format!(
                "xlsx 셀 종료 태그를 찾지 못했습니다. (row={row_num}, col={})",
                col_index + 1
            )));
        };
        let cell_body_end = cell_body_start + cell_close_rel;
        let cell_body = &row_xml[cell_body_start..cell_body_end];
        if let Some(cell) = row_cells.get_mut(col_index) {
            *cell = parse_xlsx_cell_value(cell_tag, cell_body, shared_strings);
        }
        next_col = col_index + 1;
        cursor = cell_body_end + "</c>".len();
    }
    Ok(row_cells)
}
fn parse_xlsx_cell_value(cell_tag: &str, cell_body: &str, shared_strings: &[String]) -> CellValue {
    let cell_type = extract_attr(cell_tag, "t");
    if matches!(cell_type.as_deref(), Some("inlineStr"))
        && let Some(v) = extract_all_tag_text(cell_body, "t")
    {
        return CellValue::Text(decode_xml_entities(&v));
    }
    let Some(v_raw) = extract_first_tag_text(cell_body, "v") else {
        return CellValue::Empty;
    };
    let decoded = decode_xml_entities(&v_raw);
    if matches!(cell_type.as_deref(), Some("s"))
        && let Ok(idx) = decoded.parse::<usize>()
        && let Some(v) = shared_strings.get(idx)
    {
        return CellValue::Text(v.clone());
    }
    if matches!(cell_type.as_deref(), Some("s")) {
        return CellValue::Text(decoded);
    }
    if matches!(cell_type.as_deref(), Some("b")) {
        return CellValue::Text(if decoded == "1" {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        });
    }
    if matches!(cell_type.as_deref(), Some("str")) {
        return CellValue::Text(decoded);
    }
    if let Ok(n) = decoded.parse::<f64>() {
        return CellValue::Number(n);
    }
    CellValue::Text(decoded)
}
fn cell_ref_to_col_index(cell_ref: &str) -> Option<usize> {
    let mut col = 0usize;
    let mut has_alpha = false;
    for ch in cell_ref.chars() {
        if ch.is_ascii_alphabetic() {
            has_alpha = true;
            let upper = ch.to_ascii_uppercase() as u8;
            if !upper.is_ascii_uppercase() {
                return None;
            }
            col = col
                .checked_mul(26)?
                .checked_add(usize::from(upper - b'A' + 1))?;
        } else {
            break;
        }
    }
    if has_alpha { col.checked_sub(1) } else { None }
}
fn read_xls_source(path: &Path) -> Result<Vec<SourceRecord>> {
    source_reader_biff::read_xls_source(path)
}
fn format_number(v: f64) -> String {
    const I64_MIN_F64: f64 = -9_223_372_036_854_775_808.0;
    const I64_MAX_F64: f64 = 9_223_372_036_854_775_807.0;
    if (v.fract() - 0.0).abs() < f64::EPSILON && (I64_MIN_F64..=I64_MAX_F64).contains(&v) {
        if v == 0.0 {
            "0".to_string()
        } else {
            format!("{v:.0}")
        }
    } else {
        let mut s = format!("{v}");
        if s.contains('.') {
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.pop();
            }
        }
        s
    }
}
fn build_source_records_from_rows(rows: &[(usize, Vec<CellValue>)]) -> Result<Vec<SourceRecord>> {
    let mut header_row_idx: Option<usize> = None;
    for (idx, (_row_no, row)) in rows.iter().take(source_header_scan_rows()).enumerate() {
        if parse_source_header_indices(row).is_some() {
            header_row_idx = Some(idx);
            break;
        }
    }
    let header_row_idx = header_row_idx.ok_or_else(|| err("헤더 행을 찾지 못했습니다"))?;
    let header = rows
        .get(header_row_idx)
        .map(|(_, row)| row)
        .ok_or_else(|| err("헤더 행 접근 실패"))?;
    let header_indices =
        parse_source_header_indices(header).ok_or_else(|| err("헤더 행 접근 실패"))?;
    let mut out = Vec::new();
    for (_row_no, row) in rows.iter().skip(header_row_idx + 1) {
        if let Some(record) = build_source_record_from_row(row, header_indices) {
            out.push(record);
        }
    }
    Ok(out)
}
fn parse_source_header_indices(header: &[CellValue]) -> Option<SourceHeaderIndices> {
    let mut idx_region: Option<usize> = None;
    let mut idx_name: Option<usize> = None;
    let mut idx_addr: Option<usize> = None;
    let mut idx_brand: Option<usize> = None;
    let mut idx_phone: Option<usize> = None;
    let mut idx_self: Option<usize> = None;
    let mut idx_premium: Option<usize> = None;
    let mut idx_gas: Option<usize> = None;
    let mut idx_diesel: Option<usize> = None;
    for (i, cell) in header.iter().enumerate() {
        let h = canon_header(&cell.as_string());
        match h.as_str() {
            "지역" => idx_region = Some(i),
            "상호" => idx_name = Some(i),
            "주소" => idx_addr = Some(i),
            "상표" => idx_brand = Some(i),
            "전화번호" | "전화" => idx_phone = Some(i),
            "셀프여부" | "셀프" => idx_self = Some(i),
            "고급휘발유" | "고급유" => idx_premium = Some(i),
            "휘발유" | "보통휘발유" => idx_gas = Some(i),
            "경유" => idx_diesel = Some(i),
            _ => {}
        }
    }
    let idx_name = idx_name?;
    let idx_addr = idx_addr?;
    let idx_region = idx_region?;
    Some(SourceHeaderIndices {
        region: Some(idx_region),
        name: idx_name,
        address: idx_addr,
        brand: idx_brand,
        phone: idx_phone,
        self_yn: idx_self,
        premium: idx_premium,
        gasoline: idx_gas,
        diesel: idx_diesel,
    })
}
fn build_source_record_from_row(
    row: &[CellValue],
    header_indices: SourceHeaderIndices,
) -> Option<SourceRecord> {
    let name = get_row_string(row, header_indices.name);
    let address = get_row_string(row, header_indices.address);
    if name.trim().is_empty() && address.trim().is_empty() {
        return None;
    }
    if address.trim().is_empty() {
        return None;
    }
    let brand = header_indices
        .brand
        .map(|i| get_row_string(row, i))
        .unwrap_or_default();
    let phone = header_indices
        .phone
        .map(|i| get_row_string(row, i))
        .unwrap_or_default();
    let self_yn = header_indices
        .self_yn
        .map(|i| get_row_string(row, i))
        .unwrap_or_default();
    let gasoline = header_indices.gasoline.and_then(|i| get_row_i32(row, i));
    let premium = header_indices.premium.and_then(|i| get_row_i32(row, i));
    let diesel = header_indices.diesel.and_then(|i| get_row_i32(row, i));
    let region = header_indices
        .region
        .map(|i| get_row_string(row, i))
        .unwrap_or_default();
    Some(SourceRecord {
        region,
        name,
        brand,
        self_yn,
        address,
        phone,
        gasoline,
        premium,
        diesel,
    })
}
fn get_row_string(row: &[CellValue], idx: usize) -> String {
    row.get(idx).map(CellValue::as_string).unwrap_or_default()
}
fn get_row_i32(row: &[CellValue], idx: usize) -> Option<i32> {
    row.get(idx).and_then(CellValue::as_i32)
}
fn source_header_scan_rows() -> usize {
    std::env::var("FCUPDATER_SOURCE_HEADER_SCAN_ROWS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .map_or(DEFAULT_SOURCE_HEADER_SCAN_ROWS, |v| v.min(10_000))
}
