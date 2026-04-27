use super::writer::name_to_col;
use super::xml::{
    decode_xml_entities, extract_all_tag_text, extract_attr, extract_first_tag_text, find_end_tag,
    find_start_tag, find_tag_end,
};
use crate::{
    Result, canon_header, err, numeric::round_f64_to_i32, parse_i32_str, push_display,
    source_sync::SourceRecord,
};
use core::{
    error::Error as CoreError,
    fmt::{Display, Write as _},
};
use std::env;
#[path = "source_reader_biff.rs"]
pub mod biff;
pub const MAX_XLSX_ROW: u32 = 200_000;
const DEFAULT_SOURCE_HEADER_SCAN_ROWS: usize = 200;
const MAX_XLSX_COL: usize = 1_024;
#[derive(Debug, Clone, PartialEq)]
pub enum CellValue {
    Empty,
    Number(f64),
    Text(String),
}
#[derive(Debug, Clone, Copy)]
pub struct SourceHeaderIndices {
    address: usize,
    brand: Option<usize>,
    diesel: Option<usize>,
    gasoline: Option<usize>,
    name: usize,
    premium: Option<usize>,
    region: Option<usize>,
    self_yn: Option<usize>,
}
struct XlsxRowCellParser<'xml, 'shared> {
    cursor: usize,
    next_col: usize,
    row_cells: Vec<CellValue>,
    row_num: usize,
    row_xml: &'xml str,
    shared_strings: &'shared [String],
}
trait XlsxRowCellParserExt {
    fn advance(&mut self, col_index: usize, cell_end: usize, step: usize) -> Result<()>;
    fn parse(self) -> Result<Vec<CellValue>>;
    fn parse_cell_value(&self, cell_tag: &str, cell_body: &str) -> CellValue;
    fn parse_col_index(&self, cell_tag: &str) -> Option<usize>;
    fn parse_next_cell(&mut self) -> Result<bool>;
    fn row_err(
        &self,
        prefix: &str,
        middle: &str,
        value: impl Display,
    ) -> Box<dyn CoreError + Send + Sync>;
}
impl XlsxRowCellParserExt for XlsxRowCellParser<'_, '_> {
    fn advance(&mut self, col_index: usize, cell_end: usize, step: usize) -> Result<()> {
        self.next_col = checked_one_based_index(col_index, "xlsx 다음 열 인덱스")?;
        self.cursor = checked_xml_offset_add(cell_end, step, "xlsx cell cursor 전진")?;
        Ok(())
    }
    fn parse(mut self) -> Result<Vec<CellValue>> {
        while self.parse_next_cell()? {}
        Ok(self.row_cells)
    }
    fn parse_cell_value(&self, cell_tag: &str, cell_body: &str) -> CellValue {
        let cell_type = extract_attr(cell_tag, "t");
        if matches!(cell_type.as_deref(), Some("inlineStr"))
            && let Some(text_value) = extract_all_tag_text(cell_body, "t")
        {
            return CellValue::Text(decode_xml_entities(&text_value));
        }
        let Some(v_raw) = extract_first_tag_text(cell_body, "v") else {
            return CellValue::Empty;
        };
        let decoded = decode_xml_entities(&v_raw);
        if matches!(cell_type.as_deref(), Some("s"))
            && let Ok(idx) = decoded.parse::<usize>()
            && let Some(shared_text) = self.shared_strings.get(idx)
        {
            CellValue::Text(shared_text.to_owned())
        } else if matches!(cell_type.as_deref(), Some("s" | "str")) {
            CellValue::Text(decoded)
        } else if matches!(cell_type.as_deref(), Some("b")) {
            CellValue::Text(if decoded == "1" {
                "TRUE".into()
            } else {
                "FALSE".into()
            })
        } else if let Ok(number) = decoded.parse::<f64>() {
            CellValue::Number(number)
        } else {
            CellValue::Text(decoded)
        }
    }
    fn parse_col_index(&self, cell_tag: &str) -> Option<usize> {
        extract_attr(cell_tag, "r").as_deref().and_then(|cell_ref| {
            let col_end = cell_ref
                .find(|ch: char| !ch.is_ascii_alphabetic())
                .unwrap_or(cell_ref.len());
            let one_based_col = name_to_col(cell_ref.get(..col_end)?)?;
            let zero_based_col = one_based_col.checked_sub(1)?;
            usize::try_from(zero_based_col).ok()
        })
    }
    fn parse_next_cell(&mut self) -> Result<bool> {
        let Some(cell_open_rel) = self
            .row_xml
            .get(self.cursor..)
            .and_then(|tail| tail.find("<c"))
        else {
            return Ok(false);
        };
        let cell_open = checked_xml_offset_add(self.cursor, cell_open_rel, "xlsx cell 시작")?;
        let Some(cell_tag_end) = find_tag_end(self.row_xml, cell_open) else {
            return Err(self.row_err(
                "xlsx 셀 시작 태그가 손상되었습니다. (row=",
                ", offset=",
                cell_open,
            ));
        };
        let cell_tag = self.row_xml.get(cell_open..=cell_tag_end).ok_or_else(|| {
            self.row_err(
                "xlsx 셀 태그 범위가 손상되었습니다. (row=",
                ", offset=",
                cell_open,
            )
        })?;
        let col_index = self.parse_col_index(cell_tag).unwrap_or(self.next_col);
        let col_num = checked_one_based_index(col_index, "xlsx 열 번호")?;
        if col_index >= MAX_XLSX_COL {
            let capacity = 48;
            let mut out = String::with_capacity(capacity);
            out.push_str("xlsx 열 인덱스가 비정상적으로 큽니다: ");
            push_display(&mut out, col_num);
            return Err(err(out));
        }
        if self.row_cells.len() <= col_index {
            let next_len = checked_one_based_index(col_index, "xlsx row 셀 개수")?;
            self.row_cells.resize(next_len, CellValue::Empty);
        }
        if cell_tag.ends_with("/>") {
            if let Some(cell) = self.row_cells.get_mut(col_index) {
                *cell = CellValue::Empty;
            }
            self.advance(col_index, cell_tag_end, 1)?;
            return Ok(true);
        }
        let cell_body_start = checked_xml_offset_add(cell_tag_end, 1, "xlsx cell 본문 시작")?;
        let Some(cell_body_end) = find_end_tag(self.row_xml, "c", cell_body_start) else {
            return Err(self.row_err(
                "xlsx 셀 종료 태그를 찾지 못했습니다. (row=",
                ", col=",
                col_num,
            ));
        };
        let cell_body = self
            .row_xml
            .get(cell_body_start..cell_body_end)
            .ok_or_else(|| {
                self.row_err(
                    "xlsx 셀 본문 범위가 손상되었습니다. (row=",
                    ", col=",
                    col_num,
                )
            })?;
        let cell_value = self.parse_cell_value(cell_tag, cell_body);
        if let Some(cell) = self.row_cells.get_mut(col_index) {
            *cell = cell_value;
        }
        self.advance(col_index, cell_body_end, "</c>".len())?;
        Ok(true)
    }
    fn row_err(
        &self,
        prefix: &str,
        middle: &str,
        value: impl Display,
    ) -> Box<dyn CoreError + Send + Sync> {
        let capacity = prefix.len().saturating_add(middle.len()).saturating_add(48);
        let mut out = String::with_capacity(capacity);
        out.push_str(prefix);
        push_display(&mut out, self.row_num);
        out.push_str(middle);
        push_display(&mut out, value);
        out.push(')');
        err(out)
    }
}
pub fn sheet_data_body(sheet_xml: &str) -> Result<&str> {
    let Some(sheet_data_open) = find_start_tag(sheet_xml, "sheetData", 0) else {
        return Err(err("xlsx worksheet XML에 <sheetData>가 없습니다."));
    };
    let Some(sheet_data_open_end) = find_tag_end(sheet_xml, sheet_data_open) else {
        return Err(err(
            "xlsx worksheet XML의 <sheetData> 시작 태그가 손상되었습니다.",
        ));
    };
    let sheet_data_body_start =
        (checked_xml_offset_add(sheet_data_open_end, 1, "xlsx sheetData 본문 시작"))?;
    let Some(sheet_data_close) = find_end_tag(sheet_xml, "sheetData", sheet_data_body_start) else {
        return Err(err("xlsx worksheet XML에 </sheetData>가 없습니다."));
    };
    sheet_xml
        .get(sheet_data_body_start..sheet_data_close)
        .ok_or_else(|| err("xlsx worksheet XML의 sheetData 본문 범위가 손상되었습니다."))
}
fn normalize_fuel_price(value: Option<i32>) -> Option<i32> {
    value.filter(|fuel_price| *fuel_price > 0_i32)
}
pub fn checked_xml_offset_add(base: usize, add: usize, context: &str) -> Result<usize> {
    base.checked_add(add).ok_or_else(|| {
        let capacity = context.len().saturating_add(64);
        let mut out = String::with_capacity(capacity);
        out.push_str(context);
        out.push_str(" 오프셋 계산 중 overflow가 발생했습니다. (base=");
        push_display(&mut out, base);
        out.push_str(", add=");
        push_display(&mut out, add);
        out.push(')');
        err(out)
    })
}
fn checked_one_based_index(zero_based: usize, context: &str) -> Result<usize> {
    zero_based.checked_add(1).ok_or_else(|| {
        let capacity = context.len().saturating_add(48);
        let mut out = String::with_capacity(capacity);
        out.push_str(context);
        out.push_str(" 계산 중 overflow가 발생했습니다. (index=");
        push_display(&mut out, zero_based);
        out.push(')');
        err(out)
    })
}
pub fn parse_xlsx_row_cells(
    row_xml: &str,
    row_num: usize,
    shared_strings: &[String],
) -> Result<Vec<CellValue>> {
    XlsxRowCellParser {
        cursor: 0,
        next_col: 0,
        row_cells: Vec::with_capacity(row_xml.matches("<c").count()),
        row_num,
        row_xml,
        shared_strings,
    }
    .parse()
}
pub fn build_source_records_from_rows(
    rows: &[(usize, Vec<CellValue>)],
) -> Result<Vec<SourceRecord>> {
    let (found_header_row_idx, header_indices) = rows
        .iter()
        .take(source_header_scan_rows())
        .enumerate()
        .find_map(|(idx, row_entry)| {
            parse_source_header_indices(&row_entry.1).map(|indices| (idx, indices))
        })
        .ok_or_else(|| err("헤더 행을 찾지 못했습니다"))?;
    let data_row_start = found_header_row_idx.checked_add(1).unwrap_or(rows.len());
    let mut out = Vec::with_capacity(rows.len().saturating_sub(data_row_start));
    for row_entry in rows.iter().skip(data_row_start) {
        let row = &row_entry.1;
        if let Some(record) = build_source_record_from_row(row, header_indices) {
            out.push(record);
        }
    }
    Ok(out)
}
pub fn parse_source_header_indices(header: &[CellValue]) -> Option<SourceHeaderIndices> {
    let mut idx_region: Option<usize> = None;
    let mut idx_name: Option<usize> = None;
    let mut idx_addr: Option<usize> = None;
    let mut idx_brand: Option<usize> = None;
    let mut idx_self: Option<usize> = None;
    let mut idx_premium: Option<usize> = None;
    let mut idx_gas: Option<usize> = None;
    let mut idx_diesel: Option<usize> = None;
    for (i, cell) in header.iter().enumerate() {
        let header_text = cell_to_string(cell.clone());
        let canonical_header = canon_header(&header_text);
        match canonical_header.as_str() {
            "지역" => idx_region = Some(i),
            "상호" => idx_name = Some(i),
            "주소" => idx_addr = Some(i),
            "상표" => idx_brand = Some(i),
            "셀프여부" | "셀프" => idx_self = Some(i),
            "고급휘발유" | "고급유" => {
                idx_premium = Some(i);
            }
            "휘발유" | "보통휘발유" => {
                idx_gas = Some(i);
            }
            "경유" => idx_diesel = Some(i),
            _ => {}
        }
    }
    let name_idx = (idx_name)?;
    let addr_idx = (idx_addr)?;
    let region_idx = (idx_region)?;
    Some(SourceHeaderIndices {
        region: Some(region_idx),
        name: name_idx,
        address: addr_idx,
        brand: idx_brand,
        self_yn: idx_self,
        premium: idx_premium,
        gasoline: idx_gas,
        diesel: idx_diesel,
    })
}
pub fn build_source_record_from_row(
    row: &[CellValue],
    header_indices: SourceHeaderIndices,
) -> Option<SourceRecord> {
    let name = get_row_string(row, header_indices.name);
    let address = get_row_string(row, header_indices.address);
    if address.trim().is_empty() {
        return None;
    }
    let brand = header_indices
        .brand
        .map(|i| get_row_string(row, i))
        .unwrap_or_default();
    let self_yn = header_indices
        .self_yn
        .map(|i| get_row_string(row, i))
        .unwrap_or_default();
    let gasoline = normalize_fuel_price(header_indices.gasoline.and_then(|i| get_row_i32(row, i)));
    let premium = normalize_fuel_price(header_indices.premium.and_then(|i| get_row_i32(row, i)));
    let diesel = normalize_fuel_price(header_indices.diesel.and_then(|i| get_row_i32(row, i)));
    let region = header_indices
        .region
        .map(|i| get_row_string(row, i))
        .unwrap_or_default();
    Some(SourceRecord {
        address,
        brand,
        diesel,
        gasoline,
        name,
        premium,
        region,
        self_yn,
    })
}
fn get_row_string(row: &[CellValue], idx: usize) -> String {
    row.get(idx)
        .cloned()
        .map_or_else(String::new, cell_to_string)
}
fn get_row_i32(row: &[CellValue], idx: usize) -> Option<i32> {
    match row.get(idx)?.clone() {
        CellValue::Text(text_value) => parse_i32_str(&text_value),
        CellValue::Number(number_value) => round_f64_to_i32(number_value),
        CellValue::Empty => None,
    }
}
fn cell_to_string(cell: CellValue) -> String {
    match cell {
        CellValue::Text(text_value) => text_value.trim().to_owned(),
        CellValue::Number(number_value) => {
            const I64_MIN_F64: f64 = -9_223_372_036_854_776_000.0;
            const I64_MAX_F64: f64 = 9_223_372_036_854_776_000.0;
            if number_value.fract().abs() < f64::EPSILON
                && (I64_MIN_F64..=I64_MAX_F64).contains(&number_value)
            {
                if number_value == 0.0 {
                    "0".into()
                } else {
                    let mut out = String::with_capacity(32);
                    match write!(&mut out, "{number_value:.0}") {
                        Ok(()) | Err(_) => {}
                    }
                    out
                }
            } else {
                let mut text = String::with_capacity(32);
                match write!(&mut text, "{number_value}") {
                    Ok(()) | Err(_) => {}
                }
                if text.contains('.') {
                    while text.ends_with('0') {
                        text.pop();
                    }
                    if text.ends_with('.') {
                        text.pop();
                    }
                }
                text
            }
        }
        CellValue::Empty => String::new(),
    }
}
pub fn source_header_scan_rows() -> usize {
    env::var("FCUPDATER_SOURCE_HEADER_SCAN_ROWS")
        .ok()
        .and_then(|parsed_value| parsed_value.parse::<usize>().ok())
        .filter(|parsed_value| *parsed_value > 0)
        .map_or(DEFAULT_SOURCE_HEADER_SCAN_ROWS, |parsed_value| {
            parsed_value.min(10_000)
        })
}
