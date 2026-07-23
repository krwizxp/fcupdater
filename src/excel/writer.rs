use self::cell_ref::parse_ref_with_locks;
use self::cell_ref::{
    MAX_A1_COL, MAX_A1_ROW, parse_range_token, ref_with_locks, with_unlocked_ref_parts,
};
use super::{
    CHANGE_LOG_SHEET_NAME, CHANGE_LOG_SHEET_PATH, MASTER_SHEET_NAME, MASTER_SHEET_PATH,
    SPREADSHEETML_NAMESPACE, SaveVerification, copy_text,
    xlsx_container::{XlsxContainer, validate_worksheet_core_namespaces},
    xml::{
        XmlAttrScanner, XmlScanner, decode_xml_entities, extract_all_tag_text, extract_attr,
        extract_first_tag_text, find_end_tag, find_start_tag, find_tag_end, is_valid_xml_char,
    },
};
use crate::{
    diagnostic::{AppError, Result, err, err_with_source},
    sheet_util::parse_i32_str,
};
use alloc::{borrow::Cow, rc::Rc};
use core::{
    cmp::Ordering,
    fmt::{Display, Write as FmtWrite},
    mem,
    range::{Range, RangeInclusive},
};
use std::collections::HashMap;
use std::path::Path;
mod cell_ref;
const XML_SPACE_PRESERVE_ATTR: &str = " xml:space=\"preserve\"";
const FILTER_DATABASE_NAME: &str = "_xlnm._FilterDatabase";
const FILTER_DATABASE_REF_PREFIX: &str = "유류비!$A$14:$W$";
const MAX_SHARED_STRING_COUNT: usize = 0x0010_0000;
const MAX_WORKSHEET_CELL_COUNT: usize = 0x0010_0000;
const MAX_XML_ATTRIBUTE_COUNT: usize = 128;
const MASTER_HEADERS: [&str; 23] = [
    "지역화폐적용순위",
    "지역",
    "상호",
    "상표",
    "셀프",
    "주소",
    "휘발유단가(원/L)",
    "고급유단가(원/L)",
    "울트라카젠 여부",
    "경유단가(원/L)",
    "스마트주유 할인(원/L)",
    "조정휘발유단가(원/L)",
    "조정고급유단가(원/L)",
    "조정경유단가(원/L)",
    "유종별 총가격(원)",
    "총가격(원)",
    "지역화폐 적용여부",
    "지역화폐적립율",
    "지역화폐적립액(원)",
    "지역화폐적용금액(원)",
    "지역화폐 적용단가(원/L)",
    "지역화폐 미적용 단가(원/L)",
    "정렬키",
];
const CHANGE_LOG_HEADERS: [&str; 13] = [
    "지역",
    "상호",
    "주소",
    "변경내용",
    "휘발유(이전)",
    "휘발유(신규)",
    "휘발유 Δ",
    "고급유(이전)",
    "고급유(신규)",
    "고급유 Δ",
    "경유(이전)",
    "경유(신규)",
    "경유 Δ",
];
const CHANGE_LOG_FORMULA_LAYOUT: FormulaLayout = FormulaLayout {
    data_start_row: 4,
    fixed_formulas: &[],
    optional_zero_col: None,
    required_cols: &[7, 10, 13],
};
const MASTER_FORMULA_LAYOUT: FormulaLayout = FormulaLayout {
    data_start_row: 15,
    fixed_formulas: &[
        (2, 10, "B4+B5+B6"),
        (2, 11, r#"IF(B4+B5=0,"",(B4*B7+B5*B8)/(B4+B5))"#),
        (2, 12, r#"IF(B5+B6=0,"",(B4*B7+B5*B9)/(B4+B5))"#),
    ],
    optional_zero_col: Some(11),
    required_cols: &[1, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22, 23],
};
#[derive(Debug)]
pub(crate) struct Workbook {
    change_log_sheet: Worksheet,
    container: XlsxContainer,
    master_sheet: Worksheet,
    shared_strings: SharedStringTable,
    shared_strings_xml_text: String,
    xml_text: String,
}
#[derive(Debug)]
pub(crate) struct SharedStringTable {
    index: HashMap<Rc<str>, usize>,
    original_len: usize,
    values: Vec<Rc<str>>,
}
#[derive(Debug)]
pub(crate) struct Worksheet {
    prefix: String,
    rows: Vec<Row>,
    suffix: String,
}
#[derive(Debug)]
struct XmlTagLocation {
    self_closing: bool,
    span: Range<usize>,
}
#[derive(Debug)]
pub(crate) struct Row {
    attrs_xml: String,
    cells: Vec<Cell>,
}
#[derive(Debug)]
struct Cell {
    col: u32,
    inner_xml: Option<String>,
    style: Option<u32>,
    value_type: CellValueType,
}
#[derive(Clone, Copy)]
struct FormulaLayout {
    data_start_row: u32,
    fixed_formulas: &'static [(u32, u32, &'static str)],
    optional_zero_col: Option<u32>,
    required_cols: &'static [u32],
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CellValueType {
    General,
    Number,
    SharedString,
    String,
}
impl CellValueType {
    const fn xml_attr(self) -> Option<&'static str> {
        match self {
            Self::General => None,
            Self::Number => Some("n"),
            Self::SharedString => Some("s"),
            Self::String => Some("str"),
        }
    }
}
#[derive(Debug)]
struct XmlAttr {
    name: Cow<'static, str>,
    value: String,
}
struct WorksheetRowParser<'row> {
    row_body: &'row str,
    row_num: u32,
}
struct WorksheetXmlParser<'xml> {
    xml: &'xml str,
}
#[derive(Clone, Copy)]
enum XmlEscapeContext {
    Attribute,
    Text,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CellReference {
    pub col: u32,
    pub col_locked: bool,
    pub row: u32,
    pub row_locked: bool,
}
impl SharedStringTable {
    fn intern(&mut self, value: &str) -> Result<usize> {
        if let Some(&index) = self.index.get(value) {
            return Ok(index);
        }
        if self.values.len() >= MAX_SHARED_STRING_COUNT {
            return Err(err(format!(
                "sharedStrings entry 개수가 허용 한도({MAX_SHARED_STRING_COUNT})를 초과했습니다."
            )));
        }
        self.index.try_reserve(1).map_err(|source| {
            err_with_source("shared string index 추가 메모리 확보 실패", source)
        })?;
        self.values.try_reserve(1).map_err(|source| {
            err_with_source("sharedStrings entry 추가 메모리 확보 실패", source)
        })?;
        let index = self.values.len();
        let stored_value = Rc::<str>::from(value);
        self.values.push(Rc::clone(&stored_value));
        self.index.insert(stored_value, index);
        Ok(index)
    }
    pub(crate) fn set_cell(
        &mut self,
        worksheet: &mut Worksheet,
        col: u32,
        row: u32,
        value: &str,
    ) -> Result<()> {
        let index = self.intern(value)?;
        worksheet.set_shared_string_index_at(col, row, index)?;
        Ok(())
    }
    pub(crate) fn values(&self) -> &[Rc<str>] {
        &self.values
    }
}
impl TryFrom<Vec<String>> for SharedStringTable {
    type Error = AppError;
    fn try_from(values: Vec<String>) -> Result<Self> {
        let original_len = values.len();
        let mut index = HashMap::new();
        index.try_reserve(original_len).map_err(|source| {
            err_with_source("shared string index map 메모리 확보 실패", source)
        })?;
        let mut shared_values = Vec::new();
        shared_values
            .try_reserve_exact(original_len)
            .map_err(|source| err_with_source("shared string 값 목록 메모리 확보 실패", source))?;
        for (value_index, value) in values.into_iter().enumerate() {
            if let Some((stored_value, _)) = index.get_key_value(value.as_str()) {
                shared_values.push(Rc::clone(stored_value));
                continue;
            }
            let stored_value = Rc::<str>::from(value);
            index.insert(Rc::clone(&stored_value), value_index);
            shared_values.push(stored_value);
        }
        Ok(Self {
            index,
            original_len,
            values: shared_values,
        })
    }
}
impl Workbook {
    pub(crate) const fn change_log_sheet_mut(
        &mut self,
    ) -> (&mut Worksheet, &mut SharedStringTable) {
        (&mut self.change_log_sheet, &mut self.shared_strings)
    }
    pub(crate) fn from_container(mut container: XlsxContainer) -> Result<Self> {
        let workbook_xml = container.take_text("xl/workbook.xml")?;
        let mut workbook_scanner = XmlScanner::new(&workbook_xml);
        let calc_pr = workbook_scanner
            .next_start_named("calcPr")
            .ok_or_else(|| err("workbook.xml의 calcPr 태그를 찾지 못했습니다."))?;
        if calc_pr.name() != "calcPr" || !calc_pr.self_closing() {
            return Err(err(
                "workbook.xml의 calcPr는 unprefixed self-closing 태그여야 합니다.",
            ));
        }
        if workbook_scanner.next_start_named("calcPr").is_some() {
            return Err(err("workbook.xml에 calcPr 태그가 여러 개 있습니다."));
        }
        container.ensure_fixed_sheet_catalog(&workbook_xml)?;
        let shared_strings_xml_text = container.take_shared_strings_text()?;
        let mut shared_strings_scanner = XmlScanner::new(&shared_strings_xml_text);
        let sst_tag = shared_strings_scanner
            .next_start_named("sst")
            .ok_or_else(|| err("sharedStrings XML에 <sst>가 없습니다."))?;
        if sst_tag.name() != "sst" || sst_tag.self_closing() {
            return Err(err(
                "sharedStrings XML의 sst root 형식이 고정 스키마와 다릅니다.",
            ));
        }
        let sst_attrs = parse_tag_attrs(sst_tag.raw())?;
        if sst_attrs.len() != 3
            || get_attr(&sst_attrs, "xmlns") != Some(SPREADSHEETML_NAMESPACE)
            || get_attr(&sst_attrs, "count").is_none()
            || get_attr(&sst_attrs, "uniqueCount").is_none()
        {
            return Err(err(
                "sharedStrings XML의 sst root 속성이 고정 스키마와 다릅니다.",
            ));
        }
        parse_usize_decimal(
            get_attr(&sst_attrs, "count").ok_or_else(|| err("sharedStrings count가 없습니다."))?,
            "sharedStrings count 해석 실패",
        )?;
        parse_usize_decimal(
            get_attr(&sst_attrs, "uniqueCount")
                .ok_or_else(|| err("sharedStrings uniqueCount가 없습니다."))?,
            "sharedStrings uniqueCount 해석 실패",
        )?;
        let sst_close_search =
            checked_usize_add(sst_tag.end(), 1, "sharedStrings 종료 태그 검색 시작")?;
        if find_end_tag(&shared_strings_xml_text, "sst", sst_close_search).is_none() {
            return Err(err("sharedStrings XML에 </sst>가 없습니다."));
        }
        if shared_strings_scanner.next_start_named("sst").is_some() {
            return Err(err("sharedStrings XML에 sst root가 여러 개 있습니다."));
        }
        let mut shared_string_values = Vec::new();
        let mut scanner = XmlScanner::new(&shared_strings_xml_text);
        while let Some(si_tag) = scanner.next_start_named("si") {
            if shared_string_values.len() >= MAX_SHARED_STRING_COUNT {
                return Err(err(format!(
                    "sharedStrings entry 개수가 허용 한도({MAX_SHARED_STRING_COUNT})를 초과했습니다."
                )));
            }
            if shared_string_values.len() == shared_string_values.capacity() {
                shared_string_values.try_reserve(1).map_err(|source| {
                    err_with_source("sharedStrings entry 메모리 확보 실패", source)
                })?;
            }
            if si_tag.self_closing() {
                shared_string_values.push(String::new());
                continue;
            }
            let si_tag_end = si_tag.end();
            let body_start = si_tag_end
                .checked_add(1)
                .ok_or_else(|| err("sharedStrings.xml의 <si> 본문 시작 계산에 실패했습니다."))?;
            let Some(si_end) = find_end_tag(&shared_strings_xml_text, "si", body_start) else {
                return Err(err("sharedStrings.xml의 </si> 태그를 찾지 못했습니다."));
            };
            let si_body = shared_strings_xml_text
                .get(body_start..si_end)
                .ok_or_else(|| err("sharedStrings.xml의 <si> 본문 범위가 손상되었습니다."))?;
            let text =
                extract_all_tag_text(si_body, "t")?.map_or_else(String::new, Cow::into_owned);
            shared_string_values.push(text);
            let Some(si_close_end) = find_tag_end(&shared_strings_xml_text, si_end) else {
                return Err(err("sharedStrings.xml의 </si> 태그가 손상되었습니다."));
            };
            let next_cursor = si_close_end
                .checked_add(1)
                .ok_or_else(|| err("sharedStrings.xml의 다음 <si> 위치 계산에 실패했습니다."))?;
            scanner.skip_to(next_cursor);
        }
        let shared_strings = SharedStringTable::try_from(shared_string_values)?;
        let master_xml = container.take_text(MASTER_SHEET_PATH)?;
        validate_worksheet_core_namespaces(&master_xml, MASTER_SHEET_NAME)?;
        let master_sheet = WorksheetXmlParser { xml: &master_xml }.parse()?;
        master_sheet.validate_fixed_header(MASTER_SHEET_NAME, shared_strings.values())?;
        let change_log_xml = container.take_text(CHANGE_LOG_SHEET_PATH)?;
        validate_worksheet_core_namespaces(&change_log_xml, CHANGE_LOG_SHEET_NAME)?;
        let change_log_sheet = WorksheetXmlParser {
            xml: &change_log_xml,
        }
        .parse()?;
        change_log_sheet.validate_fixed_header(CHANGE_LOG_SHEET_NAME, shared_strings.values())?;
        let workbook = Self {
            change_log_sheet,
            container,
            master_sheet,
            shared_strings,
            shared_strings_xml_text,
            xml_text: workbook_xml,
        };
        workbook.validate_fixed_semantics()?;
        Ok(workbook)
    }
    pub(crate) const fn master_sheet_mut(&mut self) -> (&mut Worksheet, &mut SharedStringTable) {
        (&mut self.master_sheet, &mut self.shared_strings)
    }
    fn request_full_recalculation(&mut self) -> Result<()> {
        let out = &mut self.xml_text;
        let location = find_start_tag_location(out, "calcPr", 0)?
            .ok_or_else(|| err("workbook.xml의 calcPr 태그를 찾지 못했습니다."))?;
        let mut attrs = parse_tag_attrs_at(
            out,
            &location,
            "workbook.xml의 calcPr 태그 범위가 손상되었습니다.",
        )?;
        reserve_xml_attrs(&mut attrs, 4, "calcPr 속성 목록 추가 메모리 확보 실패")?;
        set_attr(&mut attrs, "calcMode", "auto");
        set_attr(&mut attrs, "fullCalcOnLoad", "1");
        set_attr(&mut attrs, "forceFullCalc", "1");
        set_attr(&mut attrs, "calcCompleted", "0");
        let new_tag = build_self_closing_tag("calcPr", &attrs)?;
        out.replace_range(location.span, &new_tag);
        Ok(())
    }
    pub(crate) fn save(mut self, target_path: &Path, verification: SaveVerification) -> Result<()> {
        self.request_full_recalculation()?;
        let mut shared_string_reference_count = 0_usize;
        for (sheet_name, sheet_path, sheet) in [
            (MASTER_SHEET_NAME, MASTER_SHEET_PATH, &self.master_sheet),
            (
                CHANGE_LOG_SHEET_NAME,
                CHANGE_LOG_SHEET_PATH,
                &self.change_log_sheet,
            ),
        ] {
            sheet.validate_fixed_header(sheet_name, self.shared_strings.values())?;
            let (sheet_xml, sheet_reference_count) = sheet.to_xml()?;
            shared_string_reference_count =
                shared_string_reference_count.saturating_add(sheet_reference_count);
            self.container.put_text(sheet_path, sheet_xml)?;
        }
        self.update_shared_strings_xml_text(
            self.shared_strings.original_len,
            shared_string_reference_count,
            self.shared_strings.values.len(),
        )?;
        self.container.put_text("xl/workbook.xml", self.xml_text)?;
        self.container
            .put_text("xl/sharedStrings.xml", self.shared_strings_xml_text)?;
        self.container.save(target_path, verification)
    }
    pub(crate) fn update_filter_database_defined_name(&mut self, last_data_row: u32) -> Result<()> {
        let (row_span, _) = fixed_filter_database_row(&self.xml_text)?;
        let replacement_capacity = u32_decimal_text_len(last_data_row);
        let mut replacement = String::new();
        replacement
            .try_reserve_exact(replacement_capacity)
            .map_err(|source| err_with_source("_FilterDatabase ref 메모리 확보 실패", source))?;
        push_decimal_text(&mut replacement, last_data_row);
        self.xml_text.replace_range(row_span, &replacement);
        Ok(())
    }
    fn update_shared_strings_xml_text(
        &mut self,
        existing_table_len: usize,
        shared_string_reference_count: usize,
        unique_count: usize,
    ) -> Result<()> {
        let new_values = self
            .shared_strings
            .values()
            .get(existing_table_len..)
            .ok_or_else(|| err("sharedStrings 신규 값 범위가 손상되었습니다."))?;
        let original_xml = &self.shared_strings_xml_text;
        let mut scanner = XmlScanner::new(original_xml);
        let Some(sst_tag) = scanner.next_start_named("sst") else {
            return Err(err("sharedStrings XML에 <sst>가 없습니다."));
        };
        let (open_start, open_end) = (sst_tag.start(), sst_tag.end());
        let open_tag_span = RangeInclusive {
            start: open_start,
            last: open_end,
        };
        let open_tag = original_xml
            .get(open_tag_span)
            .ok_or_else(|| err("sharedStrings XML의 <sst> 태그 범위가 손상되었습니다."))?;
        let mut attrs = parse_tag_attrs(open_tag)?;
        set_attr(
            &mut attrs,
            "count",
            shared_string_reference_count.to_string(),
        );
        set_attr(&mut attrs, "uniqueCount", unique_count.to_string());
        let mut new_si_xml = String::new();
        for value in new_values {
            let escaped_len =
                validated_xml_escaped_len(value, XmlEscapeContext::Text, "shared string XML")?;
            let text_attrs =
                if value.starts_with(' ') || value.ends_with(' ') || value.contains("  ") {
                    XML_SPACE_PRESERVE_ATTR
                } else {
                    ""
                };
            let additional =
                checked_capacity(&["<si><t></t></si>".len(), text_attrs.len(), escaped_len])
                    .ok_or_else(|| err("shared string XML 용량 계산 실패"))?;
            new_si_xml
                .try_reserve_exact(additional)
                .map_err(|source| err_with_source("shared string XML 메모리 확보 실패", source))?;
            new_si_xml.push_str("<si><t");
            new_si_xml.push_str(text_attrs);
            new_si_xml.push('>');
            append_xml_escaped(&mut new_si_xml, value, XmlEscapeContext::Text);
            new_si_xml.push_str("</t></si>");
        }
        let replacement = build_open_tag("sst", &attrs)?;
        let close_search_from =
            checked_usize_add(open_end, 1, "sharedStrings 종료 태그 검색 시작")?;
        let original_close_start = find_end_tag(original_xml, "sst", close_search_from)
            .ok_or_else(|| err("sharedStrings XML에 </sst>가 없습니다."))?;
        let mut updated_xml = mem::take(&mut self.shared_strings_xml_text);
        updated_xml.insert_str(original_close_start, &new_si_xml);
        updated_xml.replace_range(open_tag_span, &replacement);
        self.shared_strings_xml_text = updated_xml;
        Ok(())
    }
    fn validate_filter_database(&self, filter_last_row: u32) -> Result<()> {
        let (_, defined_last_row) = fixed_filter_database_row(&self.xml_text)?;
        if defined_last_row != filter_last_row {
            return Err(err(format!(
                "_FilterDatabase 범위가 autoFilter와 다릅니다: {defined_last_row} != {filter_last_row}"
            )));
        }
        Ok(())
    }
    fn validate_fixed_semantics(&self) -> Result<()> {
        let shared_strings = self.shared_strings.values();
        let (master_shared_refs, master_last_row) =
            self.master_sheet
                .semantic_facts(MASTER_SHEET_NAME, 15, 23, shared_strings)?;
        let filter_last_row = self.master_sheet.fixed_master_auto_filter()?.2;
        if master_last_row != Some(filter_last_row) {
            return Err(err(format!(
                "유류비 autoFilter 마지막 행이 실제 데이터 마지막 행과 다릅니다: filter={filter_last_row}, actual={master_last_row:?}"
            )));
        }
        self.master_sheet.validate_formula_layout(
            MASTER_SHEET_NAME,
            master_last_row,
            MASTER_FORMULA_LAYOUT,
            shared_strings,
        )?;
        let mut address_last_row = 15_u32;
        for row in self.master_sheet.row_numbers_from(15)? {
            if !self
                .master_sheet
                .try_get_display_at(6, row, shared_strings)?
                .trim()
                .is_empty()
            {
                address_last_row = row;
            }
        }
        if address_last_row != filter_last_row {
            return Err(err(format!(
                "유류비 autoFilter 마지막 행이 실제 주소 데이터 마지막 행과 다릅니다: filter={filter_last_row}, actual={address_last_row}"
            )));
        }
        self.validate_filter_database(filter_last_row)?;
        let (change_log_shared_refs, change_log_last_row) =
            self.change_log_sheet
                .semantic_facts(CHANGE_LOG_SHEET_NAME, 4, 13, shared_strings)?;
        self.change_log_sheet.validate_formula_layout(
            CHANGE_LOG_SHEET_NAME,
            change_log_last_row,
            CHANGE_LOG_FORMULA_LAYOUT,
            shared_strings,
        )?;
        self.change_log_sheet
            .validate_change_log_formats(change_log_last_row.unwrap_or(4))?;
        let shared_ref_count = master_shared_refs
            .checked_add(change_log_shared_refs)
            .ok_or_else(|| err("shared string 참조 수 계산 실패"))?;
        self.validate_shared_string_counts(shared_ref_count)
    }
    fn validate_shared_string_counts(&self, shared_ref_count: usize) -> Result<()> {
        let sst_start = find_start_tag(&self.shared_strings_xml_text, "sst", 0)
            .ok_or_else(|| err("sharedStrings XML에 <sst>가 없습니다."))?;
        let sst_end = find_tag_end(&self.shared_strings_xml_text, sst_start)
            .ok_or_else(|| err("sharedStrings XML의 <sst> 태그가 손상되었습니다."))?;
        let sst_tag = self
            .shared_strings_xml_text
            .get(sst_start..=sst_end)
            .ok_or_else(|| err("sharedStrings XML의 <sst> 태그 범위가 손상되었습니다."))?;
        let attrs = parse_tag_attrs(sst_tag)?;
        for (name, actual) in [
            ("count", shared_ref_count),
            ("uniqueCount", self.shared_strings.values.len()),
        ] {
            let declared_text = get_attr(&attrs, name)
                .ok_or_else(|| err(format!("sharedStrings {name} 속성이 없습니다.")))?;
            let declared_count =
                parse_usize_decimal(declared_text, "sharedStrings count 해석 실패")?;
            if declared_count != actual {
                return Err(err(format!(
                    "sharedStrings {name}가 실제 값과 다릅니다: declared={declared_count}, actual={actual}"
                )));
            }
        }
        Ok(())
    }
}
impl WorksheetRowParser<'_> {
    fn parse_into(&self, row: &mut Row, cell_count: &mut usize) -> Result<()> {
        let mut scanner = XmlScanner::new(self.row_body);
        let mut next_col = 1_u32;
        while let Some(cell_info) = scanner.next_start_named("c") {
            let cell_tag_end = cell_info.end();
            let cell_tag = cell_info.raw();
            let mut attrs = parse_tag_attrs(cell_tag)?;
            let reference_text = get_attr(&attrs, "r")
                .ok_or_else(|| err(format!("cell reference가 없습니다: row={}", self.row_num)))?;
            let reference = parse_ref_with_locks(reference_text).ok_or_else(|| {
                err(format!(
                    "cell reference 형식이 비정상입니다: row={}, ref={reference_text}",
                    self.row_num
                ))
            })?;
            if reference.row != self.row_num {
                return Err(err(format!(
                    "cell reference row가 row 태그와 다릅니다: row={}, ref={reference_text}",
                    self.row_num
                )));
            }
            let col = reference.col;
            if !(1..=MAX_A1_COL).contains(&col) {
                return Err(err(row_col_error(
                    "Excel column 범위를 벗어난 cell이 있습니다. (row=",
                    self.row_num,
                    col,
                )));
            }
            if col < next_col {
                return Err(err(row_col_error(
                    "worksheet cell 순서는 column 오름차순이어야 합니다. (row=",
                    self.row_num,
                    col,
                )));
            }
            remove_attr(&mut attrs, "r");
            let style = take_attr(&mut attrs, "s")
                .map(|value| {
                    parse_u32_decimal(
                        &value,
                        row_col_error(
                            "worksheet cell style이 음이 아닌 10진수 형식이 아닙니다. (row=",
                            self.row_num,
                            col,
                        ),
                        row_col_error("worksheet cell style 해석 실패 (row=", self.row_num, col),
                    )
                })
                .transpose()?;
            let value_type = if let Some(value) = take_attr(&mut attrs, "t") {
                match value.as_str() {
                    "n" => CellValueType::Number,
                    "s" => CellValueType::SharedString,
                    "str" => CellValueType::String,
                    _ => {
                        return Err(err(format!(
                            "고정 workbook에서 지원하지 않는 cell type입니다: row={}, col={col}, type={value}",
                            self.row_num
                        )));
                    }
                }
            } else {
                CellValueType::General
            };
            if let Some(attr) = attrs.first() {
                return Err(err(format!(
                    "고정 workbook cell에 지원하지 않는 속성이 있습니다: row={}, col={col}, attribute={}",
                    self.row_num, attr.name
                )));
            }
            let has_attrs = style.is_some() || value_type != CellValueType::General;
            if cell_info.self_closing() {
                if has_attrs {
                    self.retain_cell(
                        row,
                        cell_count,
                        col,
                        Cell {
                            col,
                            inner_xml: None,
                            style,
                            value_type,
                        },
                    )?;
                }
                next_col = next_cell_col(self.row_num, col)?;
                continue;
            }
            let cell_body_start = checked_usize_add(cell_tag_end, 1, "row cell 본문 시작")?;
            let Some(cell_body_end) = find_end_tag(self.row_body, "c", cell_body_start) else {
                return Err(err(row_col_error(
                    "row 내 cell 종료 태그를 찾지 못했습니다. (row=",
                    self.row_num,
                    col,
                )));
            };
            let cell_body_span = Range {
                start: cell_body_start,
                end: cell_body_end,
            };
            let inner_xml_text = self.row_body.get(cell_body_span).ok_or_else(|| {
                err(row_col_error(
                    "row 내 cell 본문 범위가 손상되었습니다. (row=",
                    self.row_num,
                    col,
                ))
            })?;
            if has_attrs || !inner_xml_text.is_empty() {
                let inner_xml = copy_text(inner_xml_text, "row cell 본문 복사")?;
                self.retain_cell(
                    row,
                    cell_count,
                    col,
                    Cell {
                        col,
                        inner_xml: Some(inner_xml),
                        style,
                        value_type,
                    },
                )?;
            }
            next_col = next_cell_col(self.row_num, col)?;
            let cell_close_end = find_tag_end(self.row_body, cell_body_end)
                .ok_or_else(|| err("row 내 cell 종료 태그가 손상되었습니다."))?;
            scanner.skip_to(checked_usize_add(
                cell_close_end,
                1,
                "row cell 다음 cursor",
            )?);
        }
        Ok(())
    }
    fn retain_cell(
        &self,
        row: &mut Row,
        cell_count: &mut usize,
        col: u32,
        cell: Cell,
    ) -> Result<()> {
        if *cell_count >= MAX_WORKSHEET_CELL_COUNT {
            return Err(err(format!(
                "worksheet cell 개수가 허용 한도({MAX_WORKSHEET_CELL_COUNT})를 초과했습니다."
            )));
        }
        if let Some(inner_xml) = cell.inner_xml.as_deref()
            && let Some(formula_start) = find_start_tag(inner_xml, "f", 0)
        {
            let formula_end = find_tag_end(inner_xml, formula_start)
                .ok_or_else(|| err("cell formula 시작 태그가 손상되었습니다."))?;
            let formula_open = inner_xml
                .get(RangeInclusive {
                    start: formula_start,
                    last: formula_end,
                })
                .ok_or_else(|| err("cell formula 시작 태그 범위가 손상되었습니다."))?;
            let formula_attrs = parse_tag_attrs(formula_open)?;
            if !formula_attrs.is_empty()
                && (formula_attrs.len() != 1
                    || formula_attrs
                        .first()
                        .is_none_or(|attr| attr.name != "aca" || attr.value != "false"))
            {
                return Err(err(format!(
                    "고정 workbook은 aca=\"false\" 외 formula 속성을 지원하지 않습니다: row={}, col={col}",
                    self.row_num
                )));
            }
            if find_start_tag(inner_xml, "v", 0).is_none() {
                return Err(err(format!(
                    "고정 workbook formula cache가 없습니다: row={}, col={col}",
                    self.row_num
                )));
            }
        }
        row.cells.push(cell);
        *cell_count = (*cell_count)
            .checked_add(1)
            .ok_or_else(|| err("worksheet cell 수 계산 실패"))?;
        Ok(())
    }
}
impl WorksheetXmlParser<'_> {
    fn collect_rows(&self, body_span: Range<usize>) -> Result<Vec<Row>> {
        let Some(body) = self.xml.get(body_span) else {
            return Err(err("worksheet XML body 범위가 손상되었습니다."));
        };
        let mut rows = Vec::new();
        let mut cell_count = 0_usize;
        let mut scanner = XmlScanner::new(body);
        while let Some(row_info) = scanner.next_start_named("row") {
            let row_tag_end = row_info.end();
            let mut row_attrs = parse_tag_attrs(row_info.raw())?;
            let row_num_text = get_attr(&row_attrs, "r")
                .ok_or_else(|| err("고정 workbook의 worksheet row에 r 속성이 없습니다."))?;
            let row_num = parse_positive_u32_decimal(
                row_num_text,
                "worksheet row 번호가 양의 10진수 형식이 아닙니다.",
                "worksheet row 번호 해석 실패",
                "worksheet row 번호는 1 이상이어야 합니다.",
            )?;
            if !(1..=MAX_A1_ROW).contains(&row_num) {
                return Err(err(format!(
                    "worksheet row 번호가 Excel 범위를 벗어났습니다: {row_num}"
                )));
            }
            let expected_row_num = u32::try_from(rows.len())
                .ok()
                .and_then(|count| count.checked_add(1))
                .ok_or_else(|| err("worksheet 연속 row 번호 계산 실패"))?;
            if row_num != expected_row_num {
                return Err(err(format!(
                    "worksheet row 번호는 1부터 연속이어야 합니다: expected={expected_row_num}, current={row_num}"
                )));
            }
            remove_attr(&mut row_attrs, "r");
            let attrs_capacity = row_attrs.iter().try_fold(0_usize, |sum, attr| {
                let escaped_len = validated_xml_escaped_len(
                    &attr.value,
                    XmlEscapeContext::Attribute,
                    "worksheet row 속성 직렬화",
                )?;
                checked_capacity(&[
                    sum,
                    " ".len(),
                    attr.name.len(),
                    "=\"".len(),
                    escaped_len,
                    "\"".len(),
                ])
                .ok_or_else(|| err("worksheet row 속성 직렬화 용량 계산 실패"))
            })?;
            let mut attrs_xml = String::new();
            attrs_xml
                .try_reserve_exact(attrs_capacity)
                .map_err(|source| err_with_source("worksheet row 속성 직렬화", source))?;
            if !row_attrs
                .iter()
                .zip(row_attrs.iter().skip(1))
                .all(|(left, right)| attr_cmp(left, right) != Ordering::Greater)
            {
                row_attrs.sort_unstable_by(attr_cmp);
            }
            for attr in &row_attrs {
                push_attr_xml(&mut attrs_xml, attr);
            }
            rows.try_reserve(1)
                .map_err(|source| err_with_source("worksheet row 메모리 확보 실패", source))?;
            if row_info.self_closing() {
                rows.push(Row {
                    attrs_xml,
                    cells: Vec::new(),
                });
                continue;
            }
            let row_body_start = checked_usize_add(row_tag_end, 1, "sheetData row 본문 시작")?;
            let Some(row_body_end) = find_end_tag(body, "row", row_body_start) else {
                return Err(err(row_only_error(
                    "sheetData row 종료 태그를 찾지 못했습니다. (row=",
                    row_num,
                )));
            };
            let row_body = body.get(row_body_start..row_body_end).ok_or_else(|| {
                err(row_only_error(
                    "sheetData row 본문 범위가 손상되었습니다. (row=",
                    row_num,
                ))
            })?;
            let mut row = Row {
                attrs_xml,
                cells: Vec::new(),
            };
            WorksheetRowParser { row_body, row_num }.parse_into(&mut row, &mut cell_count)?;
            rows.push(row);
            let row_close_end = find_tag_end(body, row_body_end)
                .ok_or_else(|| err("sheetData row 종료 태그가 손상되었습니다."))?;
            scanner.skip_to(checked_usize_add(
                row_close_end,
                1,
                "sheetData row 다음 cursor",
            )?);
        }
        Ok(rows)
    }
    fn parse(&self) -> Result<Worksheet> {
        let mut scanner = XmlScanner::new(self.xml);
        let Some(sheet_data_tag) = scanner.next_start_named("sheetData") else {
            return Err(err("worksheet XML에 <sheetData>가 없습니다."));
        };
        let sheet_data_open_end = sheet_data_tag.end();
        if sheet_data_tag.self_closing() {
            return Err(err("고정 workbook의 sheetData는 비어 있을 수 없습니다."));
        }
        let sheet_data_body_start =
            checked_usize_add(sheet_data_open_end, 1, "worksheet sheetData body 시작")?;
        let Some(sheet_data_close) = find_end_tag(self.xml, "sheetData", sheet_data_body_start)
        else {
            return Err(err("worksheet XML에 </sheetData>가 없습니다."));
        };
        let sheet_data_body_span = Range {
            start: sheet_data_body_start,
            end: sheet_data_close,
        };
        let prefix_raw = self
            .xml
            .get(..sheet_data_body_span.start)
            .ok_or_else(|| err("worksheet XML prefix 범위가 손상되었습니다."))?;
        let suffix_raw = self
            .xml
            .get(sheet_data_body_span.end..)
            .ok_or_else(|| err("worksheet XML suffix 범위가 손상되었습니다."))?;
        let prefix = copy_text(prefix_raw, "worksheet XML prefix 복사")?;
        let suffix = copy_text(suffix_raw, "worksheet XML suffix 복사")?;
        let rows = self.collect_rows(sheet_data_body_span)?;
        Ok(Worksheet {
            prefix,
            rows,
            suffix,
        })
    }
}
impl Worksheet {
    pub(crate) fn clear_cells_in_rows_through_col(
        &mut self,
        rows: RangeInclusive<u32>,
        max_col: u32,
    ) {
        for (row_num, row_obj) in (1_u32..=MAX_A1_ROW).zip(&mut self.rows) {
            if row_num < rows.start {
                continue;
            }
            if row_num > rows.last {
                break;
            }
            for cell in row_obj
                .cells
                .iter_mut()
                .take_while(|cell| cell.col <= max_col)
            {
                cell.value_type = CellValueType::General;
                cell.inner_xml = None;
            }
        }
    }
    pub(crate) fn copy_row_style(
        &mut self,
        source_row: u32,
        target_row: u32,
        max_col: u32,
    ) -> Result<()> {
        let Some(src) = row_index(source_row).and_then(|index| self.rows.get(index)) else {
            return Ok(());
        };
        let mut copied = src.try_copy()?;
        copied
            .cells
            .truncate(copied.cells.partition_point(|cell| cell.col <= max_col));
        for cell in &mut copied.cells {
            cell.value_type = CellValueType::General;
            cell.inner_xml = None;
        }
        let target_index = usize::try_from(target_row)
            .ok()
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| err("worksheet style 대상 row 번호가 올바르지 않습니다."))?;
        let required_len = target_index
            .checked_add(1)
            .ok_or_else(|| err("worksheet style 대상 row 길이 계산 실패"))?;
        if self.rows.len() < required_len {
            self.rows
                .try_reserve(required_len.saturating_sub(self.rows.len()))
                .map_err(|source| {
                    err_with_source("worksheet style 대상 row 메모리 확보 실패", source)
                })?;
            self.rows.resize_with(required_len, Row::empty);
        }
        let target = self
            .rows
            .get_mut(target_index)
            .ok_or_else(|| err("worksheet style 대상 row 범위 오류"))?;
        *target = copied;
        Ok(())
    }
    pub(crate) fn extend_conditional_formats(
        &mut self,
        old_data_rows: RangeInclusive<u32>,
        data_rows: RangeInclusive<u32>,
        target_cols: &[u32],
    ) -> Result<()> {
        if target_cols.is_empty() {
            return Ok(());
        }
        let data_start_row = data_rows.start;
        let old_data_start_row = old_data_rows.start;
        let old_last_data_row = old_data_rows.last;
        let old_data_rows_empty = old_last_data_row < old_data_start_row;
        let last_data_row = data_rows.last;
        let out = &mut self.suffix;
        let mut cursor = 0_usize;
        while let Some(location) = find_start_tag_location(out, "conditionalFormatting", cursor)? {
            let cf_start = location.span.start;
            let mut attrs = parse_tag_attrs_at(
                out,
                &location,
                "conditionalFormatting 태그 범위가 손상되었습니다.",
            )?;
            let Some(sqref_index) = attrs.iter().position(|attr| attr.name == "sqref") else {
                cursor = location.span.end;
                continue;
            };
            let sqref = attrs.swap_remove(sqref_index).value;
            let mut changed = false;
            let mut ranges_out: Vec<Cow<'_, str>> = Vec::new();
            let range_count = sqref.split_whitespace().count();
            ranges_out.try_reserve_exact(range_count).map_err(|source| {
                err_with_source(
                    format!("conditionalFormatting range 목록 메모리 확보 실패: {range_count} ranges"),
                    source,
                )
            })?;
            for token in sqref.split_whitespace() {
                let (start_ref, end_ref) = parse_range_token(token);
                let Some(start_reference) = parse_ref_with_locks(start_ref) else {
                    ranges_out.push(Cow::Borrowed(token));
                    continue;
                };
                let Some(end_reference) = parse_ref_with_locks(end_ref) else {
                    ranges_out.push(Cow::Borrowed(token));
                    continue;
                };
                let target_col_hit = target_cols
                    .iter()
                    .any(|col| (start_reference.col..=end_reference.col).contains(col));
                let template_range =
                    start_reference.row == data_start_row && end_reference.row == data_start_row;
                let previous_data_range = start_reference.row == old_data_start_row
                    && (end_reference.row == old_last_data_row
                        || (old_data_rows_empty && end_reference.row >= old_data_start_row));
                if !target_col_hit || !(template_range || previous_data_range) {
                    ranges_out.push(Cow::Borrowed(token));
                    continue;
                }
                let mut new_start = ref_with_locks(start_reference.with_row(data_start_row))?;
                let new_end = ref_with_locks(end_reference.with_row(last_data_row))?;
                let extra_len =
                    checked_usize_add(1, new_end.len(), "conditionalFormatting range 추가 용량")?;
                new_start.try_reserve_exact(extra_len).map_err(|source| {
                    err_with_source("conditionalFormatting range 메모리 확보 실패", source)
                })?;
                new_start.push(':');
                new_start.push_str(&new_end);
                ranges_out.push(Cow::Owned(new_start));
                changed = true;
            }
            let maybe_updated_sqref = if changed {
                let mut out_sqref = String::new();
                out_sqref.try_reserve_exact(sqref.len()).map_err(|source| {
                    err_with_source("conditionalFormatting sqref 메모리 확보 실패", source)
                })?;
                for (index, range) in ranges_out.iter().enumerate() {
                    if index != 0 {
                        out_sqref.push(' ');
                    }
                    out_sqref.push_str(range.as_ref());
                }
                Some(out_sqref)
            } else {
                None
            };
            let updated_sqref = maybe_updated_sqref.unwrap_or(sqref);
            set_attr(&mut attrs, "sqref", updated_sqref);
            let new_tag = if location.self_closing {
                build_self_closing_tag("conditionalFormatting", &attrs)?
            } else {
                build_open_tag("conditionalFormatting", &attrs)?
            };
            out.replace_range(location.span, &new_tag);
            cursor =
                checked_usize_add(cf_start, new_tag.len(), "conditionalFormatting 다음 cursor")?;
        }
        Ok(())
    }
    fn fixed_master_auto_filter(&self) -> Result<(XmlTagLocation, Vec<XmlAttr>, u32)> {
        let xml = &self.suffix;
        let location = find_start_tag_location(xml, "autoFilter", 0)?
            .ok_or_else(|| err("worksheet XML의 autoFilter 태그를 찾지 못했습니다."))?;
        if find_start_tag_location(xml, "autoFilter", location.span.end)?.is_some() {
            return Err(err("worksheet XML에 autoFilter 태그가 중복되어 있습니다."));
        }
        let attrs = parse_tag_attrs_at(
            xml,
            &location,
            "worksheet XML의 autoFilter 태그 범위가 손상되었습니다.",
        )?;
        let existing_ref = get_attr(&attrs, "ref")
            .ok_or_else(|| err("worksheet autoFilter ref 속성이 없습니다."))?;
        let (start_ref, end_ref) = parse_range_token(existing_ref);
        let start_reference = parse_ref_with_locks(start_ref)
            .ok_or_else(|| err("worksheet autoFilter 시작 reference 해석 실패"))?;
        let end_reference = parse_ref_with_locks(end_ref)
            .ok_or_else(|| err("worksheet autoFilter 끝 reference 해석 실패"))?;
        if (start_reference.col, start_reference.row) != (1, 14)
            || end_reference.col != 23
            || end_reference.row < 14
        {
            return Err(err(format!(
                "worksheet autoFilter 범위가 고정 스키마와 다릅니다: {existing_ref}"
            )));
        }
        Ok((location, attrs, end_reference.row))
    }
    pub(crate) fn get_i32_at(
        &self,
        col: u32,
        row: u32,
        shared_strings: &[Rc<str>],
    ) -> Result<Option<i32>> {
        let text = self.try_get_display_at(col, row, shared_strings)?;
        Ok(parse_i32_str(&text))
    }
    fn get_or_create_cell_mut(rows: &mut Vec<Row>, col: u32, row: u32) -> Result<&mut Cell> {
        let row_index = usize::try_from(row)
            .ok()
            .and_then(|value| value.checked_sub(1))
            .ok_or_else(|| err("worksheet cell row 번호가 올바르지 않습니다."))?;
        let required_len = row_index
            .checked_add(1)
            .ok_or_else(|| err("worksheet cell row 길이 계산 실패"))?;
        if rows.len() < required_len {
            rows.try_reserve(required_len.saturating_sub(rows.len()))
                .map_err(|source| err_with_source("worksheet cell row 메모리 확보 실패", source))?;
            rows.resize_with(required_len, Row::empty);
        }
        let row_obj = rows
            .get_mut(row_index)
            .ok_or_else(|| err("worksheet cell row 범위 오류"))?;
        match row_obj.cells.binary_search_by_key(&col, |cell| cell.col) {
            Ok(index) => row_obj
                .cells
                .get_mut(index)
                .ok_or_else(|| err("worksheet cell index 범위 오류")),
            Err(index) => {
                row_obj.cells.try_reserve(1).map_err(|source| {
                    err_with_source("worksheet cell 추가 메모리 확보 실패", source)
                })?;
                Ok(row_obj.cells.insert_mut(
                    index,
                    Cell {
                        col,
                        inner_xml: None,
                        style: Some(0),
                        value_type: CellValueType::General,
                    },
                ))
            }
        }
    }
    pub(crate) fn has_any_row_format(&self, row: u32, max_col: u32) -> bool {
        row_index(row)
            .and_then(|index| self.rows.get(index))
            .is_some_and(|row_obj| {
                !row_obj.attrs_xml.is_empty()
                    || (max_col > 0
                        && row_obj
                            .cells
                            .first()
                            .is_some_and(|cell| cell.col <= max_col))
            })
    }
    fn max_cell_col(&self) -> u32 {
        self.rows
            .iter()
            .filter_map(|row| row.cells.last().map(|cell| cell.col))
            .max()
            .unwrap_or(1)
    }
    pub(crate) fn prune_empty_style_artifacts_after_col(&mut self, max_col: u32) -> Result<()> {
        for row in &mut self.rows {
            let mut index = row.cells.partition_point(|cell| cell.col <= max_col);
            while let Some(cell) = row.cells.get(index) {
                if cell_has_payload(cell)? {
                    index = index.saturating_add(1);
                } else {
                    row.cells.remove(index);
                }
            }
        }
        let mut cursor = 0_usize;
        while let Some(location) = find_start_tag_location(&self.prefix, "col", cursor)? {
            let element_span =
                empty_xml_element_span(&self.prefix, &location, "col", "worksheet col 정의")?;
            let col_start = element_span.start;
            let mut attrs = parse_tag_attrs_at(
                &self.prefix,
                &location,
                "worksheet col 정의 태그 범위가 손상되었습니다.",
            )?;
            let min_col_text = get_attr(&attrs, "min")
                .ok_or_else(|| err("worksheet col 정의에 min 속성이 없습니다."))?;
            let min_col = parse_positive_u32_decimal(
                min_col_text,
                "worksheet col min이 양의 10진수 형식이 아닙니다.",
                "worksheet col min 해석 실패",
                "worksheet col min은 1 이상이어야 합니다.",
            )?;
            let max_col_text = get_attr(&attrs, "max")
                .ok_or_else(|| err("worksheet col 정의에 max 속성이 없습니다."))?;
            let max_defined_col = parse_positive_u32_decimal(
                max_col_text,
                "worksheet col max가 양의 10진수 형식이 아닙니다.",
                "worksheet col max 해석 실패",
                "worksheet col max는 1 이상이어야 합니다.",
            )?;
            if min_col > max_col {
                self.prefix.replace_range(element_span, "");
                cursor = col_start;
                continue;
            }
            if max_defined_col > max_col {
                set_attr(
                    &mut attrs,
                    "max",
                    usize::try_from(max_col)
                        .map_err(|source| err_with_source("col max 값 변환 실패", source))?
                        .to_string(),
                );
                let new_tag = build_self_closing_tag("col", &attrs)?;
                self.prefix.replace_range(element_span, &new_tag);
                cursor = checked_usize_add(col_start, new_tag.len(), "col 정의 다음 cursor")?;
                continue;
            }
            cursor = element_span.end;
        }
        Ok(())
    }
    pub(crate) fn replace_rows(&mut self, rows: Vec<Row>) {
        self.rows = rows;
    }
    pub(crate) const fn row_count(&self) -> usize {
        self.rows.len()
    }
    pub(crate) fn row_has_any_data(
        &self,
        row: u32,
        cols: &[u32],
        shared_strings: &[Rc<str>],
    ) -> Result<bool> {
        for col in cols {
            if !self
                .try_get_display_at(*col, row, shared_strings)?
                .trim()
                .is_empty()
            {
                return Ok(true);
            }
        }
        Ok(false)
    }
    pub(crate) fn row_numbers_from(&self, start: u32) -> Result<RangeInclusive<u32>> {
        let last = u32::try_from(self.rows.len())
            .map_err(|source| err_with_source("worksheet 마지막 row 변환 실패", source))?;
        Ok(RangeInclusive { start, last })
    }
    fn semantic_facts(
        &self,
        sheet_name: &str,
        data_start_row: u32,
        last_col: u32,
        shared_strings: &[Rc<str>],
    ) -> Result<(usize, Option<u32>)> {
        self.validate_dimension(sheet_name)?;
        self.validate_column_definitions(sheet_name, last_col)?;
        let mut shared_ref_count = 0_usize;
        let mut meaningful_last_row = None;
        for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
            for cell in &row.cells {
                let col = cell.col;
                if cell.value_type == CellValueType::SharedString {
                    drop(self.try_get_display_at(col, row_num, shared_strings)?);
                    shared_ref_count = shared_ref_count
                        .checked_add(1)
                        .ok_or_else(|| err("shared string 참조 수 계산 실패"))?;
                }
                if let Some(inner) = cell.inner_xml.as_deref()
                    && let Some(raw_formula) = extract_first_tag_text(inner, "f")?
                    && decode_xml_entities(raw_formula)?.contains("#REF!")
                {
                    return Err(err(format!(
                        "worksheet에 #REF! 수식이 있습니다: {sheet_name}!row={row_num}, col={col}"
                    )));
                }
                if row_num >= data_start_row && col <= last_col && cell_has_payload(cell)? {
                    meaningful_last_row = Some(row_num);
                }
            }
        }
        Ok((shared_ref_count, meaningful_last_row))
    }
    pub(crate) fn set_formula_at(&mut self, col: u32, row: u32, formula: &str) -> Result<()> {
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        cell.value_type = CellValueType::General;
        let formula_text =
            try_xml_escape_text(formula, XmlEscapeContext::Text, "formula XML escape")?;
        if let Some(inner) = cell.inner_xml.as_mut()
            && find_start_tag(inner, "f", 0).is_some()
        {
            replace_first_tag_text(inner, "f", &formula_text)?;
            replace_first_tag_text(inner, "v", "")?;
        } else {
            let capacity = "<f></f><v></v>"
                .len()
                .checked_add(formula_text.len())
                .ok_or_else(|| err("formula XML 용량 계산 실패"))?;
            let mut inner = String::new();
            inner
                .try_reserve_exact(capacity)
                .map_err(|source| err_with_source("formula XML 메모리 확보 실패", source))?;
            inner.push_str("<f>");
            inner.push_str(&formula_text);
            inner.push_str("</f><v></v>");
            cell.inner_xml = Some(inner);
        }
        Ok(())
    }
    pub(crate) fn set_formula_cached_value_at(
        &mut self,
        col: u32,
        row: u32,
        value: Option<&str>,
        string_value: bool,
    ) -> Result<()> {
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        cell.value_type = if string_value {
            CellValueType::String
        } else {
            CellValueType::General
        };
        let inner = cell.inner_xml.as_mut().ok_or_else(|| {
            err(format!(
                "수식 cache 대상 cell이 비어 있습니다: row={row}, col={col}"
            ))
        })?;
        let encoded = value
            .map(|raw_value| {
                try_xml_escape_text(
                    raw_value,
                    XmlEscapeContext::Text,
                    "formula cache XML escape",
                )
            })
            .transpose()?;
        let value_text = encoded.as_deref().unwrap_or("");
        replace_first_tag_text(inner, "v", value_text)?;
        Ok(())
    }
    pub(crate) fn set_i32_at(&mut self, col: u32, row: u32, value: Option<i32>) -> Result<()> {
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        cell.value_type = CellValueType::General;
        if let Some(numeric_value) = value {
            cell.inner_xml = Some(build_decimal_display_text_tag(
                "v",
                numeric_value.is_negative().then_some('-'),
                numeric_value.unsigned_abs(),
            ));
        } else {
            cell.inner_xml = None;
        }
        Ok(())
    }
    fn set_shared_string_index_at(&mut self, col: u32, row: u32, value: usize) -> Result<()> {
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        cell.value_type = CellValueType::SharedString;
        cell.inner_xml = Some(build_decimal_display_text_tag("v", None, value));
        Ok(())
    }
    pub(crate) fn take_rows(&mut self) -> Vec<Row> {
        mem::take(&mut self.rows)
    }
    fn to_xml(&self) -> Result<(String, usize)> {
        let cell_name = "c";
        let row_name = "row";
        let mut shared_string_reference_count = 0_usize;
        let estimated_capacity = (|| {
            let cell_markup_len =
                checked_capacity(&["< r=\"\"></>".len(), cell_name.len(), cell_name.len()])?;
            let row_markup_len =
                checked_capacity(&["< r=\"\"></>".len(), row_name.len(), row_name.len()])?;
            let mut capacity = checked_capacity(&[self.prefix.len(), self.suffix.len()])?;
            for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
                capacity =
                    checked_capacity(&[capacity, row_markup_len, u32_decimal_text_len(row_num)])?;
                capacity = capacity.checked_add(row.attrs_xml.len())?;
                for cell in &row.cells {
                    let col = cell.col;
                    if cell.value_type == CellValueType::SharedString {
                        shared_string_reference_count =
                            shared_string_reference_count.saturating_add(1);
                    }
                    let cell_ref_len =
                        with_unlocked_ref_parts(col, row_num, |col_text, row_number| {
                            col_text.len().checked_add(u32_decimal_text_len(row_number))
                        })
                        .ok()??;
                    capacity = checked_capacity(&[capacity, cell_markup_len, cell_ref_len])?;
                    if let Some(style) = cell.style {
                        capacity = checked_capacity(&[
                            capacity,
                            " s=\"\"".len(),
                            u32_decimal_text_len(style),
                        ])?;
                    }
                    if let Some(value_type) = cell.value_type.xml_attr() {
                        capacity =
                            checked_capacity(&[capacity, " t=\"\"".len(), value_type.len()])?;
                    }
                    if let Some(inner) = cell.inner_xml.as_ref() {
                        capacity = capacity.checked_add(inner.len())?;
                    }
                }
            }
            Some(capacity)
        })();
        let capacity = estimated_capacity.ok_or_else(|| err("worksheet XML 용량 계산 실패"))?;
        let mut out = String::new();
        out.try_reserve_exact(capacity)
            .map_err(|source| err_with_source("worksheet XML 메모리 확보 실패", source))?;
        out.push_str(&self.prefix);
        for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
            out.push('<');
            out.push_str(row_name);
            out.push_str(" r=\"");
            push_decimal_text(&mut out, row_num);
            out.push('"');
            out.push_str(&row.attrs_xml);
            if row.cells.is_empty() {
                out.push_str("/>");
                continue;
            }
            out.push('>');
            for cell in &row.cells {
                let col = cell.col;
                out.push('<');
                out.push_str(cell_name);
                out.push_str(" r=\"");
                with_unlocked_ref_parts(col, row_num, |col_text, row_number| {
                    out.push_str(col_text);
                    push_decimal_text(&mut out, row_number);
                })?;
                out.push('"');
                if let Some(style) = cell.style {
                    out.push_str(" s=\"");
                    push_decimal_text(&mut out, style);
                    out.push('"');
                }
                if let Some(value_type) = cell.value_type.xml_attr() {
                    out.push_str(" t=\"");
                    out.push_str(value_type);
                    out.push('"');
                }
                if let Some(inner) = cell.inner_xml.as_ref() {
                    out.push('>');
                    out.push_str(inner);
                    push_end_tag_name(&mut out, cell_name);
                } else {
                    out.push_str("/>");
                }
            }
            push_end_tag_name(&mut out, row_name);
        }
        out.push_str(&self.suffix);
        Ok((out, shared_string_reference_count))
    }
    pub(crate) fn truncate_rows_after(&mut self, last_row_to_keep: u32) -> Result<()> {
        let keep_len = usize::try_from(last_row_to_keep)
            .map_err(|source| err_with_source("worksheet 유지 row 수 변환 실패", source))?;
        self.rows.truncate(keep_len);
        Ok(())
    }
    pub(crate) fn try_get_display_at<'text>(
        &'text self,
        col: u32,
        row: u32,
        shared_strings: &'text [Rc<str>],
    ) -> Result<Cow<'text, str>> {
        let Some(row_obj) = row_index(row).and_then(|index| self.rows.get(index)) else {
            return Ok(Cow::Borrowed(""));
        };
        let Some(cell) = row_obj.cell(col) else {
            return Ok(Cow::Borrowed(""));
        };
        let inner = cell.inner_xml.as_deref().unwrap_or("");
        if cell.value_type == CellValueType::SharedString {
            let raw_v = extract_first_tag_text(inner, "v")?
                .ok_or_else(|| err("shared string cell에 v 태그가 없습니다."))?;
            let idx = parse_usize_decimal(raw_v, "shared string index 해석 실패")?;
            return shared_strings
                .get(idx)
                .map(|value| Cow::Borrowed(value.as_ref()))
                .ok_or_else(|| err(format!("shared string index 범위 오류: {idx}")));
        }
        let raw_v = extract_first_tag_text(inner, "v")?.unwrap_or("");
        decode_xml_entities(raw_v)
    }
    pub(crate) fn try_get_formula_at(&self, col: u32, row: u32) -> Result<Option<Cow<'_, str>>> {
        let Some(inner) = row_index(row)
            .and_then(|index| self.rows.get(index))
            .and_then(|row_obj| row_obj.cell(col))
            .and_then(|cell| cell.inner_xml.as_deref())
        else {
            return Ok(None);
        };
        let Some(text) = extract_first_tag_text(inner, "f")? else {
            return Ok(None);
        };
        decode_xml_entities(text).map(Some)
    }
    pub(crate) fn update_auto_filter_ref(&mut self, last_data_row: u32) -> Result<()> {
        let (location, mut attrs, _) = self.fixed_master_auto_filter()?;
        let out = &mut self.suffix;
        let new_ref = build_ref_range(
            "A",
            RangeInclusive {
                start: 14,
                last: last_data_row.max(14),
            },
            23,
        )?;
        reserve_xml_attrs(&mut attrs, 1, "autoFilter 속성 목록 추가 메모리 확보 실패")?;
        set_attr(&mut attrs, "ref", new_ref);
        let new_tag = if location.self_closing {
            build_self_closing_tag("autoFilter", &attrs)?
        } else {
            build_open_tag("autoFilter", &attrs)?
        };
        out.replace_range(location.span, &new_tag);
        Ok(())
    }
    pub(crate) fn update_dimension(&mut self) -> Result<()> {
        let mut max_row = 1_u32;
        let mut max_col = 1_u32;
        for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
            if let Some(cell) = row.cells.last() {
                max_row = max_row.max(row_num);
                max_col = max_col.max(cell.col);
            }
        }
        if let Some(dim_location) = find_start_tag_location(&self.prefix, "dimension", 0)? {
            let element_span = empty_xml_element_span(
                &self.prefix,
                &dim_location,
                "dimension",
                "worksheet dimension",
            )?;
            let mut attrs = parse_tag_attrs_at(
                &self.prefix,
                &dim_location,
                "dimension 태그 범위가 손상되었습니다.",
            )?;
            reserve_xml_attrs(&mut attrs, 1, "dimension 속성 목록 추가 메모리 확보 실패")?;
            set_attr(
                &mut attrs,
                "ref",
                build_ref_range(
                    "A",
                    RangeInclusive {
                        start: 1,
                        last: max_row,
                    },
                    max_col,
                )?,
            );
            let new_tag = build_self_closing_tag("dimension", &attrs)?;
            self.prefix.replace_range(element_span, &new_tag);
        }
        Ok(())
    }
    fn validate_change_log_formats(&self, expected_last_row: u32) -> Result<()> {
        let mut delta_mask = 0_u8;
        let mut scanner = XmlScanner::new(&self.suffix);
        while let Some(formatting) = scanner.next_start_named("conditionalFormatting") {
            let Some(sqref) = extract_attr(formatting.raw(), "sqref")? else {
                continue;
            };
            for token in sqref.split_whitespace() {
                let (start_ref, end_ref) = parse_range_token(token);
                let start = parse_ref_with_locks(start_ref)
                    .ok_or_else(|| err("변경내역 조건부 서식 시작 reference 해석 실패"))?;
                let end = parse_ref_with_locks(end_ref)
                    .ok_or_else(|| err("변경내역 조건부 서식 끝 reference 해석 실패"))?;
                if start.row == 4 && end.row == expected_last_row && start.col == end.col {
                    delta_mask |= match start.col {
                        7 => 1_u8,
                        10 => 2_u8,
                        13 => 4_u8,
                        _ => 0_u8,
                    };
                }
            }
        }
        for (bit, column) in [(1_u8, "G"), (2_u8, "J"), (4_u8, "M")] {
            if delta_mask & bit == 0 {
                return Err(err(format!(
                    "변경내역 {column}열 조건부 서식 기준 범위가 없습니다."
                )));
            }
        }
        Ok(())
    }
    fn validate_column_definitions(&self, sheet_name: &str, last_col: u32) -> Result<()> {
        let mut scanner = XmlScanner::new(&self.prefix);
        while let Some(column) = scanner.next_start_named("col") {
            let min_text = extract_attr(column.raw(), "min")?
                .ok_or_else(|| err(format!("{sheet_name} col min 속성이 없습니다.")))?;
            let max_text = extract_attr(column.raw(), "max")?
                .ok_or_else(|| err(format!("{sheet_name} col max 속성이 없습니다.")))?;
            let min = parse_positive_u32_decimal(
                min_text.as_ref(),
                "worksheet col min이 양의 10진수 형식이 아닙니다.",
                "worksheet col min 해석 실패",
                "worksheet col min은 1 이상이어야 합니다.",
            )?;
            let max = parse_positive_u32_decimal(
                max_text.as_ref(),
                "worksheet col max가 양의 10진수 형식이 아닙니다.",
                "worksheet col max 해석 실패",
                "worksheet col max는 1 이상이어야 합니다.",
            )?;
            if min > max || max > last_col {
                return Err(err(format!(
                    "{sheet_name} col 정의가 고정 스키마 열 범위를 벗어났습니다: min={min}, max={max}"
                )));
            }
        }
        Ok(())
    }
    fn validate_dimension(&self, sheet_name: &str) -> Result<()> {
        let mut actual_bounds = None;
        for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
            for cell in &row.cells {
                let col = cell.col;
                actual_bounds = Some(actual_bounds.map_or(
                    (col, row_num, col, row_num),
                    |(min_col, min_row, max_col, max_row): (u32, u32, u32, u32)| {
                        (
                            min_col.min(col),
                            min_row.min(row_num),
                            max_col.max(col),
                            max_row.max(row_num),
                        )
                    },
                ));
            }
        }
        let bounds = actual_bounds
            .ok_or_else(|| err(format!("{sheet_name} worksheet에 cell이 없습니다.")))?;
        let mut scanner = XmlScanner::new(&self.prefix);
        let dimension = scanner
            .next_start_named("dimension")
            .ok_or_else(|| err(format!("{sheet_name} worksheet에 dimension이 없습니다.")))?;
        let declared = extract_attr(dimension.raw(), "ref")?
            .ok_or_else(|| err(format!("{sheet_name} worksheet dimension ref가 없습니다.")))?;
        if scanner.next_start_named("dimension").is_some() {
            return Err(err(format!(
                "{sheet_name} worksheet dimension이 중복되어 있습니다."
            )));
        }
        let (start_ref, end_ref) = parse_range_token(declared.as_ref());
        let start = parse_ref_with_locks(start_ref).ok_or_else(|| {
            err(format!(
                "{sheet_name} worksheet dimension 시작 ref가 잘못되었습니다."
            ))
        })?;
        let end = parse_ref_with_locks(end_ref).ok_or_else(|| {
            err(format!(
                "{sheet_name} worksheet dimension 끝 ref가 잘못되었습니다."
            ))
        })?;
        if (start.col, start.row, end.col, end.row) != bounds {
            return Err(err(format!(
                "{sheet_name} worksheet dimension이 실제 cell 범위와 다릅니다: declared={}, actual=col {} row {}:col {} row {}",
                declared.as_ref(),
                bounds.0,
                bounds.1,
                bounds.2,
                bounds.3
            )));
        }
        Ok(())
    }
    fn validate_fixed_header(&self, sheet_name: &str, shared_strings: &[Rc<str>]) -> Result<()> {
        let (header_row, headers, last_col): (u32, &[&str], u32) = match sheet_name {
            "유류비" => (14, &MASTER_HEADERS, 23),
            "변경내역" => (3, &CHANGE_LOG_HEADERS, 13),
            _ => return Err(err(format!("고정 스키마에 없는 sheet입니다: {sheet_name}"))),
        };
        if self.max_cell_col() != last_col {
            return Err(err(format!(
                "{sheet_name} 시트의 마지막 열이 고정 스키마와 다릅니다: expected={last_col}, actual={}",
                self.max_cell_col()
            )));
        }
        for (col, expected) in (1_u32..).zip(headers.iter().copied()) {
            let actual = self.try_get_display_at(col, header_row, shared_strings)?;
            if actual.as_ref() != expected {
                return Err(err(format!(
                    "{sheet_name} 헤더가 고정 스키마와 다릅니다: row={header_row}, col={col}, expected={expected}, actual={actual}"
                )));
            }
        }
        if sheet_name == "유류비" {
            self.fixed_master_auto_filter()?;
        }
        Ok(())
    }
    fn validate_formula_layout(
        &self,
        sheet_name: &str,
        last_data_row: Option<u32>,
        layout: FormulaLayout,
        shared_strings: &[Rc<str>],
    ) -> Result<()> {
        for (row_num, row_obj) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
            for cell in &row_obj.cells {
                let Some(inner) = cell.inner_xml.as_deref() else {
                    continue;
                };
                if extract_first_tag_text(inner, "f")?.is_none() {
                    continue;
                }
                let fixed = layout
                    .fixed_formulas
                    .iter()
                    .any(|&(col, fixed_row, _)| (cell.col, row_num) == (col, fixed_row));
                let data = last_data_row.is_some_and(|last| {
                    (layout.data_start_row..=last).contains(&row_num)
                        && (layout.required_cols.contains(&cell.col)
                            || layout.optional_zero_col == Some(cell.col))
                });
                if !fixed && !data {
                    return Err(err(format!(
                        "{sheet_name} 시트의 고정 위치 밖에 formula가 있습니다: row={row_num}, col={}",
                        cell.col
                    )));
                }
            }
        }
        for &(col, row, expected) in layout.fixed_formulas {
            let actual = self.try_get_formula_at(col, row)?.ok_or_else(|| {
                err(format!(
                    "{sheet_name} 시트의 고정 formula가 없습니다: row={row}, col={col}"
                ))
            })?;
            if actual.as_ref() != expected {
                return Err(err(format!(
                    "{sheet_name} 시트의 고정 formula가 다릅니다: row={row}, col={col}"
                )));
            }
        }
        let Some(data_last) = last_data_row else {
            return Ok(());
        };
        for row in layout.data_start_row..=data_last {
            for &col in layout.required_cols {
                if self.try_get_formula_at(col, row)?.is_none() {
                    return Err(err(format!(
                        "{sheet_name} 시트의 필수 formula가 없습니다: row={row}, col={col}"
                    )));
                }
            }
            if let Some(col) = layout.optional_zero_col
                && self.try_get_formula_at(col, row)?.is_none()
                && self.get_i32_at(col, row, shared_strings)? != Some(0_i32)
            {
                return Err(err(format!(
                    "{sheet_name} 시트의 선택 formula 위치는 수동 0이어야 합니다: row={row}, col={col}"
                )));
            }
        }
        Ok(())
    }
}
impl Row {
    fn cell(&self, col: u32) -> Option<&Cell> {
        self.cells.iter().find(|cell| cell.col == col)
    }
    const fn empty() -> Self {
        Self {
            attrs_xml: String::new(),
            cells: Vec::new(),
        }
    }
    pub(crate) fn try_copy(&self) -> Result<Self> {
        let mut cells = Vec::new();
        cells
            .try_reserve_exact(self.cells.len())
            .map_err(|source| err_with_source("row cell 목록 복사 메모리 확보 실패", source))?;
        for cell in &self.cells {
            cells.push(Cell {
                col: cell.col,
                inner_xml: cell
                    .inner_xml
                    .as_deref()
                    .map(|inner| copy_text(inner, "cell inner XML 복사"))
                    .transpose()?,
                style: cell.style,
                value_type: cell.value_type,
            });
        }
        Ok(Self {
            attrs_xml: copy_text(&self.attrs_xml, "row attribute XML 복사")?,
            cells,
        })
    }
}
fn row_index(row: u32) -> Option<usize> {
    usize::try_from(row).ok()?.checked_sub(1)
}
fn next_cell_col(row_num: u32, col: u32) -> Result<u32> {
    col.checked_add(1).ok_or_else(|| {
        err(row_col_error(
            "cell 다음 column 계산 중 overflow가 발생했습니다. (row=",
            row_num,
            col,
        ))
    })
}
fn checked_usize_add(base: usize, add: usize, context: &str) -> Result<usize> {
    base.checked_add(add).ok_or_else(|| {
        err(format!(
            "{context} offset 계산 중 overflow가 발생했습니다. (base={base}, add={add})"
        ))
    })
}
fn fixed_filter_database_row(workbook_xml: &str) -> Result<(Range<usize>, u32)> {
    let mut scanner = XmlScanner::new(workbook_xml);
    let tag = scanner
        .next_start_named("definedName")
        .ok_or_else(|| err("workbook.xml의 _FilterDatabase를 찾지 못했습니다."))?;
    if tag.name() != "definedName" || tag.self_closing() {
        return Err(err(
            "workbook.xml의 _FilterDatabase 태그 형식이 고정 스키마와 다릅니다.",
        ));
    }
    let attrs = parse_tag_attrs(tag.raw())?;
    if attrs.len() != 5
        || get_attr(&attrs, "function") != Some("false")
        || get_attr(&attrs, "hidden") != Some("true")
        || get_attr(&attrs, "localSheetId") != Some("0")
        || get_attr(&attrs, "name") != Some(FILTER_DATABASE_NAME)
        || get_attr(&attrs, "vbProcedure") != Some("false")
    {
        return Err(err(
            "workbook.xml의 _FilterDatabase 속성이 고정 스키마와 다릅니다.",
        ));
    }
    let content_start = checked_usize_add(tag.end(), 1, "_FilterDatabase 본문 시작")?;
    let content_end = find_end_tag(workbook_xml, "definedName", content_start)
        .ok_or_else(|| err("workbook.xml의 _FilterDatabase 종료 태그를 찾지 못했습니다."))?;
    let content = workbook_xml
        .get(content_start..content_end)
        .ok_or_else(|| err("workbook.xml의 _FilterDatabase 본문 범위가 손상되었습니다."))?;
    let row_text = content
        .strip_prefix(FILTER_DATABASE_REF_PREFIX)
        .filter(|row| !row.is_empty() && row.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| err("_FilterDatabase가 고정 유류비 범위와 다릅니다."))?;
    let last_row = row_text
        .parse::<u32>()
        .map_err(|source| err_with_source("_FilterDatabase 마지막 행 해석 실패", source))?;
    let row_start = content_start
        .checked_add(FILTER_DATABASE_REF_PREFIX.len())
        .ok_or_else(|| err("_FilterDatabase 마지막 행 시작 계산 실패"))?;
    let close_end = find_tag_end(workbook_xml, content_end)
        .ok_or_else(|| err("workbook.xml의 _FilterDatabase 종료 태그가 손상되었습니다."))?;
    scanner.skip_to(checked_usize_add(
        close_end,
        1,
        "workbook.xml 다음 definedName 위치",
    )?);
    if scanner.next_start_named("definedName").is_some() {
        return Err(err("workbook.xml에 고정 스키마 외 definedName이 있습니다."));
    }
    Ok((
        Range {
            start: row_start,
            end: content_end,
        },
        last_row,
    ))
}
fn checked_capacity(parts: &[usize]) -> Option<usize> {
    parts
        .iter()
        .try_fold(0_usize, |sum, &part| sum.checked_add(part))
}
fn u32_decimal_text_len(value: u32) -> usize {
    value
        .checked_ilog10()
        .map_or(1, |log| usize::from(log.to_le_bytes()[0]).saturating_add(1))
}
fn push_decimal_text(out: &mut String, value: impl Display) {
    match FmtWrite::write_fmt(out, format_args!("{value}")) {
        Ok(()) | Err(_) => {}
    }
}
fn cell_has_payload(cell: &Cell) -> Result<bool> {
    let Some(inner) = cell.inner_xml.as_deref() else {
        return Ok(false);
    };
    if find_start_tag(inner, "f", 0).is_some() {
        return Ok(true);
    }
    if let Some(raw_value) = extract_first_tag_text(inner, "v")? {
        return Ok(!decode_xml_entities(raw_value)?.trim().is_empty());
    }
    Ok(extract_all_tag_text(inner, "t")?.is_some_and(|text| !text.is_empty()))
}
fn find_start_tag_location(
    xml: &str,
    tag_name: &str,
    from: usize,
) -> Result<Option<XmlTagLocation>> {
    let mut scanner = XmlScanner::new(xml);
    scanner.skip_to(from);
    let Some(tag) = scanner.next_start_named(tag_name) else {
        return Ok(None);
    };
    let tag_end = checked_usize_add(tag.end(), 1, "XML 시작 태그 끝")?;
    Ok(Some(XmlTagLocation {
        self_closing: tag.self_closing(),
        span: Range {
            start: tag.start(),
            end: tag_end,
        },
    }))
}
fn empty_xml_element_span(
    xml: &str,
    location: &XmlTagLocation,
    local_name: &str,
    context: &str,
) -> Result<Range<usize>> {
    let body_start = location.span.end;
    let (element_span, body_span) = if location.self_closing {
        (
            location.span,
            Range {
                start: body_start,
                end: body_start,
            },
        )
    } else {
        let body_end = find_end_tag(xml, local_name, body_start)
            .ok_or_else(|| err(format!("{context} 종료 태그를 찾지 못했습니다.")))?;
        let close_end = find_tag_end(xml, body_end)
            .and_then(|end| end.checked_add(1))
            .ok_or_else(|| err(format!("{context} 종료 태그가 손상되었습니다.")))?;
        (
            Range {
                start: location.span.start,
                end: close_end,
            },
            Range {
                start: body_start,
                end: body_end,
            },
        )
    };
    let body = xml
        .get(body_span)
        .ok_or_else(|| err(format!("{context} 본문 범위가 손상되었습니다.")))?;
    if !body.trim().is_empty() {
        return Err(err(format!("{context}에 예상하지 않은 본문이 있습니다.")));
    }
    Ok(element_span)
}
fn attr_sort_rank(name: &str) -> u8 {
    if name == "r" {
        0
    } else if name == "s" {
        1
    } else if name == "t" {
        2
    } else {
        3
    }
}
fn attr_cmp(left: &XmlAttr, right: &XmlAttr) -> Ordering {
    attr_sort_rank(&left.name)
        .cmp(&attr_sort_rank(&right.name))
        .then_with(|| left.name.cmp(&right.name))
}
fn push_attr_xml(out: &mut String, attr: &XmlAttr) {
    let name = &attr.name;
    out.push(' ');
    out.push_str(name);
    out.push_str("=\"");
    append_xml_escaped(out, &attr.value, XmlEscapeContext::Attribute);
    out.push('"');
}
fn parse_tag_attrs_at(
    xml: &str,
    location: &XmlTagLocation,
    invalid_range_message: &'static str,
) -> Result<Vec<XmlAttr>> {
    let tag = xml
        .get(location.span)
        .ok_or_else(|| err(invalid_range_message))?;
    parse_tag_attrs(tag)
}
fn parse_tag_attrs(tag: &str) -> Result<Vec<XmlAttr>> {
    let mut out: Vec<XmlAttr> = Vec::new();
    out.try_reserve_exact(4)
        .map_err(|source| err_with_source("XML 속성 목록 메모리 확보 실패", source))?;
    let mut scanner = XmlAttrScanner::new(tag)?;
    while let Some((name, value)) = scanner.next()? {
        if out.len() >= MAX_XML_ATTRIBUTE_COUNT {
            return Err(err("XML 속성 개수가 허용 한도를 초과했습니다."));
        }
        if name.is_empty() {
            return Err(err("XML 속성 파싱 실패: 빈 속성 이름"));
        }
        if out.iter().any(|attr| attr.name == name) {
            return Err(err("XML 태그에 중복 속성이 있습니다."));
        }
        if out.len() == out.capacity() {
            reserve_xml_attrs(&mut out, 1, "XML 속성 목록 추가 메모리 확보 실패")?;
        }
        out.push(XmlAttr {
            name: Cow::Owned(copy_text(name, "XML 속성 이름 복사")?),
            value: match value {
                Cow::Borrowed(text) => copy_text(text, "XML 속성 값 복사")?,
                Cow::Owned(text) => text,
            },
        });
    }
    Ok(out)
}
fn reserve_xml_attrs(attrs: &mut Vec<XmlAttr>, additional: usize, context: &str) -> Result<()> {
    attrs
        .try_reserve(additional)
        .map_err(|source| err_with_source(format!("{context}: {additional} entries"), source))
}
fn get_attr<'attrs>(attrs: &'attrs [XmlAttr], name: &str) -> Option<&'attrs str> {
    attrs
        .iter()
        .find(|attr| attr.name == name)
        .map(|attr| attr.value.as_str())
}
fn parse_usize_decimal(value: &str, context: &'static str) -> Result<usize> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(err(format!("{context}: 음이 아닌 10진수 형식이 아닙니다.")));
    }
    value
        .parse::<usize>()
        .map_err(|source| err_with_source(context, source))
}
fn parse_u32_decimal(
    value: &str,
    format_error: impl Into<Cow<'static, str>>,
    parse_context: impl Into<Cow<'static, str>>,
) -> Result<u32> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(err(format_error));
    }
    value
        .parse::<u32>()
        .map_err(|source| err_with_source(parse_context, source))
}
fn parse_positive_u32_decimal(
    value: &str,
    format_error: &'static str,
    parse_context: &'static str,
    zero_error: &'static str,
) -> Result<u32> {
    let parsed = parse_u32_decimal(value, format_error, parse_context)?;
    if parsed == 0 {
        return Err(err(zero_error));
    }
    Ok(parsed)
}
fn set_attr(attrs: &mut Vec<XmlAttr>, name: &'static str, value_in: impl Into<String>) {
    let value = value_in.into();
    if let Some(attr) = attrs.iter_mut().find(|attr| attr.name == name) {
        attr.value = value;
    } else {
        attrs.push(XmlAttr {
            name: Cow::Borrowed(name),
            value,
        });
    }
}
fn remove_attr(attrs: &mut Vec<XmlAttr>, name: &str) {
    attrs.retain(|attr| attr.name != name);
}
fn take_attr(attrs: &mut Vec<XmlAttr>, name: &str) -> Option<String> {
    attrs
        .iter()
        .position(|attr| attr.name == name)
        .map(|index| attrs.swap_remove(index).value)
}
fn replace_first_tag_text(xml: &mut String, tag_name: &str, new_text: &str) -> Result<()> {
    let mut scanner = XmlScanner::new(xml);
    let Some(tag) = scanner.next_start_named(tag_name) else {
        return Err(err(tag_error_message(tag_name, " 태그를 찾지 못했습니다.")));
    };
    let open_start = tag.start();
    let open_end = tag.end();
    let open_end_exclusive = checked_usize_add(open_end, 1, "XML 시작 태그 끝")?;
    let open_tag = xml
        .get(Range {
            start: open_start,
            end: open_end_exclusive,
        })
        .ok_or_else(|| {
            err(tag_error_message(
                tag_name,
                " 시작 태그 범위가 손상되었습니다.",
            ))
        })?;
    let trimmed_open_tag = open_tag.trim_ascii_end();
    if trimmed_open_tag.ends_with("/>") {
        let prefix = trimmed_open_tag
            .strip_suffix("/>")
            .ok_or_else(|| err(tag_error_message(tag_name, " self-closing 태그 파싱 실패")))?;
        let capacity = checked_capacity(&[
            prefix.len(),
            ">".len(),
            new_text.len(),
            "</".len(),
            tag_name.len(),
            ">".len(),
        ])
        .ok_or_else(|| {
            err(tag_error_message(
                tag_name,
                " self-closing 치환 용량 계산 실패",
            ))
        })?;
        let mut replacement = String::new();
        replacement.try_reserve_exact(capacity).map_err(|source| {
            err_with_source(
                tag_error_message(tag_name, " self-closing 치환 메모리 확보 실패"),
                source,
            )
        })?;
        replacement.push_str(prefix);
        replacement.push('>');
        replacement.push_str(new_text);
        replacement.push_str("</");
        replacement.push_str(tag_name);
        replacement.push('>');
        xml.replace_range(
            Range {
                start: open_start,
                end: open_end_exclusive,
            },
            &replacement,
        );
        return Ok(());
    }
    let content_start = checked_usize_add(open_end, 1, "XML 태그 본문 시작")?;
    let close = find_end_tag(xml, tag_name, content_start)
        .ok_or_else(|| err(tag_error_message(tag_name, " 종료 태그를 찾지 못했습니다.")))?;
    xml.replace_range(
        Range {
            start: content_start,
            end: close,
        },
        new_text,
    );
    Ok(())
}
fn try_xml_escape_text(
    text: &str,
    context: XmlEscapeContext,
    error_context: &'static str,
) -> Result<String> {
    let capacity = validated_xml_escaped_len(text, context, error_context)?;
    let mut out = String::new();
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source(format!("{error_context} 메모리 확보 실패"), source))?;
    append_xml_escaped(&mut out, text, context);
    Ok(out)
}
fn validated_xml_escaped_len(
    text: &str,
    context: XmlEscapeContext,
    error_context: &str,
) -> Result<usize> {
    text.chars().try_fold(0_usize, |total, ch| {
        if !is_valid_xml_char(ch) {
            return Err(err(format!(
                "{error_context}: XML 1.0에서 허용되지 않는 문자가 포함되어 있습니다: U+{:04X}",
                u32::from(ch)
            )));
        }
        let encoded_len =
            xml_escape_replacement(ch, context).map_or_else(|| ch.len_utf8(), str::len);
        total
            .checked_add(encoded_len)
            .ok_or_else(|| err(format!("{error_context} 용량 계산 실패")))
    })
}
fn append_xml_escaped(out: &mut String, text: &str, context: XmlEscapeContext) {
    for ch in text.chars() {
        if let Some(replacement) = xml_escape_replacement(ch, context) {
            out.push_str(replacement);
        } else {
            out.push(ch);
        }
    }
}
const fn xml_escape_replacement(ch: char, context: XmlEscapeContext) -> Option<&'static str> {
    match ch {
        '\t' if matches!(context, XmlEscapeContext::Attribute) => Some("&#x9;"),
        '\n' if matches!(context, XmlEscapeContext::Attribute) => Some("&#xA;"),
        '\r' => Some("&#xD;"),
        '&' => Some("&amp;"),
        '<' => Some("&lt;"),
        '>' => Some("&gt;"),
        '"' if matches!(context, XmlEscapeContext::Attribute) => Some("&quot;"),
        '\'' if matches!(context, XmlEscapeContext::Attribute) => Some("&apos;"),
        _ => None,
    }
}
fn push_end_tag_name(out: &mut String, name: &str) {
    out.push_str("</");
    out.push_str(name);
    out.push('>');
}
fn build_open_tag(name: &str, attrs: &[XmlAttr]) -> Result<String> {
    let mut capacity = checked_capacity(&["<>".len(), name.len()])
        .ok_or_else(|| err("XML 시작 태그 용량 계산 실패"))?;
    for attr in attrs {
        capacity = checked_capacity(&[capacity, " =\"\"".len(), attr.name.len(), attr.value.len()])
            .ok_or_else(|| err("XML 시작 태그 속성 용량 계산 실패"))?;
    }
    let mut out = String::new();
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source("XML 시작 태그 메모리 확보 실패", source))?;
    out.push('<');
    out.push_str(name);
    for attr in attrs {
        push_attr_xml(&mut out, attr);
    }
    out.push('>');
    Ok(out)
}
fn build_self_closing_tag(name: &str, attrs: &[XmlAttr]) -> Result<String> {
    let mut capacity = checked_capacity(&["</>".len(), name.len()])
        .ok_or_else(|| err("XML self-closing 태그 용량 계산 실패"))?;
    for attr in attrs {
        capacity = checked_capacity(&[capacity, " =\"\"".len(), attr.name.len(), attr.value.len()])
            .ok_or_else(|| err("XML self-closing 태그 속성 용량 계산 실패"))?;
    }
    let mut out = String::new();
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source("XML self-closing 태그 메모리 확보 실패", source))?;
    out.push('<');
    out.push_str(name);
    for attr in attrs {
        push_attr_xml(&mut out, attr);
    }
    out.push_str("/>");
    Ok(out)
}
fn build_decimal_display_text_tag(
    name: &str,
    sign: Option<char>,
    magnitude: impl Display,
) -> String {
    let mut out = String::new();
    out.push('<');
    out.push_str(name);
    out.push('>');
    if let Some(sign_char) = sign {
        out.push(sign_char);
    }
    push_decimal_text(&mut out, magnitude);
    out.push_str("</");
    out.push_str(name);
    out.push('>');
    out
}
fn build_ref_range(
    start_col_text: &str,
    rows: RangeInclusive<u32>,
    end_col: u32,
) -> Result<String> {
    let end_ref = ref_with_locks(CellReference {
        col: end_col,
        col_locked: false,
        row: rows.last,
        row_locked: false,
    })?;
    let Some(capacity) = checked_capacity(&[
        start_col_text.len(),
        u32_decimal_text_len(rows.start),
        ":".len(),
        end_ref.len(),
    ]) else {
        return Err(err("cell range reference 용량 계산 실패"));
    };
    let mut out = String::new();
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source("cell range reference 메모리 확보 실패", source))?;
    out.push_str(start_col_text);
    push_decimal_text(&mut out, rows.start);
    out.push(':');
    out.push_str(&end_ref);
    Ok(out)
}
fn row_only_error(prefix: &str, row_num: u32) -> String {
    format!("{prefix}{row_num})")
}
fn row_col_error(prefix: &str, row_num: u32, col: u32) -> String {
    format!("{prefix}{row_num}, col={col})")
}
fn tag_error_message(tag_name: &str, suffix: &str) -> String {
    format!("{tag_name}{suffix}")
}
