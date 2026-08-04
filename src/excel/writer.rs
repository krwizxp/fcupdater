use self::cell_ref::{
    MAX_A1_COL, MAX_A1_ROW, parse_range_token, ref_with_locks, with_unlocked_ref_parts,
};
use self::cell_ref::{parse_ref_with_locks, shift_formula};
use super::{
    CALC_CHAIN_PATH, CHANGE_LOG_SHEET_NAME, CHANGE_LOG_SHEET_PATH, CanonicalStyleMap,
    MASTER_SHEET_NAME, MASTER_SHEET_PATH, SPREADSHEETML_NAMESPACE, SaveVerification, copy_text,
    xlsx_container::XlsxContainer,
    xml::{
        XmlAttrScanner, XmlScanner, decode_xml_entities, extract_all_tag_text, extract_attr,
        extract_first_tag_text, find_start_tag, is_valid_xml_char,
    },
};
use crate::diagnostic::{
    Result, append_fmt, err, err_with_source, try_string_with_capacity, try_vec_with_capacity,
};
use alloc::{
    borrow::Cow,
    collections::{BTreeMap, btree_map::Entry},
    rc::Rc,
};
use core::{
    fmt::Display,
    mem,
    range::{Range, RangeInclusive},
};
use std::collections::{HashMap, HashSet};
use std::path::Path;
mod cell_ref;
const XML_SPACE_PRESERVE_ATTR: &str = " xml:space=\"preserve\"";
const FILTER_DATABASE_NAME: &str = "_xlnm._FilterDatabase";
const FILTER_DATABASE_REF_PREFIX: &str = "유류비!$A$14:$W$";
const MAX_SHARED_STRING_COUNT: usize = 0x0010_0000;
const MAX_WORKSHEET_CELL_COUNT: usize = 0x0010_0000;
const MAX_XML_ATTRIBUTE_COUNT: usize = 128;
const MAX_SHARED_FORMULA_FOLLOWERS: u32 = 63;
const MAX_SHARED_FORMULA_FOLLOWERS_AFTER_GAP: u32 = 31;
const MIN_SHARED_FORMULA_CELLS: u32 = 6;
const CHANGE_LOG_LAST_COL: u32 = 13;
const MASTER_ADDRESS_COL: u32 = 6;
const MASTER_LAST_COL: u32 = 23;
const SHARED_STRING_GROWTH: usize = 16;
const SHARED_STRING_INITIAL_CAPACITY: usize = 4096;
const WORKSHEET_CELL_GROWTH: usize = 16;
const WORKSHEET_ROW_GROWTH: usize = 64;
const EXCEL_MASTER_PREFIX: &str = include_str!("excel_sheet1_prefix.xml");
const EXCEL_MASTER_SUFFIX: &str = include_str!("excel_sheet1_suffix.xml");
const EXCEL_CHANGE_LOG_PREFIX: &str = include_str!("excel_sheet2_prefix.xml");
const EXCEL_CHANGE_LOG_SUFFIX: &str = include_str!("excel_sheet2_suffix.xml");
const EXCEL_BOOK_VIEWS_XML: &str = "<bookViews><workbookView xWindow=\"-120\" yWindow=\"-120\" windowWidth=\"29040\" windowHeight=\"15720\" tabRatio=\"500\" xr2:uid=\"{00000000-000D-0000-FFFF-FFFF00000000}\"/></bookViews>";
const EXCEL_CALC_EXTENSIONS_XML: &str = concat!(
    "<extLst>",
    "<ext uri=\"{B58B0392-4F1F-4190-BB64-5DF3571DCE5F}\" xmlns:xcalcf=\"http://schemas.microsoft.com/office/spreadsheetml/2018/calcfeatures\"><xcalcf:calcFeatures><xcalcf:feature name=\"microsoft.com:RD\"/><xcalcf:feature name=\"microsoft.com:Single\"/><xcalcf:feature name=\"microsoft.com:FV\"/><xcalcf:feature name=\"microsoft.com:CNMTM\"/><xcalcf:feature name=\"microsoft.com:LET_WF\"/><xcalcf:feature name=\"microsoft.com:LAMBDA_WF\"/><xcalcf:feature name=\"microsoft.com:ARRAYTEXT_WF\"/></xcalcf:calcFeatures></ext>",
    "<ext uri=\"{D14903EA-33C4-47F7-8F05-3474C54BE107}\" xmlns:xlwcv=\"http://schemas.microsoft.com/office/spreadsheetml/2024/workbookCompatibilityVersion\"><xlwcv:version setVersion=\"1\"/></ext>",
    "<ext uri=\"{7626C862-2A13-11E5-B345-FEFF819CDC9F}\" xmlns:loext=\"http://schemas.libreoffice.org/\"><loext:extCalcPr stringRefSyntax=\"CalcA1ExcelA1\"/></ext>",
    "</extLst>",
);
const EXCEL_WORKBOOK_OPENING: &str = concat!(
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n",
    "<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\" xmlns:mc=\"http://schemas.openxmlformats.org/markup-compatibility/2006\" mc:Ignorable=\"x15 xr xr6 xr10 xr2\" xmlns:x15=\"http://schemas.microsoft.com/office/spreadsheetml/2010/11/main\" xmlns:xr=\"http://schemas.microsoft.com/office/spreadsheetml/2014/revision\" xmlns:xr6=\"http://schemas.microsoft.com/office/spreadsheetml/2016/revision6\" xmlns:xr10=\"http://schemas.microsoft.com/office/spreadsheetml/2016/revision10\" xmlns:xr2=\"http://schemas.microsoft.com/office/spreadsheetml/2015/revision2\">",
);
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
    required_cols: &[7, 10, CHANGE_LOG_LAST_COL],
};
const MASTER_FORMULA_LAYOUT: FormulaLayout = FormulaLayout {
    data_start_row: 15,
    fixed_formulas: &[
        (2, 10, "B4+B5+B6"),
        (2, 11, r#"IF(B4+B5=0,"",(B4*B7+B5*B8)/(B4+B5))"#),
        (2, 12, r#"IF(B5+B6=0,"",(B4*B7+B5*B9)/(B4+B5))"#),
    ],
    optional_zero_col: Some(11),
    required_cols: &[1, 12, 13, 14, 15, 16, 18, 19, 20, 21, 22, MASTER_LAST_COL],
};
pub(crate) struct Workbook {
    calc_chain_xml: Option<String>,
    change_log_sheet: Worksheet,
    container: XlsxContainer,
    input_styles: CanonicalStyleMap,
    master_sheet: Worksheet,
    shared_strings: SharedStringTable,
    xml_text: String,
}
pub(crate) struct SharedStringTable {
    entries: Vec<SharedStringEntry>,
    index: HashMap<Rc<str>, usize>,
}
struct SharedStringEntry {
    text: Rc<str>,
    xml: String,
}
pub(crate) struct Worksheet {
    prefix: String,
    rows: Vec<Row>,
    suffix: String,
}
struct XmlTagLocation {
    self_closing: bool,
    span: Range<usize>,
}
#[derive(Default)]
pub(crate) struct Row {
    attrs_xml: String,
    cells: Vec<Cell>,
}
struct Cell {
    col: u32,
    inner_xml: Option<String>,
    style: Option<u32>,
    value_type: CellValueType,
}
struct SharedFormulaHead {
    anchor_col: u32,
    anchor_row: u32,
    formula: String,
    last_row: u32,
    seen: u32,
}
#[derive(Clone, Copy)]
enum FormulaTag<'text> {
    Plain(&'text str),
    SharedFollower(u32),
    SharedRoot {
        formula: &'text str,
        reference: &'text str,
        si: u32,
    },
}
#[derive(Clone, Copy)]
struct FormulaLayout {
    data_start_row: u32,
    fixed_formulas: &'static [(u32, u32, &'static str)],
    optional_zero_col: Option<u32>,
    required_cols: &'static [u32],
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum ExcelSheetKind {
    ChangeLog,
    Master,
}
#[derive(Clone, Copy, Eq, PartialEq)]
enum CellValueType {
    General,
    Number,
    SharedString(usize),
    String,
}
impl CellValueType {
    const fn xml_attr(self) -> Option<&'static str> {
        match self {
            Self::General => None,
            Self::Number => Some("n"),
            Self::SharedString(_) => Some("s"),
            Self::String => Some("str"),
        }
    }
}
struct XmlAttr<'text> {
    name: Cow<'text, str>,
    value: Cow<'text, str>,
}
struct WorksheetParser<'xml> {
    cell_count: usize,
    shared_formula_heads: BTreeMap<u32, SharedFormulaHead>,
    xml: &'xml str,
}
struct WorksheetSemanticFacts {
    formula_count: usize,
    last_data_row: Option<u32>,
    last_master_address_row: Option<u32>,
    shared_ref_count: usize,
}
#[derive(Clone, Copy)]
enum XmlEscapeContext {
    Attribute,
    Text,
}
#[derive(Clone, Copy)]
struct CellReference {
    pub col: u32,
    pub col_locked: bool,
    pub row: u32,
    pub row_locked: bool,
}
impl SharedStringTable {
    fn canonicalize_excel_text_runs(&mut self) -> Result<()> {
        let mut replacement = String::new();
        for entry in &mut self.entries {
            let xml = &mut entry.xml;
            let mut cursor = 0_usize;
            loop {
                let mut scanner = XmlScanner::new(xml);
                scanner.skip_to(cursor);
                let Some(element) = scanner.next_element_named("t")? else {
                    break;
                };
                let start = element.span.start;
                let mut attrs = parse_tag_attrs(element.opening.raw)?;
                if element.opening.self_closing {
                    let empty_tag = build_self_closing_tag("t", &attrs)?;
                    xml.replace_range(element.span, &empty_tag);
                    cursor =
                        checked_usize_add(start, empty_tag.len(), "shared string 다음 t 위치")?;
                    continue;
                }
                let decoded = decode_xml_entities(element.body)?;
                let preserve = decoded.chars().next().is_some_and(char::is_whitespace)
                    || decoded.chars().next_back().is_some_and(char::is_whitespace);
                if preserve {
                    set_attr(&mut attrs, "xml:space", "preserve");
                } else {
                    remove_attr(&mut attrs, "xml:space");
                }
                let opening = build_open_tag("t", &attrs)?;
                let escaped = try_xml_escape_text(
                    decoded.as_ref(),
                    XmlEscapeContext::Text,
                    "shared string XML",
                )?;
                let capacity = checked_capacity(&[opening.len(), escaped.len(), "</t>".len()])
                    .ok_or_else(|| err("shared string t 직렬화 용량 계산 실패"))?;
                replacement.clear();
                replacement.try_reserve_exact(capacity).map_err(|source| {
                    err_with_source("shared string t 직렬화 메모리 확보 실패", source)
                })?;
                replacement.push_str(&opening);
                replacement.push_str(&escaped);
                replacement.push_str("</t>");
                xml.replace_range(element.span, &replacement);
                cursor = checked_usize_add(start, replacement.len(), "shared string 다음 t 위치")?;
            }
        }
        Ok(())
    }
    fn get(&self, index: usize) -> Option<&str> {
        self.entries.get(index).map(|entry| entry.text.as_ref())
    }
    fn intern(&mut self, value: &str) -> Result<usize> {
        if let Some(&index) = self.index.get(value) {
            return Ok(index);
        }
        if self.entries.len() >= MAX_SHARED_STRING_COUNT {
            return Err(err(format!(
                "sharedStrings entry 개수가 허용 한도({MAX_SHARED_STRING_COUNT})를 초과했습니다."
            )));
        }
        let growth =
            SHARED_STRING_GROWTH.min(MAX_SHARED_STRING_COUNT.strict_sub(self.entries.len()));
        if self.index.len() == self.index.capacity() {
            self.index.try_reserve(growth).map_err(|source| {
                err_with_source("shared string index 추가 메모리 확보 실패", source)
            })?;
        }
        if self.entries.len() == self.entries.capacity() {
            self.entries.try_reserve(growth).map_err(|source| {
                err_with_source("sharedStrings XML entry 추가 메모리 확보 실패", source)
            })?;
        }
        let index = self.entries.len();
        let stored_value = Rc::<str>::from(value);
        let escaped_len =
            validated_xml_escaped_len(value, XmlEscapeContext::Text, "shared string XML")?;
        let preserve = value.chars().next().is_some_and(char::is_whitespace)
            || value.chars().next_back().is_some_and(char::is_whitespace);
        let text_attrs = if preserve {
            XML_SPACE_PRESERVE_ATTR
        } else {
            ""
        };
        let capacity = checked_capacity(&["<si><t></t></si>".len(), text_attrs.len(), escaped_len])
            .ok_or_else(|| err("shared string XML 용량 계산 실패"))?;
        let mut entry = try_string_with_capacity(capacity, "shared string XML 메모리 확보 실패")?;
        entry.push_str("<si><t");
        entry.push_str(text_attrs);
        entry.push('>');
        append_xml_escaped(&mut entry, value, XmlEscapeContext::Text);
        entry.push_str("</t></si>");
        self.entries.push(SharedStringEntry {
            text: Rc::clone(&stored_value),
            xml: entry,
        });
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
        let cell = Worksheet::get_or_create_cell_mut(&mut worksheet.rows, col, row)?;
        cell.value_type = CellValueType::SharedString(index);
        cell.inner_xml = None;
        Ok(())
    }
    fn to_xml(&self, reference_count: usize) -> Result<String> {
        let mut xml = format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n<sst xmlns=\"{SPREADSHEETML_NAMESPACE}\" count=\"{reference_count}\" uniqueCount=\"{}\">",
            self.entries.len()
        );
        let additional_capacity = self.entries.iter().try_fold("</sst>".len(), |sum, entry| {
            sum.checked_add(entry.xml.len())
                .ok_or_else(|| err("sharedStrings XML 용량 계산 실패"))
        })?;
        xml.try_reserve_exact(additional_capacity)
            .map_err(|source| err_with_source("sharedStrings XML 메모리 확보 실패", source))?;
        for entry in &self.entries {
            xml.push_str(&entry.xml);
        }
        xml.push_str("</sst>");
        Ok(xml)
    }
}
impl Workbook {
    fn build_calc_chain_xml(
        &mut self,
        master_formula_count: usize,
        change_log_formula_count: usize,
    ) -> Result<String> {
        let source_xml = self.calc_chain_xml.take();
        let mut change_log_remaining = change_log_formula_count;
        let mut master_remaining = master_formula_count;
        let mut change_log_matches = source_xml.is_some();
        let mut master_matches = source_xml.is_some();
        if let Some(source) = source_xml.as_deref() {
            let mut scanner = XmlScanner::new(source);
            while let Some(tag) = scanner.next_start_named("c") {
                let attrs = parse_tag_attrs(tag.raw)?;
                let reference = get_attr(&attrs, "r")
                    .and_then(parse_ref_with_locks)
                    .filter(|value| !value.col_locked && !value.row_locked)
                    .ok_or_else(|| err("calcChain cell reference 형식이 올바르지 않습니다."))?;
                match get_attr(&attrs, "i") {
                    Some("1") => {
                        master_matches &= self
                            .master_sheet
                            .try_get_formula_at(reference.col, reference.row)?
                            .is_some();
                        master_matches &= master_remaining > 0;
                        master_remaining = master_remaining.saturating_sub(1);
                    }
                    Some("2") => {
                        change_log_matches &= self
                            .change_log_sheet
                            .try_get_formula_at(reference.col, reference.row)?
                            .is_some();
                        change_log_matches &= change_log_remaining > 0;
                        change_log_remaining = change_log_remaining.saturating_sub(1);
                    }
                    _ => return Err(err("calcChain sheet id가 올바르지 않습니다.")),
                }
            }
            master_matches &= master_remaining == 0;
            change_log_matches &= change_log_remaining == 0;
        }
        let remaining_source_xml = match source_xml {
            Some(source) if change_log_matches && master_matches => return Ok(source),
            other => other,
        };
        let formula_count = change_log_formula_count.strict_add(master_formula_count);
        if formula_count == 0 {
            return Err(err("calcChain에 기록할 formula가 없습니다."));
        }
        let header = concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n",
            "<calcChain xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\">",
        );
        let capacity = formula_count
            .checked_mul(28)
            .and_then(|cells| cells.checked_add(header.len()))
            .and_then(|bytes| bytes.checked_add("</calcChain>".len()))
            .ok_or_else(|| err("calcChain XML 용량 계산 실패"))?;
        let mut xml = try_string_with_capacity(capacity, "calcChain XML 메모리 확보 실패")?;
        xml.push_str(header);
        let mut first = true;
        for (sheet_id, worksheet, reverse, preserve) in [
            (2_u8, &self.change_log_sheet, true, change_log_matches),
            (1_u8, &self.master_sheet, false, master_matches),
        ] {
            if !preserve {
                if reverse {
                    let last_row = u32::try_from(worksheet.rows.len()).map_err(|source| {
                        err_with_source("calcChain row 번호 계산 실패", source)
                    })?;
                    for (row, row_obj) in (1_u32..=last_row).rev().zip(worksheet.rows.iter().rev())
                    {
                        for cell in row_obj.cells.iter().rev() {
                            append_calc_chain_cell(&mut xml, cell, row, sheet_id, &mut first)?;
                        }
                    }
                } else {
                    for (row, row_obj) in (1_u32..=MAX_A1_ROW).zip(&worksheet.rows) {
                        for cell in &row_obj.cells {
                            append_calc_chain_cell(&mut xml, cell, row, sheet_id, &mut first)?;
                        }
                    }
                }
                continue;
            }
            let Some(source) = remaining_source_xml.as_deref() else {
                continue;
            };
            let expected_id = if sheet_id == 1 { "1" } else { "2" };
            let mut scanner = XmlScanner::new(source);
            while let Some(tag) = scanner.next_start_named("c") {
                let mut attrs = parse_tag_attrs(tag.raw)?;
                if get_attr(&attrs, "i") != Some(expected_id) {
                    continue;
                }
                if mem::replace(&mut first, false) && get_attr(&attrs, "l").is_none() {
                    set_attr(&mut attrs, "l", "1");
                    xml.push_str(&build_self_closing_tag("c", &attrs)?);
                } else {
                    xml.push_str(tag.raw);
                }
            }
        }
        xml.push_str("</calcChain>");
        Ok(xml)
    }
    pub(crate) const fn change_log_sheet_mut(
        &mut self,
    ) -> (&mut Worksheet, &mut SharedStringTable) {
        (&mut self.change_log_sheet, &mut self.shared_strings)
    }
    pub(crate) fn from_container(mut container: XlsxContainer) -> Result<Self> {
        let mut workbook_xml = container.take_text("xl/workbook.xml")?;
        let mut workbook_scanner = XmlScanner::new(&workbook_xml);
        let calc_pr = workbook_scanner
            .next_start_named("calcPr")
            .ok_or_else(|| err("workbook.xml의 calcPr 태그를 찾지 못했습니다."))?;
        if calc_pr.name != "calcPr" || !calc_pr.self_closing {
            return Err(err(
                "workbook.xml의 calcPr는 unprefixed self-closing 태그여야 합니다.",
            ));
        }
        if workbook_scanner.next_start_named("calcPr").is_some() {
            return Err(err("workbook.xml에 calcPr 태그가 여러 개 있습니다."));
        }
        let input_calc_chain_xml = container.ensure_fixed_sheet_catalog(&mut workbook_xml)?;
        for (qualified_name, local_name) in [
            ("mc:AlternateContent", "AlternateContent"),
            ("xr:revisionPtr", "revisionPtr"),
        ] {
            let mut scanner = XmlScanner::new(&workbook_xml);
            let Some(element) = scanner.next_element_named(local_name)? else {
                continue;
            };
            let tag = element.opening;
            if tag.name != qualified_name {
                return Err(err(format!(
                    "workbook.xml의 {local_name} namespace가 올바르지 않습니다."
                )));
            }
            if scanner.next_start_named(local_name).is_some() {
                return Err(err(format!(
                    "workbook.xml에 {local_name} 요소가 여러 개 있습니다."
                )));
            }
            workbook_xml.replace_range(element.span, "");
        }
        let shared_strings_xml_text = container.take_shared_strings_text()?;
        let mut shared_strings_scanner = XmlScanner::new(&shared_strings_xml_text);
        let sst = shared_strings_scanner
            .next_element_named("sst")?
            .ok_or_else(|| err("sharedStrings XML에 <sst>가 없습니다."))?;
        let sst_tag = sst.opening;
        if sst_tag.name != "sst" || sst_tag.self_closing {
            return Err(err(
                "sharedStrings XML의 sst root 형식이 고정 스키마와 다릅니다.",
            ));
        }
        let sst_attrs = parse_tag_attrs(sst_tag.raw)?;
        let (declared_count_text, declared_unique_count_text) = get_attr(&sst_attrs, "count")
            .zip(get_attr(&sst_attrs, "uniqueCount"))
            .filter(|_| {
                sst_attrs.len() == 3
                    && get_attr(&sst_attrs, "xmlns") == Some(SPREADSHEETML_NAMESPACE)
            })
            .ok_or_else(|| err("sharedStrings XML의 sst root 속성이 고정 스키마와 다릅니다."))?;
        let declared_shared_count =
            parse_usize_decimal(declared_count_text, "sharedStrings count 해석 실패")?;
        let declared_unique_count = parse_usize_decimal(
            declared_unique_count_text,
            "sharedStrings uniqueCount 해석 실패",
        )?;
        if shared_strings_scanner.next_start_named("sst").is_some() {
            return Err(err("sharedStrings XML에 sst root가 여러 개 있습니다."));
        }
        let initial_capacity = declared_unique_count.min(SHARED_STRING_INITIAL_CAPACITY);
        let mut entries =
            try_vec_with_capacity(initial_capacity, "shared string entry 메모리 확보 실패")?;
        let mut scanner = XmlScanner::new(&shared_strings_xml_text);
        while let Some(si) = scanner.next_element_named("si")? {
            if entries.len() >= MAX_SHARED_STRING_COUNT {
                return Err(err(format!(
                    "sharedStrings entry 개수가 허용 한도({MAX_SHARED_STRING_COUNT})를 초과했습니다."
                )));
            }
            if entries.len() == entries.capacity() {
                entries
                    .try_reserve(SHARED_STRING_GROWTH)
                    .map_err(|source| {
                        err_with_source("shared string entry 메모리 확보 실패", source)
                    })?;
            }
            let value = extract_all_tag_text(si.body, "t")?.unwrap_or(Cow::Borrowed(""));
            let si_xml = shared_strings_xml_text
                .get(si.span)
                .ok_or_else(|| err("sharedStrings.xml의 si entry 범위가 손상되었습니다."))?;
            entries.push(SharedStringEntry {
                text: Rc::<str>::from(value.as_ref()),
                xml: copy_text(si_xml)?,
            });
        }
        if entries.len() != declared_unique_count {
            return Err(err(format!(
                "sharedStrings uniqueCount가 실제 entry 수와 다릅니다: declared={declared_unique_count}, actual={}",
                entries.len()
            )));
        }
        let entry_count = entries.len();
        let mut index = HashMap::new();
        index.try_reserve(entry_count).map_err(|source| {
            err_with_source("shared string index map 메모리 확보 실패", source)
        })?;
        for (value_index, entry) in entries.iter().enumerate() {
            if index.insert(Rc::clone(&entry.text), value_index).is_some() {
                return Err(err(format!(
                    "고정 sharedStrings에 중복 문자열이 있습니다: index={value_index}"
                )));
            }
        }
        let shared_strings = SharedStringTable { entries, index };
        let master_xml = container.take_worksheet_text(MASTER_SHEET_PATH, MASTER_SHEET_NAME)?;
        let master_sheet = WorksheetParser {
            cell_count: 0,
            shared_formula_heads: BTreeMap::new(),
            xml: &master_xml,
        }
        .scan_worksheet()?;
        master_sheet.validate_fixed_header(ExcelSheetKind::Master, &shared_strings)?;
        let change_log_xml =
            container.take_worksheet_text(CHANGE_LOG_SHEET_PATH, CHANGE_LOG_SHEET_NAME)?;
        let change_log_sheet = WorksheetParser {
            cell_count: 0,
            shared_formula_heads: BTreeMap::new(),
            xml: &change_log_xml,
        }
        .scan_worksheet()?;
        change_log_sheet.validate_fixed_header(ExcelSheetKind::ChangeLog, &shared_strings)?;
        let input_styles = container.package_prepare_excel_output()?;
        let workbook = Self {
            calc_chain_xml: input_calc_chain_xml,
            change_log_sheet,
            container,
            input_styles,
            master_sheet,
            shared_strings,
            xml_text: workbook_xml,
        };
        let formula_count = workbook.validate_fixed_semantics(declared_shared_count)?;
        if let Some(source_chain) = workbook.calc_chain_xml.as_deref() {
            workbook.validate_calc_chain(source_chain, formula_count)?;
        }
        Ok(workbook)
    }
    pub(crate) const fn master_sheet_mut(&mut self) -> (&mut Worksheet, &mut SharedStringTable) {
        (&mut self.master_sheet, &mut self.shared_strings)
    }
    fn request_full_recalculation(&mut self) -> Result<()> {
        let out = &mut self.xml_text;
        let root = find_start_tag_location(out, "workbook", 0)?
            .filter(|location| !location.self_closing)
            .ok_or_else(|| err("workbook.xml의 workbook root 태그를 찾지 못했습니다."))?;
        out.replace_range(0..root.span.end, EXCEL_WORKBOOK_OPENING);
        if let Some(protection) = find_start_tag_location(out, "workbookProtection", 0)? {
            let span = empty_xml_element_span(
                out,
                &protection,
                "workbookProtection",
                "workbook protection",
            )?;
            out.replace_range(span, "");
        }
        replace_single_xml_element(out, "bookViews", EXCEL_BOOK_VIEWS_XML)?;
        let calc_pr = find_start_tag_location(out, "calcPr", 0)?
            .ok_or_else(|| err("workbook.xml의 calcPr 태그를 찾지 못했습니다."))?;
        let calc_pr_span =
            empty_xml_element_span(out, &calc_pr, "calcPr", "workbook calculation properties")?;
        out.replace_range(
            calc_pr_span,
            "<calcPr calcId=\"191029\" iterateDelta=\"1E-4\" forceFullCalc=\"1\"/>",
        );
        replace_single_xml_element(out, "extLst", EXCEL_CALC_EXTENSIONS_XML)?;
        Ok(())
    }
    fn request_recalculation_caches(&mut self) -> Result<()> {
        let strings = &self.shared_strings;
        let mut input = [0_i32; 6];
        for (slot, row) in input.iter_mut().zip(4_u32..=9_u32) {
            *slot = self
                .master_sheet
                .get_i32_at(2, row, strings)?
                .ok_or_else(|| err(format!("유류비 고정 입력값이 비어 있습니다: B{row}")))?;
        }
        let [
            gasoline_qty,
            premium_qty,
            diesel_qty,
            gasoline_weight,
            premium_weight,
            diesel_weight,
        ] = input.map(i128::from);
        let total_qty = gasoline_qty.strict_add(premium_qty).strict_add(diesel_qty);
        let mut cache = String::new();
        push_decimal_text(&mut cache, total_qty);
        self.master_sheet
            .set_formula_cached_value_at(2, 10, Some(&cache), false)?;
        let fuel_denominator = gasoline_qty.strict_add(premium_qty);
        let premium_cache = if fuel_denominator == 0 {
            None
        } else {
            let numerator = gasoline_qty
                .strict_mul(gasoline_weight)
                .strict_add(premium_qty.strict_mul(premium_weight));
            format_excel_ratio_into(&mut cache, numerator, fuel_denominator)?;
            Some(cache.as_str())
        };
        self.master_sheet
            .set_formula_cached_value_at(2, 11, premium_cache, false)?;
        let diesel_cache = if premium_qty.strict_add(diesel_qty) == 0 {
            None
        } else {
            if fuel_denominator == 0 {
                return Err(err("유류비 경유 평균 단가 분모가 0입니다."));
            }
            let numerator = gasoline_qty
                .strict_mul(gasoline_weight)
                .strict_add(premium_qty.strict_mul(diesel_weight));
            format_excel_ratio_into(&mut cache, numerator, fuel_denominator)?;
            Some(cache.as_str())
        };
        self.master_sheet
            .set_formula_cached_value_at(2, 12, diesel_cache, false)
    }
    pub(crate) fn save(mut self, target_path: &Path, verification: SaveVerification) -> Result<()> {
        self.request_full_recalculation()?;
        self.update_shared_string_catalog()?;
        self.shared_strings.canonicalize_excel_text_runs()?;
        self.request_recalculation_caches()?;
        self.master_sheet
            .canonicalize_excel_output(ExcelSheetKind::Master, &self.input_styles)?;
        self.change_log_sheet
            .canonicalize_excel_output(ExcelSheetKind::ChangeLog, &self.input_styles)?;
        self.master_sheet.canonical_share_formulas()?;
        self.change_log_sheet.canonical_share_formulas()?;
        self.master_sheet
            .validate_fixed_header(ExcelSheetKind::Master, &self.shared_strings)?;
        let (master_xml, master_shared_count, master_formula_count) = self.master_sheet.to_xml()?;
        self.container.put_text(MASTER_SHEET_PATH, master_xml)?;
        self.change_log_sheet
            .validate_fixed_header(ExcelSheetKind::ChangeLog, &self.shared_strings)?;
        let (change_log_xml, change_log_shared_count, change_log_formula_count) =
            self.change_log_sheet.to_xml()?;
        self.container
            .put_text(CHANGE_LOG_SHEET_PATH, change_log_xml)?;
        let calc_chain_xml =
            self.build_calc_chain_xml(master_formula_count, change_log_formula_count)?;
        let shared_string_reference_count = master_shared_count.strict_add(change_log_shared_count);
        let shared_strings_xml = self.shared_strings.to_xml(shared_string_reference_count)?;
        self.container.put_text("xl/workbook.xml", self.xml_text)?;
        self.container
            .put_text("xl/sharedStrings.xml", shared_strings_xml)?;
        self.container.put_text(CALC_CHAIN_PATH, calc_chain_xml)?;
        self.container.save(target_path, verification)
    }
    pub(crate) fn update_filter_database_defined_name(&mut self, last_data_row: u32) -> Result<()> {
        let (row_span, _) = fixed_filter_database_row(&self.xml_text)?;
        let replacement_capacity = u32_decimal_text_len(last_data_row);
        let mut replacement =
            try_string_with_capacity(replacement_capacity, "_FilterDatabase ref 메모리 확보 실패")?;
        push_decimal_text(&mut replacement, last_data_row);
        self.xml_text.replace_range(row_span, &replacement);
        Ok(())
    }
    fn update_shared_string_catalog(&mut self) -> Result<()> {
        let string_count = self.shared_strings.entries.len();
        self.shared_strings.index = HashMap::new();
        let mut mapping =
            try_vec_with_capacity(string_count, "shared string index 변환표 메모리 확보 실패")?;
        mapping.resize(string_count, usize::MAX);
        self.master_sheet
            .canonical_mark_shared_strings(&mut mapping)?;
        self.change_log_sheet
            .canonical_mark_shared_strings(&mut mapping)?;
        let entries = &mut self.shared_strings.entries;
        let mut write = 0_usize;
        for (old_index, slot) in mapping.iter_mut().enumerate() {
            if *slot == usize::MAX {
                continue;
            }
            entries.swap(write, old_index);
            *slot = write;
            write = write.strict_add(1);
        }
        entries.truncate(write);
        self.master_sheet.canonical_remap_shared_strings(&mapping)?;
        self.change_log_sheet
            .canonical_remap_shared_strings(&mapping)?;
        Ok(())
    }
    fn validate_calc_chain(&self, calc_chain_xml: &str, expected_count: usize) -> Result<()> {
        let mut cells = HashSet::new();
        cells
            .try_reserve(expected_count)
            .map_err(|source| err_with_source("calcChain cell 집합 메모리 확보 실패", source))?;
        let mut scanner = XmlScanner::new(calc_chain_xml);
        while let Some(tag) = scanner.next_start_named("c") {
            let attrs = parse_tag_attrs(tag.raw)?;
            let reference_text =
                get_attr(&attrs, "r").ok_or_else(|| err("calcChain cell reference가 없습니다."))?;
            let reference = parse_ref_with_locks(reference_text)
                .filter(|reference| !reference.col_locked && !reference.row_locked)
                .ok_or_else(|| err("calcChain cell reference 형식이 올바르지 않습니다."))?;
            let sheet_id = get_attr(&attrs, "i")
                .and_then(|value| value.parse::<u8>().ok())
                .filter(|value| matches!(value, 1 | 2))
                .ok_or_else(|| err("calcChain sheet id가 올바르지 않습니다."))?;
            let worksheet = if sheet_id == 1 {
                &self.master_sheet
            } else {
                &self.change_log_sheet
            };
            if worksheet
                .try_get_formula_at(reference.col, reference.row)?
                .is_none()
            {
                return Err(err(format!(
                    "calcChain이 수식이 없는 cell을 참조합니다: sheet={sheet_id}, cell={reference_text}"
                )));
            }
            if !cells.insert((sheet_id, reference.col, reference.row)) {
                return Err(err(format!(
                    "calcChain cell이 중복됩니다: sheet={sheet_id}, cell={reference_text}"
                )));
            }
        }
        if cells.len() != expected_count {
            return Err(err(format!(
                "calcChain cell 수가 실제 수식 수와 다릅니다: chain={}, formulas={expected_count}",
                cells.len()
            )));
        }
        Ok(())
    }
    fn validate_fixed_semantics(&self, declared_shared_count: usize) -> Result<usize> {
        let shared_strings = &self.shared_strings;
        let master_facts = self.master_sheet.semantic_facts(
            ExcelSheetKind::Master,
            shared_strings,
            &self.input_styles,
        )?;
        let master_last_row = master_facts.last_data_row;
        let filter_last_row = self.master_sheet.fixed_master_auto_filter()?.2;
        if master_last_row != Some(filter_last_row) {
            return Err(err(format!(
                "유류비 autoFilter 마지막 행이 실제 데이터 마지막 행과 다릅니다: filter={filter_last_row}, actual={master_last_row:?}"
            )));
        }
        let address_last_row = master_facts
            .last_master_address_row
            .unwrap_or(MASTER_FORMULA_LAYOUT.data_start_row);
        if address_last_row != filter_last_row {
            return Err(err(format!(
                "유류비 autoFilter 마지막 행이 실제 주소 데이터 마지막 행과 다릅니다: filter={filter_last_row}, actual={address_last_row}"
            )));
        }
        let (_, defined_last_row) = fixed_filter_database_row(&self.xml_text)?;
        if defined_last_row != filter_last_row {
            return Err(err(format!(
                "_FilterDatabase 범위가 autoFilter와 다릅니다: {defined_last_row} != {filter_last_row}"
            )));
        }
        let change_log_facts = self.change_log_sheet.semantic_facts(
            ExcelSheetKind::ChangeLog,
            shared_strings,
            &self.input_styles,
        )?;
        let change_log_last_row = change_log_facts.last_data_row;
        self.change_log_sheet.validate_change_log_formats(
            change_log_last_row.unwrap_or(CHANGE_LOG_FORMULA_LAYOUT.data_start_row),
        )?;
        let shared_ref_count = master_facts
            .shared_ref_count
            .strict_add(change_log_facts.shared_ref_count);
        if declared_shared_count != shared_ref_count {
            return Err(err(format!(
                "sharedStrings count가 실제 참조 수와 다릅니다: declared={declared_shared_count}, actual={shared_ref_count}"
            )));
        }
        Ok(master_facts
            .formula_count
            .strict_add(change_log_facts.formula_count))
    }
}
impl WorksheetParser<'_> {
    fn parse_row(&mut self, row_body: &str, row_num: u32, row: &mut Row) -> Result<()> {
        let mut scanner = XmlScanner::new(row_body);
        let mut next_col = 1_u32;
        while let Some(cell) = scanner.next_element_named("c")? {
            let inner_xml_text = cell.body;
            let cell_info = cell.opening;
            let cell_tag = cell_info.raw;
            let mut attr_count = 0_usize;
            let mut attr_scanner = XmlAttrScanner::new(cell_tag)?;
            let mut reference_value = None;
            let mut style_value = None;
            let mut type_value = None;
            let mut unsupported_attr = None;
            while let Some((name, value)) = attr_scanner.next()? {
                if attr_count == MAX_XML_ATTRIBUTE_COUNT {
                    return Err(err("XML 속성 개수가 허용 한도를 초과했습니다."));
                }
                attr_count = attr_count.strict_add(1);
                if name.is_empty() {
                    return Err(err("XML 속성 파싱 실패: 빈 속성 이름"));
                }
                let slot = match name {
                    "r" => &mut reference_value,
                    "s" => &mut style_value,
                    "t" => &mut type_value,
                    _ => {
                        unsupported_attr.get_or_insert(name);
                        continue;
                    }
                };
                if slot.replace(value).is_some() {
                    return Err(err("XML 태그에 중복 속성이 있습니다."));
                }
            }
            let reference_text = reference_value
                .as_deref()
                .ok_or_else(|| err(format!("cell reference가 없습니다: row={row_num}")))?;
            let reference = parse_ref_with_locks(reference_text).ok_or_else(|| {
                err(format!(
                    "cell reference 형식이 비정상입니다: row={row_num}, ref={reference_text}"
                ))
            })?;
            if reference.row != row_num {
                return Err(err(format!(
                    "cell reference row가 row 태그와 다릅니다: row={row_num}, ref={reference_text}"
                )));
            }
            let col = reference.col;
            if !(1..=MAX_A1_COL).contains(&col) {
                return Err(err(row_col_error(
                    "Excel column 범위를 벗어난 cell이 있습니다. (row=",
                    row_num,
                    col,
                )));
            }
            if col < next_col {
                return Err(err(row_col_error(
                    "worksheet cell 순서는 column 오름차순이어야 합니다. (row=",
                    row_num,
                    col,
                )));
            }
            let style = style_value
                .map(|value| {
                    parse_u32_decimal(
                        &value,
                        || {
                            Cow::Owned(row_col_error(
                                "worksheet cell style이 음이 아닌 10진수 형식이 아닙니다. (row=",
                                row_num,
                                col,
                            ))
                        },
                        || {
                            Cow::Owned(row_col_error(
                                "worksheet cell style 해석 실패 (row=",
                                row_num,
                                col,
                            ))
                        },
                    )
                })
                .transpose()?;
            let (value_type, shared_string) = if let Some(value) = type_value {
                match value.as_ref() {
                    "n" => (CellValueType::Number, false),
                    "s" => (CellValueType::General, true),
                    "str" => (CellValueType::String, false),
                    _ => {
                        return Err(err(format!(
                            "고정 workbook에서 지원하지 않는 cell type입니다: row={row_num}, col={col}, type={value}"
                        )));
                    }
                }
            } else {
                (CellValueType::General, false)
            };
            if let Some(name) = unsupported_attr {
                return Err(err(format!(
                    "고정 workbook cell에 지원하지 않는 속성이 있습니다: row={row_num}, col={col}, attribute={name}"
                )));
            }
            let has_attrs =
                style.is_some() || value_type != CellValueType::General || shared_string;
            if cell_info.self_closing {
                if shared_string {
                    return Err(err("shared string cell 본문이 없습니다."));
                }
                if has_attrs {
                    self.retain_cell(
                        row_num,
                        row,
                        Cell {
                            col,
                            inner_xml: None,
                            style,
                            value_type,
                        },
                    )?;
                }
                next_col = next_cell_col(row_num, col)?;
                continue;
            }
            let retained_cell = if shared_string {
                let mut value_scanner = XmlScanner::new(inner_xml_text);
                let value = value_scanner
                    .next_element_named("v")?
                    .filter(|element| element.opening.name == "v" && !element.opening.self_closing)
                    .ok_or_else(|| err("shared string cell에 v 태그가 없습니다."))?;
                let mut value_attrs = XmlAttrScanner::new(value.opening.raw)?;
                if value_attrs.next()?.is_some() {
                    return Err(err("shared string v 태그에 속성이 있습니다."));
                }
                if value_scanner.next_tag().is_some()
                    || !inner_xml_text
                        .get(..value.span.start)
                        .is_some_and(|text| text.trim().is_empty())
                    || !inner_xml_text
                        .get(value.span.end..)
                        .is_some_and(|text| text.trim().is_empty())
                {
                    return Err(err("shared string cell 본문 형식이 올바르지 않습니다."));
                }
                let index = parse_usize_decimal(value.body, "shared string index 해석 실패")?;
                Some(Cell {
                    col,
                    inner_xml: None,
                    style,
                    value_type: CellValueType::SharedString(index),
                })
            } else if has_attrs || !inner_xml_text.is_empty() {
                Some(Cell {
                    col,
                    inner_xml: Some(copy_text(inner_xml_text)?),
                    style,
                    value_type,
                })
            } else {
                None
            };
            if let Some(parsed_cell) = retained_cell {
                self.retain_cell(row_num, row, parsed_cell)?;
            }
            next_col = next_cell_col(row_num, col)?;
        }
        Ok(())
    }
    fn retain_cell(&mut self, row_num: u32, row: &mut Row, mut cell: Cell) -> Result<()> {
        let col = cell.col;
        if self.cell_count >= MAX_WORKSHEET_CELL_COUNT {
            return Err(err(format!(
                "worksheet cell 개수가 허용 한도({MAX_WORKSHEET_CELL_COUNT})를 초과했습니다."
            )));
        }
        if let Some(inner_xml) = cell.inner_xml.as_deref() {
            let mut formula_scanner = XmlScanner::new(inner_xml);
            if let Some(formula) = formula_scanner.next_element_named("f")? {
                let formula_tag = formula.opening;
                let formula_span = formula.span;
                let decoded_formula = decode_xml_entities(formula.body)?;
                let mut aca = None;
                let mut formula_type = None;
                let mut reference = None;
                let mut shared_index = None;
                let mut attr_count = 0_usize;
                let mut attr_scanner = XmlAttrScanner::new(formula_tag.raw)?;
                while let Some((name, value)) = attr_scanner.next()? {
                    if attr_count == MAX_XML_ATTRIBUTE_COUNT {
                        return Err(err("XML 속성 개수가 허용 한도를 초과했습니다."));
                    }
                    attr_count = attr_count.strict_add(1);
                    if name.is_empty() {
                        return Err(err("XML 속성 파싱 실패: 빈 속성 이름"));
                    }
                    let slot = match name {
                        "aca" => &mut aca,
                        "ref" => &mut reference,
                        "si" => &mut shared_index,
                        "t" => &mut formula_type,
                        _ => {
                            return Err(err(format!(
                                "formula에 지원하지 않는 속성이 있습니다: {name}"
                            )));
                        }
                    };
                    if slot.replace(value).is_some() {
                        return Err(err("XML 태그에 중복 속성이 있습니다."));
                    }
                }
                let shared = formula_type.as_deref() == Some("shared");
                if !shared
                    && (formula_type.is_some()
                        || reference.is_some()
                        || shared_index.is_some()
                        || aca.as_deref().is_some_and(|value| value != "false"))
                {
                    return Err(err(format!(
                        "고정 workbook은 aca=\"false\" 외 formula 속성을 지원하지 않습니다: row={row_num}, col={col}"
                    )));
                }
                if formula_scanner.next_start_named("v").is_none() {
                    return Err(err(format!(
                        "고정 workbook formula cache가 없습니다: row={row_num}, col={col}"
                    )));
                }
                let normalized = if shared {
                    if aca.is_some() {
                        return Err(err("shared formula에 지원하지 않는 속성이 있습니다."));
                    }
                    let si_text = shared_index
                        .as_deref()
                        .ok_or_else(|| err("shared formula에 si 속성이 없습니다."))?;
                    let si = parse_u32_decimal(
                        si_text,
                        || Cow::Borrowed("shared formula si가 음이 아닌 10진수가 아닙니다."),
                        || Cow::Borrowed("shared formula si 해석 실패"),
                    )?;
                    let range = reference
                        .as_deref()
                        .map(|reference_text| -> Result<(CellReference, CellReference)> {
                            let (start_text, end_text) = parse_range_token(reference_text);
                            let start = parse_ref_with_locks(start_text)
                                .filter(|value| !value.col_locked && !value.row_locked)
                                .ok_or_else(|| {
                                    err("shared formula ref 시작 형식이 올바르지 않습니다.")
                                })?;
                            let end = parse_ref_with_locks(end_text)
                                .filter(|value| !value.col_locked && !value.row_locked)
                                .ok_or_else(|| {
                                    err("shared formula ref 끝 형식이 올바르지 않습니다.")
                                })?;
                            Ok((start, end))
                        })
                        .transpose()?;
                    let formula_text =
                        (!decoded_formula.is_empty()).then(|| decoded_formula.into_owned());
                    let is_anchor = formula_text.is_some();
                    let head = if let Some(anchor_formula) = formula_text {
                        let (start, end) = range
                            .ok_or_else(|| err("shared formula anchor에 ref 범위가 없습니다."))?;
                        if (start.col, start.row) != (col, row_num)
                            || start.col != end.col
                            || start.row > end.row
                        {
                            return Err(err(format!(
                                "shared formula ref가 세로 anchor 범위와 다릅니다: row={row_num}, col={col}"
                            )));
                        }
                        match self.shared_formula_heads.entry(si) {
                            Entry::Vacant(entry) => entry.insert(SharedFormulaHead {
                                anchor_col: col,
                                anchor_row: row_num,
                                formula: anchor_formula,
                                last_row: end.row,
                                seen: 0,
                            }),
                            Entry::Occupied(_) => {
                                return Err(err(format!(
                                    "shared formula si anchor가 중복됩니다: {si}"
                                )));
                            }
                        }
                    } else {
                        if range.is_some() {
                            return Err(err("shared formula follower에 ref 범위가 있습니다."));
                        }
                        self.shared_formula_heads.get_mut(&si).ok_or_else(|| {
                            err(format!("shared formula anchor를 찾지 못했습니다: si={si}"))
                        })?
                    };
                    if col != head.anchor_col
                        || !(head.anchor_row..=head.last_row).contains(&row_num)
                    {
                        return Err(err(format!(
                            "shared formula follower가 ref 범위를 벗어났습니다: si={si}, row={row_num}, col={col}"
                        )));
                    }
                    let shifted_formula = if is_anchor {
                        Cow::Borrowed(head.formula.as_str())
                    } else {
                        let row_delta = row_num.strict_sub(head.anchor_row).cast_signed();
                        shift_formula(&head.formula, 0, row_delta)?
                            .map_or_else(|| Cow::Borrowed(head.formula.as_str()), Cow::Owned)
                    };
                    head.seen = head.seen.strict_add(1);
                    replace_formula_tag_at(
                        inner_xml,
                        formula_span,
                        FormulaTag::Plain(&shifted_formula),
                    )?
                } else {
                    replace_formula_tag_at(
                        inner_xml,
                        formula_span,
                        FormulaTag::Plain(decoded_formula.as_ref()),
                    )?
                };
                cell.inner_xml = Some(normalized);
            }
        }
        row.cells.push(cell);
        self.cell_count = self.cell_count.strict_add(1);
        Ok(())
    }
    fn scan_rows(&mut self, body_span: Range<usize>) -> Result<Vec<Row>> {
        let xml = self.xml;
        let Some(body) = xml.get(body_span) else {
            return Err(err("worksheet XML body 범위가 손상되었습니다."));
        };
        let mut rows = Vec::new();
        let mut scanner = XmlScanner::new(body);
        while let Some(row_element) = scanner.next_element_named("row")? {
            let row_info = row_element.opening;
            let mut row_attrs = parse_tag_attrs(row_info.raw)?;
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
            let mut attrs_xml = try_string_with_capacity(
                attrs_capacity,
                "worksheet row 속성 직렬화 메모리 확보 실패",
            )?;
            for attr in &row_attrs {
                push_attr_xml(&mut attrs_xml, attr);
            }
            if rows.len() == rows.capacity() {
                rows.try_reserve(WORKSHEET_ROW_GROWTH)
                    .map_err(|source| err_with_source("worksheet row 메모리 확보 실패", source))?;
            }
            if row_info.self_closing {
                rows.push(Row {
                    attrs_xml,
                    cells: Vec::new(),
                });
                continue;
            }
            let mut row = Row {
                attrs_xml,
                cells: Vec::new(),
            };
            self.parse_row(row_element.body, row_num, &mut row)?;
            rows.push(row);
        }
        Ok(rows)
    }
    fn scan_worksheet(mut self) -> Result<Worksheet> {
        let mut scanner = XmlScanner::new(self.xml);
        let Some(sheet_data) = scanner.next_element_named("sheetData")? else {
            return Err(err("worksheet XML에 <sheetData>가 없습니다."));
        };
        if sheet_data.opening.self_closing {
            return Err(err("고정 workbook의 sheetData는 비어 있을 수 없습니다."));
        }
        let sheet_data_body_span = sheet_data.body_span;
        let prefix_raw = self
            .xml
            .get(..sheet_data_body_span.start)
            .ok_or_else(|| err("worksheet XML prefix 범위가 손상되었습니다."))?;
        let suffix_raw = self
            .xml
            .get(sheet_data_body_span.end..)
            .ok_or_else(|| err("worksheet XML suffix 범위가 손상되었습니다."))?;
        let prefix = copy_text(prefix_raw)?;
        let suffix = copy_text(suffix_raw)?;
        let rows = self.scan_rows(sheet_data_body_span)?;
        for (si, head) in self.shared_formula_heads {
            let expected = head.last_row.strict_sub(head.anchor_row).strict_add(1);
            if head.seen != expected {
                return Err(err(format!(
                    "shared formula ref의 cell 수가 다릅니다: si={si}, expected={expected}, actual={}",
                    head.seen
                )));
            }
        }
        Ok(Worksheet {
            prefix,
            rows,
            suffix,
        })
    }
}
impl Worksheet {
    fn canonical_mark_shared_strings(&self, mapping: &mut [usize]) -> Result<()> {
        for row in &self.rows {
            for cell in &row.cells {
                let CellValueType::SharedString(index) = cell.value_type else {
                    continue;
                };
                let slot = mapping
                    .get_mut(index)
                    .ok_or_else(|| err(format!("shared string index 범위 오류: {index}")))?;
                *slot = 0;
            }
        }
        Ok(())
    }
    fn canonical_plain_formula(&self, col: u32, row: u32) -> Result<Option<Cow<'_, str>>> {
        let Some(inner) = self
            .cell_at(col, row)
            .and_then(|cell| cell.inner_xml.as_deref())
        else {
            return Ok(None);
        };
        let Some(formula) = extract_first_tag_text(inner, "f")? else {
            return Ok(None);
        };
        decode_xml_entities(formula).map(Some)
    }
    fn canonical_remap_shared_strings(&mut self, mapping: &[usize]) -> Result<()> {
        for row in &mut self.rows {
            for cell in &mut row.cells {
                let &mut CellValueType::SharedString(ref mut old_index) = &mut cell.value_type
                else {
                    continue;
                };
                let new_index = mapping
                    .get(*old_index)
                    .copied()
                    .filter(|index| *index != usize::MAX)
                    .ok_or_else(|| err("사용 중인 shared string의 compact index가 없습니다."))?;
                *old_index = new_index;
            }
        }
        Ok(())
    }
    fn canonical_share_formulas(&mut self) -> Result<()> {
        let mut next_si = 0_u32;
        let mut column_state = BTreeMap::<u32, (u32, bool)>::new();
        let mut reference = String::new();
        let row_count = u32::try_from(self.rows.len())
            .map_err(|source| err_with_source("shared formula row 번호 계산 실패", source))?;
        for (row, row_index) in (1_u32..=row_count).zip(0..self.rows.len()) {
            let cell_count = self
                .rows
                .get(row_index)
                .map_or(0, |row_obj| row_obj.cells.len());
            for cell_index in 0..cell_count {
                let col = self
                    .rows
                    .get(row_index)
                    .and_then(|row_obj| row_obj.cells.get(cell_index))
                    .map(|cell| cell.col)
                    .ok_or_else(|| err("shared formula cell 범위 오류"))?;
                if column_state
                    .get(&col)
                    .is_some_and(|&(last_formula_row, _)| row <= last_formula_row)
                {
                    continue;
                }
                let Some(anchor) = self.canonical_plain_formula(col, row)? else {
                    continue;
                };
                let interrupted =
                    column_state
                        .get(&col)
                        .is_some_and(|&(last_formula_row, was_interrupted)| {
                            was_interrupted || row > last_formula_row.strict_add(1)
                        });
                let max_followers = if interrupted {
                    MAX_SHARED_FORMULA_FOLLOWERS_AFTER_GAP
                } else {
                    MAX_SHARED_FORMULA_FOLLOWERS
                };
                let max_last_row = row.strict_add(max_followers).min(MAX_A1_ROW);
                let mut last_row = row;
                while last_row < max_last_row {
                    let candidate_row = last_row.strict_add(1);
                    let Some(candidate) = self.canonical_plain_formula(col, candidate_row)? else {
                        break;
                    };
                    let row_delta = candidate_row.strict_sub(row).cast_signed();
                    let expected = shift_formula(&anchor, 0, row_delta)?;
                    if candidate.as_ref() != expected.as_deref().unwrap_or(&anchor) {
                        break;
                    }
                    last_row = candidate_row;
                }
                let group_len = last_row.strict_sub(row).strict_add(1);
                column_state.insert(col, (last_row, interrupted));
                if group_len < MIN_SHARED_FORMULA_CELLS {
                    continue;
                }
                let owned_anchor = anchor.into_owned();
                reference.clear();
                with_unlocked_ref_parts(col, row, |col_name, row_number| {
                    reference.push_str(col_name);
                    push_decimal_text(&mut reference, row_number);
                    reference.push(':');
                    reference.push_str(col_name);
                    push_decimal_text(&mut reference, last_row);
                })?;
                for shared_row in row..=last_row {
                    let cell = Self::get_or_create_cell_mut(&mut self.rows, col, shared_row)?;
                    let inner = cell
                        .inner_xml
                        .as_deref()
                        .ok_or_else(|| err("shared formula 대상 cell 본문이 없습니다."))?;
                    let tag = if shared_row == row {
                        FormulaTag::SharedRoot {
                            formula: &owned_anchor,
                            reference: &reference,
                            si: next_si,
                        }
                    } else {
                        FormulaTag::SharedFollower(next_si)
                    };
                    let formula_span = XmlScanner::new(inner)
                        .next_element_named("f")?
                        .map(|formula| formula.span)
                        .ok_or_else(|| err("cell formula 태그를 찾지 못했습니다."))?;
                    cell.inner_xml = Some(replace_formula_tag_at(inner, formula_span, tag)?);
                }
                next_si = next_si.strict_add(1);
            }
        }
        Ok(())
    }
    fn canonicalize_excel_output(
        &mut self,
        sheet: ExcelSheetKind,
        input_styles: &CanonicalStyleMap,
    ) -> Result<()> {
        match sheet {
            ExcelSheetKind::Master => {
                let (_, _, filter_last_row) = self.fixed_master_auto_filter()?;
                self.canonicalize_excel_rows(sheet, input_styles)?;
                self.prefix = canonical_excel_fragment(EXCEL_MASTER_PREFIX)?;
                self.suffix = canonical_excel_fragment(EXCEL_MASTER_SUFFIX)?;
                self.update_auto_filter_ref(filter_last_row)?;
                self.update_master_sort_references()?;
            }
            ExcelSheetKind::ChangeLog => {
                let references = self.conditional_format_references()?;
                self.canonicalize_excel_rows(sheet, input_styles)?;
                self.prefix = canonical_excel_fragment(EXCEL_CHANGE_LOG_PREFIX)?;
                self.suffix = canonical_excel_fragment(EXCEL_CHANGE_LOG_SUFFIX)?;
                self.replace_conditional_format_references(&references)?;
            }
        }
        self.update_dimension()
    }
    fn canonicalize_excel_rows(
        &mut self,
        sheet: ExcelSheetKind,
        input_styles: &CanonicalStyleMap,
    ) -> Result<()> {
        let last_col = match sheet {
            ExcelSheetKind::ChangeLog => CHANGE_LOG_LAST_COL,
            ExcelSheetKind::Master => MASTER_LAST_COL,
        };
        let mut source_tag = String::new();
        for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&mut self.rows) {
            source_tag.clear();
            source_tag.push_str("<row");
            source_tag.push_str(&row.attrs_xml);
            source_tag.push_str("/>");
            let attrs = parse_tag_attrs(&source_tag)?;
            let style = get_attr(&attrs, "s")
                .map(|value| {
                    parse_u32_decimal(
                        value,
                        || Cow::Borrowed("row style이 10진수가 아닙니다."),
                        || Cow::Borrowed("row style 해석 실패"),
                    )
                })
                .transpose()?
                .map(|value| canonical_excel_style(value, sheet, row_num, None, input_styles))
                .transpose()?;
            let custom_format = xml_bool_attr(&attrs, "customFormat")?;
            let custom_height = xml_bool_attr(&attrs, "customHeight")?;
            let height = get_attr(&attrs, "ht");
            let mut canonical_attrs = mem::take(&mut row.attrs_xml);
            canonical_attrs.clear();
            append_fmt(
                &mut canonical_attrs,
                format_args!(" spans=\"1:{last_col}\""),
            );
            if let Some(style_id) = style {
                append_fmt(&mut canonical_attrs, format_args!(" s=\"{style_id}\""));
            }
            if custom_format {
                canonical_attrs.push_str(" customFormat=\"1\"");
            }
            if let Some(height_text) = height {
                canonical_attrs.push_str(" ht=\"");
                append_xml_escaped(
                    &mut canonical_attrs,
                    height_text,
                    XmlEscapeContext::Attribute,
                );
                canonical_attrs.push('"');
            }
            if custom_height {
                canonical_attrs.push_str(" customHeight=\"1\"");
            }
            row.attrs_xml = canonical_attrs;
            for cell in &mut row.cells {
                if let Some(style_id) = cell.style {
                    cell.style = Some(canonical_excel_style(
                        style_id,
                        sheet,
                        row_num,
                        Some(cell.col),
                        input_styles,
                    )?);
                }
                if cell.value_type == CellValueType::Number {
                    cell.value_type = CellValueType::General;
                }
            }
        }
        Ok(())
    }
    fn cell_at(&self, col: u32, row: u32) -> Option<&Cell> {
        let row_obj = row_index(row).and_then(|index| self.rows.get(index))?;
        row_obj.cell(col)
    }
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
    fn conditional_format_references(&self) -> Result<[String; 3]> {
        let mut scanner = XmlScanner::new(&self.suffix);
        let references = {
            let mut next_reference = || -> Result<String> {
                let formatting = scanner
                    .next_start_named("conditionalFormatting")
                    .ok_or_else(|| {
                        err("변경내역 conditionalFormatting 항목이 3개보다 적습니다.")
                    })?;
                let reference = extract_attr(formatting.raw, "sqref")?
                    .ok_or_else(|| err("변경내역 conditionalFormatting에 sqref가 없습니다."))?;
                copy_text(reference.as_ref())
            };
            [next_reference()?, next_reference()?, next_reference()?]
        };
        if scanner.next_start_named("conditionalFormatting").is_some() {
            return Err(err(
                "변경내역 conditionalFormatting 항목이 3개보다 많습니다.",
            ));
        }
        Ok(references)
    }
    pub(crate) fn copy_row_style(
        &mut self,
        source_row: u32,
        target_row: u32,
        max_col: u32,
    ) -> Result<()> {
        let src = row_index(source_row)
            .and_then(|index| self.rows.get(index))
            .ok_or_else(|| err(format!("worksheet style 원본 row가 없습니다: {source_row}")))?;
        let cell_count = src.cells.partition_point(|cell| cell.col <= max_col);
        let mut cells =
            try_vec_with_capacity(cell_count, "row style cell 목록 복사 메모리 확보 실패")?;
        for cell in src.cells.iter().take(cell_count) {
            cells.push(Cell {
                col: cell.col,
                inner_xml: None,
                style: cell.style,
                value_type: CellValueType::General,
            });
        }
        let copied = Row {
            attrs_xml: copy_text(&src.attrs_xml)?,
            cells,
        };
        let target_index = row_index(target_row)
            .ok_or_else(|| err("worksheet style 대상 row 번호가 올바르지 않습니다."))?;
        let required_len = target_index
            .checked_add(1)
            .ok_or_else(|| err("worksheet style 대상 row 길이 계산 실패"))?;
        if self.rows.len() < required_len {
            self.rows
                .try_reserve(required_len.strict_sub(self.rows.len()))
                .map_err(|source| {
                    err_with_source("worksheet style 대상 row 메모리 확보 실패", source)
                })?;
            self.rows.resize_with(required_len, Row::default);
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
            let range_count = sqref.split_whitespace().count();
            let mut ranges_out: Vec<Cow<'_, str>> = try_vec_with_capacity(
                range_count,
                "conditionalFormatting range 목록 메모리 확보 실패",
            )?;
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
                let mut out_sqref = try_string_with_capacity(
                    sqref.len(),
                    "conditionalFormatting sqref 메모리 확보 실패",
                )?;
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
            let updated_sqref = maybe_updated_sqref.map_or(sqref, Cow::Owned);
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
    fn fixed_master_auto_filter(&self) -> Result<(XmlTagLocation, Vec<XmlAttr<'_>>, u32)> {
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
        shared_strings: &SharedStringTable,
    ) -> Result<Option<i32>> {
        let text = self.try_get_display_at(col, row, shared_strings)?;
        let trimmed = text.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        let (negative, digits) = trimmed.strip_prefix('-').map_or_else(
            || {
                trimmed
                    .strip_prefix('+')
                    .map_or((false, trimmed), |unsigned| (false, unsigned))
            },
            |unsigned| (true, unsigned),
        );
        if digits.is_empty() {
            return Ok(None);
        }
        let mut whole = 0_i64;
        let mut round_away_from_zero = false;
        let mut saw_digit = false;
        let mut saw_fraction_digit = false;
        let mut seen_decimal = false;
        for byte in digits.bytes() {
            match byte {
                b',' if !seen_decimal => {}
                b'.' if !seen_decimal => {
                    seen_decimal = true;
                }
                b'0'..=b'9' => {
                    saw_digit = true;
                    let digit = i64::from(byte.strict_sub(b'0'));
                    if seen_decimal {
                        if !saw_fraction_digit {
                            round_away_from_zero = digit >= 5_i64;
                            saw_fraction_digit = true;
                        }
                    } else {
                        let Some(next) = whole
                            .checked_mul(10)
                            .and_then(|scaled| scaled.checked_add(digit))
                        else {
                            return Ok(None);
                        };
                        whole = next;
                    }
                }
                _ => return Ok(None),
            }
        }
        if !saw_digit {
            return Ok(None);
        }
        let magnitude = if round_away_from_zero {
            let Some(rounded) = whole.checked_add(1) else {
                return Ok(None);
            };
            rounded
        } else {
            whole
        };
        let signed = if negative {
            let Some(negative_value) = magnitude.checked_neg() else {
                return Ok(None);
            };
            negative_value
        } else {
            magnitude
        };
        Ok(i32::try_from(signed).ok())
    }
    fn get_or_create_cell_mut(rows: &mut Vec<Row>, col: u32, row: u32) -> Result<&mut Cell> {
        let row_index =
            row_index(row).ok_or_else(|| err("worksheet cell row 번호가 올바르지 않습니다."))?;
        let required_len = row_index
            .checked_add(1)
            .ok_or_else(|| err("worksheet cell row 길이 계산 실패"))?;
        if rows.len() < required_len {
            rows.try_reserve(required_len.strict_sub(rows.len()))
                .map_err(|source| err_with_source("worksheet cell row 메모리 확보 실패", source))?;
            rows.resize_with(required_len, Row::default);
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
                if row_obj.cells.len() == row_obj.cells.capacity() {
                    row_obj
                        .cells
                        .try_reserve(WORKSHEET_CELL_GROWTH)
                        .map_err(|source| {
                            err_with_source("worksheet cell 추가 메모리 확보 실패", source)
                        })?;
                }
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
            let tail_start = row.cells.partition_point(|cell| cell.col <= max_col);
            let mut write = tail_start;
            for read in tail_start..row.cells.len() {
                if cell_has_payload(
                    row.cells
                        .get(read)
                        .ok_or_else(|| err("worksheet cell 정리 범위 오류"))?,
                )? {
                    row.cells.swap(write, read);
                    write = write.strict_add(1);
                }
            }
            row.cells.truncate(write);
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
                set_attr(&mut attrs, "max", max_col.to_string());
                let new_tag = build_self_closing_tag("col", &attrs)?;
                self.prefix.replace_range(element_span, &new_tag);
                cursor = checked_usize_add(col_start, new_tag.len(), "col 정의 다음 cursor")?;
                continue;
            }
            cursor = element_span.end;
        }
        Ok(())
    }
    fn replace_conditional_format_references(&mut self, references: &[String; 3]) -> Result<()> {
        let mut cursor = 0_usize;
        for reference in references {
            let location = find_start_tag_location(&self.suffix, "conditionalFormatting", cursor)?
                .ok_or_else(|| err("Excel conditionalFormatting 정규형이 손상되었습니다."))?;
            let mut attrs = parse_tag_attrs_at(
                &self.suffix,
                &location,
                "Excel conditionalFormatting 태그 범위가 손상되었습니다.",
            )?;
            set_attr(&mut attrs, "sqref", reference.as_str());
            let replacement = build_open_tag("conditionalFormatting", &attrs)?;
            let start = location.span.start;
            self.suffix.replace_range(location.span, &replacement);
            cursor =
                checked_usize_add(start, replacement.len(), "conditionalFormatting 다음 위치")?;
        }
        if find_start_tag_location(&self.suffix, "conditionalFormatting", cursor)?.is_some() {
            return Err(err(
                "Excel conditionalFormatting 정규형의 항목 수가 예상보다 많습니다.",
            ));
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
        shared_strings: &SharedStringTable,
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
        sheet: ExcelSheetKind,
        shared_strings: &SharedStringTable,
        input_styles: &CanonicalStyleMap,
    ) -> Result<WorksheetSemanticFacts> {
        let (sheet_name, layout, last_col) = match sheet {
            ExcelSheetKind::ChangeLog => (
                CHANGE_LOG_SHEET_NAME,
                CHANGE_LOG_FORMULA_LAYOUT,
                CHANGE_LOG_LAST_COL,
            ),
            ExcelSheetKind::Master => (MASTER_SHEET_NAME, MASTER_FORMULA_LAYOUT, MASTER_LAST_COL),
        };
        self.validate_columns(sheet, input_styles)?;
        let mut actual_bounds = None;
        let mut facts = WorksheetSemanticFacts {
            formula_count: 0,
            last_data_row: None,
            last_master_address_row: None,
            shared_ref_count: 0,
        };
        let mut first_layout_issue = None;
        let mut fixed_formula_count = 0_usize;
        for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
            let mut required_formula_index = 0_usize;
            let mut optional_formula = false;
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
                let shared_string = matches!(cell.value_type, CellValueType::SharedString(_));
                let master_address = sheet == ExcelSheetKind::Master && col == MASTER_ADDRESS_COL;
                if shared_string || master_address {
                    let display = self.try_get_display_at(col, row_num, shared_strings)?;
                    if master_address && !display.trim().is_empty() {
                        facts.last_master_address_row = Some(row_num);
                    }
                }
                if shared_string {
                    facts.shared_ref_count = facts.shared_ref_count.strict_add(1);
                }
                if let Some(inner) = cell.inner_xml.as_deref()
                    && let Some(raw_formula) = extract_first_tag_text(inner, "f")?
                {
                    facts.formula_count = facts.formula_count.strict_add(1);
                    let formula = decode_xml_entities(raw_formula)?;
                    if formula.contains("#REF!") {
                        return Err(err(format!(
                            "worksheet에 #REF! 수식이 있습니다: {sheet_name}!row={row_num}, col={col}"
                        )));
                    }
                    let fixed = layout
                        .fixed_formulas
                        .iter()
                        .find(|&&(fixed_col, fixed_row, _)| {
                            (col, row_num) == (fixed_col, fixed_row)
                        });
                    if let Some(&(_, _, expected)) = fixed {
                        if formula.as_ref() != expected {
                            return Err(err(format!(
                                "{sheet_name} 시트의 고정 formula가 다릅니다: row={row_num}, col={col}"
                            )));
                        }
                        fixed_formula_count = fixed_formula_count.strict_add(1);
                    }
                    let data = row_num >= layout.data_start_row
                        && (layout.required_cols.contains(&col)
                            || layout.optional_zero_col == Some(col));
                    if fixed.is_none() && !data {
                        return Err(err(format!(
                            "{sheet_name} 시트의 고정 위치 밖에 formula가 있습니다: row={row_num}, col={col}"
                        )));
                    }
                    if data {
                        if layout.required_cols.get(required_formula_index) == Some(&col) {
                            required_formula_index = required_formula_index.strict_add(1);
                        }
                        if layout.optional_zero_col == Some(col) {
                            optional_formula = true;
                        }
                    }
                }
                if row_num >= layout.data_start_row && col <= last_col && cell_has_payload(cell)? {
                    facts.last_data_row = Some(row_num);
                }
            }
            if row_num >= layout.data_start_row && first_layout_issue.is_none() {
                if let Some(&col) = layout.required_cols.get(required_formula_index) {
                    first_layout_issue = Some((row_num, col, "필수 formula가 없습니다"));
                }
                if first_layout_issue.is_none()
                    && let Some(col) = layout.optional_zero_col
                    && !optional_formula
                    && self.get_i32_at(col, row_num, shared_strings)? != Some(0_i32)
                {
                    first_layout_issue =
                        Some((row_num, col, "선택 formula 위치는 수동 0이어야 합니다"));
                }
            }
        }
        let bounds = actual_bounds
            .ok_or_else(|| err(format!("{sheet_name} worksheet에 cell이 없습니다.")))?;
        let mut scanner = XmlScanner::new(&self.prefix);
        let dimension = scanner
            .next_start_named("dimension")
            .ok_or_else(|| err(format!("{sheet_name} worksheet에 dimension이 없습니다.")))?;
        let declared = extract_attr(dimension.raw, "ref")?
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
        if fixed_formula_count != layout.fixed_formulas.len() {
            return Err(err(format!(
                "{sheet_name} 시트의 고정 formula 수가 다릅니다: expected={}, actual={fixed_formula_count}",
                layout.fixed_formulas.len()
            )));
        }
        if let (Some(last_data_row), Some((row, col, detail))) =
            (facts.last_data_row, first_layout_issue)
            && row <= last_data_row
        {
            return Err(err(format!(
                "{sheet_name} 시트의 {detail}: row={row}, col={col}"
            )));
        }
        Ok(facts)
    }
    pub(crate) fn set_formula_at_with_cache(
        &mut self,
        col: u32,
        row: u32,
        formula: &str,
        value: Option<&str>,
        string_value: bool,
    ) -> Result<()> {
        let formula_text =
            try_xml_escape_text(formula, XmlEscapeContext::Text, "formula XML escape")?;
        let value_text = value
            .map(|raw| try_xml_escape_text(raw, XmlEscapeContext::Text, "formula cache XML escape"))
            .transpose()?;
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        cell.value_type = if string_value || value.is_none() {
            CellValueType::String
        } else {
            CellValueType::General
        };
        let cached = value_text.as_deref().unwrap_or("");
        if let Some(inner) = cell.inner_xml.as_mut()
            && find_start_tag(inner, "f", 0).is_some()
        {
            replace_first_tag_text(inner, "f", &formula_text)?;
            replace_first_tag_text(inner, "v", cached)?;
        } else {
            let capacity = checked_capacity(&[
                "<f></f>".len(),
                if cached.is_empty() {
                    "<v/>".len()
                } else {
                    "<v></v>".len()
                },
                formula_text.len(),
                cached.len(),
            ])
            .ok_or_else(|| err("formula/cache XML 용량 계산 실패"))?;
            let mut inner =
                try_string_with_capacity(capacity, "formula/cache XML 메모리 확보 실패")?;
            inner.push_str("<f>");
            inner.push_str(&formula_text);
            inner.push_str("</f>");
            if cached.is_empty() {
                inner.push_str("<v/>");
            } else {
                inner.push_str("<v>");
                inner.push_str(cached);
                inner.push_str("</v>");
            }
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
        cell.value_type = if string_value || value.is_none() {
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
            let mut inner = String::new();
            inner.push_str("<v>");
            if numeric_value.is_negative() {
                inner.push('-');
            }
            push_decimal_text(&mut inner, numeric_value.unsigned_abs());
            inner.push_str("</v>");
            cell.inner_xml = Some(inner);
        } else {
            cell.inner_xml = None;
        }
        Ok(())
    }
    pub(crate) fn take_rows(&mut self) -> Vec<Row> {
        mem::take(&mut self.rows)
    }
    fn to_xml(&self) -> Result<(String, usize, usize)> {
        let mut formula_count = 0_usize;
        let mut shared_string_reference_count = 0_usize;
        let estimated_capacity = (|| {
            let cell_markup_len = checked_capacity(&["< r=\"\"></>".len(), "c".len(), "c".len()])?;
            let row_markup_len =
                checked_capacity(&["< r=\"\"></>".len(), "row".len(), "row".len()])?;
            let mut capacity = checked_capacity(&[self.prefix.len(), self.suffix.len()])?;
            for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
                capacity =
                    checked_capacity(&[capacity, row_markup_len, u32_decimal_text_len(row_num)])?;
                capacity = capacity.checked_add(row.attrs_xml.len())?;
                for cell in &row.cells {
                    let col = cell.col;
                    if let CellValueType::SharedString(index) = cell.value_type {
                        shared_string_reference_count = shared_string_reference_count.strict_add(1);
                        capacity = checked_capacity(&[
                            capacity,
                            "<v></v>".len(),
                            u32_decimal_text_len(u32::try_from(index).ok()?),
                        ])?;
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
                        formula_count = formula_count
                            .strict_add(usize::from(find_start_tag(inner, "f", 0).is_some()));
                        capacity = capacity.checked_add(inner.len())?;
                    }
                }
            }
            Some(capacity)
        })();
        let capacity = estimated_capacity.ok_or_else(|| err("worksheet XML 용량 계산 실패"))?;
        let mut out = try_string_with_capacity(capacity, "worksheet XML 메모리 확보 실패")?;
        out.push_str(&self.prefix);
        for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
            out.push_str("<row");
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
                out.push_str("<c");
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
                if let CellValueType::SharedString(index) = cell.value_type {
                    out.push_str("><v>");
                    push_decimal_text(&mut out, index);
                    out.push_str("</v>");
                    out.push_str("</c>");
                } else if let Some(inner) = cell.inner_xml.as_ref() {
                    out.push('>');
                    out.push_str(inner);
                    out.push_str("</c>");
                } else {
                    out.push_str("/>");
                }
            }
            out.push_str("</row>");
        }
        out.push_str(&self.suffix);
        Ok((out, shared_string_reference_count, formula_count))
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
        shared_strings: &'text SharedStringTable,
    ) -> Result<Cow<'text, str>> {
        let Some(cell) = self.cell_at(col, row) else {
            return Ok(Cow::Borrowed(""));
        };
        if let CellValueType::SharedString(index) = cell.value_type {
            return shared_strings
                .get(index)
                .map(Cow::Borrowed)
                .ok_or_else(|| err(format!("shared string index 범위 오류: {index}")));
        }
        let inner = cell.inner_xml.as_deref().unwrap_or("");
        let raw_v = extract_first_tag_text(inner, "v")?.unwrap_or("");
        decode_xml_entities(raw_v)
    }
    pub(crate) fn try_get_fixed_text_at<'strings>(
        &self,
        col: u32,
        row: u32,
        shared_strings: &'strings SharedStringTable,
    ) -> Result<&'strings str> {
        let Some(cell) = self.cell_at(col, row) else {
            return Ok("");
        };
        if let CellValueType::SharedString(index) = cell.value_type {
            return shared_strings
                .get(index)
                .ok_or_else(|| err(format!("shared string index 범위 오류: {index}")));
        }
        if cell.inner_xml.is_none() {
            return Ok("");
        }
        Err(err(format!(
            "고정 workbook의 텍스트 cell이 shared string이 아닙니다: row={row}, col={col}"
        )))
    }
    pub(crate) fn try_get_formula_at(&self, col: u32, row: u32) -> Result<Option<Cow<'_, str>>> {
        let Some(inner) = self
            .cell_at(col, row)
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
        let out = &mut self.suffix;
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
    fn update_master_sort_references(&mut self) -> Result<()> {
        let last_row = u32::try_from(self.rows.len())
            .map_err(|source| err_with_source("유류비 정렬 범위 row 변환 실패", source))?;
        let sort_state_ref = build_ref_range(
            "A",
            RangeInclusive {
                start: 15,
                last: last_row,
            },
            23,
        )?;
        let sort_condition_ref = build_ref_range(
            "A",
            RangeInclusive {
                start: 15,
                last: last_row,
            },
            1,
        )?;
        replace_single_ref_attr(&mut self.suffix, "sortState", &sort_state_ref)?;
        replace_single_ref_attr(&mut self.suffix, "sortCondition", &sort_condition_ref)
    }
    fn validate_change_log_formats(&self, expected_last_row: u32) -> Result<()> {
        let mut delta_mask = 0_u8;
        let mut scanner = XmlScanner::new(&self.suffix);
        while let Some(formatting) = scanner.next_start_named("conditionalFormatting") {
            let Some(sqref) = extract_attr(formatting.raw, "sqref")? else {
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
    fn validate_columns(
        &self,
        sheet: ExcelSheetKind,
        input_styles: &CanonicalStyleMap,
    ) -> Result<()> {
        let (sheet_name, last_col) = match sheet {
            ExcelSheetKind::ChangeLog => (CHANGE_LOG_SHEET_NAME, CHANGE_LOG_LAST_COL),
            ExcelSheetKind::Master => (MASTER_SHEET_NAME, MASTER_LAST_COL),
        };
        let mut scanner = XmlScanner::new(&self.prefix);
        while let Some(column) = scanner.next_start_named("col") {
            let min_text = extract_attr(column.raw, "min")?
                .ok_or_else(|| err(format!("{sheet_name} col min 속성이 없습니다.")))?;
            let max_text = extract_attr(column.raw, "max")?
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
            let Some(style_text) = extract_attr(column.raw, "style")? else {
                continue;
            };
            let style = parse_u32_decimal(
                style_text.as_ref(),
                || Cow::Borrowed("worksheet col style이 10진수가 아닙니다."),
                || Cow::Borrowed("worksheet col style 해석 실패"),
            )?;
            canonical_excel_style(style, sheet, 0, None, input_styles)?;
        }
        Ok(())
    }
    fn validate_fixed_header(
        &self,
        sheet: ExcelSheetKind,
        shared_strings: &SharedStringTable,
    ) -> Result<()> {
        let (sheet_name, header_row, headers, last_col): (&str, u32, &[&str], u32) = match sheet {
            ExcelSheetKind::Master => (MASTER_SHEET_NAME, 14, &MASTER_HEADERS, 23),
            ExcelSheetKind::ChangeLog => (CHANGE_LOG_SHEET_NAME, 3, &CHANGE_LOG_HEADERS, 13),
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
        if sheet == ExcelSheetKind::Master {
            self.fixed_master_auto_filter()?;
        }
        Ok(())
    }
}
impl Row {
    fn cell(&self, col: u32) -> Option<&Cell> {
        let index = self
            .cells
            .binary_search_by_key(&col, |cell| cell.col)
            .ok()?;
        self.cells.get(index)
    }
    pub(crate) fn try_copy(&self) -> Result<Self> {
        let mut cells =
            try_vec_with_capacity(self.cells.len(), "row cell 목록 복사 메모리 확보 실패")?;
        for cell in &self.cells {
            cells.push(Cell {
                col: cell.col,
                inner_xml: cell.inner_xml.as_deref().map(copy_text).transpose()?,
                style: cell.style,
                value_type: cell.value_type,
            });
        }
        Ok(Self {
            attrs_xml: copy_text(&self.attrs_xml)?,
            cells,
        })
    }
}
fn canonical_excel_fragment(source: &str) -> Result<String> {
    let trimmed = source.strip_suffix('\n').unwrap_or(source);
    let mut output = copy_text(trimmed)?;
    if let Some(line_break) = output.find('\n') {
        output.replace_range(line_break..=line_break, "\r\n");
    }
    Ok(output)
}
fn canonical_excel_style(
    style: u32,
    sheet: ExcelSheetKind,
    row: u32,
    col: Option<u32>,
    input_styles: &CanonicalStyleMap,
) -> Result<u32> {
    let canonical = input_styles.get(style).ok_or_else(|| {
        err(format!(
            "worksheet가 참조한 style을 Excel 정규형으로 변환할 수 없습니다: {style}"
        ))
    })?;
    if sheet == ExcelSheetKind::Master && row == 3 && col.is_some_and(|column| column <= 2) {
        return Ok(26);
    }
    Ok(canonical)
}
fn xml_bool_attr(attrs: &[XmlAttr<'_>], name: &str) -> Result<bool> {
    match get_attr(attrs, name) {
        None | Some("0" | "false") => Ok(false),
        Some("1" | "true") => Ok(true),
        Some(value) => Err(err(format!(
            "worksheet {name} 속성이 boolean 형식이 아닙니다: {value}"
        ))),
    }
}
pub(crate) fn format_excel_ratio_into(
    out: &mut String,
    numerator: i128,
    denominator: i128,
) -> Result<()> {
    if denominator == 0 {
        return Err(err("Excel 숫자 cache 분모가 0입니다."));
    }
    let negative = numerator != 0 && numerator.is_negative() != denominator.is_negative();
    let denominator_abs = denominator.unsigned_abs();
    let numerator_abs = numerator.unsigned_abs();
    let whole = numerator_abs.div_euclid(denominator_abs);
    let mut remainder = numerator_abs.rem_euclid(denominator_abs);
    out.clear();
    if negative {
        out.push('-');
    }
    push_decimal_text(out, whole);
    if remainder != 0 {
        out.push('.');
        for _ in 0_u32..32_u32 {
            remainder = remainder
                .checked_mul(10)
                .ok_or_else(|| err("Excel 숫자 cache 소수부 계산 중 overflow가 발생했습니다."))?;
            let [digit, ..] = remainder.div_euclid(denominator_abs).to_le_bytes();
            out.push(char::from(b'0'.strict_add(digit)));
            remainder = remainder.rem_euclid(denominator_abs);
            if remainder == 0 {
                break;
            }
        }
    }
    if remainder == 0 {
        return Ok(());
    }
    let value = out
        .parse::<f64>()
        .map_err(|source| err_with_source("Excel 숫자 cache 해석 실패", source))?;
    if !value.is_finite() {
        return Err(err("Excel 숫자 cache가 유한한 값이 아닙니다."));
    }
    out.clear();
    append_fmt(out, format_args!("{value:.16e}"));
    let exponent_marker = out
        .rfind('e')
        .ok_or_else(|| err("Excel 숫자 cache 지수 표기가 손상되었습니다."))?;
    let exponent_start = exponent_marker.strict_add(1);
    let exponent_text = out
        .get(exponent_start..)
        .ok_or_else(|| err("Excel 숫자 cache 지수 범위가 손상되었습니다."))?;
    let exponent = exponent_text
        .parse::<i32>()
        .map_err(|source| err_with_source("Excel 숫자 cache 지수 해석 실패", source))?;
    let sign_len = usize::from(out.starts_with('-'));
    let mantissa = out
        .get(sign_len..exponent_marker)
        .ok_or_else(|| err("Excel 숫자 cache 가수 범위가 손상되었습니다."))?;
    let point = mantissa
        .find('.')
        .map(|index| sign_len.strict_add(index))
        .ok_or_else(|| err("Excel 숫자 cache 가수 소수점이 없습니다."))?;
    let zero = mantissa.bytes().all(|byte| matches!(byte, b'.' | b'0'));
    let decimal_position = exponent
        .checked_add(1)
        .ok_or_else(|| err("Excel 숫자 cache 소수점 위치 계산 실패"))?;
    out.truncate(exponent_marker);
    out.remove(point);
    if zero {
        out.clear();
        out.push('0');
        return Ok(());
    }
    let digit_count = out.len().strict_sub(sign_len);
    if decimal_position <= 0_i32 {
        out.insert_str(sign_len, "0.");
        let zero_position = sign_len.strict_add(2);
        for _ in 0..decimal_position.unsigned_abs() {
            out.insert(zero_position, '0');
        }
    } else {
        let position = usize::try_from(decimal_position)
            .map_err(|source| err_with_source("Excel 숫자 cache 소수점 위치 변환 실패", source))?;
        if position >= digit_count {
            for _ in digit_count..position {
                out.push('0');
            }
        } else {
            out.insert(sign_len.strict_add(position), '.');
        }
    }
    if out.contains('.') {
        out.truncate(out.trim_end_matches('0').len());
        if out.ends_with('.') {
            out.pop();
        }
    }
    Ok(())
}
fn replace_single_ref_attr(xml: &mut String, name: &str, reference: &str) -> Result<()> {
    let location = find_start_tag_location(xml, name, 0)?
        .ok_or_else(|| err(format!("Excel worksheet의 {name} 태그가 없습니다.")))?;
    let mut attrs = parse_tag_attrs_at(
        xml,
        &location,
        "Excel worksheet reference 태그 범위가 손상되었습니다.",
    )?;
    set_attr(&mut attrs, "ref", reference);
    let replacement = if location.self_closing {
        build_self_closing_tag(name, &attrs)?
    } else {
        build_open_tag(name, &attrs)?
    };
    let next = checked_usize_add(
        location.span.start,
        replacement.len(),
        "Excel worksheet reference 다음 위치",
    )?;
    xml.replace_range(location.span, &replacement);
    if find_start_tag_location(xml, name, next)?.is_some() {
        return Err(err(format!(
            "Excel worksheet에 {name} 태그가 여러 개 있습니다."
        )));
    }
    Ok(())
}
fn replace_single_xml_element(xml: &mut String, name: &str, replacement: &str) -> Result<()> {
    let mut scanner = XmlScanner::new(xml);
    let element = scanner
        .next_element_named(name)?
        .ok_or_else(|| err(format!("XML의 {name} 요소를 찾지 못했습니다.")))?;
    if scanner.next_start_named(name).is_some() {
        return Err(err(format!("XML에 {name} 요소가 여러 개 있습니다.")));
    }
    xml.replace_range(element.span, replacement);
    Ok(())
}
fn append_calc_chain_cell(
    out: &mut String,
    cell: &Cell,
    row: u32,
    sheet_id: u8,
    first: &mut bool,
) -> Result<()> {
    if cell
        .inner_xml
        .as_deref()
        .is_none_or(|inner| find_start_tag(inner, "f", 0).is_none())
    {
        return Ok(());
    }
    out.push_str("<c r=\"");
    with_unlocked_ref_parts(cell.col, row, |col_name, row_number| {
        out.push_str(col_name);
        push_decimal_text(out, row_number);
    })?;
    out.push_str("\" i=\"");
    push_decimal_text(out, sheet_id);
    if mem::replace(first, false) {
        out.push_str("\" l=\"1");
    }
    out.push_str("\"/>");
    Ok(())
}
fn replace_formula_tag_at(
    inner_xml: &str,
    formula_span: Range<usize>,
    tag: FormulaTag<'_>,
) -> Result<String> {
    let prefix = inner_xml
        .get(..formula_span.start)
        .ok_or_else(|| err("cell formula prefix 범위가 손상되었습니다."))?;
    let suffix = inner_xml
        .get(formula_span.end..)
        .ok_or_else(|| err("cell formula suffix 범위가 손상되었습니다."))?;
    let mut replacement = String::new();
    match tag {
        FormulaTag::Plain(formula) => {
            let escaped =
                try_xml_escape_text(formula, XmlEscapeContext::Text, "cell formula XML escape")?;
            replacement
                .try_reserve_exact(
                    escaped
                        .len()
                        .checked_add("<f></f>".len())
                        .ok_or_else(|| err("cell formula XML 용량 계산 실패"))?,
                )
                .map_err(|source| err_with_source("cell formula XML 메모리 확보 실패", source))?;
            replacement.push_str("<f>");
            replacement.push_str(&escaped);
            replacement.push_str("</f>");
        }
        FormulaTag::SharedFollower(si) => {
            replacement.push_str("<f t=\"shared\" si=\"");
            push_decimal_text(&mut replacement, si);
            replacement.push_str("\"/>");
        }
        FormulaTag::SharedRoot {
            formula,
            reference,
            si,
        } => {
            let escaped =
                try_xml_escape_text(formula, XmlEscapeContext::Text, "shared formula XML escape")?;
            replacement.push_str("<f t=\"shared\" ref=\"");
            replacement.push_str(reference);
            replacement.push_str("\" si=\"");
            push_decimal_text(&mut replacement, si);
            replacement.push_str("\">");
            replacement.push_str(&escaped);
            replacement.push_str("</f>");
        }
    }
    let capacity = checked_capacity(&[prefix.len(), replacement.len(), suffix.len()])
        .ok_or_else(|| err("cell formula replacement 용량 계산 실패"))?;
    let mut output =
        try_string_with_capacity(capacity, "cell formula replacement 메모리 확보 실패")?;
    output.push_str(prefix);
    output.push_str(&replacement);
    output.push_str(suffix);
    Ok(output)
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
    let element = scanner
        .next_element_named("definedName")?
        .ok_or_else(|| err("workbook.xml의 _FilterDatabase를 찾지 못했습니다."))?;
    let tag = element.opening;
    if tag.name != "definedName" || tag.self_closing {
        return Err(err(
            "workbook.xml의 _FilterDatabase 태그 형식이 고정 스키마와 다릅니다.",
        ));
    }
    let attrs = parse_tag_attrs(tag.raw)?;
    if attrs.len() != 3
        || get_attr(&attrs, "hidden") != Some("1")
        || get_attr(&attrs, "localSheetId") != Some("0")
        || get_attr(&attrs, "name") != Some(FILTER_DATABASE_NAME)
    {
        return Err(err(
            "workbook.xml의 _FilterDatabase 속성이 고정 스키마와 다릅니다.",
        ));
    }
    let row_text = element
        .body
        .strip_prefix(FILTER_DATABASE_REF_PREFIX)
        .filter(|row| !row.is_empty() && row.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| err("_FilterDatabase가 고정 유류비 범위와 다릅니다."))?;
    let last_row = row_text
        .parse::<u32>()
        .map_err(|source| err_with_source("_FilterDatabase 마지막 행 해석 실패", source))?;
    let row_start = element
        .body_span
        .start
        .checked_add(FILTER_DATABASE_REF_PREFIX.len())
        .ok_or_else(|| err("_FilterDatabase 마지막 행 시작 계산 실패"))?;
    if scanner.next_start_named("definedName").is_some() {
        return Err(err("workbook.xml에 고정 스키마 외 definedName이 있습니다."));
    }
    Ok((
        Range {
            start: row_start,
            end: element.body_span.end,
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
        .map_or(1, |log| usize::from(log.to_le_bytes()[0]).strict_add(1))
}
fn push_decimal_text(out: &mut String, value: impl Display) {
    append_fmt(out, format_args!("{value}"));
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
    let tag_end = checked_usize_add(tag.end, 1, "XML 시작 태그 끝")?;
    Ok(Some(XmlTagLocation {
        self_closing: tag.self_closing,
        span: Range {
            start: tag.start,
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
    let mut scanner = XmlScanner::new(xml);
    scanner.skip_to(location.span.start);
    let element = scanner
        .next_element_named(local_name)?
        .filter(|element| element.opening.start == location.span.start)
        .ok_or_else(|| err(format!("{context} 요소를 찾지 못했습니다.")))?;
    if !element.body.trim().is_empty() {
        return Err(err(format!("{context}에 예상하지 않은 본문이 있습니다.")));
    }
    Ok(element.span)
}
fn push_attr_xml(out: &mut String, attr: &XmlAttr<'_>) {
    let name = &attr.name;
    out.push(' ');
    out.push_str(name);
    out.push_str("=\"");
    append_xml_escaped(out, &attr.value, XmlEscapeContext::Attribute);
    out.push('"');
}
fn parse_tag_attrs_at<'xml>(
    xml: &'xml str,
    location: &XmlTagLocation,
    invalid_range_message: &'static str,
) -> Result<Vec<XmlAttr<'xml>>> {
    let tag = xml
        .get(location.span)
        .ok_or_else(|| err(invalid_range_message))?;
    parse_tag_attrs(tag)
}
fn parse_tag_attrs(tag: &str) -> Result<Vec<XmlAttr<'_>>> {
    let mut out: Vec<XmlAttr<'_>> = try_vec_with_capacity(4, "XML 속성 목록 메모리 확보 실패")?;
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
            name: Cow::Borrowed(name),
            value,
        });
    }
    Ok(out)
}
fn reserve_xml_attrs(
    attrs: &mut Vec<XmlAttr<'_>>,
    additional: usize,
    context: &'static str,
) -> Result<()> {
    attrs
        .try_reserve(additional)
        .map_err(|source| err_with_source(context, source))
}
fn get_attr<'attrs>(attrs: &'attrs [XmlAttr<'_>], name: &str) -> Option<&'attrs str> {
    attrs
        .iter()
        .find(|attr| attr.name == name)
        .map(|attr| attr.value.as_ref())
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
    format_error: impl FnOnce() -> Cow<'static, str>,
    parse_context: impl FnOnce() -> Cow<'static, str>,
) -> Result<u32> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(err(format_error()));
    }
    value
        .parse::<u32>()
        .map_err(|source| err_with_source(parse_context(), source))
}
fn parse_positive_u32_decimal(
    value: &str,
    format_error: &'static str,
    parse_context: &'static str,
    zero_error: &'static str,
) -> Result<u32> {
    let parsed = parse_u32_decimal(
        value,
        || Cow::Borrowed(format_error),
        || Cow::Borrowed(parse_context),
    )?;
    if parsed == 0 {
        return Err(err(zero_error));
    }
    Ok(parsed)
}
fn set_attr<'text>(
    attrs: &mut Vec<XmlAttr<'text>>,
    name: &'static str,
    value_in: impl Into<Cow<'text, str>>,
) {
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
fn remove_attr(attrs: &mut Vec<XmlAttr<'_>>, name: &str) {
    attrs.retain(|attr| attr.name != name);
}
fn replace_first_tag_text(xml: &mut String, tag_name: &str, new_text: &str) -> Result<()> {
    let mut scanner = XmlScanner::new(xml);
    let Some(element) = scanner.next_element_named(tag_name)? else {
        return Err(err(tag_error_message(tag_name, " 태그를 찾지 못했습니다.")));
    };
    let trimmed_open_tag = element.opening.raw.trim_ascii_end();
    if new_text.is_empty() {
        if element.opening.self_closing {
            return Ok(());
        }
        let prefix = trimmed_open_tag
            .strip_suffix('>')
            .ok_or_else(|| err(tag_error_message(tag_name, " 시작 태그 파싱 실패")))?;
        let mut replacement = copy_text(prefix)?;
        replacement.push_str("/>");
        xml.replace_range(element.span, &replacement);
        return Ok(());
    }
    if element.opening.self_closing {
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
        let mut replacement =
            try_string_with_capacity(capacity, "XML self-closing 치환 메모리 확보 실패")?;
        replacement.push_str(prefix);
        replacement.push('>');
        replacement.push_str(new_text);
        replacement.push_str("</");
        replacement.push_str(tag_name);
        replacement.push('>');
        xml.replace_range(element.span, &replacement);
        return Ok(());
    }
    xml.replace_range(element.body_span, new_text);
    Ok(())
}
fn try_xml_escape_text<'text>(
    text: &'text str,
    context: XmlEscapeContext,
    error_context: &'static str,
) -> Result<Cow<'text, str>> {
    let capacity = validated_xml_escaped_len(text, context, error_context)?;
    if capacity == text.len() {
        return Ok(Cow::Borrowed(text));
    }
    let mut out = try_string_with_capacity(capacity, "XML escape 메모리 확보 실패")?;
    append_xml_escaped(&mut out, text, context);
    Ok(Cow::Owned(out))
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
fn build_open_tag(name: &str, attrs: &[XmlAttr<'_>]) -> Result<String> {
    build_tag(name, attrs, false)
}
fn build_self_closing_tag(name: &str, attrs: &[XmlAttr<'_>]) -> Result<String> {
    build_tag(name, attrs, true)
}
fn build_tag(name: &str, attrs: &[XmlAttr<'_>], self_closing: bool) -> Result<String> {
    let kind = if self_closing {
        "self-closing"
    } else {
        "시작"
    };
    let suffix = if self_closing { "/>" } else { ">" };
    let mut capacity = checked_capacity(&["<".len(), name.len(), suffix.len()])
        .ok_or_else(|| err(format!("XML {kind} 태그 용량 계산 실패")))?;
    for attr in attrs {
        capacity = checked_capacity(&[capacity, " =\"\"".len(), attr.name.len(), attr.value.len()])
            .ok_or_else(|| err(format!("XML {kind} 태그 속성 용량 계산 실패")))?;
    }
    let mut out = try_string_with_capacity(capacity, "XML 태그 메모리 확보 실패")?;
    out.push('<');
    out.push_str(name);
    for attr in attrs {
        push_attr_xml(&mut out, attr);
    }
    out.push_str(suffix);
    Ok(out)
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
    let mut out = try_string_with_capacity(capacity, "cell range reference 메모리 확보 실패")?;
    out.push_str(start_col_text);
    push_decimal_text(&mut out, rows.start);
    out.push(':');
    out.push_str(&end_ref);
    Ok(out)
}
fn row_col_error(prefix: &str, row_num: u32, col: u32) -> String {
    format!("{prefix}{row_num}, col={col})")
}
fn tag_error_message(tag_name: &str, suffix: &str) -> String {
    format!("{tag_name}{suffix}")
}
