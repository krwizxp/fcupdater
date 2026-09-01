use self::cell_ref::{MAX_A1_COL, MAX_A1_ROW, parse_ref_with_locks, with_unlocked_ref_parts};
use super::{
    CHANGE_LOG_SHEET_NAME, CHANGE_LOG_SHEET_PATH, CanonicalStyleMap, FILTER_DATABASE_REF_PREFIX,
    MASTER_SHEET_NAME, MASTER_SHEET_PATH, SPREADSHEETML_NAMESPACE, SaveVerification, copy_text,
    xlsx_container::{XlsxContainer, validate_spreadsheet_xml_document},
    xml::{
        XmlAttrScanner, XmlScanner, decode_xml_entities, extract_first_tag_text, is_valid_xml_char,
    },
};
use crate::{
    diagnostic::{
        Result, append_fmt, err, err_with_source, try_string_with_capacity, try_vec_with_capacity,
    },
    u32_to_usize,
};
use alloc::{
    borrow::Cow,
    collections::{BTreeMap, btree_map::Entry},
    rc::Rc,
};
use core::{
    fmt::NumBuffer,
    mem,
    num::IntErrorKind,
    range::{Range, RangeInclusive},
};
use std::{collections::HashMap, path::Path, process};
mod cell_ref;
macro_rules! push_decimal_text {
    ($out:expr, $value:expr) => {
        ($out).push_str(($value).format_into(&mut NumBuffer::new()));
    };
}
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
    change_log_sheet: Worksheet,
    container: XlsxContainer,
    master_sheet: Worksheet,
    shared_strings: SharedStringTable,
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
    rows: Vec<Row>,
}
#[derive(Default)]
pub(crate) struct Row {
    attrs_xml: String,
    cells: Vec<Cell>,
}
struct Cell {
    col: u32,
    inner_xml: String,
    style: Option<u32>,
    value_type: CellValueType,
}
struct SharedFormulaHead {
    anchor_col: u32,
    anchor_row: u32,
    last_row: u32,
    seen: u32,
}
#[derive(Clone, Copy)]
enum FormulaTag<'text> {
    Plain(&'text str),
    SharedFollower(&'text str),
    SharedRoot {
        formula: &'text str,
        reference: &'text str,
        si: &'text str,
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
    SharedString(usize),
    String,
}
impl CellValueType {
    const fn xml_attr(self) -> Option<&'static str> {
        match self {
            Self::General => None,
            Self::SharedString(_) => Some("s"),
            Self::String => Some("str"),
        }
    }
}
struct XmlAttr<'text> {
    name: Cow<'text, str>,
    value: Cow<'text, str>,
}
struct WorksheetParser<'xml, 'styles> {
    cell_count: usize,
    input_styles: &'styles CanonicalStyleMap,
    shared_formula_heads: BTreeMap<u32, SharedFormulaHead>,
    sheet: ExcelSheetKind,
    xml: &'xml str,
}
#[derive(Clone, Copy)]
enum XmlEscapeContext {
    Attribute,
    Text,
}
#[derive(Clone, Copy, Eq, PartialEq)]
struct CellReference {
    pub col: u32,
    pub col_locked: bool,
    pub row: u32,
    pub row_locked: bool,
}
impl SharedStringTable {
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
        let escaped = try_xml_escape_text(value, XmlEscapeContext::Text, "shared string XML")?;
        let preserve = value.chars().next().is_some_and(char::is_whitespace)
            || value.chars().next_back().is_some_and(char::is_whitespace);
        let opening = if preserve {
            "<si><t xml:space=\"preserve\">"
        } else {
            "<si><t>"
        };
        let capacity = opening
            .len()
            .strict_add(escaped.len())
            .strict_add("</t></si>".len());
        let mut entry = try_string_with_capacity(capacity, "shared string XML 메모리 확보 실패")?;
        entry.push_str(opening);
        entry.push_str(&escaped);
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
        cell.inner_xml.clear();
        Ok(())
    }
    fn to_xml(&self, reference_count: usize) -> Result<String> {
        let additional_capacity = self
            .entries
            .iter()
            .fold("</sst>".len(), |sum, entry| sum.strict_add(entry.xml.len()));
        let mut xml = try_string_with_capacity(
            additional_capacity.strict_add(256),
            "sharedStrings XML 메모리 확보 실패",
        )?;
        append_fmt(
            &mut xml,
            format_args!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n<sst xmlns=\"{SPREADSHEETML_NAMESPACE}\" count=\"{reference_count}\" uniqueCount=\"{}\">",
                self.entries.len()
            ),
        );
        for entry in &self.entries {
            xml.push_str(&entry.xml);
        }
        xml.push_str("</sst>");
        Ok(xml)
    }
}
impl Workbook {
    pub(crate) const fn change_log_sheet_mut(
        &mut self,
    ) -> (&mut Worksheet, &mut SharedStringTable) {
        (&mut self.change_log_sheet, &mut self.shared_strings)
    }
    pub(crate) fn from_container(mut container: XlsxContainer) -> Result<Self> {
        container.ensure_supported_workbook()?;
        let master_xml = container.take_worksheet_text(MASTER_SHEET_PATH)?;
        let change_log_xml = container.take_worksheet_text(CHANGE_LOG_SHEET_PATH)?;
        let input_styles = container.package_prepare_excel_output()?;
        let shared_strings_xml_text = container.take_shared_strings_text()?;
        let mut shared_strings_scanner = XmlScanner::new(&shared_strings_xml_text);
        let sst = shared_strings_scanner
            .next_start_named("sst")
            .ok_or_else(|| err("sharedStrings XML에 <sst>가 없습니다."))?;
        let mut entries = try_vec_with_capacity(
            SHARED_STRING_INITIAL_CAPACITY,
            "shared string entry 메모리 확보 실패",
        )?;
        let sst_closing = (!sst.self_closing).then_some(sst.name);
        while let Some(closing_name) = sst_closing
            && let Some(si) = shared_strings_scanner.next_direct_element_named_until(
                "si",
                closing_name,
                1,
                "sharedStrings.xml의 sst",
            )?
        {
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
            let si_xml = shared_strings_xml_text
                .get(si.span)
                .ok_or_else(|| err("sharedStrings.xml의 si entry 범위가 손상되었습니다."))?;
            let mut xml =
                try_string_with_capacity(si_xml.len(), "shared string XML 메모리 확보 실패")?;
            let mut text_scanner = XmlScanner::new(si_xml);
            let mut source_cursor = 0_usize;
            let mut first_text: Option<Cow<'_, str>> = None;
            let mut text_out: Option<String> = None;
            while let Some(element) = text_scanner.next_element_named("t")? {
                let prefix = si_xml
                    .get(source_cursor..element.span.start)
                    .ok_or_else(|| err("shared string t 시작 범위가 손상되었습니다."))?;
                xml.push_str(prefix);
                let mut attrs = parse_tag_attrs(element.opening.raw)?;
                if element.opening.self_closing {
                    xml.push_str(&build_tag("t", &attrs, true)?);
                    source_cursor = element.span.end;
                    continue;
                }
                let decoded = decode_xml_entities(element.body)?;
                let preserve = decoded.chars().next().is_some_and(char::is_whitespace)
                    || decoded.chars().next_back().is_some_and(char::is_whitespace);
                if preserve {
                    if let Some(attr) = attrs.iter_mut().find(|attr| attr.name == "xml:space") {
                        attr.value = Cow::Borrowed("preserve");
                    } else {
                        attrs.try_reserve(1).map_err(|source| {
                            err_with_source("XML 속성 목록 추가 메모리 확보 실패", source)
                        })?;
                        attrs.push(XmlAttr {
                            name: Cow::Borrowed("xml:space"),
                            value: Cow::Borrowed("preserve"),
                        });
                    }
                } else {
                    attrs.retain(|attr| attr.name != "xml:space");
                }
                let opening = build_tag("t", &attrs, false)?;
                let escaped = try_xml_escape_text(
                    decoded.as_ref(),
                    XmlEscapeContext::Text,
                    "shared string XML",
                )?;
                xml.extend([opening.as_str(), escaped.as_ref(), "</t>"]);
                if !decoded.is_empty() {
                    if let Some(out_text) = text_out.as_mut() {
                        out_text
                            .try_reserve_exact(decoded.len())
                            .map_err(|source| {
                                err_with_source("XML tag text 메모리 확보 실패", source)
                            })?;
                        out_text.push_str(decoded.as_ref());
                    } else if let Some(previous) = first_text.take() {
                        let mut out_text = try_string_with_capacity(
                            previous.len().strict_add(decoded.len()),
                            "XML tag text 메모리 확보 실패",
                        )?;
                        out_text.extend([previous.as_ref(), decoded.as_ref()]);
                        text_out = Some(out_text);
                    } else {
                        first_text = Some(decoded);
                    }
                }
                source_cursor = element.span.end;
            }
            let suffix = si_xml
                .get(source_cursor..)
                .ok_or_else(|| err("shared string t 종료 범위가 손상되었습니다."))?;
            xml.push_str(suffix);
            let value = text_out
                .map(Cow::Owned)
                .or(first_text)
                .unwrap_or(Cow::Borrowed(""));
            entries.push(SharedStringEntry {
                text: Rc::<str>::from(value.as_ref()),
                xml,
            });
        }
        let mut index = HashMap::new();
        index.try_reserve(entries.len()).map_err(|source| {
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
        let master_sheet = WorksheetParser {
            cell_count: 0,
            input_styles: &input_styles,
            shared_formula_heads: BTreeMap::new(),
            sheet: ExcelSheetKind::Master,
            xml: &master_xml,
        }
        .scan_worksheet()?;
        master_sheet.validate_fixed_header(ExcelSheetKind::Master, &shared_strings)?;
        let change_log_sheet = WorksheetParser {
            cell_count: 0,
            input_styles: &input_styles,
            shared_formula_heads: BTreeMap::new(),
            sheet: ExcelSheetKind::ChangeLog,
            xml: &change_log_xml,
        }
        .scan_worksheet()?;
        change_log_sheet.validate_fixed_header(ExcelSheetKind::ChangeLog, &shared_strings)?;
        master_sheet.semantic_validation(ExcelSheetKind::Master, &shared_strings)?;
        change_log_sheet.semantic_validation(ExcelSheetKind::ChangeLog, &shared_strings)?;
        Ok(Self {
            change_log_sheet,
            container,
            master_sheet,
            shared_strings,
        })
    }
    pub(crate) const fn master_sheet_mut(&mut self) -> (&mut Worksheet, &mut SharedStringTable) {
        (&mut self.master_sheet, &mut self.shared_strings)
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
        push_decimal_text!(&mut cache, total_qty);
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
    pub(crate) fn save(
        mut self,
        target_path: &Path,
        verification: SaveVerification,
        filter_last_row: u32,
        change_log_last_row: u32,
    ) -> Result<()> {
        self.update_shared_string_catalog()?;
        self.request_recalculation_caches()?;
        self.master_sheet
            .canonical_share_formulas(ExcelSheetKind::Master)?;
        self.change_log_sheet
            .canonical_share_formulas(ExcelSheetKind::ChangeLog)?;
        self.master_sheet
            .validate_fixed_header(ExcelSheetKind::Master, &self.shared_strings)?;
        let (master_xml, master_shared_count) = self
            .master_sheet
            .to_xml(ExcelSheetKind::Master, filter_last_row)?;
        self.container.put_text(MASTER_SHEET_PATH, master_xml);
        drop(self.master_sheet.take_rows());
        self.change_log_sheet
            .validate_fixed_header(ExcelSheetKind::ChangeLog, &self.shared_strings)?;
        let (change_log_xml, change_log_shared_count) = self
            .change_log_sheet
            .to_xml(ExcelSheetKind::ChangeLog, change_log_last_row)?;
        self.container
            .put_text(CHANGE_LOG_SHEET_PATH, change_log_xml);
        drop(self.change_log_sheet.take_rows());
        let shared_string_reference_count = master_shared_count.strict_add(change_log_shared_count);
        let shared_strings_xml = self.shared_strings.to_xml(shared_string_reference_count)?;
        let capacity = EXCEL_WORKBOOK_OPENING
            .len()
            .strict_add(EXCEL_BOOK_VIEWS_XML.len())
            .strict_add(EXCEL_CALC_EXTENSIONS_XML.len())
            .strict_add(FILTER_DATABASE_REF_PREFIX.len())
            .strict_add(512);
        let mut workbook_xml =
            try_string_with_capacity(capacity, "Excel workbook XML 메모리 확보 실패")?;
        workbook_xml.push_str(EXCEL_WORKBOOK_OPENING);
        workbook_xml.push_str("<fileVersion appName=\"xl\" lastEdited=\"7\" lowestEdited=\"7\" rupBuild=\"27932\"/><workbookPr/>");
        workbook_xml.push_str(EXCEL_BOOK_VIEWS_XML);
        workbook_xml.push_str("<sheets><sheet name=\"유류비\" sheetId=\"1\" r:id=\"rId1\"/><sheet name=\"변경내역\" sheetId=\"2\" r:id=\"rId2\"/></sheets><definedNames><definedName name=\"_xlnm._FilterDatabase\" localSheetId=\"0\" hidden=\"1\">");
        workbook_xml.push_str(FILTER_DATABASE_REF_PREFIX);
        push_decimal_text!(&mut workbook_xml, filter_last_row);
        workbook_xml.push_str("</definedName></definedNames><calcPr calcId=\"191029\"/>");
        workbook_xml.extend([EXCEL_CALC_EXTENSIONS_XML, "</workbook>"]);
        self.container.put_text("xl/workbook.xml", workbook_xml);
        self.container
            .put_text("xl/sharedStrings.xml", shared_strings_xml);
        self.container.save(target_path, verification)
    }
    fn update_shared_string_catalog(&mut self) -> Result<()> {
        let string_count = self.shared_strings.entries.len();
        self.shared_strings.index = HashMap::new();
        let mut mapping =
            try_vec_with_capacity(string_count, "shared string index 변환표 메모리 확보 실패")?;
        mapping.resize(string_count, usize::MAX);
        self.master_sheet
            .canonical_mark_shared_strings(&mut mapping);
        self.change_log_sheet
            .canonical_mark_shared_strings(&mut mapping);
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
        self.master_sheet.canonical_remap_shared_strings(&mapping);
        self.change_log_sheet
            .canonical_remap_shared_strings(&mapping);
        Ok(())
    }
}
impl WorksheetParser<'_, '_> {
    fn parse_row(
        &mut self,
        scanner: &mut XmlScanner<'_>,
        row_name: &str,
        row_num: u32,
        row: &mut Row,
    ) -> Result<()> {
        let mut next_col = 1_u32;
        while let Some(cell) =
            scanner.next_direct_element_named_until("c", row_name, 2, "worksheet row")?
        {
            let inner_xml_text = cell.body;
            let cell_info = cell.opening;
            let mut attr_scanner = XmlAttrScanner::new(cell_info.raw)?;
            let mut reference_value = None;
            let mut style_value = None;
            let mut type_value = None;
            while let Some((name, value)) = attr_scanner.next()? {
                let slot = match name {
                    "r" => &mut reference_value,
                    "s" => &mut style_value,
                    "t" => &mut type_value,
                    _ => {
                        return Err(err(format!(
                            "고정 workbook cell에 지원하지 않는 속성이 있습니다: row={row_num}, attribute={name}"
                        )));
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
            let omit_empty_master_note_cell = self.sheet == ExcelSheetKind::Master
                && row_num == 2
                && col != 1
                && cell_info.self_closing;
            let style = (!omit_empty_master_note_cell)
                .then_some(style_value)
                .flatten()
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
                .transpose()?
                .map(|style| {
                    canonical_excel_style(style, self.sheet, row_num, Some(col), self.input_styles)
                })
                .transpose()?;
            let (value_type, shared_string) = if let Some(value) = type_value {
                match value.as_ref() {
                    "n" => (CellValueType::General, false),
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
                            inner_xml: String::new(),
                            style,
                            value_type,
                        },
                    )?;
                }
                next_col = col.strict_add(1);
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
                if value.body.starts_with('+') {
                    return Err(err(
                        "shared string index 해석 실패: 음이 아닌 10진수 형식이 아닙니다.",
                    ));
                }
                let index = value.body.parse::<usize>().map_err(|source| {
                    if matches!(
                        source.kind(),
                        IntErrorKind::Empty | IntErrorKind::InvalidDigit
                    ) {
                        err("shared string index 해석 실패: 음이 아닌 10진수 형식이 아닙니다.")
                    } else {
                        err_with_source("shared string index 해석 실패", source)
                    }
                })?;
                Some(Cell {
                    col,
                    inner_xml: String::new(),
                    style,
                    value_type: CellValueType::SharedString(index),
                })
            } else if has_attrs || !inner_xml_text.is_empty() {
                Some(Cell {
                    col,
                    inner_xml: copy_text(inner_xml_text)?,
                    style,
                    value_type,
                })
            } else {
                None
            };
            if let Some(parsed_cell) = retained_cell {
                self.retain_cell(row_num, row, parsed_cell)?;
            }
            next_col = col.strict_add(1);
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
        if !cell.inner_xml.is_empty() {
            let inner_xml = cell.inner_xml.as_str();
            let mut formula_scanner = XmlScanner::new(inner_xml);
            let mut formula_element = None;
            let mut has_cache = false;
            while let Some(child) = formula_scanner.next_direct_element("worksheet cell")? {
                if child.opening.raw.contains("xmlns") {
                    let mut attributes = XmlAttrScanner::new(child.opening.raw)?;
                    while let Some((name, _)) = attributes.next()? {
                        if name == "xmlns" || name.starts_with("xmlns:") {
                            return Err(err(format!(
                                "worksheet cell 자식의 namespace 재정의는 지원하지 않습니다: {name}"
                            )));
                        }
                    }
                }
                if child.body.contains('<') {
                    return Err(err(format!(
                        "worksheet cell 자식에 중첩 요소가 있습니다: row={row_num}, col={col}"
                    )));
                }
                match child.opening.name {
                    "f" if formula_element.is_none() && !has_cache => formula_element = Some(child),
                    "v" if !has_cache => has_cache = true,
                    "f" | "v" => {
                        return Err(err(format!(
                            "고정 workbook cell 자식 순서 또는 개수가 올바르지 않습니다: row={row_num}, col={col}"
                        )));
                    }
                    name => {
                        return Err(err(format!(
                            "고정 workbook cell에 지원하지 않는 자식 요소가 있습니다: row={row_num}, col={col}, element={name}"
                        )));
                    }
                }
            }
            if let Some(formula) = formula_element {
                let formula_span = formula.span;
                let decoded_formula = decode_xml_entities(formula.body)?;
                let mut aca = None;
                let mut formula_type = None;
                let mut reference = None;
                let mut shared_index = None;
                let mut attr_scanner = XmlAttrScanner::new(formula.opening.raw)?;
                while let Some((name, value)) = attr_scanner.next()? {
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
                if !has_cache {
                    return Err(err(format!(
                        "고정 workbook formula cache가 없습니다: row={row_num}, col={col}"
                    )));
                }
                if shared {
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
                            let (start_text, end_text) = reference_text
                                .split_once(':')
                                .unwrap_or((reference_text, reference_text));
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
                    let is_anchor = !decoded_formula.is_empty();
                    let head = if is_anchor {
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
                    head.seen = head.seen.strict_add(1);
                } else {
                    cell.inner_xml = replace_formula_tag_at(
                        inner_xml,
                        formula_span,
                        FormulaTag::Plain(decoded_formula.as_ref()),
                    )?;
                }
            }
        }
        row.cells.push(cell);
        self.cell_count = self.cell_count.strict_add(1);
        Ok(())
    }
    fn scan_rows(
        &mut self,
        scanner: &mut XmlScanner<'_>,
        sheet_data_name: &str,
    ) -> Result<Vec<Row>> {
        let mut rows = Vec::new();
        let last_col = match self.sheet {
            ExcelSheetKind::ChangeLog => CHANGE_LOG_LAST_COL,
            ExcelSheetKind::Master => MASTER_LAST_COL,
        };
        let mut last_col_buffer = NumBuffer::new();
        let last_col_text = last_col.format_into(&mut last_col_buffer);
        let mut style_buffer = NumBuffer::new();
        while let Some(row_info) = scanner.next_direct_opening_named_until(
            "row",
            sheet_data_name,
            "worksheet sheetData",
        )? {
            let row_attrs = parse_tag_attrs(row_info.raw)?;
            let row_num_text = get_attr(&row_attrs, "r")
                .ok_or_else(|| err("고정 workbook의 worksheet row에 r 속성이 없습니다."))?;
            let row_num = parse_u32_decimal(
                row_num_text,
                || Cow::Borrowed("worksheet row 번호가 양의 10진수 형식이 아닙니다."),
                || Cow::Borrowed("worksheet row 번호 해석 실패"),
            )?;
            if row_num == 0 {
                return Err(err("worksheet row 번호는 1 이상이어야 합니다."));
            }
            if !(1..=MAX_A1_ROW).contains(&row_num) {
                return Err(err(format!(
                    "worksheet row 번호가 Excel 범위를 벗어났습니다: {row_num}"
                )));
            }
            let expected_row_num = worksheet_row_count(rows.len()).strict_add(1);
            if row_num != expected_row_num {
                return Err(err(format!(
                    "worksheet row 번호는 1부터 연속이어야 합니다: expected={expected_row_num}, current={row_num}"
                )));
            }
            for attr in &row_attrs {
                if attr.name == "xmlns" || attr.name.starts_with("xmlns:") {
                    return Err(err(format!(
                        "worksheet row의 namespace 재정의는 지원하지 않습니다: {}",
                        attr.name
                    )));
                }
                validated_xml_escaped_len(
                    &attr.value,
                    XmlEscapeContext::Attribute,
                    "worksheet row 속성 직렬화",
                )?;
            }
            let style = get_attr(&row_attrs, "s")
                .map(|value| {
                    parse_u32_decimal(
                        value,
                        || Cow::Borrowed("row style이 10진수가 아닙니다."),
                        || Cow::Borrowed("row style 해석 실패"),
                    )
                })
                .transpose()?
                .map(|style| {
                    canonical_excel_style(style, self.sheet, row_num, None, self.input_styles)
                })
                .transpose()?;
            let custom_format = xml_bool_attr(&row_attrs, "customFormat")?;
            let input_custom_height = xml_bool_attr(&row_attrs, "customHeight")?;
            let input_height = get_attr(&row_attrs, "ht");
            let (height, custom_height) =
                if self.sheet == ExcelSheetKind::Master && matches!(row_num, 12 | 13) {
                    (Some("27"), true)
                } else {
                    (input_height, input_custom_height)
                };
            let mut attrs_xml = try_string_with_capacity(
                row_info.raw.len().strict_add(16),
                "worksheet row 속성 직렬화 메모리 확보 실패",
            )?;
            attrs_xml.extend([" spans=\"1:", last_col_text, "\""]);
            if let Some(style_id) = style {
                attrs_xml.extend([" s=\"", style_id.format_into(&mut style_buffer), "\""]);
            }
            if custom_format {
                attrs_xml.push_str(" customFormat=\"1\"");
            }
            if let Some(height_text) = height {
                attrs_xml.push_str(" ht=\"");
                append_xml_escaped(&mut attrs_xml, height_text, XmlEscapeContext::Attribute);
                attrs_xml.push('"');
            }
            if custom_height {
                attrs_xml.push_str(" customHeight=\"1\"");
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
            self.parse_row(scanner, row_info.name, row_num, &mut row)?;
            rows.push(row);
        }
        Ok(rows)
    }
    fn scan_worksheet(mut self) -> Result<Worksheet> {
        let mut scanner = XmlScanner::new(self.xml);
        let Some(sheet_data) = scanner.next_start_named("sheetData") else {
            return Err(err("worksheet XML에 <sheetData>가 없습니다."));
        };
        if sheet_data.self_closing {
            return Err(err("고정 workbook의 sheetData는 비어 있을 수 없습니다."));
        }
        let rows = self.scan_rows(&mut scanner, sheet_data.name)?;
        let sheet_data_span = sheet_data.start..scanner.cursor();
        let context = match self.sheet {
            ExcelSheetKind::ChangeLog => "worksheet XML namespace 검증: 변경내역",
            ExcelSheetKind::Master => "worksheet XML namespace 검증: 유류비",
        };
        validate_spreadsheet_xml_document(
            self.xml,
            "worksheet",
            context,
            false,
            Some(&sheet_data_span),
        )?;
        for (si, head) in self.shared_formula_heads {
            let expected = head.last_row.strict_sub(head.anchor_row).strict_add(1);
            if head.seen != expected {
                return Err(err(format!(
                    "shared formula ref의 cell 수가 다릅니다: si={si}, expected={expected}, actual={}",
                    head.seen
                )));
            }
        }
        Ok(Worksheet { rows })
    }
}
impl Worksheet {
    fn canonical_mark_shared_strings(&self, mapping: &mut [usize]) {
        for row in &self.rows {
            for cell in &row.cells {
                let CellValueType::SharedString(index) = cell.value_type else {
                    continue;
                };
                let slot = mapping.get_mut(index).unwrap_or_else(|| process::abort());
                *slot = 0;
            }
        }
    }
    fn canonical_remap_shared_strings(&mut self, mapping: &[usize]) {
        for row in &mut self.rows {
            for cell in &mut row.cells {
                let &mut CellValueType::SharedString(ref mut old_index) = &mut cell.value_type
                else {
                    continue;
                };
                *old_index = mapping
                    .get(*old_index)
                    .copied()
                    .filter(|index| *index != usize::MAX)
                    .unwrap_or_else(|| process::abort());
            }
        }
    }
    fn canonical_share_formulas(&mut self, sheet: ExcelSheetKind) -> Result<()> {
        let layout = match sheet {
            ExcelSheetKind::ChangeLog => CHANGE_LOG_FORMULA_LAYOUT,
            ExcelSheetKind::Master => MASTER_FORMULA_LAYOUT,
        };
        let mut next_si = 0_u32;
        let formula_col_mask = layout
            .required_cols
            .iter()
            .copied()
            .chain(layout.optional_zero_col)
            .fold(0_u32, |mask, col| mask | 1_u32.strict_shl(col));
        let mut column_state = [None; MASTER_HEADERS.len().strict_add(1)];
        let mut reference = String::new();
        let row_count = worksheet_row_count(self.rows.len());
        let row_start_index = u32_to_usize(layout.data_start_row.strict_sub(1));
        for (row, row_index) in
            (layout.data_start_row..=row_count).zip(row_start_index..self.rows.len())
        {
            let cell_count = self
                .rows
                .get(row_index)
                .unwrap_or_else(|| process::abort())
                .cells
                .len();
            for cell_index in 0..cell_count {
                let current_cell = self
                    .rows
                    .get(row_index)
                    .and_then(|row_obj| row_obj.cells.get(cell_index))
                    .unwrap_or_else(|| process::abort());
                let col = current_cell.col;
                if col >= u32::BITS || formula_col_mask & 1_u32.strict_shl(col) == 0 {
                    continue;
                }
                let state = column_state
                    .get_mut(u32_to_usize(col))
                    .unwrap_or_else(|| process::abort());
                let previous = *state;
                if previous.is_some_and(|(last_formula_row, _)| row <= last_formula_row) {
                    continue;
                }
                let Some(raw_anchor) = extract_first_tag_text(&current_cell.inner_xml, "f")? else {
                    continue;
                };
                let anchor = decode_xml_entities(raw_anchor)?;
                let interrupted = previous.is_some_and(|(last_formula_row, was_interrupted)| {
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
                    if self.try_get_formula_at(col, candidate_row)?.is_none() {
                        break;
                    }
                    last_row = candidate_row;
                }
                let group_len = last_row.strict_sub(row).strict_add(1);
                *state = Some((last_row, interrupted));
                if group_len < MIN_SHARED_FORMULA_CELLS {
                    continue;
                }
                let owned_anchor = anchor.into_owned();
                reference.clear();
                let mut row_buffer = NumBuffer::new();
                let row_text = row.format_into(&mut row_buffer);
                let mut last_row_buffer = NumBuffer::new();
                let last_row_text = last_row.format_into(&mut last_row_buffer);
                with_unlocked_ref_parts(col, row, |col_name, _| {
                    reference.extend([col_name, row_text, ":", col_name, last_row_text]);
                })?;
                let mut si_buffer = NumBuffer::new();
                let si_text = next_si.format_into(&mut si_buffer);
                for shared_row in row..=last_row {
                    let cell = Self::get_or_create_cell_mut(&mut self.rows, col, shared_row)?;
                    let inner = &cell.inner_xml;
                    if inner.is_empty() {
                        return Err(err("shared formula 대상 cell 본문이 없습니다."));
                    }
                    let tag = if shared_row == row {
                        FormulaTag::SharedRoot {
                            formula: &owned_anchor,
                            reference: &reference,
                            si: si_text,
                        }
                    } else {
                        FormulaTag::SharedFollower(si_text)
                    };
                    let formula_span = XmlScanner::new(inner)
                        .next_element_named("f")?
                        .map(|formula| formula.span)
                        .ok_or_else(|| err("cell formula 태그를 찾지 못했습니다."))?;
                    cell.inner_xml = replace_formula_tag_at(inner, formula_span, tag)?;
                }
                next_si = next_si.strict_add(1);
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
        start_row: u32,
        last_row: u32,
        max_col: u32,
    ) {
        for (row_num, row_obj) in (1_u32..=MAX_A1_ROW).zip(&mut self.rows) {
            if row_num < start_row {
                continue;
            }
            if row_num > last_row {
                break;
            }
            for cell in row_obj
                .cells
                .iter_mut()
                .take_while(|cell| cell.col <= max_col)
            {
                cell.value_type = CellValueType::General;
                cell.inner_xml.clear();
            }
        }
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
                inner_xml: String::new(),
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
        let required_len = target_index.strict_add(1);
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
        let required_len = row_index.strict_add(1);
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
                        inner_xml: String::new(),
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
    pub(crate) fn replace_rows(&mut self, rows: Vec<Row>) {
        self.rows = rows;
    }
    pub(crate) const fn row_count(&self) -> usize {
        self.rows.len()
    }
    pub(crate) fn row_numbers_from(&self, start: u32) -> RangeInclusive<u32> {
        RangeInclusive {
            start,
            last: worksheet_row_count(self.rows.len()),
        }
    }
    fn semantic_validation(
        &self,
        sheet: ExcelSheetKind,
        shared_strings: &SharedStringTable,
    ) -> Result<()> {
        let (sheet_name, layout, last_col) = match sheet {
            ExcelSheetKind::ChangeLog => (
                CHANGE_LOG_SHEET_NAME,
                CHANGE_LOG_FORMULA_LAYOUT,
                CHANGE_LOG_LAST_COL,
            ),
            ExcelSheetKind::Master => (MASTER_SHEET_NAME, MASTER_FORMULA_LAYOUT, MASTER_LAST_COL),
        };
        let required_col_mask = layout
            .required_cols
            .iter()
            .fold(0_u32, |mask, &col| mask | 1_u32.strict_shl(col));
        let mut last_data_row = None;
        let mut last_master_address_row = None;
        let mut first_layout_issue = None;
        let mut fixed_formula_count = 0_usize;
        for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
            let mut required_formula_index = 0_usize;
            let mut optional_formula = false;
            for cell in &row.cells {
                let col = cell.col;
                let shared_string = matches!(cell.value_type, CellValueType::SharedString(_));
                let master_address = sheet == ExcelSheetKind::Master && col == MASTER_ADDRESS_COL;
                if shared_string || master_address {
                    let display = Self::try_get_display_for_cell(cell, shared_strings)?;
                    if master_address && !display.trim().is_empty() {
                        last_master_address_row = Some(row_num);
                    }
                }
                let mut content_scanner = XmlScanner::new(&cell.inner_xml);
                let content = content_scanner.next_direct_element("worksheet cell")?;
                let formula_text = content
                    .as_ref()
                    .filter(|element| element.opening.name == "f")
                    .map(|element| element.body);
                let has_formula = formula_text.is_some();
                if let Some(raw_formula) = formula_text {
                    let formula = decode_xml_entities(raw_formula)?;
                    if formula.contains("#REF!") {
                        return Err(err(format!(
                            "worksheet에 #REF! 수식이 있습니다: {sheet_name}!row={row_num}, col={col}"
                        )));
                    }
                    let fixed = if row_num < layout.data_start_row {
                        layout
                            .fixed_formulas
                            .iter()
                            .find(|&&(fixed_col, fixed_row, _)| {
                                (col, row_num) == (fixed_col, fixed_row)
                            })
                    } else {
                        None
                    };
                    if let Some(&(_, _, expected)) = fixed {
                        if formula.as_ref() != expected {
                            return Err(err(format!(
                                "{sheet_name} 시트의 고정 formula가 다릅니다: row={row_num}, col={col}"
                            )));
                        }
                        fixed_formula_count = fixed_formula_count.strict_add(1);
                    }
                    let required_col =
                        col <= last_col && required_col_mask & 1_u32.strict_shl(col) != 0;
                    let data = row_num >= layout.data_start_row
                        && (required_col || layout.optional_zero_col == Some(col));
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
                let has_payload = match content {
                    None => false,
                    Some(_) if has_formula => true,
                    Some(element) => !decode_xml_entities(element.body)?.trim().is_empty(),
                };
                if row_num >= layout.data_start_row && col <= last_col && has_payload {
                    last_data_row = Some(row_num);
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
        if fixed_formula_count != layout.fixed_formulas.len() {
            return Err(err(format!(
                "{sheet_name} 시트의 고정 formula 수가 다릅니다: expected={}, actual={fixed_formula_count}",
                layout.fixed_formulas.len()
            )));
        }
        if let (Some(data_end), Some((row, col, detail))) = (last_data_row, first_layout_issue)
            && row <= data_end
        {
            return Err(err(format!(
                "{sheet_name} 시트의 {detail}: row={row}, col={col}"
            )));
        }
        if sheet == ExcelSheetKind::Master {
            let address_last_row =
                last_master_address_row.unwrap_or(MASTER_FORMULA_LAYOUT.data_start_row);
            if last_data_row != Some(address_last_row) {
                return Err(err(format!(
                    "유류비의 실제 데이터 마지막 행과 주소 마지막 행이 다릅니다: data={last_data_row:?}, address={address_last_row}"
                )));
            }
        }
        Ok(())
    }
    pub(crate) fn set_existing_cell_style_in_range(
        &mut self,
        row: u32,
        start_col: u32,
        end_col: u32,
        style: u32,
    ) -> Result<()> {
        let row_obj = row_index(row)
            .and_then(|index| self.rows.get_mut(index))
            .ok_or_else(|| err(format!("worksheet style 대상 row가 없습니다: {row}")))?;
        let start = row_obj.cells.partition_point(|cell| cell.col < start_col);
        let cell_count = end_col
            .checked_sub(start_col)
            .and_then(|value| value.checked_add(1))
            .map(u32_to_usize)
            .ok_or_else(|| err("worksheet style 대상 column 범위가 올바르지 않습니다."))?;
        let end = start
            .checked_add(cell_count)
            .ok_or_else(|| err("worksheet style 대상 cell 범위 계산 실패"))?;
        let cells = row_obj
            .cells
            .get_mut(start..end)
            .ok_or_else(|| err("worksheet style 대상 cell 범위가 올바르지 않습니다."))?;
        for (expected_col, cell) in (start_col..=end_col).zip(cells) {
            if cell.col != expected_col {
                return Err(err(format!(
                    "worksheet style 대상 cell이 없습니다: row={row}, col={expected_col}"
                )));
            }
            cell.style = Some(style);
        }
        Ok(())
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
        if let Some(formula_span) = XmlScanner::new(&cell.inner_xml)
            .next_element_named("f")?
            .map(|element| element.span)
        {
            cell.inner_xml =
                replace_formula_tag_at(&cell.inner_xml, formula_span, FormulaTag::Plain(formula))?;
            replace_first_tag_text(&mut cell.inner_xml, "v", cached)?;
        } else {
            let capacity = sum_lengths(&[
                "<f></f>".len(),
                if cached.is_empty() {
                    "<v/>".len()
                } else {
                    "<v></v>".len()
                },
                formula_text.len(),
                cached.len(),
            ]);
            let mut inner =
                try_string_with_capacity(capacity, "formula/cache XML 메모리 확보 실패")?;
            inner.extend(["<f>", formula_text.as_ref(), "</f>"]);
            if cached.is_empty() {
                inner.push_str("<v/>");
            } else {
                inner.extend(["<v>", cached, "</v>"]);
            }
            cell.inner_xml = inner;
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
        if cell.inner_xml.is_empty() {
            return Err(err(format!(
                "수식 cache 대상 cell이 비어 있습니다: row={row}, col={col}"
            )));
        }
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
        replace_first_tag_text(&mut cell.inner_xml, "v", value_text)
    }
    pub(crate) fn set_i32_at(&mut self, col: u32, row: u32, value: Option<i32>) -> Result<()> {
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        cell.value_type = CellValueType::General;
        cell.inner_xml.clear();
        if let Some(numeric_value) = value {
            cell.inner_xml.push_str("<v>");
            if numeric_value.is_negative() {
                cell.inner_xml.push('-');
            }
            push_decimal_text!(&mut cell.inner_xml, numeric_value.unsigned_abs());
            cell.inner_xml.push_str("</v>");
        }
        Ok(())
    }
    pub(crate) fn take_rows(&mut self) -> Vec<Row> {
        mem::take(&mut self.rows)
    }
    fn to_fragments(&self, sheet: ExcelSheetKind, last_data_row: u32) -> Result<(String, String)> {
        let (prefix_source, suffix_source, dimension_reference, max_col) = match sheet {
            ExcelSheetKind::ChangeLog => (
                EXCEL_CHANGE_LOG_PREFIX,
                EXCEL_CHANGE_LOG_SUFFIX,
                "A1:M243",
                13,
            ),
            ExcelSheetKind::Master => (EXCEL_MASTER_PREFIX, EXCEL_MASTER_SUFFIX, "A1:W884", 23),
        };
        let mut prefix = canonical_excel_fragment(prefix_source)?;
        let mut suffix = canonical_excel_fragment(suffix_source)?;
        let max_row = self
            .rows
            .iter()
            .rposition(|row| !row.cells.is_empty())
            .map_or(1, |index| worksheet_row_count(index.strict_add(1)));
        let dimension = build_ref_range(
            "A",
            RangeInclusive {
                start: 1,
                last: max_row,
            },
            max_col,
        )?;
        replace_canonical_text(
            &mut prefix,
            dimension_reference,
            &dimension,
            "worksheet dimension",
        )?;
        match sheet {
            ExcelSheetKind::Master => {
                let filter = build_ref_range(
                    "A",
                    RangeInclusive {
                        start: 14,
                        last: last_data_row.max(14),
                    },
                    23,
                )?;
                let last_row = worksheet_row_count(self.rows.len());
                let sort_state = build_ref_range(
                    "A",
                    RangeInclusive {
                        start: 15,
                        last: last_row,
                    },
                    23,
                )?;
                let sort_condition = build_ref_range(
                    "A",
                    RangeInclusive {
                        start: 15,
                        last: last_row,
                    },
                    1,
                )?;
                for (old, new, context) in [
                    ("A14:W882", filter.as_str(), "worksheet autoFilter"),
                    ("A15:W884", sort_state.as_str(), "worksheet sortState"),
                    (
                        "A15:A884",
                        sort_condition.as_str(),
                        "worksheet sortCondition",
                    ),
                ] {
                    replace_canonical_text(&mut suffix, old, new, context)?;
                }
            }
            ExcelSheetKind::ChangeLog => {
                for (column, col, old) in [
                    ("G", 7_u32, "G4:G52"),
                    ("J", 10_u32, "J4:J52"),
                    ("M", 13_u32, "M4:M52"),
                ] {
                    let mut reference = build_ref_range(
                        column,
                        RangeInclusive {
                            start: CHANGE_LOG_FORMULA_LAYOUT.data_start_row,
                            last: last_data_row.max(CHANGE_LOG_FORMULA_LAYOUT.data_start_row),
                        },
                        col,
                    )?;
                    if last_data_row <= CHANGE_LOG_FORMULA_LAYOUT.data_start_row {
                        let colon = reference.find(':').ok_or_else(|| {
                            err("Excel conditionalFormatting 범위가 손상되었습니다.")
                        })?;
                        reference.truncate(colon);
                    }
                    replace_canonical_text(
                        &mut suffix,
                        old,
                        &reference,
                        "worksheet conditionalFormatting",
                    )?;
                }
            }
        }
        Ok((prefix, suffix))
    }
    fn to_xml(&self, sheet: ExcelSheetKind, last_data_row: u32) -> Result<(String, usize)> {
        let (mut out, suffix) = self.to_fragments(sheet, last_data_row)?;
        let additional_capacity = self.rows.iter().fold(suffix.len(), |capacity, row| {
            row.cells.iter().fold(
                capacity.strict_add(row.attrs_xml.len()).strict_add(64),
                |cell_capacity, cell| {
                    cell_capacity
                        .strict_add(cell.inner_xml.len())
                        .strict_add(80)
                },
            )
        });
        out.try_reserve(additional_capacity)
            .map_err(|source| err_with_source("worksheet XML 메모리 확보 실패", source))?;
        let mut shared_string_reference_count = 0_usize;
        for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
            let mut row_buffer = NumBuffer::new();
            let row_text = row_num.format_into(&mut row_buffer);
            out.extend(["<row", " r=\"", row_text, "\"", row.attrs_xml.as_str()]);
            if row.cells.is_empty() {
                out.push_str("/>");
                continue;
            }
            out.push('>');
            for cell in &row.cells {
                let col = cell.col;
                out.extend(["<c", " r=\""]);
                with_unlocked_ref_parts(col, row_num, |col_text, _| {
                    out.extend([col_text, row_text]);
                })?;
                out.push('"');
                if let Some(style) = cell.style {
                    out.push_str(" s=\"");
                    push_decimal_text!(&mut out, style);
                    out.push('"');
                }
                if let Some(value_type) = cell.value_type.xml_attr() {
                    out.extend([" t=\"", value_type, "\""]);
                }
                if let CellValueType::SharedString(index) = cell.value_type {
                    shared_string_reference_count = shared_string_reference_count.strict_add(1);
                    out.push_str("><v>");
                    push_decimal_text!(&mut out, index);
                    out.extend(["</v>", "</c>"]);
                } else if !cell.inner_xml.is_empty() {
                    out.extend([">", cell.inner_xml.as_str(), "</c>"]);
                } else {
                    out.push_str("/>");
                }
            }
            out.push_str("</row>");
        }
        out.push_str(&suffix);
        Ok((out, shared_string_reference_count))
    }
    pub(crate) fn truncate_rows_after(&mut self, last_row_to_keep: u32) {
        self.rows.truncate(u32_to_usize(last_row_to_keep));
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
        Self::try_get_display_for_cell(cell, shared_strings)
    }
    fn try_get_display_for_cell<'text>(
        cell: &'text Cell,
        shared_strings: &'text SharedStringTable,
    ) -> Result<Cow<'text, str>> {
        if let CellValueType::SharedString(index) = cell.value_type {
            return shared_strings
                .get(index)
                .map(Cow::Borrowed)
                .ok_or_else(|| err(format!("shared string index 범위 오류: {index}")));
        }
        let raw_v = extract_first_tag_text(&cell.inner_xml, "v")?.unwrap_or("");
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
        if cell.inner_xml.is_empty() {
            return Ok("");
        }
        Err(err(format!(
            "고정 workbook의 텍스트 cell이 shared string이 아닙니다: row={row}, col={col}"
        )))
    }
    pub(crate) fn try_get_formula_at(&self, col: u32, row: u32) -> Result<Option<Cow<'_, str>>> {
        let Some(cell) = self.cell_at(col, row) else {
            return Ok(None);
        };
        let Some(text) = extract_first_tag_text(&cell.inner_xml, "f")? else {
            return Ok(None);
        };
        decode_xml_entities(text).map(Some)
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
        let actual_last_col = self.max_cell_col();
        if actual_last_col != last_col {
            return Err(err(format!(
                "{sheet_name} 시트의 마지막 열이 고정 스키마와 다릅니다: expected={last_col}, actual={actual_last_col}"
            )));
        }
        let header_cells: &[Cell] = row_index(header_row)
            .and_then(|index| self.rows.get(index))
            .map_or(&[], |row| row.cells.as_slice());
        let mut header_iter = header_cells.iter().peekable();
        for (col, expected) in (1_u32..).zip(headers.iter().copied()) {
            let actual = header_iter.next_if(|cell| cell.col == col).map_or_else(
                || Ok(Cow::Borrowed("")),
                |cell| Self::try_get_display_for_cell(cell, shared_strings),
            )?;
            if actual.as_ref() != expected {
                return Err(err(format!(
                    "{sheet_name} 헤더가 고정 스키마와 다릅니다: row={header_row}, col={col}, expected={expected}, actual={actual}"
                )));
            }
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
                inner_xml: copy_text(&cell.inner_xml)?,
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
fn replace_canonical_text(xml: &mut String, old: &str, new: &str, context: &str) -> Result<()> {
    let start = xml
        .find(old)
        .ok_or_else(|| err(format!("Excel 정규형의 {context} 표식을 찾지 못했습니다.")))?;
    let end = start.strict_add(old.len());
    if xml
        .get(end..)
        .is_some_and(|remaining| remaining.contains(old))
    {
        return Err(err(format!(
            "Excel 정규형에 {context} 표식이 여러 개 있습니다."
        )));
    }
    xml.replace_range(start..end, new);
    Ok(())
}
fn canonical_excel_style(
    style: u32,
    sheet: ExcelSheetKind,
    row: u32,
    col: Option<u32>,
    input_styles: &CanonicalStyleMap,
) -> Result<u32> {
    if sheet == ExcelSheetKind::Master
        && let Some(column) = col
    {
        match (row, column) {
            (1, 1..=7) => return Ok(24),
            (2, 1) => return Ok(25),
            (3, 1..=2) => return Ok(22),
            _ => {}
        }
    }
    let canonical = input_styles
        .get(u32_to_usize(style))
        .copied()
        .flatten()
        .ok_or_else(|| {
            err(format!(
                "worksheet가 참조한 style을 Excel 정규형으로 변환할 수 없습니다: {style}"
            ))
        })?;
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
    push_decimal_text!(out, whole);
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
    let exponent = out
        .get(exponent_marker.strict_add(1)..)
        .ok_or_else(|| err("Excel 숫자 cache 지수 범위가 손상되었습니다."))?
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
                .try_reserve_exact(sum_lengths(&[escaped.len(), "<f></f>".len()]))
                .map_err(|source| err_with_source("cell formula XML 메모리 확보 실패", source))?;
            replacement.extend(["<f>", escaped.as_ref(), "</f>"]);
        }
        FormulaTag::SharedFollower(si) => {
            replacement.extend(["<f t=\"shared\" si=\"", si, "\"/>"]);
        }
        FormulaTag::SharedRoot {
            formula,
            reference,
            si,
        } => {
            let escaped =
                try_xml_escape_text(formula, XmlEscapeContext::Text, "shared formula XML escape")?;
            replacement.extend([
                "<f t=\"shared\" ref=\"",
                reference,
                "\" si=\"",
                si,
                "\">",
                escaped.as_ref(),
                "</f>",
            ]);
        }
    }
    let capacity = sum_lengths(&[prefix.len(), replacement.len(), suffix.len()]);
    let mut output =
        try_string_with_capacity(capacity, "cell formula replacement 메모리 확보 실패")?;
    output.extend([prefix, replacement.as_str(), suffix]);
    Ok(output)
}
const fn row_index(row: u32) -> Option<usize> {
    u32_to_usize(row).checked_sub(1)
}
fn worksheet_row_count(len: usize) -> u32 {
    u32::try_from(len).unwrap_or_else(|_| process::abort())
}
fn sum_lengths(parts: &[usize]) -> usize {
    parts.iter().copied().fold(0_usize, usize::strict_add)
}
fn parse_tag_attrs(tag: &str) -> Result<Vec<XmlAttr<'_>>> {
    let mut out: Vec<XmlAttr<'_>> = try_vec_with_capacity(4, "XML 속성 목록 메모리 확보 실패")?;
    let mut scanner = XmlAttrScanner::new(tag)?;
    while let Some((name, value)) = scanner.next()? {
        if out.len() >= MAX_XML_ATTRIBUTE_COUNT {
            return Err(err("XML 속성 개수가 허용 한도를 초과했습니다."));
        }
        if out.iter().any(|attr| attr.name == name) {
            return Err(err("XML 태그에 중복 속성이 있습니다."));
        }
        if out.len() == out.capacity() {
            out.try_reserve(1)
                .map_err(|source| err_with_source("XML 속성 목록 추가 메모리 확보 실패", source))?;
        }
        out.push(XmlAttr {
            name: Cow::Borrowed(name),
            value,
        });
    }
    Ok(out)
}
fn get_attr<'attrs>(attrs: &'attrs [XmlAttr<'_>], name: &str) -> Option<&'attrs str> {
    attrs
        .iter()
        .find(|attr| attr.name == name)
        .map(|attr| attr.value.as_ref())
}
fn parse_u32_decimal(
    value: &str,
    format_error: impl FnOnce() -> Cow<'static, str>,
    parse_context: impl FnOnce() -> Cow<'static, str>,
) -> Result<u32> {
    if value.is_empty() {
        return Err(err(format_error()));
    }
    let mut parsed = 0_u32;
    let mut overflowed = false;
    for byte in value.bytes() {
        if !byte.is_ascii_digit() {
            return Err(err(format_error()));
        }
        if overflowed {
            continue;
        }
        let digit = u32::from(byte.strict_sub(b'0'));
        match parsed
            .checked_mul(10)
            .and_then(|current| current.checked_add(digit))
        {
            Some(next) => parsed = next,
            None => overflowed = true,
        }
    }
    if overflowed {
        value
            .parse::<u32>()
            .map_err(|source| err_with_source(parse_context(), source))
    } else {
        Ok(parsed)
    }
}
fn replace_first_tag_text(xml: &mut String, tag_name: &str, new_text: &str) -> Result<()> {
    let mut scanner = XmlScanner::new(xml);
    let Some(element) = scanner.next_element_named(tag_name)? else {
        return Err(err(format!("{tag_name} 태그를 찾지 못했습니다.")));
    };
    let trimmed_open_tag = element.opening.raw.trim_ascii_end();
    if new_text.is_empty() {
        if element.opening.self_closing {
            return Ok(());
        }
        let prefix = trimmed_open_tag
            .strip_suffix('>')
            .unwrap_or_else(|| process::abort());
        let mut replacement = copy_text(prefix)?;
        replacement.push_str("/>");
        xml.replace_range(element.span, &replacement);
        return Ok(());
    }
    if element.opening.self_closing {
        let prefix = trimmed_open_tag
            .strip_suffix("/>")
            .unwrap_or_else(|| process::abort());
        let capacity = sum_lengths(&[
            prefix.len(),
            ">".len(),
            new_text.len(),
            "</".len(),
            tag_name.len(),
            ">".len(),
        ]);
        let mut replacement =
            try_string_with_capacity(capacity, "XML self-closing 치환 메모리 확보 실패")?;
        replacement.extend([prefix, ">", new_text, "</", tag_name, ">"]);
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
        Ok(total.strict_add(encoded_len))
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
fn build_tag(name: &str, attrs: &[XmlAttr<'_>], self_closing: bool) -> Result<String> {
    let suffix = if self_closing { "/>" } else { ">" };
    let mut capacity = sum_lengths(&["<".len(), name.len(), suffix.len()]);
    for attr in attrs {
        capacity = sum_lengths(&[capacity, " =\"\"".len(), attr.name.len(), attr.value.len()]);
    }
    let mut out = try_string_with_capacity(capacity, "XML 태그 메모리 확보 실패")?;
    out.extend(["<", name]);
    for attr in attrs {
        out.extend([" ", attr.name.as_ref(), "=\""]);
        append_xml_escaped(&mut out, &attr.value, XmlEscapeContext::Attribute);
        out.push('"');
    }
    out.push_str(suffix);
    Ok(out)
}
fn build_ref_range(
    start_col_text: &str,
    rows: RangeInclusive<u32>,
    end_col: u32,
) -> Result<String> {
    with_unlocked_ref_parts(end_col, rows.last, |end_col_text, end_row| {
        let mut start_row_buffer = NumBuffer::new();
        let start_row_text = rows.start.format_into(&mut start_row_buffer);
        let mut end_row_buffer = NumBuffer::new();
        let end_row_text = end_row.format_into(&mut end_row_buffer);
        let capacity = sum_lengths(&[
            start_col_text.len(),
            start_row_text.len(),
            ":".len(),
            end_col_text.len(),
            end_row_text.len(),
        ]);
        let mut out = try_string_with_capacity(capacity, "cell range reference 메모리 확보 실패")?;
        out.extend([
            start_col_text,
            start_row_text,
            ":",
            end_col_text,
            end_row_text,
        ]);
        Ok(out)
    })?
}
fn row_col_error(prefix: &str, row_num: u32, col: u32) -> String {
    format!("{prefix}{row_num}, col={col})")
}
