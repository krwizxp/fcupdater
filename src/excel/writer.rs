use self::cell_ref::{MAX_A1_ROW, SINGLE_LETTER_COLUMNS, col_name_into, parse_ref_with_locks};
use super::{
    CHANGE_LOG_SHEET_NAME, CHANGE_LOG_SHEET_PATH, CanonicalStyleMap, FILTER_DATABASE_REF_PREFIX,
    MASTER_SHEET_NAME, MASTER_SHEET_PATH, SPREADSHEETML_NAMESPACE, SaveVerification,
    canonical_excel_fragment, copy_text,
    xlsx_container::{XlsxContainer, validate_spreadsheet_xml_document},
    xml::{XmlAttrScanner, XmlScanner, decode_xml_entities, extract_first_tag_text},
};
use crate::{
    diagnostic::{
        Result, append_fmt, bail_fmt, ensure, ensure_fmt, fmt_err, map_reserve, or_abort,
        parse_unsigned_u32, try_push_vec, try_string_with_capacity, try_vec_with_capacity,
    },
    master_sheet::{DECIMAL_SCALE, MasterSheetUpdater},
};
use alloc::{borrow::Cow, rc::Rc};
use core::{
    cmp::Ordering,
    fmt::NumBuffer,
    iter::repeat_n,
    mem,
    range::{Range, RangeInclusive},
    result::Result as CoreResult,
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
const MAX_SHARED_FORMULA_FOLLOWERS: u32 = 63;
const MAX_SHARED_FORMULA_FOLLOWERS_AFTER_GAP: u32 = 31;
const MIN_SHARED_FORMULA_CELLS: u32 = 6;
const CHANGE_LOG_LAST_COL: u32 = 13;
const MASTER_LAST_COL: u32 = 23;
const SHARED_STRING_GROWTH: usize = 16;
const EXCEL_MASTER_PREFIX: &str = include_str!("excel_sheet1_prefix.xml");
const EXCEL_MASTER_SUFFIX: &str = include_str!("excel_sheet1_suffix.xml");
const EXCEL_CHANGE_LOG_PREFIX: &str = include_str!("excel_sheet2_prefix.xml");
const EXCEL_CHANGE_LOG_SUFFIX: &str = include_str!("excel_sheet2_suffix.xml");
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
        (2, 12, r#"IF(B4+B5=0,"",(B4*B7+B5*B9)/(B4+B5))"#),
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
    xml: Option<String>,
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
struct WorksheetParser<'xml, 'styles> {
    cell_count: usize,
    input_styles: &'styles CanonicalStyleMap,
    shared_formula_heads: Vec<SharedFormulaHead>,
    sheet: ExcelSheetKind,
    sparse_formula_heads: HashMap<u32, SharedFormulaHead>,
    xml: &'xml str,
}
#[derive(Clone, Copy)]
enum XmlEscapeContext {
    Attribute,
    Text,
}
impl SharedStringTable {
    fn get(&self, index: usize) -> Option<&str> {
        self.entries.get(index).map(|entry| entry.text.as_ref())
    }
    fn intern(&mut self, value: &str) -> Result<usize> {
        if self.index.is_empty() && !self.entries.is_empty() {
            map_reserve(
                self.index.try_reserve(self.entries.len()),
                "shared string index map 메모리 확보 실패",
            )?;
            for (value_index, entry) in self.entries.iter().enumerate() {
                self.index
                    .entry(Rc::clone(&entry.text))
                    .or_insert(value_index);
            }
        }
        if let Some(&index) = self.index.get(value) {
            return Ok(index);
        }
        ensure_fmt!(
            self.entries.len() < MAX_SHARED_STRING_COUNT,
            "sharedStrings entry 개수가 허용 한도({MAX_SHARED_STRING_COUNT})를 초과했습니다."
        );
        let growth =
            SHARED_STRING_GROWTH.min(MAX_SHARED_STRING_COUNT.strict_sub(self.entries.len()));
        if self.index.len() == self.index.capacity() {
            map_reserve(
                self.index.try_reserve(growth),
                "shared string index 추가 메모리 확보 실패",
            )?;
        }
        if self.entries.len() == self.entries.capacity() {
            map_reserve(
                self.entries.try_reserve(growth),
                "sharedStrings XML entry 추가 메모리 확보 실패",
            )?;
        }
        let index = self.entries.len();
        let stored_value = Rc::<str>::from(value);
        validated_xml_escaped_len(value, XmlEscapeContext::Text, "shared string XML")?;
        self.entries.push(SharedStringEntry {
            text: Rc::clone(&stored_value),
            xml: None,
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
        let additional =
            self.entries
                .iter()
                .fold(256_usize.strict_add("</sst>".len()), |sum, entry| {
                    sum.strict_add(
                        entry
                            .xml
                            .as_ref()
                            .map_or_else(|| entry.text.len().strict_add(48), String::len),
                    )
                });
        let mut xml = try_string_with_capacity(additional, "sharedStrings XML 메모리 확보 실패")?;
        append_fmt(
            &mut xml,
            format_args!(
                "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n<sst xmlns=\"{SPREADSHEETML_NAMESPACE}\" count=\"{reference_count}\" uniqueCount=\"{}\">",
                self.entries.len()
            ),
        );
        for entry in &self.entries {
            if let Some(original) = entry.xml.as_ref() {
                map_reserve(
                    xml.try_reserve(original.len()),
                    "sharedStrings XML 메모리 확보 실패",
                )?;
                xml.push_str(original);
            } else {
                let opening = if has_edge_whitespace(&entry.text) {
                    "<si><t xml:space=\"preserve\">"
                } else {
                    "<si><t>"
                };
                let worst = opening
                    .len()
                    .strict_add(entry.text.len().strict_mul(5))
                    .strict_add(9);
                if xml.capacity().strict_sub(xml.len()) < worst {
                    map_reserve(xml.try_reserve(worst), "sharedStrings XML 메모리 확보 실패")?;
                }
                xml.push_str(opening);
                append_xml_escaped(&mut xml, &entry.text, XmlEscapeContext::Text);
                xml.push_str("</t></si>");
            }
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
            .ok_or("sharedStrings XML에 <sst>가 없습니다.")?;
        let mut entries = try_vec_with_capacity(4096, "shared string entry 메모리 확보 실패")?;
        let sst_closing = (!sst.self_closing).then_some(sst.name);
        let mut attrs: Vec<(&str, Cow<'_, str>)> = Vec::new();
        while let Some(closing_name) = sst_closing
            && let Some(si) = shared_strings_scanner.next_direct_element_named_until(
                "si",
                closing_name,
                1,
                "sharedStrings.xml의 sst",
            )?
        {
            ensure_fmt!(
                entries.len() < MAX_SHARED_STRING_COUNT,
                "sharedStrings entry 개수가 허용 한도({MAX_SHARED_STRING_COUNT})를 초과했습니다."
            );
            if entries.len() == entries.capacity() {
                map_reserve(
                    entries.try_reserve(SHARED_STRING_GROWTH),
                    "shared string entry 메모리 확보 실패",
                )?;
            }
            let si_xml = or_abort(shared_strings_xml_text.get(si.span));
            if let Some(plain_body) = si_xml
                .strip_prefix("<si><t>")
                .or_else(|| si_xml.strip_prefix("<si><t xml:space=\"preserve\">"))
                .and_then(|tail| tail.strip_suffix("</t></si>"))
                .filter(|body| !body.contains('<'))
            {
                let plain_text = decode_xml_entities(plain_body)?;
                entries.push(SharedStringEntry {
                    text: Rc::<str>::from(plain_text.as_ref()),
                    xml: None,
                });
                continue;
            }
            let mut xml =
                try_string_with_capacity(si_xml.len(), "shared string XML 메모리 확보 실패")?;
            let mut text_scanner = XmlScanner::new(si_xml);
            let mut source_cursor = 0_usize;
            let mut value_text: Option<Cow<'_, str>> = None;
            while let Some(element) = text_scanner.next_element_named("t")? {
                xml.push_str(or_abort(si_xml.get(source_cursor..element.span.start)));
                attrs.clear();
                let mut attr_scanner = XmlAttrScanner::from_tag(&element.opening);
                while let Some((name, value)) = attr_scanner.next()? {
                    ensure(
                        attrs.len() < 128,
                        "XML 속성 개수가 허용 한도를 초과했습니다.",
                    )?;
                    ensure(
                        !attrs.iter().any(|&(existing, _)| existing == name),
                        "XML 태그에 중복 속성이 있습니다.",
                    )?;
                    try_push_vec(
                        &mut attrs,
                        (name, value),
                        1,
                        "XML 속성 목록 추가 메모리 확보 실패",
                    )?;
                }
                let decoded = (!element.opening.self_closing)
                    .then(|| decode_xml_entities(element.body))
                    .transpose()?;
                if let Some(text) = decoded.as_deref() {
                    attrs.retain(|&(name, _)| name != "xml:space");
                    if has_edge_whitespace(text) {
                        try_push_vec(
                            &mut attrs,
                            ("xml:space", Cow::Borrowed("preserve")),
                            1,
                            "XML 속성 목록 추가 메모리 확보 실패",
                        )?;
                    }
                }
                let suffix = if element.opening.self_closing {
                    "/>"
                } else {
                    ">"
                };
                let mut tag_cap = "<t".len().strict_add(suffix.len());
                for attr in &attrs {
                    tag_cap = tag_cap
                        .strict_add(" =\"\"".len())
                        .strict_add(attr.0.len())
                        .strict_add(validated_xml_escaped_len(
                            &attr.1,
                            XmlEscapeContext::Attribute,
                            "XML 태그",
                        )?);
                }
                map_reserve(xml.try_reserve(tag_cap), "XML 태그 메모리 확보 실패")?;
                xml.push_str("<t");
                for attr in &attrs {
                    xml.extend([" ", attr.0, "=\""]);
                    append_xml_escaped(&mut xml, &attr.1, XmlEscapeContext::Attribute);
                    xml.push('"');
                }
                xml.push_str(suffix);
                if let Some(decoded_text) = decoded {
                    let escaped = try_xml_escape_text(
                        decoded_text.as_ref(),
                        XmlEscapeContext::Text,
                        "shared string XML",
                    )?;
                    xml.extend([escaped.as_ref(), "</t>"]);
                    if !decoded_text.is_empty() {
                        if let Some(current) = value_text.as_mut() {
                            let owned = current.to_mut();
                            map_reserve(
                                owned.try_reserve_exact(decoded_text.len()),
                                "XML tag text 메모리 확보 실패",
                            )?;
                            owned.push_str(decoded_text.as_ref());
                        } else {
                            value_text = Some(decoded_text);
                        }
                    }
                }
                source_cursor = element.span.end;
            }
            xml.push_str(or_abort(si_xml.get(source_cursor..)));
            entries.push(SharedStringEntry {
                text: Rc::<str>::from(value_text.as_deref().unwrap_or("")),
                xml: Some(xml),
            });
        }
        let shared_strings = SharedStringTable {
            entries,
            index: HashMap::new(),
        };
        let master_sheet =
            WorksheetParser::parse(ExcelSheetKind::Master, &master_xml, &input_styles)?;
        master_sheet.validate_fixed_header(ExcelSheetKind::Master, &shared_strings)?;
        let change_log_sheet =
            WorksheetParser::parse(ExcelSheetKind::ChangeLog, &change_log_xml, &input_styles)?;
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
        let mut input = [0_i128; 6];
        for (slot, row) in input.iter_mut().zip(4_u32..=9_u32) {
            *slot = i128::from(
                MasterSheetUpdater::get_f64_at(&self.master_sheet, 2, row, &self.shared_strings)?
                    .ok_or_else(|| fmt_err!("유류비 고정 입력값이 비어 있습니다: B{row}"))?,
            );
        }
        let [
            gasoline_qty,
            premium_qty,
            diesel_qty,
            gasoline_weight,
            premium_weight,
            diesel_weight,
        ] = input;
        let total_qty = gasoline_qty.strict_add(premium_qty).strict_add(diesel_qty);
        let mut cache = String::new();
        format_excel_ratio_into(&mut cache, total_qty, i128::from(DECIMAL_SCALE));
        self.master_sheet
            .set_formula_cached_value_at(2, 10, Some(&cache))?;
        let fuel_denominator = gasoline_qty.strict_add(premium_qty);
        let gasoline_contribution = (fuel_denominator != 0)
            .then(|| {
                gasoline_qty
                    .checked_mul(gasoline_weight)
                    .ok_or("혼합 옥탄가 계산 중 overflow가 발생했습니다.")
            })
            .transpose()?;
        for (column, weight) in [(11, premium_weight), (12, diesel_weight)] {
            let cached = gasoline_contribution
                .map(|gasoline_term| -> Result<&str> {
                    let numerator = premium_qty
                        .checked_mul(weight)
                        .and_then(|premium| gasoline_term.checked_add(premium))
                        .ok_or("혼합 옥탄가 계산 중 overflow가 발생했습니다.")?;
                    format_excel_ratio_into(
                        &mut cache,
                        numerator,
                        fuel_denominator.strict_mul(i128::from(DECIMAL_SCALE)),
                    );
                    Ok(cache.as_str())
                })
                .transpose()?;
            self.master_sheet
                .set_formula_cached_value_at(2, column, cached)?;
        }
        Ok(())
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
        let mut shared_count = 0_usize;
        for (sheet_ref, kind, path, last_row) in [
            (
                &mut self.master_sheet,
                ExcelSheetKind::Master,
                MASTER_SHEET_PATH,
                filter_last_row,
            ),
            (
                &mut self.change_log_sheet,
                ExcelSheetKind::ChangeLog,
                CHANGE_LOG_SHEET_PATH,
                change_log_last_row,
            ),
        ] {
            sheet_ref.validate_fixed_header(kind, &self.shared_strings)?;
            let (sheet_xml, count) = sheet_ref.to_xml(kind, last_row)?;
            self.container.put_text(path, sheet_xml);
            drop(sheet_ref.take_rows());
            shared_count = shared_count.strict_add(count);
        }
        let shared_strings_xml = self.shared_strings.to_xml(shared_count)?;
        let capacity = sum_lengths(&[
            EXCEL_WORKBOOK_OPENING.len(),
            "<bookViews><workbookView/></bookViews>".len(),
            EXCEL_CALC_EXTENSIONS_XML.len(),
            FILTER_DATABASE_REF_PREFIX.len(),
            512,
        ]);
        let mut workbook_xml =
            try_string_with_capacity(capacity, "Excel workbook XML 메모리 확보 실패")?;
        workbook_xml.extend([
            EXCEL_WORKBOOK_OPENING,
            "<workbookPr/>",
            "<bookViews><workbookView/></bookViews>",
            "<sheets><sheet name=\"유류비\" sheetId=\"1\" r:id=\"rId1\"/><sheet name=\"변경내역\" sheetId=\"2\" r:id=\"rId2\"/></sheets><definedNames><definedName name=\"_xlnm._FilterDatabase\" localSheetId=\"0\" hidden=\"1\">",
            FILTER_DATABASE_REF_PREFIX,
        ]);
        push_decimal_text!(&mut workbook_xml, filter_last_row);
        workbook_xml.extend([
            "</definedName></definedNames><calcPr/>",
            EXCEL_CALC_EXTENSIONS_XML,
            "</workbook>",
        ]);
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
impl<'xml, 'styles> WorksheetParser<'xml, 'styles> {
    fn parse(
        sheet: ExcelSheetKind,
        xml: &'xml str,
        input_styles: &'styles CanonicalStyleMap,
    ) -> Result<Worksheet> {
        Self {
            cell_count: 0,
            input_styles,
            shared_formula_heads: Vec::new(),
            sheet,
            sparse_formula_heads: HashMap::new(),
            xml,
        }
        .scan_worksheet()
    }
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
            let (inner_xml_text, cell_info) = (cell.body, cell.opening);
            let (mut reference_value, mut style_value, mut type_value) = (None, None, None);
            let mut attr_scanner = XmlAttrScanner::from_tag(&cell_info);
            while let Some((name, value)) = attr_scanner.next()? {
                let slot = match name {
                    "r" => &mut reference_value,
                    "s" => &mut style_value,
                    "t" => &mut type_value,
                    _ => bail_fmt!(
                        "고정 workbook cell에 지원하지 않는 속성이 있습니다: row={row_num}, attribute={name}"
                    ),
                };
                ensure(
                    slot.replace(value).is_none(),
                    "XML 태그에 중복 속성이 있습니다.",
                )?;
            }
            let reference_text = reference_value
                .as_deref()
                .ok_or_else(|| fmt_err!("cell reference가 없습니다: row={row_num}"))?;
            let (col, ref_row, _) = parse_ref_with_locks(reference_text).ok_or_else(|| {
                fmt_err!("cell reference 형식이 비정상입니다: row={row_num}, ref={reference_text}")
            })?;
            ensure_fmt!(
                ref_row == row_num,
                "cell reference row가 row 태그와 다릅니다: row={row_num}, ref={reference_text}"
            );
            ensure_fmt!(
                col >= next_col,
                "worksheet cell 순서는 column 오름차순이어야 합니다. (row={row_num}, col={col})"
            );
            let omit_empty_note = self.sheet == ExcelSheetKind::Master
                && row_num == 2
                && col != 1
                && cell_info.self_closing;
            let style = (!omit_empty_note)
                .then_some(style_value)
                .flatten()
                .map(|value| {
                    let parsed = parse_unsigned_u32(
                        &value,
                        format_args!(
                            "worksheet cell style이 음이 아닌 10진수 형식이 아닙니다. (row={row_num}, col={col})"
                        ),
                        format_args!("worksheet cell style 해석 실패 (row={row_num}, col={col})"),
                    )?;
                    canonical_excel_style(parsed, self.sheet, row_num, Some(col), self.input_styles)
                })
                .transpose()?;
            let (mut value_type, shared_string) = match type_value.as_deref() {
                None | Some("n") => (CellValueType::General, false),
                Some("s") => (CellValueType::General, true),
                Some("str") => (CellValueType::String, false),
                Some(other) => bail_fmt!(
                    "고정 workbook에서 지원하지 않는 cell type입니다: row={row_num}, col={col}, type={other}"
                ),
            };
            let has_attrs = style.is_some() || value_type != CellValueType::General;
            let retain = if cell_info.self_closing {
                ensure(!shared_string, "shared string cell 본문이 없습니다.")?;
                has_attrs.then(String::new)
            } else if shared_string {
                let value_body = if let Some(body) = simple_value_body(inner_xml_text) {
                    body
                } else {
                    let mut value_scanner = XmlScanner::new(inner_xml_text);
                    let value = value_scanner
                        .next_element_named("v")?
                        .filter(|element| {
                            element.opening.name == "v" && !element.opening.self_closing
                        })
                        .ok_or("shared string cell에 v 태그가 없습니다.")?;
                    ensure(
                        XmlAttrScanner::from_tag(&value.opening).next()?.is_none(),
                        "shared string v 태그에 속성이 있습니다.",
                    )?;
                    ensure(
                        value_scanner.next_tag().is_none()
                            && inner_xml_text
                                .get(..value.span.start)
                                .is_some_and(|text| text.trim().is_empty())
                            && inner_xml_text
                                .get(value.span.end..)
                                .is_some_and(|text| text.trim().is_empty()),
                        "shared string cell 본문 형식이 올바르지 않습니다.",
                    )?;
                    value.body
                };
                let index = parse_unsigned_u32(
                    value_body,
                    format_args!(
                        "shared string index 해석 실패: 음이 아닌 10진수 형식이 아닙니다."
                    ),
                    format_args!("shared string index 해석 실패"),
                )? as usize;
                value_type = CellValueType::SharedString(index);
                Some(String::new())
            } else if has_attrs || !inner_xml_text.is_empty() {
                Some(copy_text(inner_xml_text)?)
            } else {
                None
            };
            if let Some(inner_xml) = retain {
                self.retain_cell(
                    row_num,
                    row,
                    Cell {
                        col,
                        inner_xml,
                        style,
                        value_type,
                    },
                )?;
            }
            next_col = col.strict_add(1);
        }
        Ok(())
    }
    fn retain_cell(&mut self, row_num: u32, row: &mut Row, mut cell: Cell) -> Result<()> {
        let col = cell.col;
        ensure_fmt!(
            self.cell_count < MAX_WORKSHEET_CELL_COUNT,
            "worksheet cell 개수가 허용 한도({MAX_WORKSHEET_CELL_COUNT})를 초과했습니다."
        );
        if !cell.inner_xml.is_empty() && simple_value_body(&cell.inner_xml).is_none() {
            let inner_xml = cell.inner_xml.as_str();
            let mut formula_scanner = XmlScanner::new(inner_xml);
            let (mut formula_element, mut has_cache) = (None, false);
            while let Some(child) = formula_scanner.next_direct_element("worksheet cell")? {
                if child.opening.raw.contains("xmlns") {
                    let mut attributes = XmlAttrScanner::from_tag(&child.opening);
                    while let Some((name, _)) = attributes.next()? {
                        ensure_fmt!(
                            name != "xmlns" && !(name.starts_with("xmlns:")),
                            "worksheet cell 자식의 namespace 재정의는 지원하지 않습니다: {name}"
                        );
                    }
                }
                ensure_fmt!(
                    !(child.body.contains('<')),
                    "worksheet cell 자식에 중첩 요소가 있습니다: row={row_num}, col={col}"
                );
                match child.opening.name {
                    "f" if formula_element.is_none() && !has_cache => formula_element = Some(child),
                    "v" if !has_cache => has_cache = true,
                    "f" | "v" => bail_fmt!(
                        "고정 workbook cell 자식 순서 또는 개수가 올바르지 않습니다: row={row_num}, col={col}"
                    ),
                    name => bail_fmt!(
                        "고정 workbook cell에 지원하지 않는 자식 요소가 있습니다: row={row_num}, col={col}, element={name}"
                    ),
                }
            }
            if let Some(formula) = formula_element {
                let decoded_formula = decode_xml_entities(formula.body)?;
                let (mut aca, mut formula_type, mut reference, mut shared_index) =
                    (None, None, None, None);
                let mut attr_scanner = XmlAttrScanner::from_tag(&formula.opening);
                while let Some((name, value)) = attr_scanner.next()? {
                    let slot = match name {
                        "aca" => &mut aca,
                        "ref" => &mut reference,
                        "si" => &mut shared_index,
                        "t" => &mut formula_type,
                        _ => bail_fmt!("formula에 지원하지 않는 속성이 있습니다: {name}"),
                    };
                    ensure(
                        slot.replace(value).is_none(),
                        "XML 태그에 중복 속성이 있습니다.",
                    )?;
                }
                let shared = formula_type.as_deref() == Some("shared");
                ensure_fmt!(
                    shared
                        || (formula_type.is_none()
                            && reference.is_none()
                            && shared_index.is_none()
                            && aca.as_deref().is_none_or(|value| value == "false")),
                    "고정 workbook은 aca=\"false\" 외 formula 속성을 지원하지 않습니다: row={row_num}, col={col}"
                );
                ensure_fmt!(
                    has_cache,
                    "고정 workbook formula cache가 없습니다: row={row_num}, col={col}"
                );
                if shared {
                    ensure(
                        aca.is_none(),
                        "shared formula에 지원하지 않는 속성이 있습니다.",
                    )?;
                    let si = parse_unsigned_u32(
                        shared_index
                            .as_deref()
                            .ok_or("shared formula에 si 속성이 없습니다.")?,
                        format_args!("shared formula si가 음이 아닌 10진수가 아닙니다."),
                        format_args!("shared formula si 해석 실패"),
                    )?;
                    let range = reference
                        .as_deref()
                        .map(|ref_text| -> Result<((u32, u32), (u32, u32))> {
                            let (start_text, end_text) =
                                ref_text.split_once(':').unwrap_or((ref_text, ref_text));
                            let parse_unlocked = |text, msg| {
                                parse_ref_with_locks(text)
                                    .filter(|&(_, _, unlocked)| unlocked)
                                    .map(|(ref_col, ref_row, _)| (ref_col, ref_row))
                                    .ok_or(msg)
                            };
                            Ok((
                                parse_unlocked(
                                    start_text,
                                    "shared formula ref 시작 형식이 올바르지 않습니다.",
                                )?,
                                parse_unlocked(
                                    end_text,
                                    "shared formula ref 끝 형식이 올바르지 않습니다.",
                                )?,
                            ))
                        })
                        .transpose()?;
                    let (dense, sparse, slot) = (
                        &mut self.shared_formula_heads,
                        &mut self.sparse_formula_heads,
                        si as usize,
                    );
                    if decoded_formula.is_empty() {
                        ensure(
                            range.is_none(),
                            "shared formula follower에 ref 범위가 있습니다.",
                        )?;
                        let head = dense
                            .get_mut(slot)
                            .or_else(|| sparse.get_mut(&si))
                            .ok_or_else(|| {
                                fmt_err!("shared formula anchor를 찾지 못했습니다: si={si}")
                            })?;
                        ensure_fmt!(
                            col == head.anchor_col
                                && (head.anchor_row..=head.last_row).contains(&row_num),
                            "shared formula follower가 ref 범위를 벗어났습니다: si={si}, row={row_num}, col={col}"
                        );
                        head.seen = head.seen.strict_add(1);
                    } else {
                        let (start, end) =
                            range.ok_or("shared formula anchor에 ref 범위가 없습니다.")?;
                        ensure_fmt!(
                            start == (col, row_num) && start.0 == end.0 && start.1 <= end.1,
                            "shared formula ref가 세로 anchor 범위와 다릅니다: row={row_num}, col={col}"
                        );
                        ensure_fmt!(
                            slot >= dense.len() && !sparse.contains_key(&si),
                            "shared formula si anchor가 중복됩니다: {si}"
                        );
                        let head = SharedFormulaHead {
                            anchor_col: col,
                            anchor_row: row_num,
                            last_row: end.1,
                            seen: 1,
                        };
                        if slot == dense.len() && sparse.is_empty() {
                            try_push_vec(dense, head, 1, "shared formula anchor 메모리 확보 실패")?;
                        } else {
                            map_reserve(
                                sparse.try_reserve(1),
                                "shared formula anchor 메모리 확보 실패",
                            )?;
                            sparse.insert(si, head);
                        }
                    }
                } else {
                    let escaped = try_xml_escape_text(
                        decoded_formula.as_ref(),
                        XmlEscapeContext::Text,
                        "cell formula XML escape",
                    )?;
                    if formula.opening.raw != "<f>" || escaped.as_ref() != formula.body {
                        cell.inner_xml = replace_formula_tag(
                            inner_xml,
                            formula.span,
                            "",
                            escaped.as_ref(),
                            "cell formula XML 메모리 확보 실패",
                        )?;
                    }
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
        let mut seen_names: Vec<&str> = Vec::new();
        while let Some(row_info) = scanner.next_direct_opening_named_until(
            "row",
            sheet_data_name,
            "worksheet sheetData",
        )? {
            seen_names.clear();
            let (mut row_num_text, mut style_text, mut input_height) = (None, None, None);
            let (mut custom_format, mut input_custom_height) = (false, false);
            let mut attr_scanner = XmlAttrScanner::from_tag(&row_info);
            while let Some((name, value)) = attr_scanner.next()? {
                ensure(
                    seen_names.len() < 128,
                    "XML 속성 개수가 허용 한도를 초과했습니다.",
                )?;
                ensure(
                    !seen_names.contains(&name),
                    "XML 태그에 중복 속성이 있습니다.",
                )?;
                try_push_vec(
                    &mut seen_names,
                    name,
                    1,
                    "XML 속성 목록 추가 메모리 확보 실패",
                )?;
                ensure_fmt!(
                    name != "xmlns" && !(name.starts_with("xmlns:")),
                    "worksheet row의 namespace 재정의는 지원하지 않습니다: {name}"
                );
                match name {
                    "r" => row_num_text = Some(value),
                    "s" => style_text = Some(value),
                    "ht" => input_height = Some(value),
                    "customFormat" => custom_format = parse_xml_bool(name, value.as_ref())?,
                    "customHeight" => input_custom_height = parse_xml_bool(name, value.as_ref())?,
                    _ => {}
                }
            }
            let row_num = parse_unsigned_u32(
                row_num_text
                    .as_deref()
                    .ok_or("고정 workbook의 worksheet row에 r 속성이 없습니다.")?,
                format_args!("worksheet row 번호가 양의 10진수 형식이 아닙니다."),
                format_args!("worksheet row 번호 해석 실패"),
            )?;
            ensure(row_num != 0, "worksheet row 번호는 1 이상이어야 합니다.")?;
            ensure_fmt!(
                (1..=MAX_A1_ROW).contains(&row_num),
                "worksheet row 번호가 Excel 범위를 벗어났습니다: {row_num}"
            );
            let expected_row_num = worksheet_row_count(rows.len()).strict_add(1);
            ensure_fmt!(
                row_num == expected_row_num,
                "worksheet row 번호는 1부터 연속이어야 합니다: expected={expected_row_num}, current={row_num}"
            );
            let style = style_text
                .as_deref()
                .map(|value| {
                    let parsed = parse_unsigned_u32(
                        value,
                        format_args!("row style이 10진수가 아닙니다."),
                        format_args!("row style 해석 실패"),
                    )?;
                    canonical_excel_style(parsed, self.sheet, row_num, None, self.input_styles)
                })
                .transpose()?;
            let (height, custom_height) =
                if self.sheet == ExcelSheetKind::Master && matches!(row_num, 12 | 13) {
                    (Some("27"), true)
                } else {
                    (input_height.as_deref(), input_custom_height)
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
                map_reserve(rows.try_reserve(64), "worksheet row 메모리 확보 실패")?;
            }
            let cells = if row_info.self_closing {
                Vec::new()
            } else {
                try_vec_with_capacity(last_col as usize, "worksheet row cell 메모리 확보 실패")?
            };
            let mut row = Row { attrs_xml, cells };
            if !row_info.self_closing {
                self.parse_row(scanner, row_info.name, row_num, &mut row)?;
            }
            rows.push(row);
        }
        Ok(rows)
    }
    fn scan_worksheet(mut self) -> Result<Worksheet> {
        let mut scanner = XmlScanner::new(self.xml);
        let sheet_data = scanner
            .next_start_named("sheetData")
            .ok_or("worksheet XML에 <sheetData>가 없습니다.")?;
        ensure(
            !sheet_data.self_closing,
            "고정 workbook의 sheetData는 비어 있을 수 없습니다.",
        )?;
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
        let invalid_count = |si, head: SharedFormulaHead| {
            let expected = head.last_row.strict_sub(head.anchor_row).strict_add(1);
            (head.seen != expected).then_some((si, expected, head.seen))
        };
        let invalid = (0_u32..)
            .zip(self.shared_formula_heads)
            .find_map(|(si, head)| invalid_count(si, head))
            .or_else(|| {
                self.sparse_formula_heads
                    .into_iter()
                    .filter_map(|(si, head)| invalid_count(si, head))
                    .min_by_key(|&(si, ..)| si)
            });
        if let Some((si, expected, actual)) = invalid {
            bail_fmt!(
                "shared formula ref의 cell 수가 다릅니다: si={si}, expected={expected}, actual={actual}"
            );
        }
        Ok(Worksheet { rows })
    }
}
impl Worksheet {
    fn canonical_mark_shared_strings(&self, mapping: &mut [usize]) {
        for row in &self.rows {
            for cell in &row.cells {
                if let CellValueType::SharedString(index) = cell.value_type {
                    *or_abort(mapping.get_mut(index)) = 0;
                }
            }
        }
    }
    fn canonical_remap_shared_strings(&mut self, mapping: &[usize]) {
        for row in &mut self.rows {
            for cell in &mut row.cells {
                if let CellValueType::SharedString(old_index) = cell.value_type {
                    let mapped = or_abort(
                        mapping
                            .get(old_index)
                            .copied()
                            .filter(|&index| index != usize::MAX),
                    );
                    cell.value_type = CellValueType::SharedString(mapped);
                }
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
            .fold(0_u32, |mask, col| mask | 1_u32.unbounded_shl(col));
        let mut column_state = [None; MASTER_HEADERS.len().strict_add(1)];
        let (mut follower_xml, mut reference) = (String::new(), String::new());
        let row_count = worksheet_row_count(self.rows.len());
        let row_start_index = layout.data_start_row.strict_sub(1) as usize;
        for (row, row_index) in
            (layout.data_start_row..=row_count).zip(row_start_index..self.rows.len())
        {
            let cell_count = or_abort(self.rows.get(row_index)).cells.len();
            for cell_index in 0..cell_count {
                let current_cell = or_abort(
                    self.rows
                        .get(row_index)
                        .and_then(|row_obj| row_obj.cells.get(cell_index)),
                );
                let col = current_cell.col;
                if formula_col_mask & 1_u32.unbounded_shl(col) == 0 {
                    continue;
                }
                let state = or_abort(column_state.get_mut(col as usize));
                let previous = *state;
                if previous.is_some_and(|(last_formula_row, _)| row <= last_formula_row)
                    || (!current_cell.inner_xml.starts_with("<f>")
                        && extract_first_tag_text(&current_cell.inner_xml, "f")?.is_none())
                {
                    continue;
                }
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
                    let Some(candidate_cell) = self.cell_at(col, candidate_row) else {
                        break;
                    };
                    if !candidate_cell.inner_xml.starts_with("<f/>")
                        && !candidate_cell.inner_xml.starts_with("<f>")
                        && extract_first_tag_text(&candidate_cell.inner_xml, "f")?.is_none()
                    {
                        break;
                    }
                    last_row = candidate_row;
                }
                *state = Some((last_row, interrupted));
                if last_row.strict_sub(row).strict_add(1) < MIN_SHARED_FORMULA_CELLS {
                    continue;
                }
                let mut col_buffer = [0_u8; 3];
                let col_name = col_name_into(col, &mut col_buffer)?;
                reference.clear();
                append_fmt(
                    &mut reference,
                    format_args!(
                        " t=\"shared\" ref=\"{col_name}{row}:{col_name}{last_row}\" si=\"{next_si}\""
                    ),
                );
                follower_xml.clear();
                append_fmt(
                    &mut follower_xml,
                    format_args!("<f t=\"shared\" si=\"{next_si}\"/>"),
                );
                for shared_row in row..=last_row {
                    let cell = Self::get_or_create_cell_mut(&mut self.rows, col, shared_row)?;
                    ensure(
                        !cell.inner_xml.is_empty(),
                        "shared formula 대상 cell 본문이 없습니다.",
                    )?;
                    if shared_row == row {
                        if cell.inner_xml.starts_with("<f>") {
                            map_reserve(
                                cell.inner_xml.try_reserve(reference.len()),
                                "shared formula root XML 메모리 확보 실패",
                            )?;
                            cell.inner_xml.insert_str(2, &reference);
                        } else {
                            let formula = XmlScanner::new(&cell.inner_xml)
                                .next_element_named("f")?
                                .ok_or("cell formula 태그를 찾지 못했습니다.")?;
                            let decoded = decode_xml_entities(formula.body)?;
                            let escaped = try_xml_escape_text(
                                &decoded,
                                XmlEscapeContext::Text,
                                "shared formula root XML escape",
                            )?;
                            cell.inner_xml = replace_formula_tag(
                                &cell.inner_xml,
                                formula.span,
                                &reference,
                                escaped.as_ref(),
                                "shared formula root XML 메모리 확보 실패",
                            )?;
                        }
                    } else {
                        let formula_span = if cell.inner_xml.starts_with("<f/>") {
                            (0..4).into()
                        } else {
                            XmlScanner::new(&cell.inner_xml)
                                .next_element_named("f")?
                                .map(|formula| formula.span)
                                .ok_or("cell formula 태그를 찾지 못했습니다.")?
                        };
                        let additional = follower_xml
                            .len()
                            .saturating_sub(formula_span.end.strict_sub(formula_span.start));
                        map_reserve(
                            cell.inner_xml.try_reserve(additional),
                            "shared formula follower XML 메모리 확보 실패",
                        )?;
                        cell.inner_xml.replace_range(formula_span, &follower_xml);
                    }
                }
                next_si = next_si.strict_add(1);
            }
        }
        Ok(())
    }
    fn cell_at(&self, col: u32, row: u32) -> Option<&Cell> {
        row_index(row)
            .and_then(|index| self.rows.get(index))?
            .cell(col)
    }
    pub(crate) fn clear_cells_in_rows_through_col(
        &mut self,
        start_row: u32,
        last_row: u32,
        max_col: u32,
    ) {
        let start = row_index(start_row).unwrap_or(0);
        let end = (last_row as usize).min(self.rows.len());
        for row_obj in self.rows.get_mut(start..end).unwrap_or(&mut []) {
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
        let copied = row_index(source_row)
            .and_then(|index| self.rows.get(index))
            .ok_or_else(|| fmt_err!("worksheet style 원본 row가 없습니다: {source_row}"))?
            .try_copy_style(max_col)?;
        *Self::ensure_row_mut(
            &mut self.rows,
            target_row,
            "worksheet style 대상 row 메모리 확보 실패",
        )? = copied;
        Ok(())
    }
    fn ensure_row_mut<'rows>(
        rows: &'rows mut Vec<Row>,
        row: u32,
        context: &'static str,
    ) -> Result<&'rows mut Row> {
        let index = or_abort(row_index(row));
        let required_len = index.strict_add(1);
        if rows.len() < required_len {
            map_reserve(
                rows.try_reserve(required_len.strict_sub(rows.len())),
                context,
            )?;
            rows.resize_with(required_len, Row::default);
        }
        Ok(or_abort(rows.get_mut(index)))
    }
    pub(crate) fn get_i32_at(
        &self,
        col: u32,
        row: u32,
        shared_strings: &SharedStringTable,
    ) -> Result<Option<i32>> {
        let text = self.try_get_display_at(col, row, shared_strings)?;
        let trimmed = text.trim();
        Ok((!trimmed.is_empty())
            .then(|| parse_decimal_scaled(trimmed, 0, false, true))
            .flatten()
            .and_then(|value| i32::try_from(value).ok()))
    }
    fn get_or_create_cell_mut(rows: &mut Vec<Row>, col: u32, row: u32) -> Result<&mut Cell> {
        let row_obj = Self::ensure_row_mut(rows, row, "worksheet cell row 메모리 확보 실패")?;
        match row_obj.cell_slot(col) {
            Ok(index) => Ok(or_abort(row_obj.cells.get_mut(index))),
            Err(index) => {
                if row_obj.cells.len() == row_obj.cells.capacity() {
                    map_reserve(
                        row_obj.cells.try_reserve(16),
                        "worksheet cell 추가 메모리 확보 실패",
                    )?;
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
    pub(crate) fn has_formula_at(&self, col: u32, row: u32) -> Result<bool> {
        let Some(cell) = self.cell_at(col, row) else {
            return Ok(false);
        };
        if simple_value_body(&cell.inner_xml).is_some() {
            return Ok(false);
        }
        Ok(extract_first_tag_text(&cell.inner_xml, "f")?.is_some())
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
            .fold(0_u32, |mask, &col| mask | 1_u32.unbounded_shl(col));
        let (mut last_data_row, mut last_master_address_row, mut first_layout_issue) =
            (None, None, None);
        let mut fixed_formula_count = 0_usize;
        for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
            let (mut required_formula_index, mut optional_formula) = (0_usize, false);
            for cell in &row.cells {
                let col = cell.col;
                let master_address = sheet == ExcelSheetKind::Master && col == 6;
                if matches!(cell.value_type, CellValueType::SharedString(_)) || master_address {
                    let display = Self::try_get_display_for_cell(cell, shared_strings)?;
                    if master_address && !display.trim().is_empty() {
                        last_master_address_row = Some(row_num);
                    }
                }
                if cell.inner_xml.is_empty() {
                    continue;
                }
                if let Some(raw_value) = simple_value_body(&cell.inner_xml) {
                    if row_num >= layout.data_start_row
                        && col <= last_col
                        && !decode_xml_entities(raw_value)?.trim().is_empty()
                    {
                        last_data_row = Some(row_num);
                    }
                    continue;
                }
                let is_shared_follower_xml = row_num >= layout.data_start_row
                    && cell.inner_xml.starts_with("<f t=\"shared\"")
                    && cell.inner_xml.find('>').is_some_and(|gt| {
                        gt > 0 && cell.inner_xml.as_bytes().get(gt.strict_sub(1)) == Some(&b'/')
                    });
                let content = (!is_shared_follower_xml)
                    .then(|| XmlScanner::new(&cell.inner_xml).next_direct_element("worksheet cell"))
                    .transpose()?
                    .flatten();
                let formula_text = if is_shared_follower_xml {
                    Some("")
                } else {
                    content
                        .as_ref()
                        .filter(|element| element.opening.name == "f")
                        .map(|element| element.body)
                };
                if let Some(raw_formula) = formula_text {
                    let formula = decode_xml_entities(raw_formula)?;
                    ensure_fmt!(
                        !(formula.contains("#REF!")),
                        "worksheet에 #REF! 수식이 있습니다: {sheet_name}!row={row_num}, col={col}"
                    );
                    let fixed = (row_num < layout.data_start_row)
                        .then(|| {
                            layout
                                .fixed_formulas
                                .iter()
                                .find(|&&(fixed_col, fixed_row, _)| {
                                    (col, row_num) == (fixed_col, fixed_row)
                                })
                        })
                        .flatten();
                    if let Some(&(_, _, expected)) = fixed {
                        ensure_fmt!(
                            formula.as_ref() == expected,
                            "{sheet_name} 시트의 고정 formula가 다릅니다: row={row_num}, col={col}"
                        );
                        fixed_formula_count = fixed_formula_count.strict_add(1);
                    }
                    let data = row_num >= layout.data_start_row
                        && ((col <= last_col && required_col_mask & 1_u32.strict_shl(col) != 0)
                            || layout.optional_zero_col == Some(col));
                    ensure_fmt!(
                        fixed.is_some() || data,
                        "{sheet_name} 시트의 고정 위치 밖에 formula가 있습니다: row={row_num}, col={col}"
                    );
                    if data {
                        if layout.required_cols.get(required_formula_index) == Some(&col) {
                            required_formula_index = required_formula_index.strict_add(1);
                        }
                        if layout.optional_zero_col == Some(col) {
                            optional_formula = true;
                        }
                    }
                }
                let has_payload = formula_text.is_some()
                    || match content {
                        None => false,
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
                    && MasterSheetUpdater::get_f64_at(self, col, row_num, shared_strings)?
                        .is_none_or(|value| value != 0_i64)
                {
                    first_layout_issue =
                        Some((row_num, col, "선택 formula 위치는 수동 0이어야 합니다"));
                }
            }
        }
        ensure_fmt!(
            fixed_formula_count == layout.fixed_formulas.len(),
            "{sheet_name} 시트의 고정 formula 수가 다릅니다: expected={}, actual={fixed_formula_count}",
            layout.fixed_formulas.len()
        );
        if let (Some(data_end), Some((row, col, detail))) = (last_data_row, first_layout_issue)
            && row <= data_end
        {
            bail_fmt!("{sheet_name} 시트의 {detail}: row={row}, col={col}");
        }
        if sheet == ExcelSheetKind::Master {
            let address_last_row =
                last_master_address_row.unwrap_or(MASTER_FORMULA_LAYOUT.data_start_row);
            ensure_fmt!(
                last_data_row == Some(address_last_row),
                "유류비의 실제 데이터 마지막 행과 주소 마지막 행이 다릅니다: data={last_data_row:?}, address={address_last_row}"
            );
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
            .ok_or_else(|| fmt_err!("worksheet style 대상 row가 없습니다: {row}"))?;
        let start = row_obj.cells.partition_point(|cell| cell.col < start_col);
        let cell_count = end_col
            .checked_sub(start_col)
            .and_then(|value| value.checked_add(1))
            .ok_or("worksheet style 대상 column 범위가 올바르지 않습니다.")?
            as usize;
        let end = start
            .checked_add(cell_count)
            .ok_or("worksheet style 대상 cell 범위 계산 실패")?;
        let cells = row_obj
            .cells
            .get_mut(start..end)
            .ok_or("worksheet style 대상 cell 범위가 올바르지 않습니다.")?;
        for (expected_col, cell) in (start_col..=end_col).zip(cells) {
            ensure_fmt!(
                cell.col == expected_col,
                "worksheet style 대상 cell이 없습니다: row={row}, col={expected_col}"
            );
            cell.style = Some(style);
        }
        Ok(())
    }
    pub(crate) fn set_formula_at_with_cache(
        &mut self,
        col: u32,
        row: u32,
        formula: Option<&str>,
        value: Option<&str>,
        string_value: bool,
    ) -> Result<()> {
        let formula_len = formula
            .map(|raw| validated_xml_escaped_len(raw, XmlEscapeContext::Text, "formula XML escape"))
            .transpose()?;
        let value_len = value
            .map(|raw| {
                validated_xml_escaped_len(raw, XmlEscapeContext::Text, "formula cache XML escape")
            })
            .transpose()?;
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        cell.value_type = if string_value || value.is_none() {
            CellValueType::String
        } else {
            CellValueType::General
        };
        let cached = value.unwrap_or("");
        let inner = &mut cell.inner_xml;
        inner.clear();
        let prefix_reserve = formula_len
            .map_or("<f t=\"shared\" si=\"99999\"/><v></v>".len(), |len| {
                "<f></f><v></v>".len().strict_add(len)
            });
        map_reserve(
            inner.try_reserve_exact(prefix_reserve.strict_add(value_len.unwrap_or(0))),
            "formula/cache XML 메모리 확보 실패",
        )?;
        let append_tag =
            |out: &mut String, tag: &str, text: Option<&str>, escaped_len: Option<usize>| {
                if let Some(raw) = text.filter(|raw| !raw.is_empty()) {
                    out.extend(["<", tag, ">"]);
                    if escaped_len == Some(raw.len()) {
                        out.push_str(raw);
                    } else {
                        append_xml_escaped(out, raw, XmlEscapeContext::Text);
                    }
                    out.extend(["</", tag, ">"]);
                } else if text.is_some() {
                    out.extend(["<", tag, "></", tag, ">"]);
                } else {
                    out.extend(["<", tag, "/>"]);
                }
            };
        append_tag(inner, "f", formula, formula_len);
        append_tag(
            inner,
            "v",
            (!cached.is_empty()).then_some(cached),
            value_len,
        );
        Ok(())
    }
    fn set_formula_cached_value_at(
        &mut self,
        col: u32,
        row: u32,
        value: Option<&str>,
    ) -> Result<()> {
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        cell.value_type = if value.is_none() {
            CellValueType::String
        } else {
            CellValueType::General
        };
        ensure_fmt!(
            !cell.inner_xml.is_empty(),
            "수식 cache 대상 cell이 비어 있습니다: row={row}, col={col}"
        );
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
        let xml = &mut cell.inner_xml;
        let element = XmlScanner::new(xml)
            .next_element_named("v")?
            .ok_or("v 태그를 찾지 못했습니다.")?;
        if value_text.is_empty() {
            if !element.opening.self_closing {
                xml.replace_range(element.opening.end..element.span.end, "/>");
            }
        } else if element.opening.self_closing {
            let prefix = or_abort(element.opening.raw.strip_suffix("/>"));
            let mut replacement = try_string_with_capacity(
                sum_lengths(&[prefix.len(), "></v>".len(), value_text.len()]),
                "XML self-closing 치환 메모리 확보 실패",
            )?;
            replacement.extend([prefix, ">", value_text, "</v>"]);
            xml.replace_range(element.span, &replacement);
        } else {
            xml.replace_range(element.body_span, value_text);
        }
        Ok(())
    }
    pub(crate) fn set_i32_at(&mut self, col: u32, row: u32, value: Option<i32>) -> Result<()> {
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        cell.value_type = CellValueType::General;
        cell.inner_xml.clear();
        if let Some(numeric_value) = value {
            cell.inner_xml.extend([
                "<v>",
                numeric_value.format_into(&mut NumBuffer::new()),
                "</v>",
            ]);
        }
        Ok(())
    }
    pub(crate) fn take_rows(&mut self) -> Vec<Row> {
        mem::take(&mut self.rows)
    }
    fn to_xml(&self, sheet: ExcelSheetKind, last_data_row: u32) -> Result<(String, usize)> {
        let (prefix_source, suffix_source, dimension_reference, end_col) = match sheet {
            ExcelSheetKind::ChangeLog => (
                EXCEL_CHANGE_LOG_PREFIX,
                EXCEL_CHANGE_LOG_SUFFIX,
                "A1:M243",
                "M",
            ),
            ExcelSheetKind::Master => (EXCEL_MASTER_PREFIX, EXCEL_MASTER_SUFFIX, "A1:W884", "W"),
        };
        let mut out = canonical_excel_fragment(prefix_source)?;
        let mut suffix = canonical_excel_fragment(suffix_source)?;
        let max_row = self
            .rows
            .iter()
            .rposition(|row| !row.cells.is_empty())
            .map_or(1, |index| worksheet_row_count(index.strict_add(1)));
        let mut ref_buf = String::with_capacity(24);
        let mut num_buf = NumBuffer::new();
        ref_buf.extend(["A1:", end_col, max_row.format_into(&mut num_buf)]);
        replace_canonical_text(
            &mut out,
            dimension_reference,
            &ref_buf,
            "worksheet dimension",
        )?;
        match sheet {
            ExcelSheetKind::Master => {
                ref_buf.clear();
                ref_buf.extend(["A14:W", last_data_row.max(14).format_into(&mut num_buf)]);
                replace_canonical_text(&mut suffix, "A14:W882", &ref_buf, "worksheet autoFilter")?;
                let last_row_text = worksheet_row_count(self.rows.len()).format_into(&mut num_buf);
                for (old, prefix_text, context) in [
                    ("A15:W884", "A15:W", "worksheet sortState"),
                    ("A15:A884", "A15:A", "worksheet sortCondition"),
                ] {
                    ref_buf.clear();
                    ref_buf.extend([prefix_text, last_row_text]);
                    replace_canonical_text(&mut suffix, old, &ref_buf, context)?;
                }
            }
            ExcelSheetKind::ChangeLog => {
                let start = CHANGE_LOG_FORMULA_LAYOUT.data_start_row;
                let mut start_buf = NumBuffer::new();
                let start_text = start.format_into(&mut start_buf);
                let end_text =
                    (last_data_row > start).then(|| last_data_row.format_into(&mut num_buf));
                for (column, old) in [("G", "G4:G52"), ("J", "J4:J52"), ("M", "M4:M52")] {
                    ref_buf.clear();
                    ref_buf.extend([column, start_text]);
                    if let Some(end_row) = end_text {
                        ref_buf.extend([":", column, end_row]);
                    }
                    replace_canonical_text(
                        &mut suffix,
                        old,
                        &ref_buf,
                        "worksheet conditionalFormatting",
                    )?;
                }
            }
        }
        let additional_capacity = self.rows.iter().fold(suffix.len(), |capacity, row| {
            row.cells.iter().fold(
                capacity.strict_add(row.attrs_xml.len()).strict_add(64),
                |cell_cap, cell| cell_cap.strict_add(cell.inner_xml.len()).strict_add(80),
            )
        });
        map_reserve(
            out.try_reserve(additional_capacity),
            "worksheet XML 메모리 확보 실패",
        )?;
        let mut shared_string_reference_count = 0_usize;
        let (mut row_buffer, mut style_buffer, mut index_buffer) =
            (NumBuffer::new(), NumBuffer::new(), NumBuffer::new());
        let mut col_buffer = [0_u8; 3];
        for (row_num, row) in (1_u32..=MAX_A1_ROW).zip(&self.rows) {
            let row_text = row_num.format_into(&mut row_buffer);
            out.extend(["<row r=\"", row_text, "\"", row.attrs_xml.as_str()]);
            if row.cells.is_empty() {
                out.push_str("/>");
                continue;
            }
            out.push('>');
            for cell in &row.cells {
                let col_text = cell
                    .col
                    .checked_sub(1)
                    .and_then(|idx| {
                        SINGLE_LETTER_COLUMNS.get(idx as usize..(idx as usize).strict_add(1))
                    })
                    .map_or_else(|| col_name_into(cell.col, &mut col_buffer), Ok)?;
                out.extend(["<c r=\"", col_text, row_text, "\""]);
                if let Some(style) = cell.style {
                    out.extend([" s=\"", style.format_into(&mut style_buffer), "\""]);
                }
                match cell.value_type {
                    CellValueType::SharedString(index) => {
                        shared_string_reference_count = shared_string_reference_count.strict_add(1);
                        out.extend([
                            " t=\"s\"><v>",
                            index.format_into(&mut index_buffer),
                            "</v></c>",
                        ]);
                    }
                    CellValueType::String if !cell.inner_xml.is_empty() => {
                        out.extend([" t=\"str\">", cell.inner_xml.as_str(), "</c>"]);
                    }
                    CellValueType::String => out.push_str(" t=\"str\"/>"),
                    CellValueType::General if !cell.inner_xml.is_empty() => {
                        out.extend([">", cell.inner_xml.as_str(), "</c>"]);
                    }
                    CellValueType::General => out.push_str("/>"),
                }
            }
            out.push_str("</row>");
        }
        out.push_str(&suffix);
        Ok((out, shared_string_reference_count))
    }
    pub(crate) fn truncate_rows_after(&mut self, last_row_to_keep: u32) {
        self.rows.truncate(last_row_to_keep as usize);
    }
    pub(crate) fn try_get_display_at<'text>(
        &'text self,
        col: u32,
        row: u32,
        shared_strings: &'text SharedStringTable,
    ) -> Result<Cow<'text, str>> {
        self.cell_at(col, row).map_or_else(
            || Ok(Cow::Borrowed("")),
            |cell| Self::try_get_display_for_cell(cell, shared_strings),
        )
    }
    fn try_get_display_for_cell<'text>(
        cell: &'text Cell,
        shared_strings: &'text SharedStringTable,
    ) -> Result<Cow<'text, str>> {
        if let CellValueType::SharedString(index) = cell.value_type {
            return shared_strings
                .get(index)
                .map(Cow::Borrowed)
                .ok_or_else(|| fmt_err!("shared string index 범위 오류: {index}"));
        }
        let raw_v = match simple_value_body(&cell.inner_xml) {
            Some(body) => body,
            None => extract_first_tag_text(&cell.inner_xml, "v")?.unwrap_or(""),
        };
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
                .ok_or_else(|| fmt_err!("shared string index 범위 오류: {index}"));
        }
        ensure_fmt!(
            cell.inner_xml.is_empty(),
            "고정 workbook의 텍스트 cell이 shared string이 아닙니다: row={row}, col={col}"
        );
        Ok("")
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
        let actual_last_col = self
            .rows
            .iter()
            .filter_map(|row| row.cells.last().map(|cell| cell.col))
            .max()
            .unwrap_or(1);
        ensure_fmt!(
            actual_last_col == last_col,
            "{sheet_name} 시트의 마지막 열이 고정 스키마와 다릅니다: expected={last_col}, actual={actual_last_col}"
        );
        let header_cells: &[Cell] = row_index(header_row)
            .and_then(|index| self.rows.get(index))
            .map_or(&[], |row| row.cells.as_slice());
        let mut header_iter = header_cells.iter().peekable();
        for (col, expected) in (1_u32..).zip(headers.iter().copied()) {
            let actual = header_iter.next_if(|cell| cell.col == col).map_or_else(
                || Ok(Cow::Borrowed("")),
                |cell| Self::try_get_display_for_cell(cell, shared_strings),
            )?;
            ensure_fmt!(
                actual.as_ref() == expected,
                "{sheet_name} 헤더가 고정 스키마와 다릅니다: row={header_row}, col={col}, expected={expected}, actual={actual}"
            );
        }
        Ok(())
    }
}
impl Row {
    fn cell(&self, col: u32) -> Option<&Cell> {
        self.cells.get(self.cell_slot(col).ok()?)
    }
    fn cell_slot(&self, col: u32) -> CoreResult<usize, usize> {
        if let Some(dense_index) = col
            .checked_sub(1)
            .and_then(|idx| usize::try_from(idx).ok())
            .filter(|&idx| self.cells.get(idx).is_some_and(|cell| cell.col == col))
        {
            return Ok(dense_index);
        }
        self.cells.binary_search_by_key(&col, |cell| cell.col)
    }
    pub(crate) fn try_copy_style(&self, max_col: u32) -> Result<Self> {
        let cell_count = self.cells.partition_point(|cell| cell.col <= max_col);
        let mut cells =
            try_vec_with_capacity(cell_count, "row style cell 목록 복사 메모리 확보 실패")?;
        cells.extend(self.cells.iter().take(cell_count).map(|cell| Cell {
            col: cell.col,
            inner_xml: String::new(),
            style: cell.style,
            value_type: CellValueType::General,
        }));
        Ok(Self {
            attrs_xml: copy_text(&self.attrs_xml)?,
            cells,
        })
    }
}
fn append_xml_escaped(out: &mut String, text: &str, context: XmlEscapeContext) {
    let mut unescaped_start = 0_usize;
    for (index, &byte) in text.as_bytes().iter().enumerate() {
        if let Some(replacement) = xml_escape_replacement(byte, context) {
            out.push_str(or_abort(text.get(unescaped_start..index)));
            out.push_str(replacement);
            unescaped_start = index.strict_add(1);
        }
    }
    out.push_str(or_abort(text.get(unescaped_start..)));
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
    input_styles
        .get(style as usize)
        .copied()
        .flatten()
        .ok_or_else(|| {
            fmt_err!("worksheet가 참조한 style을 Excel 정규형으로 변환할 수 없습니다: {style}")
        })
}
pub(crate) fn format_excel_ratio_into(out: &mut String, numerator: i128, denominator: i128) {
    const POW5_16: u64 = 152_587_890_625;
    const RATIO_SCALE: u64 = 1_000_000_000;
    let negative = numerator != 0 && numerator.is_negative() != denominator.is_negative();
    let (denominator_abs, numerator_abs) = (denominator.unsigned_abs(), numerator.unsigned_abs());
    let (whole, mut remainder) = (
        numerator_abs.div_euclid(denominator_abs),
        numerator_abs.rem_euclid(denominator_abs),
    );
    if remainder == 0 {
        out.clear();
        if negative {
            out.push('-');
        }
        push_decimal_text!(out, whole);
        return;
    }
    let fast_value = u64::try_from(numerator_abs)
        .ok()
        .zip(u64::try_from(denominator_abs).ok())
        .filter(|&(num, den)| {
            num >= den && num.is_multiple_of(RATIO_SCALE) && den.is_multiple_of(RATIO_SCALE)
        })
        .and_then(|(num, den)| {
            let (sn, sd) = (num.div_euclid(RATIO_SCALE), den.div_euclid(RATIO_SCALE));
            let odd_den = sd >> sd.trailing_zeros();
            (!u128::from(sn.rem_euclid(sd))
                .strict_mul(u128::from(POW5_16))
                .is_multiple_of(u128::from(odd_den)))
            .then(|| u32::try_from(sn).ok().zip(u32::try_from(sd).ok()))
            .flatten()
        })
        .map(|(num, den)| {
            f64::from_bits(
                (f64::from(num) / f64::from(den)).to_bits() ^ (u64::from(negative) << 63_u32),
            )
        });
    let value = if let Some(resolved) = fast_value {
        resolved
    } else {
        out.clear();
        if negative {
            out.push('-');
        }
        push_decimal_text!(out, whole);
        out.push('.');
        for _ in 0_u32..32_u32 {
            remainder = remainder.strict_mul(10);
            let [digit, ..] = remainder.div_euclid(denominator_abs).to_le_bytes();
            out.push(char::from(b'0'.strict_add(digit)));
            remainder = remainder.rem_euclid(denominator_abs);
            if remainder == 0 {
                return;
            }
        }
        out.parse::<f64>().unwrap_or_else(|_| process::abort())
    };
    out.clear();
    append_fmt(out, format_args!("{value:.16e}"));
    let exponent_marker = or_abort(out.rfind('e'));
    let exponent = or_abort(
        or_abort(out.get(exponent_marker.strict_add(1)..))
            .parse::<i32>()
            .ok(),
    );
    let sign_len = usize::from(out.starts_with('-'));
    let point = sign_len.strict_add(or_abort(
        or_abort(out.get(sign_len..exponent_marker)).find('.'),
    ));
    let decimal_position = exponent.strict_add(1);
    out.truncate(exponent_marker);
    out.remove(point);
    let digit_count = out.len().strict_sub(sign_len);
    if decimal_position <= 0_i32 {
        out.insert_str(sign_len, "0.");
        let zero_position = sign_len.strict_add(2);
        for _ in 0..decimal_position.unsigned_abs() {
            out.insert(zero_position, '0');
        }
    } else {
        let position = or_abort(usize::try_from(decimal_position).ok());
        if let Some(pad_zeros) = position.checked_sub(digit_count) {
            out.extend(repeat_n('0', pad_zeros));
        } else {
            out.insert(sign_len.strict_add(position), '.');
        }
    }
    if out.contains('.') {
        out.truncate(out.trim_end_matches('0').trim_suffix('.').len());
    }
}
fn has_edge_whitespace(text: &str) -> bool {
    text.chars().next().is_some_and(char::is_whitespace)
        || text.chars().next_back().is_some_and(char::is_whitespace)
}
pub(crate) const fn is_shared_formula_follower(offset: usize, total: usize) -> bool {
    let group_size = (MAX_SHARED_FORMULA_FOLLOWERS as usize).strict_add(1);
    let group_offset = offset.rem_euclid(group_size);
    let group_start = offset.strict_sub(group_offset);
    group_offset != 0 && total.strict_sub(group_start) >= MIN_SHARED_FORMULA_CELLS as usize
}
pub(crate) fn parse_decimal_scaled(
    text: &str,
    scale_exp: i64,
    strict_commas: bool,
    round_half_away: bool,
) -> Option<i64> {
    let (negative, digits) = text
        .strip_prefix('-')
        .map_or_else(|| (false, text.trim_prefix('+')), |rest| (true, rest));
    let (mantissa, exponent) = match digits.split_once(['e', 'E']) {
        Some((mant, _)) if !strict_commas && mant.contains(',') => return None,
        Some((mant, exp_text)) => (mant, exp_text.parse::<i64>().ok()?),
        None => (digits, 0_i64),
    };
    let int_digits = mantissa
        .bytes()
        .take_while(|&byte| byte != b'.')
        .filter(u8::is_ascii_digit)
        .count();
    let dec_pos = i64::try_from(int_digits)
        .ok()?
        .checked_add(exponent)?
        .checked_add(scale_exp)?;
    let (mut whole, mut pos, mut group_len) = (0_i64, 0_i64, 0_u32);
    let (mut has_group, mut round_up, mut saw_digit, mut seen_dec) = (false, false, false, false);
    for byte in mantissa.bytes() {
        match byte {
            b',' if !seen_dec && strict_commas => {
                if group_len == 0 || (has_group && group_len != 3) || group_len > 3 {
                    return None;
                }
                (has_group, group_len) = (true, 0);
            }
            b',' if !seen_dec => {}
            b'.' if !seen_dec && (!strict_commas || !has_group || group_len == 3) => {
                seen_dec = true;
            }
            b'0'..=b'9' => {
                saw_digit = true;
                group_len = group_len.strict_add(u32::from(!seen_dec));
                let digit = i64::from(byte.strict_sub(b'0'));
                match pos.cmp(&dec_pos) {
                    Ordering::Less => whole = whole.checked_mul(10)?.checked_add(digit)?,
                    Ordering::Equal if round_half_away => round_up = digit >= 5_i64,
                    Ordering::Equal | Ordering::Greater if !round_half_away && digit != 0 => {
                        return None;
                    }
                    Ordering::Equal | Ordering::Greater => {}
                }
                pos = pos.strict_add(1);
            }
            _ => return None,
        }
    }
    if !saw_digit || (strict_commas && has_group && group_len != 3) {
        return None;
    }
    if whole != 0 && dec_pos > pos {
        whole =
            whole.checked_mul(10_i64.checked_pow(u32::try_from(dec_pos.strict_sub(pos)).ok()?)?)?;
    }
    let mag = whole.checked_add(i64::from(round_up))?;
    if negative {
        mag.checked_neg()
    } else {
        Some(mag)
    }
}
fn parse_xml_bool(name: &str, value: &str) -> Result<bool> {
    match value {
        "0" | "false" => Ok(false),
        "1" | "true" => Ok(true),
        _ => Err(fmt_err!(
            "worksheet {name} 속성이 boolean 형식이 아닙니다: {value}"
        )),
    }
}
fn replace_canonical_text(xml: &mut String, old: &str, new: &str, context: &str) -> Result<()> {
    let start = xml
        .find(old)
        .ok_or_else(|| fmt_err!("Excel 정규형의 {context} 표식을 찾지 못했습니다."))?;
    let end = start.strict_add(old.len());
    ensure_fmt!(
        !(xml
            .get(end..)
            .is_some_and(|remaining| remaining.contains(old))),
        "Excel 정규형에 {context} 표식이 여러 개 있습니다."
    );
    xml.replace_range(start..end, new);
    Ok(())
}
fn replace_formula_tag(
    inner_xml: &str,
    span: Range<usize>,
    attrs: &str,
    escaped_body: &str,
    err_ctx: &'static str,
) -> Result<String> {
    let prefix = or_abort(inner_xml.get(..span.start));
    let suffix = or_abort(inner_xml.get(span.end..));
    let length = sum_lengths(&[
        prefix.len(),
        7,
        attrs.len(),
        escaped_body.len(),
        suffix.len(),
    ]);
    let mut updated = try_string_with_capacity(length, err_ctx)?;
    updated.extend([prefix, "<f", attrs, ">", escaped_body, "</f>", suffix]);
    Ok(updated)
}
const fn row_index(row: u32) -> Option<usize> {
    (row as usize).checked_sub(1)
}
fn simple_value_body(inner_xml: &str) -> Option<&str> {
    inner_xml
        .strip_prefix("<v>")
        .and_then(|rest| rest.strip_suffix("</v>"))
        .filter(|body| !body.as_bytes().contains(&b'<'))
}
fn sum_lengths(parts: &[usize]) -> usize {
    parts.iter().copied().fold(0_usize, usize::strict_add)
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
    let bytes = text.as_bytes();
    let mut total = bytes.len();
    for (index, &byte) in bytes.iter().enumerate() {
        match byte {
            0x00..=0x7F => {
                ensure_fmt!(
                    byte >= 0x20 || matches!(byte, b'\t' | b'\n' | b'\r'),
                    "{error_context}: XML 1.0에서 허용되지 않는 문자가 포함되어 있습니다: U+{byte:04X}"
                );
                if let Some(replacement) = xml_escape_replacement(byte, context) {
                    total = total.strict_add(replacement.len().strict_sub(1));
                }
            }
            0xEF => {
                if let Some(&[0xBF, third @ (0xBE | 0xBF)]) =
                    bytes.get(index.strict_add(1)..index.strict_add(3))
                {
                    let code = if third == 0xBE {
                        0xFFFE_u32
                    } else {
                        0xFFFF_u32
                    };
                    bail_fmt!(
                        "{error_context}: XML 1.0에서 허용되지 않는 문자가 포함되어 있습니다: U+{code:04X}"
                    );
                }
            }
            _ => {}
        }
    }
    Ok(total)
}
fn worksheet_row_count(len: usize) -> u32 {
    u32::try_from(len).unwrap_or_else(|_| process::abort())
}
const fn xml_escape_replacement(byte: u8, context: XmlEscapeContext) -> Option<&'static str> {
    match byte {
        b'\t' if matches!(context, XmlEscapeContext::Attribute) => Some("&#x9;"),
        b'\n' if matches!(context, XmlEscapeContext::Attribute) => Some("&#xA;"),
        b'\r' => Some("&#xD;"),
        b'&' => Some("&amp;"),
        b'<' => Some("&lt;"),
        b'>' => Some("&gt;"),
        b'"' if matches!(context, XmlEscapeContext::Attribute) => Some("&quot;"),
        b'\'' if matches!(context, XmlEscapeContext::Attribute) => Some("&apos;"),
        _ => None,
    }
}
