pub(crate) use self::cell_ref::AbsoluteColumnRangeRewriter;
pub(in crate::excel) use self::cell_ref::parse_ref_with_locks;
use self::cell_ref::{
    MAX_A1_COL, MAX_A1_ROW, parse_formula_cell_ref, parse_range_token, ref_with_locks,
    rewrite_formula_cell_refs, with_unlocked_ref_parts,
};
use super::{
    CHANGE_LOG_SHEET_NAME, CHANGE_LOG_SHEET_PATH, MASTER_SHEET_NAME, MASTER_SHEET_PATH,
    SaveVerification, copy_text,
    xlsx_container::{XlsxContainer, validate_worksheet_core_namespaces},
    xml::{
        XmlAttrScanner, XmlScanner, decode_xml_entities, extract_all_tag_text,
        extract_first_tag_text, find_end_tag, find_start_tag, find_tag_end, is_valid_xml_char,
    },
};
use crate::{
    diagnostic::{Result, err, err_with_source},
    sheet_util::parse_i32_str,
};
use alloc::borrow::Cow;
use alloc::collections::{BTreeMap, BTreeSet, btree_map::Entry as BTreeEntry};
use core::{
    cmp::Ordering,
    fmt::{Display, Write as FmtWrite},
    mem,
    range::{Range, RangeFrom, RangeInclusive},
};
use std::collections::HashMap;
use std::path::Path;
mod cell_ref;
const XML_SPACE_PRESERVE_ATTR: &str = " xml:space=\"preserve\"";
const FILTER_DATABASE_NAME: &str = "_xlnm._FilterDatabase";
const PENDING_SHARED_STRING_TYPE: &str = "_fcupdater_shared_string";
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
#[derive(Debug)]
pub(crate) struct Workbook {
    change_log_sheet: Worksheet,
    container: XlsxContainer,
    master_sheet: Worksheet,
    shared_strings: Vec<String>,
    shared_strings_xml_text: String,
    xml_text: String,
}
#[derive(Debug)]
pub(crate) struct Worksheet {
    formula_cells: BTreeSet<(u32, u32)>,
    prefix: String,
    rows: BTreeMap<u32, Row>,
    suffix: String,
}
#[derive(Debug)]
struct XmlTagLocation {
    name: String,
    self_closing: bool,
    span: Range<usize>,
}
#[derive(Debug, Default)]
pub(crate) struct Row {
    attrs: Vec<XmlAttr>,
    cells: BTreeMap<u32, Cell>,
}
#[derive(Debug, Default)]
struct Cell {
    attrs: Vec<XmlAttr>,
    inner_xml: Option<String>,
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
struct ParsedWorksheetRows {
    formula_cells: BTreeSet<(u32, u32)>,
    rows: BTreeMap<u32, Row>,
}
struct WorksheetXmlParser<'xml> {
    xml: &'xml str,
}
#[derive(Clone, Copy)]
enum TagRemovalRule<'rule> {
    All,
    AttrEquals {
        attr_name: &'rule str,
        expected_value: &'rule str,
    },
}
#[derive(Clone, Copy)]
enum XmlEscapeContext {
    Attribute,
    Text,
}
#[derive(Clone, Copy)]
enum XmlReserveMode {
    Additional,
    Exact,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::excel) struct CellReference {
    pub col: u32,
    pub col_locked: bool,
    pub row: u32,
    pub row_locked: bool,
}
struct FormulaCellReference {
    end_index: usize,
    reference: CellReference,
}
struct FormulaRewrite {
    end_index: usize,
    replacement: String,
}
struct RangeTokenParts<'token> {
    end_ref: &'token str,
    start_ref: &'token str,
}
impl Workbook {
    pub(crate) fn change_log_sheet_mut(&mut self) -> (&mut Worksheet, &[String]) {
        (&mut self.change_log_sheet, &self.shared_strings)
    }
    pub(crate) fn from_container(container: XlsxContainer) -> Result<Self> {
        let workbook_xml = container.read_text("xl/workbook.xml")?;
        container.ensure_fixed_sheet_catalog(&workbook_xml)?;
        let shared_strings_xml_text = container.read_shared_strings_text()?;
        let mut shared_strings = Vec::new();
        let mut scanner = XmlScanner::new(&shared_strings_xml_text);
        while let Some(si_tag) = scanner.next_start_named("si") {
            if shared_strings.len() >= MAX_SHARED_STRING_COUNT {
                return Err(err(format!(
                    "sharedStrings entry 개수가 허용 한도({MAX_SHARED_STRING_COUNT})를 초과했습니다."
                )));
            }
            if shared_strings.len() == shared_strings.capacity() {
                shared_strings.try_reserve(1).map_err(|source| {
                    err_with_source("sharedStrings entry 메모리 확보 실패", source)
                })?;
            }
            if si_tag.self_closing() {
                shared_strings.push(String::new());
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
            shared_strings.push(text);
            let Some(si_close_end) = find_tag_end(&shared_strings_xml_text, si_end) else {
                return Err(err("sharedStrings.xml의 </si> 태그가 손상되었습니다."));
            };
            let next_cursor = si_close_end
                .checked_add(1)
                .ok_or_else(|| err("sharedStrings.xml의 다음 <si> 위치 계산에 실패했습니다."))?;
            scanner.skip_to(next_cursor);
        }
        let parse_sheet = |name: &str, path: &str| -> Result<Worksheet> {
            let xml = container.read_text(path)?;
            validate_worksheet_core_namespaces(&xml, name)?;
            let worksheet = WorksheetXmlParser { xml: &xml }.parse()?;
            worksheet.validate_fixed_header(name, &shared_strings)?;
            Ok(worksheet)
        };
        let master_sheet = parse_sheet(MASTER_SHEET_NAME, MASTER_SHEET_PATH)?;
        let change_log_sheet = parse_sheet(CHANGE_LOG_SHEET_NAME, CHANGE_LOG_SHEET_PATH)?;
        Ok(Self {
            change_log_sheet,
            container,
            master_sheet,
            shared_strings,
            shared_strings_xml_text,
            xml_text: workbook_xml,
        })
    }
    fn intern_pending_shared_strings(&mut self) -> Result<()> {
        let existing_table_len = self.shared_strings.len();
        let mut existing_index: Option<HashMap<&str, usize>> = None;
        let mut new_index: HashMap<String, usize> = HashMap::new();
        let mut next_new_idx = self.shared_strings.len();
        let mut interned_any = false;
        let shared_strings = &self.shared_strings;
        for sheet in [&mut self.master_sheet, &mut self.change_log_sheet] {
            for row in sheet.rows.values_mut() {
                for cell in row.cells.values_mut() {
                    if get_attr(&cell.attrs, "t") != Some(PENDING_SHARED_STRING_TYPE) {
                        continue;
                    }
                    let text = cell
                        .inner_xml
                        .take()
                        .ok_or_else(|| err("내부 shared string cell에 값이 없습니다."))?;
                    let existing_index_ref = if let Some(index) = existing_index.as_ref() {
                        index
                    } else {
                        let mut index = HashMap::new();
                        index.try_reserve(existing_table_len).map_err(|source| {
                            err_with_source("기존 shared string index map 메모리 확보 실패", source)
                        })?;
                        for (idx, value) in shared_strings.iter().enumerate() {
                            index.entry(value.as_str()).or_insert(idx);
                        }
                        existing_index.insert(index)
                    };
                    let shared_idx = if let Some(&idx) = existing_index_ref.get(text.as_str()) {
                        idx
                    } else if let Some(&idx) = new_index.get(text.as_str()) {
                        idx
                    } else {
                        let idx = next_new_idx;
                        next_new_idx = next_new_idx.checked_add(1).ok_or_else(|| {
                            err("shared string 신규 index 계산 중 overflow가 발생했습니다.")
                        })?;
                        if new_index.len() == new_index.capacity() {
                            new_index.try_reserve(1).map_err(|source| {
                                err_with_source(
                                    "신규 shared string index map 메모리 확보 실패",
                                    source,
                                )
                            })?;
                        }
                        new_index.insert(text, idx);
                        idx
                    };
                    set_attr(&mut cell.attrs, "t", "s");
                    let shared_idx_u64 = u64::try_from(shared_idx)
                        .map_err(|source| err_with_source("XML 표시값 변환 실패", source))?;
                    cell.inner_xml =
                        Some(build_decimal_display_text_tag("v", None, shared_idx_u64));
                    interned_any = true;
                }
            }
        }
        if !interned_any {
            return Ok(());
        }
        let mut new_strings: Vec<(usize, String)> = Vec::new();
        new_strings
            .try_reserve_exact(new_index.len())
            .map_err(|source| {
                err_with_source("신규 shared string 목록 메모리 확보 실패", source)
            })?;
        new_strings.extend(new_index.into_iter().map(|(value, idx)| (idx, value)));
        new_strings.sort_unstable_by_key(|entry| entry.0);
        self.shared_strings
            .extend(new_strings.into_iter().map(|(_, value)| value));
        let shared_string_reference_count = self.shared_string_reference_count()?;
        self.update_shared_strings_xml_text(
            existing_table_len,
            shared_string_reference_count,
            self.shared_strings.len(),
        )
    }
    pub(crate) fn master_sheet_mut(&mut self) -> (&mut Worksheet, &[String]) {
        (&mut self.master_sheet, &self.shared_strings)
    }
    fn remove_excel_recovery_artifacts(&mut self) -> Result<()> {
        remove_tags_matching(&mut self.xml_text, "fileRecoveryPr", TagRemovalRule::All)?;
        let workbook_rels_path = "xl/_rels/workbook.xml.rels";
        let mut workbook_rels_xml = self.container.read_text(workbook_rels_path)?;
        if remove_tags_matching_attr(
            &mut workbook_rels_xml,
            "Relationship",
            "Type",
            "http://schemas.openxmlformats.org/officeDocument/2006/relationships/calcChain",
        )? {
            self.container
                .write_text(workbook_rels_path, &workbook_rels_xml)?;
        }
        let content_types_path = "[Content_Types].xml";
        let mut content_types_xml = self.container.read_text(content_types_path)?;
        if remove_tags_matching_attr(
            &mut content_types_xml,
            "Override",
            "PartName",
            "/xl/calcChain.xml",
        )? {
            self.container
                .write_text(content_types_path, &content_types_xml)?;
        }
        self.container.remove_calc_chain_if_exists()?;
        Ok(())
    }
    fn request_full_recalculation(&mut self) -> Result<()> {
        let set_calc_pr_attrs = |attrs: &mut Vec<XmlAttr>| {
            set_attr(attrs, "calcMode", "auto");
            set_attr(attrs, "fullCalcOnLoad", "1");
            set_attr(attrs, "forceFullCalc", "1");
            set_attr(attrs, "calcCompleted", "0");
        };
        let out = &mut self.xml_text;
        if let Some(location) = find_start_tag_location(out, "calcPr", 0, "calcPr 태그 이름 복사")?
        {
            if location.name != "calcPr" {
                return Err(err(
                    "고정 workbook은 prefixed calcPr 요소를 지원하지 않습니다.",
                ));
            }
            let (element_span, _) =
                xml_element_ranges(out, &location, "calcPr", "workbook.xml의 calcPr")?;
            let mut attrs = parse_tag_attrs_at(
                out,
                &location,
                "workbook.xml의 calcPr 태그 범위가 손상되었습니다.",
            )?;
            reserve_xml_attrs(
                &mut attrs,
                4,
                "calcPr 속성 목록 추가 메모리 확보 실패",
                XmlReserveMode::Additional,
            )?;
            set_calc_pr_attrs(&mut attrs);
            let new_tag = if location.self_closing {
                build_self_closing_tag("calcPr", &attrs)?
            } else {
                let mut new_tag = build_open_tag("calcPr", &attrs)?;
                new_tag.push_str("</calcPr>");
                new_tag
            };
            out.replace_range(element_span, &new_tag);
        } else {
            let Some(workbook_close_start) = find_end_tag(out, "workbook", 0) else {
                return Err(err("workbook.xml의 workbook 종료 태그를 찾지 못했습니다."));
            };
            let mut attrs = Vec::new();
            reserve_xml_attrs(
                &mut attrs,
                4,
                "calcPr 속성 목록 메모리 확보 실패",
                XmlReserveMode::Exact,
            )?;
            set_calc_pr_attrs(&mut attrs);
            let new_tag = build_self_closing_tag("calcPr", &attrs)?;
            out.insert_str(workbook_close_start, &new_tag);
        }
        Ok(())
    }
    pub(crate) fn save(mut self, target_path: &Path, verification: SaveVerification) -> Result<()> {
        self.intern_pending_shared_strings()?;
        self.request_full_recalculation()?;
        self.remove_excel_recovery_artifacts()?;
        self.container
            .write_text("xl/workbook.xml", &self.xml_text)?;
        self.container
            .write_text("xl/sharedStrings.xml", &self.shared_strings_xml_text)?;
        for (sheet_name, sheet_path, sheet) in [
            (MASTER_SHEET_NAME, MASTER_SHEET_PATH, &self.master_sheet),
            (
                CHANGE_LOG_SHEET_NAME,
                CHANGE_LOG_SHEET_PATH,
                &self.change_log_sheet,
            ),
        ] {
            sheet.validate_fixed_header(sheet_name, &self.shared_strings)?;
            let sheet_xml = sheet.to_xml()?;
            self.container.write_text(sheet_path, &sheet_xml)?;
        }
        self.container.save(target_path, verification)
    }
    fn shared_string_reference_count(&self) -> Result<usize> {
        let mut count = 0_usize;
        for sheet in [&self.master_sheet, &self.change_log_sheet] {
            for row in sheet.rows.values() {
                for cell in row.cells.values() {
                    if get_attr(&cell.attrs, "t") == Some("s") {
                        count = count.checked_add(1).ok_or_else(|| {
                            err("shared string 참조 수 계산 중 overflow가 발생했습니다.")
                        })?;
                    }
                }
            }
        }
        Ok(count)
    }
    pub(crate) fn update_filter_database_defined_name(&mut self, last_data_row: u32) -> Result<()> {
        const QUOTED_SHEET: &str = "'유류비'!";
        const PLAIN_SHEET: &str = "유류비!";
        const REF_PREFIX: &str = "'유류비'!$A$14:$W$";
        let replacement_capacity = REF_PREFIX
            .len()
            .checked_add(u32_decimal_text_len(last_data_row))
            .ok_or_else(|| err("_FilterDatabase ref 용량 계산에 실패했습니다."))?;
        let mut replacement = String::new();
        replacement
            .try_reserve_exact(replacement_capacity)
            .map_err(|source| err_with_source("_FilterDatabase ref 메모리 확보 실패", source))?;
        replacement.push_str(REF_PREFIX);
        push_decimal_text(&mut replacement, last_data_row);
        let span = super::workbook_defined_name_content_span(
            &self.xml_text,
            FILTER_DATABASE_NAME,
            0,
            QUOTED_SHEET,
            PLAIN_SHEET,
        )?;
        self.xml_text.replace_range(span, &replacement);
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
        reserve_xml_attrs(
            &mut attrs,
            2,
            "sharedStrings 속성 목록 추가 메모리 확보 실패",
            XmlReserveMode::Additional,
        )?;
        usize_attr_or(
            &attrs,
            "count",
            shared_string_reference_count,
            "sharedStrings count 해석 실패",
        )?;
        usize_attr_or(
            &attrs,
            "uniqueCount",
            unique_count,
            "sharedStrings uniqueCount 해석 실패",
        )?;
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
        let (replacement, maybe_close_start) = if open_tag.trim_ascii_end().ends_with("/>") {
            let mut replacement = build_open_tag("sst", &attrs)?;
            replacement.push_str(&new_si_xml);
            replacement.push_str("</sst>");
            (replacement, None)
        } else {
            let new_open_tag = build_open_tag("sst", &attrs)?;
            let close_search_from =
                checked_usize_add(open_end, 1, "sharedStrings 종료 태그 검색 시작")?;
            let Some(original_close_start) = find_end_tag(original_xml, "sst", close_search_from)
            else {
                return Err(err("sharedStrings XML에 </sst>가 없습니다."));
            };
            (new_open_tag, Some(original_close_start))
        };
        let mut updated_xml = mem::take(&mut self.shared_strings_xml_text);
        if let Some(close_start) = maybe_close_start {
            updated_xml.insert_str(close_start, &new_si_xml);
        }
        updated_xml.replace_range(open_tag_span, &replacement);
        self.shared_strings_xml_text = updated_xml;
        Ok(())
    }
    pub(super) fn verify_master_address_data_end_row(&self, filter_end_row: u32) -> Result<()> {
        let worksheet = &self.master_sheet;
        let mut actual_end_row = 15;
        for row in worksheet.row_numbers_from(15) {
            let display = worksheet.try_get_display_at(6, row, &self.shared_strings)?;
            if !display.trim().is_empty() {
                actual_end_row = row;
            }
        }
        if filter_end_row != actual_end_row {
            return Err(err(format!(
                "저장 검증 실패: 유류비 autoFilter 마지막 행이 실제 주소 데이터 마지막 행과 다릅니다: filter={filter_end_row}, actual={actual_end_row}"
            )));
        }
        Ok(())
    }
}
impl WorksheetRowParser<'_> {
    fn parse_into(
        &self,
        row: &mut Row,
        formula_cells: &mut BTreeSet<(u32, u32)>,
        cell_count: &mut usize,
    ) -> Result<()> {
        let mut scanner = XmlScanner::new(self.row_body);
        let mut next_col = 1_u32;
        while let Some(cell_info) = scanner.next_start_named("c") {
            let cell_tag_end = cell_info.end();
            let cell_tag = cell_info.raw();
            let mut attrs = parse_tag_attrs(cell_tag)?;
            let col = if let Some(reference_text) = get_attr(&attrs, "r") {
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
                reference.col
            } else {
                next_col
            };
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
            if cell_info.self_closing() {
                if !attrs.is_empty() {
                    self.retain_cell(
                        row,
                        formula_cells,
                        cell_count,
                        col,
                        Cell {
                            attrs,
                            inner_xml: None,
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
            if !attrs.is_empty() || !inner_xml_text.is_empty() {
                let inner_xml = copy_text(inner_xml_text, "row cell 본문 복사")?;
                self.retain_cell(
                    row,
                    formula_cells,
                    cell_count,
                    col,
                    Cell {
                        attrs,
                        inner_xml: Some(inner_xml),
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
        formula_cells: &mut BTreeSet<(u32, u32)>,
        cell_count: &mut usize,
        col: u32,
        cell: Cell,
    ) -> Result<()> {
        if *cell_count >= MAX_WORKSHEET_CELL_COUNT {
            return Err(err(format!(
                "worksheet cell 개수가 허용 한도({MAX_WORKSHEET_CELL_COUNT})를 초과했습니다."
            )));
        }
        match get_attr(&cell.attrs, "t") {
            None | Some("n" | "s" | "str") => {}
            Some(cell_type) => {
                return Err(err(format!(
                    "고정 workbook에서 지원하지 않는 cell type입니다: row={}, col={col}, type={cell_type}",
                    self.row_num
                )));
            }
        }
        let has_formula = if let Some(inner_xml) = cell.inner_xml.as_deref()
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
            if get_attr(&parse_tag_attrs(formula_open)?, "t") == Some("shared") {
                return Err(err(format!(
                    "고정 workbook은 shared formula를 지원하지 않습니다: row={}, col={col}",
                    self.row_num
                )));
            }
            if find_start_tag(inner_xml, "v", 0).is_none() {
                return Err(err(format!(
                    "고정 workbook formula cache가 없습니다: row={}, col={col}",
                    self.row_num
                )));
            }
            true
        } else {
            false
        };
        if has_formula {
            formula_cells.insert((self.row_num, col));
        }
        if row.cells.insert(col, cell).is_some() {
            return Err(err(row_col_error(
                "중복 cell reference가 있습니다. (row=",
                self.row_num,
                col,
            )));
        }
        *cell_count = (*cell_count)
            .checked_add(1)
            .ok_or_else(|| err("worksheet cell 수 계산 실패"))?;
        Ok(())
    }
}
impl WorksheetXmlParser<'_> {
    fn collect_rows(&self, body_span: Range<usize>) -> Result<ParsedWorksheetRows> {
        let Some(body) = self.xml.get(body_span) else {
            return Err(err("worksheet XML body 범위가 손상되었습니다."));
        };
        let mut rows: BTreeMap<u32, Row> = BTreeMap::new();
        let mut formula_cells = BTreeSet::new();
        let mut cell_count = 0_usize;
        let mut previous_row_num = 0_u32;
        let mut scanner = XmlScanner::new(body);
        while let Some(row_info) = scanner.next_start_named("row") {
            let row_tag_end = row_info.end();
            let mut row_attrs = parse_tag_attrs(row_info.raw())?;
            let row_num = if let Some(row_num_text) = get_attr(&row_attrs, "r") {
                parse_positive_u32_decimal(
                    row_num_text,
                    "worksheet row 번호가 양의 10진수 형식이 아닙니다.",
                    "worksheet row 번호 해석 실패",
                    "worksheet row 번호는 1 이상이어야 합니다.",
                )?
            } else {
                previous_row_num.checked_add(1).ok_or_else(|| {
                    err("worksheet row 번호 자동 증가 중 overflow가 발생했습니다.")
                })?
            };
            if !(1..=MAX_A1_ROW).contains(&row_num) {
                return Err(err(format!(
                    "worksheet row 번호가 Excel 범위를 벗어났습니다: {row_num}"
                )));
            }
            if row_num <= previous_row_num {
                return Err(err(format!(
                    "worksheet row 순서는 오름차순이어야 합니다: previous={previous_row_num}, current={row_num}"
                )));
            }
            previous_row_num = row_num;
            remove_attr(&mut row_attrs, "r");
            if row_info.self_closing() {
                if row_attrs.is_empty() {
                    continue;
                }
                if rows
                    .insert(
                        row_num,
                        Row {
                            attrs: row_attrs,
                            cells: BTreeMap::new(),
                        },
                    )
                    .is_some()
                {
                    return Err(err(row_only_error(
                        "중복 worksheet row가 있습니다. (row=",
                        row_num,
                    )));
                }
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
                attrs: row_attrs,
                cells: BTreeMap::new(),
            };
            WorksheetRowParser { row_body, row_num }.parse_into(
                &mut row,
                &mut formula_cells,
                &mut cell_count,
            )?;
            if (!row.attrs.is_empty() || !row.cells.is_empty())
                && rows.insert(row_num, row).is_some()
            {
                return Err(err(row_only_error(
                    "중복 worksheet row가 있습니다. (row=",
                    row_num,
                )));
            }
            let row_close_end = find_tag_end(body, row_body_end)
                .ok_or_else(|| err("sheetData row 종료 태그가 손상되었습니다."))?;
            scanner.skip_to(checked_usize_add(
                row_close_end,
                1,
                "sheetData row 다음 cursor",
            )?);
        }
        Ok(ParsedWorksheetRows {
            formula_cells,
            rows,
        })
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
        let ParsedWorksheetRows {
            formula_cells,
            rows,
        } = self.collect_rows(sheet_data_body_span)?;
        Ok(Worksheet {
            formula_cells,
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
        let clear_start = rows.start;
        let clear_last = rows.last;
        for (_, row_obj) in self.rows.range_mut(rows) {
            for (_, cell) in row_obj.cells.range_mut(..=max_col) {
                remove_attr(&mut cell.attrs, "t");
                cell.inner_xml = None;
            }
        }
        self.formula_cells
            .extract_if(
                RangeInclusive {
                    start: (clear_start, 0),
                    last: (clear_last, max_col),
                },
                |_| true,
            )
            .for_each(drop);
    }
    pub(crate) fn clear_formula_cached_values_in_range(
        &mut self,
        rows: RangeInclusive<u32>,
    ) -> Result<()> {
        let rows_map = &mut self.rows;
        let formula_cells = &mut self.formula_cells;
        let mut result = Ok(());
        formula_cells
            .extract_if(
                RangeInclusive {
                    start: (rows.start, 0),
                    last: (rows.last, u32::MAX),
                },
                |&(row_num, col)| {
                    if result.is_err() {
                        return false;
                    }
                    let Some(inner) = rows_map
                        .get_mut(&row_num)
                        .and_then(|row| row.cells.get_mut(&col))
                        .and_then(|cell| cell.inner_xml.as_mut())
                    else {
                        return true;
                    };
                    if find_start_tag(inner, "f", 0).is_none() {
                        return true;
                    }
                    match replace_first_tag_text(inner, "v", "") {
                        Ok(true) => false,
                        Ok(false) => match append_peer_text_tag(inner, "f", "v", "") {
                            Ok(()) => false,
                            Err(error) => {
                                result = Err(error);
                                false
                            }
                        },
                        Err(error) => {
                            result = Err(error);
                            false
                        }
                    }
                },
            )
            .for_each(drop);
        result
    }
    fn clear_formula_index_for_row(&mut self, row: u32) {
        self.formula_cells
            .extract_if(
                RangeInclusive {
                    start: (row, 0),
                    last: (row, u32::MAX),
                },
                |_| true,
            )
            .for_each(drop);
    }
    pub(crate) fn copy_row_style(
        &mut self,
        source_row: u32,
        target_row: u32,
        max_col: u32,
    ) -> Result<()> {
        let Some(src) = self.rows.get(&source_row) else {
            return Ok(());
        };
        let mut copied = src.copy_with_row_mapping(&|row_num| {
            Ok(if row_num == source_row {
                target_row
            } else {
                row_num
            })
        })?;
        if let Some(first_removed_col) = max_col.checked_add(1) {
            copied
                .cells
                .extract_if(
                    RangeFrom {
                        start: first_removed_col,
                    },
                    |_, _| true,
                )
                .for_each(drop);
        }
        for cell in copied.cells.values_mut() {
            let Some(inner) = cell.inner_xml.as_mut() else {
                remove_attr(&mut cell.attrs, "t");
                continue;
            };
            if extract_first_tag_text(inner, "f")?.is_none() {
                remove_attr(&mut cell.attrs, "t");
                cell.inner_xml = None;
            }
        }
        self.rows.insert(target_row, copied);
        self.reindex_formula_row(target_row);
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
        while let Some(location) = find_start_tag_location(
            out,
            "conditionalFormatting",
            cursor,
            "conditionalFormatting 태그 이름 복사",
        )? {
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
                let range_parts = parse_range_token(token);
                let Some(start_reference) = parse_ref_with_locks(range_parts.start_ref) else {
                    ranges_out.push(Cow::Borrowed(token));
                    continue;
                };
                let Some(end_reference) = parse_ref_with_locks(range_parts.end_ref) else {
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
                build_self_closing_tag(&location.name, &attrs)?
            } else {
                build_open_tag(&location.name, &attrs)?
            };
            out.replace_range(location.span, &new_tag);
            cursor =
                checked_usize_add(cf_start, new_tag.len(), "conditionalFormatting 다음 cursor")?;
        }
        Ok(())
    }
    fn fixed_master_auto_filter(&self) -> Result<(XmlTagLocation, Vec<XmlAttr>)> {
        let xml = &self.suffix;
        let location = find_start_tag_location(xml, "autoFilter", 0, "autoFilter 태그 이름 복사")?
            .ok_or_else(|| err("worksheet XML의 autoFilter 태그를 찾지 못했습니다."))?;
        if find_start_tag_location(
            xml,
            "autoFilter",
            location.span.end,
            "autoFilter 태그 이름 복사",
        )?
        .is_some()
        {
            return Err(err("worksheet XML에 autoFilter 태그가 중복되어 있습니다."));
        }
        let attrs = parse_tag_attrs_at(
            xml,
            &location,
            "worksheet XML의 autoFilter 태그 범위가 손상되었습니다.",
        )?;
        let existing_ref = get_attr(&attrs, "ref")
            .ok_or_else(|| err("worksheet autoFilter ref 속성이 없습니다."))?;
        let range = parse_range_token(existing_ref);
        let start_reference = parse_ref_with_locks(range.start_ref)
            .ok_or_else(|| err("worksheet autoFilter 시작 reference 해석 실패"))?;
        let end_reference = parse_ref_with_locks(range.end_ref)
            .ok_or_else(|| err("worksheet autoFilter 끝 reference 해석 실패"))?;
        if (start_reference.col, start_reference.row) != (1, 14)
            || end_reference.col != 23
            || end_reference.row < 14
        {
            return Err(err(format!(
                "worksheet autoFilter 범위가 고정 스키마와 다릅니다: {existing_ref}"
            )));
        }
        Ok((location, attrs))
    }
    pub(crate) fn get_i32_at(
        &self,
        col: u32,
        row: u32,
        shared_strings: &[String],
    ) -> Result<Option<i32>> {
        let text = self.try_get_display_at(col, row, shared_strings)?;
        Ok(parse_i32_str(&text))
    }
    fn get_or_create_cell_mut(
        rows: &mut BTreeMap<u32, Row>,
        col: u32,
        row: u32,
    ) -> Result<&mut Cell> {
        let row_obj = match rows.entry(row) {
            BTreeEntry::Occupied(entry) => entry.into_mut(),
            BTreeEntry::Vacant(entry) => entry.insert(Row {
                attrs: Vec::new(),
                cells: BTreeMap::new(),
            }),
        };
        Ok(match row_obj.cells.entry(col) {
            BTreeEntry::Occupied(entry) => entry.into_mut(),
            BTreeEntry::Vacant(entry) => {
                let mut attrs = Vec::new();
                attrs
                    .try_reserve_exact(1)
                    .map_err(|source| err_with_source("cell 속성 목록 메모리 확보 실패", source))?;
                attrs.push(owned_attr("s", "0"));
                entry.insert(Cell {
                    attrs,
                    inner_xml: None,
                })
            }
        })
    }
    pub(crate) fn has_any_row_format(&self, row: u32, max_col: u32) -> bool {
        self.rows.get(&row).is_some_and(|row_obj| {
            !row_obj.attrs.is_empty()
                || (max_col > 0 && row_obj.cells.range(1..=max_col).next().is_some())
        })
    }
    pub(crate) fn has_row(&self, row: u32) -> bool {
        self.rows.contains_key(&row)
    }
    fn mark_cell_formula_state(&mut self, col: u32, row: u32) {
        let has_formula = self
            .rows
            .get(&row)
            .and_then(|row_obj| row_obj.cells.get(&col))
            .is_some_and(cell_has_formula);
        if has_formula {
            self.formula_cells.insert((row, col));
        } else {
            self.formula_cells.remove(&(row, col));
        }
    }
    pub(crate) fn max_cell_col(&self) -> u32 {
        self.rows
            .values()
            .filter_map(|row| row.cells.last_key_value().map(|(&col, _)| col))
            .max()
            .unwrap_or(1)
    }
    fn prune_col_definitions_after_col(&mut self, max_col: u32) -> Result<()> {
        let mut cursor = 0_usize;
        while let Some(location) =
            find_start_tag_location(&self.prefix, "col", cursor, "col 정의 태그 이름 복사")?
        {
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
                let new_tag = build_self_closing_tag(&location.name, &attrs)?;
                self.prefix.replace_range(element_span, &new_tag);
                cursor = checked_usize_add(col_start, new_tag.len(), "col 정의 다음 cursor")?;
                continue;
            }
            cursor = element_span.end;
        }
        Ok(())
    }
    pub(crate) fn prune_empty_style_artifacts_after_col(&mut self, max_col: u32) -> Result<()> {
        let mut cols_to_remove = Vec::new();
        for row in self.rows.values_mut() {
            cols_to_remove.clear();
            for (&col, cell) in &row.cells {
                if col <= max_col {
                    continue;
                }
                let has_payload = if let Some(inner) = cell.inner_xml.as_deref() {
                    if find_start_tag(inner, "f", 0).is_some() {
                        true
                    } else if let Some(raw_value) = extract_first_tag_text(inner, "v")? {
                        let value = decode_xml_entities(raw_value)?;
                        !value.trim().is_empty()
                    } else {
                        extract_all_tag_text(inner, "t")?.is_some_and(|text| !text.is_empty())
                    }
                } else {
                    false
                };
                if !has_payload {
                    if cols_to_remove.len() == cols_to_remove.capacity() {
                        cols_to_remove.try_reserve(1).map_err(|source| {
                            err_with_source("빈 style cell 제거 목록 메모리 확보 실패", source)
                        })?;
                    }
                    cols_to_remove.push(col);
                }
            }
            for col in &cols_to_remove {
                row.cells.remove(col);
            }
        }
        self.formula_cells = formula_cells_from_rows(&self.rows);
        self.prune_col_definitions_after_col(max_col)
    }
    fn reindex_formula_row(&mut self, row: u32) {
        self.clear_formula_index_for_row(row);
        if let Some(row_obj) = self.rows.get(&row) {
            for (&col, cell) in &row_obj.cells {
                if cell_has_formula(cell) {
                    self.formula_cells.insert((row, col));
                }
            }
        }
    }
    pub(crate) fn replace_rows(&mut self, rows: BTreeMap<u32, Row>) {
        self.formula_cells = formula_cells_from_rows(&rows);
        self.rows = rows;
    }
    pub(crate) fn row_count(&self) -> usize {
        self.rows.len()
    }
    pub(crate) fn row_has_any_data(
        &self,
        row: u32,
        cols: &[u32],
        shared_strings: &[String],
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
    pub(crate) fn row_numbers_from(&self, start: u32) -> impl DoubleEndedIterator<Item = u32> + '_ {
        self.rows.range(start..).map(|(&row, _)| row)
    }
    pub(crate) fn set_formula_at(&mut self, col: u32, row: u32, formula: &str) -> Result<()> {
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        remove_attr(&mut cell.attrs, "t");
        let formula_text =
            try_xml_escape_text(formula, XmlEscapeContext::Text, "formula XML escape")?;
        if let Some(inner) = cell.inner_xml.as_mut() {
            if find_start_tag(inner, "f", 0).is_some() {
                replace_first_tag_text(inner, "f", &formula_text)?;
                if !replace_first_tag_text(inner, "v", "")? {
                    append_peer_text_tag(inner, "f", "v", "")?;
                }
            } else if inner.trim().is_empty() {
                *inner = build_formula_with_empty_value("f", "v", &formula_text)?;
            } else {
                return Err(err("cell formula 태그를 찾지 못했습니다."));
            }
        } else {
            cell.inner_xml = Some(build_formula_with_empty_value("f", "v", &formula_text)?);
        }
        self.formula_cells.insert((row, col));
        Ok(())
    }
    pub(crate) fn set_formula_cached_value_at(
        &mut self,
        col: u32,
        row: u32,
        value: Option<&str>,
        cell_type: Option<&str>,
    ) -> Result<()> {
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        match cell_type {
            Some(value_type) => set_attr(&mut cell.attrs, "t", value_type),
            None => remove_attr(&mut cell.attrs, "t"),
        }
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
        if !replace_first_tag_text(inner, "v", value_text)? {
            append_peer_text_tag(inner, "f", "v", value_text)?;
        }
        self.mark_cell_formula_state(col, row);
        Ok(())
    }
    pub(crate) fn set_i32_at(&mut self, col: u32, row: u32, value: Option<i32>) -> Result<()> {
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        remove_attr(&mut cell.attrs, "t");
        if let Some(numeric_value) = value {
            cell.inner_xml = Some(build_decimal_display_text_tag(
                "v",
                numeric_value.is_negative().then_some('-'),
                u64::from(numeric_value.unsigned_abs()),
            ));
        } else {
            cell.inner_xml = None;
        }
        self.formula_cells.remove(&(row, col));
        Ok(())
    }
    pub(crate) fn set_string_at(&mut self, col: u32, row: u32, value: &str) -> Result<()> {
        let cell = Self::get_or_create_cell_mut(&mut self.rows, col, row)?;
        set_attr(&mut cell.attrs, "t", PENDING_SHARED_STRING_TYPE);
        cell.inner_xml = Some(copy_text(value, "shared string 값 복사")?);
        self.formula_cells.remove(&(row, col));
        Ok(())
    }
    pub(crate) fn take_rows(&mut self) -> BTreeMap<u32, Row> {
        self.formula_cells.clear();
        mem::take(&mut self.rows)
    }
    fn to_xml(&self) -> Result<String> {
        let cell_name = "c";
        let row_name = "row";
        let estimated_capacity = (|| {
            let cell_markup_len =
                checked_capacity(&["< r=\"\"></>".len(), cell_name.len(), cell_name.len()])?;
            let row_markup_len =
                checked_capacity(&["< r=\"\"></>".len(), row_name.len(), row_name.len()])?;
            let mut capacity = checked_capacity(&[self.prefix.len(), self.suffix.len()])?;
            for (&row_num, row) in &self.rows {
                capacity =
                    checked_capacity(&[capacity, row_markup_len, u32_decimal_text_len(row_num)])?;
                let row_attrs_len = row.attrs.iter().try_fold(0_usize, |sum, attr| {
                    checked_capacity(&[
                        sum,
                        " ".len(),
                        attr.name.len(),
                        "=\"".len(),
                        xml_escaped_len(&attr.value, XmlEscapeContext::Attribute)?,
                        "\"".len(),
                    ])
                })?;
                capacity = capacity.checked_add(row_attrs_len)?;
                for (&col, cell) in &row.cells {
                    let cell_ref_len =
                        with_unlocked_ref_parts(col, row_num, |col_text, row_number| {
                            col_text.len().checked_add(u32_decimal_text_len(row_number))
                        })
                        .ok()??;
                    capacity = checked_capacity(&[capacity, cell_markup_len, cell_ref_len])?;
                    let cell_attrs_len = cell.attrs.iter().try_fold(0_usize, |sum, attr| {
                        checked_capacity(&[
                            sum,
                            " ".len(),
                            attr.name.len(),
                            "=\"".len(),
                            xml_escaped_len(&attr.value, XmlEscapeContext::Attribute)?,
                            "\"".len(),
                        ])
                    })?;
                    capacity = capacity.checked_add(cell_attrs_len)?;
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
        for (&row_num, row) in &self.rows {
            out.push('<');
            out.push_str(row_name);
            out.push_str(" r=\"");
            push_decimal_text(&mut out, row_num);
            out.push('"');
            push_sorted_attrs_xml(&mut out, &row.attrs)?;
            if row.cells.is_empty() {
                out.push_str("/>");
                continue;
            }
            out.push('>');
            for (&col, cell) in &row.cells {
                out.push('<');
                out.push_str(cell_name);
                out.push_str(" r=\"");
                with_unlocked_ref_parts(col, row_num, |col_text, row_number| {
                    out.push_str(col_text);
                    push_decimal_text(&mut out, row_number);
                })?;
                out.push('"');
                push_sorted_attrs_xml(&mut out, &cell.attrs)?;
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
        Ok(out)
    }
    pub(crate) fn truncate_rows_after(&mut self, last_row_to_keep: u32) -> Result<()> {
        let remove_start = last_row_to_keep
            .checked_add(1)
            .ok_or_else(|| err("worksheet row 제거 시작 행 계산 중 overflow가 발생했습니다."))?;
        if self.rows.split_off(&remove_start).is_empty() {
            return Ok(());
        }
        self.formula_cells = formula_cells_from_rows(&self.rows);
        Ok(())
    }
    pub(crate) fn try_get_display_at<'text>(
        &'text self,
        col: u32,
        row: u32,
        shared_strings: &'text [String],
    ) -> Result<Cow<'text, str>> {
        let Some(row_obj) = self.rows.get(&row) else {
            return Ok(Cow::Borrowed(""));
        };
        let Some(cell) = row_obj.cells.get(&col) else {
            return Ok(Cow::Borrowed(""));
        };
        let cell_type = get_attr(&cell.attrs, "t");
        let inner = cell.inner_xml.as_deref().unwrap_or("");
        match cell_type {
            Some(PENDING_SHARED_STRING_TYPE) => Ok(Cow::Borrowed(inner)),
            ordinary_type => match ordinary_type {
                Some("s") => {
                    let raw_v = extract_first_tag_text(inner, "v")?
                        .ok_or_else(|| err("shared string cell에 v 태그가 없습니다."))?;
                    let idx = parse_usize_decimal(raw_v, "shared string index 해석 실패")?;
                    shared_strings
                        .get(idx)
                        .map(|value| Cow::Borrowed(value.as_str()))
                        .ok_or_else(|| err(format!("shared string index 범위 오류: {idx}")))
                }
                Some("b") => {
                    let raw_v = extract_first_tag_text(inner, "v")?
                        .ok_or_else(|| err("boolean cell에 v 태그가 없습니다."))?;
                    match raw_v {
                        "0" => Ok(Cow::Borrowed("FALSE")),
                        "1" => Ok(Cow::Borrowed("TRUE")),
                        value => Err(err(format!("boolean cell 값이 비정상입니다: {value}"))),
                    }
                }
                _ => {
                    let raw_v = extract_first_tag_text(inner, "v")?.unwrap_or("");
                    decode_xml_entities(raw_v)
                }
            },
        }
    }
    pub(crate) fn try_get_formula_at(&self, col: u32, row: u32) -> Result<Option<Cow<'_, str>>> {
        let Some(inner) = self
            .rows
            .get(&row)
            .and_then(|row_obj| row_obj.cells.get(&col))
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
        let (location, mut attrs) = self.fixed_master_auto_filter()?;
        let out = &mut self.suffix;
        let new_ref = build_ref_range(
            "A",
            RangeInclusive {
                start: 14,
                last: last_data_row.max(14),
            },
            23,
        )?;
        reserve_xml_attrs(
            &mut attrs,
            1,
            "autoFilter 속성 목록 추가 메모리 확보 실패",
            XmlReserveMode::Additional,
        )?;
        set_attr(&mut attrs, "ref", new_ref);
        let new_tag = if location.self_closing {
            build_self_closing_tag(&location.name, &attrs)?
        } else {
            build_open_tag(&location.name, &attrs)?
        };
        out.replace_range(location.span, &new_tag);
        Ok(())
    }
    pub(crate) fn update_dimension(&mut self) -> Result<()> {
        let mut max_row = 1_u32;
        let mut max_col = 1_u32;
        for (&row_num, row) in &self.rows {
            if let Some((&col, _)) = row.cells.last_key_value() {
                max_row = max_row.max(row_num);
                max_col = max_col.max(col);
            }
        }
        if let Some(dim_location) =
            find_start_tag_location(&self.prefix, "dimension", 0, "dimension 태그 이름 복사")?
        {
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
            reserve_xml_attrs(
                &mut attrs,
                1,
                "dimension 속성 목록 추가 메모리 확보 실패",
                XmlReserveMode::Additional,
            )?;
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
            let new_tag = build_self_closing_tag(&dim_location.name, &attrs)?;
            self.prefix.replace_range(element_span, &new_tag);
        }
        Ok(())
    }
    fn validate_fixed_header(&self, sheet_name: &str, shared_strings: &[String]) -> Result<()> {
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
}
impl Row {
    pub(crate) fn copy_with_row_mapping(
        &self,
        resolver: &dyn Fn(u32) -> Result<u32>,
    ) -> Result<Self> {
        let mut cells = BTreeMap::new();
        for (&col, cell) in &self.cells {
            let inner_xml = match cell.inner_xml.as_deref() {
                Some(inner) => Some(copy_text(inner, "cell inner XML 복사")?),
                None => None,
            };
            cells.insert(
                col,
                Cell {
                    attrs: copy_attrs(&cell.attrs, "cell attribute 복사")?,
                    inner_xml,
                },
            );
        }
        let mut row = Self {
            attrs: copy_attrs(&self.attrs, "row attribute 복사")?,
            cells,
        };
        remap_formula_rows(&mut row, resolver)?;
        Ok(row)
    }
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
fn cell_has_formula(cell: &Cell) -> bool {
    cell.inner_xml
        .as_deref()
        .is_some_and(|inner| find_start_tag(inner, "f", 0).is_some())
}
fn copy_attrs(attrs: &[XmlAttr], context: &'static str) -> Result<Vec<XmlAttr>> {
    let mut out = Vec::new();
    out.try_reserve_exact(attrs.len())
        .map_err(|source| err_with_source(format!("{context} 메모리 확보 실패"), source))?;
    for attr in attrs {
        out.push(XmlAttr {
            name: Cow::Owned(copy_text(attr.name.as_ref(), context)?),
            value: copy_text(&attr.value, context)?,
        });
    }
    Ok(out)
}
fn find_start_tag_location(
    xml: &str,
    tag_name: &str,
    from: usize,
    context: &'static str,
) -> Result<Option<XmlTagLocation>> {
    let mut scanner = XmlScanner::new(xml);
    scanner.skip_to(from);
    let Some(tag) = scanner.next_start_named(tag_name) else {
        return Ok(None);
    };
    let tag_end = checked_usize_add(tag.end(), 1, context)?;
    Ok(Some(XmlTagLocation {
        name: copy_text(tag.name(), context)?,
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
    let (element_span, body_span) = xml_element_ranges(xml, location, local_name, context)?;
    let body = xml
        .get(body_span)
        .ok_or_else(|| err(format!("{context} 본문 범위가 손상되었습니다.")))?;
    if !body.trim().is_empty() {
        return Err(err(format!("{context}에 예상하지 않은 본문이 있습니다.")));
    }
    Ok(element_span)
}
fn xml_element_ranges(
    xml: &str,
    location: &XmlTagLocation,
    local_name: &str,
    context: &str,
) -> Result<(Range<usize>, Range<usize>)> {
    let body_start = location.span.end;
    if location.self_closing {
        return Ok((
            location.span,
            Range {
                start: body_start,
                end: body_start,
            },
        ));
    }
    let body_end = find_end_tag(xml, local_name, body_start)
        .ok_or_else(|| err(format!("{context} 종료 태그를 찾지 못했습니다.")))?;
    let close_end = find_tag_end(xml, body_end)
        .and_then(|end| end.checked_add(1))
        .ok_or_else(|| err(format!("{context} 종료 태그가 손상되었습니다.")))?;
    Ok((
        Range {
            start: location.span.start,
            end: close_end,
        },
        Range {
            start: body_start,
            end: body_end,
        },
    ))
}
pub(crate) fn remap_formula_rows(
    row: &mut Row,
    resolver: &dyn Fn(u32) -> Result<u32>,
) -> Result<()> {
    for cell in row.cells.values_mut() {
        if let Some(inner) = cell.inner_xml.as_mut()
            && let Some(text) = extract_first_tag_text(inner, "f")?
        {
            let decoded = decode_xml_entities(text)?;
            let rewrite_result = rewrite_formula_cell_refs(decoded.as_ref(), |chars, start| {
                let Some(parsed) = parse_formula_cell_ref(chars, start) else {
                    return Ok(None);
                };
                let reference = parsed.reference;
                let updated_row = if reference.row_locked {
                    reference.row
                } else {
                    resolver(reference.row)?
                };
                if updated_row == reference.row {
                    return Ok(None);
                }
                Ok(Some(FormulaRewrite {
                    end_index: parsed.end_index,
                    replacement: ref_with_locks(CellReference {
                        row: updated_row,
                        ..reference
                    })?,
                }))
            });
            let Some(rewritten) = rewrite_result? else {
                continue;
            };
            let encoded =
                try_xml_escape_text(&rewritten, XmlEscapeContext::Text, "formula XML escape")?;
            replace_first_tag_text(inner, "f", &encoded)?;
        }
    }
    Ok(())
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
fn push_sorted_attrs_xml(out: &mut String, attrs: &[XmlAttr]) -> Result<()> {
    if attrs.len() == 1
        && let Some(attr) = attrs.first()
    {
        push_attr_xml(out, attr);
        return Ok(());
    }
    if attrs
        .iter()
        .zip(attrs.iter().skip(1))
        .all(|(left, right)| attr_cmp(left, right) != Ordering::Greater)
    {
        for attr in attrs {
            push_attr_xml(out, attr);
        }
        return Ok(());
    }
    let mut sorted_attrs = Vec::new();
    sorted_attrs
        .try_reserve_exact(attrs.len())
        .map_err(|source| err_with_source("XML attribute 정렬 목록 메모리 확보 실패", source))?;
    sorted_attrs.extend(attrs.iter());
    sorted_attrs.sort_unstable_by(|left, right| attr_cmp(left, right));
    for attr in sorted_attrs {
        push_attr_xml(out, attr);
    }
    Ok(())
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
            reserve_xml_attrs(
                &mut out,
                1,
                "XML 속성 목록 추가 메모리 확보 실패",
                XmlReserveMode::Additional,
            )?;
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
fn reserve_xml_attrs(
    attrs: &mut Vec<XmlAttr>,
    additional: usize,
    context: &str,
    mode: XmlReserveMode,
) -> Result<()> {
    let reserve_result = match mode {
        XmlReserveMode::Additional => attrs.try_reserve(additional),
        XmlReserveMode::Exact => attrs.try_reserve_exact(additional),
    };
    reserve_result
        .map_err(|source| err_with_source(format!("{context}: {additional} entries"), source))
}
fn get_attr<'attrs>(attrs: &'attrs [XmlAttr], name: &str) -> Option<&'attrs str> {
    attrs
        .iter()
        .find(|attr| attr.name == name)
        .map(|attr| attr.value.as_str())
}
fn formula_cells_from_rows(rows: &BTreeMap<u32, Row>) -> BTreeSet<(u32, u32)> {
    let mut formula_cells = BTreeSet::new();
    for (&row_num, row) in rows {
        for (&col, cell) in &row.cells {
            if cell_has_formula(cell) {
                formula_cells.insert((row_num, col));
            }
        }
    }
    formula_cells
}
fn usize_attr_or(
    attrs: &[XmlAttr],
    name: &str,
    default: usize,
    context: &'static str,
) -> Result<usize> {
    get_attr(attrs, name).map_or(Ok(default), |value| parse_usize_decimal(value, context))
}
fn parse_usize_decimal(value: &str, context: &'static str) -> Result<usize> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(err(format!("{context}: 음이 아닌 10진수 형식이 아닙니다.")));
    }
    value
        .parse::<usize>()
        .map_err(|source| err_with_source(context, source))
}
fn parse_positive_u32_decimal(
    value: &str,
    format_error: &'static str,
    parse_context: &'static str,
    zero_error: &'static str,
) -> Result<u32> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(err(format_error));
    }
    let parsed = value
        .parse::<u32>()
        .map_err(|source| err_with_source(parse_context, source))?;
    if parsed == 0 {
        return Err(err(zero_error));
    }
    Ok(parsed)
}
fn owned_attr(name: &'static str, value: impl Into<String>) -> XmlAttr {
    XmlAttr {
        name: Cow::Borrowed(name),
        value: value.into(),
    }
}
fn set_attr(attrs: &mut Vec<XmlAttr>, name: &'static str, value_in: impl Into<String>) {
    let value = value_in.into();
    if let Some(attr) = attrs.iter_mut().find(|attr| attr.name == name) {
        attr.value = value;
    } else {
        attrs.push(owned_attr(name, value));
    }
}
fn remove_attr(attrs: &mut Vec<XmlAttr>, name: &str) {
    attrs.retain(|attr| attr.name != name);
}
fn remove_tags_matching_attr(
    xml: &mut String,
    tag_name: &str,
    attr_name: &str,
    expected_value: &str,
) -> Result<bool> {
    remove_tags_matching(
        xml,
        tag_name,
        TagRemovalRule::AttrEquals {
            attr_name,
            expected_value,
        },
    )
}
fn remove_tags_matching(
    out: &mut String,
    tag_name: &str,
    rule: TagRemovalRule<'_>,
) -> Result<bool> {
    let mut changed = false;
    let mut cursor = 0_usize;
    while let Some(tag_start) = find_start_tag(out, tag_name, cursor) {
        let Some(tag_end) = find_tag_end(out, tag_start) else {
            return Err(err(tag_error_message(tag_name, " 태그가 손상되었습니다.")));
        };
        let tag_open_span = RangeInclusive {
            start: tag_start,
            last: tag_end,
        };
        let (attrs, self_closing) = {
            let tag_xml = out
                .get(tag_open_span)
                .ok_or_else(|| err(tag_error_message(tag_name, " 태그 범위가 손상되었습니다.")))?;
            (
                parse_tag_attrs(tag_xml)?,
                tag_xml.trim_ascii_end().ends_with("/>"),
            )
        };
        let next_cursor = checked_usize_add(tag_end, 1, "XML 태그 다음 cursor")?;
        let remove_tag = match rule {
            TagRemovalRule::All => true,
            TagRemovalRule::AttrEquals {
                attr_name,
                expected_value,
            } => get_attr(&attrs, attr_name) == Some(expected_value),
        };
        if remove_tag {
            let tag_end_exclusive = if self_closing {
                checked_usize_add(tag_end, 1, "XML self-closing 태그 끝")?
            } else {
                let close_search_from = checked_usize_add(tag_end, 1, "XML 종료 태그 검색 시작")?;
                let Some(close_start) = find_end_tag(out, tag_name, close_search_from) else {
                    return Err(err(tag_error_message(
                        tag_name,
                        " 종료 태그를 찾지 못했습니다.",
                    )));
                };
                let Some(close_end) = find_tag_end(out, close_start) else {
                    return Err(err(tag_error_message(
                        tag_name,
                        " 종료 태그가 손상되었습니다.",
                    )));
                };
                checked_usize_add(close_end, 1, "XML 종료 태그 끝")?
            };
            out.replace_range(
                Range {
                    start: tag_start,
                    end: tag_end_exclusive,
                },
                "",
            );
            changed = true;
            cursor = tag_start;
        } else {
            cursor = next_cursor;
        }
    }
    Ok(changed)
}
fn append_peer_text_tag(
    xml: &mut String,
    anchor_tag_name: &str,
    tag_name: &'static str,
    text: &str,
) -> Result<()> {
    if find_start_tag(xml, anchor_tag_name, 0).is_none() {
        return Err(err(tag_error_message(
            anchor_tag_name,
            " anchor 태그를 찾지 못했습니다.",
        )));
    }
    let capacity = checked_capacity(&[
        "<".len(),
        tag_name.len(),
        ">".len(),
        text.len(),
        "</".len(),
        tag_name.len(),
        ">".len(),
    ])
    .ok_or_else(|| err(tag_error_message(tag_name, " text tag 용량 계산 실패")))?;
    xml.try_reserve_exact(capacity).map_err(|source| {
        err_with_source(
            tag_error_message(tag_name, " text tag 메모리 확보 실패"),
            source,
        )
    })?;
    push_start_tag_name(xml, tag_name, "");
    xml.push_str(text);
    push_end_tag_name(xml, tag_name);
    Ok(())
}
fn replace_first_tag_text(xml: &mut String, tag_name: &str, new_text: &str) -> Result<bool> {
    let mut scanner = XmlScanner::new(xml);
    let Some(tag) = scanner.next_start_named(tag_name) else {
        return Ok(false);
    };
    let open_start = tag.start();
    let open_qualified_name = tag.name();
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
            open_qualified_name.len(),
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
        replacement.push_str(open_qualified_name);
        replacement.push('>');
        xml.replace_range(
            Range {
                start: open_start,
                end: open_end_exclusive,
            },
            &replacement,
        );
        return Ok(true);
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
    Ok(true)
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
fn xml_escaped_len(text: &str, context: XmlEscapeContext) -> Option<usize> {
    text.chars().try_fold(0_usize, |total, ch| {
        let encoded_len =
            xml_escape_replacement(ch, context).map_or_else(|| ch.len_utf8(), str::len);
        total.checked_add(encoded_len)
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
fn push_start_tag_name(out: &mut String, name: &str, attrs_xml: &str) {
    out.push('<');
    out.push_str(name);
    out.push_str(attrs_xml);
    out.push('>');
}
fn build_formula_with_empty_value(
    formula_name: &str,
    value_name: &str,
    formula_text: &str,
) -> Result<String> {
    let tag_name_len = formula_name
        .len()
        .checked_add(value_name.len())
        .and_then(|len| len.checked_mul(2))
        .ok_or_else(|| err("formula XML 태그 이름 용량 계산 실패"))?;
    let capacity = checked_capacity(&["<></><></>".len(), tag_name_len, formula_text.len()])
        .ok_or_else(|| err("formula XML 용량 계산 실패"))?;
    let mut out = String::new();
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source("formula XML 메모리 확보 실패", source))?;
    push_start_tag_name(&mut out, formula_name, "");
    out.push_str(formula_text);
    push_end_tag_name(&mut out, formula_name);
    push_start_tag_name(&mut out, value_name, "");
    push_end_tag_name(&mut out, value_name);
    Ok(out)
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
fn build_decimal_display_text_tag(name: &str, sign: Option<char>, magnitude: u64) -> String {
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
    let end_ref = ref_with_locks(CellReference::unlocked(end_col, rows.last))?;
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
