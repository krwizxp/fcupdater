use self::cell_ref::{
    MAX_A1_COL, MAX_A1_ROW, parse_range_token, parse_ref_with_locks, ref_with_locks,
    rewrite_formula_cell_refs, shift_formula_index, try_parse_and_rewrite_cell_ref,
};
use super::{
    SaveVerification,
    xlsx_container::XlsxContainer,
    xml::{
        XmlScanner, decode_xml_entities, extract_all_tag_text, extract_first_tag_text,
        find_end_tag, find_start_tag, find_tag_end, is_valid_xml_char,
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
    mem,
    range::{Range, RangeFrom, RangeInclusive},
    str,
};
use std::collections::{HashMap, hash_map::Entry as HashEntry};
use std::path::Path;
mod cell_ref;
const XML_SPACE_PRESERVE_ATTR: &str = " xml:space=\"preserve\"";
const FILTER_DATABASE_NAME: &str = "_xlnm._FilterDatabase";
const RICH_INLINE_STRING_MARKERS: [&str; 4] = ["<r", "<rPr", "<rPh", "<phoneticPr"];
const MAX_DECIMAL_TEXT_LEN: usize = 39;
const U32_DECIMAL_TEXT_MAX_LEN: usize = 10;
#[derive(Debug)]
pub(crate) struct Workbook {
    container: XlsxContainer,
    shared_strings: Vec<String>,
    shared_strings_dirty: bool,
    shared_strings_xml_text: Option<String>,
    sheets: BTreeMap<String, SheetEntry>,
    xml_text: String,
}
#[derive(Debug)]
struct SheetEntry {
    dirty: bool,
    path: String,
    worksheet: Worksheet,
}
#[derive(Debug, Default)]
pub(crate) struct Worksheet {
    formula_cells: BTreeSet<(u32, u32)>,
    locations: WorksheetXmlLocations,
    prefix: String,
    rows: BTreeMap<u32, Row>,
    suffix: String,
}
#[derive(Debug, Default)]
struct WorksheetXmlLocations {
    dimension: Option<XmlTagLocation>,
    dimension_scanned: bool,
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
struct NewSharedString {
    idx: usize,
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
struct WorksheetRowsParser<'body> {
    body: &'body str,
}
struct WorksheetXmlParser<'xml> {
    xml: &'xml str,
}
struct SharedStringsXmlParser<'xml> {
    xml: &'xml str,
}
#[derive(Debug)]
struct SharedFormulaHead {
    anchor_col: u32,
    anchor_row: u32,
    formula: String,
}
#[derive(Debug)]
struct SharedFormulaSpec {
    formula_text: Option<String>,
    si: String,
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
struct CellReference {
    col: u32,
    col_locked: bool,
    row: u32,
    row_locked: bool,
}
struct RewrittenCellReference {
    col: u32,
    row: u32,
}
struct RangeTokenParts<'token> {
    end_ref: &'token str,
    start_ref: &'token str,
}
impl Workbook {
    pub(crate) fn from_container(container: XlsxContainer) -> Result<Self> {
        let workbook_xml = container.read_text("xl/workbook.xml")?;
        let sheet_catalog = container.load_sheet_catalog(&workbook_xml)?;
        let shared_strings_xml_text = container.read_shared_strings_text()?;
        let shared_strings = match shared_strings_xml_text.as_deref() {
            Some(xml) => SharedStringsXmlParser { xml }.parse()?,
            None => Vec::new(),
        };
        let mut sheets = BTreeMap::new();
        for sheet_info in sheet_catalog {
            let xml = container.read_text(&sheet_info.path)?;
            let mut worksheet = WorksheetXmlParser { xml: &xml }.parse()?;
            worksheet.normalize_shared_formulas()?;
            match sheets.entry(sheet_info.name) {
                BTreeEntry::Vacant(entry) => {
                    entry.insert(SheetEntry {
                        dirty: false,
                        path: sheet_info.path,
                        worksheet,
                    });
                }
                BTreeEntry::Occupied(entry) => {
                    return Err(err(format!(
                        "workbook에 중복 sheet name이 있습니다: {}",
                        entry.key()
                    )));
                }
            }
        }
        Ok(Self {
            container,
            shared_strings,
            shared_strings_dirty: false,
            shared_strings_xml_text,
            sheets,
            xml_text: workbook_xml,
        })
    }
    fn promote_safe_inline_strings_to_shared(&mut self) -> Result<()> {
        if self.shared_strings_xml_text.is_none() {
            return Ok(());
        }
        let existing_table_len = self.shared_strings.len();
        let mut existing_index: Option<HashMap<&str, usize>> = None;
        let mut new_index: HashMap<String, usize> = HashMap::new();
        let mut next_new_idx = self.shared_strings.len();
        let mut promoted_any = false;
        let (shared_strings, sheets) = (&self.shared_strings, &mut self.sheets);
        for sheet in sheets.values_mut() {
            if !sheet.dirty {
                continue;
            }
            for row in sheet.worksheet.rows.values_mut() {
                for cell in row.cells.values_mut() {
                    if get_attr(&cell.attrs, "t") != Some("inlineStr") {
                        continue;
                    }
                    let Some(inner_xml) = cell.inner_xml.as_deref() else {
                        continue;
                    };
                    if !inner_xml.contains("<is")
                        || RICH_INLINE_STRING_MARKERS
                            .iter()
                            .any(|needle| inner_xml.contains(needle))
                    {
                        continue;
                    }
                    let Some(text) = extract_all_tag_text(inner_xml, "t")? else {
                        continue;
                    };
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
                    let shared_idx = if let Some(&idx) = existing_index_ref.get(text.as_ref()) {
                        idx
                    } else if let Some(&idx) = new_index.get(text.as_ref()) {
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
                        new_index.insert(text.into_owned(), idx);
                        idx
                    };
                    set_attr(&mut cell.attrs, "t", "s");
                    let shared_idx_u64 = u64::try_from(shared_idx)
                        .map_err(|source| err_with_source("XML 표시값 변환 실패", source))?;
                    cell.inner_xml =
                        Some(build_decimal_display_text_tag("v", None, shared_idx_u64)?);
                    promoted_any = true;
                }
            }
        }
        if !promoted_any {
            return Ok(());
        }
        let mut new_strings: Vec<NewSharedString> = Vec::new();
        new_strings
            .try_reserve_exact(new_index.len())
            .map_err(|source| {
                let new_count = new_index.len();
                err_with_source(
                    format!("신규 shared string 목록 메모리 확보 실패: {new_count} entries"),
                    source,
                )
            })?;
        new_strings.extend(
            new_index
                .into_iter()
                .map(|(value, idx)| NewSharedString { idx, value }),
        );
        new_strings.sort_unstable_by_key(|entry| entry.idx);
        self.shared_strings
            .extend(new_strings.into_iter().map(|entry| entry.value));
        let shared_string_reference_count = self.shared_string_reference_count()?;
        self.update_shared_strings_xml_text(
            existing_table_len,
            shared_string_reference_count,
            self.shared_strings.len(),
        )
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
        let mut scanner = XmlScanner::new(out);
        if let Some(calc_pr_tag) = scanner.next_start_named("calcPr") {
            let calc_pr_start = calc_pr_tag.start();
            let calc_pr_tag_end = calc_pr_tag.end();
            let calc_pr_name = calc_pr_tag.name();
            let calc_pr_open_span = RangeInclusive {
                start: calc_pr_start,
                last: calc_pr_tag_end,
            };
            let (mut attrs, self_closing) = {
                let calc_pr_open_tag = out
                    .get(calc_pr_open_span)
                    .ok_or_else(|| err("workbook.xml의 calcPr 태그 범위가 손상되었습니다."))?;
                (
                    parse_tag_attrs(calc_pr_open_tag)?,
                    calc_pr_open_tag.ends_with("/>"),
                )
            };
            reserve_xml_attrs(
                &mut attrs,
                4,
                "calcPr 속성 목록 추가 메모리 확보 실패",
                XmlReserveMode::Additional,
            )?;
            set_calc_pr_attrs(&mut attrs);
            if self_closing {
                let new_tag = build_self_closing_tag(calc_pr_name, &attrs)?;
                out.replace_range(calc_pr_open_span, &new_tag);
            } else {
                let close_search_from =
                    checked_usize_add(calc_pr_tag_end, 1, "calcPr 종료 태그 검색 시작")?;
                let Some(calc_pr_close_start) = find_end_tag(out, "calcPr", close_search_from)
                else {
                    return Err(err("workbook.xml의 calcPr 종료 태그를 찾지 못했습니다."));
                };
                let Some(calc_pr_close_tag_end) = find_tag_end(out, calc_pr_close_start) else {
                    return Err(err("workbook.xml의 calcPr 종료 태그가 손상되었습니다."));
                };
                let calc_pr_close_end =
                    checked_usize_add(calc_pr_close_tag_end, 1, "calcPr 종료 태그 끝")?;
                let mut new_tag = build_open_tag(calc_pr_name, &attrs)?;
                new_tag.push_str("</");
                new_tag.push_str(calc_pr_name);
                new_tag.push('>');
                out.replace_range(
                    Range {
                        start: calc_pr_start,
                        end: calc_pr_close_end,
                    },
                    &new_tag,
                );
            }
        } else {
            let mut workbook_scanner = XmlScanner::new(out);
            let Some(workbook_tag) = workbook_scanner.next_start_named("workbook") else {
                return Err(err("workbook.xml의 workbook 시작 태그를 찾지 못했습니다."));
            };
            let calc_pr_name = qualified_name_with_prefix(
                workbook_tag
                    .name()
                    .rsplit_once(':')
                    .map(|(prefix, _)| prefix),
                "calcPr",
                "calcPr qualified name",
            )?;
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
            let new_tag = build_self_closing_tag(calc_pr_name.as_ref(), &attrs)?;
            out.insert_str(workbook_close_start, &new_tag);
        }
        Ok(())
    }
    pub(crate) fn save(
        &mut self,
        target_path: &Path,
        verification: SaveVerification,
    ) -> Result<()> {
        self.promote_safe_inline_strings_to_shared()?;
        self.request_full_recalculation()?;
        self.remove_excel_recovery_artifacts()?;
        self.container
            .write_text("xl/workbook.xml", &self.xml_text)?;
        if self.shared_strings_dirty
            && let Some(shared_strings_xml) = self.shared_strings_xml_text.as_ref()
        {
            self.container
                .write_text("xl/sharedStrings.xml", shared_strings_xml)?;
        }
        for sheet in self.sheets.values() {
            if !sheet.dirty {
                continue;
            }
            let sheet_xml = sheet.worksheet.to_xml()?;
            self.container.write_text(&sheet.path, &sheet_xml)?;
        }
        self.container.save(target_path, verification)
    }
    fn shared_string_reference_count(&self) -> Result<usize> {
        let mut count = 0_usize;
        for sheet in self.sheets.values() {
            for row in sheet.worksheet.rows.values() {
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
    pub(crate) fn update_filter_database_defined_name(
        &mut self,
        sheet_name: &str,
        header_row: u32,
        last_data_row: u32,
        last_filter_col: u32,
    ) -> Result<()> {
        let quoted_sheet_capacity = sheet_name
            .chars()
            .try_fold(0_usize, |len, ch| {
                len.checked_add(if ch == '\'' { 2 } else { ch.len_utf8() })
            })
            .and_then(|len| len.checked_add(3))
            .ok_or_else(|| err("시트 이름 quote 용량 계산에 실패했습니다."))?;
        let mut quoted_sheet = String::new();
        quoted_sheet
            .try_reserve_exact(quoted_sheet_capacity)
            .map_err(|source| err_with_source("시트 이름 quote 메모리 확보 실패", source))?;
        quoted_sheet.push('\'');
        for ch in sheet_name.chars() {
            if ch == '\'' {
                quoted_sheet.push('\'');
            }
            quoted_sheet.push(ch);
        }
        quoted_sheet.push_str("'!");
        let plain_sheet_capacity = sheet_name
            .len()
            .checked_add(1)
            .ok_or_else(|| err("시트 이름 prefix 용량 계산에 실패했습니다."))?;
        let mut plain_sheet = String::new();
        plain_sheet
            .try_reserve_exact(plain_sheet_capacity)
            .map_err(|source| err_with_source("시트 이름 prefix 메모리 확보 실패", source))?;
        plain_sheet.push_str(sheet_name);
        plain_sheet.push('!');
        let sheet_idx = super::workbook_sheet_index_by_name(&self.xml_text, sheet_name)?;
        let end_col = cell_ref::col_to_name(last_filter_col)?;
        let replacement_capacity = quoted_sheet
            .len()
            .checked_add("$A$".len())
            .and_then(|len| len.checked_add(u32_decimal_text_len(header_row)))
            .and_then(|len| len.checked_add(":$".len()))
            .and_then(|len| len.checked_add(end_col.len()))
            .and_then(|len| len.checked_add("$".len()))
            .and_then(|len| len.checked_add(u32_decimal_text_len(last_data_row)))
            .ok_or_else(|| err("_FilterDatabase ref 용량 계산에 실패했습니다."))?;
        let mut replacement = String::new();
        replacement
            .try_reserve_exact(replacement_capacity)
            .map_err(|source| err_with_source("_FilterDatabase ref 메모리 확보 실패", source))?;
        replacement.push_str(&quoted_sheet);
        replacement.push_str("$A$");
        push_u32_decimal_text(
            &mut replacement,
            header_row,
            "_FilterDatabase ref 작성 실패",
        )?;
        replacement.push_str(":$");
        replacement.push_str(&end_col);
        replacement.push('$');
        push_u32_decimal_text(
            &mut replacement,
            last_data_row,
            "_FilterDatabase ref 작성 실패",
        )?;
        let span = super::workbook_defined_name_content_span(
            &self.xml_text,
            FILTER_DATABASE_NAME,
            sheet_idx,
            &quoted_sheet,
            &plain_sheet,
        )?;
        self.xml_text.replace_range(span, &replacement);
        if sheet_idx != super::workbook_sheet_index_by_name(&self.xml_text, sheet_name)? {
            return Err(err(
                "workbook.xml 유류비 시트 index가 _FilterDatabase 갱신 중 변경되었습니다.",
            ));
        }
        super::workbook_defined_name_content_span(
            &self.xml_text,
            FILTER_DATABASE_NAME,
            sheet_idx,
            &quoted_sheet,
            &plain_sheet,
        )?;
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
        let original_xml = self
            .shared_strings_xml_text
            .as_ref()
            .ok_or_else(|| err("sharedStrings XML 상태가 비정상적입니다."))?;
        let mut scanner = XmlScanner::new(original_xml);
        let Some(sst_tag) = scanner.next_start_named("sst") else {
            return Err(err("sharedStrings XML에 <sst>가 없습니다."));
        };
        let (open_start, open_end, sst_name) = (sst_tag.start(), sst_tag.end(), sst_tag.name());
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
            usize_attr_value(
                shared_string_reference_count,
                "sharedStrings count 속성 메모리 확보 실패",
            )?,
        );
        set_attr(
            &mut attrs,
            "uniqueCount",
            usize_attr_value(
                unique_count,
                "sharedStrings uniqueCount 속성 메모리 확보 실패",
            )?,
        );
        let sst_prefix = sst_name.rsplit_once(':').map(|(prefix, _)| prefix);
        let si_name = qualified_name_with_prefix(sst_prefix, "si", "shared string <si> 이름")?;
        let t_name = qualified_name_with_prefix(sst_prefix, "t", "shared string <t> 이름")?;
        let mut new_si_xml = String::new();
        for value in new_values {
            append_nested_text_xml(
                &mut new_si_xml,
                si_name.as_ref(),
                t_name.as_ref(),
                value,
                "shared string XML",
            )?;
        }
        let (replacement, maybe_close_start) = if open_tag.trim_ascii_end().ends_with("/>") {
            let mut replacement = build_open_tag(sst_name, &attrs)?;
            replacement.push_str(&new_si_xml);
            replacement.push_str("</");
            replacement.push_str(sst_name);
            replacement.push('>');
            (replacement, None)
        } else {
            let new_open_tag = build_open_tag(sst_name, &attrs)?;
            let close_search_from =
                checked_usize_add(open_end, 1, "sharedStrings 종료 태그 검색 시작")?;
            let Some(original_close_start) = find_end_tag(original_xml, "sst", close_search_from)
            else {
                return Err(err("sharedStrings XML에 </sst>가 없습니다."));
            };
            (new_open_tag, Some(original_close_start))
        };
        let mut updated_xml = self
            .shared_strings_xml_text
            .take()
            .ok_or_else(|| err("sharedStrings XML 상태가 비정상적입니다."))?;
        if let Some(close_start) = maybe_close_start {
            updated_xml.insert_str(close_start, &new_si_xml);
        }
        updated_xml.replace_range(open_tag_span, &replacement);
        self.shared_strings_xml_text = Some(updated_xml);
        self.shared_strings_dirty = true;
        Ok(())
    }
    pub(super) fn verify_sheet_address_data_end_row(
        &self,
        sheet_name: &str,
        header_row: u32,
        filter_end_row: u32,
    ) -> Result<()> {
        let Some(sheet) = self.sheets.get(sheet_name) else {
            return Err(err(format!(
                "저장 검증 실패: workbook에 {sheet_name} 시트가 없습니다."
            )));
        };
        let worksheet = &sheet.worksheet;
        let mut maybe_address_col = None;
        for col in worksheet.cols_in_row(header_row) {
            let display = worksheet.try_get_display_at(col, header_row, &self.shared_strings)?;
            let mut key = String::new();
            key.try_reserve_exact(display.len()).map_err(|source| {
                err_with_source(
                    "저장 검증 실패: 유류비 header 정규화 메모리 확보 실패",
                    source,
                )
            })?;
            key.extend(display.chars().filter(|ch| !ch.is_whitespace()));
            if key == "주소" {
                maybe_address_col = Some(col);
                break;
            }
        }
        let address_col = maybe_address_col.ok_or_else(|| {
            err("저장 검증 실패: 유류비 autoFilter header에서 주소 컬럼을 찾지 못했습니다.")
        })?;
        let data_start_row = header_row
            .checked_add(1)
            .ok_or_else(|| err("저장 검증 실패: 유류비 데이터 시작 행 계산 실패"))?;
        let mut actual_end_row = data_start_row;
        for row in worksheet.row_numbers_from(data_start_row) {
            let display = worksheet.try_get_display_at(address_col, row, &self.shared_strings)?;
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
    pub(crate) fn with_sheet_mut<R, F>(&mut self, name: &str, mutator: F) -> Result<Option<R>>
    where
        F: FnOnce(&mut Worksheet, &[String]) -> Result<R>,
    {
        let (shared_strings, sheets) = (&self.shared_strings, &mut self.sheets);
        let Some(sheet) = sheets.get_mut(name) else {
            return Ok(None);
        };
        let result = mutator(&mut sheet.worksheet, shared_strings)?;
        sheet.dirty = true;
        Ok(Some(result))
    }
}
impl SharedStringsXmlParser<'_> {
    fn parse(&self) -> Result<Vec<String>> {
        let mut out: Vec<String> = Vec::new();
        let mut scanner = XmlScanner::new(self.xml);
        while let Some(si_tag) = scanner.next_start_named("si") {
            if out.len() == out.capacity() {
                out.try_reserve(1).map_err(|source| {
                    err_with_source("sharedStrings entry 메모리 확보 실패", source)
                })?;
            }
            if si_tag.self_closing() {
                out.push(String::new());
                continue;
            }
            let si_tag_end = si_tag.end();
            let Some(body_start) = si_tag_end.checked_add(1) else {
                return Err(err(
                    "sharedStrings.xml의 <si> 본문 시작 계산에 실패했습니다.",
                ));
            };
            let Some(si_end) = find_end_tag(self.xml, "si", body_start) else {
                return Err(err("sharedStrings.xml의 </si> 태그를 찾지 못했습니다."));
            };
            let si_body_span = Range {
                start: body_start,
                end: si_end,
            };
            let Some(si_body) = self.xml.get(si_body_span) else {
                return Err(err("sharedStrings.xml의 <si> 본문 범위가 손상되었습니다."));
            };
            let text =
                extract_all_tag_text(si_body, "t")?.map_or_else(String::new, Cow::into_owned);
            out.push(text);
            let Some(si_close_end) = find_tag_end(self.xml, si_end) else {
                return Err(err("sharedStrings.xml의 </si> 태그가 손상되었습니다."));
            };
            let Some(next_cursor) = si_close_end.checked_add(1) else {
                return Err(err(
                    "sharedStrings.xml의 다음 <si> 위치 계산에 실패했습니다.",
                ));
            };
            scanner.skip_to(next_cursor);
        }
        Ok(out)
    }
}
impl WorksheetRowParser<'_> {
    fn parse_into(&self, row: &mut Row, formula_cells: &mut BTreeSet<(u32, u32)>) -> Result<()> {
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
            reserve_xml_attrs(
                &mut attrs,
                1,
                "cell 속성 목록 추가 메모리 확보 실패",
                XmlReserveMode::Additional,
            )?;
            set_attr(
                &mut attrs,
                "r",
                ref_with_locks(CellReference::unlocked(col, self.row_num))?,
            );
            if cell_info.self_closing() {
                insert_cell(
                    row,
                    self.row_num,
                    col,
                    Cell {
                        attrs,
                        inner_xml: None,
                    },
                )?;
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
            let inner_xml = copy_text(inner_xml_text, "row cell 본문 복사")?;
            if find_start_tag(&inner_xml, "f", 0).is_some() {
                formula_cells.insert((self.row_num, col));
            }
            insert_cell(
                row,
                self.row_num,
                col,
                Cell {
                    attrs,
                    inner_xml: Some(inner_xml),
                },
            )?;
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
}
impl WorksheetRowsParser<'_> {
    fn parse(&self) -> Result<ParsedWorksheetRows> {
        let mut rows: BTreeMap<u32, Row> = BTreeMap::new();
        let mut formula_cells = BTreeSet::new();
        let mut scanner = XmlScanner::new(self.body);
        while let Some(row_info) = scanner.next_start_named("row") {
            let row_tag_end = row_info.end();
            let row_tag = row_info.raw();
            let mut row_attrs = parse_tag_attrs(row_tag)?;
            let row_num = if let Some(row_num_text) = get_attr(&row_attrs, "r") {
                parse_positive_u32_decimal(
                    row_num_text,
                    "worksheet row 번호가 양의 10진수 형식이 아닙니다.",
                    "worksheet row 번호 해석 실패",
                    "worksheet row 번호는 1 이상이어야 합니다.",
                )?
            } else {
                match rows.last_key_value() {
                    Some((&last_row, _)) => last_row.checked_add(1).ok_or_else(|| {
                        err("worksheet row 번호 자동 증가 중 overflow가 발생했습니다.")
                    })?,
                    None => 1_u32,
                }
            };
            reserve_xml_attrs(
                &mut row_attrs,
                1,
                "row 속성 목록 추가 메모리 확보 실패",
                XmlReserveMode::Additional,
            )?;
            set_attr(
                &mut row_attrs,
                "r",
                u32_text_value(row_num, "row 번호 속성 메모리 확보 실패")?,
            );
            if row_info.self_closing() {
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
            let Some(row_body_end) = find_end_tag(self.body, "row", row_body_start) else {
                return Err(err(row_only_error(
                    "sheetData row 종료 태그를 찾지 못했습니다. (row=",
                    row_num,
                )));
            };
            let row_body_span = Range {
                start: row_body_start,
                end: row_body_end,
            };
            let row_body = self.body.get(row_body_span).ok_or_else(|| {
                err(row_only_error(
                    "sheetData row 본문 범위가 손상되었습니다. (row=",
                    row_num,
                ))
            })?;
            let mut row = Row {
                attrs: row_attrs,
                cells: BTreeMap::new(),
            };
            WorksheetRowParser { row_body, row_num }.parse_into(&mut row, &mut formula_cells)?;
            if rows.insert(row_num, row).is_some() {
                return Err(err(row_only_error(
                    "중복 worksheet row가 있습니다. (row=",
                    row_num,
                )));
            }
            let row_close_end = find_tag_end(self.body, row_body_end)
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
}
impl WorksheetXmlParser<'_> {
    fn parse(&self) -> Result<Worksheet> {
        let mut scanner = XmlScanner::new(self.xml);
        let Some(sheet_data_tag) = scanner.next_start_named("sheetData") else {
            return Err(err("worksheet XML에 <sheetData>가 없습니다."));
        };
        let sheet_data_open = sheet_data_tag.start();
        let sheet_data_open_end = sheet_data_tag.end();
        if sheet_data_tag.self_closing() {
            return self.parse_self_closing_sheet_data(
                sheet_data_open,
                sheet_data_open_end,
                sheet_data_tag.name(),
            );
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
        let body = self
            .xml
            .get(sheet_data_body_span)
            .ok_or_else(|| err("worksheet XML body 범위가 손상되었습니다."))?;
        let suffix_raw = self
            .xml
            .get(sheet_data_body_span.end..)
            .ok_or_else(|| err("worksheet XML suffix 범위가 손상되었습니다."))?;
        let prefix = copy_text(prefix_raw, "worksheet XML prefix 복사")?;
        let suffix = copy_text(suffix_raw, "worksheet XML suffix 복사")?;
        let ParsedWorksheetRows {
            formula_cells,
            rows,
        } = WorksheetRowsParser { body }.parse()?;
        Ok(Worksheet {
            formula_cells,
            locations: WorksheetXmlLocations::default(),
            prefix,
            rows,
            suffix,
        })
    }
    fn parse_self_closing_sheet_data(
        &self,
        sheet_data_open: usize,
        sheet_data_open_end: usize,
        sheet_data_name: &str,
    ) -> Result<Worksheet> {
        let open_tag = self
            .xml
            .get(RangeInclusive {
                start: sheet_data_open,
                last: sheet_data_open_end,
            })
            .ok_or_else(|| err("worksheet XML의 <sheetData/> 태그 범위가 손상되었습니다."))?;
        let open_prefix = open_tag
            .trim_ascii_end()
            .strip_suffix("/>")
            .ok_or_else(|| {
                err("worksheet XML의 self-closing <sheetData/> 태그가 손상되었습니다.")
            })?;
        let sheet_data_end = checked_usize_add(sheet_data_open_end, 1, "worksheet sheetData 끝")?;
        let before_sheet_data = self
            .xml
            .get(..sheet_data_open)
            .ok_or_else(|| err("worksheet XML prefix 범위가 손상되었습니다."))?;
        let after_sheet_data = self
            .xml
            .get(sheet_data_end..)
            .ok_or_else(|| err("worksheet XML suffix 범위가 손상되었습니다."))?;
        let prefix_capacity =
            checked_capacity(&[before_sheet_data.len(), open_prefix.len(), ">".len()])
                .ok_or_else(|| err("worksheet XML self-closing prefix 용량 계산 실패"))?;
        let mut prefix = String::new();
        prefix
            .try_reserve_exact(prefix_capacity)
            .map_err(|source| {
                err_with_source("worksheet XML self-closing prefix 메모리 확보 실패", source)
            })?;
        prefix.push_str(before_sheet_data);
        prefix.push_str(open_prefix);
        prefix.push('>');
        let suffix_capacity = checked_capacity(&[
            "</".len(),
            sheet_data_name.len(),
            ">".len(),
            after_sheet_data.len(),
        ])
        .ok_or_else(|| err("worksheet XML self-closing suffix 용량 계산 실패"))?;
        let mut suffix = String::new();
        suffix
            .try_reserve_exact(suffix_capacity)
            .map_err(|source| {
                err_with_source("worksheet XML self-closing suffix 메모리 확보 실패", source)
            })?;
        suffix.push_str("</");
        suffix.push_str(sheet_data_name);
        suffix.push('>');
        suffix.push_str(after_sheet_data);
        Ok(Worksheet {
            formula_cells: BTreeSet::new(),
            locations: WorksheetXmlLocations::default(),
            prefix,
            rows: BTreeMap::new(),
            suffix,
        })
    }
}
impl WorksheetXmlLocations {
    fn dimension_span(&mut self, prefix: &str) -> Result<Option<&XmlTagLocation>> {
        if !self.dimension_scanned {
            self.dimension =
                find_start_tag_location(prefix, "dimension", 0, "dimension 태그 이름 복사")?;
            self.dimension_scanned = true;
        }
        Ok(self.dimension.as_ref())
    }
    fn set_dimension_span(&mut self, start: usize, len: usize) -> Result<()> {
        if let Some(location) = self.dimension.as_mut() {
            location.span = Range {
                start,
                end: checked_usize_add(start, len, "dimension cache 범위 끝")?,
            };
        }
        self.dimension_scanned = true;
        Ok(())
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
    fn cols_in_row(&self, row: u32) -> impl Iterator<Item = u32> + '_ {
        self.rows
            .get(&row)
            .into_iter()
            .flat_map(|row_obj| row_obj.cells.keys().copied())
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
        let mut copied = src.copy_for_row(target_row, &|row_num| {
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
            let mut attrs = {
                let tag = out
                    .get(location.span)
                    .ok_or_else(|| err("conditionalFormatting 태그 범위가 손상되었습니다."))?;
                parse_tag_attrs(tag)?
            };
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
    pub(crate) fn get_i32_at(
        &self,
        col: u32,
        row: u32,
        shared_strings: &[String],
    ) -> Result<Option<i32>> {
        let text = self.try_get_display_at(col, row, shared_strings)?;
        Ok(parse_i32_str(&text))
    }
    fn get_or_create_cell_mut(&mut self, col: u32, row: u32) -> Result<&mut Cell> {
        let row_obj = match self.rows.entry(row) {
            BTreeEntry::Occupied(entry) => {
                let row_obj = entry.into_mut();
                if get_attr(&row_obj.attrs, "r").is_none() {
                    set_attr(
                        &mut row_obj.attrs,
                        "r",
                        u32_text_value(row, "row 번호 속성 메모리 확보 실패")?,
                    );
                }
                row_obj
            }
            BTreeEntry::Vacant(entry) => entry.insert(Row::numbered(row)?),
        };
        Ok(match row_obj.cells.entry(col) {
            BTreeEntry::Occupied(entry) => entry.into_mut(),
            BTreeEntry::Vacant(entry) => {
                let cell_ref = ref_with_locks(CellReference::unlocked(col, row))?;
                let mut attrs = Vec::new();
                attrs
                    .try_reserve_exact(2)
                    .map_err(|source| err_with_source("cell 속성 목록 메모리 확보 실패", source))?;
                attrs.push(owned_attr("r", cell_ref));
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
    fn normalize_shared_formulas(&mut self) -> Result<()> {
        let mut heads: HashMap<String, SharedFormulaHead> = HashMap::new();
        heads.try_reserve(self.rows.len()).map_err(|source| {
            let row_count = self.rows.len();
            err_with_source(
                format!("shared formula head 맵 메모리 확보 실패: {row_count} entries"),
                source,
            )
        })?;
        for (row_num, row) in &self.rows {
            for (col_num, cell) in &row.cells {
                let Some(inner_xml) = cell.inner_xml.as_deref() else {
                    continue;
                };
                let Some(spec) = parse_shared_formula_spec(inner_xml)? else {
                    continue;
                };
                let Some(formula_text) = spec.formula_text else {
                    continue;
                };
                match heads.entry(spec.si) {
                    HashEntry::Vacant(entry) => {
                        entry.insert(SharedFormulaHead {
                            anchor_col: *col_num,
                            anchor_row: *row_num,
                            formula: formula_text,
                        });
                    }
                    HashEntry::Occupied(entry) => {
                        return Err(err(format!(
                            "shared formula si가 중복되었습니다: {}",
                            entry.key()
                        )));
                    }
                }
            }
        }
        if heads.is_empty() {
            return Ok(());
        }
        for (row_num, row) in &mut self.rows {
            for (col_num, cell) in &mut row.cells {
                let Some(inner_xml) = cell.inner_xml.as_deref() else {
                    continue;
                };
                let Some(spec) = parse_shared_formula_spec(inner_xml)? else {
                    continue;
                };
                let Some(head) = heads.get(&spec.si) else {
                    let col_name = cell_ref::col_to_name(*col_num)?;
                    return Err(err(format!(
                        "shared formula head를 찾지 못했습니다. (si={}, cell={col_name}{row_num} )",
                        spec.si
                    )));
                };
                let formula = if let Some(text) = spec.formula_text {
                    text
                } else {
                    let delta_col = (*col_num)
                        .checked_signed_diff(head.anchor_col)
                        .ok_or_else(|| err("shared formula column delta 계산 실패"))?;
                    let delta_row = (*row_num)
                        .checked_signed_diff(head.anchor_row)
                        .ok_or_else(|| err("shared formula row delta 계산 실패"))?;
                    rewrite_formula_cell_refs(&head.formula, |chars, start| {
                        try_parse_and_rewrite_cell_ref(
                            chars,
                            start,
                            |base_col, base_row, col_lock, row_lock| {
                                let new_col = if col_lock {
                                    base_col
                                } else {
                                    shift_formula_index(base_col, delta_col, MAX_A1_COL)?
                                };
                                let new_row = if row_lock {
                                    base_row
                                } else {
                                    shift_formula_index(base_row, delta_row, MAX_A1_ROW)?
                                };
                                Ok(RewrittenCellReference {
                                    col: new_col,
                                    row: new_row,
                                })
                            },
                        )
                    })?
                };
                cell.inner_xml = Some(replace_formula_tag_with_plain_formula(inner_xml, &formula)?);
            }
        }
        self.formula_cells = formula_cells_from_rows(&self.rows);
        Ok(())
    }
    fn prune_col_definitions_after_col(&mut self, max_col: u32) -> Result<()> {
        let mut cursor = 0_usize;
        while let Some(location) =
            find_start_tag_location(&self.prefix, "col", cursor, "col 정의 태그 이름 복사")?
        {
            let col_start = location.span.start;
            let tag = self
                .prefix
                .get(location.span)
                .ok_or_else(|| err("worksheet col 정의 태그 범위가 손상되었습니다."))?;
            if !location.self_closing {
                return Err(err("worksheet col 정의가 self-closing 태그가 아닙니다."));
            }
            let mut attrs = parse_tag_attrs(tag)?;
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
                self.prefix.replace_range(location.span, "");
                cursor = col_start;
                continue;
            }
            if max_defined_col > max_col {
                set_attr(
                    &mut attrs,
                    "max",
                    u32_text_value(max_col, "col max 속성 메모리 확보 실패")?,
                );
                let new_tag = build_self_closing_tag(&location.name, &attrs)?;
                self.prefix.replace_range(location.span, &new_tag);
                cursor = checked_usize_add(col_start, new_tag.len(), "col 정의 다음 cursor")?;
                continue;
            }
            cursor = location.span.end;
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
        let cell = self.get_or_create_cell_mut(col, row)?;
        if let Some(inner) = cell.inner_xml.as_mut() {
            if find_start_tag(inner, "f", 0).is_some() {
                *inner = replace_formula_tag_with_plain_formula(inner, formula)?;
                if find_start_tag(inner, "v", 0).is_none() {
                    append_peer_text_tag(inner, "f", "v", "")?;
                }
            } else if inner.trim().is_empty() {
                let formula_text =
                    try_xml_escape_text(formula, XmlEscapeContext::Text, "formula XML escape")?;
                *inner = build_formula_with_empty_value(&formula_text)?;
            } else {
                return Err(err("cell formula 태그를 찾지 못했습니다."));
            }
        } else {
            let formula_text =
                try_xml_escape_text(formula, XmlEscapeContext::Text, "formula XML escape")?;
            cell.inner_xml = Some(build_formula_with_empty_value(&formula_text)?);
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
        let cell = self.get_or_create_cell_mut(col, row)?;
        match cell_type {
            Some(value_type) => set_attr(&mut cell.attrs, "t", value_type),
            None => remove_attr(&mut cell.attrs, "t"),
        }
        let Some(inner) = cell.inner_xml.as_mut() else {
            return Ok(());
        };
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
        let cell = self.get_or_create_cell_mut(col, row)?;
        remove_attr(&mut cell.attrs, "t");
        if let Some(numeric_value) = value {
            cell.inner_xml = Some(build_decimal_display_text_tag(
                "v",
                numeric_value.is_negative().then_some('-'),
                u64::from(numeric_value.unsigned_abs()),
            )?);
        } else {
            cell.inner_xml = None;
        }
        self.formula_cells.remove(&(row, col));
        Ok(())
    }
    pub(crate) fn set_string_at(&mut self, col: u32, row: u32, value: &str) -> Result<()> {
        let cell = self.get_or_create_cell_mut(col, row)?;
        set_attr(&mut cell.attrs, "t", "inlineStr");
        let mut inner = String::new();
        append_nested_text_xml(&mut inner, "is", "t", value, "inline string XML")?;
        cell.inner_xml = Some(inner);
        self.formula_cells.remove(&(row, col));
        Ok(())
    }
    pub(crate) fn take_rows(&mut self) -> BTreeMap<u32, Row> {
        self.formula_cells.clear();
        mem::take(&mut self.rows)
    }
    fn to_xml(&self) -> Result<String> {
        let estimated_capacity = (|| {
            let mut capacity = checked_capacity(&[self.prefix.len(), self.suffix.len()])?;
            for row in self.rows.values() {
                capacity = capacity.checked_add("<row".len())?;
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
                if row.cells.is_empty() {
                    capacity = capacity.checked_add("/>".len())?;
                    continue;
                }
                capacity = capacity.checked_add(">".len())?;
                for cell in row.cells.values() {
                    capacity = capacity.checked_add("<c".len())?;
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
                        capacity =
                            checked_capacity(&[capacity, ">".len(), inner.len(), "</c>".len()])?;
                    } else {
                        capacity = capacity.checked_add("/>".len())?;
                    }
                }
                capacity = capacity.checked_add("</row>".len())?;
            }
            Some(capacity)
        })();
        let capacity = estimated_capacity.ok_or_else(|| err("worksheet XML 용량 계산 실패"))?;
        let mut out = String::new();
        out.try_reserve_exact(capacity)
            .map_err(|source| err_with_source("worksheet XML 메모리 확보 실패", source))?;
        out.push_str(&self.prefix);
        for row in self.rows.values() {
            out.push_str("<row");
            push_sorted_attrs_xml(&mut out, &row.attrs)?;
            if row.cells.is_empty() {
                out.push_str("/>");
                continue;
            }
            out.push('>');
            for cell in row.cells.values() {
                out.push_str("<c");
                push_sorted_attrs_xml(&mut out, &cell.attrs)?;
                if let Some(inner) = cell.inner_xml.as_ref() {
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
            Some("inlineStr") => extract_all_tag_text(inner, "t")?
                .ok_or_else(|| err("inlineStr cell text를 해석하지 못했습니다.")),
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
    pub(crate) fn update_auto_filter_ref(
        &mut self,
        filter_rows: RangeInclusive<u32>,
    ) -> Result<u32> {
        let out = &mut self.suffix;
        let mut cursor = 0_usize;
        let header_row = filter_rows.start;
        let target_last_row = filter_rows.start.max(filter_rows.last);
        let mut updated_end_col = None;
        while let Some(location) =
            find_start_tag_location(out, "autoFilter", cursor, "autoFilter 태그 이름 복사")?
        {
            let auto_filter_start = location.span.start;
            let mut attrs = {
                let tag = out
                    .get(location.span)
                    .ok_or_else(|| err("worksheet XML의 autoFilter 태그 범위가 손상되었습니다."))?;
                parse_tag_attrs(tag)?
            };
            let existing_ref = get_attr(&attrs, "ref")
                .ok_or_else(|| err("worksheet autoFilter ref 속성이 없습니다."))?;
            let range = parse_range_token(existing_ref);
            let start_reference = parse_ref_with_locks(range.start_ref)
                .ok_or_else(|| err("worksheet autoFilter 시작 reference 해석 실패"))?;
            let end_reference = parse_ref_with_locks(range.end_ref)
                .ok_or_else(|| err("worksheet autoFilter 끝 reference 해석 실패"))?;
            if start_reference.col != 1 || start_reference.row != header_row {
                return Err(err(format!(
                    "worksheet autoFilter 시작 범위가 예상과 다릅니다: ref={existing_ref}, expected=A{header_row}"
                )));
            }
            if end_reference.col < start_reference.col || end_reference.row < start_reference.row {
                return Err(err(format!(
                    "worksheet autoFilter 범위 순서가 올바르지 않습니다: {existing_ref}"
                )));
            }
            let end_col = end_reference.col;
            if updated_end_col.replace(end_col).is_some() {
                return Err(err("worksheet XML에 autoFilter 태그가 중복되어 있습니다."));
            }
            let new_ref = build_ref_range(
                "A",
                RangeInclusive {
                    start: header_row,
                    last: target_last_row,
                },
                end_col,
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
            cursor = checked_usize_add(auto_filter_start, new_tag.len(), "autoFilter 다음 cursor")?;
        }
        updated_end_col.ok_or_else(|| err("worksheet XML의 autoFilter 태그를 찾지 못했습니다."))
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
        if let Some(dim_location) = self.locations.dimension_span(&self.prefix)? {
            let dim_start = dim_location.span.start;
            let mut attrs = {
                let tag = self
                    .prefix
                    .get(dim_location.span)
                    .ok_or_else(|| err("dimension 태그 범위가 손상되었습니다."))?;
                parse_tag_attrs(tag)?
            };
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
            self.prefix.replace_range(dim_location.span, &new_tag);
            self.locations
                .set_dimension_span(dim_start, new_tag.len())?;
        }
        Ok(())
    }
}
impl Row {
    pub(crate) fn copy_for_row(
        &self,
        target_row: u32,
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
        remap_row_numbers(&mut row, target_row, resolver)?;
        Ok(row)
    }
    pub(crate) fn numbered(row_num: u32) -> Result<Self> {
        let mut attrs = Vec::new();
        attrs
            .try_reserve_exact(1)
            .map_err(|source| err_with_source("row 속성 목록 메모리 확보 실패", source))?;
        attrs.push(owned_attr(
            "r",
            u32_text_value(row_num, "row 번호 속성 메모리 확보 실패")?,
        ));
        Ok(Self {
            attrs,
            cells: BTreeMap::new(),
        })
    }
}
fn insert_cell(row: &mut Row, row_num: u32, col: u32, cell: Cell) -> Result<()> {
    if row.cells.insert(col, cell).is_some() {
        Err(err(row_col_error(
            "중복 cell reference가 있습니다. (row=",
            row_num,
            col,
        )))
    } else {
        Ok(())
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
const fn u32_decimal_text_len(value: u32) -> usize {
    if value < 10 {
        1
    } else if value < 100 {
        2
    } else if value < 1_000 {
        3
    } else if value < 10_000 {
        4
    } else if value < 100_000 {
        5
    } else if value < 1_000_000 {
        6
    } else if value < 10_000_000 {
        7
    } else if value < 100_000_000 {
        8
    } else if value < 1_000_000_000 {
        9
    } else {
        10
    }
}
fn usize_attr_value(value: usize, context: &'static str) -> Result<String> {
    let mut buffer = [0_u8; MAX_DECIMAL_TEXT_LEN];
    let value_u64 = u64::try_from(value).map_err(|source| err_with_source(context, source))?;
    let text = decimal_text(value_u64, &mut buffer, context)?;
    let mut out = String::new();
    out.try_reserve_exact(text.len())
        .map_err(|source| err_with_source(context, source))?;
    out.push_str(text);
    Ok(out)
}
fn u32_text_value(value: u32, context: &'static str) -> Result<String> {
    let mut buffer = [0_u8; U32_DECIMAL_TEXT_MAX_LEN];
    let text = decimal_text(u64::from(value), &mut buffer, context)?;
    let mut out = String::new();
    out.try_reserve_exact(text.len())
        .map_err(|source| err_with_source(context, source))?;
    out.push_str(text);
    Ok(out)
}
fn push_u32_decimal_text(out: &mut String, value: u32, context: &'static str) -> Result<()> {
    let mut buffer = [0_u8; U32_DECIMAL_TEXT_MAX_LEN];
    let text = decimal_text(u64::from(value), &mut buffer, context)?;
    out.push_str(text);
    Ok(())
}
fn decimal_text<'buffer>(
    mut value: u64,
    buffer: &'buffer mut [u8],
    context: &'static str,
) -> Result<&'buffer str> {
    let mut index = buffer.len();
    loop {
        let digit = u8::try_from(value.rem_euclid(10))
            .map_err(|source| err_with_source(context, source))?;
        index = index
            .checked_sub(1)
            .ok_or_else(|| err(format!("{context} buffer index 계산 실패")))?;
        let byte = b'0'
            .checked_add(digit)
            .ok_or_else(|| err(format!("{context} 문자 계산 실패")))?;
        let slot = buffer
            .get_mut(index)
            .ok_or_else(|| err(format!("{context} buffer 범위가 손상되었습니다.")))?;
        *slot = byte;
        value = value.div_euclid(10);
        if value == 0 {
            break;
        }
    }
    let bytes = buffer
        .get(index..)
        .ok_or_else(|| err(format!("{context} 결과 범위가 손상되었습니다.")))?;
    str::from_utf8(bytes).map_err(|source| err_with_source(context, source))
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
fn copy_text(text: &str, context: &'static str) -> Result<String> {
    let mut out = String::new();
    out.try_reserve_exact(text.len())
        .map_err(|source| err_with_source(format!("{context} 메모리 확보 실패"), source))?;
    out.push_str(text);
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
pub(crate) fn remap_row_numbers(
    row: &mut Row,
    new_row: u32,
    resolver: &dyn Fn(u32) -> Result<u32>,
) -> Result<()> {
    set_attr(
        &mut row.attrs,
        "r",
        u32_text_value(new_row, "row 번호 속성 메모리 확보 실패")?,
    );
    for (col, cell) in &mut row.cells {
        set_attr(
            &mut cell.attrs,
            "r",
            ref_with_locks(CellReference::unlocked(*col, new_row))?,
        );
        if let Some(inner) = cell.inner_xml.as_mut()
            && let Some(text) = extract_first_tag_text(inner, "f")?
        {
            let decoded = decode_xml_entities(text)?;
            let rewrite_result = rewrite_formula_cell_refs(decoded.as_ref(), |chars, start| {
                try_parse_and_rewrite_cell_ref(
                    chars,
                    start,
                    |base_col, base_row, _col_lock, row_lock| {
                        let updated_row = if row_lock {
                            base_row
                        } else {
                            resolver(base_row)?
                        };
                        Ok(RewrittenCellReference {
                            col: base_col,
                            row: updated_row,
                        })
                    },
                )
            });
            let rewritten = rewrite_result?;
            let encoded =
                try_xml_escape_text(&rewritten, XmlEscapeContext::Text, "formula XML escape")?;
            replace_first_tag_text(inner, "f", &encoded)?;
        }
    }
    Ok(())
}
pub(crate) fn col_to_name(col: u32) -> Result<String> {
    cell_ref::col_to_name(col)
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
fn parse_tag_attrs(tag: &str) -> Result<Vec<XmlAttr>> {
    let mut out: Vec<XmlAttr> = Vec::new();
    reserve_xml_attrs(
        &mut out,
        4,
        "XML 속성 목록 메모리 확보 실패",
        XmlReserveMode::Exact,
    )?;
    let parse_error = |prefix: &str| err(format!("{prefix}{tag}"));
    let Some(lt) = tag.find('<') else {
        return Err(parse_error("XML 태그 파싱 실패: '<' 없음. tag="));
    };
    let advance = |cursor: &mut usize| -> Result<()> {
        *cursor = cursor
            .checked_add(1)
            .ok_or_else(|| parse_error("XML 태그 파싱 실패: cursor overflow. tag="))?;
        Ok(())
    };
    let mut i = lt
        .checked_add(1)
        .ok_or_else(|| parse_error("XML 태그 파싱 실패: cursor 시작 계산 실패. tag="))?;
    let bytes = tag.as_bytes();
    while bytes
        .get(i)
        .is_some_and(|ch| !ch.is_ascii_whitespace() && !matches!(*ch, b'>' | b'/'))
    {
        advance(&mut i)?;
    }
    if i >= bytes.len() {
        return Err(parse_error("XML 태그 파싱 실패: 태그 종료 기호 없음. tag="));
    }
    while i < bytes.len() {
        while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
            advance(&mut i)?;
        }
        if bytes.get(i).is_none_or(|ch| matches!(*ch, b'>' | b'/')) {
            break;
        }
        let key_start = i;
        while bytes
            .get(i)
            .is_some_and(|ch| !ch.is_ascii_whitespace() && !matches!(*ch, b'=' | b'>' | b'/'))
        {
            advance(&mut i)?;
        }
        let key_end = i;
        if key_start == key_end {
            return Err(parse_error("XML 속성 파싱 실패: 빈 속성 이름. tag="));
        }
        while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
            advance(&mut i)?;
        }
        let Some(&equals) = bytes.get(i) else {
            return Err(parse_error("XML 속성 파싱 실패: '=' 없음. tag="));
        };
        if equals != b'=' {
            return Err(parse_error("XML 속성 파싱 실패: '='가 필요합니다. tag="));
        }
        advance(&mut i)?;
        while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
            advance(&mut i)?;
        }
        let Some(&quote) = bytes.get(i) else {
            return Err(parse_error("XML 속성 파싱 실패: 값 quote가 없습니다. tag="));
        };
        if !matches!(quote, b'"' | b'\'') {
            return Err(parse_error("XML 속성 파싱 실패: 속성 값 quote 필요. tag="));
        }
        advance(&mut i)?;
        let value_start = i;
        while bytes.get(i).is_some_and(|ch| *ch != quote) {
            advance(&mut i)?;
        }
        if i >= bytes.len() {
            return Err(parse_error("XML 속성 파싱 실패: 닫히지 않은 quote. tag="));
        }
        let key = tag
            .get(key_start..key_end)
            .ok_or_else(|| parse_error("XML 속성 파싱 실패: 키 범위를 계산할 수 없습니다. tag="))?;
        let raw_value = tag
            .get(value_start..i)
            .ok_or_else(|| parse_error("XML 속성 파싱 실패: 값 범위를 계산할 수 없습니다. tag="))?;
        let value = decode_xml_entities(raw_value)?.into_owned();
        if out.len() == out.capacity() {
            reserve_xml_attrs(
                &mut out,
                1,
                "XML 속성 목록 추가 메모리 확보 실패",
                XmlReserveMode::Additional,
            )?;
        }
        out.push(XmlAttr {
            name: Cow::Owned(copy_text(key, "XML 속성 이름 복사")?),
            value,
        });
        if i < bytes.len() {
            advance(&mut i)?;
        }
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
    if value.is_empty() {
        return Err(err(format!("{context}: 음이 아닌 10진수 형식이 아닙니다.")));
    }
    let mut parsed = 0_usize;
    for byte in value.bytes() {
        if !byte.is_ascii_digit() {
            return Err(err(format!("{context}: 음이 아닌 10진수 형식이 아닙니다.")));
        }
        let digit_raw = byte.wrapping_sub(b'0');
        let Some(next) = parsed
            .checked_mul(10)
            .and_then(|scaled| scaled.checked_add(usize::from(digit_raw)))
        else {
            return value
                .parse::<usize>()
                .map_err(|source| err_with_source(context, source));
        };
        parsed = next;
    }
    Ok(parsed)
}
fn parse_positive_u32_decimal(
    value: &str,
    format_error: &'static str,
    parse_context: &'static str,
    zero_error: &'static str,
) -> Result<u32> {
    if value.is_empty() {
        return Err(err(format_error));
    }
    let mut parsed = 0_u32;
    for byte in value.bytes() {
        if !byte.is_ascii_digit() {
            return Err(err(format_error));
        }
        let digit = u32::from(byte.wrapping_sub(b'0'));
        parsed = parsed
            .checked_mul(10)
            .and_then(|current| current.checked_add(digit))
            .ok_or_else(|| err(parse_context))?;
    }
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
fn parse_shared_formula_spec(inner_xml: &str) -> Result<Option<SharedFormulaSpec>> {
    let Some(f_start) = find_start_tag(inner_xml, "f", 0) else {
        return Ok(None);
    };
    let Some(f_end) = find_tag_end(inner_xml, f_start) else {
        return Err(err("cell formula 태그가 손상되었습니다."));
    };
    let formula_open_span = RangeInclusive {
        start: f_start,
        last: f_end,
    };
    let open_tag = inner_xml
        .get(formula_open_span)
        .ok_or_else(|| err("cell formula 시작 태그 범위가 손상되었습니다."))?;
    let mut attrs = parse_tag_attrs(open_tag)?;
    if get_attr(&attrs, "t") != Some("shared") {
        return Ok(None);
    }
    let si_index = attrs
        .iter()
        .position(|attr| attr.name == "si")
        .ok_or_else(|| err("shared formula에 si 속성이 없습니다."))?;
    let si = attrs.swap_remove(si_index).value;
    let formula_text = if let Some(text) = extract_first_tag_text(inner_xml, "f")? {
        let decoded = decode_xml_entities(text)?.into_owned();
        (!decoded.is_empty()).then_some(decoded)
    } else {
        None
    };
    Ok(Some(SharedFormulaSpec { formula_text, si }))
}
fn replace_formula_tag_with_plain_formula(inner_xml: &str, formula: &str) -> Result<String> {
    let mut scanner = XmlScanner::new(inner_xml);
    let Some(f_tag) = scanner.next_start_named("f") else {
        return Err(err("cell formula 태그를 찾지 못했습니다."));
    };
    let f_start = f_tag.start();
    let f_end = f_tag.end();
    let f_name = f_tag.name();
    let formula_open_span = RangeInclusive {
        start: f_start,
        last: f_end,
    };
    let open_tag = inner_xml
        .get(formula_open_span)
        .ok_or_else(|| err("cell formula 시작 태그 범위가 손상되었습니다."))?;
    let prefix = inner_xml
        .get(..f_start)
        .ok_or_else(|| err("cell formula prefix 범위가 손상되었습니다."))?;
    let suffix = if open_tag.trim_ascii_end().ends_with("/>") {
        let suffix_start = checked_usize_add(f_end, 1, "cell formula suffix 시작")?;
        inner_xml
            .get(suffix_start..)
            .ok_or_else(|| err("cell formula suffix 범위가 손상되었습니다."))?
    } else {
        let close_search_from = checked_usize_add(f_end, 1, "cell formula 종료 태그 검색 시작")?;
        let Some(close_start) = find_end_tag(inner_xml, "f", close_search_from) else {
            return Err(err("cell formula 종료 태그를 찾지 못했습니다."));
        };
        let Some(close_end) = find_tag_end(inner_xml, close_start) else {
            return Err(err("cell formula 종료 태그가 손상되었습니다."));
        };
        let suffix_start = checked_usize_add(close_end, 1, "cell formula suffix 시작")?;
        inner_xml
            .get(suffix_start..)
            .ok_or_else(|| err("cell formula suffix 범위가 손상되었습니다."))?
    };
    let escaped_formula =
        try_xml_escape_text(formula, XmlEscapeContext::Text, "cell formula XML escape")?;
    let capacity = checked_capacity(&[
        prefix.len(),
        "<>".len(),
        f_name.len(),
        escaped_formula.len(),
        "</>".len(),
        f_name.len(),
        suffix.len(),
    ])
    .ok_or_else(|| err("cell formula plain replacement 용량 계산 실패"))?;
    let mut out = String::new();
    out.try_reserve_exact(capacity).map_err(|source| {
        err_with_source("cell formula plain replacement 메모리 확보 실패", source)
    })?;
    out.push_str(prefix);
    out.push('<');
    out.push_str(f_name);
    out.push('>');
    out.push_str(&escaped_formula);
    out.push_str("</");
    out.push_str(f_name);
    out.push('>');
    out.push_str(suffix);
    Ok(out)
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
fn qualified_name_with_prefix(
    maybe_prefix: Option<&str>,
    local_name: &'static str,
    context: &str,
) -> Result<Cow<'static, str>> {
    let Some(prefix) = maybe_prefix else {
        return Ok(Cow::Borrowed(local_name));
    };
    let capacity = checked_capacity(&[prefix.len(), ":".len(), local_name.len()])
        .ok_or_else(|| err(format!("{context} 용량 계산 실패")))?;
    let mut out = String::new();
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source(format!("{context} 메모리 확보 실패"), source))?;
    out.push_str(prefix);
    out.push(':');
    out.push_str(local_name);
    Ok(Cow::Owned(out))
}
fn append_nested_text_xml(
    out: &mut String,
    outer_name: &str,
    text_name: &str,
    text: &str,
    context: &str,
) -> Result<()> {
    let escaped_capacity = validated_xml_escaped_len(text, XmlEscapeContext::Text, context)?;
    let text_attrs = if text.starts_with(' ') || text.ends_with(' ') || text.contains("  ") {
        XML_SPACE_PRESERVE_ATTR
    } else {
        ""
    };
    let tag_markup_len = outer_name
        .len()
        .checked_add(text_name.len())
        .and_then(|len| len.checked_mul(2))
        .and_then(|len| len.checked_add("<><></></>".len()))
        .and_then(|len| len.checked_add(text_attrs.len()))
        .ok_or_else(|| err(format!("{context} tag 용량 계산 실패")))?;
    let capacity = checked_usize_add(tag_markup_len, escaped_capacity, "XML text 용량 계산 실패")?;
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source(format!("{context} 메모리 확보 실패"), source))?;
    push_start_tag_name(out, outer_name, "");
    push_start_tag_name(out, text_name, text_attrs);
    append_xml_escaped(out, text, XmlEscapeContext::Text);
    push_end_tag_name(out, text_name);
    push_end_tag_name(out, outer_name);
    Ok(())
}
fn append_peer_text_tag(
    xml: &mut String,
    anchor_tag_name: &str,
    tag_name: &'static str,
    text: &str,
) -> Result<()> {
    let qualified_name = {
        let mut scanner = XmlScanner::new(xml);
        let Some(anchor_tag) = scanner.next_start_named(anchor_tag_name) else {
            return Err(err(tag_error_message(
                anchor_tag_name,
                " anchor 태그를 찾지 못했습니다.",
            )));
        };
        qualified_name_with_prefix(
            anchor_tag.name().rsplit_once(':').map(|(prefix, _)| prefix),
            tag_name,
            &tag_error_message(tag_name, " qualified name"),
        )?
    };
    let capacity = checked_capacity(&[
        "<".len(),
        qualified_name.len(),
        ">".len(),
        text.len(),
        "</".len(),
        qualified_name.len(),
        ">".len(),
    ])
    .ok_or_else(|| err(tag_error_message(tag_name, " text tag 용량 계산 실패")))?;
    xml.try_reserve_exact(capacity).map_err(|source| {
        err_with_source(
            tag_error_message(tag_name, " text tag 메모리 확보 실패"),
            source,
        )
    })?;
    push_start_tag_name(xml, qualified_name.as_ref(), "");
    xml.push_str(text);
    push_end_tag_name(xml, qualified_name.as_ref());
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
        let encoded_len = match ch {
            '&' => "&amp;".len(),
            '<' => "&lt;".len(),
            '>' => "&gt;".len(),
            '"' if matches!(context, XmlEscapeContext::Attribute) => "&quot;".len(),
            '\'' if matches!(context, XmlEscapeContext::Attribute) => "&apos;".len(),
            _ => ch.len_utf8(),
        };
        total
            .checked_add(encoded_len)
            .ok_or_else(|| err(format!("{error_context} 용량 계산 실패")))
    })
}
fn xml_escaped_len(text: &str, context: XmlEscapeContext) -> Option<usize> {
    text.chars().try_fold(0_usize, |total, ch| {
        let encoded_len = match ch {
            '&' => "&amp;".len(),
            '<' => "&lt;".len(),
            '>' => "&gt;".len(),
            '"' if matches!(context, XmlEscapeContext::Attribute) => "&quot;".len(),
            '\'' if matches!(context, XmlEscapeContext::Attribute) => "&apos;".len(),
            _ => ch.len_utf8(),
        };
        total.checked_add(encoded_len)
    })
}
fn append_xml_escaped(out: &mut String, text: &str, context: XmlEscapeContext) {
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' if matches!(context, XmlEscapeContext::Attribute) => out.push_str("&quot;"),
            '\'' if matches!(context, XmlEscapeContext::Attribute) => out.push_str("&apos;"),
            _ => out.push(ch),
        }
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
fn build_formula_with_empty_value(formula_text: &str) -> Result<String> {
    let capacity = checked_capacity(&["<f></f><v></v>".len(), formula_text.len()])
        .ok_or_else(|| err("formula XML 용량 계산 실패"))?;
    let mut out = String::new();
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source("formula XML 메모리 확보 실패", source))?;
    out.push_str("<f>");
    out.push_str(formula_text);
    out.push_str("</f><v></v>");
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
fn build_decimal_display_text_tag(
    name: &str,
    sign: Option<char>,
    magnitude: u64,
) -> Result<String> {
    let mut buffer = [0_u8; MAX_DECIMAL_TEXT_LEN];
    let text = decimal_text(magnitude, &mut buffer, "XML 표시값 작성 실패")?;
    let tag_name_len = name
        .len()
        .checked_mul(2)
        .ok_or_else(|| err("XML 표시값 태그 이름 용량 계산 실패"))?;
    let capacity = checked_capacity(&[
        "<></>".len(),
        tag_name_len,
        usize::from(sign.is_some()),
        text.len(),
    ])
    .ok_or_else(|| err("XML 표시값 태그 용량 계산 실패"))?;
    let mut out = String::new();
    out.try_reserve_exact(capacity)
        .map_err(|source| err_with_source("XML 표시값 태그 메모리 확보 실패", source))?;
    out.push('<');
    out.push_str(name);
    out.push('>');
    if let Some(sign_char) = sign {
        out.push(sign_char);
    }
    out.push_str(text);
    out.push_str("</");
    out.push_str(name);
    out.push('>');
    Ok(out)
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
    push_u32_decimal_text(
        &mut out,
        rows.start,
        "cell range start row 메모리 확보 실패",
    )?;
    out.push(':');
    out.push_str(&end_ref);
    Ok(out)
}
fn row_only_error(prefix: &str, row_num: u32) -> String {
    let fallback = || format!("{prefix}{row_num})");
    let Some(capacity) = checked_capacity(&[prefix.len(), U32_DECIMAL_TEXT_MAX_LEN, 1]) else {
        return fallback();
    };
    let mut out = String::new();
    if out.try_reserve_exact(capacity).is_err() {
        return fallback();
    }
    out.push_str(prefix);
    if push_u32_decimal_text(&mut out, row_num, "row error row 번호 작성 실패").is_err() {
        return fallback();
    }
    out.push(')');
    out
}
fn row_col_error(prefix: &str, row_num: u32, col: u32) -> String {
    let fallback = || format!("{prefix}{row_num}, col={col})");
    let Some(capacity) = checked_capacity(&[
        prefix.len(),
        U32_DECIMAL_TEXT_MAX_LEN,
        ", col=".len(),
        U32_DECIMAL_TEXT_MAX_LEN,
        1,
    ]) else {
        return fallback();
    };
    let mut out = String::new();
    if out.try_reserve_exact(capacity).is_err() {
        return fallback();
    }
    out.push_str(prefix);
    if push_u32_decimal_text(&mut out, row_num, "row error row 번호 작성 실패").is_err() {
        return fallback();
    }
    out.push_str(", col=");
    if push_u32_decimal_text(&mut out, col, "row error column 번호 작성 실패").is_err() {
        return fallback();
    }
    out.push(')');
    out
}
fn tag_error_message(tag_name: &str, suffix: &str) -> String {
    let Some(capacity) = checked_capacity(&[tag_name.len(), suffix.len()]) else {
        return format!("{tag_name}{suffix}");
    };
    let mut out = String::new();
    if out.try_reserve_exact(capacity).is_err() {
        return format!("{tag_name}{suffix}");
    }
    out.push_str(tag_name);
    out.push_str(suffix);
    out
}
