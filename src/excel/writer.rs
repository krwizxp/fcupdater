use self::cell_ref::{
    CellReference, MAX_A1_COL, MAX_A1_ROW, parse_range_token, parse_ref_with_locks, ref_with_locks,
    rewrite_formula_cell_refs, shift_formula_index, try_parse_and_rewrite_cell_ref,
};
use super::{
    ooxml,
    xlsx_container::XlsxContainer,
    xml::{
        XmlScanner, decode_xml_entities, extract_all_tag_text, extract_first_tag_text,
        find_end_tag, find_start_tag, find_tag_end,
    },
};
use crate::{Result, err, err_with_source, parse_i32_str};
use alloc::borrow::Cow;
use alloc::collections::BTreeMap;
use core::{
    fmt::{Display, Write as FmtWrite},
    mem,
    ops::RangeBounds,
    range::{Range, RangeInclusive},
};
use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::path::Path;
mod cell_ref;
#[derive(Debug)]
pub struct Workbook {
    container: XlsxContainer,
    shared_strings: Vec<String>,
    shared_strings_xml_text: Option<String>,
    sheets: BTreeMap<String, SheetEntry>,
    xml_text: String,
}
#[derive(Debug)]
struct SheetEntry {
    path: String,
    worksheet: Worksheet,
}
#[derive(Debug, Clone, Default)]
pub struct Worksheet {
    prefix: String,
    rows: BTreeMap<u32, Row>,
    suffix: String,
}
#[derive(Debug, Clone, Default)]
pub struct Row {
    attrs: Vec<XmlAttr>,
    cells: BTreeMap<u32, Cell>,
}
#[derive(Debug, Clone, Default)]
pub struct Cell {
    attrs: Vec<XmlAttr>,
    inner_xml: Option<String>,
}
#[derive(Debug, Clone)]
struct XmlAttr {
    name: String,
    value: String,
}
struct WorksheetRowParser<'row> {
    row_body: &'row str,
    row_num: u32,
}
struct WorksheetRowsParser<'body> {
    body: &'body str,
}
struct WorksheetXmlParser<'xml> {
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
impl Workbook {
    pub fn open(path: &Path) -> Result<Self> {
        let container = XlsxContainer::open(path)?;
        let ooxml = ooxml::XlsxOoxml {
            container: &container,
        };
        let sheet_catalog = ooxml.load_sheet_catalog()?;
        let workbook_xml = container.read_text("xl/workbook.xml")?;
        let shared_strings_xml_text = if container
            .unpack_dir()
            .join("xl")
            .join("sharedStrings.xml")
            .is_file()
        {
            Some(container.read_text("xl/sharedStrings.xml")?)
        } else {
            None
        };
        let shared_strings = ooxml.load_shared_strings()?;
        let mut sheets = BTreeMap::new();
        for sheet_info in sheet_catalog {
            let xml = container.read_text(&sheet_info.path)?;
            let mut worksheet = WorksheetXmlParser { xml: &xml }.parse()?;
            worksheet.normalize_shared_formulas()?;
            sheets.insert(
                sheet_info.name,
                SheetEntry {
                    path: sheet_info.path,
                    worksheet,
                },
            );
        }
        Ok(Self {
            container,
            shared_strings,
            shared_strings_xml_text,
            sheets,
            xml_text: workbook_xml,
        })
    }
    fn promote_safe_inline_strings_to_shared(&mut self) -> Result<()> {
        if self.shared_strings_xml_text.is_none() {
            return Ok(());
        }
        let existing_total = self.shared_strings.len();
        let existing_unique = {
            let mut seen: HashSet<&str> = HashSet::new();
            seen.try_reserve(self.shared_strings.len())
                .map_err(|source| {
                    let shared_string_count = self.shared_strings.len();
                    err_with_source(
                        format!(
                            "shared string 중복 집합 메모리 확보 실패: {shared_string_count} entries"
                        ),
                        source,
                    )
                })?;
            seen.extend(self.shared_strings.iter().map(String::as_str));
            seen.len()
        };
        let mut new_index: HashMap<String, usize> = HashMap::new();
        let mut next_new_idx = self.shared_strings.len();
        {
            let mut existing_index: HashMap<&str, usize> = HashMap::new();
            existing_index
                .try_reserve(self.shared_strings.len())
                .map_err(|source| {
                    let shared_string_count = self.shared_strings.len();
                    err_with_source(
                        format!(
                            "기존 shared string index map 메모리 확보 실패: {shared_string_count} entries"
                        ),
                        source,
                    )
                })?;
            for (idx, value) in self.shared_strings.iter().enumerate() {
                existing_index.entry(value.as_str()).or_insert(idx);
            }
            for sheet in self.sheets.values_mut() {
                for row in sheet.worksheet.rows.values_mut() {
                    for cell in row.cells.values_mut() {
                        if get_attr(&cell.attrs, "t") != Some("inlineStr") {
                            continue;
                        }
                        let Some(inner_xml) = cell.inner_xml.as_deref() else {
                            continue;
                        };
                        let Some(text) = ({
                            if !inner_xml.contains("<is")
                                || inner_xml.contains("<r")
                                || inner_xml.contains("<rPr")
                                || inner_xml.contains("<rPh")
                                || inner_xml.contains("<phoneticPr")
                            {
                                None
                            } else {
                                extract_all_tag_text(inner_xml, "t")
                            }
                        }) else {
                            continue;
                        };
                        let shared_idx = if let Some(&idx) = existing_index.get(text.as_str()) {
                            idx
                        } else {
                            match new_index.entry(text) {
                                Entry::Occupied(entry) => *entry.get(),
                                Entry::Vacant(entry) => {
                                    let idx = next_new_idx;
                                    next_new_idx = next_new_idx.checked_add(1).ok_or_else(|| {
                                        err("shared string 신규 index 계산 중 overflow가 발생했습니다.")
                                    })?;
                                    entry.insert(idx);
                                    idx
                                }
                            }
                        };
                        set_attr(&mut cell.attrs, "t", "s");
                        cell.inner_xml = Some(build_display_text_tag("v", shared_idx));
                    }
                }
            }
        }
        if new_index.is_empty() {
            return Ok(());
        }
        let mut new_strings: Vec<(usize, String)> = Vec::new();
        new_strings
            .try_reserve_exact(new_index.len())
            .map_err(|source| {
                let new_count = new_index.len();
                err_with_source(
                    format!("신규 shared string 목록 메모리 확보 실패: {new_count} entries"),
                    source,
                )
            })?;
        new_strings.extend(new_index.into_iter().map(|(value, idx)| (idx, value)));
        new_strings.sort_unstable_by_key(|entry| entry.0);
        self.shared_strings
            .extend(new_strings.into_iter().map(|(_, value)| value));
        self.update_shared_strings_xml_text(existing_total, existing_unique)
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
        self.container.remove_file_if_exists("xl/calcChain.xml")?;
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
        if let Some(calc_pr_start) = find_start_tag(out, "calcPr", 0) {
            let Some(calc_pr_tag_end) = find_tag_end(out, calc_pr_start) else {
                return Err(err("workbook.xml의 calcPr 태그가 손상되었습니다."));
            };
            let calc_pr_open_span = RangeInclusive {
                start: calc_pr_start,
                last: calc_pr_tag_end,
            };
            let (mut attrs, self_closing) = {
                let calc_pr_tag = out
                    .get(calc_pr_open_span)
                    .ok_or_else(|| err("workbook.xml의 calcPr 태그 범위가 손상되었습니다."))?;
                (parse_tag_attrs(calc_pr_tag)?, calc_pr_tag.ends_with("/>"))
            };
            reserve_xml_attrs(
                &mut attrs,
                4,
                "calcPr 속성 목록 추가 메모리 확보 실패",
                XmlReserveMode::Additional,
            )?;
            set_calc_pr_attrs(&mut attrs);
            if self_closing {
                let new_tag = build_self_closing_tag("calcPr", &attrs);
                out.replace_range(calc_pr_open_span, &new_tag);
            } else {
                let close_search_from =
                    checked_usize_add(calc_pr_tag_end, 1, "calcPr 종료 태그 검색 시작")?;
                let Some(calc_pr_close_start) = find_end_tag(out, "calcPr", close_search_from)
                else {
                    return Err(err("workbook.xml의 calcPr 종료 태그를 찾지 못했습니다."));
                };
                let calc_pr_close_end = checked_usize_add(
                    calc_pr_close_start,
                    "</calcPr>".len(),
                    "calcPr 종료 태그 끝",
                )?;
                let mut new_tag = build_open_tag("calcPr", &attrs);
                new_tag.push_str("</calcPr>");
                out.replace_range(
                    Range {
                        start: calc_pr_start,
                        end: calc_pr_close_end,
                    },
                    &new_tag,
                );
            }
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
            let new_tag = build_self_closing_tag("calcPr", &attrs);
            out.insert_str(workbook_close_start, &new_tag);
        }
        Ok(())
    }
    pub fn save(&mut self, target_path: &Path) -> Result<()> {
        self.promote_safe_inline_strings_to_shared()?;
        self.request_full_recalculation()?;
        self.remove_excel_recovery_artifacts()?;
        self.container
            .write_text("xl/workbook.xml", &self.xml_text)?;
        if let Some(shared_strings_xml) = self.shared_strings_xml_text.as_ref() {
            self.container
                .write_text("xl/sharedStrings.xml", shared_strings_xml)?;
        }
        for sheet in self.sheets.values() {
            self.container
                .write_text(&sheet.path, &sheet.worksheet.to_xml())?;
        }
        self.container.save(target_path)
    }
    fn update_shared_strings_xml_text(
        &mut self,
        existing_total: usize,
        existing_unique: usize,
    ) -> Result<()> {
        let new_values = self
            .shared_strings
            .get(existing_total..)
            .ok_or_else(|| err("sharedStrings 신규 값 범위가 손상되었습니다."))?;
        let original_xml = self
            .shared_strings_xml_text
            .take()
            .ok_or_else(|| err("sharedStrings XML 상태가 비정상적입니다."))?;
        let Some(open_start) = find_start_tag(&original_xml, "sst", 0) else {
            return Err(err("sharedStrings XML에 <sst>가 없습니다."));
        };
        let Some(open_end) = find_tag_end(&original_xml, open_start) else {
            return Err(err("sharedStrings XML의 <sst> 시작 태그가 손상되었습니다."));
        };
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
        let new_total = existing_total.saturating_add(new_values.len());
        let new_unique = existing_unique.saturating_add(new_values.len());
        set_attr(&mut attrs, "count", display_string(new_total));
        set_attr(&mut attrs, "uniqueCount", display_string(new_unique));
        let mut new_si_xml = String::new();
        for value in new_values {
            let escaped_capacity = value.len().saturating_mul(6);
            let item_capacity = "<si><t></t></si>"
                .len()
                .saturating_add(" xml:space=\"preserve\"".len())
                .saturating_add(escaped_capacity);
            new_si_xml.try_reserve(item_capacity).map_err(|source| {
                err_with_source("shared string XML item 메모리 확보 실패", source)
            })?;
            if needs_xml_space_preserve(value) {
                new_si_xml.push_str("<si><t xml:space=\"preserve\">");
            } else {
                new_si_xml.push_str("<si><t>");
            }
            append_xml_escaped(&mut new_si_xml, value, XmlEscapeContext::Text);
            new_si_xml.push_str("</t></si>");
        }
        let updated_xml = if open_tag.trim_ascii_end().ends_with("/>") {
            let mut replacement = build_open_tag("sst", &attrs);
            replacement.push_str(&new_si_xml);
            replacement.push_str("</sst>");
            let mut out = original_xml;
            out.replace_range(open_tag_span, &replacement);
            out
        } else {
            let new_open_tag = build_open_tag("sst", &attrs);
            let mut out = original_xml;
            out.replace_range(open_tag_span, &new_open_tag);
            let close_search_from = checked_usize_add(
                open_start,
                new_open_tag.len(),
                "sharedStrings 종료 태그 검색 시작",
            )?;
            let Some(close_start) = find_end_tag(&out, "sst", close_search_from) else {
                return Err(err("sharedStrings XML에 </sst>가 없습니다."));
            };
            out.insert_str(close_start, &new_si_xml);
            out
        };
        self.shared_strings_xml_text = Some(updated_xml);
        Ok(())
    }
    pub fn with_sheet_mut<R, F>(&mut self, name: &str, mutator: F) -> Option<R>
    where
        F: FnOnce(&mut Worksheet, &[String]) -> R,
    {
        let (shared_strings, sheets) = (&self.shared_strings, &mut self.sheets);
        let ws = &mut sheets.get_mut(name)?.worksheet;
        Some(mutator(ws, shared_strings))
    }
    pub const fn workbook_xml_mut(&mut self) -> &mut String {
        &mut self.xml_text
    }
}
impl WorksheetRowParser<'_> {
    fn parse_into(&self, row: &mut Row) -> Result<()> {
        let mut scanner = XmlScanner::new(self.row_body);
        let mut next_col = 1_u32;
        while let Some(cell_info) = scanner.next_start_named("c") {
            let cell_tag_end = cell_info.end();
            let cell_tag = cell_info.tag();
            let mut attrs = parse_tag_attrs(cell_tag)?;
            let col = get_attr(&attrs, "r")
                .and_then(parse_ref_with_locks)
                .map_or(next_col, |reference| reference.col);
            reserve_xml_attrs(
                &mut attrs,
                1,
                "cell 속성 목록 추가 메모리 확보 실패",
                XmlReserveMode::Additional,
            )?;
            set_attr(
                &mut attrs,
                "r",
                ref_with_locks(CellReference::unlocked(col, self.row_num)),
            );
            if cell_info.is_self_closing() {
                row.cells.insert(
                    col,
                    Cell {
                        attrs,
                        inner_xml: None,
                    },
                );
                next_col = col.saturating_add(1);
                continue;
            }
            let cell_body_start = checked_usize_add(cell_tag_end, 1, "row cell 본문 시작")?;
            let Some(cell_close_rel) = self
                .row_body
                .get(cell_body_start..)
                .and_then(|tail| tail.find("</c>"))
            else {
                return Err(err(row_col_error(
                    "row 내 cell 종료 태그를 찾지 못했습니다. (row=",
                    self.row_num,
                    col,
                )));
            };
            let cell_body_end =
                checked_usize_add(cell_body_start, cell_close_rel, "row cell 본문 끝")?;
            let cell_body_span = Range {
                start: cell_body_start,
                end: cell_body_end,
            };
            let inner_xml = self
                .row_body
                .get(cell_body_span)
                .ok_or_else(|| {
                    err(row_col_error(
                        "row 내 cell 본문 범위가 손상되었습니다. (row=",
                        self.row_num,
                        col,
                    ))
                })?
                .to_owned();
            row.cells.insert(
                col,
                Cell {
                    attrs,
                    inner_xml: Some(inner_xml),
                },
            );
            next_col = col.saturating_add(1);
            scanner.skip_to(checked_usize_add(
                cell_body_end,
                "</c>".len(),
                "row cell 다음 cursor",
            )?);
        }
        Ok(())
    }
}
impl WorksheetRowsParser<'_> {
    fn parse(&self) -> Result<BTreeMap<u32, Row>> {
        let mut rows: BTreeMap<u32, Row> = BTreeMap::new();
        let mut scanner = XmlScanner::new(self.body);
        while let Some(row_info) = scanner.next_start_named("row") {
            let row_tag_end = row_info.end();
            let row_tag = row_info.tag();
            let mut row_attrs = parse_tag_attrs(row_tag)?;
            let row_num = get_attr(&row_attrs, "r")
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or_else(|| {
                    rows.keys()
                        .next_back()
                        .copied()
                        .map_or(1_u32, |last_row| last_row.saturating_add(1))
                });
            reserve_xml_attrs(
                &mut row_attrs,
                1,
                "row 속성 목록 추가 메모리 확보 실패",
                XmlReserveMode::Additional,
            )?;
            set_attr(&mut row_attrs, "r", display_string(row_num));
            if row_info.is_self_closing() {
                rows.insert(
                    row_num,
                    Row {
                        attrs: row_attrs,
                        cells: BTreeMap::new(),
                    },
                );
                continue;
            }
            let row_body_start = checked_usize_add(row_tag_end, 1, "sheetData row 본문 시작")?;
            let Some(row_close_rel) = self
                .body
                .get(row_body_start..)
                .and_then(|tail| tail.find("</row>"))
            else {
                return Err(err(row_only_error(
                    "sheetData row 종료 태그를 찾지 못했습니다. (row=",
                    row_num,
                )));
            };
            let row_body_end =
                checked_usize_add(row_body_start, row_close_rel, "sheetData row 본문 끝")?;
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
            WorksheetRowParser { row_body, row_num }.parse_into(&mut row)?;
            rows.insert(row_num, row);
            scanner.skip_to(checked_usize_add(
                row_body_end,
                "</row>".len(),
                "sheetData row 다음 cursor",
            )?);
        }
        Ok(rows)
    }
}
impl WorksheetXmlParser<'_> {
    fn parse(&self) -> Result<Worksheet> {
        let Some(sheet_data_open) = find_start_tag(self.xml, "sheetData", 0) else {
            return Err(err("worksheet XML에 <sheetData>가 없습니다."));
        };
        let Some(sheet_data_open_end) = find_tag_end(self.xml, sheet_data_open) else {
            return Err(err(
                "worksheet XML의 <sheetData> 시작 태그가 손상되었습니다.",
            ));
        };
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
        let prefix = prefix_raw.to_owned();
        let suffix = suffix_raw.to_owned();
        let rows = WorksheetRowsParser { body }.parse()?;
        Ok(Worksheet {
            prefix,
            rows,
            suffix,
        })
    }
}
impl Worksheet {
    pub fn clear_cells_in_rows_through_col(&mut self, rows: RangeInclusive<u32>, max_col: u32) {
        for (_, row_obj) in self.rows.range_mut(rows) {
            for (_, cell) in row_obj.cells.range_mut(..=max_col) {
                remove_attr(&mut cell.attrs, "t");
                cell.inner_xml = None;
            }
        }
    }
    pub fn clear_formula_cached_values_in_range<R>(&mut self, rows: R) -> Result<()>
    where
        R: RangeBounds<u32>,
    {
        for (_, row) in self.rows.range_mut(rows) {
            for cell in row.cells.values_mut() {
                let Some(inner) = cell.inner_xml.as_mut() else {
                    continue;
                };
                if find_start_tag(inner, "f", 0).is_none() {
                    continue;
                }
                if !replace_first_tag_text(inner, "v", "")? && !inner.contains("<v>") {
                    inner.push_str("<v></v>");
                }
            }
        }
        Ok(())
    }
    pub fn clone_row_style(
        &mut self,
        source_row: u32,
        target_row: u32,
        max_col: u32,
    ) -> Result<()> {
        let Some(src) = self.rows.get(&source_row).cloned() else {
            return Ok(());
        };
        let mut cloned = src;
        remap_row_numbers(&mut cloned, target_row, &|row_num| {
            if row_num == source_row {
                target_row
            } else {
                row_num
            }
        })?;
        cloned.cells.retain(|col, _| *col <= max_col);
        for cell in cloned.cells.values_mut() {
            let Some(inner) = cell.inner_xml.as_mut() else {
                remove_attr(&mut cell.attrs, "t");
                continue;
            };
            if extract_first_tag_text(inner, "f").is_none() {
                remove_attr(&mut cell.attrs, "t");
                cell.inner_xml = None;
            }
        }
        self.rows.insert(target_row, cloned);
        Ok(())
    }
    pub fn extend_conditional_formats(
        &mut self,
        data_rows: RangeInclusive<u32>,
        target_cols: &[u32],
    ) -> Result<()> {
        if target_cols.is_empty() {
            return Ok(());
        }
        let data_start_row = data_rows.start;
        let last_data_row = data_rows.last;
        let out = &mut self.suffix;
        let mut cursor = 0_usize;
        while let Some(cf_rel) = out
            .get(cursor..)
            .and_then(|tail| tail.find("<conditionalFormatting"))
        {
            let cf_start = checked_usize_add(cursor, cf_rel, "conditionalFormatting 시작")?;
            let Some(cf_end_rel) = out.get(cf_start..).and_then(|tail| tail.find('>')) else {
                break;
            };
            let cf_end = checked_usize_add(
                checked_usize_add(cf_start, cf_end_rel, "conditionalFormatting 태그 끝")?,
                1,
                "conditionalFormatting 태그 끝",
            )?;
            let cf_span = Range {
                start: cf_start,
                end: cf_end,
            };
            let mut attrs = {
                let tag = out
                    .get(cf_span)
                    .ok_or_else(|| err("conditionalFormatting 태그 범위가 손상되었습니다."))?;
                parse_tag_attrs(tag)?
            };
            if let Some(sqref) = get_attr(&attrs, "sqref").map(ToOwned::to_owned) {
                let updated_sqref = {
                    let mut changed = false;
                    let range_count = sqref.split_whitespace().count();
                    let mut ranges_out: Vec<String> = Vec::new();
                    ranges_out
                        .try_reserve_exact(range_count)
                        .map_err(|source| {
                            err_with_source(
                                format!(
                                    "conditionalFormatting range 목록 메모리 확보 실패: {range_count} ranges"
                                ),
                                source,
                            )
                        })?;
                    for token in sqref.split_whitespace() {
                        let (start_ref, end_ref) = parse_range_token(token);
                        let Some(start_reference) = parse_ref_with_locks(start_ref) else {
                            ranges_out.push(token.to_owned());
                            continue;
                        };
                        let Some(end_reference) = parse_ref_with_locks(end_ref) else {
                            ranges_out.push(token.to_owned());
                            continue;
                        };
                        if start_reference.row != data_start_row
                            || end_reference.row != data_start_row
                        {
                            ranges_out.push(token.to_owned());
                            continue;
                        }
                        if !target_cols
                            .iter()
                            .any(|col| (start_reference.col..=end_reference.col).contains(col))
                        {
                            ranges_out.push(token.to_owned());
                            continue;
                        }
                        let new_start = ref_with_locks(start_reference.with_row(data_start_row));
                        let new_end = ref_with_locks(end_reference.with_row(last_data_row));
                        ranges_out.push(format!("{new_start}:{new_end}"));
                        changed = true;
                    }
                    if changed { ranges_out.join(" ") } else { sqref }
                };
                set_attr(&mut attrs, "sqref", updated_sqref);
                let new_tag = build_open_tag("conditionalFormatting", &attrs);
                out.replace_range(cf_span, &new_tag);
                cursor = checked_usize_add(
                    cf_start,
                    new_tag.len(),
                    "conditionalFormatting 다음 cursor",
                )?;
            } else {
                cursor = cf_end;
            }
        }
        Ok(())
    }
    pub fn get_display_at<'text>(
        &'text self,
        col: u32,
        row: u32,
        shared_strings: &'text [String],
    ) -> Cow<'text, str> {
        let Some(row_obj) = self.rows.get(&row) else {
            return Cow::Borrowed("");
        };
        let Some(cell) = row_obj.cells.get(&col) else {
            return Cow::Borrowed("");
        };
        let cell_type = get_attr(&cell.attrs, "t");
        let inner = cell.inner_xml.as_deref().unwrap_or_default();
        let display = match cell_type {
            Some("inlineStr") => extract_all_tag_text(inner, "t").map(Cow::Owned),
            ordinary_type => {
                let raw_v = extract_first_tag_text(inner, "v").unwrap_or_default();
                match ordinary_type {
                    Some("s") => raw_v
                        .parse::<usize>()
                        .ok()
                        .and_then(|idx| shared_strings.get(idx))
                        .map(|value| Cow::Borrowed(value.as_str())),
                    Some("b") => Some(if raw_v == "1" {
                        Cow::Borrowed("TRUE")
                    } else {
                        Cow::Borrowed("FALSE")
                    }),
                    _ => Some(decode_xml_entities(raw_v)),
                }
            }
        };
        display.unwrap_or(Cow::Borrowed(""))
    }
    pub fn get_formula_at(&self, col: u32, row: u32) -> Option<Cow<'_, str>> {
        self.rows
            .get(&row)?
            .cells
            .get(&col)?
            .inner_xml
            .as_deref()
            .and_then(|inner| extract_first_tag_text(inner, "f"))
            .map(decode_xml_entities)
    }
    pub fn get_i32_at(&self, col: u32, row: u32, shared_strings: &[String]) -> Option<i32> {
        let text = self.get_display_at(col, row, shared_strings);
        parse_i32_str(&text)
    }
    fn get_or_create_cell_mut(&mut self, col: u32, row: u32) -> &mut Cell {
        let row_obj = self
            .rows
            .entry(row)
            .or_insert_with_key(|&row_key| Row::numbered(row_key));
        if get_attr(&row_obj.attrs, "r").is_none() {
            set_attr(&mut row_obj.attrs, "r", display_string(row));
        }
        row_obj
            .cells
            .entry(col)
            .or_insert_with_key(|&col_key| Cell {
                attrs: vec![
                    owned_attr("r", ref_with_locks(CellReference::unlocked(col_key, row))),
                    owned_attr("s", "0"),
                ],
                inner_xml: None,
            })
    }
    pub fn has_any_row_format(&self, row: u32, max_col: u32) -> bool {
        self.rows.get(&row).is_some_and(|row_obj| {
            !row_obj.attrs.is_empty()
                || (max_col > 0 && row_obj.cells.range(1..=max_col).next().is_some())
        })
    }
    pub fn has_row(&self, row: u32) -> bool {
        self.rows.contains_key(&row)
    }
    pub fn insert_row(&mut self, row_num: u32, row: Row) -> Option<Row> {
        self.rows.insert(row_num, row)
    }
    pub fn max_cell_col(&self) -> u32 {
        self.rows
            .values()
            .filter_map(|row| row.cells.keys().next_back().copied())
            .max()
            .unwrap_or(1)
    }
    pub fn max_col_in_row(&self, row: u32) -> Option<u32> {
        let row_obj = self.rows.get(&row)?;
        row_obj.cells.keys().next_back().copied()
    }
    pub fn max_row_num(&self) -> u32 {
        self.rows.keys().next_back().copied().unwrap_or(1)
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
                heads.insert(
                    spec.si,
                    SharedFormulaHead {
                        anchor_col: *col_num,
                        anchor_row: *row_num,
                        formula: formula_text,
                    },
                );
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
                let head = heads.get(&spec.si).ok_or_else(|| {
                    let col_name = col_to_name(*col_num);
                    err(format!(
                        "shared formula head를 찾지 못했습니다. (si={}, cell={col_name}{row_num} )",
                        spec.si
                    ))
                })?;
                let formula = if let Some(text) = spec.formula_text {
                    text
                } else {
                    rewrite_formula_cell_refs(&head.formula, |chars, start| {
                        let delta_col =
                            i64::from(*col_num).saturating_sub(i64::from(head.anchor_col));
                        let delta_row =
                            i64::from(*row_num).saturating_sub(i64::from(head.anchor_row));
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
                                Ok((new_col, new_row))
                            },
                        )
                    })?
                };
                cell.inner_xml = Some(replace_formula_tag_with_plain_formula(inner_xml, &formula)?);
            }
        }
        Ok(())
    }
    pub fn remove_row(&mut self, row: u32) -> Option<Row> {
        self.rows.remove(&row)
    }
    pub fn replace_rows(&mut self, rows: BTreeMap<u32, Row>) {
        self.rows = rows;
    }
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }
    pub fn row_has_any_data(&self, row: u32, cols: &[u32], shared_strings: &[String]) -> bool {
        cols.iter().any(|col| {
            !self
                .get_display_at(*col, row, shared_strings)
                .trim()
                .is_empty()
        })
    }
    pub fn row_numbers_from(&self, start: u32) -> impl DoubleEndedIterator<Item = u32> + '_ {
        self.rows.range(start..).map(|(&row, _)| row)
    }
    pub fn set_formula_at(&mut self, col: u32, row: u32, formula: &str) -> Result<()> {
        let cell = self.get_or_create_cell_mut(col, row);
        let formula_text = xml_escape_text(formula);
        if let Some(inner) = cell.inner_xml.as_mut()
            && replace_first_tag_text(inner, "f", &formula_text)?
        {
            if !inner.contains("<v") {
                inner.push_str("<v></v>");
            }
        } else if let Some(inner) = cell.inner_xml.as_mut() {
            *inner = replace_formula_tag_with_plain_formula(inner, formula)
                .unwrap_or_else(|_| build_formula_with_empty_value(&formula_text));
            if !inner.contains("<v") {
                inner.push_str("<v></v>");
            }
        } else {
            cell.inner_xml = Some(build_formula_with_empty_value(&formula_text));
        }
        Ok(())
    }
    pub fn set_formula_cached_value_at(
        &mut self,
        col: u32,
        row: u32,
        value: Option<&str>,
        cell_type: Option<&str>,
    ) -> Result<()> {
        let cell = self.get_or_create_cell_mut(col, row);
        match cell_type {
            Some(value_type) => set_attr(&mut cell.attrs, "t", value_type),
            None => remove_attr(&mut cell.attrs, "t"),
        }
        let Some(inner) = cell.inner_xml.as_mut() else {
            return Ok(());
        };
        let encoded = value.map(xml_escape_text);
        let value_text = encoded.as_deref().unwrap_or_default();
        if !replace_first_tag_text(inner, "v", value_text)? && !inner.contains("<v>") {
            match FmtWrite::write_fmt(inner, format_args!("<v>{value_text}</v>")) {
                Ok(()) | Err(_) => {}
            }
        }
        Ok(())
    }
    pub fn set_i32_at(&mut self, col: u32, row: u32, value: Option<i32>) {
        let cell = self.get_or_create_cell_mut(col, row);
        remove_attr(&mut cell.attrs, "t");
        if let Some(numeric_value) = value {
            cell.inner_xml = Some(build_display_text_tag("v", numeric_value));
        } else {
            cell.inner_xml = None;
        }
    }
    pub fn set_string_at(&mut self, col: u32, row: u32, value: &str) {
        let cell = self.get_or_create_cell_mut(col, row);
        set_attr(&mut cell.attrs, "t", "inlineStr");
        let text = xml_escape_text(value);
        let inner = if needs_xml_space_preserve(value) {
            format!("<is><t xml:space=\"preserve\">{text}</t></is>")
        } else {
            format!("<is><t>{text}</t></is>")
        };
        cell.inner_xml = Some(inner);
    }
    pub fn take_rows(&mut self) -> BTreeMap<u32, Row> {
        mem::take(&mut self.rows)
    }
    pub fn to_xml(&self) -> String {
        let mut estimated_row_attrs = 0_usize;
        let mut estimated_cell_count = 0_usize;
        let mut estimated_inner_len = 0_usize;
        for row in self.rows.values() {
            estimated_row_attrs = estimated_row_attrs.saturating_add(row.attrs.len());
            estimated_cell_count = estimated_cell_count.saturating_add(row.cells.len());
            for cell in row.cells.values() {
                estimated_inner_len = estimated_inner_len
                    .saturating_add(cell.inner_xml.as_deref().map_or(0, str::len));
            }
        }
        let capacity = self
            .prefix
            .len()
            .saturating_add(self.suffix.len())
            .saturating_add(self.rows.len().saturating_mul("<row></row>".len()))
            .saturating_add(estimated_row_attrs.saturating_mul(12))
            .saturating_add(estimated_cell_count.saturating_mul("<c></c>".len()))
            .saturating_add(estimated_inner_len);
        let mut out = String::with_capacity(capacity);
        out.push_str(&self.prefix);
        for row in self.rows.values() {
            out.push_str("<row");
            push_sorted_attrs_xml(&mut out, &row.attrs);
            if row.cells.is_empty() {
                out.push_str("/>");
                continue;
            }
            out.push('>');
            for cell in row.cells.values() {
                out.push_str("<c");
                push_sorted_attrs_xml(&mut out, &cell.attrs);
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
        out
    }
    pub fn update_auto_filter_ref(&mut self, filter_rows: RangeInclusive<u32>) -> Result<()> {
        let out = &mut self.suffix;
        let mut cursor = 0_usize;
        let header_row = filter_rows.start;
        let target_last_row = filter_rows.start.max(filter_rows.last);
        while let Some(auto_filter_rel) =
            out.get(cursor..).and_then(|tail| tail.find("<autoFilter"))
        {
            let auto_filter_start = checked_usize_add(cursor, auto_filter_rel, "autoFilter 시작")?;
            let Some(auto_filter_end_rel) =
                out.get(auto_filter_start..).and_then(|tail| tail.find('>'))
            else {
                return Err(err("worksheet XML의 autoFilter 태그가 손상되었습니다."));
            };
            let auto_filter_end = checked_usize_add(
                checked_usize_add(auto_filter_start, auto_filter_end_rel, "autoFilter 태그 끝")?,
                1,
                "autoFilter 태그 끝",
            )?;
            let auto_filter_span = Range {
                start: auto_filter_start,
                end: auto_filter_end,
            };
            let (mut attrs, self_closing) = {
                let tag = out
                    .get(auto_filter_span)
                    .ok_or_else(|| err("worksheet XML의 autoFilter 태그 범위가 손상되었습니다."))?;
                (parse_tag_attrs(tag)?, tag.trim_ascii_end().ends_with("/>"))
            };
            let end_col = if let Some(existing_ref) = get_attr(&attrs, "ref")
                && let Some(parsed_end_reference) =
                    parse_ref_with_locks(parse_range_token(existing_ref).1)
            {
                parsed_end_reference.col
            } else {
                1_u32
            };
            let new_ref = build_ref_range(
                "A",
                RangeInclusive {
                    start: header_row,
                    last: target_last_row,
                },
                end_col,
            );
            reserve_xml_attrs(
                &mut attrs,
                1,
                "autoFilter 속성 목록 추가 메모리 확보 실패",
                XmlReserveMode::Additional,
            )?;
            set_attr(&mut attrs, "ref", new_ref);
            let new_tag = if self_closing {
                build_self_closing_tag("autoFilter", &attrs)
            } else {
                build_open_tag("autoFilter", &attrs)
            };
            out.replace_range(auto_filter_span, &new_tag);
            cursor = checked_usize_add(auto_filter_start, new_tag.len(), "autoFilter 다음 cursor")?;
        }
        Ok(())
    }
    pub fn update_dimension(&mut self) -> Result<()> {
        let max_row = self.max_row_num();
        let max_col = self.max_cell_col();
        let out = &mut self.prefix;
        if let Some(dim_pos) = out.find("<dimension")
            && let Some(dim_end_rel) = out.get(dim_pos..).and_then(|tail| tail.find('>'))
        {
            let dim_end = checked_usize_add(
                checked_usize_add(dim_pos, dim_end_rel, "dimension 태그 끝")?,
                1,
                "dimension 태그 끝",
            )?;
            let dim_span = Range {
                start: dim_pos,
                end: dim_end,
            };
            let mut attrs = {
                let tag = out
                    .get(dim_span)
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
                ),
            );
            let new_tag = build_self_closing_tag("dimension", &attrs);
            out.replace_range(dim_span, &new_tag);
        }
        Ok(())
    }
}
impl Row {
    pub fn numbered(row_num: u32) -> Self {
        Self {
            attrs: vec![owned_attr("r", display_string(row_num))],
            cells: BTreeMap::new(),
        }
    }
}
fn checked_usize_add(base: usize, add: usize, context: &str) -> Result<usize> {
    base.checked_add(add).ok_or_else(|| {
        err(format!(
            "{context} offset 계산 중 overflow가 발생했습니다. (base={base}, add={add})"
        ))
    })
}
pub fn remap_row_numbers(row: &mut Row, new_row: u32, resolver: &dyn Fn(u32) -> u32) -> Result<()> {
    set_attr(&mut row.attrs, "r", display_string(new_row));
    for (col, cell) in &mut row.cells {
        set_attr(
            &mut cell.attrs,
            "r",
            ref_with_locks(CellReference::unlocked(*col, new_row)),
        );
        if let Some(inner) = cell.inner_xml.as_mut()
            && let Some(text) = extract_first_tag_text(inner, "f")
        {
            let rewrite_result =
                rewrite_formula_cell_refs(&decode_xml_entities(text), |chars, start| {
                    try_parse_and_rewrite_cell_ref(
                        chars,
                        start,
                        |base_col, base_row, _col_lock, row_lock| {
                            let updated_row = if row_lock {
                                base_row
                            } else {
                                resolver(base_row)
                            };
                            Ok((base_col, updated_row))
                        },
                    )
                });
            let rewritten =
                rewrite_result.unwrap_or_else(|_| decode_xml_entities(text).into_owned());
            let encoded = xml_escape_text(&rewritten);
            replace_first_tag_text(inner, "f", &encoded)?;
        }
    }
    Ok(())
}
pub fn col_to_name(col: u32) -> String {
    cell_ref::col_to_name(col)
}
fn attr_sort_key(name: &str) -> (u8, &str) {
    if name == "r" {
        (0, name)
    } else if name == "s" {
        (1, name)
    } else if name == "t" {
        (2, name)
    } else {
        (3, name)
    }
}
fn push_sorted_attrs_xml(out: &mut String, attrs: &[XmlAttr]) {
    let mut sorted_attrs = attrs.iter().collect::<Vec<_>>();
    sorted_attrs
        .sort_unstable_by(|left, right| attr_sort_key(&left.name).cmp(&attr_sort_key(&right.name)));
    for attr in sorted_attrs {
        push_attr_xml(out, attr);
    }
}
fn attrs_to_xml(attrs: &[XmlAttr]) -> String {
    let mut out = String::new();
    for attr in attrs {
        push_attr_xml(&mut out, attr);
    }
    out
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
    let mut i = lt.saturating_add(1);
    let bytes = tag.as_bytes();
    while bytes
        .get(i)
        .is_some_and(|ch| !ch.is_ascii_whitespace() && !matches!(*ch, b'>' | b'/'))
    {
        i = i.saturating_add(1);
    }
    if i >= bytes.len() {
        return Err(parse_error("XML 태그 파싱 실패: 태그 종료 기호 없음. tag="));
    }
    while i < bytes.len() {
        while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
            i = i.saturating_add(1);
        }
        if bytes.get(i).is_none_or(|ch| matches!(*ch, b'>' | b'/')) {
            break;
        }
        let key_start = i;
        while bytes
            .get(i)
            .is_some_and(|ch| !ch.is_ascii_whitespace() && !matches!(*ch, b'=' | b'>' | b'/'))
        {
            i = i.saturating_add(1);
        }
        let key_end = i;
        if key_start == key_end {
            return Err(parse_error("XML 속성 파싱 실패: 빈 속성 이름. tag="));
        }
        while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
            i = i.saturating_add(1);
        }
        let Some(&equals) = bytes.get(i) else {
            return Err(parse_error("XML 속성 파싱 실패: '=' 없음. tag="));
        };
        if equals != b'=' {
            return Err(parse_error("XML 속성 파싱 실패: '='가 필요합니다. tag="));
        }
        i = i.saturating_add(1);
        while bytes.get(i).is_some_and(u8::is_ascii_whitespace) {
            i = i.saturating_add(1);
        }
        let Some(&quote) = bytes.get(i) else {
            return Err(parse_error("XML 속성 파싱 실패: 값 quote가 없습니다. tag="));
        };
        if !matches!(quote, b'"' | b'\'') {
            return Err(parse_error("XML 속성 파싱 실패: 속성 값 quote 필요. tag="));
        }
        i = i.saturating_add(1);
        let value_start = i;
        while bytes.get(i).is_some_and(|ch| *ch != quote) {
            i = i.saturating_add(1);
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
        let value = decode_xml_entities(raw_value).into_owned();
        reserve_xml_attrs(
            &mut out,
            1,
            "XML 속성 목록 추가 메모리 확보 실패",
            XmlReserveMode::Additional,
        )?;
        out.push(XmlAttr {
            name: key.to_owned(),
            value,
        });
        if i < bytes.len() {
            i = i.saturating_add(1);
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
fn owned_attr(name: &str, value: impl Into<String>) -> XmlAttr {
    XmlAttr {
        name: name.to_owned(),
        value: value.into(),
    }
}
fn set_attr(attrs: &mut Vec<XmlAttr>, name: &str, value_in: impl Into<String>) {
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
    let formula_text = extract_first_tag_text(inner_xml, "f")
        .map(|text| decode_xml_entities(text).into_owned())
        .filter(|text| !text.is_empty());
    Ok(Some(SharedFormulaSpec { formula_text, si }))
}
fn replace_formula_tag_with_plain_formula(inner_xml: &str, formula: &str) -> Result<String> {
    let Some(f_start) = find_start_tag(inner_xml, "f", 0) else {
        return Err(err("cell formula 태그를 찾지 못했습니다."));
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
        let suffix_start =
            checked_usize_add(close_start, "</f>".len(), "cell formula suffix 시작")?;
        inner_xml
            .get(suffix_start..)
            .ok_or_else(|| err("cell formula suffix 범위가 손상되었습니다."))?
    };
    let escaped_formula = xml_escape_text(formula);
    let capacity = prefix
        .len()
        .saturating_add("<f></f>".len())
        .saturating_add(escaped_formula.len())
        .saturating_add(suffix.len());
    let mut out = String::new();
    out.try_reserve(capacity).map_err(|source| {
        err_with_source("cell formula plain replacement 메모리 확보 실패", source)
    })?;
    out.push_str(prefix);
    out.push_str("<f>");
    out.push_str(&escaped_formula);
    out.push_str("</f>");
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
                checked_usize_add(
                    close_start,
                    tag_name.len().saturating_add("</>".len()),
                    "XML 종료 태그 끝",
                )?
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
fn replace_first_tag_text(xml: &mut String, tag_name: &str, new_text: &str) -> Result<bool> {
    let Some(open_start) = find_start_tag(xml, tag_name, 0) else {
        return Ok(false);
    };
    let open_end = find_tag_end(xml, open_start)
        .ok_or_else(|| err(tag_error_message(tag_name, " 시작 태그가 손상되었습니다.")))?;
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
fn xml_escape_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    append_xml_escaped(&mut out, text, XmlEscapeContext::Text);
    out
}
fn needs_xml_space_preserve(text: &str) -> bool {
    text.starts_with(' ') || text.ends_with(' ') || text.contains("  ")
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
fn build_formula_with_empty_value(formula_text: &str) -> String {
    let capacity = "<f></f><v></v>".len().saturating_add(formula_text.len());
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("<f>{formula_text}</f><v></v>");
    }
    out.push_str("<f>");
    out.push_str(formula_text);
    out.push_str("</f><v></v>");
    out
}
fn build_open_tag(name: &str, attrs: &[XmlAttr]) -> String {
    let attrs_xml = attrs_to_xml(attrs);
    let capacity = "<>"
        .len()
        .saturating_add(name.len())
        .saturating_add(attrs_xml.len());
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("<{name}{attrs_xml}>");
    }
    out.push('<');
    out.push_str(name);
    out.push_str(&attrs_xml);
    out.push('>');
    out
}
fn build_self_closing_tag(name: &str, attrs: &[XmlAttr]) -> String {
    let attrs_xml = attrs_to_xml(attrs);
    let capacity = "</>"
        .len()
        .saturating_add(name.len())
        .saturating_add(attrs_xml.len());
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("<{name}{attrs_xml}/>");
    }
    out.push('<');
    out.push_str(name);
    out.push_str(&attrs_xml);
    out.push_str("/>");
    out
}
fn build_display_text_tag(name: &str, value: impl Display) -> String {
    let value_text = value.to_string();
    let capacity = "<></>"
        .len()
        .saturating_add(name.len().saturating_mul(2))
        .saturating_add(value_text.len());
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("<{name}>{value_text}</{name}>");
    }
    out.push('<');
    out.push_str(name);
    out.push('>');
    out.push_str(&value_text);
    out.push_str("</");
    out.push_str(name);
    out.push('>');
    out
}
fn build_ref_range(start_col_text: &str, rows: RangeInclusive<u32>, end_col: u32) -> String {
    let end_ref = ref_with_locks(CellReference::unlocked(end_col, rows.last));
    let start_row_text = rows.start.to_string();
    let capacity = start_col_text
        .len()
        .saturating_add(start_row_text.len())
        .saturating_add(":".len())
        .saturating_add(end_ref.len());
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("{start_col_text}{start_row_text}:{end_ref}");
    }
    out.push_str(start_col_text);
    out.push_str(&start_row_text);
    out.push(':');
    out.push_str(&end_ref);
    out
}
fn display_string(value: impl Display) -> String {
    value.to_string()
}
fn row_only_error(prefix: &str, row_num: u32) -> String {
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
fn row_col_error(prefix: &str, row_num: u32, col: u32) -> String {
    let row_text = row_num.to_string();
    let col_text = col.to_string();
    let capacity = prefix
        .len()
        .saturating_add(row_text.len())
        .saturating_add(", col=".len())
        .saturating_add(col_text.len())
        .saturating_add(1);
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("{prefix}{row_text}, col={col_text})");
    }
    out.push_str(prefix);
    out.push_str(&row_text);
    out.push_str(", col=");
    out.push_str(&col_text);
    out.push(')');
    out
}
fn tag_error_message(tag_name: &str, suffix: &str) -> String {
    let capacity = tag_name.len().saturating_add(suffix.len());
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("{tag_name}{suffix}");
    }
    out.push_str(tag_name);
    out.push_str(suffix);
    out
}
