use super::{
    ooxml::{load_shared_strings, load_sheet_catalog, load_sheet_xml},
    xlsx_container::XlsxContainer,
    xml::{
        decode_xml_entities, extract_all_tag_text, extract_first_tag_text, find_end_tag,
        find_start_tag, find_tag_end,
    },
};
use crate::{Result, err, err_with_source, parse_i32_str, push_display};
use alloc::collections::BTreeMap;
use core::fmt::Display;
use std::collections::{HashMap, HashSet, hash_map::Entry};
use std::path::Path;
const MAX_A1_COL: u32 = 0x4000;
const MAX_A1_ROW: u32 = 0x0010_0000;
const COL_NAME_CHARS: [char; 26] = [
    'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M', 'N', 'O', 'P', 'Q', 'R', 'S',
    'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
];
#[derive(Debug)]
pub struct Workbook {
    container: XlsxContainer,
    shared_strings: Vec<String>,
    shared_strings_xml_text: Option<String>,
    sheet_paths: BTreeMap<String, String>,
    sheets: BTreeMap<String, Worksheet>,
    xml_text: String,
}
#[derive(Debug, Clone, Default)]
pub struct Worksheet {
    pub prefix: String,
    pub rows: BTreeMap<u32, Row>,
    pub suffix: String,
}
#[derive(Debug, Clone, Default)]
pub struct Row {
    pub attrs: Vec<(String, String)>,
    pub cells: BTreeMap<u32, Cell>,
}
#[derive(Debug, Clone, Default)]
pub struct Cell {
    pub attrs: Vec<(String, String)>,
    pub inner_xml: Option<String>,
}
#[derive(Debug, Clone)]
struct SharedFormulaHead {
    anchor_col: u32,
    anchor_row: u32,
    formula: String,
}
#[derive(Debug, Clone)]
struct SharedFormulaSpec {
    formula_text: Option<String>,
    si: String,
}
trait WorkbookSharedStringsExt {
    fn update_shared_strings_xml_text(
        &mut self,
        existing_total: usize,
        existing_unique: usize,
    ) -> Result<()>;
}
trait WorksheetXmlParseExt {
    fn parse(xml: &str) -> Result<Worksheet>;
    fn parse_row_cells(row_body: &str, row_num: u32, row: &mut Row) -> Result<()>;
    fn parse_rows_from_sheet_data(body: &str) -> Result<BTreeMap<u32, Row>>;
}
impl Workbook {
    pub fn open(path: &Path) -> Result<Self> {
        let container = XlsxContainer::open_for_update(path)?;
        let catalog = load_sheet_catalog(&container)?;
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
        let shared_strings = load_shared_strings(&container)?;
        let mut sheet_paths = BTreeMap::default();
        let mut sheets = BTreeMap::default();
        for sheet_name in &catalog.sheet_order {
            let Some(sheet_path) = catalog.sheet_name_to_path.get(sheet_name) else {
                continue;
            };
            let xml = load_sheet_xml(&container, &catalog, sheet_name)?;
            let mut sheet = <Worksheet as WorksheetXmlParseExt>::parse(&xml)?;
            sheet.normalize_shared_formulas()?;
            sheet_paths.insert(sheet_name.clone(), sheet_path.clone());
            sheets.insert(sheet_name.clone(), sheet);
        }
        Ok(Self {
            container,
            shared_strings,
            shared_strings_xml_text,
            sheet_paths,
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
                    let mut message = String::with_capacity(64);
                    message.push_str("shared string 중복 집합 메모리 확보 실패: ");
                    push_display(&mut message, self.shared_strings.len());
                    message.push_str(" entries");
                    err_with_source(message, source)
                })?;
            for value in &self.shared_strings {
                seen.insert(value);
            }
            seen.len()
        };
        let mut index_map: HashMap<String, usize> = HashMap::new();
        index_map
            .try_reserve(self.shared_strings.len())
            .map_err(|source| {
                let mut message = String::with_capacity(64);
                message.push_str("shared string index map 메모리 확보 실패: ");
                push_display(&mut message, self.shared_strings.len());
                message.push_str(" entries");
                err_with_source(message, source)
            })?;
        for (idx, value) in self.shared_strings.iter().enumerate() {
            index_map.entry(value.clone()).or_insert(idx);
        }
        for sheet in self.sheets.values_mut() {
            for row in sheet.rows.values_mut() {
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
                                .map(|value| decode_xml_entities(&value))
                        }
                    }) else {
                        continue;
                    };
                    let shared_idx = match index_map.entry(text) {
                        Entry::Occupied(entry) => *entry.get(),
                        Entry::Vacant(entry) => {
                            let idx = self.shared_strings.len();
                            let value = entry.key().clone();
                            self.shared_strings.push(value);
                            entry.insert(idx);
                            idx
                        }
                    };
                    set_attr(&mut cell.attrs, "t", "s");
                    cell.inner_xml = Some(build_display_text_tag("v", shared_idx));
                }
            }
        }
        if self.shared_strings.len() == existing_total {
            return Ok(());
        }
        <Self as WorkbookSharedStringsExt>::update_shared_strings_xml_text(
            self,
            existing_total,
            existing_unique,
        )
    }
    fn remove_excel_recovery_artifacts(&mut self) -> Result<()> {
        self.xml_text = remove_tags_matching(&self.xml_text, "fileRecoveryPr", |_| true)?;
        let workbook_rels_path = "xl/_rels/workbook.xml.rels";
        let workbook_rels_xml = self.container.read_text(workbook_rels_path)?;
        let updated_workbook_rels = remove_tags_matching_attr(
            &workbook_rels_xml,
            "Relationship",
            "Type",
            "http://schemas.openxmlformats.org/officeDocument/2006/relationships/calcChain",
        )?;
        if updated_workbook_rels != workbook_rels_xml {
            self.container
                .write_text(workbook_rels_path, &updated_workbook_rels)?;
        }
        let content_types_path = "[Content_Types].xml";
        let content_types_xml = self.container.read_text(content_types_path)?;
        let updated_content_types = remove_tags_matching_attr(
            &content_types_xml,
            "Override",
            "PartName",
            "/xl/calcChain.xml",
        )?;
        if updated_content_types != content_types_xml {
            self.container
                .write_text(content_types_path, &updated_content_types)?;
        }
        self.container.remove_file_if_exists("xl/calcChain.xml")?;
        Ok(())
    }
    fn request_full_recalculation(&mut self) -> Result<()> {
        self.xml_text = {
            let set_calc_pr_attrs = |attrs: &mut Vec<(String, String)>| {
                set_attr(attrs, "calcMode", "auto");
                set_attr(attrs, "fullCalcOnLoad", "1");
                set_attr(attrs, "forceFullCalc", "1");
                set_attr(attrs, "calcCompleted", "0");
            };
            let mut out = self.xml_text.clone();
            if let Some(calc_pr_start) = find_start_tag(&out, "calcPr", 0) {
                let Some(calc_pr_tag_end) = find_tag_end(&out, calc_pr_start) else {
                    return Err(err("workbook.xml의 calcPr 태그가 손상되었습니다."));
                };
                let calc_pr_tag = out
                    .get(calc_pr_start..=calc_pr_tag_end)
                    .ok_or_else(|| err("workbook.xml의 calcPr 태그 범위가 손상되었습니다."))?;
                let mut attrs = parse_tag_attrs(calc_pr_tag)?;
                set_calc_pr_attrs(&mut attrs);
                if calc_pr_tag.ends_with("/>") {
                    let new_tag = build_self_closing_tag("calcPr", &attrs);
                    out.replace_range(calc_pr_start..=calc_pr_tag_end, &new_tag);
                } else {
                    let close_search_from =
                        checked_usize_add(calc_pr_tag_end, 1, "calcPr 종료 태그 검색 시작")?;
                    let Some(calc_pr_close_start) = find_end_tag(&out, "calcPr", close_search_from)
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
                    out.replace_range(calc_pr_start..calc_pr_close_end, &new_tag);
                }
            } else {
                let Some(workbook_close_start) = find_end_tag(&out, "workbook", 0) else {
                    return Err(err("workbook.xml의 workbook 종료 태그를 찾지 못했습니다."));
                };
                let mut attrs = Vec::with_capacity(4);
                set_calc_pr_attrs(&mut attrs);
                let new_tag = build_self_closing_tag("calcPr", &attrs);
                out.insert_str(workbook_close_start, &new_tag);
            }
            out
        };
        Ok(())
    }
    pub fn save_as(&mut self, out_path: &Path, verify_saved_file: bool) -> Result<()> {
        self.promote_safe_inline_strings_to_shared()?;
        self.request_full_recalculation()?;
        self.remove_excel_recovery_artifacts()?;
        self.container
            .write_text("xl/workbook.xml", &self.xml_text)?;
        if let Some(shared_strings_xml) = self.shared_strings_xml_text.as_ref() {
            self.container
                .write_text("xl/sharedStrings.xml", shared_strings_xml)?;
        }
        for (sheet_name, sheet) in &self.sheets {
            let Some(path) = self.sheet_paths.get(sheet_name) else {
                continue;
            };
            self.container.write_text(path, &sheet.to_xml())?;
        }
        self.container.save_as(out_path, verify_saved_file)
    }
    pub fn with_sheet_mut<R, F>(&mut self, name: &str, mutator: F) -> Option<R>
    where
        F: FnOnce(&mut Worksheet, &[String]) -> R,
    {
        let (shared_strings, sheets) = (&self.shared_strings, &mut self.sheets);
        let ws = sheets.get_mut(name)?;
        Some(mutator(ws, shared_strings))
    }
    pub const fn workbook_xml_mut(&mut self) -> &mut String {
        &mut self.xml_text
    }
}
impl WorkbookSharedStringsExt for Workbook {
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
        let open_tag = original_xml
            .get(open_start..=open_end)
            .ok_or_else(|| err("sharedStrings XML의 <sst> 태그 범위가 손상되었습니다."))?;
        let mut attrs = parse_tag_attrs(open_tag)?;
        let new_total = existing_total.saturating_add(new_values.len());
        let new_unique = existing_unique.saturating_add(new_values.len());
        set_attr(&mut attrs, "count", display_string(new_total));
        set_attr(&mut attrs, "uniqueCount", display_string(new_unique));
        let mut new_values_len = 0_usize;
        for value in new_values {
            new_values_len = new_values_len.saturating_add(value.len());
        }
        let capacity = new_values_len
            .saturating_add(new_values.len().saturating_mul("<si><t></t></si>".len()))
            .saturating_add(
                new_values
                    .len()
                    .saturating_mul(" xml:space=\"preserve\"".len()),
            );
        let mut new_si_xml = String::with_capacity(capacity);
        for value in new_values {
            let text = xml_escape_text(value);
            if needs_xml_space_preserve(value) {
                new_si_xml.push_str("<si><t xml:space=\"preserve\">");
            } else {
                new_si_xml.push_str("<si><t>");
            }
            new_si_xml.push_str(&text);
            new_si_xml.push_str("</t></si>");
        }
        let updated_xml = if open_tag.trim_end().ends_with("/>") {
            let mut replacement = build_open_tag("sst", &attrs);
            replacement.push_str(&new_si_xml);
            replacement.push_str("</sst>");
            let mut out = original_xml;
            out.replace_range(open_start..=open_end, &replacement);
            out
        } else {
            let new_open_tag = build_open_tag("sst", &attrs);
            let mut out = original_xml;
            out.replace_range(open_start..=open_end, &new_open_tag);
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
}
impl Worksheet {
    pub fn clear_cell_if_exists(&mut self, col: u32, row: u32) {
        let Some(row_obj) = self.rows.get_mut(&row) else {
            return;
        };
        let Some(cell) = row_obj.cells.get_mut(&col) else {
            return;
        };
        remove_attr(&mut cell.attrs, "t");
        cell.inner_xml = None;
    }
    pub fn clear_formula_cached_values_in_range(&mut self, start_row: u32, end_row: u32) {
        for (_, row) in self.rows.range_mut(start_row..=end_row) {
            for cell in row.cells.values_mut() {
                let Some(inner) = cell.inner_xml.as_mut() else {
                    continue;
                };
                if find_start_tag(inner, "f", 0).is_none() {
                    continue;
                }
                if !replace_first_tag_text(inner, "v", "") && !inner.contains("<v>") {
                    inner.push_str("<v></v>");
                }
            }
        }
    }
    pub fn clone_row_style(&mut self, source_row: u32, target_row: u32, max_col: u32) {
        let Some(src) = self.rows.get(&source_row).cloned() else {
            return;
        };
        let mut cloned = src;
        remap_row_numbers(&mut cloned, target_row, &|row_num| {
            if row_num == source_row {
                target_row
            } else {
                row_num
            }
        });
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
    }
    pub fn extend_conditional_formats(
        &mut self,
        last_data_row: u32,
        target_cols: &[u32],
        data_start_row: u32,
    ) -> Result<()> {
        if target_cols.is_empty() {
            return Ok(());
        }
        self.suffix = {
            let mut out = self.suffix.clone();
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
                let tag = out
                    .get(cf_start..cf_end)
                    .ok_or_else(|| err("conditionalFormatting 태그 범위가 손상되었습니다."))?;
                let mut attrs = parse_tag_attrs(tag)?;
                if let Some(sqref) = get_attr(&attrs, "sqref").map(ToOwned::to_owned) {
                    let updated_sqref = {
                        let mut changed = false;
                        let range_count = sqref.split_whitespace().count();
                        let mut ranges_out: Vec<String> = Vec::new();
                        ranges_out
                            .try_reserve_exact(range_count)
                            .map_err(|source| {
                                let mut message = String::with_capacity(64);
                                message.push_str(
                                    "conditionalFormatting range 목록 메모리 확보 실패: ",
                                );
                                push_display(&mut message, range_count);
                                message.push_str(" ranges");
                                err_with_source(message, source)
                            })?;
                        for token in sqref.split_whitespace() {
                            let (start_ref, end_ref) = parse_range_token(token);
                            let Some((start_col, start_row, start_col_lock, start_row_lock)) =
                                parse_ref_with_locks(start_ref)
                            else {
                                ranges_out.push(token.to_owned());
                                continue;
                            };
                            let Some((end_col, end_row, end_col_lock, end_row_lock)) =
                                parse_ref_with_locks(end_ref)
                            else {
                                ranges_out.push(token.to_owned());
                                continue;
                            };
                            if start_row != data_start_row || end_row != data_start_row {
                                ranges_out.push(token.to_owned());
                                continue;
                            }
                            if !target_cols
                                .iter()
                                .any(|col| (start_col..=end_col).contains(col))
                            {
                                ranges_out.push(token.to_owned());
                                continue;
                            }
                            let new_start = ref_with_locks(
                                start_col,
                                data_start_row,
                                start_col_lock,
                                start_row_lock,
                            );
                            let new_end =
                                ref_with_locks(end_col, last_data_row, end_col_lock, end_row_lock);
                            let capacity = new_start
                                .len()
                                .saturating_add(new_end.len())
                                .saturating_add(1);
                            let mut range = String::with_capacity(capacity);
                            range.push_str(&new_start);
                            range.push(':');
                            range.push_str(&new_end);
                            ranges_out.push(range);
                            changed = true;
                        }
                        if changed { ranges_out.join(" ") } else { sqref }
                    };
                    set_attr(&mut attrs, "sqref", updated_sqref);
                    let new_tag = build_open_tag("conditionalFormatting", &attrs);
                    out.replace_range(cf_start..cf_end, &new_tag);
                    cursor = checked_usize_add(
                        cf_start,
                        new_tag.len(),
                        "conditionalFormatting 다음 cursor",
                    )?;
                } else {
                    cursor = cf_end;
                }
            }
            out
        };
        Ok(())
    }
    pub fn get_display_at(&self, col: u32, row: u32, shared_strings: &[String]) -> String {
        let Some(row_obj) = self.rows.get(&row) else {
            return String::new();
        };
        let Some(cell) = row_obj.cells.get(&col) else {
            return String::new();
        };
        let cell_type = get_attr(&cell.attrs, "t");
        let inner = cell.inner_xml.as_deref().unwrap_or_default();
        let display = if matches!(cell_type, Some("inlineStr")) {
            extract_all_tag_text(inner, "t").map(|value| decode_xml_entities(&value))
        } else {
            let raw_v = extract_first_tag_text(inner, "v").unwrap_or_default();
            let decoded = decode_xml_entities(&raw_v);
            if matches!(cell_type, Some("s")) {
                let Some(idx) = decoded.parse::<usize>().ok() else {
                    return String::new();
                };
                shared_strings.get(idx).cloned()
            } else if matches!(cell_type, Some("b")) {
                Some(if decoded == "1" {
                    "TRUE".into()
                } else {
                    "FALSE".into()
                })
            } else {
                Some(decoded)
            }
        };
        display.unwrap_or_default()
    }
    pub fn get_formula_at(&self, col: u32, row: u32) -> Option<String> {
        self.rows
            .get(&row)?
            .cells
            .get(&col)?
            .inner_xml
            .as_deref()
            .and_then(|inner| extract_first_tag_text(inner, "f"))
            .map(|formula| decode_xml_entities(&formula))
    }
    pub fn get_i32_at(&self, col: u32, row: u32, shared_strings: &[String]) -> Option<i32> {
        let text = self.get_display_at(col, row, shared_strings);
        parse_i32_str(&text)
    }
    fn get_or_create_cell_mut(&mut self, col: u32, row: u32) -> &mut Cell {
        let row_obj = self.rows.entry(row).or_insert_with(|| Row {
            attrs: vec![owned_attr("r", display_string(row))],
            cells: BTreeMap::default(),
        });
        if get_attr(&row_obj.attrs, "r").is_none() {
            set_attr(&mut row_obj.attrs, "r", display_string(row));
        }
        row_obj.cells.entry(col).or_insert_with(|| Cell {
            attrs: vec![
                owned_attr("r", ref_with_locks(col, row, false, false)),
                owned_attr("s", "0"),
            ],
            inner_xml: None,
        })
    }
    pub fn has_any_row_format(&self, row: u32, max_col: u32) -> bool {
        let Some(row_obj) = self.rows.get(&row) else {
            return false;
        };
        if !row_obj.attrs.is_empty() {
            return true;
        }
        if max_col == 0 {
            return false;
        }
        row_obj.cells.range(1..=max_col).next().is_some()
    }
    pub fn max_cell_col(&self) -> u32 {
        self.rows
            .values()
            .filter_map(|row| row.cells.last_key_value().map(|(&col, _)| col))
            .max()
            .unwrap_or(1)
    }
    pub fn max_row_num(&self) -> u32 {
        self.rows.last_key_value().map_or(1, |(&row, _)| row)
    }
    fn normalize_shared_formulas(&mut self) -> Result<()> {
        let mut heads: HashMap<String, SharedFormulaHead> = HashMap::new();
        heads.try_reserve(self.rows.len()).map_err(|source| {
            let mut message = String::with_capacity(64);
            message.push_str("shared formula head 맵 메모리 확보 실패: ");
            push_display(&mut message, self.rows.len());
            message.push_str(" entries");
            err_with_source(message, source)
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
                    let capacity = 64;
                    let mut message = String::with_capacity(capacity);
                    message.push_str("shared formula head를 찾지 못했습니다. (si=");
                    message.push_str(&spec.si);
                    message.push_str(", cell=");
                    message.push_str(col_to_name(*col_num).as_str());
                    push_display(&mut message, *row_num);
                    message.push_str(" )");
                    err(message)
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
    pub fn row_has_any_data(&self, row: u32, cols: &[u32], shared_strings: &[String]) -> bool {
        cols.iter().any(|col| {
            !self
                .get_display_at(*col, row, shared_strings)
                .trim()
                .is_empty()
        })
    }
    pub fn set_formula_at(&mut self, col: u32, row: u32, formula: &str) {
        let cell = self.get_or_create_cell_mut(col, row);
        let formula_text = xml_escape_text(formula);
        if let Some(inner) = cell.inner_xml.as_mut()
            && replace_first_tag_text(inner, "f", &formula_text)
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
    }
    pub fn set_formula_cached_value_at(
        &mut self,
        col: u32,
        row: u32,
        value: Option<&str>,
        cell_type: Option<&str>,
    ) {
        let cell = self.get_or_create_cell_mut(col, row);
        match cell_type {
            Some(value_type) => set_attr(&mut cell.attrs, "t", value_type),
            None => remove_attr(&mut cell.attrs, "t"),
        }
        let Some(inner) = cell.inner_xml.as_mut() else {
            return;
        };
        let encoded = value.map(xml_escape_text);
        let value_text = encoded.as_deref().unwrap_or_default();
        if !replace_first_tag_text(inner, "v", value_text) && !inner.contains("<v>") {
            inner.push_str("<v>");
            inner.push_str(value_text);
            inner.push_str("</v>");
        }
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
        let preserve = needs_xml_space_preserve(value);
        let capacity = text.len().saturating_add(if preserve {
            "<is><t xml:space=\"preserve\"></t></is>".len()
        } else {
            "<is><t></t></is>".len()
        });
        let mut inner = String::with_capacity(capacity);
        inner.push_str("<is><t");
        if preserve {
            inner.push_str(" xml:space=\"preserve\"");
        }
        inner.push('>');
        inner.push_str(&text);
        inner.push_str("</t></is>");
        cell.inner_xml = Some(inner);
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
                    .saturating_add(cell.inner_xml.as_ref().map_or(0, String::len));
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
    pub fn update_auto_filter_ref(&mut self, header_row: u32, last_data_row: u32) -> Result<()> {
        self.suffix = {
            let mut out = self.suffix.clone();
            let mut cursor = 0_usize;
            let target_last_row = header_row.max(last_data_row);
            while let Some(auto_filter_rel) =
                out.get(cursor..).and_then(|tail| tail.find("<autoFilter"))
            {
                let auto_filter_start =
                    checked_usize_add(cursor, auto_filter_rel, "autoFilter 시작")?;
                let Some(auto_filter_end_rel) =
                    out.get(auto_filter_start..).and_then(|tail| tail.find('>'))
                else {
                    return Err(err("worksheet XML의 autoFilter 태그가 손상되었습니다."));
                };
                let auto_filter_end = checked_usize_add(
                    checked_usize_add(
                        auto_filter_start,
                        auto_filter_end_rel,
                        "autoFilter 태그 끝",
                    )?,
                    1,
                    "autoFilter 태그 끝",
                )?;
                let tag = out
                    .get(auto_filter_start..auto_filter_end)
                    .ok_or_else(|| err("worksheet XML의 autoFilter 태그 범위가 손상되었습니다."))?;
                let mut attrs = parse_tag_attrs(tag)?;
                let mut end_col = 1_u32;
                if let Some(existing_ref) = get_attr(&attrs, "ref") {
                    let (_, end_ref) = parse_range_token(existing_ref);
                    if let Some((parsed_end_col, _, _, _)) = parse_ref_with_locks(end_ref) {
                        end_col = parsed_end_col;
                    }
                }
                let new_ref = build_ref_range("A", header_row, end_col, target_last_row);
                set_attr(&mut attrs, "ref", new_ref);
                let new_tag = if tag.trim_end().ends_with("/>") {
                    build_self_closing_tag("autoFilter", &attrs)
                } else {
                    build_open_tag("autoFilter", &attrs)
                };
                out.replace_range(auto_filter_start..auto_filter_end, &new_tag);
                cursor =
                    checked_usize_add(auto_filter_start, new_tag.len(), "autoFilter 다음 cursor")?;
            }
            out
        };
        Ok(())
    }
    pub fn update_dimension(&mut self) -> Result<()> {
        let max_row = self.max_row_num();
        let max_col = self.max_cell_col();
        self.prefix = {
            let mut out = self.prefix.clone();
            if let Some(dim_pos) = out.find("<dimension")
                && let Some(dim_end_rel) = out.get(dim_pos..).and_then(|tail| tail.find('>'))
            {
                let dim_end = checked_usize_add(
                    checked_usize_add(dim_pos, dim_end_rel, "dimension 태그 끝")?,
                    1,
                    "dimension 태그 끝",
                )?;
                let tag = out
                    .get(dim_pos..dim_end)
                    .ok_or_else(|| err("dimension 태그 범위가 손상되었습니다."))?;
                let mut attrs = parse_tag_attrs(tag)?;
                set_attr(&mut attrs, "ref", build_ref_range("A", 1, max_col, max_row));
                let new_tag = build_self_closing_tag("dimension", &attrs);
                out.replace_range(dim_pos..dim_end, &new_tag);
            }
            out
        };
        Ok(())
    }
}
impl WorksheetXmlParseExt for Worksheet {
    fn parse(xml: &str) -> Result<Worksheet> {
        let Some(sheet_data_open) = find_start_tag(xml, "sheetData", 0) else {
            return Err(err("worksheet XML에 <sheetData>가 없습니다."));
        };
        let Some(sheet_data_open_end) = find_tag_end(xml, sheet_data_open) else {
            return Err(err(
                "worksheet XML의 <sheetData> 시작 태그가 손상되었습니다.",
            ));
        };
        let sheet_data_body_start =
            checked_usize_add(sheet_data_open_end, 1, "worksheet sheetData body 시작")?;
        let Some(sheet_data_close) = find_end_tag(xml, "sheetData", sheet_data_body_start) else {
            return Err(err("worksheet XML에 </sheetData>가 없습니다."));
        };
        let prefix = xml
            .get(..sheet_data_body_start)
            .ok_or_else(|| err("worksheet XML prefix 범위가 손상되었습니다."))?
            .to_owned();
        let body = xml
            .get(sheet_data_body_start..sheet_data_close)
            .ok_or_else(|| err("worksheet XML body 범위가 손상되었습니다."))?;
        let suffix = xml
            .get(sheet_data_close..)
            .ok_or_else(|| err("worksheet XML suffix 범위가 손상되었습니다."))?
            .to_owned();
        let rows = Self::parse_rows_from_sheet_data(body)?;
        Ok(Self {
            prefix,
            rows,
            suffix,
        })
    }
    fn parse_row_cells(row_body: &str, row_num: u32, row: &mut Row) -> Result<()> {
        let mut cursor = 0_usize;
        let mut next_col = 1_u32;
        while let Some(cell_open_rel) = row_body.get(cursor..).and_then(|tail| tail.find("<c")) {
            let cell_open = checked_usize_add(cursor, cell_open_rel, "row cell 시작")?;
            let Some(cell_tag_end_rel) = row_body.get(cell_open..).and_then(|tail| tail.find('>'))
            else {
                return Err(err(row_offset_error(
                    "row 내 cell 시작 태그가 손상되었습니다. (row=",
                    row_num,
                    cell_open,
                )));
            };
            let cell_tag_end = checked_usize_add(cell_open, cell_tag_end_rel, "row cell 태그 끝")?;
            let cell_tag = row_body.get(cell_open..=cell_tag_end).ok_or_else(|| {
                err(row_offset_error(
                    "row 내 cell 태그 범위가 손상되었습니다. (row=",
                    row_num,
                    cell_open,
                ))
            })?;
            let mut attrs = parse_tag_attrs(cell_tag)?;
            let col = get_attr(&attrs, "r")
                .and_then(|value| parse_ref_with_locks(value).map(|(col_num, _, _, _)| col_num))
                .unwrap_or(next_col);
            set_attr(&mut attrs, "r", ref_with_locks(col, row_num, false, false));
            if cell_tag.ends_with("/>") {
                row.cells.insert(
                    col,
                    Cell {
                        attrs,
                        inner_xml: None,
                    },
                );
                next_col = col.saturating_add(1);
                cursor = cell_tag_end.saturating_add(1);
                continue;
            }
            let cell_body_start = checked_usize_add(cell_tag_end, 1, "row cell 본문 시작")?;
            let Some(cell_close_rel) = row_body
                .get(cell_body_start..)
                .and_then(|tail| tail.find("</c>"))
            else {
                return Err(err(row_col_error(
                    "row 내 cell 종료 태그를 찾지 못했습니다. (row=",
                    row_num,
                    col,
                )));
            };
            let cell_body_end =
                checked_usize_add(cell_body_start, cell_close_rel, "row cell 본문 끝")?;
            let inner_xml = row_body
                .get(cell_body_start..cell_body_end)
                .ok_or_else(|| {
                    err(row_col_error(
                        "row 내 cell 본문 범위가 손상되었습니다. (row=",
                        row_num,
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
            cursor = checked_usize_add(cell_body_end, "</c>".len(), "row cell 다음 cursor")?;
        }
        Ok(())
    }
    fn parse_rows_from_sheet_data(body: &str) -> Result<BTreeMap<u32, Row>> {
        let mut rows = BTreeMap::default();
        let mut cursor = 0_usize;
        while let Some(row_open_rel) = body.get(cursor..).and_then(|tail| tail.find("<row")) {
            let row_open = checked_usize_add(cursor, row_open_rel, "sheetData row 시작")?;
            let Some(row_tag_end_rel) = body.get(row_open..).and_then(|tail| tail.find('>')) else {
                return Err(err(offset_only_error(
                    "sheetData row 시작 태그가 손상되었습니다. (offset=",
                    row_open,
                )));
            };
            let row_tag_end =
                checked_usize_add(row_open, row_tag_end_rel, "sheetData row 태그 끝")?;
            let row_tag = body.get(row_open..=row_tag_end).ok_or_else(|| {
                err(offset_only_error(
                    "sheetData row 태그 범위가 손상되었습니다. (offset=",
                    row_open,
                ))
            })?;
            let mut row_attrs = parse_tag_attrs(row_tag)?;
            let row_num = get_attr(&row_attrs, "r")
                .and_then(|value| value.parse::<u32>().ok())
                .unwrap_or_else(|| {
                    rows.last_key_value()
                        .map_or(0_u32, |(&last_row, _)| last_row)
                        .saturating_add(1)
                });
            set_attr(&mut row_attrs, "r", display_string(row_num));
            if row_tag.ends_with("/>") {
                rows.insert(
                    row_num,
                    Row {
                        attrs: row_attrs,
                        cells: BTreeMap::default(),
                    },
                );
                cursor = row_tag_end.saturating_add(1);
                continue;
            }
            let row_body_start = checked_usize_add(row_tag_end, 1, "sheetData row 본문 시작")?;
            let Some(row_close_rel) = body
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
            let row_body = body.get(row_body_start..row_body_end).ok_or_else(|| {
                err(row_only_error(
                    "sheetData row 본문 범위가 손상되었습니다. (row=",
                    row_num,
                ))
            })?;
            let mut row = Row {
                attrs: row_attrs,
                cells: BTreeMap::default(),
            };
            Self::parse_row_cells(row_body, row_num, &mut row)?;
            rows.insert(row_num, row);
            cursor = checked_usize_add(row_body_end, "</row>".len(), "sheetData row 다음 cursor")?;
        }
        Ok(rows)
    }
}
fn checked_usize_add(base: usize, add: usize, context: &str) -> Result<usize> {
    base.checked_add(add).ok_or_else(|| {
        let capacity = context.len().saturating_add(64);
        let mut out = String::with_capacity(capacity);
        out.push_str(context);
        out.push_str(" offset 계산 중 overflow가 발생했습니다. (base=");
        push_display(&mut out, base);
        out.push_str(", add=");
        push_display(&mut out, add);
        out.push(')');
        err(out)
    })
}
pub fn remap_row_numbers(row: &mut Row, new_row: u32, resolver: &dyn Fn(u32) -> u32) {
    set_attr(&mut row.attrs, "r", display_string(new_row));
    for (col, cell) in &mut row.cells {
        set_attr(
            &mut cell.attrs,
            "r",
            ref_with_locks(*col, new_row, false, false),
        );
        if let Some(inner) = cell.inner_xml.as_mut()
            && let Some(text) = extract_first_tag_text(inner, "f")
        {
            let rewritten =
                rewrite_formula_cell_refs(&decode_xml_entities(&text), |chars, start| {
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
                })
                .unwrap_or_else(|_| decode_xml_entities(&text));
            let encoded = xml_escape_text(&rewritten);
            replace_first_tag_text(inner, "f", &encoded);
        }
    }
}
pub fn col_to_name(mut col: u32) -> String {
    if col == 0 {
        return "A".into();
    }
    let mut rev = Vec::with_capacity(4);
    while col > 0 {
        let base = col.saturating_sub(1);
        let Ok(rem) = u8::try_from(base.rem_euclid(26)) else {
            return String::new();
        };
        let Some(letter) = COL_NAME_CHARS.get(usize::from(rem)).copied() else {
            return String::new();
        };
        rev.push(letter);
        col = base.div_euclid(26);
    }
    let capacity = rev.len();
    let mut out = String::with_capacity(capacity);
    for ch in rev.into_iter().rev() {
        out.push(ch);
    }
    out
}
pub fn name_to_col(name: &str) -> Option<u32> {
    let mut out = 0_u32;
    if name.is_empty() {
        return None;
    }
    for ch in name.chars() {
        if !ch.is_ascii_alphabetic() {
            return None;
        }
        let upper = u8::try_from(u32::from(ch.to_ascii_uppercase())).ok()?;
        out = out
            .checked_mul(26)?
            .checked_add(u32::from(upper.saturating_sub(b'A')).saturating_add(1))?;
    }
    Some(out)
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
fn push_sorted_attrs_xml(out: &mut String, attrs: &[(String, String)]) {
    let mut sorted_attrs: Vec<_> = attrs.iter().collect();
    sorted_attrs.sort_by(|left, right| attr_sort_key(&left.0).cmp(&attr_sort_key(&right.0)));
    for attr in sorted_attrs {
        out.push(' ');
        out.push_str(&attr.0);
        out.push_str("=\"");
        append_xml_escaped(out, &attr.1, true);
        out.push('"');
    }
}
fn attrs_to_xml(attrs: &[(String, String)]) -> String {
    let mut estimated_len = 0_usize;
    for attr in attrs {
        estimated_len = estimated_len
            .saturating_add(attr.0.len().saturating_add(attr.1.len()).saturating_add(4));
    }
    let mut out = String::with_capacity(estimated_len);
    for attr in attrs {
        out.push(' ');
        out.push_str(&attr.0);
        out.push_str("=\"");
        append_xml_escaped(&mut out, &attr.1, true);
        out.push('"');
    }
    out
}
fn parse_tag_attrs(tag: &str) -> Result<Vec<(String, String)>> {
    let mut out: Vec<(String, String)> = Vec::new();
    reserve_xml_attrs(&mut out, 4, "XML 속성 목록 메모리 확보 실패", true)?;
    let parse_error = |prefix: &str| {
        let capacity = prefix.len().saturating_add(tag.len());
        let mut message = String::with_capacity(capacity);
        message.push_str(prefix);
        message.push_str(tag);
        err(message)
    };
    let Some(lt) = tag.find('<') else {
        return Err(parse_error(
            "XML 태그 파싱 실패: '<'를 찾지 못했습니다. tag=",
        ));
    };
    let mut i = lt.saturating_add(1);
    let bytes = tag.as_bytes();
    while matches!(bytes.get(i), Some(ch) if !ch.is_ascii_whitespace() && *ch != b'>' && *ch != b'/')
    {
        i = i.saturating_add(1);
    }
    if i >= bytes.len() {
        return Err(parse_error(
            "XML 태그 파싱 실패: 태그 종료 기호를 찾지 못했습니다. tag=",
        ));
    }
    while i < bytes.len() {
        while matches!(bytes.get(i), Some(ch) if ch.is_ascii_whitespace()) {
            i = i.saturating_add(1);
        }
        if matches!(bytes.get(i), None | Some(b'>' | b'/')) {
            break;
        }
        let key_start = i;
        while matches!(bytes.get(i), Some(ch) if !ch.is_ascii_whitespace() && *ch != b'=' && *ch != b'>' && *ch != b'/')
        {
            i = i.saturating_add(1);
        }
        let key_end = i;
        if key_start == key_end {
            return Err(parse_error(
                "XML 속성 파싱 실패: 속성 이름이 비어 있습니다. tag=",
            ));
        }
        while matches!(bytes.get(i), Some(ch) if ch.is_ascii_whitespace()) {
            i = i.saturating_add(1);
        }
        if bytes.get(i).is_none() {
            return Err(parse_error(
                "XML 속성 파싱 실패: '='를 찾지 못했습니다. tag=",
            ));
        }
        if bytes.get(i) != Some(&b'=') {
            return Err(parse_error("XML 속성 파싱 실패: '='가 필요합니다. tag="));
        }
        i = i.saturating_add(1);
        while matches!(bytes.get(i), Some(ch) if ch.is_ascii_whitespace()) {
            i = i.saturating_add(1);
        }
        if bytes.get(i).is_none() {
            return Err(parse_error("XML 속성 파싱 실패: 값 quote가 없습니다. tag="));
        }
        if !matches!(bytes.get(i), Some(b'"' | b'\'')) {
            return Err(parse_error(
                "XML 속성 파싱 실패: 속성 값은 quote로 감싸야 합니다. tag=",
            ));
        }
        let Some(&quote) = bytes.get(i) else {
            return Err(parse_error("XML 속성 파싱 실패: 값 quote가 없습니다. tag="));
        };
        i = i.saturating_add(1);
        let value_start = i;
        while matches!(bytes.get(i), Some(ch) if *ch != quote) {
            i = i.saturating_add(1);
        }
        if i >= bytes.len() {
            return Err(parse_error(
                "XML 속성 파싱 실패: 닫히지 않은 quote가 있습니다. tag=",
            ));
        }
        let key = tag
            .get(key_start..key_end)
            .ok_or_else(|| parse_error("XML 속성 파싱 실패: 키 범위를 계산할 수 없습니다. tag="))?;
        let raw_value = tag
            .get(value_start..i)
            .ok_or_else(|| parse_error("XML 속성 파싱 실패: 값 범위를 계산할 수 없습니다. tag="))?;
        let value = decode_xml_entities(raw_value);
        reserve_xml_attrs(&mut out, 1, "XML 속성 목록 추가 메모리 확보 실패", false)?;
        out.push((key.to_owned(), value));
        if i < bytes.len() {
            i = i.saturating_add(1);
        }
    }
    Ok(out)
}
fn reserve_xml_attrs(
    attrs: &mut Vec<(String, String)>,
    additional: usize,
    context: &str,
    exact: bool,
) -> Result<()> {
    let reserve_result = if exact {
        attrs.try_reserve_exact(additional)
    } else {
        attrs.try_reserve(additional)
    };
    reserve_result.map_err(|source| {
        let mut message = String::with_capacity(64);
        message.push_str(context);
        message.push_str(": ");
        push_display(&mut message, additional);
        message.push_str(" entries");
        err_with_source(message, source)
    })
}
fn get_attr<'attrs>(attrs: &'attrs [(String, String)], name: &str) -> Option<&'attrs str> {
    attrs
        .iter()
        .find_map(|attr| (attr.0 == name).then_some(attr.1.as_str()))
}
fn owned_attr(name: &str, value: impl Into<String>) -> (String, String) {
    (name.to_owned(), value.into())
}
fn set_attr(attrs: &mut Vec<(String, String)>, name: &str, value_in: impl Into<String>) {
    let value = value_in.into();
    for attr in attrs.iter_mut() {
        if attr.0 == name {
            attr.1 = value;
            return;
        }
    }
    attrs.push(owned_attr(name, value));
}
fn remove_attr(attrs: &mut Vec<(String, String)>, name: &str) {
    attrs.retain(|attr| attr.0 != name);
}
fn parse_shared_formula_spec(inner_xml: &str) -> Result<Option<SharedFormulaSpec>> {
    let Some(f_start) = find_start_tag(inner_xml, "f", 0) else {
        return Ok(None);
    };
    let Some(f_end) = find_tag_end(inner_xml, f_start) else {
        return Err(err("cell formula 태그가 손상되었습니다."));
    };
    let open_tag = inner_xml
        .get(f_start..=f_end)
        .ok_or_else(|| err("cell formula 시작 태그 범위가 손상되었습니다."))?;
    let attrs = parse_tag_attrs(open_tag)?;
    if get_attr(&attrs, "t") != Some("shared") {
        return Ok(None);
    }
    let si = get_attr(&attrs, "si")
        .ok_or_else(|| err("shared formula에 si 속성이 없습니다."))?
        .to_owned();
    let formula_text = extract_first_tag_text(inner_xml, "f")
        .map(|text| decode_xml_entities(&text))
        .filter(|text| !text.is_empty());
    Ok(Some(SharedFormulaSpec { formula_text, si }))
}
fn rewrite_formula_cell_refs<F>(formula: &str, mut try_rewrite_cell_ref: F) -> Result<String>
where
    F: FnMut(&[char], usize) -> Result<Option<(usize, String)>>,
{
    let mut chars: Vec<char> = Vec::new();
    chars.try_reserve_exact(formula.len()).map_err(|source| {
        let mut message = String::with_capacity(64);
        message.push_str("formula 문자 목록 메모리 확보 실패: ");
        push_display(&mut message, formula.len());
        message.push_str(" chars");
        err_with_source(message, source)
    })?;
    for ch in formula.chars() {
        chars.push(ch);
    }
    let mut i = 0_usize;
    let capacity = formula.len();
    let mut out = String::with_capacity(capacity);
    let mut in_string = false;
    while let Some(&ch) = chars.get(i) {
        if ch == '"' {
            out.push(ch);
            if in_string {
                let escaped_quote_idx = i
                    .checked_add(1)
                    .ok_or_else(|| err("formula 문자열 quote index 계산에 실패했습니다."))?;
                if chars.get(escaped_quote_idx) == Some(&'"') {
                    out.push('"');
                    i = i
                        .checked_add(2)
                        .ok_or_else(|| err("formula 문자열 cursor 계산에 실패했습니다."))?;
                    continue;
                }
                in_string = false;
            } else {
                in_string = true;
            }
            i = i.saturating_add(1);
            continue;
        }
        if in_string {
            out.push(ch);
            i = i.saturating_add(1);
            continue;
        }
        if ch == '\'' {
            let mut quoted_end = None;
            let mut quoted_index = i.saturating_add(1);
            while let Some(&quoted_char) = chars.get(quoted_index) {
                if quoted_char == '\'' {
                    let next_idx = quoted_index.saturating_add(1);
                    if chars.get(next_idx) == Some(&'\'') {
                        quoted_index = quoted_index.saturating_add(2);
                        continue;
                    }
                    if chars.get(next_idx) == Some(&'!') {
                        quoted_end = Some(quoted_index.saturating_add(2));
                    }
                    break;
                }
                quoted_index = quoted_index.saturating_add(1);
            }
            if let Some(next_idx) = quoted_end {
                for quoted in chars
                    .iter()
                    .skip(i)
                    .take(next_idx.saturating_sub(i))
                    .copied()
                {
                    out.push(quoted);
                }
                i = next_idx;
                continue;
            }
        }
        if (ch == '$' || ch.is_ascii_alphabetic())
            && let Some((end_idx, replaced)) = try_rewrite_cell_ref(&chars, i)?
        {
            out.push_str(&replaced);
            i = end_idx;
            continue;
        }
        out.push(ch);
        i = i.saturating_add(1);
    }
    Ok(out)
}
fn replace_formula_tag_with_plain_formula(inner_xml: &str, formula: &str) -> Result<String> {
    let Some(f_start) = find_start_tag(inner_xml, "f", 0) else {
        return Err(err("cell formula 태그를 찾지 못했습니다."));
    };
    let Some(f_end) = find_tag_end(inner_xml, f_start) else {
        return Err(err("cell formula 태그가 손상되었습니다."));
    };
    let open_tag = inner_xml
        .get(f_start..=f_end)
        .ok_or_else(|| err("cell formula 시작 태그 범위가 손상되었습니다."))?;
    let prefix = inner_xml
        .get(..f_start)
        .ok_or_else(|| err("cell formula prefix 범위가 손상되었습니다."))?;
    let suffix = if open_tag.trim_end().ends_with("/>") {
        inner_xml
            .get(checked_usize_add(f_end, 1, "cell formula suffix 시작")?..)
            .ok_or_else(|| err("cell formula suffix 범위가 손상되었습니다."))?
    } else {
        let close_search_from = checked_usize_add(f_end, 1, "cell formula 종료 태그 검색 시작")?;
        let Some(close_start) = find_end_tag(inner_xml, "f", close_search_from) else {
            return Err(err("cell formula 종료 태그를 찾지 못했습니다."));
        };
        inner_xml
            .get(checked_usize_add(close_start, "</f>".len(), "cell formula suffix 시작")?..)
            .ok_or_else(|| err("cell formula suffix 범위가 손상되었습니다."))?
    };
    let escaped_formula = xml_escape_text(formula);
    let capacity = prefix
        .len()
        .saturating_add(suffix.len())
        .saturating_add(escaped_formula.len())
        .saturating_add("<f></f>".len());
    let mut out = String::with_capacity(capacity);
    out.push_str(prefix);
    out.push_str("<f>");
    out.push_str(&escaped_formula);
    out.push_str("</f>");
    out.push_str(suffix);
    Ok(out)
}
fn try_parse_and_rewrite_cell_ref<F>(
    chars: &[char],
    start: usize,
    mut rewrite_ref: F,
) -> Result<Option<(usize, String)>>
where
    F: FnMut(u32, u32, bool, bool) -> Result<(u32, u32)>,
{
    let mut index = start;
    let mut col_lock = false;
    if chars.get(index) == Some(&'$') {
        col_lock = true;
        index = index.saturating_add(1);
    }
    let col_start = index;
    while matches!(chars.get(index), Some(ch) if ch.is_ascii_alphabetic()) {
        index = index.saturating_add(1);
    }
    if index == col_start {
        return Ok(None);
    }
    let col_chars = chars
        .get(col_start..index)
        .ok_or_else(|| err("formula column reference 범위가 손상되었습니다."))?;
    if col_chars.len() > 3 {
        return Ok(None);
    }
    let mut base_col = 0_u32;
    for ch in col_chars {
        let upper = ch.to_ascii_uppercase();
        if !upper.is_ascii_alphabetic() {
            return Ok(None);
        }
        let letter_value = u32::from(upper)
            .checked_sub(u32::from('A'))
            .and_then(|value| value.checked_add(1));
        let Some(letter) = letter_value else {
            return Ok(None);
        };
        let Some(next_col) = base_col
            .checked_mul(26)
            .and_then(|value| value.checked_add(letter))
        else {
            return Ok(None);
        };
        base_col = next_col;
    }
    if !(1..=MAX_A1_COL).contains(&base_col) {
        return Ok(None);
    }
    let mut row_lock = false;
    if chars.get(index) == Some(&'$') {
        row_lock = true;
        index = index.saturating_add(1);
    }
    let row_start = index;
    while matches!(chars.get(index), Some(ch) if ch.is_ascii_digit()) {
        index = index.saturating_add(1);
    }
    if index == row_start {
        return Ok(None);
    }
    let previous = start
        .checked_sub(1)
        .and_then(|previous_index| chars.get(previous_index))
        .copied();
    if matches!(previous, Some(ch) if is_ref_neighbor_identifier(ch)) {
        return Ok(None);
    }
    let next = chars.get(index).copied();
    if matches!(next, Some(ch) if is_ref_neighbor_identifier(ch) || matches!(ch, '!' | '\'' | '(' | '['))
    {
        return Ok(None);
    }
    let row_chars = chars
        .get(row_start..index)
        .ok_or_else(|| err("formula row reference 범위가 손상되었습니다."))?;
    let mut base_row = 0_u32;
    for ch in row_chars {
        let Some(digit) = u32::from(*ch).checked_sub(u32::from('0')) else {
            return Ok(None);
        };
        if digit > 9 {
            return Ok(None);
        }
        let Some(next_row) = base_row
            .checked_mul(10)
            .and_then(|value| value.checked_add(digit))
        else {
            return Ok(None);
        };
        base_row = next_row;
    }
    if !(1..=MAX_A1_ROW).contains(&base_row) {
        return Ok(None);
    }
    let (new_col, new_row) = rewrite_ref(base_col, base_row, col_lock, row_lock)?;
    let replaced = ref_with_locks(new_col, new_row, col_lock, row_lock);
    Ok(Some((index, replaced)))
}
fn remove_tags_matching_attr(
    xml: &str,
    tag_name: &str,
    attr_name: &str,
    expected_value: &str,
) -> Result<String> {
    remove_tags_matching(xml, tag_name, |attrs| {
        get_attr(attrs, attr_name) == Some(expected_value)
    })
}
fn remove_tags_matching<F>(xml: &str, tag_name: &str, mut should_remove: F) -> Result<String>
where
    F: FnMut(&[(String, String)]) -> bool,
{
    let mut out = xml.to_owned();
    let mut cursor = 0_usize;
    while let Some(tag_start) = find_start_tag(&out, tag_name, cursor) {
        let Some(tag_end) = find_tag_end(&out, tag_start) else {
            return Err(err(tag_error_message(tag_name, " 태그가 손상되었습니다.")));
        };
        let tag_xml = out
            .get(tag_start..=tag_end)
            .ok_or_else(|| err(tag_error_message(tag_name, " 태그 범위가 손상되었습니다.")))?
            .to_owned();
        let attrs = parse_tag_attrs(&tag_xml)?;
        let next_cursor = checked_usize_add(tag_end, 1, "XML 태그 다음 cursor")?;
        if should_remove(&attrs) {
            let tag_end_exclusive = if tag_xml.trim_end().ends_with("/>") {
                checked_usize_add(tag_end, 1, "XML self-closing 태그 끝")?
            } else {
                let close_search_from = checked_usize_add(tag_end, 1, "XML 종료 태그 검색 시작")?;
                let Some(close_start) = find_end_tag(&out, tag_name, close_search_from) else {
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
            out.replace_range(tag_start..tag_end_exclusive, "");
            cursor = tag_start;
        } else {
            cursor = next_cursor;
        }
    }
    Ok(out)
}
fn replace_first_tag_text(xml: &mut String, tag_name: &str, new_text: &str) -> bool {
    let mut open_pattern = String::with_capacity(tag_name.len().saturating_add(1));
    open_pattern.push('<');
    open_pattern.push_str(tag_name);
    let Some(open_start) = xml.find(&open_pattern) else {
        return false;
    };
    let Some(open_end_rel) = xml.get(open_start..).and_then(|tail| tail.find('>')) else {
        return false;
    };
    let Ok(content_start_base) = checked_usize_add(open_start, open_end_rel, "XML 태그 본문 시작")
    else {
        return false;
    };
    let Ok(content_start) = checked_usize_add(content_start_base, 1, "XML 태그 본문 시작")
    else {
        return false;
    };
    let mut close_pattern = String::with_capacity(tag_name.len().saturating_add(3));
    close_pattern.push_str("</");
    close_pattern.push_str(tag_name);
    close_pattern.push('>');
    let Some(close_rel) = xml
        .get(content_start..)
        .and_then(|tail| tail.find(&close_pattern))
    else {
        return false;
    };
    let Ok(close) = checked_usize_add(content_start, close_rel, "XML 태그 본문 끝") else {
        return false;
    };
    xml.replace_range(content_start..close, new_text);
    true
}
fn xml_escape_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    append_xml_escaped(&mut out, text, false);
    out
}
fn needs_xml_space_preserve(text: &str) -> bool {
    text.starts_with(' ') || text.ends_with(' ') || text.contains("  ")
}
fn parse_range_token(token: &str) -> (&str, &str) {
    token.split_once(':').unwrap_or((token, token))
}
fn parse_ref_with_locks(reference: &str) -> Option<(u32, u32, bool, bool)> {
    let (col_lock, after_col_lock) = reference
        .strip_prefix('$')
        .map_or((false, reference), |tail| (true, tail));
    let col_end = after_col_lock
        .find(|ch: char| !ch.is_ascii_alphabetic())
        .unwrap_or(after_col_lock.len());
    if col_end == 0 {
        return None;
    }
    let col_s = after_col_lock.get(..col_end)?;
    let after_col = after_col_lock.get(col_end..)?;
    let (row_lock, row_part) = after_col
        .strip_prefix('$')
        .map_or((false, after_col), |tail| (true, tail));
    let row_end = row_part
        .find(|ch: char| !ch.is_ascii_digit())
        .unwrap_or(row_part.len());
    if row_end == 0 || row_end != row_part.len() {
        return None;
    }
    let col = name_to_col(col_s)?;
    if !(1..=MAX_A1_COL).contains(&col) {
        return None;
    }
    let row = row_part.parse::<u32>().ok()?;
    Some((col, row, col_lock, row_lock))
}
fn ref_with_locks(col: u32, row: u32, col_lock: bool, row_lock: bool) -> String {
    let col_name = col_to_name(col);
    let capacity = col_name.len().saturating_add(12);
    let mut out = String::with_capacity(capacity);
    if col_lock {
        out.push('$');
    }
    out.push_str(&col_name);
    if row_lock {
        out.push('$');
    }
    push_display(&mut out, row);
    out
}
const fn is_ref_neighbor_identifier(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'
}
fn shift_formula_index(value: u32, delta: i64, max: u32) -> Result<u32> {
    let shifted = i64::from(value).checked_add(delta).ok_or_else(|| {
        let capacity = 80;
        let mut message = String::with_capacity(capacity);
        message.push_str("shared formula index 계산 overflow: ");
        push_display(&mut message, value);
        message.push_str(" + ");
        push_display(&mut message, delta);
        err(message)
    })?;
    if !(1..=i64::from(max)).contains(&shifted) {
        let capacity = 112;
        let mut message = String::with_capacity(capacity);
        message.push_str("shared formula 상대참조 이동 범위를 벗어났습니다. (");
        push_display(&mut message, value);
        message.push_str(" + ");
        push_display(&mut message, delta);
        message.push_str(", max=");
        push_display(&mut message, max);
        message.push(')');
        return Err(err(message));
    }
    u32::try_from(shifted).map_err(|source| {
        let capacity = 96;
        let mut message = String::with_capacity(capacity);
        message.push_str("shared formula index 변환 실패: ");
        push_display(&mut message, source);
        err(message)
    })
}
fn append_xml_escaped(out: &mut String, text: &str, escape_quotes: bool) {
    for ch in text.chars() {
        match ch {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' if escape_quotes => out.push_str("&quot;"),
            '\'' if escape_quotes => out.push_str("&apos;"),
            _ => out.push(ch),
        }
    }
}
fn build_formula_with_empty_value(formula_text: &str) -> String {
    let capacity = formula_text.len().saturating_add(14);
    let mut out = String::with_capacity(capacity);
    out.push_str("<f>");
    out.push_str(formula_text);
    out.push_str("</f>");
    out.push_str("<v></v>");
    out
}
fn build_open_tag(name: &str, attrs: &[(String, String)]) -> String {
    let attrs_xml = attrs_to_xml(attrs);
    let capacity = name.len().saturating_add(attrs_xml.len()).saturating_add(2);
    let mut out = String::with_capacity(capacity);
    out.push('<');
    out.push_str(name);
    out.push_str(&attrs_xml);
    out.push('>');
    out
}
fn build_self_closing_tag(name: &str, attrs: &[(String, String)]) -> String {
    let attrs_xml = attrs_to_xml(attrs);
    let capacity = name.len().saturating_add(attrs_xml.len()).saturating_add(4);
    let mut out = String::with_capacity(capacity);
    out.push('<');
    out.push_str(name);
    out.push_str(&attrs_xml);
    out.push_str("/>");
    out
}
fn build_display_text_tag(name: &str, value: impl Display) -> String {
    let capacity = name.len().saturating_mul(2).saturating_add(16);
    let mut out = String::with_capacity(capacity);
    out.push('<');
    out.push_str(name);
    out.push('>');
    push_display(&mut out, value);
    out.push_str("</");
    out.push_str(name);
    out.push('>');
    out
}
fn build_ref_range(start_col_text: &str, start_row: u32, end_col: u32, end_row: u32) -> String {
    let end_ref = ref_with_locks(end_col, end_row, false, false);
    let capacity = start_col_text
        .len()
        .saturating_add(12)
        .saturating_add(1)
        .saturating_add(end_ref.len());
    let mut out = String::with_capacity(capacity);
    out.push_str(start_col_text);
    push_display(&mut out, start_row);
    out.push(':');
    out.push_str(&end_ref);
    out
}
fn display_string(value: impl Display) -> String {
    let capacity = 16;
    let mut out = String::with_capacity(capacity);
    push_display(&mut out, value);
    out
}
fn offset_only_error(prefix: &str, offset: usize) -> String {
    let capacity = prefix.len().saturating_add(24);
    let mut out = String::with_capacity(capacity);
    out.push_str(prefix);
    push_display(&mut out, offset);
    out.push(')');
    out
}
fn row_only_error(prefix: &str, row_num: u32) -> String {
    let capacity = prefix.len().saturating_add(24);
    let mut out = String::with_capacity(capacity);
    out.push_str(prefix);
    push_display(&mut out, row_num);
    out.push(')');
    out
}
fn row_offset_error(prefix: &str, row_num: u32, offset: usize) -> String {
    let capacity = prefix.len().saturating_add(40);
    let mut out = String::with_capacity(capacity);
    out.push_str(prefix);
    push_display(&mut out, row_num);
    out.push_str(", offset=");
    push_display(&mut out, offset);
    out.push(')');
    out
}
fn row_col_error(prefix: &str, row_num: u32, col: u32) -> String {
    let capacity = prefix.len().saturating_add(40);
    let mut out = String::with_capacity(capacity);
    out.push_str(prefix);
    push_display(&mut out, row_num);
    out.push_str(", col=");
    push_display(&mut out, col);
    out.push(')');
    out
}
fn tag_error_message(tag_name: &str, suffix: &str) -> String {
    let capacity = tag_name.len().saturating_add(suffix.len());
    let mut out = String::with_capacity(capacity);
    out.push_str(tag_name);
    out.push_str(suffix);
    out
}
