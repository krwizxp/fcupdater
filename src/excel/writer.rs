use super::{
    ooxml::{load_shared_strings, load_sheet_catalog, load_sheet_xml},
    xlsx_container::XlsxContainer,
    xml::{
        decode_xml_entities, extract_all_tag_text, extract_first_tag_text, find_end_tag,
        find_start_tag, find_tag_end,
    },
};
use crate::{Result, err, parse_i32_str};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    iter::{Peekable, from_fn},
    path::Path,
    str::Chars,
};
#[derive(Debug)]
pub struct Workbook {
    container: XlsxContainer,
    xml_text: String,
    shared_strings_xml_text: Option<String>,
    shared_strings: Vec<String>,
    sheet_paths: BTreeMap<String, String>,
    sheets: BTreeMap<String, Worksheet>,
}
#[derive(Debug, Clone, Default)]
pub struct Worksheet {
    pub prefix: String,
    pub suffix: String,
    pub rows: BTreeMap<u32, Row>,
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
    si: String,
    formula_text: Option<String>,
}
const MAX_A1_COL: u32 = 0x4000;
const MAX_A1_ROW: u32 = 0x0010_0000;
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
        let mut sheet_paths = BTreeMap::new();
        let mut sheets = BTreeMap::new();
        for sheet_name in &catalog.sheet_order {
            let Some(sheet_path) = catalog.sheet_name_to_path.get(sheet_name) else {
                continue;
            };
            let xml = load_sheet_xml(&container, &catalog, sheet_name)?;
            let mut sheet = Worksheet::parse(&xml)?;
            sheet.normalize_shared_formulas()?;
            sheet_paths.insert(sheet_name.clone(), sheet_path.clone());
            sheets.insert(sheet_name.clone(), sheet);
        }
        Ok(Self {
            container,
            xml_text: workbook_xml,
            shared_strings_xml_text,
            shared_strings,
            sheet_paths,
            sheets,
        })
    }
    pub const fn workbook_xml_mut(&mut self) -> &mut String {
        &mut self.xml_text
    }
    pub fn with_sheet_mut<R, F>(&mut self, name: &str, f: F) -> Option<R>
    where
        F: FnOnce(&mut Worksheet, &[String]) -> R,
    {
        let (shared_strings, sheets) = (&self.shared_strings, &mut self.sheets);
        let ws = sheets.get_mut(name)?;
        Some(f(ws, shared_strings))
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
    fn request_full_recalculation(&mut self) -> Result<()> {
        self.xml_text = update_calc_pr_xml(&self.xml_text)?;
        Ok(())
    }
    fn remove_excel_recovery_artifacts(&mut self) -> Result<()> {
        self.xml_text = remove_tags_named(&self.xml_text, "fileRecoveryPr")?;
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
    fn promote_safe_inline_strings_to_shared(&mut self) -> Result<()> {
        if self.shared_strings_xml_text.is_none() {
            return Ok(());
        }
        let existing_total = self.shared_strings.len();
        let existing_unique = unique_string_count(&self.shared_strings);
        let mut index_map: HashMap<String, usize> = HashMap::new();
        for (idx, value) in self.shared_strings.iter().enumerate() {
            index_map.entry(value.clone()).or_insert(idx);
        }
        let mut newly_appended_shared_strings = Vec::new();
        for sheet in self.sheets.values_mut() {
            for row in sheet.rows.values_mut() {
                for cell in row.cells.values_mut() {
                    if get_attr(&cell.attrs, "t") != Some("inlineStr") {
                        continue;
                    }
                    let Some(inner_xml) = cell.inner_xml.as_deref() else {
                        continue;
                    };
                    let Some(text) = extract_plain_inline_string_text(inner_xml) else {
                        continue;
                    };
                    let shared_idx = if let Some(idx) = index_map.get(&text).copied() {
                        idx
                    } else {
                        let idx = self.shared_strings.len();
                        self.shared_strings.push(text.clone());
                        index_map.insert(text.clone(), idx);
                        newly_appended_shared_strings.push(text);
                        idx
                    };
                    set_attr(&mut cell.attrs, "t", "s".to_owned());
                    cell.inner_xml = Some(format!("<v>{shared_idx}</v>"));
                }
            }
        }
        if newly_appended_shared_strings.is_empty() {
            return Ok(());
        }
        let original_xml = self
            .shared_strings_xml_text
            .take()
            .ok_or_else(|| err("sharedStrings XML 상태가 비정상적입니다."))?;
        let updated_xml = append_shared_strings_xml(
            &original_xml,
            &newly_appended_shared_strings,
            existing_total,
            existing_unique,
        )?;
        self.shared_strings_xml_text = Some(updated_xml);
        Ok(())
    }
}
impl Worksheet {
    pub fn parse(xml: &str) -> Result<Self> {
        let Some(sheet_data_open) = find_start_tag(xml, "sheetData", 0) else {
            return Err(err("worksheet XML에 <sheetData>가 없습니다."));
        };
        let Some(sheet_data_open_end) = find_tag_end(xml, sheet_data_open) else {
            return Err(err(
                "worksheet XML의 <sheetData> 시작 태그가 손상되었습니다.",
            ));
        };
        let sheet_data_body_start = sheet_data_open_end + 1;
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
        let rows = parse_rows_from_sheet_data(body)?;
        Ok(Self {
            prefix,
            suffix,
            rows,
        })
    }
    pub fn to_xml(&self) -> String {
        let mut out = String::new();
        out.push_str(&self.prefix);
        for row in self.rows.values() {
            out.push_str(&row_to_xml(row));
        }
        out.push_str(&self.suffix);
        out
    }
    fn normalize_shared_formulas(&mut self) -> Result<()> {
        let mut heads: HashMap<String, SharedFormulaHead> = HashMap::new();
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
                    err(format!(
                        "shared formula head를 찾지 못했습니다. (si={}, cell={}{} )",
                        spec.si,
                        col_to_name(*col_num),
                        row_num
                    ))
                })?;
                let formula = if let Some(text) = spec.formula_text {
                    text
                } else {
                    shift_formula_cell_refs(
                        &head.formula,
                        i64::from(*col_num) - i64::from(head.anchor_col),
                        i64::from(*row_num) - i64::from(head.anchor_row),
                    )?
                };
                cell.inner_xml = Some(replace_formula_tag_with_plain_formula(inner_xml, &formula)?);
            }
        }
        Ok(())
    }
    pub fn get_display_at(&self, col: u32, row: u32, shared_strings: &[String]) -> String {
        let Some(row_obj) = self.rows.get(&row) else {
            return String::new();
        };
        let Some(cell) = row_obj.cells.get(&col) else {
            return String::new();
        };
        cell_display_value(cell, shared_strings).unwrap_or_default()
    }
    pub fn get_i32_at(&self, col: u32, row: u32, shared_strings: &[String]) -> Option<i32> {
        let text = self.get_display_at(col, row, shared_strings);
        parse_i32_str(&text)
    }
    pub fn set_string_at(&mut self, col: u32, row: u32, value: &str) {
        let cell = self.get_or_create_cell_mut(col, row);
        set_attr(&mut cell.attrs, "t", "inlineStr".to_owned());
        let text = xml_escape_text(value);
        let preserve = needs_xml_space_preserve(value);
        let inner = if preserve {
            format!("<is><t xml:space=\"preserve\">{text}</t></is>")
        } else {
            format!("<is><t>{text}</t></is>")
        };
        cell.inner_xml = Some(inner);
    }
    pub fn set_i32_at(&mut self, col: u32, row: u32, value: Option<i32>) {
        let cell = self.get_or_create_cell_mut(col, row);
        remove_attr(&mut cell.attrs, "t");
        if let Some(v) = value {
            cell.inner_xml = Some(format!("<v>{v}</v>"));
        } else {
            cell.inner_xml = None;
        }
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
            *inner = format!("<f>{formula_text}</f><v></v>");
        } else {
            cell.inner_xml = Some(format!("<f>{formula_text}</f><v></v>"));
        }
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
    pub fn clear_formula_cached_values(&mut self) {
        for row in self.rows.values_mut() {
            for cell in row.cells.values_mut() {
                let Some(inner) = cell.inner_xml.as_mut() else {
                    continue;
                };
                if extract_first_tag_text(inner, "f").is_some()
                    && !replace_first_tag_text(inner, "v", "")
                    && !inner.contains("<v")
                {
                    inner.push_str("<v></v>");
                }
            }
        }
    }
    pub fn has_any_row_format(&self, row: u32, max_col: u32) -> bool {
        let Some(row_obj) = self.rows.get(&row) else {
            return false;
        };
        if !row_obj.attrs.is_empty() {
            return true;
        }
        (1..=max_col).any(|col| row_obj.cells.contains_key(&col))
    }
    pub fn clone_row_style(&mut self, source_row: u32, target_row: u32, max_col: u32) {
        let Some(src) = self.rows.get(&source_row).cloned() else {
            return;
        };
        let mut cloned = src;
        remap_row_numbers(&mut cloned, target_row, &|r| {
            if r == source_row { target_row } else { r }
        });
        cloned.cells.retain(|col, _| *col <= max_col);
        for cell in cloned.cells.values_mut() {
            clear_cloned_cell_value_preserve_formula(cell);
        }
        self.rows.insert(target_row, cloned);
    }
    pub fn row_has_any_data(&self, row: u32, cols: &[u32], shared_strings: &[String]) -> bool {
        cols.iter().any(|col| {
            !self
                .get_display_at(*col, row, shared_strings)
                .trim()
                .is_empty()
        })
    }
    pub fn max_cell_col(&self) -> u32 {
        self.rows
            .values()
            .flat_map(|row| row.cells.keys().copied())
            .max()
            .unwrap_or(1)
    }
    pub fn max_row_num(&self) -> u32 {
        self.rows.keys().copied().max().unwrap_or(1)
    }
    pub fn update_dimension(&mut self) -> Result<()> {
        let max_row = self.max_row_num();
        let max_col = self.max_cell_col();
        let end_ref = format!("{}{}", col_to_name(max_col), max_row);
        self.prefix = update_dimension_ref_xml(&self.prefix, "A1", &end_ref)?;
        Ok(())
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
        self.suffix = extend_conditional_formats_in_suffix(
            &self.suffix,
            last_data_row,
            target_cols,
            data_start_row,
        )?;
        Ok(())
    }
    pub fn update_auto_filter_ref(&mut self, header_row: u32, last_data_row: u32) -> Result<()> {
        self.suffix = update_auto_filter_ref_in_suffix(&self.suffix, header_row, last_data_row)?;
        Ok(())
    }
    fn get_or_create_cell_mut(&mut self, col: u32, row: u32) -> &mut Cell {
        let row_obj = self.rows.entry(row).or_insert_with(|| Row {
            attrs: vec![("r".to_owned(), row.to_string())],
            cells: BTreeMap::new(),
        });
        if get_attr(&row_obj.attrs, "r").is_none() {
            set_attr(&mut row_obj.attrs, "r", row.to_string());
        }
        row_obj.cells.entry(col).or_insert_with(|| Cell {
            attrs: vec![
                ("r".to_owned(), format!("{}{}", col_to_name(col), row)),
                ("s".to_owned(), "0".to_owned()),
            ],
            inner_xml: None,
        })
    }
}
pub fn remap_row_numbers(row: &mut Row, new_row: u32, resolver: &dyn Fn(u32) -> u32) {
    set_attr(&mut row.attrs, "r", new_row.to_string());
    for (col, cell) in &mut row.cells {
        set_attr(
            &mut cell.attrs,
            "r",
            format!("{}{}", col_to_name(*col), new_row),
        );
        if let Some(inner) = cell.inner_xml.as_mut() {
            *inner = rewrite_formula_rows_in_inner(inner, resolver);
        }
    }
}
fn rewrite_formula_rows(formula: &str, resolver: &dyn Fn(u32) -> u32) -> String {
    rewrite_formula_cell_refs(formula, |chars, start| {
        Ok(try_parse_and_rewrite_cell_ref(chars, start, resolver))
    })
    .unwrap_or_else(|_| formula.to_owned())
}
pub fn col_to_name(mut col: u32) -> String {
    if col == 0 {
        return "A".to_owned();
    }
    let mut rev = Vec::new();
    while col > 0 {
        let Ok(rem) = u8::try_from((col - 1) % 26) else {
            return String::new();
        };
        rev.push(char::from(b'A' + rem));
        col = (col - 1) / 26;
    }
    let mut out = String::with_capacity(rev.len());
    for ch in rev.into_iter().rev() {
        out.push(ch);
    }
    out
}
fn name_to_col(name: &str) -> Option<u32> {
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
            .checked_add(u32::from(upper - b'A' + 1))?;
    }
    Some(out)
}
fn parse_rows_from_sheet_data(body: &str) -> Result<BTreeMap<u32, Row>> {
    let mut rows = BTreeMap::new();
    let mut cursor = 0_usize;
    while let Some(row_open_rel) = body.get(cursor..).and_then(|tail| tail.find("<row")) {
        let row_open = cursor + row_open_rel;
        let Some(row_tag_end_rel) = body.get(row_open..).and_then(|tail| tail.find('>')) else {
            return Err(err(format!(
                "sheetData row 시작 태그가 손상되었습니다. (offset={row_open})"
            )));
        };
        let row_tag_end = row_open + row_tag_end_rel;
        let row_tag = body.get(row_open..=row_tag_end).ok_or_else(|| {
            err(format!(
                "sheetData row 태그 범위가 손상되었습니다. (offset={row_open})"
            ))
        })?;
        let mut row_attrs = parse_tag_attrs(row_tag)?;
        let row_num = get_attr(&row_attrs, "r")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or_else(|| rows.keys().last().copied().unwrap_or(0) + 1);
        set_attr(&mut row_attrs, "r", row_num.to_string());
        if row_tag.ends_with("/>") {
            rows.insert(
                row_num,
                Row {
                    attrs: row_attrs,
                    cells: BTreeMap::new(),
                },
            );
            cursor = row_tag_end + 1;
            continue;
        }
        let row_body_start = row_tag_end + 1;
        let Some(row_close_rel) = body
            .get(row_body_start..)
            .and_then(|tail| tail.find("</row>"))
        else {
            return Err(err(format!(
                "sheetData row 종료 태그를 찾지 못했습니다. (row={row_num})"
            )));
        };
        let row_body_end = row_body_start + row_close_rel;
        let row_body = body.get(row_body_start..row_body_end).ok_or_else(|| {
            err(format!(
                "sheetData row 본문 범위가 손상되었습니다. (row={row_num})"
            ))
        })?;
        let mut row = Row {
            attrs: row_attrs,
            cells: BTreeMap::new(),
        };
        parse_row_cells(row_body, row_num, &mut row)?;
        rows.insert(row_num, row);
        cursor = row_body_end + "</row>".len();
    }
    Ok(rows)
}
fn parse_row_cells(row_body: &str, row_num: u32, row: &mut Row) -> Result<()> {
    let mut cursor = 0_usize;
    let mut next_col = 1_u32;
    while let Some(cell_open_rel) = row_body.get(cursor..).and_then(|tail| tail.find("<c")) {
        let cell_open = cursor + cell_open_rel;
        let Some(cell_tag_end_rel) = row_body.get(cell_open..).and_then(|tail| tail.find('>'))
        else {
            return Err(err(format!(
                "row 내 cell 시작 태그가 손상되었습니다. (row={row_num}, offset={cell_open})"
            )));
        };
        let cell_tag_end = cell_open + cell_tag_end_rel;
        let cell_tag = row_body.get(cell_open..=cell_tag_end).ok_or_else(|| {
            err(format!(
                "row 내 cell 태그 범위가 손상되었습니다. (row={row_num}, offset={cell_open})"
            ))
        })?;
        let mut attrs = parse_tag_attrs(cell_tag)?;
        let col = get_attr(&attrs, "r")
            .and_then(|v| parse_cell_ref(v).map(|(c, _)| c))
            .unwrap_or(next_col);
        set_attr(&mut attrs, "r", format!("{}{}", col_to_name(col), row_num));
        if cell_tag.ends_with("/>") {
            row.cells.insert(
                col,
                Cell {
                    attrs,
                    inner_xml: None,
                },
            );
            next_col = col.saturating_add(1);
            cursor = cell_tag_end + 1;
            continue;
        }
        let cell_body_start = cell_tag_end + 1;
        let Some(cell_close_rel) = row_body
            .get(cell_body_start..)
            .and_then(|tail| tail.find("</c>"))
        else {
            return Err(err(format!(
                "row 내 cell 종료 태그를 찾지 못했습니다. (row={row_num}, col={col})"
            )));
        };
        let cell_body_end = cell_body_start + cell_close_rel;
        let inner_xml = row_body
            .get(cell_body_start..cell_body_end)
            .ok_or_else(|| {
                err(format!(
                    "row 내 cell 본문 범위가 손상되었습니다. (row={row_num}, col={col})"
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
        cursor = cell_body_end + "</c>".len();
    }
    Ok(())
}
fn row_to_xml(row: &Row) -> String {
    let mut attrs = row.attrs.clone();
    attrs.sort_by(|a, b| attr_sort_key(&a.0).cmp(&attr_sort_key(&b.0)));
    let mut out = String::new();
    out.push_str("<row");
    out.push_str(&attrs_to_xml(&attrs));
    if row.cells.is_empty() {
        out.push_str("/>");
        return out;
    }
    out.push('>');
    for cell in row.cells.values() {
        out.push_str(&cell_to_xml(cell));
    }
    out.push_str("</row>");
    out
}
fn cell_to_xml(cell: &Cell) -> String {
    let mut attrs = cell.attrs.clone();
    attrs.sort_by(|a, b| attr_sort_key(&a.0).cmp(&attr_sort_key(&b.0)));
    let mut out = String::new();
    out.push_str("<c");
    out.push_str(&attrs_to_xml(&attrs));
    if let Some(inner) = cell.inner_xml.as_ref() {
        out.push('>');
        out.push_str(inner);
        out.push_str("</c>");
    } else {
        out.push_str("/>");
    }
    out
}
fn clear_cloned_cell_value_preserve_formula(cell: &mut Cell) {
    let Some(inner) = cell.inner_xml.as_mut() else {
        remove_attr(&mut cell.attrs, "t");
        return;
    };
    if extract_first_tag_text(inner, "f").is_some() {
        if !replace_first_tag_text(inner, "v", "") && !inner.contains("<v") {
            inner.push_str("<v></v>");
        }
    } else {
        remove_attr(&mut cell.attrs, "t");
        cell.inner_xml = None;
    }
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
fn attrs_to_xml(attrs: &[(String, String)]) -> String {
    let mut out = String::new();
    for attr in attrs {
        out.push(' ');
        out.push_str(&attr.0);
        out.push_str("=\"");
        out.push_str(&xml_escape_attr(&attr.1));
        out.push('"');
    }
    out
}
fn parse_tag_attrs(tag: &str) -> Result<Vec<(String, String)>> {
    let mut out = Vec::new();
    let Some(lt) = tag.find('<') else {
        return Err(err(format!(
            "XML 태그 파싱 실패: '<'를 찾지 못했습니다. tag={tag}"
        )));
    };
    let mut i = lt + 1;
    let bytes = tag.as_bytes();
    while matches!(bytes.get(i), Some(ch) if !ch.is_ascii_whitespace() && *ch != b'>' && *ch != b'/')
    {
        i += 1;
    }
    if i >= bytes.len() {
        return Err(err(format!(
            "XML 태그 파싱 실패: 태그 종료 기호를 찾지 못했습니다. tag={tag}"
        )));
    }
    while i < bytes.len() {
        while matches!(bytes.get(i), Some(ch) if ch.is_ascii_whitespace()) {
            i += 1;
        }
        if matches!(bytes.get(i), None | Some(b'>' | b'/')) {
            break;
        }
        let key_start = i;
        while matches!(bytes.get(i), Some(ch) if !ch.is_ascii_whitespace() && *ch != b'=' && *ch != b'>' && *ch != b'/')
        {
            i += 1;
        }
        let key_end = i;
        if key_start == key_end {
            return Err(err(format!(
                "XML 속성 파싱 실패: 속성 이름이 비어 있습니다. tag={tag}"
            )));
        }
        while matches!(bytes.get(i), Some(ch) if ch.is_ascii_whitespace()) {
            i += 1;
        }
        if bytes.get(i).is_none() {
            return Err(err(format!(
                "XML 속성 파싱 실패: '='를 찾지 못했습니다. tag={tag}"
            )));
        }
        if bytes.get(i) != Some(&b'=') {
            return Err(err(format!(
                "XML 속성 파싱 실패: '='가 필요합니다. tag={tag}"
            )));
        }
        i += 1;
        while matches!(bytes.get(i), Some(ch) if ch.is_ascii_whitespace()) {
            i += 1;
        }
        if bytes.get(i).is_none() {
            return Err(err(format!(
                "XML 속성 파싱 실패: 값 quote가 없습니다. tag={tag}"
            )));
        }
        if !matches!(bytes.get(i), Some(b'"' | b'\'')) {
            return Err(err(format!(
                "XML 속성 파싱 실패: 속성 값은 quote로 감싸야 합니다. tag={tag}"
            )));
        }
        let Some(&quote) = bytes.get(i) else {
            return Err(err(format!(
                "XML 속성 파싱 실패: 값 quote가 없습니다. tag={tag}"
            )));
        };
        i += 1;
        let value_start = i;
        while matches!(bytes.get(i), Some(ch) if *ch != quote) {
            i += 1;
        }
        if i >= bytes.len() {
            return Err(err(format!(
                "XML 속성 파싱 실패: 닫히지 않은 quote가 있습니다. tag={tag}"
            )));
        }
        let key = tag.get(key_start..key_end).ok_or_else(|| {
            err(format!(
                "XML 속성 파싱 실패: 키 범위를 계산할 수 없습니다. tag={tag}"
            ))
        })?;
        let raw_value = tag.get(value_start..i).ok_or_else(|| {
            err(format!(
                "XML 속성 파싱 실패: 값 범위를 계산할 수 없습니다. tag={tag}"
            ))
        })?;
        let value = decode_xml_entities(raw_value);
        out.push((key.to_owned(), value));
        if i < bytes.len() {
            i += 1;
        }
    }
    Ok(out)
}
fn get_attr<'attrs>(attrs: &'attrs [(String, String)], name: &str) -> Option<&'attrs str> {
    attrs
        .iter()
        .find_map(|attr| (attr.0 == name).then_some(attr.1.as_str()))
}
fn set_attr(attrs: &mut Vec<(String, String)>, name: &str, value: String) {
    for attr in attrs.iter_mut() {
        if attr.0 == name {
            attr.1 = value;
            return;
        }
    }
    attrs.push((name.to_owned(), value));
}
fn remove_attr(attrs: &mut Vec<(String, String)>, name: &str) {
    attrs.retain(|attr| attr.0 != name);
}
fn parse_cell_ref(cell_ref: &str) -> Option<(u32, u32)> {
    let mut col_s = String::new();
    let mut row_s = String::new();
    for ch in cell_ref.chars() {
        if ch == '$' {
            continue;
        }
        if ch.is_ascii_alphabetic() {
            if !row_s.is_empty() {
                return None;
            }
            col_s.push(ch);
        } else if ch.is_ascii_digit() {
            row_s.push(ch);
        } else {
            return None;
        }
    }
    let col = name_to_col(&col_s)?;
    let row = row_s.parse::<u32>().ok()?;
    Some((col, row))
}
fn cell_display_value(cell: &Cell, shared_strings: &[String]) -> Option<String> {
    let cell_type = get_attr(&cell.attrs, "t");
    let inner = cell.inner_xml.as_deref().unwrap_or_default();
    if matches!(cell_type, Some("inlineStr")) {
        return extract_all_tag_text(inner, "t").map(|v| decode_xml_entities(&v));
    }
    let raw_v = extract_first_tag_text(inner, "v").unwrap_or_default();
    let decoded = decode_xml_entities(&raw_v);
    if matches!(cell_type, Some("s")) {
        let idx = decoded.parse::<usize>().ok()?;
        return shared_strings.get(idx).cloned();
    }
    if matches!(cell_type, Some("b")) {
        return Some(if decoded == "1" {
            "TRUE".to_owned()
        } else {
            "FALSE".to_owned()
        });
    }
    Some(decoded)
}
fn rewrite_formula_rows_in_inner(inner_xml: &str, resolver: &dyn Fn(u32) -> u32) -> String {
    let mut out = inner_xml.to_owned();
    if let Some(text) = extract_first_tag_text(&out, "f") {
        let rewritten = rewrite_formula_rows(&decode_xml_entities(&text), resolver);
        let encoded = xml_escape_text(&rewritten);
        replace_first_tag_text(&mut out, "f", &encoded);
    }
    out
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
    Ok(Some(SharedFormulaSpec { si, formula_text }))
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
            .get(f_end + 1..)
            .ok_or_else(|| err("cell formula suffix 범위가 손상되었습니다."))?
    } else {
        let Some(close_start) = find_end_tag(inner_xml, "f", f_end + 1) else {
            return Err(err("cell formula 종료 태그를 찾지 못했습니다."));
        };
        inner_xml
            .get(close_start + "</f>".len()..)
            .ok_or_else(|| err("cell formula suffix 범위가 손상되었습니다."))?
    };
    Ok(format!(
        "{prefix}<f>{}</f>{suffix}",
        xml_escape_text(formula)
    ))
}
fn shift_formula_cell_refs(formula: &str, delta_col: i64, delta_row: i64) -> Result<String> {
    rewrite_formula_cell_refs(formula, |chars, start| {
        try_parse_and_shift_cell_ref(chars, start, delta_col, delta_row)
    })
}
fn rewrite_formula_cell_refs<F>(formula: &str, mut try_rewrite_cell_ref: F) -> Result<String>
where
    F: FnMut(&[char], usize) -> Result<Option<(usize, String)>>,
{
    let chars: Vec<char> = formula.chars().collect();
    let mut i = 0_usize;
    let mut out = String::with_capacity(formula.len());
    let mut in_string = false;
    while let Some(&ch) = chars.get(i) {
        if ch == '"' {
            out.push(ch);
            if in_string {
                if chars.get(i + 1) == Some(&'"') {
                    out.push('"');
                    i += 2;
                    continue;
                }
                in_string = false;
            } else {
                in_string = true;
            }
            i += 1;
            continue;
        }
        if in_string {
            out.push(ch);
            i += 1;
            continue;
        }
        if ch == '\''
            && let Some(next_idx) = try_parse_quoted_sheet_prefix(&chars, i)
        {
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
        if is_cell_ref_start(ch)
            && let Some((end_idx, replaced)) = try_rewrite_cell_ref(&chars, i)?
        {
            out.push_str(&replaced);
            i = end_idx;
            continue;
        }
        out.push(ch);
        i += 1;
    }
    Ok(out)
}
fn remove_tags_named(xml: &str, tag_name: &str) -> Result<String> {
    remove_tags_matching(xml, tag_name, |_| true)
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
            return Err(err(format!("{tag_name} 태그가 손상되었습니다.")));
        };
        let tag_xml = out
            .get(tag_start..=tag_end)
            .ok_or_else(|| err(format!("{tag_name} 태그 범위가 손상되었습니다.")))?
            .to_owned();
        let attrs = parse_tag_attrs(&tag_xml)?;
        let next_cursor = tag_end + 1;
        if should_remove(&attrs) {
            let tag_end_exclusive = if tag_xml.trim_end().ends_with("/>") {
                tag_end + 1
            } else {
                let Some(close_start) = find_end_tag(&out, tag_name, tag_end + 1) else {
                    return Err(err(format!("{tag_name} 종료 태그를 찾지 못했습니다.")));
                };
                close_start + format!("</{tag_name}>").len()
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
    let open_pattern = format!("<{tag_name}");
    let Some(open_start) = xml.find(&open_pattern) else {
        return false;
    };
    let Some(open_end_rel) = xml.get(open_start..).and_then(|tail| tail.find('>')) else {
        return false;
    };
    let content_start = open_start + open_end_rel + 1;
    let close_pattern = format!("</{tag_name}>");
    let Some(close_rel) = xml
        .get(content_start..)
        .and_then(|tail| tail.find(&close_pattern))
    else {
        return false;
    };
    let close = content_start + close_rel;
    xml.replace_range(content_start..close, new_text);
    true
}
fn xml_escape_text(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}
fn xml_escape_attr(s: &str) -> String {
    xml_escape_text(s)
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
fn needs_xml_space_preserve(s: &str) -> bool {
    s.starts_with(' ') || s.ends_with(' ') || s.contains("  ")
}
fn unique_string_count(values: &[String]) -> usize {
    let mut seen: HashSet<&str> = HashSet::new();
    for value in values {
        seen.insert(value);
    }
    seen.len()
}
fn extract_plain_inline_string_text(inner_xml: &str) -> Option<String> {
    if !inner_xml.contains("<is") {
        return None;
    }
    if inner_xml.contains("<r")
        || inner_xml.contains("<rPr")
        || inner_xml.contains("<rPh")
        || inner_xml.contains("<phoneticPr")
    {
        return None;
    }
    extract_all_tag_text(inner_xml, "t").map(|v| decode_xml_entities(&v))
}
fn append_shared_strings_xml(
    original_xml: &str,
    new_values: &[String],
    existing_total: usize,
    existing_unique: usize,
) -> Result<String> {
    if new_values.is_empty() {
        return Ok(original_xml.to_owned());
    }
    let Some(open_start) = find_start_tag(original_xml, "sst", 0) else {
        return Err(err("sharedStrings XML에 <sst>가 없습니다."));
    };
    let Some(open_end) = find_tag_end(original_xml, open_start) else {
        return Err(err("sharedStrings XML의 <sst> 시작 태그가 손상되었습니다."));
    };
    let open_tag = original_xml
        .get(open_start..=open_end)
        .ok_or_else(|| err("sharedStrings XML의 <sst> 태그 범위가 손상되었습니다."))?;
    let mut attrs = parse_tag_attrs(open_tag)?;
    let new_total = existing_total
        .checked_add(new_values.len())
        .ok_or_else(|| err("sharedStrings count 계산 중 overflow가 발생했습니다."))?;
    let new_unique = existing_unique
        .checked_add(new_values.len())
        .ok_or_else(|| err("sharedStrings uniqueCount 계산 중 overflow가 발생했습니다."))?;
    set_attr(&mut attrs, "count", new_total.to_string());
    set_attr(&mut attrs, "uniqueCount", new_unique.to_string());
    let mut new_si_xml = String::new();
    for value in new_values {
        new_si_xml.push_str(&shared_string_si_xml(value));
    }
    if open_tag.trim_end().ends_with("/>") {
        let replacement = format!("<sst{}>{new_si_xml}</sst>", attrs_to_xml(&attrs));
        let mut out = original_xml.to_owned();
        out.replace_range(open_start..=open_end, &replacement);
        return Ok(out);
    }
    let new_open_tag = format!("<sst{}>", attrs_to_xml(&attrs));
    let mut out = original_xml.to_owned();
    out.replace_range(open_start..=open_end, &new_open_tag);
    let close_search_from = open_start + new_open_tag.len();
    let Some(close_start) = find_end_tag(&out, "sst", close_search_from) else {
        return Err(err("sharedStrings XML에 </sst>가 없습니다."));
    };
    out.insert_str(close_start, &new_si_xml);
    Ok(out)
}
fn shared_string_si_xml(value: &str) -> String {
    let text = xml_escape_text(value);
    if needs_xml_space_preserve(value) {
        format!("<si><t xml:space=\"preserve\">{text}</t></si>")
    } else {
        format!("<si><t>{text}</t></si>")
    }
}
fn update_dimension_ref_xml(prefix_xml: &str, start_ref: &str, end_ref: &str) -> Result<String> {
    let mut out = prefix_xml.to_owned();
    if let Some(dim_pos) = out.find("<dimension")
        && let Some(dim_end_rel) = out.get(dim_pos..).and_then(|tail| tail.find('>'))
    {
        let dim_end = dim_pos + dim_end_rel + 1;
        let tag = out
            .get(dim_pos..dim_end)
            .ok_or_else(|| err("dimension 태그 범위가 손상되었습니다."))?;
        let mut attrs = parse_tag_attrs(tag)?;
        set_attr(&mut attrs, "ref", format!("{start_ref}:{end_ref}"));
        let new_tag = format!("<dimension{}/>", attrs_to_xml(&attrs));
        out.replace_range(dim_pos..dim_end, &new_tag);
        return Ok(out);
    }
    Ok(out)
}
fn update_calc_pr_xml(workbook_xml: &str) -> Result<String> {
    let set_calc_pr_attrs = |attrs: &mut Vec<(String, String)>| {
        set_attr(attrs, "calcMode", "auto".to_owned());
        set_attr(attrs, "fullCalcOnLoad", "1".to_owned());
        set_attr(attrs, "forceFullCalc", "1".to_owned());
        set_attr(attrs, "calcCompleted", "0".to_owned());
    };
    let mut out = workbook_xml.to_owned();
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
            let new_tag = format!("<calcPr{}/>", attrs_to_xml(&attrs));
            out.replace_range(calc_pr_start..=calc_pr_tag_end, &new_tag);
            return Ok(out);
        }
        let Some(calc_pr_close_start) = find_end_tag(&out, "calcPr", calc_pr_tag_end + 1) else {
            return Err(err("workbook.xml의 calcPr 종료 태그를 찾지 못했습니다."));
        };
        let calc_pr_close_end = calc_pr_close_start + "</calcPr>".len();
        let new_tag = format!("<calcPr{}></calcPr>", attrs_to_xml(&attrs));
        out.replace_range(calc_pr_start..calc_pr_close_end, &new_tag);
        return Ok(out);
    }
    let Some(workbook_close_start) = find_end_tag(&out, "workbook", 0) else {
        return Err(err("workbook.xml의 workbook 종료 태그를 찾지 못했습니다."));
    };
    let mut attrs = Vec::new();
    set_calc_pr_attrs(&mut attrs);
    let new_tag = format!("<calcPr{}/>", attrs_to_xml(&attrs));
    out.insert_str(workbook_close_start, &new_tag);
    Ok(out)
}
fn extend_conditional_formats_in_suffix(
    suffix: &str,
    last_data_row: u32,
    target_cols: &[u32],
    data_start_row: u32,
) -> Result<String> {
    let mut out = suffix.to_owned();
    let mut cursor = 0_usize;
    while let Some(cf_rel) = out
        .get(cursor..)
        .and_then(|tail| tail.find("<conditionalFormatting"))
    {
        let cf_start = cursor + cf_rel;
        let Some(cf_end_rel) = out.get(cf_start..).and_then(|tail| tail.find('>')) else {
            break;
        };
        let cf_end = cf_start + cf_end_rel + 1;
        let tag = out
            .get(cf_start..cf_end)
            .ok_or_else(|| err("conditionalFormatting 태그 범위가 손상되었습니다."))?
            .to_owned();
        let mut attrs = parse_tag_attrs(&tag)?;
        let Some(sqref) = get_attr(&attrs, "sqref").map(ToString::to_string) else {
            cursor = cf_end;
            continue;
        };
        let updated_sqref = extend_sqref_ranges(&sqref, last_data_row, target_cols, data_start_row);
        if updated_sqref == sqref {
            cursor = cf_end;
        } else {
            set_attr(&mut attrs, "sqref", updated_sqref);
            let new_tag = format!("<conditionalFormatting{}>", attrs_to_xml(&attrs));
            out.replace_range(cf_start..cf_end, &new_tag);
            cursor = cf_start + new_tag.len();
        }
    }
    Ok(out)
}
fn update_auto_filter_ref_in_suffix(
    suffix: &str,
    header_row: u32,
    last_data_row: u32,
) -> Result<String> {
    let mut out = suffix.to_owned();
    let mut cursor = 0_usize;
    let target_last_row = header_row.max(last_data_row);
    while let Some(auto_filter_rel) = out.get(cursor..).and_then(|tail| tail.find("<autoFilter")) {
        let auto_filter_start = cursor + auto_filter_rel;
        let Some(auto_filter_end_rel) =
            out.get(auto_filter_start..).and_then(|tail| tail.find('>'))
        else {
            return Err(err("worksheet XML의 autoFilter 태그가 손상되었습니다."));
        };
        let auto_filter_end = auto_filter_start + auto_filter_end_rel + 1;
        let tag = out
            .get(auto_filter_start..auto_filter_end)
            .ok_or_else(|| err("worksheet XML의 autoFilter 태그 범위가 손상되었습니다."))?
            .to_owned();
        let mut attrs = parse_tag_attrs(&tag)?;
        let Some(ref_value) = get_attr(&attrs, "ref").map(ToString::to_string) else {
            cursor = auto_filter_end;
            continue;
        };
        let (start_ref, end_ref) = parse_range_token(&ref_value);
        let Some((start_col, start_row, start_col_lock, start_row_lock)) =
            parse_ref_with_locks(&start_ref)
        else {
            return Err(err(format!(
                "autoFilter ref가 손상되었습니다. (ref={ref_value})"
            )));
        };
        let Some((end_col, end_row, end_col_lock, end_row_lock)) = parse_ref_with_locks(&end_ref)
        else {
            return Err(err(format!(
                "autoFilter ref가 손상되었습니다. (ref={ref_value})"
            )));
        };
        let (new_start_row, new_end_row) = if start_row <= end_row {
            (header_row, target_last_row)
        } else {
            (target_last_row, header_row)
        };
        let new_start = ref_with_locks(start_col, new_start_row, start_col_lock, start_row_lock);
        let new_end = ref_with_locks(end_col, new_end_row, end_col_lock, end_row_lock);
        let new_ref = format!("{new_start}:{new_end}");
        if new_ref == ref_value {
            cursor = auto_filter_end;
            continue;
        }
        set_attr(&mut attrs, "ref", new_ref);
        let new_tag = if tag.trim_end().ends_with("/>") {
            format!("<autoFilter{}/>", attrs_to_xml(&attrs))
        } else {
            format!("<autoFilter{}>", attrs_to_xml(&attrs))
        };
        out.replace_range(auto_filter_start..auto_filter_end, &new_tag);
        cursor = auto_filter_start + new_tag.len();
    }
    Ok(out)
}
fn extend_sqref_ranges(
    sqref: &str,
    last_data_row: u32,
    target_cols: &[u32],
    data_start_row: u32,
) -> String {
    let mut changed = false;
    let mut ranges_out = Vec::new();
    for token in sqref.split_whitespace() {
        let (start_ref, end_ref) = parse_range_token(token);
        let Some((start_col, start_row, start_col_lock, start_row_lock)) =
            parse_ref_with_locks(&start_ref)
        else {
            ranges_out.push(token.to_owned());
            continue;
        };
        let Some((end_col, end_row, end_col_lock, end_row_lock)) = parse_ref_with_locks(&end_ref)
        else {
            ranges_out.push(token.to_owned());
            continue;
        };
        let col_min = start_col.min(end_col);
        let col_max = start_col.max(end_col);
        let row_max = start_row.max(end_row);
        let overlaps_target_col = target_cols
            .iter()
            .any(|col| *col >= col_min && *col <= col_max);
        if !overlaps_target_col || row_max < data_start_row || row_max >= last_data_row {
            ranges_out.push(token.to_owned());
            continue;
        }
        let (new_start_row, new_end_row) = if start_row <= end_row {
            (start_row, last_data_row)
        } else {
            (last_data_row, end_row)
        };
        let new_start = ref_with_locks(start_col, new_start_row, start_col_lock, start_row_lock);
        let new_end = ref_with_locks(end_col, new_end_row, end_col_lock, end_row_lock);
        ranges_out.push(format!("{new_start}:{new_end}"));
        changed = true;
    }
    if changed {
        ranges_out.join(" ")
    } else {
        sqref.to_owned()
    }
}
fn parse_range_token(token: &str) -> (String, String) {
    if let Some((a, b)) = token.split_once(':') {
        (a.to_owned(), b.to_owned())
    } else {
        (token.to_owned(), token.to_owned())
    }
}
fn take_while_next_if_map(
    chars: &mut Peekable<Chars<'_>>,
    predicate: impl Fn(char) -> bool,
) -> String {
    from_fn(|| chars.next_if_map(|ch| if predicate(ch) { Ok(ch) } else { Err(ch) })).collect()
}
fn parse_ref_with_locks(r: &str) -> Option<(u32, u32, bool, bool)> {
    let mut chars = r.chars().peekable();
    let col_lock = chars.next_if_eq(&'$').is_some();
    let col_s = take_while_next_if_map(&mut chars, |ch| ch.is_ascii_alphabetic());
    if col_s.is_empty() {
        return None;
    }
    let row_lock = chars.next_if_eq(&'$').is_some();
    let row_s = take_while_next_if_map(&mut chars, |ch| ch.is_ascii_digit());
    if row_s.is_empty() || chars.next().is_some() {
        return None;
    }
    let col = name_to_col(&col_s)?;
    let row = row_s.parse::<u32>().ok()?;
    Some((col, row, col_lock, row_lock))
}
fn ref_with_locks(col: u32, row: u32, col_lock: bool, row_lock: bool) -> String {
    format!(
        "{}{}{}{}",
        if col_lock { "$" } else { "" },
        col_to_name(col),
        if row_lock { "$" } else { "" },
        row
    )
}
const fn is_cell_ref_start(ch: char) -> bool {
    ch == '$' || ch.is_ascii_alphabetic()
}
fn try_parse_quoted_sheet_prefix(chars: &[char], start: usize) -> Option<usize> {
    if chars.get(start) != Some(&'\'') {
        return None;
    }
    let mut i = start + 1;
    while let Some(&ch) = chars.get(i) {
        if ch == '\'' {
            if chars.get(i + 1) == Some(&'\'') {
                i += 2;
                continue;
            }
            if chars.get(i + 1) == Some(&'!') {
                return Some(i + 2);
            }
            return None;
        }
        i += 1;
    }
    None
}
fn parse_a1_col_index(col_text: &str) -> Option<u32> {
    if col_text.is_empty() || col_text.len() > 3 {
        return None;
    }
    let col = name_to_col(col_text)?;
    (1..=MAX_A1_COL).contains(&col).then_some(col)
}
const fn is_ref_neighbor_identifier(ch: char) -> bool {
    ch.is_ascii_alphanumeric() || ch == '_' || ch == '.'
}
const fn is_invalid_ref_suffix(ch: char) -> bool {
    matches!(ch, '!' | '\'' | '(' | '[')
}
fn try_parse_and_rewrite_cell_ref(
    chars: &[char],
    start: usize,
    resolver: &dyn Fn(u32) -> u32,
) -> Option<(usize, String)> {
    let mut i = start;
    let mut col_lock = false;
    if chars.get(i) == Some(&'$') {
        col_lock = true;
        i += 1;
    }
    let col_start = i;
    while matches!(chars.get(i), Some(ch) if ch.is_ascii_alphabetic()) {
        i += 1;
    }
    if i == col_start {
        return None;
    }
    let col_text: String = chars
        .iter()
        .skip(col_start)
        .take(i - col_start)
        .copied()
        .collect();
    parse_a1_col_index(&col_text)?;
    let mut row_lock = false;
    if chars.get(i) == Some(&'$') {
        row_lock = true;
        i += 1;
    }
    let row_start = i;
    while matches!(chars.get(i), Some(ch) if ch.is_ascii_digit()) {
        i += 1;
    }
    if i == row_start {
        return None;
    }
    let prev = start.checked_sub(1).and_then(|idx| chars.get(idx)).copied();
    if matches!(prev, Some(ch) if is_ref_neighbor_identifier(ch)) {
        return None;
    }
    let next = chars.get(i).copied();
    if matches!(next, Some(ch) if is_ref_neighbor_identifier(ch) || is_invalid_ref_suffix(ch)) {
        return None;
    }
    let row_text: String = chars
        .iter()
        .skip(row_start)
        .take(i - row_start)
        .copied()
        .collect();
    let old_row = row_text.parse::<u32>().ok()?;
    if !(1..=MAX_A1_ROW).contains(&old_row) {
        return None;
    }
    let new_row = resolver(old_row);
    if !(1..=MAX_A1_ROW).contains(&new_row) {
        return None;
    }
    let replaced = format!(
        "{}{}{}{}",
        if col_lock { "$" } else { "" },
        col_text,
        if row_lock { "$" } else { "" },
        new_row
    );
    Some((i, replaced))
}
fn try_parse_and_shift_cell_ref(
    chars: &[char],
    start: usize,
    delta_col: i64,
    delta_row: i64,
) -> Result<Option<(usize, String)>> {
    let mut i = start;
    let mut col_lock = false;
    if chars.get(i) == Some(&'$') {
        col_lock = true;
        i += 1;
    }
    let col_start = i;
    while matches!(chars.get(i), Some(ch) if ch.is_ascii_alphabetic()) {
        i += 1;
    }
    if i == col_start {
        return Ok(None);
    }
    let col_text: String = chars
        .iter()
        .skip(col_start)
        .take(i - col_start)
        .copied()
        .collect();
    let Some(old_col) = parse_a1_col_index(&col_text) else {
        return Ok(None);
    };
    let mut row_lock = false;
    if chars.get(i) == Some(&'$') {
        row_lock = true;
        i += 1;
    }
    let row_start = i;
    while matches!(chars.get(i), Some(ch) if ch.is_ascii_digit()) {
        i += 1;
    }
    if i == row_start {
        return Ok(None);
    }
    let prev = start.checked_sub(1).and_then(|idx| chars.get(idx)).copied();
    if matches!(prev, Some(ch) if is_ref_neighbor_identifier(ch)) {
        return Ok(None);
    }
    let next = chars.get(i).copied();
    if matches!(next, Some(ch) if is_ref_neighbor_identifier(ch) || is_invalid_ref_suffix(ch)) {
        return Ok(None);
    }
    let row_text: String = chars
        .iter()
        .skip(row_start)
        .take(i - row_start)
        .copied()
        .collect();
    let Ok(old_row) = row_text.parse::<u32>() else {
        return Ok(None);
    };
    if !(1..=MAX_A1_ROW).contains(&old_row) {
        return Ok(None);
    }
    let new_col = if col_lock {
        old_col
    } else {
        shift_formula_index(old_col, delta_col, MAX_A1_COL)?
    };
    let new_row = if row_lock {
        old_row
    } else {
        shift_formula_index(old_row, delta_row, MAX_A1_ROW)?
    };
    let replaced = ref_with_locks(new_col, new_row, col_lock, row_lock);
    Ok(Some((i, replaced)))
}
fn shift_formula_index(value: u32, delta: i64, max: u32) -> Result<u32> {
    let shifted = i64::from(value) + delta;
    if !(1..=i64::from(max)).contains(&shifted) {
        return Err(err(format!(
            "shared formula 상대참조 이동 범위를 벗어났습니다. ({value} + {delta}, max={max})"
        )));
    }
    u32::try_from(shifted)
        .map_err(|source| err(format!("shared formula index 변환 실패: {source}")))
}
