use super::{
    ooxml::{load_shared_strings, load_sheet_catalog, load_sheet_xml},
    xlsx_container::XlsxContainer,
};
use crate::{Result, err, parse_i32_str};
use std::collections::{BTreeMap, HashMap};
use std::path::Path;
#[derive(Debug)]
pub struct Workbook {
    container: XlsxContainer,
    workbook_xml: String,
    shared_strings: Vec<String>,
    sheet_paths: HashMap<String, String>,
    sheets: HashMap<String, Worksheet>,
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
impl Workbook {
    pub fn open(path: &Path) -> Result<Self> {
        let container = XlsxContainer::open_for_update(path)?;
        let catalog = load_sheet_catalog(&container)?;
        let workbook_xml = container.read_text("xl/workbook.xml")?;
        let shared_strings = load_shared_strings(&container)?;
        let mut sheet_paths = HashMap::new();
        let mut sheets = HashMap::new();
        for sheet_name in &catalog.sheet_order {
            let Some(sheet_path) = catalog.sheet_name_to_path.get(sheet_name) else {
                continue;
            };
            let xml = load_sheet_xml(&container, &catalog, sheet_name)?;
            let sheet = Worksheet::parse(&xml)?;
            sheet_paths.insert(sheet_name.clone(), sheet_path.clone());
            sheets.insert(sheet_name.clone(), sheet);
        }
        Ok(Self {
            container,
            workbook_xml,
            shared_strings,
            sheet_paths,
            sheets,
        })
    }
    pub fn shared_strings(&self) -> &[String] {
        &self.shared_strings
    }
    pub fn workbook_xml(&self) -> &str {
        &self.workbook_xml
    }
    pub fn workbook_xml_mut(&mut self) -> &mut String {
        &mut self.workbook_xml
    }
    pub fn sheet(&self, name: &str) -> Option<&Worksheet> {
        self.sheets.get(name)
    }
    pub fn sheet_mut(&mut self, name: &str) -> Option<&mut Worksheet> {
        self.sheets.get_mut(name)
    }
    pub fn save_as(&mut self, out_path: &Path) -> Result<()> {
        self.container
            .write_text("xl/workbook.xml", &self.workbook_xml)?;
        for (sheet_name, sheet) in &self.sheets {
            let Some(path) = self.sheet_paths.get(sheet_name) else {
                continue;
            };
            self.container.write_text(path, &sheet.to_xml())?;
        }
        self.container.save_as(out_path)
    }
}
impl Worksheet {
    pub fn parse(xml: &str) -> Result<Self> {
        let Some(sheet_data_open) = xml.find("<sheetData") else {
            return Err(err("worksheet XML에 <sheetData>가 없습니다."));
        };
        let Some(sheet_data_open_end_rel) = xml[sheet_data_open..].find('>') else {
            return Err(err(
                "worksheet XML의 <sheetData> 시작 태그가 손상되었습니다.",
            ));
        };
        let sheet_data_open_end = sheet_data_open + sheet_data_open_end_rel + 1;
        let Some(sheet_data_close_rel) = xml[sheet_data_open_end..].find("</sheetData>") else {
            return Err(err("worksheet XML에 </sheetData>가 없습니다."));
        };
        let sheet_data_close = sheet_data_open_end + sheet_data_close_rel;
        let prefix = xml[..sheet_data_open_end].to_string();
        let body = &xml[sheet_data_open_end..sheet_data_close];
        let suffix = xml[sheet_data_close..].to_string();
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
        set_attr(&mut cell.attrs, "t", "inlineStr".to_string());
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
        match value {
            Some(v) => {
                remove_attr(&mut cell.attrs, "t");
                cell.inner_xml = Some(format!("<v>{v}</v>"));
            }
            None => {
                remove_attr(&mut cell.attrs, "t");
                cell.inner_xml = None;
            }
        }
    }
    pub fn set_formula_at(&mut self, col: u32, row: u32, formula: &str) {
        let cell = self.get_or_create_cell_mut(col, row);
        let formula_text = xml_escape_text(formula);
        if let Some(inner) = cell.inner_xml.as_mut() {
            if replace_first_tag_text(inner, "f", &formula_text) {
                if !inner.contains("<v") {
                    inner.push_str("<v></v>");
                }
            } else {
                *inner = format!("<f>{formula_text}</f><v></v>");
            }
        } else {
            cell.inner_xml = Some(format!("<f>{formula_text}</f><v></v>"));
        }
    }
    pub fn set_blank_at(&mut self, col: u32, row: u32) {
        let cell = self.get_or_create_cell_mut(col, row);
        remove_attr(&mut cell.attrs, "t");
        cell.inner_xml = None;
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
        let mut cloned = src.clone();
        remap_row_numbers(&mut cloned, target_row, &|r| {
            if r == source_row { target_row } else { r }
        });
        cloned.cells.retain(|col, _| *col <= max_col);
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
    pub fn update_dimension(&mut self) {
        let max_row = self.max_row_num();
        let max_col = self.max_cell_col();
        let end_ref = format!("{}{}", col_to_name(max_col), max_row);
        self.prefix = update_dimension_ref_xml(&self.prefix, "A1", &end_ref);
    }
    pub fn extend_conditional_formats(
        &mut self,
        last_data_row: u32,
        target_cols: &[u32],
        data_start_row: u32,
    ) {
        if target_cols.is_empty() {
            return;
        }
        self.suffix = extend_conditional_formats_in_suffix(
            &self.suffix,
            last_data_row,
            target_cols,
            data_start_row,
        );
    }
    pub fn get_or_create_cell_mut(&mut self, col: u32, row: u32) -> &mut Cell {
        let row_obj = self.rows.entry(row).or_insert_with(|| Row {
            attrs: vec![("r".to_string(), row.to_string())],
            cells: BTreeMap::new(),
        });
        if get_attr(&row_obj.attrs, "r").is_none() {
            set_attr(&mut row_obj.attrs, "r", row.to_string());
        }
        row_obj.cells.entry(col).or_insert_with(|| Cell {
            attrs: vec![
                ("r".to_string(), format!("{}{}", col_to_name(col), row)),
                ("s".to_string(), "0".to_string()),
            ],
            inner_xml: None,
        })
    }
}
pub fn remap_row_numbers(row: &mut Row, new_row: u32, resolver: &dyn Fn(u32) -> u32) {
    set_attr(&mut row.attrs, "r", new_row.to_string());
    let mut new_cells = BTreeMap::new();
    for (col, mut cell) in row.cells.clone() {
        set_attr(
            &mut cell.attrs,
            "r",
            format!("{}{}", col_to_name(col), new_row),
        );
        if let Some(inner) = cell.inner_xml.as_mut() {
            *inner = rewrite_formula_rows_in_inner(inner, resolver);
        }
        new_cells.insert(col, cell);
    }
    row.cells = new_cells;
}
pub fn rewrite_formula_rows(formula: &str, resolver: &dyn Fn(u32) -> u32) -> String {
    let chars: Vec<char> = formula.chars().collect();
    let mut i = 0usize;
    let mut out = String::with_capacity(formula.len());
    while i < chars.len() {
        let ch = chars[i];
        if is_cell_ref_start(ch)
            && let Some((end_idx, replaced)) = try_parse_and_rewrite_cell_ref(&chars, i, resolver)
        {
            out.push_str(&replaced);
            i = end_idx;
            continue;
        }
        out.push(ch);
        i += 1;
    }
    out
}
pub fn col_to_name(mut col: u32) -> String {
    if col == 0 {
        return "A".to_string();
    }
    let mut out = String::new();
    while col > 0 {
        let rem = ((col - 1) % 26) as u8;
        out.insert(0, (b'A' + rem) as char);
        col = (col - 1) / 26;
    }
    out
}
pub fn name_to_col(name: &str) -> Option<u32> {
    let mut out = 0u32;
    if name.is_empty() {
        return None;
    }
    for ch in name.chars() {
        if !ch.is_ascii_alphabetic() {
            return None;
        }
        let upper = ch.to_ascii_uppercase() as u8;
        out = out
            .saturating_mul(26)
            .saturating_add(u32::from(upper - b'A' + 1));
    }
    Some(out)
}
fn parse_rows_from_sheet_data(body: &str) -> Result<BTreeMap<u32, Row>> {
    let mut rows = BTreeMap::new();
    let mut cursor = 0usize;
    while let Some(row_open_rel) = body[cursor..].find("<row") {
        let row_open = cursor + row_open_rel;
        let Some(row_tag_end_rel) = body[row_open..].find('>') else {
            break;
        };
        let row_tag_end = row_open + row_tag_end_rel;
        let row_tag = &body[row_open..=row_tag_end];
        let mut row_attrs = parse_tag_attrs(row_tag);
        let row_num = get_attr(&row_attrs, "r")
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(rows.keys().last().copied().unwrap_or(0) + 1);
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
        let Some(row_close_rel) = body[row_body_start..].find("</row>") else {
            break;
        };
        let row_body_end = row_body_start + row_close_rel;
        let row_body = &body[row_body_start..row_body_end];
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
    let mut cursor = 0usize;
    let mut next_col = 1u32;
    while let Some(cell_open_rel) = row_body[cursor..].find("<c") {
        let cell_open = cursor + cell_open_rel;
        let Some(cell_tag_end_rel) = row_body[cell_open..].find('>') else {
            break;
        };
        let cell_tag_end = cell_open + cell_tag_end_rel;
        let cell_tag = &row_body[cell_open..=cell_tag_end];
        let mut attrs = parse_tag_attrs(cell_tag);
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
        let Some(cell_close_rel) = row_body[cell_body_start..].find("</c>") else {
            break;
        };
        let cell_body_end = cell_body_start + cell_close_rel;
        let inner_xml = row_body[cell_body_start..cell_body_end].to_string();
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
    if let Some(inner) = &cell.inner_xml {
        out.push('>');
        out.push_str(inner);
        out.push_str("</c>");
    } else {
        out.push_str("/>");
    }
    out
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
    for (name, value) in attrs {
        out.push(' ');
        out.push_str(name);
        out.push_str("=\"");
        out.push_str(&xml_escape_attr(value));
        out.push('"');
    }
    out
}
fn parse_tag_attrs(tag: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let Some(lt) = tag.find('<') else {
        return out;
    };
    let mut i = lt + 1;
    let bytes = tag.as_bytes();
    while i < bytes.len() && !bytes[i].is_ascii_whitespace() && bytes[i] != b'>' && bytes[i] != b'/'
    {
        i += 1;
    }
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] == b'>' || bytes[i] == b'/' {
            break;
        }
        let key_start = i;
        while i < bytes.len()
            && !bytes[i].is_ascii_whitespace()
            && bytes[i] != b'='
            && bytes[i] != b'>'
            && bytes[i] != b'/'
        {
            i += 1;
        }
        let key_end = i;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || bytes[i] != b'=' {
            break;
        }
        i += 1;
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i >= bytes.len() || (bytes[i] != b'"' && bytes[i] != b'\'') {
            break;
        }
        let quote = bytes[i];
        i += 1;
        let value_start = i;
        while i < bytes.len() && bytes[i] != quote {
            i += 1;
        }
        if i > bytes.len() {
            break;
        }
        let key = &tag[key_start..key_end];
        let value = decode_xml_entities(&tag[value_start..i]);
        out.push((key.to_string(), value));
        if i < bytes.len() {
            i += 1;
        }
    }
    out
}
fn get_attr<'a>(attrs: &'a [(String, String)], name: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find_map(|(k, v)| if k == name { Some(v.as_str()) } else { None })
}
fn set_attr(attrs: &mut Vec<(String, String)>, name: &str, value: String) {
    for (k, v) in attrs.iter_mut() {
        if k == name {
            *v = value;
            return;
        }
    }
    attrs.push((name.to_string(), value));
}
fn remove_attr(attrs: &mut Vec<(String, String)>, name: &str) {
    attrs.retain(|(k, _)| k != name);
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
    let inner = cell.inner_xml.as_deref().unwrap_or("");
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
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        });
    }
    Some(decoded)
}
fn rewrite_formula_rows_in_inner(inner_xml: &str, resolver: &dyn Fn(u32) -> u32) -> String {
    let mut out = inner_xml.to_string();
    if let Some(text) = extract_first_tag_text(&out, "f") {
        let rewritten = rewrite_formula_rows(&decode_xml_entities(&text), resolver);
        let encoded = xml_escape_text(&rewritten);
        let _ = replace_first_tag_text(&mut out, "f", &encoded);
    }
    out
}
fn replace_first_tag_text(xml: &mut String, tag_name: &str, new_text: &str) -> bool {
    let open_pattern = format!("<{tag_name}");
    let Some(open_start) = xml.find(&open_pattern) else {
        return false;
    };
    let Some(open_end_rel) = xml[open_start..].find('>') else {
        return false;
    };
    let content_start = open_start + open_end_rel + 1;
    let close_pattern = format!("</{tag_name}>");
    let Some(close_rel) = xml[content_start..].find(&close_pattern) else {
        return false;
    };
    let close = content_start + close_rel;
    xml.replace_range(content_start..close, new_text);
    true
}
fn extract_first_tag_text(xml: &str, tag_name: &str) -> Option<String> {
    let open_pattern = format!("<{tag_name}");
    let open_start = xml.find(&open_pattern)?;
    let open_end = open_start + xml[open_start..].find('>')?;
    let body_start = open_end + 1;
    let close_pattern = format!("</{tag_name}>");
    let close_rel = xml[body_start..].find(&close_pattern)?;
    let body_end = body_start + close_rel;
    Some(xml[body_start..body_end].to_string())
}
fn extract_all_tag_text(xml: &str, tag_name: &str) -> Option<String> {
    let open_pattern = format!("<{tag_name}");
    let close_pattern = format!("</{tag_name}>");
    let mut cursor = 0usize;
    let mut out = String::new();
    while let Some(open_rel) = xml[cursor..].find(&open_pattern) {
        let open_start = cursor + open_rel;
        let open_end = open_start + xml[open_start..].find('>')?;
        let body_start = open_end + 1;
        let close_rel = xml[body_start..].find(&close_pattern)?;
        let body_end = body_start + close_rel;
        out.push_str(&xml[body_start..body_end]);
        cursor = body_end + close_pattern.len();
    }
    if out.is_empty() { None } else { Some(out) }
}
fn decode_xml_entities(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
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
fn update_dimension_ref_xml(prefix_xml: &str, start_ref: &str, end_ref: &str) -> String {
    let mut out = prefix_xml.to_string();
    if let Some(dim_pos) = out.find("<dimension")
        && let Some(dim_end_rel) = out[dim_pos..].find('>')
    {
        let dim_end = dim_pos + dim_end_rel + 1;
        let tag = &out[dim_pos..dim_end];
        let mut attrs = parse_tag_attrs(tag);
        set_attr(&mut attrs, "ref", format!("{start_ref}:{end_ref}"));
        let new_tag = format!("<dimension{}/>", attrs_to_xml(&attrs));
        out.replace_range(dim_pos..dim_end, &new_tag);
        return out;
    }
    out
}
fn extend_conditional_formats_in_suffix(
    suffix: &str,
    last_data_row: u32,
    target_cols: &[u32],
    data_start_row: u32,
) -> String {
    let mut out = suffix.to_string();
    let mut cursor = 0usize;
    while let Some(cf_rel) = out[cursor..].find("<conditionalFormatting") {
        let cf_start = cursor + cf_rel;
        let Some(cf_end_rel) = out[cf_start..].find('>') else {
            break;
        };
        let cf_end = cf_start + cf_end_rel + 1;
        let tag = out[cf_start..cf_end].to_string();
        let mut attrs = parse_tag_attrs(&tag);
        let Some(sqref) = get_attr(&attrs, "sqref").map(|v| v.to_string()) else {
            cursor = cf_end;
            continue;
        };
        let updated_sqref = extend_sqref_ranges(&sqref, last_data_row, target_cols, data_start_row);
        if updated_sqref != sqref {
            set_attr(&mut attrs, "sqref", updated_sqref);
            let new_tag = format!("<conditionalFormatting{}>", attrs_to_xml(&attrs));
            out.replace_range(cf_start..cf_end, &new_tag);
            cursor = cf_start + new_tag.len();
        } else {
            cursor = cf_end;
        }
    }
    out
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
        let Some((start_ref, end_ref)) = parse_range_token(token) else {
            ranges_out.push(token.to_string());
            continue;
        };
        let Some((start_col, start_row, start_col_lock, start_row_lock)) =
            parse_ref_with_locks(&start_ref)
        else {
            ranges_out.push(token.to_string());
            continue;
        };
        let Some((end_col, end_row, end_col_lock, end_row_lock)) = parse_ref_with_locks(&end_ref)
        else {
            ranges_out.push(token.to_string());
            continue;
        };
        let col_min = start_col.min(end_col);
        let col_max = start_col.max(end_col);
        let row_max = start_row.max(end_row);
        let overlaps_target_col = target_cols
            .iter()
            .any(|col| *col >= col_min && *col <= col_max);
        if !overlaps_target_col || row_max < data_start_row || row_max >= last_data_row {
            ranges_out.push(token.to_string());
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
        sqref.to_string()
    }
}
fn parse_range_token(token: &str) -> Option<(String, String)> {
    if let Some((a, b)) = token.split_once(':') {
        Some((a.to_string(), b.to_string()))
    } else {
        Some((token.to_string(), token.to_string()))
    }
}
fn parse_ref_with_locks(r: &str) -> Option<(u32, u32, bool, bool)> {
    let mut chars = r.chars().peekable();
    let col_lock = if chars.peek() == Some(&'$') {
        chars.next();
        true
    } else {
        false
    };
    let mut col_s = String::new();
    while let Some(ch) = chars.peek() {
        if ch.is_ascii_alphabetic() {
            col_s.push(*ch);
            chars.next();
        } else {
            break;
        }
    }
    if col_s.is_empty() {
        return None;
    }
    let row_lock = if chars.peek() == Some(&'$') {
        chars.next();
        true
    } else {
        false
    };
    let mut row_s = String::new();
    while let Some(ch) = chars.peek() {
        if ch.is_ascii_digit() {
            row_s.push(*ch);
            chars.next();
        } else {
            break;
        }
    }
    if row_s.is_empty() || chars.peek().is_some() {
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
fn is_cell_ref_start(ch: char) -> bool {
    ch == '$' || ch.is_ascii_alphabetic()
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
    while i < chars.len() && chars[i].is_ascii_alphabetic() {
        i += 1;
    }
    if i == col_start {
        return None;
    }
    let col_text: String = chars[col_start..i].iter().collect();
    let _ = name_to_col(&col_text)?;
    let mut row_lock = false;
    if i < chars.len() && chars[i] == '$' {
        row_lock = true;
        i += 1;
    }
    let row_start = i;
    while i < chars.len() && chars[i].is_ascii_digit() {
        i += 1;
    }
    if i == row_start {
        return None;
    }
    let prev = if start > 0 {
        Some(chars[start - 1])
    } else {
        None
    };
    if matches!(prev, Some(ch) if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.') {
        return None;
    }
    let next = chars.get(i).copied();
    if matches!(next, Some(ch) if ch.is_ascii_alphanumeric() || ch == '_' || ch == '.') {
        return None;
    }
    let row_text: String = chars[row_start..i].iter().collect();
    let old_row = row_text.parse::<u32>().ok()?;
    let new_row = resolver(old_row);
    let replaced = format!(
        "{}{}{}{}",
        if col_lock { "$" } else { "" },
        col_text,
        if row_lock { "$" } else { "" },
        new_row
    );
    Some((i, replaced))
}
