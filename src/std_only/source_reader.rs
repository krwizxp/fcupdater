use super::{
    ooxml::{load_shared_strings, load_sheet_catalog, load_sheet_xml},
    xlsx_container::XlsxContainer,
};
use crate::{Result, SourceRecord, canon_header, err, parse_i32_str};
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::Path,
};
#[derive(Debug, Clone, PartialEq)]
enum CellValue {
    Empty,
    Text(String),
    Number(f64),
}
impl CellValue {
    fn as_string(&self) -> String {
        match self {
            Self::Empty => String::new(),
            Self::Text(v) => v.trim().to_string(),
            Self::Number(v) => format_number(*v),
        }
    }
    fn as_i32(&self) -> Option<i32> {
        match self {
            Self::Empty => None,
            Self::Number(v) => Some(v.round() as i32),
            Self::Text(v) => parse_i32_str(v),
        }
    }
}
pub fn read_source_file(path: &Path) -> Result<Vec<SourceRecord>> {
    let ext = path
        .extension()
        .and_then(|v| v.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    match ext.as_str() {
        "xlsx" => read_xlsx_source(path),
        "xls" => read_xls_source(path),
        _ => Err(err(format!(
            "지원하지 않는 소스 확장자입니다: {}",
            path.display()
        ))),
    }
}
fn read_xlsx_source(path: &Path) -> Result<Vec<SourceRecord>> {
    let container = XlsxContainer::open_for_update(path)?;
    let catalog = load_sheet_catalog(&container)?;
    let first_sheet = catalog
        .sheet_order
        .first()
        .ok_or_else(|| err("xlsx에 시트가 없습니다."))?;
    let sheet_xml = load_sheet_xml(&container, &catalog, first_sheet)?;
    let shared_strings = load_shared_strings(&container)?;
    let rows = parse_xlsx_rows(&sheet_xml, &shared_strings);
    build_source_records_from_rows(&rows)
}
fn parse_xlsx_rows(sheet_xml: &str, shared_strings: &[String]) -> Vec<Vec<CellValue>> {
    let mut out: Vec<Vec<CellValue>> = Vec::new();
    let mut cursor = 0usize;
    while let Some(row_open_rel) = sheet_xml[cursor..].find("<row") {
        let row_open = cursor + row_open_rel;
        let Some(row_tag_end_rel) = sheet_xml[row_open..].find('>') else {
            break;
        };
        let row_tag_end = row_open + row_tag_end_rel;
        let row_tag = &sheet_xml[row_open..=row_tag_end];
        let row_num = parse_row_number(row_tag).unwrap_or(out.len() as u32 + 1);
        if row_num == 0 {
            cursor = row_tag_end + 1;
            continue;
        }
        while out.len() < row_num as usize {
            out.push(Vec::new());
        }
        if row_tag.ends_with("/>") {
            cursor = row_tag_end + 1;
            continue;
        }
        let row_body_start = row_tag_end + 1;
        let Some(row_close_rel) = sheet_xml[row_body_start..].find("</row>") else {
            break;
        };
        let row_body_end = row_body_start + row_close_rel;
        let row_body = &sheet_xml[row_body_start..row_body_end];
        out[row_num as usize - 1] = parse_xlsx_row_cells(row_body, shared_strings);
        cursor = row_body_end + "</row>".len();
    }
    out
}
fn parse_row_number(row_tag: &str) -> Option<u32> {
    let value = extract_attr(row_tag, "r")?;
    value.parse::<u32>().ok()
}
fn parse_xlsx_row_cells(row_xml: &str, shared_strings: &[String]) -> Vec<CellValue> {
    let mut row_cells: Vec<CellValue> = Vec::new();
    let mut cursor = 0usize;
    let mut next_col = 0usize;
    while let Some(cell_open_rel) = row_xml[cursor..].find("<c") {
        let cell_open = cursor + cell_open_rel;
        let Some(cell_tag_end_rel) = row_xml[cell_open..].find('>') else {
            break;
        };
        let cell_tag_end = cell_open + cell_tag_end_rel;
        let cell_tag = &row_xml[cell_open..=cell_tag_end];
        let col_index = extract_attr(cell_tag, "r")
            .as_deref()
            .and_then(cell_ref_to_col_index)
            .unwrap_or(next_col);
        if row_cells.len() <= col_index {
            row_cells.resize(col_index + 1, CellValue::Empty);
        }
        if cell_tag.ends_with("/>") {
            row_cells[col_index] = CellValue::Empty;
            next_col = col_index + 1;
            cursor = cell_tag_end + 1;
            continue;
        }
        let cell_body_start = cell_tag_end + 1;
        let Some(cell_close_rel) = row_xml[cell_body_start..].find("</c>") else {
            break;
        };
        let cell_body_end = cell_body_start + cell_close_rel;
        let cell_body = &row_xml[cell_body_start..cell_body_end];
        row_cells[col_index] = parse_xlsx_cell_value(cell_tag, cell_body, shared_strings);
        next_col = col_index + 1;
        cursor = cell_body_end + "</c>".len();
    }
    row_cells
}
fn parse_xlsx_cell_value(cell_tag: &str, cell_body: &str, shared_strings: &[String]) -> CellValue {
    let cell_type = extract_attr(cell_tag, "t");
    if matches!(cell_type.as_deref(), Some("inlineStr"))
        && let Some(v) = extract_all_tag_text(cell_body, "t")
    {
        return CellValue::Text(decode_xml_entities(&v));
    }
    let Some(v_raw) = extract_first_tag_text(cell_body, "v") else {
        return CellValue::Empty;
    };
    let decoded = decode_xml_entities(&v_raw);
    if matches!(cell_type.as_deref(), Some("s")) {
        if let Ok(idx) = decoded.parse::<usize>()
            && let Some(v) = shared_strings.get(idx)
        {
            return CellValue::Text(v.clone());
        }
        return CellValue::Text(decoded);
    }
    if matches!(cell_type.as_deref(), Some("b")) {
        return CellValue::Text(if decoded == "1" {
            "TRUE".to_string()
        } else {
            "FALSE".to_string()
        });
    }
    if matches!(cell_type.as_deref(), Some("str")) {
        return CellValue::Text(decoded);
    }
    if let Ok(n) = decoded.parse::<f64>() {
        return CellValue::Number(n);
    }
    CellValue::Text(decoded)
}
fn cell_ref_to_col_index(cell_ref: &str) -> Option<usize> {
    let mut col = 0usize;
    let mut has_alpha = false;
    for ch in cell_ref.chars() {
        if ch.is_ascii_alphabetic() {
            has_alpha = true;
            let upper = ch.to_ascii_uppercase() as u8;
            if !upper.is_ascii_uppercase() {
                return None;
            }
            col = col * 26 + usize::from(upper - b'A' + 1);
        } else {
            break;
        }
    }
    if has_alpha {
        Some(col.saturating_sub(1))
    } else {
        None
    }
}
fn extract_attr(tag: &str, attr_name: &str) -> Option<String> {
    let pattern = format!("{attr_name}=");
    let mut cursor = 0usize;
    while let Some(rel) = tag[cursor..].find(&pattern) {
        let idx = cursor + rel;
        if idx > 0
            && !tag.as_bytes()[idx - 1].is_ascii_whitespace()
            && tag.as_bytes()[idx - 1] != b'<'
        {
            cursor = idx + pattern.len();
            continue;
        }
        let quote_idx = idx + pattern.len();
        let quote = *tag.as_bytes().get(quote_idx)?;
        if quote != b'"' && quote != b'\'' {
            cursor = quote_idx;
            continue;
        }
        let value_start = quote_idx + 1;
        let value_end_rel = tag[value_start..].find(char::from(quote))?;
        let value_end = value_start + value_end_rel;
        return Some(decode_xml_entities(&tag[value_start..value_end]));
    }
    None
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
    let mut out = String::new();
    let mut cursor = 0usize;
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
fn read_xls_source(path: &Path) -> Result<Vec<SourceRecord>> {
    let cfb = CfbFile::open(path)?;
    let workbook = match cfb.read_stream_by_name("Workbook") {
        Ok(v) => v,
        Err(_) => cfb.read_stream_by_name("Book")?,
    };
    let rows = parse_biff_first_sheet_rows(&workbook)?;
    build_source_records_from_rows(&rows)
}
const CFB_SIGNATURE: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
const CFB_FREE_SECT: u32 = 0xFFFF_FFFF;
const CFB_END_OF_CHAIN: u32 = 0xFFFF_FFFE;
const CFB_FAT_SECT: u32 = 0xFFFF_FFFD;
const CFB_DIFAT_SECT: u32 = 0xFFFF_FFFC;
#[derive(Debug, Clone)]
struct CfbDirectoryEntry {
    name: String,
    object_type: u8,
    start_sector: u32,
    stream_size: u64,
}
#[derive(Debug)]
struct CfbFile {
    data: Vec<u8>,
    sector_size: usize,
    mini_sector_size: usize,
    mini_stream_cutoff_size: u32,
    fat: Vec<u32>,
    mini_fat: Vec<u32>,
    root_stream: Vec<u8>,
    directory: Vec<CfbDirectoryEntry>,
}
impl CfbFile {
    fn open(path: &Path) -> Result<Self> {
        let data = fs::read(path)
            .map_err(|e| err(format!("xls 파일 읽기 실패: {} ({e})", path.display())))?;
        if data.len() < 512 || data[0..8] != CFB_SIGNATURE {
            return Err(err(format!(
                "유효한 OLE2(CFB) xls 파일이 아닙니다: {}",
                path.display()
            )));
        }
        let major_version = read_u16_le(&data, 0x1A)?;
        let sector_shift = read_u16_le(&data, 0x1E)?;
        let mini_sector_shift = read_u16_le(&data, 0x20)?;
        let sector_size = 1usize << sector_shift;
        let mini_sector_size = 1usize << mini_sector_shift;
        if sector_size < 512 || (sector_size & (sector_size - 1)) != 0 {
            return Err(err(format!("지원하지 않는 CFB sector size: {sector_size}")));
        }
        let num_fat_sectors = read_u32_le(&data, 0x2C)?;
        let first_dir_sector = read_u32_le(&data, 0x30)?;
        let mini_stream_cutoff_size = read_u32_le(&data, 0x38)?;
        let first_mini_fat_sector = read_u32_le(&data, 0x3C)?;
        let num_mini_fat_sectors = read_u32_le(&data, 0x40)?;
        let first_difat_sector = read_u32_le(&data, 0x44)?;
        let num_difat_sectors = read_u32_le(&data, 0x48)?;
        let mut difat_entries: Vec<u32> = Vec::new();
        for i in 0..109usize {
            let sid = read_u32_le(&data, 0x4C + i * 4)?;
            if is_regular_sector_id(sid) {
                difat_entries.push(sid);
            }
        }
        if num_difat_sectors > 0 {
            let mut sid = first_difat_sector;
            let mut seen: HashSet<u32> = HashSet::new();
            for _ in 0..num_difat_sectors {
                if !is_regular_sector_id(sid) {
                    break;
                }
                if !seen.insert(sid) {
                    break;
                }
                let sector = get_sector_slice(&data, sector_size, sid)?;
                let entries_per_sector = sector_size / 4 - 1;
                for idx in 0..entries_per_sector {
                    let entry = read_u32_le(sector, idx * 4)?;
                    if is_regular_sector_id(entry) {
                        difat_entries.push(entry);
                    }
                }
                sid = read_u32_le(sector, entries_per_sector * 4)?;
            }
        }
        let fat_sector_ids: Vec<u32> = difat_entries
            .into_iter()
            .take(num_fat_sectors as usize)
            .collect();
        if fat_sector_ids.is_empty() {
            return Err(err("CFB FAT 정보를 찾지 못했습니다."));
        }
        let mut fat: Vec<u32> = Vec::new();
        for sid in fat_sector_ids {
            let sector = get_sector_slice(&data, sector_size, sid)?;
            for i in 0..(sector_size / 4) {
                fat.push(read_u32_le(sector, i * 4)?);
            }
        }
        let dir_stream = read_stream_from_fat_chain(
            &data,
            sector_size,
            &fat,
            first_dir_sector,
            None,
            "CFB 디렉터리",
        )?;
        let directory = parse_directory_entries(&dir_stream, major_version)?;
        let root_entry = directory
            .iter()
            .find(|v| v.object_type == 5)
            .ok_or_else(|| err("CFB root entry를 찾지 못했습니다."))?;
        let root_stream = read_stream_from_fat_chain(
            &data,
            sector_size,
            &fat,
            root_entry.start_sector,
            Some(root_entry.stream_size),
            "CFB root stream",
        )?;
        let mini_fat = if num_mini_fat_sectors > 0 && is_regular_sector_id(first_mini_fat_sector) {
            let mini_fat_bytes = read_stream_from_fat_chain(
                &data,
                sector_size,
                &fat,
                first_mini_fat_sector,
                Some(u64::from(num_mini_fat_sectors) * sector_size as u64),
                "CFB mini FAT",
            )?;
            let mut out = Vec::new();
            let mut idx = 0usize;
            while idx + 4 <= mini_fat_bytes.len() {
                out.push(read_u32_le(&mini_fat_bytes, idx)?);
                idx += 4;
            }
            out
        } else {
            Vec::new()
        };
        Ok(Self {
            data,
            sector_size,
            mini_sector_size,
            mini_stream_cutoff_size,
            fat,
            mini_fat,
            root_stream,
            directory,
        })
    }
    fn read_stream_by_name(&self, name: &str) -> Result<Vec<u8>> {
        let entry = self
            .directory
            .iter()
            .find(|v| v.object_type == 2 && v.name == name)
            .ok_or_else(|| err(format!("CFB stream을 찾지 못했습니다: {name}")))?;
        if entry.stream_size < u64::from(self.mini_stream_cutoff_size)
            && is_regular_sector_id(entry.start_sector)
        {
            return self.read_stream_from_mini_chain(entry.start_sector, entry.stream_size, name);
        }
        read_stream_from_fat_chain(
            &self.data,
            self.sector_size,
            &self.fat,
            entry.start_sector,
            Some(entry.stream_size),
            name,
        )
    }
    fn read_stream_from_mini_chain(
        &self,
        start_mini_sector: u32,
        size: u64,
        name: &str,
    ) -> Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut sid = start_mini_sector;
        let mut seen: HashSet<u32> = HashSet::new();
        while sid != CFB_END_OF_CHAIN {
            if !seen.insert(sid) {
                return Err(err(format!("mini stream chain 순환 감지: {name}")));
            }
            let offset = sid as usize * self.mini_sector_size;
            let end = offset + self.mini_sector_size;
            if end > self.root_stream.len() {
                return Err(err(format!(
                    "mini stream 범위를 벗어났습니다: {name} (sector={sid})"
                )));
            }
            out.extend_from_slice(&self.root_stream[offset..end]);
            let next = *self
                .mini_fat
                .get(sid as usize)
                .ok_or_else(|| err(format!("mini FAT 인덱스 범위 오류: {sid}")))?;
            if next == CFB_FREE_SECT {
                break;
            }
            sid = next;
        }
        out.truncate(size as usize);
        Ok(out)
    }
}
fn is_regular_sector_id(v: u32) -> bool {
    !matches!(
        v,
        CFB_FREE_SECT | CFB_END_OF_CHAIN | CFB_FAT_SECT | CFB_DIFAT_SECT
    )
}
fn read_stream_from_fat_chain(
    data: &[u8],
    sector_size: usize,
    fat: &[u32],
    start_sector: u32,
    size_limit: Option<u64>,
    stream_name: &str,
) -> Result<Vec<u8>> {
    if !is_regular_sector_id(start_sector) {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut sid = start_sector;
    let mut seen: HashSet<u32> = HashSet::new();
    while sid != CFB_END_OF_CHAIN {
        if !is_regular_sector_id(sid) {
            return Err(err(format!(
                "FAT chain에 잘못된 sector id가 있습니다: {stream_name} ({sid:#x})"
            )));
        }
        if !seen.insert(sid) {
            return Err(err(format!(
                "FAT chain 순환 감지: {stream_name} (sector={sid})"
            )));
        }
        let sector = get_sector_slice(data, sector_size, sid)?;
        out.extend_from_slice(sector);
        let next = *fat
            .get(sid as usize)
            .ok_or_else(|| err(format!("FAT 인덱스 범위 오류: sector={sid}")))?;
        if next == CFB_FREE_SECT {
            break;
        }
        sid = next;
    }
    if let Some(limit) = size_limit {
        out.truncate(limit as usize);
    }
    Ok(out)
}
fn get_sector_slice(data: &[u8], sector_size: usize, sector_id: u32) -> Result<&[u8]> {
    let start = (sector_id as usize + 1) * sector_size;
    let end = start + sector_size;
    if end > data.len() {
        return Err(err(format!(
            "CFB sector 범위를 벗어났습니다: sector={sector_id}, size={sector_size}"
        )));
    }
    Ok(&data[start..end])
}
fn parse_directory_entries(
    dir_stream: &[u8],
    major_version: u16,
) -> Result<Vec<CfbDirectoryEntry>> {
    let mut entries = Vec::new();
    let mut cursor = 0usize;
    while cursor + 128 <= dir_stream.len() {
        let entry = &dir_stream[cursor..cursor + 128];
        let name_len = read_u16_le(entry, 0x40)? as usize;
        let object_type = entry[0x42];
        let start_sector = read_u32_le(entry, 0x74)?;
        let stream_size_raw = read_u64_le(entry, 0x78)?;
        let stream_size = if major_version == 3 {
            stream_size_raw & 0xFFFF_FFFF
        } else {
            stream_size_raw
        };
        let name = if (2..=64).contains(&name_len) {
            let bytes = &entry[0..name_len - 2];
            decode_utf16_le(bytes)
        } else {
            String::new()
        };
        entries.push(CfbDirectoryEntry {
            name,
            object_type,
            start_sector,
            stream_size,
        });
        cursor += 128;
    }
    Ok(entries)
}
fn parse_biff_first_sheet_rows(workbook_stream: &[u8]) -> Result<Vec<Vec<CellValue>>> {
    let (shared_strings, boundsheets) = parse_biff_globals(workbook_stream)?;
    let first_sheet_offset = boundsheets
        .iter()
        .find(|v| v.sheet_type == 0)
        .map(|v| v.offset)
        .ok_or_else(|| err("xls 첫 worksheet 정보를 찾지 못했습니다."))?;
    parse_biff_worksheet_cells(workbook_stream, first_sheet_offset, &shared_strings)
}
#[derive(Debug, Clone)]
struct BiffBoundSheet {
    offset: usize,
    sheet_type: u8,
}
fn parse_biff_globals(workbook_stream: &[u8]) -> Result<(Vec<String>, Vec<BiffBoundSheet>)> {
    let mut pos = 0usize;
    let mut shared_strings = Vec::new();
    let mut boundsheets: Vec<BiffBoundSheet> = Vec::new();
    while pos + 4 <= workbook_stream.len() {
        let record_id = read_u16_le(workbook_stream, pos)?;
        let record_len = read_u16_le(workbook_stream, pos + 2)? as usize;
        let data_start = pos + 4;
        let data_end = data_start + record_len;
        if data_end > workbook_stream.len() {
            break;
        }
        let data = &workbook_stream[data_start..data_end];
        if record_id == 0x0085 && data.len() >= 8 {
            let offset = read_u32_le(data, 0)? as usize;
            let sheet_type = data[5];
            boundsheets.push(BiffBoundSheet { offset, sheet_type });
        } else if record_id == 0x00FC {
            let mut chunks: Vec<&[u8]> = vec![data];
            let mut next = data_end;
            while next + 4 <= workbook_stream.len() {
                let next_id = read_u16_le(workbook_stream, next)?;
                let next_len = read_u16_le(workbook_stream, next + 2)? as usize;
                let next_data_start = next + 4;
                let next_data_end = next_data_start + next_len;
                if next_data_end > workbook_stream.len() {
                    break;
                }
                if next_id != 0x003C {
                    break;
                }
                chunks.push(&workbook_stream[next_data_start..next_data_end]);
                next = next_data_end;
            }
            shared_strings = parse_sst_from_chunks(&chunks)?;
            pos = next;
            continue;
        }
        pos = data_end;
        if record_id == 0x000A && !boundsheets.is_empty() {
            break;
        }
    }
    if boundsheets.is_empty() {
        return Err(err("xls에서 BoundSheet를 찾지 못했습니다."));
    }
    Ok((shared_strings, boundsheets))
}
struct SstChunkReader<'a> {
    chunks: Vec<&'a [u8]>,
    chunk_index: usize,
    offset_in_chunk: usize,
}
impl<'a> SstChunkReader<'a> {
    fn new(chunks: Vec<&'a [u8]>) -> Self {
        Self {
            chunks,
            chunk_index: 0,
            offset_in_chunk: 0,
        }
    }
    fn ensure_available(&mut self) -> Result<()> {
        while self.chunk_index < self.chunks.len()
            && self.offset_in_chunk >= self.chunks[self.chunk_index].len()
        {
            self.chunk_index += 1;
            self.offset_in_chunk = 0;
        }
        if self.chunk_index >= self.chunks.len() {
            return Err(err("SST data가 예상보다 짧습니다."));
        }
        Ok(())
    }
    fn read_u8(&mut self) -> Result<u8> {
        self.ensure_available()?;
        let value = self.chunks[self.chunk_index][self.offset_in_chunk];
        self.offset_in_chunk += 1;
        Ok(value)
    }
    fn read_u16(&mut self) -> Result<u16> {
        let b0 = self.read_u8()? as u16;
        let b1 = self.read_u8()? as u16;
        Ok(b0 | (b1 << 8))
    }
    fn read_u32(&mut self) -> Result<u32> {
        let b0 = self.read_u8()? as u32;
        let b1 = self.read_u8()? as u32;
        let b2 = self.read_u8()? as u32;
        let b3 = self.read_u8()? as u32;
        Ok(b0 | (b1 << 8) | (b2 << 16) | (b3 << 24))
    }
    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(len);
        while out.len() < len {
            self.ensure_available()?;
            let chunk = self.chunks[self.chunk_index];
            let remain = chunk.len() - self.offset_in_chunk;
            let need = len - out.len();
            let take = remain.min(need);
            out.extend_from_slice(&chunk[self.offset_in_chunk..self.offset_in_chunk + take]);
            self.offset_in_chunk += take;
        }
        Ok(out)
    }
    fn read_xl_unicode_chars(&mut self, char_count: usize, mut high_byte: bool) -> Result<String> {
        let mut out = String::new();
        let mut remaining = char_count;
        let mut continuation = false;
        while remaining > 0 {
            self.ensure_available()?;
            if continuation && self.offset_in_chunk == 0 {
                let option = self.read_u8()?;
                high_byte = (option & 0x01) != 0;
                self.ensure_available()?;
            }
            let chunk = self.chunks[self.chunk_index];
            let available_bytes = chunk.len() - self.offset_in_chunk;
            let bytes_per_char = if high_byte { 2 } else { 1 };
            let chars_here = (available_bytes / bytes_per_char).min(remaining);
            if chars_here == 0 {
                self.chunk_index += 1;
                self.offset_in_chunk = 0;
                continuation = true;
                continue;
            }
            let byte_len = chars_here * bytes_per_char;
            let bytes = &chunk[self.offset_in_chunk..self.offset_in_chunk.saturating_add(byte_len)];
            if high_byte {
                out.push_str(&decode_utf16_le(bytes));
            } else {
                out.push_str(&decode_single_byte_text(bytes));
            }
            self.offset_in_chunk += byte_len;
            remaining -= chars_here;
            if remaining > 0 && self.offset_in_chunk >= chunk.len() {
                self.chunk_index += 1;
                self.offset_in_chunk = 0;
                continuation = true;
            } else {
                continuation = false;
            }
        }
        Ok(out)
    }
}
fn parse_sst_from_chunks(chunks: &[&[u8]]) -> Result<Vec<String>> {
    if chunks.is_empty() {
        return Ok(Vec::new());
    }
    let mut reader = SstChunkReader::new(chunks.to_vec());
    let _total_count = reader.read_u32()?;
    let unique_count = reader.read_u32()? as usize;
    let mut out = Vec::with_capacity(unique_count);
    for _ in 0..unique_count {
        let char_count = reader.read_u16()? as usize;
        let flags = reader.read_u8()?;
        let high_byte = (flags & 0x01) != 0;
        let rich = (flags & 0x08) != 0;
        let ext = (flags & 0x04) != 0;
        let rich_run_count = if rich {
            reader.read_u16()? as usize
        } else {
            0usize
        };
        let ext_len = if ext {
            reader.read_u32()? as usize
        } else {
            0usize
        };
        let value = reader.read_xl_unicode_chars(char_count, high_byte)?;
        if rich_run_count > 0 {
            let _ = reader.read_bytes(rich_run_count * 4)?;
        }
        if ext_len > 0 {
            let _ = reader.read_bytes(ext_len)?;
        }
        out.push(value);
    }
    Ok(out)
}
fn parse_biff_worksheet_cells(
    workbook_stream: &[u8],
    sheet_offset: usize,
    shared_strings: &[String],
) -> Result<Vec<Vec<CellValue>>> {
    if sheet_offset >= workbook_stream.len() {
        return Err(err(format!(
            "worksheet offset이 workbook stream 범위를 벗어났습니다: {sheet_offset}"
        )));
    }
    let mut pos = sheet_offset;
    let mut cells: HashMap<(usize, usize), CellValue> = HashMap::new();
    let mut max_row = 0usize;
    let mut max_col = 0usize;
    while pos + 4 <= workbook_stream.len() {
        let record_id = read_u16_le(workbook_stream, pos)?;
        let record_len = read_u16_le(workbook_stream, pos + 2)? as usize;
        let data_start = pos + 4;
        let data_end = data_start + record_len;
        if data_end > workbook_stream.len() {
            break;
        }
        let data = &workbook_stream[data_start..data_end];
        match record_id {
            0x00FD => {
                if data.len() >= 10 {
                    let row = read_u16_le(data, 0)? as usize;
                    let col = read_u16_le(data, 2)? as usize;
                    let idx = read_u32_le(data, 6)? as usize;
                    let value = shared_strings.get(idx).cloned().unwrap_or_default();
                    cells.insert((row, col), CellValue::Text(value));
                    max_row = max_row.max(row);
                    max_col = max_col.max(col);
                }
            }
            0x0203 => {
                if data.len() >= 14 {
                    let row = read_u16_le(data, 0)? as usize;
                    let col = read_u16_le(data, 2)? as usize;
                    let mut bytes = [0u8; 8];
                    bytes.copy_from_slice(&data[6..14]);
                    let value = f64::from_le_bytes(bytes);
                    cells.insert((row, col), CellValue::Number(value));
                    max_row = max_row.max(row);
                    max_col = max_col.max(col);
                }
            }
            0x027E => {
                if data.len() >= 10 {
                    let row = read_u16_le(data, 0)? as usize;
                    let col = read_u16_le(data, 2)? as usize;
                    let rk = read_u32_le(data, 6)?;
                    let value = decode_rk_number(rk);
                    cells.insert((row, col), CellValue::Number(value));
                    max_row = max_row.max(row);
                    max_col = max_col.max(col);
                }
            }
            0x00BD => {
                if data.len() >= 10 {
                    let row = read_u16_le(data, 0)? as usize;
                    let col_first = read_u16_le(data, 2)? as usize;
                    let col_last = read_u16_le(data, data.len() - 2)? as usize;
                    let mut offset = 4usize;
                    let mut col = col_first;
                    while offset + 6 <= data.len().saturating_sub(2) && col <= col_last {
                        let rk = read_u32_le(data, offset + 2)?;
                        cells.insert((row, col), CellValue::Number(decode_rk_number(rk)));
                        max_row = max_row.max(row);
                        max_col = max_col.max(col);
                        offset += 6;
                        col += 1;
                    }
                }
            }
            0x0204 => {
                if data.len() >= 9 {
                    let row = read_u16_le(data, 0)? as usize;
                    let col = read_u16_le(data, 2)? as usize;
                    if let Some(text) = parse_biff8_label(&data[6..]) {
                        cells.insert((row, col), CellValue::Text(text));
                        max_row = max_row.max(row);
                        max_col = max_col.max(col);
                    }
                }
            }
            0x000A => break,
            _ => {}
        }
        pos = data_end;
    }
    if cells.is_empty() {
        return Ok(Vec::new());
    }
    let mut rows = vec![vec![CellValue::Empty; max_col + 1]; max_row + 1];
    for ((r, c), value) in cells {
        if r < rows.len() && c < rows[r].len() {
            rows[r][c] = value;
        }
    }
    Ok(rows)
}
fn parse_biff8_label(data: &[u8]) -> Option<String> {
    if data.len() < 3 {
        return None;
    }
    let cch = read_u16_le(data, 0).ok()? as usize;
    let flags = *data.get(2)?;
    let high_byte = (flags & 0x01) != 0;
    let byte_len = if high_byte {
        cch.saturating_mul(2)
    } else {
        cch
    };
    if data.len() < 3 + byte_len {
        return None;
    }
    let text_bytes = &data[3..3 + byte_len];
    if high_byte {
        Some(decode_utf16_le(text_bytes))
    } else {
        Some(decode_single_byte_text(text_bytes))
    }
}
fn decode_rk_number(rk: u32) -> f64 {
    let div100 = (rk & 0x01) != 0;
    let is_int = (rk & 0x02) != 0;
    let mut value = if is_int {
        let signed = (rk as i32) >> 2;
        signed as f64
    } else {
        let bits = u64::from(rk & 0xFFFF_FFFC) << 32;
        f64::from_bits(bits)
    };
    if div100 {
        value /= 100.0;
    }
    value
}
fn decode_utf16_le(bytes: &[u8]) -> String {
    let mut data = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0usize;
    while i + 1 < bytes.len() {
        data.push(u16::from_le_bytes([bytes[i], bytes[i + 1]]));
        i += 2;
    }
    String::from_utf16_lossy(&data)
}
fn decode_single_byte_text(bytes: &[u8]) -> String {
    bytes
        .iter()
        .map(|b| {
            if b.is_ascii() {
                char::from(*b)
            } else {
                char::from_u32(u32::from(*b)).unwrap_or('?')
            }
        })
        .collect()
}
fn read_u16_le(bytes: &[u8], offset: usize) -> Result<u16> {
    let end = offset + 2;
    let slice = bytes
        .get(offset..end)
        .ok_or_else(|| err(format!("u16 read out of range at {offset}")))?;
    Ok(u16::from_le_bytes([slice[0], slice[1]]))
}
fn read_u32_le(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = offset + 4;
    let slice = bytes
        .get(offset..end)
        .ok_or_else(|| err(format!("u32 read out of range at {offset}")))?;
    Ok(u32::from_le_bytes([slice[0], slice[1], slice[2], slice[3]]))
}
fn read_u64_le(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = offset + 8;
    let slice = bytes
        .get(offset..end)
        .ok_or_else(|| err(format!("u64 read out of range at {offset}")))?;
    Ok(u64::from_le_bytes([
        slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7],
    ]))
}
fn format_number(v: f64) -> String {
    if (v.fract() - 0.0).abs() < f64::EPSILON {
        format!("{}", v as i64)
    } else {
        let mut s = format!("{v}");
        if s.contains('.') {
            while s.ends_with('0') {
                s.pop();
            }
            if s.ends_with('.') {
                s.pop();
            }
        }
        s
    }
}
fn build_source_records_from_rows(rows: &[Vec<CellValue>]) -> Result<Vec<SourceRecord>> {
    let mut header_row_idx: Option<usize> = None;
    for (idx, row) in rows.iter().take(50).enumerate() {
        let row_str: Vec<String> = row.iter().map(CellValue::as_string).collect();
        if row_str.iter().any(|s| canon_header(s) == "지역")
            && row_str.iter().any(|s| canon_header(s) == "상호")
            && row_str.iter().any(|s| canon_header(s) == "주소")
        {
            header_row_idx = Some(idx);
            break;
        }
    }
    let header_row_idx = header_row_idx.ok_or_else(|| err("헤더 행을 찾지 못했습니다"))?;
    let header = rows
        .get(header_row_idx)
        .ok_or_else(|| err("헤더 행 접근 실패"))?;
    let mut idx_region: Option<usize> = None;
    let mut idx_name: Option<usize> = None;
    let mut idx_addr: Option<usize> = None;
    let mut idx_brand: Option<usize> = None;
    let mut idx_phone: Option<usize> = None;
    let mut idx_self: Option<usize> = None;
    let mut idx_premium: Option<usize> = None;
    let mut idx_gas: Option<usize> = None;
    let mut idx_diesel: Option<usize> = None;
    for (i, cell) in header.iter().enumerate() {
        let h = canon_header(&cell.as_string());
        match h.as_str() {
            "지역" => idx_region = Some(i),
            "상호" => idx_name = Some(i),
            "주소" => idx_addr = Some(i),
            "상표" => idx_brand = Some(i),
            "전화번호" | "전화" => idx_phone = Some(i),
            "셀프여부" | "셀프" => idx_self = Some(i),
            "고급휘발유" | "고급유" => idx_premium = Some(i),
            "휘발유" | "보통휘발유" => idx_gas = Some(i),
            "경유" => idx_diesel = Some(i),
            _ => {}
        }
    }
    let idx_name = idx_name.ok_or_else(|| err("헤더에 '상호' 컬럼이 없습니다"))?;
    let idx_addr = idx_addr.ok_or_else(|| err("헤더에 '주소' 컬럼이 없습니다"))?;
    let mut out = Vec::new();
    for row in rows.iter().skip(header_row_idx + 1) {
        let name = get_row_string(row, idx_name);
        let address = get_row_string(row, idx_addr);
        if name.trim().is_empty() && address.trim().is_empty() {
            continue;
        }
        if address.trim().is_empty() {
            continue;
        }
        let brand = idx_brand
            .map(|i| get_row_string(row, i))
            .unwrap_or_default();
        let phone = idx_phone
            .map(|i| get_row_string(row, i))
            .unwrap_or_default();
        let self_yn = idx_self.map(|i| get_row_string(row, i)).unwrap_or_default();
        let gasoline = idx_gas.and_then(|i| get_row_i32(row, i));
        let premium = idx_premium.and_then(|i| get_row_i32(row, i));
        let diesel = idx_diesel.and_then(|i| get_row_i32(row, i));
        let region = idx_region
            .map(|i| get_row_string(row, i))
            .unwrap_or_default();
        out.push(SourceRecord {
            region,
            name,
            brand,
            self_yn,
            address,
            phone,
            gasoline,
            premium,
            diesel,
        });
    }
    Ok(out)
}
fn get_row_string(row: &[CellValue], idx: usize) -> String {
    row.get(idx).map(CellValue::as_string).unwrap_or_default()
}
fn get_row_i32(row: &[CellValue], idx: usize) -> Option<i32> {
    row.get(idx).and_then(CellValue::as_i32)
}
