use super::{CellValue, MAX_XLSX_COL, MAX_XLSX_ROW, build_source_records_from_rows};
use crate::source_sync::SourceRecord;
use crate::{Result, err};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs,
    path::Path,
};
pub(super) fn read_xls_source(path: &Path) -> Result<Vec<SourceRecord>> {
    let cfb = CfbFile::open(path)?;
    let workbook = match cfb.read_stream_by_name("Workbook") {
        Ok(v) => v,
        Err(_) => cfb.read_stream_by_name("Book")?,
    };
    let globals = parse_biff_globals(&workbook)?;
    let mut all = Vec::new();
    let mut last_err: Option<Box<dyn std::error::Error + Send + Sync>> = None;
    for sheet in globals
        .boundsheets
        .iter()
        .filter(|sheet| sheet.sheet_type == 0)
    {
        let rows = parse_biff_worksheet_cells(
            &workbook,
            sheet.offset,
            &globals.shared_strings,
            globals.code_page,
        )?;
        match build_source_records_from_rows(&rows) {
            Ok(records) if !records.is_empty() => all.extend(records),
            Ok(_) => {}
            Err(e) => last_err = Some(e),
        }
    }
    if !all.is_empty() {
        return Ok(all);
    }
    if let Some(e) = last_err {
        return Err(err(format!(
            "xls 시트에서 유효한 소스 데이터를 찾지 못했습니다. ({e})"
        )));
    }
    Err(err("xls 시트에서 유효한 소스 데이터를 찾지 못했습니다."))
}
const CFB_SIGNATURE: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
const CFB_FREE_SECT: u32 = 0xFFFF_FFFF;
const CFB_END_OF_CHAIN: u32 = 0xFFFF_FFFE;
const CFB_FAT_SECT: u32 = 0xFFFF_FFFD;
const CFB_DIFAT_SECT: u32 = 0xFFFF_FFFC;
const MAX_XLS_FILE_SIZE_BYTES: u64 = 512 * 1024 * 1024;
#[derive(Debug, Clone)]
struct CfbDirectoryEntry {
    name: String,
    object_type: u8,
    start_sector: u32,
    stream_size: u64,
}
#[derive(Debug, Clone, Copy)]
struct CfbHeader {
    major_version: u16,
    sector_size: usize,
    mini_sector_size: usize,
    num_fat_sectors: u32,
    first_dir_sector: u32,
    mini_stream_cutoff_size: u32,
    first_mini_fat_sector: u32,
    num_mini_fat_sectors: u32,
    first_difat_sector: u32,
    num_difat_sectors: u32,
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
        let file_size = fs::metadata(path)
            .map_err(|e| {
                err(format!(
                    "xls 파일 메타데이터 조회 실패: {} ({e})",
                    path.display()
                ))
            })?
            .len();
        if file_size > MAX_XLS_FILE_SIZE_BYTES {
            return Err(err(format!(
                "xls 파일이 너무 큽니다: {} ({} bytes, 최대 {} bytes)",
                path.display(),
                file_size,
                MAX_XLS_FILE_SIZE_BYTES
            )));
        }
        let data = fs::read(path)
            .map_err(|e| err(format!("xls 파일 읽기 실패: {} ({e})", path.display())))?;
        if data.len() < 512 || data.get(..CFB_SIGNATURE.len()) != Some(CFB_SIGNATURE.as_slice()) {
            return Err(err(format!(
                "유효한 OLE2(CFB) xls 파일이 아닙니다: {}",
                path.display()
            )));
        }
        let header = parse_cfb_header(&data)?;
        let max_sector_count = max_regular_sector_count(data.len(), header.sector_size);
        if max_sector_count == 0 {
            return Err(err("CFB sector 개수가 비정상적입니다."));
        }
        let declared_fat_sectors = usize::try_from(header.num_fat_sectors)
            .map_err(|_| err("CFB FAT sector 개수 변환에 실패했습니다."))?;
        if declared_fat_sectors > max_sector_count {
            return Err(err(format!(
                "CFB FAT sector 개수가 비정상적으로 큽니다: {declared_fat_sectors} (최대 {max_sector_count})"
            )));
        }
        let difat_entries = collect_difat_entries(&data, &header, max_sector_count)?;
        let fat_sector_ids: Vec<u32> = difat_entries
            .into_iter()
            .take(declared_fat_sectors)
            .collect();
        if fat_sector_ids.is_empty() {
            return Err(err("CFB FAT 정보를 찾지 못했습니다."));
        }
        if fat_sector_ids.len() < declared_fat_sectors {
            return Err(err(format!(
                "CFB FAT 엔트리가 부족합니다: 필요 {}, 실제 {}",
                declared_fat_sectors,
                fat_sector_ids.len()
            )));
        }
        let fat = build_fat_table(&data, header.sector_size, &fat_sector_ids)?;
        let dir_stream = read_stream_from_fat_chain(
            &data,
            header.sector_size,
            &fat,
            header.first_dir_sector,
            None,
            "CFB 디렉터리",
        )?;
        let directory = parse_directory_entries(&dir_stream, header.major_version)?;
        let root_entry = directory
            .iter()
            .find(|v| v.object_type == 5)
            .ok_or_else(|| err("CFB root entry를 찾지 못했습니다."))?;
        let root_stream = read_stream_from_fat_chain(
            &data,
            header.sector_size,
            &fat,
            root_entry.start_sector,
            Some(root_entry.stream_size),
            "CFB root stream",
        )?;
        let mini_fat = build_mini_fat_table(&data, &fat, header, max_sector_count)?;
        Ok(Self {
            data,
            sector_size: header.sector_size,
            mini_sector_size: header.mini_sector_size,
            mini_stream_cutoff_size: header.mini_stream_cutoff_size,
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
        let mut remaining = usize::try_from(size)
            .map_err(|_| err(format!("mini stream 길이 변환 실패: {size} ({name})")))?;
        let mut sid = start_mini_sector;
        let mut seen: HashSet<u32> = HashSet::new();
        while sid != CFB_END_OF_CHAIN && remaining > 0 {
            if !seen.insert(sid) {
                return Err(err(format!("mini stream chain 순환 감지: {name}")));
            }
            let sid_usize = usize::try_from(sid)
                .map_err(|_| err(format!("mini stream sector 변환 실패: {sid}")))?;
            let offset = sid_usize
                .checked_mul(self.mini_sector_size)
                .ok_or_else(|| {
                    err(format!(
                        "mini stream offset 계산 overflow: {name} (sector={sid})"
                    ))
                })?;
            let end = offset.checked_add(self.mini_sector_size).ok_or_else(|| {
                err(format!(
                    "mini stream end 계산 overflow: {name} (sector={sid})"
                ))
            })?;
            if end > self.root_stream.len() {
                return Err(err(format!(
                    "mini stream 범위를 벗어났습니다: {name} (sector={sid})"
                )));
            }
            let chunk = self.root_stream.get(offset..end).ok_or_else(|| {
                err(format!(
                    "mini stream 범위를 벗어났습니다: {name} (sector={sid})"
                ))
            })?;
            let take = remaining.min(chunk.len());
            out.extend_from_slice(&chunk[..take]);
            remaining -= take;
            let next = *self
                .mini_fat
                .get(sid_usize)
                .ok_or_else(|| err(format!("mini FAT 인덱스 범위 오류: {sid}")))?;
            if next == CFB_FREE_SECT {
                break;
            }
            sid = next;
        }
        Ok(out)
    }
}
fn parse_cfb_header(data: &[u8]) -> Result<CfbHeader> {
    let major_version = read_u16_le(data, 0x1A)?;
    let sector_shift = read_u16_le(data, 0x1E)?;
    let mini_sector_shift = read_u16_le(data, 0x20)?;
    if !matches!(major_version, 3 | 4) {
        return Err(err(format!(
            "지원하지 않는 CFB major version: {major_version}"
        )));
    }
    let sector_size = checked_pow2_from_shift(sector_shift, "CFB sector shift")?;
    let mini_sector_size = checked_pow2_from_shift(mini_sector_shift, "CFB mini sector shift")?;
    if !matches!(sector_size, 512 | 4096) {
        return Err(err(format!("지원하지 않는 CFB sector size: {sector_size}")));
    }
    if (major_version == 3 && sector_size != 512) || (major_version == 4 && sector_size != 4096) {
        return Err(err(format!(
            "CFB 헤더 버전/sector size 조합이 유효하지 않습니다: version={major_version}, sector={sector_size}"
        )));
    }
    if mini_sector_size != 64 {
        return Err(err(format!(
            "지원하지 않는 CFB mini sector size: {mini_sector_size}"
        )));
    }
    Ok(CfbHeader {
        major_version,
        sector_size,
        mini_sector_size,
        num_fat_sectors: read_u32_le(data, 0x2C)?,
        first_dir_sector: read_u32_le(data, 0x30)?,
        mini_stream_cutoff_size: read_u32_le(data, 0x38)?,
        first_mini_fat_sector: read_u32_le(data, 0x3C)?,
        num_mini_fat_sectors: read_u32_le(data, 0x40)?,
        first_difat_sector: read_u32_le(data, 0x44)?,
        num_difat_sectors: read_u32_le(data, 0x48)?,
    })
}
fn checked_pow2_from_shift(shift: u16, context: &str) -> Result<usize> {
    let shift_u32 = u32::from(shift);
    if shift_u32 >= usize::BITS {
        return Err(err(format!(
            "{context}가 비정상적으로 큽니다: {shift_u32} (usize bits={})",
            usize::BITS
        )));
    }
    1usize
        .checked_shl(shift_u32)
        .ok_or_else(|| err(format!("{context} 계산에 실패했습니다: shift={shift_u32}")))
}
fn max_regular_sector_count(data_len: usize, sector_size: usize) -> usize {
    data_len
        .checked_sub(512)
        .map_or(0, |payload| payload / sector_size)
}
fn collect_difat_entries(
    data: &[u8],
    header: &CfbHeader,
    max_sector_count: usize,
) -> Result<Vec<u32>> {
    let mut difat_entries: Vec<u32> = Vec::new();
    for i in 0..109usize {
        let sid = read_u32_le(data, 0x4C + i * 4)?;
        if is_regular_sector_id(sid) {
            difat_entries.push(sid);
        }
    }
    if header.num_difat_sectors == 0 {
        return Ok(difat_entries);
    }
    let num_difat_sectors = usize::try_from(header.num_difat_sectors)
        .map_err(|_| err("CFB DIFAT sector 개수 변환에 실패했습니다."))?;
    if num_difat_sectors > max_sector_count {
        return Err(err(format!(
            "CFB DIFAT sector 개수가 비정상적으로 큽니다: {num_difat_sectors} (최대 {max_sector_count})"
        )));
    }
    let mut sid = header.first_difat_sector;
    let mut seen: HashSet<u32> = HashSet::new();
    for _ in 0..num_difat_sectors {
        if !is_regular_sector_id(sid) {
            break;
        }
        if !seen.insert(sid) {
            break;
        }
        let sector = get_sector_slice(data, header.sector_size, sid)?;
        let entries_per_sector = header.sector_size / 4 - 1;
        for idx in 0..entries_per_sector {
            let entry = read_u32_le(sector, idx * 4)?;
            if is_regular_sector_id(entry) {
                difat_entries.push(entry);
            }
        }
        sid = read_u32_le(sector, entries_per_sector * 4)?;
    }
    Ok(difat_entries)
}
fn build_fat_table(data: &[u8], sector_size: usize, fat_sector_ids: &[u32]) -> Result<Vec<u32>> {
    let entries_per_sector = sector_size / 4;
    let total_entries = fat_sector_ids
        .len()
        .checked_mul(entries_per_sector)
        .ok_or_else(|| err("CFB FAT 엔트리 개수 계산 중 overflow가 발생했습니다."))?;
    let mut fat: Vec<u32> = Vec::new();
    fat.try_reserve(total_entries)
        .map_err(|_| err(format!("CFB FAT 메모리 확보 실패: {total_entries} entries")))?;
    for sid in fat_sector_ids {
        let sector = get_sector_slice(data, sector_size, *sid)?;
        for i in 0..entries_per_sector {
            fat.push(read_u32_le(sector, i * 4)?);
        }
    }
    Ok(fat)
}
fn build_mini_fat_table(
    data: &[u8],
    fat: &[u32],
    header: CfbHeader,
    max_sector_count: usize,
) -> Result<Vec<u32>> {
    if header.num_mini_fat_sectors == 0 || !is_regular_sector_id(header.first_mini_fat_sector) {
        return Ok(Vec::new());
    }
    let mini_fat_sector_count = usize::try_from(header.num_mini_fat_sectors)
        .map_err(|_| err("CFB mini FAT sector 개수 변환에 실패했습니다."))?;
    if mini_fat_sector_count > max_sector_count {
        return Err(err(format!(
            "CFB mini FAT sector 개수가 비정상적으로 큽니다: {mini_fat_sector_count} (최대 {max_sector_count})"
        )));
    }
    let sector_size_u64 = u64::try_from(header.sector_size)
        .map_err(|_| err("CFB sector size 변환에 실패했습니다."))?;
    let mini_fat_limit = u64::from(header.num_mini_fat_sectors)
        .checked_mul(sector_size_u64)
        .ok_or_else(|| err("CFB mini FAT 길이 계산 중 overflow가 발생했습니다."))?;
    let mini_fat_bytes = read_stream_from_fat_chain(
        data,
        header.sector_size,
        fat,
        header.first_mini_fat_sector,
        Some(mini_fat_limit),
        "CFB mini FAT",
    )?;
    let mut out = Vec::new();
    let mut idx = 0usize;
    while idx + 4 <= mini_fat_bytes.len() {
        out.push(read_u32_le(&mini_fat_bytes, idx)?);
        idx += 4;
    }
    Ok(out)
}
const fn is_regular_sector_id(v: u32) -> bool {
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
    let mut remaining = size_limit
        .map(|limit| {
            usize::try_from(limit).map_err(|_| {
                err(format!(
                    "FAT stream 길이 변환 실패: {limit} ({stream_name})"
                ))
            })
        })
        .transpose()?;
    let mut sid = start_sector;
    let mut seen: HashSet<u32> = HashSet::new();
    while sid != CFB_END_OF_CHAIN {
        if matches!(remaining, Some(0)) {
            break;
        }
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
        if let Some(remain) = remaining.as_mut() {
            let take = (*remain).min(sector.len());
            out.extend_from_slice(&sector[..take]);
            *remain -= take;
        } else {
            out.extend_from_slice(sector);
        }
        let sid_usize = usize::try_from(sid)
            .map_err(|_| err(format!("FAT sector 변환 실패: {sid} ({stream_name})")))?;
        let next = *fat
            .get(sid_usize)
            .ok_or_else(|| err(format!("FAT 인덱스 범위 오류: sector={sid}")))?;
        if next == CFB_FREE_SECT {
            break;
        }
        sid = next;
    }
    Ok(out)
}
fn get_sector_slice(data: &[u8], sector_size: usize, sector_id: u32) -> Result<&[u8]> {
    let sector_idx = usize::try_from(sector_id)
        .map_err(|_| err(format!("CFB sector id 변환 실패: {sector_id}")))?;
    let start = sector_idx
        .checked_add(1)
        .and_then(|v| v.checked_mul(sector_size))
        .ok_or_else(|| {
            err(format!(
                "CFB sector offset 계산 overflow: sector={sector_id}, size={sector_size}"
            ))
        })?;
    let end = start.checked_add(sector_size).ok_or_else(|| {
        err(format!(
            "CFB sector 끝 offset 계산 overflow: sector={sector_id}, size={sector_size}"
        ))
    })?;
    data.get(start..end).ok_or_else(|| {
        err(format!(
            "CFB sector 범위를 벗어났습니다: sector={sector_id}, size={sector_size}"
        ))
    })
}
fn parse_directory_entries(
    dir_stream: &[u8],
    major_version: u16,
) -> Result<Vec<CfbDirectoryEntry>> {
    let mut entries = Vec::new();
    let mut cursor = 0usize;
    while cursor + 128 <= dir_stream.len() {
        let entry = dir_stream
            .get(cursor..cursor + 128)
            .ok_or_else(|| err("CFB 디렉터리 엔트리 범위 오류"))?;
        let name_len = read_u16_le(entry, 0x40)? as usize;
        let object_type = *entry
            .get(0x42)
            .ok_or_else(|| err("CFB 디렉터리 object_type 범위 오류"))?;
        let start_sector = read_u32_le(entry, 0x74)?;
        let stream_size_raw = read_u64_le(entry, 0x78)?;
        let stream_size = if major_version == 3 {
            stream_size_raw & 0xFFFF_FFFF
        } else {
            stream_size_raw
        };
        let name = if (2..=64).contains(&name_len) {
            let bytes = entry
                .get(0..name_len - 2)
                .ok_or_else(|| err("CFB 디렉터리 이름 범위 오류"))?;
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
#[derive(Debug, Clone)]
struct BiffBoundSheet {
    offset: usize,
    sheet_type: u8,
}
struct BiffGlobals {
    shared_strings: Vec<String>,
    boundsheets: Vec<BiffBoundSheet>,
    code_page: Option<u16>,
}
fn parse_biff_globals(workbook_stream: &[u8]) -> Result<BiffGlobals> {
    let mut pos = 0usize;
    let mut shared_strings = Vec::new();
    let mut boundsheets: Vec<BiffBoundSheet> = Vec::new();
    let mut code_page: Option<u16> = detect_biff_code_page(workbook_stream);
    while pos + 4 <= workbook_stream.len() {
        let record_id = read_u16_le(workbook_stream, pos)?;
        let record_len = read_u16_le(workbook_stream, pos + 2)? as usize;
        let data_start = pos + 4;
        let data_end = data_start
            .checked_add(record_len)
            .ok_or_else(|| err("xls BIFF globals 레코드 길이 계산 중 overflow가 발생했습니다."))?;
        if data_end > workbook_stream.len() {
            return Err(err("xls BIFF globals 레코드가 손상되었습니다."));
        }
        let data = workbook_stream
            .get(data_start..data_end)
            .ok_or_else(|| err("xls BIFF globals 레코드 범위 오류"))?;
        if record_id == 0x0085 && data.len() >= 8 {
            let offset = usize::try_from(read_u32_le(data, 0)?)
                .map_err(|_| err("xls BoundSheet offset 변환에 실패했습니다."))?;
            let sheet_type = *data
                .get(5)
                .ok_or_else(|| err("xls BoundSheet sheet_type 범위 오류"))?;
            boundsheets.push(BiffBoundSheet { offset, sheet_type });
        } else if record_id == 0x0042 && data.len() >= 2 {
            code_page = Some(read_u16_le(data, 0)?);
        } else if record_id == 0x00FC {
            let mut chunks: Vec<&[u8]> = vec![data];
            let mut next = data_end;
            while next + 4 <= workbook_stream.len() {
                let next_id = read_u16_le(workbook_stream, next)?;
                let next_len = read_u16_le(workbook_stream, next + 2)? as usize;
                let next_data_start = next + 4;
                let Some(next_data_end) = next_data_start.checked_add(next_len) else {
                    break;
                };
                if next_data_end > workbook_stream.len() {
                    break;
                }
                if next_id != 0x003C {
                    break;
                }
                if let Some(chunk) = workbook_stream.get(next_data_start..next_data_end) {
                    chunks.push(chunk);
                } else {
                    break;
                }
                next = next_data_end;
            }
            shared_strings = parse_sst_from_chunks(&chunks, code_page)?;
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
    Ok(BiffGlobals {
        shared_strings,
        boundsheets,
        code_page,
    })
}
fn detect_biff_code_page(workbook_stream: &[u8]) -> Option<u16> {
    let mut pos = 0usize;
    while pos + 4 <= workbook_stream.len() {
        let record_id = read_u16_le(workbook_stream, pos).ok()?;
        let record_len = read_u16_le(workbook_stream, pos + 2).ok()? as usize;
        let data_start = pos + 4;
        let data_end = data_start.checked_add(record_len)?;
        if data_end > workbook_stream.len() {
            break;
        }
        if record_id == 0x0042 && record_len >= 2 {
            return read_u16_le(workbook_stream, data_start).ok();
        }
        pos = data_end;
        if record_id == 0x000A {
            break;
        }
    }
    None
}
struct SstChunkReader<'a> {
    chunks: Vec<&'a [u8]>,
    chunk_index: usize,
    offset_in_chunk: usize,
    code_page: Option<u16>,
}
impl<'a> SstChunkReader<'a> {
    const fn new(chunks: Vec<&'a [u8]>, code_page: Option<u16>) -> Self {
        Self {
            chunks,
            chunk_index: 0,
            offset_in_chunk: 0,
            code_page,
        }
    }
    fn ensure_available(&mut self) -> Result<()> {
        while let Some(chunk) = self.chunks.get(self.chunk_index) {
            if self.offset_in_chunk < chunk.len() {
                break;
            }
            self.chunk_index += 1;
            self.offset_in_chunk = 0;
        }
        if self.chunks.get(self.chunk_index).is_none() {
            return Err(err("SST data가 예상보다 짧습니다."));
        }
        Ok(())
    }
    fn read_u8(&mut self) -> Result<u8> {
        self.ensure_available()?;
        let value = *self
            .chunks
            .get(self.chunk_index)
            .and_then(|chunk| chunk.get(self.offset_in_chunk))
            .ok_or_else(|| err("SST byte 접근 범위 오류"))?;
        self.offset_in_chunk += 1;
        Ok(value)
    }
    fn read_u16(&mut self) -> Result<u16> {
        let b0 = u16::from(self.read_u8()?);
        let b1 = u16::from(self.read_u8()?);
        Ok(b0 | (b1 << 8))
    }
    fn read_u32(&mut self) -> Result<u32> {
        let b0 = u32::from(self.read_u8()?);
        let b1 = u32::from(self.read_u8()?);
        let b2 = u32::from(self.read_u8()?);
        let b3 = u32::from(self.read_u8()?);
        Ok(b0 | (b1 << 8) | (b2 << 16) | (b3 << 24))
    }
    fn remaining_bytes(&self) -> usize {
        let mut total = 0usize;
        for (idx, chunk) in self.chunks.iter().enumerate().skip(self.chunk_index) {
            let consumed = if idx == self.chunk_index {
                self.offset_in_chunk.min(chunk.len())
            } else {
                0
            };
            total = total.saturating_add(chunk.len().saturating_sub(consumed));
        }
        total
    }
    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        if len > self.remaining_bytes() {
            return Err(err(format!(
                "SST data가 예상보다 짧습니다. (요청 {len} bytes)"
            )));
        }
        let mut out = Vec::new();
        out.try_reserve(len)
            .map_err(|_| err(format!("SST 버퍼 메모리 확보 실패: {len} bytes")))?;
        while out.len() < len {
            self.ensure_available()?;
            let chunk = *self
                .chunks
                .get(self.chunk_index)
                .ok_or_else(|| err("SST chunk 접근 범위 오류"))?;
            let remain = chunk.len() - self.offset_in_chunk;
            let need = len - out.len();
            let take = remain.min(need);
            let bytes = chunk
                .get(self.offset_in_chunk..self.offset_in_chunk + take)
                .ok_or_else(|| err("SST chunk slice 범위 오류"))?;
            out.extend_from_slice(bytes);
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
            let chunk = *self
                .chunks
                .get(self.chunk_index)
                .ok_or_else(|| err("SST chunk 접근 범위 오류"))?;
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
            let bytes = chunk
                .get(self.offset_in_chunk..self.offset_in_chunk.saturating_add(byte_len))
                .ok_or_else(|| err("SST 문자열 slice 범위 오류"))?;
            if high_byte {
                out.push_str(&decode_utf16_le(bytes));
            } else {
                out.push_str(&super::super::text_decode::decode_single_byte_text(
                    bytes,
                    self.code_page,
                )?);
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
fn parse_sst_from_chunks(chunks: &[&[u8]], code_page: Option<u16>) -> Result<Vec<String>> {
    if chunks.is_empty() {
        return Ok(Vec::new());
    }
    let total_chunk_bytes = chunks.iter().try_fold(0usize, |acc, chunk| {
        acc.checked_add(chunk.len())
            .ok_or_else(|| err("SST chunk 총길이 계산 중 overflow가 발생했습니다."))
    })?;
    if total_chunk_bytes < 8 {
        return Err(err("SST 데이터가 비정상적으로 짧습니다."));
    }
    let mut reader = SstChunkReader::new(chunks.to_vec(), code_page);
    let _total_count = reader.read_u32()?;
    let unique_count = usize::try_from(reader.read_u32()?)
        .map_err(|_| err("SST unique count 변환에 실패했습니다."))?;
    let max_unique_count = total_chunk_bytes.saturating_sub(8) / 3;
    if unique_count > max_unique_count {
        return Err(err(format!(
            "SST unique count가 비정상적으로 큽니다: {unique_count} (최대 {max_unique_count})"
        )));
    }
    let mut out = Vec::new();
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
            usize::try_from(reader.read_u32()?)
                .map_err(|_| err("SST ext 길이 변환에 실패했습니다."))?
        } else {
            0usize
        };
        let value = reader.read_xl_unicode_chars(char_count, high_byte)?;
        if rich_run_count > 0 {
            let rich_bytes = rich_run_count
                .checked_mul(4)
                .ok_or_else(|| err("SST rich-text 길이 계산 중 overflow가 발생했습니다."))?;
            let _ = reader.read_bytes(rich_bytes)?;
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
    code_page: Option<u16>,
) -> Result<Vec<(usize, Vec<CellValue>)>> {
    if sheet_offset >= workbook_stream.len() {
        return Err(err(format!(
            "worksheet offset이 workbook stream 범위를 벗어났습니다: {sheet_offset}"
        )));
    }
    let mut pos = sheet_offset;
    let mut rows_map: BTreeMap<usize, HashMap<usize, CellValue>> = BTreeMap::new();
    while let Some((record_id, data)) = read_biff_record(workbook_stream, &mut pos)? {
        if handle_biff_worksheet_record(record_id, data, shared_strings, code_page, &mut rows_map)?
        {
            break;
        }
    }
    Ok(finalize_sparse_rows(rows_map))
}
fn read_biff_record<'a>(
    workbook_stream: &'a [u8],
    pos: &mut usize,
) -> Result<Option<(u16, &'a [u8])>> {
    if *pos + 4 > workbook_stream.len() {
        return Ok(None);
    }
    let record_id = read_u16_le(workbook_stream, *pos)?;
    let record_len = usize::from(read_u16_le(workbook_stream, *pos + 2)?);
    let data_start = *pos + 4;
    let data_end = data_start
        .checked_add(record_len)
        .ok_or_else(|| err("xls worksheet 레코드 길이 계산 중 overflow가 발생했습니다."))?;
    if data_end > workbook_stream.len() {
        return Err(err("xls worksheet 레코드가 손상되었습니다."));
    }
    *pos = data_end;
    let data = workbook_stream
        .get(data_start..data_end)
        .ok_or_else(|| err("xls worksheet 레코드 범위 오류"))?;
    Ok(Some((record_id, data)))
}
fn handle_biff_worksheet_record(
    record_id: u16,
    data: &[u8],
    shared_strings: &[String],
    code_page: Option<u16>,
    rows_map: &mut BTreeMap<usize, HashMap<usize, CellValue>>,
) -> Result<bool> {
    match record_id {
        0x00FD => handle_biff_label_sst_record(data, shared_strings, rows_map)?,
        0x0203 => handle_biff_number_record(data, rows_map)?,
        0x027E => handle_biff_rk_record(data, rows_map)?,
        0x00BD => handle_biff_mulrk_record(data, rows_map)?,
        0x0204 => handle_biff_label_record(data, code_page, rows_map)?,
        0x000A => return Ok(true),
        _ => {}
    }
    Ok(false)
}
fn handle_biff_label_sst_record(
    data: &[u8],
    shared_strings: &[String],
    rows_map: &mut BTreeMap<usize, HashMap<usize, CellValue>>,
) -> Result<()> {
    if data.len() < 10 {
        return Ok(());
    }
    let row = usize::from(read_u16_le(data, 0)?) + 1;
    let col = usize::from(read_u16_le(data, 2)?);
    validate_sheet_cell_bounds(row, col)?;
    let idx = usize::try_from(read_u32_le(data, 6)?).ok();
    let value = idx
        .and_then(|i| shared_strings.get(i).cloned())
        .unwrap_or_default();
    insert_sparse_cell(rows_map, row, col, CellValue::Text(value));
    Ok(())
}
fn handle_biff_number_record(
    data: &[u8],
    rows_map: &mut BTreeMap<usize, HashMap<usize, CellValue>>,
) -> Result<()> {
    if data.len() < 14 {
        return Ok(());
    }
    let row = usize::from(read_u16_le(data, 0)?) + 1;
    let col = usize::from(read_u16_le(data, 2)?);
    validate_sheet_cell_bounds(row, col)?;
    let mut bytes = [0u8; 8];
    let raw = data
        .get(6..14)
        .ok_or_else(|| err("NUMBER 레코드 숫자 범위 오류"))?;
    bytes.copy_from_slice(raw);
    insert_sparse_cell(
        rows_map,
        row,
        col,
        CellValue::Number(f64::from_le_bytes(bytes)),
    );
    Ok(())
}
fn handle_biff_rk_record(
    data: &[u8],
    rows_map: &mut BTreeMap<usize, HashMap<usize, CellValue>>,
) -> Result<()> {
    if data.len() < 10 {
        return Ok(());
    }
    let row = usize::from(read_u16_le(data, 0)?) + 1;
    let col = usize::from(read_u16_le(data, 2)?);
    validate_sheet_cell_bounds(row, col)?;
    let rk = read_u32_le(data, 6)?;
    insert_sparse_cell(rows_map, row, col, CellValue::Number(decode_rk_number(rk)));
    Ok(())
}
fn handle_biff_mulrk_record(
    data: &[u8],
    rows_map: &mut BTreeMap<usize, HashMap<usize, CellValue>>,
) -> Result<()> {
    if data.len() < 10 {
        return Ok(());
    }
    let row = usize::from(read_u16_le(data, 0)?) + 1;
    let col_first = usize::from(read_u16_le(data, 2)?);
    let col_last = usize::from(read_u16_le(data, data.len() - 2)?);
    validate_sheet_cell_bounds(row, col_first)?;
    validate_sheet_cell_bounds(row, col_last)?;
    let mut offset = 4usize;
    let mut col = col_first;
    while offset + 6 <= data.len().saturating_sub(2) && col <= col_last {
        let rk = read_u32_le(data, offset + 2)?;
        insert_sparse_cell(rows_map, row, col, CellValue::Number(decode_rk_number(rk)));
        offset += 6;
        col += 1;
    }
    Ok(())
}
fn handle_biff_label_record(
    data: &[u8],
    code_page: Option<u16>,
    rows_map: &mut BTreeMap<usize, HashMap<usize, CellValue>>,
) -> Result<()> {
    if data.len() < 9 {
        return Ok(());
    }
    let Some(text) = parse_biff8_label(
        data.get(6..)
            .ok_or_else(|| err("LABEL 레코드 문자열 범위 오류"))?,
        code_page,
    )?
    else {
        return Ok(());
    };
    let row = usize::from(read_u16_le(data, 0)?) + 1;
    let col = usize::from(read_u16_le(data, 2)?);
    validate_sheet_cell_bounds(row, col)?;
    insert_sparse_cell(rows_map, row, col, CellValue::Text(text));
    Ok(())
}
fn finalize_sparse_rows(
    rows_map: BTreeMap<usize, HashMap<usize, CellValue>>,
) -> Vec<(usize, Vec<CellValue>)> {
    if rows_map.is_empty() {
        return Vec::new();
    }
    let mut rows = Vec::with_capacity(rows_map.len());
    for (row_num, cells) in rows_map {
        let Some(max_col) = cells.keys().copied().max() else {
            rows.push((row_num, Vec::new()));
            continue;
        };
        let mut row_values = vec![CellValue::Empty; max_col + 1];
        for (col, value) in cells {
            if let Some(slot) = row_values.get_mut(col) {
                *slot = value;
            }
        }
        rows.push((row_num, row_values));
    }
    rows
}
fn validate_sheet_cell_bounds(row: usize, col: usize) -> Result<()> {
    if row == 0 || row > MAX_XLSX_ROW as usize {
        return Err(err(format!(
            "시트 행 인덱스가 비정상적으로 큽니다: {row} (최대 {MAX_XLSX_ROW})"
        )));
    }
    if col >= MAX_XLSX_COL {
        return Err(err(format!(
            "시트 열 인덱스가 비정상적으로 큽니다: {}",
            col + 1
        )));
    }
    Ok(())
}
fn insert_sparse_cell(
    rows_map: &mut BTreeMap<usize, HashMap<usize, CellValue>>,
    row: usize,
    col: usize,
    value: CellValue,
) {
    rows_map.entry(row).or_default().insert(col, value);
}
fn parse_biff8_label(data: &[u8], code_page: Option<u16>) -> Result<Option<String>> {
    if data.len() < 3 {
        return Ok(None);
    }
    let cch = usize::from(read_u16_le(data, 0)?);
    let Some(flags) = data.get(2) else {
        return Ok(None);
    };
    let high_byte = (flags & 0x01) != 0;
    let byte_len = if high_byte {
        cch.saturating_mul(2)
    } else {
        cch
    };
    if data.len() < 3 + byte_len {
        return Ok(None);
    }
    let text_bytes = data
        .get(3..3 + byte_len)
        .ok_or_else(|| err("LABEL 문자열 범위 오류"))?;
    if high_byte {
        Ok(Some(decode_utf16_le(text_bytes)))
    } else {
        Ok(Some(super::super::text_decode::decode_single_byte_text(
            text_bytes, code_page,
        )?))
    }
}
fn decode_rk_number(rk: u32) -> f64 {
    let div100 = (rk & 0x01) != 0;
    let is_int = (rk & 0x02) != 0;
    let mut value = if is_int {
        let signed = rk.cast_signed() >> 2;
        f64::from(signed)
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
        let b0 = bytes.get(i).copied().unwrap_or_default();
        let b1 = bytes.get(i + 1).copied().unwrap_or_default();
        data.push(u16::from_le_bytes([b0, b1]));
        i += 2;
    }
    String::from_utf16_lossy(&data)
}
fn read_u16_le(bytes: &[u8], offset: usize) -> Result<u16> {
    let arr = bytes
        .get(offset..offset + 2)
        .and_then(|s| s.as_array::<2>())
        .ok_or_else(|| err(format!("u16 read out of range at {offset}")))?;
    Ok(u16::from_le_bytes(*arr))
}
fn read_u32_le(bytes: &[u8], offset: usize) -> Result<u32> {
    let arr = bytes
        .get(offset..offset + 4)
        .and_then(|s| s.as_array::<4>())
        .ok_or_else(|| err(format!("u32 read out of range at {offset}")))?;
    Ok(u32::from_le_bytes(*arr))
}
fn read_u64_le(bytes: &[u8], offset: usize) -> Result<u64> {
    let arr = bytes
        .get(offset..offset + 8)
        .and_then(|s| s.as_array::<8>())
        .ok_or_else(|| err(format!("u64 read out of range at {offset}")))?;
    Ok(u64::from_le_bytes(*arr))
}
