use super::{
    super::text_decode::decode_single_byte_text, CellValue, MAX_XLSX_COL, MAX_XLSX_ROW,
    build_source_records_from_rows,
};
use crate::{
    Result, err, err_with_source, path_source_message, prefixed_message, push_display,
    source_sync::SourceRecord,
};
use alloc::collections::BTreeMap;
use core::{
    error::Error,
    fmt::{Display, Write as _},
    ops::Div as _,
};
use std::{collections::HashSet, fs, path::Path};
const CFB_SIGNATURE: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
const CFB_FREE_SECT: u32 = 0xFFFF_FFFF;
const CFB_END_OF_CHAIN: u32 = 0xFFFF_FFFE;
const CFB_FAT_SECT: u32 = 0xFFFF_FFFD;
const CFB_DIFAT_SECT: u32 = 0xFFFF_FFFC;
const MAX_XLS_FILE_SIZE_BYTES: u64 = 512 * 1024 * 1024;
pub struct SourceReader;
#[derive(Debug, Clone)]
struct CfbDirectoryEntry {
    name: String,
    object_type: u8,
    start_sector: u32,
    stream_size: u64,
}
#[derive(Debug, Clone, Copy)]
struct CfbHeader {
    first_difat_sector: u32,
    first_dir_sector: u32,
    first_mini_fat_sector: u32,
    major_version: u16,
    mini_sector_size: usize,
    mini_stream_cutoff_size: u32,
    num_difat_sectors: u32,
    num_fat_sectors: u32,
    num_mini_fat_sectors: u32,
    sector_size: usize,
}
#[derive(Debug)]
struct CfbFile {
    data: Vec<u8>,
    directory: Vec<CfbDirectoryEntry>,
    fat: Vec<u32>,
    mini_fat: Vec<u32>,
    mini_sector_size: usize,
    mini_stream_cutoff_size: u32,
    root_stream: Vec<u8>,
    sector_size: usize,
}
#[derive(Debug, Clone)]
struct BiffBoundSheet {
    offset: usize,
    sheet_type: u8,
}
struct BiffGlobals {
    boundsheets: Vec<BiffBoundSheet>,
    code_page: Option<u16>,
    shared_strings: Vec<String>,
}
struct SstChunkReader<'chunks, 'chunk> {
    chunk_index: usize,
    chunks: &'chunks [&'chunk [u8]],
    code_page: Option<u16>,
    offset_in_chunk: usize,
}
pub trait SourceReaderApi {
    fn read_xls_source(&self, path: &Path) -> Result<Vec<SourceRecord>>;
}
trait CfbFileExt {
    fn build_fat_table(data: &[u8], sector_size: usize, fat_sector_ids: &[u32]) -> Result<Vec<u32>>
    where
        Self: Sized;
    fn build_mini_fat_table(
        data: &[u8],
        fat: &[u32],
        header: CfbHeader,
        max_sector_count: usize,
    ) -> Result<Vec<u32>>
    where
        Self: Sized;
    fn collect_difat_entries(
        data: &[u8],
        header: &CfbHeader,
        max_sector_count: usize,
    ) -> Result<Vec<u32>>
    where
        Self: Sized;
    fn max_regular_sector_count(data_len: usize, sector_size: usize) -> usize
    where
        Self: Sized;
    fn open(path: &Path) -> Result<Self>
    where
        Self: Sized;
    fn parse_cfb_header(data: &[u8]) -> Result<CfbHeader>
    where
        Self: Sized;
    fn parse_directory_entries(
        dir_stream: &[u8],
        major_version: u16,
    ) -> Result<Vec<CfbDirectoryEntry>>
    where
        Self: Sized;
    fn read_stream_by_name(&self, name: &str) -> Result<Vec<u8>>;
    fn read_stream_from_mini_chain(
        &self,
        start_mini_sector: u32,
        size: u64,
        name: &str,
    ) -> Result<Vec<u8>>;
}
trait SstChunkReaderExt<'chunks, 'chunk> {
    fn ensure_available(&mut self) -> Result<()>;
    fn new(chunks: &'chunks [&'chunk [u8]], code_page: Option<u16>) -> Self
    where
        Self: Sized;
    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]>;
    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>>;
    fn read_u16(&mut self) -> Result<u16>;
    fn read_u32(&mut self) -> Result<u32>;
    fn read_u8(&mut self) -> Result<u8>;
    fn read_xl_unicode_chars(&mut self, char_count: usize, high_byte: bool) -> Result<String>;
    fn remaining_bytes(&self) -> usize;
}
trait SourceReaderBiffExt {
    fn read_xls_source_impl(&self, path: &Path) -> Result<Vec<SourceRecord>>;
}
trait SourceReaderBiffParseExt {
    fn detect_biff_code_page(&self, workbook_stream: &[u8]) -> Option<u16>;
    fn finalize_sparse_rows(
        &self,
        rows_map: BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Vec<(usize, Vec<CellValue>)>;
    fn handle_biff_label_record(
        &self,
        data: &[u8],
        code_page: Option<u16>,
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Result<()>;
    fn handle_biff_label_sst_record(
        &self,
        data: &[u8],
        shared_strings: &[String],
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Result<()>;
    fn handle_biff_mulrk_record(
        &self,
        data: &[u8],
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Result<()>;
    fn handle_biff_number_record(
        &self,
        data: &[u8],
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Result<()>;
    fn handle_biff_rk_record(
        &self,
        data: &[u8],
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Result<()>;
    fn handle_biff_worksheet_record(
        &self,
        record_id: u16,
        data: &[u8],
        shared_strings: &[String],
        code_page: Option<u16>,
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Result<bool>;
    fn insert_sparse_cell(
        &self,
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
        row: usize,
        col: usize,
        value: CellValue,
    );
    fn parse_biff8_label(&self, data: &[u8], code_page: Option<u16>) -> Result<Option<String>>;
    fn parse_biff_globals(&self, workbook_stream: &[u8]) -> Result<BiffGlobals>;
    fn parse_biff_worksheet_cells(
        &self,
        workbook_stream: &[u8],
        sheet_offset: usize,
        shared_strings: &[String],
        code_page: Option<u16>,
    ) -> Result<Vec<(usize, Vec<CellValue>)>>;
    fn parse_sst_from_chunks(
        &self,
        chunks: &[&[u8]],
        code_page: Option<u16>,
    ) -> Result<Vec<String>>;
    fn read_biff_record<'stream>(
        &self,
        workbook_stream: &'stream [u8],
        pos: &mut usize,
    ) -> Result<Option<(u16, &'stream [u8])>>;
    fn validate_sheet_cell_bounds(&self, row: usize, col: usize) -> Result<()>;
}
impl SourceReaderApi for SourceReader {
    fn read_xls_source(&self, path: &Path) -> Result<Vec<SourceRecord>> {
        self.read_xls_source_impl(path)
    }
}
impl CfbFileExt for CfbFile {
    fn build_fat_table(
        data: &[u8],
        sector_size: usize,
        fat_sector_ids: &[u32],
    ) -> Result<Vec<u32>> {
        let entries_per_sector = sector_size
            .checked_div(4)
            .ok_or_else(|| err("CFB FAT sector 크기가 비정상적입니다."))?;
        if entries_per_sector == 0 {
            return Err(err("CFB FAT sector 크기가 비정상적입니다."));
        }
        let total_entries = fat_sector_ids
            .len()
            .checked_mul(entries_per_sector)
            .ok_or_else(|| err("CFB FAT 엔트리 개수 계산 중 overflow가 발생했습니다."))?;
        let mut fat: Vec<u32> = Vec::new();
        fat.try_reserve_exact(total_entries).map_err(|source| {
            err_with_source(
                {
                    let mut out = String::with_capacity(48);
                    out.push_str("CFB FAT 메모리 확보 실패: ");
                    push_display(&mut out, total_entries);
                    out.push_str(" entries");
                    out
                },
                source,
            )
        })?;
        for sid in fat_sector_ids {
            let sector = get_sector_slice(data, sector_size, *sid)?;
            let (chunks, _) = sector.as_chunks::<4>();
            for chunk in chunks.iter().take(entries_per_sector) {
                fat.push(u32::from_le_bytes(*chunk));
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
            return Ok(Vec::default());
        }
        let mini_fat_sector_count =
            usize::try_from(header.num_mini_fat_sectors).map_err(|source| {
                err_with_source("CFB mini FAT sector 개수 변환에 실패했습니다.", source)
            })?;
        if mini_fat_sector_count > max_sector_count {
            let mut message = String::with_capacity(96);
            message.push_str("CFB mini FAT sector 개수가 비정상적으로 큽니다: ");
            push_display(&mut message, mini_fat_sector_count);
            message.push_str(" (최대 ");
            push_display(&mut message, max_sector_count);
            message.push(')');
            return Err(err(message));
        }
        let sector_size_u64 = u64::try_from(header.sector_size)
            .map_err(|source| err_with_source("CFB sector size 변환에 실패했습니다.", source))?;
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
        let (chunks, _) = mini_fat_bytes.as_chunks::<4>();
        let out: Vec<u32> = chunks.iter().copied().map(u32::from_le_bytes).collect();
        Ok(out)
    }
    fn collect_difat_entries(
        data: &[u8],
        header: &CfbHeader,
        max_sector_count: usize,
    ) -> Result<Vec<u32>> {
        let mut difat_entries: Vec<u32> =
            Vec::with_capacity(109_usize.saturating_add(max_sector_count.min(32)));
        let header_difat_end = checked_index_offset(0x4C, 109_usize, 4, "CFB DIFAT 헤더")?;
        let header_difat = data
            .get(0x4C..header_difat_end)
            .ok_or_else(|| err("CFB DIFAT 헤더 범위가 손상되었습니다."))?;
        let (header_difat_chunks, _) = header_difat.as_chunks::<4>();
        for chunk in header_difat_chunks {
            let sid = u32::from_le_bytes(*chunk);
            if is_regular_sector_id(sid) {
                difat_entries.push(sid);
            }
        }
        if header.num_difat_sectors == 0 {
            return Ok(difat_entries);
        }
        let num_difat_sectors = usize::try_from(header.num_difat_sectors).map_err(|source| {
            err_with_source("CFB DIFAT sector 개수 변환에 실패했습니다.", source)
        })?;
        if num_difat_sectors > max_sector_count {
            return Err(err(display_limit_message(
                "CFB DIFAT sector 개수가 비정상적으로 큽니다: ",
                num_difat_sectors,
                "최대 ",
                max_sector_count,
            )));
        }
        let mut sid = header.first_difat_sector;
        let mut seen: HashSet<u32> = HashSet::with_capacity(num_difat_sectors);
        for _ in 0..num_difat_sectors {
            if !is_regular_sector_id(sid) {
                break;
            }
            if !seen.insert(sid) {
                break;
            }
            let sector = get_sector_slice(data, header.sector_size, sid)?;
            let entries_per_sector = header
                .sector_size
                .checked_div(4)
                .and_then(|word_count| word_count.checked_sub(1))
                .ok_or_else(|| err("CFB DIFAT sector 크기가 비정상적입니다."))?;
            let (chunks, _) = sector.as_chunks::<4>();
            for chunk in chunks.iter().take(entries_per_sector) {
                let entry = u32::from_le_bytes(*chunk);
                if is_regular_sector_id(entry) {
                    difat_entries.push(entry);
                }
            }
            let next_sid_offset =
                checked_index_offset(0, entries_per_sector, 4, "CFB DIFAT sector")?;
            sid = read_u32_le(sector, next_sid_offset)?;
        }
        Ok(difat_entries)
    }
    fn max_regular_sector_count(data_len: usize, sector_size: usize) -> usize {
        data_len
            .checked_sub(512)
            .and_then(|payload| payload.checked_div(sector_size))
            .unwrap_or(0)
    }
    fn open(path: &Path) -> Result<Self> {
        let file_size = fs::metadata(path)
            .map_err(|error| {
                err(path_source_message(
                    "xls 파일 메타데이터 조회 실패",
                    path,
                    error,
                ))
            })?
            .len();
        if file_size > MAX_XLS_FILE_SIZE_BYTES {
            let mut message = String::with_capacity(160);
            message.push_str("xls 파일이 너무 큽니다: ");
            push_display(&mut message, path.display());
            message.push_str(" (");
            push_display(&mut message, file_size);
            message.push_str(" bytes, 최대 ");
            push_display(&mut message, MAX_XLS_FILE_SIZE_BYTES);
            message.push_str(" bytes)");
            return Err(err(message));
        }
        let data = fs::read(path)
            .map_err(|error| err(path_source_message("xls 파일 읽기 실패", path, error)))?;
        if data.len() < 512 || data.get(..CFB_SIGNATURE.len()) != Some(CFB_SIGNATURE.as_slice()) {
            return Err(err(prefixed_message(
                "유효한 OLE2(CFB) xls 파일이 아닙니다: ",
                path.display(),
            )));
        }
        let header = Self::parse_cfb_header(&data)?;
        let max_sector_count = Self::max_regular_sector_count(data.len(), header.sector_size);
        if max_sector_count == 0 {
            return Err(err("CFB sector 개수가 비정상적입니다."));
        }
        let declared_fat_sectors = usize::try_from(header.num_fat_sectors).map_err(|source| {
            err_with_source("CFB FAT sector 개수 변환에 실패했습니다.", source)
        })?;
        if declared_fat_sectors > max_sector_count {
            let mut message = String::with_capacity(96);
            message.push_str("CFB FAT sector 개수가 비정상적으로 큽니다: ");
            push_display(&mut message, declared_fat_sectors);
            message.push_str(" (최대 ");
            push_display(&mut message, max_sector_count);
            message.push(')');
            return Err(err(message));
        }
        let difat_entries = Self::collect_difat_entries(&data, &header, max_sector_count)?;
        let fat_sector_ids: Vec<u32> = difat_entries
            .into_iter()
            .take(declared_fat_sectors)
            .collect();
        if fat_sector_ids.is_empty() {
            return Err(err("CFB FAT 정보를 찾지 못했습니다."));
        }
        if fat_sector_ids.len() < declared_fat_sectors {
            let mut message = String::with_capacity(96);
            message.push_str("CFB FAT 엔트리가 부족합니다: 필요 ");
            push_display(&mut message, declared_fat_sectors);
            message.push_str(", 실제 ");
            push_display(&mut message, fat_sector_ids.len());
            return Err(err(message));
        }
        let fat = Self::build_fat_table(&data, header.sector_size, &fat_sector_ids)?;
        let dir_stream = read_stream_from_fat_chain(
            &data,
            header.sector_size,
            &fat,
            header.first_dir_sector,
            None,
            "CFB 디렉터리",
        )?;
        let directory = Self::parse_directory_entries(&dir_stream, header.major_version)?;
        let root_entry = directory
            .iter()
            .find(|entry| entry.object_type == 5)
            .ok_or_else(|| err("CFB root entry를 찾지 못했습니다."))?;
        let root_stream = read_stream_from_fat_chain(
            &data,
            header.sector_size,
            &fat,
            root_entry.start_sector,
            Some(root_entry.stream_size),
            "CFB root stream",
        )?;
        let mini_fat = Self::build_mini_fat_table(&data, &fat, header, max_sector_count)?;
        Ok(Self {
            data,
            directory,
            fat,
            mini_fat,
            mini_sector_size: header.mini_sector_size,
            mini_stream_cutoff_size: header.mini_stream_cutoff_size,
            root_stream,
            sector_size: header.sector_size,
        })
    }
    fn parse_cfb_header(data: &[u8]) -> Result<CfbHeader> {
        let major_version = read_u16_le(data, 0x1A)?;
        let sector_shift = read_u16_le(data, 0x1E)?;
        let mini_sector_shift = read_u16_le(data, 0x20)?;
        if !matches!(major_version, 3 | 4) {
            return Err(err(prefixed_display_message(
                "지원하지 않는 CFB major version: ",
                major_version,
            )));
        }
        let sector_size = checked_pow2_from_shift(sector_shift, "CFB sector shift")?;
        let mini_sector_size = checked_pow2_from_shift(mini_sector_shift, "CFB mini sector shift")?;
        if !matches!(sector_size, 512 | 4096) {
            return Err(err(prefixed_display_message(
                "지원하지 않는 CFB sector size: ",
                sector_size,
            )));
        }
        if (major_version == 3 && sector_size != 512) || (major_version == 4 && sector_size != 4096)
        {
            let mut message = String::with_capacity(96);
            message.push_str("CFB 헤더 버전/sector size 조합이 유효하지 않습니다: version=");
            push_display(&mut message, major_version);
            message.push_str(", sector=");
            push_display(&mut message, sector_size);
            return Err(err(message));
        }
        if mini_sector_size != 64 {
            return Err(err(prefixed_display_message(
                "지원하지 않는 CFB mini sector size: ",
                mini_sector_size,
            )));
        }
        Ok(CfbHeader {
            first_difat_sector: read_u32_le(data, 0x44)?,
            first_dir_sector: read_u32_le(data, 0x30)?,
            first_mini_fat_sector: read_u32_le(data, 0x3C)?,
            major_version,
            mini_sector_size,
            mini_stream_cutoff_size: read_u32_le(data, 0x38)?,
            num_difat_sectors: read_u32_le(data, 0x48)?,
            num_fat_sectors: read_u32_le(data, 0x2C)?,
            num_mini_fat_sectors: read_u32_le(data, 0x40)?,
            sector_size,
        })
    }
    fn parse_directory_entries(
        dir_stream: &[u8],
        major_version: u16,
    ) -> Result<Vec<CfbDirectoryEntry>> {
        let (chunks, _) = dir_stream.as_chunks::<128>();
        let mut entries = Vec::with_capacity(chunks.len());
        for entry in chunks {
            let name_len = usize::from(read_u16_le(entry, 0x40)?);
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
                    .get(0..name_len.saturating_sub(2))
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
        }
        Ok(entries)
    }
    fn read_stream_by_name(&self, name: &str) -> Result<Vec<u8>> {
        let entry = self
            .directory
            .iter()
            .find(|entry| entry.object_type == 2 && entry.name == name)
            .ok_or_else(|| {
                err(prefixed_name_message(
                    "CFB stream을 찾지 못했습니다: ",
                    name,
                ))
            })?;
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
        let mut remaining = usize::try_from(size).map_err(|source| {
            let mut message = String::with_capacity(name.len().saturating_add(64));
            message.push_str("mini stream 길이 변환 실패: ");
            push_display(&mut message, size);
            message.push_str(" (");
            message.push_str(name);
            message.push(')');
            err_with_source(message, source)
        })?;
        let mut out = Vec::new();
        out.try_reserve_exact(remaining.min(self.root_stream.len()))
            .map_err(|source| {
                let mut message = String::with_capacity(name.len().saturating_add(64));
                message.push_str("mini stream 메모리 확보 실패: ");
                push_display(&mut message, remaining);
                message.push_str(" bytes (");
                message.push_str(name);
                message.push(')');
                err_with_source(message, source)
            })?;
        let mut sid = start_mini_sector;
        let mut seen: HashSet<u32> = HashSet::with_capacity(self.mini_fat.len().min(64));
        while sid != CFB_END_OF_CHAIN && remaining > 0 {
            if !seen.insert(sid) {
                return Err(err(prefixed_name_message(
                    "mini stream chain 순환 감지: ",
                    name,
                )));
            }
            let sid_usize = usize::try_from(sid).map_err(|source| {
                err_with_source(
                    prefixed_display_message("mini stream sector 변환 실패: ", sid),
                    source,
                )
            })?;
            let offset = sid_usize
                .checked_mul(self.mini_sector_size)
                .ok_or_else(|| {
                    err(prefixed_name_paren_display_message(
                        "mini stream offset 계산 overflow: ",
                        name,
                        "sector=",
                        sid,
                    ))
                })?;
            let end = offset.checked_add(self.mini_sector_size).ok_or_else(|| {
                err(prefixed_name_paren_display_message(
                    "mini stream end 계산 overflow: ",
                    name,
                    "sector=",
                    sid,
                ))
            })?;
            if end > self.root_stream.len() {
                return Err(err(prefixed_name_paren_display_message(
                    "mini stream 범위를 벗어났습니다: ",
                    name,
                    "sector=",
                    sid,
                )));
            }
            let chunk = self.root_stream.get(offset..end).ok_or_else(|| {
                err(prefixed_name_paren_display_message(
                    "mini stream 범위를 벗어났습니다: ",
                    name,
                    "sector=",
                    sid,
                ))
            })?;
            let take = remaining.min(chunk.len());
            out.extend_from_slice(
                chunk
                    .get(..take)
                    .ok_or_else(|| err("mini stream 슬라이스 범위 오류"))?,
            );
            remaining = remaining.saturating_sub(take);
            let next = *self
                .mini_fat
                .get(sid_usize)
                .ok_or_else(|| err(prefixed_display_message("mini FAT 인덱스 범위 오류: ", sid)))?;
            if next == CFB_FREE_SECT {
                break;
            }
            sid = next;
        }
        Ok(out)
    }
}
impl SourceReaderBiffExt for SourceReader {
    fn read_xls_source_impl(&self, path: &Path) -> Result<Vec<SourceRecord>> {
        let cfb = <CfbFile as CfbFileExt>::open(path)?;
        let workbook = match cfb.read_stream_by_name("Workbook") {
            Ok(workbook_stream) => workbook_stream,
            Err(_) => cfb.read_stream_by_name("Book")?,
        };
        let globals = self.parse_biff_globals(&workbook)?;
        let mut all = Vec::with_capacity(globals.boundsheets.len().saturating_mul(32));
        let mut last_err: Option<Box<dyn Error + Send + Sync>> = None;
        for sheet in globals
            .boundsheets
            .iter()
            .filter(|sheet| sheet.sheet_type == 0)
        {
            let rows = self.parse_biff_worksheet_cells(
                &workbook,
                sheet.offset,
                &globals.shared_strings,
                globals.code_page,
            )?;
            match build_source_records_from_rows(&rows) {
                Ok(records) if !records.is_empty() => all.extend(records),
                Ok(_) => {}
                Err(error) => last_err = Some(error),
            }
        }
        if !all.is_empty() {
            return Ok(all);
        }
        if let Some(error) = last_err {
            return Err(err(parenthesized_message(
                "xls 시트에서 유효한 소스 데이터를 찾지 못했습니다. (",
                error,
                ")",
            )));
        }
        Err(err("xls 시트에서 유효한 소스 데이터를 찾지 못했습니다."))
    }
}
impl<'chunks, 'chunk> SstChunkReaderExt<'chunks, 'chunk> for SstChunkReader<'chunks, 'chunk> {
    fn ensure_available(&mut self) -> Result<()> {
        while let Some(chunk) = self.chunks.get(self.chunk_index) {
            if self.offset_in_chunk < chunk.len() {
                break;
            }
            self.chunk_index = self
                .chunk_index
                .checked_add(1)
                .ok_or_else(|| err("SST chunk index overflow가 발생했습니다."))?;
            self.offset_in_chunk = 0;
        }
        if self.chunks.get(self.chunk_index).is_none() {
            return Err(err("SST data가 예상보다 짧습니다."));
        }
        Ok(())
    }
    fn new(chunks: &'chunks [&'chunk [u8]], code_page: Option<u16>) -> Self {
        Self {
            chunk_index: 0,
            chunks,
            code_page,
            offset_in_chunk: 0,
        }
    }
    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let mut out = [0_u8; N];
        for byte in &mut out {
            *byte = self.read_u8()?;
        }
        Ok(out)
    }
    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>> {
        if len > self.remaining_bytes() {
            return Err(err(parenthesized_message(
                "SST data가 예상보다 짧습니다. (요청 ",
                len,
                " bytes)",
            )));
        }
        let mut out = Vec::new();
        out.try_reserve_exact(len).map_err(|source| {
            let mut message = String::with_capacity(64);
            message.push_str("SST 버퍼 메모리 확보 실패: ");
            push_display(&mut message, len);
            message.push_str(" bytes");
            err_with_source(message, source)
        })?;
        while out.len() < len {
            self.ensure_available()?;
            let chunk = *self
                .chunks
                .get(self.chunk_index)
                .ok_or_else(|| err("SST chunk 접근 범위 오류"))?;
            let remain = chunk
                .len()
                .checked_sub(self.offset_in_chunk)
                .ok_or_else(|| err("SST chunk 남은 길이 계산에 실패했습니다."))?;
            let need = len.saturating_sub(out.len());
            let take = remain.min(need);
            let end_offset = self
                .offset_in_chunk
                .checked_add(take)
                .ok_or_else(|| err("SST chunk slice 끝 offset 계산에 실패했습니다."))?;
            let bytes = chunk
                .get(self.offset_in_chunk..end_offset)
                .ok_or_else(|| err("SST chunk slice 범위 오류"))?;
            out.extend_from_slice(bytes);
            self.offset_in_chunk = self
                .offset_in_chunk
                .checked_add(take)
                .ok_or_else(|| err("SST chunk offset overflow가 발생했습니다."))?;
        }
        Ok(out)
    }
    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.read_array::<2>()?))
    }
    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_array::<4>()?))
    }
    fn read_u8(&mut self) -> Result<u8> {
        self.ensure_available()?;
        let value = *self
            .chunks
            .get(self.chunk_index)
            .and_then(|chunk| chunk.get(self.offset_in_chunk))
            .ok_or_else(|| err("SST byte 접근 범위 오류"))?;
        self.offset_in_chunk = self
            .offset_in_chunk
            .checked_add(1)
            .ok_or_else(|| err("SST byte offset overflow가 발생했습니다."))?;
        Ok(value)
    }
    fn read_xl_unicode_chars(&mut self, char_count: usize, mut high_byte: bool) -> Result<String> {
        let mut out = String::with_capacity(char_count);
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
            let available_bytes = chunk
                .len()
                .checked_sub(self.offset_in_chunk)
                .ok_or_else(|| err("SST chunk 남은 길이 계산에 실패했습니다."))?;
            let bytes_per_char = if high_byte { 2 } else { 1 };
            let chars_here = available_bytes
                .checked_div(bytes_per_char)
                .unwrap_or(0)
                .min(remaining);
            if chars_here == 0 {
                self.chunk_index = self
                    .chunk_index
                    .checked_add(1)
                    .ok_or_else(|| err("SST chunk index overflow가 발생했습니다."))?;
                self.offset_in_chunk = 0;
                continuation = true;
                continue;
            }
            let byte_len = chars_here
                .checked_mul(bytes_per_char)
                .ok_or_else(|| err("SST 문자열 길이 계산 중 overflow가 발생했습니다."))?;
            let bytes = chunk
                .get(self.offset_in_chunk..self.offset_in_chunk.saturating_add(byte_len))
                .ok_or_else(|| err("SST 문자열 slice 범위 오류"))?;
            if high_byte {
                let decoded = decode_utf16_le(bytes);
                out.push_str(&decoded);
            } else {
                let decoded = decode_single_byte_text(bytes, self.code_page)?;
                out.push_str(&decoded);
            }
            self.offset_in_chunk = self
                .offset_in_chunk
                .checked_add(byte_len)
                .ok_or_else(|| err("SST chunk offset overflow가 발생했습니다."))?;
            remaining = remaining.saturating_sub(chars_here);
            if remaining > 0 && self.offset_in_chunk >= chunk.len() {
                self.chunk_index = self
                    .chunk_index
                    .checked_add(1)
                    .ok_or_else(|| err("SST chunk index overflow가 발생했습니다."))?;
                self.offset_in_chunk = 0;
                continuation = true;
            } else {
                continuation = false;
            }
        }
        Ok(out)
    }
    fn remaining_bytes(&self) -> usize {
        let mut total = 0_usize;
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
}
impl SourceReaderBiffParseExt for SourceReader {
    fn detect_biff_code_page(&self, workbook_stream: &[u8]) -> Option<u16> {
        let mut pos = 0_usize;
        while pos
            .checked_add(4)
            .is_some_and(|next_pos| next_pos <= workbook_stream.len())
        {
            let record_id = read_u16_le(workbook_stream, pos).ok()?;
            let record_len = usize::from(read_u16_le(workbook_stream, pos.checked_add(2)?).ok()?);
            let data_start = pos.checked_add(4)?;
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
    fn finalize_sparse_rows(
        &self,
        rows_map: BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Vec<(usize, Vec<CellValue>)> {
        if rows_map.is_empty() {
            return Vec::default();
        }
        let mut rows = Vec::with_capacity(rows_map.len());
        for (row_num, cells) in rows_map {
            let Some(max_col) = cells.last_key_value().map(|(&col, _)| col) else {
                rows.push((row_num, Vec::default()));
                continue;
            };
            let row_len = max_col
                .checked_add(1)
                .ok_or_else(|| err("worksheet row 길이 계산 중 overflow가 발생했습니다."))
                .unwrap_or(max_col);
            let mut row_values = vec![CellValue::Empty; row_len];
            for (col, value) in cells {
                if let Some(slot) = row_values.get_mut(col) {
                    *slot = value;
                }
            }
            rows.push((row_num, row_values));
        }
        rows
    }
    fn handle_biff_label_record(
        &self,
        data: &[u8],
        code_page: Option<u16>,
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Result<()> {
        if data.len() < 9 {
            return Ok(());
        }
        let Some(text) = self.parse_biff8_label(
            data.get(6..)
                .ok_or_else(|| err("LABEL 레코드 문자열 범위 오류"))?,
            code_page,
        )?
        else {
            return Ok(());
        };
        let row = usize::from(read_u16_le(data, 0)?) + 1;
        let col = usize::from(read_u16_le(data, 2)?);
        self.validate_sheet_cell_bounds(row, col)?;
        self.insert_sparse_cell(rows_map, row, col, CellValue::Text(text));
        Ok(())
    }
    fn handle_biff_label_sst_record(
        &self,
        data: &[u8],
        shared_strings: &[String],
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Result<()> {
        if data.len() < 10 {
            return Ok(());
        }
        let row = usize::from(read_u16_le(data, 0)?) + 1;
        let col = usize::from(read_u16_le(data, 2)?);
        self.validate_sheet_cell_bounds(row, col)?;
        let idx = usize::try_from(read_u32_le(data, 6)?).ok();
        let value = idx
            .and_then(|i| shared_strings.get(i).cloned())
            .unwrap_or_default();
        self.insert_sparse_cell(rows_map, row, col, CellValue::Text(value));
        Ok(())
    }
    fn handle_biff_mulrk_record(
        &self,
        data: &[u8],
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Result<()> {
        if data.len() < 10 {
            return Ok(());
        }
        let row = usize::from(read_u16_le(data, 0)?) + 1;
        let col_first = usize::from(read_u16_le(data, 2)?);
        let Some(last_col_offset) = data.len().checked_sub(2) else {
            return Ok(());
        };
        let col_last = usize::from(read_u16_le(data, last_col_offset)?);
        self.validate_sheet_cell_bounds(row, col_first)?;
        self.validate_sheet_cell_bounds(row, col_last)?;
        let mut offset = 4_usize;
        let mut col = col_first;
        while col <= col_last {
            let Some(next_offset) = offset.checked_add(6) else {
                break;
            };
            if next_offset > last_col_offset {
                break;
            }
            let rk_offset = checked_offset_add(offset, 2, "MULRK 레코드")?;
            let rk = read_u32_le(data, rk_offset)?;
            self.insert_sparse_cell(rows_map, row, col, CellValue::Number(decode_rk_number(rk)));
            offset = next_offset;
            let Some(next_col) = col.checked_add(1) else {
                break;
            };
            col = next_col;
        }
        Ok(())
    }
    fn handle_biff_number_record(
        &self,
        data: &[u8],
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Result<()> {
        if data.len() < 14 {
            return Ok(());
        }
        let row = usize::from(read_u16_le(data, 0)?) + 1;
        let col = usize::from(read_u16_le(data, 2)?);
        self.validate_sheet_cell_bounds(row, col)?;
        self.insert_sparse_cell(
            rows_map,
            row,
            col,
            CellValue::Number(f64::from_bits(read_u64_le(data, 6)?)),
        );
        Ok(())
    }
    fn handle_biff_rk_record(
        &self,
        data: &[u8],
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Result<()> {
        if data.len() < 10 {
            return Ok(());
        }
        let row = usize::from(read_u16_le(data, 0)?) + 1;
        let col = usize::from(read_u16_le(data, 2)?);
        self.validate_sheet_cell_bounds(row, col)?;
        let rk = read_u32_le(data, 6)?;
        self.insert_sparse_cell(rows_map, row, col, CellValue::Number(decode_rk_number(rk)));
        Ok(())
    }
    fn handle_biff_worksheet_record(
        &self,
        record_id: u16,
        data: &[u8],
        shared_strings: &[String],
        code_page: Option<u16>,
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
    ) -> Result<bool> {
        match record_id {
            0x00FD => self.handle_biff_label_sst_record(data, shared_strings, rows_map)?,
            0x0203 => self.handle_biff_number_record(data, rows_map)?,
            0x027E => self.handle_biff_rk_record(data, rows_map)?,
            0x00BD => self.handle_biff_mulrk_record(data, rows_map)?,
            0x0204 => self.handle_biff_label_record(data, code_page, rows_map)?,
            0x000A => return Ok(true),
            _ => {}
        }
        Ok(false)
    }
    fn insert_sparse_cell(
        &self,
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, CellValue>>,
        row: usize,
        col: usize,
        value: CellValue,
    ) {
        rows_map.entry(row).or_default().insert(col, value);
    }
    fn parse_biff8_label(&self, data: &[u8], code_page: Option<u16>) -> Result<Option<String>> {
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
        let Some(text_end) = 3_usize.checked_add(byte_len) else {
            return Ok(None);
        };
        if data.len() < text_end {
            return Ok(None);
        }
        let text_bytes = data
            .get(3..text_end)
            .ok_or_else(|| err("LABEL 문자열 범위 오류"))?;
        if high_byte {
            Ok(Some(decode_utf16_le(text_bytes)))
        } else {
            Ok(Some(decode_single_byte_text(text_bytes, code_page)?))
        }
    }
    fn parse_biff_globals(&self, workbook_stream: &[u8]) -> Result<BiffGlobals> {
        let mut pos = 0_usize;
        let mut boundsheets: Vec<BiffBoundSheet> = Vec::with_capacity(8);
        let mut code_page: Option<u16> = self.detect_biff_code_page(workbook_stream);
        let mut shared_strings = Vec::with_capacity(64);
        while pos
            .checked_add(4)
            .is_some_and(|next_pos| next_pos <= workbook_stream.len())
        {
            let record_id = read_u16_le(workbook_stream, pos)?;
            let record_len = usize::from(read_u16_le(
                workbook_stream,
                pos.checked_add(2).ok_or_else(|| {
                    err("xls BIFF globals 레코드 길이 offset 계산에 실패했습니다.")
                })?,
            )?);
            let data_start = pos
                .checked_add(4)
                .ok_or_else(|| err("xls BIFF globals 데이터 시작 offset 계산에 실패했습니다."))?;
            let data_end = data_start.checked_add(record_len).ok_or_else(|| {
                err("xls BIFF globals 레코드 길이 계산 중 overflow가 발생했습니다.")
            })?;
            if data_end > workbook_stream.len() {
                return Err(err("xls BIFF globals 레코드가 손상되었습니다."));
            }
            let data = workbook_stream
                .get(data_start..data_end)
                .ok_or_else(|| err("xls BIFF globals 레코드 범위 오류"))?;
            match record_id {
                0x0085 if data.len() >= 8 => {
                    let offset = usize::try_from(read_u32_le(data, 0)?).map_err(|source| {
                        err_with_source("xls BoundSheet offset 변환에 실패했습니다.", source)
                    })?;
                    let sheet_type = *data
                        .get(5)
                        .ok_or_else(|| err("xls BoundSheet sheet_type 범위 오류"))?;
                    boundsheets.push(BiffBoundSheet { offset, sheet_type });
                }
                0x0042 if data.len() >= 2 => {
                    code_page = Some(read_u16_le(data, 0)?);
                }
                0x00FC => {
                    let mut chunks: Vec<&[u8]> = Vec::with_capacity(8);
                    chunks.push(data);
                    let mut next = data_end;
                    while next
                        .checked_add(4)
                        .is_some_and(|next_record| next_record <= workbook_stream.len())
                    {
                        let next_id = read_u16_le(workbook_stream, next)?;
                        let next_len = usize::from(read_u16_le(
                            workbook_stream,
                            next.checked_add(2).ok_or_else(|| {
                                err("xls SST Continue 레코드 길이 offset 계산에 실패했습니다.")
                            })?,
                        )?);
                        let next_data_start = next.checked_add(4).ok_or_else(|| {
                            err("xls SST Continue 데이터 시작 offset 계산에 실패했습니다.")
                        })?;
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
                    shared_strings = self.parse_sst_from_chunks(&chunks, code_page)?;
                    pos = next;
                    continue;
                }
                _ => {}
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
            boundsheets,
            code_page,
            shared_strings,
        })
    }
    fn parse_biff_worksheet_cells(
        &self,
        workbook_stream: &[u8],
        sheet_offset: usize,
        shared_strings: &[String],
        code_page: Option<u16>,
    ) -> Result<Vec<(usize, Vec<CellValue>)>> {
        if sheet_offset >= workbook_stream.len() {
            return Err(err(prefixed_display_message(
                "worksheet offset이 workbook stream 범위를 벗어났습니다: ",
                sheet_offset,
            )));
        }
        let mut pos = sheet_offset;
        let mut rows_map: BTreeMap<usize, BTreeMap<usize, CellValue>> = BTreeMap::new();
        while let Some((record_id, data)) = self.read_biff_record(workbook_stream, &mut pos)? {
            if self.handle_biff_worksheet_record(
                record_id,
                data,
                shared_strings,
                code_page,
                &mut rows_map,
            )? {
                break;
            }
        }
        Ok(self.finalize_sparse_rows(rows_map))
    }
    fn parse_sst_from_chunks(
        &self,
        chunks: &[&[u8]],
        code_page: Option<u16>,
    ) -> Result<Vec<String>> {
        if chunks.is_empty() {
            return Ok(Vec::default());
        }
        let total_chunk_bytes = chunks.iter().try_fold(0_usize, |acc, chunk| {
            acc.checked_add(chunk.len())
                .ok_or_else(|| err("SST chunk 총길이 계산 중 overflow가 발생했습니다."))
        })?;
        if total_chunk_bytes < 8 {
            return Err(err("SST 데이터가 비정상적으로 짧습니다."));
        }
        let mut reader =
            <SstChunkReader<'_, '_> as SstChunkReaderExt<'_, '_>>::new(chunks, code_page);
        let _total_count = reader.read_u32()?;
        let unique_count = usize::try_from(reader.read_u32()?)
            .map_err(|source| err_with_source("SST unique count 변환에 실패했습니다.", source))?;
        let max_unique_count = total_chunk_bytes
            .saturating_sub(8)
            .checked_div(3)
            .unwrap_or(0);
        if unique_count > max_unique_count {
            return Err(err(display_limit_message(
                "SST unique count가 비정상적으로 큽니다: ",
                unique_count,
                "최대 ",
                max_unique_count,
            )));
        }
        let mut out = Vec::with_capacity(unique_count);
        for _ in 0..unique_count {
            let char_count = usize::from(reader.read_u16()?);
            let flags = reader.read_u8()?;
            let high_byte = (flags & 0x01) != 0;
            let rich = (flags & 0x08) != 0;
            let ext = (flags & 0x04) != 0;
            let rich_run_count = if rich {
                usize::from(reader.read_u16()?)
            } else {
                0_usize
            };
            let ext_len = if ext {
                usize::try_from(reader.read_u32()?).map_err(|source| {
                    err_with_source("SST ext 길이 변환에 실패했습니다.", source)
                })?
            } else {
                0_usize
            };
            let value = reader.read_xl_unicode_chars(char_count, high_byte)?;
            if rich_run_count > 0 {
                let rich_bytes = rich_run_count
                    .checked_mul(4)
                    .ok_or_else(|| err("SST rich-text 길이 계산 중 overflow가 발생했습니다."))?;
                reader.read_bytes(rich_bytes)?;
            }
            if ext_len > 0 {
                reader.read_bytes(ext_len)?;
            }
            out.push(value);
        }
        Ok(out)
    }
    fn read_biff_record<'stream>(
        &self,
        workbook_stream: &'stream [u8],
        pos: &mut usize,
    ) -> Result<Option<(u16, &'stream [u8])>> {
        if (*pos)
            .checked_add(4)
            .is_none_or(|next_pos| next_pos > workbook_stream.len())
        {
            return Ok(None);
        }
        let record_id = read_u16_le(workbook_stream, *pos)?;
        let record_len = usize::from(read_u16_le(
            workbook_stream,
            (*pos)
                .checked_add(2)
                .ok_or_else(|| err("xls worksheet 레코드 길이 offset 계산에 실패했습니다."))?,
        )?);
        let data_start = (*pos)
            .checked_add(4)
            .ok_or_else(|| err("xls worksheet 데이터 시작 offset 계산에 실패했습니다."))?;
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
    fn validate_sheet_cell_bounds(&self, row: usize, col: usize) -> Result<()> {
        let row_u32 = u32::try_from(row).unwrap_or(MAX_XLSX_ROW.saturating_add(1));
        if row_u32 == 0 || row_u32 > MAX_XLSX_ROW {
            return Err(err(display_limit_message(
                "시트 행 인덱스가 비정상적으로 큽니다: ",
                row,
                "최대 ",
                MAX_XLSX_ROW,
            )));
        }
        if col >= MAX_XLSX_COL {
            return Err(err(prefixed_display_message(
                "시트 열 인덱스가 비정상적으로 큽니다: ",
                col.checked_add(1).unwrap_or(col),
            )));
        }
        Ok(())
    }
}
fn checked_pow2_from_shift(shift: u16, context: &str) -> Result<usize> {
    let shift_u32 = u32::from(shift);
    if shift_u32 >= usize::BITS {
        let mut message = String::with_capacity(context.len().saturating_add(96));
        message.push_str(context);
        message.push_str("가 비정상적으로 큽니다: ");
        push_display(&mut message, shift_u32);
        message.push_str(" (usize bits=");
        push_display(&mut message, usize::BITS);
        message.push(')');
        return Err(err(message));
    }
    1_usize.checked_shl(shift_u32).ok_or_else(|| {
        let mut message = String::with_capacity(context.len().saturating_add(64));
        message.push_str(context);
        message.push_str(" 계산에 실패했습니다: shift=");
        push_display(&mut message, shift_u32);
        err(message)
    })
}
fn checked_offset_add(offset: usize, add: usize, context: &str) -> Result<usize> {
    offset.checked_add(add).ok_or_else(|| {
        let mut message = String::with_capacity(context.len().saturating_add(80));
        message.push_str(context);
        message.push_str(" 오프셋 계산 중 overflow가 발생했습니다. (offset=");
        push_display(&mut message, offset);
        message.push_str(", add=");
        push_display(&mut message, add);
        message.push(')');
        err(message)
    })
}
fn checked_index_offset(base: usize, index: usize, stride: usize, context: &str) -> Result<usize> {
    index
        .checked_mul(stride)
        .and_then(|delta| base.checked_add(delta))
        .ok_or_else(|| {
            let mut message = String::with_capacity(context.len().saturating_add(96));
            message.push_str(context);
            message.push_str(" 오프셋 계산 중 overflow가 발생했습니다. (base=");
            push_display(&mut message, base);
            message.push_str(", index=");
            push_display(&mut message, index);
            message.push_str(", stride=");
            push_display(&mut message, stride);
            message.push(')');
            err(message)
        })
}
const fn is_regular_sector_id(sector_id: u32) -> bool {
    !matches!(
        sector_id,
        CFB_FREE_SECT | CFB_END_OF_CHAIN | CFB_FAT_SECT | CFB_DIFAT_SECT
    )
}
fn get_sector_slice(data: &[u8], sector_size: usize, sector_id: u32) -> Result<&[u8]> {
    let sector_idx = usize::try_from(sector_id).map_err(|source| {
        err_with_source(
            prefixed_display_message("CFB sector id 변환 실패: ", sector_id),
            source,
        )
    })?;
    let start = sector_idx
        .checked_add(1)
        .and_then(|value| value.checked_mul(sector_size))
        .ok_or_else(|| {
            err(sector_size_message(
                "CFB sector offset 계산 overflow: sector=",
                sector_id,
                sector_size,
            ))
        })?;
    let end = start.checked_add(sector_size).ok_or_else(|| {
        err(sector_size_message(
            "CFB sector 끝 offset 계산 overflow: sector=",
            sector_id,
            sector_size,
        ))
    })?;
    data.get(start..end).ok_or_else(|| {
        err(sector_size_message(
            "CFB sector 범위를 벗어났습니다: sector=",
            sector_id,
            sector_size,
        ))
    })
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
        return Ok(Vec::default());
    }
    let mut remaining = size_limit
        .map(|limit| {
            usize::try_from(limit).map_err(|source| {
                let mut message = String::with_capacity(stream_name.len().saturating_add(48));
                message.push_str("FAT stream 길이 변환 실패: ");
                push_display(&mut message, limit);
                message.push_str(" (");
                message.push_str(stream_name);
                message.push(')');
                err_with_source(message, source)
            })
        })
        .transpose()?;
    let reserve_size = remaining.unwrap_or(sector_size);
    let mut out = Vec::new();
    out.try_reserve_exact(reserve_size).map_err(|source| {
        let mut message = String::with_capacity(stream_name.len().saturating_add(64));
        message.push_str("FAT stream 메모리 확보 실패: ");
        push_display(&mut message, reserve_size);
        message.push_str(" bytes (");
        message.push_str(stream_name);
        message.push(')');
        err_with_source(message, source)
    })?;
    let mut sid = start_sector;
    let mut seen: HashSet<u32> = HashSet::with_capacity(fat.len().min(64));
    while sid != CFB_END_OF_CHAIN {
        if matches!(remaining, Some(0)) {
            break;
        }
        if !is_regular_sector_id(sid) {
            let mut message = String::with_capacity(stream_name.len().saturating_add(48));
            message.push_str("FAT chain에 잘못된 sector id가 있습니다: ");
            message.push_str(stream_name);
            message.push_str(" (");
            match write!(&mut message, "{sid:#x}") {
                Ok(()) | Err(_) => {}
            }
            message.push(')');
            return Err(err(message));
        }
        if !seen.insert(sid) {
            return Err(err(stream_sid_message(
                "FAT chain 순환 감지: ",
                stream_name,
                sid,
            )));
        }
        let sector = get_sector_slice(data, sector_size, sid)?;
        if let Some(remain) = remaining.as_mut() {
            let take = (*remain).min(sector.len());
            out.extend_from_slice(
                sector
                    .get(..take)
                    .ok_or_else(|| err("sector 슬라이스 범위 오류"))?,
            );
            *remain = remain.saturating_sub(take);
        } else {
            out.extend_from_slice(sector);
        }
        let sid_usize = usize::try_from(sid).map_err(|source| {
            err_with_source(
                stream_sid_message("FAT sector 변환 실패: ", stream_name, sid),
                source,
            )
        })?;
        let next = *fat.get(sid_usize).ok_or_else(|| {
            err(prefixed_display_message(
                "FAT 인덱스 범위 오류: sector=",
                sid,
            ))
        })?;
        if next == CFB_FREE_SECT {
            break;
        }
        sid = next;
    }
    Ok(out)
}
fn decode_rk_number(rk: u32) -> f64 {
    let div100 = (rk & 0x01) != 0;
    let is_int = (rk & 0x02) != 0;
    let mut value = if is_int {
        let signed = rk.cast_signed() >> 2_i32;
        f64::from(signed)
    } else {
        let bits = u64::from(rk & 0xFFFF_FFFC) << 32_i32;
        f64::from_bits(bits)
    };
    if div100 {
        value = value.div(100.0_f64);
    }
    value
}
fn decode_utf16_le(bytes: &[u8]) -> String {
    let (chunks, _) = bytes.as_chunks::<2>();
    let mut data = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        data.push(u16::from_le_bytes(*chunk));
    }
    String::from_utf16_lossy(&data)
}
fn read_u16_le(bytes: &[u8], offset: usize) -> Result<u16> {
    let arr = read_le_array::<2>(bytes, offset, "u16 read", "u16 read out of range at ")?;
    Ok(u16::from_le_bytes(arr))
}
fn read_u32_le(bytes: &[u8], offset: usize) -> Result<u32> {
    let arr = read_le_array::<4>(bytes, offset, "u32 read", "u32 read out of range at ")?;
    Ok(u32::from_le_bytes(arr))
}
fn read_u64_le(bytes: &[u8], offset: usize) -> Result<u64> {
    let arr = read_le_array::<8>(bytes, offset, "u64 read", "u64 read out of range at ")?;
    Ok(u64::from_le_bytes(arr))
}
fn read_le_array<const N: usize>(
    bytes: &[u8],
    offset: usize,
    label: &str,
    out_of_range_prefix: &str,
) -> Result<[u8; N]> {
    let end = checked_offset_add(offset, N, label)?;
    let arr = bytes
        .get(offset..end)
        .and_then(|slice| slice.first_chunk::<N>())
        .copied()
        .ok_or_else(|| err(prefixed_display_message(out_of_range_prefix, offset)))?;
    Ok(arr)
}
fn prefixed_display_message(prefix: &str, value: impl Display) -> String {
    let mut out = String::with_capacity(prefix.len().saturating_add(32));
    out.push_str(prefix);
    push_display(&mut out, value);
    out
}
fn display_limit_message(
    prefix: &str,
    value: impl Display,
    limit_label: &str,
    limit: impl Display,
) -> String {
    let mut out = String::with_capacity(
        prefix
            .len()
            .saturating_add(limit_label.len())
            .saturating_add(96),
    );
    out.push_str(prefix);
    push_display(&mut out, value);
    out.push_str(" (");
    out.push_str(limit_label);
    push_display(&mut out, limit);
    out.push(')');
    out
}
fn parenthesized_message(prefix: &str, value: impl Display, suffix: &str) -> String {
    let mut out =
        String::with_capacity(prefix.len().saturating_add(suffix.len()).saturating_add(32));
    out.push_str(prefix);
    push_display(&mut out, value);
    out.push_str(suffix);
    out
}
fn prefixed_name_message(prefix: &str, name: &str) -> String {
    let mut out = String::with_capacity(prefix.len().saturating_add(name.len()));
    out.push_str(prefix);
    out.push_str(name);
    out
}
fn prefixed_name_paren_display_message(
    prefix: &str,
    name: &str,
    label: &str,
    value: impl Display,
) -> String {
    let mut out = String::with_capacity(
        prefix
            .len()
            .saturating_add(name.len())
            .saturating_add(label.len())
            .saturating_add(32)
            .saturating_add(" ()".len()),
    );
    out.push_str(prefix);
    out.push_str(name);
    out.push_str(" (");
    out.push_str(label);
    push_display(&mut out, value);
    out.push(')');
    out
}
fn sector_size_message(prefix: &str, sector_id: u32, sector_size: usize) -> String {
    let mut out = String::with_capacity(prefix.len().saturating_add(48));
    out.push_str(prefix);
    push_display(&mut out, sector_id);
    out.push_str(", size=");
    push_display(&mut out, sector_size);
    out
}
fn stream_sid_message(prefix: &str, stream_name: &str, sid: impl Display) -> String {
    let mut out = String::with_capacity(
        prefix
            .len()
            .saturating_add(stream_name.len())
            .saturating_add(40),
    );
    out.push_str(prefix);
    out.push_str(stream_name);
    out.push_str(" (sector=");
    push_display(&mut out, sid);
    out.push(')');
    out
}
