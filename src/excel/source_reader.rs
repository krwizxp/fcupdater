use crate::{
    Result, SourceRecord, err, err_with_source, parse_i32_str, path_source_message,
    prefixed_message,
};
use alloc::collections::BTreeMap;
use core::{
    char::{REPLACEMENT_CHARACTER, decode_utf16},
    fmt::Display,
};
use std::{collections::HashSet, fs, path::Path};
const CFB_SIGNATURE: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
const CFB_FREE_SECT: u32 = 0xFFFF_FFFF;
const CFB_END_OF_CHAIN: u32 = 0xFFFF_FFFE;
const CFB_FAT_SECT: u32 = 0xFFFF_FFFD;
const CFB_DIFAT_SECT: u32 = 0xFFFF_FFFC;
const MAX_XLS_FILE_SIZE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SOURCE_ROW: u32 = 50_000;
const MAX_SOURCE_COL: usize = 64;
const EXPECTED_BIFF_CODE_PAGE: u16 = 1200;
const SOURCE_HEADER_ROW: usize = 4;
const SOURCE_FIRST_DATA_ROW: usize = 5;
const COL_REGION: usize = 1;
const COL_NAME: usize = 2;
const COL_ADDRESS: usize = 3;
const COL_BRAND: usize = 4;
const COL_SELF_YN: usize = 5;
const COL_PREMIUM: usize = 6;
const COL_GASOLINE: usize = 7;
const COL_DIESEL: usize = 8;
pub struct SourceReader;
pub trait ReadXlsSource {
    fn read_xls_source(&self, path: &Path) -> Result<Vec<SourceRecord>>;
}
#[derive(Debug, Clone)]
struct CfbDirectoryEntry {
    name: String,
    object_type: u8,
    start_sector: u32,
    stream_size: u64,
}
#[derive(Debug, Clone, Copy)]
struct CfbHeader {
    first_dir_sector: u32,
    mini_stream_cutoff_size: u32,
    num_fat_sectors: u32,
    sector_size: usize,
}
#[derive(Debug)]
struct CfbFile {
    data: Vec<u8>,
    directory: Vec<CfbDirectoryEntry>,
    fat: Vec<u32>,
    mini_stream_cutoff_size: u32,
    sector_size: usize,
}
#[derive(Debug, Clone)]
struct BiffBoundSheet {
    offset: usize,
    sheet_type: u8,
}
struct BiffGlobals {
    boundsheet: BiffBoundSheet,
    shared_strings: Vec<String>,
}
struct SstChunkReader<'chunks, 'chunk> {
    chunk_index: usize,
    chunks: &'chunks [&'chunk [u8]],
    offset_in_chunk: usize,
}
trait CfbFileExt {
    fn build_fat_table(data: &[u8], sector_size: usize, fat_sector_ids: &[u32]) -> Result<Vec<u32>>
    where
        Self: Sized;
    fn collect_difat_entries(data: &[u8]) -> Result<Vec<u32>>
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
    fn parse_directory_entries(dir_stream: &[u8]) -> Result<Vec<CfbDirectoryEntry>>
    where
        Self: Sized;
    fn read_stream_by_name(&self, name: &str) -> Result<Vec<u8>>;
}
trait SstChunkReaderExt<'chunks, 'chunk> {
    fn ensure_available(&mut self) -> Result<()>;
    fn new(chunks: &'chunks [&'chunk [u8]]) -> Self
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
    fn collect_sst_chunks<'workbook>(
        &self,
        workbook_stream: &'workbook [u8],
        first_chunk: &'workbook [u8],
        first_chunk_end: usize,
    ) -> Result<(Vec<&'workbook [u8]>, usize)>;
    fn finalize_sparse_rows(
        &self,
        rows_map: BTreeMap<usize, BTreeMap<usize, String>>,
    ) -> Result<Vec<(usize, Vec<String>)>>;
    fn handle_biff_label_sst_record(
        &self,
        data: &[u8],
        shared_strings: &[String],
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, String>>,
    ) -> Result<()>;
    fn handle_biff_worksheet_record(
        &self,
        record_id: u16,
        data: &[u8],
        shared_strings: &[String],
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, String>>,
    ) -> Result<bool>;
    fn insert_sparse_cell(
        &self,
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, String>>,
        row: usize,
        col: usize,
        value: String,
    );
    fn parse_biff_globals(&self, workbook_stream: &[u8]) -> Result<BiffGlobals>;
    fn parse_biff_worksheet_cells(
        &self,
        workbook_stream: &[u8],
        sheet_offset: usize,
        shared_strings: &[String],
    ) -> Result<Vec<(usize, Vec<String>)>>;
    fn parse_sst_from_chunks(&self, chunks: &[&[u8]]) -> Result<Vec<String>>;
    fn read_biff_record<'stream>(
        &self,
        workbook_stream: &'stream [u8],
        pos: &mut usize,
    ) -> Result<Option<(u16, &'stream [u8])>>;
    fn validate_fixed_header(&self, rows: &[(usize, Vec<String>)]) -> Result<()>;
    fn validate_sheet_cell_bounds(&self, row: usize, col: usize) -> Result<()>;
}
impl ReadXlsSource for SourceReader {
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
                format!("CFB FAT 메모리 확보 실패: {total_entries} entries"),
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
    fn collect_difat_entries(data: &[u8]) -> Result<Vec<u32>> {
        let mut difat_entries: Vec<u32> = Vec::new();
        reserve_vec_entries_exact(
            &mut difat_entries,
            109,
            "CFB DIFAT entry 목록 메모리 확보 실패",
        )?;
        let header_difat_end = 109_usize
            .checked_mul(4)
            .and_then(|delta| 0x4C_usize.checked_add(delta))
            .ok_or_else(|| {
                err("CFB DIFAT 헤더 오프셋 계산 중 overflow가 발생했습니다. (base=76, index=109, stride=4)")
            })?;
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
        Ok(difat_entries)
    }
    fn max_regular_sector_count(data_len: usize, sector_size: usize) -> usize {
        let Some(payload) = data_len.checked_sub(512) else {
            return 0;
        };
        let Some(sector_count) = payload.checked_div(sector_size) else {
            return 0;
        };
        sector_count
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
            return Err(err(format!(
                "xls 파일이 너무 큽니다: {} ({file_size} bytes, 최대 {MAX_XLS_FILE_SIZE_BYTES} bytes)",
                path.display()
            )));
        }
        let data = fs::read(path)
            .map_err(|error| err(path_source_message("xls 파일 읽기 실패", path, error)))?;
        let Some(cfb_header_block) = data.first_chunk::<512>() else {
            return Err(err(prefixed_message(
                "유효한 OLE2(CFB) xls 파일이 아닙니다: ",
                path.display(),
            )));
        };
        if !cfb_header_block.starts_with(&CFB_SIGNATURE) {
            return Err(err(prefixed_message(
                "유효한 OLE2(CFB) xls 파일이 아닙니다: ",
                path.display(),
            )));
        }
        let header = Self::parse_cfb_header(cfb_header_block)?;
        let max_sector_count = Self::max_regular_sector_count(data.len(), header.sector_size);
        if max_sector_count == 0 {
            return Err(err("CFB sector 개수가 비정상적입니다."));
        }
        let declared_fat_sectors = usize::try_from(header.num_fat_sectors).map_err(|source| {
            err_with_source("CFB FAT sector 개수 변환에 실패했습니다.", source)
        })?;
        if declared_fat_sectors > max_sector_count {
            return Err(err(format!(
                "CFB FAT sector 개수가 비정상적으로 큽니다: {declared_fat_sectors} (최대 {max_sector_count})"
            )));
        }
        let difat_entries = Self::collect_difat_entries(&data)?;
        if declared_fat_sectors == 0 || difat_entries.is_empty() {
            return Err(err("CFB FAT 정보를 찾지 못했습니다."));
        }
        if difat_entries.len() < declared_fat_sectors {
            let difat_entry_count = difat_entries.len();
            return Err(err(format!(
                "CFB FAT 엔트리가 부족합니다: 필요 {declared_fat_sectors}, 실제 {difat_entry_count}"
            )));
        }
        let fat_sector_ids = difat_entries
            .get(..declared_fat_sectors)
            .ok_or_else(|| err("CFB FAT entry 범위가 손상되었습니다."))?;
        let fat = Self::build_fat_table(&data, header.sector_size, fat_sector_ids)?;
        let dir_stream = read_stream_from_fat_chain(
            &data,
            header.sector_size,
            &fat,
            header.first_dir_sector,
            None,
            "CFB 디렉터리",
        )?;
        let directory = Self::parse_directory_entries(&dir_stream)?;
        Ok(Self {
            data,
            directory,
            fat,
            mini_stream_cutoff_size: header.mini_stream_cutoff_size,
            sector_size: header.sector_size,
        })
    }
    fn parse_cfb_header(data: &[u8]) -> Result<CfbHeader> {
        let major_version = read_u16_le(data, 0x1A)?;
        let sector_shift = read_u16_le(data, 0x1E)?;
        let mini_sector_shift = read_u16_le(data, 0x20)?;
        if major_version != 3 {
            return Err(err(prefixed_display_message(
                "Opinet 고정 소스에서 예상하지 않은 CFB major version: ",
                major_version,
            )));
        }
        let sector_size = checked_pow2_from_shift(sector_shift, "CFB sector shift")?;
        let mini_sector_size = checked_pow2_from_shift(mini_sector_shift, "CFB mini sector shift")?;
        if sector_size != 512 {
            return Err(err(prefixed_display_message(
                "Opinet 고정 소스에서 예상하지 않은 CFB sector size: ",
                sector_size,
            )));
        }
        if mini_sector_size != 64 {
            return Err(err(prefixed_display_message(
                "지원하지 않는 CFB mini sector size: ",
                mini_sector_size,
            )));
        }
        let num_difat_sectors = read_u32_le(data, 0x48)?;
        if num_difat_sectors != 0 {
            return Err(err(prefixed_display_message(
                "Opinet 고정 소스에서 예상하지 않은 CFB DIFAT sector 개수: ",
                num_difat_sectors,
            )));
        }
        let num_mini_fat_sectors = read_u32_le(data, 0x40)?;
        if num_mini_fat_sectors != 0 {
            return Err(err(prefixed_display_message(
                "Opinet 고정 소스에서 예상하지 않은 CFB mini FAT sector 개수: ",
                num_mini_fat_sectors,
            )));
        }
        Ok(CfbHeader {
            first_dir_sector: read_u32_le(data, 0x30)?,
            mini_stream_cutoff_size: read_u32_le(data, 0x38)?,
            num_fat_sectors: read_u32_le(data, 0x2C)?,
            sector_size,
        })
    }
    fn parse_directory_entries(dir_stream: &[u8]) -> Result<Vec<CfbDirectoryEntry>> {
        let (chunks, _) = dir_stream.as_chunks::<128>();
        let mut entries: Vec<CfbDirectoryEntry> = Vec::new();
        entries.try_reserve_exact(chunks.len()).map_err(|source| {
            let chunk_count = chunks.len();
            err_with_source(
                format!("CFB 디렉터리 메모리 확보 실패: {chunk_count} entries"),
                source,
            )
        })?;
        for entry in chunks {
            let name_len = usize::from(read_u16_le(entry, 0x40)?);
            let object_type = *entry
                .get(0x42)
                .ok_or_else(|| err("CFB 디렉터리 object_type 범위 오류"))?;
            let start_sector = read_u32_le(entry, 0x74)?;
            let stream_size = u64::from_le_bytes(read_le_array::<8>(
                entry,
                0x78,
                "u64 read",
                "u64 read out of range at ",
            )?) & 0xFFFF_FFFF;
            let name = if (2..=64).contains(&name_len) {
                let bytes = entry
                    .get(0..name_len.saturating_sub(2))
                    .ok_or_else(|| err("CFB 디렉터리 이름 범위 오류"))?;
                let (name_units, remainder) = bytes.as_chunks::<2>();
                if !remainder.is_empty() {
                    return Err(err("UTF-16 문자열 길이가 홀수입니다."));
                }
                let capacity = name_units
                    .len()
                    .checked_mul(3)
                    .ok_or_else(|| err("UTF-16 문자열 용량 계산 실패"))?;
                let mut decoded = String::new();
                decoded.try_reserve(capacity).map_err(|source| {
                    err_with_source(
                        format!(
                            "UTF-16 문자열 메모리 확보 실패: {} code units",
                            name_units.len()
                        ),
                        source,
                    )
                })?;
                for item in decode_utf16(name_units.iter().map(|chunk| u16::from_le_bytes(*chunk)))
                {
                    decoded.push(item.unwrap_or(REPLACEMENT_CHARACTER));
                }
                decoded
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
            return Err(err(prefixed_name_message(
                "Opinet 고정 소스에서 예상하지 않은 mini stream입니다: ",
                name,
            )));
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
}
impl SourceReaderBiffExt for SourceReader {
    fn read_xls_source_impl(&self, path: &Path) -> Result<Vec<SourceRecord>> {
        let cfb = <CfbFile as CfbFileExt>::open(path)?;
        let workbook = cfb.read_stream_by_name("Workbook")?;
        let globals = self.parse_biff_globals(&workbook)?;
        let sheet = &globals.boundsheet;
        if sheet.sheet_type != 0 {
            return Err(err(prefixed_display_message(
                "Opinet 고정 소스에서 예상하지 않은 sheet type: ",
                sheet.sheet_type,
            )));
        }
        let rows =
            self.parse_biff_worksheet_cells(&workbook, sheet.offset, &globals.shared_strings)?;
        self.validate_fixed_header(&rows)?;
        let data_row_capacity = rows
            .iter()
            .filter(|row_entry| row_entry.0 >= SOURCE_FIRST_DATA_ROW)
            .count();
        let mut records: Vec<SourceRecord> = Vec::new();
        records
            .try_reserve_exact(data_row_capacity)
            .map_err(|source| {
                err_with_source(
                    format!("소스 레코드 목록 메모리 확보 실패: {data_row_capacity} rows"),
                    source,
                )
            })?;
        for row_entry in &rows {
            if row_entry.0 < SOURCE_FIRST_DATA_ROW {
                continue;
            }
            let row = &row_entry.1;
            let name = row_text(row, COL_NAME);
            let address = row_text(row, COL_ADDRESS);
            if address.trim().is_empty() {
                continue;
            }
            records.push(SourceRecord {
                address,
                brand: row_text(row, COL_BRAND),
                diesel: normalize_fuel_price(row_i32(row, COL_DIESEL)),
                gasoline: normalize_fuel_price(row_i32(row, COL_GASOLINE)),
                name,
                premium: normalize_fuel_price(row_i32(row, COL_PREMIUM)),
                region: row_text(row, COL_REGION),
                self_yn: row_text(row, COL_SELF_YN),
            });
        }
        if records.is_empty() {
            return Err(err("xls 시트에서 유효한 소스 데이터를 찾지 못했습니다."));
        }
        Ok(records)
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
        if self.chunk_index >= self.chunks.len() {
            return Err(err("SST data가 예상보다 짧습니다."));
        }
        Ok(())
    }
    fn new(chunks: &'chunks [&'chunk [u8]]) -> Self {
        Self {
            chunk_index: 0,
            chunks,
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
            return Err(err(format!(
                "SST data가 예상보다 짧습니다. (요청 {len} bytes)"
            )));
        }
        let mut out = Vec::new();
        out.try_reserve_exact(len).map_err(|source| {
            err_with_source(format!("SST 버퍼 메모리 확보 실패: {len} bytes"), source)
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
        let capacity = char_count
            .checked_mul(3)
            .ok_or_else(|| err("SST 문자열 용량 계산 중 overflow가 발생했습니다."))?;
        let mut out = String::new();
        out.try_reserve_exact(capacity).map_err(|source| {
            err_with_source(
                format!("SST 문자열 메모리 확보 실패: {capacity} bytes"),
                source,
            )
        })?;
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
            let Some(available_chars) = available_bytes.checked_div(bytes_per_char) else {
                return Err(err("SST chunk 문자 수 계산에 실패했습니다."));
            };
            let chars_here = available_chars.min(remaining);
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
                let (chunks, _) = bytes.as_chunks::<2>();
                out.extend(
                    decode_utf16(chunks.iter().map(|unit| u16::from_le_bytes(*unit)))
                        .map(|decoded| decoded.unwrap_or(REPLACEMENT_CHARACTER)),
                );
            } else {
                out.extend(bytes.iter().copied().map(char::from));
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
    fn collect_sst_chunks<'workbook>(
        &self,
        workbook_stream: &'workbook [u8],
        first_chunk: &'workbook [u8],
        first_chunk_end: usize,
    ) -> Result<(Vec<&'workbook [u8]>, usize)> {
        let mut chunks: Vec<&[u8]> = Vec::new();
        reserve_vec_entries_exact(&mut chunks, 8, "xls SST chunk 목록 메모리 확보 실패")?;
        chunks.push(first_chunk);
        let mut next = first_chunk_end;
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
            let next_data_start = next
                .checked_add(4)
                .ok_or_else(|| err("xls SST Continue 데이터 시작 offset 계산에 실패했습니다."))?;
            let Some(next_data_end) = next_data_start.checked_add(next_len) else {
                break;
            };
            if next_data_end > workbook_stream.len() || next_id != 0x003C {
                break;
            }
            if let Some(chunk) = workbook_stream.get(next_data_start..next_data_end) {
                chunks.try_reserve(1).map_err(|source| {
                    err_with_source(
                        "xls SST chunk 목록 추가 메모리 확보 실패: 1 entries",
                        source,
                    )
                })?;
                chunks.push(chunk);
            } else {
                break;
            }
            next = next_data_end;
        }
        Ok((chunks, next))
    }
    fn finalize_sparse_rows(
        &self,
        rows_map: BTreeMap<usize, BTreeMap<usize, String>>,
    ) -> Result<Vec<(usize, Vec<String>)>> {
        if rows_map.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows: Vec<(usize, Vec<String>)> = Vec::new();
        rows.try_reserve_exact(rows_map.len()).map_err(|source| {
            let row_count = rows_map.len();
            err_with_source(
                format!("BIFF worksheet 행 메모리 확보 실패: {row_count} rows"),
                source,
            )
        })?;
        for (row_num, cells) in rows_map {
            let Some(max_col) = cells.last_key_value().map(|(&col, _)| col) else {
                rows.push((row_num, Vec::new()));
                continue;
            };
            let row_len = max_col
                .checked_add(1)
                .ok_or_else(|| err("worksheet row 길이 계산 중 overflow가 발생했습니다."))?;
            let mut row_values: Vec<String> = Vec::new();
            row_values.try_reserve_exact(row_len).map_err(|source| {
                err_with_source(
                    format!("BIFF worksheet 셀 메모리 확보 실패: {row_len} cells"),
                    source,
                )
            })?;
            row_values.resize(row_len, String::new());
            for (col, value) in cells {
                if let Some(slot) = row_values.get_mut(col) {
                    *slot = value;
                }
            }
            rows.push((row_num, row_values));
        }
        Ok(rows)
    }
    fn handle_biff_label_sst_record(
        &self,
        data: &[u8],
        shared_strings: &[String],
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, String>>,
    ) -> Result<()> {
        let header = data
            .first_chunk::<10>()
            .ok_or_else(|| err("LABELSST record가 예상보다 짧습니다."))?;
        let row = usize::from(read_u16_le(header, 0)?) + 1;
        let col = usize::from(read_u16_le(header, 2)?);
        self.validate_sheet_cell_bounds(row, col)?;
        let idx_u32 = read_u32_le(header, 6)?;
        let idx = usize::try_from(idx_u32)
            .map_err(|source| err_with_source("SST index 변환에 실패했습니다.", source))?;
        let value = shared_strings.get(idx).cloned().ok_or_else(|| {
            err(format!(
                "LABELSST가 존재하지 않는 SST index를 참조합니다: {idx}"
            ))
        })?;
        self.insert_sparse_cell(rows_map, row, col, value);
        Ok(())
    }
    fn handle_biff_worksheet_record(
        &self,
        record_id: u16,
        data: &[u8],
        shared_strings: &[String],
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, String>>,
    ) -> Result<bool> {
        match record_id {
            0x00FD => self.handle_biff_label_sst_record(data, shared_strings, rows_map)?,
            0x0203 | 0x027E | 0x00BD | 0x0204 => {
                return Err(err(format!(
                    "Opinet 고정 소스에서 예상하지 않은 BIFF cell record입니다: {record_id:#06x}"
                )));
            }
            0x000A => return Ok(true),
            _ => {}
        }
        Ok(false)
    }
    fn insert_sparse_cell(
        &self,
        rows_map: &mut BTreeMap<usize, BTreeMap<usize, String>>,
        row: usize,
        col: usize,
        value: String,
    ) {
        rows_map.entry(row).or_default().insert(col, value);
    }
    fn parse_biff_globals(&self, workbook_stream: &[u8]) -> Result<BiffGlobals> {
        let mut pos = 0_usize;
        let mut boundsheet: Option<BiffBoundSheet> = None;
        let mut code_page: Option<u16> = None;
        let mut shared_strings: Vec<String> = Vec::new();
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
                0x0085 if let Some(header) = data.first_chunk::<8>() => {
                    if boundsheet.is_some() {
                        return Err(err("Opinet 고정 소스와 다른 worksheet 개수입니다."));
                    }
                    let offset = usize::try_from(read_u32_le(header, 0)?).map_err(|source| {
                        err_with_source("xls BoundSheet offset 변환에 실패했습니다.", source)
                    })?;
                    let sheet_type = header[5];
                    boundsheet = Some(BiffBoundSheet { offset, sheet_type });
                }
                0x0042 if let Some(header) = data.first_chunk::<2>() => {
                    code_page = Some(read_u16_le(header, 0)?);
                    if code_page != Some(EXPECTED_BIFF_CODE_PAGE) {
                        return Err(err(format!(
                            "Opinet 고정 소스의 BIFF code page가 예상과 다릅니다: {code_page:?}"
                        )));
                    }
                }
                0x00FC => {
                    let (chunks, next) =
                        self.collect_sst_chunks(workbook_stream, data, data_end)?;
                    if code_page != Some(EXPECTED_BIFF_CODE_PAGE) {
                        return Err(err(format!(
                            "Opinet 고정 소스의 BIFF code page가 예상과 다릅니다: {code_page:?}"
                        )));
                    }
                    shared_strings = self.parse_sst_from_chunks(&chunks)?;
                    pos = next;
                    continue;
                }
                _ => {}
            }
            pos = data_end;
            if record_id == 0x000A && boundsheet.is_some() {
                break;
            }
        }
        let Some(parsed_boundsheet) = boundsheet else {
            return Err(err("xls에서 BoundSheet를 찾지 못했습니다."));
        };
        if code_page != Some(EXPECTED_BIFF_CODE_PAGE) {
            return Err(err(format!(
                "Opinet 고정 소스의 BIFF code page가 예상과 다릅니다: {code_page:?}"
            )));
        }
        if shared_strings.is_empty() {
            return Err(err("Opinet 고정 소스에서 SST를 찾지 못했습니다."));
        }
        Ok(BiffGlobals {
            boundsheet: parsed_boundsheet,
            shared_strings,
        })
    }
    fn parse_biff_worksheet_cells(
        &self,
        workbook_stream: &[u8],
        sheet_offset: usize,
        shared_strings: &[String],
    ) -> Result<Vec<(usize, Vec<String>)>> {
        if sheet_offset >= workbook_stream.len() {
            return Err(err(prefixed_display_message(
                "worksheet offset이 workbook stream 범위를 벗어났습니다: ",
                sheet_offset,
            )));
        }
        let mut pos = sheet_offset;
        let mut rows_map: BTreeMap<usize, BTreeMap<usize, String>> = BTreeMap::new();
        while let Some((record_id, data)) = self.read_biff_record(workbook_stream, &mut pos)? {
            if self.handle_biff_worksheet_record(record_id, data, shared_strings, &mut rows_map)? {
                break;
            }
        }
        self.finalize_sparse_rows(rows_map)
    }
    fn parse_sst_from_chunks(&self, chunks: &[&[u8]]) -> Result<Vec<String>> {
        if chunks.is_empty() {
            return Ok(Vec::new());
        }
        let total_chunk_bytes = chunks.iter().try_fold(0_usize, |acc, chunk| {
            acc.checked_add(chunk.len())
                .ok_or_else(|| err("SST chunk 총길이 계산 중 overflow가 발생했습니다."))
        })?;
        if total_chunk_bytes < 8 {
            return Err(err("SST 데이터가 비정상적으로 짧습니다."));
        }
        let mut reader = <SstChunkReader<'_, '_> as SstChunkReaderExt<'_, '_>>::new(chunks);
        let _total_count = reader.read_u32()?;
        let unique_count = usize::try_from(reader.read_u32()?)
            .map_err(|source| err_with_source("SST unique count 변환에 실패했습니다.", source))?;
        let Some(max_unique_count) = total_chunk_bytes.saturating_sub(8).checked_div(3) else {
            return Err(err("SST unique count 한도 계산에 실패했습니다."));
        };
        if unique_count > max_unique_count {
            return Err(err(display_limit_message(
                "SST unique count가 비정상적으로 큽니다: ",
                unique_count,
                "최대 ",
                max_unique_count,
            )));
        }
        let mut out: Vec<String> = Vec::new();
        out.try_reserve_exact(unique_count).map_err(|source| {
            err_with_source(
                format!("SST 문자열 테이블 메모리 확보 실패: {unique_count} entries"),
                source,
            )
        })?;
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
    fn validate_fixed_header(&self, rows: &[(usize, Vec<String>)]) -> Result<()> {
        let Some(header) = rows
            .iter()
            .find_map(|row_entry| (row_entry.0 == SOURCE_HEADER_ROW).then_some(&row_entry.1))
        else {
            return Err(err("Opinet 소스 헤더 행을 찾지 못했습니다."));
        };
        for (col, expected) in [
            (COL_REGION, "지역"),
            (COL_NAME, "상호"),
            (COL_ADDRESS, "주소"),
            (COL_BRAND, "상표"),
            (COL_SELF_YN, "셀프여부"),
            (COL_PREMIUM, "고급휘발유"),
            (COL_GASOLINE, "휘발유"),
            (COL_DIESEL, "경유"),
        ] {
            let actual = row_text(header, col);
            if actual != expected {
                return Err(err(format!(
                    "Opinet 소스 헤더가 예상과 다릅니다: col={}, expected={expected}, actual={actual}",
                    col.saturating_add(1)
                )));
            }
        }
        Ok(())
    }
    fn validate_sheet_cell_bounds(&self, row: usize, col: usize) -> Result<()> {
        let row_u32 = u32::try_from(row).map_err(|source| {
            err_with_source(
                display_limit_message(
                    "시트 행 인덱스가 비정상적으로 큽니다: ",
                    row,
                    "최대 ",
                    MAX_SOURCE_ROW,
                ),
                source,
            )
        })?;
        if row_u32 == 0 || row_u32 > MAX_SOURCE_ROW {
            return Err(err(display_limit_message(
                "시트 행 인덱스가 비정상적으로 큽니다: ",
                row,
                "최대 ",
                MAX_SOURCE_ROW,
            )));
        }
        if col >= MAX_SOURCE_COL {
            return Err(err(prefixed_display_message(
                "시트 열 인덱스가 비정상적으로 큽니다: ",
                col.saturating_add(1),
            )));
        }
        Ok(())
    }
}
fn reserve_vec_entries_exact<T>(
    values: &mut Vec<T>,
    additional: usize,
    context: &str,
) -> Result<()> {
    values
        .try_reserve_exact(additional)
        .map_err(|source| err_with_source(format!("{context}: {additional} entries"), source))
}
fn reserve_seen_set(
    seen: &mut HashSet<u32>,
    additional: usize,
    context: &str,
    stream_name: &str,
) -> Result<()> {
    seen.try_reserve(additional).map_err(|source| {
        err_with_source(
            format!("{context}: {additional} entries ({stream_name})"),
            source,
        )
    })
}
fn normalize_fuel_price(value: Option<i32>) -> Option<i32> {
    value.filter(|fuel_price| *fuel_price > 0_i32)
}
fn row_i32(row: &[String], idx: usize) -> Option<i32> {
    parse_i32_str(row.get(idx)?)
}
fn row_text(row: &[String], idx: usize) -> String {
    row.get(idx)
        .map(|cell| cell.trim().to_owned())
        .unwrap_or_default()
}
fn checked_pow2_from_shift(shift: u16, context: &str) -> Result<usize> {
    let shift_u32 = u32::from(shift);
    if shift_u32 >= usize::BITS {
        return Err(err(format!(
            "{context}가 비정상적으로 큽니다: {shift_u32} (usize bits={})",
            usize::BITS
        )));
    }
    1_usize
        .checked_shl(shift_u32)
        .ok_or_else(|| err(format!("{context} 계산에 실패했습니다: shift={shift_u32}")))
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
        return Ok(Vec::new());
    }
    let mut remaining = size_limit
        .map(|limit| {
            usize::try_from(limit).map_err(|source| {
                err_with_source(
                    format!("FAT stream 길이 변환 실패: {limit} ({stream_name})"),
                    source,
                )
            })
        })
        .transpose()?;
    let reserve_size = remaining.unwrap_or(sector_size);
    let mut out = Vec::new();
    out.try_reserve_exact(reserve_size).map_err(|source| {
        err_with_source(
            format!("FAT stream 메모리 확보 실패: {reserve_size} bytes ({stream_name})"),
            source,
        )
    })?;
    let mut sid = start_sector;
    let seen_capacity = fat.len().min(64);
    let mut seen: HashSet<u32> = HashSet::new();
    reserve_seen_set(
        &mut seen,
        seen_capacity,
        "FAT chain 방문 집합 메모리 확보 실패",
        stream_name,
    )?;
    while sid != CFB_END_OF_CHAIN {
        if remaining.is_some_and(|remaining_len| remaining_len == 0) {
            break;
        }
        if !is_regular_sector_id(sid) {
            return Err(err(format!(
                "FAT chain에 잘못된 sector id가 있습니다: {stream_name} ({sid:#x})"
            )));
        }
        reserve_seen_set(
            &mut seen,
            1,
            "FAT chain 방문 집합 추가 메모리 확보 실패",
            stream_name,
        )?;
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
            out.try_reserve(sector.len()).map_err(|source| {
                err_with_source(
                    format!(
                        "FAT stream 추가 메모리 확보 실패: {} bytes ({stream_name})",
                        sector.len()
                    ),
                    source,
                )
            })?;
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
fn read_u16_le(bytes: &[u8], offset: usize) -> Result<u16> {
    let arr = read_le_array::<2>(bytes, offset, "u16 read", "u16 read out of range at ")?;
    Ok(u16::from_le_bytes(arr))
}
fn read_u32_le(bytes: &[u8], offset: usize) -> Result<u32> {
    let arr = read_le_array::<4>(bytes, offset, "u32 read", "u32 read out of range at ")?;
    Ok(u32::from_le_bytes(arr))
}
fn read_le_array<const N: usize>(
    bytes: &[u8],
    offset: usize,
    label: &str,
    out_of_range_prefix: &str,
) -> Result<[u8; N]> {
    let end = offset.checked_add(N).ok_or_else(|| {
        err(format!(
            "{label} 오프셋 계산 중 overflow가 발생했습니다. (offset={offset}, add={N})"
        ))
    })?;
    let arr = bytes
        .get(offset..end)
        .and_then(|slice| slice.as_array::<N>())
        .copied()
        .ok_or_else(|| err(prefixed_display_message(out_of_range_prefix, offset)))?;
    Ok(arr)
}
fn prefixed_display_message(prefix: &str, value: impl Display) -> String {
    format!("{prefix}{value}")
}
fn display_limit_message(
    prefix: &str,
    value: impl Display,
    limit_label: &str,
    limit: impl Display,
) -> String {
    format!("{prefix}{value} ({limit_label}{limit})")
}
fn prefixed_name_message(prefix: &str, name: &str) -> String {
    format!("{prefix}{name}")
}
fn sector_size_message(prefix: &str, sector_id: u32, sector_size: usize) -> String {
    format!("{prefix}{sector_id}, size={sector_size}")
}
fn stream_sid_message(prefix: &str, stream_name: &str, sid: impl Display) -> String {
    format!("{prefix}{stream_name} (sector={sid})")
}
