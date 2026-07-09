use crate::{
    diagnostic::{Result, err, err_with_source, path_context_message, prefixed_message},
    sheet_util::parse_i32_str,
};
use alloc::collections::BTreeMap;
use core::{char::decode_utf16, fmt::Display, range::Range};
use std::{fs::File, io::Read as _, path::Path};
const CFB_SIGNATURE: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
const CFB_FREE_SECT: u32 = 0xFFFF_FFFF;
const CFB_END_OF_CHAIN: u32 = 0xFFFF_FFFE;
const CFB_FAT_SECT: u32 = 0xFFFF_FFFD;
const CFB_DIFAT_SECT: u32 = 0xFFFF_FFFC;
const CFB_OBJECT_UNUSED: u8 = 0;
const CFB_OBJECT_STORAGE: u8 = 1;
const CFB_OBJECT_STREAM: u8 = 2;
const CFB_OBJECT_ROOT_STORAGE: u8 = 5;
const MAX_XLS_FILE_SIZE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SOURCE_ROW: u32 = 50_000;
const MAX_SOURCE_COL: usize = 64;
const MAX_UTF8_BYTES_PER_COMPRESSED_XL_CHAR: usize = 2;
const MAX_UTF8_BYTES_PER_UTF16_CODE_UNIT: usize = 3;
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
const SOURCE_COLUMN_COUNT: usize = COL_DIESEL + 1;
#[derive(Debug)]
pub(crate) struct SourceRecord {
    pub address: String,
    pub brand: String,
    pub diesel: Option<i32>,
    pub gasoline: Option<i32>,
    pub name: String,
    pub premium: Option<i32>,
    pub region: String,
    pub self_yn: String,
}
#[derive(Debug)]
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
#[derive(Debug)]
struct BiffBoundSheet {
    offset: usize,
    sheet_type: u8,
}
struct BiffGlobals {
    boundsheet: BiffBoundSheet,
    shared_strings: Vec<String>,
}
struct BiffWorkbookReader<'workbook> {
    workbook_stream: &'workbook [u8],
}
struct CfbDataParser<'data, 'path> {
    data: &'data [u8],
    path: &'path Path,
}
struct CfbDirectoryParser<'stream> {
    dir_stream: &'stream [u8],
}
pub(crate) struct SourceReader<'path> {
    pub path: &'path Path,
}
struct SourceHeaderValidator<'rows, 'strings> {
    rows: &'rows [SourceRowEntry<'strings>],
}
#[derive(Default)]
struct SourceRow<'strings> {
    cells: [Option<&'strings str>; SOURCE_COLUMN_COUNT],
}
struct SourceRowEntry<'strings> {
    row: SourceRow<'strings>,
    row_num: usize,
}
struct BiffRecord<'workbook> {
    data: &'workbook [u8],
    id: u16,
}
struct SstChunks<'workbook> {
    chunks: Vec<&'workbook [u8]>,
    next_offset: usize,
}
struct SstParser<'chunks, 'chunk> {
    chunks: &'chunks [&'chunk [u8]],
}
struct SstChunkReader<'chunks, 'chunk> {
    chunk_index: usize,
    chunks: &'chunks [&'chunk [u8]],
    offset_in_chunk: usize,
}
struct WorksheetCellsParser<'workbook, 'strings> {
    pos: usize,
    rows_map: BTreeMap<usize, SourceRow<'strings>>,
    shared_strings: &'strings [String],
    workbook_stream: &'workbook [u8],
}
impl<'strings> SourceRow<'strings> {
    fn set(&mut self, col: usize, value: &'strings str) -> Result<()> {
        let Some(cell) = self.cells.get_mut(col) else {
            let display_col = col
                .checked_add(1)
                .ok_or_else(|| err("Opinet 고정 소스 열 번호 표시 계산에 실패했습니다."))?;
            return Err(err(prefixed_display_message(
                "Opinet 고정 소스 열 범위 오류: ",
                display_col,
            )));
        };
        *cell = Some(value);
        Ok(())
    }
    fn text(&self, idx: usize) -> Option<&'strings str> {
        self.cells.get(idx).copied().flatten()
    }
}
impl CfbDataParser<'_, '_> {
    fn build_fat_table(&self, sector_size: usize, fat_sector_ids: &[u32]) -> Result<Vec<u32>> {
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
            let sector_idx = sector_id_to_index(*sid, || {
                prefixed_display_message("CFB sector id 변환 실패: ", *sid)
            })?;
            let sector = get_sector_slice_at_index(self.data, sector_size, sector_idx, *sid)?;
            let (chunks, &[]) = sector.as_chunks::<4>() else {
                return Err(err("CFB FAT sector 길이가 4바이트 단위가 아닙니다."));
            };
            fat.extend(
                chunks
                    .iter()
                    .take(entries_per_sector)
                    .map(|chunk| u32::from_le_bytes(*chunk)),
            );
        }
        Ok(fat)
    }
    fn collect_difat_entries(&self) -> Result<Vec<u32>> {
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
        let header_difat = self
            .data
            .get(0x4C..header_difat_end)
            .ok_or_else(|| err("CFB DIFAT 헤더 범위가 손상되었습니다."))?;
        let (header_difat_chunks, &[]) = header_difat.as_chunks::<4>() else {
            return Err(err("CFB DIFAT 헤더 길이가 4바이트 단위가 아닙니다."));
        };
        difat_entries.extend(
            header_difat_chunks
                .iter()
                .map(|chunk| u32::from_le_bytes(*chunk))
                .filter(|&sid| is_regular_sector_id(sid)),
        );
        Ok(difat_entries)
    }
    const fn max_regular_sector_count(&self, sector_size: usize) -> usize {
        let Some(payload) = self.data.len().checked_sub(512) else {
            return 0;
        };
        let Some(sector_count) = payload.checked_div(sector_size) else {
            return 0;
        };
        sector_count
    }
    fn parse_cfb_header(&self) -> Result<CfbHeader> {
        let Some(data) = self.data.first_chunk::<512>() else {
            return Err(err(prefixed_message(
                "유효한 OLE2(CFB) xls 파일이 아닙니다: ",
                self.path.display(),
            )));
        };
        if !data.starts_with(&CFB_SIGNATURE) {
            return Err(err(prefixed_message(
                "유효한 OLE2(CFB) xls 파일이 아닙니다: ",
                self.path.display(),
            )));
        }
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
}
impl CfbDirectoryParser<'_> {
    fn parse_entries(&self) -> Result<Vec<CfbDirectoryEntry>> {
        let (chunks, &[]) = self.dir_stream.as_chunks::<128>() else {
            return Err(err("CFB 디렉터리 stream 길이가 128바이트 단위가 아닙니다."));
        };
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
            if !matches!(
                object_type,
                CFB_OBJECT_UNUSED
                    | CFB_OBJECT_STORAGE
                    | CFB_OBJECT_STREAM
                    | CFB_OBJECT_ROOT_STORAGE
            ) {
                return Err(err(format!(
                    "CFB directory entry object_type이 비정상입니다: {object_type}"
                )));
            }
            let start_sector = read_u32_le(entry, 0x74)?;
            let stream_size = u64::from_le_bytes(read_le_array::<8>(
                entry,
                0x78,
                "u64 read out of range at ",
            )?);
            if stream_size > u64::from(u32::MAX) {
                return Err(err(format!(
                    "CFB directory stream size 상위 32비트가 0이 아닙니다: {stream_size}"
                )));
            }
            let name = if object_type == CFB_OBJECT_UNUSED {
                String::new()
            } else if (2..=64).contains(&name_len) {
                let text_len = name_len
                    .checked_sub(2)
                    .ok_or_else(|| err("CFB 디렉터리 이름 길이 계산 실패"))?;
                let bytes = entry
                    .get(..text_len)
                    .ok_or_else(|| err("CFB 디렉터리 이름 범위 오류"))?;
                let (name_units, &[]) = bytes.as_chunks::<2>() else {
                    return Err(err("UTF-16 문자열 길이가 홀수입니다."));
                };
                let capacity = name_units
                    .len()
                    .checked_mul(3)
                    .ok_or_else(|| err("UTF-16 문자열 용량 계산 실패"))?;
                let mut decoded = String::new();
                decoded.try_reserve_exact(capacity).map_err(|source| {
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
                    decoded.push(item.map_err(|source| {
                        err_with_source("CFB UTF-16 문자열 해석 실패", source)
                    })?);
                }
                decoded
            } else {
                return Err(err(format!(
                    "CFB directory entry 이름 길이가 비정상입니다: object_type={object_type}, name_len={name_len}"
                )));
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
}
impl SourceReader<'_> {
    fn open(&self) -> Result<CfbFile> {
        let data = self.read_file_bytes()?;
        let parser = CfbDataParser {
            data: &data,
            path: self.path,
        };
        let header = parser.parse_cfb_header()?;
        let max_sector_count = parser.max_regular_sector_count(header.sector_size);
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
        let difat_entries = parser.collect_difat_entries()?;
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
        let fat = parser.build_fat_table(header.sector_size, fat_sector_ids)?;
        let dir_stream = read_stream_from_fat_chain(
            &data,
            header.sector_size,
            &fat,
            header.first_dir_sector,
            None,
            "CFB 디렉터리",
        )?;
        let directory = CfbDirectoryParser {
            dir_stream: &dir_stream,
        }
        .parse_entries()?;
        Ok(CfbFile {
            data,
            directory,
            fat,
            mini_stream_cutoff_size: header.mini_stream_cutoff_size,
            sector_size: header.sector_size,
        })
    }
    fn read_file_bytes(&self) -> Result<Vec<u8>> {
        let file = File::open(self.path).map_err(|source| {
            err_with_source(
                path_context_message("xls 파일 열기 실패", self.path),
                source,
            )
        })?;
        let file_size = file
            .metadata()
            .map_err(|source| {
                err_with_source(
                    path_context_message("xls 파일 메타데이터 조회 실패", self.path),
                    source,
                )
            })?
            .len();
        if file_size > MAX_XLS_FILE_SIZE_BYTES {
            return Err(err(format!(
                "xls 파일이 너무 큽니다: {} ({file_size} bytes, 최대 {MAX_XLS_FILE_SIZE_BYTES} bytes)",
                self.path.display()
            )));
        }
        let data_len = usize::try_from(file_size)
            .map_err(|source| err_with_source("xls 파일 크기 변환 실패", source))?;
        let mut data = Vec::new();
        data.try_reserve_exact(data_len)
            .map_err(|source| err_with_source("xls 파일 메모리 확보 실패", source))?;
        let read_limit = MAX_XLS_FILE_SIZE_BYTES
            .checked_add(1)
            .ok_or_else(|| err("xls 파일 읽기 한도 계산 실패"))?;
        let mut limited = file.take(read_limit);
        limited.read_to_end(&mut data).map_err(|source| {
            err_with_source(
                path_context_message("xls 파일 읽기 실패", self.path),
                source,
            )
        })?;
        if u64::try_from(data.len()).is_ok_and(|actual| actual > MAX_XLS_FILE_SIZE_BYTES) {
            return Err(err(format!(
                "xls 파일이 너무 큽니다: {} (최대 {MAX_XLS_FILE_SIZE_BYTES} bytes)",
                self.path.display()
            )));
        }
        if data.len() != data_len {
            return Err(err(format!(
                "xls 파일이 읽는 중 변경되었습니다: {}",
                self.path.display()
            )));
        }
        Ok(data)
    }
    pub(crate) fn read_xls_source(&self) -> Result<Vec<SourceRecord>> {
        let cfb = self.open()?;
        let workbook = cfb.read_stream_by_name("Workbook")?;
        let biff = BiffWorkbookReader {
            workbook_stream: &workbook,
        };
        let globals = biff.parse_globals()?;
        let sheet = &globals.boundsheet;
        if sheet.sheet_type != 0 {
            return Err(err(prefixed_display_message(
                "Opinet 고정 소스에서 예상하지 않은 sheet type: ",
                sheet.sheet_type,
            )));
        }
        let rows = biff.parse_worksheet_cells(sheet.offset, &globals.shared_strings)?;
        SourceHeaderValidator { rows: &rows }.validate()?;
        let data_row_capacity = rows.len();
        let mut records: Vec<SourceRecord> = Vec::new();
        records
            .try_reserve_exact(data_row_capacity)
            .map_err(|source| {
                err_with_source(
                    format!("소스 레코드 목록 메모리 확보 실패: {data_row_capacity} rows"),
                    source,
                )
            })?;
        for entry in rows {
            if entry.row_num < SOURCE_FIRST_DATA_ROW {
                continue;
            }
            let row = entry.row;
            let address_text = row_text_trimmed(&row, COL_ADDRESS);
            if address_text.is_empty() {
                continue;
            }
            let address = copy_text(address_text, "소스 주소 메모리 확보 실패")?;
            let diesel = normalize_fuel_price(row_i32(&row, COL_DIESEL));
            let gasoline = normalize_fuel_price(row_i32(&row, COL_GASOLINE));
            let premium = normalize_fuel_price(row_i32(&row, COL_PREMIUM));
            records.push(SourceRecord {
                address,
                brand: row_text_owned(&row, COL_BRAND, "소스 브랜드 메모리 확보 실패")?,
                diesel,
                gasoline,
                name: row_text_owned(&row, COL_NAME, "소스 상호명 메모리 확보 실패")?,
                premium,
                region: row_text_owned(&row, COL_REGION, "소스 지역 메모리 확보 실패")?,
                self_yn: row_text_owned(&row, COL_SELF_YN, "소스 셀프 여부 메모리 확보 실패")?,
            });
        }
        if records.is_empty() {
            return Err(err("xls 시트에서 유효한 소스 데이터를 찾지 못했습니다."));
        }
        Ok(records)
    }
}
impl CfbFile {
    fn read_stream_by_name(&self, name: &str) -> Result<Vec<u8>> {
        let entry = self
            .directory
            .iter()
            .find(|entry| entry.object_type == CFB_OBJECT_STREAM && entry.name == name)
            .ok_or_else(|| {
                err(prefixed_display_message(
                    "CFB stream을 찾지 못했습니다: ",
                    name,
                ))
            })?;
        if entry.stream_size < u64::from(self.mini_stream_cutoff_size)
            && is_regular_sector_id(entry.start_sector)
        {
            return Err(err(prefixed_display_message(
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
impl SstChunkReader<'_, '_> {
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
    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.ensure_available()?;
        let chunk = self
            .chunks
            .get(self.chunk_index)
            .ok_or_else(|| err("SST chunk 접근 범위 오류"))?;
        if let Some(bytes) = chunk
            .get(self.offset_in_chunk..)
            .and_then(|remaining| remaining.first_chunk::<N>())
        {
            self.offset_in_chunk = self
                .offset_in_chunk
                .checked_add(N)
                .ok_or_else(|| err("SST chunk offset overflow가 발생했습니다."))?;
            return Ok(*bytes);
        }
        let mut out = [0_u8; N];
        for byte in &mut out {
            *byte = self.read_u8()?;
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
            let start = self.offset_in_chunk;
            if start > chunk.len() {
                return Err(err("SST 문자열 slice 시작 범위 오류"));
            }
            let end = start
                .checked_add(byte_len)
                .ok_or_else(|| err("SST chunk offset overflow가 발생했습니다."))?;
            let Some(bytes) = chunk.get(Range { start, end }) else {
                return Err(err("SST 문자열 slice 길이 오류"));
            };
            let max_utf8_bytes_per_unit = if high_byte {
                MAX_UTF8_BYTES_PER_UTF16_CODE_UNIT
            } else {
                MAX_UTF8_BYTES_PER_COMPRESSED_XL_CHAR
            };
            let additional_capacity = chars_here
                .checked_mul(max_utf8_bytes_per_unit)
                .ok_or_else(|| err("SST 문자열 용량 계산 중 overflow가 발생했습니다."))?;
            let required_capacity = out
                .len()
                .checked_add(additional_capacity)
                .ok_or_else(|| err("SST 문자열 용량 계산 중 overflow가 발생했습니다."))?;
            if out.capacity() < required_capacity {
                let additional = required_capacity
                    .checked_sub(out.len())
                    .ok_or_else(|| err("SST 문자열 용량 계산 중 overflow가 발생했습니다."))?;
                out.try_reserve_exact(additional).map_err(|source| {
                    err_with_source(
                        format!("SST 문자열 메모리 확보 실패: {required_capacity} bytes"),
                        source,
                    )
                })?;
            }
            if high_byte {
                let (chunks, &[]) = bytes.as_chunks::<2>() else {
                    return Err(err("SST UTF-16 문자열 길이가 홀수입니다."));
                };
                for decoded in decode_utf16(chunks.iter().map(|unit| u16::from_le_bytes(*unit))) {
                    out.push(decoded.map_err(|source| {
                        err_with_source("SST UTF-16 문자열 해석 실패", source)
                    })?);
                }
            } else {
                out.extend(bytes.iter().copied().map(char::from));
            }
            self.offset_in_chunk = end;
            remaining = remaining
                .checked_sub(chars_here)
                .ok_or_else(|| err("SST 문자열 남은 문자 수 계산에 실패했습니다."))?;
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
    fn skip_bytes(&mut self, len: usize) -> Result<()> {
        let mut remaining = len;
        while remaining > 0 {
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
            let Some(chunk) = self.chunks.get(self.chunk_index).copied() else {
                return Err(err(format!(
                    "SST data가 예상보다 짧습니다. (요청 {len} bytes)"
                )));
            };
            let remain = chunk
                .len()
                .checked_sub(self.offset_in_chunk)
                .ok_or_else(|| err("SST chunk 남은 길이 계산에 실패했습니다."))?;
            let take = remain.min(remaining);
            self.offset_in_chunk = self
                .offset_in_chunk
                .checked_add(take)
                .ok_or_else(|| err("SST chunk offset overflow가 발생했습니다."))?;
            remaining = remaining
                .checked_sub(take)
                .ok_or_else(|| err("SST skip 남은 길이 계산에 실패했습니다."))?;
        }
        Ok(())
    }
}
impl<'workbook> BiffWorkbookReader<'workbook> {
    fn collect_sst_chunks(
        &self,
        first_chunk: &'workbook [u8],
        first_chunk_end: usize,
    ) -> Result<SstChunks<'workbook>> {
        let mut chunks: Vec<&[u8]> = Vec::new();
        reserve_vec_entries_exact(&mut chunks, 8, "xls SST chunk 목록 메모리 확보 실패")?;
        chunks.push(first_chunk);
        let mut next = first_chunk_end;
        loop {
            let Some(tail) = self.workbook_stream.get(next..) else {
                return Err(err(
                    "xls SST Continue record 위치가 파일 범위를 벗어났습니다.",
                ));
            };
            if tail.is_empty() {
                break;
            }
            let Some((record_header, record_tail)) = tail.split_first_chunk::<4>() else {
                return Err(err("xls SST Continue record header 범위 오류"));
            };
            let next_id = read_u16_le(record_header, 0)?;
            let next_len = usize::from(read_u16_le(record_header, 2)?);
            let next_data_start = next
                .checked_add(4)
                .ok_or_else(|| err("xls SST Continue 데이터 시작 offset 계산에 실패했습니다."))?;
            let next_data_end = next_data_start.checked_add(next_len).ok_or_else(|| {
                err("xls SST Continue 레코드 길이 계산 중 overflow가 발생했습니다.")
            })?;
            let next_data = Range {
                start: next_data_start,
                end: next_data_end,
            };
            if next_id != 0x003C {
                break;
            }
            let chunk = record_tail
                .get(..next_len)
                .ok_or_else(|| err("xls SST Continue 레코드 범위 오류"))?;
            if chunks.len() == chunks.capacity() {
                chunks.try_reserve(1).map_err(|source| {
                    err_with_source(
                        "xls SST chunk 목록 추가 메모리 확보 실패: 1 entries",
                        source,
                    )
                })?;
            }
            chunks.push(chunk);
            next = next_data.end;
        }
        Ok(SstChunks {
            chunks,
            next_offset: next,
        })
    }
    fn parse_globals(&self) -> Result<BiffGlobals> {
        let mut pos = 0_usize;
        let mut boundsheet: Option<BiffBoundSheet> = None;
        let mut code_page: Option<u16> = None;
        let mut shared_strings: Vec<String> = Vec::new();
        let mut saw_eof = false;
        loop {
            let Some(tail) = self.workbook_stream.get(pos..) else {
                return Err(err(
                    "xls BIFF globals record 위치가 파일 범위를 벗어났습니다.",
                ));
            };
            if tail.is_empty() {
                break;
            }
            let Some((record_header, record_tail)) = tail.split_first_chunk::<4>() else {
                return Err(err("xls BIFF globals record header 범위 오류"));
            };
            let record_id = read_u16_le(record_header, 0)?;
            let record_len = usize::from(read_u16_le(record_header, 2)?);
            let data_start = pos
                .checked_add(4)
                .ok_or_else(|| err("xls BIFF globals 데이터 시작 offset 계산에 실패했습니다."))?;
            let data_end = data_start.checked_add(record_len).ok_or_else(|| {
                err("xls BIFF globals 레코드 길이 계산 중 overflow가 발생했습니다.")
            })?;
            let data = record_tail
                .get(..record_len)
                .ok_or_else(|| err("xls BIFF globals 레코드 범위 오류"))?;
            match record_id {
                0x0085 => {
                    let header = data
                        .first_chunk::<8>()
                        .ok_or_else(|| err("xls BoundSheet record가 예상보다 짧습니다."))?;
                    let offset = usize::try_from(read_u32_le(header, 0)?).map_err(|source| {
                        err_with_source("xls BoundSheet offset 변환에 실패했습니다.", source)
                    })?;
                    let sheet_type = header[5];
                    if boundsheet
                        .replace(BiffBoundSheet { offset, sheet_type })
                        .is_some()
                    {
                        return Err(err("Opinet 고정 소스와 다른 worksheet 개수입니다."));
                    }
                }
                0x0042 => {
                    let header = data
                        .first_chunk::<2>()
                        .ok_or_else(|| err("xls CodePage record가 예상보다 짧습니다."))?;
                    code_page = Some(read_u16_le(header, 0)?);
                    if code_page != Some(EXPECTED_BIFF_CODE_PAGE) {
                        return Err(err(format!(
                            "Opinet 고정 소스의 BIFF code page가 예상과 다릅니다: {code_page:?}"
                        )));
                    }
                }
                0x00FC => {
                    let sst_chunks = self.collect_sst_chunks(data, data_end)?;
                    if code_page != Some(EXPECTED_BIFF_CODE_PAGE) {
                        return Err(err(format!(
                            "Opinet 고정 소스의 BIFF code page가 예상과 다릅니다: {code_page:?}"
                        )));
                    }
                    shared_strings = SstParser {
                        chunks: &sst_chunks.chunks,
                    }
                    .parse()?;
                    pos = sst_chunks.next_offset;
                    continue;
                }
                _ => {}
            }
            pos = data_end;
            if record_id == 0x000A {
                saw_eof = true;
                break;
            }
        }
        if !saw_eof {
            return Err(err("xls BIFF globals EOF record를 찾지 못했습니다."));
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
    fn parse_worksheet_cells<'strings>(
        &self,
        sheet_offset: usize,
        shared_strings: &'strings [String],
    ) -> Result<Vec<SourceRowEntry<'strings>>> {
        if sheet_offset >= self.workbook_stream.len() {
            return Err(err(prefixed_display_message(
                "worksheet offset이 workbook stream 범위를 벗어났습니다: ",
                sheet_offset,
            )));
        }
        WorksheetCellsParser {
            pos: sheet_offset,
            rows_map: BTreeMap::new(),
            shared_strings,
            workbook_stream: self.workbook_stream,
        }
        .parse()
    }
}
impl SourceHeaderValidator<'_, '_> {
    fn validate(&self) -> Result<()> {
        struct HeaderExpectation {
            col: usize,
            text: &'static str,
        }
        let Some(header) = self
            .rows
            .iter()
            .find(|entry| entry.row_num == SOURCE_HEADER_ROW)
            .map(|entry| &entry.row)
        else {
            return Err(err("Opinet 소스 헤더 행을 찾지 못했습니다."));
        };
        for expected_header in [
            HeaderExpectation {
                col: COL_REGION,
                text: "지역",
            },
            HeaderExpectation {
                col: COL_NAME,
                text: "상호",
            },
            HeaderExpectation {
                col: COL_ADDRESS,
                text: "주소",
            },
            HeaderExpectation {
                col: COL_BRAND,
                text: "상표",
            },
            HeaderExpectation {
                col: COL_SELF_YN,
                text: "셀프여부",
            },
            HeaderExpectation {
                col: COL_PREMIUM,
                text: "고급휘발유",
            },
            HeaderExpectation {
                col: COL_GASOLINE,
                text: "휘발유",
            },
            HeaderExpectation {
                col: COL_DIESEL,
                text: "경유",
            },
        ] {
            let actual = row_text_trimmed(header, expected_header.col);
            if actual != expected_header.text {
                let col = expected_header
                    .col
                    .checked_add(1)
                    .ok_or_else(|| err("Opinet 소스 헤더 열 번호 표시 계산에 실패했습니다."))?;
                let expected = expected_header.text;
                return Err(err(format!(
                    "Opinet 소스 헤더가 예상과 다릅니다: col={col}, expected={expected}, actual={actual}"
                )));
            }
        }
        Ok(())
    }
}
impl SstParser<'_, '_> {
    fn parse(&self) -> Result<Vec<String>> {
        if self.chunks.is_empty() {
            return Ok(Vec::new());
        }
        let total_chunk_bytes = self.chunks.iter().try_fold(0_usize, |acc, chunk| {
            acc.checked_add(chunk.len())
                .ok_or_else(|| err("SST chunk 총길이 계산 중 overflow가 발생했습니다."))
        })?;
        if total_chunk_bytes < 8 {
            return Err(err("SST 데이터가 비정상적으로 짧습니다."));
        }
        let mut reader = SstChunkReader {
            chunk_index: 0,
            chunks: self.chunks,
            offset_in_chunk: 0,
        };
        reader.read_u32()?;
        let unique_count = usize::try_from(reader.read_u32()?)
            .map_err(|source| err_with_source("SST unique count 변환에 실패했습니다.", source))?;
        let max_unique_count = total_chunk_bytes
            .checked_sub(8)
            .and_then(|value| value.checked_div(3))
            .ok_or_else(|| err("SST unique count 한도 계산에 실패했습니다."))?;
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
                reader.skip_bytes(rich_bytes)?;
            }
            if ext_len > 0 {
                reader.skip_bytes(ext_len)?;
            }
            out.push(value);
        }
        Ok(out)
    }
}
impl<'workbook, 'strings> WorksheetCellsParser<'workbook, 'strings> {
    fn finalize_source_rows(self) -> Result<Vec<SourceRowEntry<'strings>>> {
        if self.rows_map.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows: Vec<SourceRowEntry<'strings>> = Vec::new();
        rows.try_reserve_exact(self.rows_map.len())
            .map_err(|source| {
                let row_count = self.rows_map.len();
                err_with_source(
                    format!("BIFF worksheet 행 메모리 확보 실패: {row_count} rows"),
                    source,
                )
            })?;
        rows.extend(
            self.rows_map
                .into_iter()
                .map(|(row_num, row)| SourceRowEntry { row, row_num }),
        );
        Ok(rows)
    }
    fn handle_label_sst_record(&mut self, data: &[u8]) -> Result<()> {
        let header = data
            .first_chunk::<10>()
            .ok_or_else(|| err("LABELSST record가 예상보다 짧습니다."))?;
        let row = usize::from(read_u16_le(header, 0)?) + 1;
        let col = usize::from(read_u16_le(header, 2)?);
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
            let display_col = col
                .checked_add(1)
                .ok_or_else(|| err("시트 열 번호 표시 계산에 실패했습니다."))?;
            return Err(err(prefixed_display_message(
                "시트 열 인덱스가 비정상적으로 큽니다: ",
                display_col,
            )));
        }
        if row < SOURCE_HEADER_ROW || col >= SOURCE_COLUMN_COUNT {
            return Ok(());
        }
        let idx_u32 = read_u32_le(header, 6)?;
        let idx = usize::try_from(idx_u32)
            .map_err(|source| err_with_source("SST index 변환에 실패했습니다.", source))?;
        let value = self
            .shared_strings
            .get(idx)
            .map(String::as_str)
            .ok_or_else(|| {
                err(format!(
                    "LABELSST가 존재하지 않는 SST index를 참조합니다: {idx}"
                ))
            })?;
        self.rows_map.entry(row).or_default().set(col, value)?;
        Ok(())
    }
    fn handle_record(&mut self, record_id: u16, data: &[u8]) -> Result<bool> {
        match record_id {
            0x00FD => self.handle_label_sst_record(data)?,
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
    fn parse(mut self) -> Result<Vec<SourceRowEntry<'strings>>> {
        let mut saw_eof = false;
        while let Some(record) = self.read_record()? {
            if self.handle_record(record.id, record.data)? {
                saw_eof = true;
                break;
            }
        }
        if !saw_eof {
            return Err(err("xls worksheet EOF record를 찾지 못했습니다."));
        }
        self.finalize_source_rows()
    }
    fn read_record(&mut self) -> Result<Option<BiffRecord<'workbook>>> {
        let tail = self
            .workbook_stream
            .get(self.pos..)
            .ok_or_else(|| err("xls worksheet record 위치가 파일 범위를 벗어났습니다."))?;
        if tail.is_empty() {
            return Ok(None);
        }
        let Some((record_header, record_tail)) = tail.split_first_chunk::<4>() else {
            return Err(err("xls worksheet record header 범위 오류"));
        };
        let record_id = read_u16_le(record_header, 0)?;
        let record_len = usize::from(read_u16_le(record_header, 2)?);
        let data_start = self
            .pos
            .checked_add(4)
            .ok_or_else(|| err("xls worksheet 데이터 시작 offset 계산에 실패했습니다."))?;
        let data_end = data_start
            .checked_add(record_len)
            .ok_or_else(|| err("xls worksheet 레코드 길이 계산 중 overflow가 발생했습니다."))?;
        self.pos = data_end;
        let data = record_tail
            .get(..record_len)
            .ok_or_else(|| err("xls worksheet 레코드 범위 오류"))?;
        Ok(Some(BiffRecord {
            data,
            id: record_id,
        }))
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
fn normalize_fuel_price(value: Option<i32>) -> Option<i32> {
    value.filter(|fuel_price| *fuel_price > 0_i32)
}
fn row_i32(row: &SourceRow<'_>, idx: usize) -> Option<i32> {
    parse_i32_str(row.text(idx)?)
}
fn row_text_trimmed<'strings>(row: &SourceRow<'strings>, idx: usize) -> &'strings str {
    row.text(idx).map_or("", str::trim)
}
fn copy_text(text: &str, context: &'static str) -> Result<String> {
    let mut out = String::new();
    out.try_reserve_exact(text.len())
        .map_err(|source| err_with_source(context, source))?;
    out.push_str(text);
    Ok(out)
}
fn row_text_owned(row: &SourceRow<'_>, idx: usize, context: &'static str) -> Result<String> {
    copy_text(row_text_trimmed(row, idx), context)
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
fn sector_id_to_index(sector_id: u32, message: impl FnOnce() -> String) -> Result<usize> {
    usize::try_from(sector_id).map_err(|source| err_with_source(message(), source))
}
fn get_sector_slice_at_index(
    data: &[u8],
    sector_size: usize,
    sector_idx: usize,
    sector_id: u32,
) -> Result<&[u8]> {
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
    let sector_span = Range { start, end };
    data.get(sector_span).ok_or_else(|| {
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
        if size_limit == Some(0) {
            return Ok(Vec::new());
        }
        return Err(err(format!(
            "FAT stream 시작 sector가 비정상입니다: {stream_name} ({start_sector:#x})"
        )));
    }
    let mut remaining = size_limit
        .map(|limit| {
            let data_len = u64::try_from(data.len())
                .map_err(|source| err_with_source("CFB 파일 크기 변환 실패", source))?;
            if limit > data_len {
                return Err(err(format!(
                    "FAT stream 선언 크기가 파일 크기보다 큽니다: {stream_name}, size={limit}, file_size={data_len}"
                )));
            }
            usize::try_from(limit)
                .map_err(|source| err_with_source("FAT stream 길이 변환 실패", source))
        })
        .transpose()?;
    let mut out = Vec::new();
    out.try_reserve_exact(remaining.unwrap_or(sector_size))
        .map_err(|source| err_with_source("FAT stream 메모리 확보 실패", source))?;
    if remaining.is_none() {
        let mut tortoise = start_sector;
        let mut hare = start_sector;
        loop {
            tortoise = next_fat_sector(fat, tortoise, stream_name)?;
            if tortoise == CFB_END_OF_CHAIN {
                break;
            }
            hare = next_fat_sector(fat, hare, stream_name)?;
            if hare == CFB_END_OF_CHAIN {
                break;
            }
            hare = next_fat_sector(fat, hare, stream_name)?;
            if hare == CFB_END_OF_CHAIN {
                break;
            }
            if tortoise == hare {
                return Err(err(format!(
                    "FAT chain 순환 감지: {stream_name} (sector={hare})"
                )));
            }
        }
    }
    let mut sid = start_sector;
    while sid != CFB_END_OF_CHAIN {
        if remaining == Some(0) {
            return Err(err(format!(
                "FAT stream이 선언 크기 이후에도 계속됩니다: {stream_name} (sector={sid})"
            )));
        }
        let (sid_usize, next_sid) = next_fat_sector_indexed(fat, sid, stream_name)?;
        let sector = get_sector_slice_at_index(data, sector_size, sid_usize, sid)?;
        if let Some(remain) = remaining.as_mut() {
            let take = (*remain).min(sector.len());
            let prefix = sector
                .get(..take)
                .ok_or_else(|| err("sector 슬라이스 범위 오류"))?;
            out.extend_from_slice(prefix);
            *remain = remain
                .checked_sub(take)
                .ok_or_else(|| err("FAT stream 남은 길이 계산에 실패했습니다."))?;
        } else {
            out.try_reserve(sector.len())
                .map_err(|source| err_with_source("FAT stream 추가 메모리 확보 실패", source))?;
            out.extend_from_slice(sector);
        }
        sid = next_sid;
    }
    if let Some(remaining_bytes) = remaining.filter(|bytes| *bytes != 0) {
        return Err(err(format!(
            "FAT stream이 선언 크기보다 짧습니다: {stream_name}, remaining={remaining_bytes}"
        )));
    }
    Ok(out)
}
fn next_fat_sector(fat: &[u32], sector_id: u32, stream_name: &str) -> Result<u32> {
    let (_index, next) = next_fat_sector_indexed(fat, sector_id, stream_name)?;
    Ok(next)
}
fn next_fat_sector_indexed(fat: &[u32], sector_id: u32, stream_name: &str) -> Result<(usize, u32)> {
    if !is_regular_sector_id(sector_id) {
        return Err(err(format!(
            "FAT chain에 잘못된 sector id가 있습니다: {stream_name} ({sector_id:#x})"
        )));
    }
    let index = sector_id_to_index(sector_id, || {
        format!("FAT sector 변환 실패: {stream_name} (sector={sector_id})")
    })?;
    let next = *fat.get(index).ok_or_else(|| {
        err(prefixed_display_message(
            "FAT 인덱스 범위 오류: sector=",
            sector_id,
        ))
    })?;
    if next == CFB_FREE_SECT {
        return Err(err(format!(
            "FAT chain이 free sector를 참조합니다: {stream_name} (sector={next})"
        )));
    }
    Ok((index, next))
}
fn read_u16_le(bytes: &[u8], offset: usize) -> Result<u16> {
    let arr = read_le_array::<2>(bytes, offset, "u16 read out of range at ")?;
    Ok(u16::from_le_bytes(arr))
}
fn read_u32_le(bytes: &[u8], offset: usize) -> Result<u32> {
    let arr = read_le_array::<4>(bytes, offset, "u32 read out of range at ")?;
    Ok(u32::from_le_bytes(arr))
}
fn read_le_array<const N: usize>(
    bytes: &[u8],
    offset: usize,
    out_of_range_prefix: &str,
) -> Result<[u8; N]> {
    let Some(raw_bytes) = bytes.get(offset..).and_then(|tail| tail.first_chunk::<N>()) else {
        return Err(err(prefixed_display_message(out_of_range_prefix, offset)));
    };
    Ok(*raw_bytes)
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
fn sector_size_message(prefix: &str, sector_id: u32, sector_size: usize) -> String {
    format!("{prefix}{sector_id}, size={sector_size}")
}
