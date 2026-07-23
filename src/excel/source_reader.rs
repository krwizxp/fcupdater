use super::copy_text;
use crate::{
    diagnostic::{AppError, Result, err, err_with_source},
    sheet_util::parse_i32_str,
};
use core::{fmt::Display, range::Range};
const CFB_SIGNATURE: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
const CFB_FREE_SECT: u32 = 0xFFFF_FFFF;
const CFB_END_OF_CHAIN: u32 = 0xFFFF_FFFE;
const CFB_FAT_SECT: u32 = 0xFFFF_FFFD;
const CFB_DIFAT_SECT: u32 = 0xFFFF_FFFC;
const CFB_BYTE_ORDER_LITTLE_ENDIAN: u16 = 0xFFFE;
const CFB_MINI_STREAM_CUTOFF_SIZE: u32 = 4096;
const CFB_SECTOR_SIZE: usize = 512;
const CFB_OBJECT_UNUSED: u8 = 0;
const CFB_OBJECT_STORAGE: u8 = 1;
const CFB_OBJECT_STREAM: u8 = 2;
const CFB_OBJECT_ROOT_STORAGE: u8 = 5;
const BIFF_RECORD_BOF: u16 = 0x0809;
const BIFF_RECORD_CONTINUE: u16 = 0x003C;
const BIFF_RECORD_EOF: u16 = 0x000A;
const BIFF_RECORD_SST: u16 = 0x00FC;
const BIFF_RECORD_LABEL_SST: u16 = 0x00FD;
const BIFF_VERSION_8: u16 = 0x0600;
const BIFF_SUBSTREAM_WORKBOOK_GLOBALS: u16 = 0x0005;
const BIFF_SUBSTREAM_WORKSHEET: u16 = 0x0010;
const BIFF_SST_STRING_FLAGS_MASK: u8 = 0x0D;
const BIFF_SST_CONTINUATION_FLAGS_MASK: u8 = 0x01;
const MAX_SOURCE_ROW: usize = 50_000;
const MAX_SOURCE_COL: usize = 64;
const MAX_SOURCE_CELL_COUNT: usize = MAX_SOURCE_ROW * MAX_SOURCE_COL;
const MAX_UTF8_BYTES_PER_COMPRESSED_XL_CHAR: usize = 2;
const MAX_UTF8_BYTES_PER_UTF16_CODE_UNIT: usize = 3;
const EXPECTED_BIFF_CODE_PAGE: u16 = 1200;
const SOURCE_HEADER_ROW: usize = 4;
const COL_REGION: usize = 1;
const COL_NAME: usize = 2;
const COL_ADDRESS: usize = 3;
const COL_BRAND: usize = 4;
const COL_SELF_YN: usize = 5;
const COL_PREMIUM: usize = 6;
const COL_GASOLINE: usize = 7;
const COL_DIESEL: usize = 8;
const SOURCE_COLUMN_COUNT: usize = COL_DIESEL + 1;
const MIN_FUEL_PRICE: i32 = 100;
const MAX_FUEL_PRICE: i32 = 100_000;
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct FuelValues<T> {
    pub diesel: T,
    pub gasoline: T,
    pub premium: T,
}
impl<T> FuelValues<T> {
    pub(crate) fn map<U>(self, mut map: impl FnMut(T) -> U) -> FuelValues<U> {
        FuelValues {
            diesel: map(self.diesel),
            gasoline: map(self.gasoline),
            premium: map(self.premium),
        }
    }
}
#[derive(Debug)]
pub(crate) struct SourceRecord {
    pub address: String,
    pub brand: String,
    pub fuels: FuelValues<Option<i32>>,
    pub name: String,
    pub region: &'static str,
    pub self_yn: String,
}
pub(crate) struct SourceRecordRef<'record> {
    pub address: &'record str,
    pub brand: &'record str,
    pub fuels: FuelValues<Option<i32>>,
    pub name: &'record str,
    pub region: &'record str,
    pub self_yn: &'record str,
}
impl SourceRecordRef<'_> {
    pub(crate) fn into_owned_with_region(self, region: &'static str) -> Result<SourceRecord> {
        Ok(SourceRecord {
            address: copy_text(self.address, "소스 주소")?,
            brand: copy_text(self.brand, "소스 브랜드")?,
            fuels: self.fuels,
            name: copy_text(self.name, "소스 상호명")?,
            region,
            self_yn: copy_text(self.self_yn, "소스 셀프 여부")?,
        })
    }
}
#[derive(Debug, Clone, Copy)]
struct CfbHeader {
    first_dir_sector: u32,
    num_fat_sectors: u32,
}
#[derive(Debug)]
struct BiffBoundSheet {
    offset: usize,
    sheet_type: u8,
}
struct BiffRecordReader<'workbook> {
    context: &'static str,
    pos: usize,
    workbook_stream: &'workbook [u8],
}
struct BiffSharedStrings {
    declared_total: usize,
    values: Vec<String>,
}
struct BiffWorkbookReader<'workbook> {
    workbook_stream: &'workbook [u8],
}
struct CfbDataParser<'data> {
    data: &'data [u8],
}
pub(crate) struct SourceReader<'source> {
    pub data: &'source [u8],
}
#[derive(Default)]
struct SourceRow<'strings> {
    cells: [Option<&'strings str>; SOURCE_COLUMN_COUNT],
}
struct SstChunkReader<'chunks, 'chunk> {
    chunk_index: usize,
    chunks: &'chunks [&'chunk [u8]],
    offset_in_chunk: usize,
}
impl<'strings> SourceRow<'strings> {
    fn text(&self, idx: usize) -> Option<&'strings str> {
        self.cells.get(idx).copied().flatten()
    }
}
impl CfbDataParser<'_> {
    fn build_fat_table(&self, fat_sector_ids: &[u32]) -> Result<Vec<u32>> {
        let entries_per_sector = CFB_SECTOR_SIZE.div_euclid(4);
        let total_entries = fat_sector_ids.len().wrapping_mul(entries_per_sector);
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
            let sector = get_sector_slice_at_index(self.data, sector_idx, *sid)?;
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
        let header_difat = self
            .data
            .get(0x4C..512)
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
    fn parse_cfb_header(&self) -> Result<CfbHeader> {
        let Some(data) = self.data.first_chunk::<512>() else {
            return Err(err("유효한 OLE2(CFB) Opinet xls 응답이 아닙니다."));
        };
        if !data.starts_with(&CFB_SIGNATURE) {
            return Err(err("유효한 OLE2(CFB) Opinet xls 응답이 아닙니다."));
        }
        let major_version = read_u16_le(data, 0x1A)?;
        let byte_order = read_u16_le(data, 0x1C)?;
        let sector_shift = read_u16_le(data, 0x1E)?;
        let mini_sector_shift = read_u16_le(data, 0x20)?;
        if major_version != 3 {
            return Err(err(prefixed_display_message(
                "Opinet 고정 소스에서 예상하지 않은 CFB major version: ",
                major_version,
            )));
        }
        if byte_order != CFB_BYTE_ORDER_LITTLE_ENDIAN {
            return Err(err(format!(
                "Opinet 고정 소스에서 예상하지 않은 CFB byte order: {byte_order:#06x}"
            )));
        }
        if sector_shift != 9 {
            return Err(err(prefixed_display_message(
                "Opinet 고정 소스에서 예상하지 않은 CFB sector shift: ",
                sector_shift,
            )));
        }
        if mini_sector_shift != 6 {
            return Err(err(prefixed_display_message(
                "지원하지 않는 CFB mini sector shift: ",
                mini_sector_shift,
            )));
        }
        if !self.data.len().is_multiple_of(CFB_SECTOR_SIZE) {
            return Err(err(format!(
                "CFB 파일 길이가 sector 크기 단위가 아닙니다: file_size={}, sector_size={CFB_SECTOR_SIZE}",
                self.data.len()
            )));
        }
        let num_difat_sectors = read_u32_le(data, 0x48)?;
        if num_difat_sectors != 0 {
            return Err(err(prefixed_display_message(
                "Opinet 고정 소스에서 예상하지 않은 CFB DIFAT sector 개수: ",
                num_difat_sectors,
            )));
        }
        let first_difat_sector = read_u32_le(data, 0x44)?;
        if first_difat_sector != CFB_END_OF_CHAIN {
            return Err(err(format!(
                "CFB DIFAT sector가 없지만 시작 sector가 end-of-chain이 아닙니다: {first_difat_sector:#x}"
            )));
        }
        let num_mini_fat_sectors = read_u32_le(data, 0x40)?;
        if num_mini_fat_sectors != 0 {
            return Err(err(prefixed_display_message(
                "Opinet 고정 소스에서 예상하지 않은 CFB mini FAT sector 개수: ",
                num_mini_fat_sectors,
            )));
        }
        let first_mini_fat_sector = read_u32_le(data, 0x3C)?;
        if first_mini_fat_sector != CFB_END_OF_CHAIN {
            return Err(err(format!(
                "CFB mini FAT sector가 없지만 시작 sector가 end-of-chain이 아닙니다: {first_mini_fat_sector:#x}"
            )));
        }
        let mini_stream_cutoff_size = read_u32_le(data, 0x38)?;
        if mini_stream_cutoff_size != CFB_MINI_STREAM_CUTOFF_SIZE {
            return Err(err(format!(
                "Opinet 고정 소스에서 예상하지 않은 CFB mini stream cutoff: {mini_stream_cutoff_size}"
            )));
        }
        Ok(CfbHeader {
            first_dir_sector: read_u32_le(data, 0x30)?,
            num_fat_sectors: read_u32_le(data, 0x2C)?,
        })
    }
    fn read_workbook_stream(&self, header: CfbHeader, fat: &[u32]) -> Result<Vec<u8>> {
        let dir_stream = read_stream_from_fat_chain(
            self.data,
            fat,
            header.first_dir_sector,
            None,
            "CFB 디렉터리",
        )?;
        let (chunks, &[]) = dir_stream.as_chunks::<128>() else {
            return Err(err("CFB 디렉터리 stream 길이가 128바이트 단위가 아닙니다."));
        };
        let mut decoded_name = String::new();
        let mut workbook_entry = None;
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
            if object_type == CFB_OBJECT_UNUSED {
                continue;
            }
            if !(2..=64).contains(&name_len) {
                return Err(err(format!(
                    "CFB directory entry 이름 길이가 비정상입니다: object_type={object_type}, name_len={name_len}"
                )));
            }
            let text_len = name_len.wrapping_sub(2);
            let terminator = read_u16_le(entry, text_len)?;
            if terminator != 0 {
                return Err(err(format!(
                    "CFB directory entry 이름이 NUL로 끝나지 않습니다: object_type={object_type}"
                )));
            }
            let bytes = entry
                .get(..text_len)
                .ok_or_else(|| err("CFB 디렉터리 이름 범위 오류"))?;
            let (name_units, &[]) = bytes.as_chunks::<2>() else {
                return Err(err("UTF-16 문자열 길이가 홀수입니다."));
            };
            decoded_name.clear();
            decoded_name
                .try_reserve_exact(name_units.len().wrapping_mul(3))
                .map_err(|source| {
                    err_with_source(
                        format!(
                            "UTF-16 문자열 메모리 확보 실패: {} code units",
                            name_units.len()
                        ),
                        source,
                    )
                })?;
            for item in
                char::decode_utf16(name_units.iter().map(|chunk| u16::from_le_bytes(*chunk)))
            {
                decoded_name.push(
                    item.map_err(|source| err_with_source("CFB UTF-16 문자열 해석 실패", source))?,
                );
            }
            if object_type == CFB_OBJECT_STREAM
                && decoded_name == "Workbook"
                && workbook_entry
                    .replace((start_sector, stream_size))
                    .is_some()
            {
                return Err(err("CFB stream이 중복 선언되었습니다: Workbook"));
            }
        }
        let (start_sector, stream_size) =
            workbook_entry.ok_or_else(|| err("CFB stream을 찾지 못했습니다: Workbook"))?;
        if stream_size < u64::from(CFB_MINI_STREAM_CUTOFF_SIZE)
            && is_regular_sector_id(start_sector)
        {
            return Err(err(
                "Opinet 고정 소스에서 예상하지 않은 mini stream입니다: Workbook",
            ));
        }
        read_stream_from_fat_chain(self.data, fat, start_sector, Some(stream_size), "Workbook")
    }
}
impl SourceReader<'_> {
    fn open(&self) -> Result<Vec<u8>> {
        let parser = CfbDataParser { data: self.data };
        let header = parser.parse_cfb_header()?;
        let max_sector_count = parser
            .data
            .len()
            .wrapping_sub(CFB_SECTOR_SIZE)
            .div_euclid(CFB_SECTOR_SIZE);
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
        if difat_entries.len() != declared_fat_sectors {
            let difat_entry_count = difat_entries.len();
            return Err(err(format!(
                "CFB FAT 엔트리 개수가 선언과 다릅니다: 선언 {declared_fat_sectors}, 실제 {difat_entry_count}"
            )));
        }
        let fat_sector_ids = difat_entries.as_slice();
        for (index, sector_id) in fat_sector_ids.iter().enumerate() {
            if fat_sector_ids
                .iter()
                .take(index)
                .any(|previous| previous == sector_id)
            {
                return Err(err(prefixed_display_message(
                    "CFB FAT sector가 중복 선언되었습니다: ",
                    sector_id,
                )));
            }
        }
        let fat = parser.build_fat_table(fat_sector_ids)?;
        parser.read_workbook_stream(header, &fat)
    }
    pub(crate) fn visit_xls_source<F>(self, mut visitor: F) -> Result<Result<()>>
    where
        F: FnMut(SourceRecordRef<'_>) -> Result<()>,
    {
        let workbook = self.open()?;
        let biff = BiffWorkbookReader {
            workbook_stream: &workbook,
        };
        let (boundsheet, shared_strings) = biff.parse_globals()?;
        if boundsheet.sheet_type != 0 {
            return Err(err(prefixed_display_message(
                "Opinet 고정 소스에서 예상하지 않은 sheet type: ",
                boundsheet.sheet_type,
            )));
        }
        biff.visit_worksheet(
            boundsheet.offset,
            shared_strings.declared_total,
            &shared_strings.values,
            &mut visitor,
        )
    }
}
impl SstChunkReader<'_, '_> {
    fn ensure_available(&mut self) -> Result<()> {
        while let Some(chunk) = self.chunks.get(self.chunk_index) {
            if self.offset_in_chunk < chunk.len() {
                break;
            }
            self.chunk_index = self.chunk_index.wrapping_add(1);
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
            self.offset_in_chunk = self.offset_in_chunk.wrapping_add(N);
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
        self.offset_in_chunk = self.offset_in_chunk.wrapping_add(1);
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
                validate_sst_option(
                    option,
                    BIFF_SST_CONTINUATION_FLAGS_MASK,
                    "SST Continue 문자열 option",
                )?;
                high_byte = (option & 0x01) != 0;
                self.ensure_available()?;
            }
            let chunk = *self
                .chunks
                .get(self.chunk_index)
                .ok_or_else(|| err("SST chunk 접근 범위 오류"))?;
            let available_bytes = chunk.len().wrapping_sub(self.offset_in_chunk);
            let bytes_per_char = if high_byte { 2 } else { 1 };
            let available_chars = available_bytes.div_euclid(bytes_per_char);
            let chars_here = available_chars.min(remaining);
            if chars_here == 0 {
                self.chunk_index = self.chunk_index.wrapping_add(1);
                self.offset_in_chunk = 0;
                continuation = true;
                continue;
            }
            let byte_len = chars_here.wrapping_mul(bytes_per_char);
            let start = self.offset_in_chunk;
            let end = start.wrapping_add(byte_len);
            let Some(bytes) = chunk.get(Range { start, end }) else {
                return Err(err("SST 문자열 slice 길이 오류"));
            };
            let max_utf8_bytes_per_unit = if high_byte {
                MAX_UTF8_BYTES_PER_UTF16_CODE_UNIT
            } else {
                MAX_UTF8_BYTES_PER_COMPRESSED_XL_CHAR
            };
            let additional_capacity = chars_here.wrapping_mul(max_utf8_bytes_per_unit);
            let required_capacity = out.len().wrapping_add(additional_capacity);
            if out.capacity() < required_capacity {
                out.try_reserve_exact(additional_capacity)
                    .map_err(|source| {
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
                for decoded in
                    char::decode_utf16(chunks.iter().map(|unit| u16::from_le_bytes(*unit)))
                {
                    out.push(decoded.map_err(|source| {
                        err_with_source("SST UTF-16 문자열 해석 실패", source)
                    })?);
                }
            } else {
                out.extend(bytes.iter().copied().map(char::from));
            }
            self.offset_in_chunk = end;
            remaining = remaining.wrapping_sub(chars_here);
            if remaining > 0 && self.offset_in_chunk >= chunk.len() {
                self.chunk_index = self.chunk_index.wrapping_add(1);
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
                self.chunk_index = self.chunk_index.wrapping_add(1);
                self.offset_in_chunk = 0;
            }
            let Some(chunk) = self.chunks.get(self.chunk_index).copied() else {
                return Err(err(format!(
                    "SST data가 예상보다 짧습니다. (요청 {len} bytes)"
                )));
            };
            let remain = chunk.len().wrapping_sub(self.offset_in_chunk);
            let take = remain.min(remaining);
            self.offset_in_chunk = self.offset_in_chunk.wrapping_add(take);
            remaining = remaining.wrapping_sub(take);
        }
        Ok(())
    }
}
impl<'workbook> BiffRecordReader<'workbook> {
    fn next(&mut self) -> Result<Option<(u16, &'workbook [u8])>> {
        let record_start = self.pos;
        let tail = self.workbook_stream.get(record_start..).ok_or_else(|| {
            err(format!(
                "xls {} record 위치가 파일 범위를 벗어났습니다.",
                self.context
            ))
        })?;
        if tail.is_empty() {
            return Ok(None);
        }
        let Some((record_header, record_tail)) = tail.split_first_chunk::<4>() else {
            return Err(err(format!("xls {} record header 범위 오류", self.context)));
        };
        let [id_low, id_high, len_low, len_high] = *record_header;
        let record_id = u16::from_le_bytes([id_low, id_high]);
        let record_len = usize::from(u16::from_le_bytes([len_low, len_high]));
        let data = record_tail
            .get(..record_len)
            .ok_or_else(|| err(format!("xls {} 레코드 범위 오류", self.context)))?;
        self.pos = record_start.wrapping_add(4).wrapping_add(record_len);
        Ok(Some((record_id, data)))
    }
}
impl<'workbook> BiffWorkbookReader<'workbook> {
    fn collect_sst_chunks(
        &self,
        first_chunk: &'workbook [u8],
        first_chunk_end: usize,
    ) -> Result<(Vec<&'workbook [u8]>, usize)> {
        let mut chunks: Vec<&[u8]> = Vec::new();
        reserve_vec_entries_exact(&mut chunks, 8, "xls SST chunk 목록 메모리 확보 실패")?;
        chunks.push(first_chunk);
        let mut records = BiffRecordReader {
            context: "SST Continue",
            pos: first_chunk_end,
            workbook_stream: self.workbook_stream,
        };
        loop {
            let record_start = records.pos;
            let Some((record_id, chunk)) = records.next()? else {
                return Ok((chunks, records.pos));
            };
            if record_id != BIFF_RECORD_CONTINUE {
                return Ok((chunks, record_start));
            }
            if chunks.len() == chunks.capacity() {
                chunks.try_reserve(1).map_err(|source| {
                    err_with_source(
                        "xls SST chunk 목록 추가 메모리 확보 실패: 1 entries",
                        source,
                    )
                })?;
            }
            chunks.push(chunk);
        }
    }
    fn parse_globals(&self) -> Result<(BiffBoundSheet, BiffSharedStrings)> {
        let mut boundsheet: Option<BiffBoundSheet> = None;
        let mut code_page_seen = false;
        let mut records = BiffRecordReader {
            context: "BIFF globals",
            pos: 0,
            workbook_stream: self.workbook_stream,
        };
        let mut shared_strings: Option<BiffSharedStrings> = None;
        loop {
            let first_record = records.pos == 0;
            let Some((record_id, data)) = records.next()? else {
                return Err(err("xls BIFF globals EOF record를 찾지 못했습니다."));
            };
            if first_record {
                validate_biff_bof(
                    record_id,
                    data,
                    BIFF_SUBSTREAM_WORKBOOK_GLOBALS,
                    "workbook globals",
                )?;
            }
            match record_id {
                0x0085 => {
                    let offset = usize::try_from(read_u32_le(data, 0)?).map_err(|source| {
                        err_with_source("xls BoundSheet offset 변환에 실패했습니다.", source)
                    })?;
                    let sheet_type = *data
                        .get(5)
                        .ok_or_else(|| err("xls BoundSheet record가 예상보다 짧습니다."))?;
                    if boundsheet
                        .replace(BiffBoundSheet { offset, sheet_type })
                        .is_some()
                    {
                        return Err(err("Opinet 고정 소스와 다른 worksheet 개수입니다."));
                    }
                }
                0x0042 => {
                    let parsed_code_page = read_u16_le(data, 0)?;
                    if code_page_seen {
                        return Err(err("xls CodePage record가 중복 선언되었습니다."));
                    }
                    if parsed_code_page != EXPECTED_BIFF_CODE_PAGE {
                        return Err(err(format!(
                            "Opinet 고정 소스의 BIFF code page가 예상과 다릅니다: {parsed_code_page}"
                        )));
                    }
                    code_page_seen = true;
                }
                BIFF_RECORD_SST => {
                    if shared_strings.is_some() {
                        return Err(err("xls SST record가 중복 선언되었습니다."));
                    }
                    if !code_page_seen {
                        return Err(err("BIFF CodePage record보다 SST가 먼저 선언되었습니다."));
                    }
                    let (parsed_shared_strings, next_offset) = self.read_sst(data, records.pos)?;
                    shared_strings = Some(parsed_shared_strings);
                    records.pos = next_offset;
                    continue;
                }
                _ => {}
            }
            if record_id == BIFF_RECORD_EOF {
                break;
            }
        }
        let Some(parsed_boundsheet) = boundsheet else {
            return Err(err("xls에서 BoundSheet를 찾지 못했습니다."));
        };
        if !code_page_seen {
            return Err(err(
                "Opinet 고정 소스에서 CodePage record를 찾지 못했습니다.",
            ));
        }
        let Some(parsed_shared_strings) = shared_strings else {
            return Err(err("Opinet 고정 소스에서 SST를 찾지 못했습니다."));
        };
        if parsed_shared_strings.values.is_empty() {
            return Err(err("Opinet 고정 소스의 SST가 비어 있습니다."));
        }
        Ok((parsed_boundsheet, parsed_shared_strings))
    }
    fn read_sst(
        &self,
        first_chunk: &[u8],
        first_chunk_end: usize,
    ) -> Result<(BiffSharedStrings, usize)> {
        let (chunks, next_offset) = self.collect_sst_chunks(first_chunk, first_chunk_end)?;
        let total_chunk_bytes: usize = chunks.iter().map(|chunk| chunk.len()).sum();
        if total_chunk_bytes < 8 {
            return Err(err("SST 데이터가 비정상적으로 짧습니다."));
        }
        let mut reader = SstChunkReader {
            chunk_index: 0,
            chunks: &chunks,
            offset_in_chunk: 0,
        };
        let declared_total = usize::try_from(reader.read_u32()?)
            .map_err(|source| err_with_source("SST total count 변환에 실패했습니다.", source))?;
        let unique_count = usize::try_from(reader.read_u32()?)
            .map_err(|source| err_with_source("SST unique count 변환에 실패했습니다.", source))?;
        if declared_total < unique_count {
            return Err(err(format!(
                "SST total count가 unique count보다 작습니다: total={declared_total}, unique={unique_count}"
            )));
        }
        if declared_total > MAX_SOURCE_CELL_COUNT {
            return Err(err(display_limit_message(
                "SST total count가 소스 시트 셀 한도를 초과했습니다: ",
                declared_total,
                "최대 ",
                MAX_SOURCE_CELL_COUNT,
            )));
        }
        let max_unique_count = total_chunk_bytes.wrapping_sub(8).div_euclid(3);
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
            validate_sst_option(flags, BIFF_SST_STRING_FLAGS_MASK, "SST 문자열 option")?;
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
                let rich_bytes = rich_run_count.wrapping_mul(4);
                reader.skip_bytes(rich_bytes)?;
            }
            if ext_len > 0 {
                reader.skip_bytes(ext_len)?;
            }
            out.push(value);
        }
        Ok((
            BiffSharedStrings {
                declared_total,
                values: out,
            },
            next_offset,
        ))
    }
    fn visit_worksheet<'strings, F>(
        &self,
        sheet_offset: usize,
        declared_total: usize,
        shared_strings: &'strings [String],
        visitor: &mut F,
    ) -> Result<Result<()>>
    where
        F: FnMut(SourceRecordRef<'strings>) -> Result<()>,
    {
        if sheet_offset >= self.workbook_stream.len() {
            return Err(err(prefixed_display_message(
                "worksheet offset이 workbook stream 범위를 벗어났습니다: ",
                sheet_offset,
            )));
        }
        let mut found_header = false;
        let mut found_record = false;
        let mut visitor_error: Option<AppError> = None;
        let mut flush_row = |row_num: usize, row: &SourceRow<'strings>| -> Result<()> {
            if row_num < SOURCE_HEADER_ROW {
                return Ok(());
            }
            if row_num == SOURCE_HEADER_ROW {
                for (expected_col, expected_text) in [
                    (COL_REGION, "지역"),
                    (COL_NAME, "상호"),
                    (COL_ADDRESS, "주소"),
                    (COL_BRAND, "상표"),
                    (COL_SELF_YN, "셀프여부"),
                    (COL_PREMIUM, "고급휘발유"),
                    (COL_GASOLINE, "휘발유"),
                    (COL_DIESEL, "경유"),
                ] {
                    let actual = row_text_trimmed(row, expected_col);
                    if actual != expected_text {
                        return Err(err(format!(
                            "Opinet 소스 헤더가 예상과 다릅니다: col={}, expected={expected_text}, actual={actual}",
                            expected_col.wrapping_add(1)
                        )));
                    }
                }
                found_header = true;
                return Ok(());
            }
            if !found_header {
                return Err(err("Opinet 소스 헤더 행을 찾지 못했습니다."));
            }
            if visitor_error.is_some() {
                return Ok(());
            }
            let address = row_text_trimmed(row, COL_ADDRESS);
            if address.is_empty() {
                if row
                    .cells
                    .iter()
                    .flatten()
                    .any(|text| !text.trim().is_empty())
                {
                    return Err(err(format!(
                        "Opinet 소스 {row_num}행에 주소 없이 데이터가 존재합니다."
                    )));
                }
                return Ok(());
            }
            let diesel = row_fuel_price(row, COL_DIESEL, row_num, "경유")?;
            let gasoline = row_fuel_price(row, COL_GASOLINE, row_num, "휘발유")?;
            let name = row_text_trimmed(row, COL_NAME);
            if name.is_empty() {
                return Err(format!("Opinet 소스 {row_num}행 상호명 값이 비어 있습니다.").into());
            }
            let premium = row_fuel_price(row, COL_PREMIUM, row_num, "고급휘발유")?;
            let self_yn = row_text_trimmed(row, COL_SELF_YN);
            if !matches!(self_yn, "셀프" | "일반") {
                return Err(format!(
                    "Opinet 소스 {row_num}행 셀프 여부 값이 올바르지 않습니다: {self_yn}"
                )
                .into());
            }
            found_record = true;
            if let Err(source) = visitor(SourceRecordRef {
                address,
                brand: row_text_trimmed(row, COL_BRAND),
                fuels: FuelValues {
                    diesel,
                    gasoline,
                    premium,
                },
                name,
                region: row_text_trimmed(row, COL_REGION),
                self_yn,
            }) {
                visitor_error = Some(source);
            }
            Ok(())
        };
        let mut current_row = SourceRow::default();
        let mut current_row_num = None;
        let mut first_record = true;
        let mut label_sst_count = 0_usize;
        let mut previous_cell = None;
        let mut records = BiffRecordReader {
            context: "worksheet",
            pos: sheet_offset,
            workbook_stream: self.workbook_stream,
        };
        loop {
            let Some((record_id, record_data)) = records.next()? else {
                return Err(err("xls worksheet EOF record를 찾지 못했습니다."));
            };
            if first_record {
                validate_biff_bof(
                    record_id,
                    record_data,
                    BIFF_SUBSTREAM_WORKSHEET,
                    "worksheet",
                )?;
                first_record = false;
            }
            match record_id {
                BIFF_RECORD_LABEL_SST => {
                    if record_data.len() != 10 {
                        return Err(err(format!(
                            "LABELSST record 길이가 예상과 다릅니다: expected=10, actual={}",
                            record_data.len()
                        )));
                    }
                    label_sst_count = label_sst_count.wrapping_add(1);
                    let row = usize::from(read_u16_le(record_data, 0)?) + 1;
                    let col = usize::from(read_u16_le(record_data, 2)?);
                    if row > MAX_SOURCE_ROW {
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
                            col + 1,
                        )));
                    }
                    if previous_cell.is_some_and(|(previous_row, previous_col)| {
                        row < previous_row || (row == previous_row && col <= previous_col)
                    }) {
                        return Err(err(format!(
                            "Opinet 고정 소스 셀 순서가 올바르지 않습니다: row={row}, col={}",
                            col.wrapping_add(1)
                        )));
                    }
                    if let Some(completed_row_num) =
                        current_row_num.filter(|&current| current != row)
                    {
                        flush_row(completed_row_num, &current_row)?;
                        current_row = SourceRow::default();
                    }
                    current_row_num = Some(row);
                    previous_cell = Some((row, col));
                    let idx = usize::try_from(read_u32_le(record_data, 6)?).map_err(|source| {
                        err_with_source("SST index 변환에 실패했습니다.", source)
                    })?;
                    let value = shared_strings.get(idx).map(String::as_str).ok_or_else(|| {
                        err(format!(
                            "LABELSST가 존재하지 않는 SST index를 참조합니다: {idx}"
                        ))
                    })?;
                    if row >= SOURCE_HEADER_ROW && col < SOURCE_COLUMN_COUNT {
                        let cell = current_row.cells.get_mut(col).ok_or_else(|| {
                            err(prefixed_display_message(
                                "Opinet 고정 소스 열 범위 오류: ",
                                col.wrapping_add(1),
                            ))
                        })?;
                        if cell.replace(value).is_some() {
                            return Err(err(format!(
                                "Opinet 고정 소스 셀이 중복 선언되었습니다: row={row}, col={}",
                                col.wrapping_add(1)
                            )));
                        }
                    }
                }
                0x0203 | 0x027E | 0x00BD | 0x0204 => {
                    return Err(err(format!(
                        "Opinet 고정 소스에서 예상하지 않은 BIFF cell record입니다: {record_id:#06x}"
                    )));
                }
                BIFF_RECORD_EOF => break,
                _ => {}
            }
        }
        if let Some(row_num) = current_row_num {
            flush_row(row_num, &current_row)?;
        }
        if label_sst_count != declared_total {
            return Err(err(format!(
                "SST total count가 LABELSST 레코드 수와 다릅니다: declared={declared_total}, actual={label_sst_count}"
            )));
        }
        if !found_header {
            return Err(err("Opinet 소스 헤더 행을 찾지 못했습니다."));
        }
        if let Some(source) = visitor_error {
            return Ok(Err(source));
        }
        if !found_record {
            return Err(err("xls 시트에서 유효한 소스 데이터를 찾지 못했습니다."));
        }
        Ok(Ok(()))
    }
}
fn validate_biff_bof(
    record_id: u16,
    data: &[u8],
    expected_substream: u16,
    substream_name: &str,
) -> Result<()> {
    if record_id != BIFF_RECORD_BOF {
        return Err(err(format!(
            "xls {substream_name}의 첫 레코드가 BOF가 아닙니다: {record_id:#06x}"
        )));
    }
    if data.len() != 16 {
        return Err(err(format!(
            "xls {substream_name} BOF 길이가 예상과 다릅니다: expected=16, actual={}",
            data.len()
        )));
    }
    let version = read_u16_le(data, 0)?;
    let substream = read_u16_le(data, 2)?;
    if version != BIFF_VERSION_8 || substream != expected_substream {
        return Err(err(format!(
            "xls {substream_name} BOF가 예상과 다릅니다: version={version:#06x}, substream={substream:#06x}"
        )));
    }
    Ok(())
}
fn validate_sst_option(flags: u8, allowed_mask: u8, context: &str) -> Result<()> {
    if flags & !allowed_mask != 0 {
        return Err(err(format!(
            "{context}에 예약 비트가 설정되었습니다: {flags:#04x}"
        )));
    }
    Ok(())
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
fn row_fuel_price(
    row: &SourceRow<'_>,
    idx: usize,
    row_num: usize,
    label: &'static str,
) -> Result<Option<i32>> {
    let Some(raw) = row.text(idx) else {
        return Ok(None);
    };
    let text = raw.trim();
    if text.is_empty() || text == "-" {
        return Ok(None);
    }
    if text.starts_with('-') {
        return Err(err(format!(
            "Opinet 소스 {row_num}행 {label} 가격은 음수일 수 없습니다: {text}"
        )));
    }
    let unsigned = text.strip_prefix('+').unwrap_or(text);
    let (whole, fraction) = unsigned
        .split_once('.')
        .map_or((unsigned, None), |(whole, fraction)| {
            (whole, Some(fraction))
        });
    let valid_fraction = fraction.is_none_or(|digits| {
        !digits.is_empty()
            && !digits.contains('.')
            && digits.bytes().all(|byte| byte.is_ascii_digit())
    });
    let valid_whole = if whole.contains(',') {
        let mut groups = whole.split(',');
        groups.next().is_some_and(|first| {
            (1..=3).contains(&first.len()) && first.bytes().all(|byte| byte.is_ascii_digit())
        }) && groups
            .all(|group| group.len() == 3 && group.bytes().all(|byte| byte.is_ascii_digit()))
    } else {
        !whole.is_empty() && whole.bytes().all(|byte| byte.is_ascii_digit())
    };
    if !valid_whole || !valid_fraction {
        return Err(err(format!(
            "Opinet 소스 {row_num}행 {label} 가격 형식이 올바르지 않습니다: {text}"
        )));
    }
    let value = parse_i32_str(text).ok_or_else(|| {
        err(format!(
            "Opinet 소스 {row_num}행 {label} 가격 형식이 올바르지 않습니다: {text}"
        ))
    })?;
    if value == 0_i32 {
        return Ok(None);
    }
    if !(MIN_FUEL_PRICE..=MAX_FUEL_PRICE).contains(&value) {
        return Err(err(format!(
            "Opinet 소스 {row_num}행 {label} 가격이 허용 범위({MIN_FUEL_PRICE}~{MAX_FUEL_PRICE})를 벗어났습니다: {text}"
        )));
    }
    Ok(Some(value))
}
fn row_text_trimmed<'strings>(row: &SourceRow<'strings>, idx: usize) -> &'strings str {
    row.text(idx).map_or("", str::trim)
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
fn get_sector_slice_at_index(data: &[u8], sector_idx: usize, sector_id: u32) -> Result<&[u8]> {
    data.get(CFB_SECTOR_SIZE..)
        .and_then(|sectors| sectors.chunks_exact(CFB_SECTOR_SIZE).nth(sector_idx))
        .ok_or_else(|| {
            err(format!(
                "CFB sector 범위를 벗어났습니다: sector={sector_id}, size={CFB_SECTOR_SIZE}"
            ))
        })
}
fn read_stream_from_fat_chain(
    data: &[u8],
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
    out.try_reserve_exact(remaining.unwrap_or(CFB_SECTOR_SIZE))
        .map_err(|source| err_with_source("FAT stream 메모리 확보 실패", source))?;
    let mut sid = start_sector;
    let mut traversed = 0_usize;
    while sid != CFB_END_OF_CHAIN {
        if traversed >= fat.len() {
            return Err(err(format!(
                "FAT chain이 FAT entry 수를 초과했습니다(순환 가능): {stream_name} (sector={sid})"
            )));
        }
        traversed = traversed.saturating_add(1);
        if remaining == Some(0) {
            return Err(err(format!(
                "FAT stream이 선언 크기 이후에도 계속됩니다: {stream_name} (sector={sid})"
            )));
        }
        if !is_regular_sector_id(sid) {
            return Err(err(format!(
                "FAT chain에 잘못된 sector id가 있습니다: {stream_name} ({sid:#x})"
            )));
        }
        let sid_usize = sector_id_to_index(sid, || {
            format!("FAT sector 변환 실패: {stream_name} (sector={sid})")
        })?;
        let next_sid = *fat.get(sid_usize).ok_or_else(|| {
            err(prefixed_display_message(
                "FAT 인덱스 범위 오류: sector=",
                sid,
            ))
        })?;
        if next_sid == CFB_FREE_SECT {
            return Err(err(format!(
                "FAT chain이 free sector를 참조합니다: {stream_name} (sector={next_sid})"
            )));
        }
        let sector = get_sector_slice_at_index(data, sid_usize, sid)?;
        if let Some(remain) = remaining.as_mut() {
            let take = (*remain).min(sector.len());
            let prefix = sector
                .get(..take)
                .ok_or_else(|| err("sector 슬라이스 범위 오류"))?;
            out.extend_from_slice(prefix);
            *remain = remain.wrapping_sub(take);
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
