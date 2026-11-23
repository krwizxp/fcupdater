use super::{copy_text, read_le_chunk, writer::parse_decimal_scaled};
use crate::diagnostic::{
    Result, bail, bail_fmt, ensure, ensure_fmt, err, err_with_source, fmt_err, map_reserve,
    try_push_vec, try_string_with_capacity, try_vec_with_capacity,
};
use alloc::borrow::Cow;
use core::{iter, range::Range};
const CFB_SIGNATURE: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
const CFB_FREE_SECT: u32 = 0xFFFF_FFFF;
const CFB_END_OF_CHAIN: u32 = 0xFFFF_FFFE;
const CFB_DIFAT_SECT: u32 = 0xFFFF_FFFC;
const CFB_MINI_STREAM_CUTOFF_SIZE: u32 = 4096;
const CFB_SECTOR_SIZE: usize = 512;
const CFB_OBJECT_UNUSED: u8 = 0;
const CFB_OBJECT_STREAM: u8 = 2;
const BIFF_RECORD_EOF: u16 = 0x000A;
const MAX_SOURCE_ROW: usize = 50_000;
const MAX_SOURCE_COL: usize = 64;
const MAX_SOURCE_CELL_COUNT: usize = MAX_SOURCE_ROW * MAX_SOURCE_COL;
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
const WORKBOOK_STREAM_NAME: [char; 8] = ['W', 'o', 'r', 'k', 'b', 'o', 'o', 'k'];
const MIN_FUEL_PRICE: i32 = 100;
const MAX_FUEL_PRICE: i32 = 100_000;
#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub(crate) struct FuelValues<T> {
    pub diesel: T,
    pub gasoline: T,
    pub premium: T,
}
impl<T> FuelValues<T> {
    pub(crate) fn into_array(self) -> [T; 3] {
        [self.gasoline, self.premium, self.diesel]
    }
    pub(crate) fn map<U>(self, mut map: impl FnMut(T) -> U) -> FuelValues<U> {
        FuelValues {
            diesel: map(self.diesel),
            gasoline: map(self.gasoline),
            premium: map(self.premium),
        }
    }
}
pub(crate) struct SourceRecord {
    pub address: String,
    pub brand: String,
    pub fuels: FuelValues<Option<i32>>,
    pub name: String,
    pub region: &'static str,
    pub service: StationService,
}
pub(crate) struct SourceRecordRef<'record> {
    pub address: &'record str,
    pub brand: &'record str,
    pub fuels: FuelValues<Option<i32>>,
    pub name: &'record str,
    pub region: &'record str,
    pub service: StationService,
}
#[derive(Clone, Copy)]
pub(crate) enum StationService {
    General,
    SelfService,
}
impl StationService {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::General => "일반",
            Self::SelfService => "셀프",
        }
    }
}
impl SourceRecordRef<'_> {
    pub(crate) fn into_owned_with_region(self, region: &'static str) -> Result<SourceRecord> {
        Ok(SourceRecord {
            address: copy_text(self.address)?,
            brand: copy_text(self.brand)?,
            fuels: self.fuels,
            name: copy_text(self.name)?,
            region,
            service: self.service,
        })
    }
}
#[derive(Clone, Copy)]
struct CfbHeader {
    first_dir_sector: u32,
    first_mini_fat_sector: u32,
    num_fat_sectors: u32,
    num_mini_fat_sectors: u32,
}
struct BiffRecordReader<'workbook> {
    context: &'static str,
    pos: usize,
    workbook_stream: &'workbook [u8],
}
struct BiffSharedStrings {
    declared_total: usize,
    offsets: Vec<u32>,
    text: String,
}
impl BiffSharedStrings {
    fn get(&self, index: usize) -> Option<&str> {
        let &[start, end] = self.offsets.get(index..)?.first_chunk::<2>()?;
        self.text.get(start as usize..end as usize)
    }
}
struct BiffWorkbookReader<'workbook>(&'workbook [u8]);
pub(crate) struct SourceReader(Vec<u8>);
type SourceRow<'strings> = [Option<&'strings str>; SOURCE_COLUMN_COUNT];
struct SstChunkReader<'chunks, 'chunk> {
    chunk_index: usize,
    chunks: &'chunks [&'chunk [u8]],
    offset_in_chunk: usize,
}
impl SourceReader {
    fn parse_cfb_header(&self) -> Result<CfbHeader> {
        let data = self
            .0
            .first_chunk::<512>()
            .filter(|header| header.starts_with(&CFB_SIGNATURE))
            .ok_or("유효한 OLE2(CFB) Opinet xls 응답이 아닙니다.")?;
        for (offset, expected, label) in [
            (0x1A, 3, "major version"),
            (0x1C, 0xFFFE, "byte order"),
            (0x1E, 9, "sector shift"),
            (0x20, 6, "mini sector shift"),
        ] {
            let actual = read_u16_le(data, offset)?;
            ensure_fmt!(
                actual == expected,
                "Opinet 고정 소스에서 예상하지 않은 CFB {label}: {actual:#06x}"
            );
        }
        ensure_fmt!(
            self.0.len().is_multiple_of(CFB_SECTOR_SIZE),
            "CFB 파일 길이가 sector 크기 단위가 아닙니다: file_size={}, sector_size={CFB_SECTOR_SIZE}",
            self.0.len()
        );
        for (offset, expected, label) in [
            (0x48, 0, "DIFAT sector 개수"),
            (0x44, CFB_END_OF_CHAIN, "DIFAT 시작 sector"),
            (0x38, CFB_MINI_STREAM_CUTOFF_SIZE, "mini stream cutoff"),
        ] {
            let actual = read_u32_le(data, offset)?;
            ensure_fmt!(
                actual == expected,
                "Opinet 고정 소스에서 예상하지 않은 CFB {label}: {actual:#010x}"
            );
        }
        Ok(CfbHeader {
            first_dir_sector: read_u32_le(data, 0x30)?,
            first_mini_fat_sector: read_u32_le(data, 0x3C)?,
            num_fat_sectors: read_u32_le(data, 0x2C)?,
            num_mini_fat_sectors: read_u32_le(data, 0x40)?,
        })
    }
    fn read_workbook_stream<'source>(
        &'source self,
        header: CfbHeader,
        fat: &[u32],
        mini_fat_last_sector: Option<u32>,
    ) -> Result<Cow<'source, [u8]>> {
        let (dir_stream, dir_last_sector) = read_stream_from_fat_chain(
            &self.0,
            fat,
            header.first_dir_sector,
            None,
            "CFB 디렉터리",
        )?;
        ensure(
            dir_last_sector.is_none() || dir_last_sector != mini_fat_last_sector,
            "CFB 디렉터리와 Mini FAT의 sector가 겹칩니다.",
        )?;
        let (chunks, &[]) = dir_stream.as_ref().as_chunks::<128>() else {
            bail!("CFB 디렉터리 stream 길이가 128바이트 단위가 아닙니다.");
        };
        let mut workbook_entry = None;
        for entry in chunks {
            let name_len = usize::from(read_u16_le(entry, 0x40)?);
            let object_type = *entry
                .get(0x42)
                .ok_or("CFB 디렉터리 object_type 범위 오류")?;
            ensure_fmt!(
                matches!(object_type, CFB_OBJECT_UNUSED | 1 | CFB_OBJECT_STREAM | 5),
                "CFB directory entry object_type이 비정상입니다: {object_type}"
            );
            let start_sector = read_u32_le(entry, 0x74)?;
            let stream_size = u64::from_le_bytes(
                read_le_chunk::<8>(entry, 0x78).ok_or("u64 read out of range at ")?,
            );
            ensure_fmt!(
                u32::try_from(stream_size).is_ok(),
                "CFB directory stream size 상위 32비트가 0이 아닙니다: {stream_size}"
            );
            if object_type == CFB_OBJECT_UNUSED {
                continue;
            }
            ensure_fmt!(
                (2..=64).contains(&name_len),
                "CFB directory entry 이름 길이가 비정상입니다: object_type={object_type}, name_len={name_len}"
            );
            let text_len = name_len.strict_sub(2);
            ensure_fmt!(
                read_u16_le(entry, text_len)? == 0,
                "CFB directory entry 이름이 NUL로 끝나지 않습니다: object_type={object_type}"
            );
            let (name_units, &[]) = entry
                .get(..text_len)
                .ok_or("CFB 디렉터리 이름 범위 오류")?
                .as_chunks::<2>()
            else {
                bail!("UTF-16 문자열 길이가 홀수입니다.");
            };
            let mut workbook_name = name_units.len() == WORKBOOK_STREAM_NAME.len();
            for (index, item) in
                char::decode_utf16(name_units.iter().map(|chunk| u16::from_le_bytes(*chunk)))
                    .enumerate()
            {
                let decoded =
                    item.map_err(|source| err_with_source("CFB UTF-16 문자열 해석 실패", source))?;
                workbook_name &= WORKBOOK_STREAM_NAME.get(index) == Some(&decoded);
            }
            ensure(
                object_type != CFB_OBJECT_STREAM
                    || !workbook_name
                    || workbook_entry
                        .replace((start_sector, stream_size))
                        .is_none(),
                "CFB stream이 중복 선언되었습니다: Workbook",
            )?;
        }
        let (start_sector, stream_size) =
            workbook_entry.ok_or("CFB stream을 찾지 못했습니다: Workbook")?;
        ensure(
            !(stream_size < u64::from(CFB_MINI_STREAM_CUTOFF_SIZE)
                && is_regular_sector_id(start_sector)),
            "Opinet 고정 소스에서 예상하지 않은 mini stream입니다: Workbook",
        )?;
        let (workbook, last_sector) =
            read_stream_from_fat_chain(&self.0, fat, start_sector, Some(stream_size), "Workbook")?;
        ensure(
            !(last_sector.is_some()
                && (last_sector == dir_last_sector || last_sector == mini_fat_last_sector)),
            "CFB Workbook과 메타데이터의 sector가 겹칩니다.",
        )?;
        Ok(workbook)
    }
    fn read_xls_workbook(&self) -> Result<Cow<'_, [u8]>> {
        let header = self.parse_cfb_header()?;
        let max_sector_count = self
            .0
            .len()
            .strict_sub(CFB_SECTOR_SIZE)
            .div_euclid(CFB_SECTOR_SIZE);
        ensure(max_sector_count != 0, "CFB sector 개수가 비정상적입니다.")?;
        let declared_fat_sectors = header.num_fat_sectors as usize;
        ensure_fmt!(
            declared_fat_sectors <= max_sector_count,
            "CFB FAT sector 개수가 비정상적으로 큽니다: {declared_fat_sectors} (최대 {max_sector_count})"
        );
        let difat_chunks = self
            .0
            .get(0x4C..512)
            .ok_or("CFB DIFAT 헤더 범위가 손상되었습니다.")?
            .as_chunks::<4>()
            .0;
        ensure_fmt!(
            declared_fat_sectors <= difat_chunks.len(),
            "CFB FAT 엔트리 개수가 header DIFAT 용량을 초과했습니다: {declared_fat_sectors}"
        );
        let mut difat_entries =
            try_vec_with_capacity(declared_fat_sectors, "CFB DIFAT 목록 메모리 확보 실패")?;
        for &chunk in difat_chunks {
            let sector_id = u32::from_le_bytes(chunk);
            if !is_regular_sector_id(sector_id) {
                continue;
            }
            ensure_fmt!(
                !difat_entries.contains(&sector_id),
                "CFB FAT sector가 중복 선언되었습니다: {sector_id}"
            );
            difat_entries.push(sector_id);
        }
        ensure(
            declared_fat_sectors != 0 && !difat_entries.is_empty(),
            "CFB FAT 정보를 찾지 못했습니다.",
        )?;
        ensure_fmt!(
            difat_entries.len() == declared_fat_sectors,
            "CFB FAT 엔트리 개수가 선언과 다릅니다: 선언 {declared_fat_sectors}, 실제 {}",
            difat_entries.len()
        );
        let mut fat = try_vec_with_capacity(
            difat_entries
                .len()
                .strict_mul(CFB_SECTOR_SIZE.div_euclid(4)),
            "CFB FAT 메모리 확보 실패",
        )?;
        for &sid in &difat_entries {
            fat.extend(
                get_sector_slice(&self.0, sid)?
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|chunk| u32::from_le_bytes(*chunk)),
            );
        }
        let mini_fat_last_sector = if header.num_mini_fat_sectors == 0 {
            ensure_fmt!(
                header.first_mini_fat_sector == CFB_END_OF_CHAIN,
                "Opinet 고정 소스에서 예상하지 않은 CFB mini FAT 시작 sector: {:#010x}",
                header.first_mini_fat_sector
            );
            None
        } else {
            read_stream_from_fat_chain(
                &self.0,
                &fat,
                header.first_mini_fat_sector,
                Some(u64::from(header.num_mini_fat_sectors).strict_mul(CFB_SECTOR_SIZE as u64)),
                "CFB Mini FAT",
            )?
            .1
        };
        self.read_workbook_stream(header, &fat, mini_fat_last_sector)
    }
    pub(crate) fn visit_rows(
        self,
        mut visitor: impl FnMut(SourceRecordRef<'_>) -> Result<()>,
    ) -> Result<()> {
        let workbook = self.read_xls_workbook()?;
        let biff = BiffWorkbookReader(workbook.as_ref());
        let (sheet_offset, shared_strings) = biff.parse_globals()?;
        biff.visit_worksheet(
            sheet_offset,
            shared_strings.declared_total,
            &shared_strings,
            &mut visitor,
        )
    }
}
impl From<Vec<u8>> for SourceReader {
    fn from(data: Vec<u8>) -> Self {
        Self(data)
    }
}
impl SstChunkReader<'_, '_> {
    fn ensure_available(&mut self) -> Result<()> {
        while let Some(chunk) = self.chunks.get(self.chunk_index) {
            if self.offset_in_chunk < chunk.len() {
                break;
            }
            self.chunk_index = self.chunk_index.strict_add(1);
            self.offset_in_chunk = 0;
        }
        ensure(
            self.chunk_index < self.chunks.len(),
            "SST data가 예상보다 짧습니다.",
        )
    }
    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.ensure_available()?;
        let bytes = self
            .chunks
            .get(self.chunk_index)
            .and_then(|chunk| chunk.get(self.offset_in_chunk..))
            .and_then(|remaining| remaining.first_chunk::<N>())
            .copied()
            .ok_or("SST 숫자 필드가 record 경계를 넘습니다.")?;
        self.offset_in_chunk = self.offset_in_chunk.strict_add(N);
        Ok(bytes)
    }
    fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.read_array::<2>()?))
    }
    fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.read_array::<4>()?))
    }
    fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_array::<1>()?[0])
    }
    fn read_xl_unicode_chars(
        &mut self,
        char_count: usize,
        mut high_byte: bool,
        out: &mut String,
    ) -> Result<Range<usize>> {
        let value_start = out.len();
        let (mut remaining, mut pending_surrogate) = (char_count, None);
        while remaining > 0 {
            let previous_chunk = self.chunk_index;
            self.ensure_available()?;
            if self.chunk_index != previous_chunk {
                let option = self.read_u8()?;
                validate_sst_option(option, 0x01, "SST Continue 문자열 option")?;
                high_byte = (option & 0x01) != 0;
            }
            let chunk = *self
                .chunks
                .get(self.chunk_index)
                .ok_or("SST chunk 접근 범위 오류")?;
            let available_bytes = chunk.len().strict_sub(self.offset_in_chunk);
            if available_bytes == 0 {
                continue;
            }
            let bytes_per_char = if high_byte { 2_usize } else { 1_usize };
            let chars_here = available_bytes.div_euclid(bytes_per_char).min(remaining);
            ensure(
                chars_here != 0,
                "SST UTF-16 code unit이 record 경계에서 나뉘어져 있습니다.",
            )?;
            let end = self
                .offset_in_chunk
                .strict_add(chars_here.strict_mul(bytes_per_char));
            let bytes = chunk
                .get(self.offset_in_chunk..end)
                .ok_or("SST 문자열 slice 길이 오류")?;
            map_reserve(
                out.try_reserve(chars_here.strict_mul(if high_byte { 3 } else { 2 })),
                "SST 문자열 메모리 확보 실패",
            )?;
            if high_byte {
                let (mut chunks, _) = bytes.as_chunks::<2>();
                let mut fallback_head = pending_surrogate.take();
                if let Some((last, rest)) = chunks.split_last()
                    && matches!(u16::from_le_bytes(*last), 0xd800..=0xdbff)
                {
                    pending_surrogate = Some(u16::from_le_bytes(*last));
                    chunks = rest;
                }
                let mut iter = chunks.iter();
                if fallback_head.is_none() {
                    for &unit in iter.by_ref() {
                        let code_unit = u16::from_le_bytes(unit);
                        if let Some(ch) = char::from_u32(u32::from(code_unit)) {
                            out.push(ch);
                        } else {
                            fallback_head = Some(code_unit);
                            break;
                        }
                    }
                }
                if let Some(first) = fallback_head {
                    for decoded in char::decode_utf16(
                        iter::once(first).chain(iter.map(|unit| u16::from_le_bytes(*unit))),
                    ) {
                        out.push(decoded.map_err(|source| {
                            err_with_source("SST UTF-16 문자열 해석 실패", source)
                        })?);
                    }
                }
            } else {
                ensure(
                    pending_surrogate.is_none(),
                    "SST UTF-16 서로게이트 쌍이 올바르지 않습니다.",
                )?;
                if bytes.is_ascii()
                    && let Ok(ascii) = str::from_utf8(bytes)
                {
                    out.push_str(ascii);
                } else {
                    out.extend(bytes.iter().copied().map(char::from));
                }
            }
            self.offset_in_chunk = end;
            remaining = remaining.strict_sub(chars_here);
        }
        ensure(
            pending_surrogate.is_none(),
            "SST UTF-16 서로게이트 쌍이 올바르지 않습니다.",
        )?;
        Ok(Range {
            start: value_start,
            end: out.len(),
        })
    }
    fn skip_bytes(&mut self, mut remaining: usize) -> Result<()> {
        while remaining > 0 {
            self.ensure_available()?;
            let chunk = *self
                .chunks
                .get(self.chunk_index)
                .ok_or("SST chunk 접근 범위 오류")?;
            let take = chunk.len().strict_sub(self.offset_in_chunk).min(remaining);
            self.offset_in_chunk = self.offset_in_chunk.strict_add(take);
            remaining = remaining.strict_sub(take);
        }
        Ok(())
    }
}
impl<'workbook> BiffRecordReader<'workbook> {
    fn next(&mut self) -> Result<Option<(u16, &'workbook [u8])>> {
        let record_start = self.pos;
        let tail = self.workbook_stream.get(record_start..).ok_or_else(|| {
            fmt_err!(
                "xls {} record 위치가 파일 범위를 벗어났습니다.",
                self.context
            )
        })?;
        if tail.is_empty() {
            return Ok(None);
        }
        let Some((&[id_lo, id_hi, len_lo, len_hi], record_tail)) = tail.split_first_chunk::<4>()
        else {
            bail_fmt!("xls {} record header 범위 오류", self.context);
        };
        let record_id = u16::from_le_bytes([id_lo, id_hi]);
        let record_len = usize::from(u16::from_le_bytes([len_lo, len_hi]));
        let data = record_tail
            .get(..record_len)
            .ok_or_else(|| fmt_err!("xls {} 레코드 범위 오류", self.context))?;
        self.pos = record_start.strict_add(4).strict_add(record_len);
        Ok(Some((record_id, data)))
    }
}
impl<'workbook> BiffWorkbookReader<'workbook> {
    fn collect_sst_chunks(
        &self,
        first_chunk: &'workbook [u8],
        first_chunk_end: usize,
    ) -> Result<(Vec<&'workbook [u8]>, usize, usize)> {
        let mut chunks =
            try_vec_with_capacity(8, "xls SST chunk 목록 메모리 확보 실패: 8 entries")?;
        chunks.push(first_chunk);
        let mut total_chunk_bytes = first_chunk.len();
        let mut records = BiffRecordReader {
            context: "SST Continue",
            pos: first_chunk_end,
            workbook_stream: self.0,
        };
        loop {
            let record_start = records.pos;
            let Some((record_id, chunk)) = records.next()? else {
                return Ok((chunks, records.pos, total_chunk_bytes));
            };
            if record_id != 0x003C {
                return Ok((chunks, record_start, total_chunk_bytes));
            }
            total_chunk_bytes = total_chunk_bytes.strict_add(chunk.len());
            try_push_vec(
                &mut chunks,
                chunk,
                1,
                "xls SST chunk 목록 추가 메모리 확보 실패: 1 entries",
            )?;
        }
    }
    fn parse_globals(&self) -> Result<(usize, BiffSharedStrings)> {
        let (mut sheet_offset, mut code_page_seen, mut shared_strings) = (None, false, None);
        let mut records = BiffRecordReader {
            context: "BIFF globals",
            pos: 0,
            workbook_stream: self.0,
        };
        loop {
            let first_record = records.pos == 0;
            let Some((record_id, data)) = records.next()? else {
                bail!("xls BIFF globals EOF record를 찾지 못했습니다.");
            };
            if first_record {
                validate_biff_bof(record_id, data, 0x0005, "workbook globals")?;
            }
            match record_id {
                0x0085 => {
                    let offset = read_u32_le(data, 0)? as usize;
                    let sheet_type = *data
                        .get(5)
                        .ok_or("xls BoundSheet record가 예상보다 짧습니다.")?;
                    ensure_fmt!(
                        sheet_type == 0,
                        "Opinet 고정 소스에서 예상하지 않은 sheet type: {sheet_type}"
                    );
                    ensure(
                        sheet_offset.replace(offset).is_none(),
                        "Opinet 고정 소스와 다른 worksheet 개수입니다.",
                    )?;
                }
                0x0042 => {
                    let parsed_code_page = read_u16_le(data, 0)?;
                    ensure(
                        !code_page_seen,
                        "xls CodePage record가 중복 선언되었습니다.",
                    )?;
                    ensure_fmt!(
                        parsed_code_page == 1200,
                        "Opinet 고정 소스의 BIFF code page가 예상과 다릅니다: {parsed_code_page}"
                    );
                    code_page_seen = true;
                }
                0x00FC => {
                    ensure(
                        shared_strings.is_none(),
                        "xls SST record가 중복 선언되었습니다.",
                    )?;
                    ensure(
                        code_page_seen,
                        "BIFF CodePage record보다 SST가 먼저 선언되었습니다.",
                    )?;
                    let (parsed_shared_strings, next_offset) = self.read_sst(data, records.pos)?;
                    shared_strings = Some(parsed_shared_strings);
                    records.pos = next_offset;
                }
                BIFF_RECORD_EOF => break,
                _ => {}
            }
        }
        let parsed_sheet_offset = sheet_offset.ok_or("xls에서 BoundSheet를 찾지 못했습니다.")?;
        ensure(
            code_page_seen,
            "Opinet 고정 소스에서 CodePage record를 찾지 못했습니다.",
        )?;
        let parsed_shared_strings =
            shared_strings.ok_or("Opinet 고정 소스에서 SST를 찾지 못했습니다.")?;
        ensure(
            parsed_shared_strings.offsets.len() > 1,
            "Opinet 고정 소스의 SST가 비어 있습니다.",
        )?;
        Ok((parsed_sheet_offset, parsed_shared_strings))
    }
    fn read_sst(
        &self,
        first_chunk: &[u8],
        first_chunk_end: usize,
    ) -> Result<(BiffSharedStrings, usize)> {
        let (chunks, next_offset, total_chunk_bytes) =
            self.collect_sst_chunks(first_chunk, first_chunk_end)?;
        ensure(
            first_chunk.len() >= 8,
            "SST 데이터가 비정상적으로 짧습니다.",
        )?;
        let mut reader = SstChunkReader {
            chunk_index: 0,
            chunks: &chunks,
            offset_in_chunk: 0,
        };
        let (declared_total, unique_count) =
            (reader.read_u32()? as usize, reader.read_u32()? as usize);
        ensure_fmt!(
            declared_total >= unique_count,
            "SST total count가 unique count보다 작습니다: total={declared_total}, unique={unique_count}"
        );
        ensure_fmt!(
            declared_total <= MAX_SOURCE_CELL_COUNT,
            "SST total count가 소스 시트 셀 한도를 초과했습니다: {declared_total} (최대 {MAX_SOURCE_CELL_COUNT})"
        );
        let max_unique_count = total_chunk_bytes.strict_sub(8).div_euclid(3);
        ensure_fmt!(
            unique_count <= max_unique_count,
            "SST unique count가 비정상적으로 큽니다: {unique_count} (최대 {max_unique_count})"
        );
        let mut offsets = try_vec_with_capacity(
            unique_count.strict_add(1),
            "SST 문자열 테이블 메모리 확보 실패",
        )?;
        offsets.push(0_u32);
        let mut text = try_string_with_capacity(
            total_chunk_bytes.strict_add(total_chunk_bytes.div_euclid(2)),
            "SST 문자열 메모리 확보 실패",
        )?;
        for _ in 0..unique_count {
            reader.ensure_available()?;
            let header_chunk = reader.chunk_index;
            let char_count = usize::from(reader.read_u16()?);
            let flags = reader.read_u8()?;
            validate_sst_option(flags, 0x0D, "SST 문자열 option")?;
            let rich_run_count = if flags & 0x08 != 0 {
                usize::from(reader.read_u16()?)
            } else {
                0_usize
            };
            let ext_len = if flags & 0x04 != 0 {
                reader.read_u32()? as usize
            } else {
                0_usize
            };
            ensure(
                reader.chunk_index == header_chunk,
                "SST 문자열 header가 record 경계를 넘습니다.",
            )?;
            let value = reader.read_xl_unicode_chars(char_count, flags & 0x01 != 0, &mut text)?;
            if rich_run_count != 0 {
                reader.skip_bytes(rich_run_count.strict_mul(4))?;
            }
            if ext_len != 0 {
                reader.skip_bytes(ext_len)?;
            }
            offsets.push(
                u32::try_from(value.end)
                    .map_err(|source| err_with_source("SST 문자열 오프셋 범위 초과", source))?,
            );
        }
        Ok((
            BiffSharedStrings {
                declared_total,
                offsets,
                text,
            },
            next_offset,
        ))
    }
    fn visit_worksheet<'strings>(
        &self,
        sheet_offset: usize,
        declared_total: usize,
        shared_strings: &'strings BiffSharedStrings,
        visitor: &mut impl FnMut(SourceRecordRef<'strings>) -> Result<()>,
    ) -> Result<()> {
        ensure_fmt!(
            sheet_offset < self.0.len(),
            "worksheet offset이 workbook stream 범위를 벗어났습니다: {sheet_offset}"
        );
        let (mut found_header, mut found_record) = (false, false);
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
                    ensure_fmt!(
                        actual == expected_text,
                        "Opinet 소스 헤더가 예상과 다릅니다: col={}, expected={expected_text}, actual={actual}",
                        expected_col.strict_add(1)
                    );
                }
                found_header = true;
                return Ok(());
            }
            ensure(found_header, "Opinet 소스 헤더 행을 찾지 못했습니다.")?;
            let address = row_text_trimmed(row, COL_ADDRESS);
            if address.is_empty() {
                ensure_fmt!(
                    !row.iter().flatten().any(|text| !text.trim().is_empty()),
                    "Opinet 소스 {row_num}행에 주소 없이 데이터가 존재합니다."
                );
                return Ok(());
            }
            let diesel = row_fuel_price(row, COL_DIESEL, row_num, "경유")?;
            let gasoline = row_fuel_price(row, COL_GASOLINE, row_num, "휘발유")?;
            let name = row_text_trimmed(row, COL_NAME);
            ensure_fmt!(
                !name.is_empty(),
                "Opinet 소스 {row_num}행 상호명 값이 비어 있습니다."
            );
            let premium = row_fuel_price(row, COL_PREMIUM, row_num, "고급휘발유")?;
            let service = match row_text_trimmed(row, COL_SELF_YN) {
                "셀프" => StationService::SelfService,
                "일반" => StationService::General,
                self_yn => {
                    bail_fmt!("Opinet 소스 {row_num}행 셀프 여부 값이 올바르지 않습니다: {self_yn}")
                }
            };
            found_record = true;
            visitor(SourceRecordRef {
                address,
                brand: row_text_trimmed(row, COL_BRAND),
                fuels: FuelValues {
                    diesel,
                    gasoline,
                    premium,
                },
                name,
                region: row_text_trimmed(row, COL_REGION),
                service,
            })
        };
        let mut current_row = SourceRow::default();
        let (mut first_record, mut label_sst_count, mut previous_cell) = (true, 0_usize, None);
        let mut records = BiffRecordReader {
            context: "worksheet",
            pos: sheet_offset,
            workbook_stream: self.0,
        };
        loop {
            let Some((record_id, record_data)) = records.next()? else {
                bail!("xls worksheet EOF record를 찾지 못했습니다.");
            };
            if first_record {
                validate_biff_bof(record_id, record_data, 0x0010, "worksheet")?;
                first_record = false;
            }
            match record_id {
                0x00FD => {
                    ensure_fmt!(
                        record_data.len() == 10,
                        "LABELSST record 길이가 예상과 다릅니다: expected=10, actual={}",
                        record_data.len()
                    );
                    label_sst_count = label_sst_count.strict_add(1);
                    let row = usize::from(read_u16_le(record_data, 0)?).strict_add(1);
                    let col = usize::from(read_u16_le(record_data, 2)?);
                    ensure_fmt!(
                        row <= MAX_SOURCE_ROW,
                        "시트 행 인덱스가 비정상적으로 큽니다: {row} (최대 {MAX_SOURCE_ROW})"
                    );
                    ensure_fmt!(
                        col < MAX_SOURCE_COL,
                        "시트 열 인덱스가 비정상적으로 큽니다: {}",
                        col.strict_add(1)
                    );
                    ensure_fmt!(
                        !previous_cell.is_some_and(|(prev_row, prev_col)| {
                            row < prev_row || (row == prev_row && col <= prev_col)
                        }),
                        "Opinet 고정 소스 셀 순서가 올바르지 않습니다: row={row}, col={}",
                        col.strict_add(1)
                    );
                    if let Some((completed_row_num, _)) =
                        previous_cell.filter(|&(current, _)| current != row)
                    {
                        flush_row(completed_row_num, &current_row)?;
                        current_row = SourceRow::default();
                    }
                    previous_cell = Some((row, col));
                    let idx = read_u32_le(record_data, 6)? as usize;
                    let value = shared_strings.get(idx).ok_or_else(|| {
                        fmt_err!("LABELSST가 존재하지 않는 SST index를 참조합니다: {idx}")
                    })?;
                    if row >= SOURCE_HEADER_ROW
                        && let Some(cell) = current_row.get_mut(col)
                    {
                        *cell = Some(value);
                    }
                }
                0x0203 | 0x027E | 0x00BD | 0x0204 => {
                    bail_fmt!(
                        "Opinet 고정 소스에서 예상하지 않은 BIFF cell record입니다: {record_id:#06x}"
                    );
                }
                BIFF_RECORD_EOF => break,
                _ => {}
            }
        }
        if let Some((row_num, _)) = previous_cell {
            flush_row(row_num, &current_row)?;
        }
        ensure_fmt!(
            label_sst_count == declared_total,
            "SST total count가 LABELSST 레코드 수와 다릅니다: declared={declared_total}, actual={label_sst_count}"
        );
        ensure(found_header, "Opinet 소스 헤더 행을 찾지 못했습니다.")?;
        found_record.ok_or_else(|| err("xls 시트에서 유효한 소스 데이터를 찾지 못했습니다."))
    }
}
fn get_sector_slice(data: &[u8], sector_id: u32) -> Result<&[u8; CFB_SECTOR_SIZE]> {
    sector_offset(sector_id)
        .and_then(|offset| data.get(offset..))
        .and_then(<[u8]>::first_chunk::<CFB_SECTOR_SIZE>)
        .ok_or_else(|| {
            fmt_err!("CFB sector 범위를 벗어났습니다: sector={sector_id}, size={CFB_SECTOR_SIZE}")
        })
}
const fn is_regular_sector_id(sector_id: u32) -> bool {
    sector_id < CFB_DIFAT_SECT
}
fn next_fat_sector(fat: &[u32], sector_id: u32) -> Result<u32> {
    fat.get(sector_id as usize)
        .copied()
        .ok_or_else(|| fmt_err!("FAT 인덱스 범위 오류: sector={sector_id}"))
}
fn read_stream_from_fat_chain<'data>(
    data: &'data [u8],
    fat: &[u32],
    start_sector: u32,
    size_limit: Option<u64>,
    stream_name: &str,
) -> Result<(Cow<'data, [u8]>, Option<u32>)> {
    if !is_regular_sector_id(start_sector) {
        if size_limit == Some(0) {
            return Ok((Cow::Borrowed(&[]), None));
        }
        bail_fmt!("FAT stream 시작 sector가 비정상입니다: {stream_name} ({start_sector:#x})");
    }
    let mut remaining = size_limit
        .map(|limit| {
            let data_len = data.len() as u64;
            ensure_fmt!(
                limit <= data_len,
                "FAT stream 선언 크기가 파일 크기보다 큽니다: {stream_name}, size={limit}, file_size={data_len}"
            );
            usize::try_from(limit)
                .map_err(|source| err_with_source("FAT stream 길이 변환 실패", source))
        })
        .transpose()?;
    let (mut sid, mut previous_sid) = (start_sector, None);
    let (mut traversed, mut stream_len, mut contiguous) = (0_usize, 0_usize, true);
    while sid != CFB_END_OF_CHAIN {
        ensure_fmt!(
            traversed < fat.len(),
            "FAT chain이 FAT entry 수를 초과했습니다(순환 가능): {stream_name} (sector={sid})"
        );
        traversed = traversed.strict_add(1);
        ensure_fmt!(
            remaining != Some(0),
            "FAT stream이 선언 크기 이후에도 계속됩니다: {stream_name} (sector={sid})"
        );
        ensure_fmt!(
            is_regular_sector_id(sid),
            "FAT chain에 잘못된 sector id가 있습니다: {stream_name} ({sid:#x})"
        );
        if previous_sid.is_some_and(|previous: u32| sid != previous.strict_add(1)) {
            contiguous = false;
        }
        let next_sid = next_fat_sector(fat, sid)?;
        ensure_fmt!(
            next_sid != CFB_FREE_SECT,
            "FAT chain이 free sector를 참조합니다: {stream_name} (sector={next_sid})"
        );
        let take = remaining.map_or(CFB_SECTOR_SIZE, |remain| remain.min(CFB_SECTOR_SIZE));
        let _sector = get_sector_slice(data, sid)?;
        stream_len = stream_len.strict_add(take);
        if let Some(remain) = remaining.as_mut() {
            *remain = remain.strict_sub(take);
        }
        previous_sid = Some(sid);
        sid = next_sid;
    }
    if let Some(remaining_bytes) = remaining.filter(|&bytes| bytes != 0) {
        bail_fmt!(
            "FAT stream이 선언 크기보다 짧습니다: {stream_name}, remaining={remaining_bytes}"
        );
    }
    if contiguous {
        let start_offset = sector_offset(start_sector).ok_or("CFB stream 시작 범위 계산 실패")?;
        let end_offset = start_offset
            .checked_add(stream_len)
            .ok_or("CFB stream 끝 범위 계산 실패")?;
        let stream = data
            .get(start_offset..end_offset)
            .ok_or("CFB stream 범위가 파일을 벗어났습니다.")?;
        return Ok((Cow::Borrowed(stream), previous_sid));
    }
    let mut out = try_vec_with_capacity(stream_len, "FAT stream 메모리 확보 실패")?;
    let (mut copy_remaining, mut copy_sid) = (stream_len, start_sector);
    while copy_remaining != 0 {
        let sector = get_sector_slice(data, copy_sid)?;
        let take = copy_remaining.min(sector.len());
        out.extend_from_slice(sector.split_at(take).0);
        copy_remaining = copy_remaining.strict_sub(take);
        copy_sid = next_fat_sector(fat, copy_sid)?;
    }
    Ok((Cow::Owned(out), previous_sid))
}
fn read_u16_le(bytes: &[u8], offset: usize) -> Result<u16> {
    read_le_chunk::<2>(bytes, offset)
        .map(u16::from_le_bytes)
        .ok_or_else(|| fmt_err!("u16 read out of range at {offset}"))
}
fn read_u32_le(bytes: &[u8], offset: usize) -> Result<u32> {
    read_le_chunk::<4>(bytes, offset)
        .map(u32::from_le_bytes)
        .ok_or_else(|| fmt_err!("u32 read out of range at {offset}"))
}
fn row_fuel_price(
    row: &SourceRow<'_>,
    idx: usize,
    row_num: usize,
    label: &'static str,
) -> Result<Option<i32>> {
    let text = row_text_trimmed(row, idx);
    if text.is_empty() || text == "-" {
        return Ok(None);
    }
    ensure_fmt!(
        !text.starts_with('-'),
        "Opinet 소스 {row_num}행 {label} 가격은 음수일 수 없습니다: {text}"
    );
    let parsed_value = text
        .parse::<i32>()
        .ok()
        .or_else(|| parse_grouped_price(text))
        .ok_or_else(|| {
            fmt_err!("Opinet 소스 {row_num}행 {label} 가격 형식이 올바르지 않습니다: {text}")
        })?;
    if parsed_value == 0_i32 {
        return Ok(None);
    }
    ensure_fmt!(
        (MIN_FUEL_PRICE..=MAX_FUEL_PRICE).contains(&parsed_value),
        "Opinet 소스 {row_num}행 {label} 가격이 허용 범위({MIN_FUEL_PRICE}~{MAX_FUEL_PRICE})를 벗어났습니다: {text}"
    );
    Ok(Some(parsed_value))
}
#[cold]
#[inline(never)]
fn parse_grouped_price(text: &str) -> Option<i32> {
    let unsigned = text.trim_prefix('+');
    if !unsigned.as_bytes().first().is_some_and(u8::is_ascii_digit)
        || unsigned.ends_with('.')
        || unsigned.contains(['e', 'E'])
    {
        return None;
    }
    i32::try_from(parse_decimal_scaled(unsigned, 0, true, true)?).ok()
}
fn row_text_trimmed<'strings>(row: &SourceRow<'strings>, idx: usize) -> &'strings str {
    row.get(idx).copied().flatten().map_or_default(str::trim)
}
fn sector_offset(sector_id: u32) -> Option<usize> {
    (sector_id as usize)
        .checked_add(1)?
        .checked_mul(CFB_SECTOR_SIZE)
}
fn validate_biff_bof(
    record_id: u16,
    data: &[u8],
    expected_substream: u16,
    substream_name: &str,
) -> Result<()> {
    ensure_fmt!(
        record_id == 0x0809,
        "xls {substream_name}의 첫 레코드가 BOF가 아닙니다: {record_id:#06x}"
    );
    ensure_fmt!(
        data.len() == 16,
        "xls {substream_name} BOF 길이가 예상과 다릅니다: expected=16, actual={}",
        data.len()
    );
    let (version, substream) = (read_u16_le(data, 0)?, read_u16_le(data, 2)?);
    ensure_fmt!(
        version == 0x0600 && substream == expected_substream,
        "xls {substream_name} BOF가 예상과 다릅니다: version={version:#06x}, substream={substream:#06x}"
    );
    Ok(())
}
fn validate_sst_option(flags: u8, allowed_mask: u8, context: &str) -> Result<()> {
    ensure_fmt!(
        flags & !allowed_mask == 0,
        "{context}에 예약 비트가 설정되었습니다: {flags:#04x}"
    );
    Ok(())
}
