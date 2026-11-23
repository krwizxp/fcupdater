use super::{copy_text, read_le_chunk, writer::parse_decimal_scaled};
use crate::diagnostic::{
    Result, bail, bail_fmt, ensure, ensure_fmt, err, err_src, fmt_err, push_vec, reserve, str_cap,
    vec_cap,
};
use alloc::borrow::Cow;
use core::{iter, range::Range};
const CFB_SIG: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
const FREE_SECT: u32 = 0xFFFF_FFFF;
const END_CHAIN: u32 = 0xFFFF_FFFE;
const DIFAT_SECT: u32 = 0xFFFF_FFFC;
const MINI_CUTOFF: u32 = 4096;
const SECT_SZ: usize = 512;
const OBJ_UNUSED: u8 = 0;
const OBJ_STREAM: u8 = 2;
const BIFF_EOF: u16 = 0x000A;
const MAX_ROW: usize = 50_000;
const MAX_COL: usize = 64;
const MAX_CELLS: usize = MAX_ROW * MAX_COL;
const HEADER_ROW: usize = 4;
const COL_COUNT: usize = 9;
const WB_NAME: [char; 8] = ['W', 'o', 'r', 'k', 'b', 'o', 'o', 'k'];
const MIN_PRICE: i32 = 100;
const MAX_PRICE: i32 = 100_000;
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
    first_dir: u32,
    first_mini_fat: u32,
    num_fat: u32,
    num_mini_fat: u32,
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
type SourceRow<'strings> = [Option<&'strings str>; COL_COUNT];
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
            .filter(|hdr| hdr.starts_with(&CFB_SIG))
            .ok_or("유효한 OLE2(CFB) Opinet xls 응답이 아닙니다.")?;
        for (off, exp, label) in [
            (0x1A, 3, "major version"),
            (0x1C, 0xFFFE, "byte order"),
            (0x1E, 9, "sector shift"),
            (0x20, 6, "mini sector shift"),
        ] {
            let actual = read_u16_le(data, off)?;
            ensure_fmt!(actual == exp, "CFB {label} 오류: {actual:#06x}");
        }
        ensure_fmt!(
            self.0.len().is_multiple_of(SECT_SZ),
            "CFB 파일 길이 오류: {}",
            self.0.len()
        );
        for (off, exp, label) in [
            (0x48, 0, "DIFAT sector 개수"),
            (0x44, END_CHAIN, "DIFAT 시작 sector"),
            (0x38, MINI_CUTOFF, "mini stream cutoff"),
        ] {
            let actual = read_u32_le(data, off)?;
            ensure_fmt!(actual == exp, "CFB {label} 오류: {actual:#010x}");
        }
        Ok(CfbHeader {
            first_dir: read_u32_le(data, 0x30)?,
            first_mini_fat: read_u32_le(data, 0x3C)?,
            num_fat: read_u32_le(data, 0x2C)?,
            num_mini_fat: read_u32_le(data, 0x40)?,
        })
    }
    fn read_workbook_stream<'source>(
        &'source self,
        hdr: CfbHeader,
        fat: &[u32],
        mini_last: Option<u32>,
    ) -> Result<Cow<'source, [u8]>> {
        let (dir_stream, dir_last) =
            read_fat_chain(&self.0, fat, hdr.first_dir, None, "CFB 디렉터리")?;
        ensure(
            dir_last.is_none() || dir_last != mini_last,
            "CFB 디렉터리와 Mini FAT sector 겹침",
        )?;
        let (chunks, &[]) = dir_stream.as_ref().as_chunks::<128>() else {
            bail!("CFB 디렉터리 stream 길이 오류");
        };
        let mut wb_entry = None;
        for entry in chunks {
            let name_len = usize::from(read_u16_le(entry, 0x40)?);
            let obj_type = *entry.get(0x42).ok_or("CFB object_type 범위 오류")?;
            ensure_fmt!(
                matches!(obj_type, OBJ_UNUSED | 1 | OBJ_STREAM | 5),
                "CFB object_type 오류: {obj_type}"
            );
            let start_sector = read_u32_le(entry, 0x74)?;
            let stream_size =
                u64::from_le_bytes(read_le_chunk::<8>(entry, 0x78).ok_or("u64 범위 오류")?);
            ensure_fmt!(
                u32::try_from(stream_size).is_ok(),
                "CFB stream size 상위 32비트 오류: {stream_size}"
            );
            if obj_type == OBJ_UNUSED {
                continue;
            }
            ensure_fmt!(
                (2..=64).contains(&name_len),
                "CFB 이름 길이 오류: {name_len}"
            );
            let text_len = name_len.strict_sub(2);
            ensure_fmt!(read_u16_le(entry, text_len)? == 0, "CFB 이름 NUL 종료 누락");
            let (units, &[]) = entry
                .get(..text_len)
                .ok_or("CFB 이름 범위 오류")?
                .as_chunks::<2>()
            else {
                bail!("UTF-16 문자열 길이 홀수");
            };
            let mut is_wb = units.len() == WB_NAME.len();
            for (idx, item) in
                char::decode_utf16(units.iter().map(|chunk| u16::from_le_bytes(*chunk))).enumerate()
            {
                let ch = item.map_err(|src| err_src("CFB UTF-16 해석 실패", src))?;
                is_wb &= WB_NAME.get(idx) == Some(&ch);
            }
            ensure(
                obj_type != OBJ_STREAM
                    || !is_wb
                    || wb_entry.replace((start_sector, stream_size)).is_none(),
                "CFB Workbook stream 중복",
            )?;
        }
        let (start_sector, stream_size) = wb_entry.ok_or("CFB Workbook stream 없음")?;
        ensure(
            !(stream_size < u64::from(MINI_CUTOFF) && is_regular_sect(start_sector)),
            "예상하지 않은 mini stream: Workbook",
        )?;
        let (workbook, last_sector) =
            read_fat_chain(&self.0, fat, start_sector, Some(stream_size), "Workbook")?;
        ensure(
            !(last_sector.is_some() && (last_sector == dir_last || last_sector == mini_last)),
            "CFB Workbook과 메타데이터 sector 겹침",
        )?;
        Ok(workbook)
    }
    fn read_xls_workbook(&self) -> Result<Cow<'_, [u8]>> {
        let hdr = self.parse_cfb_header()?;
        let max_sectors = self.0.len().strict_sub(SECT_SZ).div_euclid(SECT_SZ);
        ensure(max_sectors != 0, "CFB sector 개수 오류")?;
        let declared_fat = hdr.num_fat as usize;
        ensure_fmt!(
            declared_fat <= max_sectors,
            "CFB FAT sector 개수 초과: {declared_fat}"
        );
        let difat_chunks = self
            .0
            .get(0x4C..512)
            .ok_or("CFB DIFAT 헤더 손상")?
            .as_chunks::<4>()
            .0;
        ensure_fmt!(
            declared_fat <= difat_chunks.len(),
            "CFB FAT 엔트리 DIFAT 초과: {declared_fat}"
        );
        let mut difat = vec_cap(declared_fat, "CFB DIFAT")?;
        for &chunk in difat_chunks {
            let sid = u32::from_le_bytes(chunk);
            if !is_regular_sect(sid) {
                continue;
            }
            ensure_fmt!(!difat.contains(&sid), "CFB FAT sector 중복: {sid}");
            difat.push(sid);
        }
        ensure(declared_fat != 0 && !difat.is_empty(), "CFB FAT 없음")?;
        ensure_fmt!(difat.len() == declared_fat, "CFB FAT 개수 불일치");
        let mut fat = vec_cap(difat.len().strict_mul(SECT_SZ.div_euclid(4)), "CFB FAT")?;
        for &sid in &difat {
            fat.extend(
                sector_slice(&self.0, sid)?
                    .as_chunks::<4>()
                    .0
                    .iter()
                    .map(|chunk| u32::from_le_bytes(*chunk)),
            );
        }
        let mini_last = if hdr.num_mini_fat == 0 {
            ensure_fmt!(
                hdr.first_mini_fat == END_CHAIN,
                "CFB mini FAT 시작 sector 오류: {:#010x}",
                hdr.first_mini_fat
            );
            None
        } else {
            read_fat_chain(
                &self.0,
                &fat,
                hdr.first_mini_fat,
                Some(u64::from(hdr.num_mini_fat).strict_mul(SECT_SZ as u64)),
                "CFB Mini FAT",
            )?
            .1
        };
        self.read_workbook_stream(hdr, &fat, mini_last)
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
            .and_then(|rem| rem.first_chunk::<N>())
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
        let start = out.len();
        let (mut rem, mut pending_surr) = (char_count, None);
        while rem > 0 {
            let prev = self.chunk_index;
            self.ensure_available()?;
            if self.chunk_index != prev {
                let opt = self.read_u8()?;
                validate_sst_option(opt, 0x01, "SST Continue option")?;
                high_byte = opt & 0x01 != 0;
            }
            let chunk = *self
                .chunks
                .get(self.chunk_index)
                .ok_or("SST chunk 범위 오류")?;
            let avail = chunk.len().strict_sub(self.offset_in_chunk);
            if avail == 0 {
                continue;
            }
            let bpc = if high_byte { 2_usize } else { 1_usize };
            let chars_here = avail.div_euclid(bpc).min(rem);
            ensure(chars_here != 0, "SST UTF-16 code unit 경계 분할")?;
            let end = self.offset_in_chunk.strict_add(chars_here.strict_mul(bpc));
            let bytes = chunk
                .get(self.offset_in_chunk..end)
                .ok_or("SST slice 길이 오류")?;
            reserve(
                out.try_reserve(chars_here.strict_mul(if high_byte { 3 } else { 2 })),
                "SST 문자열",
            )?;
            if high_byte {
                let (mut chunks, _) = bytes.as_chunks::<2>();
                let mut fb_head = pending_surr.take();
                if let Some((last, rest)) = chunks.split_last()
                    && matches!(u16::from_le_bytes(*last), 0xd800..=0xdbff)
                {
                    pending_surr = Some(u16::from_le_bytes(*last));
                    chunks = rest;
                }
                let mut iter = chunks.iter();
                if fb_head.is_none() {
                    for &unit in iter.by_ref() {
                        let code = u16::from_le_bytes(unit);
                        if let Some(ch) = char::from_u32(u32::from(code)) {
                            out.push(ch);
                        } else {
                            fb_head = Some(code);
                            break;
                        }
                    }
                }
                if let Some(first) = fb_head {
                    for dec in char::decode_utf16(
                        iter::once(first).chain(iter.map(|unit| u16::from_le_bytes(*unit))),
                    ) {
                        out.push(dec.map_err(|src| err_src("SST UTF-16 해석 실패", src))?);
                    }
                }
            } else {
                ensure(pending_surr.is_none(), "SST UTF-16 서로게이트 쌍 오류")?;
                if bytes.is_ascii()
                    && let Ok(ascii) = str::from_utf8(bytes)
                {
                    out.push_str(ascii);
                } else {
                    out.extend(bytes.iter().copied().map(char::from));
                }
            }
            self.offset_in_chunk = end;
            rem = rem.strict_sub(chars_here);
        }
        ensure(pending_surr.is_none(), "SST UTF-16 서로게이트 쌍 오류")?;
        Ok(Range {
            start,
            end: out.len(),
        })
    }
    fn skip_bytes(&mut self, mut rem: usize) -> Result<()> {
        while rem > 0 {
            self.ensure_available()?;
            let chunk = *self
                .chunks
                .get(self.chunk_index)
                .ok_or("SST chunk 범위 오류")?;
            let take = chunk.len().strict_sub(self.offset_in_chunk).min(rem);
            self.offset_in_chunk = self.offset_in_chunk.strict_add(take);
            rem = rem.strict_sub(take);
        }
        Ok(())
    }
}
impl<'workbook> BiffRecordReader<'workbook> {
    fn next(&mut self) -> Result<Option<(u16, &'workbook [u8])>> {
        let start = self.pos;
        let tail = self
            .workbook_stream
            .get(start..)
            .ok_or_else(|| fmt_err!("xls {} record 위치 범위 초과", self.context))?;
        if tail.is_empty() {
            return Ok(None);
        }
        let Some((&[id_lo, id_hi, len_lo, len_hi], rec_tail)) = tail.split_first_chunk::<4>()
        else {
            bail_fmt!("xls {} record header 범위 오류", self.context);
        };
        let (id, len) = (
            u16::from_le_bytes([id_lo, id_hi]),
            usize::from(u16::from_le_bytes([len_lo, len_hi])),
        );
        let data = rec_tail
            .get(..len)
            .ok_or_else(|| fmt_err!("xls {} 레코드 범위 오류", self.context))?;
        self.pos = start.strict_add(4).strict_add(len);
        Ok(Some((id, data)))
    }
}
impl<'workbook> BiffWorkbookReader<'workbook> {
    fn collect_sst_chunks(
        &self,
        first_chunk: &'workbook [u8],
        first_end: usize,
    ) -> Result<(Vec<&'workbook [u8]>, usize, usize)> {
        let mut chunks = vec_cap(8, "xls SST chunk 목록")?;
        chunks.push(first_chunk);
        let mut total = first_chunk.len();
        let mut recs = BiffRecordReader {
            context: "SST Continue",
            pos: first_end,
            workbook_stream: self.0,
        };
        loop {
            let start = recs.pos;
            let Some((id, chunk)) = recs.next()? else {
                return Ok((chunks, recs.pos, total));
            };
            if id != 0x003C {
                return Ok((chunks, start, total));
            }
            total = total.strict_add(chunk.len());
            push_vec(&mut chunks, chunk, 1, "xls SST chunk")?;
        }
    }
    fn parse_globals(&self) -> Result<(usize, BiffSharedStrings)> {
        let (mut sheet_off, mut cp_seen, mut sst) = (None, false, None);
        let mut recs = BiffRecordReader {
            context: "BIFF globals",
            pos: 0,
            workbook_stream: self.0,
        };
        loop {
            let first = recs.pos == 0;
            let Some((id, data)) = recs.next()? else {
                bail!("xls BIFF globals EOF 없음");
            };
            if first {
                validate_biff_bof(id, data, 0x0005, "workbook globals")?;
            }
            match id {
                0x0085 => {
                    let off = read_u32_le(data, 0)? as usize;
                    let stype = *data.get(5).ok_or("BoundSheet record 짧음")?;
                    ensure_fmt!(stype == 0, "예상하지 않은 sheet type: {stype}");
                    ensure(sheet_off.replace(off).is_none(), "worksheet 개수 불일치")?;
                }
                0x0042 => {
                    let cp = read_u16_le(data, 0)?;
                    ensure(!cp_seen, "CodePage 중복")?;
                    ensure_fmt!(cp == 1200, "BIFF code page 오류: {cp}");
                    cp_seen = true;
                }
                0x00FC => {
                    ensure(sst.is_none(), "SST 중복")?;
                    ensure(cp_seen, "CodePage 이전 SST")?;
                    let (parsed, next_off) = self.read_sst(data, recs.pos)?;
                    sst = Some(parsed);
                    recs.pos = next_off;
                }
                BIFF_EOF => break,
                _ => {}
            }
        }
        let parsed_off = sheet_off.ok_or("BoundSheet 없음")?;
        ensure(cp_seen, "CodePage 없음")?;
        let parsed_sst = sst.ok_or("SST 없음")?;
        ensure(parsed_sst.offsets.len() > 1, "SST 비어 있음")?;
        Ok((parsed_off, parsed_sst))
    }
    fn read_sst(&self, first_chunk: &[u8], first_end: usize) -> Result<(BiffSharedStrings, usize)> {
        let (chunks, next_off, total_bytes) = self.collect_sst_chunks(first_chunk, first_end)?;
        ensure(first_chunk.len() >= 8, "SST 데이터 짧음")?;
        let mut reader = SstChunkReader {
            chunk_index: 0,
            chunks: &chunks,
            offset_in_chunk: 0,
        };
        let (declared_total, unique_count) =
            (reader.read_u32()? as usize, reader.read_u32()? as usize);
        ensure_fmt!(declared_total >= unique_count, "SST total < unique");
        ensure_fmt!(
            declared_total <= MAX_CELLS,
            "SST total 초과: {declared_total}"
        );
        ensure_fmt!(
            unique_count <= total_bytes.strict_sub(8).div_euclid(3),
            "SST unique 초과: {unique_count}"
        );
        let mut offsets = vec_cap(unique_count.strict_add(1), "SST 테이블")?;
        offsets.push(0_u32);
        let mut text = str_cap(
            total_bytes.strict_add(total_bytes.div_euclid(2)),
            "SST 문자열",
        )?;
        for _ in 0..unique_count {
            reader.ensure_available()?;
            let hdr_chunk = reader.chunk_index;
            let char_count = usize::from(reader.read_u16()?);
            let flags = reader.read_u8()?;
            validate_sst_option(flags, 0x0D, "SST 문자열 option")?;
            let rich = if flags & 0x08 != 0 {
                usize::from(reader.read_u16()?)
            } else {
                0
            };
            let ext = if flags & 0x04 != 0 {
                reader.read_u32()? as usize
            } else {
                0
            };
            ensure(
                reader.chunk_index == hdr_chunk,
                "SST header record 경계 초과",
            )?;
            let val = reader.read_xl_unicode_chars(char_count, flags & 0x01 != 0, &mut text)?;
            if rich != 0 {
                reader.skip_bytes(rich.strict_mul(4))?;
            }
            if ext != 0 {
                reader.skip_bytes(ext)?;
            }
            offsets
                .push(u32::try_from(val.end).map_err(|src| err_src("SST 오프셋 범위 초과", src))?);
        }
        Ok((
            BiffSharedStrings {
                declared_total,
                offsets,
                text,
            },
            next_off,
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
            "worksheet offset 범위 초과: {sheet_offset}"
        );
        let (mut found_header, mut found_record) = (false, false);
        let mut flush_row = |row_num: usize, row: &SourceRow<'strings>| -> Result<()> {
            if row_num < HEADER_ROW {
                return Ok(());
            }
            if row_num == HEADER_ROW {
                for (col, exp) in (1_usize..).zip([
                    "지역",
                    "상호",
                    "주소",
                    "상표",
                    "셀프여부",
                    "고급휘발유",
                    "휘발유",
                    "경유",
                ]) {
                    let actual = row_text(row, col);
                    ensure_fmt!(
                        actual == exp,
                        "Opinet 헤더 불일치: col={}, expected={exp}, actual={actual}",
                        col.strict_add(1)
                    );
                }
                found_header = true;
                return Ok(());
            }
            ensure(found_header, "Opinet 소스 헤더 행 없음")?;
            let address = row_text(row, 3);
            if address.is_empty() {
                ensure_fmt!(
                    !row.iter().flatten().any(|text| !text.trim().is_empty()),
                    "Opinet {row_num}행 주소 없이 데이터 존재"
                );
                return Ok(());
            }
            let diesel = row_fuel_price(row, 8, row_num, "경유")?;
            let gasoline = row_fuel_price(row, 7, row_num, "휘발유")?;
            let name = row_text(row, 2);
            ensure_fmt!(!name.is_empty(), "Opinet {row_num}행 상호명 비어 있음");
            let premium = row_fuel_price(row, 6, row_num, "고급휘발유")?;
            let service = match row_text(row, 5) {
                "셀프" => StationService::SelfService,
                "일반" => StationService::General,
                other => bail_fmt!("Opinet {row_num}행 셀프 여부 오류: {other}"),
            };
            found_record = true;
            visitor(SourceRecordRef {
                address,
                brand: row_text(row, 4),
                fuels: FuelValues {
                    diesel,
                    gasoline,
                    premium,
                },
                name,
                region: row_text(row, 1),
                service,
            })
        };
        let mut cur_row = SourceRow::default();
        let (mut first, mut sst_count, mut prev_cell) = (true, 0_usize, None);
        let mut recs = BiffRecordReader {
            context: "worksheet",
            pos: sheet_offset,
            workbook_stream: self.0,
        };
        loop {
            let Some((id, data)) = recs.next()? else {
                bail!("xls worksheet EOF 없음");
            };
            if first {
                validate_biff_bof(id, data, 0x0010, "worksheet")?;
                first = false;
            }
            match id {
                0x00FD => {
                    ensure_fmt!(data.len() == 10, "LABELSST 길이 오류: {}", data.len());
                    sst_count = sst_count.strict_add(1);
                    let row = usize::from(read_u16_le(data, 0)?).strict_add(1);
                    let col = usize::from(read_u16_le(data, 2)?);
                    ensure_fmt!(row <= MAX_ROW, "행 인덱스 초과: {row}");
                    ensure_fmt!(col < MAX_COL, "열 인덱스 초과: {}", col.strict_add(1));
                    ensure_fmt!(
                        !prev_cell.is_some_and(|(pr, pc)| row < pr || (row == pr && col <= pc)),
                        "셀 순서 오류: row={row}, col={}",
                        col.strict_add(1)
                    );
                    if let Some((done_row, _)) = prev_cell.filter(|&(cur, _)| cur != row) {
                        flush_row(done_row, &cur_row)?;
                        cur_row = SourceRow::default();
                    }
                    prev_cell = Some((row, col));
                    let idx = read_u32_le(data, 6)? as usize;
                    let val = shared_strings
                        .get(idx)
                        .ok_or_else(|| fmt_err!("LABELSST 잘못된 SST index: {idx}"))?;
                    if row >= HEADER_ROW
                        && let Some(cell) = cur_row.get_mut(col)
                    {
                        *cell = Some(val);
                    }
                }
                0x0203 | 0x027E | 0x00BD | 0x0204 => {
                    bail_fmt!("미지원 BIFF cell record: {id:#06x}")
                }
                BIFF_EOF => break,
                _ => {}
            }
        }
        if let Some((row_num, _)) = prev_cell {
            flush_row(row_num, &cur_row)?;
        }
        ensure_fmt!(
            sst_count == declared_total,
            "SST total 불일치: declared={declared_total}, actual={sst_count}"
        );
        ensure(found_header, "Opinet 소스 헤더 행 없음")?;
        found_record.ok_or_else(|| err("xls 시트에 유효한 소스 데이터 없음"))
    }
}
const fn is_regular_sect(sid: u32) -> bool {
    sid < DIFAT_SECT
}
fn next_fat(fat: &[u32], sid: u32) -> Result<u32> {
    fat.get(sid as usize)
        .copied()
        .ok_or_else(|| fmt_err!("FAT 인덱스 범위 오류: {sid}"))
}
fn read_fat_chain<'data>(
    data: &'data [u8],
    fat: &[u32],
    start: u32,
    limit: Option<u64>,
    name: &str,
) -> Result<(Cow<'data, [u8]>, Option<u32>)> {
    if !is_regular_sect(start) {
        if limit == Some(0) {
            return Ok((Cow::Borrowed(&[]), None));
        }
        bail_fmt!("FAT stream 시작 sector 오류: {name} ({start:#x})");
    }
    let mut rem = limit
        .map(|lim| {
            ensure_fmt!(lim <= data.len() as u64, "FAT stream 크기 초과: {name}");
            usize::try_from(lim).map_err(|src| err_src("FAT stream 길이 변환 실패", src))
        })
        .transpose()?;
    let (mut sid, mut prev_sid, mut traversed, mut stream_len, mut contig) =
        (start, None, 0_usize, 0_usize, true);
    while sid != END_CHAIN {
        ensure_fmt!(traversed < fat.len(), "FAT chain 순환 가능: {name} ({sid})");
        traversed = traversed.strict_add(1);
        ensure_fmt!(rem != Some(0), "FAT stream 선언 크기 초과: {name}");
        ensure_fmt!(
            is_regular_sect(sid),
            "FAT chain 잘못된 sector: {name} ({sid:#x})"
        );
        if prev_sid.is_some_and(|prev: u32| sid != prev.strict_add(1)) {
            contig = false;
        }
        let next_sid = next_fat(fat, sid)?;
        ensure_fmt!(next_sid != FREE_SECT, "FAT chain free sector 참조: {name}");
        let take = rem.map_or(SECT_SZ, |left| left.min(SECT_SZ));
        let _sector = sector_slice(data, sid)?;
        stream_len = stream_len.strict_add(take);
        if let Some(left) = rem.as_mut() {
            *left = left.strict_sub(take);
        }
        prev_sid = Some(sid);
        sid = next_sid;
    }
    if let Some(left) = rem.filter(|&bytes| bytes != 0) {
        bail_fmt!("FAT stream 짧음: {name}, remaining={left}");
    }
    if contig {
        let beg = sector_offset(start).ok_or("CFB stream 시작 범위 계산 실패")?;
        let end = beg
            .checked_add(stream_len)
            .ok_or("CFB stream 끝 범위 계산 실패")?;
        return Ok((
            Cow::Borrowed(data.get(beg..end).ok_or("CFB stream 파일 범위 초과")?),
            prev_sid,
        ));
    }
    let mut out = vec_cap(stream_len, "FAT stream")?;
    let (mut copy_rem, mut copy_sid) = (stream_len, start);
    while copy_rem != 0 {
        let sector = sector_slice(data, copy_sid)?;
        let take = copy_rem.min(sector.len());
        out.extend_from_slice(sector.split_at(take).0);
        copy_rem = copy_rem.strict_sub(take);
        copy_sid = next_fat(fat, copy_sid)?;
    }
    Ok((Cow::Owned(out), prev_sid))
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
    let text = row_text(row, idx);
    if text.is_empty() || text == "-" {
        return Ok(None);
    }
    ensure_fmt!(
        !text.starts_with('-'),
        "Opinet {row_num}행 {label} 음수 가격: {text}"
    );
    let parsed = text
        .parse::<i32>()
        .ok()
        .or_else(|| {
            let unsigned = text.trim_prefix('+');
            if !unsigned.as_bytes().first().is_some_and(u8::is_ascii_digit)
                || unsigned.ends_with('.')
                || unsigned.contains(['e', 'E'])
            {
                return None;
            }
            i32::try_from(parse_decimal_scaled(unsigned, 0, true, true)?).ok()
        })
        .ok_or_else(|| fmt_err!("Opinet {row_num}행 {label} 가격 형식 오류: {text}"))?;
    if parsed == 0_i32 {
        return Ok(None);
    }
    ensure_fmt!(
        (MIN_PRICE..=MAX_PRICE).contains(&parsed),
        "Opinet {row_num}행 {label} 가격 범위 초과: {text}"
    );
    Ok(Some(parsed))
}
fn row_text<'strings>(row: &SourceRow<'strings>, idx: usize) -> &'strings str {
    row.get(idx).copied().flatten().map_or_default(str::trim)
}
fn sector_offset(sid: u32) -> Option<usize> {
    (sid as usize).checked_add(1)?.checked_mul(SECT_SZ)
}
fn sector_slice(data: &[u8], sid: u32) -> Result<&[u8; SECT_SZ]> {
    sector_offset(sid)
        .and_then(|off| data.get(off..))
        .and_then(<[u8]>::first_chunk::<SECT_SZ>)
        .ok_or_else(|| fmt_err!("CFB sector 범위 초과: {sid}"))
}
fn validate_biff_bof(id: u16, data: &[u8], expected_sub: u16, name: &str) -> Result<()> {
    ensure_fmt!(id == 0x0809, "xls {name} 첫 레코드 BOF 아님: {id:#06x}");
    ensure_fmt!(data.len() == 16, "xls {name} BOF 길이 오류: {}", data.len());
    ensure_fmt!(
        read_u16_le(data, 0)? == 0x0600 && read_u16_le(data, 2)? == expected_sub,
        "xls {name} BOF 불일치"
    );
    Ok(())
}
fn validate_sst_option(flags: u8, allowed: u8, context: &str) -> Result<()> {
    ensure_fmt!(
        flags & !allowed == 0,
        "{context} 예약 비트 오류: {flags:#04x}"
    );
    Ok(())
}
