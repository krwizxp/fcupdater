use super::{
    ArchiveFingerprint, MAX_XLSX_PART_BYTES, PackagePart, PartRole, XLSX_PARTS, read_le_chunk,
};
use crate::diagnostic::{
    AppError, Result, bail_fmt, ensure, ensure_fmt, err, err_src, fmt_err, or_abort, path_err,
    reserve, vec_cap,
};
use core::mem;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::fs::Permissions;
use std::{
    fs::File,
    io::{BufWriter, Read as _, Seek as _, SeekFrom, Write as IoWrite},
    path::Path,
};
mod deflate;
const CD_HDR_LEN: usize = 46;
const CD_SIG: u32 = 0x0201_4b50;
const CODE_LENGTH_SYMBOLS: usize = 19;
const CRC32_TABLES: [[u32; 256]; 8] = {
    let mut previous = [0_u32; 256];
    let (mut seeds, mut seed): (&mut [u32], u32) = (&mut previous, 0);
    while let Some((slot, tail)) = seeds.split_first_mut() {
        *slot = seed;
        seeds = tail;
        seed = seed.strict_add(1);
    }
    let mut tables = [[0_u32; 256]; 8];
    let mut remaining: &mut [[u32; 256]] = &mut tables;
    while let Some((slot, tail)) = remaining.split_first_mut() {
        let mut values: &mut [u32] = &mut previous;
        while let Some((value, values_tail)) = values.split_first_mut() {
            let mut bit = 0_u8;
            while bit < 8_u8 {
                *value =
                    value.wrapping_shr(1) ^ (0xedb8_8320_u32 & 0_u32.wrapping_sub(*value & 1_u32));
                bit = bit.strict_add(1);
            }
            values = values_tail;
        }
        *slot = previous;
        remaining = tail;
    }
    tables
};
const DEFLATE_MAX_BITS: usize = 15;
const DEFLATE_MAX_BITS_U8: u8 = 15;
const DISTANCE_SYMBOLS: usize = 30;
const EOCD_LEN: usize = 22;
const EOCD_SIG: u32 = 0x0605_4b50;
const FIXED_DISTANCE_SYMBOLS: usize = 32;
const FIXED_LITERAL_SYMBOLS: usize = 288;
const EXCEL_FLAGS: u16 = 0x0006;
const HASH_SIZE: usize = 0x4000;
const LITERAL_LENGTH_SYMBOLS: usize = 286;
const LENGTH_SYMBOLS: usize = 29;
const LFH_LEN: usize = 30;
const MAX_CHAIN: usize = 16;
const MAX_MATCH: usize = 258;
const MIN_MATCH: usize = 3;
const LFH_SIG: u32 = 0x0403_4b50;
const METHOD_DEFLATE: u16 = 8;
const METHOD_STORED: u16 = 0;
const VER_MADE: u16 = 45;
const VER_NEED: u16 = 20;
const BAD_LOCAL_MSG: &str = "ZIP local header signature가 올바르지 않습니다";
const CD_SIZE_MSG: &str = "ZIP 중앙 디렉터리 크기가 entry 목록과 일치하지 않습니다.";
pub(super) const ZIP_FINGERPRINT_BUFFER_BYTES: usize = 64 * 1024;
const MAX_ARCHIVE: usize = 128 * 1024 * 1024;
const MAX_UNCOMP: usize = 256 * 1024 * 1024;
const LENGTH_BASES: [u16; LENGTH_SYMBOLS] = [
    3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131,
    163, 195, 227, 258,
];
const LENGTH_EXTRA_BITS: [u8; LENGTH_SYMBOLS] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0,
];
const DISTANCE_BASES: [u16; DISTANCE_SYMBOLS] = [
    1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537,
    2049, 3073, 0x1001, 0x1801, 0x2001, 0x3001, 0x4001, 0x6001,
];
const DISTANCE_EXTRA_BITS: [u8; DISTANCE_SYMBOLS] = [
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13,
    13,
];
const CODE_LENGTH_ORDER: [u8; CODE_LENGTH_SYMBOLS] = [
    16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
];
const _: () = assert!(
    MAX_MATCH >= MIN_MATCH,
    "deflate match bounds must be ordered"
);
const _: () = assert!(
    MAX_UNCOMP >= MAX_XLSX_PART_BYTES,
    "ZIP total limit must cover at least one entry"
);
struct StreamingZipWriter<'path> {
    archive_path: &'path Path,
    bytes_written: usize,
    deflate_workspace: deflate::DeflateWorkspace,
    file: BufWriter<&'path mut File>,
    header_buffer: Vec<u8>,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    permissions: Permissions,
}
struct ZipEntry<'zip> {
    compressed_size: u32,
    crc32: u32,
    flags: u16,
    local_header_offset: u32,
    method: u16,
    modified_date: u16,
    modified_time: u16,
    name: &'zip str,
    uncompressed_size: u32,
    version_needed: u16,
}
pub(super) struct ZipOut<'part, 'path>(
    pub &'path Path,
    pub &'path mut File,
    pub &'part [PackagePart],
    #[cfg(any(target_os = "linux", target_os = "macos"))] pub Permissions,
);
pub(super) struct ZipPkg<'path>(pub File, pub &'path Path);
struct ZipCentralDirectory<'bytes> {
    bytes: &'bytes [u8],
    cursor: usize,
    end: usize,
}
impl StreamingZipWriter<'_> {
    fn ensure_output_limit(&self, len: usize, context: &str) -> Result<usize> {
        let next = self
            .bytes_written
            .checked_add(len)
            .ok_or_else(|| fmt_err!("{context} overflow"))?;
        ensure_fmt!(
            next <= MAX_ARCHIVE,
            "xlsx 압축 크기 한도({MAX_ARCHIVE} bytes) 초과: {}",
            self.archive_path.display()
        );
        Ok(next)
    }
    fn prepare_header_buffer(&mut self, capacity: usize, context: &'static str) -> Result<()> {
        self.header_buffer.clear();
        reserve(self.header_buffer.try_reserve_exact(capacity), context)
    }
    fn write(mut self, parts: &[PackagePart]) -> Result<()> {
        let mut entries = vec_cap(parts.len(), "ZIP entry 목록")?;
        for part in parts {
            let uncomp = part.bytes.len();
            ensure_fmt!(
                uncomp <= MAX_XLSX_PART_BYTES,
                "xlsx part 크기 한도({MAX_XLSX_PART_BYTES} bytes) 초과: {} -> {}",
                self.archive_path.display(),
                part.name
            );
            let Some(plan) = self.deflate_workspace.plan(&part.bytes)? else {
                bail_fmt!("고정 XLSX part 압축 한도 초과: {}", part.name);
            };
            let comp = plan.len();
            let (comp_u32, uncomp_u32, local_off) = (
                to_u32(comp, "ZIP 압축 크기")?,
                to_u32(uncomp, "ZIP 원본 크기")?,
                to_u32(self.bytes_written, "ZIP offset")?,
            );
            let crc32 = plan.crc32();
            let hdr_len = LFH_LEN
                .checked_add(part.name.len())
                .ok_or("ZIP local header 길이 계산 실패")?;
            self.ensure_output_limit(
                hdr_len
                    .checked_add(comp)
                    .ok_or("ZIP entry 출력 크기 계산 실패")?,
                "ZIP entry 출력 크기",
            )?;
            self.prepare_header_buffer(hdr_len, "ZIP local header")?;
            self.header_buffer.extend_from_slice(&LFH_SIG.to_le_bytes());
            append_common_zip_fields(
                &mut self.header_buffer,
                crc32,
                comp_u32,
                uncomp_u32,
                part.name.len(),
            )?;
            self.header_buffer.extend_from_slice(part.name.as_bytes());
            self.write_header_buffer("xlsx local header 쓰기 실패")?;
            let actual = plan.write_to(&mut self.file)?;
            ensure_fmt!(
                actual == comp,
                "ZIP deflate 크기 불일치: expected={comp}, actual={actual}"
            );
            self.bytes_written = self.bytes_written.strict_add(comp);
            self.deflate_workspace.recycle(plan);
            entries.push((comp_u32, crc32, local_off, part, uncomp_u32));
        }
        let cd_off = to_u32(self.bytes_written, "ZIP 중앙 디렉터리 offset")?;
        let cd_size_usize = entries
            .iter()
            .map(|en| CD_HDR_LEN.saturating_add(en.3.name.len()))
            .try_fold(0_usize, usize::checked_add)
            .ok_or("ZIP 중앙 디렉터리 크기 계산 실패")?;
        let count_u16 =
            u16::try_from(entries.len()).map_err(|src| err_src("ZIP entry 수 변환 실패", src))?;
        self.ensure_output_limit(
            cd_size_usize
                .checked_add(EOCD_LEN)
                .ok_or("ZIP 중앙 디렉터리 출력 크기 계산 실패")?,
            "ZIP 중앙 디렉터리 출력 크기",
        )?;
        let cd_size = to_u32(cd_size_usize, "ZIP 중앙 디렉터리 크기")?;
        for (comp, crc32, local_off, part, uncomp) in entries {
            let rec_len = CD_HDR_LEN
                .checked_add(part.name.len())
                .ok_or("ZIP 중앙 디렉터리 entry 길이 계산 실패")?;
            self.prepare_header_buffer(rec_len, "ZIP 중앙 디렉터리")?;
            self.header_buffer.extend_from_slice(&CD_SIG.to_le_bytes());
            self.header_buffer
                .extend_from_slice(&VER_MADE.to_le_bytes());
            append_common_zip_fields(
                &mut self.header_buffer,
                crc32,
                comp,
                uncomp,
                part.name.len(),
            )?;
            self.header_buffer.extend_from_slice(&[0_u8; 10]);
            self.header_buffer
                .extend_from_slice(&local_off.to_le_bytes());
            self.header_buffer.extend_from_slice(part.name.as_bytes());
            self.write_header_buffer("xlsx 중앙 디렉터리 쓰기 실패")?;
        }
        self.prepare_header_buffer(EOCD_LEN, "ZIP EOCD")?;
        for chunk in [
            EOCD_SIG.to_le_bytes().as_slice(),
            &0_u32.to_le_bytes(),
            &count_u16.to_le_bytes(),
            &count_u16.to_le_bytes(),
            &cd_size.to_le_bytes(),
            &cd_off.to_le_bytes(),
            &0_u16.to_le_bytes(),
        ] {
            self.header_buffer.extend_from_slice(chunk);
        }
        self.write_header_buffer("xlsx EOCD 쓰기 실패")?;
        IoWrite::flush(&mut self.file)
            .map_err(|src| path_err("xlsx 압축 flush 실패", self.archive_path, src))?;
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        self.file
            .get_ref()
            .set_permissions(self.permissions)
            .map_err(|src| path_err("xlsx 권한 적용 실패", self.archive_path, src))?;
        self.file
            .get_ref()
            .sync_all()
            .map_err(|src| path_err("xlsx sync 실패", self.archive_path, src))
    }
    fn write_header_buffer(&mut self, context: &str) -> Result<()> {
        let next = self.ensure_output_limit(self.header_buffer.len(), "ZIP written bytes")?;
        IoWrite::write_all(&mut self.file, &self.header_buffer)
            .map_err(|src| path_err(context, self.archive_path, src))?;
        self.bytes_written = next;
        Ok(())
    }
}
impl ZipEntry<'_> {
    fn data(
        &self,
        bytes: &mut Vec<u8>,
        bytes_offset: usize,
        expected_len: usize,
        expected_local_offset: usize,
    ) -> Result<(Vec<u8>, usize)> {
        let local_offset = self.local_header_offset as usize;
        ensure_fmt!(
            local_offset == expected_local_offset,
            "ZIP local record 순서 오류: {}",
            self.name
        );
        let rel_off = local_offset
            .checked_sub(bytes_offset)
            .ok_or("ZIP local record 상대 offset 계산 실패")?;
        let (hdr, after_hdr) =
            split_header_at::<LFH_LEN>(bytes, rel_off, BAD_LOCAL_MSG).map_err(|mut src| {
                src.update_message(|msg| format!("{msg}: {}", self.name));
                src
            })?;
        let mismatch = |msg: &'static str| zip_entry_err(msg, self.name);
        if read_u32(hdr, 0)? != LFH_SIG {
            return Err(mismatch(BAD_LOCAL_MSG));
        }
        for (off, exp, msg) in [
            (4, self.version_needed, "ZIP local version 불일치"),
            (6, self.flags, "ZIP local flags 불일치"),
            (8, self.method, "ZIP local 압축 방식 불일치"),
            (10, self.modified_time, "ZIP local 수정 시각 불일치"),
            (12, self.modified_date, "ZIP local 수정 시각 불일치"),
        ] {
            if read_u16(hdr, off)? != exp {
                return Err(mismatch(msg));
            }
        }
        let (l_crc, l_comp, l_uncomp) =
            (read_u32(hdr, 14)?, read_u32(hdr, 18)?, read_u32(hdr, 22)?);
        let data_err = if self.flags & 0x0008 == 0 {
            (l_crc != self.crc32
                || l_comp != self.compressed_size
                || l_uncomp != self.uncompressed_size)
                .then_some("ZIP local CRC/크기 불일치")
        } else {
            ((l_crc != 0 && l_crc != self.crc32)
                || (l_comp != 0 && l_comp != self.compressed_size)
                || (l_uncomp != 0 && l_uncomp != self.uncompressed_size))
                .then_some("ZIP data descriptor local CRC/크기 오류")
        };
        if let Some(msg) = data_err {
            return Err(mismatch(msg));
        }
        let (name_len, extra_len) = (
            usize::from(read_u16(hdr, 26)?),
            usize::from(read_u16(hdr, 28)?),
        );
        let (local_name, after_name) = after_hdr
            .split_at_checked(name_len)
            .ok_or("ZIP local header 이름 범위 오류")?;
        if local_name != self.name.as_bytes() {
            return Err(mismatch("ZIP local header 이름 불일치"));
        }
        let (local_extra, after_extra) = after_name
            .split_at_checked(extra_len)
            .ok_or("ZIP local extra 범위 오류")?;
        validate_zip_extra(local_extra, self.name)?;
        let comp_len = self.compressed_size as usize;
        let (compressed, desc_tail) = after_extra
            .split_at_checked(comp_len)
            .ok_or_else(|| mismatch("ZIP entry 데이터 범위 초과"))?;
        let data_start = rel_off
            .strict_add(LFH_LEN)
            .strict_add(name_len)
            .strict_add(extra_len);
        let data_end = data_start.strict_add(comp_len);
        let local_end = if self.flags & 0x0008 == 0 {
            data_end
        } else {
            let (crc_off, desc_len) = if read_u32(desc_tail, 0)? == 0x0807_4b50 {
                (4, 16)
            } else {
                (0, 12)
            };
            let desc = desc_tail
                .get(..desc_len)
                .ok_or("ZIP data descriptor 범위 오류")?;
            if read_u32(desc, crc_off)? != self.crc32
                || read_u32(desc, crc_off.strict_add(4))? != self.compressed_size
                || read_u32(desc, crc_off.strict_add(8))? != self.uncompressed_size
            {
                return Err(mismatch("ZIP data descriptor 불일치"));
            }
            data_end
                .checked_add(desc_len)
                .ok_or("ZIP local record 끝 계산 실패")?
        };
        let (output, output_crc) = if self.method == METHOD_DEFLATE {
            deflate::Inflater(compressed, expected_len).inflate()?
        } else {
            if comp_len != expected_len {
                return Err(mismatch("ZIP stored entry 크기 불일치"));
            }
            let crc = !crc32_update(u32::MAX, compressed);
            bytes.copy_within(data_start..data_end, 0);
            bytes.truncate(comp_len);
            (mem::take(bytes), crc)
        };
        if output.len() != expected_len {
            return Err(mismatch("ZIP 해제 크기 불일치"));
        }
        if output_crc != self.crc32 {
            return Err(mismatch("ZIP CRC 불일치"));
        }
        Ok((
            output,
            bytes_offset
                .checked_add(local_end)
                .ok_or("ZIP local record 절대 끝 계산 실패")?,
        ))
    }
}
impl<'bytes> ZipCentralDirectory<'bytes> {
    fn next_entry(&mut self) -> Result<ZipEntry<'bytes>> {
        let (hdr, tail) = split_header_at::<CD_HDR_LEN>(
            self.bytes,
            self.cursor,
            "ZIP 중앙 디렉터리 header 범위 오류",
        )?;
        ensure(
            read_u32(hdr, 0)? == CD_SIG,
            "ZIP 중앙 디렉터리 signature 오류",
        )?;
        let (version_needed, flags, method) =
            (read_u16(hdr, 6)?, read_u16(hdr, 8)?, read_u16(hdr, 10)?);
        ensure(
            method == METHOD_DEFLATE || method == METHOD_STORED,
            "ZIP 미지원 압축 방식",
        )?;
        let min_ver = if method == METHOD_DEFLATE { 20 } else { 10 };
        ensure(
            (min_ver..=VER_NEED).contains(&version_needed),
            "ZIP version 범위 오류",
        )?;
        ensure(flags & !0x080e == 0, "ZIP 미지원 flags")?;
        ensure(
            method != METHOD_STORED || flags & 0x0006 == 0,
            "ZIP stored DEFLATE flag 오류",
        )?;
        let (name_len, extra_len, comment_len) = (
            usize::from(read_u16(hdr, 28)?),
            usize::from(read_u16(hdr, 30)?),
            usize::from(read_u16(hdr, 32)?),
        );
        ensure(read_u16(hdr, 34)? == 0, "분할 ZIP archive 미지원")?;
        let next_cursor = CD_HDR_LEN
            .checked_add(name_len)
            .and_then(|len| len.checked_add(extra_len))
            .and_then(|len| len.checked_add(comment_len))
            .and_then(|entry_len| self.cursor.checked_add(entry_len))
            .ok_or("ZIP 중앙 디렉터리 위치 계산 실패")?;
        ensure(next_cursor <= self.end, CD_SIZE_MSG)?;
        let name = str::from_utf8(tail.get(..name_len).ok_or("ZIP entry 이름 범위 오류")?)
            .map_err(|src| err_src("ZIP entry 이름 UTF-8 오류", src))?;
        let extra_end = name_len
            .checked_add(extra_len)
            .ok_or("ZIP extra 범위 계산 실패")?;
        validate_zip_extra(
            tail.get(name_len..extra_end).ok_or("ZIP extra 범위 오류")?,
            name,
        )?;
        self.cursor = next_cursor;
        Ok(ZipEntry {
            compressed_size: read_u32(hdr, 20)?,
            crc32: read_u32(hdr, 16)?,
            flags,
            local_header_offset: read_u32(hdr, 42)?,
            method,
            modified_date: read_u16(hdr, 14)?,
            modified_time: read_u16(hdr, 12)?,
            name,
            uncompressed_size: read_u32(hdr, 24)?,
            version_needed,
        })
    }
}
impl ZipOut<'_, '_> {
    pub(super) fn create(self) -> Result<()> {
        StreamingZipWriter {
            archive_path: self.0,
            bytes_written: 0,
            deflate_workspace: deflate::DeflateWorkspace::default(),
            file: BufWriter::with_capacity(64 * 1024, self.1),
            header_buffer: Vec::new(),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            permissions: self.3,
        }
        .write(self.2)
    }
}
impl ZipPkg<'_> {
    pub(super) fn read(self) -> Result<(ArchiveFingerprint, Vec<PackagePart>)> {
        let (mut archive_file, archive_path) = (self.0, self.1);
        let archive_len = archive_file_len(&archive_file, archive_path)?;
        ensure(archive_len >= EOCD_LEN, "ZIP 파일이 너무 짧습니다.")?;
        let tail_start = archive_len.saturating_sub(EOCD_LEN.strict_add(0xffff));
        let mut tail_bytes = Vec::new();
        read_archive_range(
            &mut archive_file,
            archive_path,
            Some(tail_start),
            archive_len.strict_sub(tail_start),
            &mut tail_bytes,
        )?;
        let search_bytes = tail_bytes
            .get(..tail_bytes.len().strict_sub(EOCD_LEN).strict_add(4))
            .ok_or("ZIP EOCD 검색 범위 오류")?;
        let eocd_sig = EOCD_SIG.to_le_bytes();
        let mut search_len = search_bytes.len();
        let (eocd_offset, eocd_rel, eocd) = loop {
            let rel = search_bytes
                .get(..search_len)
                .ok_or("ZIP EOCD 검색 범위 오류")?
                .array_windows::<4>()
                .rposition(|win| *win == eocd_sig)
                .ok_or("ZIP EOCD를 찾지 못했습니다.")?;
            let (eocd, _) = split_header_at::<EOCD_LEN>(
                tail_bytes.as_slice(),
                rel,
                "ZIP EOCD header 범위 오류",
            )?;
            let offset = tail_start.strict_add(rel);
            let comment_len = usize::from(read_u16(eocd, 20)?);
            if offset
                .checked_add(EOCD_LEN)
                .and_then(|val| val.checked_add(comment_len))
                == Some(archive_len)
            {
                break (offset, rel, eocd);
            }
            search_len = rel;
        };
        let entries_total = read_u16(eocd, 10)?;
        ensure(
            read_u16(eocd, 4)? == 0
                && read_u16(eocd, 6)? == 0
                && read_u16(eocd, 8)? == entries_total,
            "분할 ZIP archive 미지원",
        )?;
        let entry_count = usize::from(entries_total);
        let mut slots = [const { None }; XLSX_PARTS.len()];
        let entries = slots
            .get_mut(..entry_count)
            .ok_or_else(|| fmt_err!("ZIP entry 수 상한 초과: {entry_count}"))?;
        let (cd_size, cd_offset) = (read_u32(eocd, 12)? as usize, read_u32(eocd, 16)? as usize);
        ensure(
            cd_offset.checked_add(cd_size) == Some(eocd_offset),
            "ZIP 중앙 디렉터리와 EOCD 사이 추가 데이터 미지원",
        )?;
        let mut central_bytes = Vec::new();
        read_archive_range(
            &mut archive_file,
            archive_path,
            Some(cd_offset),
            cd_size,
            &mut central_bytes,
        )?;
        let mut cd = ZipCentralDirectory {
            bytes: central_bytes.as_slice(),
            cursor: 0,
            end: cd_size,
        };
        let (mut total_uncomp, mut seen) = (0_usize, [false; XLSX_PARTS.len()]);
        for slot in entries.iter_mut() {
            let entry = cd.next_entry()?;
            let (part_idx, (part_name, _, _)) = XLSX_PARTS
                .iter()
                .copied()
                .enumerate()
                .find(|&(_, (name, _, _))| name == entry.name)
                .ok_or_else(|| fmt_err!("ZIP entry 미지원 이름: {}", entry.name))?;
            ensure_fmt!(
                !mem::replace(or_abort(seen.get_mut(part_idx)), true),
                "ZIP entry 중복: {part_name}"
            );
            let exp_len = entry.uncompressed_size as usize;
            ensure_fmt!(
                exp_len <= MAX_XLSX_PART_BYTES,
                "ZIP entry 해제 크기 한도({MAX_XLSX_PART_BYTES} bytes) 초과: {}",
                entry.name
            );
            total_uncomp = total_uncomp
                .checked_add(exp_len)
                .ok_or("ZIP 전체 해제 크기 계산 실패")?;
            ensure_fmt!(
                total_uncomp <= MAX_UNCOMP,
                "ZIP 전체 해제 크기 한도({MAX_UNCOMP} bytes) 초과: {}",
                entry.name
            );
            *slot = Some((entry, part_idx, exp_len));
        }
        ensure(cd.cursor == cd.end, CD_SIZE_MSG)?;
        for ((name, role, _), present) in XLSX_PARTS.into_iter().zip(seen) {
            ensure_fmt!(
                role != PartRole::Required || present,
                "ZIP 필수 entry 없음: {name}"
            );
        }
        entries.sort_unstable_by_key(|slot| slot.as_ref().map(|item| item.0.local_header_offset));
        let (mut exp_local_off, mut archive_crc) = (0_usize, u32::MAX);
        let mut parts = vec_cap(entry_count, "ZIP package part 목록")?;
        let mut rec_bytes = Vec::new();
        let mut iter = entries.iter_mut().filter_map(Option::take).peekable();
        while let Some((entry, part_idx, exp_len)) = iter.next() {
            let local_off = entry.local_header_offset as usize;
            let next_off = iter
                .peek()
                .map_or(cd_offset, |item| item.0.local_header_offset as usize);
            let rec_len = next_off
                .checked_sub(local_off)
                .ok_or("ZIP local record 순서 오류")?;
            read_archive_range(
                &mut archive_file,
                archive_path,
                (exp_local_off == 0 || exp_local_off != local_off).then_some(local_off),
                rec_len,
                &mut rec_bytes,
            )?;
            archive_crc = crc32_update(archive_crc, &rec_bytes);
            let (bytes, local_end) =
                entry.data(&mut rec_bytes, local_off, exp_len, exp_local_off)?;
            exp_local_off = local_end;
            parts.push(PackagePart {
                bytes,
                name: or_abort(XLSX_PARTS.get(part_idx)).0,
            });
        }
        ensure(
            exp_local_off == cd_offset,
            "ZIP local/central record 범위 불일치",
        )?;
        let expected_end = tail_bytes
            .get(eocd_rel..)
            .ok_or("ZIP EOCD 검증 범위 오류")?;
        for (len, expected) in [
            (cd_size, central_bytes.as_slice()),
            (archive_len.strict_sub(eocd_offset), expected_end),
        ] {
            read_archive_range(&mut archive_file, archive_path, None, len, &mut rec_bytes)?;
            if rec_bytes != expected {
                return Err(archive_changed(archive_path));
            }
            archive_crc = crc32_update(archive_crc, &rec_bytes);
        }
        if archive_file_len(&archive_file, archive_path)? != archive_len {
            return Err(archive_changed(archive_path));
        }
        Ok((
            ArchiveFingerprint {
                crc32: !archive_crc,
                len: archive_len,
            },
            parts,
        ))
    }
}
fn append_common_zip_fields(
    out: &mut Vec<u8>,
    crc32: u32,
    comp: u32,
    uncomp: u32,
    name_len: usize,
) -> Result<()> {
    let name_len_u16 =
        u16::try_from(name_len).map_err(|src| err_src("ZIP 이름 길이 변환 실패", src))?;
    for chunk in [
        VER_NEED.to_le_bytes().as_slice(),
        &EXCEL_FLAGS.to_le_bytes(),
        &METHOD_DEFLATE.to_le_bytes(),
        &[0, 0, 0x21, 0],
        &crc32.to_le_bytes(),
        &comp.to_le_bytes(),
        &uncomp.to_le_bytes(),
        &name_len_u16.to_le_bytes(),
        &0_u16.to_le_bytes(),
    ] {
        out.extend_from_slice(chunk);
    }
    Ok(())
}
fn archive_changed(archive_path: &Path) -> AppError {
    fmt_err!(
        "xlsx 압축 파일이 읽는 중 변경됨: {}",
        archive_path.display()
    )
}
pub(super) fn archive_file_len(file: &File, archive_path: &Path) -> Result<usize> {
    let meta = file
        .metadata()
        .map_err(|src| path_err("xlsx 파일 정보 확인 실패", archive_path, src))?;
    let len = usize::try_from(meta.len()).map_err(|src| {
        fmt_err!(
            "xlsx 파일 크기 변환 실패({}): {src}",
            archive_path.display()
        )
    })?;
    ensure_fmt!(
        len <= MAX_ARCHIVE,
        "xlsx 파일 크기 한도({MAX_ARCHIVE} bytes) 초과: {}",
        archive_path.display()
    );
    Ok(len)
}
#[inline]
fn crc32_table_value(table_index: usize, byte: u8) -> u32 {
    *or_abort(or_abort(CRC32_TABLES.get(table_index)).get(usize::from(byte)))
}
pub(super) fn crc32_update(initial: u32, bytes: &[u8]) -> u32 {
    let (mut crc, mut remaining) = (initial, bytes);
    while let Some((chunk, tail)) = remaining.split_first_chunk::<8>() {
        let [b0, b1, b2, b3, b4, b5, b6, b7] = *chunk;
        let [c0, c1, c2, c3] = (crc ^ u32::from_le_bytes([b0, b1, b2, b3])).to_le_bytes();
        crc = crc32_table_value(7, c0)
            ^ crc32_table_value(6, c1)
            ^ crc32_table_value(5, c2)
            ^ crc32_table_value(4, c3)
            ^ crc32_table_value(3, b4)
            ^ crc32_table_value(2, b5)
            ^ crc32_table_value(1, b6)
            ^ crc32_table_value(0, b7);
        remaining = tail;
    }
    remaining.iter().fold(crc, |value, &byte| {
        let [table_index, ..] = (value ^ u32::from(byte)).to_le_bytes();
        (value >> 8_u8) ^ crc32_table_value(0, table_index)
    })
}
fn read_archive_range(
    file: &mut File,
    archive_path: &Path,
    offset: Option<usize>,
    len: usize,
    buffer: &mut Vec<u8>,
) -> Result<()> {
    if let Some(off) = offset {
        file.seek(SeekFrom::Start(off as u64))
            .map_err(|src| path_err("xlsx range 이동 실패", archive_path, src))?;
    }
    buffer.clear();
    reserve(buffer.try_reserve_exact(len), "ZIP 입력 range")?;
    file.take(len as u64)
        .read_to_end(buffer)
        .map_err(|src| path_err("xlsx range 읽기 실패", archive_path, src))?;
    if buffer.len() != len {
        return Err(archive_changed(archive_path));
    }
    Ok(())
}
fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    Ok(u16::from_le_bytes(
        read_le_chunk::<2>(bytes, offset).ok_or("ZIP u16 범위 오류")?,
    ))
}
fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        read_le_chunk::<4>(bytes, offset).ok_or("ZIP u32 범위 오류")?,
    ))
}
fn split_header_at<'bytes, const LEN: usize>(
    bytes: &'bytes [u8],
    offset: usize,
    context: &'static str,
) -> Result<(&'bytes [u8; LEN], &'bytes [u8])> {
    bytes
        .get(offset..)
        .and_then(|rem| rem.split_first_chunk::<LEN>())
        .ok_or_else(|| err(context))
}
fn to_u32(value: usize, context: &str) -> Result<u32> {
    u32::try_from(value).map_err(|src| err_src(format!("{context} 변환 실패"), src))
}
fn validate_zip_extra(extra: &[u8], entry_name: &str) -> Result<()> {
    let mut cursor = 0_usize;
    while cursor < extra.len() {
        let hdr = extra
            .get(cursor..)
            .and_then(|bytes| bytes.split_first_chunk::<4>())
            .map(|val| val.0)
            .ok_or_else(|| zip_entry_err("ZIP extra header 손상", entry_name))?;
        let id = u16::from_le_bytes([hdr[0], hdr[1]]);
        let len = usize::from(u16::from_le_bytes([hdr[2], hdr[3]]));
        if matches!(id, 0x0001 | 0x9901) {
            return Err(zip_entry_err("ZIP64/AES extra 미지원", entry_name));
        }
        cursor = cursor
            .checked_add(4)
            .and_then(|start| start.checked_add(len))
            .filter(|&end| end <= extra.len())
            .ok_or_else(|| zip_entry_err("ZIP extra 범위 손상", entry_name))?;
    }
    Ok(())
}
fn zip_entry_err(context: &str, entry_name: &str) -> AppError {
    fmt_err!("{context}: {entry_name}")
}
