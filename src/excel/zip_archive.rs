use super::{
    ArchiveFingerprint, MAX_XLSX_PART_BYTES, PackagePart, PartRole, XLSX_PARTS, ZipPackageReader,
    read_le_chunk,
};
use crate::diagnostic::{
    AppError, Result, ensure, ensure_fmt, err, err_with_source, fmt_err, map_reserve, or_abort,
    path_err_with_source, try_vec_with_capacity,
};
use core::mem;
use std::{
    fs::File,
    io::{Read as _, Seek as _, SeekFrom},
    path::Path,
};
mod deflate;
mod write;
const CENTRAL_DIRECTORY_HEADER_LEN: usize = 46;
const CENTRAL_DIRECTORY_SIGNATURE: u32 = 0x0201_4b50;
const CODE_LENGTH_SYMBOLS: usize = 19;
const CRC32_TABLES: [[u32; 256]; 8] = {
    let mut previous = [0_u32; 256];
    let mut seeds: &mut [u32] = &mut previous;
    let mut seed = 0_u32;
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
const END_OF_CENTRAL_DIRECTORY_LEN: usize = 22;
const END_OF_CENTRAL_DIRECTORY_SIGNATURE: u32 = 0x0605_4b50;
const FIXED_DISTANCE_SYMBOLS: usize = 32;
const FIXED_LITERAL_SYMBOLS: usize = 288;
const EXCEL_ENTRY_FLAGS: u16 = 0x0006;
const HASH_SIZE: usize = 0x4000;
const LITERAL_LENGTH_SYMBOLS: usize = 286;
const LENGTH_SYMBOLS: usize = 29;
const LOCAL_FILE_HEADER_LEN: usize = 30;
const MAX_CHAIN: usize = 16;
const MAX_MATCH: usize = 258;
const MIN_MATCH: usize = 3;
const LOCAL_FILE_HEADER_SIGNATURE: u32 = 0x0403_4b50;
const METHOD_DEFLATE: u16 = 8;
const METHOD_STORED: u16 = 0;
const VERSION_MADE_BY: u16 = 45;
const VERSION_NEEDED: u16 = 20;
const ZIP_BAD_LOCAL_HEADER_MESSAGE: &str = "ZIP local header signature가 올바르지 않습니다";
const ZIP_CENTRAL_DIRECTORY_SIZE_MISMATCH_MESSAGE: &str =
    "ZIP 중앙 디렉터리 크기가 entry 목록과 일치하지 않습니다.";
pub(super) const ZIP_FINGERPRINT_BUFFER_BYTES: usize = 64 * 1024;
const ZIP_MAX_ARCHIVE_BYTES: usize = 128 * 1024 * 1024;
const ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES: usize = 256 * 1024 * 1024;
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
    ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES >= MAX_XLSX_PART_BYTES,
    "ZIP total limit must cover at least one entry"
);
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
struct ZipCentralDirectory<'bytes> {
    bytes: &'bytes [u8],
    cursor: usize,
    end: usize,
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
            "ZIP local record가 연속된 고정 순서가 아닙니다: {}",
            self.name
        );
        let relative_local_offset = local_offset
            .checked_sub(bytes_offset)
            .ok_or("ZIP local record 상대 offset 계산 실패")?;
        let (local_header, after_header) = split_header_at::<LOCAL_FILE_HEADER_LEN>(
            bytes,
            relative_local_offset,
            ZIP_BAD_LOCAL_HEADER_MESSAGE,
        )
        .map_err(|mut source| {
            source.update_message(|message| format!("{message}: {}", self.name));
            source
        })?;
        let local_mismatch = |message: &'static str| zip_entry_err(message, self.name);
        if read_u32(local_header, 0)? != LOCAL_FILE_HEADER_SIGNATURE {
            return Err(local_mismatch(ZIP_BAD_LOCAL_HEADER_MESSAGE));
        }
        for (offset, expected, message) in [
            (
                4,
                self.version_needed,
                "ZIP local header version이 중앙 디렉터리와 다릅니다",
            ),
            (
                6,
                self.flags,
                "ZIP local header flags가 중앙 디렉터리와 다릅니다",
            ),
            (
                8,
                self.method,
                "ZIP local header 압축 방식이 중앙 디렉터리와 다릅니다",
            ),
            (
                10,
                self.modified_time,
                "ZIP local header 수정 시각이 중앙 디렉터리와 다릅니다",
            ),
            (
                12,
                self.modified_date,
                "ZIP local header 수정 시각이 중앙 디렉터리와 다릅니다",
            ),
        ] {
            if read_u16(local_header, offset)? != expected {
                return Err(local_mismatch(message));
            }
        }
        let (local_crc, local_comp, local_uncomp) = (
            read_u32(local_header, 14)?,
            read_u32(local_header, 18)?,
            read_u32(local_header, 22)?,
        );
        let local_data_error = if self.flags & 0x0008 == 0 {
            (local_crc != self.crc32
                || local_comp != self.compressed_size
                || local_uncomp != self.uncompressed_size)
                .then_some("ZIP local CRC 또는 크기가 중앙 디렉터리와 다릅니다")
        } else {
            ((local_crc != 0 && local_crc != self.crc32)
                || (local_comp != 0 && local_comp != self.compressed_size)
                || (local_uncomp != 0 && local_uncomp != self.uncompressed_size))
                .then_some("ZIP data descriptor local CRC 또는 크기가 올바르지 않습니다")
        };
        if let Some(message) = local_data_error {
            return Err(local_mismatch(message));
        }
        let (name_len, extra_len) = (
            usize::from(read_u16(local_header, 26)?),
            usize::from(read_u16(local_header, 28)?),
        );
        let (local_name, after_name) = after_header
            .split_at_checked(name_len)
            .ok_or("ZIP local header 이름 범위 오류")?;
        if local_name != self.name.as_bytes() {
            return Err(local_mismatch(
                "ZIP local header 이름이 중앙 디렉터리와 다릅니다",
            ));
        }
        let (local_extra, after_extra) = after_name
            .split_at_checked(extra_len)
            .ok_or("ZIP local extra 범위 오류")?;
        validate_zip_extra(local_extra, self.name)?;
        let compressed_len = self.compressed_size as usize;
        let (compressed, descriptor_tail) = after_extra
            .split_at_checked(compressed_len)
            .ok_or_else(|| local_mismatch("ZIP entry 데이터가 파일 범위를 벗어났습니다"))?;
        let data_start = relative_local_offset
            .strict_add(LOCAL_FILE_HEADER_LEN)
            .strict_add(name_len)
            .strict_add(extra_len);
        let data_end = data_start.strict_add(compressed_len);
        let local_end = if self.flags & 0x0008 == 0 {
            data_end
        } else {
            let (crc_offset, descriptor_len) = if read_u32(descriptor_tail, 0)? == 0x0807_4b50 {
                (4, 16)
            } else {
                (0, 12)
            };
            let descriptor = descriptor_tail
                .get(..descriptor_len)
                .ok_or("ZIP data descriptor 범위 오류")?;
            if read_u32(descriptor, crc_offset)? != self.crc32
                || read_u32(descriptor, crc_offset.strict_add(4))? != self.compressed_size
                || read_u32(descriptor, crc_offset.strict_add(8))? != self.uncompressed_size
            {
                return Err(local_mismatch(
                    "ZIP data descriptor가 중앙 디렉터리와 다릅니다",
                ));
            }
            data_end
                .checked_add(descriptor_len)
                .ok_or("ZIP local record 끝 계산 실패")?
        };
        let (output, output_crc32) = if self.method == METHOD_DEFLATE {
            (deflate::DeflateInflater {
                bytes: compressed,
                expected_len,
            })
            .inflate()?
        } else {
            if compressed_len != expected_len {
                return Err(local_mismatch(
                    "ZIP stored entry의 압축/해제 크기가 다릅니다",
                ));
            }
            let crc = !crc32_update(u32::MAX, compressed);
            bytes.copy_within(data_start..data_end, 0);
            bytes.truncate(compressed_len);
            (mem::take(bytes), crc)
        };
        if output.len() != expected_len {
            return Err(local_mismatch("ZIP 해제 크기가 일치하지 않습니다"));
        }
        if output_crc32 != self.crc32 {
            return Err(local_mismatch("ZIP CRC가 일치하지 않습니다"));
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
        let (header, tail) = split_header_at::<CENTRAL_DIRECTORY_HEADER_LEN>(
            self.bytes,
            self.cursor,
            "ZIP 중앙 디렉터리 header 범위 오류",
        )?;
        ensure(
            read_u32(header, 0)? == CENTRAL_DIRECTORY_SIGNATURE,
            "ZIP 중앙 디렉터리 signature가 올바르지 않습니다.",
        )?;
        let (version_needed, flags, method) = (
            read_u16(header, 6)?,
            read_u16(header, 8)?,
            read_u16(header, 10)?,
        );
        ensure(
            method == METHOD_DEFLATE || method == METHOD_STORED,
            "ZIP entry 압축 방식을 지원하지 않습니다.",
        )?;
        let minimum_version = if method == METHOD_DEFLATE { 20 } else { 10 };
        ensure(
            (minimum_version..=VERSION_NEEDED).contains(&version_needed),
            "ZIP entry version이 지원 범위를 벗어났습니다.",
        )?;
        ensure(
            flags & !0x080e == 0,
            "ZIP entry flags에 지원하지 않는 기능이 있습니다.",
        )?;
        ensure(
            method != METHOD_STORED || flags & 0x0006 == 0,
            "ZIP stored entry에 DEFLATE 압축 옵션 flag가 있습니다.",
        )?;
        let (name_len, extra_len, comment_len) = (
            usize::from(read_u16(header, 28)?),
            usize::from(read_u16(header, 30)?),
            usize::from(read_u16(header, 32)?),
        );
        ensure(
            read_u16(header, 34)? == 0,
            "분할 ZIP archive는 지원하지 않습니다.",
        )?;
        let next_cursor = CENTRAL_DIRECTORY_HEADER_LEN
            .checked_add(name_len)
            .and_then(|len| len.checked_add(extra_len))
            .and_then(|len| len.checked_add(comment_len))
            .and_then(|entry_len| self.cursor.checked_add(entry_len))
            .ok_or("ZIP 중앙 디렉터리 다음 entry 위치 계산 실패")?;
        ensure(
            next_cursor <= self.end,
            ZIP_CENTRAL_DIRECTORY_SIZE_MISMATCH_MESSAGE,
        )?;
        let name_bytes = tail
            .get(..name_len)
            .ok_or("ZIP entry 이름이 파일 범위를 벗어났습니다.")?;
        let name = str::from_utf8(name_bytes)
            .map_err(|source| err_with_source("ZIP entry 이름이 UTF-8이 아닙니다", source))?;
        let extra_end = name_len
            .checked_add(extra_len)
            .ok_or("ZIP 중앙 extra 범위 계산 실패")?;
        validate_zip_extra(
            tail.get(name_len..extra_end)
                .ok_or("ZIP 중앙 extra 범위 오류")?,
            name,
        )?;
        self.cursor = next_cursor;
        Ok(ZipEntry {
            compressed_size: read_u32(header, 20)?,
            crc32: read_u32(header, 16)?,
            flags,
            local_header_offset: read_u32(header, 42)?,
            method,
            modified_date: read_u16(header, 14)?,
            modified_time: read_u16(header, 12)?,
            name,
            uncompressed_size: read_u32(header, 24)?,
            version_needed,
        })
    }
}
impl ZipPackageReader<'_> {
    pub(super) fn read(mut self) -> Result<(ArchiveFingerprint, Vec<PackagePart>)> {
        let archive_len = archive_file_len(&self.archive_file, self.archive_path)?;
        ensure(
            archive_len >= END_OF_CENTRAL_DIRECTORY_LEN,
            "ZIP 파일이 너무 짧습니다.",
        )?;
        let tail_start =
            archive_len.saturating_sub(END_OF_CENTRAL_DIRECTORY_LEN.strict_add(0xffff));
        let mut tail_bytes = Vec::new();
        read_archive_range(
            &mut self.archive_file,
            self.archive_path,
            Some(tail_start),
            archive_len.strict_sub(tail_start),
            &mut tail_bytes,
        )?;
        let search_bytes = tail_bytes
            .get(
                ..tail_bytes
                    .len()
                    .strict_sub(END_OF_CENTRAL_DIRECTORY_LEN)
                    .strict_add(4),
            )
            .ok_or("ZIP EOCD 검색 범위 오류")?;
        let eocd_signature = END_OF_CENTRAL_DIRECTORY_SIGNATURE.to_le_bytes();
        let mut search_len = search_bytes.len();
        let (eocd_offset, eocd_relative_offset, eocd) = loop {
            let relative_offset = search_bytes
                .get(..search_len)
                .ok_or("ZIP EOCD 검색 범위 오류")?
                .array_windows::<4>()
                .rposition(|window| *window == eocd_signature)
                .ok_or("ZIP EOCD를 찾지 못했습니다.")?;
            let (eocd, _) = split_header_at::<END_OF_CENTRAL_DIRECTORY_LEN>(
                tail_bytes.as_slice(),
                relative_offset,
                "ZIP EOCD header 범위 오류",
            )?;
            let offset = tail_start.strict_add(relative_offset);
            let comment_len = usize::from(read_u16(eocd, 20)?);
            if offset
                .checked_add(END_OF_CENTRAL_DIRECTORY_LEN)
                .and_then(|value| value.checked_add(comment_len))
                == Some(archive_len)
            {
                break (offset, relative_offset, eocd);
            }
            search_len = relative_offset;
        };
        let entries_total = read_u16(eocd, 10)?;
        ensure(
            read_u16(eocd, 4)? == 0
                && read_u16(eocd, 6)? == 0
                && read_u16(eocd, 8)? == entries_total,
            "분할 ZIP archive는 지원하지 않습니다.",
        )?;
        let entry_count = usize::from(entries_total);
        let mut slots = [const { None }; XLSX_PARTS.len()];
        let entries = slots
            .get_mut(..entry_count)
            .ok_or_else(|| fmt_err!("ZIP entry 수가 지원 상한을 초과했습니다: {entry_count}"))?;
        let (central_dir_size, central_dir_offset) =
            (read_u32(eocd, 12)? as usize, read_u32(eocd, 16)? as usize);
        let central_dir_end = central_dir_offset
            .checked_add(central_dir_size)
            .ok_or("ZIP 중앙 디렉터리 범위 계산 실패")?;
        ensure(
            central_dir_end == eocd_offset,
            "ZIP 중앙 디렉터리와 EOCD 사이의 추가 데이터는 지원하지 않습니다.",
        )?;
        let mut central_bytes = Vec::new();
        read_archive_range(
            &mut self.archive_file,
            self.archive_path,
            Some(central_dir_offset),
            central_dir_size,
            &mut central_bytes,
        )?;
        let mut central_directory = ZipCentralDirectory {
            bytes: central_bytes.as_slice(),
            cursor: 0,
            end: central_dir_size,
        };
        let mut total_uncompressed = 0_usize;
        let mut seen = [false; XLSX_PARTS.len()];
        for slot in entries.iter_mut() {
            let entry = central_directory.next_entry()?;
            let (part_index, (part_name, _, _)) = XLSX_PARTS
                .iter()
                .copied()
                .enumerate()
                .find(|&(_, (name, _, _))| name == entry.name)
                .ok_or_else(|| {
                    fmt_err!("ZIP entry 이름이 고정 스키마에 없습니다: {}", entry.name)
                })?;
            ensure_fmt!(
                !mem::replace(or_abort(seen.get_mut(part_index)), true),
                "ZIP entry 이름이 중복되었습니다: {part_name}"
            );
            let expected_len = entry.uncompressed_size as usize;
            ensure_zip_size_limit("entry 해제", expected_len, MAX_XLSX_PART_BYTES, entry.name)?;
            total_uncompressed = total_uncompressed
                .checked_add(expected_len)
                .ok_or("ZIP 전체 해제 크기 계산 실패")?;
            ensure_zip_size_limit(
                "전체 해제",
                total_uncompressed,
                ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES,
                entry.name,
            )?;
            *slot = Some((entry, part_index, expected_len));
        }
        ensure(
            central_directory.cursor == central_directory.end,
            ZIP_CENTRAL_DIRECTORY_SIZE_MISMATCH_MESSAGE,
        )?;
        for ((name, role, _), present) in XLSX_PARTS.into_iter().zip(seen) {
            ensure_fmt!(
                role != PartRole::Required || present,
                "ZIP 필수 entry가 없습니다: {name}"
            );
        }
        entries.sort_unstable_by_key(|slot| slot.as_ref().map(|item| item.0.local_header_offset));
        let (mut expected_local_offset, mut archive_crc) = (0_usize, u32::MAX);
        let mut parts =
            try_vec_with_capacity(entry_count, "ZIP package part 목록 메모리 확보 실패")?;
        let mut record_bytes = Vec::new();
        let mut entry_iter = entries.iter_mut().filter_map(Option::take).peekable();
        while let Some((entry, part_index, expected_len)) = entry_iter.next() {
            let local_offset = entry.local_header_offset as usize;
            let next_offset = entry_iter.peek().map_or(central_dir_offset, |item| {
                item.0.local_header_offset as usize
            });
            let record_len = next_offset
                .checked_sub(local_offset)
                .ok_or("ZIP local record 범위 순서 오류")?;
            read_archive_range(
                &mut self.archive_file,
                self.archive_path,
                (expected_local_offset == 0 || expected_local_offset != local_offset)
                    .then_some(local_offset),
                record_len,
                &mut record_bytes,
            )?;
            archive_crc = crc32_update(archive_crc, &record_bytes);
            let (bytes, local_end) = entry.data(
                &mut record_bytes,
                local_offset,
                expected_len,
                expected_local_offset,
            )?;
            expected_local_offset = local_end;
            parts.push(PackagePart {
                bytes,
                name: or_abort(XLSX_PARTS.get(part_index)).0,
            });
        }
        ensure(
            expected_local_offset == central_dir_offset,
            "ZIP local/central record 범위가 고정 package 표현과 다릅니다.",
        )?;
        let expected_end = tail_bytes
            .get(eocd_relative_offset..)
            .ok_or("ZIP EOCD 검증 범위 오류")?;
        for (len, expected) in [
            (central_dir_size, central_bytes.as_slice()),
            (archive_len.strict_sub(eocd_offset), expected_end),
        ] {
            read_archive_range(
                &mut self.archive_file,
                self.archive_path,
                None,
                len,
                &mut record_bytes,
            )?;
            if record_bytes != expected {
                return Err(archive_changed(self.archive_path));
            }
            archive_crc = crc32_update(archive_crc, &record_bytes);
        }
        if archive_file_len(&self.archive_file, self.archive_path)? != archive_len {
            return Err(archive_changed(self.archive_path));
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
fn archive_changed(archive_path: &Path) -> AppError {
    fmt_err!(
        "xlsx 압축 파일이 읽는 중 변경되었습니다: {}",
        archive_path.display()
    )
}
pub(super) fn archive_file_len(file: &File, archive_path: &Path) -> Result<usize> {
    let metadata = file.metadata().map_err(|source| {
        path_err_with_source("xlsx 압축 파일 정보 확인 실패", archive_path, source)
    })?;
    let archive_len = usize::try_from(metadata.len()).map_err(|source| {
        fmt_err!(
            "xlsx 압축 파일 크기 변환 실패({}): {source}",
            archive_path.display()
        )
    })?;
    ensure_fmt!(
        archive_len <= ZIP_MAX_ARCHIVE_BYTES,
        "xlsx 압축 파일 크기가 허용 한도({ZIP_MAX_ARCHIVE_BYTES} bytes)를 초과했습니다: {}",
        archive_path.display()
    );
    Ok(archive_len)
}
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
fn ensure_zip_size_limit(
    scope: &str,
    actual_len: usize,
    limit: usize,
    entry_name: &str,
) -> Result<()> {
    ensure_fmt!(
        actual_len <= limit,
        "ZIP {scope} 크기가 허용 한도({limit} bytes)를 초과했습니다: {entry_name}"
    );
    Ok(())
}
fn read_archive_range(
    file: &mut File,
    archive_path: &Path,
    offset: Option<usize>,
    len: usize,
    buffer: &mut Vec<u8>,
) -> Result<()> {
    if let Some(seek_offset) = offset {
        file.seek(SeekFrom::Start(seek_offset as u64))
            .map_err(|source| {
                path_err_with_source("xlsx 압축 파일 range 이동 실패", archive_path, source)
            })?;
    }
    buffer.clear();
    map_reserve(
        buffer.try_reserve_exact(len),
        "ZIP 입력 range 메모리 확보 실패",
    )?;
    file.take(len as u64)
        .read_to_end(buffer)
        .map_err(|source| {
            path_err_with_source("xlsx 압축 파일 range 읽기 실패", archive_path, source)
        })?;
    if buffer.len() != len {
        return Err(archive_changed(archive_path));
    }
    Ok(())
}
fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    Ok(u16::from_le_bytes(
        read_le_chunk::<2>(bytes, offset).ok_or("ZIP u16 읽기 범위 오류")?,
    ))
}
fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    Ok(u32::from_le_bytes(
        read_le_chunk::<4>(bytes, offset).ok_or("ZIP u32 읽기 범위 오류")?,
    ))
}
fn split_header_at<'bytes, const LEN: usize>(
    bytes: &'bytes [u8],
    offset: usize,
    context: &'static str,
) -> Result<(&'bytes [u8; LEN], &'bytes [u8])> {
    bytes
        .get(offset..)
        .and_then(|remaining| remaining.split_first_chunk::<LEN>())
        .ok_or_else(|| err(context))
}
fn validate_zip_extra(extra: &[u8], entry_name: &str) -> Result<()> {
    let mut cursor = 0_usize;
    while cursor < extra.len() {
        let header = extra
            .get(cursor..)
            .and_then(|bytes| bytes.split_first_chunk::<4>())
            .map(|value| value.0)
            .ok_or_else(|| {
                zip_entry_err("ZIP extra field header 범위가 손상되었습니다", entry_name)
            })?;
        let field_id = u16::from_le_bytes([header[0], header[1]]);
        let field_len = usize::from(u16::from_le_bytes([header[2], header[3]]));
        if matches!(field_id, 0x0001 | 0x9901) {
            return Err(zip_entry_err(
                "ZIP64 또는 AES extra field는 지원하지 않습니다",
                entry_name,
            ));
        }
        cursor = cursor
            .checked_add(4)
            .and_then(|start| start.checked_add(field_len))
            .filter(|&end| end <= extra.len())
            .ok_or_else(|| zip_entry_err("ZIP extra field 범위가 손상되었습니다", entry_name))?;
    }
    Ok(())
}
fn zip_entry_err(context: &str, entry_name: &str) -> AppError {
    fmt_err!("{context}: {entry_name}")
}
