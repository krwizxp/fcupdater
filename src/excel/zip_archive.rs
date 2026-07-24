use super::{ArchiveFingerprint, PackagePart, XlsxPackageKind, ZipPackageReader};
use crate::diagnostic::{
    AppError, Result, Result as ZipResult, err, err as zip_static, err_with_source,
    err_with_source as zip_with_source, path_context_message,
};
use core::{range::Range, str};
use std::{fs::File, io::Read as _, path::Path};
mod deflate;
mod write;
const CENTRAL_DIRECTORY_HEADER_LEN: usize = 46;
const CENTRAL_DIRECTORY_SIGNATURE: u32 = 0x0201_4b50;
const CODE_LENGTH_SYMBOLS: usize = 19;
const CRC32_TABLE: [u32; 256] = {
    let mut table = [0_u32; 256];
    let mut remaining: &mut [u32] = &mut table;
    let mut seed = 0_u32;
    while let [ref mut slot, ref mut tail @ ..] = *remaining {
        let mut value = seed;
        let mut bit = 0_u8;
        while bit < 8_u8 {
            value = value.wrapping_shr(1) ^ (0xedb8_8320_u32 & 0_u32.wrapping_sub(value & 1_u32));
            bit = bit.wrapping_add(1);
        }
        *slot = value;
        remaining = tail;
        seed = seed.wrapping_add(1);
    }
    table
};
const DEFLATE_MAX_BITS: usize = 15;
const DEFLATE_MAX_BITS_U8: u8 = 15;
const DATA_DESCRIPTOR_LEN: usize = 16;
const DATA_DESCRIPTOR_SIGNATURE: u32 = 0x0807_4b50;
const DISTANCE_SYMBOLS: usize = 30;
const END_OF_CENTRAL_DIRECTORY_LEN: usize = 22;
const END_OF_CENTRAL_DIRECTORY_SIGNATURE: u32 = 0x0605_4b50;
const FIXED_DISTANCE_SYMBOLS: usize = 32;
const FIXED_LITERAL_SYMBOLS: usize = 288;
const EXCEL_ENTRY_FLAGS: u16 = 0x0006;
const HASH_SIZE: usize = 0x8000;
const LITERAL_LENGTH_SYMBOLS: usize = 286;
const LOCAL_FILE_HEADER_LEN: usize = 30;
const MAX_CHAIN: usize = 8;
const MAX_MATCH: usize = 258;
const MIN_MATCH: usize = 3;
const LOCAL_FILE_HEADER_SIGNATURE: u32 = 0x0403_4b50;
const METHOD_DEFLATE: u16 = 8;
const VERSION_MADE_BY: u16 = 45;
const VERSION_NEEDED: u16 = 20;
const ZIP_COMMENT_MAX_LEN: usize = 0xffff;
const ZIP_BAD_CRC_MESSAGE: &str = "ZIP CRC가 일치하지 않습니다";
const ZIP_BAD_CENTRAL_SIGNATURE_MESSAGE: &str = "ZIP 중앙 디렉터리 signature가 올바르지 않습니다.";
const ZIP_BAD_LOCAL_HEADER_MESSAGE: &str = "ZIP local header signature가 올바르지 않습니다";
const ZIP_BAD_SIZE_MESSAGE: &str = "ZIP 해제 크기가 일치하지 않습니다";
const ZIP_CENTRAL_DIRECTORY_SIZE_MISMATCH_MESSAGE: &str =
    "ZIP 중앙 디렉터리 크기가 entry 목록과 일치하지 않습니다.";
const ZIP_CENTRAL_HEADER_RANGE: &str = "ZIP 중앙 디렉터리 header 범위 오류";
const ZIP_DATA_RANGE_MESSAGE: &str = "ZIP entry 데이터가 파일 범위를 벗어났습니다";
const ZIP_EOCD_HEADER_RANGE: &str = "ZIP EOCD header 범위 오류";
const ZIP_FINGERPRINT_BUFFER_BYTES: usize = 64 * 1024;
const ZIP_MAX_ARCHIVE_BYTES: usize = 128 * 1024 * 1024;
const ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES: usize = 64 * 1024 * 1024;
const ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES: usize = 256 * 1024 * 1024;
const LENGTH_BASES: [usize; 29] = [
    3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131,
    163, 195, 227, 258,
];
const LENGTH_EXTRA_BITS: [u8; 29] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0,
];
const DISTANCE_BASES: [usize; 30] = [
    1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537,
    2049, 3073, 0x1001, 0x1801, 0x2001, 0x3001, 0x4001, 0x6001,
];
const DISTANCE_EXTRA_BITS: [u8; 30] = [
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13,
    13,
];
const CODE_LENGTH_ORDER: [usize; 19] = [
    16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
];
const _: () = assert!(
    LENGTH_BASES.len() == LENGTH_EXTRA_BITS.len(),
    "deflate length tables must have matching lengths"
);
const _: () = assert!(
    DISTANCE_BASES.len() == DISTANCE_EXTRA_BITS.len(),
    "deflate distance tables must have matching lengths"
);
const _: () = assert!(
    CODE_LENGTH_ORDER.len() == CODE_LENGTH_SYMBOLS,
    "deflate code length order must cover all symbols"
);
const _: () = assert!(
    MAX_MATCH >= MIN_MATCH,
    "deflate match bounds must be ordered"
);
const _: () = assert!(
    ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES >= ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES,
    "ZIP total limit must cover at least one entry"
);
struct ZipEntry<'zip> {
    central_record: Range<usize>,
    compressed_size: u32,
    crc32: u32,
    flags: u16,
    local_header_offset: u32,
    method: u16,
    modified_date: u16,
    modified_time: u16,
    name: &'zip str,
    uncompressed_size: u32,
}
struct ZipCentralDirectory<'bytes> {
    bytes: &'bytes [u8],
    cursor: usize,
    end: usize,
    package_kind: XlsxPackageKind,
    remaining_entries: usize,
}
impl ZipEntry<'_> {
    fn data(
        &self,
        bytes: &[u8],
        expected_len: usize,
        package_kind: XlsxPackageKind,
    ) -> Result<(Vec<u8>, Range<usize>)> {
        let local_offset = usize::try_from(self.local_header_offset)
            .map_err(|source| err_with_source("ZIP local header offset 변환 실패", source))?;
        let (local_header, _) = split_header_at::<LOCAL_FILE_HEADER_LEN>(
            bytes,
            local_offset,
            ZIP_BAD_LOCAL_HEADER_MESSAGE,
        )
        .map_err(|mut source| {
            source.update_message(|message| zip_entry_message(message, self.name));
            source
        })?;
        let local_mismatch =
            |message: &'static str| -> AppError { zip_entry_message(message, self.name).into() };
        if read_u32(local_header, 0)? != LOCAL_FILE_HEADER_SIGNATURE {
            return Err(zip_entry_message(ZIP_BAD_LOCAL_HEADER_MESSAGE, self.name).into());
        }
        if read_u16(local_header, 4)? != VERSION_NEEDED {
            return Err(local_mismatch(
                "ZIP local header version이 지원 표현과 다릅니다",
            ));
        }
        let local_flags = read_u16(local_header, 6)?;
        if local_flags != self.flags || local_flags != package_kind.entry_flags() {
            return Err(local_mismatch(
                "ZIP local header flags가 중앙 디렉터리와 다릅니다",
            ));
        }
        if read_u16(local_header, 8)? != self.method {
            return Err(local_mismatch(
                "ZIP local header 압축 방식이 중앙 디렉터리와 다릅니다",
            ));
        }
        if read_u16(local_header, 10)? != self.modified_time
            || read_u16(local_header, 12)? != self.modified_date
        {
            return Err(local_mismatch(
                "ZIP local header 수정 시각이 중앙 디렉터리와 다릅니다",
            ));
        }
        match package_kind {
            XlsxPackageKind::Excel => {
                if read_u32(local_header, 14)? != self.crc32
                    || read_u32(local_header, 18)? != self.compressed_size
                    || read_u32(local_header, 22)? != self.uncompressed_size
                {
                    return Err(local_mismatch(
                        "ZIP local CRC 또는 크기가 중앙 디렉터리와 다릅니다",
                    ));
                }
            }
            XlsxPackageKind::LibreOffice => {
                if read_u32(local_header, 14)? != 0
                    || read_u32(local_header, 18)? != 0
                    || read_u32(local_header, 22)? != 0
                {
                    return Err(local_mismatch(
                        "LibreOffice ZIP data descriptor local 필드가 0이 아닙니다",
                    ));
                }
            }
        }
        let name_len = usize::from(read_u16(local_header, 26)?);
        let extra_len = usize::from(read_u16(local_header, 28)?);
        let name_start = local_offset
            .checked_add(LOCAL_FILE_HEADER_LEN)
            .ok_or_else(|| zip_static("ZIP local entry 이름 시작 계산 실패"))?;
        let extra_start = name_start
            .checked_add(name_len)
            .ok_or_else(|| zip_static("ZIP local extra offset 계산 실패"))?;
        let local_name = bytes
            .get(name_start..extra_start)
            .ok_or_else(|| zip_static("ZIP local header 이름 범위 오류"))?;
        if local_name != self.name.as_bytes() {
            return Err(local_mismatch(
                "ZIP local header 이름이 중앙 디렉터리와 다릅니다",
            ));
        }
        let data_start = extra_start
            .checked_add(extra_len)
            .ok_or_else(|| zip_static("ZIP data offset 계산 실패"))?;
        let local_extra = bytes
            .get(extra_start..data_start)
            .ok_or_else(|| zip_static("ZIP local extra 범위 오류"))?;
        let extra_is_valid = match package_kind {
            XlsxPackageKind::Excel => excel_local_extra(self.name).map_or_else(
                || local_extra.is_empty(),
                |(len, header)| {
                    local_extra.len() == len
                        && local_extra.get(..header.len()) == Some(header.as_slice())
                        && local_extra
                            .get(header.len()..)
                            .is_some_and(|padding| padding.iter().all(|byte| *byte == 0))
                },
            ),
            XlsxPackageKind::LibreOffice => local_extra.is_empty(),
        };
        if !extra_is_valid {
            return Err(err(zip_entry_message(
                "ZIP local extra가 고정 package 표현과 다릅니다",
                self.name,
            )));
        }
        let compressed_len = usize::try_from(self.compressed_size)
            .map_err(|source| err_with_source("ZIP 압축 크기 변환 실패", source))?;
        let data_end = data_start
            .checked_add(compressed_len)
            .ok_or_else(|| zip_static("ZIP data end 계산 실패"))?;
        let Some(compressed) = bytes.get(data_start..data_end) else {
            return Err(zip_entry_message(ZIP_DATA_RANGE_MESSAGE, self.name).into());
        };
        if self.method != METHOD_DEFLATE {
            return Err(err(format!(
                "지원하지 않는 ZIP 압축 방식({}): {}",
                self.method, self.name
            )));
        }
        let local_end = match package_kind {
            XlsxPackageKind::Excel => data_end,
            XlsxPackageKind::LibreOffice => {
                let (descriptor, _) = split_header_at::<DATA_DESCRIPTOR_LEN>(
                    bytes,
                    data_end,
                    "ZIP data descriptor 범위 오류",
                )?;
                if read_u32(descriptor, 0)? != DATA_DESCRIPTOR_SIGNATURE
                    || read_u32(descriptor, 4)? != self.crc32
                    || read_u32(descriptor, 8)? != self.compressed_size
                    || read_u32(descriptor, 12)? != self.uncompressed_size
                {
                    return Err(local_mismatch(
                        "ZIP data descriptor가 중앙 디렉터리와 다릅니다",
                    ));
                }
                data_end
                    .checked_add(DATA_DESCRIPTOR_LEN)
                    .ok_or_else(|| zip_static("ZIP local record 끝 계산 실패"))?
            }
        };
        let output = deflate::DeflateInflater {
            bytes: compressed,
            expected_len,
        }
        .inflate()?;
        if output.len() != expected_len {
            return Err(zip_entry_message(ZIP_BAD_SIZE_MESSAGE, self.name).into());
        }
        if crc32(&output)? != self.crc32 {
            return Err(zip_entry_message(ZIP_BAD_CRC_MESSAGE, self.name).into());
        }
        Ok((
            output,
            Range {
                start: local_offset,
                end: local_end,
            },
        ))
    }
}
impl ZipCentralDirectory<'_> {
    fn next_entry(&mut self) -> Result<Option<ZipEntry<'_>>> {
        if self.remaining_entries == 0 {
            if self.cursor != self.end {
                return Err(zip_static(ZIP_CENTRAL_DIRECTORY_SIZE_MISMATCH_MESSAGE));
            }
            return Ok(None);
        }
        let entry_start = self.cursor;
        let (header, tail) = split_header_at::<CENTRAL_DIRECTORY_HEADER_LEN>(
            self.bytes,
            self.cursor,
            ZIP_CENTRAL_HEADER_RANGE,
        )?;
        if read_u32(header, 0)? != CENTRAL_DIRECTORY_SIGNATURE {
            return Err(zip_static(ZIP_BAD_CENTRAL_SIGNATURE_MESSAGE));
        }
        if read_u16(header, 4)? != self.package_kind.version_made_by()
            || read_u16(header, 6)? != VERSION_NEEDED
        {
            return Err(zip_static("ZIP entry version이 지원 표현과 다릅니다."));
        }
        let flags = read_u16(header, 8)?;
        if flags != self.package_kind.entry_flags() {
            return Err(zip_static("ZIP entry flags가 지원 표현과 다릅니다."));
        }
        if read_u16(header, 10)? != METHOD_DEFLATE {
            return Err(zip_static("ZIP entry 압축 방식이 지원 표현과 다릅니다."));
        }
        if self.package_kind == XlsxPackageKind::Excel
            && (read_u16(header, 12)? != 0 || read_u16(header, 14)? != 0x0021)
        {
            return Err(zip_static(
                "ZIP entry 수정 시각이 Excel 고정 표현과 다릅니다.",
            ));
        }
        let name_len = usize::from(read_u16(header, 28)?);
        let extra_len = usize::from(read_u16(header, 30)?);
        let comment_len = usize::from(read_u16(header, 32)?);
        if extra_len != 0 || comment_len != 0 {
            return Err(zip_static(
                "ZIP 중앙 디렉터리 extra/comment는 지원하지 않습니다.",
            ));
        }
        if read_u16(header, 34)? != 0 || read_u16(header, 36)? != 0 || read_u32(header, 38)? != 0 {
            return Err(zip_static(
                "ZIP 중앙 디렉터리 disk/attribute 표현이 지원 형식과 다릅니다.",
            ));
        }
        let entry_len = CENTRAL_DIRECTORY_HEADER_LEN
            .checked_add(name_len)
            .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 entry 길이 계산 실패"))?;
        let next_cursor = self
            .cursor
            .checked_add(entry_len)
            .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 다음 entry 위치 계산 실패"))?;
        if next_cursor > self.end {
            return Err(zip_static(ZIP_CENTRAL_DIRECTORY_SIZE_MISMATCH_MESSAGE));
        }
        let payload = tail
            .get(..name_len)
            .ok_or_else(|| zip_static(ZIP_CENTRAL_HEADER_RANGE))?;
        let Some(name_bytes) = payload.get(..name_len) else {
            return Err(zip_static("ZIP entry 이름이 파일 범위를 벗어났습니다."));
        };
        let name = str::from_utf8(name_bytes)
            .map_err(|source| err_with_source("ZIP entry 이름이 UTF-8이 아닙니다", source))?;
        self.cursor = next_cursor;
        self.remaining_entries = self
            .remaining_entries
            .checked_sub(1)
            .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 entry 개수 계산 실패"))?;
        Ok(Some(ZipEntry {
            central_record: Range {
                start: entry_start,
                end: next_cursor,
            },
            compressed_size: read_u32(header, 20)?,
            crc32: read_u32(header, 16)?,
            flags,
            local_header_offset: read_u32(header, 42)?,
            method: read_u16(header, 10)?,
            modified_date: read_u16(header, 14)?,
            modified_time: read_u16(header, 12)?,
            name,
            uncompressed_size: read_u32(header, 24)?,
        }))
    }
}
impl ZipPackageReader<'_> {
    pub(super) fn read(
        &self,
    ) -> Result<(
        ArchiveFingerprint,
        Vec<u8>,
        XlsxPackageKind,
        Vec<PackagePart>,
    )> {
        let mut archive_bytes = Vec::new();
        let fingerprint = scan_open_archive(
            &self.archive_file,
            self.archive_path,
            Some(&mut archive_bytes),
        )?;
        if archive_bytes.len() < END_OF_CENTRAL_DIRECTORY_LEN {
            return Err(zip_static("ZIP 파일이 너무 짧습니다."));
        }
        let search_window = END_OF_CENTRAL_DIRECTORY_LEN
            .checked_add(ZIP_COMMENT_MAX_LEN)
            .ok_or_else(|| zip_static("ZIP EOCD 최대 검색 범위 계산 실패"))?;
        let min_offset = archive_bytes.len().saturating_sub(search_window);
        let max_offset = archive_bytes
            .len()
            .checked_sub(END_OF_CENTRAL_DIRECTORY_LEN)
            .ok_or_else(|| zip_static("ZIP EOCD 최대 offset 계산 실패"))?;
        let search_end = max_offset
            .checked_add(4_usize)
            .ok_or_else(|| zip_static("ZIP EOCD 검색 범위 계산 실패"))?;
        let search_bytes = archive_bytes
            .get(min_offset..search_end)
            .ok_or_else(|| zip_static("ZIP EOCD 검색 범위 오류"))?;
        let eocd_signature = END_OF_CENTRAL_DIRECTORY_SIGNATURE.to_le_bytes();
        let mut search_len = search_bytes.len();
        let (eocd_offset, eocd) = loop {
            let search_prefix = search_bytes
                .get(..search_len)
                .ok_or_else(|| zip_static("ZIP EOCD 검색 범위 오류"))?;
            let Some(relative_offset) = search_prefix
                .array_windows::<4>()
                .rposition(|window| *window == eocd_signature)
            else {
                return Err(zip_static("ZIP EOCD를 찾지 못했습니다."));
            };
            let offset = min_offset
                .checked_add(relative_offset)
                .ok_or_else(|| zip_static("ZIP EOCD offset 계산 실패"))?;
            let (eocd, _) = split_header_at::<END_OF_CENTRAL_DIRECTORY_LEN>(
                archive_bytes.as_slice(),
                offset,
                ZIP_EOCD_HEADER_RANGE,
            )?;
            let comment_len = usize::from(read_u16(eocd, 20)?);
            if offset
                .checked_add(END_OF_CENTRAL_DIRECTORY_LEN)
                .and_then(|value| value.checked_add(comment_len))
                == Some(archive_bytes.len())
            {
                break (offset, eocd);
            }
            search_len = relative_offset;
        };
        let disk_no = read_u16(eocd, 4)?;
        let central_dir_start_disk = read_u16(eocd, 6)?;
        let entries_this_disk = read_u16(eocd, 8)?;
        let entries_total = read_u16(eocd, 10)?;
        if disk_no != 0 || central_dir_start_disk != 0 || entries_this_disk != entries_total {
            return Err(zip_static("분할 ZIP archive는 지원하지 않습니다."));
        }
        let entry_count = usize::from(entries_total);
        let package_kind = match entry_count {
            13 => XlsxPackageKind::Excel,
            14 => XlsxPackageKind::LibreOffice,
            _ => {
                return Err(err(format!(
                    "ZIP entry 수가 지원하는 Excel/LibreOffice 고정 스키마와 다릅니다: {entry_count} (13 또는 14 필요)"
                )));
            }
        };
        let part_names = package_kind.part_names();
        if read_u16(eocd, 20)? != 0 {
            return Err(zip_static("ZIP archive comment는 지원하지 않습니다."));
        }
        let central_dir_size = usize::try_from(read_u32(eocd, 12)?)
            .map_err(|source| err_with_source("ZIP 중앙 디렉터리 크기 변환 실패", source))?;
        let central_dir_offset = usize::try_from(read_u32(eocd, 16)?)
            .map_err(|source| err_with_source("ZIP 중앙 디렉터리 offset 변환 실패", source))?;
        let central_dir_end = central_dir_offset
            .checked_add(central_dir_size)
            .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 범위 계산 실패"))?;
        if central_dir_end != eocd_offset {
            return Err(zip_static(
                "ZIP 중앙 디렉터리와 EOCD 사이의 추가 데이터는 지원하지 않습니다.",
            ));
        }
        let mut central_directory = ZipCentralDirectory {
            bytes: archive_bytes.as_slice(),
            cursor: central_dir_offset,
            end: central_dir_end,
            package_kind,
            remaining_entries: entry_count,
        };
        let mut total_uncompressed = 0_usize;
        let mut expected_local_offset = 0_usize;
        let mut parts = Vec::new();
        parts
            .try_reserve_exact(part_names.len())
            .map_err(|source| err_with_source("ZIP package part 목록 메모리 확보 실패", source))?;
        for &expected_name in part_names {
            let entry = central_directory
                .next_entry()?
                .ok_or_else(|| zip_static("ZIP entry가 고정 스키마보다 적습니다."))?;
            if entry.name != expected_name {
                return Err(err(format!(
                    "ZIP entry 순서 또는 이름이 고정 스키마와 다릅니다: {} != {expected_name}",
                    entry.name
                )));
            }
            let expected_len = usize::try_from(entry.uncompressed_size)
                .map_err(|source| err_with_source("ZIP 해제 크기 변환 실패", source))?;
            ensure_zip_size_limit(
                "entry 해제",
                expected_len,
                ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES,
                entry.name,
            )?;
            total_uncompressed = total_uncompressed
                .checked_add(expected_len)
                .ok_or_else(|| zip_static("ZIP 전체 해제 크기 계산 실패"))?;
            ensure_zip_size_limit(
                "전체 해제",
                total_uncompressed,
                ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES,
                entry.name,
            )?;
            let (mut bytes, local_record) =
                entry.data(archive_bytes.as_slice(), expected_len, package_kind)?;
            if local_record.start != expected_local_offset {
                return Err(err(format!(
                    "ZIP local record가 연속된 고정 순서가 아닙니다: {expected_name}"
                )));
            }
            expected_local_offset = local_record.end;
            if package_kind == XlsxPackageKind::Excel
                && !matches!(
                    expected_name,
                    "[Content_Types].xml"
                        | "_rels/.rels"
                        | "xl/_rels/workbook.xml.rels"
                        | "xl/workbook.xml"
                        | "xl/sharedStrings.xml"
                        | "xl/calcChain.xml"
                        | "xl/worksheets/sheet1.xml"
                        | "xl/worksheets/sheet2.xml"
                )
            {
                bytes = Vec::new();
            }
            parts.push(PackagePart {
                bytes,
                central_record: entry.central_record,
                changed: false,
                local_record,
                name: expected_name,
            });
        }
        if central_directory.next_entry()?.is_some() || expected_local_offset != central_dir_offset
        {
            return Err(zip_static(
                "ZIP local/central record 범위가 고정 package 표현과 다릅니다.",
            ));
        }
        Ok((fingerprint, archive_bytes, package_kind, parts))
    }
}
pub(super) fn excel_local_extra(name: &str) -> Option<(usize, [u8; 8])> {
    match name {
        "[Content_Types].xml" | "_rels/.rels" => {
            Some((520, [0x20, 0xa2, 0x04, 0x02, 0x28, 0xa0, 0x00, 0x02]))
        }
        "xl/_rels/workbook.xml.rels" | "docProps/core.xml" | "docProps/app.xml" => {
            Some((264, [0x20, 0xa2, 0x04, 0x01, 0x28, 0xa0, 0x00, 0x01]))
        }
        _ => None,
    }
}
pub(super) fn scan_open_archive(
    file: &File,
    archive_path: &Path,
    mut retained: Option<&mut Vec<u8>>,
) -> Result<ArchiveFingerprint> {
    let metadata = file.metadata().map_err(|source_err| {
        err_with_source(
            path_context_message("xlsx 압축 파일 정보 확인 실패", archive_path),
            source_err,
        )
    })?;
    let archive_len = usize::try_from(metadata.len()).map_err(|source| {
        err(format!(
            "xlsx 압축 파일 크기 변환 실패({}): {source}",
            archive_path.display()
        ))
    })?;
    if archive_len > ZIP_MAX_ARCHIVE_BYTES {
        return Err(err(format!(
            "xlsx 압축 파일 크기가 허용 한도({ZIP_MAX_ARCHIVE_BYTES} bytes)를 초과했습니다: {}",
            archive_path.display()
        )));
    }
    if let Some(bytes) = retained.as_mut() {
        bytes
            .try_reserve_exact(archive_len)
            .map_err(|source| err_with_source("xlsx 압축 파일 메모리 확보 실패", source))?;
    }
    let read_limit = metadata.len().saturating_add(1);
    let mut limited = file.take(read_limit);
    let mut buffer = vec![0_u8; ZIP_FINGERPRINT_BUFFER_BYTES].into_boxed_slice();
    let mut crc = u32::MAX;
    let mut bytes_read = 0_usize;
    loop {
        let read_len = limited.read(buffer.as_mut()).map_err(|source_err| {
            err_with_source(
                path_context_message("xlsx 압축 파일 읽기 실패", archive_path),
                source_err,
            )
        })?;
        if read_len == 0 {
            break;
        }
        bytes_read = bytes_read
            .checked_add(read_len)
            .ok_or_else(|| err("xlsx 압축 파일 읽기 크기 계산 실패"))?;
        let (chunk, _) = buffer.split_at(read_len);
        crc = crc32_update(crc, chunk)?;
        if let Some(bytes) = retained.as_mut() {
            if bytes.capacity().saturating_sub(bytes.len()) < read_len {
                bytes.try_reserve(read_len).map_err(|source| {
                    err_with_source("xlsx 압축 파일 메모리 추가 확보 실패", source)
                })?;
            }
            bytes.extend_from_slice(chunk);
        }
    }
    if bytes_read > ZIP_MAX_ARCHIVE_BYTES {
        return Err(err(format!(
            "xlsx 압축 파일 크기가 허용 한도({ZIP_MAX_ARCHIVE_BYTES} bytes)를 초과했습니다: {}",
            archive_path.display()
        )));
    }
    if bytes_read != archive_len {
        return Err(err(format!(
            "xlsx 압축 파일이 읽는 중 변경되었습니다: {}",
            archive_path.display()
        )));
    }
    Ok(ArchiveFingerprint {
        crc32: !crc,
        len: bytes_read,
    })
}
fn ensure_zip_size_limit(
    scope: &str,
    actual_len: usize,
    limit: usize,
    entry_name: &str,
) -> Result<()> {
    if actual_len > limit {
        Err(err(format!(
            "ZIP {scope} 크기가 허용 한도({limit} bytes)를 초과했습니다: {entry_name}"
        )))
    } else {
        Ok(())
    }
}
fn zip_entry_message(context: &str, entry_name: &str) -> String {
    format!("{context}: {entry_name}")
}
fn crc32(bytes: &[u8]) -> ZipResult<u32> {
    crc32_update(u32::MAX, bytes).map(|crc| !crc)
}
fn crc32_update(initial: u32, bytes: &[u8]) -> ZipResult<u32> {
    bytes.iter().try_fold(initial, |crc, &byte| {
        let table_index = usize::from((crc ^ u32::from(byte)).to_le_bytes()[0]);
        let Some(table_value) = CRC32_TABLE.get(table_index).copied() else {
            return Err(zip_static("ZIP CRC32 table 범위가 손상되었습니다."));
        };
        Ok((crc >> 8_u8) ^ table_value)
    })
}
fn split_header_at<'bytes, const LEN: usize>(
    bytes: &'bytes [u8],
    offset: usize,
    context: &'static str,
) -> ZipResult<(&'bytes [u8; LEN], &'bytes [u8])> {
    let Some((header, tail)) = bytes
        .get(offset..)
        .and_then(|remaining| remaining.split_first_chunk::<LEN>())
    else {
        return Err(zip_static(context));
    };
    Ok((header, tail))
}
fn read_u16(bytes: &[u8], offset: usize) -> ZipResult<u16> {
    Ok(u16::from_le_bytes(read_array::<2>(
        bytes,
        offset,
        "ZIP u16 읽기 범위 오류",
    )?))
}
fn read_u32(bytes: &[u8], offset: usize) -> ZipResult<u32> {
    Ok(u32::from_le_bytes(read_array::<4>(
        bytes,
        offset,
        "ZIP u32 읽기 범위 오류",
    )?))
}
fn read_array<const N: usize>(
    bytes: &[u8],
    offset: usize,
    error_message: &'static str,
) -> ZipResult<[u8; N]> {
    let Some(raw_bytes) = bytes.get(offset..).and_then(|tail| tail.first_chunk::<N>()) else {
        return Err(zip_static(error_message));
    };
    Ok(*raw_bytes)
}
