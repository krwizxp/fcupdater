use super::{
    ArchiveFingerprint, MAX_XLSX_PART_BYTES, PackagePart, PartRole, XLSX_PARTS, ZipPackageReader,
};
use crate::{
    diagnostic::{
        AppError, Result, Result as ZipResult, err, err as zip_static, err_with_source,
        err_with_source as zip_with_source, path_context_message, try_vec_with_capacity,
    },
    u32_to_usize,
};
use core::mem;
use std::{fs::File, io::Read as _, path::Path, process};
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
            bit = bit.strict_add(1);
        }
        *slot = value;
        remaining = tail;
        seed = seed.strict_add(1);
    }
    table
};
const DEFLATE_MAX_BITS: usize = 15;
const DEFLATE_MAX_BITS_U8: u8 = 15;
const DATA_DESCRIPTOR_LEN: usize = 16;
const DATA_DESCRIPTOR_LEN_WITHOUT_SIGNATURE: usize = 12;
const DATA_DESCRIPTOR_SIGNATURE: u32 = 0x0807_4b50;
const DISTANCE_SYMBOLS: usize = 30;
const END_OF_CENTRAL_DIRECTORY_LEN: usize = 22;
const END_OF_CENTRAL_DIRECTORY_SIGNATURE: u32 = 0x0605_4b50;
const FIXED_DISTANCE_SYMBOLS: usize = 32;
const FIXED_LITERAL_SYMBOLS: usize = 288;
const EXCEL_ENTRY_FLAGS: u16 = 0x0006;
const HASH_SIZE: usize = 0x4000;
const LITERAL_LENGTH_SYMBOLS: usize = 286;
const LOCAL_FILE_HEADER_LEN: usize = 30;
const MAX_CHAIN: usize = 16;
const MAX_MATCH: usize = 258;
const MIN_MATCH: usize = 3;
const LOCAL_FILE_HEADER_SIGNATURE: u32 = 0x0403_4b50;
const METHOD_DEFLATE: u16 = 8;
const METHOD_STORED: u16 = 0;
const SUPPORTED_FLAGS: u16 = 0x080e;
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
        bytes: &[u8],
        expected_len: usize,
        expected_local_offset: usize,
    ) -> Result<(Vec<u8>, usize)> {
        let local_offset = u32_to_usize(self.local_header_offset);
        if local_offset != expected_local_offset {
            return Err(err(format!(
                "ZIP local record가 연속된 고정 순서가 아닙니다: {}",
                self.name
            )));
        }
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
        if read_u16(local_header, 4)? != self.version_needed {
            return Err(local_mismatch(
                "ZIP local header version이 중앙 디렉터리와 다릅니다",
            ));
        }
        let local_flags = read_u16(local_header, 6)?;
        if local_flags != self.flags {
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
        let local_crc = read_u32(local_header, 14)?;
        let local_compressed_size = read_u32(local_header, 18)?;
        let local_uncompressed_size = read_u32(local_header, 22)?;
        if self.flags & 0x0008 == 0
            && (local_crc != self.crc32
                || local_compressed_size != self.compressed_size
                || local_uncompressed_size != self.uncompressed_size)
        {
            return Err(local_mismatch(
                "ZIP local CRC 또는 크기가 중앙 디렉터리와 다릅니다",
            ));
        }
        if self.flags & 0x0008 != 0
            && ((local_crc != 0 && local_crc != self.crc32)
                || (local_compressed_size != 0 && local_compressed_size != self.compressed_size)
                || (local_uncompressed_size != 0
                    && local_uncompressed_size != self.uncompressed_size))
        {
            return Err(local_mismatch(
                "ZIP data descriptor local CRC 또는 크기가 올바르지 않습니다",
            ));
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
        validate_zip_extra(local_extra, self.name)?;
        let compressed_len = u32_to_usize(self.compressed_size);
        let data_end = data_start
            .checked_add(compressed_len)
            .ok_or_else(|| zip_static("ZIP data end 계산 실패"))?;
        let Some(compressed) = bytes.get(data_start..data_end) else {
            return Err(zip_entry_message(ZIP_DATA_RANGE_MESSAGE, self.name).into());
        };
        let local_end = if self.flags & 0x0008 == 0 {
            data_end
        } else {
            let descriptor_tail = bytes
                .get(data_end..)
                .ok_or_else(|| zip_static("ZIP data descriptor 범위 오류"))?;
            let first = read_u32(descriptor_tail, 0)?;
            let (crc_offset, descriptor_len) = if first == DATA_DESCRIPTOR_SIGNATURE {
                (4, DATA_DESCRIPTOR_LEN)
            } else {
                (0, DATA_DESCRIPTOR_LEN_WITHOUT_SIGNATURE)
            };
            let descriptor = descriptor_tail
                .get(..descriptor_len)
                .ok_or_else(|| zip_static("ZIP data descriptor 범위 오류"))?;
            if read_u32(descriptor, crc_offset)? != self.crc32
                || read_u32(descriptor, crc_offset.strict_add(4))? != self.compressed_size
                || read_u32(descriptor, crc_offset.strict_add(8))? != self.uncompressed_size
            {
                return Err(err(zip_entry_message(
                    "ZIP data descriptor가 중앙 디렉터리와 다릅니다",
                    self.name,
                )));
            }
            data_end
                .checked_add(descriptor_len)
                .ok_or_else(|| zip_static("ZIP local record 끝 계산 실패"))?
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
            let mut output =
                try_vec_with_capacity(expected_len, "ZIP stored entry 메모리 확보 실패")?;
            output.extend_from_slice(compressed);
            let crc = !crc32_update(u32::MAX, &output);
            (output, crc)
        };
        if output.len() != expected_len {
            return Err(zip_entry_message(ZIP_BAD_SIZE_MESSAGE, self.name).into());
        }
        if output_crc32 != self.crc32 {
            return Err(zip_entry_message(ZIP_BAD_CRC_MESSAGE, self.name).into());
        }
        Ok((output, local_end))
    }
}
impl<'bytes> ZipCentralDirectory<'bytes> {
    fn next_entry(&mut self) -> Result<ZipEntry<'bytes>> {
        let (header, tail) = split_header_at::<CENTRAL_DIRECTORY_HEADER_LEN>(
            self.bytes,
            self.cursor,
            ZIP_CENTRAL_HEADER_RANGE,
        )?;
        if read_u32(header, 0)? != CENTRAL_DIRECTORY_SIGNATURE {
            return Err(zip_static(ZIP_BAD_CENTRAL_SIGNATURE_MESSAGE));
        }
        let version_needed = read_u16(header, 6)?;
        let flags = read_u16(header, 8)?;
        let method = read_u16(header, 10)?;
        if method != METHOD_DEFLATE && method != METHOD_STORED {
            return Err(zip_static("ZIP entry 압축 방식을 지원하지 않습니다."));
        }
        let minimum_version = if method == METHOD_DEFLATE { 20 } else { 10 };
        if version_needed < minimum_version || version_needed > VERSION_NEEDED {
            return Err(zip_static("ZIP entry version이 지원 범위를 벗어났습니다."));
        }
        if flags & !SUPPORTED_FLAGS != 0 {
            return Err(zip_static(
                "ZIP entry flags에 지원하지 않는 기능이 있습니다.",
            ));
        }
        if method == METHOD_STORED && flags & 0x0006 != 0 {
            return Err(zip_static(
                "ZIP stored entry에 DEFLATE 압축 옵션 flag가 있습니다.",
            ));
        }
        let name_len = usize::from(read_u16(header, 28)?);
        let extra_len = usize::from(read_u16(header, 30)?);
        let comment_len = usize::from(read_u16(header, 32)?);
        if read_u16(header, 34)? != 0 {
            return Err(zip_static("분할 ZIP entry는 지원하지 않습니다."));
        }
        let entry_len = CENTRAL_DIRECTORY_HEADER_LEN
            .checked_add(name_len)
            .and_then(|len| len.checked_add(extra_len))
            .and_then(|len| len.checked_add(comment_len))
            .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 entry 길이 계산 실패"))?;
        let next_cursor = self
            .cursor
            .checked_add(entry_len)
            .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 다음 entry 위치 계산 실패"))?;
        if next_cursor > self.end {
            return Err(zip_static(ZIP_CENTRAL_DIRECTORY_SIZE_MISMATCH_MESSAGE));
        }
        let Some(name_bytes) = tail.get(..name_len) else {
            return Err(zip_static("ZIP entry 이름이 파일 범위를 벗어났습니다."));
        };
        let name = str::from_utf8(name_bytes)
            .map_err(|source| err_with_source("ZIP entry 이름이 UTF-8이 아닙니다", source))?;
        let extra_start = name_len;
        let extra_end = extra_start
            .checked_add(extra_len)
            .ok_or_else(|| zip_static("ZIP 중앙 extra 범위 계산 실패"))?;
        let central_extra = tail
            .get(extra_start..extra_end)
            .ok_or_else(|| zip_static("ZIP 중앙 extra 범위 오류"))?;
        validate_zip_extra(central_extra, name)?;
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
    pub(super) fn read(self) -> Result<(ArchiveFingerprint, Vec<PackagePart>)> {
        let mut archive_bytes = Vec::new();
        let fingerprint = scan_open_archive(
            &self.archive_file,
            self.archive_path,
            Some(&mut archive_bytes),
        )?;
        if archive_bytes.len() < END_OF_CENTRAL_DIRECTORY_LEN {
            return Err(zip_static("ZIP 파일이 너무 짧습니다."));
        }
        let search_window = END_OF_CENTRAL_DIRECTORY_LEN.strict_add(ZIP_COMMENT_MAX_LEN);
        let min_offset = archive_bytes.len().saturating_sub(search_window);
        let max_offset = archive_bytes.len().strict_sub(END_OF_CENTRAL_DIRECTORY_LEN);
        let search_end = max_offset.strict_add(4_usize);
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
            let offset = min_offset.strict_add(relative_offset);
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
        if entry_count > XLSX_PARTS.len() {
            return Err(err(format!(
                "ZIP entry 수가 지원 상한을 초과했습니다: {entry_count}"
            )));
        }
        let central_dir_size = u32_to_usize(read_u32(eocd, 12)?);
        let central_dir_offset = u32_to_usize(read_u32(eocd, 16)?);
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
        };
        let mut total_uncompressed = 0_usize;
        let mut seen = [false; XLSX_PARTS.len()];
        let mut entries = try_vec_with_capacity(entry_count, "ZIP entry 목록 메모리 확보 실패")?;
        for _ in 0..entry_count {
            let entry = central_directory.next_entry()?;
            let Some((part_index, (part_name, _, _))) = XLSX_PARTS
                .iter()
                .copied()
                .enumerate()
                .find(|&(_, (name, _, _))| name == entry.name)
            else {
                return Err(err(format!(
                    "ZIP entry 이름이 고정 스키마에 없습니다: {}",
                    entry.name
                )));
            };
            let present = seen.get_mut(part_index).unwrap_or_else(|| process::abort());
            if mem::replace(present, true) {
                return Err(err(format!("ZIP entry 이름이 중복되었습니다: {part_name}")));
            }
            let expected_len = u32_to_usize(entry.uncompressed_size);
            ensure_zip_size_limit("entry 해제", expected_len, MAX_XLSX_PART_BYTES, entry.name)?;
            total_uncompressed = total_uncompressed
                .checked_add(expected_len)
                .ok_or_else(|| zip_static("ZIP 전체 해제 크기 계산 실패"))?;
            ensure_zip_size_limit(
                "전체 해제",
                total_uncompressed,
                ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES,
                entry.name,
            )?;
            entries.push((entry, part_index, expected_len));
        }
        if central_directory.cursor != central_directory.end {
            return Err(zip_static(ZIP_CENTRAL_DIRECTORY_SIZE_MISMATCH_MESSAGE));
        }
        for ((name, role, _), present) in XLSX_PARTS.into_iter().zip(seen) {
            if role == PartRole::Required && !present {
                return Err(err(format!("ZIP 필수 entry가 없습니다: {name}")));
            }
        }
        entries.sort_unstable_by_key(|item| item.0.local_header_offset);
        let mut expected_local_offset = 0_usize;
        let mut parts =
            try_vec_with_capacity(entry_count, "ZIP package part 목록 메모리 확보 실패")?;
        for (entry, part_index, expected_len) in entries {
            let (bytes, local_end) = entry.data(
                archive_bytes.as_slice(),
                expected_len,
                expected_local_offset,
            )?;
            expected_local_offset = local_end;
            let &(name, _, _) = XLSX_PARTS
                .get(part_index)
                .unwrap_or_else(|| process::abort());
            parts.push(PackagePart { bytes, name });
        }
        if expected_local_offset != central_dir_offset {
            return Err(zip_static(
                "ZIP local/central record 범위가 고정 package 표현과 다릅니다.",
            ));
        }
        Ok((fingerprint, parts))
    }
}
fn validate_zip_extra(extra: &[u8], entry_name: &str) -> Result<()> {
    let mut cursor = 0_usize;
    while cursor < extra.len() {
        let header = extra
            .get(cursor..)
            .and_then(|bytes| bytes.split_first_chunk::<4>())
            .map(|value| value.0)
            .ok_or_else(|| {
                err(zip_entry_message(
                    "ZIP extra field header 범위가 손상되었습니다",
                    entry_name,
                ))
            })?;
        let field_id = u16::from_le_bytes([header[0], header[1]]);
        let field_len = usize::from(u16::from_le_bytes([header[2], header[3]]));
        if matches!(field_id, 0x0001 | 0x9901) {
            return Err(err(zip_entry_message(
                "ZIP64 또는 AES extra field는 지원하지 않습니다",
                entry_name,
            )));
        }
        cursor = cursor
            .checked_add(4)
            .and_then(|start| start.checked_add(field_len))
            .filter(|end| *end <= extra.len())
            .ok_or_else(|| {
                err(zip_entry_message(
                    "ZIP extra field 범위가 손상되었습니다",
                    entry_name,
                ))
            })?;
    }
    Ok(())
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
        bytes.clear();
        bytes
            .try_reserve_exact(archive_len.strict_add(1))
            .map_err(|source| err_with_source("xlsx 압축 파일 메모리 확보 실패", source))?;
    }
    let mut limited = file.take(metadata.len().strict_add(1));
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
        bytes_read = bytes_read.strict_add(read_len);
        let (chunk, _) = buffer.split_at(read_len);
        crc = crc32_update(crc, chunk);
        if let Some(bytes) = retained.as_mut() {
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
    (actual_len <= limit).ok_or_else(|| {
        err(format!(
            "ZIP {scope} 크기가 허용 한도({limit} bytes)를 초과했습니다: {entry_name}"
        ))
    })
}
fn zip_entry_message(context: &str, entry_name: &str) -> String {
    format!("{context}: {entry_name}")
}
fn crc32_update(initial: u32, bytes: &[u8]) -> u32 {
    bytes
        .iter()
        .fold(initial, |crc, &byte| crc32_update_byte(crc, byte))
}
fn crc32_update_byte(crc: u32, byte: u8) -> u32 {
    let table_index = usize::from((crc ^ u32::from(byte)).to_le_bytes()[0]);
    let table_value = *CRC32_TABLE
        .get(table_index)
        .unwrap_or_else(|| process::abort());
    (crc >> 8_u8) ^ table_value
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
