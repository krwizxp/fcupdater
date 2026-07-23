use super::path_util::reject_windows_special_component;
use super::{ArchiveFingerprint, ZipArchiveExtractor, path_util::path_to_slashes};
use crate::diagnostic::{
    AppError, Result, Result as ZipResult, err, err as zip_static, err_with_source,
    err_with_source as zip_with_source, path_context_message, path_pair_context_message,
};
use alloc::borrow::Cow;
use core::{range::Range, str};
use std::{
    collections::HashSet,
    fs::{self, File},
    io::{Read as _, Write as IoWrite},
    path::{Path, PathBuf},
};
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
const DISTANCE_SYMBOLS: usize = 30;
const DOS_DATE_1980_01_01: u16 = 33;
const DATA_DESCRIPTOR_FLAG: u16 = 0x0008;
const END_OF_CENTRAL_DIRECTORY_LEN: usize = 22;
const END_OF_CENTRAL_DIRECTORY_SIGNATURE: u32 = 0x0605_4b50;
const ENCRYPTED_FLAG: u16 = 0x0001;
const FIXED_DISTANCE_SYMBOLS: usize = 32;
const FIXED_LITERAL_SYMBOLS: usize = 288;
const GENERAL_PURPOSE_UTF8_FLAG: u16 = 0x0800;
const HASH_SIZE: usize = 0x8000;
const LITERAL_LENGTH_SYMBOLS: usize = 286;
const LOCAL_FILE_HEADER_LEN: usize = 30;
const MAX_CHAIN: usize = 4096;
const MAX_MATCH: usize = 258;
const MIN_MATCH: usize = 3;
const LOCAL_FILE_HEADER_SIGNATURE: u32 = 0x0403_4b50;
const METHOD_DEFLATE: u16 = 8;
const METHOD_STORE: u16 = 0;
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
const ZIP_MAX_ENTRY_COUNT: usize = 4096;
const ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES: usize = 256 * 1024 * 1024;
const ZIP_UNSAFE_PATH_MESSAGE: &str = "허용되지 않은 압축 경로가 포함되어 있습니다";
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
    compressed_size: u32,
    crc32: u32,
    flags: u16,
    local_header_offset: u32,
    method: u16,
    name: &'zip str,
    uncompressed_size: u32,
}
struct ZipCentralDirectory<'bytes> {
    bytes: &'bytes [u8],
    cursor: usize,
    end: usize,
    remaining_entries: usize,
}
struct PendingFile {
    name: String,
    path: PathBuf,
}
impl ZipEntry<'_> {
    fn data<'bytes>(&self, bytes: &'bytes [u8], expected_len: usize) -> Result<Cow<'bytes, [u8]>> {
        let local_offset = usize::try_from(self.local_header_offset)
            .map_err(|source| err_with_source("ZIP local header offset 변환 실패", source))?;
        let (local_header, local_tail) = split_header_at::<LOCAL_FILE_HEADER_LEN>(
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
        let uses_data_descriptor = local_flags & DATA_DESCRIPTOR_FLAG != 0;
        let local_value_matches =
            |local: u32, central: u32| local == central || (uses_data_descriptor && local == 0);
        if !local_value_matches(read_u32(local_header, 14)?, self.crc32) {
            return Err(local_mismatch(
                "ZIP local header CRC가 중앙 디렉터리와 다릅니다",
            ));
        }
        if !local_value_matches(read_u32(local_header, 18)?, self.compressed_size) {
            return Err(local_mismatch(
                "ZIP local header 압축 크기가 중앙 디렉터리와 다릅니다",
            ));
        }
        if !local_value_matches(read_u32(local_header, 22)?, self.uncompressed_size) {
            return Err(local_mismatch(
                "ZIP local header 해제 크기가 중앙 디렉터리와 다릅니다",
            ));
        }
        let name_len = usize::from(read_u16(local_header, 26)?);
        let extra_len = usize::from(read_u16(local_header, 28)?);
        let local_name = local_tail
            .get(..name_len)
            .ok_or_else(|| zip_static("ZIP local header 이름 범위 오류"))?;
        if local_name != self.name.as_bytes() {
            return Err(local_mismatch(
                "ZIP local header 이름이 중앙 디렉터리와 다릅니다",
            ));
        }
        let data_start = name_len
            .checked_add(extra_len)
            .ok_or_else(|| zip_static("ZIP data offset 계산 실패"))?;
        let compressed_len = usize::try_from(self.compressed_size)
            .map_err(|source| err_with_source("ZIP 압축 크기 변환 실패", source))?;
        let compressed_span = Range {
            start: data_start,
            end: data_start
                .checked_add(compressed_len)
                .ok_or_else(|| zip_static("ZIP data end 계산 실패"))?,
        };
        let Some(compressed) = local_tail.get(compressed_span) else {
            return Err(zip_entry_message(ZIP_DATA_RANGE_MESSAGE, self.name).into());
        };
        let output = match self.method {
            METHOD_STORE => Cow::Borrowed(compressed),
            METHOD_DEFLATE => Cow::Owned(
                deflate::DeflateInflater {
                    bytes: compressed,
                    expected_len,
                }
                .inflate()?,
            ),
            method => {
                return Err(err(format!(
                    "지원하지 않는 ZIP 압축 방식({method}): {}",
                    self.name
                )));
            }
        };
        if output.len() != expected_len {
            return Err(zip_entry_message(ZIP_BAD_SIZE_MESSAGE, self.name).into());
        }
        if crc32(output.as_ref())? != self.crc32 {
            return Err(zip_entry_message(ZIP_BAD_CRC_MESSAGE, self.name).into());
        }
        Ok(output)
    }
    fn relative_path(&self) -> Result<PathBuf> {
        if self.name.is_empty() || self.name.starts_with(['/', '\\']) {
            return Err(zip_entry_message(ZIP_UNSAFE_PATH_MESSAGE, self.name).into());
        }
        let entry_name_bytes = self.name.as_bytes();
        if let Some(&[first, colon]) = entry_name_bytes.first_chunk::<2>()
            && colon == b':'
            && first.is_ascii_alphabetic()
        {
            return Err(zip_entry_message(ZIP_UNSAFE_PATH_MESSAGE, self.name).into());
        }
        let mut relative_path = PathBuf::new();
        for part in self
            .name
            .split(['/', '\\'])
            .filter(|part| !part.is_empty() && *part != ".")
        {
            if part == ".." {
                return Err(zip_entry_message(ZIP_UNSAFE_PATH_MESSAGE, self.name).into());
            }
            reject_windows_special_component(part, &self.name)?;
            relative_path.push(part);
        }
        if relative_path.as_os_str().is_empty() {
            return Err(zip_entry_message(ZIP_UNSAFE_PATH_MESSAGE, self.name).into());
        }
        Ok(relative_path)
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
        let (header, tail) = split_header_at::<CENTRAL_DIRECTORY_HEADER_LEN>(
            self.bytes,
            self.cursor,
            ZIP_CENTRAL_HEADER_RANGE,
        )?;
        if read_u32(header, 0)? != CENTRAL_DIRECTORY_SIGNATURE {
            return Err(zip_static(ZIP_BAD_CENTRAL_SIGNATURE_MESSAGE));
        }
        let flags = read_u16(header, 8)?;
        if flags & ENCRYPTED_FLAG != 0 {
            return Err(zip_static("암호화된 ZIP entry는 지원하지 않습니다."));
        }
        let name_len = usize::from(read_u16(header, 28)?);
        let extra_len = usize::from(read_u16(header, 30)?);
        let comment_len = usize::from(read_u16(header, 32)?);
        let entry_len = CENTRAL_DIRECTORY_HEADER_LEN
            .checked_add(name_len)
            .and_then(|value| value.checked_add(extra_len))
            .and_then(|value| value.checked_add(comment_len))
            .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 entry 길이 계산 실패"))?;
        let next_cursor = self
            .cursor
            .checked_add(entry_len)
            .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 다음 entry 위치 계산 실패"))?;
        if next_cursor > self.end {
            return Err(zip_static(ZIP_CENTRAL_DIRECTORY_SIZE_MISMATCH_MESSAGE));
        }
        let payload_len = entry_len
            .checked_sub(CENTRAL_DIRECTORY_HEADER_LEN)
            .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 entry payload 길이 계산 실패"))?;
        let payload = tail
            .get(..payload_len)
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
            compressed_size: read_u32(header, 20)?,
            crc32: read_u32(header, 16)?,
            flags,
            local_header_offset: read_u32(header, 42)?,
            method: read_u16(header, 10)?,
            name,
            uncompressed_size: read_u32(header, 24)?,
        }))
    }
}
impl ZipArchiveExtractor<'_> {
    pub(super) fn extract(&self) -> Result<ArchiveFingerprint> {
        let mut bytes = Vec::new();
        let fingerprint =
            scan_open_archive(&self.archive_file, self.archive_path, Some(&mut bytes))?;
        if bytes.len() < END_OF_CENTRAL_DIRECTORY_LEN {
            return Err(zip_static("ZIP 파일이 너무 짧습니다."));
        }
        let search_window = END_OF_CENTRAL_DIRECTORY_LEN
            .checked_add(ZIP_COMMENT_MAX_LEN)
            .ok_or_else(|| zip_static("ZIP EOCD 최대 검색 범위 계산 실패"))?;
        let min_offset = bytes.len().saturating_sub(search_window);
        let max_offset = bytes
            .len()
            .checked_sub(END_OF_CENTRAL_DIRECTORY_LEN)
            .ok_or_else(|| zip_static("ZIP EOCD 최대 offset 계산 실패"))?;
        let search_end = max_offset
            .checked_add(4_usize)
            .ok_or_else(|| zip_static("ZIP EOCD 검색 범위 계산 실패"))?;
        let search_bytes = bytes
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
                bytes.as_slice(),
                offset,
                ZIP_EOCD_HEADER_RANGE,
            )?;
            let comment_len = usize::from(read_u16(eocd, 20)?);
            if offset
                .checked_add(END_OF_CENTRAL_DIRECTORY_LEN)
                .and_then(|value| value.checked_add(comment_len))
                == Some(bytes.len())
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
        if entry_count > ZIP_MAX_ENTRY_COUNT {
            return Err(err(format!(
                "ZIP entry 수 {entry_count} > {ZIP_MAX_ENTRY_COUNT}"
            )));
        }
        let central_dir_size = usize::try_from(read_u32(eocd, 12)?)
            .map_err(|source| err_with_source("ZIP 중앙 디렉터리 크기 변환 실패", source))?;
        let central_dir_offset = usize::try_from(read_u32(eocd, 16)?)
            .map_err(|source| err_with_source("ZIP 중앙 디렉터리 offset 변환 실패", source))?;
        let central_dir_end = central_dir_offset
            .checked_add(central_dir_size)
            .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 범위 계산 실패"))?;
        if central_dir_end > eocd_offset {
            return Err(zip_static("ZIP 중앙 디렉터리가 EOCD 범위를 벗어났습니다."));
        }
        let mut central_directory = ZipCentralDirectory {
            bytes: bytes.as_slice(),
            cursor: central_dir_offset,
            end: central_dir_end,
            remaining_entries: entry_count,
        };
        let mut total_uncompressed = 0_usize;
        let mut seen_paths = HashSet::new();
        seen_paths.try_reserve(entry_count).map_err(|source| {
            err_with_source("ZIP entry path 방문 집합 메모리 확보 실패", source)
        })?;
        while let Some(entry) = central_directory.next_entry()? {
            let relative_path = entry.relative_path()?;
            let entry_path = self.unpack_dir.join(&relative_path);
            if !seen_paths.insert(relative_path) {
                return Err(err(format!("ZIP 중복 entry 경로: {}", entry.name)));
            }
            total_uncompressed =
                self.extract_entry(bytes.as_slice(), &entry, &entry_path, total_uncompressed)?;
        }
        Ok(fingerprint)
    }
    fn extract_entry(
        &self,
        bytes: &[u8],
        entry: &ZipEntry<'_>,
        entry_path: &Path,
        total_uncompressed: usize,
    ) -> Result<usize> {
        let entry_name = entry.name;
        let is_dir = entry_name.ends_with('/');
        if is_dir {
            create_zip_dir(entry_path, "xlsx 압축 폴더 생성 실패")?;
            return Ok(total_uncompressed);
        }
        if let Some(parent) = entry_path.parent() {
            create_zip_dir(parent, "xlsx 압축 해제 폴더 생성 실패")?;
        }
        let expected_len = usize::try_from(entry.uncompressed_size)
            .map_err(|source| err_with_source("ZIP 해제 크기 변환 실패", source))?;
        ensure_zip_size_limit(
            "entry 해제",
            expected_len,
            ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES,
            entry_name,
        )?;
        let next_total_uncompressed = total_uncompressed
            .checked_add(expected_len)
            .ok_or_else(|| zip_static("ZIP 전체 해제 크기 계산 실패"))?;
        ensure_zip_size_limit(
            "전체 해제",
            next_total_uncompressed,
            ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES,
            entry_name,
        )?;
        let data = entry.data(bytes, expected_len)?;
        let mut output = File::options()
            .write(true)
            .create_new(true)
            .open(entry_path)
            .map_err(|source_err| {
                err_with_source(
                    path_pair_context_message(
                        "xlsx 압축 해제 파일 생성 실패",
                        self.archive_path,
                        entry_path,
                    ),
                    source_err,
                )
            })?;
        IoWrite::write_all(&mut output, data.as_ref()).map_err(|source_err| {
            err_with_source(
                path_pair_context_message(
                    "xlsx 압축 해제 파일 쓰기 실패",
                    self.archive_path,
                    entry_path,
                ),
                source_err,
            )
        })?;
        Ok(next_total_uncompressed)
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
    let read_limit = u64::try_from(ZIP_MAX_ARCHIVE_BYTES)
        .ok()
        .and_then(|value| value.checked_add(1))
        .ok_or_else(|| err("xlsx 압축 파일 읽기 한도 계산 실패"))?;
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
        let chunk = buffer
            .get(..read_len)
            .ok_or_else(|| err("xlsx 압축 파일 읽기 buffer 범위 오류"))?;
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
fn create_zip_dir(path: &Path, context: &str) -> Result<()> {
    fs::create_dir_all(path)
        .map_err(|source_err| err_with_source(path_context_message(context, path), source_err))
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
fn collect_files(root: &Path, dir: &Path, files: &mut Vec<PendingFile>) -> Result<()> {
    for entry_result in fs::read_dir(dir).map_err(|source_err| {
        err_with_source(
            path_context_message("xlsx 파트 폴더 읽기 실패", dir),
            source_err,
        )
    })? {
        let entry = entry_result.map_err(|source_err| {
            err_with_source(
                path_context_message("xlsx 파트 항목 읽기 실패", dir),
                source_err,
            )
        })?;
        let path = entry.path();
        let metadata = entry.metadata().map_err(|source_err| {
            err_with_source(
                path_context_message("xlsx 파트 속성 읽기 실패", &path),
                source_err,
            )
        })?;
        match (metadata.is_dir(), metadata.is_file()) {
            (true, _) => collect_files(root, &path, files)?,
            (false, true) => {
                let rel = path
                    .strip_prefix(root)
                    .map_err(|source| err_with_source("xlsx 상대 경로 계산 실패", source))?;
                let name = path_to_slashes(rel, rel.display())?;
                files
                    .try_reserve(1)
                    .map_err(|source| err_with_source("xlsx 파트 목록 메모리 확보 실패", source))?;
                files.push(PendingFile { name, path });
            }
            (false, false) => {}
        }
    }
    Ok(())
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
