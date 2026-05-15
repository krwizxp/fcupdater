use crate::{
    Result, err, err_with_source, path_pair_source_message, path_source_message, prefixed_message,
};
use alloc::{borrow::Cow, string::String, vec::Vec};
use core::{result::Result as StdResult, str};
use std::{
    fs,
    io::Write as _,
    path::{Component, Path, PathBuf},
};
mod deflate;
const CENTRAL_DIRECTORY_HEADER_LEN: usize = 46;
const CENTRAL_DIRECTORY_SIGNATURE: u32 = 0x0201_4b50;
const CODE_LENGTH_SYMBOLS: usize = 19;
const CRC32_POLY: u32 = 0xedb8_8320;
const DEFLATE_MAX_BITS: usize = 15;
const DEFLATE_MAX_BITS_U8: u8 = 15;
const DISTANCE_SYMBOLS: usize = 30;
const DOS_DATE_1980_01_01: u16 = 33;
const END_OF_CENTRAL_DIRECTORY_LEN: usize = 22;
const END_OF_CENTRAL_DIRECTORY_SIGNATURE: u32 = 0x0605_4b50;
const ENCRYPTED_FLAG: u16 = 0x0001;
const FIXED_DISTANCE_SYMBOLS: usize = 32;
const FIXED_LITERAL_SYMBOLS: usize = 288;
const GENERAL_PURPOSE_UTF8_FLAG: u16 = 0x0800;
const HASH_SIZE: usize = 0x8000;
const LITERAL_LENGTH_SYMBOLS: usize = 286;
const LOCAL_FILE_HEADER_LEN: usize = 30;
const MAX_CHAIN: usize = 64;
const MAX_MATCH: usize = 258;
const MIN_MATCH: usize = 3;
const LOCAL_FILE_HEADER_SIGNATURE: u32 = 0x0403_4b50;
const METHOD_DEFLATE: u16 = 8;
const METHOD_STORE: u16 = 0;
const VERSION_NEEDED: u16 = 20;
const ZIP_COMMENT_MAX_LEN: usize = 0xffff;
const ZIP_BAD_CRC_MESSAGE: &str = "ZIP CRC가 일치하지 않습니다";
const ZIP_BAD_LOCAL_HEADER_MESSAGE: &str = "ZIP local header signature가 올바르지 않습니다";
const ZIP_BAD_SIZE_MESSAGE: &str = "ZIP 해제 크기가 일치하지 않습니다";
const ZIP_DATA_RANGE_MESSAGE: &str = "ZIP entry 데이터가 파일 범위를 벗어났습니다";
const ZIP_MAX_ARCHIVE_BYTES: usize = 128 * 1024 * 1024;
const ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES: usize = 64 * 1024 * 1024;
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
type ZipResult<T> = StdResult<T, Cow<'static, str>>;
#[derive(Clone)]
struct ZipEntry {
    compressed_size: u32,
    crc32: u32,
    local_header_offset: u32,
    method: u16,
    name: String,
    uncompressed_size: u32,
}
#[derive(Clone)]
struct WriteEntry {
    compressed_data: Vec<u8>,
    crc32: u32,
    local_header_offset: u32,
    method: u16,
    name: String,
    uncompressed_size: usize,
}
#[derive(Clone, Copy)]
enum ZipFileHeader<'a> {
    Central(&'a WriteEntry),
    Local {
        crc32: u32,
        compressed_len: usize,
        method: u16,
        name: &'a str,
        uncompressed_len: usize,
    },
}
pub(super) struct ZipArchiveOps;
pub(super) trait ZipArchiveOpsExt {
    fn create_from_directory(&self, root: &Path, archive_path: &Path) -> Result<()>;
    fn extract_to_directory(&self, archive_path: &Path, unpack_dir: &Path) -> Result<()>;
    fn list_entries(&self, archive_path: &Path) -> Result<Vec<String>>;
}
impl ZipArchiveOpsExt for ZipArchiveOps {
    fn create_from_directory(&self, root: &Path, archive_path: &Path) -> Result<()> {
        let mut files = Vec::new();
        collect_files(root, root, &mut files)?;
        files.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        let mut archive = Vec::new();
        let mut entries = Vec::new();
        entries.try_reserve(files.len()).map_err(|source| {
            err(prefixed_message(
                "ZIP entry 목록 메모리 확보 실패: ",
                source,
            ))
        })?;
        for file in files {
            let data = fs::read(&file.path).map_err(|source_err| {
                err(path_source_message(
                    "xlsx 파트 읽기 실패",
                    &file.path,
                    source_err,
                ))
            })?;
            let crc32 = crc32(&data);
            let uncompressed_size = data.len();
            let deflated = <deflate::DeflateWriter as deflate::DeflateWriterExt>::deflate(&data)
                .map_err(err)?;
            let (method, compressed_data) = if deflated.len() < uncompressed_size {
                (METHOD_DEFLATE, deflated)
            } else {
                (METHOD_STORE, data)
            };
            let local_header_offset = u32::try_from(archive.len())
                .map_err(|source| err(prefixed_message("ZIP offset 변환 실패: ", source)))?;
            write_file_header(
                &mut archive,
                ZipFileHeader::Local {
                    crc32,
                    compressed_len: compressed_data.len(),
                    method,
                    name: &file.name,
                    uncompressed_len: uncompressed_size,
                },
            )?;
            archive.extend_from_slice(&compressed_data);
            entries.push(WriteEntry {
                compressed_data,
                crc32,
                local_header_offset,
                method,
                name: file.name,
                uncompressed_size,
            });
        }
        let central_dir_offset = u32::try_from(archive.len()).map_err(|source| {
            err(prefixed_message(
                "ZIP 중앙 디렉터리 offset 변환 실패: ",
                source,
            ))
        })?;
        for entry in &entries {
            write_file_header(&mut archive, ZipFileHeader::Central(entry))?;
        }
        let central_dir_offset_usize = usize::try_from(central_dir_offset).map_err(|source| {
            err(prefixed_message(
                "ZIP 중앙 디렉터리 offset 재변환 실패: ",
                source,
            ))
        })?;
        let central_dir_size = u32::try_from(
            archive.len().saturating_sub(central_dir_offset_usize),
        )
        .map_err(|source| {
            err(prefixed_message(
                "ZIP 중앙 디렉터리 크기 변환 실패: ",
                source,
            ))
        })?;
        let entry_count_u16 = u16::try_from(entries.len())
            .map_err(|source| err(prefixed_message("ZIP entry 수 변환 실패: ", source)))?;
        write_u32(&mut archive, END_OF_CENTRAL_DIRECTORY_SIGNATURE);
        write_u16(&mut archive, 0);
        write_u16(&mut archive, 0);
        write_u16(&mut archive, entry_count_u16);
        write_u16(&mut archive, entry_count_u16);
        write_u32(&mut archive, central_dir_size);
        write_u32(&mut archive, central_dir_offset);
        write_u16(&mut archive, 0);
        let mut file = fs::File::create(archive_path).map_err(|source_err| {
            err(path_source_message(
                "xlsx 압축 파일 생성 실패",
                archive_path,
                source_err,
            ))
        })?;
        file.write_all(&archive).map_err(|source_err| {
            err(path_source_message(
                "xlsx 압축 파일 쓰기 실패",
                archive_path,
                source_err,
            ))
        })
    }
    fn extract_to_directory(&self, archive_path: &Path, unpack_dir: &Path) -> Result<()> {
        let bytes = read_archive_bytes(archive_path)?;
        let entries = parse_entries(&bytes).map_err(err)?;
        let mut total_uncompressed = 0_usize;
        for entry in entries {
            let entry_name = entry.name.as_str();
            if !is_safe_archive_entry_path(&entry.name) {
                return Err(err(zip_entry_message(ZIP_UNSAFE_PATH_MESSAGE, entry_name)));
            }
            if entry.name.ends_with('/') {
                let dir_path = unpack_dir.join(path_from_entry_name(&entry.name)?);
                create_zip_dir(&dir_path, "xlsx 압축 폴더 생성 실패")?;
                continue;
            }
            let output_path = unpack_dir.join(path_from_entry_name(&entry.name)?);
            if let Some(parent) = output_path.parent() {
                create_zip_dir(parent, "xlsx 압축 해제 폴더 생성 실패")?;
            }
            let expected_len = usize::try_from(entry.uncompressed_size)
                .map_err(|source| err(format!("ZIP 해제 크기 변환 실패: {source}")))?;
            ensure_zip_size_limit(
                "entry 해제",
                expected_len,
                ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES,
                entry_name,
            )?;
            total_uncompressed = total_uncompressed
                .checked_add(expected_len)
                .ok_or_else(|| err(zip_static("ZIP 전체 해제 크기 계산 실패")))?;
            ensure_zip_size_limit(
                "전체 해제",
                total_uncompressed,
                ZIP_MAX_TOTAL_UNCOMPRESSED_BYTES,
                entry_name,
            )?;
            let data = (|| -> ZipResult<Vec<u8>> {
                let local_offset = usize::try_from(entry.local_header_offset)
                    .map_err(|source| format!("ZIP local header offset 변환 실패: {source}"))?;
                if read_u32(&bytes, local_offset)? != LOCAL_FILE_HEADER_SIGNATURE {
                    return Err(zip_entry_message(ZIP_BAD_LOCAL_HEADER_MESSAGE, entry_name).into());
                }
                let name_len = usize::from(read_u16(&bytes, local_offset.saturating_add(26))?);
                let extra_len = usize::from(read_u16(&bytes, local_offset.saturating_add(28))?);
                let data_start = local_offset
                    .checked_add(LOCAL_FILE_HEADER_LEN)
                    .and_then(|value| value.checked_add(name_len))
                    .and_then(|value| value.checked_add(extra_len))
                    .ok_or_else(|| zip_static("ZIP data offset 계산 실패"))?;
                let compressed_len = usize::try_from(entry.compressed_size)
                    .map_err(|source| format!("ZIP 압축 크기 변환 실패: {source}"))?;
                let data_end = data_start
                    .checked_add(compressed_len)
                    .ok_or_else(|| zip_static("ZIP data end 계산 실패"))?;
                let Some(compressed) = bytes.get(data_start..data_end) else {
                    return Err(zip_entry_message(ZIP_DATA_RANGE_MESSAGE, entry_name).into());
                };
                let output = match entry.method {
                    METHOD_STORE => {
                        let mut output = Vec::new();
                        output.try_reserve(compressed.len()).map_err(|source| {
                            format!("ZIP 저장 entry 메모리 확보 실패: {source}")
                        })?;
                        output.extend_from_slice(compressed);
                        output
                    }
                    METHOD_DEFLATE => {
                        <deflate::DeflateInflater as deflate::DeflateInflaterExt>::inflate(
                            compressed,
                            expected_len,
                        )?
                    }
                    method => {
                        return Err(format!(
                            "지원하지 않는 ZIP 압축 방식({method}): {}",
                            entry.name
                        )
                        .into());
                    }
                };
                if output.len() != expected_len {
                    return Err(zip_entry_message(ZIP_BAD_SIZE_MESSAGE, entry_name).into());
                }
                let actual_crc = crc32(&output);
                if actual_crc != entry.crc32 {
                    return Err(zip_entry_message(ZIP_BAD_CRC_MESSAGE, entry_name).into());
                }
                Ok(output)
            })()
            .map_err(err)?;
            fs::write(&output_path, data).map_err(|source_err| {
                err(path_pair_source_message(
                    "xlsx 압축 해제 파일 쓰기 실패",
                    archive_path,
                    &output_path,
                    source_err,
                ))
            })?;
        }
        Ok(())
    }
    fn list_entries(&self, archive_path: &Path) -> Result<Vec<String>> {
        let bytes = read_archive_bytes(archive_path)?;
        let entries = parse_entries(&bytes).map_err(err)?;
        let mut names = Vec::new();
        names
            .try_reserve(entries.len())
            .map_err(|source| err_with_source("ZIP entry 이름 목록 메모리 확보 실패", source))?;
        for entry in entries {
            names.push(entry.name);
        }
        Ok(names)
    }
}
struct PendingFile {
    name: String,
    path: PathBuf,
}
const fn zip_static(message: &'static str) -> Cow<'static, str> {
    Cow::Borrowed(message)
}
fn create_zip_dir(path: &Path, context: &str) -> Result<()> {
    fs::create_dir_all(path)
        .map_err(|source_err| err(path_source_message(context, path, source_err)))
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
fn read_archive_bytes(archive_path: &Path) -> Result<Vec<u8>> {
    let metadata = fs::metadata(archive_path).map_err(|source_err| {
        err(path_source_message(
            "xlsx 압축 파일 정보 확인 실패",
            archive_path,
            source_err,
        ))
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
    fs::read(archive_path).map_err(|source_err| {
        err(path_source_message(
            "xlsx 압축 파일 읽기 실패",
            archive_path,
            source_err,
        ))
    })
}
fn zip_entry_message(context: &str, entry_name: &str) -> String {
    format!("{context}: {entry_name}")
}
fn collect_files(root: &Path, dir: &Path, files: &mut Vec<PendingFile>) -> Result<()> {
    for entry_result in fs::read_dir(dir).map_err(|source_err| {
        err(path_source_message(
            "xlsx 파트 폴더 읽기 실패",
            dir,
            source_err,
        ))
    })? {
        let entry = entry_result.map_err(|source_err| {
            err(path_source_message(
                "xlsx 파트 항목 읽기 실패",
                dir,
                source_err,
            ))
        })?;
        let path = entry.path();
        let metadata = entry.metadata().map_err(|source_err| {
            err(path_source_message(
                "xlsx 파트 속성 읽기 실패",
                &path,
                source_err,
            ))
        })?;
        match (metadata.is_dir(), metadata.is_file()) {
            (true, _) => collect_files(root, &path, files)?,
            (false, true) => {
                let rel = path.strip_prefix(root).map_err(|source| {
                    err(prefixed_message("xlsx 상대 경로 계산 실패: ", source))
                })?;
                let mut name = String::new();
                for component in rel.components() {
                    let Component::Normal(part) = component else {
                        return Err(err(prefixed_message(
                            "xlsx 압축 경로에 허용되지 않은 component가 있습니다: ",
                            rel.display(),
                        )));
                    };
                    let Some(text) = part.to_str() else {
                        return Err(err(prefixed_message(
                            "xlsx 압축 경로가 UTF-8이 아닙니다: ",
                            rel.display(),
                        )));
                    };
                    let separator_len = usize::from(!name.is_empty());
                    name.try_reserve(separator_len.saturating_add(text.len()))
                        .map_err(|source| {
                            err(prefixed_message(
                                "xlsx 압축 경로 메모리 확보 실패: ",
                                source,
                            ))
                        })?;
                    if !name.is_empty() {
                        name.push('/');
                    }
                    name.push_str(text);
                }
                files.push(PendingFile { name, path });
            }
            (false, false) => {}
        }
    }
    Ok(())
}
fn crc32(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0_u8..8_u8 {
            if crc & 1_u32 == 0 {
                crc >>= 1_u8;
            } else {
                crc = (crc >> 1_u8) ^ CRC32_POLY;
            }
        }
    }
    !crc
}
pub(super) fn is_safe_archive_entry_path(entry_name: &str) -> bool {
    if entry_name.is_empty() || entry_name.starts_with(['/', '\\']) {
        return false;
    }
    let bytes = entry_name.as_bytes();
    if let Some(&[first, colon]) = bytes.first_chunk::<2>()
        && colon == b':'
        && first.is_ascii_alphabetic()
    {
        return false;
    }
    let mut has_name = false;
    for part in entry_name.split(['/', '\\']) {
        if part.is_empty() || part == "." {
            continue;
        }
        if part == ".." {
            return false;
        }
        has_name = true;
    }
    has_name
}
fn parse_entries(bytes: &[u8]) -> ZipResult<Vec<ZipEntry>> {
    if bytes.len() < END_OF_CENTRAL_DIRECTORY_LEN {
        return Err(zip_static("ZIP 파일이 너무 짧습니다."));
    }
    let min_offset = bytes
        .len()
        .saturating_sub(END_OF_CENTRAL_DIRECTORY_LEN.saturating_add(ZIP_COMMENT_MAX_LEN));
    let max_offset = bytes.len().saturating_sub(END_OF_CENTRAL_DIRECTORY_LEN);
    let search_end = max_offset.saturating_add(4_usize);
    let search_bytes = bytes
        .get(min_offset..search_end)
        .ok_or_else(|| zip_static("ZIP EOCD 검색 범위 오류"))?;
    let eocd_signature = END_OF_CENTRAL_DIRECTORY_SIGNATURE.to_le_bytes();
    let mut search_len = search_bytes.len();
    let eocd_offset = loop {
        let Some(relative_offset) = search_bytes
            .get(..search_len)
            .ok_or_else(|| zip_static("ZIP EOCD 검색 범위 오류"))?
            .array_windows::<4>()
            .rposition(|window| *window == eocd_signature)
        else {
            return Err(zip_static("ZIP EOCD를 찾지 못했습니다."));
        };
        let offset = min_offset.saturating_add(relative_offset);
        let comment_len = usize::from(read_u16(bytes, offset.saturating_add(20))?);
        if offset
            .checked_add(END_OF_CENTRAL_DIRECTORY_LEN)
            .and_then(|value| value.checked_add(comment_len))
            == Some(bytes.len())
        {
            break offset;
        }
        search_len = relative_offset;
    };
    let total_entries = usize::from(read_u16(bytes, eocd_offset.saturating_add(10))?);
    let central_dir_size = usize::try_from(read_u32(bytes, eocd_offset.saturating_add(12))?)
        .map_err(|source| format!("ZIP 중앙 디렉터리 크기 변환 실패: {source}"))?;
    let central_dir_offset = usize::try_from(read_u32(bytes, eocd_offset.saturating_add(16))?)
        .map_err(|source| format!("ZIP 중앙 디렉터리 offset 변환 실패: {source}"))?;
    let central_dir_end = central_dir_offset
        .checked_add(central_dir_size)
        .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 범위 계산 실패"))?;
    if central_dir_end > eocd_offset {
        return Err(zip_static("ZIP 중앙 디렉터리가 EOCD 범위를 벗어났습니다."));
    }
    let mut entries = Vec::new();
    entries
        .try_reserve(total_entries)
        .map_err(|source| format!("ZIP 중앙 디렉터리 entry 목록 메모리 확보 실패: {source}"))?;
    let mut cursor = central_dir_offset;
    for _ in 0..total_entries {
        if read_u32(bytes, cursor)? != CENTRAL_DIRECTORY_SIGNATURE {
            return Err(zip_static(
                "ZIP 중앙 디렉터리 signature가 올바르지 않습니다.",
            ));
        }
        let flags = read_u16(bytes, cursor.saturating_add(8))?;
        if flags & ENCRYPTED_FLAG != 0 {
            return Err(zip_static("암호화된 ZIP entry는 지원하지 않습니다."));
        }
        let method = read_u16(bytes, cursor.saturating_add(10))?;
        let crc32 = read_u32(bytes, cursor.saturating_add(16))?;
        let compressed_size = read_u32(bytes, cursor.saturating_add(20))?;
        let uncompressed_size = read_u32(bytes, cursor.saturating_add(24))?;
        let name_len = usize::from(read_u16(bytes, cursor.saturating_add(28))?);
        let extra_len = usize::from(read_u16(bytes, cursor.saturating_add(30))?);
        let comment_len = usize::from(read_u16(bytes, cursor.saturating_add(32))?);
        let local_header_offset = read_u32(bytes, cursor.saturating_add(42))?;
        let name_start = cursor.saturating_add(CENTRAL_DIRECTORY_HEADER_LEN);
        let name_end = name_start
            .checked_add(name_len)
            .ok_or_else(|| zip_static("ZIP entry 이름 범위 계산 실패"))?;
        let Some(name_bytes) = bytes.get(name_start..name_end) else {
            return Err(zip_static("ZIP entry 이름이 파일 범위를 벗어났습니다."));
        };
        let name = str::from_utf8(name_bytes)
            .map_err(|source| format!("ZIP entry 이름이 UTF-8이 아닙니다: {source}"))?
            .to_owned();
        entries.push(ZipEntry {
            compressed_size,
            crc32,
            local_header_offset,
            method,
            name,
            uncompressed_size,
        });
        cursor = name_end
            .checked_add(extra_len)
            .and_then(|value| value.checked_add(comment_len))
            .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 다음 entry 위치 계산 실패"))?;
    }
    if cursor != central_dir_end {
        return Err(zip_static(
            "ZIP 중앙 디렉터리 크기가 entry 목록과 일치하지 않습니다.",
        ));
    }
    Ok(entries)
}
fn path_from_entry_name(entry_name: &str) -> ZipResult<PathBuf> {
    if !is_safe_archive_entry_path(entry_name) {
        return Err(format!("허용되지 않은 압축 경로가 포함되어 있습니다: {entry_name}").into());
    }
    let mut path = PathBuf::new();
    for part in entry_name.split(['/', '\\']) {
        if !part.is_empty() && part != "." {
            path.push(part);
        }
    }
    Ok(path)
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
    let Some(end) = offset.checked_add(N) else {
        return Err(zip_static(error_message));
    };
    let Some(raw_bytes) = bytes.get(offset..end).and_then(<[u8]>::as_array::<N>) else {
        return Err(zip_static(error_message));
    };
    Ok(*raw_bytes)
}
fn write_file_header(out: &mut Vec<u8>, header: ZipFileHeader<'_>) -> Result<()> {
    match header {
        ZipFileHeader::Central(entry) => {
            let name = entry.name.as_bytes();
            let name_len = u16::try_from(name.len()).map_err(|source| {
                err(prefixed_message("ZIP entry 이름 길이 변환 실패: ", source))
            })?;
            let compressed_size = u32::try_from(entry.compressed_data.len()).map_err(|source| {
                err(prefixed_message("ZIP entry 압축 크기 변환 실패: ", source))
            })?;
            let uncompressed_size = u32::try_from(entry.uncompressed_size).map_err(|source| {
                err(prefixed_message("ZIP entry 원본 크기 변환 실패: ", source))
            })?;
            write_u32(out, CENTRAL_DIRECTORY_SIGNATURE);
            write_u16(out, VERSION_NEEDED);
            write_u16(out, VERSION_NEEDED);
            write_u16(out, GENERAL_PURPOSE_UTF8_FLAG);
            write_u16(out, entry.method);
            write_u16(out, 0);
            write_u16(out, DOS_DATE_1980_01_01);
            write_u32(out, entry.crc32);
            write_u32(out, compressed_size);
            write_u32(out, uncompressed_size);
            write_u16(out, name_len);
            write_u16(out, 0);
            write_u16(out, 0);
            write_u16(out, 0);
            write_u16(out, 0);
            write_u32(out, 0);
            write_u32(out, entry.local_header_offset);
            out.extend_from_slice(name);
        }
        ZipFileHeader::Local {
            compressed_len,
            crc32,
            method,
            name,
            uncompressed_len,
        } => {
            let name_bytes = name.as_bytes();
            let name_len = u16::try_from(name_bytes.len()).map_err(|source| {
                err(prefixed_message("ZIP entry 이름 길이 변환 실패: ", source))
            })?;
            let compressed_size = u32::try_from(compressed_len).map_err(|source| {
                err(prefixed_message("ZIP entry 압축 크기 변환 실패: ", source))
            })?;
            let uncompressed_size = u32::try_from(uncompressed_len).map_err(|source| {
                err(prefixed_message("ZIP entry 원본 크기 변환 실패: ", source))
            })?;
            write_u32(out, LOCAL_FILE_HEADER_SIGNATURE);
            write_u16(out, VERSION_NEEDED);
            write_u16(out, GENERAL_PURPOSE_UTF8_FLAG);
            write_u16(out, method);
            write_u16(out, 0);
            write_u16(out, DOS_DATE_1980_01_01);
            write_u32(out, crc32);
            write_u32(out, compressed_size);
            write_u32(out, uncompressed_size);
            write_u16(out, name_len);
            write_u16(out, 0);
            out.extend_from_slice(name_bytes);
        }
    }
    Ok(())
}
fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}
