use super::super::ZipArchiveBuilder;
use super::{
    CENTRAL_DIRECTORY_SIGNATURE, DOS_DATE_1980_01_01, END_OF_CENTRAL_DIRECTORY_LEN,
    END_OF_CENTRAL_DIRECTORY_SIGNATURE, GENERAL_PURPOSE_UTF8_FLAG, LOCAL_FILE_HEADER_LEN,
    LOCAL_FILE_HEADER_SIGNATURE, METHOD_DEFLATE, METHOD_STORE, PendingFile, VERSION_NEEDED,
    ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES, collect_files, crc32, deflate,
};
use crate::diagnostic::{
    Result, err, err_with_source, path_context_message, path_pair_context_message, prefixed_message,
};
use std::{
    fs::File,
    io::{Read as IoRead, Write as IoWrite},
    path::Path,
};
const CENTRAL_FILE_HEADER_BASE_LEN: usize = 46;
struct CentralDirectoryPlan {
    max_header_capacity: usize,
    size: usize,
}
struct CompressedPart {
    data: Vec<u8>,
    method: u16,
}
struct WriteEntry {
    compressed_size: usize,
    crc32: u32,
    local_header_offset: u32,
    method: u16,
    name: String,
    uncompressed_size: usize,
}
#[derive(Clone, Copy)]
enum ZipFileHeader<'entry> {
    Central(&'entry WriteEntry),
    Local {
        crc32: u32,
        compressed_len: usize,
        method: u16,
        name: &'entry str,
        uncompressed_len: usize,
    },
}
struct StreamingZipWriter<'path> {
    archive_path: &'path Path,
    bytes_written: u64,
    entries: Vec<WriteEntry>,
    file: File,
}
impl ZipArchiveBuilder<'_> {
    pub(in crate::excel) fn create(&self) -> Result<()> {
        let mut files = Vec::new();
        collect_files(self.root, self.root, &mut files)?;
        files.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        let file = File::create(self.archive_path).map_err(|source_err| {
            err_with_source(
                path_context_message("xlsx 압축 파일 생성 실패", self.archive_path),
                source_err,
            )
        })?;
        StreamingZipWriter {
            archive_path: self.archive_path,
            bytes_written: 0,
            entries: Vec::new(),
            file,
        }
        .write(files)
    }
}
impl StreamingZipWriter<'_> {
    fn append_central_directory(&mut self) -> Result<()> {
        let central_dir_offset = u32::try_from(self.bytes_written)
            .map_err(|source| err_with_source("ZIP 중앙 디렉터리 offset 변환 실패", source))?;
        let Some(central_dir_plan) = self.entries.iter().try_fold(
            CentralDirectoryPlan {
                max_header_capacity: 0,
                size: 0,
            },
            |plan, entry| {
                let header_len = CENTRAL_FILE_HEADER_BASE_LEN.checked_add(entry.name.len())?;
                Some(CentralDirectoryPlan {
                    max_header_capacity: plan.max_header_capacity.max(header_len),
                    size: plan.size.checked_add(header_len)?,
                })
            },
        ) else {
            return Err(err(
                "ZIP 중앙 디렉터리 크기 계산 중 overflow가 발생했습니다.",
            ));
        };
        let central_dir_size = u32::try_from(central_dir_plan.size)
            .map_err(|source| err_with_source("ZIP 중앙 디렉터리 크기 변환 실패", source))?;
        let entry_count_u16 = u16::try_from(self.entries.len())
            .map_err(|source| err_with_source("ZIP entry 수 변환 실패", source))?;
        let mut central_header = Vec::new();
        central_header
            .try_reserve(central_dir_plan.max_header_capacity)
            .map_err(|source| {
                err(prefixed_message(
                    "ZIP 중앙 header 메모리 확보 실패: ",
                    source,
                ))
            })?;
        for index in 0..self.entries.len() {
            central_header.clear();
            {
                let entry = self
                    .entries
                    .get(index)
                    .ok_or_else(|| err("ZIP entry 접근 범위 오류"))?;
                write_file_header(&mut central_header, ZipFileHeader::Central(entry))?;
            }
            self.write_all(&central_header, "xlsx 압축 중앙 디렉터리 쓰기 실패")?;
        }
        let mut eocd = Vec::new();
        eocd.try_reserve(END_OF_CENTRAL_DIRECTORY_LEN)
            .map_err(|source| {
                err(prefixed_message(
                    "ZIP 중앙 디렉터리 footer 메모리 확보 실패: ",
                    source,
                ))
            })?;
        write_u32(&mut eocd, END_OF_CENTRAL_DIRECTORY_SIGNATURE);
        write_u16(&mut eocd, 0);
        write_u16(&mut eocd, 0);
        write_u16(&mut eocd, entry_count_u16);
        write_u16(&mut eocd, entry_count_u16);
        write_u32(&mut eocd, central_dir_size);
        write_u32(&mut eocd, central_dir_offset);
        write_u16(&mut eocd, 0);
        self.write_all(&eocd, "xlsx 압축 중앙 디렉터리 쓰기 실패")
    }
    fn append_file(&mut self, file: PendingFile) -> Result<()> {
        let data = self.read_part_bytes(&file)?;
        let crc32 = crc32(&data)?;
        let uncompressed_size = data.len();
        let deflated = deflate::DeflateWriter { bytes: &data }.deflate()?;
        let compressed = if deflated.len() < uncompressed_size {
            CompressedPart {
                data: deflated,
                method: METHOD_DEFLATE,
            }
        } else {
            CompressedPart {
                data,
                method: METHOD_STORE,
            }
        };
        let local_header_offset = u32::try_from(self.bytes_written)
            .map_err(|source| err_with_source("ZIP offset 변환 실패", source))?;
        let local_header_capacity = LOCAL_FILE_HEADER_LEN
            .checked_add(file.name.len())
            .ok_or_else(|| err("ZIP local header 크기 계산 중 overflow가 발생했습니다."))?;
        let mut local_header = Vec::new();
        local_header
            .try_reserve(local_header_capacity)
            .map_err(|source| {
                err(prefixed_message(
                    "ZIP local header 메모리 확보 실패: ",
                    source,
                ))
            })?;
        let compressed_size = compressed.data.len();
        write_file_header(
            &mut local_header,
            ZipFileHeader::Local {
                crc32,
                compressed_len: compressed_size,
                method: compressed.method,
                name: &file.name,
                uncompressed_len: uncompressed_size,
            },
        )?;
        self.write_all(&local_header, "xlsx 압축 local header 쓰기 실패")?;
        self.write_all(&compressed.data, "xlsx 압축 파일 데이터 쓰기 실패")?;
        self.entries.push(WriteEntry {
            compressed_size,
            crc32,
            local_header_offset,
            method: compressed.method,
            name: file.name,
            uncompressed_size,
        });
        Ok(())
    }
    fn read_part_bytes(&self, file: &PendingFile) -> Result<Vec<u8>> {
        let source_file = File::open(&file.path).map_err(|source_err| {
            err_with_source(
                path_pair_context_message("xlsx 파트 열기 실패", self.archive_path, &file.path),
                source_err,
            )
        })?;
        let metadata = source_file.metadata().map_err(|source_err| {
            err_with_source(
                path_pair_context_message(
                    "xlsx 파트 정보 확인 실패",
                    self.archive_path,
                    &file.path,
                ),
                source_err,
            )
        })?;
        let part_len = usize::try_from(metadata.len()).map_err(|source| {
            err(format!(
                "xlsx 파트 크기 변환 실패({} -> {}): {source}",
                self.archive_path.display(),
                file.path.display()
            ))
        })?;
        if part_len > ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES {
            return Err(err(format!(
                "xlsx 파트 크기가 허용 한도({ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES} bytes)를 초과했습니다: {} -> {}",
                self.archive_path.display(),
                file.path.display()
            )));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(part_len)
            .map_err(|source| err_with_source("xlsx 파트 메모리 확보 실패", source))?;
        let read_limit = u64::try_from(ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| err("xlsx 파트 읽기 한도 계산 실패"))?;
        let mut limited = source_file.take(read_limit);
        IoRead::read_to_end(&mut limited, &mut bytes).map_err(|source_err| {
            err_with_source(
                path_pair_context_message("xlsx 파트 읽기 실패", self.archive_path, &file.path),
                source_err,
            )
        })?;
        if bytes.len() > ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES {
            return Err(err(format!(
                "xlsx 파트 크기가 허용 한도({ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES} bytes)를 초과했습니다: {} -> {}",
                self.archive_path.display(),
                file.path.display()
            )));
        }
        if bytes.len() != part_len {
            return Err(err(format!(
                "xlsx 파트가 읽는 중 변경되었습니다: {} -> {}",
                self.archive_path.display(),
                file.path.display()
            )));
        }
        Ok(bytes)
    }
    fn write(mut self, files: Vec<PendingFile>) -> Result<()> {
        self.entries.try_reserve(files.len()).map_err(|source| {
            err(prefixed_message(
                "ZIP entry 목록 메모리 확보 실패: ",
                source,
            ))
        })?;
        for file in files {
            self.append_file(file)?;
        }
        self.append_central_directory()
    }
    fn write_all(&mut self, bytes: &[u8], context: &str) -> Result<()> {
        IoWrite::write_all(&mut self.file, bytes).map_err(|source_err| {
            err_with_source(path_context_message(context, self.archive_path), source_err)
        })?;
        self.bytes_written =
            self.bytes_written
                .checked_add(u64::try_from(bytes.len()).map_err(|source| {
                    err_with_source("ZIP written byte count 변환 실패", source)
                })?)
                .ok_or_else(|| err("ZIP written byte count 계산 중 overflow가 발생했습니다."))?;
        Ok(())
    }
}
fn write_file_header(out: &mut Vec<u8>, header: ZipFileHeader<'_>) -> Result<()> {
    match header {
        ZipFileHeader::Central(entry) => {
            let name = entry.name.as_bytes();
            let name_len = u16::try_from(name.len())
                .map_err(|source| err_with_source("ZIP entry 이름 길이 변환 실패", source))?;
            let compressed_size = u32::try_from(entry.compressed_size)
                .map_err(|source| err_with_source("ZIP entry 압축 크기 변환 실패", source))?;
            let uncompressed_size = u32::try_from(entry.uncompressed_size)
                .map_err(|source| err_with_source("ZIP entry 원본 크기 변환 실패", source))?;
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
            let name_len = u16::try_from(name_bytes.len())
                .map_err(|source| err_with_source("ZIP entry 이름 길이 변환 실패", source))?;
            let compressed_size = u32::try_from(compressed_len)
                .map_err(|source| err_with_source("ZIP entry 압축 크기 변환 실패", source))?;
            let uncompressed_size = u32::try_from(uncompressed_len)
                .map_err(|source| err_with_source("ZIP entry 원본 크기 변환 실패", source))?;
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
