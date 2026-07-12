use super::super::ZipArchiveBuilder;
use super::{
    CENTRAL_DIRECTORY_SIGNATURE, DOS_DATE_1980_01_01, END_OF_CENTRAL_DIRECTORY_LEN,
    END_OF_CENTRAL_DIRECTORY_SIGNATURE, GENERAL_PURPOSE_UTF8_FLAG, LOCAL_FILE_HEADER_LEN,
    LOCAL_FILE_HEADER_SIGNATURE, METHOD_DEFLATE, METHOD_STORE, PendingFile, VERSION_NEEDED,
    ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES, collect_files, crc32, deflate,
};
use crate::diagnostic::{
    Result, err, err_with_source, path_context_message, path_pair_context_message,
};
use core::mem;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::fs::Permissions;
use std::{
    fs::File,
    io::{BufWriter, Read as IoRead, Write as IoWrite},
    path::Path,
};
const CENTRAL_FILE_HEADER_BASE_LEN: usize = 46;
const DEFLATE_MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
const ZIP_OUTPUT_BUFFER_CAPACITY: usize = 64 * 1024;
const STORE_WITHOUT_DEFLATE_EXTENSIONS: [&str; 10] = [
    ".bin", ".gif", ".jpeg", ".jpg", ".mp3", ".mp4", ".png", ".webp", ".zip", ".zst",
];
struct CentralDirectoryPlan {
    max_header_capacity: usize,
    size: usize,
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
    deflate_workspace: deflate::DeflateWorkspace,
    entries: Vec<WriteEntry>,
    file: BufWriter<File>,
    header_buffer: Vec<u8>,
    part_buffer: Vec<u8>,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    permissions: Permissions,
}
impl ZipArchiveBuilder<'_> {
    pub(in crate::excel) fn create(self) -> Result<()> {
        let mut files = Vec::new();
        collect_files(self.root, self.root, &mut files)?;
        files.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        StreamingZipWriter {
            archive_path: self.archive_path,
            bytes_written: 0,
            deflate_workspace: deflate::DeflateWorkspace::default(),
            entries: Vec::new(),
            file: BufWriter::with_capacity(ZIP_OUTPUT_BUFFER_CAPACITY, self.file),
            header_buffer: Vec::new(),
            part_buffer: Vec::new(),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            permissions: self.permissions,
        }
        .write(files)
    }
}
impl StreamingZipWriter<'_> {
    fn add_bytes_written(&self, len: usize, context: &str) -> Result<u64> {
        let len_u64 = u64::try_from(len)
            .map_err(|source| err_with_source("ZIP written byte count 변환 실패", source))?;
        self.bytes_written
            .checked_add(len_u64)
            .ok_or_else(|| err(format!("{context} 중 overflow가 발생했습니다.")))
    }
    fn append_central_directory(&mut self) -> Result<()> {
        let central_dir_offset = u32::try_from(self.bytes_written)
            .map_err(|source| err_with_source("ZIP 중앙 디렉터리 offset 변환 실패", source))?;
        let entries = mem::take(&mut self.entries);
        let Some(central_dir_plan) = entries.iter().try_fold(
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
        let entry_count_u16 = u16::try_from(entries.len())
            .map_err(|source| err_with_source("ZIP entry 수 변환 실패", source))?;
        self.prepare_header_buffer(
            central_dir_plan.max_header_capacity,
            "ZIP 중앙 header 메모리 확보 실패",
        )?;
        for entry in &entries {
            self.header_buffer.clear();
            write_file_header(&mut self.header_buffer, ZipFileHeader::Central(entry))?;
            self.write_header_buffer("xlsx 압축 중앙 디렉터리 쓰기 실패")?;
        }
        self.prepare_header_buffer(
            END_OF_CENTRAL_DIRECTORY_LEN,
            "ZIP 중앙 디렉터리 footer 메모리 확보 실패",
        )?;
        write_u32(&mut self.header_buffer, END_OF_CENTRAL_DIRECTORY_SIGNATURE);
        write_u16(&mut self.header_buffer, 0);
        write_u16(&mut self.header_buffer, 0);
        write_u16(&mut self.header_buffer, entry_count_u16);
        write_u16(&mut self.header_buffer, entry_count_u16);
        write_u32(&mut self.header_buffer, central_dir_size);
        write_u32(&mut self.header_buffer, central_dir_offset);
        write_u16(&mut self.header_buffer, 0);
        self.write_header_buffer("xlsx 압축 중앙 디렉터리 쓰기 실패")
    }
    fn append_file(&mut self, file: PendingFile) -> Result<()> {
        self.read_part_bytes(&file)?;
        let crc32 = crc32(&self.part_buffer)?;
        let uncompressed_size = self.part_buffer.len();
        let store_without_deflate = uncompressed_size > DEFLATE_MAX_INPUT_BYTES
            || STORE_WITHOUT_DEFLATE_EXTENSIONS.iter().any(|extension| {
                file.name
                    .get(file.name.len().saturating_sub(extension.len())..)
                    .is_some_and(|tail| tail.eq_ignore_ascii_case(extension))
            });
        let deflate_plan = if uncompressed_size == 0 || store_without_deflate {
            None
        } else {
            Some(
                (deflate::DeflateWriter {
                    bytes: &self.part_buffer,
                    workspace: &mut self.deflate_workspace,
                })
                .plan()?,
            )
        };
        let use_deflate = deflate_plan
            .as_ref()
            .is_some_and(|plan| plan.len() < uncompressed_size);
        let (compressed_size, method) = if use_deflate {
            let plan = deflate_plan
                .as_ref()
                .ok_or_else(|| err("ZIP deflate 계획 상태가 손상되었습니다."))?;
            (plan.len(), METHOD_DEFLATE)
        } else {
            (uncompressed_size, METHOD_STORE)
        };
        let local_header_offset = u32::try_from(self.bytes_written)
            .map_err(|source| err_with_source("ZIP offset 변환 실패", source))?;
        let local_header_capacity = LOCAL_FILE_HEADER_LEN
            .checked_add(file.name.len())
            .ok_or_else(|| err("ZIP local header 크기 계산 중 overflow가 발생했습니다."))?;
        self.prepare_header_buffer(local_header_capacity, "ZIP local header 메모리 확보 실패")?;
        write_file_header(
            &mut self.header_buffer,
            ZipFileHeader::Local {
                crc32,
                compressed_len: compressed_size,
                method,
                name: &file.name,
                uncompressed_len: uncompressed_size,
            },
        )?;
        self.write_header_buffer("xlsx 압축 local header 쓰기 실패")?;
        if use_deflate {
            let plan = deflate_plan
                .as_ref()
                .ok_or_else(|| err("ZIP deflate 계획 상태가 손상되었습니다."))?;
            let actual_written = plan.write_to(&mut self.file)?;
            if actual_written != compressed_size {
                return Err(err(format!(
                    "ZIP deflate 출력 크기가 계획과 다릅니다: expected={compressed_size}, actual={actual_written}"
                )));
            }
            self.bytes_written =
                self.add_bytes_written(actual_written, "ZIP deflated byte count 계산")?;
        } else {
            IoWrite::write_all(&mut self.file, &self.part_buffer).map_err(|source_err| {
                err_with_source(
                    path_context_message("xlsx 압축 파일 데이터 쓰기 실패", self.archive_path),
                    source_err,
                )
            })?;
            self.bytes_written =
                self.add_bytes_written(compressed_size, "ZIP stored byte count 계산")?;
        }
        if let Some(plan) = deflate_plan {
            self.deflate_workspace.recycle(plan);
        }
        self.entries.push(WriteEntry {
            compressed_size,
            crc32,
            local_header_offset,
            method,
            name: file.name,
            uncompressed_size,
        });
        Ok(())
    }
    fn prepare_header_buffer(&mut self, capacity: usize, context: &'static str) -> Result<()> {
        self.header_buffer.clear();
        if self.header_buffer.capacity() < capacity {
            self.header_buffer
                .try_reserve_exact(capacity)
                .map_err(|source| err_with_source(context, source))?;
        }
        Ok(())
    }
    fn read_part_bytes(&mut self, file: &PendingFile) -> Result<()> {
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
            err_with_source(
                path_pair_context_message(
                    "xlsx 파트 크기 변환 실패",
                    self.archive_path,
                    &file.path,
                ),
                source,
            )
        })?;
        if part_len > ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES {
            return Err(err(format!(
                "xlsx 파트 크기가 허용 한도({ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES} bytes)를 초과했습니다: {} -> {}",
                self.archive_path.display(),
                file.path.display()
            )));
        }
        self.part_buffer.clear();
        if self.part_buffer.capacity() < part_len {
            self.part_buffer
                .try_reserve_exact(part_len)
                .map_err(|source| err_with_source("xlsx 파트 메모리 확보 실패", source))?;
        }
        let read_limit = u64::try_from(ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| err("xlsx 파트 읽기 한도 계산 실패"))?;
        let mut limited = source_file.take(read_limit);
        IoRead::read_to_end(&mut limited, &mut self.part_buffer).map_err(|source_err| {
            err_with_source(
                path_pair_context_message("xlsx 파트 읽기 실패", self.archive_path, &file.path),
                source_err,
            )
        })?;
        if self.part_buffer.len() > ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES {
            return Err(err(format!(
                "xlsx 파트 크기가 허용 한도({ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES} bytes)를 초과했습니다: {} -> {}",
                self.archive_path.display(),
                file.path.display()
            )));
        }
        if self.part_buffer.len() != part_len {
            return Err(err(format!(
                "xlsx 파트가 읽는 중 변경되었습니다: {} -> {}",
                self.archive_path.display(),
                file.path.display()
            )));
        }
        Ok(())
    }
    fn write(mut self, files: Vec<PendingFile>) -> Result<()> {
        self.entries
            .try_reserve_exact(files.len())
            .map_err(|source| err_with_source("ZIP entry 목록 메모리 확보 실패", source))?;
        for file in files {
            self.append_file(file)?;
        }
        self.append_central_directory()?;
        IoWrite::flush(&mut self.file).map_err(|source_err| {
            err_with_source(
                path_context_message("xlsx 압축 파일 flush 실패", self.archive_path),
                source_err,
            )
        })?;
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        self.file
            .get_ref()
            .set_permissions(self.permissions)
            .map_err(|source_err| {
                err_with_source(
                    path_context_message("xlsx 압축 파일 권한 적용 실패", self.archive_path),
                    source_err,
                )
            })?;
        self.file.get_ref().sync_all().map_err(|source_err| {
            err_with_source(
                path_context_message("xlsx 압축 파일 sync 실패", self.archive_path),
                source_err,
            )
        })
    }
    fn write_header_buffer(&mut self, context: &str) -> Result<()> {
        IoWrite::write_all(&mut self.file, &self.header_buffer).map_err(|source_err| {
            err_with_source(path_context_message(context, self.archive_path), source_err)
        })?;
        self.bytes_written =
            self.add_bytes_written(self.header_buffer.len(), "ZIP written byte count 계산")?;
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
