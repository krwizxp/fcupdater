use super::super::{PackagePart, ZipArchiveBuilder};
use super::{
    DATA_DESCRIPTOR_LEN, DATA_DESCRIPTOR_SIGNATURE, END_OF_CENTRAL_DIRECTORY_LEN,
    END_OF_CENTRAL_DIRECTORY_SIGNATURE, LOCAL_FILE_HEADER_LEN, ZIP_MAX_ARCHIVE_BYTES,
    ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES, crc32, deflate, read_u32,
};
use crate::diagnostic::{Result, err, err_with_source, path_context_message};
use core::mem;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::fs::Permissions;
use std::{
    fs::File,
    io::{BufWriter, Write as IoWrite},
    path::Path,
};
const ZIP_OUTPUT_BUFFER_CAPACITY: usize = 64 * 1024;
struct WriteEntry<'part> {
    compressed_size: u32,
    crc32: u32,
    local_header_offset: u32,
    part: &'part PackagePart,
    uncompressed_size: u32,
}
struct StreamingZipWriter<'part, 'path> {
    archive_path: &'path Path,
    bytes_written: usize,
    deflate_workspace: deflate::DeflateWorkspace,
    entries: Vec<WriteEntry<'part>>,
    file: BufWriter<File>,
    header_buffer: Vec<u8>,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    permissions: Permissions,
    source_bytes: &'part [u8],
}
impl ZipArchiveBuilder<'_, '_> {
    pub(in crate::excel) fn create(self) -> Result<()> {
        StreamingZipWriter {
            archive_path: self.archive_path,
            bytes_written: 0,
            deflate_workspace: deflate::DeflateWorkspace::default(),
            entries: Vec::new(),
            file: BufWriter::with_capacity(ZIP_OUTPUT_BUFFER_CAPACITY, self.file),
            header_buffer: Vec::new(),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            permissions: self.permissions,
            source_bytes: self.source_bytes,
        }
        .write(self.parts)
    }
}
impl<'part> StreamingZipWriter<'part, '_> {
    fn append_central_directory(&mut self) -> Result<()> {
        let central_dir_offset = u32::try_from(self.bytes_written)
            .map_err(|source| err_with_source("ZIP 중앙 디렉터리 offset 변환 실패", source))?;
        let entries = mem::take(&mut self.entries);
        let mut central_dir_size_usize = 0_usize;
        for entry in &entries {
            let record = self
                .source_bytes
                .get(entry.part.central_record)
                .ok_or_else(|| err("ZIP 원본 중앙 디렉터리 범위 오류"))?;
            central_dir_size_usize = central_dir_size_usize
                .checked_add(record.len())
                .ok_or_else(|| err("ZIP 중앙 디렉터리 크기 계산 실패"))?;
        }
        let entry_count_u16 = u16::try_from(entries.len())
            .map_err(|source| err_with_source("ZIP entry 수 변환 실패", source))?;
        let central_output_size = central_dir_size_usize
            .checked_add(END_OF_CENTRAL_DIRECTORY_LEN)
            .ok_or_else(|| err("ZIP 중앙 디렉터리 출력 크기 계산 실패"))?;
        self.ensure_output_limit(central_output_size, "ZIP 중앙 디렉터리 출력 크기 계산")?;
        let central_dir_size = u32::try_from(central_dir_size_usize)
            .map_err(|source| err_with_source("ZIP 중앙 디렉터리 크기 변환 실패", source))?;
        for entry in &entries {
            let central_record = self
                .source_bytes
                .get(entry.part.central_record)
                .ok_or_else(|| err("ZIP 원본 중앙 디렉터리 범위 오류"))?;
            self.prepare_header_buffer(central_record.len(), "ZIP 중앙 디렉터리 메모리 확보 실패")?;
            self.header_buffer.extend_from_slice(central_record);
            replace_u32(&mut self.header_buffer, 16, entry.crc32)?;
            replace_u32(&mut self.header_buffer, 20, entry.compressed_size)?;
            replace_u32(&mut self.header_buffer, 24, entry.uncompressed_size)?;
            replace_u32(&mut self.header_buffer, 42, entry.local_header_offset)?;
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
    fn append_changed(&mut self, part: &'part PackagePart) -> Result<WriteEntry<'part>> {
        let uncompressed_size = part.bytes.len();
        if uncompressed_size > ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES {
            return Err(err(format!(
                "xlsx part 크기가 허용 한도({ZIP_MAX_ENTRY_UNCOMPRESSED_BYTES} bytes)를 초과했습니다: {} -> {}",
                self.archive_path.display(),
                part.name
            )));
        }
        let Some(plan) = (deflate::DeflateWriter {
            bytes: &part.bytes,
            workspace: &mut self.deflate_workspace,
        })
        .plan()?
        else {
            return Err(err(format!(
                "고정 XLSX part 압축 작업 한도를 초과했습니다: {}",
                part.name
            )));
        };
        let compressed_size = plan.len();
        let compressed_size_u32 = u32::try_from(compressed_size)
            .map_err(|source| err_with_source("ZIP entry 압축 크기 변환 실패", source))?;
        let uncompressed_size_u32 = u32::try_from(uncompressed_size)
            .map_err(|source| err_with_source("ZIP entry 원본 크기 변환 실패", source))?;
        let crc32 = crc32(&part.bytes)?;
        let local_header_offset = u32::try_from(self.bytes_written)
            .map_err(|source| err_with_source("ZIP offset 변환 실패", source))?;
        let local_header_len = LOCAL_FILE_HEADER_LEN
            .checked_add(part.name.len())
            .ok_or_else(|| err("ZIP local header 길이 계산 실패"))?;
        let entry_output_size = local_header_len
            .checked_add(compressed_size)
            .and_then(|size| size.checked_add(DATA_DESCRIPTOR_LEN))
            .ok_or_else(|| err("ZIP entry 출력 크기 계산 실패"))?;
        self.ensure_output_limit(entry_output_size, "ZIP entry 출력 크기 계산")?;
        let source_local = self
            .source_bytes
            .get(part.local_record)
            .and_then(|record| record.get(..local_header_len))
            .ok_or_else(|| err("ZIP 원본 local header 범위 오류"))?;
        self.prepare_header_buffer(local_header_len, "ZIP local header 메모리 확보 실패")?;
        self.header_buffer.extend_from_slice(source_local);
        self.write_header_buffer("xlsx 압축 local header 쓰기 실패")?;
        let actual_written = plan.write_to(&mut self.file)?;
        if actual_written != compressed_size {
            return Err(err(format!(
                "ZIP deflate 출력 크기가 계획과 다릅니다: expected={compressed_size}, actual={actual_written}"
            )));
        }
        self.bytes_written =
            self.ensure_output_limit(compressed_size, "ZIP 압축 데이터 출력 크기 계산")?;
        self.prepare_header_buffer(DATA_DESCRIPTOR_LEN, "ZIP data descriptor 메모리 확보 실패")?;
        write_u32(&mut self.header_buffer, DATA_DESCRIPTOR_SIGNATURE);
        write_u32(&mut self.header_buffer, crc32);
        write_u32(&mut self.header_buffer, compressed_size_u32);
        write_u32(&mut self.header_buffer, uncompressed_size_u32);
        self.write_header_buffer("xlsx 압축 data descriptor 쓰기 실패")?;
        self.deflate_workspace.recycle(plan);
        Ok(WriteEntry {
            compressed_size: compressed_size_u32,
            crc32,
            local_header_offset,
            part,
            uncompressed_size: uncompressed_size_u32,
        })
    }
    fn append_file(&mut self, part: &'part PackagePart) -> Result<()> {
        let entry = if part.changed {
            self.append_changed(part)?
        } else {
            let central_record = self
                .source_bytes
                .get(part.central_record)
                .ok_or_else(|| err("ZIP 원본 중앙 디렉터리 범위 오류"))?;
            let entry = WriteEntry {
                compressed_size: read_u32(central_record, 20)?,
                crc32: read_u32(central_record, 16)?,
                local_header_offset: u32::try_from(self.bytes_written)
                    .map_err(|source| err_with_source("ZIP offset 변환 실패", source))?,
                part,
                uncompressed_size: read_u32(central_record, 24)?,
            };
            let local_record = self
                .source_bytes
                .get(part.local_record)
                .ok_or_else(|| err("ZIP 원본 local record 범위 오류"))?;
            let next =
                self.ensure_output_limit(local_record.len(), "ZIP local record 출력 크기 계산")?;
            IoWrite::write_all(&mut self.file, local_record).map_err(|source_err| {
                err_with_source(
                    path_context_message("xlsx 압축 원본 record 쓰기 실패", self.archive_path),
                    source_err,
                )
            })?;
            self.bytes_written = next;
            entry
        };
        self.entries.push(entry);
        Ok(())
    }
    fn ensure_output_limit(&self, len: usize, context: &str) -> Result<usize> {
        let next = self
            .bytes_written
            .checked_add(len)
            .ok_or_else(|| err(format!("{context} 중 overflow가 발생했습니다.")))?;
        if next > ZIP_MAX_ARCHIVE_BYTES {
            return Err(err(format!(
                "xlsx 압축 출력 크기가 허용 한도({ZIP_MAX_ARCHIVE_BYTES} bytes)를 초과합니다: {}",
                self.archive_path.display()
            )));
        }
        Ok(next)
    }
    fn prepare_header_buffer(&mut self, capacity: usize, context: &'static str) -> Result<()> {
        self.header_buffer.clear();
        self.header_buffer
            .try_reserve_exact(capacity)
            .map_err(|source| err_with_source(context, source))
    }
    fn write(mut self, parts: &'part [PackagePart]) -> Result<()> {
        self.entries
            .try_reserve_exact(parts.len())
            .map_err(|source| err_with_source("ZIP entry 목록 메모리 확보 실패", source))?;
        for part in parts {
            self.append_file(part)?;
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
        let next_bytes_written =
            self.ensure_output_limit(self.header_buffer.len(), "ZIP written byte count 계산")?;
        IoWrite::write_all(&mut self.file, &self.header_buffer).map_err(|source_err| {
            err_with_source(path_context_message(context, self.archive_path), source_err)
        })?;
        self.bytes_written = next_bytes_written;
        Ok(())
    }
}
fn replace_u32(out: &mut [u8], offset: usize, value: u32) -> Result<()> {
    let Some(target) = out
        .get_mut(offset..)
        .and_then(|tail| tail.first_chunk_mut::<4>())
    else {
        return Err(err("ZIP 중앙 디렉터리 필드 범위 오류"));
    };
    *target = value.to_le_bytes();
    Ok(())
}
fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}
fn write_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}
