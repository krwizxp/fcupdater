use super::super::{MAX_XLSX_PART_BYTES, PackagePart, ZipArchiveBuilder};
use super::{
    CENTRAL_DIRECTORY_HEADER_LEN, CENTRAL_DIRECTORY_SIGNATURE, END_OF_CENTRAL_DIRECTORY_LEN,
    END_OF_CENTRAL_DIRECTORY_SIGNATURE, EXCEL_ENTRY_FLAGS, LOCAL_FILE_HEADER_LEN,
    LOCAL_FILE_HEADER_SIGNATURE, METHOD_DEFLATE, VERSION_MADE_BY, VERSION_NEEDED,
    ZIP_MAX_ARCHIVE_BYTES, deflate,
};
use crate::diagnostic::{
    Result, bail_fmt, ensure_fmt, err_with_source, fmt_err, map_reserve, path_err_with_source,
    try_vec_with_capacity,
};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::fs::Permissions;
use std::{
    fs::File,
    io::{BufWriter, Write as IoWrite},
    path::Path,
};
struct StreamingZipWriter<'path> {
    archive_path: &'path Path,
    bytes_written: usize,
    deflate_workspace: deflate::DeflateWorkspace,
    file: BufWriter<&'path mut File>,
    header_buffer: Vec<u8>,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    permissions: Permissions,
}
impl StreamingZipWriter<'_> {
    fn ensure_output_limit(&self, len: usize, context: &str) -> Result<usize> {
        let next = self
            .bytes_written
            .checked_add(len)
            .ok_or_else(|| fmt_err!("{context} 중 overflow가 발생했습니다."))?;
        ensure_fmt!(
            next <= ZIP_MAX_ARCHIVE_BYTES,
            "xlsx 압축 출력 크기가 허용 한도({ZIP_MAX_ARCHIVE_BYTES} bytes)를 초과합니다: {}",
            self.archive_path.display()
        );
        Ok(next)
    }
    fn prepare_header_buffer(&mut self, capacity: usize, context: &'static str) -> Result<()> {
        self.header_buffer.clear();
        map_reserve(self.header_buffer.try_reserve_exact(capacity), context)
    }
    fn write(mut self, parts: &[PackagePart]) -> Result<()> {
        let mut entries = try_vec_with_capacity(parts.len(), "ZIP entry 목록 메모리 확보 실패")?;
        for part in parts {
            let uncompressed_size = part.bytes.len();
            ensure_fmt!(
                uncompressed_size <= MAX_XLSX_PART_BYTES,
                "xlsx part 크기가 허용 한도({MAX_XLSX_PART_BYTES} bytes)를 초과했습니다: {} -> {}",
                self.archive_path.display(),
                part.name
            );
            let Some(plan) = (deflate::DeflateWriter {
                bytes: &part.bytes,
                workspace: &mut self.deflate_workspace,
            })
            .plan()?
            else {
                bail_fmt!(
                    "고정 XLSX part 압축 작업 한도를 초과했습니다: {}",
                    part.name
                );
            };
            let compressed_size = plan.len();
            let compressed_size_u32 = u32::try_from(compressed_size)
                .map_err(|source| err_with_source("ZIP entry 압축 크기 변환 실패", source))?;
            let uncompressed_size_u32 = u32::try_from(uncompressed_size)
                .map_err(|source| err_with_source("ZIP entry 원본 크기 변환 실패", source))?;
            let crc32 = plan.crc32();
            let local_header_offset = u32::try_from(self.bytes_written)
                .map_err(|source| err_with_source("ZIP offset 변환 실패", source))?;
            let local_header_len = LOCAL_FILE_HEADER_LEN
                .checked_add(part.name.len())
                .ok_or("ZIP local header 길이 계산 실패")?;
            let entry_output_size = local_header_len
                .checked_add(compressed_size)
                .ok_or("ZIP entry 출력 크기 계산 실패")?;
            self.ensure_output_limit(entry_output_size, "ZIP entry 출력 크기 계산")?;
            self.prepare_header_buffer(local_header_len, "ZIP local header 메모리 확보 실패")?;
            self.header_buffer
                .extend_from_slice(&LOCAL_FILE_HEADER_SIGNATURE.to_le_bytes());
            append_common_zip_fields(
                &mut self.header_buffer,
                crc32,
                compressed_size_u32,
                uncompressed_size_u32,
                part.name.len(),
            )?;
            self.header_buffer.extend_from_slice(part.name.as_bytes());
            self.write_header_buffer("xlsx 압축 local header 쓰기 실패")?;
            let actual_written = plan.write_to(&mut self.file)?;
            ensure_fmt!(
                actual_written == compressed_size,
                "ZIP deflate 출력 크기가 계획과 다릅니다: expected={compressed_size}, actual={actual_written}"
            );
            self.bytes_written = self.bytes_written.strict_add(compressed_size);
            self.deflate_workspace.recycle(plan);
            entries.push((
                compressed_size_u32,
                crc32,
                local_header_offset,
                part,
                uncompressed_size_u32,
            ));
        }
        let central_dir_offset = u32::try_from(self.bytes_written)
            .map_err(|source| err_with_source("ZIP 중앙 디렉터리 offset 변환 실패", source))?;
        let central_dir_size_usize = entries
            .iter()
            .map(|entry| CENTRAL_DIRECTORY_HEADER_LEN.saturating_add(entry.3.name.len()))
            .try_fold(0_usize, usize::checked_add)
            .ok_or("ZIP 중앙 디렉터리 크기 계산 실패")?;
        let entry_count_u16 = u16::try_from(entries.len())
            .map_err(|source| err_with_source("ZIP entry 수 변환 실패", source))?;
        self.ensure_output_limit(
            central_dir_size_usize
                .checked_add(END_OF_CENTRAL_DIRECTORY_LEN)
                .ok_or("ZIP 중앙 디렉터리 출력 크기 계산 실패")?,
            "ZIP 중앙 디렉터리 출력 크기 계산",
        )?;
        let central_dir_size = u32::try_from(central_dir_size_usize)
            .map_err(|source| err_with_source("ZIP 중앙 디렉터리 크기 변환 실패", source))?;
        for (compressed_size, crc32, local_header_offset, part, uncompressed_size) in entries {
            let record_len = CENTRAL_DIRECTORY_HEADER_LEN
                .checked_add(part.name.len())
                .ok_or("ZIP 중앙 디렉터리 entry 길이 계산 실패")?;
            self.prepare_header_buffer(record_len, "ZIP 중앙 디렉터리 메모리 확보 실패")?;
            self.header_buffer
                .extend_from_slice(&CENTRAL_DIRECTORY_SIGNATURE.to_le_bytes());
            self.header_buffer
                .extend_from_slice(&VERSION_MADE_BY.to_le_bytes());
            append_common_zip_fields(
                &mut self.header_buffer,
                crc32,
                compressed_size,
                uncompressed_size,
                part.name.len(),
            )?;
            self.header_buffer.extend_from_slice(&[0_u8; 10]);
            self.header_buffer
                .extend_from_slice(&local_header_offset.to_le_bytes());
            self.header_buffer.extend_from_slice(part.name.as_bytes());
            self.write_header_buffer("xlsx 압축 중앙 디렉터리 쓰기 실패")?;
        }
        self.prepare_header_buffer(
            END_OF_CENTRAL_DIRECTORY_LEN,
            "ZIP 중앙 디렉터리 footer 메모리 확보 실패",
        )?;
        for chunk in [
            END_OF_CENTRAL_DIRECTORY_SIGNATURE.to_le_bytes().as_slice(),
            &0_u32.to_le_bytes(),
            &entry_count_u16.to_le_bytes(),
            &entry_count_u16.to_le_bytes(),
            &central_dir_size.to_le_bytes(),
            &central_dir_offset.to_le_bytes(),
            &0_u16.to_le_bytes(),
        ] {
            self.header_buffer.extend_from_slice(chunk);
        }
        self.write_header_buffer("xlsx 압축 중앙 디렉터리 쓰기 실패")?;
        IoWrite::flush(&mut self.file).map_err(|source| {
            path_err_with_source("xlsx 압축 파일 flush 실패", self.archive_path, source)
        })?;
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        self.file
            .get_ref()
            .set_permissions(self.permissions)
            .map_err(|source| {
                path_err_with_source("xlsx 압축 파일 권한 적용 실패", self.archive_path, source)
            })?;
        self.file.get_ref().sync_all().map_err(|source| {
            path_err_with_source("xlsx 압축 파일 sync 실패", self.archive_path, source)
        })
    }
    fn write_header_buffer(&mut self, context: &str) -> Result<()> {
        let next_bytes_written =
            self.ensure_output_limit(self.header_buffer.len(), "ZIP written byte count 계산")?;
        IoWrite::write_all(&mut self.file, &self.header_buffer)
            .map_err(|source| path_err_with_source(context, self.archive_path, source))?;
        self.bytes_written = next_bytes_written;
        Ok(())
    }
}
impl ZipArchiveBuilder<'_, '_> {
    pub(in crate::excel) fn create(self) -> Result<()> {
        StreamingZipWriter {
            archive_path: self.archive_path,
            bytes_written: 0,
            deflate_workspace: deflate::DeflateWorkspace::default(),
            file: BufWriter::with_capacity(64 * 1024, self.file),
            header_buffer: Vec::new(),
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            permissions: self.permissions,
        }
        .write(self.parts)
    }
}
fn append_common_zip_fields(
    out: &mut Vec<u8>,
    crc32: u32,
    compressed_size: u32,
    uncompressed_size: u32,
    name_len: usize,
) -> Result<()> {
    let name_len_u16 = u16::try_from(name_len)
        .map_err(|source| err_with_source("ZIP entry 이름 길이 변환 실패", source))?;
    for chunk in [
        VERSION_NEEDED.to_le_bytes().as_slice(),
        &EXCEL_ENTRY_FLAGS.to_le_bytes(),
        &METHOD_DEFLATE.to_le_bytes(),
        &[0, 0, 0x21, 0],
        &crc32.to_le_bytes(),
        &compressed_size.to_le_bytes(),
        &uncompressed_size.to_le_bytes(),
        &name_len_u16.to_le_bytes(),
        &0_u16.to_le_bytes(),
    ] {
        out.extend_from_slice(chunk);
    }
    Ok(())
}
