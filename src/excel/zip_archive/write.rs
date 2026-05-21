use super::{
    CENTRAL_DIRECTORY_SIGNATURE, DOS_DATE_1980_01_01, END_OF_CENTRAL_DIRECTORY_LEN,
    END_OF_CENTRAL_DIRECTORY_SIGNATURE, GENERAL_PURPOSE_UTF8_FLAG, LOCAL_FILE_HEADER_LEN,
    LOCAL_FILE_HEADER_SIGNATURE, METHOD_DEFLATE, METHOD_STORE, PendingFile, VERSION_NEEDED,
    collect_files, crc32, deflate,
};
use crate::{Result, err, path_source_message, prefixed_message};
use std::{
    fs::{self, File},
    io::Write as IoWrite,
    path::Path,
};
const CENTRAL_FILE_HEADER_BASE_LEN: usize = 46;
#[derive(Clone)]
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
pub(in crate::excel) struct ZipArchiveBuilder<'path> {
    pub archive_path: &'path Path,
    pub root: &'path Path,
}
struct StreamingZipWriter<'path> {
    archive_path: &'path Path,
    bytes_written: u64,
    entries: Vec<WriteEntry>,
    file: File,
}
impl ZipArchiveBuilder<'_> {
    pub fn create(&self) -> Result<()> {
        let mut files = Vec::new();
        collect_files(self.root, self.root, &mut files)?;
        files.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        let file = File::create(self.archive_path).map_err(|source_err| {
            err(path_source_message(
                "xlsx 압축 파일 생성 실패",
                self.archive_path,
                source_err,
            ))
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
        let central_dir_offset = u32::try_from(self.bytes_written).map_err(|source| {
            err(prefixed_message(
                "ZIP 중앙 디렉터리 offset 변환 실패: ",
                source,
            ))
        })?;
        let Some(central_dir_capacity) =
            self.entries
                .iter()
                .try_fold(END_OF_CENTRAL_DIRECTORY_LEN, |acc, entry| {
                    let len = acc.checked_add(CENTRAL_FILE_HEADER_BASE_LEN)?;
                    len.checked_add(entry.name.len())
                })
        else {
            return Err(err(
                "ZIP 중앙 디렉터리 크기 계산 중 overflow가 발생했습니다.",
            ));
        };
        let mut central_dir = Vec::new();
        central_dir
            .try_reserve(central_dir_capacity)
            .map_err(|source| {
                err(prefixed_message(
                    "ZIP 중앙 디렉터리 메모리 확보 실패: ",
                    source,
                ))
            })?;
        for entry in &self.entries {
            write_file_header(&mut central_dir, ZipFileHeader::Central(entry))?;
        }
        let central_dir_size = u32::try_from(central_dir.len()).map_err(|source| {
            err(prefixed_message(
                "ZIP 중앙 디렉터리 크기 변환 실패: ",
                source,
            ))
        })?;
        let entry_count_u16 = u16::try_from(self.entries.len())
            .map_err(|source| err(prefixed_message("ZIP entry 수 변환 실패: ", source)))?;
        write_u32(&mut central_dir, END_OF_CENTRAL_DIRECTORY_SIGNATURE);
        write_u16(&mut central_dir, 0);
        write_u16(&mut central_dir, 0);
        write_u16(&mut central_dir, entry_count_u16);
        write_u16(&mut central_dir, entry_count_u16);
        write_u32(&mut central_dir, central_dir_size);
        write_u32(&mut central_dir, central_dir_offset);
        write_u16(&mut central_dir, 0);
        self.write_all(&central_dir, "xlsx 압축 중앙 디렉터리 쓰기 실패")
    }
    fn append_file(&mut self, file: PendingFile) -> Result<()> {
        let data = fs::read(&file.path).map_err(|source_err| {
            err(path_source_message(
                "xlsx 파트 읽기 실패",
                &file.path,
                source_err,
            ))
        })?;
        let crc32 = crc32(&data);
        let uncompressed_size = data.len();
        let deflated = deflate::DeflateWriter { bytes: &data }
            .deflate()
            .map_err(err)?;
        let (method, compressed_data) = if deflated.len() < uncompressed_size {
            drop(data);
            (METHOD_DEFLATE, deflated)
        } else {
            (METHOD_STORE, data)
        };
        let local_header_offset = u32::try_from(self.bytes_written)
            .map_err(|source| err(prefixed_message("ZIP offset 변환 실패: ", source)))?;
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
        write_file_header(
            &mut local_header,
            ZipFileHeader::Local {
                crc32,
                compressed_len: compressed_data.len(),
                method,
                name: &file.name,
                uncompressed_len: uncompressed_size,
            },
        )?;
        self.write_all(&local_header, "xlsx 압축 local header 쓰기 실패")?;
        self.write_all(&compressed_data, "xlsx 압축 파일 데이터 쓰기 실패")?;
        self.entries.push(WriteEntry {
            compressed_size: compressed_data.len(),
            crc32,
            local_header_offset,
            method,
            name: file.name,
            uncompressed_size,
        });
        Ok(())
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
            err(path_source_message(context, self.archive_path, source_err))
        })?;
        self.bytes_written = self
            .bytes_written
            .checked_add(u64::try_from(bytes.len()).map_err(|source| {
                err(prefixed_message(
                    "ZIP written byte count 변환 실패: ",
                    source,
                ))
            })?)
            .ok_or_else(|| err("ZIP written byte count 계산 중 overflow가 발생했습니다."))?;
        Ok(())
    }
}
fn write_file_header(out: &mut Vec<u8>, header: ZipFileHeader<'_>) -> Result<()> {
    match header {
        ZipFileHeader::Central(entry) => {
            let name = entry.name.as_bytes();
            let name_len = u16::try_from(name.len()).map_err(|source| {
                err(prefixed_message("ZIP entry 이름 길이 변환 실패: ", source))
            })?;
            let compressed_size = u32::try_from(entry.compressed_size).map_err(|source| {
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
