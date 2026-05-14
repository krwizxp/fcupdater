use crate::{Result, err, path_pair_source_message, path_source_message, prefixed_message};
use alloc::{string::String, vec::Vec};
use core::{array::from_fn, cmp::Ordering, iter::repeat_n, result::Result as StdResult, str};
use std::{
    fs,
    io::Write as _,
    path::{Component, Path, PathBuf},
};
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
type ZipResult<T> = StdResult<T, String>;
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
struct BitReader<'a> {
    bit_buffer: u32,
    bit_count: u8,
    bytes: &'a [u8],
    cursor: usize,
}
struct Huffman {
    codes: [Vec<HuffmanCode>; DEFLATE_MAX_BITS + 1],
}
struct WriteHuffman {
    codes: Vec<u16>,
    lengths: Vec<u8>,
}
struct BitWriter {
    bit_buffer: u8,
    bit_count: u8,
    bytes: Vec<u8>,
}
#[derive(Clone, Copy)]
enum DeflateToken {
    Literal(u8),
    Match { distance: usize, length: usize },
}
#[derive(Clone)]
struct HuffmanCode {
    code: u16,
    symbol: u16,
}
#[derive(Clone, Copy)]
struct CodeLengthToken {
    extra: u16,
    extra_bits: u8,
    symbol: u8,
}
struct HuffmanBuildNode {
    freq: u64,
    parent: Option<usize>,
}
pub(super) struct ZipArchiveOps;
pub(super) trait ZipArchiveOpsExt {
    fn create_from_directory(&self, root: &Path, archive_path: &Path) -> Result<()>;
    fn extract_to_directory(&self, archive_path: &Path, unpack_dir: &Path) -> Result<()>;
    fn list_entries(&self, archive_path: &Path) -> Result<Vec<String>>;
}
struct DeflateInflater;
struct DeflateWriter;
trait DeflateInflaterExt {
    fn copy_previous(
        output: &mut Vec<u8>,
        distance: usize,
        length: usize,
        expected_len: usize,
    ) -> ZipResult<()>;
    fn decode_distance(symbol: u16, reader: &mut BitReader<'_>) -> ZipResult<usize>;
    fn decode_length(symbol: u16, reader: &mut BitReader<'_>) -> ZipResult<usize>;
    fn dynamic_trees(reader: &mut BitReader<'_>) -> ZipResult<(Huffman, Option<Huffman>)>;
    fn fixed_trees() -> ZipResult<(Huffman, Huffman)>;
    fn inflate(bytes: &[u8], expected_len: usize) -> ZipResult<Vec<u8>>;
    fn inflate_compressed_block(
        reader: &mut BitReader<'_>,
        literal_tree: &Huffman,
        distance_tree: Option<&Huffman>,
        output: &mut Vec<u8>,
        expected_len: usize,
    ) -> ZipResult<()>;
    fn inflate_stored_block(
        reader: &mut BitReader<'_>,
        output: &mut Vec<u8>,
        expected_len: usize,
    ) -> ZipResult<()>;
}
trait DeflateWriterExt {
    fn best_match(
        bytes: &[u8],
        position: usize,
        head: &[usize],
        previous: &[usize],
    ) -> Option<(usize, usize)>;
    fn code_length_tokens(lengths: &[u8]) -> ZipResult<Vec<CodeLengthToken>>;
    fn deflate(bytes: &[u8]) -> ZipResult<Vec<u8>>;
    fn deflate_dynamic(tokens: &[DeflateToken]) -> ZipResult<Option<Vec<u8>>>;
    fn deflate_fixed(tokens: &[DeflateToken], byte_len: usize) -> ZipResult<Vec<u8>>;
    fn distance_symbol(distance: usize) -> Option<(u16, u8, u16)>;
    fn dynamic_frequencies(
        tokens: &[DeflateToken],
    ) -> ZipResult<Option<([u32; LITERAL_LENGTH_SYMBOLS], [u32; DISTANCE_SYMBOLS])>>;
    fn huffman_lengths(frequencies: &[u32], max_bits: u8) -> Option<Vec<u8>>;
    fn insert_position(bytes: &[u8], position: usize, head: &mut [usize], previous: &mut [usize]);
    fn length_symbol(length: usize) -> Option<(u16, u8, u16)>;
    fn tokens(bytes: &[u8]) -> ZipResult<Vec<DeflateToken>>;
    fn write_distance(writer: &mut BitWriter, distance: usize) -> ZipResult<()>;
    fn write_dynamic_token(
        writer: &mut BitWriter,
        token: DeflateToken,
        literal_huffman: &WriteHuffman,
        distance_huffman: &WriteHuffman,
    ) -> ZipResult<()>;
    fn write_fixed_symbol(writer: &mut BitWriter, symbol: u16) -> ZipResult<()>;
    fn write_fixed_token(writer: &mut BitWriter, token: DeflateToken) -> ZipResult<()>;
    fn write_length(writer: &mut BitWriter, length: usize) -> ZipResult<()>;
}
impl ZipArchiveOpsExt for ZipArchiveOps {
    fn create_from_directory(&self, root: &Path, archive_path: &Path) -> Result<()> {
        let mut files = Vec::new();
        collect_files(root, root, &mut files)?;
        files.sort_unstable_by(|left, right| left.name.cmp(&right.name));
        let mut archive = Vec::new();
        let mut entries = Vec::with_capacity(files.len());
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
            let deflated = DeflateWriter::deflate(&data).map_err(err)?;
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
                .ok_or_else(|| err(String::from("ZIP 전체 해제 크기 계산 실패")))?;
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
                    return Err(zip_entry_message(ZIP_BAD_LOCAL_HEADER_MESSAGE, entry_name));
                }
                let name_len = usize::from(read_u16(&bytes, local_offset.saturating_add(26))?);
                let extra_len = usize::from(read_u16(&bytes, local_offset.saturating_add(28))?);
                let data_start = local_offset
                    .checked_add(LOCAL_FILE_HEADER_LEN)
                    .and_then(|value| value.checked_add(name_len))
                    .and_then(|value| value.checked_add(extra_len))
                    .ok_or_else(|| String::from("ZIP data offset 계산 실패"))?;
                let compressed_len = usize::try_from(entry.compressed_size)
                    .map_err(|source| format!("ZIP 압축 크기 변환 실패: {source}"))?;
                let data_end = data_start
                    .checked_add(compressed_len)
                    .ok_or_else(|| String::from("ZIP data end 계산 실패"))?;
                let Some(compressed) = bytes.get(data_start..data_end) else {
                    return Err(zip_entry_message(ZIP_DATA_RANGE_MESSAGE, entry_name));
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
                        <DeflateInflater as DeflateInflaterExt>::inflate(compressed, expected_len)?
                    }
                    method => {
                        return Err(format!(
                            "지원하지 않는 ZIP 압축 방식({method}): {}",
                            entry.name
                        ));
                    }
                };
                if output.len() != expected_len {
                    return Err(zip_entry_message(ZIP_BAD_SIZE_MESSAGE, entry_name));
                }
                let actual_crc = crc32(&output);
                if actual_crc != entry.crc32 {
                    return Err(zip_entry_message(ZIP_BAD_CRC_MESSAGE, entry_name));
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
        parse_entries(&bytes)
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| entry.name)
                    .collect::<Vec<_>>()
            })
            .map_err(err)
    }
}
impl BitReader<'_> {
    const fn align_to_byte(&mut self) {
        self.bit_buffer = 0;
        self.bit_count = 0;
    }
    fn read_bits(&mut self, count: u8) -> ZipResult<u32> {
        while self.bit_count < count {
            let Some(&byte) = self.bytes.get(self.cursor) else {
                return Err(String::from("deflate bitstream이 예기치 않게 끝났습니다."));
            };
            self.bit_buffer |= u32::from(byte) << u32::from(self.bit_count);
            self.cursor = self.cursor.saturating_add(1);
            self.bit_count = self.bit_count.saturating_add(8);
        }
        let mask = if count == 32 {
            u32::MAX
        } else {
            (1_u32 << u32::from(count)).saturating_sub(1)
        };
        let value = self.bit_buffer & mask;
        self.bit_buffer >>= u32::from(count);
        self.bit_count = self.bit_count.saturating_sub(count);
        Ok(value)
    }
    fn read_stored_bytes(&mut self, len: usize) -> ZipResult<&[u8]> {
        self.align_to_byte();
        let end = self
            .cursor
            .checked_add(len)
            .ok_or_else(|| String::from("deflate 저장 블록 크기 계산 실패"))?;
        let Some(bytes) = self.bytes.get(self.cursor..end) else {
            return Err(String::from("deflate 저장 블록이 입력보다 깁니다."));
        };
        self.cursor = end;
        Ok(bytes)
    }
}
impl BitWriter {
    fn finish(mut self) -> Vec<u8> {
        if self.bit_count > 0 {
            self.bytes.push(self.bit_buffer);
        }
        self.bytes
    }
    fn write_bits(&mut self, mut value: u16, count: u8) {
        for _ in 0_u8..count {
            if value & 1_u16 != 0 {
                self.bit_buffer |= 1_u8 << self.bit_count;
            }
            value >>= 1_u8;
            self.bit_count = self.bit_count.saturating_add(1);
            if self.bit_count == 8 {
                self.bytes.push(self.bit_buffer);
                self.bit_buffer = 0;
                self.bit_count = 0;
            }
        }
    }
}
impl Huffman {
    fn decode(&self, reader: &mut BitReader<'_>) -> ZipResult<u16> {
        let mut code = 0_u16;
        for bit_len in 1..=DEFLATE_MAX_BITS {
            let bit = u16::try_from(reader.read_bits(1)?)
                .map_err(|source| format!("deflate bit 변환 실패: {source}"))?;
            let shift = u32::try_from(bit_len.saturating_sub(1))
                .map_err(|source| format!("deflate bit 길이 변환 실패: {source}"))?;
            code |= bit << shift;
            for candidate in self.codes.get(bit_len).into_iter().flatten() {
                if candidate.code == code {
                    return Ok(candidate.symbol);
                }
            }
        }
        Err(String::from("deflate Huffman code를 해석하지 못했습니다."))
    }
    fn from_lengths(lengths: &[u8]) -> ZipResult<Option<Self>> {
        let mut bl_count = [0_u16; DEFLATE_MAX_BITS + 1];
        for &len in lengths {
            if len == 0 {
                continue;
            }
            let len_index = usize::from(len);
            if len_index > DEFLATE_MAX_BITS {
                return Err(String::from("deflate Huffman code 길이가 너무 깁니다."));
            }
            let Some(count) = bl_count.get_mut(len_index) else {
                return Err(String::from("deflate Huffman count 범위 오류"));
            };
            *count = count.saturating_add(1);
        }
        if bl_count.iter().skip(1).all(|count| *count == 0) {
            return Ok(None);
        }
        let mut next_code = [0_u16; DEFLATE_MAX_BITS + 1];
        let mut code = 0_u16;
        for bits in 1..=DEFLATE_MAX_BITS {
            let previous = bits.saturating_sub(1);
            let Some(&previous_count) = bl_count.get(previous) else {
                return Err(String::from("deflate Huffman count 범위 오류"));
            };
            code = code.saturating_add(previous_count) << 1_u8;
            let Some(next_slot) = next_code.get_mut(bits) else {
                return Err(String::from("deflate Huffman next code 범위 오류"));
            };
            *next_slot = code;
        }
        let mut codes: [Vec<HuffmanCode>; DEFLATE_MAX_BITS + 1] = from_fn(|_| Vec::new());
        for (symbol, &len) in lengths.iter().enumerate() {
            if len == 0 {
                continue;
            }
            let len_index = usize::from(len);
            let Some(next_slot) = next_code.get_mut(len_index) else {
                return Err(String::from("deflate Huffman next code 범위 오류"));
            };
            let assigned = *next_slot;
            *next_slot = next_slot.saturating_add(1);
            let symbol_u16 = u16::try_from(symbol)
                .map_err(|source| format!("deflate symbol 변환 실패: {source}"))?;
            let Some(code_bucket) = codes.get_mut(len_index) else {
                return Err(String::from("deflate Huffman code bucket 범위 오류"));
            };
            let mut assigned_code = assigned;
            let mut reversed = 0_u16;
            for _ in 0_u8..len {
                reversed = (reversed << 1_u8) | (assigned_code & 1_u16);
                assigned_code >>= 1_u8;
            }
            code_bucket.push(HuffmanCode {
                code: reversed,
                symbol: symbol_u16,
            });
        }
        Ok(Some(Self { codes }))
    }
}
impl WriteHuffman {
    fn from_lengths(lengths: Vec<u8>) -> ZipResult<Self> {
        let mut bl_count = [0_u16; DEFLATE_MAX_BITS + 1];
        for &len in &lengths {
            if len == 0 {
                continue;
            }
            let Some(count) = bl_count.get_mut(usize::from(len)) else {
                return Err(String::from("deflate 출력 Huffman 길이 범위 오류"));
            };
            *count = count.saturating_add(1);
        }
        let mut next_code = [0_u16; DEFLATE_MAX_BITS + 1];
        let mut code = 0_u16;
        for bits in 1..=DEFLATE_MAX_BITS {
            let previous = bits.saturating_sub(1);
            let Some(&previous_count) = bl_count.get(previous) else {
                return Err(String::from("deflate 출력 Huffman count 범위 오류"));
            };
            code = code.saturating_add(previous_count) << 1_u8;
            let Some(next_slot) = next_code.get_mut(bits) else {
                return Err(String::from("deflate 출력 Huffman next code 범위 오류"));
            };
            *next_slot = code;
        }
        let mut codes = vec![0_u16; lengths.len()];
        for (symbol, &len) in lengths.iter().enumerate() {
            if len == 0 {
                continue;
            }
            let Some(next_slot) = next_code.get_mut(usize::from(len)) else {
                return Err(String::from("deflate 출력 Huffman code 범위 오류"));
            };
            let Some(code_slot) = codes.get_mut(symbol) else {
                return Err(String::from("deflate 출력 Huffman symbol 범위 오류"));
            };
            *code_slot = reverse_bits(*next_slot, len);
            *next_slot = next_slot.saturating_add(1);
        }
        Ok(Self { codes, lengths })
    }
    fn write_symbol(&self, writer: &mut BitWriter, symbol: u16) -> ZipResult<()> {
        let index = usize::from(symbol);
        let Some(&len) = self.lengths.get(index) else {
            return Err(String::from("deflate 출력 Huffman symbol 길이 범위 오류"));
        };
        if len == 0 {
            return Err(String::from("deflate 출력 Huffman symbol code가 없습니다."));
        }
        let Some(&code) = self.codes.get(index) else {
            return Err(String::from("deflate 출력 Huffman symbol code 범위 오류"));
        };
        writer.write_bits(code, len);
        Ok(())
    }
}
#[derive(Clone)]
struct PendingFile {
    name: String,
    path: PathBuf,
}
impl DeflateInflaterExt for DeflateInflater {
    fn copy_previous(
        output: &mut Vec<u8>,
        distance: usize,
        length: usize,
        expected_len: usize,
    ) -> ZipResult<()> {
        if distance == 0 || distance > output.len() {
            return Err(String::from(
                "deflate back-reference distance가 올바르지 않습니다.",
            ));
        }
        reserve_deflate_output(output, length, expected_len)?;
        for _ in 0..length {
            let source_index = output
                .len()
                .checked_sub(distance)
                .ok_or_else(|| String::from("deflate back-reference index 계산 실패"))?;
            let Some(&byte) = output.get(source_index) else {
                return Err(String::from("deflate back-reference 범위 오류"));
            };
            output.push(byte);
        }
        Ok(())
    }
    fn decode_distance(symbol: u16, reader: &mut BitReader<'_>) -> ZipResult<usize> {
        let index = usize::from(symbol);
        let Some((&base, &extra_bits)) = DISTANCE_BASES
            .get(index)
            .zip(DISTANCE_EXTRA_BITS.get(index))
        else {
            return Err(String::from("deflate distance symbol 범위 오류"));
        };
        let extra = if extra_bits == 0 {
            0
        } else {
            usize::try_from(reader.read_bits(extra_bits)?)
                .map_err(|source| format!("deflate distance extra 변환 실패: {source}"))?
        };
        base.checked_add(extra)
            .ok_or_else(|| String::from("deflate distance 계산 실패"))
    }
    fn decode_length(symbol: u16, reader: &mut BitReader<'_>) -> ZipResult<usize> {
        let index = usize::from(symbol.saturating_sub(257));
        let Some((&base, &extra_bits)) = LENGTH_BASES.get(index).zip(LENGTH_EXTRA_BITS.get(index))
        else {
            return Err(String::from("deflate length symbol 범위 오류"));
        };
        let extra = if extra_bits == 0 {
            0
        } else {
            usize::try_from(reader.read_bits(extra_bits)?)
                .map_err(|source| format!("deflate length extra 변환 실패: {source}"))?
        };
        base.checked_add(extra)
            .ok_or_else(|| String::from("deflate length 계산 실패"))
    }
    fn dynamic_trees(reader: &mut BitReader<'_>) -> ZipResult<(Huffman, Option<Huffman>)> {
        let literal_count = usize::try_from(reader.read_bits(5)?)
            .map_err(|source| format!("deflate HLIT 변환 실패: {source}"))?
            .saturating_add(257);
        let distance_count = usize::try_from(reader.read_bits(5)?)
            .map_err(|source| format!("deflate HDIST 변환 실패: {source}"))?
            .saturating_add(1);
        let code_length_count = usize::try_from(reader.read_bits(4)?)
            .map_err(|source| format!("deflate HCLEN 변환 실패: {source}"))?
            .saturating_add(4);
        let mut code_lengths = [0_u8; 19];
        for &symbol in CODE_LENGTH_ORDER.iter().take(code_length_count) {
            let Some(slot) = code_lengths.get_mut(symbol) else {
                return Err(String::from("deflate code length symbol 범위 오류"));
            };
            *slot = u8::try_from(reader.read_bits(3)?)
                .map_err(|source| format!("deflate code length 변환 실패: {source}"))?;
        }
        let code_tree = Huffman::from_lengths(&code_lengths)?
            .ok_or_else(|| String::from("deflate code length tree가 비어 있습니다."))?;
        let total = literal_count
            .checked_add(distance_count)
            .ok_or_else(|| String::from("deflate code length 총합 계산 실패"))?;
        let mut lengths = Vec::with_capacity(total);
        while lengths.len() < total {
            let symbol = code_tree.decode(reader)?;
            match symbol {
                0..=15 => {
                    lengths.push(u8::try_from(symbol).map_err(|source| {
                        format!("deflate code length symbol 변환 실패: {source}")
                    })?);
                }
                16 => {
                    let Some(&previous) = lengths.last() else {
                        return Err(String::from("deflate repeat code에 이전 길이가 없습니다."));
                    };
                    let repeat = usize::try_from(reader.read_bits(2)?)
                        .map_err(|source| format!("deflate repeat 변환 실패: {source}"))?
                        .saturating_add(3);
                    push_repeated(&mut lengths, previous, repeat, total)?;
                }
                17 => {
                    let repeat = usize::try_from(reader.read_bits(3)?)
                        .map_err(|source| format!("deflate zero repeat 변환 실패: {source}"))?
                        .saturating_add(3);
                    push_repeated(&mut lengths, 0, repeat, total)?;
                }
                18 => {
                    let repeat = usize::try_from(reader.read_bits(7)?)
                        .map_err(|source| format!("deflate long zero repeat 변환 실패: {source}"))?
                        .saturating_add(11);
                    push_repeated(&mut lengths, 0, repeat, total)?;
                }
                _ => {
                    return Err(String::from(
                        "deflate code length symbol이 올바르지 않습니다.",
                    ));
                }
            }
        }
        let literal_lengths = lengths
            .get(..literal_count)
            .ok_or_else(|| String::from("deflate literal length 범위 오류"))?;
        let distance_lengths = lengths
            .get(literal_count..)
            .ok_or_else(|| String::from("deflate distance length 범위 오류"))?;
        let literal = Huffman::from_lengths(literal_lengths)?
            .ok_or_else(|| String::from("deflate literal Huffman tree가 비어 있습니다."))?;
        let distance = Huffman::from_lengths(distance_lengths)?;
        Ok((literal, distance))
    }
    fn fixed_trees() -> ZipResult<(Huffman, Huffman)> {
        let literal_lengths: [u8; FIXED_LITERAL_SYMBOLS] = from_fn(|symbol| match symbol {
            0..=143 | 280..=287 => 8,
            144..=255 => 9,
            256..=279 => 7,
            _ => 0,
        });
        let distance_lengths = [5_u8; FIXED_DISTANCE_SYMBOLS];
        let literal = Huffman::from_lengths(&literal_lengths)?
            .ok_or_else(|| String::from("fixed literal Huffman tree 생성 실패"))?;
        let distance = Huffman::from_lengths(&distance_lengths)?
            .ok_or_else(|| String::from("fixed distance Huffman tree 생성 실패"))?;
        Ok((literal, distance))
    }
    fn inflate(bytes: &[u8], expected_len: usize) -> ZipResult<Vec<u8>> {
        let mut reader = BitReader {
            bit_buffer: 0,
            bit_count: 0,
            bytes,
            cursor: 0,
        };
        let mut output = Vec::new();
        loop {
            let final_block = reader.read_bits(1)? != 0;
            let block_type = reader.read_bits(2)?;
            match block_type {
                0 => Self::inflate_stored_block(&mut reader, &mut output, expected_len)?,
                1 => {
                    let (literal, distance) = Self::fixed_trees()?;
                    Self::inflate_compressed_block(
                        &mut reader,
                        &literal,
                        Some(&distance),
                        &mut output,
                        expected_len,
                    )?;
                }
                2 => {
                    let (literal, distance) = Self::dynamic_trees(&mut reader)?;
                    Self::inflate_compressed_block(
                        &mut reader,
                        &literal,
                        distance.as_ref(),
                        &mut output,
                        expected_len,
                    )?;
                }
                _ => return Err(String::from("지원하지 않는 deflate block type입니다.")),
            }
            if final_block {
                return Ok(output);
            }
        }
    }
    fn inflate_compressed_block(
        reader: &mut BitReader<'_>,
        literal_tree: &Huffman,
        distance_tree: Option<&Huffman>,
        output: &mut Vec<u8>,
        expected_len: usize,
    ) -> ZipResult<()> {
        loop {
            let symbol = literal_tree.decode(reader)?;
            match symbol {
                0..=255 => {
                    reserve_deflate_output(output, 1, expected_len)?;
                    output.push(
                        u8::try_from(symbol)
                            .map_err(|source| format!("deflate literal 변환 실패: {source}"))?,
                    );
                }
                256 => return Ok(()),
                257..=285 => {
                    let length = Self::decode_length(symbol, reader)?;
                    let Some(distance_huffman) = distance_tree else {
                        return Err(String::from("deflate distance tree가 없습니다."));
                    };
                    let distance_symbol = distance_huffman.decode(reader)?;
                    let distance = Self::decode_distance(distance_symbol, reader)?;
                    Self::copy_previous(output, distance, length, expected_len)?;
                }
                _ => {
                    return Err(String::from(
                        "deflate literal/length symbol이 올바르지 않습니다.",
                    ));
                }
            }
        }
    }
    fn inflate_stored_block(
        reader: &mut BitReader<'_>,
        output: &mut Vec<u8>,
        expected_len: usize,
    ) -> ZipResult<()> {
        reader.align_to_byte();
        let header = reader.read_stored_bytes(4)?;
        let len = read_u16(header, 0)?;
        let nlen = read_u16(header, 2)?;
        if len != !nlen {
            return Err(String::from(
                "deflate 저장 블록 LEN/NLEN이 일치하지 않습니다.",
            ));
        }
        let stored = reader.read_stored_bytes(usize::from(len))?;
        reserve_deflate_output(output, stored.len(), expected_len)?;
        output.extend_from_slice(stored);
        Ok(())
    }
}
impl DeflateWriterExt for DeflateWriter {
    fn best_match(
        bytes: &[u8],
        position: usize,
        head: &[usize],
        previous: &[usize],
    ) -> Option<(usize, usize)> {
        let hash = hash3(bytes, position)?;
        let mut candidate = *head.get(hash)?;
        let min_candidate = position.saturating_sub(0x8000);
        let max_len = bytes.len().saturating_sub(position).min(MAX_MATCH);
        let mut best_len = 0_usize;
        let mut best_distance = 0_usize;
        let mut chain_len = 0_usize;
        while candidate != usize::MAX
            && candidate >= min_candidate
            && candidate < position
            && chain_len < MAX_CHAIN
        {
            let mut len = 0_usize;
            while len < max_len {
                let Some(&left) = bytes.get(candidate.saturating_add(len)) else {
                    break;
                };
                let Some(&right) = bytes.get(position.saturating_add(len)) else {
                    break;
                };
                if left != right {
                    break;
                }
                len = len.saturating_add(1);
            }
            if len > best_len && len >= MIN_MATCH {
                best_len = len;
                best_distance = position.saturating_sub(candidate);
                if len == MAX_MATCH {
                    break;
                }
            }
            candidate = *previous.get(candidate)?;
            chain_len = chain_len.saturating_add(1);
        }
        (best_len >= MIN_MATCH).then_some((best_len, best_distance))
    }
    fn code_length_tokens(lengths: &[u8]) -> ZipResult<Vec<CodeLengthToken>> {
        let mut tokens = Vec::new();
        let mut index = 0_usize;
        while index < lengths.len() {
            let Some(&value) = lengths.get(index) else {
                break;
            };
            if value == 0 {
                let mut run = 1_usize;
                while lengths
                    .get(index.saturating_add(run))
                    .is_some_and(|&candidate| candidate == 0)
                {
                    run = run.saturating_add(1);
                }
                let mut remaining = run;
                while remaining >= 11 {
                    let count = remaining.min(138);
                    tokens.push(CodeLengthToken {
                        extra: u16::try_from(count.saturating_sub(11)).map_err(|source| {
                            format!("deflate repeat-zero-11 변환 실패: {source}")
                        })?,
                        extra_bits: 7,
                        symbol: 18,
                    });
                    remaining = remaining.saturating_sub(count);
                }
                if remaining >= 3 {
                    let count = remaining.min(10);
                    tokens.push(CodeLengthToken {
                        extra: u16::try_from(count.saturating_sub(3)).map_err(|source| {
                            format!("deflate repeat-zero-3 변환 실패: {source}")
                        })?,
                        extra_bits: 3,
                        symbol: 17,
                    });
                    remaining = remaining.saturating_sub(count);
                }
                tokens.extend(repeat_n(
                    CodeLengthToken {
                        extra: 0,
                        extra_bits: 0,
                        symbol: 0,
                    },
                    remaining,
                ));
                index = index.saturating_add(run);
            } else {
                tokens.push(CodeLengthToken {
                    extra: 0,
                    extra_bits: 0,
                    symbol: value,
                });
                let mut run = 0_usize;
                while lengths
                    .get(index.saturating_add(1).saturating_add(run))
                    .is_some_and(|&candidate| candidate == value)
                {
                    run = run.saturating_add(1);
                }
                let mut remaining = run;
                while remaining >= 3 {
                    let count = remaining.min(6);
                    tokens.push(CodeLengthToken {
                        extra: u16::try_from(count.saturating_sub(3)).map_err(|source| {
                            format!("deflate repeat-length 변환 실패: {source}")
                        })?,
                        extra_bits: 2,
                        symbol: 16,
                    });
                    remaining = remaining.saturating_sub(count);
                }
                tokens.extend(repeat_n(
                    CodeLengthToken {
                        extra: 0,
                        extra_bits: 0,
                        symbol: value,
                    },
                    remaining,
                ));
                index = index.saturating_add(1).saturating_add(run);
            }
        }
        Ok(tokens)
    }
    fn deflate(bytes: &[u8]) -> ZipResult<Vec<u8>> {
        let tokens = Self::tokens(bytes)?;
        let fixed = Self::deflate_fixed(&tokens, bytes.len())?;
        let Some(dynamic) = Self::deflate_dynamic(&tokens)? else {
            return Ok(fixed);
        };
        if dynamic.len() < fixed.len() {
            Ok(dynamic)
        } else {
            Ok(fixed)
        }
    }
    fn deflate_dynamic(tokens: &[DeflateToken]) -> ZipResult<Option<Vec<u8>>> {
        let Some((literal_freq, distance_freq)) = Self::dynamic_frequencies(tokens)? else {
            return Ok(None);
        };
        let Some(literal_lengths) = Self::huffman_lengths(&literal_freq, DEFLATE_MAX_BITS_U8)
        else {
            return Ok(None);
        };
        let Some(distance_lengths) = Self::huffman_lengths(&distance_freq, DEFLATE_MAX_BITS_U8)
        else {
            return Ok(None);
        };
        let literal_count = literal_lengths
            .iter()
            .rposition(|&len| len != 0)
            .map_or(257, |index| index.saturating_add(1).max(257));
        let distance_count = distance_lengths
            .iter()
            .rposition(|&len| len != 0)
            .map_or(1, |index| index.saturating_add(1).max(1));
        let mut combined_lengths = Vec::with_capacity(literal_count.saturating_add(distance_count));
        combined_lengths.extend_from_slice(
            literal_lengths
                .get(..literal_count)
                .ok_or_else(|| String::from("deflate literal length 범위 오류"))?,
        );
        combined_lengths.extend_from_slice(
            distance_lengths
                .get(..distance_count)
                .ok_or_else(|| String::from("deflate distance length 범위 오류"))?,
        );
        let code_length_tokens = Self::code_length_tokens(&combined_lengths)?;
        let mut code_length_freq = [0_u32; CODE_LENGTH_SYMBOLS];
        for token in &code_length_tokens {
            let Some(freq) = code_length_freq.get_mut(usize::from(token.symbol)) else {
                return Err(String::from("deflate code length frequency 범위 오류"));
            };
            *freq = freq.saturating_add(1);
        }
        let Some(code_lengths) = Self::huffman_lengths(&code_length_freq, 7) else {
            return Ok(None);
        };
        let mut code_length_count = 4_usize;
        for (index, &symbol) in CODE_LENGTH_ORDER.iter().enumerate().rev() {
            let len = code_lengths
                .get(symbol)
                .copied()
                .ok_or_else(|| String::from("deflate code length order 범위 오류"))?;
            if len != 0 {
                code_length_count = index.saturating_add(1).max(4);
                break;
            }
        }
        let literal_huffman = WriteHuffman::from_lengths(literal_lengths)?;
        let distance_huffman = WriteHuffman::from_lengths(distance_lengths)?;
        let code_huffman = WriteHuffman::from_lengths(code_lengths.clone())?;
        let mut writer = BitWriter {
            bit_buffer: 0,
            bit_count: 0,
            bytes: Vec::with_capacity(tokens.len()),
        };
        writer.write_bits(1, 1);
        writer.write_bits(2, 2);
        writer.write_bits(
            u16::try_from(literal_count.saturating_sub(257))
                .map_err(|source| format!("deflate HLIT 변환 실패: {source}"))?,
            5,
        );
        writer.write_bits(
            u16::try_from(distance_count.saturating_sub(1))
                .map_err(|source| format!("deflate HDIST 변환 실패: {source}"))?,
            5,
        );
        writer.write_bits(
            u16::try_from(code_length_count.saturating_sub(4))
                .map_err(|source| format!("deflate HCLEN 변환 실패: {source}"))?,
            4,
        );
        for &symbol in CODE_LENGTH_ORDER.iter().take(code_length_count) {
            let len = code_lengths
                .get(symbol)
                .copied()
                .ok_or_else(|| String::from("deflate code length 쓰기 범위 오류"))?;
            writer.write_bits(u16::from(len), 3);
        }
        for token in code_length_tokens {
            code_huffman.write_symbol(&mut writer, u16::from(token.symbol))?;
            if token.extra_bits > 0 {
                writer.write_bits(token.extra, token.extra_bits);
            }
        }
        for &token in tokens {
            Self::write_dynamic_token(&mut writer, token, &literal_huffman, &distance_huffman)?;
        }
        literal_huffman.write_symbol(&mut writer, 256)?;
        Ok(Some(writer.finish()))
    }
    fn deflate_fixed(tokens: &[DeflateToken], byte_len: usize) -> ZipResult<Vec<u8>> {
        let mut writer = BitWriter {
            bit_buffer: 0,
            bit_count: 0,
            bytes: Vec::with_capacity(byte_len.saturating_div(2)),
        };
        writer.write_bits(1, 1);
        writer.write_bits(1, 2);
        for &token in tokens {
            Self::write_fixed_token(&mut writer, token)?;
        }
        Self::write_fixed_symbol(&mut writer, 256)?;
        Ok(writer.finish())
    }
    fn distance_symbol(distance: usize) -> Option<(u16, u8, u16)> {
        for (index, &base) in DISTANCE_BASES.iter().enumerate().rev() {
            if distance < base {
                continue;
            }
            let &extra_bits = DISTANCE_EXTRA_BITS.get(index)?;
            let extra = distance.checked_sub(base)?;
            let extra_u16 = u16::try_from(extra).ok()?;
            let symbol = u16::try_from(index).ok()?;
            return Some((symbol, extra_bits, extra_u16));
        }
        None
    }
    fn dynamic_frequencies(
        tokens: &[DeflateToken],
    ) -> ZipResult<Option<([u32; LITERAL_LENGTH_SYMBOLS], [u32; DISTANCE_SYMBOLS])>> {
        let mut literal_freq = [0_u32; LITERAL_LENGTH_SYMBOLS];
        let mut distance_freq = [0_u32; DISTANCE_SYMBOLS];
        let Some(end_freq) = literal_freq.get_mut(256) else {
            return Err(String::from("deflate end symbol 범위 오류"));
        };
        *end_freq = 1;
        let mut has_distance = false;
        for &token in tokens {
            match token {
                DeflateToken::Literal(byte) => {
                    let Some(freq) = literal_freq.get_mut(usize::from(byte)) else {
                        return Err(String::from("deflate literal frequency 범위 오류"));
                    };
                    *freq = freq.saturating_add(1);
                }
                DeflateToken::Match { distance, length } => {
                    let Some((length_symbol, _, _)) = Self::length_symbol(length) else {
                        return Err(String::from("deflate length 범위 오류"));
                    };
                    let Some(length_freq) = literal_freq.get_mut(usize::from(length_symbol)) else {
                        return Err(String::from("deflate length frequency 범위 오류"));
                    };
                    *length_freq = length_freq.saturating_add(1);
                    let Some((distance_symbol, _, _)) = Self::distance_symbol(distance) else {
                        return Err(String::from("deflate distance 범위 오류"));
                    };
                    let Some(distance_freq_slot) =
                        distance_freq.get_mut(usize::from(distance_symbol))
                    else {
                        return Err(String::from("deflate distance frequency 범위 오류"));
                    };
                    *distance_freq_slot = distance_freq_slot.saturating_add(1);
                    has_distance = true;
                }
            }
        }
        Ok(has_distance.then_some((literal_freq, distance_freq)))
    }
    fn huffman_lengths(frequencies: &[u32], max_bits: u8) -> Option<Vec<u8>> {
        let mut lengths = vec![0_u8; frequencies.len()];
        let mut nodes = Vec::new();
        let mut leaves = Vec::new();
        let mut active = Vec::new();
        for (symbol, &freq) in frequencies.iter().enumerate() {
            if freq == 0 {
                continue;
            }
            let node_index = nodes.len();
            nodes.push(HuffmanBuildNode {
                freq: u64::from(freq),
                parent: None,
            });
            leaves.push((symbol, node_index));
            active.push(node_index);
        }
        if active.is_empty() {
            return None;
        }
        if active.len() == 1 {
            let &(symbol, _) = leaves.first()?;
            *lengths.get_mut(symbol)? = 1;
            return Some(lengths);
        }
        while active.len() > 1 {
            active.sort_unstable_by(|&left, &right| {
                let Some(right_node) = nodes.get(right) else {
                    return Ordering::Equal;
                };
                let Some(left_node) = nodes.get(left) else {
                    return Ordering::Equal;
                };
                right_node
                    .freq
                    .cmp(&left_node.freq)
                    .then_with(|| right.cmp(&left))
            });
            let left = active.pop()?;
            let right = active.pop()?;
            let parent = nodes.len();
            let freq = nodes.get(left)?.freq.checked_add(nodes.get(right)?.freq)?;
            nodes.push(HuffmanBuildNode { freq, parent: None });
            nodes.get_mut(left)?.parent = Some(parent);
            nodes.get_mut(right)?.parent = Some(parent);
            active.push(parent);
        }
        for (symbol, node_index) in leaves {
            let mut len = 0_usize;
            let mut cursor = node_index;
            while let Some(parent) = nodes.get(cursor)?.parent {
                len = len.saturating_add(1);
                cursor = parent;
            }
            let len_u8 = u8::try_from(len).ok()?;
            if len_u8 == 0 || len_u8 > max_bits {
                return None;
            }
            *lengths.get_mut(symbol)? = len_u8;
        }
        Some(lengths)
    }
    fn insert_position(bytes: &[u8], position: usize, head: &mut [usize], previous: &mut [usize]) {
        let Some(hash) = hash3(bytes, position) else {
            return;
        };
        let Some(slot) = previous.get_mut(position) else {
            return;
        };
        let Some(head_slot) = head.get_mut(hash) else {
            return;
        };
        *slot = *head_slot;
        *head_slot = position;
    }
    fn length_symbol(length: usize) -> Option<(u16, u8, u16)> {
        for (index, &base) in LENGTH_BASES.iter().enumerate().rev() {
            if length < base {
                continue;
            }
            let &extra_bits = LENGTH_EXTRA_BITS.get(index)?;
            let extra = length.checked_sub(base)?;
            let extra_u16 = u16::try_from(extra).ok()?;
            let index_u16 = u16::try_from(index).ok()?;
            let symbol = 257_u16.checked_add(index_u16)?;
            return Some((symbol, extra_bits, extra_u16));
        }
        None
    }
    fn tokens(bytes: &[u8]) -> ZipResult<Vec<DeflateToken>> {
        let mut tokens = Vec::with_capacity(bytes.len());
        let mut head = vec![usize::MAX; HASH_SIZE];
        let mut previous = vec![usize::MAX; bytes.len()];
        let mut position = 0_usize;
        while position < bytes.len() {
            if let Some((length, distance)) = Self::best_match(bytes, position, &head, &previous) {
                tokens.push(DeflateToken::Match { distance, length });
                let next_position = position
                    .checked_add(length)
                    .ok_or_else(|| String::from("deflate 위치 계산 실패"))?;
                for insert_position in position..next_position {
                    Self::insert_position(bytes, insert_position, &mut head, &mut previous);
                }
                position = next_position;
            } else {
                let Some(&byte) = bytes.get(position) else {
                    return Err(String::from("deflate literal 범위 오류"));
                };
                tokens.push(DeflateToken::Literal(byte));
                Self::insert_position(bytes, position, &mut head, &mut previous);
                position = position.saturating_add(1);
            }
        }
        Ok(tokens)
    }
    fn write_distance(writer: &mut BitWriter, distance: usize) -> ZipResult<()> {
        let Some((symbol, extra_bits, extra)) = Self::distance_symbol(distance) else {
            return Err(String::from("deflate distance 범위 오류"));
        };
        writer.write_bits(reverse_bits(symbol, 5), 5);
        if extra_bits > 0 {
            writer.write_bits(extra, extra_bits);
        }
        Ok(())
    }
    fn write_dynamic_token(
        writer: &mut BitWriter,
        token: DeflateToken,
        literal_huffman: &WriteHuffman,
        distance_huffman: &WriteHuffman,
    ) -> ZipResult<()> {
        match token {
            DeflateToken::Literal(byte) => literal_huffman.write_symbol(writer, u16::from(byte)),
            DeflateToken::Match { distance, length } => {
                let Some((length_symbol, length_extra_bits, length_extra)) =
                    Self::length_symbol(length)
                else {
                    return Err(String::from("deflate length 범위 오류"));
                };
                literal_huffman.write_symbol(writer, length_symbol)?;
                if length_extra_bits > 0 {
                    writer.write_bits(length_extra, length_extra_bits);
                }
                let Some((distance_symbol, distance_extra_bits, distance_extra)) =
                    Self::distance_symbol(distance)
                else {
                    return Err(String::from("deflate distance 범위 오류"));
                };
                distance_huffman.write_symbol(writer, distance_symbol)?;
                if distance_extra_bits > 0 {
                    writer.write_bits(distance_extra, distance_extra_bits);
                }
                Ok(())
            }
        }
    }
    fn write_fixed_symbol(writer: &mut BitWriter, symbol: u16) -> ZipResult<()> {
        let (code, bit_count) = match symbol {
            0..=143 => (0x30_u16.saturating_add(symbol), 8_u8),
            144..=255 => (0x190_u16.saturating_add(symbol.saturating_sub(144)), 9_u8),
            256..=279 => (symbol.saturating_sub(256), 7_u8),
            280..=287 => (0xc0_u16.saturating_add(symbol.saturating_sub(280)), 8_u8),
            _ => return Err(String::from("deflate fixed symbol 범위 오류")),
        };
        writer.write_bits(reverse_bits(code, bit_count), bit_count);
        Ok(())
    }
    fn write_fixed_token(writer: &mut BitWriter, token: DeflateToken) -> ZipResult<()> {
        match token {
            DeflateToken::Literal(byte) => Self::write_fixed_symbol(writer, u16::from(byte)),
            DeflateToken::Match { distance, length } => {
                Self::write_length(writer, length)?;
                Self::write_distance(writer, distance)
            }
        }
    }
    fn write_length(writer: &mut BitWriter, length: usize) -> ZipResult<()> {
        let Some((symbol, extra_bits, extra)) = Self::length_symbol(length) else {
            return Err(String::from("deflate length 범위 오류"));
        };
        Self::write_fixed_symbol(writer, symbol)?;
        if extra_bits > 0 {
            writer.write_bits(extra, extra_bits);
        }
        Ok(())
    }
}
fn create_zip_dir(path: &Path, context: &'static str) -> Result<()> {
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
fn reserve_deflate_output(
    output: &mut Vec<u8>,
    additional_len: usize,
    expected_len: usize,
) -> ZipResult<()> {
    let next_len = output
        .len()
        .checked_add(additional_len)
        .ok_or_else(|| String::from("deflate 출력 크기 계산 실패"))?;
    if next_len > expected_len {
        return Err(String::from(
            "deflate 출력이 ZIP 선언 해제 크기를 초과했습니다.",
        ));
    }
    output
        .try_reserve(additional_len)
        .map_err(|source| format!("deflate 출력 메모리 확보 실패: {source}"))
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
                let mut parts = Vec::new();
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
                    parts.push(text);
                }
                files.push(PendingFile {
                    name: parts.join("/"),
                    path,
                });
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
fn hash3(bytes: &[u8], position: usize) -> Option<usize> {
    let &[first_byte, second_byte, third_byte] = bytes.get(position..)?.first_chunk::<3>()?;
    let first = usize::from(first_byte);
    let second = usize::from(second_byte);
    let third = usize::from(third_byte);
    Some(((first << 10_usize) ^ (second << 5_usize) ^ third) & HASH_SIZE.saturating_sub(1))
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
        return Err(String::from("ZIP 파일이 너무 짧습니다."));
    }
    let min_offset = bytes
        .len()
        .saturating_sub(END_OF_CENTRAL_DIRECTORY_LEN.saturating_add(ZIP_COMMENT_MAX_LEN));
    let max_offset = bytes.len().saturating_sub(END_OF_CENTRAL_DIRECTORY_LEN);
    let search_end = max_offset.saturating_add(4_usize);
    let search_bytes = bytes
        .get(min_offset..search_end)
        .ok_or_else(|| String::from("ZIP EOCD 검색 범위 오류"))?;
    let eocd_signature = END_OF_CENTRAL_DIRECTORY_SIGNATURE.to_le_bytes();
    let mut search_len = search_bytes.len();
    let eocd_offset = loop {
        let Some(relative_offset) = search_bytes
            .get(..search_len)
            .ok_or_else(|| String::from("ZIP EOCD 검색 범위 오류"))?
            .array_windows::<4>()
            .rposition(|window| *window == eocd_signature)
        else {
            return Err(String::from("ZIP EOCD를 찾지 못했습니다."));
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
        .ok_or_else(|| String::from("ZIP 중앙 디렉터리 범위 계산 실패"))?;
    if central_dir_end > eocd_offset {
        return Err(String::from(
            "ZIP 중앙 디렉터리가 EOCD 범위를 벗어났습니다.",
        ));
    }
    let mut entries = Vec::with_capacity(total_entries);
    let mut cursor = central_dir_offset;
    for _ in 0..total_entries {
        if read_u32(bytes, cursor)? != CENTRAL_DIRECTORY_SIGNATURE {
            return Err(String::from(
                "ZIP 중앙 디렉터리 signature가 올바르지 않습니다.",
            ));
        }
        let flags = read_u16(bytes, cursor.saturating_add(8))?;
        if flags & ENCRYPTED_FLAG != 0 {
            return Err(String::from("암호화된 ZIP entry는 지원하지 않습니다."));
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
            .ok_or_else(|| String::from("ZIP entry 이름 범위 계산 실패"))?;
        let Some(name_bytes) = bytes.get(name_start..name_end) else {
            return Err(String::from("ZIP entry 이름이 파일 범위를 벗어났습니다."));
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
            .ok_or_else(|| String::from("ZIP 중앙 디렉터리 다음 entry 위치 계산 실패"))?;
    }
    if cursor != central_dir_end {
        return Err(String::from(
            "ZIP 중앙 디렉터리 크기가 entry 목록과 일치하지 않습니다.",
        ));
    }
    Ok(entries)
}
fn path_from_entry_name(entry_name: &str) -> ZipResult<PathBuf> {
    if !is_safe_archive_entry_path(entry_name) {
        return Err(format!(
            "허용되지 않은 압축 경로가 포함되어 있습니다: {entry_name}"
        ));
    }
    let mut path = PathBuf::new();
    for part in entry_name.split(['/', '\\']) {
        if !part.is_empty() && part != "." {
            path.push(part);
        }
    }
    Ok(path)
}
fn push_repeated(lengths: &mut Vec<u8>, value: u8, repeat: usize, total: usize) -> ZipResult<()> {
    let next_len = lengths
        .len()
        .checked_add(repeat)
        .ok_or_else(|| String::from("deflate repeat 길이 계산 실패"))?;
    if next_len > total {
        return Err(String::from(
            "deflate repeat 길이가 code length 총합을 초과합니다.",
        ));
    }
    lengths.extend(repeat_n(value, repeat));
    Ok(())
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
    error_message: &str,
) -> ZipResult<[u8; N]> {
    let Some(end) = offset.checked_add(N) else {
        return Err(String::from(error_message));
    };
    let Some(raw_bytes) = bytes.get(offset..end).and_then(<[u8]>::as_array::<N>) else {
        return Err(String::from(error_message));
    };
    Ok(*raw_bytes)
}
fn reverse_bits(mut value: u16, count: u8) -> u16 {
    let mut out = 0_u16;
    for _ in 0_u8..count {
        out = (out << 1_u8) | (value & 1_u16);
        value >>= 1_u8;
    }
    out
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
