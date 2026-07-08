use super::path_util::reject_windows_special_component;
use super::{ZipArchiveExtractor, path_util::path_to_slashes};
use crate::diagnostic::{
    AppError, Result, err, err_with_source, path_context_message, path_pair_context_message,
};
use alloc::{borrow::Cow, boxed::Box, string::String, vec::Vec};
use core::{error::Error, fmt, range::Range, result::Result as CoreResult, str};
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
const CRC32_TABLE: [u32; 256] = [
    0x0000_0000,
    0x7707_3096,
    0xee0e_612c,
    0x9909_51ba,
    0x076d_c419,
    0x706a_f48f,
    0xe963_a535,
    0x9e64_95a3,
    0x0edb_8832,
    0x79dc_b8a4,
    0xe0d5_e91e,
    0x97d2_d988,
    0x09b6_4c2b,
    0x7eb1_7cbd,
    0xe7b8_2d07,
    0x90bf_1d91,
    0x1db7_1064,
    0x6ab0_20f2,
    0xf3b9_7148,
    0x84be_41de,
    0x1ada_d47d,
    0x6ddd_e4eb,
    0xf4d4_b551,
    0x83d3_85c7,
    0x136c_9856,
    0x646b_a8c0,
    0xfd62_f97a,
    0x8a65_c9ec,
    0x1401_5c4f,
    0x6306_6cd9,
    0xfa0f_3d63,
    0x8d08_0df5,
    0x3b6e_20c8,
    0x4c69_105e,
    0xd560_41e4,
    0xa267_7172,
    0x3c03_e4d1,
    0x4b04_d447,
    0xd20d_85fd,
    0xa50a_b56b,
    0x35b5_a8fa,
    0x42b2_986c,
    0xdbbb_c9d6,
    0xacbc_f940,
    0x32d8_6ce3,
    0x45df_5c75,
    0xdcd6_0dcf,
    0xabd1_3d59,
    0x26d9_30ac,
    0x51de_003a,
    0xc8d7_5180,
    0xbfd0_6116,
    0x21b4_f4b5,
    0x56b3_c423,
    0xcfba_9599,
    0xb8bd_a50f,
    0x2802_b89e,
    0x5f05_8808,
    0xc60c_d9b2,
    0xb10b_e924,
    0x2f6f_7c87,
    0x5868_4c11,
    0xc161_1dab,
    0xb666_2d3d,
    0x76dc_4190,
    0x01db_7106,
    0x98d2_20bc,
    0xefd5_102a,
    0x71b1_8589,
    0x06b6_b51f,
    0x9fbf_e4a5,
    0xe8b8_d433,
    0x7807_c9a2,
    0x0f00_f934,
    0x9609_a88e,
    0xe10e_9818,
    0x7f6a_0dbb,
    0x086d_3d2d,
    0x9164_6c97,
    0xe663_5c01,
    0x6b6b_51f4,
    0x1c6c_6162,
    0x8565_30d8,
    0xf262_004e,
    0x6c06_95ed,
    0x1b01_a57b,
    0x8208_f4c1,
    0xf50f_c457,
    0x65b0_d9c6,
    0x12b7_e950,
    0x8bbe_b8ea,
    0xfcb9_887c,
    0x62dd_1ddf,
    0x15da_2d49,
    0x8cd3_7cf3,
    0xfbd4_4c65,
    0x4db2_6158,
    0x3ab5_51ce,
    0xa3bc_0074,
    0xd4bb_30e2,
    0x4adf_a541,
    0x3dd8_95d7,
    0xa4d1_c46d,
    0xd3d6_f4fb,
    0x4369_e96a,
    0x346e_d9fc,
    0xad67_8846,
    0xda60_b8d0,
    0x4404_2d73,
    0x3303_1de5,
    0xaa0a_4c5f,
    0xdd0d_7cc9,
    0x5005_713c,
    0x2702_41aa,
    0xbe0b_1010,
    0xc90c_2086,
    0x5768_b525,
    0x206f_85b3,
    0xb966_d409,
    0xce61_e49f,
    0x5ede_f90e,
    0x29d9_c998,
    0xb0d0_9822,
    0xc7d7_a8b4,
    0x59b3_3d17,
    0x2eb4_0d81,
    0xb7bd_5c3b,
    0xc0ba_6cad,
    0xedb8_8320,
    0x9abf_b3b6,
    0x03b6_e20c,
    0x74b1_d29a,
    0xead5_4739,
    0x9dd2_77af,
    0x04db_2615,
    0x73dc_1683,
    0xe363_0b12,
    0x9464_3b84,
    0x0d6d_6a3e,
    0x7a6a_5aa8,
    0xe40e_cf0b,
    0x9309_ff9d,
    0x0a00_ae27,
    0x7d07_9eb1,
    0xf00f_9344,
    0x8708_a3d2,
    0x1e01_f268,
    0x6906_c2fe,
    0xf762_575d,
    0x8065_67cb,
    0x196c_3671,
    0x6e6b_06e7,
    0xfed4_1b76,
    0x89d3_2be0,
    0x10da_7a5a,
    0x67dd_4acc,
    0xf9b9_df6f,
    0x8ebe_eff9,
    0x17b7_be43,
    0x60b0_8ed5,
    0xd6d6_a3e8,
    0xa1d1_937e,
    0x38d8_c2c4,
    0x4fdf_f252,
    0xd1bb_67f1,
    0xa6bc_5767,
    0x3fb5_06dd,
    0x48b2_364b,
    0xd80d_2bda,
    0xaf0a_1b4c,
    0x3603_4af6,
    0x4104_7a60,
    0xdf60_efc3,
    0xa867_df55,
    0x316e_8eef,
    0x4669_be79,
    0xcb61_b38c,
    0xbc66_831a,
    0x256f_d2a0,
    0x5268_e236,
    0xcc0c_7795,
    0xbb0b_4703,
    0x2202_16b9,
    0x5505_262f,
    0xc5ba_3bbe,
    0xb2bd_0b28,
    0x2bb4_5a92,
    0x5cb3_6a04,
    0xc2d7_ffa7,
    0xb5d0_cf31,
    0x2cd9_9e8b,
    0x5bde_ae1d,
    0x9b64_c2b0,
    0xec63_f226,
    0x756a_a39c,
    0x026d_930a,
    0x9c09_06a9,
    0xeb0e_363f,
    0x7207_6785,
    0x0500_5713,
    0x95bf_4a82,
    0xe2b8_7a14,
    0x7bb1_2bae,
    0x0cb6_1b38,
    0x92d2_8e9b,
    0xe5d5_be0d,
    0x7cdc_efb7,
    0x0bdb_df21,
    0x86d3_d2d4,
    0xf1d4_e242,
    0x68dd_b3f8,
    0x1fda_836e,
    0x81be_16cd,
    0xf6b9_265b,
    0x6fb0_77e1,
    0x18b7_4777,
    0x8808_5ae6,
    0xff0f_6a70,
    0x6606_3bca,
    0x1101_0b5c,
    0x8f65_9eff,
    0xf862_ae69,
    0x616b_ffd3,
    0x166c_cf45,
    0xa00a_e278,
    0xd70d_d2ee,
    0x4e04_8354,
    0x3903_b3c2,
    0xa767_2661,
    0xd060_16f7,
    0x4969_474d,
    0x3e6e_77db,
    0xaed1_6a4a,
    0xd9d6_5adc,
    0x40df_0b66,
    0x37d8_3bf0,
    0xa9bc_ae53,
    0xdebb_9ec5,
    0x47b2_cf7f,
    0x30b5_ffe9,
    0xbdbd_f21c,
    0xcaba_c28a,
    0x53b3_9330,
    0x24b4_a3a6,
    0xbad0_3605,
    0xcdd7_0693,
    0x54de_5729,
    0x23d9_67bf,
    0xb366_7a2e,
    0xc461_4ab8,
    0x5d68_1b02,
    0x2a6f_2b94,
    0xb40b_be37,
    0xc30c_8ea1,
    0x5a05_df1b,
    0x2d02_ef8d,
];
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
type ZipResult<T> = CoreResult<T, ZipError>;
#[derive(Debug)]
struct ZipError {
    message: Cow<'static, str>,
    source: Option<Box<dyn Error + Send + Sync>>,
}
struct ZipEntry<'zip> {
    compressed_size: u32,
    crc32: u32,
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
struct HeaderSplit<'bytes, const LEN: usize> {
    header: &'bytes [u8; LEN],
    tail: &'bytes [u8],
}
impl fmt::Display for ZipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message.as_ref())
    }
}
impl Error for ZipError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|source| {
            let source_ref: &(dyn Error + 'static) = source;
            source_ref
        })
    }
}
impl From<Cow<'static, str>> for ZipError {
    fn from(value: Cow<'static, str>) -> Self {
        Self {
            message: value,
            source: None,
        }
    }
}
impl From<String> for ZipError {
    fn from(value: String) -> Self {
        Self {
            message: Cow::Owned(value),
            source: None,
        }
    }
}
impl From<&'static str> for ZipError {
    fn from(value: &'static str) -> Self {
        Self {
            message: Cow::Borrowed(value),
            source: None,
        }
    }
}
impl From<ZipError> for AppError {
    fn from(value: ZipError) -> Self {
        let ZipError {
            message,
            source: source_error,
        } = value;
        match source_error {
            Some(source) => err_with_source(message, source),
            None => Self::from(message),
        }
    }
}
impl ZipEntry<'_> {
    fn data<'bytes>(&self, bytes: &'bytes [u8], expected_len: usize) -> Result<Cow<'bytes, [u8]>> {
        let local_offset = usize::try_from(self.local_header_offset)
            .map_err(|source| err_with_source("ZIP local header offset 변환 실패", source))?;
        let local_split = split_header_at::<LOCAL_FILE_HEADER_LEN>(
            bytes,
            local_offset,
            ZIP_BAD_LOCAL_HEADER_MESSAGE,
        )
        .map_err(|source| err(zip_entry_message(source.message.as_ref(), self.name)))?;
        let local_header = local_split.header;
        let local_tail = local_split.tail;
        if read_u32(local_header, 0)? != LOCAL_FILE_HEADER_SIGNATURE {
            return Err(zip_entry_message(ZIP_BAD_LOCAL_HEADER_MESSAGE, self.name).into());
        }
        let name_len = usize::from(read_u16(local_header, 26)?);
        let extra_len = usize::from(read_u16(local_header, 28)?);
        let local_name = local_tail
            .get(..name_len)
            .ok_or_else(|| zip_static("ZIP local header 이름 범위 오류"))?;
        if local_name != self.name.as_bytes() {
            return Err(zip_entry_message(
                "ZIP local header 이름이 중앙 디렉터리와 다릅니다",
                self.name,
            )
            .into());
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
                return Err(zip_static(ZIP_CENTRAL_DIRECTORY_SIZE_MISMATCH_MESSAGE).into());
            }
            return Ok(None);
        }
        let central_split = split_header_at::<CENTRAL_DIRECTORY_HEADER_LEN>(
            self.bytes,
            self.cursor,
            ZIP_CENTRAL_HEADER_RANGE,
        )?;
        let header = central_split.header;
        let tail = central_split.tail;
        if read_u32(header, 0)? != CENTRAL_DIRECTORY_SIGNATURE {
            return Err(zip_static(ZIP_BAD_CENTRAL_SIGNATURE_MESSAGE).into());
        }
        if read_u16(header, 8)? & ENCRYPTED_FLAG != 0 {
            return Err(zip_static("암호화된 ZIP entry는 지원하지 않습니다.").into());
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
            return Err(zip_static(ZIP_CENTRAL_DIRECTORY_SIZE_MISMATCH_MESSAGE).into());
        }
        let payload_len = entry_len
            .checked_sub(CENTRAL_DIRECTORY_HEADER_LEN)
            .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 entry payload 길이 계산 실패"))?;
        let payload = tail
            .get(..payload_len)
            .ok_or_else(|| zip_static(ZIP_CENTRAL_HEADER_RANGE))?;
        let Some(name_bytes) = payload.get(..name_len) else {
            return Err(zip_static("ZIP entry 이름이 파일 범위를 벗어났습니다.").into());
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
            local_header_offset: read_u32(header, 42)?,
            method: read_u16(header, 10)?,
            name,
            uncompressed_size: read_u32(header, 24)?,
        }))
    }
}
impl ZipArchiveExtractor<'_> {
    pub(super) fn extract(&self) -> Result<()> {
        let bytes = self.read_archive_bytes()?;
        if bytes.len() < END_OF_CENTRAL_DIRECTORY_LEN {
            return Err(zip_static("ZIP 파일이 너무 짧습니다.").into());
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
                return Err(zip_static("ZIP EOCD를 찾지 못했습니다.").into());
            };
            let offset = min_offset
                .checked_add(relative_offset)
                .ok_or_else(|| zip_static("ZIP EOCD offset 계산 실패"))?;
            let eocd = split_header_at::<END_OF_CENTRAL_DIRECTORY_LEN>(
                bytes.as_slice(),
                offset,
                ZIP_EOCD_HEADER_RANGE,
            )?
            .header;
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
            return Err(zip_static("분할 ZIP archive는 지원하지 않습니다.").into());
        }
        let central_dir_size = usize::try_from(read_u32(eocd, 12)?)
            .map_err(|source| err_with_source("ZIP 중앙 디렉터리 크기 변환 실패", source))?;
        let central_dir_offset = usize::try_from(read_u32(eocd, 16)?)
            .map_err(|source| err_with_source("ZIP 중앙 디렉터리 offset 변환 실패", source))?;
        let central_dir_end = central_dir_offset
            .checked_add(central_dir_size)
            .ok_or_else(|| zip_static("ZIP 중앙 디렉터리 범위 계산 실패"))?;
        if central_dir_end > eocd_offset {
            return Err(zip_static("ZIP 중앙 디렉터리가 EOCD 범위를 벗어났습니다.").into());
        }
        let mut central_directory = ZipCentralDirectory {
            bytes: bytes.as_slice(),
            cursor: central_dir_offset,
            end: central_dir_end,
            remaining_entries: usize::from(entries_total),
        };
        let mut total_uncompressed = 0_usize;
        let mut seen_paths = HashSet::new();
        seen_paths
            .try_reserve(usize::from(entries_total))
            .map_err(|source| {
                err_with_source("ZIP entry path 방문 집합 메모리 확보 실패", source)
            })?;
        while let Some(entry) = central_directory.next_entry()? {
            let relative_path = entry.relative_path()?;
            let entry_path = self.unpack_dir.join(&relative_path);
            if !seen_paths.insert(relative_path) {
                return Err(err(format!(
                    "ZIP 중복 entry 경로가 있습니다: {}",
                    entry.name
                )));
            }
            total_uncompressed =
                self.extract_entry(bytes.as_slice(), &entry, &entry_path, total_uncompressed)?;
        }
        Ok(())
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
    fn read_archive_bytes(&self) -> Result<Vec<u8>> {
        let file = File::open(self.archive_path).map_err(|source_err| {
            err_with_source(
                path_context_message("xlsx 압축 파일 열기 실패", self.archive_path),
                source_err,
            )
        })?;
        let metadata = file.metadata().map_err(|source_err| {
            err_with_source(
                path_context_message("xlsx 압축 파일 정보 확인 실패", self.archive_path),
                source_err,
            )
        })?;
        let archive_len = usize::try_from(metadata.len()).map_err(|source| {
            err(format!(
                "xlsx 압축 파일 크기 변환 실패({}): {source}",
                self.archive_path.display()
            ))
        })?;
        if archive_len > ZIP_MAX_ARCHIVE_BYTES {
            return Err(err(format!(
                "xlsx 압축 파일 크기가 허용 한도({ZIP_MAX_ARCHIVE_BYTES} bytes)를 초과했습니다: {}",
                self.archive_path.display()
            )));
        }
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(archive_len)
            .map_err(|source| err_with_source("xlsx 압축 파일 메모리 확보 실패", source))?;
        let read_limit = u64::try_from(ZIP_MAX_ARCHIVE_BYTES)
            .ok()
            .and_then(|value| value.checked_add(1))
            .ok_or_else(|| err("xlsx 압축 파일 읽기 한도 계산 실패"))?;
        let mut limited = file.take(read_limit);
        limited.read_to_end(&mut bytes).map_err(|source_err| {
            err_with_source(
                path_context_message("xlsx 압축 파일 읽기 실패", self.archive_path),
                source_err,
            )
        })?;
        if bytes.len() > ZIP_MAX_ARCHIVE_BYTES {
            return Err(err(format!(
                "xlsx 압축 파일 크기가 허용 한도({ZIP_MAX_ARCHIVE_BYTES} bytes)를 초과했습니다: {}",
                self.archive_path.display()
            )));
        }
        if bytes.len() != archive_len {
            return Err(err(format!(
                "xlsx 압축 파일이 읽는 중 변경되었습니다: {}",
                self.archive_path.display()
            )));
        }
        Ok(bytes)
    }
}
struct PendingFile {
    name: String,
    path: PathBuf,
}
const fn zip_static(message: &'static str) -> ZipError {
    ZipError {
        message: Cow::Borrowed(message),
        source: None,
    }
}
fn zip_with_source<E>(message: impl Into<Cow<'static, str>>, source: E) -> ZipError
where
    E: Error + Send + Sync + 'static,
{
    ZipError {
        message: message.into(),
        source: Some(Box::new(source)),
    }
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
                if files.len() == files.capacity() {
                    files.try_reserve(1).map_err(|source| {
                        err_with_source("xlsx 파트 목록 메모리 확보 실패", source)
                    })?;
                }
                files.push(PendingFile { name, path });
            }
            (false, false) => {}
        }
    }
    Ok(())
}
fn crc32(bytes: &[u8]) -> ZipResult<u32> {
    bytes
        .iter()
        .try_fold(u32::MAX, |crc, &byte| {
            let table_index = usize::from((crc ^ u32::from(byte)).to_le_bytes()[0]);
            let Some(table_value) = CRC32_TABLE.get(table_index).copied() else {
                return Err(zip_static("ZIP CRC32 table 범위가 손상되었습니다."));
            };
            Ok((crc >> 8_u8) ^ table_value)
        })
        .map(|crc| !crc)
}
fn split_header_at<'bytes, const LEN: usize>(
    bytes: &'bytes [u8],
    offset: usize,
    context: &'static str,
) -> ZipResult<HeaderSplit<'bytes, LEN>> {
    let Some((header, tail)) = bytes
        .get(offset..)
        .and_then(|remaining| remaining.split_first_chunk::<LEN>())
    else {
        return Err(zip_static(context));
    };
    Ok(HeaderSplit { header, tail })
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
