use self::{
    bit_io::{BitReader, BitWriter},
    huffman::{Huffman, HuffmanCode, WriteHuffman},
    token::{CodeLengthToken, DeflateToken, HuffmanBuildNode, HuffmanLeafLength},
};
use super::{
    CODE_LENGTH_ORDER, CODE_LENGTH_SYMBOLS, DEFLATE_MAX_BITS, DEFLATE_MAX_BITS_U8, DISTANCE_BASES,
    DISTANCE_EXTRA_BITS, DISTANCE_SYMBOLS, FIXED_DISTANCE_SYMBOLS, FIXED_LITERAL_SYMBOLS,
    HASH_SIZE, LENGTH_BASES, LENGTH_EXTRA_BITS, LITERAL_LENGTH_SYMBOLS, MAX_CHAIN, MAX_MATCH,
    MIN_MATCH, ZipResult, read_u16, zip_static,
};
use alloc::vec::Vec;
use core::{array::from_fn, cmp::Ordering, iter::repeat_n};
pub(in crate::excel::zip_archive) mod bit_io {
    use alloc::vec::Vec;
    pub(in crate::excel::zip_archive) struct BitReader<'bytes> {
        pub bit_buffer: u32,
        pub bit_count: u8,
        pub bytes: &'bytes [u8],
        pub cursor: usize,
    }
    pub(in crate::excel::zip_archive) struct BitWriter {
        pub bit_buffer: u8,
        pub bit_count: u8,
        pub bytes: Vec<u8>,
    }
}
pub(in crate::excel::zip_archive) mod huffman {
    use super::DEFLATE_MAX_BITS;
    use alloc::vec::Vec;
    pub(in crate::excel::zip_archive) struct Huffman {
        pub codes: [Vec<HuffmanCode>; DEFLATE_MAX_BITS + 1],
    }
    pub(in crate::excel::zip_archive) struct WriteHuffman {
        pub codes: Vec<u16>,
        pub lengths: Vec<u8>,
    }
    #[derive(Clone)]
    pub(in crate::excel::zip_archive) struct HuffmanCode {
        pub code: u16,
        pub symbol: u16,
    }
}
pub(in crate::excel::zip_archive) mod token {
    #[derive(Clone, Copy)]
    pub(in crate::excel::zip_archive) enum DeflateToken {
        Literal(u8),
        Match { distance: usize, length: usize },
    }
    #[derive(Clone, Copy)]
    pub(in crate::excel::zip_archive) struct CodeLengthToken {
        pub extra: u16,
        pub extra_bits: u8,
        pub symbol: u8,
    }
    pub(in crate::excel::zip_archive) struct HuffmanBuildNode {
        pub freq: u64,
        pub parent: Option<usize>,
    }
    pub(in crate::excel::zip_archive) struct HuffmanLeafLength {
        pub freq: u32,
        pub len: usize,
        pub symbol: usize,
    }
}
const TOO_FAR_MATCH_DISTANCE: usize = 4096;
const UTF8_BOM: &[u8] = &[0xEF, 0xBB, 0xBF];
const XML_NICE_MATCH_LEN: usize = 128;
const XLSX_XML_NEEDLES: [&[u8]; 7] = [
    b"<worksheet",
    b"<sst",
    b"<workbook",
    b"<styleSheet",
    b"<Relationships",
    b"<Types",
    b"schemas.openxmlformats.org",
];
pub(super) struct DeflateInflater<'bytes> {
    pub bytes: &'bytes [u8],
    pub expected_len: usize,
}
pub(super) struct DeflateWriter<'bytes> {
    pub bytes: &'bytes [u8],
}
struct CodeLengthTokenizer<'lengths> {
    lengths: &'lengths [u8],
}
struct DynamicDeflateWriter<'tokens> {
    tokens: &'tokens [DeflateToken],
}
struct DynamicBlockHeaderWriter<'writer, 'lengths> {
    code_length_count: usize,
    code_lengths: &'lengths [u8],
    distance_count: usize,
    literal_count: usize,
    writer: &'writer mut BitWriter,
}
struct DynamicFrequencyCounter<'tokens> {
    tokens: &'tokens [DeflateToken],
}
struct DynamicTokenWriter<'writer, 'huffman> {
    distance_huffman: &'huffman WriteHuffman,
    literal_huffman: &'huffman WriteHuffman,
    writer: &'writer mut BitWriter,
}
#[derive(Clone, Copy)]
struct DeflateProfile {
    max_chain: usize,
    nice_match_len: usize,
}
struct FixedDeflateWriter<'tokens> {
    byte_len: usize,
    tokens: &'tokens [DeflateToken],
}
struct FixedTokenWriter<'writer> {
    writer: &'writer mut BitWriter,
}
struct HuffmanLengthBuilder<'frequencies> {
    frequencies: &'frequencies [u32],
    max_bits: u8,
}
struct LimitedHuffmanLengthAssigner<'counts> {
    length_counts: &'counts [usize],
    max_bit_count: usize,
}
struct LimitedLengthCountReducer<'counts> {
    length_counts: &'counts mut [usize],
}
impl BitReader<'_> {
    const fn align_to_byte(&mut self) {
        self.bit_buffer = 0;
        self.bit_count = 0;
    }
    fn read_bits(&mut self, count: u8) -> ZipResult<u32> {
        while self.bit_count < count {
            let Some(&byte) = self.bytes.get(self.cursor) else {
                return Err(zip_static("deflate bitstream이 예기치 않게 끝났습니다."));
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
        let Some((_, remaining)) = self.bytes.split_at_checked(self.cursor) else {
            return Err(zip_static("deflate 저장 블록 시작 위치가 입력보다 깁니다."));
        };
        let Some((bytes, _)) = remaining.split_at_checked(len) else {
            return Err(zip_static("deflate 저장 블록이 입력보다 깁니다."));
        };
        self.cursor = self
            .cursor
            .checked_add(len)
            .ok_or_else(|| zip_static("deflate 저장 블록 크기 계산 실패"))?;
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
    fn with_capacity(capacity: usize, context: &str) -> ZipResult<Self> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve(capacity)
            .map_err(|source| format!("{context} 메모리 확보 실패: {source}"))?;
        Ok(Self {
            bit_buffer: 0,
            bit_count: 0,
            bytes,
        })
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
        Err(zip_static("deflate Huffman code를 해석하지 못했습니다."))
    }
    fn from_lengths(lengths: &[u8]) -> ZipResult<Option<Self>> {
        let mut bl_count = [0_u16; DEFLATE_MAX_BITS + 1];
        for &len in lengths {
            if len == 0 {
                continue;
            }
            let len_index = usize::from(len);
            if len_index > DEFLATE_MAX_BITS {
                return Err(zip_static("deflate Huffman code 길이가 너무 깁니다."));
            }
            let Some(count) = bl_count.get_mut(len_index) else {
                return Err(zip_static("deflate Huffman count 범위 오류"));
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
                return Err(zip_static("deflate Huffman count 범위 오류"));
            };
            code = code.saturating_add(previous_count) << 1_u8;
            let Some(next_slot) = next_code.get_mut(bits) else {
                return Err(zip_static("deflate Huffman next code 범위 오류"));
            };
            *next_slot = code;
        }
        let mut codes: [Vec<HuffmanCode>; DEFLATE_MAX_BITS + 1] =
            [const { Vec::new() }; DEFLATE_MAX_BITS + 1];
        for (symbol, &len) in lengths.iter().enumerate() {
            if len == 0 {
                continue;
            }
            let len_index = usize::from(len);
            let Some(next_slot) = next_code.get_mut(len_index) else {
                return Err(zip_static("deflate Huffman next code 범위 오류"));
            };
            let assigned = *next_slot;
            *next_slot = next_slot.saturating_add(1);
            let symbol_u16 = u16::try_from(symbol)
                .map_err(|source| format!("deflate symbol 변환 실패: {source}"))?;
            let Some(code_bucket) = codes.get_mut(len_index) else {
                return Err(zip_static("deflate Huffman code bucket 범위 오류"));
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
                return Err(zip_static("deflate 출력 Huffman 길이 범위 오류"));
            };
            *count = count.saturating_add(1);
        }
        let mut next_code = [0_u16; DEFLATE_MAX_BITS + 1];
        let mut code = 0_u16;
        for bits in 1..=DEFLATE_MAX_BITS {
            let previous = bits.saturating_sub(1);
            let Some(&previous_count) = bl_count.get(previous) else {
                return Err(zip_static("deflate 출력 Huffman count 범위 오류"));
            };
            code = code.saturating_add(previous_count) << 1_u8;
            let Some(next_slot) = next_code.get_mut(bits) else {
                return Err(zip_static("deflate 출력 Huffman next code 범위 오류"));
            };
            *next_slot = code;
        }
        let mut codes = Vec::new();
        codes
            .try_reserve_exact(lengths.len())
            .map_err(|source| format!("deflate 출력 Huffman code 메모리 확보 실패: {source}"))?;
        codes.resize(lengths.len(), 0_u16);
        for (symbol, &len) in lengths.iter().enumerate() {
            if len == 0 {
                continue;
            }
            let Some(next_slot) = next_code.get_mut(usize::from(len)) else {
                return Err(zip_static("deflate 출력 Huffman code 범위 오류"));
            };
            let Some(code_slot) = codes.get_mut(symbol) else {
                return Err(zip_static("deflate 출력 Huffman symbol 범위 오류"));
            };
            *code_slot = reverse_bits(*next_slot, len);
            *next_slot = next_slot.saturating_add(1);
        }
        Ok(Self { codes, lengths })
    }
    fn write_symbol(&self, writer: &mut BitWriter, symbol: u16) -> ZipResult<()> {
        let index = usize::from(symbol);
        let Some(&len) = self.lengths.get(index) else {
            return Err(zip_static("deflate 출력 Huffman symbol 길이 범위 오류"));
        };
        if len == 0 {
            return Err(zip_static("deflate 출력 Huffman symbol code가 없습니다."));
        }
        let Some(&code) = self.codes.get(index) else {
            return Err(zip_static("deflate 출력 Huffman symbol code 범위 오류"));
        };
        writer.write_bits(code, len);
        Ok(())
    }
}
struct InflateState<'bytes> {
    expected_len: usize,
    output: Vec<u8>,
    reader: BitReader<'bytes>,
}
impl InflateState<'_> {
    fn copy_previous(&mut self, distance: usize, length: usize) -> ZipResult<()> {
        if distance == 0 || distance > self.output.len() {
            return Err(zip_static(
                "deflate back-reference distance가 올바르지 않습니다.",
            ));
        }
        reserve_deflate_output(&mut self.output, length, self.expected_len)?;
        for _ in 0..length {
            let source_index = self
                .output
                .len()
                .checked_sub(distance)
                .ok_or_else(|| zip_static("deflate back-reference index 계산 실패"))?;
            let Some(&byte) = self.output.get(source_index) else {
                return Err(zip_static("deflate back-reference 범위 오류"));
            };
            self.output.push(byte);
        }
        Ok(())
    }
    fn decode_distance(&mut self, symbol: u16) -> ZipResult<usize> {
        let index = usize::from(symbol);
        let Some((&base, &extra_bits)) = DISTANCE_BASES
            .get(index)
            .zip(DISTANCE_EXTRA_BITS.get(index))
        else {
            return Err(zip_static("deflate distance symbol 범위 오류"));
        };
        let extra = if extra_bits == 0 {
            0
        } else {
            usize::try_from(self.reader.read_bits(extra_bits)?)
                .map_err(|source| format!("deflate distance extra 변환 실패: {source}"))?
        };
        base.checked_add(extra)
            .ok_or_else(|| zip_static("deflate distance 계산 실패"))
    }
    fn decode_length(&mut self, symbol: u16) -> ZipResult<usize> {
        let index = usize::from(symbol.saturating_sub(257));
        let Some((&base, &extra_bits)) = LENGTH_BASES.get(index).zip(LENGTH_EXTRA_BITS.get(index))
        else {
            return Err(zip_static("deflate length symbol 범위 오류"));
        };
        let extra = if extra_bits == 0 {
            0
        } else {
            usize::try_from(self.reader.read_bits(extra_bits)?)
                .map_err(|source| format!("deflate length extra 변환 실패: {source}"))?
        };
        base.checked_add(extra)
            .ok_or_else(|| zip_static("deflate length 계산 실패"))
    }
    fn dynamic_trees(&mut self) -> ZipResult<(Huffman, Option<Huffman>)> {
        let literal_count = usize::try_from(self.reader.read_bits(5)?)
            .map_err(|source| format!("deflate HLIT 변환 실패: {source}"))?
            .saturating_add(257);
        let distance_count = usize::try_from(self.reader.read_bits(5)?)
            .map_err(|source| format!("deflate HDIST 변환 실패: {source}"))?
            .saturating_add(1);
        let code_length_count = usize::try_from(self.reader.read_bits(4)?)
            .map_err(|source| format!("deflate HCLEN 변환 실패: {source}"))?
            .saturating_add(4);
        let mut code_lengths = [0_u8; 19];
        for &symbol in CODE_LENGTH_ORDER.iter().take(code_length_count) {
            let Some(slot) = code_lengths.get_mut(symbol) else {
                return Err(zip_static("deflate code length symbol 범위 오류"));
            };
            *slot = u8::try_from(self.reader.read_bits(3)?)
                .map_err(|source| format!("deflate code length 변환 실패: {source}"))?;
        }
        let code_tree = Huffman::from_lengths(&code_lengths)?
            .ok_or_else(|| zip_static("deflate code length tree가 비어 있습니다."))?;
        let total = literal_count
            .checked_add(distance_count)
            .ok_or_else(|| zip_static("deflate code length 총합 계산 실패"))?;
        let mut lengths = Vec::new();
        lengths
            .try_reserve(total)
            .map_err(|source| format!("deflate code length 메모리 확보 실패: {source}"))?;
        while lengths.len() < total {
            let symbol = code_tree.decode(&mut self.reader)?;
            match symbol {
                0..=15 => {
                    lengths.push(u8::try_from(symbol).map_err(|source| {
                        format!("deflate code length symbol 변환 실패: {source}")
                    })?);
                }
                16 => {
                    let Some(&previous) = lengths.last() else {
                        return Err(zip_static("deflate repeat code에 이전 길이가 없습니다."));
                    };
                    let repeat = usize::try_from(self.reader.read_bits(2)?)
                        .map_err(|source| format!("deflate repeat 변환 실패: {source}"))?
                        .saturating_add(3);
                    push_repeated(&mut lengths, previous, repeat, total)?;
                }
                17 => {
                    let repeat = usize::try_from(self.reader.read_bits(3)?)
                        .map_err(|source| format!("deflate zero repeat 변환 실패: {source}"))?
                        .saturating_add(3);
                    push_repeated(&mut lengths, 0, repeat, total)?;
                }
                18 => {
                    let repeat = usize::try_from(self.reader.read_bits(7)?)
                        .map_err(|source| format!("deflate long zero repeat 변환 실패: {source}"))?
                        .saturating_add(11);
                    push_repeated(&mut lengths, 0, repeat, total)?;
                }
                _ => {
                    return Err(zip_static(
                        "deflate code length symbol이 올바르지 않습니다.",
                    ));
                }
            }
        }
        let Some((literal_lengths, distance_lengths)) = lengths.split_at_checked(literal_count)
        else {
            return Err(zip_static("deflate literal/distance length 범위 오류"));
        };
        let literal = Huffman::from_lengths(literal_lengths)?
            .ok_or_else(|| zip_static("deflate literal Huffman tree가 비어 있습니다."))?;
        let distance = Huffman::from_lengths(distance_lengths)?;
        Ok((literal, distance))
    }
    fn inflate_compressed_block(
        &mut self,
        literal_tree: &Huffman,
        distance_tree: Option<&Huffman>,
    ) -> ZipResult<()> {
        loop {
            let symbol = literal_tree.decode(&mut self.reader)?;
            match symbol {
                0..=255 => {
                    reserve_deflate_output(&mut self.output, 1, self.expected_len)?;
                    self.output.push(
                        u8::try_from(symbol)
                            .map_err(|source| format!("deflate literal 변환 실패: {source}"))?,
                    );
                }
                256 => return Ok(()),
                257..=285 => {
                    let length = self.decode_length(symbol)?;
                    let Some(distance_huffman) = distance_tree else {
                        return Err(zip_static("deflate distance tree가 없습니다."));
                    };
                    let distance_symbol = distance_huffman.decode(&mut self.reader)?;
                    let distance = self.decode_distance(distance_symbol)?;
                    self.copy_previous(distance, length)?;
                }
                _ => {
                    return Err(zip_static(
                        "deflate literal/length symbol이 올바르지 않습니다.",
                    ));
                }
            }
        }
    }
    fn inflate_stored_block(&mut self) -> ZipResult<()> {
        self.reader.align_to_byte();
        let header = self.reader.read_stored_bytes(4)?;
        let len = read_u16(header, 0)?;
        let nlen = read_u16(header, 2)?;
        if len != !nlen {
            return Err(zip_static(
                "deflate 저장 블록 LEN/NLEN이 일치하지 않습니다.",
            ));
        }
        let stored = self.reader.read_stored_bytes(usize::from(len))?;
        reserve_deflate_output(&mut self.output, stored.len(), self.expected_len)?;
        self.output.extend_from_slice(stored);
        Ok(())
    }
}
impl DeflateInflater<'_> {
    pub(super) fn inflate(&self) -> ZipResult<Vec<u8>> {
        let mut state = InflateState {
            expected_len: self.expected_len,
            output: Vec::new(),
            reader: BitReader {
                bit_buffer: 0,
                bit_count: 0,
                bytes: self.bytes,
                cursor: 0,
            },
        };
        loop {
            let final_block = state.reader.read_bits(1)? != 0;
            let block_type = state.reader.read_bits(2)?;
            match block_type {
                0 => state.inflate_stored_block()?,
                1 => {
                    let literal_lengths: [u8; FIXED_LITERAL_SYMBOLS] =
                        from_fn(|symbol| match symbol {
                            0..=143 | 280..=287 => 8,
                            144..=255 => 9,
                            256..=279 => 7,
                            _ => 0,
                        });
                    let distance_lengths = [5_u8; FIXED_DISTANCE_SYMBOLS];
                    let literal = Huffman::from_lengths(&literal_lengths)?
                        .ok_or_else(|| zip_static("fixed literal Huffman tree 생성 실패"))?;
                    let distance = Huffman::from_lengths(&distance_lengths)?
                        .ok_or_else(|| zip_static("fixed distance Huffman tree 생성 실패"))?;
                    state.inflate_compressed_block(&literal, Some(&distance))?;
                }
                2 => {
                    let (literal, distance) = state.dynamic_trees()?;
                    state.inflate_compressed_block(&literal, distance.as_ref())?;
                }
                _ => return Err(zip_static("지원하지 않는 deflate block type입니다.")),
            }
            if final_block {
                return Ok(state.output);
            }
        }
    }
}
impl CodeLengthTokenizer<'_> {
    fn tokens(&self) -> ZipResult<Vec<CodeLengthToken>> {
        let mut tokens = Vec::new();
        let mut index = 0_usize;
        while index < self.lengths.len() {
            let Some(&value) = self.lengths.get(index) else {
                break;
            };
            if value == 0 {
                let mut run = 1_usize;
                while self
                    .lengths
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
                while self
                    .lengths
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
}
impl DynamicDeflateWriter<'_> {
    fn deflate(&self) -> ZipResult<Option<Vec<u8>>> {
        let Some((literal_freq, distance_freq)) = DynamicFrequencyCounter {
            tokens: self.tokens,
        }
        .count()?
        else {
            return Ok(None);
        };
        let Some(literal_lengths) = (HuffmanLengthBuilder {
            frequencies: &literal_freq,
            max_bits: DEFLATE_MAX_BITS_U8,
        })
        .build() else {
            return Ok(None);
        };
        let Some(distance_lengths) = (HuffmanLengthBuilder {
            frequencies: &distance_freq,
            max_bits: DEFLATE_MAX_BITS_U8,
        })
        .build() else {
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
        let mut combined_lengths = Vec::new();
        combined_lengths
            .try_reserve(literal_count.saturating_add(distance_count))
            .map_err(|source| format!("deflate combined length 메모리 확보 실패: {source}"))?;
        let Some((literal_prefix, _)) = literal_lengths.split_at_checked(literal_count) else {
            return Err(zip_static("deflate literal length 범위 오류"));
        };
        let Some((distance_prefix, _)) = distance_lengths.split_at_checked(distance_count) else {
            return Err(zip_static("deflate distance length 범위 오류"));
        };
        combined_lengths.extend_from_slice(literal_prefix);
        combined_lengths.extend_from_slice(distance_prefix);
        let code_length_tokens = CodeLengthTokenizer {
            lengths: &combined_lengths,
        }
        .tokens()?;
        let mut code_length_freq = [0_u32; CODE_LENGTH_SYMBOLS];
        for token in &code_length_tokens {
            let Some(freq) = code_length_freq.get_mut(usize::from(token.symbol)) else {
                return Err(zip_static("deflate code length frequency 범위 오류"));
            };
            *freq = freq.saturating_add(1);
        }
        let Some(code_lengths) = (HuffmanLengthBuilder {
            frequencies: &code_length_freq,
            max_bits: 7,
        })
        .build() else {
            return Ok(None);
        };
        let mut code_length_count = 4_usize;
        for (index, &symbol) in CODE_LENGTH_ORDER.iter().enumerate().rev() {
            let len = code_lengths
                .get(symbol)
                .copied()
                .ok_or_else(|| zip_static("deflate code length order 범위 오류"))?;
            if len != 0 {
                code_length_count = index.saturating_add(1).max(4);
                break;
            }
        }
        let literal_huffman = WriteHuffman::from_lengths(literal_lengths)?;
        let distance_huffman = WriteHuffman::from_lengths(distance_lengths)?;
        let code_huffman = WriteHuffman::from_lengths(code_lengths)?;
        let mut writer = BitWriter::with_capacity(self.tokens.len(), "deflate dynamic 출력")?;
        DynamicBlockHeaderWriter {
            code_length_count,
            code_lengths: &code_huffman.lengths,
            distance_count,
            literal_count,
            writer: &mut writer,
        }
        .write()?;
        for token in code_length_tokens {
            code_huffman.write_symbol(&mut writer, u16::from(token.symbol))?;
            if token.extra_bits > 0 {
                writer.write_bits(token.extra, token.extra_bits);
            }
        }
        for &token in self.tokens {
            DynamicTokenWriter {
                distance_huffman: &distance_huffman,
                literal_huffman: &literal_huffman,
                writer: &mut writer,
            }
            .write(token)?;
        }
        literal_huffman.write_symbol(&mut writer, 256)?;
        Ok(Some(writer.finish()))
    }
}
impl DynamicBlockHeaderWriter<'_, '_> {
    fn write(&mut self) -> ZipResult<()> {
        self.writer.write_bits(1, 1);
        self.writer.write_bits(2, 2);
        self.writer.write_bits(
            u16::try_from(self.literal_count.saturating_sub(257))
                .map_err(|source| format!("deflate HLIT 변환 실패: {source}"))?,
            5,
        );
        self.writer.write_bits(
            u16::try_from(self.distance_count.saturating_sub(1))
                .map_err(|source| format!("deflate HDIST 변환 실패: {source}"))?,
            5,
        );
        self.writer.write_bits(
            u16::try_from(self.code_length_count.saturating_sub(4))
                .map_err(|source| format!("deflate HCLEN 변환 실패: {source}"))?,
            4,
        );
        for &symbol in CODE_LENGTH_ORDER.iter().take(self.code_length_count) {
            let len = self
                .code_lengths
                .get(symbol)
                .copied()
                .ok_or_else(|| zip_static("deflate code length 쓰기 범위 오류"))?;
            self.writer.write_bits(u16::from(len), 3);
        }
        Ok(())
    }
}
impl DynamicFrequencyCounter<'_> {
    fn count(&self) -> ZipResult<Option<([u32; LITERAL_LENGTH_SYMBOLS], [u32; DISTANCE_SYMBOLS])>> {
        let mut literal_freq = [0_u32; LITERAL_LENGTH_SYMBOLS];
        let mut distance_freq = [0_u32; DISTANCE_SYMBOLS];
        let Some(end_freq) = literal_freq.get_mut(256) else {
            return Err(zip_static("deflate end symbol 범위 오류"));
        };
        *end_freq = 1;
        let mut has_distance = false;
        for &token in self.tokens {
            match token {
                DeflateToken::Literal(byte) => {
                    let Some(freq) = literal_freq.get_mut(usize::from(byte)) else {
                        return Err(zip_static("deflate literal frequency 범위 오류"));
                    };
                    *freq = freq.saturating_add(1);
                }
                DeflateToken::Match { distance, length } => {
                    let Some((length_symbol, _, _)) = DeflateWriter::length_symbol(length) else {
                        return Err(zip_static("deflate length 범위 오류"));
                    };
                    let Some(length_freq) = literal_freq.get_mut(usize::from(length_symbol)) else {
                        return Err(zip_static("deflate length frequency 범위 오류"));
                    };
                    *length_freq = length_freq.saturating_add(1);
                    let Some((distance_symbol, _, _)) = DeflateWriter::distance_symbol(distance)
                    else {
                        return Err(zip_static("deflate distance 범위 오류"));
                    };
                    let Some(distance_freq_slot) =
                        distance_freq.get_mut(usize::from(distance_symbol))
                    else {
                        return Err(zip_static("deflate distance frequency 범위 오류"));
                    };
                    *distance_freq_slot = distance_freq_slot.saturating_add(1);
                    has_distance = true;
                }
            }
        }
        Ok(has_distance.then_some((literal_freq, distance_freq)))
    }
}
impl DynamicTokenWriter<'_, '_> {
    fn write(&mut self, token: DeflateToken) -> ZipResult<()> {
        match token {
            DeflateToken::Literal(byte) => self
                .literal_huffman
                .write_symbol(self.writer, u16::from(byte)),
            DeflateToken::Match { distance, length } => {
                let Some((length_symbol, length_extra_bits, length_extra)) =
                    DeflateWriter::length_symbol(length)
                else {
                    return Err(zip_static("deflate length 범위 오류"));
                };
                self.literal_huffman
                    .write_symbol(self.writer, length_symbol)?;
                if length_extra_bits > 0 {
                    self.writer.write_bits(length_extra, length_extra_bits);
                }
                let Some((distance_symbol, distance_extra_bits, distance_extra)) =
                    DeflateWriter::distance_symbol(distance)
                else {
                    return Err(zip_static("deflate distance 범위 오류"));
                };
                self.distance_huffman
                    .write_symbol(self.writer, distance_symbol)?;
                if distance_extra_bits > 0 {
                    self.writer.write_bits(distance_extra, distance_extra_bits);
                }
                Ok(())
            }
        }
    }
}
impl FixedDeflateWriter<'_> {
    fn deflate(&self) -> ZipResult<Vec<u8>> {
        let mut writer =
            BitWriter::with_capacity(self.byte_len.saturating_div(2), "deflate fixed 출력")?;
        writer.write_bits(1, 1);
        writer.write_bits(1, 2);
        for &token in self.tokens {
            FixedTokenWriter {
                writer: &mut writer,
            }
            .write_token(token)?;
        }
        FixedTokenWriter {
            writer: &mut writer,
        }
        .write_symbol(256)?;
        Ok(writer.finish())
    }
}
impl FixedTokenWriter<'_> {
    fn write_distance(&mut self, distance: usize) -> ZipResult<()> {
        let Some((symbol, extra_bits, extra)) = DeflateWriter::distance_symbol(distance) else {
            return Err(zip_static("deflate distance 범위 오류"));
        };
        self.writer.write_bits(reverse_bits(symbol, 5), 5);
        if extra_bits > 0 {
            self.writer.write_bits(extra, extra_bits);
        }
        Ok(())
    }
    fn write_length(&mut self, length: usize) -> ZipResult<()> {
        let Some((symbol, extra_bits, extra)) = DeflateWriter::length_symbol(length) else {
            return Err(zip_static("deflate length 범위 오류"));
        };
        self.write_symbol(symbol)?;
        if extra_bits > 0 {
            self.writer.write_bits(extra, extra_bits);
        }
        Ok(())
    }
    fn write_symbol(&mut self, symbol: u16) -> ZipResult<()> {
        let (code, bit_count) = match symbol {
            0..=143 => (0x30_u16.saturating_add(symbol), 8_u8),
            144..=255 => (0x190_u16.saturating_add(symbol.saturating_sub(144)), 9_u8),
            256..=279 => (symbol.saturating_sub(256), 7_u8),
            280..=287 => (0xc0_u16.saturating_add(symbol.saturating_sub(280)), 8_u8),
            _ => return Err(zip_static("deflate fixed symbol 범위 오류")),
        };
        self.writer
            .write_bits(reverse_bits(code, bit_count), bit_count);
        Ok(())
    }
    fn write_token(&mut self, token: DeflateToken) -> ZipResult<()> {
        match token {
            DeflateToken::Literal(byte) => self.write_symbol(u16::from(byte)),
            DeflateToken::Match { distance, length } => {
                self.write_length(length)?;
                self.write_distance(distance)
            }
        }
    }
}
impl HuffmanLengthBuilder<'_> {
    fn build(&self) -> Option<Vec<u8>> {
        let mut lengths = Vec::new();
        lengths.try_reserve(self.frequencies.len()).ok()?;
        lengths.resize(self.frequencies.len(), 0_u8);
        let mut leaf_lengths = self.leaf_lengths()?;
        if leaf_lengths.is_empty() {
            return None;
        }
        if leaf_lengths.len() == 1 {
            let symbol = leaf_lengths.first()?.symbol;
            *lengths.get_mut(symbol)? = 1;
            return Some(lengths);
        }
        let longest_len = leaf_lengths.iter().map(|leaf| leaf.len).max()?;
        let max_bit_count = usize::from(self.max_bits);
        if longest_len <= max_bit_count {
            for leaf in leaf_lengths {
                *lengths.get_mut(leaf.symbol)? = u8::try_from(leaf.len).ok()?;
            }
            return Some(lengths);
        }
        let length_counts = self.limited_length_counts(&leaf_lengths, longest_len)?;
        LimitedHuffmanLengthAssigner {
            length_counts: &length_counts,
            max_bit_count,
        }
        .assign(&mut lengths, &mut leaf_lengths)?;
        Some(lengths)
    }
    fn leaf_lengths(&self) -> Option<Vec<HuffmanLeafLength>> {
        let mut nodes = Vec::new();
        let mut leaves = Vec::new();
        let mut active = Vec::new();
        let node_capacity = self.frequencies.len().checked_mul(2)?.saturating_sub(1);
        nodes.try_reserve(node_capacity).ok()?;
        leaves.try_reserve(self.frequencies.len()).ok()?;
        active.try_reserve(self.frequencies.len()).ok()?;
        for (symbol, &freq) in self.frequencies.iter().enumerate() {
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
        if active.len() == 1 {
            let (symbol, _) = *leaves.first()?;
            let freq = *self.frequencies.get(symbol)?;
            return Some(vec![HuffmanLeafLength {
                freq,
                len: 1,
                symbol,
            }]);
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
        self.leaf_lengths_from_nodes(&leaves, &nodes)
    }
    fn leaf_lengths_from_nodes(
        &self,
        leaves: &[(usize, usize)],
        nodes: &[HuffmanBuildNode],
    ) -> Option<Vec<HuffmanLeafLength>> {
        let mut leaf_lengths = Vec::new();
        leaf_lengths.try_reserve(leaves.len()).ok()?;
        for &(symbol, node_index) in leaves {
            let mut bit_len = 0_usize;
            let mut cursor = node_index;
            while let Some(parent) = nodes.get(cursor)?.parent {
                bit_len = bit_len.saturating_add(1);
                cursor = parent;
            }
            if bit_len == 0 {
                return None;
            }
            leaf_lengths.push(HuffmanLeafLength {
                freq: *self.frequencies.get(symbol)?,
                len: bit_len,
                symbol,
            });
        }
        Some(leaf_lengths)
    }
    fn limited_length_counts(
        &self,
        leaf_lengths: &[HuffmanLeafLength],
        longest_len: usize,
    ) -> Option<Vec<usize>> {
        let max_bit_count = usize::from(self.max_bits);
        let mut length_counts = Vec::new();
        length_counts
            .try_reserve(longest_len.saturating_add(1))
            .ok()?;
        length_counts.resize(longest_len.saturating_add(1), 0_usize);
        for leaf in leaf_lengths {
            let count = length_counts.get_mut(leaf.len)?;
            *count = count.saturating_add(1);
        }
        let mut current_bits = longest_len;
        while current_bits > max_bit_count {
            while length_counts.get(current_bits).copied()? > 0 {
                LimitedLengthCountReducer {
                    length_counts: &mut length_counts,
                }
                .reduce(current_bits)?;
            }
            current_bits = current_bits.checked_sub(1)?;
        }
        Some(length_counts)
    }
}
impl LimitedHuffmanLengthAssigner<'_> {
    fn assign(&self, lengths: &mut [u8], leaf_lengths: &mut [HuffmanLeafLength]) -> Option<()> {
        leaf_lengths.sort_unstable_by(|left, right| {
            left.freq
                .cmp(&right.freq)
                .then_with(|| right.symbol.cmp(&left.symbol))
        });
        let mut symbol_index = 0_usize;
        for bit_len in (1..=self.max_bit_count).rev() {
            let count = self.length_counts.get(bit_len).copied()?;
            for _ in 0..count {
                let leaf = leaf_lengths.get(symbol_index)?;
                *lengths.get_mut(leaf.symbol)? = u8::try_from(bit_len).ok()?;
                symbol_index = symbol_index.saturating_add(1);
            }
        }
        (symbol_index == leaf_lengths.len()).then_some(())
    }
}
impl LimitedLengthCountReducer<'_> {
    fn reduce(&mut self, current_bits: usize) -> Option<()> {
        let mut shorter_bits = current_bits.checked_sub(2)?;
        while shorter_bits > 0 && self.length_counts.get(shorter_bits).copied()? == 0 {
            shorter_bits = shorter_bits.checked_sub(1)?;
        }
        if shorter_bits == 0 {
            return None;
        }
        let count = self.length_counts.get_mut(current_bits)?;
        *count = count.checked_sub(2)?;
        let previous_count = self.length_counts.get_mut(current_bits.checked_sub(1)?)?;
        *previous_count = previous_count.checked_add(1)?;
        let split_count = self.length_counts.get_mut(shorter_bits.saturating_add(1))?;
        *split_count = split_count.checked_add(2)?;
        let shorter_count = self.length_counts.get_mut(shorter_bits)?;
        *shorter_count = shorter_count.checked_sub(1)?;
        Some(())
    }
}
impl DeflateWriter<'_> {
    fn best_match(
        bytes: &[u8],
        position: usize,
        head: &[usize],
        previous: &[usize],
        profile: DeflateProfile,
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
            && chain_len < profile.max_chain
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
                if len >= profile.nice_match_len {
                    break;
                }
            }
            candidate = *previous.get(candidate)?;
            chain_len = chain_len.saturating_add(1);
        }
        (best_len >= MIN_MATCH).then_some((best_len, best_distance))
    }
    pub(super) fn deflate(&self) -> ZipResult<Vec<u8>> {
        let tokens = self.tokens()?;
        let fixed = FixedDeflateWriter {
            byte_len: self.bytes.len(),
            tokens: &tokens,
        }
        .deflate()?;
        match (DynamicDeflateWriter { tokens: &tokens }).deflate()? {
            Some(dynamic) if dynamic.len() < fixed.len() => Ok(dynamic),
            _ => Ok(fixed),
        }
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
    fn tokens(&self) -> ZipResult<Vec<DeflateToken>> {
        let has_bom = self.bytes.starts_with(UTF8_BOM);
        let mut xml_cursor = if has_bom { UTF8_BOM.len() } else { 0 };
        while self
            .bytes
            .get(xml_cursor)
            .is_some_and(u8::is_ascii_whitespace)
        {
            xml_cursor = xml_cursor.saturating_add(1);
        }
        let looks_like_xlsx_xml = self.bytes.get(xml_cursor..).is_some_and(|tail| {
            (tail.starts_with(b"<?xml") || tail.starts_with(b"<"))
                && XLSX_XML_NEEDLES
                    .iter()
                    .any(|needle| tail.windows(needle.len()).any(|window| window == *needle))
        });
        let nice_match_len = if looks_like_xlsx_xml {
            XML_NICE_MATCH_LEN
        } else {
            MAX_MATCH
        };
        let profile = DeflateProfile {
            max_chain: MAX_CHAIN,
            nice_match_len,
        };
        let mut tokens = Vec::new();
        tokens
            .try_reserve(self.bytes.len())
            .map_err(|source| format!("deflate token 메모리 확보 실패: {source}"))?;
        let mut head = Vec::new();
        head.try_reserve_exact(HASH_SIZE)
            .map_err(|source| format!("deflate hash head 메모리 확보 실패: {source}"))?;
        head.resize(HASH_SIZE, usize::MAX);
        let mut previous = Vec::new();
        previous
            .try_reserve_exact(self.bytes.len())
            .map_err(|source| format!("deflate hash previous 메모리 확보 실패: {source}"))?;
        previous.resize(self.bytes.len(), usize::MAX);
        let mut position = 0_usize;
        while position < self.bytes.len() {
            if let Some((length, distance)) =
                Self::best_match(self.bytes, position, &head, &previous, profile)
            {
                if length == MIN_MATCH && distance > TOO_FAR_MATCH_DISTANCE {
                    let Some(&byte) = self.bytes.get(position) else {
                        return Err(zip_static("deflate literal 범위 오류"));
                    };
                    tokens.push(DeflateToken::Literal(byte));
                    Self::insert_position(self.bytes, position, &mut head, &mut previous);
                    position = position
                        .checked_add(1)
                        .ok_or_else(|| zip_static("deflate 위치 계산 실패"))?;
                    continue;
                }
                let mut inserted_current = false;
                let lazy_literal = if length < MAX_MATCH
                    && let Some(next_position) = position.checked_add(1)
                    && next_position < self.bytes.len()
                {
                    Self::insert_position(self.bytes, position, &mut head, &mut previous);
                    inserted_current = true;
                    Self::best_match(self.bytes, next_position, &head, &previous, profile)
                        .is_some_and(|(next_length, _)| next_length > length)
                } else {
                    false
                };
                if lazy_literal {
                    let Some(&byte) = self.bytes.get(position) else {
                        return Err(zip_static("deflate literal 범위 오류"));
                    };
                    tokens.push(DeflateToken::Literal(byte));
                    position = position
                        .checked_add(1)
                        .ok_or_else(|| zip_static("deflate 위치 계산 실패"))?;
                    continue;
                }
                tokens.push(DeflateToken::Match { distance, length });
                let next_position = position
                    .checked_add(length)
                    .ok_or_else(|| zip_static("deflate 위치 계산 실패"))?;
                let insert_start = if inserted_current {
                    position
                        .checked_add(1)
                        .ok_or_else(|| zip_static("deflate 위치 계산 실패"))?
                } else {
                    position
                };
                for insert_position in insert_start..next_position {
                    Self::insert_position(self.bytes, insert_position, &mut head, &mut previous);
                }
                position = next_position;
            } else {
                let Some(&byte) = self.bytes.get(position) else {
                    return Err(zip_static("deflate literal 범위 오류"));
                };
                tokens.push(DeflateToken::Literal(byte));
                Self::insert_position(self.bytes, position, &mut head, &mut previous);
                position = position.saturating_add(1);
            }
        }
        Ok(tokens)
    }
}
fn reserve_deflate_output(
    output: &mut Vec<u8>,
    additional_len: usize,
    expected_len: usize,
) -> ZipResult<()> {
    let next_len = output
        .len()
        .checked_add(additional_len)
        .ok_or_else(|| zip_static("deflate 출력 크기 계산 실패"))?;
    if next_len > expected_len {
        return Err(zip_static(
            "deflate 출력이 ZIP 선언 해제 크기를 초과했습니다.",
        ));
    }
    output
        .try_reserve(additional_len)
        .map_err(|source| format!("deflate 출력 메모리 확보 실패: {source}").into())
}
fn hash3(bytes: &[u8], position: usize) -> Option<usize> {
    let &[first_byte, second_byte, third_byte] = bytes.get(position..)?.first_chunk::<3>()?;
    let first = usize::from(first_byte);
    let second = usize::from(second_byte);
    let third = usize::from(third_byte);
    Some(((first << 10_usize) ^ (second << 5_usize) ^ third) & HASH_SIZE.saturating_sub(1))
}
fn push_repeated(lengths: &mut Vec<u8>, value: u8, repeat: usize, total: usize) -> ZipResult<()> {
    let next_len = lengths
        .len()
        .checked_add(repeat)
        .ok_or_else(|| zip_static("deflate repeat 길이 계산 실패"))?;
    if next_len > total {
        return Err(zip_static(
            "deflate repeat 길이가 code length 총합을 초과합니다.",
        ));
    }
    lengths.extend(repeat_n(value, repeat));
    Ok(())
}
fn reverse_bits(mut value: u16, count: u8) -> u16 {
    let mut out = 0_u16;
    for _ in 0_u8..count {
        out = (out << 1_u8) | (value & 1_u16);
        value >>= 1_u8;
    }
    out
}
