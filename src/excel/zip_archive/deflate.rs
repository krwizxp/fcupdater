use super::{
    CODE_LENGTH_ORDER, CODE_LENGTH_SYMBOLS, DEFLATE_MAX_BITS, DEFLATE_MAX_BITS_U8, DISTANCE_BASES,
    DISTANCE_EXTRA_BITS, DISTANCE_SYMBOLS, FIXED_DISTANCE_SYMBOLS, FIXED_LITERAL_SYMBOLS,
    HASH_SIZE, LENGTH_BASES, LENGTH_EXTRA_BITS, LITERAL_LENGTH_SYMBOLS, MAX_CHAIN, MAX_MATCH,
    MIN_MATCH, ZipResult, read_u16, zip_static,
};
use alloc::vec::Vec;
use core::{array::from_fn, cmp::Ordering, iter::repeat_n};
pub(super) struct BitReader<'a> {
    bit_buffer: u32,
    bit_count: u8,
    bytes: &'a [u8],
    cursor: usize,
}
pub(super) struct Huffman {
    codes: [Vec<HuffmanCode>; DEFLATE_MAX_BITS + 1],
}
pub(super) struct WriteHuffman {
    codes: Vec<u16>,
    lengths: Vec<u8>,
}
pub(super) struct BitWriter {
    bit_buffer: u8,
    bit_count: u8,
    bytes: Vec<u8>,
}
#[derive(Clone, Copy)]
pub(super) enum DeflateToken {
    Literal(u8),
    Match { distance: usize, length: usize },
}
#[derive(Clone)]
struct HuffmanCode {
    code: u16,
    symbol: u16,
}
#[derive(Clone, Copy)]
pub(super) struct CodeLengthToken {
    extra: u16,
    extra_bits: u8,
    symbol: u8,
}
struct HuffmanBuildNode {
    freq: u64,
    parent: Option<usize>,
}
pub(super) struct DeflateInflater;
pub(super) struct DeflateWriter;
pub(super) trait DeflateInflaterExt {
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
pub(super) trait DeflateWriterExt {
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
        let end = self
            .cursor
            .checked_add(len)
            .ok_or_else(|| zip_static("deflate 저장 블록 크기 계산 실패"))?;
        let Some(bytes) = self.bytes.get(self.cursor..end) else {
            return Err(zip_static("deflate 저장 블록이 입력보다 깁니다."));
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
        let mut codes: [Vec<HuffmanCode>; DEFLATE_MAX_BITS + 1] = from_fn(|_| Vec::new());
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
impl DeflateInflaterExt for DeflateInflater {
    fn copy_previous(
        output: &mut Vec<u8>,
        distance: usize,
        length: usize,
        expected_len: usize,
    ) -> ZipResult<()> {
        if distance == 0 || distance > output.len() {
            return Err(zip_static(
                "deflate back-reference distance가 올바르지 않습니다.",
            ));
        }
        reserve_deflate_output(output, length, expected_len)?;
        for _ in 0..length {
            let source_index = output
                .len()
                .checked_sub(distance)
                .ok_or_else(|| zip_static("deflate back-reference index 계산 실패"))?;
            let Some(&byte) = output.get(source_index) else {
                return Err(zip_static("deflate back-reference 범위 오류"));
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
            return Err(zip_static("deflate distance symbol 범위 오류"));
        };
        let extra = if extra_bits == 0 {
            0
        } else {
            usize::try_from(reader.read_bits(extra_bits)?)
                .map_err(|source| format!("deflate distance extra 변환 실패: {source}"))?
        };
        base.checked_add(extra)
            .ok_or_else(|| zip_static("deflate distance 계산 실패"))
    }
    fn decode_length(symbol: u16, reader: &mut BitReader<'_>) -> ZipResult<usize> {
        let index = usize::from(symbol.saturating_sub(257));
        let Some((&base, &extra_bits)) = LENGTH_BASES.get(index).zip(LENGTH_EXTRA_BITS.get(index))
        else {
            return Err(zip_static("deflate length symbol 범위 오류"));
        };
        let extra = if extra_bits == 0 {
            0
        } else {
            usize::try_from(reader.read_bits(extra_bits)?)
                .map_err(|source| format!("deflate length extra 변환 실패: {source}"))?
        };
        base.checked_add(extra)
            .ok_or_else(|| zip_static("deflate length 계산 실패"))
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
                return Err(zip_static("deflate code length symbol 범위 오류"));
            };
            *slot = u8::try_from(reader.read_bits(3)?)
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
            let symbol = code_tree.decode(reader)?;
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
                    return Err(zip_static(
                        "deflate code length symbol이 올바르지 않습니다.",
                    ));
                }
            }
        }
        let literal_lengths = lengths
            .get(..literal_count)
            .ok_or_else(|| zip_static("deflate literal length 범위 오류"))?;
        let distance_lengths = lengths
            .get(literal_count..)
            .ok_or_else(|| zip_static("deflate distance length 범위 오류"))?;
        let literal = Huffman::from_lengths(literal_lengths)?
            .ok_or_else(|| zip_static("deflate literal Huffman tree가 비어 있습니다."))?;
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
            .ok_or_else(|| zip_static("fixed literal Huffman tree 생성 실패"))?;
        let distance = Huffman::from_lengths(&distance_lengths)?
            .ok_or_else(|| zip_static("fixed distance Huffman tree 생성 실패"))?;
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
                _ => return Err(zip_static("지원하지 않는 deflate block type입니다.")),
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
                        return Err(zip_static("deflate distance tree가 없습니다."));
                    };
                    let distance_symbol = distance_huffman.decode(reader)?;
                    let distance = Self::decode_distance(distance_symbol, reader)?;
                    Self::copy_previous(output, distance, length, expected_len)?;
                }
                _ => {
                    return Err(zip_static(
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
            return Err(zip_static(
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
        let mut combined_lengths = Vec::new();
        combined_lengths
            .try_reserve(literal_count.saturating_add(distance_count))
            .map_err(|source| format!("deflate combined length 메모리 확보 실패: {source}"))?;
        combined_lengths.extend_from_slice(
            literal_lengths
                .get(..literal_count)
                .ok_or_else(|| zip_static("deflate literal length 범위 오류"))?,
        );
        combined_lengths.extend_from_slice(
            distance_lengths
                .get(..distance_count)
                .ok_or_else(|| zip_static("deflate distance length 범위 오류"))?,
        );
        let code_length_tokens = Self::code_length_tokens(&combined_lengths)?;
        let mut code_length_freq = [0_u32; CODE_LENGTH_SYMBOLS];
        for token in &code_length_tokens {
            let Some(freq) = code_length_freq.get_mut(usize::from(token.symbol)) else {
                return Err(zip_static("deflate code length frequency 범위 오류"));
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
                .ok_or_else(|| zip_static("deflate code length order 범위 오류"))?;
            if len != 0 {
                code_length_count = index.saturating_add(1).max(4);
                break;
            }
        }
        let literal_huffman = WriteHuffman::from_lengths(literal_lengths)?;
        let distance_huffman = WriteHuffman::from_lengths(distance_lengths)?;
        let code_huffman = WriteHuffman::from_lengths(code_lengths.clone())?;
        let mut writer = BitWriter::with_capacity(tokens.len(), "deflate dynamic 출력")?;
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
                .ok_or_else(|| zip_static("deflate code length 쓰기 범위 오류"))?;
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
        let mut writer =
            BitWriter::with_capacity(byte_len.saturating_div(2), "deflate fixed 출력")?;
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
            return Err(zip_static("deflate end symbol 범위 오류"));
        };
        *end_freq = 1;
        let mut has_distance = false;
        for &token in tokens {
            match token {
                DeflateToken::Literal(byte) => {
                    let Some(freq) = literal_freq.get_mut(usize::from(byte)) else {
                        return Err(zip_static("deflate literal frequency 범위 오류"));
                    };
                    *freq = freq.saturating_add(1);
                }
                DeflateToken::Match { distance, length } => {
                    let Some((length_symbol, _, _)) = Self::length_symbol(length) else {
                        return Err(zip_static("deflate length 범위 오류"));
                    };
                    let Some(length_freq) = literal_freq.get_mut(usize::from(length_symbol)) else {
                        return Err(zip_static("deflate length frequency 범위 오류"));
                    };
                    *length_freq = length_freq.saturating_add(1);
                    let Some((distance_symbol, _, _)) = Self::distance_symbol(distance) else {
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
    fn huffman_lengths(frequencies: &[u32], max_bits: u8) -> Option<Vec<u8>> {
        let mut lengths = Vec::new();
        lengths.try_reserve(frequencies.len()).ok()?;
        lengths.resize(frequencies.len(), 0_u8);
        let mut nodes = Vec::new();
        let mut leaves = Vec::new();
        let mut active = Vec::new();
        let node_capacity = frequencies.len().checked_mul(2)?.saturating_sub(1);
        nodes.try_reserve(node_capacity).ok()?;
        leaves.try_reserve(frequencies.len()).ok()?;
        active.try_reserve(frequencies.len()).ok()?;
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
        let mut tokens = Vec::new();
        tokens
            .try_reserve(bytes.len())
            .map_err(|source| format!("deflate token 메모리 확보 실패: {source}"))?;
        let mut head = Vec::new();
        head.try_reserve_exact(HASH_SIZE)
            .map_err(|source| format!("deflate hash head 메모리 확보 실패: {source}"))?;
        head.resize(HASH_SIZE, usize::MAX);
        let mut previous = Vec::new();
        previous
            .try_reserve_exact(bytes.len())
            .map_err(|source| format!("deflate hash previous 메모리 확보 실패: {source}"))?;
        previous.resize(bytes.len(), usize::MAX);
        let mut position = 0_usize;
        while position < bytes.len() {
            if let Some((length, distance)) = Self::best_match(bytes, position, &head, &previous) {
                tokens.push(DeflateToken::Match { distance, length });
                let next_position = position
                    .checked_add(length)
                    .ok_or_else(|| zip_static("deflate 위치 계산 실패"))?;
                for insert_position in position..next_position {
                    Self::insert_position(bytes, insert_position, &mut head, &mut previous);
                }
                position = next_position;
            } else {
                let Some(&byte) = bytes.get(position) else {
                    return Err(zip_static("deflate literal 범위 오류"));
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
            return Err(zip_static("deflate distance 범위 오류"));
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
                    return Err(zip_static("deflate length 범위 오류"));
                };
                literal_huffman.write_symbol(writer, length_symbol)?;
                if length_extra_bits > 0 {
                    writer.write_bits(length_extra, length_extra_bits);
                }
                let Some((distance_symbol, distance_extra_bits, distance_extra)) =
                    Self::distance_symbol(distance)
                else {
                    return Err(zip_static("deflate distance 범위 오류"));
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
            _ => return Err(zip_static("deflate fixed symbol 범위 오류")),
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
            return Err(zip_static("deflate length 범위 오류"));
        };
        Self::write_fixed_symbol(writer, symbol)?;
        if extra_bits > 0 {
            writer.write_bits(extra, extra_bits);
        }
        Ok(())
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
