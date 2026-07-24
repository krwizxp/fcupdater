use super::{
    CODE_LENGTH_ORDER, CODE_LENGTH_SYMBOLS, DEFLATE_MAX_BITS, DEFLATE_MAX_BITS_U8, DISTANCE_BASES,
    DISTANCE_EXTRA_BITS, DISTANCE_SYMBOLS, FIXED_DISTANCE_SYMBOLS, FIXED_LITERAL_SYMBOLS,
    HASH_SIZE, LENGTH_BASES, LENGTH_EXTRA_BITS, LITERAL_LENGTH_SYMBOLS, MAX_CHAIN, MAX_MATCH,
    MIN_MATCH, ZipResult, read_u16, zip_static, zip_with_source,
};
use core::{array::from_fn, iter::repeat_n, mem, range::Range};
use std::io::Write as IoWrite;
#[cfg(target_arch = "x86_64")]
macro_rules! matching_prefix_16 {
    ($left:expr, $right:expr) => {{
        use core::arch::x86_64::{_mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8};
        // SAFETY: The caller keeps the complete 16-byte candidate range inside the input slice.
        let left_vector = unsafe { _mm_loadu_si128($left.cast()) };
        // SAFETY: The caller keeps the complete 16-byte current range inside the input slice.
        let right_vector = unsafe { _mm_loadu_si128($right.cast()) };
        // SAFETY: SSE2 is part of the x86-64 baseline, and both operands are complete vectors.
        let equal = unsafe { _mm_cmpeq_epi8(left_vector, right_vector) };
        // SAFETY: SSE2 is part of the x86-64 baseline, and equal is a complete vector.
        let mismatch_mask = !unsafe { _mm_movemask_epi8(equal) } & 0xffff_i32;
        let [prefix, ..] = mismatch_mask.trailing_zeros().to_le_bytes();
        usize::from(prefix).min(SIMD_MATCH_BYTES)
    }};
}
#[cfg(target_arch = "aarch64")]
macro_rules! matching_prefix_16 {
    ($left:expr, $right:expr) => {{
        use core::arch::aarch64::{vceqq_u8, vld1q_u8, vminvq_u8, vst1q_u8};
        // SAFETY: The caller keeps the complete unaligned 16-byte candidate range inside the input slice.
        let left_vector = unsafe { vld1q_u8($left) };
        // SAFETY: The caller keeps the complete unaligned 16-byte current range inside the input slice.
        let right_vector = unsafe { vld1q_u8($right) };
        // SAFETY: Advanced SIMD is part of the AArch64 baseline, and both operands are complete vectors.
        let equal = unsafe { vceqq_u8(left_vector, right_vector) };
        // SAFETY: Advanced SIMD is part of the AArch64 baseline, and equal is a complete vector.
        if unsafe { vminvq_u8(equal) } == u8::MAX {
            SIMD_MATCH_BYTES
        } else {
            let mut lanes = [0_u8; SIMD_MATCH_BYTES];
            // SAFETY: lanes owns exactly 16 writable bytes required by the NEON store.
            unsafe { vst1q_u8(lanes.as_mut_ptr(), equal); }
            lanes
                .iter()
                .position(|lane| *lane == 0)
                .unwrap_or(SIMD_MATCH_BYTES)
        }
    }};
}
const TOO_FAR_MATCH_DISTANCE: usize = 4096;
const CALC_CHAIN_BLOCK_BYTES: usize = 96 * 1024;
const DEFLATE_SEARCH_WORK_LIMIT: usize = 512 * 1024 * 1024;
const DEFLATE_STREAM_BUFFER_LEN: usize = 8192;
const EXCEL_FLUSH_BLOCK_COUNT: u8 = 2;
const SHARED_STRINGS_BLOCK_BYTES: usize = 76 * 1024;
const SIMD_MATCH_BYTES: usize = 16;
const WORKSHEET_BLOCK_BYTES: usize = 144 * 1024;
const XML_NICE_MATCH_LEN: usize = 128;
struct BitReader<'bytes> {
    bit_buffer: u32,
    bit_count: u8,
    bytes: &'bytes [u8],
    cursor: usize,
}
struct BitCounter {
    bit_len: usize,
}
trait BitSink {
    fn align_to_byte(&mut self) -> ZipResult<()>;
    fn write_bits(&mut self, value: u16, count: u8) -> ZipResult<()>;
}
struct BitWriter<'writer> {
    bit_buffer: u8,
    bit_count: u8,
    buffer: [u8; DEFLATE_STREAM_BUFFER_LEN],
    buffered_len: usize,
    len: usize,
    writer: &'writer mut dyn IoWrite,
}
struct Huffman {
    codes: [Vec<HuffmanCode>; DEFLATE_MAX_BITS + 1],
}
struct WriteHuffman {
    codes: Vec<u16>,
    lengths: Vec<u8>,
}
struct HuffmanCode {
    code: u16,
    symbol: u16,
}
#[derive(Clone, Copy)]
enum DeflateToken {
    Literal(u16),
    Match { distance: u16, length: u16 },
}
impl DeflateToken {
    fn literal(byte: u8) -> Self {
        Self::Literal(u16::from(byte))
    }
    fn output_len(self) -> usize {
        match self {
            Self::Literal(_) => 1,
            Self::Match { length, .. } => usize::from(length),
        }
    }
}
#[derive(Clone, Copy)]
struct CodeLengthToken {
    extra: u16,
    extra_bits: u8,
    symbol: u8,
}
struct HuffmanLeafLength {
    freq: u32,
    len: usize,
    symbol: usize,
}
struct DeflateWorkBudget {
    remaining: usize,
}
pub(super) struct DeflateInflater<'bytes> {
    pub bytes: &'bytes [u8],
    pub expected_len: usize,
}
#[derive(Default)]
pub(super) struct DeflateWorkspace {
    head: Vec<usize>,
    previous: Vec<usize>,
    tokens: Vec<DeflateToken>,
    work_budget: DeflateWorkBudget,
}
pub(super) struct DeflateWriter<'bytes, 'workspace> {
    pub bytes: &'bytes [u8],
    pub workspace: &'workspace mut DeflateWorkspace,
}
pub(super) struct DeflatePlan {
    blocks: Vec<DeflateBlockPlan>,
    compressed_len: usize,
    tokens: Vec<DeflateToken>,
}
struct DeflateBlockPlan {
    dynamic_plan: Option<DynamicDeflatePlan>,
    empty_stored_after: u8,
    token_range: Range<usize>,
}
struct DeflateOutputBoundary {
    empty_stored_after: u8,
    output_end: usize,
}
struct DynamicDeflatePlan {
    code_huffman: WriteHuffman,
    code_length_count: usize,
    code_length_tokens: Vec<CodeLengthToken>,
    distance_count: usize,
    distance_huffman: WriteHuffman,
    literal_count: usize,
    literal_huffman: WriteHuffman,
}
struct DynamicFrequencies {
    distance: [u32; DISTANCE_SYMBOLS],
    literal: [u32; LITERAL_LENGTH_SYMBOLS],
}
struct DynamicTrees {
    distance: Option<Huffman>,
    literal: Huffman,
}
#[derive(Clone, Copy)]
struct DeflateSymbol {
    extra: u16,
    extra_bits: u8,
    symbol: u16,
}
struct HuffmanLengthBuilder<'frequencies> {
    frequencies: &'frequencies [u32],
    max_bits: u8,
}
struct HuffmanLeafRef {
    freq: u32,
    node_index: usize,
    symbol: usize,
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
        let start = self.cursor;
        if start > self.bytes.len() {
            return Err(zip_static("deflate 저장 블록 시작 위치가 입력보다 깁니다."));
        }
        let end = start
            .checked_add(len)
            .ok_or_else(|| zip_static("deflate 저장 블록 크기 계산 실패"))?;
        let Some(bytes) = self.bytes.get(Range { start, end }) else {
            return Err(zip_static("deflate 저장 블록이 입력보다 깁니다."));
        };
        self.cursor = end;
        Ok(bytes)
    }
}
impl BitCounter {
    const fn byte_len(&self) -> usize {
        self.bit_len.div_ceil(8)
    }
    const fn counting() -> Self {
        Self { bit_len: 0 }
    }
}
impl BitSink for BitCounter {
    fn align_to_byte(&mut self) -> ZipResult<()> {
        self.bit_len = self
            .bit_len
            .checked_next_multiple_of(8)
            .ok_or_else(|| zip_static("deflate 출력 byte 정렬 길이 계산 실패"))?;
        Ok(())
    }
    fn write_bits(&mut self, _value: u16, count: u8) -> ZipResult<()> {
        self.bit_len = self
            .bit_len
            .checked_add(usize::from(count))
            .ok_or_else(|| zip_static("deflate 출력 bit 길이 계산 실패"))?;
        Ok(())
    }
}
impl BitSink for BitWriter<'_> {
    fn align_to_byte(&mut self) -> ZipResult<()> {
        let padding = 8_u8
            .checked_sub(self.bit_count)
            .ok_or_else(|| zip_static("deflate 출력 byte 정렬 계산 실패"))?
            .rem_euclid(8);
        self.write_bits(0, padding)
    }
    fn write_bits(&mut self, mut value: u16, count: u8) -> ZipResult<()> {
        for _ in 0_u8..count {
            if value & 1_u16 != 0 {
                self.bit_buffer |= 1_u8 << self.bit_count;
            }
            value >>= 1_u8;
            self.bit_count = self.bit_count.saturating_add(1);
            if self.bit_count == 8 {
                self.write_byte(self.bit_buffer)?;
                self.bit_buffer = 0;
                self.bit_count = 0;
            }
        }
        Ok(())
    }
}
impl BitWriter<'_> {
    fn finish_stream(mut self) -> ZipResult<usize> {
        if self.bit_count > 0 {
            self.write_byte(self.bit_buffer)?;
        }
        self.flush_buffer()?;
        Ok(self.len)
    }
    fn flush_buffer(&mut self) -> ZipResult<()> {
        if self.buffered_len == 0 {
            return Ok(());
        }
        let Some(buffered) = self.buffer.get(..self.buffered_len) else {
            return Err(zip_static("deflate stream buffer 범위 오류"));
        };
        self.writer
            .write_all(buffered)
            .map_err(|source| zip_with_source("deflate stream 쓰기 실패", source))?;
        self.buffered_len = 0;
        Ok(())
    }
    fn write_byte(&mut self, byte: u8) -> ZipResult<()> {
        if self.buffered_len == self.buffer.len() {
            self.flush_buffer()?;
        }
        let Some(slot) = self.buffer.get_mut(self.buffered_len) else {
            return Err(zip_static("deflate stream buffer 범위 오류"));
        };
        *slot = byte;
        self.buffered_len = self.buffered_len.saturating_add(1);
        self.len = self
            .len
            .checked_add(1)
            .ok_or_else(|| zip_static("deflate 출력 길이 계산 실패"))?;
        Ok(())
    }
}
impl Huffman {
    fn decode(&self, reader: &mut BitReader<'_>) -> ZipResult<u16> {
        let mut code = 0_u16;
        for bit_len in 1..=DEFLATE_MAX_BITS {
            let bit = u16::try_from(reader.read_bits(1)?)
                .map_err(|source| zip_with_source("deflate bit 변환 실패", source))?;
            let shift = u32::try_from(bit_len.saturating_sub(1))
                .map_err(|source| zip_with_source("deflate bit 길이 변환 실패", source))?;
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
        let Some(mut next_code) = canonical_next_codes(lengths)? else {
            return Ok(None);
        };
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
            let code_limit = 1_u16
                .checked_shl(u32::from(len))
                .ok_or_else(|| zip_static("deflate Huffman code 범위 계산 실패"))?;
            if assigned >= code_limit {
                return Err(zip_static("deflate Huffman code가 과포화되었습니다."));
            }
            *next_slot = next_slot.saturating_add(1);
            let symbol_u16 = u16::try_from(symbol)
                .map_err(|source| zip_with_source("deflate symbol 변환 실패", source))?;
            let Some(code_bucket) = codes.get_mut(len_index) else {
                return Err(zip_static("deflate Huffman code bucket 범위 오류"));
            };
            code_bucket.push(HuffmanCode {
                code: reverse_low_bits(assigned, len),
                symbol: symbol_u16,
            });
        }
        Ok(Some(Self { codes }))
    }
}
impl WriteHuffman {
    fn from_lengths(lengths: Vec<u8>) -> ZipResult<Self> {
        let Some(mut next_code) = canonical_next_codes(&lengths)? else {
            return Err(zip_static("deflate 출력 Huffman code가 비어 있습니다."));
        };
        let mut codes = Vec::new();
        codes.try_reserve_exact(lengths.len()).map_err(|source| {
            zip_with_source("deflate 출력 Huffman code 메모리 확보 실패", source)
        })?;
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
            *code_slot = reverse_low_bits(*next_slot, len);
            *next_slot = next_slot.saturating_add(1);
        }
        Ok(Self { codes, lengths })
    }
    fn write_symbol<W>(&self, writer: &mut W, symbol: u16) -> ZipResult<()>
    where
        W: BitSink,
    {
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
        writer.write_bits(code, len)
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
        ensure_deflate_output_len(self.output.len(), length, self.expected_len)?;
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
                .map_err(|source| zip_with_source("deflate distance extra 변환 실패", source))?
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
                .map_err(|source| zip_with_source("deflate length extra 변환 실패", source))?
        };
        base.checked_add(extra)
            .ok_or_else(|| zip_static("deflate length 계산 실패"))
    }
    fn dynamic_trees(&mut self) -> ZipResult<DynamicTrees> {
        let literal_count = usize::try_from(self.reader.read_bits(5)?)
            .map_err(|source| zip_with_source("deflate HLIT 변환 실패", source))?
            .saturating_add(257);
        if literal_count > LITERAL_LENGTH_SYMBOLS {
            return Err(zip_static("deflate HLIT 범위 오류"));
        }
        let distance_count = usize::try_from(self.reader.read_bits(5)?)
            .map_err(|source| zip_with_source("deflate HDIST 변환 실패", source))?
            .saturating_add(1);
        let code_length_count = usize::try_from(self.reader.read_bits(4)?)
            .map_err(|source| zip_with_source("deflate HCLEN 변환 실패", source))?
            .saturating_add(4);
        let mut code_lengths = [0_u8; 19];
        for &symbol in CODE_LENGTH_ORDER.iter().take(code_length_count) {
            let Some(slot) = code_lengths.get_mut(symbol) else {
                return Err(zip_static("deflate code length symbol 범위 오류"));
            };
            *slot = u8::try_from(self.reader.read_bits(3)?)
                .map_err(|source| zip_with_source("deflate code length 변환 실패", source))?;
        }
        let code_tree = Huffman::from_lengths(&code_lengths)?
            .ok_or_else(|| zip_static("deflate code length tree가 비어 있습니다."))?;
        let total = literal_count
            .checked_add(distance_count)
            .ok_or_else(|| zip_static("deflate code length 총합 계산 실패"))?;
        let mut lengths = Vec::new();
        lengths
            .try_reserve_exact(total)
            .map_err(|source| zip_with_source("deflate code length 메모리 확보 실패", source))?;
        while lengths.len() < total {
            let symbol = code_tree.decode(&mut self.reader)?;
            match symbol {
                0..=15 => {
                    lengths.push(u8::try_from(symbol).map_err(|source| {
                        zip_with_source("deflate code length symbol 변환 실패", source)
                    })?);
                }
                16 => {
                    let Some(&previous) = lengths.last() else {
                        return Err(zip_static("deflate repeat code에 이전 길이가 없습니다."));
                    };
                    let repeat = usize::try_from(self.reader.read_bits(2)?)
                        .map_err(|source| zip_with_source("deflate repeat 변환 실패", source))?
                        .saturating_add(3);
                    push_repeated(&mut lengths, previous, repeat, total)?;
                }
                17 => {
                    let repeat = usize::try_from(self.reader.read_bits(3)?)
                        .map_err(|source| zip_with_source("deflate zero repeat 변환 실패", source))?
                        .saturating_add(3);
                    push_repeated(&mut lengths, 0, repeat, total)?;
                }
                18 => {
                    let repeat = usize::try_from(self.reader.read_bits(7)?)
                        .map_err(|source| {
                            zip_with_source("deflate long zero repeat 변환 실패", source)
                        })?
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
        Ok(DynamicTrees { distance, literal })
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
                    ensure_deflate_output_len(self.output.len(), 1, self.expected_len)?;
                    self.output.push(
                        u8::try_from(symbol).map_err(|source| {
                            zip_with_source("deflate literal 변환 실패", source)
                        })?,
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
        ensure_deflate_output_len(self.output.len(), stored.len(), self.expected_len)?;
        self.output.extend_from_slice(stored);
        Ok(())
    }
}
impl DeflateInflater<'_> {
    pub(super) fn inflate(&self) -> ZipResult<Vec<u8>> {
        let mut output = Vec::new();
        output
            .try_reserve_exact(self.expected_len)
            .map_err(|source| zip_with_source("deflate 출력 메모리 확보 실패", source))?;
        let mut state = InflateState {
            expected_len: self.expected_len,
            output,
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
                    let trees = state.dynamic_trees()?;
                    state.inflate_compressed_block(&trees.literal, trees.distance.as_ref())?;
                }
                _ => return Err(zip_static("지원하지 않는 deflate block type입니다.")),
            }
            if final_block {
                return Ok(state.output);
            }
        }
    }
}
impl DynamicFrequencies {
    fn collect(&mut self, tokens: &[DeflateToken]) -> ZipResult<bool> {
        let end_freq = self
            .literal
            .get_mut(256)
            .ok_or_else(|| zip_static("deflate end symbol 범위 오류"))?;
        *end_freq = 1;
        let mut has_distance = false;
        for &token in tokens {
            match token {
                DeflateToken::Literal(byte) => {
                    let freq = self
                        .literal
                        .get_mut(usize::from(byte))
                        .ok_or_else(|| zip_static("deflate literal frequency 범위 오류"))?;
                    *freq = freq.saturating_add(1);
                }
                DeflateToken::Match { distance, length } => {
                    let length_code = DeflateWriter::length_symbol(length)
                        .ok_or_else(|| zip_static("deflate length 범위 오류"))?;
                    let length_freq = self
                        .literal
                        .get_mut(usize::from(length_code.symbol))
                        .ok_or_else(|| zip_static("deflate length frequency 범위 오류"))?;
                    *length_freq = length_freq.saturating_add(1);
                    let distance_code = DeflateWriter::distance_symbol(distance)
                        .ok_or_else(|| zip_static("deflate distance 범위 오류"))?;
                    let distance_freq = self
                        .distance
                        .get_mut(usize::from(distance_code.symbol))
                        .ok_or_else(|| zip_static("deflate distance frequency 범위 오류"))?;
                    *distance_freq = distance_freq.saturating_add(1);
                    has_distance = true;
                }
            }
        }
        Ok(has_distance)
    }
    fn plan(self) -> ZipResult<DynamicDeflatePlan> {
        let literal_lengths = (HuffmanLengthBuilder {
            frequencies: &self.literal,
            max_bits: DEFLATE_MAX_BITS_U8,
        })
        .build()?;
        let distance_lengths = (HuffmanLengthBuilder {
            frequencies: &self.distance,
            max_bits: DEFLATE_MAX_BITS_U8,
        })
        .build()?;
        let literal_count = literal_lengths
            .iter()
            .rposition(|&len| len != 0)
            .map_or(257, |index| index.saturating_add(1).max(257));
        let distance_count = distance_lengths
            .iter()
            .rposition(|&len| len != 0)
            .map_or(1, |index| index.saturating_add(1));
        let mut combined_lengths = Vec::new();
        combined_lengths
            .try_reserve_exact(literal_count.saturating_add(distance_count))
            .map_err(|source| {
                zip_with_source("deflate combined length 메모리 확보 실패", source)
            })?;
        let Some(literal_prefix) = literal_lengths.get(..literal_count) else {
            return Err(zip_static("deflate literal length 범위 오류"));
        };
        let Some(distance_prefix) = distance_lengths.get(..distance_count) else {
            return Err(zip_static("deflate distance length 범위 오류"));
        };
        combined_lengths.extend_from_slice(literal_prefix);
        combined_lengths.extend_from_slice(distance_prefix);
        let mut code_length_tokens = Vec::new();
        code_length_tokens
            .try_reserve_exact(combined_lengths.len())
            .map_err(|source| {
                zip_with_source("deflate code length token 메모리 확보 실패", source)
            })?;
        let mut length_index = 0_usize;
        while length_index < combined_lengths.len() {
            let Some(&value) = combined_lengths.get(length_index) else {
                break;
            };
            if value == 0 {
                let mut run = 1_usize;
                while combined_lengths.get(length_index.saturating_add(run)) == Some(&0) {
                    run = run.saturating_add(1);
                }
                let mut remaining = run;
                while remaining >= 11 {
                    let count = remaining.min(138);
                    code_length_tokens.push(CodeLengthToken {
                        extra: u16::try_from(count.saturating_sub(11)).map_err(|source| {
                            zip_with_source("deflate repeat-zero-11 변환 실패", source)
                        })?,
                        extra_bits: 7,
                        symbol: 18,
                    });
                    remaining = remaining.saturating_sub(count);
                }
                if remaining >= 3 {
                    let count = remaining.min(10);
                    code_length_tokens.push(CodeLengthToken {
                        extra: u16::try_from(count.saturating_sub(3)).map_err(|source| {
                            zip_with_source("deflate repeat-zero-3 변환 실패", source)
                        })?,
                        extra_bits: 3,
                        symbol: 17,
                    });
                    remaining = remaining.saturating_sub(count);
                }
                code_length_tokens.extend(repeat_n(
                    CodeLengthToken {
                        extra: 0,
                        extra_bits: 0,
                        symbol: 0,
                    },
                    remaining,
                ));
                length_index = length_index.saturating_add(run);
            } else {
                code_length_tokens.push(CodeLengthToken {
                    extra: 0,
                    extra_bits: 0,
                    symbol: value,
                });
                let mut run = 0_usize;
                while combined_lengths.get(length_index.saturating_add(1).saturating_add(run))
                    == Some(&value)
                {
                    run = run.saturating_add(1);
                }
                let mut remaining = run;
                while remaining >= 3 {
                    let count = remaining.min(6);
                    code_length_tokens.push(CodeLengthToken {
                        extra: u16::try_from(count.saturating_sub(3)).map_err(|source| {
                            zip_with_source("deflate repeat-length 변환 실패", source)
                        })?,
                        extra_bits: 2,
                        symbol: 16,
                    });
                    remaining = remaining.saturating_sub(count);
                }
                code_length_tokens.extend(repeat_n(
                    CodeLengthToken {
                        extra: 0,
                        extra_bits: 0,
                        symbol: value,
                    },
                    remaining,
                ));
                length_index = length_index.saturating_add(1).saturating_add(run);
            }
        }
        let mut code_length_freq = [0_u32; CODE_LENGTH_SYMBOLS];
        for token in &code_length_tokens {
            let Some(freq) = code_length_freq.get_mut(usize::from(token.symbol)) else {
                return Err(zip_static("deflate code length frequency 범위 오류"));
            };
            *freq = freq.saturating_add(1);
        }
        let code_lengths = (HuffmanLengthBuilder {
            frequencies: &code_length_freq,
            max_bits: 7,
        })
        .build()?;
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
        Ok(DynamicDeflatePlan {
            code_huffman,
            code_length_count,
            code_length_tokens,
            distance_count,
            distance_huffman,
            literal_count,
            literal_huffman,
        })
    }
}
impl DynamicDeflatePlan {
    fn bit_len(&self, tokens: &[DeflateToken]) -> ZipResult<usize> {
        let mut counter = BitCounter::counting();
        self.write_block(tokens, &mut counter)?;
        Ok(counter.bit_len)
    }
    fn write_block<W>(&self, tokens: &[DeflateToken], writer: &mut W) -> ZipResult<()>
    where
        W: BitSink,
    {
        writer.write_bits(0, 1)?;
        writer.write_bits(2, 2)?;
        writer.write_bits(
            u16::try_from(self.literal_count.saturating_sub(257))
                .map_err(|source| zip_with_source("deflate HLIT 변환 실패", source))?,
            5,
        )?;
        writer.write_bits(
            u16::try_from(self.distance_count.saturating_sub(1))
                .map_err(|source| zip_with_source("deflate HDIST 변환 실패", source))?,
            5,
        )?;
        writer.write_bits(
            u16::try_from(self.code_length_count.saturating_sub(4))
                .map_err(|source| zip_with_source("deflate HCLEN 변환 실패", source))?,
            4,
        )?;
        for &symbol in CODE_LENGTH_ORDER.iter().take(self.code_length_count) {
            let len = self
                .code_huffman
                .lengths
                .get(symbol)
                .copied()
                .ok_or_else(|| zip_static("deflate code length 쓰기 범위 오류"))?;
            writer.write_bits(u16::from(len), 3)?;
        }
        for &token in &self.code_length_tokens {
            self.code_huffman
                .write_symbol(writer, u16::from(token.symbol))?;
            if token.extra_bits > 0 {
                writer.write_bits(token.extra, token.extra_bits)?;
            }
        }
        for &token in tokens {
            match token {
                DeflateToken::Literal(byte) => self.literal_huffman.write_symbol(writer, byte)?,
                DeflateToken::Match { distance, length } => {
                    let Some(length_code) = DeflateWriter::length_symbol(length) else {
                        return Err(zip_static("deflate length 범위 오류"));
                    };
                    self.literal_huffman
                        .write_symbol(writer, length_code.symbol)?;
                    if length_code.extra_bits > 0 {
                        writer.write_bits(length_code.extra, length_code.extra_bits)?;
                    }
                    let Some(distance_code) = DeflateWriter::distance_symbol(distance) else {
                        return Err(zip_static("deflate distance 범위 오류"));
                    };
                    self.distance_huffman
                        .write_symbol(writer, distance_code.symbol)?;
                    if distance_code.extra_bits > 0 {
                        writer.write_bits(distance_code.extra, distance_code.extra_bits)?;
                    }
                }
            }
        }
        self.literal_huffman.write_symbol(writer, 256)
    }
}
impl HuffmanLengthBuilder<'_> {
    fn build(&self) -> ZipResult<Vec<u8>> {
        let mut lengths = Vec::new();
        lengths
            .try_reserve_exact(self.frequencies.len())
            .map_err(|source| zip_with_source("deflate Huffman length 메모리 확보 실패", source))?;
        lengths.resize(self.frequencies.len(), 0_u8);
        let mut leaf_lengths = self.leaf_lengths()?;
        if leaf_lengths.len() == 1 {
            let leaf = leaf_lengths
                .first()
                .ok_or_else(|| zip_static("deflate Huffman 단일 symbol이 없습니다."))?;
            let slot = lengths
                .get_mut(leaf.symbol)
                .ok_or_else(|| zip_static("deflate Huffman 단일 symbol 범위 오류"))?;
            *slot = 1;
            return Ok(lengths);
        }
        let longest_len = leaf_lengths
            .iter()
            .fold(0_usize, |longest, leaf| longest.max(leaf.len));
        let max_bit_count = usize::from(self.max_bits);
        if longest_len <= max_bit_count {
            for leaf in leaf_lengths {
                let slot = lengths
                    .get_mut(leaf.symbol)
                    .ok_or_else(|| zip_static("deflate Huffman symbol 범위 오류"))?;
                let bit_len = u8::try_from(leaf.len).map_err(|source| {
                    zip_with_source("deflate Huffman bit 길이 변환 실패", source)
                })?;
                *slot = bit_len;
            }
            return Ok(lengths);
        }
        let length_counts = self.limited_length_counts(&leaf_lengths, longest_len)?;
        leaf_lengths.sort_unstable_by(|left, right| {
            left.freq
                .cmp(&right.freq)
                .then_with(|| right.symbol.cmp(&left.symbol))
        });
        let mut symbol_index = 0_usize;
        for bit_len in (1..=max_bit_count).rev() {
            let count = length_counts
                .get(bit_len)
                .copied()
                .ok_or_else(|| zip_static("deflate limited Huffman count 범위 오류"))?;
            let assigned_len = u8::try_from(bit_len).map_err(|source| {
                zip_with_source("deflate limited Huffman bit 길이 변환 실패", source)
            })?;
            for _ in 0..count {
                let leaf = leaf_lengths
                    .get(symbol_index)
                    .ok_or_else(|| zip_static("deflate limited Huffman leaf 범위 오류"))?;
                let slot = lengths
                    .get_mut(leaf.symbol)
                    .ok_or_else(|| zip_static("deflate limited Huffman symbol 범위 오류"))?;
                *slot = assigned_len;
                symbol_index = symbol_index.saturating_add(1);
            }
        }
        if symbol_index != leaf_lengths.len() {
            return Err(zip_static("deflate limited Huffman length 할당 실패"));
        }
        Ok(lengths)
    }
    fn leaf_lengths(&self) -> ZipResult<Vec<HuffmanLeafLength>> {
        let mut nodes = Vec::new();
        let mut leaves = Vec::new();
        let mut active = Vec::new();
        let node_capacity = self
            .frequencies
            .len()
            .checked_mul(2)
            .map(|len| len.saturating_sub(1))
            .ok_or_else(|| zip_static("deflate Huffman node 용량 계산 실패"))?;
        nodes
            .try_reserve_exact(node_capacity)
            .map_err(|source| zip_with_source("deflate Huffman node 메모리 확보 실패", source))?;
        leaves
            .try_reserve_exact(self.frequencies.len())
            .map_err(|source| zip_with_source("deflate Huffman leaf 메모리 확보 실패", source))?;
        active
            .try_reserve_exact(self.frequencies.len())
            .map_err(|source| {
                zip_with_source("deflate Huffman active node 메모리 확보 실패", source)
            })?;
        for (symbol, &freq) in self.frequencies.iter().enumerate() {
            if freq == 0 {
                continue;
            }
            let node_index = nodes.len();
            nodes.push(None);
            leaves.push(HuffmanLeafRef {
                freq,
                node_index,
                symbol,
            });
            active.push((u64::from(freq), node_index));
        }
        if active.is_empty() {
            return Err(zip_static("deflate Huffman frequency가 비어 있습니다."));
        }
        if active.len() == 1 {
            let leaf = leaves
                .first()
                .ok_or_else(|| zip_static("deflate Huffman 단일 leaf가 없습니다."))?;
            let mut leaf_lengths = Vec::new();
            leaf_lengths.try_reserve_exact(1).map_err(|source| {
                zip_with_source("deflate Huffman single leaf 메모리 확보 실패", source)
            })?;
            leaf_lengths.push(HuffmanLeafLength {
                freq: leaf.freq,
                len: 1,
                symbol: leaf.symbol,
            });
            return Ok(leaf_lengths);
        }
        while active.len() > 1 {
            active.sort_unstable_by(|&(left_freq, left), &(right_freq, right)| {
                right_freq.cmp(&left_freq).then_with(|| right.cmp(&left))
            });
            let (left_freq, left) = active
                .pop()
                .ok_or_else(|| zip_static("deflate Huffman left node가 없습니다."))?;
            let (right_freq, right) = active
                .pop()
                .ok_or_else(|| zip_static("deflate Huffman right node가 없습니다."))?;
            let parent = nodes.len();
            let freq = left_freq
                .checked_add(right_freq)
                .ok_or_else(|| zip_static("deflate Huffman frequency 계산 실패"))?;
            nodes.push(None);
            let left_child_node = nodes
                .get_mut(left)
                .ok_or_else(|| zip_static("deflate Huffman left node 범위 오류"))?;
            *left_child_node = Some(parent);
            let right_child_node = nodes
                .get_mut(right)
                .ok_or_else(|| zip_static("deflate Huffman right node 범위 오류"))?;
            *right_child_node = Some(parent);
            active.push((freq, parent));
        }
        let mut leaf_lengths = Vec::new();
        leaf_lengths
            .try_reserve_exact(leaves.len())
            .map_err(|source| {
                zip_with_source("deflate Huffman leaf length 메모리 확보 실패", source)
            })?;
        for leaf in leaves {
            let mut bit_len = 0_usize;
            let mut cursor = leaf.node_index;
            let mut parent = nodes
                .get(cursor)
                .copied()
                .ok_or_else(|| zip_static("deflate Huffman leaf node 범위 오류"))?;
            while let Some(parent_index) = parent {
                bit_len = bit_len.saturating_add(1);
                cursor = parent_index;
                parent = nodes
                    .get(cursor)
                    .copied()
                    .ok_or_else(|| zip_static("deflate Huffman parent node 범위 오류"))?;
            }
            if bit_len == 0 {
                return Err(zip_static("deflate Huffman leaf 길이가 0입니다."));
            }
            leaf_lengths.push(HuffmanLeafLength {
                freq: leaf.freq,
                len: bit_len,
                symbol: leaf.symbol,
            });
        }
        Ok(leaf_lengths)
    }
    fn limited_length_counts(
        &self,
        leaf_lengths: &[HuffmanLeafLength],
        longest_len: usize,
    ) -> ZipResult<Vec<usize>> {
        let max_bit_count = usize::from(self.max_bits);
        let mut length_counts = Vec::new();
        length_counts
            .try_reserve_exact(longest_len.saturating_add(1))
            .map_err(|source| zip_with_source("deflate length count 메모리 확보 실패", source))?;
        length_counts.resize(longest_len.saturating_add(1), 0_usize);
        for leaf in leaf_lengths {
            let count = length_counts
                .get_mut(leaf.len)
                .ok_or_else(|| zip_static("deflate Huffman length count 범위 오류"))?;
            *count = count.saturating_add(1);
        }
        let mut current_bits = longest_len;
        while current_bits > max_bit_count {
            while {
                let count = length_counts
                    .get(current_bits)
                    .ok_or_else(|| zip_static("deflate Huffman current length 범위 오류"))?;
                *count > 0
            } {
                let shorter_bits = (1..current_bits.saturating_sub(1))
                    .rev()
                    .find(|&bits| length_counts.get(bits).is_some_and(|&count| count != 0))
                    .ok_or_else(|| zip_static("deflate shorter Huffman length가 없습니다."))?;
                let current_count = length_counts
                    .get_mut(current_bits)
                    .ok_or_else(|| zip_static("deflate current Huffman length 범위 오류"))?;
                *current_count = current_count
                    .checked_sub(2)
                    .ok_or_else(|| zip_static("deflate current Huffman count 축소 실패"))?;
                let previous_bits = current_bits
                    .checked_sub(1)
                    .ok_or_else(|| zip_static("deflate previous Huffman length 계산 실패"))?;
                let previous_count = length_counts
                    .get_mut(previous_bits)
                    .ok_or_else(|| zip_static("deflate previous Huffman length 범위 오류"))?;
                *previous_count = previous_count
                    .checked_add(1)
                    .ok_or_else(|| zip_static("deflate previous Huffman count 증가 실패"))?;
                let split_bits = shorter_bits.saturating_add(1);
                let split_count = length_counts
                    .get_mut(split_bits)
                    .ok_or_else(|| zip_static("deflate split Huffman length 범위 오류"))?;
                *split_count = split_count
                    .checked_add(2)
                    .ok_or_else(|| zip_static("deflate split Huffman count 증가 실패"))?;
                let shorter_count = length_counts
                    .get_mut(shorter_bits)
                    .ok_or_else(|| zip_static("deflate shorter Huffman length 범위 오류"))?;
                *shorter_count = shorter_count
                    .checked_sub(1)
                    .ok_or_else(|| zip_static("deflate shorter Huffman count 축소 실패"))?;
            }
            current_bits = current_bits
                .checked_sub(1)
                .ok_or_else(|| zip_static("deflate Huffman current length 계산 실패"))?;
        }
        Ok(length_counts)
    }
}
impl DeflatePlan {
    pub(super) const fn len(&self) -> usize {
        self.compressed_len
    }
    fn write<W>(&self, writer: &mut W) -> ZipResult<()>
    where
        W: BitSink,
    {
        for block in &self.blocks {
            let tokens = self
                .tokens
                .get(block.token_range)
                .ok_or_else(|| zip_static("deflate block token 범위 오류"))?;
            if let Some(dynamic_plan) = block.dynamic_plan.as_ref() {
                dynamic_plan.write_block(tokens, writer)?;
            } else {
                write_fixed_block(tokens, writer)?;
            }
            for _ in 0..block.empty_stored_after {
                write_empty_stored_block(writer)?;
            }
        }
        write_empty_stored_block(writer)?;
        writer.write_bits(1, 1)?;
        writer.write_bits(1, 2)?;
        write_fixed_symbol(writer, 256)
    }
    pub(super) fn write_to(&self, writer: &mut dyn IoWrite) -> ZipResult<usize> {
        let mut bit_writer = BitWriter {
            bit_buffer: 0,
            bit_count: 0,
            buffer: [0; DEFLATE_STREAM_BUFFER_LEN],
            buffered_len: 0,
            len: 0,
            writer,
        };
        self.write(&mut bit_writer)?;
        bit_writer.finish_stream()
    }
}
impl Default for DeflateWorkBudget {
    fn default() -> Self {
        Self {
            remaining: DEFLATE_SEARCH_WORK_LIMIT,
        }
    }
}
impl DeflateWorkBudget {
    const fn consume(&mut self) -> bool {
        let Some(remaining) = self.remaining.checked_sub(1) else {
            return false;
        };
        self.remaining = remaining;
        true
    }
}
impl DeflateWorkspace {
    fn prepare_for_input(&mut self, input_len: usize) -> ZipResult<()> {
        self.work_budget = DeflateWorkBudget::default();
        self.tokens.clear();
        if self.tokens.capacity() < input_len {
            self.tokens
                .try_reserve_exact(input_len)
                .map_err(|source| zip_with_source("deflate token 메모리 확보 실패", source))?;
        }
        self.head.clear();
        if self.head.capacity() < HASH_SIZE {
            self.head
                .try_reserve_exact(HASH_SIZE)
                .map_err(|source| zip_with_source("deflate hash head 메모리 확보 실패", source))?;
        }
        self.head.resize(HASH_SIZE, usize::MAX);
        self.previous.clear();
        if self.previous.capacity() < input_len {
            self.previous
                .try_reserve_exact(input_len)
                .map_err(|source| {
                    zip_with_source("deflate hash previous 메모리 확보 실패", source)
                })?;
        }
        self.previous.resize(input_len, usize::MAX);
        Ok(())
    }
    pub(super) fn recycle(&mut self, plan: DeflatePlan) {
        self.tokens = plan.tokens;
    }
}
impl DeflateWriter<'_, '_> {
    fn distance_symbol(distance: u16) -> Option<DeflateSymbol> {
        let distance_value = usize::from(distance);
        for (index, &base) in DISTANCE_BASES.iter().enumerate().rev() {
            if distance_value < base {
                continue;
            }
            let &extra_bits = DISTANCE_EXTRA_BITS.get(index)?;
            let extra = distance_value.checked_sub(base)?;
            let extra_u16 = u16::try_from(extra).ok()?;
            let symbol = u16::try_from(index).ok()?;
            return Some(DeflateSymbol {
                extra: extra_u16,
                extra_bits,
                symbol,
            });
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
    fn length_symbol(length: u16) -> Option<DeflateSymbol> {
        let length_value = usize::from(length);
        for (index, &base) in LENGTH_BASES.iter().enumerate().rev() {
            if length_value < base {
                continue;
            }
            let &extra_bits = LENGTH_EXTRA_BITS.get(index)?;
            let extra = length_value.checked_sub(base)?;
            let extra_u16 = u16::try_from(extra).ok()?;
            let index_u16 = u16::try_from(index).ok()?;
            let symbol = 257_u16.checked_add(index_u16)?;
            return Some(DeflateSymbol {
                extra: extra_u16,
                extra_bits,
                symbol,
            });
        }
        None
    }
    pub(super) fn plan(&mut self, part_name: &str) -> ZipResult<Option<DeflatePlan>> {
        let mut boundaries = Vec::new();
        boundaries
            .try_reserve(16)
            .map_err(|source| zip_with_source("deflate block 경계 메모리 확보 실패", source))?;
        if matches!(
            part_name,
            "xl/worksheets/sheet1.xml" | "xl/worksheets/sheet2.xml"
        ) {
            let sheet_data_open = b"<sheetData>";
            let sheet_data_close = b"</sheetData>";
            let open_start = find_bytes(self.bytes, sheet_data_open)
                .ok_or_else(|| zip_static("worksheet sheetData 시작 태그가 없습니다."))?;
            let close_start = find_bytes(self.bytes, sheet_data_close)
                .ok_or_else(|| zip_static("worksheet sheetData 종료 태그가 없습니다."))?;
            let close_end = close_start
                .checked_add(sheet_data_close.len())
                .ok_or_else(|| zip_static("worksheet sheetData 종료 경계 계산 실패"))?;
            if open_start >= close_start || close_end > self.bytes.len() {
                return Err(zip_static(
                    "worksheet sheetData 경계 순서가 올바르지 않습니다.",
                ));
            }
            boundaries.push(DeflateOutputBoundary {
                empty_stored_after: EXCEL_FLUSH_BLOCK_COUNT,
                output_end: open_start,
            });
            push_chunk_boundaries(
                &mut boundaries,
                open_start,
                close_end,
                WORKSHEET_BLOCK_BYTES,
            )?;
            boundaries.push(DeflateOutputBoundary {
                empty_stored_after: EXCEL_FLUSH_BLOCK_COUNT,
                output_end: close_end,
            });
        } else {
            let part_chunk_bytes = match part_name {
                "xl/sharedStrings.xml" => Some(SHARED_STRINGS_BLOCK_BYTES),
                "xl/calcChain.xml" => Some(CALC_CHAIN_BLOCK_BYTES),
                _ => None,
            };
            if let Some(chunk_bytes) = part_chunk_bytes {
                push_chunk_boundaries(&mut boundaries, 0, self.bytes.len(), chunk_bytes)?;
            }
        }
        boundaries.push(DeflateOutputBoundary {
            empty_stored_after: 0,
            output_end: self.bytes.len(),
        });
        let Some(tokens) = self.tokens(&boundaries)? else {
            return Ok(None);
        };
        let mut blocks = Vec::<DeflateBlockPlan>::new();
        blocks
            .try_reserve_exact(boundaries.len())
            .map_err(|source| zip_with_source("deflate block plan 메모리 확보 실패", source))?;
        let mut output_len = 0_usize;
        let mut token_end = 0_usize;
        let mut token_start = 0_usize;
        for boundary in boundaries {
            while output_len < boundary.output_end {
                let token = tokens
                    .get(token_end)
                    .copied()
                    .ok_or_else(|| zip_static("deflate block 출력 경계가 token을 초과합니다."))?;
                output_len = output_len
                    .checked_add(token.output_len())
                    .ok_or_else(|| zip_static("deflate block 출력 길이 계산 실패"))?;
                token_end = token_end
                    .checked_add(1)
                    .ok_or_else(|| zip_static("deflate block token 위치 계산 실패"))?;
            }
            if token_start == token_end {
                if let Some(last) = blocks.last_mut() {
                    last.empty_stored_after = last
                        .empty_stored_after
                        .checked_add(boundary.empty_stored_after)
                        .ok_or_else(|| zip_static("deflate empty stored block 수 계산 실패"))?;
                }
                continue;
            }
            let token_range = Range {
                start: token_start,
                end: token_end,
            };
            let block_tokens = tokens
                .get(token_range)
                .ok_or_else(|| zip_static("deflate block token 범위 오류"))?;
            let mut fixed_counter = BitCounter::counting();
            write_fixed_block(block_tokens, &mut fixed_counter)?;
            let mut frequencies = DynamicFrequencies {
                distance: [0_u32; DISTANCE_SYMBOLS],
                literal: [0_u32; LITERAL_LENGTH_SYMBOLS],
            };
            let dynamic_plan = frequencies
                .collect(block_tokens)?
                .then(|| frequencies.plan())
                .transpose()?;
            let chosen_dynamic = if let Some(dynamic) = dynamic_plan {
                (dynamic.bit_len(block_tokens)? < fixed_counter.bit_len).then_some(dynamic)
            } else {
                None
            };
            blocks.push(DeflateBlockPlan {
                dynamic_plan: chosen_dynamic,
                empty_stored_after: boundary.empty_stored_after,
                token_range,
            });
            token_start = token_end;
        }
        if token_end != tokens.len() || output_len != self.bytes.len() {
            return Err(zip_static(
                "deflate block plan이 전체 입력을 포함하지 않습니다.",
            ));
        }
        let mut plan = DeflatePlan {
            blocks,
            compressed_len: 0,
            tokens,
        };
        let mut counter = BitCounter::counting();
        plan.write(&mut counter)?;
        plan.compressed_len = counter.byte_len();
        Ok(Some(plan))
    }
    fn tokens(
        &mut self,
        boundaries: &[DeflateOutputBoundary],
    ) -> ZipResult<Option<Vec<DeflateToken>>> {
        let bytes = self.bytes;
        let workspace = &mut *self.workspace;
        workspace.prepare_for_input(bytes.len())?;
        let mut tokens = mem::take(&mut workspace.tokens);
        let head = &mut workspace.head;
        let previous = &mut workspace.previous;
        let work_budget = &mut workspace.work_budget;
        let mut boundary_index = 0_usize;
        let mut position = 0_usize;
        while position < bytes.len() {
            while boundaries
                .get(boundary_index)
                .is_some_and(|boundary| boundary.output_end <= position)
            {
                boundary_index = boundary_index.saturating_add(1);
            }
            let boundary_end = boundaries
                .get(boundary_index)
                .map_or(bytes.len(), |boundary| boundary.output_end);
            let mut best_len = 0_usize;
            let mut best_distance = 0_usize;
            if let Some(hash) = hash3(bytes, position)
                && let Some(&head_candidate) = head.get(hash)
            {
                let mut candidate = head_candidate;
                let min_candidate = position.saturating_sub(0x8000);
                let max_len = boundary_end
                    .saturating_sub(position)
                    .min(bytes.len().saturating_sub(position))
                    .min(MAX_MATCH);
                let mut chain_len = 0_usize;
                while candidate != usize::MAX
                    && candidate >= min_candidate
                    && candidate < position
                    && chain_len < MAX_CHAIN
                {
                    if !work_budget.consume() {
                        workspace.tokens = tokens;
                        return Ok(None);
                    }
                    let mut len = 0_usize;
                    let mut mismatch_found = false;
                    while max_len.saturating_sub(len) >= SIMD_MATCH_BYTES
                        && work_budget.remaining >= SIMD_MATCH_BYTES
                    {
                        let left = bytes.as_ptr().wrapping_add(candidate.saturating_add(len));
                        let right = bytes.as_ptr().wrapping_add(position.saturating_add(len));
                        let prefix = matching_prefix_16!(left, right);
                        let compared = if prefix < SIMD_MATCH_BYTES {
                            prefix.saturating_add(1)
                        } else {
                            SIMD_MATCH_BYTES
                        };
                        work_budget.remaining = work_budget.remaining.saturating_sub(compared);
                        len = len.saturating_add(prefix);
                        if prefix < SIMD_MATCH_BYTES {
                            mismatch_found = true;
                            break;
                        }
                    }
                    while !mismatch_found && len < max_len {
                        if !work_budget.consume() {
                            workspace.tokens = tokens;
                            return Ok(None);
                        }
                        if bytes.get(candidate.saturating_add(len))
                            != bytes.get(position.saturating_add(len))
                        {
                            break;
                        }
                        len = len.saturating_add(1);
                    }
                    if len > best_len && len >= MIN_MATCH {
                        best_len = len;
                        best_distance = position.saturating_sub(candidate);
                        if len >= XML_NICE_MATCH_LEN {
                            break;
                        }
                    }
                    let Some(&previous_candidate) = previous.get(candidate) else {
                        break;
                    };
                    candidate = previous_candidate;
                    chain_len = chain_len.saturating_add(1);
                }
            }
            let current_match = (best_len >= MIN_MATCH).then_some((best_distance, best_len));
            if let Some((match_distance, match_len)) = current_match {
                if match_len == MIN_MATCH && match_distance > TOO_FAR_MATCH_DISTANCE {
                    let Some(&byte) = bytes.get(position) else {
                        return Err(zip_static("deflate literal 범위 오류"));
                    };
                    tokens.push(DeflateToken::literal(byte));
                    Self::insert_position(bytes, position, head, previous);
                    position = position
                        .checked_add(1)
                        .ok_or_else(|| zip_static("deflate 위치 계산 실패"))?;
                    continue;
                }
                tokens.push(DeflateToken::Match {
                    distance: deflate_u16(match_distance, "deflate match distance 변환 실패")?,
                    length: deflate_u16(match_len, "deflate match length 변환 실패")?,
                });
                let next_position = position
                    .checked_add(match_len)
                    .ok_or_else(|| zip_static("deflate 위치 계산 실패"))?;
                for insert_position in position..next_position {
                    Self::insert_position(bytes, insert_position, head, previous);
                }
                position = next_position;
            } else {
                let Some(&byte) = bytes.get(position) else {
                    return Err(zip_static("deflate literal 범위 오류"));
                };
                tokens.push(DeflateToken::literal(byte));
                Self::insert_position(bytes, position, head, previous);
                position = position.saturating_add(1);
            }
        }
        Ok(Some(tokens))
    }
}
fn canonical_next_codes(lengths: &[u8]) -> ZipResult<Option<[u16; DEFLATE_MAX_BITS + 1]>> {
    let mut length_counts = [0_u16; DEFLATE_MAX_BITS + 1];
    for &length in lengths {
        if length == 0 {
            continue;
        }
        let Some(count) = length_counts.get_mut(usize::from(length)) else {
            return Err(zip_static("deflate Huffman code 길이가 너무 깁니다."));
        };
        *count = count.saturating_add(1);
    }
    if length_counts.iter().skip(1).all(|count| *count == 0) {
        return Ok(None);
    }
    let mut next_codes = [0_u16; DEFLATE_MAX_BITS + 1];
    let mut code = 0_u16;
    for bits in 1..=DEFLATE_MAX_BITS {
        let previous = bits.saturating_sub(1);
        let Some(&previous_count) = length_counts.get(previous) else {
            return Err(zip_static("deflate Huffman count 범위 오류"));
        };
        code = code.saturating_add(previous_count) << 1_u8;
        let Some(next_code) = next_codes.get_mut(bits) else {
            return Err(zip_static("deflate Huffman next code 범위 오류"));
        };
        *next_code = code;
    }
    Ok(Some(next_codes))
}
fn deflate_u16(value: usize, context: &'static str) -> ZipResult<u16> {
    u16::try_from(value).map_err(|source| zip_with_source(context, source))
}
fn ensure_deflate_output_len(
    current_len: usize,
    additional_len: usize,
    expected_len: usize,
) -> ZipResult<()> {
    let next_len = current_len
        .checked_add(additional_len)
        .ok_or_else(|| zip_static("deflate 출력 크기 계산 실패"))?;
    if next_len > expected_len {
        return Err(zip_static(
            "deflate 출력이 ZIP 선언 해제 크기를 초과했습니다.",
        ));
    }
    Ok(())
}
fn hash3(bytes: &[u8], position: usize) -> Option<usize> {
    let &[first_byte, second_byte, third_byte] = bytes.get(position..)?.first_chunk::<3>()?;
    let first = usize::from(first_byte);
    let second = usize::from(second_byte);
    let third = usize::from(third_byte);
    Some(((first << 10_usize) ^ (second << 5_usize) ^ third) & HASH_SIZE.saturating_sub(1))
}
fn find_bytes(bytes: &[u8], needle: &[u8]) -> Option<usize> {
    bytes
        .windows(needle.len())
        .position(|window| window == needle)
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
fn reverse_low_bits(value: u16, count: u8) -> u16 {
    value
        .reverse_bits()
        .unbounded_shr(u16::BITS.saturating_sub(u32::from(count)))
}
fn push_chunk_boundaries(
    boundaries: &mut Vec<DeflateOutputBoundary>,
    start: usize,
    end: usize,
    chunk_bytes: usize,
) -> ZipResult<()> {
    let mut next = start
        .checked_add(chunk_bytes)
        .ok_or_else(|| zip_static("deflate chunk 경계 계산 실패"))?;
    while next < end {
        boundaries.push(DeflateOutputBoundary {
            empty_stored_after: 0,
            output_end: next,
        });
        next = next
            .checked_add(chunk_bytes)
            .ok_or_else(|| zip_static("deflate 다음 chunk 경계 계산 실패"))?;
    }
    Ok(())
}
fn write_empty_stored_block<W>(writer: &mut W) -> ZipResult<()>
where
    W: BitSink,
{
    writer.write_bits(0, 3)?;
    writer.align_to_byte()?;
    writer.write_bits(0, 16)?;
    writer.write_bits(u16::MAX, 16)
}
fn write_fixed_block<W>(tokens: &[DeflateToken], writer: &mut W) -> ZipResult<()>
where
    W: BitSink,
{
    writer.write_bits(0, 1)?;
    writer.write_bits(1, 2)?;
    for &token in tokens {
        match token {
            DeflateToken::Literal(byte) => write_fixed_symbol(writer, byte)?,
            DeflateToken::Match { distance, length } => {
                let Some(length_code) = DeflateWriter::length_symbol(length) else {
                    return Err(zip_static("deflate length 범위 오류"));
                };
                write_fixed_symbol(writer, length_code.symbol)?;
                if length_code.extra_bits > 0 {
                    writer.write_bits(length_code.extra, length_code.extra_bits)?;
                }
                let Some(distance_code) = DeflateWriter::distance_symbol(distance) else {
                    return Err(zip_static("deflate distance 범위 오류"));
                };
                writer.write_bits(reverse_low_bits(distance_code.symbol, 5), 5)?;
                if distance_code.extra_bits > 0 {
                    writer.write_bits(distance_code.extra, distance_code.extra_bits)?;
                }
            }
        }
    }
    write_fixed_symbol(writer, 256)
}
fn write_fixed_symbol<W>(writer: &mut W, symbol: u16) -> ZipResult<()>
where
    W: BitSink,
{
    let (code, bit_count) = match symbol {
        0..=143 => (0x30_u16.saturating_add(symbol), 8),
        144..=255 => (0x190_u16.saturating_add(symbol.saturating_sub(144)), 9),
        256..=279 => (symbol.saturating_sub(256), 7),
        280..=287 => (0xc0_u16.saturating_add(symbol.saturating_sub(280)), 8),
        _ => return Err(zip_static("deflate fixed symbol 범위 오류")),
    };
    writer.write_bits(reverse_low_bits(code, bit_count), bit_count)
}
