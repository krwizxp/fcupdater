use super::{
    CODE_LENGTH_ORDER, CODE_LENGTH_SYMBOLS, DEFLATE_MAX_BITS, DEFLATE_MAX_BITS_U8, DISTANCE_BASES,
    DISTANCE_EXTRA_BITS, DISTANCE_SYMBOLS, FIXED_DISTANCE_SYMBOLS, FIXED_LITERAL_SYMBOLS,
    HASH_SIZE, LENGTH_BASES, LENGTH_EXTRA_BITS, LITERAL_LENGTH_SYMBOLS, MAX_CHAIN, MAX_MATCH,
    MIN_MATCH, ZipResult, crc32_update, crc32_update_byte, read_u16, zip_static, zip_with_source,
};
use crate::diagnostic::try_vec_with_capacity;
use core::{
    array::from_fn,
    iter::{chain, once, repeat_n, zip},
    mem,
    range::Range,
};
use std::{io::Write as IoWrite, process, sync::LazyLock};
macro_rules! matching_prefix_16 {
    ($left:expr, $right:expr) => {{
        // SAFETY: The caller keeps both complete 16-byte ranges inside the input slice.
        let first = unsafe { $left.cast::<u64>().read_unaligned() }
            // SAFETY: The caller keeps both complete 16-byte ranges inside the input slice.
            ^ unsafe { $right.cast::<u64>().read_unaligned() };
        if first == 0 {
            // SAFETY: The caller keeps both complete 16-byte ranges inside the input slice.
            let second = unsafe { $left.add(8).cast::<u64>().read_unaligned() }
                // SAFETY: The caller keeps both complete 16-byte ranges inside the input slice.
                ^ unsafe { $right.add(8).cast::<u64>().read_unaligned() };
            let [prefix, ..] = second
                .trailing_zeros()
                .div_euclid(u8::BITS)
                .to_le_bytes();
            8_usize.strict_add(usize::from(prefix))
        } else {
            let [prefix, ..] = first
                .trailing_zeros()
                .div_euclid(u8::BITS)
                .to_le_bytes();
            usize::from(prefix)
        }
    }};
}
const DECODE_MAX_SYMBOLS: usize = FIXED_LITERAL_SYMBOLS;
const DECODE_ROOT_BITS: u8 = 9;
const DECODE_ROOT_SIZE: usize = 1 << DECODE_ROOT_BITS;
const HUFFMAN_NODE_CAPACITY: usize = LITERAL_LENGTH_SYMBOLS.strict_mul(2).strict_add(1);
const DEFLATE_SEARCH_WORK_LIMIT: usize = 512 * 1024 * 1024;
const DEFLATE_STREAM_BUFFER_LEN: usize = 8192;
const DEFLATE_TOKEN_RESERVE_CHUNK: usize = 64 * 1024;
const DEFLATE_WINDOW_LEN: usize = 0x8000;
const DEFLATE_MAX_DISTANCE: usize = DEFLATE_WINDOW_LEN - MAX_MATCH - MIN_MATCH - 1;
const EXCEL_FLUSH_BLOCK_COUNT: u8 = 2;
const FINAL_BLOCK_BOUNDARY_COUNT: usize = 1;
const EXCEL_TOKEN_BLOCK_LIMIT: usize = 8191;
const MATCH_CHUNK_BYTES: usize = 16;
const XML_MAX_INSERT_MATCH_LEN: usize = 4;
const XML_NICE_MATCH_LEN: usize = 128;
static FIXED_DECODE_TREES: LazyLock<DynamicTrees> = LazyLock::new(|| {
    let literal_lengths: [u8; FIXED_LITERAL_SYMBOLS] = from_fn(|symbol| match symbol {
        0..=143 | 280..=287 => 8,
        144..=255 => 9,
        256..=279 => 7,
        _ => 0,
    });
    let distance_lengths = [5_u8; FIXED_DISTANCE_SYMBOLS];
    let literal = DecodeHuffman::from_lengths(&literal_lengths)
        .unwrap_or_else(|_| process::abort())
        .unwrap_or_else(|| process::abort());
    let distance = DecodeHuffman::from_lengths(&distance_lengths)
        .unwrap_or_else(|_| process::abort())
        .unwrap_or_else(|| process::abort());
    DynamicTrees {
        distance: Some(distance),
        literal,
    }
});
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
    bit_buffer: u64,
    bit_count: u8,
    buffer: [u8; DEFLATE_STREAM_BUFFER_LEN],
    buffered_len: usize,
    len: usize,
    writer: &'writer mut dyn IoWrite,
}
struct DecodeHuffman {
    counts: [u16; DEFLATE_MAX_BITS + 1],
    first_codes: [u16; DEFLATE_MAX_BITS + 1],
    first_symbols: [u16; DEFLATE_MAX_BITS + 1],
    root: [DecodeEntry; DECODE_ROOT_SIZE],
    symbols: [u16; DECODE_MAX_SYMBOLS],
}
#[derive(Clone, Copy)]
struct DecodeEntry(u16);
struct WriteHuffman {
    codes: Vec<u16>,
    lengths: Vec<u8>,
}
#[derive(Clone, Copy)]
enum DeflateToken {
    Literal(u16),
    Match { distance: u16, length: u16 },
}
impl DeflateToken {
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
    previous: Vec<u16>,
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
    crc32: u32,
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
    distance: Option<DecodeHuffman>,
    literal: DecodeHuffman,
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
struct HuffmanBuildState {
    depths: [u16; HUFFMAN_NODE_CAPACITY],
    frequencies: [u32; HUFFMAN_NODE_CAPACITY],
    heap: [usize; HUFFMAN_NODE_CAPACITY],
    heap_len: usize,
    heap_max: usize,
    max_symbol: usize,
    parents: [usize; HUFFMAN_NODE_CAPACITY],
}
impl DecodeEntry {
    const EMPTY: Self = Self(0);
    const fn bit_len(self) -> u8 {
        self.0.to_le_bytes()[1] >> 1
    }
    const fn is_direct(self) -> bool {
        self.0 != 0
    }
    const fn symbol(self) -> u16 {
        self.0 & 0x01ff
    }
}
impl BitReader<'_> {
    const fn align_to_byte(&mut self) {
        self.bit_buffer = 0;
        self.bit_count = 0;
    }
    fn consume_bits(&mut self, count: u8) -> ZipResult<()> {
        (count <= self.bit_count).ok_or_else(|| zip_static("deflate bit buffer 소비 범위 오류"))?;
        self.bit_buffer >>= u32::from(count);
        self.bit_count = self.bit_count.strict_sub(count);
        Ok(())
    }
    fn read_bits(&mut self, count: u8) -> ZipResult<u16> {
        let value = self
            .try_peek_bits(count)
            .ok_or_else(|| zip_static("deflate bitstream이 예기치 않게 끝났습니다."))?;
        self.consume_bits(count)?;
        Ok(value)
    }
    fn read_stored_bytes(&mut self, len: usize) -> ZipResult<&[u8]> {
        self.align_to_byte();
        let start = self.cursor;
        let end = start
            .checked_add(len)
            .ok_or_else(|| zip_static("deflate 저장 블록 크기 계산 실패"))?;
        let Some(bytes) = self.bytes.get(Range { start, end }) else {
            return Err(zip_static("deflate 저장 블록이 입력보다 깁니다."));
        };
        self.cursor = end;
        Ok(bytes)
    }
    fn try_peek_bits(&mut self, count: u8) -> Option<u16> {
        while self.bit_count < count {
            let &byte = self.bytes.get(self.cursor)?;
            self.bit_buffer |= u32::from(byte) << u32::from(self.bit_count);
            self.cursor = self.cursor.strict_add(1);
            self.bit_count = self.bit_count.strict_add(8);
        }
        let mask = 1_u32.strict_shl(u32::from(count)).strict_sub(1);
        let value = self.bit_buffer & mask;
        let [low, high, _, _] = value.to_le_bytes();
        Some(u16::from_le_bytes([low, high]))
    }
}
impl BitCounter {
    const fn add_bits(&mut self, count: usize) {
        self.bit_len = self.bit_len.strict_add(count);
    }
    const fn byte_len(&self) -> usize {
        self.bit_len.div_ceil(8)
    }
}
impl BitSink for BitCounter {
    fn align_to_byte(&mut self) -> ZipResult<()> {
        self.bit_len = self.bit_len.next_multiple_of(8);
        Ok(())
    }
    fn write_bits(&mut self, _value: u16, count: u8) -> ZipResult<()> {
        self.add_bits(usize::from(count));
        Ok(())
    }
}
impl BitSink for BitWriter<'_> {
    fn align_to_byte(&mut self) -> ZipResult<()> {
        let padding = 8_u8.strict_sub(self.bit_count).rem_euclid(8);
        self.write_bits(0, padding)
    }
    fn write_bits(&mut self, value: u16, count: u8) -> ZipResult<()> {
        let mask = u16::MAX.unbounded_shr(u16::BITS.strict_sub(u32::from(count)));
        self.bit_buffer |= u64::from(value & mask) << self.bit_count;
        self.bit_count = self.bit_count.strict_add(count);
        while self.bit_count >= 8 {
            self.write_byte(self.bit_buffer.to_le_bytes()[0])?;
            self.bit_buffer >>= 8_u8;
            self.bit_count = self.bit_count.strict_sub(8);
        }
        Ok(())
    }
}
impl BitWriter<'_> {
    fn flush_buffer(&mut self) -> ZipResult<()> {
        if self.buffered_len == 0 {
            return Ok(());
        }
        let buffered = self
            .buffer
            .get(..self.buffered_len)
            .unwrap_or_else(|| process::abort());
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
        let slot = self
            .buffer
            .get_mut(self.buffered_len)
            .unwrap_or_else(|| process::abort());
        *slot = byte;
        self.buffered_len = self.buffered_len.strict_add(1);
        self.len = self.len.strict_add(1);
        Ok(())
    }
}
impl DecodeHuffman {
    fn decode(&self, reader: &mut BitReader<'_>) -> ZipResult<u16> {
        if let Some(prefix) = reader.try_peek_bits(DECODE_ROOT_BITS) {
            let entry = self
                .root
                .get(usize::from(prefix))
                .copied()
                .ok_or_else(|| zip_static("deflate root decode table 범위 오류"))?;
            if entry.is_direct() {
                reader.consume_bits(entry.bit_len())?;
                return Ok(entry.symbol());
            }
        }
        let mut code = 0_u16;
        for ((&count, &first_code), &first_symbol) in self
            .counts
            .iter()
            .zip(self.first_codes.iter())
            .zip(self.first_symbols.iter())
            .skip(1)
        {
            code = code.strict_shl(1) | reader.read_bits(1)?;
            let Some(offset) = code.checked_sub(first_code) else {
                continue;
            };
            if offset >= count {
                continue;
            }
            let symbol_index = usize::from(first_symbol.strict_add(offset));
            return self
                .symbols
                .get(symbol_index)
                .copied()
                .ok_or_else(|| zip_static("deflate canonical symbol 범위 오류"));
        }
        Err(zip_static("deflate Huffman code를 해석하지 못했습니다."))
    }
    fn from_lengths(lengths: &[u8]) -> ZipResult<Option<Self>> {
        if lengths.len() > DECODE_MAX_SYMBOLS {
            return Err(zip_static("deflate decode symbol 수가 너무 많습니다."));
        }
        let mut counts = [0_u16; DEFLATE_MAX_BITS + 1];
        let Some(mut next_codes) = canonical_next_codes(lengths, &mut counts)? else {
            return Ok(None);
        };
        let first_codes = next_codes;
        let mut symbol_count = 0_u16;
        let first_symbols = counts.map(|count| {
            let first_symbol = symbol_count;
            symbol_count = symbol_count.strict_add(count);
            first_symbol
        });
        let mut root = [DecodeEntry::EMPTY; DECODE_ROOT_SIZE];
        let mut symbols = [0_u16; DECODE_MAX_SYMBOLS];
        for (symbol_u16, &bit_len) in zip(0_u16.., lengths) {
            if bit_len == 0 {
                continue;
            }
            let bit_index = usize::from(bit_len);
            let assigned = huffman_get(&next_codes, bit_index);
            let code_limit = 1_u16.strict_shl(u32::from(bit_len));
            if assigned >= code_limit {
                return Err(zip_static("deflate Huffman code가 과포화되었습니다."));
            }
            huffman_set(&mut next_codes, bit_index, assigned.strict_add(1));
            let symbol_index = usize::from(
                huffman_get(&first_symbols, bit_index)
                    .strict_add(assigned.strict_sub(huffman_get(&first_codes, bit_index))),
            );
            huffman_set(&mut symbols, symbol_index, symbol_u16);
            if bit_len > DECODE_ROOT_BITS {
                continue;
            }
            let suffix_bits = DECODE_ROOT_BITS.strict_sub(bit_len);
            let repetitions = 1_usize.strict_shl(u32::from(suffix_bits));
            let base = usize::from(reverse_low_bits(assigned, bit_len));
            let entry = DecodeEntry(
                symbol_u16 | u16::from(bit_len).strict_shl(u32::from(DECODE_ROOT_BITS)),
            );
            for suffix in 0..repetitions {
                let root_index = base | suffix.strict_shl(u32::from(bit_len));
                let root_slot = root
                    .get_mut(root_index)
                    .ok_or_else(|| zip_static("deflate root decode table 생성 범위 오류"))?;
                if root_slot.is_direct() {
                    return Err(zip_static("deflate root decode code가 충돌합니다."));
                }
                *root_slot = entry;
            }
        }
        Ok(Some(Self {
            counts,
            first_codes,
            first_symbols,
            root,
            symbols,
        }))
    }
}
impl WriteHuffman {
    fn from_lengths(lengths: Vec<u8>) -> ZipResult<Self> {
        let mut length_counts = [0_u16; DEFLATE_MAX_BITS + 1];
        let mut next_code =
            canonical_next_codes(&lengths, &mut length_counts)?.unwrap_or_else(|| process::abort());
        let mut codes =
            try_vec_with_capacity(lengths.len(), "deflate 출력 Huffman code 메모리 확보 실패")?;
        codes.resize(lengths.len(), 0_u16);
        for (code_slot, &len) in codes.iter_mut().zip(&lengths) {
            if len == 0 {
                continue;
            }
            let next_slot = next_code
                .get_mut(usize::from(len))
                .unwrap_or_else(|| process::abort());
            *code_slot = reverse_low_bits(*next_slot, len);
            *next_slot = next_slot.strict_add(1);
        }
        Ok(Self { codes, lengths })
    }
    fn write_symbol<W>(&self, writer: &mut W, symbol: u16) -> ZipResult<()>
    where
        W: BitSink,
    {
        let index = usize::from(symbol);
        let len = huffman_get(&self.lengths, index);
        if len == 0 {
            process::abort();
        }
        let code = huffman_get(&self.codes, index);
        writer.write_bits(code, len)
    }
}
struct InflateState<'bytes> {
    crc32: u32,
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
        let output_start = self.output.len();
        let source_start = self.output.len().strict_sub(distance);
        let initial_copy = length.min(distance);
        let initial_end = source_start.strict_add(initial_copy);
        self.output.extend_from_within(source_start..initial_end);
        let mut copied = initial_copy;
        while copied < length {
            let copy_len = copied.min(length.strict_sub(copied));
            let copy_end = source_start.strict_add(copy_len);
            self.output.extend_from_within(source_start..copy_end);
            copied = copied.strict_add(copy_len);
        }
        let appended = self
            .output
            .get(output_start..)
            .ok_or_else(|| zip_static("deflate back-reference 출력 범위 오류"))?;
        self.crc32 = crc32_update(self.crc32, appended);
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
        let extra = usize::from(self.reader.read_bits(extra_bits)?);
        Ok(base.strict_add(extra))
    }
    fn decode_length(&mut self, symbol: u16) -> ZipResult<usize> {
        let index = usize::from(symbol.strict_sub(257));
        let Some((&base, &extra_bits)) = LENGTH_BASES.get(index).zip(LENGTH_EXTRA_BITS.get(index))
        else {
            return Err(zip_static("deflate length symbol 범위 오류"));
        };
        let extra = usize::from(self.reader.read_bits(extra_bits)?);
        Ok(base.strict_add(extra))
    }
    fn dynamic_trees(&mut self) -> ZipResult<DynamicTrees> {
        let literal_count = usize::from(self.reader.read_bits(5)?).strict_add(257);
        if literal_count > LITERAL_LENGTH_SYMBOLS {
            return Err(zip_static("deflate HLIT 범위 오류"));
        }
        let distance_count = usize::from(self.reader.read_bits(5)?).strict_add(1);
        let code_length_count = usize::from(self.reader.read_bits(4)?).strict_add(4);
        let mut code_lengths = [0_u8; 19];
        for &symbol in CODE_LENGTH_ORDER.iter().take(code_length_count) {
            let Some(slot) = code_lengths.get_mut(symbol) else {
                return Err(zip_static("deflate code length symbol 범위 오류"));
            };
            let [length, _] = self.reader.read_bits(3)?.to_le_bytes();
            *slot = length;
        }
        let code_tree = DecodeHuffman::from_lengths(&code_lengths)?
            .ok_or_else(|| zip_static("deflate code length tree가 비어 있습니다."))?;
        let total = literal_count
            .checked_add(distance_count)
            .ok_or_else(|| zip_static("deflate code length 총합 계산 실패"))?;
        let mut lengths = try_vec_with_capacity(total, "deflate code length 메모리 확보 실패")?;
        while lengths.len() < total {
            let symbol = code_tree.decode(&mut self.reader)?;
            match symbol {
                0..=15 => {
                    let [length, _] = symbol.to_le_bytes();
                    lengths.push(length);
                }
                16 => {
                    let Some(&previous) = lengths.last() else {
                        return Err(zip_static("deflate repeat code에 이전 길이가 없습니다."));
                    };
                    let repeat = usize::from(self.reader.read_bits(2)?).strict_add(3);
                    push_repeated(&mut lengths, previous, repeat, total)?;
                }
                17 => {
                    let repeat = usize::from(self.reader.read_bits(3)?).strict_add(3);
                    push_repeated(&mut lengths, 0, repeat, total)?;
                }
                18 => {
                    let repeat = usize::from(self.reader.read_bits(7)?).strict_add(11);
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
        let literal = DecodeHuffman::from_lengths(literal_lengths)?
            .ok_or_else(|| zip_static("deflate literal Huffman tree가 비어 있습니다."))?;
        let distance = DecodeHuffman::from_lengths(distance_lengths)?;
        Ok(DynamicTrees { distance, literal })
    }
    fn inflate_compressed_block(
        &mut self,
        literal_tree: &DecodeHuffman,
        distance_tree: Option<&DecodeHuffman>,
    ) -> ZipResult<()> {
        loop {
            let symbol = literal_tree.decode(&mut self.reader)?;
            match symbol {
                0..=255 => {
                    ensure_deflate_output_len(self.output.len(), 1, self.expected_len)?;
                    let [byte, _] = symbol.to_le_bytes();
                    self.crc32 = crc32_update_byte(self.crc32, byte);
                    self.output.push(byte);
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
        self.crc32 = crc32_update(self.crc32, stored);
        self.output.extend_from_slice(stored);
        Ok(())
    }
}
impl DeflateInflater<'_> {
    pub(super) fn inflate(self) -> ZipResult<(Vec<u8>, u32)> {
        let output = try_vec_with_capacity(self.expected_len, "deflate 출력 메모리 확보 실패")?;
        let mut state = InflateState {
            crc32: u32::MAX,
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
                1 => state.inflate_compressed_block(
                    &FIXED_DECODE_TREES.literal,
                    FIXED_DECODE_TREES.distance.as_ref(),
                )?,
                2 => {
                    let trees = state.dynamic_trees()?;
                    state.inflate_compressed_block(&trees.literal, trees.distance.as_ref())?;
                }
                _ => return Err(zip_static("지원하지 않는 deflate block type입니다.")),
            }
            if final_block {
                return Ok((state.output, !state.crc32));
            }
        }
    }
}
impl DynamicFrequencies {
    fn collect(&mut self, tokens: &[DeflateToken]) -> (bool, usize) {
        let end_freq = self
            .literal
            .get_mut(256)
            .unwrap_or_else(|| process::abort());
        *end_freq = 1;
        let mut fixed_bit_len = 3_usize;
        let mut has_distance = false;
        for &token in tokens {
            match token {
                DeflateToken::Literal(byte) => {
                    let freq = self
                        .literal
                        .get_mut(usize::from(byte))
                        .unwrap_or_else(|| process::abort());
                    *freq = freq.strict_add(1);
                    fixed_bit_len = fixed_bit_len.strict_add(if byte <= 143 { 8 } else { 9 });
                }
                DeflateToken::Match { distance, length } => {
                    let length_code = DeflateWriter::length_symbol(length);
                    let length_freq = self
                        .literal
                        .get_mut(usize::from(length_code.symbol))
                        .unwrap_or_else(|| process::abort());
                    *length_freq = length_freq.strict_add(1);
                    let distance_code = DeflateWriter::distance_symbol(distance);
                    let distance_freq = self
                        .distance
                        .get_mut(usize::from(distance_code.symbol))
                        .unwrap_or_else(|| process::abort());
                    *distance_freq = distance_freq.strict_add(1);
                    let match_bit_len = (if length_code.symbol <= 279 {
                        7_usize
                    } else {
                        8_usize
                    })
                    .strict_add(usize::from(length_code.extra_bits))
                    .strict_add(5)
                    .strict_add(usize::from(distance_code.extra_bits));
                    fixed_bit_len = fixed_bit_len.strict_add(match_bit_len);
                    has_distance = true;
                }
            }
        }
        (has_distance, fixed_bit_len.strict_add(7))
    }
    fn encoded_bit_len(&self, plan: &DynamicDeflatePlan) -> usize {
        let mut bit_len = 17_usize.strict_add(plan.code_length_count.strict_mul(3));
        for token in &plan.code_length_tokens {
            bit_len = bit_len
                .strict_add(usize::from(huffman_get(
                    &plan.code_huffman.lengths,
                    usize::from(token.symbol),
                )))
                .strict_add(usize::from(token.extra_bits));
        }
        for (symbol, (&frequency, &code_len)) in self
            .literal
            .iter()
            .zip(&plan.literal_huffman.lengths)
            .enumerate()
        {
            let extra_bits = symbol
                .checked_sub(257)
                .and_then(|index| LENGTH_EXTRA_BITS.get(index))
                .copied()
                .unwrap_or(0);
            let symbol_bits = usize::from(code_len).strict_add(usize::from(extra_bits));
            bit_len = bit_len.strict_add(
                usize::try_from(frequency)
                    .unwrap_or_else(|_| process::abort())
                    .strict_mul(symbol_bits),
            );
        }
        for ((&frequency, &code_len), &extra_bits) in self
            .distance
            .iter()
            .zip(&plan.distance_huffman.lengths)
            .zip(&DISTANCE_EXTRA_BITS)
        {
            let symbol_bits = usize::from(code_len).strict_add(usize::from(extra_bits));
            bit_len = bit_len.strict_add(
                usize::try_from(frequency)
                    .unwrap_or_else(|_| process::abort())
                    .strict_mul(symbol_bits),
            );
        }
        bit_len
    }
    fn plan(&self) -> ZipResult<DynamicDeflatePlan> {
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
            .map_or(257, |index| index.strict_add(1).max(257));
        let distance_count = distance_lengths
            .iter()
            .rposition(|&len| len != 0)
            .map_or(1, |index| index.strict_add(1));
        let literal_prefix = literal_lengths
            .get(..literal_count)
            .unwrap_or_else(|| process::abort());
        let distance_prefix = distance_lengths
            .get(..distance_count)
            .unwrap_or_else(|| process::abort());
        let mut code_length_tokens = try_vec_with_capacity(
            literal_count.strict_add(distance_count),
            "deflate code length token 메모리 확보 실패",
        )?;
        let mut lengths = chain(literal_prefix, distance_prefix)
            .copied()
            .chain(once(u8::MAX));
        let mut previous = u8::MAX;
        let mut next = lengths.next().unwrap_or_else(|| process::abort());
        let mut count = 0_usize;
        let (mut maximum_count, mut minimum_count) = if next == 0 {
            (138_usize, 3_usize)
        } else {
            (7_usize, 4_usize)
        };
        for next_value in lengths {
            let current = next;
            next = next_value;
            count = count.strict_add(1);
            if count < maximum_count && current == next {
                continue;
            }
            if count < minimum_count {
                code_length_tokens.extend(repeat_n(
                    CodeLengthToken {
                        extra: 0,
                        extra_bits: 0,
                        symbol: current,
                    },
                    count,
                ));
            } else if current != 0 {
                if current != previous {
                    code_length_tokens.push(CodeLengthToken {
                        extra: 0,
                        extra_bits: 0,
                        symbol: current,
                    });
                    count = count.strict_sub(1);
                }
                code_length_tokens.push(CodeLengthToken {
                    extra: u16::try_from(count.strict_sub(3)).unwrap_or_else(|_| process::abort()),
                    extra_bits: 2,
                    symbol: 16,
                });
            } else if count <= 10 {
                code_length_tokens.push(CodeLengthToken {
                    extra: u16::try_from(count.strict_sub(3)).unwrap_or_else(|_| process::abort()),
                    extra_bits: 3,
                    symbol: 17,
                });
            } else {
                code_length_tokens.push(CodeLengthToken {
                    extra: u16::try_from(count.strict_sub(11)).unwrap_or_else(|_| process::abort()),
                    extra_bits: 7,
                    symbol: 18,
                });
            }
            count = 0;
            previous = current;
            (maximum_count, minimum_count) = if next == 0 {
                (138, 3)
            } else if current == next {
                (6, 3)
            } else {
                (7, 4)
            };
        }
        let mut code_length_freq = [0_u32; CODE_LENGTH_SYMBOLS];
        for token in &code_length_tokens {
            let freq = code_length_freq
                .get_mut(usize::from(token.symbol))
                .unwrap_or_else(|| process::abort());
            *freq = freq.strict_add(1);
        }
        let code_lengths = (HuffmanLengthBuilder {
            frequencies: &code_length_freq,
            max_bits: 7,
        })
        .build()?;
        let code_length_count = CODE_LENGTH_ORDER
            .iter()
            .rposition(|&symbol| huffman_get(&code_lengths, symbol) != 0)
            .map_or(4, |index| index.strict_add(1).max(4));
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
    fn write_block<W>(&self, tokens: &[DeflateToken], writer: &mut W) -> ZipResult<()>
    where
        W: BitSink,
    {
        writer.write_bits(0, 1)?;
        writer.write_bits(2, 2)?;
        writer.write_bits(
            u16::try_from(self.literal_count.strict_sub(257)).unwrap_or_else(|_| process::abort()),
            5,
        )?;
        writer.write_bits(
            u16::try_from(self.distance_count.strict_sub(1)).unwrap_or_else(|_| process::abort()),
            5,
        )?;
        writer.write_bits(
            u16::try_from(self.code_length_count.strict_sub(4))
                .unwrap_or_else(|_| process::abort()),
            4,
        )?;
        for &symbol in CODE_LENGTH_ORDER.iter().take(self.code_length_count) {
            let len = huffman_get(&self.code_huffman.lengths, symbol);
            writer.write_bits(u16::from(len), 3)?;
        }
        for &token in &self.code_length_tokens {
            self.code_huffman
                .write_symbol(writer, u16::from(token.symbol))?;
            writer.write_bits(token.extra, token.extra_bits)?;
        }
        for &token in tokens {
            match token {
                DeflateToken::Literal(byte) => self.literal_huffman.write_symbol(writer, byte)?,
                DeflateToken::Match { distance, length } => {
                    let length_code = DeflateWriter::length_symbol(length);
                    self.literal_huffman
                        .write_symbol(writer, length_code.symbol)?;
                    writer.write_bits(length_code.extra, length_code.extra_bits)?;
                    let distance_code = DeflateWriter::distance_symbol(distance);
                    self.distance_huffman
                        .write_symbol(writer, distance_code.symbol)?;
                    writer.write_bits(distance_code.extra, distance_code.extra_bits)?;
                }
            }
        }
        self.literal_huffman.write_symbol(writer, 256)
    }
}
impl HuffmanLengthBuilder<'_> {
    fn build(&self) -> ZipResult<Vec<u8>> {
        let symbol_count = self.frequencies.len();
        let node_capacity = symbol_count.strict_mul(2).strict_add(1);
        let mut state = HuffmanBuildState {
            depths: [0_u16; HUFFMAN_NODE_CAPACITY],
            frequencies: [0_u32; HUFFMAN_NODE_CAPACITY],
            heap: [0_usize; HUFFMAN_NODE_CAPACITY],
            heap_len: 0,
            heap_max: node_capacity,
            max_symbol: 0,
            parents: [usize::MAX; HUFFMAN_NODE_CAPACITY],
        };
        state
            .frequencies
            .get_mut(..symbol_count)
            .unwrap_or_else(|| process::abort())
            .copy_from_slice(self.frequencies);
        for (symbol, &frequency) in self.frequencies.iter().enumerate() {
            if frequency == 0 {
                continue;
            }
            state.heap_len = state.heap_len.strict_add(1);
            huffman_set(&mut state.heap, state.heap_len, symbol);
            state.max_symbol = symbol;
        }
        while state.heap_len < 2 {
            let symbol = if state.max_symbol < 2 {
                state.max_symbol = state.max_symbol.strict_add(1);
                state.max_symbol
            } else {
                0
            };
            huffman_set(&mut state.frequencies, symbol, 1);
            state.heap_len = state.heap_len.strict_add(1);
            huffman_set(&mut state.heap, state.heap_len, symbol);
        }
        for index in (1..=state.heap_len.div_euclid(2)).rev() {
            state.heap_down(index);
        }
        let mut next_node = symbol_count;
        while state.heap_len >= 2 {
            let left = huffman_get(&state.heap, 1);
            let tail = huffman_get(&state.heap, state.heap_len);
            huffman_set(&mut state.heap, 1, tail);
            state.heap_len = state.heap_len.strict_sub(1);
            state.heap_down(1);
            let right = huffman_get(&state.heap, 1);
            state.heap_max = state.heap_max.strict_sub(1);
            huffman_set(&mut state.heap, state.heap_max, left);
            state.heap_max = state.heap_max.strict_sub(1);
            huffman_set(&mut state.heap, state.heap_max, right);
            let parent_frequency = huffman_get(&state.frequencies, left)
                .strict_add(huffman_get(&state.frequencies, right));
            huffman_set(&mut state.frequencies, next_node, parent_frequency);
            let parent_depth = huffman_get(&state.depths, left)
                .max(huffman_get(&state.depths, right))
                .strict_add(1);
            huffman_set(&mut state.depths, next_node, parent_depth);
            huffman_set(&mut state.parents, left, next_node);
            huffman_set(&mut state.parents, right, next_node);
            huffman_set(&mut state.heap, 1, next_node);
            next_node = next_node.strict_add(1);
            state.heap_down(1);
        }
        state.heap_max = state.heap_max.strict_sub(1);
        let root = huffman_get(&state.heap, 1);
        huffman_set(&mut state.heap, state.heap_max, root);
        let maximum_length = usize::from(self.max_bits);
        let mut length_counts = [0_usize; DEFLATE_MAX_BITS + 1];
        let mut node_lengths = [0_u8; HUFFMAN_NODE_CAPACITY];
        let mut overflow = 0_usize;
        let ordered_nodes = state
            .heap
            .get(state.heap_max.strict_add(1)..node_capacity)
            .unwrap_or_else(|| process::abort());
        for &node in ordered_nodes {
            let parent = huffman_get(&state.parents, node);
            let mut length = usize::from(huffman_get(&node_lengths, parent)).strict_add(1);
            if length > maximum_length {
                length = maximum_length;
                overflow = overflow.strict_add(1);
            }
            huffman_set(
                &mut node_lengths,
                node,
                u8::try_from(length).unwrap_or_else(|_| process::abort()),
            );
            if node <= state.max_symbol {
                let count = huffman_get(&length_counts, length).strict_add(1);
                huffman_set(&mut length_counts, length, count);
            }
        }
        if overflow == 0 {
            let mut lengths = try_vec_filled(
                symbol_count,
                0_u8,
                "deflate Huffman length 메모리 확보 실패",
            )?;
            lengths
                .get_mut(..symbol_count)
                .unwrap_or_else(|| process::abort())
                .copy_from_slice(
                    node_lengths
                        .get(..symbol_count)
                        .unwrap_or_else(|| process::abort()),
                );
            return Ok(lengths);
        }
        while overflow != 0 {
            let shorter_bits = (1..maximum_length)
                .rev()
                .find(|&bits| huffman_get(&length_counts, bits) != 0)
                .unwrap_or_else(|| process::abort());
            let shorter = huffman_get(&length_counts, shorter_bits).strict_sub(1);
            huffman_set(&mut length_counts, shorter_bits, shorter);
            let longer_bits = shorter_bits.strict_add(1);
            let longer = huffman_get(&length_counts, longer_bits).strict_add(2);
            huffman_set(&mut length_counts, longer_bits, longer);
            let maximum = huffman_get(&length_counts, maximum_length).strict_sub(1);
            huffman_set(&mut length_counts, maximum_length, maximum);
            overflow = overflow.strict_sub(2);
        }
        let mut lengths = try_vec_filled(
            symbol_count,
            0_u8,
            "deflate Huffman length 메모리 확보 실패",
        )?;
        let mut ordered_index = node_capacity;
        for bits in (1..=maximum_length).rev() {
            let mut remaining = huffman_get(&length_counts, bits);
            while remaining != 0 {
                let symbol = loop {
                    ordered_index = ordered_index.strict_sub(1);
                    let candidate = huffman_get(&state.heap, ordered_index);
                    if candidate <= state.max_symbol {
                        break candidate;
                    }
                };
                let assigned = u8::try_from(bits).unwrap_or_else(|_| process::abort());
                huffman_set(&mut lengths, symbol, assigned);
                remaining = remaining.strict_sub(1);
            }
        }
        Ok(lengths)
    }
}
impl HuffmanBuildState {
    fn heap_down(&mut self, mut index: usize) {
        let node = huffman_get(&self.heap, index);
        let mut child = index.strict_mul(2);
        while child <= self.heap_len {
            if child < self.heap_len {
                let left = huffman_get(&self.heap, child);
                let right = huffman_get(&self.heap, child.strict_add(1));
                if self.node_is_smaller(right, left) {
                    child = child.strict_add(1);
                }
            }
            let child_node = huffman_get(&self.heap, child);
            if self.node_is_smaller(node, child_node) {
                break;
            }
            huffman_set(&mut self.heap, index, child_node);
            index = child;
            child = index.strict_mul(2);
        }
        huffman_set(&mut self.heap, index, node);
    }
    fn node_is_smaller(&self, left: usize, right: usize) -> bool {
        let left_frequency = huffman_get(&self.frequencies, left);
        let right_frequency = huffman_get(&self.frequencies, right);
        let left_depth = huffman_get(&self.depths, left);
        let right_depth = huffman_get(&self.depths, right);
        left_frequency < right_frequency
            || (left_frequency == right_frequency && left_depth <= right_depth)
    }
}
impl DeflatePlan {
    pub(super) const fn crc32(&self) -> u32 {
        self.crc32
    }
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
                writer.write_bits(0, 1)?;
                writer.write_bits(1, 2)?;
                for &token in tokens {
                    match token {
                        DeflateToken::Literal(byte) => write_fixed_symbol(writer, byte)?,
                        DeflateToken::Match { distance, length } => {
                            let length_code = DeflateWriter::length_symbol(length);
                            write_fixed_symbol(writer, length_code.symbol)?;
                            writer.write_bits(length_code.extra, length_code.extra_bits)?;
                            let distance_code = DeflateWriter::distance_symbol(distance);
                            writer.write_bits(reverse_low_bits(distance_code.symbol, 5), 5)?;
                            writer.write_bits(distance_code.extra, distance_code.extra_bits)?;
                        }
                    }
                }
                write_fixed_symbol(writer, 256)?;
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
        if bit_writer.bit_count > 0 {
            bit_writer.write_byte(bit_writer.bit_buffer.to_le_bytes()[0])?;
        }
        bit_writer.flush_buffer()?;
        Ok(bit_writer.len)
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
        let initial_token_capacity = input_len.min(DEFLATE_TOKEN_RESERVE_CHUNK);
        self.tokens
            .try_reserve_exact(initial_token_capacity)
            .map_err(|source| zip_with_source("deflate token 메모리 확보 실패", source))?;
        self.head.clear();
        self.head
            .try_reserve_exact(HASH_SIZE)
            .map_err(|source| zip_with_source("deflate hash head 메모리 확보 실패", source))?;
        self.head.resize(HASH_SIZE, usize::MAX);
        self.previous.clear();
        self.previous
            .try_reserve_exact(DEFLATE_WINDOW_LEN)
            .map_err(|source| zip_with_source("deflate hash previous 메모리 확보 실패", source))?;
        self.previous.resize(DEFLATE_WINDOW_LEN, 0);
        Ok(())
    }
    pub(super) fn recycle(&mut self, plan: DeflatePlan) {
        self.tokens = plan.tokens;
    }
}
impl DeflateWriter<'_, '_> {
    fn distance_symbol(distance: u16) -> DeflateSymbol {
        let distance_value = usize::from(distance);
        let index = DISTANCE_BASES
            .partition_point(|&base| base <= distance_value)
            .checked_sub(1)
            .unwrap_or_else(|| process::abort());
        let (&base, &extra_bits) = DISTANCE_BASES
            .get(index)
            .zip(DISTANCE_EXTRA_BITS.get(index))
            .unwrap_or_else(|| process::abort());
        DeflateSymbol {
            extra: u16::try_from(distance_value.strict_sub(base))
                .unwrap_or_else(|_| process::abort()),
            extra_bits,
            symbol: u16::try_from(index).unwrap_or_else(|_| process::abort()),
        }
    }
    fn insert_position(bytes: &[u8], position: usize, head: &mut [usize], previous: &mut [u16]) {
        let Some(hash) = hash3(bytes, position) else {
            return;
        };
        let head_slot = head.get_mut(hash).unwrap_or_else(|| process::abort());
        let previous_distance = position
            .checked_sub(*head_slot)
            .and_then(|distance| u16::try_from(distance).ok())
            .filter(|distance| usize::from(*distance) <= DEFLATE_WINDOW_LEN)
            .unwrap_or(0);
        let slot = previous
            .get_mut(position.rem_euclid(DEFLATE_WINDOW_LEN))
            .unwrap_or_else(|| process::abort());
        *slot = previous_distance;
        *head_slot = position;
    }
    fn length_symbol(length: u16) -> DeflateSymbol {
        let length_value = usize::from(length);
        let index = LENGTH_BASES
            .partition_point(|&base| base <= length_value)
            .checked_sub(1)
            .unwrap_or_else(|| process::abort());
        let (&base, &extra_bits) = LENGTH_BASES
            .get(index)
            .zip(LENGTH_EXTRA_BITS.get(index))
            .unwrap_or_else(|| process::abort());
        DeflateSymbol {
            extra: u16::try_from(length_value.strict_sub(base))
                .unwrap_or_else(|_| process::abort()),
            extra_bits,
            symbol: 257_u16.strict_add(u16::try_from(index).unwrap_or_else(|_| process::abort())),
        }
    }
    pub(super) fn plan(&mut self, part_name: &str) -> ZipResult<Option<DeflatePlan>> {
        let mut boundaries = try_vec_with_capacity(
            FINAL_BLOCK_BOUNDARY_COUNT,
            "deflate block 경계 메모리 확보 실패",
        )?;
        if matches!(
            part_name,
            "xl/worksheets/sheet1.xml" | "xl/worksheets/sheet2.xml"
        ) {
            let sheet_data_open = b"<sheetData>";
            let sheet_data_close = b"</sheetData>";
            let open_start = find_bytes(self.bytes, sheet_data_open)
                .ok_or_else(|| zip_static("worksheet sheetData 시작 태그가 없습니다."))?;
            let content_start = open_start
                .checked_add(sheet_data_open.len())
                .ok_or_else(|| zip_static("worksheet sheetData 내용 시작 계산 실패"))?;
            let after_open = self
                .bytes
                .get(content_start..)
                .ok_or_else(|| zip_static("worksheet sheetData 내용 범위 오류"))?;
            let close_start = find_bytes(after_open, sheet_data_close)
                .and_then(|offset| content_start.checked_add(offset))
                .ok_or_else(|| zip_static("worksheet sheetData 종료 태그가 없습니다."))?;
            let close_end = close_start
                .checked_add(sheet_data_close.len())
                .ok_or_else(|| zip_static("worksheet sheetData 종료 경계 계산 실패"))?;
            if open_start >= close_start || close_end > self.bytes.len() {
                return Err(zip_static(
                    "worksheet sheetData 경계 순서가 올바르지 않습니다.",
                ));
            }
            boundaries
                .try_reserve_exact(2)
                .map_err(|source| zip_with_source("deflate block 경계 메모리 확보 실패", source))?;
            boundaries.push(DeflateOutputBoundary {
                empty_stored_after: EXCEL_FLUSH_BLOCK_COUNT,
                output_end: open_start,
            });
            boundaries.push(DeflateOutputBoundary {
                empty_stored_after: EXCEL_FLUSH_BLOCK_COUNT,
                output_end: close_end,
            });
        }
        boundaries.push(DeflateOutputBoundary {
            empty_stored_after: 0,
            output_end: self.bytes.len(),
        });
        let Some((tokens, crc32)) = self.tokens(&boundaries)? else {
            return Ok(None);
        };
        let maximum_block_count = tokens
            .len()
            .div_ceil(EXCEL_TOKEN_BLOCK_LIMIT)
            .checked_add(boundaries.len())
            .ok_or_else(|| zip_static("deflate block plan 수 계산 실패"))?;
        let mut blocks: Vec<DeflateBlockPlan> =
            try_vec_with_capacity(maximum_block_count, "deflate block plan 메모리 확보 실패")?;
        let mut output_len = 0_usize;
        let mut token_end = 0_usize;
        let mut token_start = 0_usize;
        let mut counter = BitCounter { bit_len: 0 };
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
            if output_len != boundary.output_end {
                return Err(zip_static("deflate token이 출력 경계를 교차했습니다."));
            }
            while token_start < token_end {
                let block_end = token_start
                    .checked_add(EXCEL_TOKEN_BLOCK_LIMIT)
                    .map_or(token_end, |candidate| candidate.min(token_end));
                let empty_stored_after = if block_end == token_end {
                    boundary.empty_stored_after
                } else {
                    0
                };
                let token_range = Range {
                    start: token_start,
                    end: block_end,
                };
                let block_tokens = tokens
                    .get(token_range)
                    .ok_or_else(|| zip_static("deflate block token 범위 오류"))?;
                let mut frequencies = DynamicFrequencies {
                    distance: [0_u32; DISTANCE_SYMBOLS],
                    literal: [0_u32; LITERAL_LENGTH_SYMBOLS],
                };
                let (has_distance, fixed_bit_len) = frequencies.collect(block_tokens);
                let dynamic_plan = has_distance.then(|| frequencies.plan()).transpose()?;
                let (chosen_dynamic, bit_len) =
                    dynamic_plan.map_or((None, fixed_bit_len), |dynamic| {
                        let dynamic_bit_len = frequencies.encoded_bit_len(&dynamic);
                        if dynamic_bit_len < fixed_bit_len {
                            (Some(dynamic), dynamic_bit_len)
                        } else {
                            (None, fixed_bit_len)
                        }
                    });
                counter.add_bits(bit_len);
                for _ in 0..empty_stored_after {
                    write_empty_stored_block(&mut counter)?;
                }
                blocks.push(DeflateBlockPlan {
                    dynamic_plan: chosen_dynamic,
                    empty_stored_after,
                    token_range,
                });
                token_start = block_end;
            }
            if token_start == token_end && boundary.empty_stored_after != 0 {
                let last = blocks.last().ok_or_else(|| {
                    zip_static("deflate empty stored block 앞에 data block이 없습니다.")
                })?;
                if last.token_range.end != token_end {
                    return Err(zip_static(
                        "deflate empty stored block 경계가 올바르지 않습니다.",
                    ));
                }
            }
        }
        if token_end != tokens.len() || output_len != self.bytes.len() {
            return Err(zip_static(
                "deflate block plan이 전체 입력을 포함하지 않습니다.",
            ));
        }
        write_empty_stored_block(&mut counter)?;
        counter.write_bits(1, 1)?;
        counter.write_bits(1, 2)?;
        write_fixed_symbol(&mut counter, 256)?;
        let compressed_len = counter.byte_len();
        Ok(Some(DeflatePlan {
            blocks,
            compressed_len,
            crc32,
            tokens,
        }))
    }
    fn tokens(
        &mut self,
        boundaries: &[DeflateOutputBoundary],
    ) -> ZipResult<Option<(Vec<DeflateToken>, u32)>> {
        let bytes = self.bytes;
        let workspace = &mut *self.workspace;
        workspace.prepare_for_input(bytes.len())?;
        let mut tokens = mem::take(&mut workspace.tokens);
        let head = &mut workspace.head;
        let previous = &mut workspace.previous;
        let work_budget = &mut workspace.work_budget;
        let mut boundary_index = 0_usize;
        let mut skip_next_hash_insert = true;
        let mut position = 0_usize;
        while position < bytes.len() {
            while let Some(boundary) = boundaries
                .get(boundary_index)
                .filter(|boundary| boundary.output_end <= position)
            {
                if boundary.empty_stored_after != 0 {
                    head.fill(usize::MAX);
                    previous.fill(0);
                    skip_next_hash_insert = true;
                }
                boundary_index = boundary_index.strict_add(1);
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
                let min_candidate = position.saturating_sub(DEFLATE_MAX_DISTANCE);
                let max_len = boundary_end
                    .strict_sub(position)
                    .min(bytes.len().strict_sub(position))
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
                    while max_len.strict_sub(len) >= MATCH_CHUNK_BYTES
                        && work_budget.remaining >= MATCH_CHUNK_BYTES
                    {
                        let left = bytes.as_ptr().wrapping_add(candidate.strict_add(len));
                        let right = bytes.as_ptr().wrapping_add(position.strict_add(len));
                        let prefix = matching_prefix_16!(left, right);
                        let compared = prefix.saturating_add(1).min(MATCH_CHUNK_BYTES);
                        work_budget.remaining = work_budget.remaining.strict_sub(compared);
                        len = len.strict_add(prefix);
                        if prefix < MATCH_CHUNK_BYTES {
                            mismatch_found = true;
                            break;
                        }
                    }
                    while !mismatch_found && len < max_len {
                        if !work_budget.consume() {
                            workspace.tokens = tokens;
                            return Ok(None);
                        }
                        if bytes.get(candidate.strict_add(len))
                            != bytes.get(position.strict_add(len))
                        {
                            break;
                        }
                        len = len.strict_add(1);
                    }
                    if len > best_len && len >= MIN_MATCH {
                        best_len = len;
                        best_distance = position.strict_sub(candidate);
                        if len >= XML_NICE_MATCH_LEN {
                            break;
                        }
                    }
                    let Some(&previous_distance) =
                        previous.get(candidate.rem_euclid(DEFLATE_WINDOW_LEN))
                    else {
                        break;
                    };
                    if previous_distance == 0 {
                        break;
                    }
                    let Some(previous_candidate) =
                        candidate.checked_sub(usize::from(previous_distance))
                    else {
                        break;
                    };
                    candidate = previous_candidate;
                    chain_len = chain_len.strict_add(1);
                }
            }
            if skip_next_hash_insert {
                skip_next_hash_insert = false;
            } else {
                Self::insert_position(bytes, position, head, previous);
            }
            if best_len >= MIN_MATCH {
                push_deflate_token(
                    &mut tokens,
                    DeflateToken::Match {
                        distance: deflate_u16(best_distance, "deflate match distance 변환 실패")?,
                        length: deflate_u16(best_len, "deflate match length 변환 실패")?,
                    },
                )?;
                let next_position = position
                    .checked_add(best_len)
                    .ok_or_else(|| zip_static("deflate 위치 계산 실패"))?;
                if best_len <= XML_MAX_INSERT_MATCH_LEN {
                    for insert_position in position.strict_add(1)..next_position {
                        Self::insert_position(bytes, insert_position, head, previous);
                    }
                }
                position = next_position;
            } else {
                let Some(&byte) = bytes.get(position) else {
                    return Err(zip_static("deflate literal 범위 오류"));
                };
                push_deflate_token(&mut tokens, DeflateToken::Literal(u16::from(byte)))?;
                position = position.strict_add(1);
            }
        }
        Ok(Some((tokens, !crc32_update(u32::MAX, bytes))))
    }
}
fn huffman_get<T>(values: &[T], index: usize) -> T
where
    T: Copy,
{
    values
        .get(index)
        .copied()
        .unwrap_or_else(|| process::abort())
}
fn huffman_set<T>(values: &mut [T], index: usize, value: T) {
    *values.get_mut(index).unwrap_or_else(|| process::abort()) = value;
}
fn try_vec_filled<T>(len: usize, value: T, context: &'static str) -> ZipResult<Vec<T>>
where
    T: Clone,
{
    let mut values = try_vec_with_capacity(len, context)?;
    values.resize(len, value);
    Ok(values)
}
fn canonical_next_codes(
    lengths: &[u8],
    length_counts: &mut [u16; DEFLATE_MAX_BITS + 1],
) -> ZipResult<Option<[u16; DEFLATE_MAX_BITS + 1]>> {
    for &length in lengths {
        if length == 0 {
            continue;
        }
        let Some(count) = length_counts.get_mut(usize::from(length)) else {
            return Err(zip_static("deflate Huffman code 길이가 너무 깁니다."));
        };
        *count = count.strict_add(1);
    }
    if length_counts.iter().skip(1).all(|count| *count == 0) {
        return Ok(None);
    }
    let mut next_codes = [0_u16; DEFLATE_MAX_BITS + 1];
    let mut code = 0_u16;
    for (next_code, &previous_count) in next_codes.iter_mut().skip(1).zip(length_counts.iter()) {
        code = code
            .checked_add(previous_count)
            .and_then(|sum| sum.checked_mul(2))
            .ok_or_else(|| zip_static("deflate Huffman canonical code가 범위를 초과했습니다."))?;
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
    (next_len <= expected_len)
        .ok_or_else(|| zip_static("deflate 출력이 ZIP 선언 해제 크기를 초과했습니다."))
}
fn hash3(bytes: &[u8], position: usize) -> Option<usize> {
    let &[first_byte, second_byte, third_byte] = bytes.get(position..)?.first_chunk::<3>()?;
    let first = usize::from(first_byte);
    let second = usize::from(second_byte);
    let third = usize::from(third_byte);
    Some(((first << 10_usize) ^ (second << 5_usize) ^ third) & HASH_SIZE.strict_sub(1))
}
fn push_deflate_token(tokens: &mut Vec<DeflateToken>, token: DeflateToken) -> ZipResult<()> {
    if tokens.len() == tokens.capacity() {
        tokens
            .try_reserve(DEFLATE_TOKEN_RESERVE_CHUNK)
            .map_err(|source| zip_with_source("deflate token 메모리 확장 실패", source))?;
    }
    tokens.push(token);
    Ok(())
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
    (next_len <= total)
        .ok_or_else(|| zip_static("deflate repeat 길이가 code length 총합을 초과합니다."))?;
    lengths.extend(repeat_n(value, repeat));
    Ok(())
}
fn reverse_low_bits(value: u16, count: u8) -> u16 {
    value
        .reverse_bits()
        .unbounded_shr(u16::BITS.strict_sub(u32::from(count)))
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
fn write_fixed_symbol<W>(writer: &mut W, symbol: u16) -> ZipResult<()>
where
    W: BitSink,
{
    let (code, bit_count) = match symbol {
        0..=143 => (0x30_u16.strict_add(symbol), 8),
        144..=255 => (0x190_u16.strict_add(symbol.strict_sub(144)), 9),
        256..=279 => (symbol.strict_sub(256), 7),
        280..=287 => (0xc0_u16.strict_add(symbol.strict_sub(280)), 8),
        _ => return Err(zip_static("deflate fixed symbol 범위 오류")),
    };
    writer.write_bits(reverse_low_bits(code, bit_count), bit_count)
}
