use super::{
    CODE_LENGTH_ORDER, CODE_LENGTH_SYMBOLS, DEFLATE_MAX_BITS, DEFLATE_MAX_BITS_U8, DISTANCE_BASES,
    DISTANCE_EXTRA_BITS, DISTANCE_SYMBOLS, FIXED_DISTANCE_SYMBOLS, FIXED_LITERAL_SYMBOLS,
    HASH_SIZE, LENGTH_BASES, LENGTH_EXTRA_BITS, LITERAL_LENGTH_SYMBOLS, MAX_CHAIN, MAX_MATCH,
    MIN_MATCH, crc32_update, read_u16,
};
use crate::diagnostic::{
    Result, bail, ensure, err, err_with_source, map_reserve, or_abort, try_push_vec,
    try_vec_with_capacity,
};
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
            8_usize.strict_add(second.trailing_zeros().div_euclid(u8::BITS) as usize)
        } else {
            first.trailing_zeros().div_euclid(u8::BITS) as usize
        }
    }};
}
const DECODE_MAX_SYMBOLS: usize = FIXED_LITERAL_SYMBOLS;
const DECODE_ROOT_BITS: u8 = 9;
const DECODE_ROOT_SIZE: usize = 1 << DECODE_ROOT_BITS;
const HUFFMAN_NODE_CAPACITY: usize = LITERAL_LENGTH_SYMBOLS.strict_mul(2).strict_add(1);
const DEFLATE_TOKEN_RESERVE_CHUNK: usize = 64 * 1024;
const DEFLATE_WINDOW_LEN: usize = 0x8000;
const DEFLATE_MAX_DISTANCE: usize = DEFLATE_WINDOW_LEN - MAX_MATCH - MIN_MATCH - 1;
const TOKEN_BLOCK_LIMIT: usize = 8191;
const DEFLATE_STREAM_BUFFER_LEN: usize = 8192;
const MATCH_CHUNK_BYTES: usize = 16;
const LENGTH_SYMBOL_TABLE: [u16; 256] = {
    let mut table = [28_u16; 256];
    let mut slots: &mut [u16] = &mut table;
    let mut value = 0_u16;
    while let Some((slot, tail)) = slots.split_first_mut() {
        if value < 255 {
            let [extra_bits, _, _, _] = value.bit_width().saturating_sub(3).to_le_bytes();
            let extra_u16 = extra_bits as u16;
            let code_idx = if extra_bits == 0 {
                value
            } else {
                extra_u16.strict_add(1).strict_mul(4) | ((value >> extra_bits) & 3)
            };
            let extra = value & 1_u16.strict_shl(extra_bits as u32).strict_sub(1);
            *slot = code_idx | (extra_u16 << 5_u32) | (extra << 8_u32);
        }
        value = value.strict_add(1);
        slots = tail;
    }
    table
};
static FIXED_DECODE_TREES: LazyLock<(DecodeHuffman, Option<DecodeHuffman>)> = LazyLock::new(|| {
    let literal_lengths: [u8; FIXED_LITERAL_SYMBOLS] = from_fn(|symbol| match symbol {
        0..=143 | 280..=287 => 8,
        144..=255 => 9,
        256..=279 => 7,
        _ => 0,
    });
    let literal = or_abort(
        DecodeHuffman::from_lengths(&literal_lengths, false).unwrap_or_else(|_| process::abort()),
    );
    let distance = or_abort(
        DecodeHuffman::from_lengths(&[5_u8; FIXED_DISTANCE_SYMBOLS], false)
            .unwrap_or_else(|_| process::abort()),
    );
    (literal, Some(distance))
});
struct BitReader<'bytes> {
    bit_buffer: u64,
    bit_count: u8,
    bytes: &'bytes [u8],
    cursor: usize,
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
    root: [u16; DECODE_ROOT_SIZE],
    symbols: [u16; DECODE_MAX_SYMBOLS],
}
pub(super) struct DeflateInflater<'bytes> {
    pub bytes: &'bytes [u8],
    pub expected_len: usize,
}
#[derive(Default)]
pub(super) struct DeflateWorkspace {
    head: Vec<u32>,
    huffman_state: Option<Box<HuffmanBuildState>>,
    position_bias: u32,
    previous: Vec<u16>,
    tokens: Vec<u32>,
    work_budget: usize,
}
pub(super) struct DeflateWriter<'bytes, 'workspace> {
    pub bytes: &'bytes [u8],
    pub workspace: &'workspace mut DeflateWorkspace,
}
pub(super) struct DeflatePlan {
    blocks: Vec<(Option<DynamicDeflatePlan>, Range<usize>)>,
    compressed_len: usize,
    crc32: u32,
    tokens: Vec<u32>,
}
struct DynamicDeflatePlan {
    code_huffman: [u32; CODE_LENGTH_SYMBOLS],
    code_length_count: usize,
    code_length_tokens: Vec<(u8, u16, u8)>,
    distance_count: usize,
    distance_huffman: [u32; DISTANCE_SYMBOLS],
    literal_count: usize,
    literal_huffman: [u32; LITERAL_LENGTH_SYMBOLS],
}
struct DynamicFrequencies {
    distance: [u32; DISTANCE_SYMBOLS],
    literal: [u32; LITERAL_LENGTH_SYMBOLS],
}
struct HuffmanBuildState {
    depths: [u16; HUFFMAN_NODE_CAPACITY],
    frequencies: [u32; HUFFMAN_NODE_CAPACITY],
    heap: [u16; HUFFMAN_NODE_CAPACITY],
    heap_len: usize,
    heap_max: usize,
    max_symbol: u16,
    parents: [u16; HUFFMAN_NODE_CAPACITY],
}
struct InflateState<'bytes> {
    expected_len: usize,
    output: Vec<u8>,
    reader: BitReader<'bytes>,
}
impl BitReader<'_> {
    fn read_bits(&mut self, count: u8) -> Result<u16> {
        let value = self
            .try_peek_bits(count)
            .ok_or("deflate bitstream이 예기치 않게 끝났습니다.")?;
        ensure(count <= self.bit_count, "deflate bit buffer 소비 범위 오류")?;
        self.bit_buffer >>= u32::from(count);
        self.bit_count = self.bit_count.strict_sub(count);
        Ok(value)
    }
    fn read_stored_bytes(&mut self, len: usize) -> Result<&[u8]> {
        self.cursor = self
            .cursor
            .strict_sub(usize::from(self.bit_count).div_euclid(8));
        self.bit_buffer = 0;
        self.bit_count = 0;
        let end = self
            .cursor
            .checked_add(len)
            .ok_or("deflate 저장 블록 크기 계산 실패")?;
        let bytes = self
            .bytes
            .get(self.cursor..end)
            .ok_or("deflate 저장 블록이 입력보다 깁니다.")?;
        self.cursor = end;
        Ok(bytes)
    }
    fn try_peek_bits(&mut self, count: u8) -> Option<u16> {
        if self.bit_count < count
            && let Some(&chunk) = self
                .bytes
                .get(self.cursor..)
                .and_then(|tail| tail.first_chunk::<4>())
        {
            self.bit_buffer |= u64::from(u32::from_le_bytes(chunk)) << u32::from(self.bit_count);
            self.cursor = self.cursor.strict_add(4);
            self.bit_count = self.bit_count.strict_add(32);
        }
        while self.bit_count < count {
            let &byte = self.bytes.get(self.cursor)?;
            self.bit_buffer |= u64::from(byte) << u32::from(self.bit_count);
            self.cursor = self.cursor.strict_add(1);
            self.bit_count = self.bit_count.strict_add(8);
        }
        let mask = 1_u64.strict_shl(u32::from(count)).strict_sub(1);
        let [low, high, ..] = (self.bit_buffer & mask).to_le_bytes();
        Some(u16::from_le_bytes([low, high]))
    }
}
impl BitWriter<'_> {
    #[cold]
    #[inline(never)]
    fn flush_buffer(&mut self) -> Result<()> {
        if self.buffered_len == 0 {
            return Ok(());
        }
        self.writer
            .write_all(or_abort(self.buffer.get(..self.buffered_len)))
            .map_err(|source| err_with_source("deflate stream 쓰기 실패", source))?;
        self.len = self.len.strict_add(self.buffered_len);
        self.buffered_len = 0;
        Ok(())
    }
    fn write_bits(&mut self, value: u16, count: u8) -> Result<()> {
        self.write_bits_u32(u32::from(value), count)
    }
    fn write_bits_u32(&mut self, value: u32, count: u8) -> Result<()> {
        self.bit_buffer |= u64::from(value) << self.bit_count;
        self.bit_count = self.bit_count.strict_add(count);
        if self.bit_count >= 32 {
            if self.buffer.len().strict_sub(self.buffered_len) < 4 {
                self.flush_buffer()?;
            }
            let [b0, b1, b2, b3, ..] = self.bit_buffer.to_le_bytes();
            or_abort(
                self.buffer
                    .get_mut(self.buffered_len..self.buffered_len.strict_add(4)),
            )
            .copy_from_slice(&[b0, b1, b2, b3]);
            self.buffered_len = self.buffered_len.strict_add(4);
            self.bit_buffer >>= 32_u8;
            self.bit_count = self.bit_count.strict_sub(32);
        }
        Ok(())
    }
    fn write_byte(&mut self, byte: u8) -> Result<()> {
        if self.buffered_len == self.buffer.len() {
            self.flush_buffer()?;
        }
        *or_abort(self.buffer.get_mut(self.buffered_len)) = byte;
        self.buffered_len = self.buffered_len.strict_add(1);
        Ok(())
    }
}
impl DecodeHuffman {
    fn decode(&self, reader: &mut BitReader<'_>) -> Result<u16> {
        let (mut code, skip_bits) = if let Some(prefix) = reader.try_peek_bits(DECODE_ROOT_BITS) {
            let entry = *self
                .root
                .get(usize::from(prefix))
                .ok_or("deflate root decode table 범위 오류")?;
            if entry != 0 {
                let len = entry.to_le_bytes()[1] >> 1_u8;
                reader.bit_buffer >>= len;
                reader.bit_count = reader.bit_count.strict_sub(len);
                return Ok(entry & 0x01ff);
            }
            reader.bit_buffer >>= DECODE_ROOT_BITS;
            reader.bit_count = reader.bit_count.strict_sub(DECODE_ROOT_BITS);
            (
                prefix.reverse_bits() >> u16::BITS.strict_sub(u32::from(DECODE_ROOT_BITS)),
                usize::from(DECODE_ROOT_BITS).strict_add(1),
            )
        } else {
            (0_u16, 1_usize)
        };
        for ((&count, &first_code), &first_symbol) in self
            .counts
            .iter()
            .zip(&self.first_codes)
            .zip(&self.first_symbols)
            .skip(skip_bits)
        {
            code = code.strict_shl(1) | reader.read_bits(1)?;
            if let Some(offset) = code.checked_sub(first_code)
                && offset < count
            {
                return self
                    .symbols
                    .get(usize::from(first_symbol.strict_add(offset)))
                    .copied()
                    .ok_or_else(|| err("deflate canonical symbol 범위 오류"));
            }
        }
        Err(err("deflate Huffman code를 해석하지 못했습니다."))
    }
    fn from_lengths(lengths: &[u8], allow_single_code: bool) -> Result<Option<Self>> {
        ensure(
            lengths.len() <= DECODE_MAX_SYMBOLS,
            "deflate decode symbol 수가 너무 많습니다.",
        )?;
        let mut counts = [0_u16; DEFLATE_MAX_BITS + 1];
        let Some(mut next_codes) = canonical_next_codes(lengths, &mut counts)? else {
            return Ok(None);
        };
        let first_codes = next_codes;
        let mut symbol_count = 0_u16;
        let first_symbols = counts.map(|count| {
            let first = symbol_count;
            symbol_count = symbol_count.strict_add(count);
            first
        });
        let code_end = u32::from(huffman_get(&first_codes, DEFLATE_MAX_BITS))
            .strict_add(u32::from(huffman_get(&counts, DEFLATE_MAX_BITS)));
        let code_limit = 1_u32.strict_shl(u32::from(DEFLATE_MAX_BITS_U8));
        ensure(
            code_end <= code_limit,
            "Oversubscribed deflate Huffman tree",
        )?;
        ensure(
            code_end == code_limit
                || (allow_single_code && symbol_count == 1 && huffman_get(&counts, 1) == 1),
            "Incomplete deflate Huffman tree",
        )?;
        let mut root = [0_u16; DECODE_ROOT_SIZE];
        let mut symbols = [0_u16; DECODE_MAX_SYMBOLS];
        for (symbol_u16, &bit_len) in zip(0_u16.., lengths) {
            if bit_len == 0 {
                continue;
            }
            let bit_index = usize::from(bit_len);
            let assigned = huffman_get(&next_codes, bit_index);
            huffman_set(&mut next_codes, bit_index, assigned.strict_add(1));
            huffman_set(
                &mut symbols,
                usize::from(
                    huffman_get(&first_symbols, bit_index)
                        .strict_add(assigned.strict_sub(huffman_get(&first_codes, bit_index))),
                ),
                symbol_u16,
            );
            if bit_len > DECODE_ROOT_BITS {
                continue;
            }
            let base = usize::from(reverse_low_bits(assigned, bit_len));
            let entry = symbol_u16 | u16::from(bit_len).strict_shl(u32::from(DECODE_ROOT_BITS));
            for suffix in 0..1_usize.strict_shl(u32::from(DECODE_ROOT_BITS.strict_sub(bit_len))) {
                let root_slot = root
                    .get_mut(base | suffix.strict_shl(u32::from(bit_len)))
                    .ok_or("deflate root decode table 생성 범위 오류")?;
                ensure(*root_slot == 0, "deflate root decode code가 충돌합니다.")?;
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
impl InflateState<'_> {
    fn copy_previous(&mut self, distance: usize, length: usize) -> Result<()> {
        ensure(
            distance != 0 && distance <= self.output.len(),
            "deflate back-reference distance가 올바르지 않습니다.",
        )?;
        ensure_deflate_output_len(self.output.len(), length, self.expected_len)?;
        let source_start = self.output.len().strict_sub(distance);
        let mut copied = length.min(distance);
        self.output
            .extend_from_within(source_start..source_start.strict_add(copied));
        while copied < length {
            let copy_len = copied.min(length.strict_sub(copied));
            self.output
                .extend_from_within(source_start..source_start.strict_add(copy_len));
            copied = copied.strict_add(copy_len);
        }
        Ok(())
    }
    fn decode_extra(
        &mut self,
        index: usize,
        bases: &[u16],
        extra_bits_table: &[u8],
        error_message: &'static str,
    ) -> Result<usize> {
        let (&base, &extra_bits) = bases
            .get(index)
            .zip(extra_bits_table.get(index))
            .ok_or(error_message)?;
        let extra = if extra_bits == 0 {
            0_usize
        } else {
            usize::from(self.reader.read_bits(extra_bits)?)
        };
        Ok(usize::from(base).strict_add(extra))
    }
    fn dynamic_trees(&mut self) -> Result<(DecodeHuffman, Option<DecodeHuffman>)> {
        let literal_count = usize::from(self.reader.read_bits(5)?).strict_add(257);
        ensure(
            literal_count <= LITERAL_LENGTH_SYMBOLS,
            "deflate HLIT 범위 오류",
        )?;
        let distance_count = usize::from(self.reader.read_bits(5)?).strict_add(1);
        let code_length_count = usize::from(self.reader.read_bits(4)?).strict_add(4);
        let mut code_lengths = [0_u8; 19];
        for &symbol in CODE_LENGTH_ORDER.iter().take(code_length_count) {
            let slot = code_lengths
                .get_mut(usize::from(symbol))
                .ok_or("deflate code length symbol 범위 오류")?;
            *slot = self.reader.read_bits(3)?.to_le_bytes()[0];
        }
        let code_tree = DecodeHuffman::from_lengths(&code_lengths, false)?
            .ok_or("deflate code length tree가 비어 있습니다.")?;
        let total = literal_count
            .checked_add(distance_count)
            .ok_or("deflate code length 총합 계산 실패")?;
        let mut lengths = [0_u8; LITERAL_LENGTH_SYMBOLS + FIXED_DISTANCE_SYMBOLS];
        let mut length_count = 0_usize;
        while length_count < total {
            let symbol = code_tree.decode(&mut self.reader)?;
            let (value, repeat) = match symbol {
                0..=15 => (symbol.to_le_bytes()[0], 1),
                16 => (
                    length_count
                        .checked_sub(1)
                        .and_then(|index| lengths.get(index))
                        .copied()
                        .ok_or("deflate repeat code에 이전 길이가 없습니다.")?,
                    usize::from(self.reader.read_bits(2)?).strict_add(3),
                ),
                17 => (0, usize::from(self.reader.read_bits(3)?).strict_add(3)),
                18 => (0, usize::from(self.reader.read_bits(7)?).strict_add(11)),
                _ => bail!("deflate code length symbol이 올바르지 않습니다."),
            };
            let next_len = length_count
                .checked_add(repeat)
                .ok_or("deflate repeat 길이 계산 실패")?;
            ensure(
                next_len <= total,
                "deflate repeat 길이가 code length 총합을 초과합니다.",
            )?;
            or_abort(lengths.get_mut(length_count..next_len)).fill(value);
            length_count = next_len;
        }
        let (literal_lengths, distance_lengths) = lengths
            .get(..total)
            .and_then(|all| all.split_at_checked(literal_count))
            .ok_or("deflate literal/distance length 범위 오류")?;
        let literal = DecodeHuffman::from_lengths(literal_lengths, true)?
            .ok_or("deflate literal Huffman tree가 비어 있습니다.")?;
        let distance = DecodeHuffman::from_lengths(distance_lengths, true)?;
        Ok((literal, distance))
    }
    fn inflate_compressed_block(
        &mut self,
        literal_tree: &DecodeHuffman,
        distance_tree: Option<&DecodeHuffman>,
    ) -> Result<()> {
        loop {
            match literal_tree.decode(&mut self.reader)? {
                symbol @ 0..=255 => {
                    ensure(
                        self.output.len() != self.expected_len,
                        "deflate 출력이 ZIP 선언 해제 크기를 초과했습니다.",
                    )?;
                    self.output.push(symbol.to_le_bytes()[0]);
                }
                256 => return Ok(()),
                symbol @ 257..=285 => {
                    let length = self.decode_extra(
                        usize::from(symbol.strict_sub(257)),
                        &LENGTH_BASES,
                        &LENGTH_EXTRA_BITS,
                        "deflate length symbol 범위 오류",
                    )?;
                    let distance_symbol = distance_tree
                        .ok_or("deflate distance tree가 없습니다.")?
                        .decode(&mut self.reader)?;
                    let distance = self.decode_extra(
                        usize::from(distance_symbol),
                        &DISTANCE_BASES,
                        &DISTANCE_EXTRA_BITS,
                        "deflate distance symbol 범위 오류",
                    )?;
                    self.copy_previous(distance, length)?;
                }
                _ => bail!("deflate literal/length symbol이 올바르지 않습니다."),
            }
        }
    }
    fn inflate_stored_block(&mut self) -> Result<()> {
        let header = self.reader.read_stored_bytes(4)?;
        let (len, nlen) = (read_u16(header, 0)?, read_u16(header, 2)?);
        ensure(
            len == !nlen,
            "deflate 저장 블록 LEN/NLEN이 일치하지 않습니다.",
        )?;
        let stored = self.reader.read_stored_bytes(usize::from(len))?;
        ensure_deflate_output_len(self.output.len(), stored.len(), self.expected_len)?;
        self.output.extend_from_slice(stored);
        Ok(())
    }
}
impl DeflateInflater<'_> {
    pub(super) fn inflate(self) -> Result<(Vec<u8>, u32)> {
        let mut state = InflateState {
            expected_len: self.expected_len,
            output: try_vec_with_capacity(self.expected_len, "deflate 출력 메모리 확보 실패")?,
            reader: BitReader {
                bit_buffer: 0,
                bit_count: 0,
                bytes: self.bytes,
                cursor: 0,
            },
        };
        loop {
            let final_block = state.reader.read_bits(1)? != 0;
            match state.reader.read_bits(2)? {
                0 => state.inflate_stored_block()?,
                1 => state.inflate_compressed_block(
                    &FIXED_DECODE_TREES.0,
                    FIXED_DECODE_TREES.1.as_ref(),
                )?,
                2 => {
                    let (literal, distance) = state.dynamic_trees()?;
                    state.inflate_compressed_block(&literal, distance.as_ref())?;
                }
                _ => bail!("지원하지 않는 deflate block type입니다."),
            }
            if final_block {
                let crc32 = !crc32_update(u32::MAX, &state.output);
                return Ok((state.output, crc32));
            }
        }
    }
}
impl DynamicFrequencies {
    fn collect(&mut self, tokens: &[u32]) -> (bool, usize) {
        *or_abort(self.literal.get_mut(256)) = 1;
        let (mut fixed_bit_len, mut has_distance) = (3_usize, false);
        for &token in tokens {
            if token & 0x8000_0000 == 0 {
                let byte = (token & 0xFF) as u16;
                let freq = or_abort(self.literal.get_mut(usize::from(byte)));
                *freq = freq.strict_add(1);
                fixed_bit_len = fixed_bit_len.strict_add(if byte <= 143 { 8 } else { 9 });
            } else {
                let (length_sym, _, length_extra) =
                    DeflateWriter::length_symbol((((token >> 16_u32) & 0xFF) as u16).strict_add(3));
                let (dist_sym, _, dist_extra) =
                    DeflateWriter::distance_symbol(((token & 0x7FFF) as u16).strict_add(1));
                let lf = or_abort(self.literal.get_mut(usize::from(length_sym)));
                *lf = lf.strict_add(1);
                let df = or_abort(self.distance.get_mut(usize::from(dist_sym)));
                *df = df.strict_add(1);
                fixed_bit_len = fixed_bit_len
                    .strict_add(if length_sym <= 279 { 7 } else { 8 })
                    .strict_add(usize::from(length_extra))
                    .strict_add(5)
                    .strict_add(usize::from(dist_extra));
                has_distance = true;
            }
        }
        (has_distance, fixed_bit_len.strict_add(7))
    }
    fn encoded_bit_len(&self, plan: &DynamicDeflatePlan) -> usize {
        let mut bit_len = 17_usize.strict_add(plan.code_length_count.strict_mul(3));
        for &(symbol, _, extra_bits) in &plan.code_length_tokens {
            bit_len = bit_len
                .strict_add(usize::from(
                    huffman_get(&plan.code_huffman, usize::from(symbol)).to_le_bytes()[2],
                ))
                .strict_add(usize::from(extra_bits));
        }
        for (symbol, (&frequency, &code)) in
            self.literal.iter().zip(&plan.literal_huffman).enumerate()
        {
            let extra_bits = symbol
                .checked_sub(257)
                .and_then(|index| LENGTH_EXTRA_BITS.get(index))
                .copied()
                .unwrap_or(0);
            bit_len = bit_len.strict_add((frequency as usize).strict_mul(
                usize::from(code.to_le_bytes()[2]).strict_add(usize::from(extra_bits)),
            ));
        }
        for ((&frequency, &code), &extra_bits) in self
            .distance
            .iter()
            .zip(&plan.distance_huffman)
            .zip(&DISTANCE_EXTRA_BITS)
        {
            bit_len = bit_len.strict_add((frequency as usize).strict_mul(
                usize::from(code.to_le_bytes()[2]).strict_add(usize::from(extra_bits)),
            ));
        }
        bit_len
    }
    fn plan(&self, state: &mut HuffmanBuildState) -> Result<DynamicDeflatePlan> {
        let (mut literal_lengths, mut distance_lengths) =
            ([0_u8; LITERAL_LENGTH_SYMBOLS], [0_u8; DISTANCE_SYMBOLS]);
        build_huffman_lengths(
            state,
            &self.literal,
            DEFLATE_MAX_BITS_U8,
            &mut literal_lengths,
        );
        build_huffman_lengths(
            state,
            &self.distance,
            DEFLATE_MAX_BITS_U8,
            &mut distance_lengths,
        );
        let literal_count = literal_lengths
            .iter()
            .rposition(|&len| len != 0)
            .map_or(257, |index| index.strict_add(1).max(257));
        let distance_count = distance_lengths
            .iter()
            .rposition(|&len| len != 0)
            .map_or(1, |index| index.strict_add(1));
        let mut code_length_tokens = try_vec_with_capacity(
            literal_count.strict_add(distance_count),
            "deflate code length token 메모리 확보 실패",
        )?;
        let mut lengths = chain(
            or_abort(literal_lengths.get(..literal_count)),
            or_abort(distance_lengths.get(..distance_count)),
        )
        .copied()
        .chain(once(u8::MAX));
        let (mut previous, mut next, mut count) = (u8::MAX, or_abort(lengths.next()), 0_usize);
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
                code_length_tokens.extend(repeat_n((current, 0_u16, 0_u8), count));
            } else {
                if current != 0 && current != previous {
                    code_length_tokens.push((current, 0, 0));
                    count = count.strict_sub(1);
                }
                let (symbol, extra_bits, base) = if current != 0 {
                    (16_u8, 2_u8, 3_usize)
                } else if count <= 10 {
                    (17, 3, 3)
                } else {
                    (18, 7, 11)
                };
                code_length_tokens.push((
                    symbol,
                    or_abort(u16::try_from(count.strict_sub(base)).ok()),
                    extra_bits,
                ));
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
        for &(symbol, _, _) in &code_length_tokens {
            let freq = or_abort(code_length_freq.get_mut(usize::from(symbol)));
            *freq = freq.strict_add(1);
        }
        let mut code_lengths = [0_u8; CODE_LENGTH_SYMBOLS];
        build_huffman_lengths(state, &code_length_freq, 7, &mut code_lengths);
        let code_length_count = CODE_LENGTH_ORDER
            .iter()
            .rposition(|&symbol| huffman_get(&code_lengths, usize::from(symbol)) != 0)
            .map_or(4, |index| index.strict_add(1).max(4));
        let mut literal_huffman = [0_u32; LITERAL_LENGTH_SYMBOLS];
        fill_write_huffman_codes(&literal_lengths, &mut literal_huffman)?;
        let mut distance_huffman = [0_u32; DISTANCE_SYMBOLS];
        fill_write_huffman_codes(&distance_lengths, &mut distance_huffman)?;
        let mut code_huffman = [0_u32; CODE_LENGTH_SYMBOLS];
        fill_write_huffman_codes(&code_lengths, &mut code_huffman)?;
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
    fn write_block(&self, tokens: &[u32], writer: &mut BitWriter<'_>) -> Result<()> {
        writer.write_bits(0, 1)?;
        writer.write_bits(2, 2)?;
        writer.write_bits(
            or_abort(u16::try_from(self.literal_count.strict_sub(257)).ok()),
            5,
        )?;
        writer.write_bits(
            or_abort(u16::try_from(self.distance_count.strict_sub(1)).ok()),
            5,
        )?;
        writer.write_bits(
            or_abort(u16::try_from(self.code_length_count.strict_sub(4)).ok()),
            4,
        )?;
        for &symbol in CODE_LENGTH_ORDER.iter().take(self.code_length_count) {
            let len = huffman_get(&self.code_huffman, usize::from(symbol)).to_le_bytes()[2];
            writer.write_bits(u16::from(len), 3)?;
        }
        for &(symbol, extra, extra_bits) in &self.code_length_tokens {
            write_huffman_symbol(&self.code_huffman, writer, u16::from(symbol))?;
            writer.write_bits(extra, extra_bits)?;
        }
        write_deflate_tokens(
            tokens,
            writer,
            Some((
                self.literal_huffman.as_slice(),
                self.distance_huffman.as_slice(),
            )),
        )
    }
}
impl HuffmanBuildState {
    fn heap_down(&mut self, mut index: usize) {
        let node = huffman_get(&self.heap, index);
        let mut child = index.strict_mul(2);
        while child <= self.heap_len {
            if child < self.heap_len
                && self.node_is_smaller(
                    huffman_get(&self.heap, child.strict_add(1)),
                    huffman_get(&self.heap, child),
                )
            {
                child = child.strict_add(1);
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
    fn node_is_smaller(&self, left: u16, right: u16) -> bool {
        let (li, ri) = (usize::from(left), usize::from(right));
        let (lf, rf) = (
            huffman_get(&self.frequencies, li),
            huffman_get(&self.frequencies, ri),
        );
        lf < rf || (lf == rf && huffman_get(&self.depths, li) <= huffman_get(&self.depths, ri))
    }
}
impl DeflatePlan {
    pub(super) const fn crc32(&self) -> u32 {
        self.crc32
    }
    pub(super) const fn len(&self) -> usize {
        self.compressed_len
    }
    pub(super) fn write_to(&self, writer: &mut dyn IoWrite) -> Result<usize> {
        let mut bit_writer = BitWriter {
            bit_buffer: 0,
            bit_count: 0,
            buffer: [0; DEFLATE_STREAM_BUFFER_LEN],
            buffered_len: 0,
            len: 0,
            writer,
        };
        for block in &self.blocks {
            let tokens = self
                .tokens
                .get(block.1)
                .ok_or("deflate block token 범위 오류")?;
            if let Some(plan) = block.0.as_ref() {
                plan.write_block(tokens, &mut bit_writer)?;
            } else {
                bit_writer.write_bits(0, 1)?;
                bit_writer.write_bits(1, 2)?;
                write_deflate_tokens(tokens, &mut bit_writer, None)?;
            }
        }
        bit_writer.write_bits(1, 1)?;
        bit_writer.write_bits(1, 2)?;
        write_fixed_symbol(&mut bit_writer, 256)?;
        while bit_writer.bit_count >= 8 {
            bit_writer.write_byte(bit_writer.bit_buffer.to_le_bytes()[0])?;
            bit_writer.bit_buffer >>= 8_u8;
            bit_writer.bit_count = bit_writer.bit_count.strict_sub(8);
        }
        if bit_writer.bit_count > 0 {
            bit_writer.write_byte(bit_writer.bit_buffer.to_le_bytes()[0])?;
        }
        bit_writer.flush_buffer()?;
        Ok(bit_writer.len)
    }
}
impl DeflateWorkspace {
    fn prepare_for_input(&mut self, input_len: usize) -> Result<()> {
        self.work_budget = 512 * 1024 * 1024;
        self.tokens.clear();
        map_reserve(
            self.tokens
                .try_reserve_exact(input_len.min(DEFLATE_TOKEN_RESERVE_CHUNK)),
            "deflate token 메모리 확보 실패",
        )?;
        let needs_reset = self.head.len() != HASH_SIZE
            || self.position_bias == 0
            || self
                .position_bias
                .checked_add(u32::try_from(input_len).unwrap_or(u32::MAX))
                .and_then(|end| end.checked_add(1))
                .is_none();
        if needs_reset {
            if self.head.len() == HASH_SIZE {
                self.head.fill(0);
            } else {
                self.head.clear();
                map_reserve(
                    self.head.try_reserve_exact(HASH_SIZE),
                    "deflate hash head 메모리 확보 실패",
                )?;
                self.head.resize(HASH_SIZE, 0);
            }
            self.position_bias = 1;
        }
        let previous_len = input_len.min(DEFLATE_WINDOW_LEN);
        if self.previous.len() < previous_len {
            map_reserve(
                self.previous
                    .try_reserve_exact(previous_len.strict_sub(self.previous.len())),
                "deflate hash previous 메모리 확보 실패",
            )?;
            self.previous.resize(previous_len, 0);
        }
        Ok(())
    }
    pub(super) fn recycle(&mut self, plan: DeflatePlan) {
        self.tokens = plan.tokens;
    }
}
impl DeflateWriter<'_, '_> {
    fn distance_symbol(distance: u16) -> (u16, u16, u8) {
        let value = distance.strict_sub(1);
        let extra_bits = value.bit_width().saturating_sub(2).to_le_bytes()[0];
        let symbol = if extra_bits == 0 {
            value
        } else {
            u16::from(extra_bits).strict_add(1).strict_mul(2) | ((value >> extra_bits) & 1)
        };
        (
            symbol,
            value & 1_u16.strict_shl(u32::from(extra_bits)).strict_sub(1),
            extra_bits,
        )
    }
    fn insert_position(
        position: usize,
        position_bias: u32,
        hash_index: Option<usize>,
        head: &mut [u32],
        previous: &mut [u16],
    ) {
        let Some(hash) = hash_index else {
            return;
        };
        let head_slot = or_abort(head.get_mut(hash));
        let previous_distance = (*head_slot)
            .checked_sub(position_bias)
            .and_then(|head_pos| position.checked_sub(head_pos as usize))
            .and_then(|distance| u16::try_from(distance).ok())
            .filter(|&distance| usize::from(distance) <= DEFLATE_WINDOW_LEN)
            .unwrap_or(0);
        *or_abort(previous.get_mut(position.rem_euclid(DEFLATE_WINDOW_LEN))) = previous_distance;
        *head_slot = u32::try_from(position)
            .ok()
            .and_then(|pos| pos.checked_add(position_bias))
            .unwrap_or(0);
    }
    fn length_symbol(length: u16) -> (u16, u16, u8) {
        let [low, extra_byte] =
            huffman_get(&LENGTH_SYMBOL_TABLE, usize::from(length.strict_sub(3))).to_le_bytes();
        (
            257_u16.strict_add(u16::from(low & 0x1f)),
            u16::from(extra_byte),
            low >> 5_u8,
        )
    }
    pub(super) fn plan(&mut self) -> Result<Option<DeflatePlan>> {
        let Some((tokens, crc32)) = self.tokens()? else {
            return Ok(None);
        };
        let mut blocks = try_vec_with_capacity(
            tokens.len().div_ceil(TOKEN_BLOCK_LIMIT),
            "deflate block plan 메모리 확보 실패",
        )?;
        let mut total_bits = 10_usize;
        for (block_index, block_tokens) in tokens.chunks(TOKEN_BLOCK_LIMIT).enumerate() {
            let start = block_index.strict_mul(TOKEN_BLOCK_LIMIT);
            let token_range = Range {
                start,
                end: start.strict_add(block_tokens.len()),
            };
            let mut frequencies = DynamicFrequencies {
                distance: [0_u32; DISTANCE_SYMBOLS],
                literal: [0_u32; LITERAL_LENGTH_SYMBOLS],
            };
            let (has_distance, fixed_bit_len) = frequencies.collect(block_tokens);
            let dynamic_plan = if has_distance {
                let state = self.workspace.huffman_state.get_or_insert_with(|| {
                    Box::new(HuffmanBuildState {
                        depths: [0_u16; HUFFMAN_NODE_CAPACITY],
                        frequencies: [0_u32; HUFFMAN_NODE_CAPACITY],
                        heap: [0_u16; HUFFMAN_NODE_CAPACITY],
                        heap_len: 0,
                        heap_max: 0,
                        max_symbol: 0,
                        parents: [u16::MAX; HUFFMAN_NODE_CAPACITY],
                    })
                });
                Some(frequencies.plan(state)?)
            } else {
                None
            };
            let (chosen_dynamic, bit_len) = dynamic_plan.map_or((None, fixed_bit_len), |dynamic| {
                let dynamic_bit_len = frequencies.encoded_bit_len(&dynamic);
                if dynamic_bit_len < fixed_bit_len {
                    (Some(dynamic), dynamic_bit_len)
                } else {
                    (None, fixed_bit_len)
                }
            });
            total_bits = total_bits.strict_add(bit_len);
            blocks.push((chosen_dynamic, token_range));
        }
        Ok(Some(DeflatePlan {
            blocks,
            compressed_len: total_bits.div_ceil(8),
            crc32,
            tokens,
        }))
    }
    fn tokens(&mut self) -> Result<Option<(Vec<u32>, u32)>> {
        let bytes = self.bytes;
        let workspace = &mut *self.workspace;
        workspace.prepare_for_input(bytes.len())?;
        let mut tokens = mem::take(&mut workspace.tokens);
        let position_bias = workspace.position_bias;
        workspace.position_bias = u32::try_from(bytes.len())
            .ok()
            .and_then(|len| position_bias.checked_add(len))
            .and_then(|end| end.checked_add(1))
            .unwrap_or(0);
        let (head, previous, work_budget) = (
            &mut workspace.head,
            &mut workspace.previous,
            &mut workspace.work_budget,
        );
        let mut position = 0_usize;
        while position < bytes.len() {
            let (mut best_len, mut best_distance) = (0_usize, 0_usize);
            let position_hash = hash3(bytes, position);
            if let Some(hash) = position_hash
                && let Some(&head_candidate) = head.get(hash)
            {
                let mut candidate = head_candidate
                    .checked_sub(position_bias)
                    .map_or(usize::MAX, |pos| pos as usize);
                let min_candidate = position.saturating_sub(DEFLATE_MAX_DISTANCE);
                let max_len = bytes.len().strict_sub(position).min(MAX_MATCH);
                let nice_len = 128_usize.min(max_len);
                let (mut best_tail, mut chain_len) = (None, 0_usize);
                while candidate != usize::MAX
                    && candidate >= min_candidate
                    && candidate < position
                    && chain_len < MAX_CHAIN
                {
                    let Some(rem) = work_budget.checked_sub(1) else {
                        workspace.tokens = tokens;
                        return Ok(None);
                    };
                    *work_budget = rem;
                    if best_len < MIN_MATCH
                        || bytes.get(candidate.strict_add(best_len)).copied() == best_tail
                    {
                        let (mut len, mut mismatch_found) = (0_usize, false);
                        while max_len.strict_sub(len) >= MATCH_CHUNK_BYTES
                            && *work_budget >= MATCH_CHUNK_BYTES
                        {
                            let left = bytes.as_ptr().wrapping_add(candidate.strict_add(len));
                            let right = bytes.as_ptr().wrapping_add(position.strict_add(len));
                            let prefix = matching_prefix_16!(left, right);
                            *work_budget = work_budget
                                .strict_sub(prefix.saturating_add(1).min(MATCH_CHUNK_BYTES));
                            len = len.strict_add(prefix);
                            if prefix < MATCH_CHUNK_BYTES {
                                mismatch_found = true;
                                break;
                            }
                        }
                        if !mismatch_found && len < max_len {
                            let candidate_tail = or_abort(
                                bytes.get(candidate.strict_add(len)..candidate.strict_add(max_len)),
                            );
                            let current_tail = or_abort(
                                bytes.get(position.strict_add(len)..position.strict_add(max_len)),
                            );
                            for (&left, &right) in zip(candidate_tail, current_tail) {
                                let Some(remaining) = work_budget.checked_sub(1) else {
                                    workspace.tokens = tokens;
                                    return Ok(None);
                                };
                                *work_budget = remaining;
                                if left != right {
                                    break;
                                }
                                len = len.strict_add(1);
                            }
                        }
                        if len > best_len && len >= MIN_MATCH {
                            best_len = len;
                            best_distance = position.strict_sub(candidate);
                            if len >= nice_len {
                                break;
                            }
                            best_tail = bytes.get(position.strict_add(best_len)).copied();
                        }
                    }
                    let Some(prev_candidate) = previous
                        .get(candidate.rem_euclid(DEFLATE_WINDOW_LEN))
                        .copied()
                        .filter(|&dist| dist != 0)
                        .and_then(|dist| candidate.checked_sub(usize::from(dist)))
                    else {
                        break;
                    };
                    candidate = prev_candidate;
                    chain_len = chain_len.strict_add(1);
                }
            }
            Self::insert_position(position, position_bias, position_hash, head, previous);
            if best_len >= MIN_MATCH {
                let [dist_low, dist_high, ..] = best_distance.strict_sub(1).to_le_bytes();
                let dist_bits = u32::from(u16::from_le_bytes([dist_low, dist_high])) & 0x7FFF;
                let len_bits = u32::from(best_len.strict_sub(3).to_le_bytes()[0]);
                try_push_vec(
                    &mut tokens,
                    0x8000_0000 | (len_bits << 16_u32) | dist_bits,
                    DEFLATE_TOKEN_RESERVE_CHUNK,
                    "deflate token 메모리 확장 실패",
                )?;
                let next_position = position
                    .checked_add(best_len)
                    .ok_or("deflate 위치 계산 실패")?;
                if best_len <= 4 {
                    for insert_pos in position.strict_add(1)..next_position {
                        Self::insert_position(
                            insert_pos,
                            position_bias,
                            hash3(bytes, insert_pos),
                            head,
                            previous,
                        );
                    }
                }
                position = next_position;
            } else {
                let &byte = bytes.get(position).ok_or("deflate literal 범위 오류")?;
                try_push_vec(
                    &mut tokens,
                    u32::from(byte),
                    DEFLATE_TOKEN_RESERVE_CHUNK,
                    "deflate token 메모리 확장 실패",
                )?;
                position = position.strict_add(1);
            }
        }
        Ok(Some((tokens, !crc32_update(u32::MAX, bytes))))
    }
}
fn build_huffman_lengths(
    state: &mut HuffmanBuildState,
    frequencies: &[u32],
    max_bits: u8,
    lengths: &mut [u8],
) {
    let symbol_count = frequencies.len();
    let node_capacity = symbol_count.strict_mul(2).strict_add(1);
    or_abort(state.depths.get_mut(..symbol_count)).fill(0);
    (state.heap_len, state.heap_max, state.max_symbol) = (0, node_capacity, 0);
    or_abort(state.frequencies.get_mut(..symbol_count)).copy_from_slice(frequencies);
    for (symbol, &frequency) in frequencies.iter().enumerate() {
        if frequency == 0 {
            continue;
        }
        let symbol_u16 = or_abort(u16::try_from(symbol).ok());
        state.heap_len = state.heap_len.strict_add(1);
        huffman_set(&mut state.heap, state.heap_len, symbol_u16);
        state.max_symbol = symbol_u16;
    }
    while state.heap_len < 2 {
        let symbol = if state.max_symbol < 2 {
            state.max_symbol = state.max_symbol.strict_add(1);
            state.max_symbol
        } else {
            0
        };
        huffman_set(&mut state.frequencies, usize::from(symbol), 1);
        state.heap_len = state.heap_len.strict_add(1);
        huffman_set(&mut state.heap, state.heap_len, symbol);
    }
    for index in (1..=state.heap_len.div_euclid(2)).rev() {
        state.heap_down(index);
    }
    let mut next_node = or_abort(u16::try_from(symbol_count).ok());
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
        let (li, ri, ni) = (
            usize::from(left),
            usize::from(right),
            usize::from(next_node),
        );
        let parent_freq =
            huffman_get(&state.frequencies, li).strict_add(huffman_get(&state.frequencies, ri));
        huffman_set(&mut state.frequencies, ni, parent_freq);
        let parent_depth = huffman_get(&state.depths, li)
            .max(huffman_get(&state.depths, ri))
            .strict_add(1);
        huffman_set(&mut state.depths, ni, parent_depth);
        huffman_set(&mut state.parents, li, next_node);
        huffman_set(&mut state.parents, ri, next_node);
        huffman_set(&mut state.heap, 1, next_node);
        next_node = next_node.strict_add(1);
        state.heap_down(1);
    }
    state.heap_max = state.heap_max.strict_sub(1);
    let root = huffman_get(&state.heap, 1);
    huffman_set(&mut state.heap, state.heap_max, root);
    let maximum_length = usize::from(max_bits);
    let mut length_counts = [0_usize; DEFLATE_MAX_BITS + 1];
    let mut node_lengths = [0_u8; HUFFMAN_NODE_CAPACITY];
    let mut overflow = 0_usize;
    for &node in or_abort(state.heap.get(state.heap_max.strict_add(1)..node_capacity)) {
        let node_idx = usize::from(node);
        let parent = usize::from(huffman_get(&state.parents, node_idx));
        let mut length = usize::from(huffman_get(&node_lengths, parent)).strict_add(1);
        if length > maximum_length {
            length = maximum_length;
            overflow = overflow.strict_add(1);
        }
        huffman_set(
            &mut node_lengths,
            node_idx,
            or_abort(u8::try_from(length).ok()),
        );
        if node <= state.max_symbol {
            let count = huffman_get(&length_counts, length).strict_add(1);
            huffman_set(&mut length_counts, length, count);
        }
    }
    if overflow == 0 {
        lengths.copy_from_slice(or_abort(node_lengths.get(..symbol_count)));
        return;
    }
    while overflow != 0 {
        let shorter_bits = or_abort(
            (1..maximum_length)
                .rev()
                .find(|&bits| huffman_get(&length_counts, bits) != 0),
        );
        let shorter = huffman_get(&length_counts, shorter_bits).strict_sub(1);
        huffman_set(&mut length_counts, shorter_bits, shorter);
        let longer_bits = shorter_bits.strict_add(1);
        let longer = huffman_get(&length_counts, longer_bits).strict_add(2);
        huffman_set(&mut length_counts, longer_bits, longer);
        let maximum = huffman_get(&length_counts, maximum_length).strict_sub(1);
        huffman_set(&mut length_counts, maximum_length, maximum);
        overflow = overflow.strict_sub(2);
    }
    let mut ordered_index = node_capacity;
    for bits in (1..=maximum_length).rev() {
        let mut remaining = huffman_get(&length_counts, bits);
        while remaining != 0 {
            let symbol = loop {
                ordered_index = ordered_index.strict_sub(1);
                let candidate = huffman_get(&state.heap, ordered_index);
                if candidate <= state.max_symbol {
                    break usize::from(candidate);
                }
            };
            huffman_set(lengths, symbol, or_abort(u8::try_from(bits).ok()));
            remaining = remaining.strict_sub(1);
        }
    }
}
fn canonical_next_codes(
    lengths: &[u8],
    length_counts: &mut [u16; DEFLATE_MAX_BITS + 1],
) -> Result<Option<[u16; DEFLATE_MAX_BITS + 1]>> {
    for &length in lengths {
        if length != 0 {
            let count = length_counts
                .get_mut(usize::from(length))
                .ok_or("deflate Huffman code 길이가 너무 깁니다.")?;
            *count = count.strict_add(1);
        }
    }
    if length_counts.iter().skip(1).all(|&count| count == 0) {
        return Ok(None);
    }
    let mut next_codes = [0_u16; DEFLATE_MAX_BITS + 1];
    let mut code = 0_u16;
    for (next_code, &previous_count) in next_codes.iter_mut().skip(1).zip(length_counts.iter()) {
        code = code
            .checked_add(previous_count)
            .and_then(|sum| sum.checked_mul(2))
            .ok_or("deflate Huffman canonical code가 범위를 초과했습니다.")?;
        *next_code = code;
    }
    Ok(Some(next_codes))
}
fn ensure_deflate_output_len(
    current_len: usize,
    additional_len: usize,
    expected_len: usize,
) -> Result<()> {
    let next_len = current_len
        .checked_add(additional_len)
        .ok_or("deflate 출력 크기 계산 실패")?;
    ensure(
        next_len <= expected_len,
        "deflate 출력이 ZIP 선언 해제 크기를 초과했습니다.",
    )
}
fn fill_write_huffman_codes(lengths: &[u8], codes: &mut [u32]) -> Result<()> {
    let mut length_counts = [0_u16; DEFLATE_MAX_BITS + 1];
    let mut next_code = or_abort(canonical_next_codes(lengths, &mut length_counts)?);
    for (slot, &len) in codes.iter_mut().zip(lengths) {
        let code = if len == 0 {
            0
        } else {
            let next_slot = or_abort(next_code.get_mut(usize::from(len)));
            let code = reverse_low_bits(*next_slot, len);
            *next_slot = next_slot.strict_add(1);
            code
        };
        let [low, high] = code.to_le_bytes();
        *slot = u32::from_le_bytes([low, high, len, 0]);
    }
    Ok(())
}
fn hash3(bytes: &[u8], position: usize) -> Option<usize> {
    let &[first, second, third] = bytes.get(position..)?.first_chunk::<3>()?;
    Some(
        ((usize::from(first) << 10_usize) ^ (usize::from(second) << 5_usize) ^ usize::from(third))
            & HASH_SIZE.strict_sub(1),
    )
}
fn huffman_get<T>(values: &[T], index: usize) -> T
where
    T: Copy,
{
    or_abort(values.get(index).copied())
}
fn huffman_set<T>(values: &mut [T], index: usize, value: T) {
    *or_abort(values.get_mut(index)) = value;
}
fn reverse_low_bits(value: u16, count: u8) -> u16 {
    value
        .reverse_bits()
        .unbounded_shr(u16::BITS.strict_sub(u32::from(count)))
}
fn write_deflate_tokens(
    tokens: &[u32],
    writer: &mut BitWriter<'_>,
    dynamic: Option<(&[u32], &[u32])>,
) -> Result<()> {
    if let Some((literal_huffman, distance_huffman)) = dynamic {
        for &token in tokens {
            if token & 0x8000_0000 == 0 {
                write_huffman_symbol(literal_huffman, writer, (token & 0xFF) as u16)?;
            } else {
                let (ls, le, lb) =
                    DeflateWriter::length_symbol((((token >> 16_u32) & 0xFF) as u16).strict_add(3));
                let (ds, de, db) =
                    DeflateWriter::distance_symbol(((token & 0x7FFF) as u16).strict_add(1));
                write_huffman_with_extra(literal_huffman, writer, ls, le, lb)?;
                write_huffman_with_extra(distance_huffman, writer, ds, de, db)?;
            }
        }
        write_huffman_symbol(literal_huffman, writer, 256)
    } else {
        for &token in tokens {
            if token & 0x8000_0000 == 0 {
                write_fixed_symbol(writer, (token & 0xFF) as u16)?;
            } else {
                let (ls, le, lb) =
                    DeflateWriter::length_symbol((((token >> 16_u32) & 0xFF) as u16).strict_add(3));
                let (ds, de, db) =
                    DeflateWriter::distance_symbol(((token & 0x7FFF) as u16).strict_add(1));
                write_fixed_symbol(writer, ls)?;
                writer.write_bits(le, lb)?;
                let dist_combined = u32::from(reverse_low_bits(ds, 5)) | (u32::from(de) << 5_u8);
                writer.write_bits_u32(dist_combined, 5_u8.strict_add(db))?;
            }
        }
        write_fixed_symbol(writer, 256)
    }
}
fn write_fixed_symbol(writer: &mut BitWriter<'_>, symbol: u16) -> Result<()> {
    let (code, bit_count) = match symbol {
        0..=143 => (0x30_u16.strict_add(symbol), 8),
        144..=255 => (0x190_u16.strict_add(symbol.strict_sub(144)), 9),
        256..=279 => (symbol.strict_sub(256), 7),
        280..=287 => (0xc0_u16.strict_add(symbol.strict_sub(280)), 8),
        _ => bail!("deflate fixed symbol 범위 오류"),
    };
    writer.write_bits(reverse_low_bits(code, bit_count), bit_count)
}
fn write_huffman_symbol(codes: &[u32], writer: &mut BitWriter<'_>, symbol: u16) -> Result<()> {
    let [low, high, len, _] = huffman_get(codes, usize::from(symbol)).to_le_bytes();
    debug_assert!(len != 0, "deflate huffman symbol length must be non-zero");
    writer.write_bits(u16::from_le_bytes([low, high]), len)
}
fn write_huffman_with_extra(
    codes: &[u32],
    writer: &mut BitWriter<'_>,
    symbol: u16,
    extra: u16,
    extra_bits: u8,
) -> Result<()> {
    let [low, high, len, _] = huffman_get(codes, usize::from(symbol)).to_le_bytes();
    debug_assert!(len != 0, "deflate huffman symbol length must be non-zero");
    let combined = u32::from(u16::from_le_bytes([low, high])) | (u32::from(extra) << len);
    writer.write_bits_u32(combined, len.strict_add(extra_bits))
}
