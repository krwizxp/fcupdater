use super::{
    CODE_LENGTH_ORDER as CL_ORDER, CODE_LENGTH_SYMBOLS as CL_SYMS, DEFLATE_MAX_BITS as MAX_BITS,
    DEFLATE_MAX_BITS_U8 as MAX_BITS_U8, DISTANCE_BASES as DIST_BASES,
    DISTANCE_EXTRA_BITS as DIST_EXTRA, DISTANCE_SYMBOLS as DIST_SYMS,
    FIXED_DISTANCE_SYMBOLS as FIX_DIST, FIXED_LITERAL_SYMBOLS as FIX_LIT, HASH_SIZE,
    LENGTH_BASES as LEN_BASES, LENGTH_EXTRA_BITS as LEN_EXTRA, LITERAL_LENGTH_SYMBOLS as LIT_SYMS,
    MAX_CHAIN, MAX_MATCH, MIN_MATCH, crc32_update, read_u16,
};
use crate::diagnostic::{Result, bail, ensure, err, err_src, or_abort, push_vec, reserve, vec_cap};
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
const MAX_SYMS: usize = FIX_LIT;
const ROOT_BITS: u8 = 9;
const ROOT_SIZE: usize = 1 << ROOT_BITS;
const NODE_CAP: usize = LIT_SYMS.strict_mul(2).strict_add(1);
const TOK_CHUNK: usize = 64 * 1024;
const WIN_LEN: usize = 0x8000;
const MAX_DIST: usize = WIN_LEN - MAX_MATCH - MIN_MATCH - 1;
const BLOCK_LIMIT: usize = 8191;
const BUF_LEN: usize = 8192;
const CHUNK_BYTES: usize = 16;
const LEN_SYM_TABLE: [u16; 256] = {
    let mut table = [28_u16; 256];
    let (mut slots, mut val): (&mut [u16], u16) = (&mut table, 0);
    while let Some((slot, tail)) = slots.split_first_mut() {
        if val < 255 {
            let [eb, _, _, _] = val.bit_width().saturating_sub(3).to_le_bytes();
            let eu = eb as u16;
            let ci = if eb == 0 {
                val
            } else {
                eu.strict_add(1).strict_mul(4) | ((val >> eb) & 3)
            };
            let ex = val & 1_u16.strict_shl(eb as u32).strict_sub(1);
            *slot = ci | (eu << 5_u32) | (ex << 8_u32);
        }
        val = val.strict_add(1);
        slots = tail;
    }
    table
};
static FIXED_DECODE_TREES: LazyLock<(DecodeHuffman, Option<DecodeHuffman>)> = LazyLock::new(|| {
    let lit_lens: [u8; FIX_LIT] = from_fn(|sym| match sym {
        0..=143 | 280..=287 => 8,
        144..=255 => 9,
        256..=279 => 7,
        _ => 0,
    });
    let lit = or_abort(
        DecodeHuffman::from_lengths(&lit_lens, false).unwrap_or_else(|_| process::abort()),
    );
    let dist = or_abort(
        DecodeHuffman::from_lengths(&[5_u8; FIX_DIST], false).unwrap_or_else(|_| process::abort()),
    );
    (lit, Some(dist))
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
    buffer: [u8; BUF_LEN],
    buffered_len: usize,
    len: usize,
    writer: &'writer mut dyn IoWrite,
}
struct DecodeHuffman {
    counts: [u16; MAX_BITS + 1],
    first_codes: [u16; MAX_BITS + 1],
    first_symbols: [u16; MAX_BITS + 1],
    root: [u16; ROOT_SIZE],
    symbols: [u16; MAX_SYMS],
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
pub(super) struct DeflatePlan {
    blocks: Vec<(Option<DynamicDeflatePlan>, Range<usize>)>,
    compressed_len: usize,
    crc32: u32,
    tokens: Vec<u32>,
}
struct DynamicDeflatePlan {
    code_huffman: [u32; CL_SYMS],
    code_length_count: usize,
    code_length_tokens: Vec<(u8, u16, u8)>,
    distance_count: usize,
    distance_huffman: [u32; DIST_SYMS],
    literal_count: usize,
    literal_huffman: [u32; LIT_SYMS],
}
struct DynamicFrequencies {
    distance: [u32; DIST_SYMS],
    literal: [u32; LIT_SYMS],
}
struct HuffmanBuildState {
    depths: [u16; NODE_CAP],
    frequencies: [u32; NODE_CAP],
    heap: [u16; NODE_CAP],
    heap_len: usize,
    heap_max: usize,
    max_symbol: u16,
    parents: [u16; NODE_CAP],
}
pub(super) struct Inflater<'bytes>(pub &'bytes [u8], pub usize);
struct InflateState<'bytes> {
    expected_len: usize,
    output: Vec<u8>,
    reader: BitReader<'bytes>,
}
impl BitReader<'_> {
    fn read_bits(&mut self, count: u8) -> Result<u16> {
        let val = self
            .try_peek_bits(count)
            .ok_or("deflate bitstream 조기 종료")?;
        ensure(count <= self.bit_count, "deflate bit buffer 소비 범위 오류")?;
        self.bit_buffer >>= u32::from(count);
        self.bit_count = self.bit_count.strict_sub(count);
        Ok(val)
    }
    fn read_stored_bytes(&mut self, len: usize) -> Result<&[u8]> {
        self.cursor = self
            .cursor
            .strict_sub(usize::from(self.bit_count).div_euclid(8));
        (self.bit_buffer, self.bit_count) = (0, 0);
        let end = self
            .cursor
            .checked_add(len)
            .ok_or("deflate 저장 블록 크기 계산 실패")?;
        let bytes = self
            .bytes
            .get(self.cursor..end)
            .ok_or("deflate 저장 블록 범위 초과")?;
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
            .map_err(|src| err_src("deflate stream 쓰기 실패", src))?;
        self.len = self.len.strict_add(self.buffered_len);
        self.buffered_len = 0;
        Ok(())
    }
    #[inline]
    fn write_bits(&mut self, value: u32, count: u8) -> Result<()> {
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
        *at_mut(&mut self.buffer, self.buffered_len) = byte;
        self.buffered_len = self.buffered_len.strict_add(1);
        Ok(())
    }
}
impl DecodeHuffman {
    fn decode(&self, reader: &mut BitReader<'_>) -> Result<u16> {
        let (mut code, skip_bits) = if let Some(prefix) = reader.try_peek_bits(ROOT_BITS) {
            let entry = *self
                .root
                .get(usize::from(prefix))
                .ok_or("deflate root table 범위 오류")?;
            if entry != 0 {
                let len = entry.to_le_bytes()[1] >> 1_u8;
                reader.bit_buffer >>= len;
                reader.bit_count = reader.bit_count.strict_sub(len);
                return Ok(entry & 0x01ff);
            }
            reader.bit_buffer >>= ROOT_BITS;
            reader.bit_count = reader.bit_count.strict_sub(ROOT_BITS);
            (
                prefix.reverse_bits() >> u16::BITS.strict_sub(u32::from(ROOT_BITS)),
                usize::from(ROOT_BITS).strict_add(1),
            )
        } else {
            (0_u16, 1_usize)
        };
        for ((&count, &first_code), &first_sym) in self
            .counts
            .iter()
            .zip(&self.first_codes)
            .zip(&self.first_symbols)
            .skip(skip_bits)
        {
            code = code.strict_shl(1) | reader.read_bits(1)?;
            if let Some(off) = code.checked_sub(first_code)
                && off < count
            {
                return self
                    .symbols
                    .get(usize::from(first_sym.strict_add(off)))
                    .copied()
                    .ok_or_else(|| err("deflate canonical symbol 범위 오류"));
            }
        }
        Err(err("deflate Huffman code 해석 실패"))
    }
    fn from_lengths(lengths: &[u8], allow_single: bool) -> Result<Option<Self>> {
        ensure(lengths.len() <= MAX_SYMS, "deflate decode symbol 수 초과")?;
        let mut counts = [0_u16; MAX_BITS + 1];
        let Some(mut next_codes) = canonical_next_codes(lengths, &mut counts)? else {
            return Ok(None);
        };
        let first_codes = next_codes;
        let mut sym_count = 0_u16;
        let first_symbols = counts.map(|cnt| {
            let first = sym_count;
            sym_count = sym_count.strict_add(cnt);
            first
        });
        let code_end =
            u32::from(at(&first_codes, MAX_BITS)).strict_add(u32::from(at(&counts, MAX_BITS)));
        let limit = 1_u32.strict_shl(u32::from(MAX_BITS_U8));
        ensure(code_end <= limit, "Oversubscribed deflate Huffman tree")?;
        ensure(
            code_end == limit || (allow_single && sym_count == 1 && at(&counts, 1) == 1),
            "Incomplete deflate Huffman tree",
        )?;
        let (mut root, mut symbols) = ([0_u16; ROOT_SIZE], [0_u16; MAX_SYMS]);
        for (sym_u16, &bit_len) in zip(0_u16.., lengths) {
            if bit_len == 0 {
                continue;
            }
            let bi = usize::from(bit_len);
            let slot = at_mut(&mut next_codes, bi);
            let assigned = *slot;
            *slot = assigned.strict_add(1);
            let pos = at(&first_symbols, bi).strict_add(assigned.strict_sub(at(&first_codes, bi)));
            *at_mut(&mut symbols, usize::from(pos)) = sym_u16;
            if bit_len > ROOT_BITS {
                continue;
            }
            let base = usize::from(reverse_low_bits(assigned, bit_len));
            let entry = sym_u16 | u16::from(bit_len).strict_shl(u32::from(ROOT_BITS));
            for suffix in 0..1_usize.strict_shl(u32::from(ROOT_BITS.strict_sub(bit_len))) {
                let rslot = root
                    .get_mut(base | suffix.strict_shl(u32::from(bit_len)))
                    .ok_or("deflate root 범위 오류")?;
                ensure(*rslot == 0, "deflate root decode code 충돌")?;
                *rslot = entry;
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
impl Inflater<'_> {
    pub(super) fn inflate(self) -> Result<(Vec<u8>, u32)> {
        let mut state = InflateState {
            expected_len: self.1,
            output: vec_cap(self.1, "deflate 출력")?,
            reader: BitReader {
                bit_buffer: 0,
                bit_count: 0,
                bytes: self.0,
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
                    let (lit, dist) = state.dynamic_trees()?;
                    state.inflate_compressed_block(&lit, dist.as_ref())?;
                }
                _ => bail!("미지원 deflate block type"),
            }
            if final_block {
                let crc = !crc32_update(u32::MAX, &state.output);
                return Ok((state.output, crc));
            }
        }
    }
}
impl InflateState<'_> {
    fn copy_previous(&mut self, distance: usize, length: usize) -> Result<()> {
        ensure(
            distance != 0 && distance <= self.output.len(),
            "deflate distance 오류",
        )?;
        ensure_out_len(self.output.len(), length, self.expected_len)?;
        let start = self.output.len().strict_sub(distance);
        let mut copied = length.min(distance);
        self.output
            .extend_from_within(start..start.strict_add(copied));
        while copied < length {
            let take = copied.min(length.strict_sub(copied));
            self.output
                .extend_from_within(start..start.strict_add(take));
            copied = copied.strict_add(take);
        }
        Ok(())
    }
    fn decode_extra(
        &mut self,
        idx: usize,
        bases: &[u16],
        extra_bits: &[u8],
        err_msg: &'static str,
    ) -> Result<usize> {
        let (&base, &bits) = bases.get(idx).zip(extra_bits.get(idx)).ok_or(err_msg)?;
        let extra = if bits == 0 {
            0
        } else {
            usize::from(self.reader.read_bits(bits)?)
        };
        Ok(usize::from(base).strict_add(extra))
    }
    fn dynamic_trees(&mut self) -> Result<(DecodeHuffman, Option<DecodeHuffman>)> {
        let lit_cnt = usize::from(self.reader.read_bits(5)?).strict_add(257);
        ensure(lit_cnt <= LIT_SYMS, "deflate HLIT 범위 오류")?;
        let dist_cnt = usize::from(self.reader.read_bits(5)?).strict_add(1);
        let cl_cnt = usize::from(self.reader.read_bits(4)?).strict_add(4);
        let mut cl_lens = [0_u8; 19];
        for &sym in CL_ORDER.iter().take(cl_cnt) {
            *cl_lens
                .get_mut(usize::from(sym))
                .ok_or("deflate code length 범위 오류")? =
                self.reader.read_bits(3)?.to_le_bytes()[0];
        }
        let cl_tree = DecodeHuffman::from_lengths(&cl_lens, false)?
            .ok_or("deflate code length tree 비어 있음")?;
        let total = lit_cnt
            .checked_add(dist_cnt)
            .ok_or("deflate code length 총합 오류")?;
        let (mut lengths, mut len_cnt) = ([0_u8; LIT_SYMS + FIX_DIST], 0_usize);
        while len_cnt < total {
            let sym = cl_tree.decode(&mut self.reader)?;
            let (val, rep) = match sym {
                0..=15 => (sym.to_le_bytes()[0], 1),
                16 => (
                    len_cnt
                        .checked_sub(1)
                        .and_then(|idx| lengths.get(idx))
                        .copied()
                        .ok_or("deflate repeat 이전 길이 없음")?,
                    usize::from(self.reader.read_bits(2)?).strict_add(3),
                ),
                17 => (0, usize::from(self.reader.read_bits(3)?).strict_add(3)),
                18 => (0, usize::from(self.reader.read_bits(7)?).strict_add(11)),
                _ => bail!("deflate code length symbol 오류"),
            };
            let next = len_cnt.checked_add(rep).ok_or("deflate repeat 길이 오류")?;
            ensure(next <= total, "deflate repeat 총합 초과")?;
            or_abort(lengths.get_mut(len_cnt..next)).fill(val);
            len_cnt = next;
        }
        let (lit_lens, dist_lens) = lengths
            .get(..total)
            .and_then(|all| all.split_at_checked(lit_cnt))
            .ok_or("deflate literal/distance 범위 오류")?;
        let lit =
            DecodeHuffman::from_lengths(lit_lens, true)?.ok_or("deflate literal tree 비어 있음")?;
        Ok((lit, DecodeHuffman::from_lengths(dist_lens, true)?))
    }
    fn inflate_compressed_block(
        &mut self,
        lit_tree: &DecodeHuffman,
        dist_tree: Option<&DecodeHuffman>,
    ) -> Result<()> {
        loop {
            match lit_tree.decode(&mut self.reader)? {
                sym @ 0..=255 => {
                    ensure(
                        self.output.len() != self.expected_len,
                        "deflate 출력 크기 초과",
                    )?;
                    self.output.push(sym.to_le_bytes()[0]);
                }
                256 => return Ok(()),
                sym @ 257..=285 => {
                    let length = self.decode_extra(
                        usize::from(sym.strict_sub(257)),
                        &LEN_BASES,
                        &LEN_EXTRA,
                        "deflate length 오류",
                    )?;
                    let dist_sym = dist_tree
                        .ok_or("deflate distance tree 없음")?
                        .decode(&mut self.reader)?;
                    let distance = self.decode_extra(
                        usize::from(dist_sym),
                        &DIST_BASES,
                        &DIST_EXTRA,
                        "deflate distance 오류",
                    )?;
                    self.copy_previous(distance, length)?;
                }
                _ => bail!("deflate literal/length symbol 오류"),
            }
        }
    }
    fn inflate_stored_block(&mut self) -> Result<()> {
        let hdr = self.reader.read_stored_bytes(4)?;
        let (len, nlen) = (read_u16(hdr, 0)?, read_u16(hdr, 2)?);
        ensure(len == !nlen, "deflate 저장 블록 LEN/NLEN 불일치")?;
        let stored = self.reader.read_stored_bytes(usize::from(len))?;
        ensure_out_len(self.output.len(), stored.len(), self.expected_len)?;
        self.output.extend_from_slice(stored);
        Ok(())
    }
}
impl DynamicFrequencies {
    fn collect(&mut self, tokens: &[u32]) -> (bool, usize) {
        *at_mut(&mut self.literal, 256) = 1;
        let (mut fixed_bits, mut has_dist) = (3_usize, false);
        for &tok in tokens {
            if tok & 0x8000_0000 == 0 {
                let byte = (tok & 0xFF) as u16;
                let freq = at_mut(&mut self.literal, usize::from(byte));
                *freq = freq.strict_add(1);
                fixed_bits = fixed_bits.strict_add(if byte <= 143 { 8 } else { 9 });
            } else {
                let (ls, _, lb, ds, _, db) = decode_match_token(tok);
                let lf = at_mut(&mut self.literal, usize::from(ls));
                *lf = lf.strict_add(1);
                let df = at_mut(&mut self.distance, usize::from(ds));
                *df = df.strict_add(1);
                fixed_bits = fixed_bits
                    .strict_add(if ls <= 279 { 7 } else { 8 })
                    .strict_add(usize::from(lb))
                    .strict_add(5)
                    .strict_add(usize::from(db));
                has_dist = true;
            }
        }
        (has_dist, fixed_bits.strict_add(7))
    }
    fn encoded_bit_len(&self, plan: &DynamicDeflatePlan) -> usize {
        let mut bits = 17_usize.strict_add(plan.code_length_count.strict_mul(3));
        for &(sym, _, extra) in &plan.code_length_tokens {
            bits = bits
                .strict_add(usize::from(
                    at(&plan.code_huffman, usize::from(sym)).to_le_bytes()[2],
                ))
                .strict_add(usize::from(extra));
        }
        for (sym, (&freq, &code)) in self.literal.iter().zip(&plan.literal_huffman).enumerate() {
            let extra = sym
                .checked_sub(257)
                .and_then(|idx| LEN_EXTRA.get(idx))
                .copied()
                .unwrap_or(0);
            bits = bits.strict_add(
                (freq as usize)
                    .strict_mul(usize::from(code.to_le_bytes()[2]).strict_add(usize::from(extra))),
            );
        }
        for ((&freq, &code), &extra) in self
            .distance
            .iter()
            .zip(&plan.distance_huffman)
            .zip(&DIST_EXTRA)
        {
            bits = bits.strict_add(
                (freq as usize)
                    .strict_mul(usize::from(code.to_le_bytes()[2]).strict_add(usize::from(extra))),
            );
        }
        bits
    }
    fn plan(&self, state: &mut HuffmanBuildState) -> Result<DynamicDeflatePlan> {
        let (mut lit_lens, mut dist_lens) = ([0_u8; LIT_SYMS], [0_u8; DIST_SYMS]);
        build_huffman_lengths(state, &self.literal, MAX_BITS_U8, &mut lit_lens);
        build_huffman_lengths(state, &self.distance, MAX_BITS_U8, &mut dist_lens);
        let literal_count = lit_lens
            .iter()
            .rposition(|&len| len != 0)
            .map_or(257, |idx| idx.strict_add(1).max(257));
        let distance_count = dist_lens
            .iter()
            .rposition(|&len| len != 0)
            .map_or(1, |idx| idx.strict_add(1));
        let mut cl_tokens = vec_cap(literal_count.strict_add(distance_count), "deflate cl token")?;
        let mut lengths = chain(
            or_abort(lit_lens.get(..literal_count)),
            or_abort(dist_lens.get(..distance_count)),
        )
        .copied()
        .chain(once(u8::MAX));
        let (mut prev, mut next, mut count) = (u8::MAX, or_abort(lengths.next()), 0_usize);
        let (mut max_cnt, mut min_cnt) = if next == 0 {
            (138_usize, 3_usize)
        } else {
            (7_usize, 4_usize)
        };
        for next_val in lengths {
            let cur = next;
            next = next_val;
            count = count.strict_add(1);
            if count < max_cnt && cur == next {
                continue;
            }
            if count < min_cnt {
                cl_tokens.extend(repeat_n((cur, 0_u16, 0_u8), count));
            } else {
                if cur != 0 && cur != prev {
                    cl_tokens.push((cur, 0, 0));
                    count = count.strict_sub(1);
                }
                let (sym, bits, base) = if cur != 0 {
                    (16_u8, 2_u8, 3_usize)
                } else if count <= 10 {
                    (17, 3, 3)
                } else {
                    (18, 7, 11)
                };
                cl_tokens.push((
                    sym,
                    or_abort(u16::try_from(count.strict_sub(base)).ok()),
                    bits,
                ));
            }
            (count, prev) = (0, cur);
            (max_cnt, min_cnt) = if next == 0 {
                (138, 3)
            } else if cur == next {
                (6, 3)
            } else {
                (7, 4)
            };
        }
        let mut cl_freq = [0_u32; CL_SYMS];
        for &(sym, _, _) in &cl_tokens {
            let freq = at_mut(&mut cl_freq, usize::from(sym));
            *freq = freq.strict_add(1);
        }
        let mut cl_lens = [0_u8; CL_SYMS];
        build_huffman_lengths(state, &cl_freq, 7, &mut cl_lens);
        let code_length_count = CL_ORDER
            .iter()
            .rposition(|&sym| at(&cl_lens, usize::from(sym)) != 0)
            .map_or(4, |idx| idx.strict_add(1).max(4));
        let (mut literal_huffman, mut distance_huffman, mut code_huffman) =
            ([0_u32; LIT_SYMS], [0_u32; DIST_SYMS], [0_u32; CL_SYMS]);
        fill_write_huffman_codes(&lit_lens, &mut literal_huffman)?;
        fill_write_huffman_codes(&dist_lens, &mut distance_huffman)?;
        fill_write_huffman_codes(&cl_lens, &mut code_huffman)?;
        Ok(DynamicDeflatePlan {
            code_huffman,
            code_length_count,
            code_length_tokens: cl_tokens,
            distance_count,
            distance_huffman,
            literal_count,
            literal_huffman,
        })
    }
}
impl DynamicDeflatePlan {
    fn write_block(&self, tokens: &[u32], writer: &mut BitWriter<'_>) -> Result<()> {
        for (val, bits) in [
            (0_u16, 1_u8),
            (2, 2),
            (
                or_abort(u16::try_from(self.literal_count.strict_sub(257)).ok()),
                5,
            ),
            (
                or_abort(u16::try_from(self.distance_count.strict_sub(1)).ok()),
                5,
            ),
            (
                or_abort(u16::try_from(self.code_length_count.strict_sub(4)).ok()),
                4,
            ),
        ] {
            writer.write_bits(u32::from(val), bits)?;
        }
        for &sym in CL_ORDER.iter().take(self.code_length_count) {
            writer.write_bits(
                u32::from(at(&self.code_huffman, usize::from(sym)).to_le_bytes()[2]),
                3,
            )?;
        }
        for &(sym, extra, bits) in &self.code_length_tokens {
            write_huffman_with_extra(&self.code_huffman, writer, u16::from(sym), 0, 0)?;
            writer.write_bits(u32::from(extra), bits)?;
        }
        write_deflate_tokens(
            tokens,
            writer,
            Some((&self.literal_huffman, &self.distance_huffman)),
        )
    }
}
impl HuffmanBuildState {
    fn heap_down(&mut self, mut idx: usize) {
        let node = at(&self.heap, idx);
        let mut child = idx.strict_mul(2);
        while child <= self.heap_len {
            if child < self.heap_len
                && self.node_is_smaller(at(&self.heap, child.strict_add(1)), at(&self.heap, child))
            {
                child = child.strict_add(1);
            }
            let child_node = at(&self.heap, child);
            if self.node_is_smaller(node, child_node) {
                break;
            }
            *at_mut(&mut self.heap, idx) = child_node;
            idx = child;
            child = idx.strict_mul(2);
        }
        *at_mut(&mut self.heap, idx) = node;
    }
    fn node_is_smaller(&self, left: u16, right: u16) -> bool {
        let (li, ri) = (usize::from(left), usize::from(right));
        let (lf, rf) = (at(&self.frequencies, li), at(&self.frequencies, ri));
        lf < rf || (lf == rf && at(&self.depths, li) <= at(&self.depths, ri))
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
        let mut bw = BitWriter {
            bit_buffer: 0,
            bit_count: 0,
            buffer: [0; BUF_LEN],
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
                plan.write_block(tokens, &mut bw)?;
            } else {
                bw.write_bits(0, 1)?;
                bw.write_bits(1, 2)?;
                write_deflate_tokens(tokens, &mut bw, None)?;
            }
        }
        bw.write_bits(1, 1)?;
        bw.write_bits(1, 2)?;
        write_fixed_symbol(&mut bw, 256)?;
        while bw.bit_count >= 8 {
            bw.write_byte(bw.bit_buffer.to_le_bytes()[0])?;
            bw.bit_buffer >>= 8_u8;
            bw.bit_count = bw.bit_count.strict_sub(8);
        }
        if bw.bit_count > 0 {
            bw.write_byte(bw.bit_buffer.to_le_bytes()[0])?;
        }
        bw.flush_buffer()?;
        Ok(bw.len)
    }
}
impl DeflateWorkspace {
    fn insert_position(
        pos: usize,
        bias: u32,
        hash_index: Option<usize>,
        head: &mut [u32],
        previous: &mut [u16],
    ) {
        let Some(hash) = hash_index else {
            return;
        };
        let slot = at_mut(head, hash);
        let prev_dist = (*slot)
            .checked_sub(bias)
            .and_then(|hp| pos.checked_sub(hp as usize))
            .and_then(|dist| u16::try_from(dist).ok())
            .filter(|&dist| usize::from(dist) <= WIN_LEN)
            .unwrap_or(0);
        *at_mut(previous, pos.rem_euclid(WIN_LEN)) = prev_dist;
        *slot = u32::try_from(pos)
            .ok()
            .and_then(|val| val.checked_add(bias))
            .unwrap_or(0);
    }
    pub(super) fn plan(&mut self, bytes: &[u8]) -> Result<Option<DeflatePlan>> {
        let Some((tokens, crc32)) = self.tokens(bytes)? else {
            return Ok(None);
        };
        let mut blocks = vec_cap(tokens.len().div_ceil(BLOCK_LIMIT), "deflate block plan")?;
        let mut total_bits = 10_usize;
        for (idx, chunk) in tokens.chunks(BLOCK_LIMIT).enumerate() {
            let start = idx.strict_mul(BLOCK_LIMIT);
            let range = Range {
                start,
                end: start.strict_add(chunk.len()),
            };
            let mut freqs = DynamicFrequencies {
                distance: [0_u32; DIST_SYMS],
                literal: [0_u32; LIT_SYMS],
            };
            let (has_dist, fixed_bits) = freqs.collect(chunk);
            let dyn_plan = if has_dist {
                let state = self.huffman_state.get_or_insert_with(|| {
                    Box::new(HuffmanBuildState {
                        depths: [0_u16; NODE_CAP],
                        frequencies: [0_u32; NODE_CAP],
                        heap: [0_u16; NODE_CAP],
                        heap_len: 0,
                        heap_max: 0,
                        max_symbol: 0,
                        parents: [u16::MAX; NODE_CAP],
                    })
                });
                Some(freqs.plan(state)?)
            } else {
                None
            };
            let (chosen, bits) = dyn_plan.map_or((None, fixed_bits), |dynamic| {
                let dyn_bits = freqs.encoded_bit_len(&dynamic);
                if dyn_bits < fixed_bits {
                    (Some(dynamic), dyn_bits)
                } else {
                    (None, fixed_bits)
                }
            });
            total_bits = total_bits.strict_add(bits);
            blocks.push((chosen, range));
        }
        Ok(Some(DeflatePlan {
            blocks,
            compressed_len: total_bits.div_ceil(8),
            crc32,
            tokens,
        }))
    }
    fn prepare_for_input(&mut self, input_len: usize) -> Result<()> {
        self.work_budget = 512 * 1024 * 1024;
        self.tokens.clear();
        reserve(
            self.tokens.try_reserve_exact(input_len.min(TOK_CHUNK)),
            "deflate token",
        )?;
        if self.head.len() != HASH_SIZE
            || self.position_bias == 0
            || self
                .position_bias
                .checked_add(u32::try_from(input_len).unwrap_or(u32::MAX))
                .and_then(|end| end.checked_add(1))
                .is_none()
        {
            if self.head.len() == HASH_SIZE {
                self.head.fill(0);
            } else {
                self.head.clear();
                reserve(self.head.try_reserve_exact(HASH_SIZE), "deflate hash head")?;
                self.head.resize(HASH_SIZE, 0);
            }
            self.position_bias = 1;
        }
        let prev_len = input_len.min(WIN_LEN);
        if self.previous.len() < prev_len {
            reserve(
                self.previous
                    .try_reserve_exact(prev_len.strict_sub(self.previous.len())),
                "deflate prev",
            )?;
            self.previous.resize(prev_len, 0);
        }
        Ok(())
    }
    pub(super) fn recycle(&mut self, plan: DeflatePlan) {
        self.tokens = plan.tokens;
    }
    fn tokens(&mut self, bytes: &[u8]) -> Result<Option<(Vec<u32>, u32)>> {
        let ws = self;
        ws.prepare_for_input(bytes.len())?;
        let mut tokens = mem::take(&mut ws.tokens);
        let bias = ws.position_bias;
        ws.position_bias = u32::try_from(bytes.len())
            .ok()
            .and_then(|len| bias.checked_add(len))
            .and_then(|end| end.checked_add(1))
            .unwrap_or(0);
        let (head, previous, budget) = (&mut ws.head, &mut ws.previous, &mut ws.work_budget);
        let mut pos = 0_usize;
        while pos < bytes.len() {
            let (mut best_len, mut best_dist) = (0_usize, 0_usize);
            let pos_hash = hash3(bytes, pos);
            if let Some(hash) = pos_hash
                && let Some(&head_cand) = head.get(hash)
            {
                let mut cand = head_cand
                    .checked_sub(bias)
                    .map_or(usize::MAX, |val| val as usize);
                let min_cand = pos.saturating_sub(MAX_DIST);
                let max_len = bytes.len().strict_sub(pos).min(MAX_MATCH);
                let nice_len = 128_usize.min(max_len);
                let (mut best_tail, mut chain_len) = (None, 0_usize);
                while cand != usize::MAX && cand >= min_cand && cand < pos && chain_len < MAX_CHAIN
                {
                    let Some(rem) = budget.checked_sub(1) else {
                        ws.tokens = tokens;
                        return Ok(None);
                    };
                    *budget = rem;
                    if best_len < MIN_MATCH
                        || bytes.get(cand.strict_add(best_len)).copied() == best_tail
                    {
                        let (mut len, mut mismatch) = (0_usize, false);
                        while max_len.strict_sub(len) >= CHUNK_BYTES && *budget >= CHUNK_BYTES {
                            let left = bytes.as_ptr().wrapping_add(cand.strict_add(len));
                            let right = bytes.as_ptr().wrapping_add(pos.strict_add(len));
                            let prefix = matching_prefix_16!(left, right);
                            *budget = budget.strict_sub(prefix.saturating_add(1).min(CHUNK_BYTES));
                            len = len.strict_add(prefix);
                            if prefix < CHUNK_BYTES {
                                mismatch = true;
                                break;
                            }
                        }
                        if !mismatch && len < max_len {
                            let cand_tail =
                                or_abort(bytes.get(cand.strict_add(len)..cand.strict_add(max_len)));
                            let cur_tail =
                                or_abort(bytes.get(pos.strict_add(len)..pos.strict_add(max_len)));
                            for (&left, &right) in zip(cand_tail, cur_tail) {
                                let Some(left_b) = budget.checked_sub(1) else {
                                    ws.tokens = tokens;
                                    return Ok(None);
                                };
                                *budget = left_b;
                                if left != right {
                                    break;
                                }
                                len = len.strict_add(1);
                            }
                        }
                        if len > best_len && len >= MIN_MATCH {
                            (best_len, best_dist) = (len, pos.strict_sub(cand));
                            if len >= nice_len {
                                break;
                            }
                            best_tail = bytes.get(pos.strict_add(best_len)).copied();
                        }
                    }
                    let Some(prev_cand) = previous
                        .get(cand.rem_euclid(WIN_LEN))
                        .copied()
                        .filter(|&dist| dist != 0)
                        .and_then(|dist| cand.checked_sub(usize::from(dist)))
                    else {
                        break;
                    };
                    cand = prev_cand;
                    chain_len = chain_len.strict_add(1);
                }
            }
            Self::insert_position(pos, bias, pos_hash, head, previous);
            if best_len >= MIN_MATCH {
                let [dl, dh, ..] = best_dist.strict_sub(1).to_le_bytes();
                let tok = 0x8000_0000
                    | (u32::from(best_len.strict_sub(3).to_le_bytes()[0]) << 16_u32)
                    | (u32::from(u16::from_le_bytes([dl, dh])) & 0x7FFF);
                push_vec(&mut tokens, tok, TOK_CHUNK, "deflate token")?;
                let next_pos = pos.checked_add(best_len).ok_or("deflate 위치 계산 실패")?;
                if best_len <= 4 {
                    for ins in pos.strict_add(1)..next_pos {
                        Self::insert_position(ins, bias, hash3(bytes, ins), head, previous);
                    }
                }
                pos = next_pos;
            } else {
                let &byte = bytes.get(pos).ok_or("deflate literal 범위 오류")?;
                push_vec(&mut tokens, u32::from(byte), TOK_CHUNK, "deflate token")?;
                pos = pos.strict_add(1);
            }
        }
        Ok(Some((tokens, !crc32_update(u32::MAX, bytes))))
    }
}
fn at<T>(values: &[T], index: usize) -> T
where
    T: Copy,
{
    or_abort(values.get(index).copied())
}
fn at_mut<T>(values: &mut [T], index: usize) -> &mut T {
    or_abort(values.get_mut(index))
}
fn build_huffman_lengths(
    state: &mut HuffmanBuildState,
    frequencies: &[u32],
    max_bits: u8,
    lengths: &mut [u8],
) {
    let sym_cnt = frequencies.len();
    let node_cap = sym_cnt.strict_mul(2).strict_add(1);
    or_abort(state.depths.get_mut(..sym_cnt)).fill(0);
    (state.heap_len, state.heap_max, state.max_symbol) = (0, node_cap, 0);
    or_abort(state.frequencies.get_mut(..sym_cnt)).copy_from_slice(frequencies);
    for (sym, &freq) in frequencies.iter().enumerate() {
        if freq != 0 {
            let sym_u16 = or_abort(u16::try_from(sym).ok());
            state.heap_len = state.heap_len.strict_add(1);
            *at_mut(&mut state.heap, state.heap_len) = sym_u16;
            state.max_symbol = sym_u16;
        }
    }
    while state.heap_len < 2 {
        let sym = if state.max_symbol < 2 {
            state.max_symbol = state.max_symbol.strict_add(1);
            state.max_symbol
        } else {
            0
        };
        *at_mut(&mut state.frequencies, usize::from(sym)) = 1;
        state.heap_len = state.heap_len.strict_add(1);
        *at_mut(&mut state.heap, state.heap_len) = sym;
    }
    for idx in (1..=state.heap_len.div_euclid(2)).rev() {
        state.heap_down(idx);
    }
    let mut next_node = or_abort(u16::try_from(sym_cnt).ok());
    while state.heap_len >= 2 {
        let left = at(&state.heap, 1);
        *at_mut(&mut state.heap, 1) = at(&state.heap, state.heap_len);
        state.heap_len = state.heap_len.strict_sub(1);
        state.heap_down(1);
        let right = at(&state.heap, 1);
        state.heap_max = state.heap_max.strict_sub(1);
        *at_mut(&mut state.heap, state.heap_max) = left;
        state.heap_max = state.heap_max.strict_sub(1);
        *at_mut(&mut state.heap, state.heap_max) = right;
        let (li, ri, ni) = (
            usize::from(left),
            usize::from(right),
            usize::from(next_node),
        );
        *at_mut(&mut state.frequencies, ni) =
            at(&state.frequencies, li).strict_add(at(&state.frequencies, ri));
        *at_mut(&mut state.depths, ni) = at(&state.depths, li)
            .max(at(&state.depths, ri))
            .strict_add(1);
        *at_mut(&mut state.parents, li) = next_node;
        *at_mut(&mut state.parents, ri) = next_node;
        *at_mut(&mut state.heap, 1) = next_node;
        next_node = next_node.strict_add(1);
        state.heap_down(1);
    }
    state.heap_max = state.heap_max.strict_sub(1);
    *at_mut(&mut state.heap, state.heap_max) = at(&state.heap, 1);
    let max_len = usize::from(max_bits);
    let (mut len_counts, mut node_lens, mut overflow) =
        ([0_usize; MAX_BITS + 1], [0_u8; NODE_CAP], 0_usize);
    for &node in or_abort(state.heap.get(state.heap_max.strict_add(1)..node_cap)) {
        let ni = usize::from(node);
        let mut len =
            usize::from(at(&node_lens, usize::from(at(&state.parents, ni)))).strict_add(1);
        if len > max_len {
            len = max_len;
            overflow = overflow.strict_add(1);
        }
        *at_mut(&mut node_lens, ni) = or_abort(u8::try_from(len).ok());
        if node <= state.max_symbol {
            let cnt = at_mut(&mut len_counts, len);
            *cnt = cnt.strict_add(1);
        }
    }
    if overflow == 0 {
        lengths.copy_from_slice(or_abort(node_lens.get(..sym_cnt)));
        return;
    }
    while overflow != 0 {
        let short = or_abort((1..max_len).rev().find(|&bits| at(&len_counts, bits) != 0));
        *at_mut(&mut len_counts, short) = at(&len_counts, short).strict_sub(1);
        *at_mut(&mut len_counts, short.strict_add(1)) =
            at(&len_counts, short.strict_add(1)).strict_add(2);
        *at_mut(&mut len_counts, max_len) = at(&len_counts, max_len).strict_sub(1);
        overflow = overflow.strict_sub(2);
    }
    let mut ord_idx = node_cap;
    for bits in (1..=max_len).rev() {
        let mut rem = at(&len_counts, bits);
        while rem != 0 {
            let sym = loop {
                ord_idx = ord_idx.strict_sub(1);
                let cand = at(&state.heap, ord_idx);
                if cand <= state.max_symbol {
                    break usize::from(cand);
                }
            };
            *at_mut(lengths, sym) = or_abort(u8::try_from(bits).ok());
            rem = rem.strict_sub(1);
        }
    }
}
fn canonical_next_codes(
    lengths: &[u8],
    counts: &mut [u16; MAX_BITS + 1],
) -> Result<Option<[u16; MAX_BITS + 1]>> {
    for &len in lengths {
        if len != 0 {
            let cnt = counts
                .get_mut(usize::from(len))
                .ok_or("deflate Huffman code 길이 초과")?;
            *cnt = cnt.strict_add(1);
        }
    }
    if counts.iter().skip(1).all(|&cnt| cnt == 0) {
        return Ok(None);
    }
    let (mut next_codes, mut code) = ([0_u16; MAX_BITS + 1], 0_u16);
    for (slot, &prev) in next_codes.iter_mut().skip(1).zip(counts.iter()) {
        code = code
            .checked_add(prev)
            .and_then(|sum| sum.checked_mul(2))
            .ok_or("deflate canonical code 초과")?;
        *slot = code;
    }
    Ok(Some(next_codes))
}
#[inline]
fn decode_match_token(token: u32) -> (u16, u16, u8, u16, u16, u8) {
    let [low, len_extra] = at(&LEN_SYM_TABLE, ((token >> 16_u32) & 0xFF) as usize).to_le_bytes();
    let dist_val = (token & 0x7FFF) as u16;
    let dist_bits = dist_val.bit_width().saturating_sub(2).to_le_bytes()[0];
    let dist_sym = if dist_bits == 0 {
        dist_val
    } else {
        u16::from(dist_bits).strict_add(1).strict_mul(2) | ((dist_val >> dist_bits) & 1)
    };
    let dist_extra = dist_val & 1_u16.strict_shl(u32::from(dist_bits)).strict_sub(1);
    (
        257_u16.strict_add(u16::from(low & 0x1f)),
        u16::from(len_extra),
        low >> 5_u8,
        dist_sym,
        dist_extra,
        dist_bits,
    )
}
fn ensure_out_len(current: usize, add: usize, expected: usize) -> Result<()> {
    let next = current
        .checked_add(add)
        .ok_or("deflate 출력 크기 계산 실패")?;
    ensure(
        next <= expected,
        "deflate 출력이 ZIP 선언 해제 크기를 초과했습니다.",
    )
}
fn fill_write_huffman_codes(lengths: &[u8], codes: &mut [u32]) -> Result<()> {
    let mut counts = [0_u16; MAX_BITS + 1];
    let mut next_code = or_abort(canonical_next_codes(lengths, &mut counts)?);
    for (slot, &len) in codes.iter_mut().zip(lengths) {
        let code = if len == 0 {
            0
        } else {
            let next = at_mut(&mut next_code, usize::from(len));
            let rev = reverse_low_bits(*next, len);
            *next = next.strict_add(1);
            rev
        };
        let [low, high] = code.to_le_bytes();
        *slot = u32::from_le_bytes([low, high, len, 0]);
    }
    Ok(())
}
fn hash3(bytes: &[u8], pos: usize) -> Option<usize> {
    let &[first, second, third] = bytes.get(pos..)?.first_chunk::<3>()?;
    Some(
        ((usize::from(first) << 10_usize) ^ (usize::from(second) << 5_usize) ^ usize::from(third))
            & HASH_SIZE.strict_sub(1),
    )
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
    if let Some((lit, dist)) = dynamic {
        for &tok in tokens {
            if tok & 0x8000_0000 == 0 {
                write_huffman_with_extra(lit, writer, (tok & 0xFF) as u16, 0, 0)?;
            } else {
                let (ls, le, lb, ds, de, db) = decode_match_token(tok);
                write_huffman_with_extra(lit, writer, ls, le, lb)?;
                write_huffman_with_extra(dist, writer, ds, de, db)?;
            }
        }
        write_huffman_with_extra(lit, writer, 256, 0, 0)
    } else {
        for &tok in tokens {
            if tok & 0x8000_0000 == 0 {
                write_fixed_symbol(writer, (tok & 0xFF) as u16)?;
            } else {
                let (ls, le, lb, ds, de, db) = decode_match_token(tok);
                write_fixed_symbol(writer, ls)?;
                writer.write_bits(u32::from(le), lb)?;
                writer.write_bits(
                    u32::from(reverse_low_bits(ds, 5)) | (u32::from(de) << 5_u8),
                    5_u8.strict_add(db),
                )?;
            }
        }
        write_fixed_symbol(writer, 256)
    }
}
fn write_fixed_symbol(writer: &mut BitWriter<'_>, symbol: u16) -> Result<()> {
    let (code, bits) = match symbol {
        0..=143 => (0x30_u16.strict_add(symbol), 8),
        144..=255 => (0x190_u16.strict_add(symbol.strict_sub(144)), 9),
        256..=279 => (symbol.strict_sub(256), 7),
        280..=287 => (0xc0_u16.strict_add(symbol.strict_sub(280)), 8),
        _ => bail!("deflate fixed symbol 범위 오류"),
    };
    writer.write_bits(u32::from(reverse_low_bits(code, bits)), bits)
}
#[inline]
fn write_huffman_with_extra(
    codes: &[u32],
    writer: &mut BitWriter<'_>,
    symbol: u16,
    extra: u16,
    extra_bits: u8,
) -> Result<()> {
    let [low, high, len, _] = at(codes, usize::from(symbol)).to_le_bytes();
    debug_assert!(len != 0, "deflate huffman symbol length must be non-zero");
    writer.write_bits(
        u32::from(u16::from_le_bytes([low, high])) | (u32::from(extra) << len),
        len.strict_add(extra_bits),
    )
}
