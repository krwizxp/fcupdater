use super::{BiffWorkbookReader, Result, SstChunkReader, err};

fn decode(chunks: &[&[u8]], offset: usize, count: usize, high_byte: bool) -> Result<String> {
    let mut reader = SstChunkReader {
        chunk_index: 0,
        chunks,
        offset_in_chunk: offset,
    };
    let mut text = String::new();
    reader.read_xl_unicode_chars(count, high_byte, &mut text)?;
    Ok(text)
}

#[test]
fn characters_starting_in_continue_use_its_encoding_flag() -> Result<()> {
    let text = decode(&[&[2, 0, 1], &[1, 0, 0xac, 0x98, 0xb0]], 3, 2, true)?;
    (text == "\u{ac00}\u{b098}").ok_or_else(|| err(format!("Incorrect continued text: {text:?}")))
}

#[test]
fn surrogate_pair_can_cross_a_continue_record() -> Result<()> {
    let text = decode(&[&[0x3d, 0xd8], &[1, 0, 0xde]], 0, 2, true)?;
    (text == "\u{1f600}").ok_or_else(|| err(format!("Incorrect surrogate pair: {text:?}")))
}

#[test]
fn split_utf16_code_unit_is_rejected() -> Result<()> {
    decode(&[&[0x41, 0, 0x99], &[1, 0x42, 0]], 0, 2, true)
        .is_err()
        .ok_or_else(|| err("An incomplete UTF-16 code unit was discarded."))
}

#[test]
fn ordinary_continuations_and_encoding_changes_remain_valid() -> Result<()> {
    for (chunks, expected) in [
        (vec![&b"ab"[..], &b"\0cd"[..]], "abcd"),
        (vec![&b"ab"[..], &b"\x01\x00\xac"[..]], "ab\u{ac00}"),
        (vec![&b"\x00\xac"[..], &b"\0bc"[..]], "\u{ac00}bc"),
    ] {
        let count = expected.encode_utf16().count();
        let high_byte = chunks.first().and_then(|chunk| chunk.first()) == Some(&0);
        (decode(&chunks, 0, count, high_byte)? == expected)
            .ok_or_else(|| err("Valid continuation changed its text."))?;
    }
    Ok(())
}

#[test]
fn utf16_pairs_survive_every_legal_split() -> Result<()> {
    let expected = "A\u{e9}\u{ac00}\u{1f600}\u{20000}Z";
    let bytes: Vec<u8> = expected.encode_utf16().flat_map(u16::to_le_bytes).collect();
    for split in (2..bytes.len()).step_by(2) {
        let (first, tail) = bytes.split_at(split);
        let mut continuation = vec![1];
        continuation.extend_from_slice(tail);
        (decode(
            &[first, &continuation],
            0,
            expected.encode_utf16().count(),
            true,
        )? == expected)
            .ok_or_else(|| err("A legal UTF-16 record split changed the string."))?;
    }
    Ok(())
}

#[test]
fn string_header_must_remain_in_one_record() -> Result<()> {
    let first_chunk = [1, 0, 0, 0, 1, 0, 0, 0, 1];
    let continuation_record = [0x3c, 0, 3, 0, 0, 0, b'A'];
    BiffWorkbookReader(&continuation_record)
        .read_sst(&first_chunk, 0)
        .is_err()
        .ok_or_else(|| err("An SST header split across records was accepted."))
}

#[test]
fn dangling_surrogates_and_partial_code_units_are_rejected() -> Result<()> {
    for chunks in [
        vec![&b"\x3d\xd8"[..]],
        vec![&b"\x00\xde"[..]],
        vec![&b"\x3d\xd8"[..], &b"\x00A"[..]],
        vec![&b"\x3d\xd8"[..], &b"\x01A\x00"[..]],
    ] {
        let count = if chunks.len() == 1 { 1 } else { 2 };
        decode(&chunks, 0, count, true)
            .is_err()
            .ok_or_else(|| err("An unpaired surrogate was accepted."))?;
    }
    Ok(())
}

#[test]
fn continuation_with_only_an_encoding_flag_is_handled() -> Result<()> {
    (decode(&[&b"a"[..], &b"\x01"[..], &b"\x00b"[..]], 0, 2, false)? == "ab")
        .ok_or_else(|| err("A continuation flag was decoded as a character."))
}

#[test]
fn sst_counts_must_remain_in_the_first_record() -> Result<()> {
    let first_chunk = [1, 0, 0, 0];
    let continuation_record = [0x3c, 0, 8, 0, 1, 0, 0, 0, 1, 0, 0, b'A'];
    BiffWorkbookReader(&continuation_record)
        .read_sst(&first_chunk, 0)
        .is_err()
        .ok_or_else(|| err("SST counts split across records were accepted."))
}

#[test]
fn a_complete_string_header_can_start_in_a_continue_record() -> Result<()> {
    let first_chunk = [1, 0, 0, 0, 1, 0, 0, 0];
    let continuation_record = [0x3c, 0, 4, 0, 1, 0, 0, b'A'];
    let (strings, _) = BiffWorkbookReader(&continuation_record).read_sst(&first_chunk, 0)?;
    (strings.text == "A").ok_or_else(|| err("A string starting in CONTINUE was rejected."))
}
