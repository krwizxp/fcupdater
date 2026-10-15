use super::{
    DeflateInflater, DeflateToken, DeflateWorkspace, DeflateWriter, ZipResult, zip_static,
};

#[test]
fn matches_can_refer_to_the_first_input_position() -> ZipResult<()> {
    let mut workspace = DeflateWorkspace::default();
    let (tokens, _) = DeflateWriter {
        bytes: b"abcabcabcabc",
        workspace: &mut workspace,
    }
    .tokens()?
    .ok_or_else(|| zip_static("Unexpected work-budget exhaustion"))?;
    matches!(
        tokens.get(3),
        Some(DeflateToken::Match {
            distance: 3,
            length: 9
        })
    )
    .then_some(())
    .ok_or_else(|| zip_static("The first input position was omitted from the match index"))
}

#[test]
fn compression_round_trips_boundaries_and_reused_workspace() -> ZipResult<()> {
    let mut workspace = DeflateWorkspace::default();
    for size in [
        0,
        1,
        2,
        3,
        4,
        257,
        258,
        259,
        8191,
        8192,
        0x7fff,
        0x8000,
        0x0001_0001,
    ] {
        for pattern in [b"a".as_slice(), b"abc", b"<c r=\"A1\"><v>123</v></c>"] {
            let input: Vec<_> = pattern.iter().copied().cycle().take(size).collect();
            let plan = DeflateWriter {
                bytes: &input,
                workspace: &mut workspace,
            }
            .plan()?
            .ok_or_else(|| zip_static("Unexpected work-budget exhaustion"))?;
            let mut compressed = Vec::new();
            let written = plan.write_to(&mut compressed)?;
            let (decoded, crc) = DeflateInflater {
                bytes: &compressed,
                expected_len: size,
            }
            .inflate()?;
            (decoded == input
                && crc == plan.crc32()
                && written == plan.len()
                && written == compressed.len())
            .then_some(())
            .ok_or_else(|| zip_static("Compression round trip or plan size mismatch"))?;
            workspace.recycle(plan);
        }
    }
    Ok(())
}

#[test]
fn incomplete_literal_tree_validation() -> ZipResult<()> {
    let bytes = [
        5, 128, 1, 4, 0, 0, 0, 64, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2,
    ];
    DeflateInflater {
        bytes: &bytes,
        expected_len: 0,
    }
    .inflate()
    .is_err()
    .then_some(())
    .ok_or_else(|| zip_static("Independent zlib tree-validity expectation differs"))
}
#[test]
fn incomplete_code_lengths_tree_validation() -> ZipResult<()> {
    let bytes = [
        5, 192, 1, 8, 0, 0, 0, 0, 32, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1,
    ];
    DeflateInflater {
        bytes: &bytes,
        expected_len: 0,
    }
    .inflate()
    .is_err()
    .then_some(())
    .ok_or_else(|| zip_static("Independent zlib tree-validity expectation differs"))
}
#[test]
fn incomplete_distance_tree_validation() -> ZipResult<()> {
    let bytes = [
        5, 192, 1, 8, 0, 0, 0, 64, 160, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170,
        170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170,
        170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170,
        170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 170, 1,
    ];
    DeflateInflater {
        bytes: &bytes,
        expected_len: 0,
    }
    .inflate()
    .is_err()
    .then_some(())
    .ok_or_else(|| zip_static("Independent zlib tree-validity expectation differs"))
}
#[test]
fn valid_single_eob_tree_validation() -> ZipResult<()> {
    let bytes = [
        5, 192, 1, 4, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 0,
    ];
    DeflateInflater {
        bytes: &bytes,
        expected_len: 0,
    }
    .inflate()
    .is_ok_and(|(decoded, crc)| decoded.is_empty() && crc == 0)
    .then_some(())
    .ok_or_else(|| zip_static("Independent zlib tree-validity expectation differs"))
}
#[test]
fn valid_single_eob_distance_tree_validation() -> ZipResult<()> {
    let bytes = [
        5, 192, 1, 4, 0, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 128, 1,
    ];
    DeflateInflater {
        bytes: &bytes,
        expected_len: 0,
    }
    .inflate()
    .is_ok_and(|(decoded, crc)| decoded.is_empty() && crc == 0)
    .then_some(())
    .ok_or_else(|| zip_static("Independent zlib tree-validity expectation differs"))
}
