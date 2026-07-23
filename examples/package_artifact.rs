use std::env;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write as _};
use std::path::{Path, PathBuf};
const TAR_BLOCK_LEN: usize = 512;
const TAR_BLOCK_LEN_U64: u64 = 512;
const ZERO_BLOCK: [u8; TAR_BLOCK_LEN] = [0; TAR_BLOCK_LEN];
fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn copy_verified(
    source: &Path,
    destination: &Path,
    source_len: u64,
    changed_message: &'static str,
) -> io::Result<()> {
    let copied = fs::copy(source, destination)?;
    if copied == source_len {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            changed_message,
        ))
    }
}
fn write_octal(field: &mut [u8], mut value: u64) -> io::Result<()> {
    field.fill(b'0');
    let Some((terminator, digits)) = field.split_last_mut() else {
        return Err(invalid_input("tar octal field must not be empty"));
    };
    *terminator = 0;
    for digit in digits.iter_mut().rev() {
        *digit |= value.to_le_bytes()[0] & 7;
        value >>= 3_u32;
    }
    if value == 0 {
        Ok(())
    } else {
        Err(invalid_input("tar octal value exceeds its header field"))
    }
}
fn main() -> io::Result<()> {
    let mut args = env::args_os().skip(1);
    let Some(raw_entry_name) = args.next() else {
        return Err(invalid_input("artifact entry name is required"));
    };
    let Ok(entry_name) = raw_entry_name.into_string() else {
        return Err(invalid_input("artifact entry name must be valid UTF-8"));
    };
    if args.next().is_some() {
        return Err(invalid_input("unexpected package artifact argument"));
    }
    let artifact_dir = Path::new("artifacts");
    if entry_name == "--workbook" {
        fs::create_dir_all(artifact_dir)?;
        let source = Path::new("fuel_cost_chungcheong.xlsx");
        let source_len = source.metadata()?.len();
        return copy_verified(
            source,
            &artifact_dir.join("fcupdater-result.xlsx"),
            source_len,
            "source workbook changed while copying",
        );
    }
    if entry_name.is_empty()
        || entry_name.len() > 100
        || entry_name.contains('/')
        || entry_name.contains('\\')
    {
        return Err(invalid_input(
            "artifact entry name must be a 1-100 byte file name",
        ));
    }
    fs::create_dir_all(artifact_dir)?;
    let source = PathBuf::from("target").join("release").join(format!(
        "{}{}",
        env!("CARGO_PKG_NAME"),
        env::consts::EXE_SUFFIX
    ));
    let destination = artifact_dir.join(format!(
        "{entry_name}.{}",
        if cfg!(windows) { "exe" } else { "tar" }
    ));
    let source_len = source.metadata()?.len();
    if cfg!(windows) {
        return copy_verified(
            &source,
            &destination,
            source_len,
            "source binary changed while copying",
        );
    }
    let mut header = ZERO_BLOCK;
    let Some(name_field) = header
        .get_mut(..100)
        .and_then(|field| field.get_mut(..entry_name.len()))
    else {
        return Err(invalid_input(
            "tar entry name must be a 1-100 byte file name",
        ));
    };
    name_field.copy_from_slice(entry_name.as_bytes());
    write_octal(&mut header[100..108], 0o755)?;
    write_octal(&mut header[108..116], 0)?;
    write_octal(&mut header[116..124], 0)?;
    write_octal(&mut header[124..136], source_len)?;
    write_octal(&mut header[136..148], 0)?;
    header[148..156].fill(b' ');
    header[156] = b'0';
    header[257..265].copy_from_slice(b"ustar\x0000");
    write_octal(&mut header[329..337], 0)?;
    write_octal(&mut header[337..345], 0)?;
    let checksum = header.iter().map(|byte| u64::from(*byte)).sum::<u64>();
    write_octal(&mut header[148..155], checksum)?;
    header[155] = b' ';
    let mut input = File::open(&source)?;
    let mut output = BufWriter::new(File::create(destination)?);
    output.write_all(&header)?;
    let copied = io::copy(&mut input, &mut output)?;
    if copied != source_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "source binary changed while packaging",
        ));
    }
    let remainder = source_len.rem_euclid(TAR_BLOCK_LEN_U64);
    if remainder != 0 {
        let remainder_usize = usize::try_from(remainder).map_err(|conversion_error| {
            io::Error::new(io::ErrorKind::InvalidInput, conversion_error)
        })?;
        let padding = TAR_BLOCK_LEN.abs_diff(remainder_usize);
        let (padding_bytes, _) = ZERO_BLOCK.split_at(padding);
        output.write_all(padding_bytes)?;
    }
    output.write_all(&ZERO_BLOCK)?;
    output.write_all(&ZERO_BLOCK)?;
    output.flush()
}
