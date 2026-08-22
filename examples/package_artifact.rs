use std::env;
use std::fs::{self, File};
use std::io::{self, BufWriter, Write as _};
use std::path::{Path, PathBuf};
use std::process;
const TAR_BLOCK_LEN: usize = 512;
const TAR_BLOCK_LEN_U64: u64 = 512;
const ZERO_BLOCK: [u8; TAR_BLOCK_LEN] = [0; TAR_BLOCK_LEN];
fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn source_changed() -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "source binary changed while packaging",
    )
}
fn write_octal(field: &mut [u8], mut value: u64) -> io::Result<()> {
    field.fill(b'0');
    let (terminator, digits) = field.split_last_mut().unwrap_or_else(|| process::abort());
    *terminator = 0;
    for digit in digits.iter_mut().rev() {
        *digit |= value.to_le_bytes()[0] & 7;
        value >>= 3_u32;
    }
    (value == 0).ok_or_else(|| invalid_input("tar octal value exceeds its header field"))
}
fn main() -> io::Result<()> {
    let mut args = env::args_os().skip(1);
    let Some(raw_entry_name) = args.next() else {
        return Err(invalid_input("artifact entry name is required"));
    };
    let Ok(entry_name) = raw_entry_name.into_string() else {
        return Err(invalid_input("artifact entry name must be valid UTF-8"));
    };
    args.next()
        .is_none()
        .ok_or_else(|| invalid_input("unexpected package artifact argument"))?;
    ((1..=100).contains(&entry_name.len())
        && !matches!(entry_name.as_str(), "." | "..")
        && !entry_name.contains([':', '/', '\\']))
    .ok_or_else(|| invalid_input("artifact entry name must be a 1-100 byte file name"))?;
    let mut source = env::var_os("CARGO_TARGET_DIR")
        .map_or_else(|| PathBuf::from("target"), PathBuf::from)
        .join("release")
        .join(env!("CARGO_PKG_NAME"));
    source.add_extension(env::consts::EXE_EXTENSION);
    let source_len = fs::metadata(&source)?.len();
    let artifact_dir = Path::new("artifacts");
    fs::create_dir_all(artifact_dir)?;
    let mut destination = artifact_dir.join(&entry_name);
    destination.add_extension(if cfg!(windows) { "exe" } else { "tar" });
    if cfg!(windows) {
        (fs::copy(source, destination)? == source_len).ok_or_else(source_changed)?;
        return Ok(());
    }
    let mut input = File::open(source)?;
    let mut header = ZERO_BLOCK;
    let name_field = header
        .get_mut(..entry_name.len())
        .unwrap_or_else(|| process::abort());
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
    let mut output = BufWriter::new(File::create(destination)?);
    output.write_all(&header)?;
    let copied = io::copy(&mut input, &mut output)?;
    (copied == source_len).ok_or_else(source_changed)?;
    let remainder = source_len.rem_euclid(TAR_BLOCK_LEN_U64);
    if remainder != 0 {
        let padding = TAR_BLOCK_LEN
            .strict_sub(usize::try_from(remainder).unwrap_or_else(|_| process::abort()));
        output.write_all(ZERO_BLOCK.split_at(padding).0)?;
    }
    output.write_all([ZERO_BLOCK; 2].as_flattened())?;
    output.flush()
}
