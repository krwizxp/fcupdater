use std::env;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write as _};
use std::path::PathBuf;
const TAR_BLOCK_LEN: usize = 512;
const TAR_BLOCK_LEN_U64: u64 = 512;
struct TarHeader {
    checksum: [u8; 8],
    mode: [u8; 8],
    mtime: [u8; 12],
    name: [u8; 100],
    size: [u8; 12],
    zero_octal: [u8; 8],
}
impl TarHeader {
    const fn fields(&self) -> [&[u8]; 17] {
        const EMPTY_12: [u8; 12] = [0; 12];
        const EMPTY_32: [u8; 32] = [0; 32];
        const EMPTY_100: [u8; 100] = [0; 100];
        const EMPTY_155: [u8; 155] = [0; 155];
        [
            &self.name,
            &self.mode,
            &self.zero_octal,
            &self.zero_octal,
            &self.size,
            &self.mtime,
            &self.checksum,
            b"0",
            &EMPTY_100,
            b"ustar\0",
            b"00",
            &EMPTY_32,
            &EMPTY_32,
            &self.zero_octal,
            &self.zero_octal,
            &EMPTY_155,
            &EMPTY_12,
        ]
    }
}
fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn required_arg(args: &mut impl Iterator<Item = OsString>) -> io::Result<OsString> {
    args.next()
        .ok_or_else(|| invalid_input("source, destination, entry name arguments are required"))
}
fn octal_field<const WIDTH: usize>(value: u64) -> io::Result<[u8; WIDTH]> {
    let digit_count = WIDTH
        .checked_sub(1)
        .ok_or_else(|| invalid_input("tar octal field width is zero"))?;
    let text = format!("{value:0digit_count$o}\0");
    if text.len() != WIDTH {
        return Err(invalid_input("tar octal value exceeds its header field"));
    }
    let mut field = [0_u8; WIDTH];
    for (slot, byte) in field.iter_mut().zip(text.bytes()) {
        *slot = byte;
    }
    Ok(field)
}
fn main() -> io::Result<()> {
    let mut args = env::args_os().skip(1);
    let source = PathBuf::from(required_arg(&mut args)?);
    let destination = PathBuf::from(required_arg(&mut args)?);
    let Ok(entry_name) = required_arg(&mut args)?.into_string() else {
        return Err(invalid_input("artifact entry name must be valid UTF-8"));
    };
    if args.next().is_some() {
        return Err(invalid_input("unexpected package artifact argument"));
    }
    if let Some(parent) = destination
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    if destination.extension() == Some(OsStr::new("tar")) {
        if entry_name.is_empty()
            || entry_name.len() > 100
            || entry_name.contains('/')
            || entry_name.contains('\\')
        {
            return Err(invalid_input(
                "tar entry name must be a 1-100 byte file name",
            ));
        }
        let source_len = source.metadata()?.len();
        let mut name_field = [0_u8; 100];
        for (slot, byte) in name_field.iter_mut().zip(entry_name.bytes()) {
            *slot = byte;
        }
        let mut header = TarHeader {
            checksum: [b' '; 8],
            mode: octal_field::<8>(0o755)?,
            mtime: octal_field::<12>(0)?,
            name: name_field,
            size: octal_field::<12>(source_len)?,
            zero_octal: octal_field::<8>(0)?,
        };
        let checksum = header
            .fields()
            .into_iter()
            .flatten()
            .map(|byte| u64::from(*byte))
            .sum::<u64>();
        let checksum_text = format!("{checksum:06o}\0 ");
        if checksum_text.len() != 8 {
            return Err(invalid_input("tar checksum exceeds its header field"));
        }
        for (slot, byte) in header.checksum.iter_mut().zip(checksum_text.bytes()) {
            *slot = byte;
        }
        let mut input = File::open(&source)?;
        let mut output = BufWriter::new(File::create(destination)?);
        for field in header.fields() {
            output.write_all(field)?;
        }
        let copied = io::copy(&mut input, &mut output)?;
        if copied != source_len {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source binary changed while packaging",
            ));
        }
        let remainder = source_len.rem_euclid(TAR_BLOCK_LEN_U64);
        if remainder != 0 {
            for _ in 0..TAR_BLOCK_LEN_U64.abs_diff(remainder) {
                output.write_all(&[0_u8])?;
            }
        }
        output.write_all(&[0_u8; TAR_BLOCK_LEN])?;
        output.write_all(&[0_u8; TAR_BLOCK_LEN])?;
        output.flush()
    } else {
        let source_len = source.metadata()?.len();
        let copied = fs::copy(source, destination)?;
        if copied == source_len {
            Ok(())
        } else {
            Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source binary changed while copying",
            ))
        }
    }
}
