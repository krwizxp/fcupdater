use std::env;
use std::ffi::{OsStr, OsString};
use std::fs::{self, File};
use std::io::{self, BufWriter, Write as _};
use std::path::PathBuf;
const TAR_BLOCK_LEN: usize = 512;
const TAR_BLOCK_LEN_U64: u64 = 512;
const TAR_NAME_LEN: usize = 100;
const ZERO_BLOCK: [u8; TAR_BLOCK_LEN] = [0; TAR_BLOCK_LEN];
const ZERO_NAME: [u8; TAR_NAME_LEN] = [0; TAR_NAME_LEN];
fn invalid_input(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
fn required_arg(args: &mut impl Iterator<Item = OsString>) -> io::Result<OsString> {
    args.next()
        .ok_or_else(|| invalid_input("source, destination, entry name arguments are required"))
}
fn octal_field<const WIDTH: usize>(value: u64) -> io::Result<[u8; WIDTH]> {
    let digit_count = const {
        assert!(WIDTH > 0, "tar octal field width must be positive");
        WIDTH - 1
    };
    let text = format!("{value:0digit_count$o}\0");
    if text.len() != WIDTH {
        return Err(invalid_input("tar octal value exceeds its header field"));
    }
    let mut field = [0_u8; WIDTH];
    field.copy_from_slice(text.as_bytes());
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
    let source_len = source.metadata()?.len();
    if destination.extension() == Some(OsStr::new("tar")) {
        if entry_name.is_empty()
            || entry_name.len() > TAR_NAME_LEN
            || entry_name.contains('/')
            || entry_name.contains('\\')
        {
            return Err(invalid_input(
                "tar entry name must be a 1-100 byte file name",
            ));
        }
        let mut header = ZERO_BLOCK;
        {
            let mut header_writer = header.as_mut_slice();
            header_writer.write_all(entry_name.as_bytes())?;
            let name_padding = TAR_NAME_LEN.abs_diff(entry_name.len());
            let (name_padding_bytes, _) = ZERO_NAME.split_at(name_padding);
            header_writer.write_all(name_padding_bytes)?;
            header_writer.write_all(&octal_field::<8>(0o755)?)?;
            let zero_octal = octal_field::<8>(0)?;
            header_writer.write_all(&zero_octal)?;
            header_writer.write_all(&zero_octal)?;
            header_writer.write_all(&octal_field::<12>(source_len)?)?;
            header_writer.write_all(&octal_field::<12>(0)?)?;
            header_writer.write_all(b"        0")?;
            header_writer.write_all(&ZERO_NAME)?;
            header_writer.write_all(b"ustar\x0000")?;
            header_writer.write_all(&[0_u8; 64])?;
            header_writer.write_all(&zero_octal)?;
            header_writer.write_all(&zero_octal)?;
            header_writer.write_all(&[0_u8; 167])?;
        }
        let checksum = header.iter().map(|byte| u64::from(*byte)).sum::<u64>();
        let (_, checksum_and_tail) = header.split_at_mut(148);
        let (checksum_field, _) = checksum_and_tail.split_at_mut(8);
        checksum_field.copy_from_slice(format!("{checksum:06o}\0 ").as_bytes());
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
            let padding = usize::try_from(TAR_BLOCK_LEN_U64.abs_diff(remainder)).map_err(
                |conversion_error| io::Error::new(io::ErrorKind::InvalidInput, conversion_error),
            )?;
            let (padding_bytes, _) = ZERO_BLOCK.split_at(padding);
            output.write_all(padding_bytes)?;
        }
        output.write_all(&ZERO_BLOCK)?;
        output.write_all(&ZERO_BLOCK)?;
        output.flush()
    } else {
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
