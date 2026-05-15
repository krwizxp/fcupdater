use core::fmt::Arguments;
use std::io::Write;
pub fn write_line_ignored(output: &mut dyn Write, args: Arguments<'_>) {
    match output.write_fmt(args) {
        Ok(()) | Err(_) => {}
    }
    match output.write_all(b"\n") {
        Ok(()) | Err(_) => {}
    }
}
