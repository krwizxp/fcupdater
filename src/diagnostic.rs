use alloc::{borrow::Cow, collections::TryReserveError};
use core::{
    error::Error,
    fmt::{self, Display, Write as _},
    num::IntErrorKind,
    result::Result as CoreResult,
};
use std::{io::Error as IoError, path::Path, process};
macro_rules! fmt_err {
    ($($arg:tt)*) => {
        $crate::diagnostic::err_fmt(::core::format_args!($($arg)*))
    };
}
macro_rules! bail_fmt {
    ($($arg:tt)*) => {
        return ::core::result::Result::Err($crate::diagnostic::err_fmt(::core::format_args!($($arg)*)))
    };
}
macro_rules! ensure_fmt {
    ($cond:expr, $($arg:tt)*) => {
        if !($cond) {
            return ::core::result::Result::Err($crate::diagnostic::err_fmt(::core::format_args!($($arg)*)));
        }
    };
}
macro_rules! bail {
    ($msg:expr) => {
        return ::core::result::Result::Err($crate::diagnostic::err($msg))
    };
}
macro_rules! fmt_src {
    ($src:expr, $($arg:tt)*) => {
        $crate::diagnostic::err_fmt_with_source(::core::format_args!($($arg)*), ($src).into())
    };
}
pub(crate) use {bail, bail_fmt, ensure_fmt, fmt_err, fmt_src};
type BoxError = Box<dyn Error + Send + Sync>;
pub(super) type Result<T> = CoreResult<T, AppError>;
pub(super) struct AppError {
    message: Cow<'static, str>,
    source: Option<BoxError>,
}
struct ControlEscapingWriter<'formatter, 'output>(&'formatter mut fmt::Formatter<'output>);
impl AppError {
    #[cold]
    #[inline(never)]
    fn context_boxed(message: Cow<'static, str>, source: BoxError) -> Self {
        Self {
            message,
            source: Some(source),
        }
    }
    pub(super) fn update_message(&mut self, update: impl FnOnce(&str) -> String) {
        self.message = Cow::Owned(update(self.message.as_ref()));
    }
}
impl Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_control_escaped(f, self.message.as_ref())?;
        if let Some(source) = self.source.as_ref() {
            f.write_str(": ")?;
            write!(&mut ControlEscapingWriter(f), "{source}")?;
        }
        Ok(())
    }
}
impl fmt::Write for ControlEscapingWriter<'_, '_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        write_control_escaped(self.0, s)
    }
}
impl fmt::Debug for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}
impl Error for AppError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source
            .as_deref()
            .map(|source| -> &(dyn Error + 'static) { source })
    }
}
impl From<String> for AppError {
    fn from(value: String) -> Self {
        err(value)
    }
}
impl From<&'static str> for AppError {
    fn from(value: &'static str) -> Self {
        err(value)
    }
}
impl From<IoError> for AppError {
    fn from(value: IoError) -> Self {
        err_with_source("I/O 오류", value)
    }
}
pub(super) fn add_row_offset(base_row: u32, offset: usize, context: &str) -> Result<u32> {
    let offset_u32 = usize_to_u32(offset, context)?;
    base_row.checked_add(offset_u32).ok_or_else(|| {
        fmt_err!("{context} 계산 중 overflow가 발생했습니다. ({base_row} + {offset_u32})")
    })
}
pub(super) fn append_fmt(target: &mut String, args: fmt::Arguments<'_>) {
    assert!(
        target.write_fmt(args).is_ok(),
        "String formatting must be infallible"
    );
}
#[inline]
pub(super) fn ensure(condition: bool, message: &'static str) -> Result<()> {
    if condition { Ok(()) } else { Err(err(message)) }
}
pub(super) fn err(msg: impl Into<Cow<'static, str>>) -> AppError {
    AppError {
        message: msg.into(),
        source: None,
    }
}
#[cold]
#[inline(never)]
pub(super) fn err_fmt(args: fmt::Arguments<'_>) -> AppError {
    let mut message = String::new();
    append_fmt(&mut message, args);
    err(message)
}
#[cold]
#[inline(never)]
pub(super) fn err_fmt_with_source(args: fmt::Arguments<'_>, source: BoxError) -> AppError {
    let mut message = String::new();
    append_fmt(&mut message, args);
    AppError::context_boxed(Cow::Owned(message), source)
}
pub(super) fn err_with_source(
    context: impl Into<Cow<'static, str>>,
    source: impl Into<BoxError>,
) -> AppError {
    AppError::context_boxed(context.into(), source.into())
}
pub(super) fn map_reserve(
    result: CoreResult<(), TryReserveError>,
    context: &'static str,
) -> Result<()> {
    result.map_err(|source| err_with_source(context, source))
}
#[inline]
pub(super) fn or_abort<T>(value: Option<T>) -> T {
    value.unwrap_or_else(|| process::abort())
}
pub(super) fn parse_unsigned_u32(
    value: &str,
    format_error: fmt::Arguments<'_>,
    parse_context: fmt::Arguments<'_>,
) -> Result<u32> {
    if value.starts_with('+') {
        return Err(err_fmt(format_error));
    }
    value.parse::<u32>().map_err(|source| {
        if source.kind() == &IntErrorKind::PosOverflow {
            err_fmt_with_source(parse_context, source.into())
        } else {
            err_fmt(format_error)
        }
    })
}
pub(super) fn path_err_with_source(
    label: &str,
    path: &Path,
    source: impl Into<BoxError>,
) -> AppError {
    err_fmt_with_source(format_args!("{label}: {}", path.display()), source.into())
}
pub(super) const fn terminal_safe<T>(value: &T) -> impl Display + '_
where
    T: Display + ?Sized,
{
    fmt::from_fn(move |formatter| write!(&mut ControlEscapingWriter(formatter), "{value}"))
}
pub(super) fn try_push_vec<T>(
    vec: &mut Vec<T>,
    value: T,
    growth: usize,
    context: &'static str,
) -> Result<()> {
    if vec.len() == vec.capacity() {
        map_reserve(vec.try_reserve(growth), context)?;
    }
    vec.push(value);
    Ok(())
}
pub(super) fn try_string_with_capacity(capacity: usize, context: &'static str) -> Result<String> {
    let mut value = String::new();
    map_reserve(value.try_reserve_exact(capacity), context)?;
    Ok(value)
}
pub(super) fn try_vec_with_capacity<T>(capacity: usize, context: &'static str) -> Result<Vec<T>> {
    let mut value = Vec::new();
    map_reserve(value.try_reserve_exact(capacity), context)?;
    Ok(value)
}
pub(super) fn usize_to_u32(value: usize, context: &str) -> Result<u32> {
    u32::try_from(value)
        .map_err(|source| fmt_src!(source, "{context} 값이 너무 큽니다. (value={value})"))
}
fn write_control_escaped(formatter: &mut fmt::Formatter<'_>, text: &str) -> fmt::Result {
    let (mut remaining, mut consumed) = (text, 0_usize);
    for (index, character) in text.char_indices() {
        if character.is_control()
            || matches!(
                character,
                '\u{061c}'
                    | '\u{200e}'
                    | '\u{200f}'
                    | '\u{202a}'..='\u{202e}'
                    | '\u{2066}'..='\u{2069}'
            )
        {
            let (plain, escaped_tail) = remaining.split_at(index.strict_sub(consumed));
            if !plain.is_empty() {
                formatter.write_str(plain)?;
            }
            write!(formatter, "{}", character.escape_debug())?;
            let char_len = character.len_utf8();
            remaining = escaped_tail.split_at(char_len).1;
            consumed = index.strict_add(char_len);
        }
    }
    if remaining.is_empty() {
        Ok(())
    } else {
        formatter.write_str(remaining)
    }
}
