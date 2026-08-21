use alloc::borrow::Cow;
use core::{
    error::Error,
    fmt::{self, Display, Write as _},
    result::Result as CoreResult,
};
use std::{io::Error as IoError, path::Path};
type BoxError = Box<dyn Error + Send + Sync>;
pub(super) type Result<T> = CoreResult<T, AppError>;
pub(super) struct AppError {
    message: Cow<'static, str>,
    source: Option<BoxError>,
}
struct ControlEscapingWriter<'formatter, 'output>(&'formatter mut fmt::Formatter<'output>);
impl AppError {
    fn context(context: impl Into<Cow<'static, str>>, source: impl Into<BoxError>) -> Self {
        Self {
            message: context.into(),
            source: Some(source.into()),
        }
    }
    fn message(message: impl Into<Cow<'static, str>>) -> Self {
        Self {
            message: message.into(),
            source: None,
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
        Self::message(value)
    }
}
impl From<&'static str> for AppError {
    fn from(value: &'static str) -> Self {
        Self::message(value)
    }
}
impl From<IoError> for AppError {
    fn from(value: IoError) -> Self {
        Self::context("I/O 오류", value)
    }
}
pub(super) fn err(msg: impl Into<Cow<'static, str>>) -> AppError {
    AppError::message(msg)
}
pub(super) fn err_with_source(
    context: impl Into<Cow<'static, str>>,
    source: impl Into<BoxError>,
) -> AppError {
    AppError::context(context, source)
}
pub(super) fn try_string_with_capacity(capacity: usize, context: &'static str) -> Result<String> {
    let mut value = String::new();
    value
        .try_reserve_exact(capacity)
        .map_err(|source| err_with_source(context, source))?;
    Ok(value)
}
pub(super) fn try_vec_with_capacity<T>(capacity: usize, context: &'static str) -> Result<Vec<T>> {
    let mut value = Vec::new();
    value
        .try_reserve_exact(capacity)
        .map_err(|source| err_with_source(context, source))?;
    Ok(value)
}
pub(super) fn path_context_message(label: &str, path: &Path) -> String {
    format!("{label}: {}", path.display())
}
pub(super) fn append_fmt(target: &mut String, args: fmt::Arguments<'_>) {
    assert!(
        target.write_fmt(args).is_ok(),
        "String formatting must be infallible"
    );
}
pub(super) const fn terminal_safe<T>(value: &T) -> impl Display + '_
where
    T: Display + ?Sized,
{
    fmt::from_fn(move |formatter| write!(&mut ControlEscapingWriter(formatter), "{value}"))
}
fn write_control_escaped(formatter: &mut fmt::Formatter<'_>, text: &str) -> fmt::Result {
    for character in text.chars() {
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
            for escaped in character.escape_debug() {
                formatter.write_char(escaped)?;
            }
        } else {
            formatter.write_char(character)?;
        }
    }
    Ok(())
}
