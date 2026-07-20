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
struct ControlEscapingWriter<'formatter, 'output> {
    formatter: &'formatter mut fmt::Formatter<'output>,
}
struct TerminalSafeText<'text> {
    text: &'text str,
}
impl AppError {
    pub(super) fn context(
        context: impl Into<Cow<'static, str>>,
        source: impl Into<BoxError>,
    ) -> Self {
        Self {
            message: context.into(),
            source: Some(source.into()),
        }
    }
    pub(super) fn message(message: impl Into<Cow<'static, str>>) -> Self {
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
            write!(&mut ControlEscapingWriter { formatter: f }, "{source}")?;
        }
        Ok(())
    }
}
impl fmt::Write for ControlEscapingWriter<'_, '_> {
    fn write_str(&mut self, s: &str) -> fmt::Result {
        write_control_escaped(self.formatter, s)
    }
}
impl Display for TerminalSafeText<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write_control_escaped(f, self.text)
    }
}
impl fmt::Debug for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(self, f)
    }
}
impl Error for AppError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|source| {
            let source_ref: &(dyn Error + 'static) = source;
            source_ref
        })
    }
}
impl From<Cow<'static, str>> for AppError {
    fn from(value: Cow<'static, str>) -> Self {
        Self::message(value)
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
pub(super) fn err<M>(msg: M) -> AppError
where
    M: Into<Cow<'static, str>>,
{
    AppError::message(msg)
}
pub(super) fn err_with_source<M, E>(context: M, source: E) -> AppError
where
    M: Into<Cow<'static, str>>,
    E: Into<BoxError>,
{
    AppError::context(context, source)
}
pub(super) fn prefixed_message<D>(prefix: &str, detail: D) -> String
where
    D: Display,
{
    format!("{prefix}{detail}")
}
pub(super) fn path_context_message(label: &str, path: &Path) -> String {
    format!("{label}: {}", path.display())
}
pub(super) fn path_pair_context_message(label: &str, from: &Path, to: &Path) -> String {
    format!("{label}: {} -> {}", from.display(), to.display())
}
pub(super) const fn terminal_safe(text: &str) -> impl Display + '_ {
    TerminalSafeText { text }
}
fn write_control_escaped(formatter: &mut fmt::Formatter<'_>, text: &str) -> fmt::Result {
    for character in text.chars() {
        if character.is_control() {
            for escaped in character.escape_debug() {
                formatter.write_char(escaped)?;
            }
        } else {
            formatter.write_char(character)?;
        }
    }
    Ok(())
}
