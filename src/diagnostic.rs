use alloc::borrow::Cow;
use core::{error::Error, fmt, fmt::Display, result::Result as CoreResult};
use std::{io::Error as IoError, path::Path};
type BoxError = Box<dyn Error + Send + Sync>;
pub type Result<T> = CoreResult<T, AppError>;
pub struct AppError {
    message: Cow<'static, str>,
    source: Option<BoxError>,
}
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
    pub(super) fn prepend_context<M>(self, context: M) -> Self
    where
        M: Into<Cow<'static, str>>,
    {
        Self {
            message: Cow::Owned(format!("{}: {}", context.into(), self.message)),
            source: self.source,
        }
    }
}
impl Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(source) = self.source.as_ref() {
            write!(f, "{}: {source}", self.message)
        } else {
            f.write_str(self.message.as_ref())
        }
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
pub fn err<M>(msg: M) -> AppError
where
    M: Into<Cow<'static, str>>,
{
    AppError::message(msg)
}
pub fn err_with_source<M, E>(context: M, source: E) -> AppError
where
    M: Into<Cow<'static, str>>,
    E: Into<BoxError>,
{
    AppError::context(context, source)
}
pub fn prefixed_message<D>(prefix: &str, detail: D) -> String
where
    D: Display,
{
    let detail_text = detail.to_string();
    let Some(capacity) = prefix.len().checked_add(detail_text.len()) else {
        return format!("{prefix}{detail_text}");
    };
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("{prefix}{detail_text}");
    }
    out.push_str(prefix);
    out.push_str(&detail_text);
    out
}
pub fn path_context_message(label: &str, path: &Path) -> String {
    let path_text = path.display().to_string();
    let Some(capacity) = label
        .len()
        .checked_add(": ".len())
        .and_then(|value| value.checked_add(path_text.len()))
    else {
        return format!("{label}: {path_text}");
    };
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("{label}: {path_text}");
    }
    out.push_str(label);
    out.push_str(": ");
    out.push_str(&path_text);
    out
}
pub fn path_pair_context_message(label: &str, from: &Path, to: &Path) -> String {
    let from_text = from.display().to_string();
    let to_text = to.display().to_string();
    let Some(capacity) = label
        .len()
        .checked_add(": ".len())
        .and_then(|value| value.checked_add(from_text.len()))
        .and_then(|value| value.checked_add(" -> ".len()))
        .and_then(|value| value.checked_add(to_text.len()))
    else {
        return format!("{label}: {from_text} -> {to_text}");
    };
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("{label}: {from_text} -> {to_text}");
    }
    out.push_str(label);
    out.push_str(": ");
    out.push_str(&from_text);
    out.push_str(" -> ");
    out.push_str(&to_text);
    out
}
pub fn path_source_message<D>(label: &str, path: &Path, source: D) -> String
where
    D: Display,
{
    let path_text = path.display().to_string();
    let source_text = source.to_string();
    let Some(capacity) = label
        .len()
        .checked_add(": ".len())
        .and_then(|value| value.checked_add(path_text.len()))
        .and_then(|value| value.checked_add(" ()".len()))
        .and_then(|value| value.checked_add(source_text.len()))
    else {
        return format!("{label}: {path_text} ({source_text})");
    };
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("{label}: {path_text} ({source_text})");
    }
    out.push_str(label);
    out.push_str(": ");
    out.push_str(&path_text);
    out.push_str(" (");
    out.push_str(&source_text);
    out.push(')');
    out
}
