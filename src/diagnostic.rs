use alloc::borrow::Cow;
use core::{
    error::Error,
    fmt::{self, Display, Write as _},
    result::Result as CoreResult,
};
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
    let mut out = String::new();
    if out.try_reserve_exact(prefix.len()).is_err() {
        return format!("{prefix}{detail}");
    }
    out.push_str(prefix);
    if write!(&mut out, "{detail}").is_err() {
        return out;
    }
    out
}
pub fn path_context_message(label: &str, path: &Path) -> String {
    let fallback = || format!("{label}: {}", path.display());
    let Some(capacity) = label.len().checked_add(": ".len()) else {
        return fallback();
    };
    let mut out = String::new();
    if out.try_reserve_exact(capacity).is_err() {
        return fallback();
    }
    out.push_str(label);
    out.push_str(": ");
    if write!(&mut out, "{}", path.display()).is_err() {
        return out;
    }
    out
}
pub fn path_pair_context_message(label: &str, from: &Path, to: &Path) -> String {
    let fallback = || format!("{label}: {} -> {}", from.display(), to.display());
    let Some(capacity) = label
        .len()
        .checked_add(": ".len())
        .and_then(|value| value.checked_add(" -> ".len()))
    else {
        return fallback();
    };
    let mut out = String::new();
    if out.try_reserve_exact(capacity).is_err() {
        return fallback();
    }
    out.push_str(label);
    out.push_str(": ");
    if write!(&mut out, "{}", from.display()).is_err() {
        return out;
    }
    out.push_str(" -> ");
    if write!(&mut out, "{}", to.display()).is_err() {
        return out;
    }
    out
}
