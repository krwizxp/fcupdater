extern crate alloc;
use alloc::borrow::Cow;
use cli::{APP_NAME, APP_VERSION, ParseAction, usage_text};
use core::{error::Error, fmt, fmt::Display, result::Result as CoreResult};
use io_util::write_line_ignored;
pub(crate) use region::{normalize_address_key, parse_region_label};
pub(crate) use sheet_util::{
    add_row_offset, canon_header, parse_i32_str, same_trimmed, shift_row, usize_to_u32,
};
use std::{
    collections::HashMap,
    env,
    ffi::OsStr,
    io::{Error as IoError, Write, stdout},
    path::Path,
};
mod change_log;
mod cli;
mod excel;
mod io_util;
mod master_sheet;
mod region;
mod sheet_util;
mod source_download;
mod update_run;
pub(crate) type BoxError = Box<dyn Error + Send + Sync>;
type Result<T> = CoreResult<T, AppError>;
#[derive(Debug)]
struct SourceRecord {
    address: String,
    brand: String,
    diesel: Option<i32>,
    gasoline: Option<i32>,
    name: String,
    premium: Option<i32>,
    region: String,
    self_yn: String,
}
#[derive(Debug)]
struct AddedStoreRow<'source> {
    record: &'source SourceRecord,
    region: &'source str,
}
#[derive(Debug)]
struct ChangeRow<'source> {
    address: &'source str,
    name: &'source str,
    new_diesel: Option<i32>,
    new_gasoline: Option<i32>,
    new_premium: Option<i32>,
    old_diesel: Option<i32>,
    old_gasoline: Option<i32>,
    old_premium: Option<i32>,
    reason: String,
    region: String,
}
#[derive(Debug)]
struct StoreRow {
    address: String,
    diesel: Option<i32>,
    gasoline: Option<i32>,
    name: String,
    premium: Option<i32>,
    region: String,
}
pub(crate) struct AppError {
    message: Cow<'static, str>,
    source: Option<BoxError>,
}
struct ChangeLogUpdater<'sheet, 'shared, 'data, 'source> {
    added: &'data [AddedStoreRow<'source>],
    changes: &'data [ChangeRow<'source>],
    deleted: &'data [StoreRow],
    shared_string_table: &'shared [String],
    today: &'data str,
    worksheet: &'sheet mut excel::writer::Worksheet,
}
struct MasterSheetUpdater<'source> {
    source_index: &'source HashMap<String, SourceRecord>,
}
struct SourceDownload<'dir, 'out, W: Write + ?Sized> {
    dir: &'dir Path,
    out: &'out mut W,
}
struct UpdateRunContext<'out> {
    out: &'out mut dyn Write,
}
impl AppError {
    fn context(
        context: impl Into<Cow<'static, str>>,
        source: impl Error + Send + Sync + 'static,
    ) -> Self {
        Self {
            message: context.into(),
            source: Some(Box::new(source)),
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
fn err(msg: impl Into<Cow<'static, str>>) -> AppError {
    AppError::message(msg)
}
fn err_with_source(
    context: impl Into<Cow<'static, str>>,
    source: impl Error + Send + Sync + 'static,
) -> AppError {
    AppError::context(context, source)
}
fn prefixed_message(prefix: &str, detail: impl Display) -> String {
    let detail_text = detail.to_string();
    let capacity = prefix.len().saturating_add(detail_text.len());
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("{prefix}{detail_text}");
    }
    out.push_str(prefix);
    out.push_str(&detail_text);
    out
}
fn append_error_text(error_text: &str, detail_text: &str) -> String {
    let capacity = error_text
        .len()
        .saturating_add("; ".len())
        .saturating_add(detail_text.len());
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("{error_text}; {detail_text}");
    }
    out.push_str(error_text);
    out.push_str("; ");
    out.push_str(detail_text);
    out
}
fn path_source_message(label: &str, path: &Path, source: impl Display) -> String {
    let path_text = path.display().to_string();
    let source_text = source.to_string();
    let capacity = label
        .len()
        .saturating_add(": ".len())
        .saturating_add(path_text.len())
        .saturating_add(" ()".len())
        .saturating_add(source_text.len());
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
fn path_pair_source_message(label: &str, from: &Path, to: &Path, source: impl Display) -> String {
    let from_text = from.display().to_string();
    let to_text = to.display().to_string();
    let source_text = source.to_string();
    let capacity = label
        .len()
        .saturating_add(": ".len())
        .saturating_add(from_text.len())
        .saturating_add(" -> ".len())
        .saturating_add(to_text.len())
        .saturating_add(" ()".len())
        .saturating_add(source_text.len());
    let mut out = String::new();
    if out.try_reserve(capacity).is_err() {
        return format!("{label}: {from_text} -> {to_text} ({source_text})");
    }
    out.push_str(label);
    out.push_str(": ");
    out.push_str(&from_text);
    out.push_str(" -> ");
    out.push_str(&to_text);
    out.push_str(" (");
    out.push_str(&source_text);
    out.push(')');
    out
}
fn main() -> Result<()> {
    let mut out = stdout();
    let mut raw_args = env::args_os().skip(1);
    let first_arg = raw_args.next();
    let has_extra = raw_args.next().is_some();
    let action = match first_arg {
        None => ParseAction::Run,
        Some(token) => {
            if has_extra {
                let usage = usage_text();
                return Err(err(format!(
                    "알 수 없는 옵션: {}\n\n{usage}",
                    token.to_string_lossy()
                )));
            }
            if token == OsStr::new("-h") || token == OsStr::new("--help") {
                ParseAction::Help(usage_text())
            } else if token == OsStr::new("--version") {
                ParseAction::Version(format!("{APP_NAME} {APP_VERSION}"))
            } else {
                let usage = usage_text();
                return Err(err(format!(
                    "알 수 없는 옵션: {}\n\n{usage}",
                    token.to_string_lossy()
                )));
            }
        }
    };
    match action {
        ParseAction::Run => {
            let mut context = UpdateRunContext { out: &mut out };
            context.run_update()
        }
        ParseAction::Help(text) | ParseAction::Version(text) => {
            write_line_ignored(&mut out, format_args!("{text}"));
            Ok(())
        }
    }
}
