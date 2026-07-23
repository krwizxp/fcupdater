pub(super) use self::source_reader::{FuelValues, SourceReader, SourceRecord, SourceRecordRef};
use crate::diagnostic::{Result, err_with_source};
use core::range::Range;
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::fs::Permissions;
use std::{fs::File, path::Path};
mod source_reader;
pub(super) mod writer;
pub(super) mod xlsx_container;
mod xml;
mod zip_archive;
pub(super) const SPREADSHEETML_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
pub(super) const CHANGE_LOG_SHEET_NAME: &str = "변경내역";
pub(super) const CHANGE_LOG_SHEET_PATH: &str = "xl/worksheets/sheet2.xml";
pub(super) const MASTER_SHEET_NAME: &str = "유류비";
pub(super) const MASTER_SHEET_PATH: &str = "xl/worksheets/sheet1.xml";
const XLSX_PART_NAMES: [&str; 14] = [
    "docProps/custom.xml",
    "docProps/core.xml",
    "docProps/app.xml",
    "xl/worksheets/sheet1.xml",
    "xl/worksheets/_rels/sheet1.xml.rels",
    "xl/drawings/drawing1.xml",
    "xl/styles.xml",
    "xl/_rels/workbook.xml.rels",
    "xl/workbook.xml",
    "xl/theme/theme1.xml",
    "_rels/.rels",
    "[Content_Types].xml",
    "xl/sharedStrings.xml",
    "xl/worksheets/sheet2.xml",
];
#[derive(Clone, Copy)]
pub(super) enum SaveVerification {
    Skip,
    Verify,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ArchiveFingerprint {
    crc32: u32,
    len: usize,
}
#[derive(Debug)]
struct PackagePart {
    bytes: Vec<u8>,
    central_record: Range<usize>,
    changed: bool,
    local_record: Range<usize>,
    name: &'static str,
}
struct ZipArchiveBuilder<'part, 'path> {
    archive_path: &'path Path,
    file: File,
    parts: &'part [PackagePart],
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    permissions: Permissions,
    source_bytes: &'part [u8],
}
struct ZipPackageReader<'path> {
    archive_file: File,
    archive_path: &'path Path,
}
fn copy_text(text: &str, context: &str) -> Result<String> {
    let mut out = String::new();
    out.try_reserve_exact(text.len())
        .map_err(|source| err_with_source(format!("{context} 메모리 확보 실패"), source))?;
    out.push_str(text);
    Ok(out)
}
