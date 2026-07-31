pub(super) use self::source_reader::{FuelValues, SourceReader, SourceRecord};
use crate::diagnostic::{Result, err_with_source};
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
pub(super) const CALC_CHAIN_PATH: &str = "xl/calcChain.xml";
const XLSX_PARTS: [(&str, XlsxPartRole); 16] = [
    ("[Content_Types].xml", XlsxPartRole::Required),
    ("_rels/.rels", XlsxPartRole::Required),
    ("xl/workbook.xml", XlsxPartRole::Required),
    ("xl/_rels/workbook.xml.rels", XlsxPartRole::Required),
    ("xl/worksheets/sheet1.xml", XlsxPartRole::Required),
    ("xl/worksheets/sheet2.xml", XlsxPartRole::Required),
    ("xl/theme/theme1.xml", XlsxPartRole::Required),
    ("xl/styles.xml", XlsxPartRole::Required),
    ("xl/sharedStrings.xml", XlsxPartRole::Required),
    ("docProps/thumbnail.emf", XlsxPartRole::OptionalInput),
    (CALC_CHAIN_PATH, XlsxPartRole::OptionalInput),
    ("docProps/core.xml", XlsxPartRole::Required),
    ("docProps/app.xml", XlsxPartRole::Required),
    ("docProps/custom.xml", XlsxPartRole::InputOnly),
    (
        "xl/worksheets/_rels/sheet1.xml.rels",
        XlsxPartRole::InputOnly,
    ),
    ("xl/drawings/drawing1.xml", XlsxPartRole::InputOnly),
];
#[derive(Clone, Copy, Eq, PartialEq)]
enum XlsxPartRole {
    InputOnly,
    OptionalInput,
    Required,
}
#[derive(Debug)]
struct CanonicalStyleMap {
    entries: Vec<Option<u32>>,
}
impl CanonicalStyleMap {
    fn get(&self, style: u32) -> Option<u32> {
        usize::try_from(style)
            .ok()
            .and_then(|index| self.entries.get(index))
            .copied()
            .flatten()
    }
}
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
    name: &'static str,
}
struct ZipArchiveBuilder<'part, 'path> {
    archive_path: &'path Path,
    file: &'path mut File,
    parts: &'part [PackagePart],
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    permissions: Permissions,
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
