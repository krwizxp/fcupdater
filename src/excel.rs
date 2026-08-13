pub(super) use self::source_reader::{FuelValues, SourceReader, SourceRecord};
use crate::diagnostic::{Result, try_string_with_capacity};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::fs::Permissions;
use std::{fs::File, path::Path};
mod source_reader;
pub(super) mod writer;
pub(super) mod xlsx_container;
mod xml;
mod zip_archive;
macro_rules! xlsx_part {
    ($name:expr, $role:ident) => {
        ($name, PartRole::$role, None)
    };
    ($name:expr, $role:ident, $content_type:expr) => {
        ($name, PartRole::$role, Some($content_type))
    };
}
pub(super) const SPREADSHEETML_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/spreadsheetml/2006/main";
pub(super) const CHANGE_LOG_SHEET_NAME: &str = "변경내역";
pub(super) const CHANGE_LOG_SHEET_PATH: &str = "xl/worksheets/sheet2.xml";
pub(super) const MASTER_SHEET_NAME: &str = "유류비";
pub(super) const MASTER_SHEET_PATH: &str = "xl/worksheets/sheet1.xml";
pub(super) const CALC_CHAIN_PATH: &str = "xl/calcChain.xml";
pub(super) const FILTER_DATABASE_REF_PREFIX: &str = "유류비!$A$14:$W$";
pub(super) const MAX_XLSX_PART_BYTES: usize = 64 * 1024 * 1024;
const CT_WORKBOOK: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml";
const CT_WORKSHEET: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml";
const CT_THEME: &str = "application/vnd.openxmlformats-officedocument.theme+xml";
const CT_STYLES: &str = "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml";
const CT_SHARED_STRINGS: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml";
const CT_CORE_PROPERTIES: &str = "application/vnd.openxmlformats-package.core-properties+xml";
const CT_APP_PROPERTIES: &str =
    "application/vnd.openxmlformats-officedocument.extended-properties+xml";
const CT_CALC_CHAIN: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.calcChain+xml";
const CT_CUSTOM_PROPERTIES: &str =
    "application/vnd.openxmlformats-officedocument.custom-properties+xml";
const CT_DRAWING: &str = "application/vnd.openxmlformats-officedocument.drawing+xml";
const XLSX_PARTS: [(&str, PartRole, Option<&str>); 16] = [
    xlsx_part!("[Content_Types].xml", Required),
    xlsx_part!("_rels/.rels", Required),
    xlsx_part!("xl/workbook.xml", Required, CT_WORKBOOK),
    xlsx_part!("xl/_rels/workbook.xml.rels", Required),
    xlsx_part!("xl/worksheets/sheet1.xml", Required, CT_WORKSHEET),
    xlsx_part!("xl/worksheets/sheet2.xml", Required, CT_WORKSHEET),
    xlsx_part!("xl/theme/theme1.xml", Required, CT_THEME),
    xlsx_part!("xl/styles.xml", Required, CT_STYLES),
    xlsx_part!("xl/sharedStrings.xml", Required, CT_SHARED_STRINGS),
    xlsx_part!("docProps/thumbnail.emf", OptionalInput),
    xlsx_part!(CALC_CHAIN_PATH, InputOnly, CT_CALC_CHAIN),
    xlsx_part!("docProps/core.xml", Required, CT_CORE_PROPERTIES),
    xlsx_part!("docProps/app.xml", Required, CT_APP_PROPERTIES),
    xlsx_part!("docProps/custom.xml", InputOnly, CT_CUSTOM_PROPERTIES),
    xlsx_part!("xl/worksheets/_rels/sheet1.xml.rels", InputOnly),
    xlsx_part!("xl/drawings/drawing1.xml", InputOnly, CT_DRAWING),
];
#[derive(Clone, Copy, Eq, PartialEq)]
enum PartRole {
    InputOnly,
    OptionalInput,
    Required,
}
type CanonicalStyleMap = Vec<Option<u32>>;
#[derive(Clone, Copy)]
pub(super) enum SaveVerification {
    Skip,
    Verify,
}
#[derive(Clone, Copy, Eq, PartialEq)]
struct ArchiveFingerprint {
    crc32: u32,
    len: usize,
}
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
fn copy_text(text: &str) -> Result<String> {
    let mut out = try_string_with_capacity(text.len(), "텍스트 복사 메모리 확보 실패")?;
    out.push_str(text);
    Ok(out)
}
