pub(super) use self::source_reader::{FuelValues, SourceReader, SourceRecord};
use crate::diagnostic::{Result, str_cap};
mod source_reader;
pub(super) mod writer;
pub(super) mod xlsx_container;
mod xml;
mod zip_archive;
macro_rules! xp {
    ($n:expr, $r:ident) => {
        ($n, PartRole::$r, None)
    };
    ($n:expr, $r:ident, s $t:expr) => {
        (
            $n,
            PartRole::$r,
            Some(concat!(
                "application/vnd.openxmlformats-officedocument.spreadsheetml.",
                $t,
                "+xml"
            )),
        )
    };
    ($n:expr, $r:ident, o $t:expr) => {
        (
            $n,
            PartRole::$r,
            Some(concat!(
                "application/vnd.openxmlformats-officedocument.",
                $t,
                "+xml"
            )),
        )
    };
    ($n:expr, $r:ident, p $t:expr) => {
        (
            $n,
            PartRole::$r,
            Some(concat!(
                "application/vnd.openxmlformats-package.",
                $t,
                "+xml"
            )),
        )
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
const XLSX_PARTS: [(&str, PartRole, Option<&str>); 16] = [
    xp!("[Content_Types].xml", Required),
    xp!("_rels/.rels", Required),
    xp!("xl/workbook.xml", Required, s "sheet.main"),
    xp!("xl/_rels/workbook.xml.rels", Required),
    xp!("xl/worksheets/sheet1.xml", Required, s "worksheet"),
    xp!("xl/worksheets/sheet2.xml", Required, s "worksheet"),
    xp!("xl/theme/theme1.xml", Required, o "theme"),
    xp!("xl/styles.xml", Required, s "styles"),
    xp!("xl/sharedStrings.xml", Required, s "sharedStrings"),
    xp!("docProps/thumbnail.emf", InputOnly),
    xp!(CALC_CHAIN_PATH, InputOnly, s "calcChain"),
    xp!("docProps/core.xml", Required, p "core-properties"),
    xp!("docProps/app.xml", Required, o "extended-properties"),
    xp!("docProps/custom.xml", InputOnly, o "custom-properties"),
    xp!("xl/worksheets/_rels/sheet1.xml.rels", InputOnly),
    xp!("xl/drawings/drawing1.xml", InputOnly, o "drawing"),
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
fn canonical_excel_fragment(source: &str) -> Result<String> {
    let trimmed = source.trim_suffix('\n');
    let Some((first_line, remainder)) = trimmed.split_once('\n') else {
        return copy_text(trimmed);
    };
    let mut output = str_cap(trimmed.len().strict_add(1), "텍스트 복사")?;
    output.extend([first_line, "\r\n", remainder]);
    Ok(output)
}
fn copy_text(text: &str) -> Result<String> {
    let mut out = str_cap(text.len(), "텍스트 복사")?;
    out.push_str(text);
    Ok(out)
}
fn read_le_chunk<const N: usize>(bytes: &[u8], offset: usize) -> Option<[u8; N]> {
    bytes.get(offset..)?.first_chunk::<N>().copied()
}
