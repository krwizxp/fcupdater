use std::path::Path;
mod path_util;
mod source_reader;
pub mod writer;
pub mod xlsx_container;
mod xml;
mod zip_archive;
pub enum SaveVerification {
    Skip,
    Verify,
}
impl SaveVerification {
    pub(in crate::excel) const fn should_verify(&self) -> bool {
        matches!(self, Self::Verify)
    }
}
pub struct SourceReader<'path> {
    pub path: &'path Path,
}
struct ZipArchiveBuilder<'path> {
    archive_path: &'path Path,
    root: &'path Path,
}
struct ZipArchiveExtractor<'path> {
    archive_path: &'path Path,
    unpack_dir: &'path Path,
}
struct SheetInfo {
    name: String,
    path: String,
}
