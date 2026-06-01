use std::path::Path;
mod ooxml;
mod path_util;
pub mod source_reader;
pub mod writer;
pub mod xlsx_container;
mod xml;
mod zip_archive;
struct ZipArchiveBuilder<'path> {
    archive_path: &'path Path,
    root: &'path Path,
}
struct ZipArchiveExtractor<'path> {
    archive_path: &'path Path,
    unpack_dir: &'path Path,
}
