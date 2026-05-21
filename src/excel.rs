pub mod ooxml;
mod path_util;
pub mod source_reader;
cfg_select! {
    windows => {
        pub mod windows_api;
    }
    _ => {}
}
pub mod writer;
pub mod xlsx_container;
mod xml;
mod zip_archive;
