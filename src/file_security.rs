use std::{
    fs::{File, Metadata, OpenOptions},
    io,
};
cfg_select! {
    target_os = "windows" => {
        use core::{ffi::c_void, mem::size_of};
        use std::os::windows::{
            fs::{MetadataExt as _, OpenOptionsExt as _},
            io::AsRawHandle as _,
        };
        mod sys;
    }
    any(target_os = "linux", target_os = "macos") => {
        use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
    }
    _ => {
        compile_error!("Secure file opening supports only Windows, Linux, and macOS.");
    }
}
cfg_select! {
    target_os = "linux" => {
        const OPEN_NOFOLLOW_FLAG: i32 = 0x2_0000;
    }
    target_os = "macos" => {
        const OPEN_NOFOLLOW_FLAG: i32 = 0x0100;
    }
    target_os = "windows" => {
        const FILE_ATTRIBUTE_REPARSE_POINT_FLAG: u32 = 0x0000_0400;
        const FILE_FLAG_OPEN_REPARSE_POINT_FLAG: u32 = 0x0020_0000;
        const FILE_STANDARD_INFO_CLASS: i32 = 1;
        const FILE_STANDARD_INFO_SIZE: u32 = 24;
    }
}
#[cfg(target_os = "windows")]
const _: () = assert!(
    size_of::<FileStandardInfo>() == 24,
    "Windows FILE_STANDARD_INFO size mismatch"
);
#[cfg(target_os = "windows")]
#[repr(C)]
#[derive(Default)]
struct FileStandardInfo {
    allocation_size: i64,
    end_of_file: i64,
    number_of_links: u32,
    delete_pending: u8,
    directory: u8,
}
pub(crate) fn apply_no_follow(options: &mut OpenOptions) {
    cfg_select! {
        target_os = "windows" => {
            options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT_FLAG);
        }
        any(target_os = "linux", target_os = "macos") => {
            options.custom_flags(OPEN_NOFOLLOW_FLAG);
        }
    }
}
fn invalid_file_path(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}
pub(crate) fn validate_regular_file(file: &File) -> io::Result<Metadata> {
    let metadata = file.metadata()?;
    #[cfg(target_os = "windows")]
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT_FLAG != 0 {
        return Err(invalid_file_path(
            "파일은 일반 파일이어야 하며 리파스 포인트는 허용되지 않습니다.",
        ));
    }
    if !metadata.is_file() {
        return Err(invalid_file_path("경로는 일반 파일이어야 합니다."));
    }
    let link_count = cfg_select! {
        target_os = "windows" => {{
            let mut standard_info = FileStandardInfo::default();
            // SAFETY: standard_info is a valid FILE_STANDARD_INFO buffer for the borrowed file handle.
            let result = unsafe {
                sys::get_file_information_by_handle_ex(
                    file.as_raw_handle(),
                    FILE_STANDARD_INFO_CLASS,
                    (&raw mut standard_info).cast::<c_void>(),
                    FILE_STANDARD_INFO_SIZE,
                )
            };
            if result == 0_i32 {
                return Err(io::Error::last_os_error());
            }
            u64::from(standard_info.number_of_links)
        }}
        any(target_os = "linux", target_os = "macos") => {
            metadata.nlink()
        }
    };
    if link_count != 1 {
        return Err(invalid_file_path("파일의 하드 링크 수는 1이어야 합니다."));
    }
    Ok(metadata)
}
