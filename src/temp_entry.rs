#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::fs::Permissions;
use std::{
    fs::{File, Metadata, OpenOptions},
    io,
    path::Path,
};
cfg_select! {
    target_os = "windows" => {
        use core::ffi::c_void;
        use std::os::windows::{
            fs::{MetadataExt as _, OpenOptionsExt as _},
            io::AsRawHandle as _,
        };
        const FILE_ATTRIBUTE_REPARSE_POINT: u32 = 0x0000_0400;
        const FILE_FLAG_OPEN_REPARSE_POINT: u32 = 0x0020_0000;
    }
    any(target_os = "linux", target_os = "macos") => {
        use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
    }
    _ => {}
}
#[cfg(target_os = "linux")]
const OPEN_NOFOLLOW: i32 = 0x0002_0000;
#[cfg(target_os = "macos")]
const OPEN_NOFOLLOW: i32 = 0x0000_0100;
#[cfg(target_os = "windows")]
const _: () = assert!(
    size_of::<ByHandleFileInformation>() == 52,
    "Windows BY_HANDLE_FILE_INFORMATION size mismatch"
);
#[cfg(target_os = "windows")]
#[repr(C)]
#[derive(Default)]
struct ByHandleFileInformation {
    file_attributes: u32,
    creation_time_low: u32,
    creation_time_high: u32,
    last_access_time_low: u32,
    last_access_time_high: u32,
    last_write_time_low: u32,
    last_write_time_high: u32,
    volume_serial_number: u32,
    file_size_high: u32,
    file_size_low: u32,
    number_of_links: u32,
    file_index_high: u32,
    file_index_low: u32,
}
#[cfg(target_os = "windows")]
unsafe extern "system" {
    #[link_name = "GetFileInformationByHandle"]
    fn get_file_information_by_handle(
        file: *mut c_void,
        information: *mut ByHandleFileInformation,
    ) -> i32;
}
#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) struct FileIdentity {
    index: u64,
    volume: u64,
}
pub(crate) struct ValidatedFile {
    pub file: File,
    pub identity: FileIdentity,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    pub permissions: Permissions,
}
pub(crate) fn configure_no_follow(options: &mut OpenOptions) {
    cfg_select! {
        target_os = "windows" => {
            options.custom_flags(FILE_FLAG_OPEN_REPARSE_POINT);
        }
        any(target_os = "linux", target_os = "macos") => {
            options.custom_flags(OPEN_NOFOLLOW);
        }
        _ => {
            compile_error!("Secure file opening supports only Windows, Linux, and macOS.");
        }
    }
}
#[cfg(target_os = "windows")]
pub(crate) fn configure_replaceable_file(options: &mut OpenOptions) {
    const FILE_SHARE_READ_WRITE_DELETE: u32 = 0x0000_0007;
    options.share_mode(FILE_SHARE_READ_WRITE_DELETE);
}
pub(crate) fn open_regular(path: &Path, writable: bool) -> io::Result<ValidatedFile> {
    let mut options = File::options();
    options.read(true);
    if writable {
        options.write(true);
        #[cfg(target_os = "windows")]
        configure_replaceable_file(&mut options);
    }
    configure_no_follow(&mut options);
    let file = options.open(path)?;
    validate_open_file(file)
}
pub(crate) fn validate_open_file(file: File) -> io::Result<ValidatedFile> {
    let validation = validate_regular_file(&file)?;
    Ok(ValidatedFile {
        file,
        identity: validation.1,
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        permissions: validation.0.permissions(),
    })
}
pub(crate) fn validate_regular_file(file: &File) -> io::Result<(Metadata, FileIdentity)> {
    let metadata = file.metadata()?;
    #[cfg(target_os = "windows")]
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "리파스 포인트는 허용되지 않습니다.",
        ));
    }
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "경로는 일반 파일이어야 합니다.",
        ));
    }
    let (link_count, identity) = cfg_select! {
        target_os = "windows" => {{
            let mut information = ByHandleFileInformation::default();
            // SAFETY: information is a writable BY_HANDLE_FILE_INFORMATION buffer and file is open.
            let status = unsafe {
                get_file_information_by_handle(
                    file.as_raw_handle(),
                    &raw mut information,
                )
            };
            if status == 0_i32 {
                return Err(io::Error::last_os_error());
            }
            let index = u64::from(information.file_index_high)
                .strict_shl(32)
                | u64::from(information.file_index_low);
            (u64::from(information.number_of_links), FileIdentity {
                index,
                volume: u64::from(information.volume_serial_number),
            })
        }}
        any(target_os = "linux", target_os = "macos") => {
            (metadata.nlink(), FileIdentity {
                index: metadata.ino(),
                volume: metadata.dev(),
            })
        }
        _ => {
            compile_error!("File identity supports only Windows, Linux, and macOS.")
        }
    };
    if link_count != 1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "파일의 하드 링크 수는 1이어야 합니다.",
        ));
    }
    Ok((metadata, identity))
}
