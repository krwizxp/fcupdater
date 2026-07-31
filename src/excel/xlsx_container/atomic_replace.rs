use core::{error::Error, fmt};
use std::{io, path::Path};
cfg_select! {
    target_os = "windows" => {
        use core::{ffi::c_void, ptr::null};
        use std::{
            fs,
            os::windows::ffi::OsStrExt as _,
            path::{Component, Prefix, absolute},
        };
    }
    any(target_os = "linux", target_os = "macos") => {
        use alloc::ffi::CString;
        use core::ffi::{c_char, c_int, c_uint};
        use std::os::unix::ffi::OsStrExt as _;
    }
    _ => {}
}
cfg_select! {
    target_os = "windows" => {
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn ReplaceFileW(
                replaced_file_name: *const u16,
                replacement_file_name: *const u16,
                backup_file_name: *const u16,
                replace_flags: u32,
                exclude: *const c_void,
                reserved: *const c_void,
            ) -> i32;
        }
    }
    target_os = "linux" => {
        unsafe extern "C" {
            fn renameat2(
                old_dir_fd: c_int,
                old_path: *const c_char,
                new_dir_fd: c_int,
                new_path: *const c_char,
                flags: c_uint,
            ) -> c_int;
        }
    }
    target_os = "macos" => {
        unsafe extern "C" {
            fn renamex_np(
                old_path: *const c_char,
                new_path: *const c_char,
                flags: c_uint,
            ) -> c_int;
        }
    }
    _ => {}
}
cfg_select! {
    target_os = "windows" => {
        const ERROR_UNABLE_TO_MOVE_REPLACEMENT_2: i32 = 1177;
    }
    target_os = "linux" => {
        const AT_FDCWD: c_int = -100;
        const RENAME_EXCHANGE: c_uint = 2;
    }
    target_os = "macos" => {
        const RENAME_SWAP: c_uint = 2;
    }
    _ => {}
}
#[derive(Debug)]
pub(super) struct ReplaceFailure {
    replace: io::Error,
    #[cfg(target_os = "windows")]
    restore: Option<io::Error>,
}
#[derive(Debug)]
pub(super) enum ReplaceFilesError {
    Failed(ReplaceFailure),
    #[cfg(target_os = "windows")]
    RecoveryRequired(ReplaceFailure),
    #[cfg(target_os = "windows")]
    Restored(ReplaceFailure),
}
impl ReplaceFailure {
    const fn new(replace: io::Error) -> Self {
        Self {
            replace,
            #[cfg(target_os = "windows")]
            restore: None,
        }
    }
}
impl fmt::Display for ReplaceFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(target_os = "windows")]
        if let Some(restore) = self.restore.as_ref() {
            return write!(
                f,
                "{}; 원본 대상 파일 자동 복원도 실패했습니다: {restore}",
                self.replace,
            );
        }
        fmt::Display::fmt(&self.replace, f)
    }
}
impl Error for ReplaceFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.replace)
    }
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn path_to_c_string(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|source| io::Error::new(io::ErrorKind::InvalidInput, source))
}
cfg_select! {
    target_os = "windows" => {
        fn path_to_wide(path: &Path) -> io::Result<Vec<u16>> {
            let absolute = absolute(path)?;
            let (extended_prefix, skipped_units) = match absolute.components().next() {
                Some(Component::Prefix(component)) => match component.kind() {
                    Prefix::Disk(_) => (r"\\?\", 0_usize),
                    Prefix::UNC(_, _) => (r"\\?\UNC\", 2_usize),
                    Prefix::Verbatim(_)
                    | Prefix::VerbatimUNC(_, _)
                    | Prefix::VerbatimDisk(_)
                    | Prefix::DeviceNS(_) => ("", 0_usize),
                },
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Windows file path is not absolute",
                    ));
                }
            };
            let capacity = extended_prefix
                .len()
                .checked_add(absolute.as_os_str().len())
                .and_then(|len| len.checked_add(1))
                .ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Windows file path length overflow",
                    )
                })?;
            let mut wide = Vec::new();
            wide.try_reserve_exact(capacity).map_err(io::Error::other)?;
            wide.extend(extended_prefix.encode_utf16());
            let mut path_wide = absolute.as_os_str().encode_wide();
            for _ in 0_usize..skipped_units {
                let Some(unit) = path_wide.next() else {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Windows UNC path is invalid",
                    ));
                };
                if unit == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Windows file path contains a NUL character",
                    ));
                }
            }
            for unit in path_wide {
                if unit == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "Windows file path contains a NUL character",
                    ));
                }
                wide.push(unit);
            }
            wide.push(0);
            Ok(wide)
        }
        pub(super) fn replace_files(
            target: &Path,
            replacement: &Path,
            backup: &Path,
            rollback: bool,
        ) -> Result<(), ReplaceFilesError> {
            let (incoming, backup_output) = if rollback {
                (backup, replacement)
            } else {
                (replacement, backup)
            };
            let target_wide = path_to_wide(target)
                .map_err(ReplaceFailure::new)
                .map_err(ReplaceFilesError::Failed)?;
            let incoming_wide = path_to_wide(incoming)
                .map_err(ReplaceFailure::new)
                .map_err(ReplaceFilesError::Failed)?;
            let backup_output_wide = path_to_wide(backup_output)
                .map_err(ReplaceFailure::new)
                .map_err(ReplaceFilesError::Failed)?;
            // SAFETY: all three paths are valid NUL-terminated UTF-16 buffers and reserved pointers
            // are null as required by ReplaceFileW.
            let status = unsafe {
                ReplaceFileW(
                    target_wide.as_ptr(),
                    incoming_wide.as_ptr(),
                    backup_output_wide.as_ptr(),
                    0,
                    null(),
                    null(),
                )
            };
            if status != 0_i32 {
                return Ok(());
            }
            let replace = io::Error::last_os_error();
            if replace.raw_os_error() != Some(ERROR_UNABLE_TO_MOVE_REPLACEMENT_2) {
                return Err(ReplaceFilesError::Failed(ReplaceFailure::new(replace)));
            }
            let restore_from = if rollback { incoming } else { backup_output };
            match fs::rename(restore_from, target) {
                Ok(()) => Err(ReplaceFilesError::Restored(ReplaceFailure::new(replace))),
                Err(restore) => Err(ReplaceFilesError::RecoveryRequired(ReplaceFailure {
                    replace,
                    restore: Some(restore),
                })),
            }
        }
    }
    any(target_os = "linux", target_os = "macos") => {
        pub(super) fn replace_files(
            target: &Path,
            replacement: &Path,
        ) -> Result<(), ReplaceFilesError> {
            let target_c = path_to_c_string(target)
                .map_err(ReplaceFailure::new)
                .map_err(ReplaceFilesError::Failed)?;
            let replacement_c = path_to_c_string(replacement)
                .map_err(ReplaceFailure::new)
                .map_err(ReplaceFilesError::Failed)?;
            #[cfg(target_os = "linux")]
            let status = {
                // SAFETY: both paths are valid NUL-terminated byte strings and AT_FDCWD selects
                // the current process path resolution context.
                unsafe {
                    renameat2(
                        AT_FDCWD,
                        target_c.as_ptr(),
                        AT_FDCWD,
                        replacement_c.as_ptr(),
                        RENAME_EXCHANGE,
                    )
                }
            };
            #[cfg(target_os = "macos")]
            let status = {
                // SAFETY: both paths are valid NUL-terminated byte strings and RENAME_SWAP
                // requests an atomic exchange of two existing paths.
                unsafe { renamex_np(target_c.as_ptr(), replacement_c.as_ptr(), RENAME_SWAP) }
            };
            if status == 0_i32 {
                Ok(())
            } else {
                Err(ReplaceFilesError::Failed(ReplaceFailure::new(
                    io::Error::last_os_error(),
                )))
            }
        }
    }
    _ => {
        compile_error!("fcupdater atomic replacement supports only Windows, Linux, and macOS.");
    }
}
