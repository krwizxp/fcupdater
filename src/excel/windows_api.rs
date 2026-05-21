use core::{
    ffi::c_void,
    ptr::{null, null_mut},
    time::Duration,
};
use std::thread;
const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
const REPLACEFILE_WRITE_THROUGH: u32 = 0x0000_0001;
const WINDOWS_FILE_PROMOTION_ATTEMPTS: u32 = 5;
const WINDOWS_FILE_PROMOTION_RETRY_DELAY: Duration = Duration::from_millis(50);
pub(super) struct WindowsFileApi;
#[derive(Clone, Copy)]
pub(super) enum WindowsFileOperation {
    MoveReplaceWriteThrough,
    ReplaceWriteThrough,
}
unsafe extern "system" {
    fn GetLastError() -> u32;
    fn ReplaceFileW(
        replaced_file_name: *const u16,
        replacement_file_name: *const u16,
        backup_file_name: *const u16,
        replace_flags: u32,
        exclude: *mut c_void,
        reserved: *mut c_void,
    ) -> i32;
    fn MoveFileExW(existing_file_name: *const u16, new_file_name: *const u16, flags: u32) -> i32;
}
impl WindowsFileApi {
    pub(super) fn run_with_retry(
        operation: WindowsFileOperation,
        primary_file_name: &[u16],
        secondary_file_name: &[u16],
    ) -> Option<u32> {
        let mut last_code = 0;
        for attempt in 1..=WINDOWS_FILE_PROMOTION_ATTEMPTS {
            let succeeded = match operation {
                WindowsFileOperation::MoveReplaceWriteThrough => {
                    // SAFETY: both path buffers are NUL-terminated and valid for the duration of the call.
                    unsafe {
                        MoveFileExW(
                            primary_file_name.as_ptr(),
                            secondary_file_name.as_ptr(),
                            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
                        ) != 0_i32
                    }
                }
                WindowsFileOperation::ReplaceWriteThrough => {
                    // SAFETY: both path buffers are NUL-terminated and live across the call; optional pointers are intentionally null.
                    unsafe {
                        ReplaceFileW(
                            primary_file_name.as_ptr(),
                            secondary_file_name.as_ptr(),
                            null(),
                            REPLACEFILE_WRITE_THROUGH,
                            null_mut(),
                            null_mut(),
                        ) != 0_i32
                    }
                }
            };
            if succeeded {
                return None;
            }
            // SAFETY: GetLastError has no preconditions and is read immediately after the failed Win32 call.
            last_code = unsafe { GetLastError() };
            if attempt < WINDOWS_FILE_PROMOTION_ATTEMPTS {
                thread::sleep(WINDOWS_FILE_PROMOTION_RETRY_DELAY);
            }
        }
        Some(last_code)
    }
}
