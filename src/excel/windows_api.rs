use core::{
    ffi::c_void,
    ptr::{null, null_mut},
};
const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
const REPLACEFILE_WRITE_THROUGH: u32 = 0x0000_0001;
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
    pub(super) fn last_error() -> u32 {
        // SAFETY: GetLastError has no preconditions.
        unsafe { GetLastError() }
    }
    pub(super) fn run(
        operation: WindowsFileOperation,
        primary_file_name: &[u16],
        secondary_file_name: &[u16],
    ) -> bool {
        match operation {
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
        }
    }
}
