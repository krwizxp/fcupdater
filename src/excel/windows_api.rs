use core::ffi::c_void;
pub const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
pub const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
pub const REPLACEFILE_WRITE_THROUGH: u32 = 0x0000_0001;
unsafe extern "system" {
    pub fn GetLastError() -> u32;
    pub fn ReplaceFileW(
        replaced_file_name: *const u16,
        replacement_file_name: *const u16,
        backup_file_name: *const u16,
        replace_flags: u32,
        exclude: *mut c_void,
        reserved: *mut c_void,
    ) -> i32;
    pub fn MoveFileExW(
        existing_file_name: *const u16,
        new_file_name: *const u16,
        flags: u32,
    ) -> i32;
}
