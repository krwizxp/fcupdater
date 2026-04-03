use core::ffi::c_void;
pub const MB_ERR_INVALID_CHARS: u32 = 0x0000_0008;
pub const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
pub const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
pub const REPLACEFILE_WRITE_THROUGH: u32 = 0x0000_0001;
#[repr(C)]
pub struct SystemTime {
    pub year: u16,
    pub month: u16,
    pub day_of_week: u16,
    pub day: u16,
    pub hour: u16,
    pub minute: u16,
    pub second: u16,
    pub milliseconds: u16,
}
unsafe extern "system" {
    pub fn GetLocalTime(system_time: *mut SystemTime);
    pub fn GetLastError() -> u32;
    pub fn MultiByteToWideChar(
        code_page: u32,
        flags: u32,
        src: *const u8,
        src_len: i32,
        dst: *mut u16,
        dst_len: i32,
    ) -> i32;
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
