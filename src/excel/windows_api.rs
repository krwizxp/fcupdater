use crate::{Result, err};
use std::{os::windows::ffi::OsStrExt, path::Path};
#[repr(C)]
struct SystemTime {
    year: u16,
    month: u16,
    day_of_week: u16,
    day: u16,
    hour: u16,
    minute: u16,
    second: u16,
    milliseconds: u16,
}
unsafe extern "system" {
    fn GetLocalTime(system_time: *mut SystemTime);
    fn GetLastError() -> u32;
    fn MultiByteToWideChar(
        code_page: u32,
        flags: u32,
        src: *const u8,
        src_len: i32,
        dst: *mut u16,
        dst_len: i32,
    ) -> i32;
    fn ReplaceFileW(
        replaced_file_name: *const u16,
        replacement_file_name: *const u16,
        backup_file_name: *const u16,
        replace_flags: u32,
        exclude: *mut core::ffi::c_void,
        reserved: *mut core::ffi::c_void,
    ) -> i32;
    fn MoveFileExW(existing_file_name: *const u16, new_file_name: *const u16, flags: u32) -> i32;
}
const MB_ERR_INVALID_CHARS: u32 = 0x0000_0008;
const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
const REPLACEFILE_WRITE_THROUGH: u32 = 0x0000_0001;
pub fn local_date_yyyy_mm_dd() -> Result<String> {
    let mut st = SystemTime {
        year: 0,
        month: 0,
        day_of_week: 0,
        day: 0,
        hour: 0,
        minute: 0,
        second: 0,
        milliseconds: 0,
    };
    unsafe {
        GetLocalTime(&raw mut st);
    }
    if st.month == 0 || st.month > 12 || st.day == 0 || st.day > 31 {
        return Err(err(format!(
            "OS 날짜 조회 결과가 비정상적입니다: {:04}-{:02}-{:02}",
            st.year, st.month, st.day
        )));
    }
    Ok(format!("{:04}-{:02}-{:02}", st.year, st.month, st.day))
}
pub fn decode_code_page(bytes: &[u8], code_page: u32) -> Option<String> {
    if bytes.is_empty() {
        return Some(String::new());
    }
    let src_len = i32::try_from(bytes.len()).ok()?;
    let required = unsafe {
        MultiByteToWideChar(
            code_page,
            MB_ERR_INVALID_CHARS,
            bytes.as_ptr(),
            src_len,
            std::ptr::null_mut(),
            0,
        )
    };
    if required <= 0 {
        return None;
    }
    let required_usize = usize::try_from(required).ok()?;
    let mut wide = vec![0u16; required_usize];
    let written = unsafe {
        MultiByteToWideChar(
            code_page,
            0,
            bytes.as_ptr(),
            src_len,
            wide.as_mut_ptr(),
            required,
        )
    };
    if written <= 0 {
        return None;
    }
    let written_usize = usize::try_from(written).ok()?;
    let view = wide.get(..written_usize.min(required_usize))?;
    Some(String::from_utf16_lossy(view))
}
pub fn replace_file_atomic(replacement: &Path, destination: &Path) -> Result<()> {
    let replacement_w = encode_path_wide(replacement);
    let destination_w = encode_path_wide(destination);
    if destination.exists() {
        let replaced = unsafe {
            ReplaceFileW(
                destination_w.as_ptr(),
                replacement_w.as_ptr(),
                std::ptr::null(),
                REPLACEFILE_WRITE_THROUGH,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        };
        if replaced != 0 {
            return Ok(());
        }
        let code = unsafe { GetLastError() };
        return Err(err(format!(
            "파일 교체 실패(ReplaceFileW): {} <- {} (GetLastError={code})",
            destination.display(),
            replacement.display()
        )));
    }
    let moved = unsafe {
        MoveFileExW(
            replacement_w.as_ptr(),
            destination_w.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    };
    if moved != 0 {
        return Ok(());
    }
    let code = unsafe { GetLastError() };
    Err(err(format!(
        "파일 이동 실패(MoveFileExW): {} -> {} (GetLastError={code})",
        replacement.display(),
        destination.display()
    )))
}
fn encode_path_wide(path: &Path) -> Vec<u16> {
    path.as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect()
}
