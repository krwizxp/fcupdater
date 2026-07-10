use super::{HInternet, c_void};
#[link(name = "winhttp")]
unsafe extern "system" {
    pub(super) fn WinHttpCloseHandle(h_internet: HInternet) -> i32;
    pub(super) fn WinHttpConnect(
        h_session: HInternet,
        server_name: *const u16,
        server_port: u16,
        reserved: u32,
    ) -> HInternet;
    pub(super) fn WinHttpOpen(
        user_agent: *const u16,
        access_type: u32,
        proxy_name: *const u16,
        proxy_bypass: *const u16,
        flags: u32,
    ) -> HInternet;
    pub(super) fn WinHttpOpenRequest(
        h_connect: HInternet,
        verb: *const u16,
        object_name: *const u16,
        version: *const u16,
        referrer: *const u16,
        accept_types: *const *const u16,
        flags: u32,
    ) -> HInternet;
    pub(super) fn WinHttpQueryHeaders(
        h_request: HInternet,
        info_level: u32,
        name: *const u16,
        buffer: *mut c_void,
        buffer_length: *mut u32,
        index: *mut u32,
    ) -> i32;
    pub(super) fn WinHttpReadData(
        h_request: HInternet,
        buffer: *mut c_void,
        bytes_to_read: u32,
        bytes_read: *mut u32,
    ) -> i32;
    pub(super) fn WinHttpReceiveResponse(h_request: HInternet, reserved: *mut c_void) -> i32;
    pub(super) fn WinHttpSendRequest(
        h_request: HInternet,
        headers: *const u16,
        headers_length: u32,
        optional: *const c_void,
        optional_length: u32,
        total_length: u32,
        context: usize,
    ) -> i32;
    pub(super) fn WinHttpSetOption(
        h_internet: *const c_void,
        option: u32,
        buffer: *const c_void,
        buffer_length: u32,
    ) -> i32;
    pub(super) fn WinHttpSetTimeouts(
        h_internet: HInternet,
        resolve_timeout: i32,
        connect_timeout: i32,
        send_timeout: i32,
        receive_timeout: i32,
    ) -> i32;
}
#[link(name = "kernel32")]
unsafe extern "system" {
    pub(super) fn GetLastError() -> u32;
}
