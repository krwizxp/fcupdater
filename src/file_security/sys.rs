use super::c_void;
unsafe extern "system" {
    #[link_name = "GetFileInformationByHandleEx"]
    pub(super) fn get_file_information_by_handle_ex(
        h_file: *mut c_void,
        file_information_class: i32,
        file_information: *mut c_void,
        buffer_size: u32,
    ) -> i32;
}
