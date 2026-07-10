use super::{
    Curl, CurlCode, CurlInfo, CurlOption, CurlSlist, CurlVersion, CurlVersionInfoData, c_char, c_long,
};
#[link(name = "curl")]
unsafe extern "C" {
    pub(super) fn curl_easy_cleanup(curl: *mut Curl);
    pub(super) fn curl_easy_getinfo(curl: *mut Curl, info: CurlInfo, ...) -> CurlCode;
    pub(super) fn curl_easy_init() -> *mut Curl;
    pub(super) fn curl_easy_perform(curl: *mut Curl) -> CurlCode;
    pub(super) fn curl_easy_reset(curl: *mut Curl);
    pub(super) fn curl_easy_setopt(curl: *mut Curl, option: CurlOption, ...) -> CurlCode;
    pub(super) fn curl_easy_strerror(code: CurlCode) -> *const c_char;
    pub(super) fn curl_global_init(flags: c_long) -> CurlCode;
    pub(super) fn curl_version_info(age: CurlVersion) -> *mut CurlVersionInfoData;
    pub(super) fn curl_slist_append(
        list: *mut CurlSlist,
        string: *const c_char,
    ) -> *mut CurlSlist;
    pub(super) fn curl_slist_free_all(list: *mut CurlSlist);
}
