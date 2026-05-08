#![allow(
    clippy::arbitrary_source_item_ordering,
    clippy::borrow_as_ptr,
    clippy::default_numeric_fallback,
    clippy::indexing_slicing,
    clippy::multiple_unsafe_ops_per_block,
    clippy::pattern_type_mismatch,
    clippy::semicolon_outside_block,
    clippy::shadow_reuse,
    clippy::shadow_unrelated,
    clippy::single_call_fn,
    clippy::undocumented_unsafe_blocks,
    clippy::unnecessary_wraps,
    reason = "WinHTTP FFI wrapper keeps raw platform calls small and local without external crates"
)]
use crate::{
    Result, err, err_with_source, is_metropolitan_token, is_province_token, normalize_address_key,
    path_source_message, prefixed_message, push_display, source_sync::SourceRecord,
    strip_basic_region_suffix,
};
use alloc::{string::String, vec::Vec};
use core::{result::Result as StdResult, time::Duration};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::LazyLock,
    thread::sleep,
    time::{SystemTime, UNIX_EPOCH},
};
const OPINET_HOST: &str = "www.opinet.co.kr";
const NETFUNNEL_HOST: &str = "nfl.opinet.co.kr";
const OPDOWNLOAD_PATH: &str = "/user/opdown/opDownload.do";
const OPDOWNLOAD_URL: &str = "https://www.opinet.co.kr/user/opdown/opDownload.do";
const OPDOWNLOAD_LAYOUT_PATH: &str = "/user/main/main_move_price.do";
const OPDOWNLOAD_EXCEL_PATH: &str = "/user/main/main_download_excel.do";
const OIL_PRICE_DOWNLOAD_TAR_URL: &str = "/user/opdown/oil_price_download";
const OPINET_KEY: &str = "tNNJ/zjnjSUqxRLpgiO/at1/w4SoJGbzzDOFVmlgEO0=";
const NETFUNNEL_SERVICE_ID: &str = "service_1";
const NETFUNNEL_ENTRY_ACTION_ID: &str = "B1";
const NETFUNNEL_DOWNLOAD_ACTION_ID: &str = "B7";
const CURRENT_PRICE_PAGE_DIV: &str = "PAGE_DIV_2";
const GAS_STATION_LPG_CODE: &str = "A";
const GAS_STATION_API_GBN: &str = "A";
const DEFAULT_REGION_LABEL: &str = "선택하세요.";
const USER_AGENT: &str = concat!("fcupdater/", env!("CARGO_PKG_VERSION"));
const NETFUNNEL_POLL_LIMIT: usize = 20;
pub const AUTO_SOURCE_MARKER: &str = "__fcupdater_auto__";
const TASKS: [Task; 11] = [
    Task {
        sido: "대전광역시",
        sigungu: "대덕구",
    },
    Task {
        sido: "대전광역시",
        sigungu: "동구",
    },
    Task {
        sido: "대전광역시",
        sigungu: "서구",
    },
    Task {
        sido: "대전광역시",
        sigungu: "유성구",
    },
    Task {
        sido: "대전광역시",
        sigungu: "중구",
    },
    Task {
        sido: "세종특별자치시",
        sigungu: "세종시",
    },
    Task {
        sido: "충청북도",
        sigungu: "청주시",
    },
    Task {
        sido: "충청남도",
        sigungu: "공주시",
    },
    Task {
        sido: "충청남도",
        sigungu: "보령시",
    },
    Task {
        sido: "충청남도",
        sigungu: "아산시",
    },
    Task {
        sido: "충청남도",
        sigungu: "천안시",
    },
];
#[derive(Debug, Clone, Copy)]
struct Task {
    sido: &'static str,
    sigungu: &'static str,
}
struct TaskMatcher {
    sido_key: String,
    task_keys: Vec<String>,
}
#[derive(Debug)]
struct HttpResponse {
    status: u32,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}
struct HttpClient {
    cookies: Vec<Cookie>,
}
#[derive(Clone)]
struct Cookie {
    name: String,
    value: String,
}
pub struct SourceDownloadOps;
pub trait SourceDownloadApi {
    fn filter_target_region_records(&self, records: Vec<SourceRecord>)
    -> Result<Vec<SourceRecord>>;
    fn refresh_sources(
        &self,
        dir: &Path,
        prefix: &str,
        _out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>>;
}
impl SourceDownloadApi for SourceDownloadOps {
    fn filter_target_region_records(
        &self,
        records: Vec<SourceRecord>,
    ) -> Result<Vec<SourceRecord>> {
        self.filter_target_region_records_impl(records)
    }
    fn refresh_sources(
        &self,
        dir: &Path,
        prefix: &str,
        out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>> {
        self.refresh_sources_impl(dir, prefix, out)
    }
}
trait SourceDownloadOpsExt {
    fn filter_target_region_records_impl(
        &self,
        records: Vec<SourceRecord>,
    ) -> Result<Vec<SourceRecord>>;
    fn refresh_sources_impl(
        &self,
        dir: &Path,
        prefix: &str,
        out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>>;
}
trait SourceDownloadWorkflowExt {
    fn auto_source_name(&self, prefix: &str, extension: &str) -> String;
    fn cleanup_auto_source_files(&self, dir: &Path, prefix: &str) -> StdResult<usize, String>;
    fn download_nationwide_source(&self, dir: &Path, prefix: &str) -> Result<Vec<PathBuf>>;
    fn download_nationwide_source_http(
        &self,
        dir: &Path,
        prefix: &str,
    ) -> StdResult<PathBuf, String>;
    fn record_matches_any_task(&self, record: &SourceRecord, matchers: &[TaskMatcher]) -> bool;
    fn region_has_explicit_sigungu(&self, region: &str) -> bool;
    fn task_match_keys(&self, task: &Task) -> Vec<String>;
    fn task_matchers(&self) -> &'static [TaskMatcher];
}
trait HttpClientExt {
    fn add_cookie(&mut self, name: &str, value: &str) -> StdResult<(), String>;
    fn cookie_header(&self) -> Option<String>;
    fn fetch_netfunnel_ticket(&mut self, action_id: &str) -> StdResult<String, String>;
    fn get_text(
        &mut self,
        host: &str,
        path: &str,
        referer: Option<&str>,
    ) -> StdResult<String, String>;
    fn post_form(
        &mut self,
        host: &str,
        path: &str,
        form: &[(&str, &str)],
        referer: Option<&str>,
        ajax: bool,
    ) -> StdResult<HttpResponse, String>;
    fn request(
        &mut self,
        method: &str,
        host: &str,
        path: &str,
        body: Option<&[u8]>,
        headers: &[(&str, &str)],
    ) -> StdResult<HttpResponse, String>;
    fn request_netfunnel(
        &mut self,
        action_id: &str,
        key: Option<&str>,
        ttl: Option<u32>,
    ) -> StdResult<String, String>;
    fn store_response_cookies(&mut self, response: &HttpResponse) -> StdResult<(), String>;
}
impl SourceDownloadOpsExt for SourceDownloadOps {
    fn filter_target_region_records_impl(
        &self,
        records: Vec<SourceRecord>,
    ) -> Result<Vec<SourceRecord>> {
        let matchers = self.task_matchers();
        let mut filtered = Vec::new();
        filtered
            .try_reserve_exact(records.len())
            .map_err(|source| {
                let mut message = String::with_capacity(64);
                message.push_str("필터링 소스 레코드 목록 메모리 확보 실패: ");
                push_display(&mut message, records.len());
                message.push_str(" records");
                err_with_source(message, source)
            })?;
        for record in records {
            if self.record_matches_any_task(&record, matchers) {
                filtered.push(record);
            }
        }
        Ok(filtered)
    }
    fn refresh_sources_impl(
        &self,
        dir: &Path,
        prefix: &str,
        out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>> {
        fs::create_dir_all(dir).map_err(|source_err| {
            err(path_source_message("소스 폴더 생성 실패", dir, source_err))
        })?;
        let canonical_dir = dir.canonicalize().map_err(|source_err| {
            err(path_source_message(
                "소스 폴더 경로 확인 실패",
                dir,
                source_err,
            ))
        })?;
        let removed = self
            .cleanup_auto_source_files(&canonical_dir, prefix)
            .map_err(|error_text| {
                err(prefixed_message("기존 자동 소스 정리 실패: ", error_text))
            })?;
        if removed > 0 {
            let _write_result = writeln!(out, "이전 임시 소스 파일 {removed}개 정리");
        }
        self.download_nationwide_source(&canonical_dir, prefix)
    }
}
impl SourceDownloadWorkflowExt for SourceDownloadOps {
    fn auto_source_name(&self, prefix: &str, extension: &str) -> String {
        let capacity = prefix
            .len()
            .saturating_add(AUTO_SOURCE_MARKER.len())
            .saturating_add("_opdownload_current_price.".len())
            .saturating_add(extension.len());
        let mut auto_source_name = String::with_capacity(capacity);
        auto_source_name.push_str(prefix);
        auto_source_name.push_str(AUTO_SOURCE_MARKER);
        auto_source_name.push_str("_opdownload_current_price.");
        auto_source_name.push_str(extension);
        auto_source_name
    }
    fn cleanup_auto_source_files(&self, dir: &Path, prefix: &str) -> StdResult<usize, String> {
        let mut removed = 0_usize;
        let prefix_fold = prefix.to_lowercase();
        let entries =
            fs::read_dir(dir).map_err(|error| path_source_message("폴더 읽기 실패", dir, error))?;
        for entry_result in entries {
            let dir_entry = entry_result
                .map_err(|error| prefixed_message("디렉터리 항목 읽기 실패: ", error))?;
            let path = dir_entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let folded = file_name.to_lowercase();
            if !(folded.starts_with(&prefix_fold) && folded.contains(AUTO_SOURCE_MARKER)) {
                continue;
            }
            fs::remove_file(&path)
                .map_err(|error| path_source_message("자동 소스 파일 삭제 실패", &path, error))?;
            removed = removed.saturating_add(1);
        }
        Ok(removed)
    }
    fn download_nationwide_source(&self, dir: &Path, prefix: &str) -> Result<Vec<PathBuf>> {
        let downloaded =
            self.download_nationwide_source_http(dir, prefix)
                .map_err(|error_text| {
                    err(prefixed_message("Opinet 자동 다운로드 실패: ", error_text))
                })?;
        Ok(vec![downloaded])
    }
    fn download_nationwide_source_http(
        &self,
        dir: &Path,
        prefix: &str,
    ) -> StdResult<PathBuf, String> {
        let mut client = HttpClient {
            cookies: Vec::new(),
        };
        let _gate_html = client.get_text(OPINET_HOST, OPDOWNLOAD_PATH, None)?;
        let entry_key = client.fetch_netfunnel_ticket(NETFUNNEL_ENTRY_ACTION_ID)?;
        let _entry_page = client.post_form(
            OPINET_HOST,
            OPDOWNLOAD_PATH,
            &[
                ("netfunnel_key", entry_key.as_str()),
                ("opinet_key", OPINET_KEY),
            ],
            Some(OPDOWNLOAD_URL),
            false,
        )?;
        let _layout = client.post_form(
            OPINET_HOST,
            OPDOWNLOAD_LAYOUT_PATH,
            &[("tarUrl", OIL_PRICE_DOWNLOAD_TAR_URL)],
            Some(OPDOWNLOAD_URL),
            true,
        )?;
        let download_key = client.fetch_netfunnel_ticket(NETFUNNEL_DOWNLOAD_ACTION_ID)?;
        let response = client.post_form(
            OPINET_HOST,
            OPDOWNLOAD_EXCEL_PATH,
            &[
                ("LPG_CD", GAS_STATION_LPG_CODE),
                ("DATE_DIV_CD", ""),
                ("PAGE_DIV", CURRENT_PRICE_PAGE_DIV),
                ("SIDO_NM", DEFAULT_REGION_LABEL),
                ("SIGUN_NM", DEFAULT_REGION_LABEL),
                ("API_GBN", GAS_STATION_API_GBN),
                ("netfunnel_key", download_key.as_str()),
            ],
            Some(OPDOWNLOAD_URL),
            false,
        )?;
        if !looks_like_excel(&response.body) {
            let preview = String::from_utf8_lossy(
                response
                    .body
                    .get(..response.body.len().min(512))
                    .unwrap_or(&[]),
            );
            return Err(prefixed_message(
                "다운로드 응답이 Excel 파일이 아닙니다: ",
                preview,
            ));
        }
        let extension = download_extension(&response.headers);
        let target = dir.join(self.auto_source_name(prefix, extension));
        let temp = dir.join(self.auto_source_name(prefix, "tmp"));
        fs::write(&temp, &response.body)
            .map_err(|error| path_source_message("다운로드 파일 쓰기 실패", &temp, error))?;
        match fs::rename(&temp, &target) {
            Ok(()) => {}
            Err(error) => {
                let _cleanup_result = fs::remove_file(&temp);
                return Err(path_source_message(
                    "다운로드 파일 이름 변경 실패",
                    &target,
                    error,
                ));
            }
        }
        Ok(target)
    }
    fn record_matches_any_task(&self, record: &SourceRecord, matchers: &[TaskMatcher]) -> bool {
        let region_key = normalize_address_key(&record.region);
        let region_has_explicit_sigungu =
            !region_key.is_empty() && self.region_has_explicit_sigungu(&record.region);
        let mut combined_key: Option<String> = None;
        matchers.iter().any(|matcher| {
            let matches_task = |value: &str| {
                matcher
                    .task_keys
                    .iter()
                    .any(|task_key| value.contains(task_key))
            };
            if !region_key.is_empty() {
                if !region_key.contains(&matcher.sido_key) {
                    return false;
                }
                if matches_task(&region_key) {
                    return true;
                }
                if region_has_explicit_sigungu {
                    return false;
                }
            }
            let combined = combined_key.get_or_insert_with(|| {
                let capacity = record
                    .region
                    .len()
                    .saturating_add(record.address.len())
                    .saturating_add(1);
                let mut combined_source = String::with_capacity(capacity);
                combined_source.push_str(&record.region);
                combined_source.push(' ');
                combined_source.push_str(&record.address);
                normalize_address_key(&combined_source)
            });
            combined.contains(&matcher.sido_key) && matches_task(combined)
        })
    }
    fn region_has_explicit_sigungu(&self, region: &str) -> bool {
        let mut tokens = region.split_whitespace().filter(|token| !token.is_empty());
        let Some(first_token) = tokens.next() else {
            return false;
        };
        if strip_basic_region_suffix(first_token).is_some() {
            return true;
        }
        (is_province_token(first_token) || is_metropolitan_token(first_token))
            && tokens
                .next()
                .is_some_and(|second_token| strip_basic_region_suffix(second_token).is_some())
    }
    fn task_match_keys(&self, task: &Task) -> Vec<String> {
        let mut keys = Vec::with_capacity(4);
        let mut push_alias_key = |alias: &str| {
            let alias_key = normalize_address_key(alias);
            if !alias_key.is_empty() && !keys.contains(&alias_key) {
                keys.push(alias_key);
            }
            let stripped = strip_basic_region_suffix(alias)
                .map(normalize_address_key)
                .unwrap_or_default();
            if !stripped.is_empty() && !keys.contains(&stripped) {
                keys.push(stripped);
            }
        };
        push_alias_key(task.sigungu);
        if task.sigungu == "세종시" {
            push_alias_key("세종특별자치시");
        }
        let sigungu_key = normalize_address_key(task.sigungu);
        if !sigungu_key.is_empty() && !keys.contains(&sigungu_key) {
            keys.push(sigungu_key);
        }
        keys
    }
    fn task_matchers(&self) -> &'static [TaskMatcher] {
        static TASK_MATCHERS: LazyLock<Vec<TaskMatcher>> = LazyLock::new(|| {
            let ops = SourceDownloadOps;
            TASKS
                .iter()
                .map(|task| TaskMatcher {
                    sido_key: normalize_address_key(task.sido),
                    task_keys: ops.task_match_keys(task),
                })
                .collect()
        });
        TASK_MATCHERS.as_slice()
    }
}
impl HttpClientExt for HttpClient {
    fn add_cookie(&mut self, name: &str, value: &str) -> StdResult<(), String> {
        if let Some(cookie) = self.cookies.iter_mut().find(|cookie| cookie.name == name) {
            cookie.value.clear();
            cookie
                .value
                .try_reserve(value.len())
                .map_err(|source| prefixed_message("Cookie 값 메모리 확보 실패: ", source))?;
            cookie.value.push_str(value);
            return Ok(());
        }
        let mut cookie = Cookie {
            name: String::new(),
            value: String::new(),
        };
        cookie
            .name
            .try_reserve(name.len())
            .map_err(|source| prefixed_message("Cookie 이름 메모리 확보 실패: ", source))?;
        cookie
            .value
            .try_reserve(value.len())
            .map_err(|source| prefixed_message("Cookie 값 메모리 확보 실패: ", source))?;
        cookie.name.push_str(name);
        cookie.value.push_str(value);
        self.cookies.push(cookie);
        Ok(())
    }
    fn cookie_header(&self) -> Option<String> {
        if self.cookies.is_empty() {
            return None;
        }
        let mut capacity = self.cookies.len().saturating_sub(1).saturating_mul(2);
        for cookie in &self.cookies {
            capacity = capacity
                .saturating_add(cookie.name.len())
                .saturating_add(cookie.value.len())
                .saturating_add(1);
        }
        let mut out = String::with_capacity(capacity);
        for (index, cookie) in self.cookies.iter().enumerate() {
            if index > 0 {
                out.push_str("; ");
            }
            out.push_str(&cookie.name);
            out.push('=');
            out.push_str(&cookie.value);
        }
        Some(out)
    }
    fn fetch_netfunnel_ticket(&mut self, action_id: &str) -> StdResult<String, String> {
        let mut current_key: Option<String> = None;
        for _ in 0..NETFUNNEL_POLL_LIMIT {
            let result = self.request_netfunnel(action_id, current_key.as_deref(), None)?;
            self.add_cookie("NetFunnel_ID", &result)?;
            let code = netfunnel_code(&result)?;
            if matches!(code, 200 | 300 | 303) {
                return extract_netfunnel_key(&result);
            }
            if matches!(code, 201 | 202 | 302) {
                current_key = Some(extract_netfunnel_key(&result)?);
                let wait_secs = netfunnel_ttl(&result).unwrap_or(1).clamp(1, 30);
                sleep(Duration::from_secs(u64::from(wait_secs)));
                continue;
            }
            return Err(prefixed_message("NetFunnel 응답 오류: ", result));
        }
        Err(String::from("NetFunnel 대기 횟수를 초과했습니다."))
    }
    fn get_text(
        &mut self,
        host: &str,
        path: &str,
        referer: Option<&str>,
    ) -> StdResult<String, String> {
        let mut headers = Vec::with_capacity(3);
        headers.push((
            "Accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        ));
        if let Some(referer_value) = referer {
            headers.push(("Referer", referer_value));
        }
        let response = self.request("GET", host, path, None, &headers)?;
        String::from_utf8(response.body)
            .map_err(|source| prefixed_message("HTTP 응답 UTF-8 변환 실패: ", source))
    }
    fn post_form(
        &mut self,
        host: &str,
        path: &str,
        form: &[(&str, &str)],
        referer: Option<&str>,
        ajax: bool,
    ) -> StdResult<HttpResponse, String> {
        let body = form_urlencode(form)?;
        let mut headers = Vec::with_capacity(6);
        headers.push((
            "Content-Type",
            "application/x-www-form-urlencoded; charset=UTF-8",
        ));
        headers.push(("Accept", "text/html, */*; q=0.01"));
        if ajax {
            headers.push(("X-Requested-With", "XMLHttpRequest"));
        }
        if let Some(referer_value) = referer {
            headers.push(("Referer", referer_value));
        }
        self.request("POST", host, path, Some(body.as_bytes()), &headers)
    }
    fn request(
        &mut self,
        method: &str,
        host: &str,
        path: &str,
        body: Option<&[u8]>,
        headers: &[(&str, &str)],
    ) -> StdResult<HttpResponse, String> {
        let mut merged_headers = Vec::with_capacity(headers.len().saturating_add(3));
        merged_headers.push(("User-Agent", USER_AGENT));
        merged_headers.push(("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.5,en;q=0.3"));
        for header in headers {
            merged_headers.push(*header);
        }
        let cookie_header = self.cookie_header();
        if let Some(cookie_text) = cookie_header.as_deref() {
            merged_headers.push(("Cookie", cookie_text));
        }
        let response = platform_https_request(method, host, path, body, &merged_headers)?;
        self.store_response_cookies(&response)?;
        if !(200..300).contains(&response.status) {
            let body_preview = String::from_utf8_lossy(
                response
                    .body
                    .get(..response.body.len().min(512))
                    .unwrap_or(&[]),
            );
            let mut out = String::with_capacity(body_preview.len().saturating_add(64));
            out.push_str("HTTP ");
            push_display(&mut out, response.status);
            out.push_str(": ");
            out.push_str(&body_preview);
            return Err(out);
        }
        Ok(response)
    }
    fn request_netfunnel(
        &mut self,
        action_id: &str,
        key: Option<&str>,
        ttl: Option<u32>,
    ) -> StdResult<String, String> {
        let timestamp = unix_epoch_millis()?;
        let opcode = if key.is_some() { "5002" } else { "5101" };
        let mut path = String::with_capacity(256);
        path.push_str("/ts.wseq?opcode=");
        path.push_str(opcode);
        if let Some(key_value) = key {
            path.push_str("&key=");
            push_percent_encoded(&mut path, key_value.as_bytes());
        }
        path.push_str("&nfid=0&prefix=NetFunnel.gRtype%3D");
        path.push_str(opcode);
        path.push_str("%3B");
        if let Some(ttl_value) = ttl {
            path.push_str("&ttl=");
            push_display(&mut path, ttl_value);
        }
        path.push_str("&sid=");
        path.push_str(NETFUNNEL_SERVICE_ID);
        path.push_str("&aid=");
        path.push_str(action_id);
        path.push_str("&js=yes&");
        push_display(&mut path, timestamp);
        let response = self.request(
            "GET",
            NETFUNNEL_HOST,
            &path,
            None,
            &[("Accept", "application/javascript,*/*;q=0.8")],
        )?;
        let text = String::from_utf8(response.body)
            .map_err(|source| prefixed_message("NetFunnel 응답 UTF-8 변환 실패: ", source))?;
        extract_quoted_value(&text, "result='", '\'')
            .map(str::to_owned)
            .ok_or_else(|| prefixed_message("NetFunnel result 파싱 실패: ", text))
    }
    fn store_response_cookies(&mut self, response: &HttpResponse) -> StdResult<(), String> {
        for (name, value) in &response.headers {
            if !name.eq_ignore_ascii_case("set-cookie") {
                continue;
            }
            let cookie_pair = value
                .split_once(';')
                .map_or(value.as_str(), |(head, _)| head);
            let Some((cookie_name, cookie_value)) = cookie_pair.split_once('=') else {
                continue;
            };
            self.add_cookie(cookie_name.trim(), cookie_value.trim())?;
        }
        Ok(())
    }
}
fn download_extension(headers: &[(String, String)]) -> &'static str {
    for (name, value) in headers {
        if !name.eq_ignore_ascii_case("content-disposition") {
            continue;
        }
        let folded = value.to_ascii_lowercase();
        if folded.contains(".xlsx") {
            return "xlsx";
        }
        if folded.contains(".xls") {
            return "xls";
        }
    }
    "xls"
}
fn extract_netfunnel_key(result: &str) -> StdResult<String, String> {
    let Some(start) = result.find("key=") else {
        return Err(prefixed_message("NetFunnel key 없음: ", result));
    };
    let value_start = start.saturating_add("key=".len());
    let tail = result
        .get(value_start..)
        .ok_or_else(|| prefixed_message("NetFunnel key 범위 오류: ", result))?;
    let value = tail.split('&').next().unwrap_or(tail);
    if value.is_empty() {
        return Err(prefixed_message("NetFunnel key 비어 있음: ", result));
    }
    Ok(value.to_owned())
}
fn extract_quoted_value<'text>(text: &'text str, marker: &str, quote: char) -> Option<&'text str> {
    let start = text.find(marker)?.checked_add(marker.len())?;
    let rest = text.get(start..)?;
    let end = rest.find(quote)?;
    rest.get(..end)
}
fn form_urlencode(pairs: &[(&str, &str)]) -> StdResult<String, String> {
    let mut out = String::new();
    for (index, (key, value)) in pairs.iter().enumerate() {
        if index > 0 {
            out.push('&');
        }
        push_percent_encoded(&mut out, key.as_bytes());
        out.push('=');
        push_percent_encoded(&mut out, value.as_bytes());
    }
    Ok(out)
}
fn looks_like_excel(bytes: &[u8]) -> bool {
    bytes.starts_with(&[0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1])
        || bytes.starts_with(b"PK\x03\x04")
}
fn netfunnel_code(result: &str) -> StdResult<u32, String> {
    let mut parts = result.split(':');
    let _opcode = parts.next();
    let Some(code_text) = parts.next() else {
        return Err(prefixed_message("NetFunnel 코드 없음: ", result));
    };
    code_text
        .parse::<u32>()
        .map_err(|source| prefixed_message("NetFunnel 코드 파싱 실패: ", source))
}
fn netfunnel_ttl(result: &str) -> Option<u32> {
    let start = result.find("ttl=")?.checked_add("ttl=".len())?;
    let tail = result.get(start..)?;
    tail.split('&').next()?.parse::<u32>().ok()
}
fn push_percent_encoded(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    for byte in bytes {
        match *byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(char::from(*byte));
            }
            b' ' => out.push('+'),
            other => {
                out.push('%');
                out.push(char::from(HEX[usize::from(other >> 4)]));
                out.push(char::from(HEX[usize::from(other & 0x0F)]));
            }
        }
    }
}
fn unix_epoch_millis() -> StdResult<u128, String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .map_err(|source| prefixed_message("현재 시간 조회 실패: ", source))
}
#[cfg(not(windows))]
fn platform_https_request(
    _method: &str,
    _host: &str,
    _path: &str,
    _body: Option<&[u8]>,
    _headers: &[(&str, &str)],
) -> StdResult<HttpResponse, String> {
    Err(String::from(
        "외부 TLS 크레이트 없이 HTTPS 다운로드를 수행하려면 현재 구현에서는 Windows WinHTTP가 필요합니다.",
    ))
}
#[cfg(windows)]
fn platform_https_request(
    method: &str,
    host: &str,
    path: &str,
    body: Option<&[u8]>,
    headers: &[(&str, &str)],
) -> StdResult<HttpResponse, String> {
    winhttp::request(method, host, path, body, headers)
}
#[cfg(windows)]
mod winhttp {
    use super::HttpResponse;
    use alloc::{string::String, vec::Vec};
    use core::{
        ffi::c_void,
        ptr::{null, null_mut},
    };
    use std::{ffi::OsStr, os::windows::ffi::OsStrExt as _};
    const ERROR_INSUFFICIENT_BUFFER: u32 = 122;
    const INTERNET_DEFAULT_HTTPS_PORT: u16 = 443;
    const WINHTTP_ACCESS_TYPE_DEFAULT_PROXY: u32 = 0;
    const WINHTTP_FLAG_SECURE: u32 = 0x0080_0000;
    const WINHTTP_QUERY_FLAG_NUMBER: u32 = 0x2000_0000;
    const WINHTTP_QUERY_RAW_HEADERS_CRLF: u32 = 22;
    const WINHTTP_QUERY_STATUS_CODE: u32 = 19;
    type HInternet = *mut c_void;
    #[link(name = "winhttp")]
    unsafe extern "system" {
        fn WinHttpCloseHandle(h_internet: HInternet) -> i32;
        fn WinHttpConnect(
            h_session: HInternet,
            server_name: *const u16,
            server_port: u16,
            reserved: u32,
        ) -> HInternet;
        fn WinHttpOpen(
            user_agent: *const u16,
            access_type: u32,
            proxy_name: *const u16,
            proxy_bypass: *const u16,
            flags: u32,
        ) -> HInternet;
        fn WinHttpOpenRequest(
            h_connect: HInternet,
            verb: *const u16,
            object_name: *const u16,
            version: *const u16,
            referrer: *const u16,
            accept_types: *const *const u16,
            flags: u32,
        ) -> HInternet;
        fn WinHttpQueryDataAvailable(h_request: HInternet, bytes_available: *mut u32) -> i32;
        fn WinHttpQueryHeaders(
            h_request: HInternet,
            info_level: u32,
            name: *const u16,
            buffer: *mut c_void,
            buffer_length: *mut u32,
            index: *mut u32,
        ) -> i32;
        fn WinHttpReadData(
            h_request: HInternet,
            buffer: *mut c_void,
            bytes_to_read: u32,
            bytes_read: *mut u32,
        ) -> i32;
        fn WinHttpReceiveResponse(h_request: HInternet, reserved: *mut c_void) -> i32;
        fn WinHttpSendRequest(
            h_request: HInternet,
            headers: *const u16,
            headers_length: u32,
            optional: *const c_void,
            optional_length: u32,
            total_length: u32,
            context: usize,
        ) -> i32;
        fn WinHttpSetTimeouts(
            h_internet: HInternet,
            resolve_timeout: i32,
            connect_timeout: i32,
            send_timeout: i32,
            receive_timeout: i32,
        ) -> i32;
    }
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn GetLastError() -> u32;
    }
    struct Handle(HInternet);
    impl Drop for Handle {
        fn drop(&mut self) {
            if !self.0.is_null() {
                unsafe {
                    WinHttpCloseHandle(self.0);
                }
            }
        }
    }
    pub(super) fn request(
        method: &str,
        host: &str,
        path: &str,
        body: Option<&[u8]>,
        headers: &[(&str, &str)],
    ) -> Result<HttpResponse, String> {
        let user_agent = wide(super::USER_AGENT);
        let host_wide = wide(host);
        let method_wide = wide(method);
        let path_wide = wide(path);
        let session = unsafe {
            WinHttpOpen(
                user_agent.as_ptr(),
                WINHTTP_ACCESS_TYPE_DEFAULT_PROXY,
                null(),
                null(),
                0,
            )
        };
        let session = non_null_handle(session, "WinHttpOpen")?;
        unsafe {
            WinHttpSetTimeouts(session.0, 30_000, 30_000, 60_000, 60_000);
        }
        let connect = unsafe {
            WinHttpConnect(
                session.0,
                host_wide.as_ptr(),
                INTERNET_DEFAULT_HTTPS_PORT,
                0,
            )
        };
        let connect = non_null_handle(connect, "WinHttpConnect")?;
        let request = unsafe {
            WinHttpOpenRequest(
                connect.0,
                method_wide.as_ptr(),
                path_wide.as_ptr(),
                null(),
                null(),
                null(),
                WINHTTP_FLAG_SECURE,
            )
        };
        let request = non_null_handle(request, "WinHttpOpenRequest")?;
        let headers_text = build_headers(headers)?;
        let headers_wide = wide(&headers_text);
        let body_slice = body.unwrap_or(&[]);
        let body_len = u32::try_from(body_slice.len())
            .map_err(|source| format!("요청 본문 길이 변환 실패: {source}"))?;
        let sent = unsafe {
            WinHttpSendRequest(
                request.0,
                headers_wide.as_ptr(),
                u32::try_from(headers_wide.len().saturating_sub(1))
                    .map_err(|source| format!("요청 헤더 길이 변환 실패: {source}"))?,
                if body_slice.is_empty() {
                    null()
                } else {
                    body_slice.as_ptr().cast::<c_void>()
                },
                body_len,
                body_len,
                0,
            )
        };
        if sent == 0 {
            return Err(last_error_message("WinHttpSendRequest"));
        }
        let received = unsafe { WinHttpReceiveResponse(request.0, null_mut()) };
        if received == 0 {
            return Err(last_error_message("WinHttpReceiveResponse"));
        }
        let status = query_status(request.0)?;
        let headers = query_headers(request.0)?;
        let body = read_body(request.0)?;
        Ok(HttpResponse {
            status,
            headers,
            body,
        })
    }
    fn build_headers(headers: &[(&str, &str)]) -> Result<String, String> {
        let mut out = String::new();
        for (name, value) in headers {
            out.try_reserve(name.len().saturating_add(value.len()).saturating_add(4))
                .map_err(|source| format!("요청 헤더 메모리 확보 실패: {source}"))?;
            out.push_str(name);
            out.push_str(": ");
            out.push_str(value);
            out.push_str("\r\n");
        }
        Ok(out)
    }
    fn last_error_message(context: &str) -> String {
        let code = unsafe { GetLastError() };
        format!("{context} 실패: Windows error {code}")
    }
    fn non_null_handle(handle: HInternet, context: &str) -> Result<Handle, String> {
        if handle.is_null() {
            Err(last_error_message(context))
        } else {
            Ok(Handle(handle))
        }
    }
    fn query_headers(request: HInternet) -> Result<Vec<(String, String)>, String> {
        let mut bytes = 0_u32;
        let mut index = 0_u32;
        let ok = unsafe {
            WinHttpQueryHeaders(
                request,
                WINHTTP_QUERY_RAW_HEADERS_CRLF,
                null(),
                null_mut(),
                &mut bytes,
                &mut index,
            )
        };
        if ok != 0 {
            return Ok(Vec::new());
        }
        let last_error = unsafe { GetLastError() };
        if last_error != ERROR_INSUFFICIENT_BUFFER {
            return Err(last_error_message("WinHttpQueryHeaders"));
        }
        let units = usize::try_from(bytes)
            .map_err(|source| format!("응답 헤더 길이 변환 실패: {source}"))?
            .checked_div(2)
            .ok_or_else(|| String::from("응답 헤더 길이 계산 실패"))?;
        let mut buffer = vec![0_u16; units];
        index = 0;
        let ok = unsafe {
            WinHttpQueryHeaders(
                request,
                WINHTTP_QUERY_RAW_HEADERS_CRLF,
                null(),
                buffer.as_mut_ptr().cast::<c_void>(),
                &mut bytes,
                &mut index,
            )
        };
        if ok == 0 {
            return Err(last_error_message("WinHttpQueryHeaders"));
        }
        while buffer.last().copied() == Some(0) {
            buffer.pop();
        }
        let raw = String::from_utf16_lossy(&buffer);
        let mut parsed = Vec::new();
        for line in raw.lines().skip(1) {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            parsed.push((name.trim().to_owned(), value.trim().to_owned()));
        }
        Ok(parsed)
    }
    fn query_status(request: HInternet) -> Result<u32, String> {
        let mut status = 0_u32;
        let mut bytes = u32::try_from(size_of::<u32>())
            .map_err(|source| format!("상태 코드 버퍼 길이 변환 실패: {source}"))?;
        let ok = unsafe {
            WinHttpQueryHeaders(
                request,
                WINHTTP_QUERY_STATUS_CODE | WINHTTP_QUERY_FLAG_NUMBER,
                null(),
                (&raw mut status).cast::<c_void>(),
                &mut bytes,
                null_mut(),
            )
        };
        if ok == 0 {
            Err(last_error_message("WinHttpQueryHeaders status"))
        } else {
            Ok(status)
        }
    }
    fn read_body(request: HInternet) -> Result<Vec<u8>, String> {
        let mut body = Vec::new();
        loop {
            let mut available = 0_u32;
            let ok = unsafe { WinHttpQueryDataAvailable(request, &mut available) };
            if ok == 0 {
                return Err(last_error_message("WinHttpQueryDataAvailable"));
            }
            if available == 0 {
                break;
            }
            let chunk_len = usize::try_from(available)
                .map_err(|source| format!("응답 chunk 길이 변환 실패: {source}"))?;
            let old_len = body.len();
            body.try_reserve(chunk_len)
                .map_err(|source| format!("응답 본문 메모리 확보 실패: {source}"))?;
            body.resize(old_len.saturating_add(chunk_len), 0);
            let mut read = 0_u32;
            let ok = unsafe {
                WinHttpReadData(
                    request,
                    body.as_mut_ptr().add(old_len).cast::<c_void>(),
                    available,
                    &mut read,
                )
            };
            if ok == 0 {
                return Err(last_error_message("WinHttpReadData"));
            }
            let read_len = usize::try_from(read)
                .map_err(|source| format!("응답 read 길이 변환 실패: {source}"))?;
            body.truncate(old_len.saturating_add(read_len));
            if read == 0 {
                break;
            }
        }
        Ok(body)
    }
    fn wide(value: &str) -> Vec<u16> {
        OsStr::new(value).encode_wide().chain([0]).collect()
    }
}
