use crate::{
    Result, err, is_metropolitan_token, is_province_token, normalize_address_key,
    path_source_message, prefixed_message, push_display,
    source_download_opdownload::{
        OPDOWNLOAD_DIAGNOSTIC_SCRIPT, OPDOWNLOAD_DISCOVERY_SCRIPT, OPDOWNLOAD_PAGE_READY_SCRIPT,
        OPDOWNLOAD_TRIGGER_SCRIPT,
    },
    source_sync::SourceRecord,
    strip_basic_region_suffix,
};
use alloc::{string::String, vec::Vec};
use core::{fmt::Display, result::Result as StdResult, time::Duration};
#[cfg(windows)]
use std::os::windows::process::CommandExt as _;
use std::{
    collections::HashSet,
    env::{current_dir, current_exe},
    fs,
    io::{Error, ErrorKind, Read as _, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::LazyLock,
    thread::sleep,
    time::{Instant, SystemTime},
};
const WEBDRIVER_HOST: &str = "127.0.0.1";
const CHROMEDRIVER_CMD: &str = "chromedriver";
const CHROMEDRIVER_DIR_NAME: &str = "chromedriver";
const EDGEDRIVER_CMD: &str = "msedgedriver";
const EDGEDRIVER_DIR_NAME: &str = "edgedriver";
const OPDOWNLOAD_URL: &str = "https://www.opinet.co.kr/user/opdown/opDownload.do";
pub const AUTO_SOURCE_MARKER: &str = "__fcupdater_auto__";
const DOWNLOAD_WAIT_TIMEOUT: Duration = Duration::from_mins(3);
const TASK_SESSION_RETRY_LIMIT: usize = 2;
const HEX_DIGITS: &[u8; 16] = b"0123456789abcdef";
cfg_select! {
    windows => {
        const CREATE_NO_WINDOW: u32 = 0x0800_0000;
        const CHROMEDRIVER_BIN_NAME: &str = "chromedriver.exe";
        const EDGEDRIVER_BIN_NAME: &str = "msedgedriver.exe";
    }
    _ => {
        const CHROMEDRIVER_BIN_NAME: &str = "chromedriver";
        const EDGEDRIVER_BIN_NAME: &str = "msedgedriver";
    }
}
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
struct ChildGuard {
    child: Option<Child>,
}
pub struct SourceDownloadOps;
pub trait SourceDownloadApi {
    fn filter_target_region_records(&self, records: Vec<SourceRecord>) -> Vec<SourceRecord>;
    fn refresh_sources(
        &self,
        dir: &Path,
        prefix: &str,
        _out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>>;
}
impl SourceDownloadApi for SourceDownloadOps {
    fn filter_target_region_records(&self, records: Vec<SourceRecord>) -> Vec<SourceRecord> {
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
    fn filter_target_region_records_impl(&self, records: Vec<SourceRecord>) -> Vec<SourceRecord>;
    fn refresh_sources_impl(
        &self,
        dir: &Path,
        prefix: &str,
        out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>>;
}
trait SourceDownloadWorkflowExt {
    fn cleanup_auto_source_files(&self, dir: &Path, prefix: &str) -> StdResult<usize, String>;
    fn download_nationwide_source(
        &self,
        dir: &Path,
        prefix: &str,
        out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>>;
    fn record_matches_any_task(&self, record: &SourceRecord, matchers: &[TaskMatcher]) -> bool;
    fn region_has_explicit_sigungu(&self, region: &str) -> bool;
    fn task_match_keys(&self, task: &Task) -> Vec<String>;
    fn task_matchers(&self) -> &'static [TaskMatcher];
}
trait SourceDownloadWebDriverExt {
    fn apply_webdriver_spawn_options<'command>(
        &self,
        command: &'command mut Command,
    ) -> &'command mut Command;
    fn download_nationwide_source_in_session(
        &self,
        webdriver_addr: &str,
        session_id: &str,
        download_dir: &Path,
        prefix: &str,
    ) -> StdResult<PathBuf, String>;
    fn download_nationwide_source_once(
        &self,
        webdriver_addr: &str,
        browser: BrowserKind,
        download_dir: &Path,
        prefix: &str,
    ) -> StdResult<PathBuf, String>;
    fn download_nationwide_source_with_retries(
        &self,
        webdriver_addr: &str,
        browser: BrowserKind,
        download_dir: &Path,
        prefix: &str,
        out: &mut dyn Write,
    ) -> StdResult<PathBuf, String>;
    fn ensure_webdriver_for_browser(
        &self,
        browser: BrowserKind,
    ) -> StdResult<WebDriverContext, String>;
    fn extract_json_optional_string_by_key(&self, json: &str, key: &str)
    -> Option<JsonStringField>;
    fn find_http_header_end(&self, raw: &[u8]) -> Option<(usize, usize)>;
    fn find_relative_webdriver(&self, browser: BrowserKind) -> StdResult<Option<PathBuf>, String>;
    fn http_request(
        &self,
        webdriver_addr: &str,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> StdResult<String, String>;
    fn is_recoverable_session_error(&self, error: &str) -> bool;
    fn is_transient_rename_error(&self, error: &Error) -> bool;
    fn json_escape(&self, input: &str) -> String;
    fn os_dev_null(&self) -> &'static str;
    fn parse_content_length(&self, header: &str) -> StdResult<Option<usize>, String>;
    fn read_http_response(&self, stream: &mut TcpStream) -> StdResult<String, String>;
    fn rename_with_retries(
        &self,
        source: &Path,
        target: &Path,
        timeout: Duration,
    ) -> StdResult<(), String>;
    fn reserve_webdriver_addr(&self) -> StdResult<String, String>;
    fn resolve_webdriver_program(&self, browser: BrowserKind) -> StdResult<PathBuf, String>;
    fn snapshot_files(&self, dir: &Path) -> StdResult<HashSet<PathBuf>, String>;
    fn split_http_response<'text>(&self, raw: &'text str) -> StdResult<(u16, &'text str), String>;
    fn wait_for_new_download(
        &self,
        dir: &Path,
        before: &HashSet<PathBuf>,
        timeout: Duration,
    ) -> StdResult<PathBuf, String>;
    fn wait_for_webdriver_ready(
        &self,
        webdriver_addr: &str,
        timeout: Duration,
    ) -> StdResult<(), String>;
    fn wait_until(
        &self,
        webdriver_addr: &str,
        session_id: &str,
        params: &WaitUntilParams<'_>,
    ) -> StdResult<(), String>;
    fn webdriver_accept_alert(
        &self,
        webdriver_addr: &str,
        session_id: &str,
    ) -> StdResult<(), String>;
    fn webdriver_candidates_from_base(&self, browser: BrowserKind, base_dir: &Path)
    -> [PathBuf; 2];
    fn webdriver_delete_session(
        &self,
        webdriver_addr: &str,
        session_id: &str,
    ) -> StdResult<(), String>;
    fn webdriver_download_dir_string(&self, path: &Path) -> String;
    fn webdriver_execute_optional_string(
        &self,
        webdriver_addr: &str,
        session_id: &str,
        script: &str,
    ) -> StdResult<Option<String>, String>;
    fn webdriver_execute_string(
        &self,
        webdriver_addr: &str,
        session_id: &str,
        script: &str,
    ) -> StdResult<String, String>;
    fn webdriver_get(
        &self,
        webdriver_addr: &str,
        session_id: &str,
        url: &str,
    ) -> StdResult<(), String>;
    fn webdriver_new_session(
        &self,
        browser: BrowserKind,
        webdriver_addr: &str,
        download_dir: &Path,
    ) -> StdResult<String, String>;
    fn webdriver_port(&self, webdriver_addr: &str) -> u16;
    fn webdriver_setup_hint(&self) -> String;
    fn webdriver_try_accept_alert(
        &self,
        webdriver_addr: &str,
        session_id: &str,
        timeout: Duration,
    ) -> StdResult<bool, String>;
    fn webdriver_version_mismatch_hint(&self, browser: BrowserKind, error: &str) -> &'static str;
}
struct WaitUntilParams<'text> {
    expected: &'text str,
    interval: Duration,
    label: &'text str,
    script: &'text str,
    timeout: Duration,
}
impl SourceDownloadOpsExt for SourceDownloadOps {
    fn filter_target_region_records_impl(&self, records: Vec<SourceRecord>) -> Vec<SourceRecord> {
        let matchers = self.task_matchers();
        records
            .into_iter()
            .filter(|record| self.record_matches_any_task(record, matchers))
            .collect()
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
        self.download_nationwide_source(&canonical_dir, prefix, out)
    }
}
#[derive(Debug, Clone, Copy)]
enum BrowserKind {
    Chrome,
    Edge,
}
impl BrowserKind {
    const fn browser_name(self) -> &'static str {
        match self {
            Self::Chrome => "chrome",
            Self::Edge => "MicrosoftEdge",
        }
    }
    const fn display_name(self) -> &'static str {
        match self {
            Self::Chrome => "Chrome",
            Self::Edge => "Edge",
        }
    }
    const fn driver_bin_name(self) -> &'static str {
        match self {
            Self::Chrome => CHROMEDRIVER_BIN_NAME,
            Self::Edge => EDGEDRIVER_BIN_NAME,
        }
    }
    const fn driver_cmd(self) -> &'static str {
        match self {
            Self::Chrome => CHROMEDRIVER_CMD,
            Self::Edge => EDGEDRIVER_CMD,
        }
    }
    const fn driver_dir_name(self) -> &'static str {
        match self {
            Self::Chrome => CHROMEDRIVER_DIR_NAME,
            Self::Edge => EDGEDRIVER_DIR_NAME,
        }
    }
    const fn options_key(self) -> &'static str {
        match self {
            Self::Chrome => "goog:chromeOptions",
            Self::Edge => "ms:edgeOptions",
        }
    }
}
struct WebDriverContext {
    addr: String,
    driver: ChildGuard,
}
enum JsonStringField {
    Null,
    String(String),
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _cleanup_diagnostic = self.shutdown();
    }
}
impl ChildGuard {
    fn shutdown(&mut self) -> Option<String> {
        let mut child = self.child.take()?;
        let mut diagnostic = String::with_capacity(96);
        match child.try_wait() {
            Ok(Some(_)) => return None,
            Ok(None) => {}
            Err(source_err) => {
                diagnostic.push_str("상태 확인 실패: ");
                push_display(&mut diagnostic, source_err);
            }
        }
        match child.kill() {
            Ok(()) => {}
            Err(source_err) if source_err.kind() == ErrorKind::InvalidInput => {}
            Err(source_err) => {
                if !diagnostic.is_empty() {
                    diagnostic.push_str(" / ");
                }
                diagnostic.push_str("종료 실패: ");
                push_display(&mut diagnostic, source_err);
            }
        }
        match child.wait() {
            Ok(_) => {}
            Err(source_err) => {
                if !diagnostic.is_empty() {
                    diagnostic.push_str(" / ");
                }
                diagnostic.push_str("대기 실패: ");
                push_display(&mut diagnostic, source_err);
            }
        }
        if diagnostic.is_empty() {
            None
        } else {
            Some(diagnostic)
        }
    }
}
impl WebDriverContext {
    fn shutdown(mut self) -> Option<String> {
        self.driver.shutdown()
    }
}
impl SourceDownloadWorkflowExt for SourceDownloadOps {
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
    fn download_nationwide_source(
        &self,
        dir: &Path,
        prefix: &str,
        out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>> {
        let mut errors = Vec::with_capacity(4);
        for browser in [BrowserKind::Chrome, BrowserKind::Edge] {
            let webdriver = match self.ensure_webdriver_for_browser(browser) {
                Ok(context) => context,
                Err(driver_error) => {
                    errors.push(browser_source_message(
                        browser.display_name(),
                        " WebDriver 준비 실패: ",
                        driver_error,
                    ));
                    continue;
                }
            };
            match self.download_nationwide_source_with_retries(
                &webdriver.addr,
                browser,
                dir,
                prefix,
                out,
            ) {
                Ok(downloaded) => {
                    let _shutdown_diagnostic = webdriver.shutdown();
                    let _write_result = writeln!(
                        out,
                        "다운로드 완료: {downloaded_path}",
                        downloaded_path = downloaded.display()
                    );
                    return Ok(vec![downloaded]);
                }
                Err(download_error) => {
                    let shutdown_diagnostic = webdriver.shutdown();
                    let mut download_failure = download_error;
                    if let Some(cleanup) = shutdown_diagnostic.as_deref() {
                        download_failure.push_str(" (정리 경고: ");
                        download_failure.push_str(cleanup);
                        download_failure.push(')');
                    }
                    errors.push(browser_source_message(
                        browser.display_name(),
                        " 다운로드 실패: ",
                        download_failure,
                    ));
                }
            }
        }
        let setup_hint = self.webdriver_setup_hint();
        let mut joined_len = errors.len().saturating_sub(1);
        for error_text in &errors {
            joined_len = joined_len.saturating_add(error_text.len());
        }
        let capacity = joined_len
            .saturating_add(setup_hint.len())
            .saturating_add(64);
        let mut message = String::with_capacity(capacity);
        message.push_str("Opinet 자동 다운로드 실패: ");
        for (index, error_message) in errors.iter().enumerate() {
            if index > 0 {
                message.push('\n');
            }
            message.push_str(error_message);
        }
        message.push_str("\nChrome 또는 Edge 설치와 ");
        message.push_str(&setup_hint);
        message.push_str("를 확인하세요.");
        Err(err(message))
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
impl SourceDownloadWebDriverExt for SourceDownloadOps {
    fn apply_webdriver_spawn_options<'command>(
        &self,
        command: &'command mut Command,
    ) -> &'command mut Command {
        cfg_select! {
            windows => {
                command.creation_flags(CREATE_NO_WINDOW)
            }
            _ => {
                command
            }
        }
    }
    fn download_nationwide_source_in_session(
        &self,
        webdriver_addr: &str,
        session_id: &str,
        download_dir: &Path,
        prefix: &str,
    ) -> StdResult<PathBuf, String> {
        self.webdriver_get(webdriver_addr, session_id, OPDOWNLOAD_URL)?;
        self.wait_until(
            webdriver_addr,
            session_id,
            &WaitUntilParams {
                expected: "READY",
                interval: Duration::from_millis(500),
                label: "opDownload 페이지 로딩",
                script: OPDOWNLOAD_PAGE_READY_SCRIPT,
                timeout: Duration::from_secs(30),
            },
        )?;
        sleep(Duration::from_secs(2));
        let before = self.snapshot_files(download_dir)?;
        let trigger = match self.webdriver_execute_optional_string(
            webdriver_addr,
            session_id,
            OPDOWNLOAD_TRIGGER_SCRIPT,
        ) {
            Ok(Some(value)) => value,
            Ok(None) => String::from("OK|null"),
            Err(error) => {
                return Err(prefixed_message(
                    "opDownload 다운로드 트리거 실행 실패: ",
                    error,
                ));
            }
        };
        let _alert_result =
            self.webdriver_try_accept_alert(webdriver_addr, session_id, Duration::from_secs(5));
        if !trigger.starts_with("OK|") {
            let discovery = self
                .webdriver_execute_string(webdriver_addr, session_id, OPDOWNLOAD_DISCOVERY_SCRIPT)
                .unwrap_or_else(|err| prefixed_message("후보 컨트롤 조회 실패: ", err));
            let capacity = trigger
                .len()
                .saturating_add(discovery.len())
                .saturating_add(48);
            let mut out = String::with_capacity(capacity);
            out.push_str("opDownload 다운로드 트리거를 찾지 못했습니다.\n트리거 결과: ");
            out.push_str(&trigger);
            out.push_str("\n후보 컨트롤:\n");
            out.push_str(&discovery);
            return Err(out);
        }
        let downloaded = self
            .wait_for_new_download(download_dir, &before, DOWNLOAD_WAIT_TIMEOUT)
            .map_err(|error| {
                let diagnostic = self
                    .webdriver_execute_string(
                        webdriver_addr,
                        session_id,
                        OPDOWNLOAD_DIAGNOSTIC_SCRIPT,
                    )
                    .unwrap_or_else(|err| prefixed_message("후속 진단 실패: ", err));
                let capacity = error
                    .len()
                    .saturating_add(trigger.len())
                    .saturating_add(diagnostic.len())
                    .saturating_add(40);
                let mut out = String::with_capacity(capacity);
                out.push_str("다운로드 대기 실패: ");
                out.push_str(&error);
                out.push_str("\n트리거 결과: ");
                out.push_str(&trigger);
                out.push_str("\n후속 진단:\n");
                out.push_str(&diagnostic);
                out
            })?;
        let ext = downloaded
            .extension()
            .and_then(|value| value.to_str())
            .map_or_else(|| String::from("xls"), str::to_owned);
        let capacity = prefix
            .len()
            .saturating_add(AUTO_SOURCE_MARKER.len())
            .saturating_add("_opdownload_current_price.".len())
            .saturating_add(ext.len());
        let mut auto_source_name = String::with_capacity(capacity);
        auto_source_name.push_str(prefix);
        auto_source_name.push_str(AUTO_SOURCE_MARKER);
        auto_source_name.push_str("_opdownload_current_price.");
        auto_source_name.push_str(&ext);
        let renamed = download_dir.join(auto_source_name);
        self.rename_with_retries(&downloaded, &renamed, Duration::from_secs(10))?;
        Ok(renamed)
    }
    fn download_nationwide_source_once(
        &self,
        webdriver_addr: &str,
        browser: BrowserKind,
        download_dir: &Path,
        prefix: &str,
    ) -> StdResult<PathBuf, String> {
        let session_id = self
            .webdriver_new_session(browser, webdriver_addr, download_dir)
            .map_err(|error| {
                let version_hint = self.webdriver_version_mismatch_hint(browser, &error);
                let capacity = error
                    .len()
                    .saturating_add(version_hint.len())
                    .saturating_add(16);
                let mut out = String::with_capacity(capacity);
                out.push_str("브라우저 세션 생성 실패: ");
                out.push_str(&error);
                out.push_str(version_hint);
                out
            })?;
        let result = self.download_nationwide_source_in_session(
            webdriver_addr,
            &session_id,
            download_dir,
            prefix,
        );
        let _delete_result = self.webdriver_delete_session(webdriver_addr, &session_id);
        result
    }
    fn download_nationwide_source_with_retries(
        &self,
        webdriver_addr: &str,
        browser: BrowserKind,
        download_dir: &Path,
        prefix: &str,
        out: &mut dyn Write,
    ) -> StdResult<PathBuf, String> {
        let mut last_error = None;
        for attempt in 1..=TASK_SESSION_RETRY_LIMIT {
            match self.download_nationwide_source_once(
                webdriver_addr,
                browser,
                download_dir,
                prefix,
            ) {
                Ok(path) => return Ok(path),
                Err(err) => {
                    let should_retry = attempt < TASK_SESSION_RETRY_LIMIT
                        && self.is_recoverable_session_error(&err);
                    last_error = Some(err);
                    if should_retry {
                        let _write_result = writeln!(
                            out,
                            "다운로드 재시도 {attempt}/{TASK_SESSION_RETRY_LIMIT}: 브라우저 세션을 다시 시작합니다."
                        );
                        sleep(Duration::from_secs(2));
                        continue;
                    }
                    break;
                }
            }
        }
        Err(last_error.unwrap_or_else(|| String::from("다운로드 실패")))
    }
    fn ensure_webdriver_for_browser(
        &self,
        browser: BrowserKind,
    ) -> StdResult<WebDriverContext, String> {
        let webdriver_addr = self.reserve_webdriver_addr()?;
        let webdriver_port = self.webdriver_port(&webdriver_addr);
        let program = self.resolve_webdriver_program(browser)?;
        let mut command = Command::new(&program);
        let child = self
            .apply_webdriver_spawn_options(
                command
                    .env("CHROME_LOG_FILE", self.os_dev_null())
                    .env("MSEDGEDRIVER_TELEMETRY_OPTOUT", "1")
                    .arg({
                        let capacity = "--port=".len().saturating_add(6);
                        let mut arg = String::with_capacity(capacity);
                        arg.push_str("--port=");
                        push_display(&mut arg, webdriver_port);
                        arg
                    })
                    .stdout(Stdio::null())
                    .stderr(Stdio::null()),
            )
            .spawn()
            .map_err(|error| {
                let capacity = 96;
                let mut out = String::with_capacity(capacity);
                out.push('`');
                push_display(&mut out, program.display());
                out.push_str("` 실행 실패: ");
                push_display(&mut out, error);
                out
            })?;
        let guard = ChildGuard { child: Some(child) };
        self.wait_for_webdriver_ready(&webdriver_addr, Duration::from_secs(15))?;
        Ok(WebDriverContext {
            addr: webdriver_addr,
            driver: guard,
        })
    }
    fn extract_json_optional_string_by_key(
        &self,
        json: &str,
        key: &str,
    ) -> Option<JsonStringField> {
        let capacity = key.len().saturating_add(2);
        let mut needle = String::with_capacity(capacity);
        needle.push('"');
        needle.push_str(key);
        needle.push('"');
        let start = json.find(&needle)?;
        let bytes = json.as_bytes();
        let mut index = start.checked_add(needle.len())?;
        while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
            index = index.checked_add(1)?;
        }
        if bytes.get(index).copied() != Some(b':') {
            return None;
        }
        index = index.checked_add(1)?;
        while bytes.get(index).is_some_and(u8::is_ascii_whitespace) {
            index = index.checked_add(1)?;
        }
        match bytes.get(index).copied() {
            Some(b'"') => {
                index = index.checked_add(1)?;
                let mut out = String::with_capacity(bytes.len().saturating_sub(index).min(128));
                let mut escaped = false;
                while let Some(byte) = bytes.get(index).copied() {
                    index = index.checked_add(1)?;
                    if escaped {
                        let ch = match byte {
                            b'"' => '"',
                            b'\\' => '\\',
                            b'/' => '/',
                            b'b' => '\u{0008}',
                            b'f' => '\u{000C}',
                            b'n' => '\n',
                            b'r' => '\r',
                            b't' => '\t',
                            _ => char::from(byte),
                        };
                        out.push(ch);
                        escaped = false;
                        continue;
                    }
                    match byte {
                        b'\\' => escaped = true,
                        b'"' => return Some(JsonStringField::String(out)),
                        _ => out.push(char::from(byte)),
                    }
                }
                None
            }
            Some(b'n') if json.get(index..)?.starts_with("null") => Some(JsonStringField::Null),
            _ => None,
        }
    }
    fn find_http_header_end(&self, raw: &[u8]) -> Option<(usize, usize)> {
        raw.array_windows::<4>()
            .position(|window| window == b"\r\n\r\n")
            .map(|pos| (pos, 4))
            .or_else(|| {
                raw.array_windows::<2>()
                    .position(|window| window == b"\n\n")
                    .map(|pos| (pos, 2))
            })
    }
    fn find_relative_webdriver(&self, browser: BrowserKind) -> StdResult<Option<PathBuf>, String> {
        let mut base_dirs = Vec::with_capacity(4);
        let push_unique_path = |paths: &mut Vec<PathBuf>, candidate: PathBuf| {
            if !paths.contains(&candidate) {
                paths.push(candidate);
            }
        };
        let path_is_file = |path: &Path| -> StdResult<bool, String> {
            if !path
                .try_exists()
                .map_err(|error| path_source_message("경로 확인 실패", path, error))?
            {
                return Ok(false);
            }
            fs::metadata(path)
                .map(|metadata| metadata.is_file())
                .map_err(|error| path_source_message("메타데이터 확인 실패", path, error))
        };
        if let Ok(current_dir) = current_dir() {
            push_unique_path(&mut base_dirs, current_dir);
        }
        if let Ok(current_exe) = current_exe()
            && let Some(exe_dir) = current_exe.parent()
        {
            for ancestor in exe_dir.ancestors().take(3) {
                push_unique_path(&mut base_dirs, ancestor.to_path_buf());
            }
        }
        for base_dir in base_dirs {
            for candidate in self.webdriver_candidates_from_base(browser, &base_dir) {
                if path_is_file(&candidate)? {
                    return Ok(Some(candidate));
                }
            }
        }
        Ok(None)
    }
    fn http_request(
        &self,
        webdriver_addr: &str,
        method: &str,
        path: &str,
        body: Option<&str>,
    ) -> StdResult<String, String> {
        let mut stream = TcpStream::connect(webdriver_addr)
            .map_err(|error| prefixed_message("WebDriver 연결 실패: ", error))?;
        let _read_timeout_result = stream.set_read_timeout(Some(Duration::from_mins(1)));
        let _write_timeout_result = stream.set_write_timeout(Some(Duration::from_mins(1)));
        let request_body = body.unwrap_or_default();
        let capacity = method
            .len()
            .saturating_add(path.len())
            .saturating_add(webdriver_addr.len().saturating_mul(2))
            .saturating_add(request_body.len())
            .saturating_add(20)
            .saturating_add(128);
        let mut request = String::with_capacity(capacity);
        request.push_str(method);
        request.push(' ');
        request.push_str(path);
        request.push_str(" HTTP/1.1\r\nHost: ");
        request.push_str(webdriver_addr);
        request.push_str(
            "\r\nConnection: close\r\nContent-Type: application/json; charset=utf-8\r\nContent-Length: ",
        );
        push_display(&mut request, request_body.len());
        request.push_str("\r\n\r\n");
        request.push_str(request_body);
        stream
            .write_all(request.as_bytes())
            .map_err(|error| prefixed_message("요청 전송 실패: ", error))?;
        let _flush_result = stream.flush();
        let raw = self.read_http_response(&mut stream)?;
        let (status, response_body) = self.split_http_response(&raw)?;
        if !(200..300).contains(&status) {
            let message_capacity = response_body.len().saturating_add(64);
            let mut message = String::with_capacity(message_capacity);
            message.push_str("HTTP ");
            push_display(&mut message, status);
            message.push_str(" 오류: ");
            message.push_str(response_body);
            return Err(message);
        }
        Ok(response_body.to_owned())
    }
    fn is_recoverable_session_error(&self, error: &str) -> bool {
        error.contains("invalid session id")
            || error.contains("session deleted as the browser has closed the connection")
            || error.contains("disconnected: not connected to DevTools")
            || error.contains("chrome not reachable")
    }
    fn is_transient_rename_error(&self, error: &Error) -> bool {
        matches!(
            error.kind(),
            ErrorKind::PermissionDenied | ErrorKind::WouldBlock
        ) || matches!(error.raw_os_error(), Some(32 | 33))
    }
    fn json_escape(&self, input: &str) -> String {
        let mut out = String::with_capacity(input.len().saturating_add(16));
        for ch in input.chars() {
            match ch {
                '"' => out.push_str("\\\""),
                '\\' => out.push_str("\\\\"),
                '\n' => out.push_str("\\n"),
                '\r' => out.push_str("\\r"),
                '\t' => out.push_str("\\t"),
                control_char if (u32::from(control_char)) < 0x20 => {
                    let value = u32::from(control_char);
                    out.push_str("\\u");
                    for shift in [12_u32, 8, 4, 0] {
                        let nibble = usize::try_from((value >> shift) & 0x0f).ok();
                        if let Some(index) = nibble
                            && let Some(&digit) = HEX_DIGITS.get(index)
                        {
                            out.push(char::from(digit));
                        }
                    }
                }
                regular_char => out.push(regular_char),
            }
        }
        out
    }
    fn os_dev_null(&self) -> &'static str {
        cfg_select! {
            windows => {
                "NUL"
            }
            _ => {
                "/dev/null"
            }
        }
    }
    fn parse_content_length(&self, header: &str) -> StdResult<Option<usize>, String> {
        for line in header.lines() {
            let Some((name, value)) = line.split_once(':') else {
                continue;
            };
            if name.eq_ignore_ascii_case("content-length") {
                let value_text = value.trim();
                let length = value_text.parse::<usize>().map_err(|err| {
                    let capacity = value_text.len().saturating_add(40);
                    let mut out = String::with_capacity(capacity);
                    out.push_str("Content-Length 파싱 실패: ");
                    out.push_str(value_text);
                    out.push_str(" (");
                    push_display(&mut out, err);
                    out.push(')');
                    out
                })?;
                return Ok(Some(length));
            }
        }
        Ok(None)
    }
    fn read_http_response(&self, stream: &mut TcpStream) -> StdResult<String, String> {
        let mut raw = Vec::with_capacity(8192);
        let mut expected_total_len = None;
        loop {
            let mut chunk = [0_u8; 4096];
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(read) => {
                    let chunk_bytes = chunk
                        .get(..read)
                        .ok_or_else(|| "HTTP 응답 chunk 범위 오류".to_owned())?;
                    raw.extend_from_slice(chunk_bytes);
                    if expected_total_len.is_none()
                        && let Some((header_end, separator_len)) = self.find_http_header_end(&raw)
                    {
                        let header_bytes = raw
                            .get(..header_end)
                            .ok_or_else(|| "HTTP 헤더 범위 오류".to_owned())?;
                        let header = String::from_utf8_lossy(header_bytes);
                        if let Some(content_length) = self.parse_content_length(&header)? {
                            expected_total_len = Some(
                                header_end
                                    .checked_add(separator_len)
                                    .and_then(|value| value.checked_add(content_length))
                                    .ok_or_else(|| "HTTP 응답 길이 계산 overflow".to_owned())?,
                            );
                            if let Some(expected) = expected_total_len
                                && expected > raw.len()
                            {
                                raw.reserve(expected.saturating_sub(raw.len()));
                            }
                        }
                    }
                    if expected_total_len.is_some_and(|expected| raw.len() >= expected) {
                        break;
                    }
                }
                Err(err) if matches!(err.kind(), ErrorKind::TimedOut | ErrorKind::WouldBlock) => {
                    if raw.is_empty() {
                        return Err("HTTP 응답이 비어 있습니다".into());
                    }
                    break;
                }
                Err(err) => return Err(prefixed_message("응답 수신 실패: ", err)),
            }
        }
        if raw.is_empty() {
            return Err("HTTP 응답이 비어 있습니다".into());
        }
        if let Some(expected) = expected_total_len
            && raw.len() < expected
        {
            let capacity = 96;
            let mut out = String::with_capacity(capacity);
            out.push_str("HTTP 응답 본문이 끝나기 전에 연결이 종료되었습니다. (received=");
            push_display(&mut out, raw.len());
            out.push_str(", expected=");
            push_display(&mut out, expected);
            out.push(')');
            return Err(out);
        }
        Ok(String::from_utf8_lossy(&raw).into_owned())
    }
    fn rename_with_retries(
        &self,
        source: &Path,
        target: &Path,
        timeout: Duration,
    ) -> StdResult<(), String> {
        let start = Instant::now();
        let mut last_error = None;
        loop {
            match fs::rename(source, target) {
                Ok(()) => return Ok(()),
                Err(error) => {
                    let error_text = format!("{error}");
                    if !self.is_transient_rename_error(&error) || start.elapsed() > timeout {
                        return Err(last_error.unwrap_or(error_text));
                    }
                    last_error = Some(error_text);
                    sleep(Duration::from_millis(250));
                }
            }
        }
    }
    fn reserve_webdriver_addr(&self) -> StdResult<String, String> {
        for _ in 0_u32..32_u32 {
            let listener_v4 = TcpListener::bind((WEBDRIVER_HOST, 0))
                .map_err(|error| prefixed_message("빈 WebDriver 포트 확보 실패: ", error))?;
            let port = listener_v4
                .local_addr()
                .map_err(|error| prefixed_message("할당 포트 확인 실패: ", error))?
                .port();
            match TcpListener::bind(("::1", port)) {
                Ok(listener_v6) => {
                    drop(listener_v6);
                    drop(listener_v4);
                    let capacity = WEBDRIVER_HOST.len().saturating_add(6).saturating_add(1);
                    let mut addr = String::with_capacity(capacity);
                    addr.push_str(WEBDRIVER_HOST);
                    addr.push(':');
                    push_display(&mut addr, port);
                    return Ok(addr);
                }
                Err(err) if err.kind() == ErrorKind::AddrInUse => {}
                Err(err) if err.kind() == ErrorKind::AddrNotAvailable => {
                    return Err(
                        "IPv6 loopback(::1)을 사용할 수 없습니다. 현재 ChromeDriver는 IPv6 바인딩이 가능한 환경이 필요합니다."
                            .to_owned(),
                    );
                }
                Err(err) => {
                    return Err(prefixed_message("IPv6 포트 확인 실패: ", err));
                }
            }
        }
        Err(String::from(
            "사용 가능한 WebDriver 포트를 찾지 못했습니다.",
        ))
    }
    fn resolve_webdriver_program(&self, browser: BrowserKind) -> StdResult<PathBuf, String> {
        if let Some(candidate) = self.find_relative_webdriver(browser)? {
            return Ok(candidate);
        }
        Ok(PathBuf::from(browser.driver_cmd()))
    }
    fn snapshot_files(&self, dir: &Path) -> StdResult<HashSet<PathBuf>, String> {
        let mut set = HashSet::with_capacity(32);
        if !dir
            .try_exists()
            .map_err(|error| path_source_message("다운로드 폴더 경로 확인 실패", dir, error))?
        {
            return Ok(set);
        }
        let entries = fs::read_dir(dir)
            .map_err(|error| path_source_message("다운로드 폴더 읽기 실패", dir, error))?;
        for entry_result in entries {
            let dir_entry = entry_result
                .map_err(|error| prefixed_message("디렉터리 항목 읽기 실패: ", error))?;
            let path = dir_entry.path();
            if path.is_file() {
                set.insert(path);
            }
        }
        Ok(set)
    }
    fn split_http_response<'text>(&self, raw: &'text str) -> StdResult<(u16, &'text str), String> {
        if raw.trim().is_empty() {
            return Err("HTTP 응답이 비어 있습니다".into());
        }
        let status_line = raw
            .lines()
            .find(|line| !line.is_empty())
            .ok_or_else(|| "HTTP 상태줄을 읽지 못했습니다".to_owned())?;
        let mut parts = status_line.split_whitespace();
        let _http = parts.next();
        let code = parts
            .next()
            .ok_or_else(|| prefixed_message("HTTP 상태코드 없음: ", status_line))?
            .parse::<u16>()
            .map_err(|error| prefixed_message("HTTP 상태코드 파싱 실패: ", error))?;
        let body = raw
            .split_once("\r\n\r\n")
            .or_else(|| raw.split_once("\n\n"))
            .map(|(_, body)| body)
            .ok_or_else(|| "HTTP 본문을 찾지 못했습니다".to_owned())?;
        Ok((code, body))
    }
    fn wait_for_new_download(
        &self,
        dir: &Path,
        before: &HashSet<PathBuf>,
        timeout: Duration,
    ) -> StdResult<PathBuf, String> {
        let start = Instant::now();
        loop {
            let mut latest_complete: Option<(Option<SystemTime>, PathBuf)> = None;
            let mut temp_exists = false;
            if dir
                .try_exists()
                .map_err(|error| path_source_message("다운로드 폴더 경로 확인 실패", dir, error))?
            {
                let entries = fs::read_dir(dir)
                    .map_err(|error| path_source_message("다운로드 폴더 읽기 실패", dir, error))?;
                for entry_result in entries {
                    let dir_entry = entry_result
                        .map_err(|error| prefixed_message("디렉터리 항목 읽기 실패: ", error))?;
                    let path = dir_entry.path();
                    if !path.is_file() || before.contains(&path) {
                        continue;
                    }
                    let ext = path
                        .extension()
                        .and_then(|suffix| suffix.to_str())
                        .unwrap_or_default();
                    if ext.eq_ignore_ascii_case("xls") || ext.eq_ignore_ascii_case("xlsx") {
                        let modified = fs::metadata(&path).and_then(|meta| meta.modified()).ok();
                        let should_replace = latest_complete.as_ref().is_none_or(|best| {
                            modified > best.0 || (modified == best.0 && path > best.1)
                        });
                        if should_replace {
                            latest_complete = Some((modified, path));
                        }
                    } else {
                        let is_temp_download = ext.eq_ignore_ascii_case("crdownload")
                            || ext.eq_ignore_ascii_case("part")
                            || ext.eq_ignore_ascii_case("tmp");
                        if is_temp_download {
                            temp_exists = true;
                        }
                    }
                }
            }
            if let Some((_, path)) = latest_complete
                && !temp_exists
            {
                return Ok(path);
            }
            if start.elapsed() > timeout {
                return Err("다운로드 완료 파일을 찾지 못했습니다".into());
            }
            sleep(Duration::from_millis(500));
        }
    }
    fn wait_for_webdriver_ready(
        &self,
        webdriver_addr: &str,
        timeout: Duration,
    ) -> StdResult<(), String> {
        let start = Instant::now();
        let mut last_error = "아직 /status 응답이 없습니다.".into();
        loop {
            if start.elapsed() > timeout {
                return Err(prefixed_message(
                    "WebDriver 준비 대기 시간 초과: ",
                    &last_error,
                ));
            }
            match self.http_request(webdriver_addr, "GET", "/status", None) {
                Ok(response)
                    if response.contains(r#""ready":true"#)
                        || response.contains(r#""ready": true"#)
                        || response.contains("ChromeDriver ready for new sessions") =>
                {
                    return Ok(());
                }
                Ok(response) => {
                    last_error = prefixed_message("WebDriver 준비 전 응답: ", response);
                }
                Err(err) => {
                    last_error = err;
                }
            }
            sleep(Duration::from_millis(200));
        }
    }
    fn wait_until(
        &self,
        webdriver_addr: &str,
        session_id: &str,
        params: &WaitUntilParams<'_>,
    ) -> StdResult<(), String> {
        let start = Instant::now();
        loop {
            let value = self.webdriver_execute_string(webdriver_addr, session_id, params.script)?;
            if value == params.expected {
                return Ok(());
            }
            if start.elapsed() > params.timeout {
                let mut out = String::with_capacity(params.label.len().saturating_add(16));
                out.push_str("대기 시간 초과: ");
                out.push_str(params.label);
                return Err(out);
            }
            sleep(params.interval);
        }
    }
    fn webdriver_accept_alert(
        &self,
        webdriver_addr: &str,
        session_id: &str,
    ) -> StdResult<(), String> {
        let path = build_webdriver_session_path(session_id, "/alert/accept");
        self.http_request(webdriver_addr, "POST", &path, Some("{}"))?;
        Ok(())
    }
    fn webdriver_candidates_from_base(
        &self,
        browser: BrowserKind,
        base_dir: &Path,
    ) -> [PathBuf; 2] {
        [
            base_dir.join(browser.driver_bin_name()),
            base_dir
                .join(browser.driver_dir_name())
                .join(browser.driver_bin_name()),
        ]
    }
    fn webdriver_delete_session(
        &self,
        webdriver_addr: &str,
        session_id: &str,
    ) -> StdResult<(), String> {
        let path = build_webdriver_session_path(session_id, "");
        self.http_request(webdriver_addr, "DELETE", &path, None)?;
        Ok(())
    }
    fn webdriver_download_dir_string(&self, path: &Path) -> String {
        cfg_select! {
            windows => {
                let raw = path.to_string_lossy();
                raw.strip_prefix(r"\\?\").unwrap_or(&raw).to_owned()
            }
            _ => {
                path.to_string_lossy().into_owned()
            }
        }
    }
    fn webdriver_execute_optional_string(
        &self,
        webdriver_addr: &str,
        session_id: &str,
        script: &str,
    ) -> StdResult<Option<String>, String> {
        let path = build_webdriver_session_path(session_id, "/execute/sync");
        let escaped_script = self.json_escape(script);
        let mut body = String::with_capacity(
            escaped_script
                .len()
                .saturating_add(r#"{"script":"","args":[]}"#.len()),
        );
        body.push_str(r#"{"script":""#);
        body.push_str(&escaped_script);
        body.push_str(r#"","args":[]}"#);
        let response = self.http_request(webdriver_addr, "POST", &path, Some(&body))?;
        match self.extract_json_optional_string_by_key(&response, "value") {
            Some(JsonStringField::String(value)) => Ok(Some(value)),
            Some(JsonStringField::Null) => Ok(None),
            None => Err(prefixed_message("execute/sync 응답 파싱 실패: ", response)),
        }
    }
    fn webdriver_execute_string(
        &self,
        webdriver_addr: &str,
        session_id: &str,
        script: &str,
    ) -> StdResult<String, String> {
        self.webdriver_execute_optional_string(webdriver_addr, session_id, script)?
            .map_or_else(|| Err(String::from("execute/sync 응답이 null 입니다.")), Ok)
    }
    fn webdriver_get(
        &self,
        webdriver_addr: &str,
        session_id: &str,
        url: &str,
    ) -> StdResult<(), String> {
        let path = build_webdriver_session_path(session_id, "/url");
        let escaped_url = self.json_escape(url);
        let mut body =
            String::with_capacity(escaped_url.len().saturating_add(r#"{"url":""}"#.len()));
        body.push_str(r#"{"url":""#);
        body.push_str(&escaped_url);
        body.push_str(r#""}"#);
        self.http_request(webdriver_addr, "POST", &path, Some(&body))?;
        Ok(())
    }
    fn webdriver_new_session(
        &self,
        browser: BrowserKind,
        webdriver_addr: &str,
        download_dir: &Path,
    ) -> StdResult<String, String> {
        let dir_str = self.webdriver_download_dir_string(download_dir);
        let escaped_dir = self.json_escape(&dir_str);
        let browser_name = browser.browser_name();
        let options_key = browser.options_key();
        let mut body = String::with_capacity(
            escaped_dir
                .len()
                .saturating_add(browser_name.len())
                .saturating_add(options_key.len())
                .saturating_add(256),
        );
        body.push_str(r#"{"capabilities":{"alwaysMatch":{"browserName":""#);
        body.push_str(browser_name);
        body.push_str(r#"",""#);
        body.push_str(options_key);
        body.push_str(r#"":{"args":["--headless=new","--window-size=1920,1080","--disable-background-networking","--disable-default-apps","--disable-gpu","--disable-sync","--log-level=3","--no-first-run"],"excludeSwitches":["enable-logging"],"prefs":{"download.default_directory":""#);
        body.push_str(&escaped_dir);
        body.push_str(r#"","download.prompt_for_download":false,"download.directory_upgrade":true,"safebrowsing.enabled":true,"profile.default_content_setting_values.automatic_downloads":1}}}}}"#);
        let response = self.http_request(webdriver_addr, "POST", "/session", Some(&body))?;
        match self.extract_json_optional_string_by_key(&response, "sessionId") {
            Some(JsonStringField::String(session_id)) => Ok(session_id),
            _ => Err(prefixed_message("sessionId 파싱 실패: ", response)),
        }
    }
    fn webdriver_port(&self, webdriver_addr: &str) -> u16 {
        webdriver_addr
            .rsplit_once(':')
            .and_then(|(_, port)| port.parse::<u16>().ok())
            .unwrap_or(9515)
    }
    fn webdriver_setup_hint(&self) -> String {
        let capacity = BrowserKind::Chrome
            .driver_dir_name()
            .len()
            .saturating_add(BrowserKind::Chrome.driver_bin_name().len())
            .saturating_add(1);
        let mut chrome_hint = String::with_capacity(capacity);
        chrome_hint.push_str(BrowserKind::Chrome.driver_dir_name());
        chrome_hint.push('/');
        chrome_hint.push_str(BrowserKind::Chrome.driver_bin_name());
        let edge_capacity = BrowserKind::Edge
            .driver_dir_name()
            .len()
            .saturating_add(BrowserKind::Edge.driver_bin_name().len())
            .saturating_add(1);
        let mut edge_hint = String::with_capacity(edge_capacity);
        edge_hint.push_str(BrowserKind::Edge.driver_dir_name());
        edge_hint.push('/');
        edge_hint.push_str(BrowserKind::Edge.driver_bin_name());
        let output_capacity = BrowserKind::Chrome
            .driver_cmd()
            .len()
            .saturating_add(BrowserKind::Edge.driver_cmd().len())
            .saturating_add(chrome_hint.len())
            .saturating_add(edge_hint.len())
            .saturating_add(32);
        let mut out = String::with_capacity(output_capacity);
        out.push('`');
        out.push_str(BrowserKind::Chrome.driver_cmd());
        out.push_str("` 또는 `");
        out.push_str(BrowserKind::Edge.driver_cmd());
        out.push_str("` PATH 등록, 또는 프로젝트 내 `");
        out.push_str(&chrome_hint);
        out.push_str("` / `");
        out.push_str(&edge_hint);
        out.push_str("` 배치");
        out
    }
    fn webdriver_try_accept_alert(
        &self,
        webdriver_addr: &str,
        session_id: &str,
        timeout: Duration,
    ) -> StdResult<bool, String> {
        let start = Instant::now();
        loop {
            match self.webdriver_accept_alert(webdriver_addr, session_id) {
                Ok(()) => return Ok(true),
                Err(err) if err.contains("no such alert") => {
                    if start.elapsed() > timeout {
                        return Ok(false);
                    }
                    sleep(Duration::from_millis(200));
                }
                Err(err) => {
                    if start.elapsed() > timeout {
                        return Err(err);
                    }
                    sleep(Duration::from_millis(200));
                }
            }
        }
    }
    fn webdriver_version_mismatch_hint(&self, browser: BrowserKind, error: &str) -> &'static str {
        if error.contains("only supports Chrome version")
            || error.contains("Current browser version is")
            || error.contains("only supports Microsoft Edge version")
        {
            match browser {
                BrowserKind::Chrome => {
                    "\n설치된 Chrome과 ChromeDriver의 메이저 버전을 맞춰 주세요."
                }
                BrowserKind::Edge => "\n설치된 Edge와 EdgeDriver의 메이저 버전을 맞춰 주세요.",
            }
        } else {
            ""
        }
    }
}
fn build_webdriver_session_path(session_id: &str, suffix: &str) -> String {
    let capacity = "/session/"
        .len()
        .saturating_add(session_id.len())
        .saturating_add(suffix.len());
    let mut path = String::with_capacity(capacity);
    path.push_str("/session/");
    path.push_str(session_id);
    path.push_str(suffix);
    path
}
fn browser_source_message(browser_name: &str, label: &str, source: impl Display) -> String {
    let capacity = browser_name
        .len()
        .saturating_add(label.len())
        .saturating_add(64);
    let mut out = String::with_capacity(capacity);
    out.push_str(browser_name);
    out.push_str(label);
    push_display(&mut out, source);
    out
}
