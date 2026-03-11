use crate::{Result, err};
#[cfg(windows)]
use std::os::windows::process::CommandExt;
use std::{
    collections::HashSet,
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::LazyLock,
    thread::sleep,
    time::{Duration, Instant, SystemTime},
};
const WEBDRIVER_HOST: &str = "127.0.0.1";
const CHROMEDRIVER_CMD: &str = "chromedriver";
const CHROMEDRIVER_DIR_NAME: &str = "chromedriver";
const EDGEDRIVER_CMD: &str = "msedgedriver";
const EDGEDRIVER_DIR_NAME: &str = "edgedriver";
const OPDOWNLOAD_URL: &str = "https://www.opinet.co.kr/user/opdown/opDownload.do";
pub const AUTO_SOURCE_MARKER: &str = "__fcupdater_auto__";
const DOWNLOAD_WAIT_TIMEOUT: Duration = Duration::from_mins(3);
const RENAME_WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const TASK_SESSION_RETRY_LIMIT: usize = 2;
#[cfg(windows)]
const CREATE_NO_WINDOW: u32 = 0x0800_0000;
#[cfg(windows)]
const CHROMEDRIVER_BIN_NAME: &str = "chromedriver.exe";
#[cfg(not(windows))]
const CHROMEDRIVER_BIN_NAME: &str = "chromedriver";
#[cfg(windows)]
const EDGEDRIVER_BIN_NAME: &str = "msedgedriver.exe";
#[cfg(not(windows))]
const EDGEDRIVER_BIN_NAME: &str = "msedgedriver";
#[derive(Debug, Clone, Copy)]
struct Task {
    sido: &'static str,
    sigungu: &'static str,
}
struct TaskMatcher {
    sido_key: String,
    task_keys: Vec<String>,
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
struct ChildGuard {
    child: Option<Child>,
}
#[derive(Debug, Clone, Copy)]
enum BrowserKind {
    Chrome,
    Edge,
}
impl BrowserKind {
    const fn display_name(self) -> &'static str {
        match self {
            Self::Chrome => "Chrome",
            Self::Edge => "Edge",
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
    const fn driver_bin_name(self) -> &'static str {
        match self {
            Self::Chrome => CHROMEDRIVER_BIN_NAME,
            Self::Edge => EDGEDRIVER_BIN_NAME,
        }
    }
    const fn browser_name(self) -> &'static str {
        match self {
            Self::Chrome => "chrome",
            Self::Edge => "MicrosoftEdge",
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
    _driver: ChildGuard,
}
enum JsonStringField {
    String(String),
    Null,
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if let Some(child) = self.child.as_mut() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
pub fn refresh_sources(dir: &Path, prefix: &str) -> Result<Vec<PathBuf>> {
    fs::create_dir_all(dir)
        .map_err(|e| err(format!("소스 폴더 생성 실패: {} ({e})", dir.display())))?;
    let dir = dir
        .canonicalize()
        .map_err(|e| err(format!("소스 폴더 경로 확인 실패: {} ({e})", dir.display())))?;
    let removed = cleanup_auto_source_files(&dir, prefix)
        .map_err(|e| err(format!("기존 자동 소스 정리 실패: {e}")))?;
    if removed > 0 {
        println!("이전 임시 소스 파일 {removed}개 정리");
    }
    download_nationwide_source(&dir, prefix)
}
pub fn filter_target_region_records(
    records: Vec<crate::source_sync::SourceRecord>,
) -> Vec<crate::source_sync::SourceRecord> {
    records
        .into_iter()
        .filter(is_target_region_record)
        .collect()
}
pub fn is_target_region_record(record: &crate::source_sync::SourceRecord) -> bool {
    task_matchers()
        .iter()
        .any(|matcher| record_matches_task(record, matcher))
}
fn download_nationwide_source(dir: &Path, prefix: &str) -> Result<Vec<PathBuf>> {
    let mut errors = Vec::new();
    for browser in [BrowserKind::Chrome, BrowserKind::Edge] {
        let webdriver = match ensure_webdriver_for_browser(browser) {
            Ok(context) => context,
            Err(err) => {
                errors.push(format!(
                    "{} WebDriver 준비 실패: {err}",
                    browser.display_name()
                ));
                continue;
            }
        };
        match download_nationwide_source_with_retries(&webdriver.addr, browser, dir, prefix) {
            Ok(downloaded) => {
                println!("다운로드 완료: {}", downloaded.display());
                return Ok(vec![downloaded]);
            }
            Err(err) => {
                errors.push(format!("{} 다운로드 실패: {err}", browser.display_name()));
            }
        }
    }
    Err(err(format!(
        "Opinet 자동 다운로드 실패: {}\nChrome 또는 Edge 설치와 {}를 확인하세요.",
        errors.join("\n"),
        webdriver_setup_hint()
    )))
}
pub fn is_auto_source_file_name_folded(file_name: &str, prefix_fold: &str) -> bool {
    let folded = file_name.to_lowercase();
    folded.starts_with(prefix_fold) && folded.contains(AUTO_SOURCE_MARKER)
}
pub fn cleanup_downloaded_sources(paths: &[PathBuf]) -> Result<usize> {
    cleanup_downloaded_source_files(paths)
        .map_err(|e| err(format!("자동 소스 파일 정리 실패: {e}")))
}
fn record_matches_task(record: &crate::source_sync::SourceRecord, matcher: &TaskMatcher) -> bool {
    let region_key = crate::normalize_address_key(&record.region);
    let matches_task = |value: &str| matcher.task_keys.iter().any(|key| value.contains(key));
    if !region_key.is_empty() {
        if !region_key.contains(&matcher.sido_key) {
            return false;
        }
        if matches_task(&region_key) {
            return true;
        }
        if region_has_explicit_sigungu(&record.region) {
            return false;
        }
    }
    let combined = crate::normalize_address_key(&format!("{} {}", record.region, record.address));
    combined.contains(&matcher.sido_key) && matches_task(&combined)
}
fn region_has_explicit_sigungu(region: &str) -> bool {
    let mut tokens = region.split_whitespace().filter(|token| !token.is_empty());
    let Some(first) = tokens.next() else {
        return false;
    };
    if crate::strip_basic_region_suffix(first).is_some() {
        return true;
    }
    (crate::is_province_token(first) || crate::is_metropolitan_token(first))
        && tokens
            .next()
            .is_some_and(|second| crate::strip_basic_region_suffix(second).is_some())
}
fn task_match_keys(task: &Task) -> Vec<String> {
    let mut keys = Vec::new();
    for alias in sigungu_aliases(task.sigungu) {
        let alias_key = crate::normalize_address_key(alias);
        if !alias_key.is_empty() && !keys.contains(&alias_key) {
            keys.push(alias_key);
        }
        let stripped = strip_basic_region_suffix_owned(alias);
        if !stripped.is_empty() && !keys.contains(&stripped) {
            keys.push(stripped);
        }
    }
    let sigungu_key = crate::normalize_address_key(task.sigungu);
    if !sigungu_key.is_empty() && !keys.contains(&sigungu_key) {
        keys.push(sigungu_key);
    }
    keys
}
fn task_matchers() -> &'static [TaskMatcher] {
    static TASK_MATCHERS: LazyLock<Vec<TaskMatcher>> = LazyLock::new(|| {
        TASKS
            .iter()
            .map(|task| TaskMatcher {
                sido_key: crate::normalize_address_key(task.sido),
                task_keys: task_match_keys(task),
            })
            .collect()
    });
    TASK_MATCHERS.as_slice()
}
fn strip_basic_region_suffix_owned(value: &str) -> String {
    crate::strip_basic_region_suffix(value)
        .map(crate::normalize_address_key)
        .unwrap_or_default()
}
fn cleanup_auto_source_files(dir: &Path, prefix: &str) -> std::result::Result<usize, String> {
    let mut removed = 0usize;
    let prefix_fold = prefix.to_lowercase();
    let entries =
        fs::read_dir(dir).map_err(|e| format!("폴더 읽기 실패: {} ({e})", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("디렉터리 항목 읽기 실패: {e}"))?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !is_auto_source_file_name_folded(file_name, &prefix_fold) {
            continue;
        }
        fs::remove_file(&path)
            .map_err(|e| format!("자동 소스 파일 삭제 실패: {} ({e})", path.display()))?;
        removed = removed.saturating_add(1);
    }
    Ok(removed)
}
fn cleanup_downloaded_source_files(paths: &[PathBuf]) -> std::result::Result<usize, String> {
    let mut removed = 0usize;
    for path in paths {
        if !is_auto_source_path(path) {
            continue;
        }
        if !path
            .try_exists()
            .map_err(|e| format!("자동 소스 파일 경로 확인 실패: {} ({e})", path.display()))?
        {
            continue;
        }
        if !path_is_file(path)? {
            continue;
        }
        fs::remove_file(path)
            .map_err(|e| format!("자동 소스 파일 삭제 실패: {} ({e})", path.display()))?;
        removed = removed.saturating_add(1);
    }
    Ok(removed)
}
fn is_auto_source_path(path: &Path) -> bool {
    path.file_name()
        .and_then(|s| s.to_str())
        .is_some_and(|file_name| file_name.contains(AUTO_SOURCE_MARKER))
}
fn ensure_webdriver_for_browser(
    browser: BrowserKind,
) -> std::result::Result<WebDriverContext, String> {
    let webdriver_addr = reserve_webdriver_addr()?;
    let webdriver_port = webdriver_port(&webdriver_addr);
    let program = resolve_webdriver_program(browser)?;
    let mut command = Command::new(&program);
    let child = apply_webdriver_spawn_options(
        command
            .env("CHROME_LOG_FILE", os_dev_null())
            .env("MSEDGEDRIVER_TELEMETRY_OPTOUT", "1")
            .arg(format!("--port={webdriver_port}"))
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )
    .spawn()
    .map_err(|e| format!("`{}` 실행 실패: {e}", program.display()))?;
    let guard = ChildGuard { child: Some(child) };
    wait_for_webdriver_ready(&webdriver_addr, Duration::from_secs(15))?;
    Ok(WebDriverContext {
        addr: webdriver_addr,
        _driver: guard,
    })
}
fn resolve_webdriver_program(browser: BrowserKind) -> std::result::Result<PathBuf, String> {
    if let Some(candidate) = find_relative_webdriver(browser)? {
        return Ok(candidate);
    }
    Ok(PathBuf::from(browser.driver_cmd()))
}
fn find_relative_webdriver(browser: BrowserKind) -> std::result::Result<Option<PathBuf>, String> {
    let mut base_dirs = Vec::new();
    if let Ok(current_dir) = std::env::current_dir() {
        push_unique_path(&mut base_dirs, current_dir);
    }
    if let Ok(current_exe) = std::env::current_exe()
        && let Some(exe_dir) = current_exe.parent()
    {
        for ancestor in exe_dir.ancestors().take(3) {
            push_unique_path(&mut base_dirs, ancestor.to_path_buf());
        }
    }
    for base_dir in base_dirs {
        for candidate in webdriver_candidates_from_base(browser, &base_dir) {
            if path_is_file(&candidate)? {
                return Ok(Some(candidate));
            }
        }
    }
    Ok(None)
}
fn webdriver_candidates_from_base(browser: BrowserKind, base_dir: &Path) -> [PathBuf; 2] {
    [
        base_dir.join(browser.driver_bin_name()),
        base_dir
            .join(browser.driver_dir_name())
            .join(browser.driver_bin_name()),
    ]
}
fn reserve_webdriver_addr() -> std::result::Result<String, String> {
    for _ in 0..32 {
        let listener_v4 = TcpListener::bind((WEBDRIVER_HOST, 0))
            .map_err(|e| format!("빈 WebDriver 포트 확보 실패: {e}"))?;
        let port = listener_v4
            .local_addr()
            .map_err(|e| format!("할당 포트 확인 실패: {e}"))?
            .port();
        match TcpListener::bind(("::1", port)) {
            Ok(listener_v6) => {
                drop(listener_v6);
                drop(listener_v4);
                return Ok(format!("{WEBDRIVER_HOST}:{port}"));
            }
            Err(err) if err.kind() == std::io::ErrorKind::AddrInUse => {}
            Err(err) if err.kind() == std::io::ErrorKind::AddrNotAvailable => {
                return Err(
                    "IPv6 loopback(::1)을 사용할 수 없습니다. 현재 ChromeDriver는 IPv6 바인딩이 가능한 환경이 필요합니다."
                        .to_string(),
                );
            }
            Err(err) => {
                return Err(format!("IPv6 포트 확인 실패: {err}"));
            }
        }
    }
    Err("사용 가능한 WebDriver 포트를 찾지 못했습니다.".to_string())
}
fn webdriver_port(webdriver_addr: &str) -> u16 {
    webdriver_addr
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .unwrap_or(9515)
}
fn path_is_file(path: &Path) -> std::result::Result<bool, String> {
    if !path
        .try_exists()
        .map_err(|e| format!("경로 확인 실패: {} ({e})", path.display()))?
    {
        return Ok(false);
    }
    fs::metadata(path)
        .map(|metadata| metadata.is_file())
        .map_err(|e| format!("메타데이터 확인 실패: {} ({e})", path.display()))
}
fn push_unique_path(paths: &mut Vec<PathBuf>, candidate: PathBuf) {
    if !paths.contains(&candidate) {
        paths.push(candidate);
    }
}
fn download_nationwide_source_with_retries(
    webdriver_addr: &str,
    browser: BrowserKind,
    download_dir: &Path,
    prefix: &str,
) -> std::result::Result<PathBuf, String> {
    let mut last_error = None;
    for attempt in 1..=TASK_SESSION_RETRY_LIMIT {
        match download_nationwide_source_once(webdriver_addr, browser, download_dir, prefix) {
            Ok(path) => return Ok(path),
            Err(err) => {
                let should_retry =
                    attempt < TASK_SESSION_RETRY_LIMIT && is_recoverable_session_error(&err);
                last_error = Some(err);
                if should_retry {
                    println!(
                        "다운로드 재시도 {attempt}/{TASK_SESSION_RETRY_LIMIT}: 브라우저 세션을 다시 시작합니다."
                    );
                    sleep(Duration::from_secs(2));
                    continue;
                }
                break;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| "다운로드 실패".to_string()))
}
fn download_nationwide_source_once(
    webdriver_addr: &str,
    browser: BrowserKind,
    download_dir: &Path,
    prefix: &str,
) -> std::result::Result<PathBuf, String> {
    let session_id = webdriver_new_session(browser, webdriver_addr, download_dir).map_err(|e| {
        format!(
            "브라우저 세션 생성 실패: {e}{}",
            webdriver_version_mismatch_hint(browser, &e)
        )
    })?;
    let result =
        download_nationwide_source_in_session(webdriver_addr, &session_id, download_dir, prefix);
    let _ = webdriver_delete_session(webdriver_addr, &session_id);
    result
}
fn download_nationwide_source_in_session(
    webdriver_addr: &str,
    session_id: &str,
    download_dir: &Path,
    prefix: &str,
) -> std::result::Result<PathBuf, String> {
    webdriver_get(webdriver_addr, session_id, OPDOWNLOAD_URL)?;
    wait_until(
        webdriver_addr,
        session_id,
        OPDOWNLOAD_PAGE_READY_SCRIPT,
        "READY",
        Duration::from_secs(30),
        Duration::from_millis(500),
        "opDownload 페이지 로딩",
    )?;
    sleep(Duration::from_secs(2));
    let before = snapshot_files(download_dir)?;
    let trigger = match webdriver_execute_optional_string(
        webdriver_addr,
        session_id,
        OPDOWNLOAD_TRIGGER_SCRIPT,
    ) {
        Ok(Some(value)) => value,
        Ok(None) => "OK|null".to_string(),
        Err(e) => return Err(format!("opDownload 다운로드 트리거 실행 실패: {e}")),
    };
    let _ = webdriver_try_accept_alert(webdriver_addr, session_id, Duration::from_secs(5));
    if !trigger.starts_with("OK|") {
        let discovery =
            webdriver_execute_string(webdriver_addr, session_id, OPDOWNLOAD_DISCOVERY_SCRIPT)
                .unwrap_or_else(|err| format!("후보 컨트롤 조회 실패: {err}"));
        return Err(format!(
            "opDownload 다운로드 트리거를 찾지 못했습니다.\n트리거 결과: {trigger}\n후보 컨트롤:\n{discovery}"
        ));
    }
    let downloaded = wait_for_new_download(download_dir, &before, DOWNLOAD_WAIT_TIMEOUT).map_err(|e| {
        let diagnostic = webdriver_execute_string(webdriver_addr, session_id, OPDOWNLOAD_DIAGNOSTIC_SCRIPT)
            .unwrap_or_else(|diag_err| format!("진단 조회 실패: {diag_err}"));
        format!(
            "opDownload 파일 다운로드 대기 실패: {e}\n트리거 결과: {trigger}\n현재 페이지: {diagnostic}"
        )
    })?;
    let ext = downloaded
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("xls");
    let renamed = download_dir.join(build_nationwide_auto_source_name(prefix, ext));
    rename_with_retries(&downloaded, &renamed, RENAME_WAIT_TIMEOUT).map_err(|e| {
        format!(
            "전국 소스 파일 이름 변경 실패: {} -> {} ({e})",
            downloaded.display(),
            renamed.display()
        )
    })?;
    Ok(renamed)
}
fn build_nationwide_auto_source_name(prefix: &str, ext: &str) -> String {
    format!("{prefix}{AUTO_SOURCE_MARKER}_opdownload_current_price.{ext}")
}
fn wait_for_webdriver_ready(
    webdriver_addr: &str,
    timeout: Duration,
) -> std::result::Result<(), String> {
    let start = Instant::now();
    let mut last_error = String::from("아직 /status 응답이 없습니다.");
    loop {
        if start.elapsed() > timeout {
            return Err(format!("WebDriver 준비 대기 시간 초과: {last_error}"));
        }
        match http_request(webdriver_addr, "GET", "/status", None) {
            Ok(response) if webdriver_status_is_ready(&response) => return Ok(()),
            Ok(response) => {
                last_error = format!("WebDriver 준비 전 응답: {response}");
            }
            Err(err) => {
                last_error = err;
            }
        }
        sleep(Duration::from_millis(200));
    }
}
fn webdriver_status_is_ready(response: &str) -> bool {
    response.contains(r#""ready":true"#)
        || response.contains(r#""ready": true"#)
        || response.contains("ChromeDriver ready for new sessions")
}
fn webdriver_new_session(
    browser: BrowserKind,
    webdriver_addr: &str,
    download_dir: &Path,
) -> std::result::Result<String, String> {
    let dir_str = webdriver_download_dir_string(download_dir);
    let body = format!(
        r#"{{"capabilities":{{"alwaysMatch":{{"browserName":"{}","{}":{{"args":["--headless=new","--window-size=1920,1080","--disable-background-networking","--disable-default-apps","--disable-gpu","--disable-sync","--log-level=3","--no-first-run"],"excludeSwitches":["enable-logging"],"prefs":{{"download.default_directory":"{}","download.prompt_for_download":false,"download.directory_upgrade":true,"safebrowsing.enabled":true,"profile.default_content_setting_values.automatic_downloads":1}}}}}}}}}}"#,
        browser.browser_name(),
        browser.options_key(),
        json_escape(&dir_str)
    );
    let response = http_request(webdriver_addr, "POST", "/session", Some(&body))?;
    match extract_json_optional_string_by_key(&response, "sessionId") {
        Some(JsonStringField::String(session_id)) => Ok(session_id),
        _ => Err(format!("sessionId 파싱 실패: {response}")),
    }
}
fn webdriver_delete_session(
    webdriver_addr: &str,
    session_id: &str,
) -> std::result::Result<(), String> {
    let path = format!("/session/{session_id}");
    let _ = http_request(webdriver_addr, "DELETE", &path, None)?;
    Ok(())
}
fn webdriver_get(
    webdriver_addr: &str,
    session_id: &str,
    url: &str,
) -> std::result::Result<(), String> {
    let path = format!("/session/{session_id}/url");
    let body = format!(r#"{{"url":"{}"}}"#, json_escape(url));
    let _ = http_request(webdriver_addr, "POST", &path, Some(&body))?;
    Ok(())
}
fn webdriver_try_accept_alert(
    webdriver_addr: &str,
    session_id: &str,
    timeout: Duration,
) -> std::result::Result<bool, String> {
    let start = Instant::now();
    loop {
        match webdriver_accept_alert(webdriver_addr, session_id) {
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
fn webdriver_accept_alert(
    webdriver_addr: &str,
    session_id: &str,
) -> std::result::Result<(), String> {
    let path = format!("/session/{session_id}/alert/accept");
    let _ = http_request(webdriver_addr, "POST", &path, Some("{}"))?;
    Ok(())
}
fn webdriver_execute_string(
    webdriver_addr: &str,
    session_id: &str,
    script: &str,
) -> std::result::Result<String, String> {
    webdriver_execute_optional_string(webdriver_addr, session_id, script)?
        .map_or_else(|| Err("execute/sync 응답이 null 입니다.".to_string()), Ok)
}
fn webdriver_execute_optional_string(
    webdriver_addr: &str,
    session_id: &str,
    script: &str,
) -> std::result::Result<Option<String>, String> {
    let path = format!("/session/{session_id}/execute/sync");
    let body = format!(r#"{{"script":"{}","args":[]}}"#, json_escape(script));
    let response = http_request(webdriver_addr, "POST", &path, Some(&body))?;
    match extract_json_optional_string_by_key(&response, "value") {
        Some(JsonStringField::String(value)) => Ok(Some(value)),
        Some(JsonStringField::Null) => Ok(None),
        None => Err(format!("execute/sync 응답 파싱 실패: {response}")),
    }
}
fn http_request(
    webdriver_addr: &str,
    method: &str,
    path: &str,
    body: Option<&str>,
) -> std::result::Result<String, String> {
    let mut stream =
        TcpStream::connect(webdriver_addr).map_err(|e| format!("WebDriver 연결 실패: {e}"))?;
    let _ = stream.set_read_timeout(Some(Duration::from_mins(1)));
    let _ = stream.set_write_timeout(Some(Duration::from_mins(1)));
    let body = body.unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: {webdriver_addr}\r\n\
         Connection: close\r\n\
         Content-Type: application/json; charset=utf-8\r\n\
         Content-Length: {}\r\n\
         \r\n\
         {}",
        body.len(),
        body
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|e| format!("요청 전송 실패: {e}"))?;
    let _ = stream.flush();
    let raw = read_http_response(&mut stream)?;
    let (status, response_body) = split_http_response(&raw)?;
    if !(200..300).contains(&status) {
        return Err(format!("HTTP {status} 오류: {response_body}"));
    }
    Ok(response_body.to_string())
}
fn read_http_response(stream: &mut TcpStream) -> std::result::Result<String, String> {
    let mut raw = Vec::new();
    let mut expected_total_len = None;
    loop {
        let mut chunk = [0u8; 4096];
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(read) => {
                raw.extend_from_slice(&chunk[..read]);
                if expected_total_len.is_none()
                    && let Some((header_end, separator_len)) = find_http_header_end(&raw)
                {
                    let header = String::from_utf8_lossy(&raw[..header_end]);
                    if let Some(content_length) = parse_content_length(&header)? {
                        expected_total_len = Some(header_end + separator_len + content_length);
                    }
                }
                if expected_total_len.is_some_and(|expected| raw.len() >= expected) {
                    break;
                }
            }
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock
                ) =>
            {
                if raw.is_empty() {
                    return Err("HTTP 응답이 비어 있습니다".to_string());
                }
                break;
            }
            Err(err) => return Err(format!("응답 수신 실패: {err}")),
        }
    }
    if raw.is_empty() {
        return Err("HTTP 응답이 비어 있습니다".to_string());
    }
    if let Some(expected) = expected_total_len
        && raw.len() < expected
    {
        return Err(format!(
            "HTTP 응답 본문이 끝나기 전에 연결이 종료되었습니다. (received={}, expected={expected})",
            raw.len()
        ));
    }
    Ok(String::from_utf8_lossy(&raw).into_owned())
}
fn find_http_header_end(raw: &[u8]) -> Option<(usize, usize)> {
    raw.array_windows::<4>()
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| (pos, 4))
        .or_else(|| {
            raw.array_windows::<2>()
                .position(|window| window == b"\n\n")
                .map(|pos| (pos, 2))
        })
}
fn parse_content_length(header: &str) -> std::result::Result<Option<usize>, String> {
    for line in header.lines() {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        if name.eq_ignore_ascii_case("content-length") {
            let value = value.trim();
            let length = value
                .parse::<usize>()
                .map_err(|err| format!("Content-Length 파싱 실패: {value} ({err})"))?;
            return Ok(Some(length));
        }
    }
    Ok(None)
}
fn split_http_response(raw: &str) -> std::result::Result<(u16, &str), String> {
    if raw.trim().is_empty() {
        return Err("HTTP 응답이 비어 있습니다".to_string());
    }
    let status_line = raw
        .lines()
        .find(|line| !line.is_empty())
        .ok_or_else(|| "HTTP 상태줄을 읽지 못했습니다".to_string())?;
    let mut parts = status_line.split_whitespace();
    let _http = parts.next();
    let code = parts
        .next()
        .ok_or_else(|| format!("HTTP 상태코드 없음: {status_line}"))?
        .parse::<u16>()
        .map_err(|e| format!("HTTP 상태코드 파싱 실패: {e}"))?;
    let body = raw
        .split_once("\r\n\r\n")
        .or_else(|| raw.split_once("\n\n"))
        .map(|(_, body)| body)
        .ok_or_else(|| "HTTP 본문을 찾지 못했습니다".to_string())?;
    Ok((code, body))
}
fn extract_json_optional_string_by_key(json: &str, key: &str) -> Option<JsonStringField> {
    let needle = format!(r#""{key}""#);
    let start = json.find(&needle)?;
    let bytes = json.as_bytes();
    let mut i = start + needle.len();
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() || bytes[i] != b':' {
        return None;
    }
    i += 1;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i + 3 < bytes.len() && &bytes[i..i + 4] == b"null" {
        return Some(JsonStringField::Null);
    }
    if i >= bytes.len() || bytes[i] != b'"' {
        return None;
    }
    i += 1;
    let mut out = String::new();
    let mut segment_start = i;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                if segment_start < i {
                    out.push_str(&String::from_utf8_lossy(&bytes[segment_start..i]));
                }
                return Some(JsonStringField::String(out));
            }
            b'\\' => {
                if segment_start < i {
                    out.push_str(&String::from_utf8_lossy(&bytes[segment_start..i]));
                }
                i += 1;
                if i >= bytes.len() {
                    return None;
                }
                match bytes[i] {
                    b'"' => out.push('"'),
                    b'\\' => out.push('\\'),
                    b'/' => out.push('/'),
                    b'b' => out.push('\u{0008}'),
                    b'f' => out.push('\u{000C}'),
                    b'n' => out.push('\n'),
                    b'r' => out.push('\r'),
                    b't' => out.push('\t'),
                    b'u' => {
                        if i + 4 >= bytes.len() {
                            return None;
                        }
                        let hex = &json[i + 1..i + 5];
                        let code = u16::from_str_radix(hex, 16).ok()?;
                        let ch = char::from_u32(u32::from(code))?;
                        out.push(ch);
                        i += 4;
                    }
                    _ => return None,
                }
                segment_start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    None
}
fn json_escape(input: &str) -> String {
    let mut out = String::with_capacity(input.len() + 16);
    for ch in input.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (u32::from(c)) < 0x20 => {
                let escaped = format!("\\u{:04x}", u32::from(c));
                out.push_str(&escaped);
            }
            c => out.push(c),
        }
    }
    out
}
fn wait_until(
    webdriver_addr: &str,
    session_id: &str,
    script: &str,
    expected: &str,
    timeout: Duration,
    interval: Duration,
    label: &str,
) -> std::result::Result<(), String> {
    let start = Instant::now();
    loop {
        let value = webdriver_execute_string(webdriver_addr, session_id, script)?;
        if value == expected {
            return Ok(());
        }
        if start.elapsed() > timeout {
            return Err(format!("대기 시간 초과: {label}"));
        }
        sleep(interval);
    }
}
fn sigungu_aliases(name: &str) -> Vec<&str> {
    match name {
        "세종시" => vec!["세종시", "세종특별자치시"],
        _ => vec![name],
    }
}
const OPDOWNLOAD_PAGE_READY_SCRIPT: &str = r#"
return (function() {
  if (document.readyState !== "complete") return "";
  var bodyText = document.body ? String(document.body.innerText || document.body.textContent || "") : "";
  bodyText = bodyText.replace(/\s+/g, " ").trim();
  if (!bodyText) return "";
  if (!/(사업자별|판매가격|엑셀|다운로드)/.test(bodyText)) return "";
  return "READY";
})();"#;
const OPDOWNLOAD_DISCOVERY_SCRIPT: &str = r#"
return (function() {
  function clean(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }
  function attr(el, name) {
    return clean(el && el.getAttribute ? el.getAttribute(name) : "");
  }
  function textOf(el) {
    if (!el) return "";
    return clean(el.innerText || el.textContent || el.value || attr(el, "aria-label") || attr(el, "title") || attr(el, "alt"));
  }
  function isVisible(el) {
    if (!el) return false;
    if (el.hidden) return false;
    var style = window.getComputedStyle ? window.getComputedStyle(el) : null;
    if (style && (style.display === "none" || style.visibility === "hidden")) return false;
    return !!(el.offsetWidth || el.offsetHeight || (el.getClientRects && el.getClientRects().length));
  }
  function contextOf(el) {
    var cur = el;
    while (cur && cur !== document.body) {
      var tag = cur.tagName ? cur.tagName.toLowerCase() : "";
      if (/^(tr|li|dd|dt|p|div|section|article|td|th|form)$/.test(tag)) {
        var text = clean(cur.innerText || cur.textContent || "");
        if (text && text.length <= 260) return text;
      }
      cur = cur.parentElement;
    }
    return "";
  }
  function pushLine(lines, el) {
    var text = textOf(el);
    var href = attr(el, "href");
    var onclick = attr(el, "onclick");
    var ctx = contextOf(el);
    var blob = [text, href, onclick, ctx].join(" ");
    if (!/(사업자별|현재|판매가격|엑셀|다운로드|저장|excel|download|xls|xlsx)/i.test(blob)) return;
    lines.push([
      "el",
      (el.tagName || "").toLowerCase(),
      "id=" + attr(el, "id"),
      "name=" + attr(el, "name"),
      "type=" + attr(el, "type"),
      "text=" + text,
      "href=" + href,
      "onclick=" + onclick,
      "ctx=" + ctx
    ].join(" | "));
  }
  var lines = [];
  lines.push("title=" + clean(document.title));
  lines.push("url=" + clean(location.href));
  lines.push("body=" + clean(document.body ? (document.body.innerText || document.body.textContent || "") : "").slice(0, 400));
  if (typeof fn_Download === "function") {
    lines.push("fn_Download=" + clean(String(fn_Download)).slice(0, 2000));
  }
  var forms = Array.prototype.slice.call(document.forms || []);
  for (var f = 0; f < forms.length; f++) {
    var form = forms[f];
    lines.push([
      "form",
      "id=" + attr(form, "id"),
      "name=" + attr(form, "name"),
      "method=" + attr(form, "method"),
      "action=" + attr(form, "action"),
      "target=" + attr(form, "target")
    ].join(" | "));
    var inputs = Array.prototype.slice.call(form.querySelectorAll('input[type="hidden"],input[type="text"],input[type="radio"],select'));
    for (var p = 0; p < inputs.length; p++) {
      var input = inputs[p];
      lines.push([
        "field",
        "form=" + (attr(form, "id") || attr(form, "name")),
        "tag=" + (input.tagName || "").toLowerCase(),
        "type=" + attr(input, "type"),
        "name=" + attr(input, "name"),
        "id=" + attr(input, "id"),
        "value=" + clean(input.value || ""),
        "checked=" + (input.checked ? "Y" : "N")
      ].join(" | "));
    }
  }
  var all = Array.prototype.slice.call(document.querySelectorAll('a,button,input[type="button"],input[type="submit"],input[type="image"],*[onclick]'));
  for (var i = 0; i < all.length; i++) {
    if (!isVisible(all[i])) continue;
    pushLine(lines, all[i]);
  }
  return lines.join("\n");
})();"#;
const OPDOWNLOAD_TRIGGER_SCRIPT: &str = r#"
return (function() {
  function clean(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }
  function attr(el, name) {
    return clean(el && el.getAttribute ? el.getAttribute(name) : "");
  }
  function textOf(el) {
    if (!el) return "";
    return clean(el.innerText || el.textContent || el.value || attr(el, "aria-label") || attr(el, "title") || attr(el, "alt"));
  }
  function isVisible(el) {
    if (!el) return false;
    if (el.hidden) return false;
    var style = window.getComputedStyle ? window.getComputedStyle(el) : null;
    if (style && (style.display === "none" || style.visibility === "hidden")) return false;
    return !!(el.offsetWidth || el.offsetHeight || (el.getClientRects && el.getClientRects().length));
  }
  function collectClickables(root) {
    var items = [];
    if (root && root.matches && root.matches('a,button,input[type="button"],input[type="submit"],input[type="image"],*[onclick]')) {
      items.push(root);
    }
    if (root && root.querySelectorAll) {
      var descendants = root.querySelectorAll('a,button,input[type="button"],input[type="submit"],input[type="image"],*[onclick]');
      for (var i = 0; i < descendants.length; i++) items.push(descendants[i]);
    }
    return items.filter(isVisible);
  }
  function contextOf(el) {
    var cur = el;
    var fallback = "";
    while (cur && cur !== document.body) {
      var tag = cur.tagName ? cur.tagName.toLowerCase() : "";
      if (/^(tr|li|dd|dt|p|div|section|article|td|th|form)$/.test(tag)) {
        var text = clean(cur.innerText || cur.textContent || "");
        if (text && !fallback) fallback = text;
        if (text && text.length <= 320 && /(사업자별|판매가격|현재)/.test(text)) return text;
      }
      cur = cur.parentElement;
    }
    return fallback.slice(0, 320);
  }
  function score(blob) {
    var total = 0;
    if (/사업자별/.test(blob)) total += 25;
    if (/현재 판매가격/.test(blob)) total += 25;
    if (/판매가격/.test(blob)) total += 16;
    if (/현재/.test(blob)) total += 4;
    if (/(엑셀|excel)/i.test(blob)) total += 14;
    if (/(다운로드|저장)/.test(blob)) total += 10;
    if (/(download|xls|xlsx)/i.test(blob)) total += 8;
    return total;
  }
  function click(el) {
    try { el.scrollIntoView({ block: "center" }); } catch (e) {}
    if (typeof el.click === "function") {
      el.click();
      return;
    }
    var evt = document.createEvent("MouseEvents");
    evt.initMouseEvent("click", true, true, window, 1);
    el.dispatchEvent(evt);
  }
  if (typeof fn_Download === "function") {
    fn_Download(2);
    return "OK|fn_Download(2)|target=사업자별 현재 판매가격 엑셀";
  }
  var direct = document.querySelector('a[href*="fn_Download(2)"]');
  if (direct && isVisible(direct)) {
    click(direct);
    return "OK|href=fn_Download(2)|target=사업자별 현재 판매가격 엑셀";
  }
  var best = null;
  var containers = Array.prototype.slice.call(document.querySelectorAll("tr,li,dd,dt,p,div,section,article,td,th,form"));
  for (var i = 0; i < containers.length; i++) {
    var ctx = clean(containers[i].innerText || containers[i].textContent || "");
    if (!ctx || ctx.length > 320) continue;
    if (!/사업자별/.test(ctx) || !/(현재 판매가격|판매가격)/.test(ctx)) continue;
    var clickables = collectClickables(containers[i]);
    for (var j = 0; j < clickables.length; j++) {
      var el = clickables[j];
      var blob = [ctx, textOf(el), attr(el, "href"), attr(el, "onclick"), attr(el, "title")].join(" ");
      var candidate = {
        el: el,
        score: score(blob),
        text: textOf(el),
        href: attr(el, "href"),
        onclick: attr(el, "onclick"),
        ctx: ctx,
        tag: (el.tagName || "").toLowerCase()
      };
      if (!best || candidate.score > best.score || (candidate.score === best.score && candidate.ctx.length < best.ctx.length)) {
        best = candidate;
      }
    }
  }
  if (!best) {
    var all = collectClickables(document);
    for (var k = 0; k < all.length; k++) {
      var item = all[k];
      var ctx2 = contextOf(item);
      var blob2 = [ctx2, textOf(item), attr(item, "href"), attr(item, "onclick"), attr(item, "title")].join(" ");
      if (!/사업자별/.test(blob2) || !/(현재 판매가격|판매가격)/.test(blob2)) continue;
      if (!/(엑셀|다운로드|저장|excel|download|xls|xlsx)/i.test(blob2)) continue;
      var fallback = {
        el: item,
        score: score(blob2),
        text: textOf(item),
        href: attr(item, "href"),
        onclick: attr(item, "onclick"),
        ctx: ctx2,
        tag: (item.tagName || "").toLowerCase()
      };
      if (!best || fallback.score > best.score) best = fallback;
    }
  }
  if (!best || best.score < 25) {
    return "ERR:NO_TARGET";
  }
  click(best.el);
  return [
    "OK",
    "tag=" + best.tag,
    "score=" + String(best.score),
    "text=" + best.text,
    "href=" + best.href,
    "onclick=" + best.onclick,
    "ctx=" + best.ctx
  ].join("|");
})();"#;
const OPDOWNLOAD_DIAGNOSTIC_SCRIPT: &str = r#"
return (function() {
  function clean(value) {
    return String(value || "").replace(/\s+/g, " ").trim();
  }
  return [
    "title=" + clean(document.title),
    "url=" + clean(location.href),
    "ready=" + clean(document.readyState),
    "body=" + clean(document.body ? (document.body.innerText || document.body.textContent || "") : "").slice(0, 500)
  ].join(" | ");
})();"#;
fn snapshot_files(dir: &Path) -> std::result::Result<HashSet<PathBuf>, String> {
    let mut set = HashSet::new();
    if !dir
        .try_exists()
        .map_err(|e| format!("다운로드 폴더 경로 확인 실패: {} ({e})", dir.display()))?
    {
        return Ok(set);
    }
    let entries = fs::read_dir(dir)
        .map_err(|e| format!("다운로드 폴더 읽기 실패: {} ({e})", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("디렉터리 항목 읽기 실패: {e}"))?;
        let path = entry.path();
        if path.is_file() {
            set.insert(path);
        }
    }
    Ok(set)
}
fn wait_for_new_download(
    dir: &Path,
    before: &HashSet<PathBuf>,
    timeout: Duration,
) -> std::result::Result<PathBuf, String> {
    let start = Instant::now();
    loop {
        let mut latest_complete: Option<(Option<SystemTime>, PathBuf)> = None;
        let mut temp_exists = false;
        if dir
            .try_exists()
            .map_err(|e| format!("다운로드 폴더 경로 확인 실패: {} ({e})", dir.display()))?
        {
            let entries = fs::read_dir(dir)
                .map_err(|e| format!("다운로드 폴더 읽기 실패: {} ({e})", dir.display()))?;
            for entry in entries {
                let entry = entry.map_err(|e| format!("디렉터리 항목 읽기 실패: {e}"))?;
                let path = entry.path();
                if !path.is_file() || before.contains(&path) {
                    continue;
                }
                let ext = path
                    .extension()
                    .and_then(|s| s.to_str())
                    .unwrap_or_default();
                if ext.eq_ignore_ascii_case("xls") || ext.eq_ignore_ascii_case("xlsx") {
                    let modified = fs::metadata(&path).and_then(|meta| meta.modified()).ok();
                    let should_replace =
                        latest_complete
                            .as_ref()
                            .is_none_or(|(best_modified, best_path)| {
                                modified > *best_modified
                                    || (modified == *best_modified && path > *best_path)
                            });
                    if should_replace {
                        latest_complete = Some((modified, path));
                    }
                } else if ext.eq_ignore_ascii_case("crdownload")
                    || ext.eq_ignore_ascii_case("part")
                    || ext.eq_ignore_ascii_case("tmp")
                {
                    temp_exists = true;
                }
            }
        }
        if let Some((_, path)) = latest_complete
            && !temp_exists
        {
            return Ok(path);
        }
        if start.elapsed() > timeout {
            return Err("다운로드 완료 파일을 찾지 못했습니다".to_string());
        }
        sleep(Duration::from_millis(500));
    }
}
fn rename_with_retries(
    source: &Path,
    target: &Path,
    timeout: Duration,
) -> std::result::Result<(), String> {
    let start = Instant::now();
    let mut last_error = None;
    loop {
        match fs::rename(source, target) {
            Ok(()) => return Ok(()),
            Err(error) => {
                if !is_transient_rename_error(&error) || start.elapsed() > timeout {
                    return Err(last_error.unwrap_or_else(|| error.to_string()));
                }
                last_error = Some(error.to_string());
                sleep(Duration::from_millis(250));
            }
        }
    }
}
fn is_transient_rename_error(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::PermissionDenied | std::io::ErrorKind::WouldBlock
    ) || matches!(error.raw_os_error(), Some(32 | 33))
}
#[cfg(windows)]
fn apply_webdriver_spawn_options(command: &mut Command) -> &mut Command {
    command.creation_flags(CREATE_NO_WINDOW)
}
#[cfg(not(windows))]
fn apply_webdriver_spawn_options(command: &mut Command) -> &mut Command {
    command
}
#[cfg(windows)]
fn webdriver_download_dir_string(path: &Path) -> String {
    let raw = path.to_string_lossy();
    raw.strip_prefix(r"\\?\").unwrap_or(&raw).to_string()
}
#[cfg(not(windows))]
fn webdriver_download_dir_string(path: &Path) -> String {
    path.to_string_lossy().into_owned()
}
#[cfg(windows)]
const fn os_dev_null() -> &'static str {
    "NUL"
}
#[cfg(not(windows))]
const fn os_dev_null() -> &'static str {
    "/dev/null"
}
fn project_relative_driver_hint(browser: BrowserKind) -> String {
    format!(
        "{}/{}",
        browser.driver_dir_name(),
        browser.driver_bin_name()
    )
}
fn webdriver_setup_hint() -> String {
    format!(
        "`{}` 또는 `{}` PATH 등록, 또는 프로젝트 내 `{}` / `{}` 배치",
        BrowserKind::Chrome.driver_cmd(),
        BrowserKind::Edge.driver_cmd(),
        project_relative_driver_hint(BrowserKind::Chrome),
        project_relative_driver_hint(BrowserKind::Edge)
    )
}
fn webdriver_version_mismatch_hint(browser: BrowserKind, error: &str) -> &'static str {
    if error.contains("only supports Chrome version")
        || error.contains("Current browser version is")
        || error.contains("only supports Microsoft Edge version")
    {
        match browser {
            BrowserKind::Chrome => "\n설치된 Chrome과 ChromeDriver의 메이저 버전을 맞춰 주세요.",
            BrowserKind::Edge => "\n설치된 Edge와 EdgeDriver의 메이저 버전을 맞춰 주세요.",
        }
    } else {
        ""
    }
}
fn is_recoverable_session_error(error: &str) -> bool {
    error.contains("invalid session id")
        || error.contains("session deleted as the browser has closed the connection")
        || error.contains("disconnected: not connected to DevTools")
        || error.contains("chrome not reachable")
}
