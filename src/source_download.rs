use crate::{Result, err};
use std::{
    collections::HashSet,
    fs,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread::sleep,
    time::{Duration, Instant},
};
const WEBDRIVER_HOST: &str = "127.0.0.1";
const CHROMEDRIVER_CMD: &str = "chromedriver";
const CHROMEDRIVER_DIR_NAME: &str = "chromedriver";
const EDGEDRIVER_CMD: &str = "msedgedriver";
const EDGEDRIVER_DIR_NAME: &str = "edgedriver";
const OPINET_URL: &str = "https://www.opinet.co.kr/searRgSelect.do";
pub const AUTO_SOURCE_MARKER: &str = "__fcupdater_auto__";
const DOWNLOAD_WAIT_TIMEOUT: Duration = Duration::from_secs(180);
const TASK_SESSION_RETRY_LIMIT: usize = 2;
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
    browser: BrowserKind,
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
        eprintln!("[소스 다운로드] 이전 자동 생성 파일 {removed}개 정리");
    }
    let webdriver = ensure_webdriver(&dir).map_err(|e| {
        err(format!(
            "Chrome/Edge WebDriver 준비 실패: {e}\nChrome 또는 Edge 설치 및 {}를 확인하세요.",
            webdriver_setup_hint()
        ))
    })?;
    run_tasks(&webdriver.addr, webdriver.browser, &dir, prefix)
        .map_err(|e| err(format!("Opinet 다운로드 실패: {e}")))
}
pub fn is_auto_source_file_name(file_name: &str, prefix: &str) -> bool {
    let folded = file_name.to_lowercase();
    folded.starts_with(&prefix.to_lowercase()) && folded.contains(AUTO_SOURCE_MARKER)
}
pub fn cleanup_downloaded_sources(paths: &[PathBuf]) -> Result<usize> {
    cleanup_downloaded_source_files(paths)
        .map_err(|e| err(format!("자동 소스 파일 정리 실패: {e}")))
}
fn cleanup_auto_source_files(dir: &Path, prefix: &str) -> std::result::Result<usize, String> {
    let mut removed = 0usize;
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
        if !is_auto_source_file_name(file_name, prefix) {
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
fn ensure_webdriver(download_dir: &Path) -> std::result::Result<WebDriverContext, String> {
    let mut errors = Vec::new();
    for browser in [BrowserKind::Chrome, BrowserKind::Edge] {
        match ensure_webdriver_for_browser(browser, download_dir) {
            Ok(context) => return Ok(context),
            Err(err) => errors.push(format!("{}: {err}", browser.display_name())),
        }
    }
    Err(errors.join("\n"))
}
fn ensure_webdriver_for_browser(
    browser: BrowserKind,
    download_dir: &Path,
) -> std::result::Result<WebDriverContext, String> {
    let webdriver_addr = reserve_webdriver_addr()?;
    let webdriver_port = webdriver_port(&webdriver_addr);
    let program = resolve_webdriver_program(browser)?;
    let child = Command::new(&program)
        .env("CHROME_LOG_FILE", os_dev_null())
        .env("MSEDGEDRIVER_TELEMETRY_OPTOUT", "1")
        .arg(format!("--port={webdriver_port}"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .map_err(|e| format!("`{}` 실행 실패: {e}", program.display()))?;
    let guard = ChildGuard { child: Some(child) };
    wait_for_webdriver_ready(&webdriver_addr, Duration::from_secs(15))?;
    probe_webdriver_session(browser, &webdriver_addr, download_dir).map_err(|e| {
        format!(
            "WebDriver 세션 생성 실패: {e}{}",
            webdriver_version_mismatch_hint(browser, &e)
        )
    })?;
    Ok(WebDriverContext {
        addr: webdriver_addr,
        browser,
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
fn run_tasks(
    webdriver_addr: &str,
    browser: BrowserKind,
    download_dir: &Path,
    prefix: &str,
) -> std::result::Result<Vec<PathBuf>, String> {
    let mut downloaded_paths = Vec::with_capacity(TASKS.len());
    for (idx, task) in TASKS.iter().enumerate() {
        let new_path =
            run_task_with_retries(webdriver_addr, browser, download_dir, prefix, idx, task)?;
        eprintln!(
            "[소스 다운로드 {}/{}] {} {} -> {}",
            idx + 1,
            TASKS.len(),
            task.sido,
            task.sigungu,
            new_path.display()
        );
        downloaded_paths.push(new_path);
        sleep(Duration::from_millis(1000));
    }
    Ok(downloaded_paths)
}
fn probe_webdriver_session(
    browser: BrowserKind,
    webdriver_addr: &str,
    download_dir: &Path,
) -> std::result::Result<(), String> {
    let session_id = webdriver_new_session(browser, webdriver_addr, download_dir)?;
    let _ = webdriver_delete_session(webdriver_addr, &session_id);
    Ok(())
}
fn run_task_with_retries(
    webdriver_addr: &str,
    browser: BrowserKind,
    download_dir: &Path,
    prefix: &str,
    index: usize,
    task: &Task,
) -> std::result::Result<PathBuf, String> {
    let mut last_error = None;
    for attempt in 1..=TASK_SESSION_RETRY_LIMIT {
        match run_task_once(webdriver_addr, browser, download_dir, prefix, index, task) {
            Ok(path) => return Ok(path),
            Err(err) => {
                let should_retry =
                    attempt < TASK_SESSION_RETRY_LIMIT && is_recoverable_session_error(&err);
                last_error = Some(err);
                if should_retry {
                    eprintln!(
                        "[소스 다운로드 재시도 {attempt}/{TASK_SESSION_RETRY_LIMIT}] {} {} 세션을 다시 시작합니다.",
                        task.sido, task.sigungu
                    );
                    sleep(Duration::from_secs(2));
                    continue;
                }
                break;
            }
        }
    }
    Err(last_error.unwrap_or_else(|| format!("작업 실행 실패 ({} {})", task.sido, task.sigungu)))
}
fn run_task_once(
    webdriver_addr: &str,
    browser: BrowserKind,
    download_dir: &Path,
    prefix: &str,
    index: usize,
    task: &Task,
) -> std::result::Result<PathBuf, String> {
    let session_id = webdriver_new_session(browser, webdriver_addr, download_dir).map_err(|e| {
        format!(
            "작업 세션 생성 실패 ({} {}): {e}{}",
            task.sido,
            task.sigungu,
            webdriver_version_mismatch_hint(browser, &e)
        )
    })?;
    let result = run_task_in_session(
        webdriver_addr,
        &session_id,
        download_dir,
        prefix,
        index,
        task,
    );
    let _ = webdriver_delete_session(webdriver_addr, &session_id);
    result
}
fn run_task_in_session(
    webdriver_addr: &str,
    session_id: &str,
    download_dir: &Path,
    prefix: &str,
    index: usize,
    task: &Task,
) -> std::result::Result<PathBuf, String> {
    webdriver_get(webdriver_addr, session_id, OPINET_URL)?;
    wait_for_page_ready(webdriver_addr, session_id, "페이지 초기 로딩")?;
    select_region(webdriver_addr, session_id, task)?;
    submit_search(webdriver_addr, session_id)?;
    download_task_file(
        webdriver_addr,
        session_id,
        download_dir,
        prefix,
        index,
        task,
    )
}
fn wait_for_page_ready(
    webdriver_addr: &str,
    session_id: &str,
    label: &str,
) -> std::result::Result<(), String> {
    wait_until(
        webdriver_addr,
        session_id,
        PAGE_READY_SCRIPT,
        "READY",
        Duration::from_secs(20),
        Duration::from_millis(300),
        label,
    )
}
fn select_region(
    webdriver_addr: &str,
    session_id: &str,
    task: &Task,
) -> std::result::Result<(), String> {
    let sido_candidates = [task.sido];
    let sigungu_candidates = sigungu_aliases(task.sigungu);
    let result = webdriver_execute_string(
        webdriver_addr,
        session_id,
        &script_select_by_candidates("SIDO_NM0", &sido_candidates, true),
    )?;
    if result != "OK" {
        return Err(format!("시도 선택 실패 ({}): {result}", task.sido));
    }
    wait_until(
        webdriver_addr,
        session_id,
        &script_option_ready("SIGUNGU_NM0", &sigungu_candidates),
        "READY",
        Duration::from_secs(20),
        Duration::from_millis(300),
        &format!("시군구 목록 로딩: {} {}", task.sido, task.sigungu),
    )?;
    let result = webdriver_execute_string(
        webdriver_addr,
        session_id,
        &script_select_by_candidates("SIGUNGU_NM0", &sigungu_candidates, false),
    )?;
    if result != "OK" {
        return Err(format!(
            "시군구 선택 실패 ({} {}): {result}",
            task.sido, task.sigungu
        ));
    }
    wait_until(
        webdriver_addr,
        session_id,
        &script_selected_candidate_ready("SIGUNGU_NM0", &sigungu_candidates),
        "READY",
        Duration::from_secs(20),
        Duration::from_millis(300),
        &format!("시군구 선택 반영: {} {}", task.sido, task.sigungu),
    )?;
    Ok(())
}
fn submit_search(webdriver_addr: &str, session_id: &str) -> std::result::Result<(), String> {
    webdriver_execute_ok_or_null(
        webdriver_addr,
        session_id,
        SEARCH_SUBMIT_SCRIPT,
        "조회 제출",
    )?;
    sleep(Duration::from_millis(1500));
    wait_for_page_ready(webdriver_addr, session_id, "검색 결과 로딩")
}
fn download_task_file(
    webdriver_addr: &str,
    session_id: &str,
    download_dir: &Path,
    prefix: &str,
    index: usize,
    task: &Task,
) -> std::result::Result<PathBuf, String> {
    let before = snapshot_files(download_dir)?;
    webdriver_execute_ok_or_null(
        webdriver_addr,
        session_id,
        EXCEL_DOWNLOAD_SCRIPT,
        "엑셀 저장 실행",
    )?;
    let downloaded =
        wait_for_task_download(webdriver_addr, session_id, download_dir, &before, task)?;
    rename_downloaded_file(download_dir, prefix, index, task, &downloaded)
}
fn wait_for_task_download(
    webdriver_addr: &str,
    session_id: &str,
    download_dir: &Path,
    before: &HashSet<PathBuf>,
    task: &Task,
) -> std::result::Result<PathBuf, String> {
    wait_for_new_download(download_dir, before, DOWNLOAD_WAIT_TIMEOUT).map_err(|err| {
        let diag = webdriver_execute_string(webdriver_addr, session_id, DOWNLOAD_DIAGNOSTIC_SCRIPT)
            .unwrap_or_else(|diag_err| format!("진단 조회 실패: {diag_err}"));
        format!("{err} ({}, {}; {diag})", task.sido, task.sigungu)
    })
}
fn rename_downloaded_file(
    download_dir: &Path,
    prefix: &str,
    index: usize,
    task: &Task,
    downloaded: &Path,
) -> std::result::Result<PathBuf, String> {
    let ext = downloaded
        .extension()
        .and_then(|s| s.to_str())
        .unwrap_or("xlsx");
    let new_path = download_dir.join(build_auto_source_name(
        prefix,
        index + 1,
        task.sido,
        task.sigungu,
        ext,
    ));
    fs::rename(downloaded, &new_path).map_err(|e| {
        format!(
            "파일 이름 변경 실패: {} -> {} ({e})",
            downloaded.display(),
            new_path.display()
        )
    })?;
    Ok(new_path)
}
fn build_auto_source_name(
    prefix: &str,
    order: usize,
    sido: &str,
    sigungu: &str,
    ext: &str,
) -> String {
    format!(
        "{prefix}{AUTO_SOURCE_MARKER}_{order:02}_{}_{}.{}",
        safe_filename(sido),
        safe_filename(sigungu),
        ext
    )
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
        r#"{{"capabilities":{{"alwaysMatch":{{"browserName":"{}","{}":{{"args":["start-maximized","--disable-background-networking","--disable-default-apps","--disable-sync","--log-level=3","--no-first-run"],"excludeSwitches":["enable-logging"],"prefs":{{"download.default_directory":"{}","download.prompt_for_download":false,"download.directory_upgrade":true,"safebrowsing.enabled":true,"profile.default_content_setting_values.automatic_downloads":1}}}}}}}}}}"#,
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
fn webdriver_execute_string(
    webdriver_addr: &str,
    session_id: &str,
    script: &str,
) -> std::result::Result<String, String> {
    webdriver_execute_optional_string(webdriver_addr, session_id, script)?
        .map_or_else(|| Err("execute/sync 응답이 null 입니다.".to_string()), Ok)
}
fn webdriver_execute_ok_or_null(
    webdriver_addr: &str,
    session_id: &str,
    script: &str,
    label: &str,
) -> std::result::Result<(), String> {
    match webdriver_execute_optional_string(webdriver_addr, session_id, script)? {
        Some(value) if value == "OK" => Ok(()),
        Some(value) => Err(format!("{label} 실패: {value}")),
        None => Ok(()),
    }
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
    let _ = stream.set_read_timeout(Some(Duration::from_secs(60)));
    let _ = stream.set_write_timeout(Some(Duration::from_secs(60)));
    let body = body.unwrap_or("");
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
    raw.windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| (pos, 4))
        .or_else(|| {
            raw.windows(2)
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
    while i < bytes.len() {
        match bytes[i] {
            b'"' => return Some(JsonStringField::String(out)),
            b'\\' => {
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
            }
            b => out.push(char::from(b)),
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
fn js_string_array(items: &[&str]) -> String {
    let mut out = String::from("[");
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        out.push_str(&json_escape(item));
        out.push('"');
    }
    out.push(']');
    out
}
const PAGE_READY_SCRIPT: &str = r#"return (document.readyState === "complete" && !!document.getElementById("SIDO_NM0")) ? "READY" : "";"#;
const SEARCH_SUBMIT_SCRIPT: &str = r#"
return (function() {
  var form = document.getElementById("searrgVO");
  var sidoSel = document.getElementById("SIDO_NM0");
  var sigunguSel = document.getElementById("SIGUNGU_NM0");
  var sidoHidden = document.getElementById("SIDO_NM");
  var sigunguHidden = document.getElementById("SIGUNGU_NM");
  var searchMode = document.getElementById("SEARCH_MOD1");
  var osNm = document.getElementById("OS_NM");
  var osAddr = document.getElementById("OS_ADDR");
  if (!form) return "ERR:NO_FORM";
  if (!sidoSel) return "ERR:NO_SIDO";
  if (!sigunguSel) return "ERR:NO_SIGUNGU";
  if (sidoHidden) sidoHidden.value = (sidoSel.value || "").trim();
  if (sigunguHidden) sigunguHidden.value = (sigunguSel.value || "").trim();
  if (searchMode) searchMode.checked = true;
  if (osNm) osNm.value = "";
  if (osAddr) osAddr.value = "";
  form.action = "/searRgSelect.do";
  form.target = "_self";
  form.submit();
  return "OK";
})();"#;
const EXCEL_DOWNLOAD_SCRIPT: &str = r#"
return (function() {
  var sidoSel = document.getElementById("SIDO_NM0");
  var sigunguSel = document.getElementById("SIGUNGU_NM0");
  var sidoHidden = document.getElementById("SIDO_NM");
  var sigunguHidden = document.getElementById("SIGUNGU_NM");
  var searchMode = document.getElementById("SEARCH_MOD1");
  if (sidoHidden && sidoSel) sidoHidden.value = (sidoSel.value || "").trim();
  if (sigunguHidden && sigunguSel) sigunguHidden.value = (sigunguSel.value || "").trim();
  if (searchMode) searchMode.checked = true;
  if (typeof fn_excel_download === "function") {
    fn_excel_download("os_btn");
    return "OK";
  }
  var anchor = document.querySelector('a[href*="fn_excel_download"]');
  if (!anchor) return "ERR:NO_DOWNLOAD_TRIGGER";
  anchor.click();
  return "OK";
})();"#;
const DOWNLOAD_DIAGNOSTIC_SCRIPT: &str = r#"
return (function() {
  function text(id) {
    var el = document.getElementById(id);
    return el ? (el.textContent || el.innerText || "").trim() : "";
  }
  function value(id) {
    var el = document.getElementById(id);
    return el ? (el.value || "").trim() : "";
  }
  var listLen = "";
  try {
    if (typeof listpop !== "undefined" && listpop && typeof listpop.length === "number") {
      listLen = String(listpop.length);
    }
  } catch (e) {}
  var downloadAnchor = document.querySelector('a[href*="fn_excel_download"]');
  return [
    "title=" + (document.title || "").trim(),
    "ready=" + document.readyState,
    "sido=" + value("SIDO_NM0"),
    "sigungu=" + value("SIGUNGU_NM0"),
    "totCnt1=" + text("totCnt1"),
    "totCnt11=" + text("totCnt11"),
    "listpop=" + listLen,
    "downloadTrigger=" + (downloadAnchor ? "Y" : "N")
  ].join(" | ");
})();"#;
fn script_option_ready(select_id: &str, candidates: &[&str]) -> String {
    let template = r#"
return (function() {
  var sel = document.getElementById("__ID__");
  var candidates = __CANDS__;
  var placeholders = { "": true, "선택": true, "시/군/구": true };
  var stateKey = "__STATE_KEY__";
  var stableForMs = __STABLE_FOR_MS__;
  if (!sel) return "";
  var options = Array.prototype.slice.call(sel.options || []);
  function norm(v) { return (v || "").trim(); }
  function matches(candidate, opt) {
    var t = norm(opt.text);
    var v = norm(opt.value);
    return t === candidate || v === candidate ||
           (t && (t.indexOf(candidate) >= 0 || candidate.indexOf(t) >= 0)) ||
           (v && (v.indexOf(candidate) >= 0 || candidate.indexOf(v) >= 0));
  }
  var hasCandidate = false;
  for (var i = 0; i < candidates.length; i++) {
    for (var j = 0; j < options.length; j++) {
      if (matches(candidates[i], options[j])) hasCandidate = true;
    }
  }
  var meaningful = 0;
  for (var k = 0; k < options.length; k++) {
    var t = norm(options[k].text);
    var v = norm(options[k].value);
    if (!(placeholders[t] && placeholders[v])) {
      meaningful++;
    }
  }
  if (!hasCandidate && meaningful !== 1) return "";
  var signature = options.map(function(opt) {
    return norm(opt.value) + "::" + norm(opt.text);
  }).join("|");
  var now = Date.now();
  var state = window[stateKey];
  if (!state || state.signature !== signature) {
    window[stateKey] = { signature: signature, changedAt: now };
    return "";
  }
  return (now - state.changedAt) >= stableForMs ? "READY" : "";
})();"#;
    template
        .replace("__ID__", &json_escape(select_id))
        .replace("__CANDS__", &js_string_array(candidates))
        .replace(
            "__STATE_KEY__",
            &json_escape(&format!("__fcupdater_option_ready_{select_id}")),
        )
        .replace("__STABLE_FOR_MS__", "700")
}
fn script_selected_candidate_ready(select_id: &str, candidates: &[&str]) -> String {
    let template = r#"
return (function() {
  var sel = document.getElementById("__ID__");
  var candidates = __CANDS__;
  var stateKey = "__STATE_KEY__";
  var stableForMs = __STABLE_FOR_MS__;
  if (!sel || sel.selectedIndex < 0) return "";
  function norm(v) { return (v || "").trim(); }
  function matches(candidate, opt) {
    var t = norm(opt.text);
    var v = norm(opt.value);
    return t === candidate || v === candidate ||
           (t && (t.indexOf(candidate) >= 0 || candidate.indexOf(t) >= 0)) ||
           (v && (v.indexOf(candidate) >= 0 || candidate.indexOf(v) >= 0));
  }
  var selected = sel.options[sel.selectedIndex];
  if (!selected) return "";
  var matched = false;
  for (var i = 0; i < candidates.length; i++) {
    if (matches(candidates[i], selected)) {
      matched = true;
      break;
    }
  }
  if (!matched) return "";
  var signature = [
    String(sel.selectedIndex),
    norm(sel.value),
    norm(selected.text)
  ].join("::");
  var now = Date.now();
  var state = window[stateKey];
  if (!state || state.signature !== signature) {
    window[stateKey] = { signature: signature, changedAt: now };
    return "";
  }
  return (now - state.changedAt) >= stableForMs ? "READY" : "";
})();"#;
    template
        .replace("__ID__", &json_escape(select_id))
        .replace("__CANDS__", &js_string_array(candidates))
        .replace(
            "__STATE_KEY__",
            &json_escape(&format!("__fcupdater_selected_ready_{select_id}")),
        )
        .replace("__STABLE_FOR_MS__", "700")
}
fn script_select_by_candidates(
    select_id: &str,
    candidates: &[&str],
    dispatch_change: bool,
) -> String {
    let template = r#"
return (function() {
  var sel = document.getElementById("__ID__");
  var candidates = __CANDS__;
  var dispatchChange = __DISPATCH_CHANGE__;
  var placeholders = { "": true, "선택": true, "시/군/구": true };
  if (!sel) return "ERR:NO_SELECT";
  var options = Array.prototype.slice.call(sel.options || []);
  function norm(v) { return (v || "").trim(); }
  function findExact() {
    for (var i = 0; i < candidates.length; i++) {
      for (var j = 0; j < options.length; j++) {
        var t = norm(options[j].text);
        var v = norm(options[j].value);
        if (t === candidates[i] || v === candidates[i]) {
          return j;
        }
      }
    }
    return -1;
  }
  function findPartial() {
    for (var i = 0; i < candidates.length; i++) {
      for (var j = 0; j < options.length; j++) {
        var t = norm(options[j].text);
        var v = norm(options[j].value);
        if ((t && (t.indexOf(candidates[i]) >= 0 || candidates[i].indexOf(t) >= 0)) ||
            (v && (v.indexOf(candidates[i]) >= 0 || candidates[i].indexOf(v) >= 0))) {
          return j;
        }
      }
    }
    return -1;
  }
  var idx = findExact();
  if (idx < 0) idx = findPartial();
  if (idx < 0) {
    var meaningfulIndexes = [];
    for (var k = 0; k < options.length; k++) {
      var t = norm(options[k].text);
      var v = norm(options[k].value);
      if (!(placeholders[t] && placeholders[v])) {
        meaningfulIndexes.push(k);
      }
    }
    if (meaningfulIndexes.length === 1) {
      idx = meaningfulIndexes[0];
    }
  }
  if (idx < 0) return "ERR:NO_OPTION";
  sel.selectedIndex = idx;
  if (dispatchChange) {
    try {
      sel.dispatchEvent(new Event("change", { bubbles: true }));
    } catch (e) {
      var evt = document.createEvent("HTMLEvents");
      evt.initEvent("change", true, false);
      sel.dispatchEvent(evt);
    }
  }
  return "OK";
})();"#;
    template
        .replace("__ID__", &json_escape(select_id))
        .replace("__CANDS__", &js_string_array(candidates))
        .replace(
            "__DISPATCH_CHANGE__",
            if dispatch_change { "true" } else { "false" },
        )
}
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
        let now = snapshot_files(dir)?;
        let mut complete_files = Vec::new();
        let mut temp_exists = false;
        for path in now.difference(before) {
            let ext = path
                .extension()
                .and_then(|s| s.to_str())
                .unwrap_or("")
                .to_ascii_lowercase();
            match ext.as_str() {
                "xls" | "xlsx" => complete_files.push(path.clone()),
                "crdownload" | "part" | "tmp" => temp_exists = true,
                _ => {}
            }
        }
        if !complete_files.is_empty() && !temp_exists {
            complete_files
                .sort_by_key(|path| fs::metadata(path).and_then(|meta| meta.modified()).ok());
            return complete_files
                .pop()
                .ok_or_else(|| "완료 파일 선택 실패".to_string());
        }
        if start.elapsed() > timeout {
            return Err("다운로드 완료 파일을 찾지 못했습니다".to_string());
        }
        sleep(Duration::from_millis(500));
    }
}
fn safe_filename(input: &str) -> String {
    input
        .chars()
        .map(|ch| match ch {
            '\\' | '/' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => ch,
        })
        .collect()
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
