use crate::{Result, err};
#[cfg(not(windows))]
use std::process;
use std::{
    env,
    ffi::{OsStr, OsString},
    path::PathBuf,
};
const APP_NAME: &str = "fcupdater";
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
#[derive(Debug, Clone)]
pub enum OutputTarget {
    Auto,
    Explicit(PathBuf),
    InPlace,
}
#[derive(Debug, Clone)]
pub enum ParseAction {
    Run(Args),
    Help(String),
    Version(String),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaveMode {
    Verify,
    Fast,
    DryRun,
}
impl SaveMode {
    pub const fn is_dry_run(self) -> bool {
        matches!(self, Self::DryRun)
    }
    pub const fn verify_saved_file(self) -> bool {
        matches!(self, Self::Verify)
    }
}
#[derive(Debug, Clone)]
pub struct Args {
    pub master: PathBuf,
    pub sources_dir: PathBuf,
    pub sources_prefix: String,
    pub output_target: OutputTarget,
    pub no_change_log: bool,
    pub save_mode: SaveMode,
}
impl Default for Args {
    fn default() -> Self {
        Self {
            master: PathBuf::from("fuel_cost_chungcheong.xlsx"),
            sources_dir: PathBuf::from("."),
            sources_prefix: "지역_위치별(주유소)".to_string(),
            output_target: OutputTarget::Auto,
            no_change_log: false,
            save_mode: SaveMode::Verify,
        }
    }
}
impl Args {
    pub fn parse_action() -> Result<ParseAction> {
        let raw_args: Vec<OsString> = env::args_os().skip(1).collect();
        parse_args(&raw_args)
    }
}
fn parse_args(raw_args: &[OsString]) -> Result<ParseAction> {
    let mut args = Args::default();
    let mut dry_run = false;
    let mut fast_save = false;
    let mut i = 0usize;
    while let Some(raw_token) = raw_args.get(i) {
        let token = raw_token.as_os_str();
        if token == OsStr::new("-h") || token == OsStr::new("--help") {
            return Ok(ParseAction::Help(usage_text()));
        } else if token == OsStr::new("--version") {
            return Ok(ParseAction::Version(format!("{APP_NAME} {APP_VERSION}")));
        } else if token == OsStr::new("--in-place") {
            set_output_target_in_place(&mut args.output_target)?;
        } else if token == OsStr::new("--no-change-log") {
            args.no_change_log = true;
        } else if token == OsStr::new("--dry-run") {
            dry_run = true;
        } else if token == OsStr::new("--fast-save") {
            fast_save = true;
        } else if token == OsStr::new("--master") {
            let value = take_option_value(raw_args, &mut i, "--master")?;
            args.master = PathBuf::from(value);
        } else if token == OsStr::new("--sources-dir") {
            let value = take_option_value(raw_args, &mut i, "--sources-dir")?;
            args.sources_dir = PathBuf::from(value);
        } else if token == OsStr::new("--sources-prefix") {
            let value = take_option_value(raw_args, &mut i, "--sources-prefix")?;
            args.sources_prefix = value
                .into_string()
                .map_err(|_| err("--sources-prefix 값은 UTF-8 문자열이어야 합니다."))?;
        } else if token == OsStr::new("--output") {
            let value = take_option_value(raw_args, &mut i, "--output")?;
            set_output_target_explicit(&mut args.output_target, PathBuf::from(value))?;
        } else if let Some(token_str) = token.to_str() {
            if let Some(v) = token_str.strip_prefix("--master=") {
                args.master = PathBuf::from(v);
            } else if let Some(v) = token_str.strip_prefix("--sources-dir=") {
                args.sources_dir = PathBuf::from(v);
            } else if let Some(v) = token_str.strip_prefix("--sources-prefix=") {
                args.sources_prefix = v.to_string();
            } else if let Some(v) = token_str.strip_prefix("--output=") {
                set_output_target_explicit(&mut args.output_target, PathBuf::from(v))?;
            } else {
                return Err(err(format!(
                    "알 수 없는 옵션: {token_str}\n\n{}",
                    usage_text()
                )));
            }
        } else {
            return Err(err(format!(
                "알 수 없는 옵션: {}\n\n{}",
                token.to_string_lossy(),
                usage_text()
            )));
        }
        i += 1;
    }
    if dry_run && fast_save {
        return Err(err(
            "--dry-run 과 --fast-save 는 동시에 사용할 수 없습니다. (--dry-run에서는 저장 검증 설정이 무의미합니다.)",
        ));
    }
    args.save_mode = if dry_run {
        SaveMode::DryRun
    } else if fast_save {
        SaveMode::Fast
    } else {
        SaveMode::Verify
    };
    Ok(ParseAction::Run(args))
}
fn set_output_target_in_place(output_target: &mut OutputTarget) -> Result<()> {
    if matches!(output_target, OutputTarget::Explicit(_)) {
        return Err(err("--in-place 와 --output 은 동시에 사용할 수 없습니다."));
    }
    *output_target = OutputTarget::InPlace;
    Ok(())
}
fn set_output_target_explicit(output_target: &mut OutputTarget, value: PathBuf) -> Result<()> {
    if matches!(output_target, OutputTarget::InPlace) {
        return Err(err("--in-place 와 --output 은 동시에 사용할 수 없습니다."));
    }
    *output_target = OutputTarget::Explicit(value);
    Ok(())
}
fn take_option_value(raw_args: &[OsString], i: &mut usize, opt_name: &str) -> Result<OsString> {
    *i += 1;
    let Some(value) = raw_args.get(*i) else {
        return Err(err(format!("{opt_name} 옵션에 값이 필요합니다.")));
    };
    if is_long_option_token(value.as_os_str()) {
        return Err(err(format!(
            "{opt_name} 옵션에 값이 필요합니다. (다음 토큰: {})",
            value.to_string_lossy()
        )));
    }
    Ok(value.clone())
}
fn is_long_option_token(value: &OsStr) -> bool {
    value.to_str().is_some_and(|s| s.starts_with("--"))
}
fn usage_text() -> String {
    let mut out = format!(
        "{APP_NAME} {APP_VERSION}\n주유소 가격/정보 현행화 (Excel 미설치 OK)\n\n\
사용법:\n  {APP_NAME} [OPTIONS]\n\n\
옵션:\n  --master <PATH>          마스터 파일 경로 (기본: fuel_cost_chungcheong.xlsx)\n  --sources-dir <PATH>     소스 폴더 (기본: .)\n  --sources-prefix <TEXT>  소스 파일 prefix (기본: 지역_위치별(주유소))\n  --output <PATH>          출력 파일 경로\n  --in-place               마스터 파일 덮어쓰기(백업 생성)\n  --no-change-log          변경내역 시트 갱신 안 함\n  --dry-run                파일 저장 없이 요약만 출력\n  --fast-save              저장 후 무결성 재검증 생략(속도 우선)\n  -h, --help               도움말\n  --version                버전\n\n주의:\n  --in-place 와 --output 은 동시에 사용할 수 없음\n  --dry-run 과 --fast-save 는 동시에 사용할 수 없음"
    );
    out.push_str(
        "\n\n환경 변수(선택):\n  FCUPDATER_SOURCE_HEADER_SCAN_ROWS\n  FCUPDATER_MASTER_HEADER_SCAN_ROWS\n  FCUPDATER_CHANGELOG_HEADER_SCAN_ROWS\n  FCUPDATER_CHANGELOG_HEADER_SCAN_COLS\n  FCUPDATER_CHANGELOG_STYLE_TEMPLATE_ROW\n  FCUPDATER_CP949_STRICT\n  FCUPDATER_DURABILITY_STRICT",
    );
    out.push_str("\n  FCUPDATER_COMMAND_TIMEOUT_SECS\n  FCUPDATER_DECODER_TIMEOUT_SECS");
    out
}
pub fn local_today_yyyy_mm_dd() -> Result<String> {
    #[cfg(windows)]
    {
        local_today_windows()
    }
    #[cfg(not(windows))]
    {
        local_today_non_windows()
    }
}
fn is_yyyy_mm_dd(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 10 {
        return false;
    }
    if b.get(4) != Some(&b'-') || b.get(7) != Some(&b'-') {
        return false;
    }
    b.iter().enumerate().all(|(i, ch)| {
        if i == 4 || i == 7 {
            true
        } else {
            ch.is_ascii_digit()
        }
    })
}
#[cfg(windows)]
fn local_today_windows() -> Result<String> {
    let today = crate::excel::windows_api::local_date_yyyy_mm_dd()?;
    if !is_yyyy_mm_dd(&today) {
        return Err(err(format!("오늘 날짜 형식이 올바르지 않습니다: {today}")));
    }
    Ok(today)
}
#[cfg(not(windows))]
fn local_today_non_windows() -> Result<String> {
    if let Ok(today) = local_today_from_date_command() {
        return Ok(today);
    }
    if let Ok(today) = local_today_from_python_command("python3") {
        return Ok(today);
    }
    if let Ok(today) = local_today_from_python_command("python") {
        return Ok(today);
    }
    local_today_utc_fallback()
}
#[cfg(not(windows))]
fn local_today_from_date_command() -> Result<String> {
    let output = process::Command::new("date")
        .args(["+%Y-%m-%d"])
        .output()
        .map_err(|e| err(format!("date 명령 실행 실패: {e}")))?;
    if !output.status.success() {
        return Err(err(format!(
            "date 명령 실행 실패: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let today = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !is_yyyy_mm_dd(&today) {
        return Err(err(format!(
            "date 명령 결과 형식이 올바르지 않습니다: {today}"
        )));
    }
    Ok(today)
}
#[cfg(not(windows))]
fn local_today_from_python_command(program: &str) -> Result<String> {
    let script = "from datetime import datetime;print(datetime.now().strftime('%Y-%m-%d'))";
    let output = process::Command::new(program)
        .args(["-c", script])
        .output()
        .map_err(|e| err(format!("{program} 실행 실패: {e}")))?;
    if !output.status.success() {
        return Err(err(format!(
            "{program} 비정상 종료: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let today = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if !is_yyyy_mm_dd(&today) {
        return Err(err(format!(
            "{program} 결과 형식이 올바르지 않습니다: {today}"
        )));
    }
    Ok(today)
}
#[cfg(not(windows))]
fn local_today_utc_fallback() -> Result<String> {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|e| err(format!("현재 시간 조회 실패: {e}")))?;
    let days = i64::try_from(now.as_secs() / 86_400)
        .map_err(|_| err("UTC 날짜 계산 중 일수 변환에 실패했습니다."))?;
    let (year, month, day) = civil_from_days(days);
    let today = format!("{year:04}-{month:02}-{day:02}");
    if !is_yyyy_mm_dd(&today) {
        return Err(err(format!("오늘 날짜 형식이 올바르지 않습니다: {today}")));
    }
    Ok(today)
}
#[cfg(not(windows))]
fn civil_from_days(days_since_unix_epoch: i64) -> (i32, u32, u32) {
    let z = days_since_unix_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = mp + if mp < 10 { 3 } else { -9 };
    let year = (y + if m <= 2 { 1 } else { 0 }) as i32;
    (year, m as u32, d as u32)
}
