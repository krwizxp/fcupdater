use crate::{Result, err};
use core::fmt::Write as _;
use std::{
    env,
    ffi::{OsStr, OsString},
    path::PathBuf,
};
pub const APP_NAME: &str = "fcupdater";
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
#[derive(Debug, Clone)]
pub enum OutputTarget {
    Auto,
    Explicit(PathBuf),
    InPlace,
}
#[derive(Debug, Clone)]
pub enum ParseAction {
    Help(String),
    Run(Args),
    Version(String),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SaveMode {
    DryRun,
    Fast,
    Verify,
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
    pub no_change_log: bool,
    pub output_target: OutputTarget,
    pub save_mode: SaveMode,
    pub skip_download: bool,
    pub sources_dir: PathBuf,
    pub sources_prefix: String,
}
impl Default for Args {
    fn default() -> Self {
        Self {
            master: PathBuf::from("fuel_cost_chungcheong.xlsx"),
            no_change_log: false,
            output_target: OutputTarget::Auto,
            save_mode: SaveMode::Verify,
            skip_download: false,
            sources_dir: PathBuf::from("."),
            sources_prefix: String::from("현재 판매가격(주유소)"),
        }
    }
}
impl TryFrom<&[OsString]> for ParseAction {
    type Error = crate::BoxError;
    fn try_from(raw_args: &[OsString]) -> Result<Self> {
        let mut args = Args::default();
        let mut dry_run = false;
        let mut fast_save = false;
        let mut arg_index = 0_usize;
        loop {
            let Some(raw_token) = raw_args.get(arg_index) else {
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
                return Ok(Self::Run(args));
            };
            let token = raw_token.as_os_str();
            if token == OsStr::new("-h") || token == OsStr::new("--help") {
                return Ok(Self::Help(usage_text()));
            }
            if token == OsStr::new("--version") {
                let mut version_text = String::with_capacity(
                    APP_NAME
                        .len()
                        .saturating_add(APP_VERSION.len())
                        .saturating_add(1),
                );
                version_text.push_str(APP_NAME);
                version_text.push(' ');
                version_text.push_str(APP_VERSION);
                return Ok(Self::Version(version_text));
            }
            if token == OsStr::new("--in-place") {
                if matches!(args.output_target, OutputTarget::Explicit(_)) {
                    return Err(err("--in-place 와 --output 은 동시에 사용할 수 없습니다."));
                }
                args.output_target = OutputTarget::InPlace;
            } else if token == OsStr::new("--no-change-log") {
                args.no_change_log = true;
            } else if token == OsStr::new("--skip-download") {
                args.skip_download = true;
            } else if token == OsStr::new("--dry-run") {
                dry_run = true;
            } else if token == OsStr::new("--fast-save") {
                fast_save = true;
            } else if token == OsStr::new("--master") {
                let value = take_option_value(raw_args, &mut arg_index, "--master")?;
                args.master = value.into();
            } else if token == OsStr::new("--sources-dir") {
                let value = take_option_value(raw_args, &mut arg_index, "--sources-dir")?;
                args.sources_dir = value.into();
            } else if token == OsStr::new("--sources-prefix") {
                let raw_value = take_option_value(raw_args, &mut arg_index, "--sources-prefix")?;
                let value_str: &str = raw_value.try_into().map_err(|source| {
                    let mut out = String::with_capacity(96);
                    out.push_str("--sources-prefix 값은 UTF-8 문자열이어야 합니다. (");
                    match write!(&mut out, "{source}") {
                        Ok(()) | Err(_) => {}
                    }
                    out.push(')');
                    err(out)
                })?;
                args.sources_prefix = parse_sources_prefix(value_str)?;
            } else if token == OsStr::new("--output") {
                let value = take_option_value(raw_args, &mut arg_index, "--output")?;
                if matches!(args.output_target, OutputTarget::InPlace) {
                    return Err(err("--in-place 와 --output 은 동시에 사용할 수 없습니다."));
                }
                args.output_target = OutputTarget::Explicit(value.into());
            } else if let Some(token_str) = token.to_str() {
                if let Some(option_value) = token_str.strip_prefix("--master=") {
                    args.master = option_value.into();
                } else if let Some(option_value) = token_str.strip_prefix("--sources-dir=") {
                    args.sources_dir = option_value.into();
                } else if let Some(option_value) = token_str.strip_prefix("--sources-prefix=") {
                    args.sources_prefix = parse_sources_prefix(option_value)?;
                } else if let Some(option_value) = token_str.strip_prefix("--output=") {
                    if matches!(args.output_target, OutputTarget::InPlace) {
                        return Err(err("--in-place 와 --output 은 동시에 사용할 수 없습니다."));
                    }
                    args.output_target = OutputTarget::Explicit(option_value.into());
                } else {
                    return Err(err(unknown_option_message(token_str)));
                }
            } else {
                return Err(err(unknown_option_message(&token.to_string_lossy())));
            }
            advance_arg_index(&mut arg_index)?;
        }
    }
}
pub fn take_option_value<'args>(
    raw_args: &'args [OsString],
    i: &mut usize,
    opt_name: &str,
) -> Result<&'args OsStr> {
    advance_arg_index(i)?;
    let Some(value) = raw_args.get(*i) else {
        let capacity = opt_name.len().saturating_add(16);
        let mut out = String::with_capacity(capacity);
        out.push_str(opt_name);
        out.push_str(" 옵션에 값이 필요합니다.");
        return Err(err(out));
    };
    if value.to_str().is_some_and(|text| text.starts_with("--")) {
        let capacity = opt_name.len().saturating_add(64);
        let mut out = String::with_capacity(capacity);
        out.push_str(opt_name);
        out.push_str(" 옵션에 값이 필요합니다. (다음 토큰: ");
        out.push_str(value.to_string_lossy().as_ref());
        out.push(')');
        return Err(err(out));
    }
    Ok(value.as_os_str())
}
pub fn advance_arg_index(i: &mut usize) -> Result<()> {
    *i = (*i)
        .checked_add(1)
        .ok_or_else(|| err("명령행 인덱스 계산 중 범위 오류"))?;
    Ok(())
}
fn unknown_option_message(token: &str) -> String {
    let usage = usage_text();
    let capacity = token.len().saturating_add(usage.len()).saturating_add(32);
    let mut out = String::with_capacity(capacity);
    out.push_str("알 수 없는 옵션: ");
    out.push_str(token);
    out.push_str("\n\n");
    out.push_str(&usage);
    out
}
pub fn parse_sources_prefix(value: &str) -> Result<String> {
    if value.is_empty() {
        return Err(err("--sources-prefix 는 비어 있을 수 없습니다."));
    }
    if matches!(value, "." | "..") {
        return Err(err(
            "--sources-prefix 에는 '.' 또는 '..' 를 사용할 수 없습니다.",
        ));
    }
    if value.chars().any(|ch| matches!(ch, '/' | '\\')) {
        return Err(err(
            "--sources-prefix 에는 경로 구분자(/, \\\\)를 사용할 수 없습니다.",
        ));
    }
    if value
        .chars()
        .any(|ch| matches!(ch, '<' | '>' | ':' | '"' | '|' | '?' | '*'))
    {
        return Err(err(
            "--sources-prefix 에는 파일명에 사용할 수 없는 문자를 넣을 수 없습니다. (< > : \" | ? *)",
        ));
    }
    if value.ends_with(' ') || value.ends_with('.') {
        return Err(err(
            "--sources-prefix 는 공백 또는 '.'으로 끝날 수 없습니다.",
        ));
    }
    Ok(value.to_owned())
}
pub fn usage_text() -> String {
    let mut out = String::with_capacity(1024);
    out.push_str(APP_NAME);
    out.push(' ');
    out.push_str(APP_VERSION);
    out.push_str(
        "\n주유소 가격/정보 현행화 (Excel 미설치 OK)\n\n\
사용법:\n  ",
    );
    out.push_str(APP_NAME);
    out.push_str(" [OPTIONS]\n\n\
옵션:\n  --master <PATH>          마스터 파일 경로 (기본: fuel_cost_chungcheong.xlsx)\n  --sources-dir <PATH>     소스 폴더/자동 다운로드 저장 폴더 (기본: .)\n  --sources-prefix <TEXT>  소스 파일명 접두어 (경로 아님, 기본: 현재 판매가격(주유소))\n  --skip-download          Opinet 자동 다운로드 생략, 기존 소스 파일만 사용\n  --output <PATH>          출력 파일 경로\n  --in-place               마스터 파일 덮어쓰기(백업 생성)\n  --no-change-log          변경내역 시트 갱신 안 함\n  --dry-run                파일 저장 없이 요약만 출력\n  --fast-save              저장 후 무결성 재검증 생략(속도 우선)\n  -h, --help               도움말\n  --version                버전\n\n주의:\n  기본 동작은 Opinet 자동 다운로드 후 현행화\n  --sources-prefix 는 파일명 접두어만 허용 (경로 구분자, Windows 금지 문자, 끝 공백/점 불가)\n  --in-place 와 --output 은 동시에 사용할 수 없음\n  --dry-run 과 --fast-save 는 동시에 사용할 수 없음");
    out.push_str(
        "\n\n환경 변수(선택):\n  FCUPDATER_SOURCE_HEADER_SCAN_ROWS\n  FCUPDATER_MASTER_HEADER_SCAN_ROWS\n  FCUPDATER_CHANGELOG_HEADER_SCAN_ROWS\n  FCUPDATER_CHANGELOG_HEADER_SCAN_COLS\n  FCUPDATER_CHANGELOG_STYLE_TEMPLATE_ROW\n  FCUPDATER_CP949_STRICT\n  FCUPDATER_DURABILITY_STRICT",
    );
    out.push_str("\n  FCUPDATER_COMMAND_TIMEOUT_SECS\n  FCUPDATER_DECODER_TIMEOUT_SECS");
    out
}
