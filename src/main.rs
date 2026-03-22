mod change_log;
mod cli;
mod defined_name;
mod excel;
mod master_sheet;
#[expect(
    clippy::pub_with_shorthand,
    reason = "rustfmt rewrites root-module visibility back to pub(crate)"
)]
pub(crate) mod numeric;
mod path_policy;
mod source_download;
mod source_sync;
mod summary;
use crate::{
    cli::{Args, OutputTarget, ParseAction},
    excel::writer::Workbook as StdWorkbook,
    numeric::round_f64_to_i32,
    source_sync::{build_source_index_with_report, find_source_files},
};
use std::{
    error::Error, fmt::Display, fs, io::Error as IoError, path::PathBuf,
    result::Result as StdResult,
};
type BoxError = Box<dyn Error + Send + Sync>;
type Result<T> = StdResult<T, BoxError>;
fn err(msg: impl Into<String>) -> BoxError {
    IoError::other(msg.into()).into()
}
fn err_with_source(context: impl Into<String>, source: impl Display) -> BoxError {
    let context_text = context.into();
    IoError::other(format!("{context_text}: {source}")).into()
}
#[derive(Debug, Clone)]
struct ChangeRow {
    reason: String,
    region: String,
    name: String,
    address: String,
    old_gasoline: Option<i32>,
    new_gasoline: Option<i32>,
    old_premium: Option<i32>,
    new_premium: Option<i32>,
    old_diesel: Option<i32>,
    new_diesel: Option<i32>,
}
#[derive(Debug, Clone)]
struct StoreRow {
    region: String,
    name: String,
    address: String,
    gasoline: Option<i32>,
    premium: Option<i32>,
    diesel: Option<i32>,
}
#[derive(Debug, Default)]
struct DownloadedSourceGuard {
    paths: Vec<PathBuf>,
}
impl DownloadedSourceGuard {
    fn track(&mut self, paths: Vec<PathBuf>) {
        self.paths = paths;
    }
    fn cleanup(&mut self) -> Result<()> {
        self.cleanup_with_message("임시 소스 파일")
    }
    fn cleanup_with_message(&mut self, message: &str) -> Result<()> {
        let removed = source_download::cleanup_downloaded_sources(&self.paths)?;
        if removed > 0 {
            println!("{message} {removed}개 정리");
        }
        self.paths.clear();
        Ok(())
    }
}
impl Drop for DownloadedSourceGuard {
    fn drop(&mut self) {
        if self.paths.is_empty() {
            return;
        }
        if let Err(e) = self.cleanup_with_message("종료 중 임시 소스 파일") {
            eprintln!("종료 중 임시 소스 파일 정리 실패: {e}");
        }
    }
}
fn main() -> Result<()> {
    match Args::parse_action()? {
        ParseAction::Run(args) => run(&args),
        ParseAction::Help(text) | ParseAction::Version(text) => {
            println!("{text}");
            Ok(())
        }
    }
}
fn run(args: &Args) -> Result<()> {
    let master_exists = args.master.try_exists().map_err(|e| {
        err(format!(
            "마스터 파일 경로 확인 실패: {master_path} ({e})",
            master_path = args.master.display()
        ))
    })?;
    if !master_exists {
        return Err(err(format!(
            "마스터 파일이 없습니다: {master_path} (같은 폴더에 두거나 --master로 경로를 지정하세요)",
            master_path = args.master.display()
        )));
    }
    let mut downloaded_sources = DownloadedSourceGuard::default();
    if !args.skip_download {
        let downloaded = source_download::refresh_sources(&args.sources_dir, &args.sources_prefix)
            .map_err(|e| {
                err(format!(
                    "{e}\n자동 다운로드를 건너뛰려면 --skip-download 를 지정하세요."
                ))
            })?;
        println!(
            "소스 파일 {downloaded_count}개 준비 완료",
            downloaded_count = downloaded.len()
        );
        downloaded_sources.track(downloaded);
    }
    let source_paths = find_source_files(&args.sources_dir, &args.sources_prefix)?;
    if source_paths.is_empty() {
        return Err(err(format!(
            "소스 파일을 찾지 못했습니다. 폴더: {sources_dir} / prefix: {sources_prefix} / 확장자: .xls,.xlsx",
            sources_dir = args.sources_dir.display(),
            sources_prefix = args.sources_prefix
        )));
    }
    let source_sync::SourceIndexBuildResult {
        index: source_index,
        report: source_report,
    } = build_source_index_with_report(&source_paths)?;
    downloaded_sources.cleanup()?;
    let mut book = StdWorkbook::open(&args.master)?;
    let (changes, added, deleted) = master_sheet::update_master_sheet(&mut book, &source_index)?;
    let today = cli::local_today_yyyy_mm_dd()?;
    if !args.no_change_log {
        change_log::update_change_log_sheet(&mut book, &today, &changes, &added, &deleted)?;
    }
    let reserved_output =
        !args.save_mode.is_dry_run() && !matches!(args.output_target, OutputTarget::InPlace);
    let out_path = path_policy::decide_output_path(args, &today, args.save_mode.is_dry_run())?;
    if !args.save_mode.is_dry_run() {
        if matches!(args.output_target, OutputTarget::InPlace) {
            let backup = path_policy::reserve_backup_path(&args.master, &today)?;
            if let Err(e) = fs::copy(&args.master, &backup) {
                let _cleanup_result = fs::remove_file(&backup);
                return Err(err(format!(
                    "백업 파일 생성에 실패했습니다: {master_path} -> {backup_path} ({e})",
                    master_path = args.master.display(),
                    backup_path = backup.display()
                )));
            }
            println!(
                "백업 파일 생성: {backup_path}",
                backup_path = backup.display()
            );
        }
        if let Err(e) = book.save_as(&out_path, args.save_mode.verify_saved_file()) {
            if reserved_output {
                path_policy::cleanup_reservation_file(&out_path);
            }
            return Err(e);
        }
    }
    summary::print_summary(
        args,
        &out_path,
        source_paths.len(),
        &source_report,
        &changes,
        &added,
        &deleted,
    );
    Ok(())
}
fn canon_header(s: &str) -> String {
    s.chars().filter(|ch| !ch.is_whitespace()).collect()
}
fn same_trimmed(a: &str, b: &str) -> bool {
    a.trim() == b.trim()
}
fn same_self_yn(a: &str, b: &str) -> bool {
    canon_header(a) == canon_header(b)
}
fn parse_i32_str(s: &str) -> Option<i32> {
    let trimmed = s.trim();
    if trimmed.is_empty() || trimmed == "-" {
        return None;
    }
    let normalized = trimmed.replace(',', "");
    normalized.parse::<f64>().ok().and_then(round_f64_to_i32)
}
fn usize_to_u32(value: usize, context: &str) -> Result<u32> {
    u32::try_from(value).map_err(|source| {
        err_with_source(
            format!("{context} 값이 너무 큽니다. (value={value})"),
            source,
        )
    })
}
fn shift_row(row: u32, increase: u32, decrease: u32) -> u32 {
    if increase > 0 {
        row.saturating_add(increase)
    } else {
        row.saturating_sub(decrease).max(1)
    }
}
fn add_row_offset(base_row: u32, offset: usize, context: &str) -> Result<u32> {
    let offset_u32 = usize_to_u32(offset, context)?;
    base_row.checked_add(offset_u32).ok_or_else(|| {
        err(format!(
            "{context} 계산 중 overflow가 발생했습니다. ({base_row} + {offset_u32})"
        ))
    })
}
fn normalize_address_key(addr: &str) -> String {
    let mut rest = addr.trim();
    let mut out = String::with_capacity(rest.len());
    while let Some(ch) = rest.chars().next() {
        if let Some((from, to)) = [
            ("충청남도", "충남"),
            ("충청북도", "충북"),
            ("대전광역시", "대전"),
            ("세종특별자치시", "세종"),
        ]
        .iter()
        .copied()
        .find(|candidate| rest.starts_with(candidate.0))
        {
            out.push_str(to);
            rest = rest.get(from.len()..).unwrap_or_default();
            continue;
        }
        rest = rest.get(ch.len_utf8()..).unwrap_or_default();
        if ch.is_whitespace() {
            continue;
        }
        if matches!(ch, '(' | ')' | '[' | ']' | '{' | '}' | ',' | '.') {
            continue;
        }
        out.push(ch);
    }
    out
}
fn display_region_label_from_source(region: &str, address: &str) -> String {
    parse_region_label(region)
        .or_else(|| parse_region_label(address))
        .unwrap_or_else(|| region.trim().to_owned())
}
fn parse_region_label(text: &str) -> Option<String> {
    let mut tokens = text
        .split_whitespace()
        .map(str::trim)
        .filter(|t| !t.is_empty());
    let first = tokens.next()?;
    let second = tokens.next();
    if let Some(label) = strip_metropolitan_suffix(first) {
        return Some(label.to_owned());
    }
    if is_province_token(first) {
        return second.map(normalize_basic_region_token);
    }
    if is_metropolitan_token(first) {
        return Some(first.to_owned());
    }
    strip_basic_region_suffix(first)
        .map(ToString::to_string)
        .or_else(|| (second.is_none()).then(|| first.to_owned()))
}
fn normalize_basic_region_token(token: &str) -> String {
    strip_basic_region_suffix(token).map_or_else(|| token.to_owned(), ToString::to_string)
}
fn strip_metropolitan_suffix(token: &str) -> Option<&str> {
    ["특별자치시", "광역시", "특별시"]
        .iter()
        .find_map(|suffix| token.strip_suffix(suffix))
        .filter(|label| !label.is_empty())
}
fn strip_basic_region_suffix(token: &str) -> Option<&str> {
    ["시", "군", "구"]
        .iter()
        .find_map(|suffix| token.strip_suffix(suffix))
        .filter(|label| !label.is_empty())
}
fn is_province_token(token: &str) -> bool {
    token.ends_with('도')
        || token.ends_with("특별자치도")
        || matches!(
            token,
            "충남" | "충북" | "경기" | "강원" | "전북" | "전남" | "경북" | "경남" | "제주"
        )
}
fn is_metropolitan_token(token: &str) -> bool {
    matches!(
        token,
        "서울" | "부산" | "대구" | "인천" | "광주" | "대전" | "울산" | "세종"
    )
}
