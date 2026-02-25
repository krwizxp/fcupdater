use std::fs;
mod change_log;
mod cli;
mod defined_name;
mod excel;
mod master_sheet;
pub(crate) mod numeric;
mod path_policy;
mod source_sync;
mod summary;
use cli::{Args, OutputTarget, ParseAction};
use excel::writer::Workbook as StdWorkbook;
use numeric::round_f64_to_i32;
use source_sync::{build_source_index_with_report, find_source_files};
type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, BoxError>;
fn err(msg: impl Into<String>) -> BoxError {
    std::io::Error::other(msg.into()).into()
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
    if !args.master.exists() {
        return Err(err(format!(
            "마스터 파일이 없습니다: {} (같은 폴더에 두거나 --master로 경로를 지정하세요)",
            args.master.display()
        )));
    }
    let source_paths = find_source_files(&args.sources_dir, &args.sources_prefix)?;
    if source_paths.is_empty() {
        return Err(err(format!(
            "소스 파일을 찾지 못했습니다. 폴더: {} / prefix: {} / 확장자: .xls,.xlsx",
            args.sources_dir.display(),
            args.sources_prefix
        )));
    }
    let source_sync::SourceIndexBuildResult {
        index: source_index,
        report: source_report,
    } = build_source_index_with_report(&source_paths)?;
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
                let _ = fs::remove_file(&backup);
                return Err(err(format!(
                    "백업 파일 생성에 실패했습니다: {} -> {} ({e})",
                    args.master.display(),
                    backup.display()
                )));
            }
            eprintln!("[백업 생성] {}", backup.display());
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
fn normalize_phone(s: &str) -> String {
    s.chars().filter(char::is_ascii_digit).collect()
}
fn same_phone(a: &str, b: &str) -> bool {
    let na = normalize_phone(a);
    let nb = normalize_phone(b);
    if !na.is_empty() || !nb.is_empty() {
        na == nb
    } else {
        same_trimmed(a, b)
    }
}
fn same_self_yn(a: &str, b: &str) -> bool {
    canon_header(a) == canon_header(b)
}
fn parse_i32_str(s: &str) -> Option<i32> {
    let t = s.trim();
    if t.is_empty() || t == "-" {
        return None;
    }
    let t = t.replace(',', "");
    t.parse::<f64>().ok().and_then(round_f64_to_i32)
}
fn usize_to_u32(value: usize, context: &str) -> Result<u32> {
    u32::try_from(value).map_err(|_| err(format!("{context} 값이 너무 큽니다. (value={value})")))
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
    let mut s = addr.trim().to_string();
    let replacements = [
        ("충청남도", "충남"),
        ("충청북도", "충북"),
        ("대전광역시", "대전"),
        ("세종특별자치시", "세종"),
    ];
    for (from, to) in replacements {
        s = s.replace(from, to);
    }
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
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
