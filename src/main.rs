extern crate alloc;
use core::fmt::Arguments;
use diagnostic::{Result, err, err_with_source};
use excel::SaveVerification;
use std::{
    env,
    ffi::OsStr,
    fs::{File, Metadata, TryLockError},
    io::{self, Write, stdout},
    path::Path,
};
cfg_select! {
    target_os = "windows" => {
        use std::os::windows::fs::OpenOptionsExt as _;
    }
    any(target_os = "linux", target_os = "macos") => {
        use std::os::unix::fs::OpenOptionsExt as _;
    }
}
use update_run::UpdateRun;
mod change_log;
mod diagnostic;
mod excel;
mod master_sheet;
mod region;
mod sheet_util;
mod source_download;
mod temp_entry;
mod update_run;
const APP_NAME: &str = env!("CARGO_PKG_NAME");
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const HELP_TEXT: &str = concat!(
    env!("CARGO_PKG_NAME"),
    " ",
    env!("CARGO_PKG_VERSION"),
    "\n주유소 가격/정보 현행화 (Excel 미설치 OK)\n\n",
    "사용법:\n  ",
    env!("CARGO_PKG_NAME"),
    " [--verify]\n\n",
    "고정 동작:\n",
    "  마스터: fuel_cost_chungcheong.xlsx 직접 현행화\n",
    "  소스: Opinet 현재 판매가격(주유소) 자동 다운로드 .xls\n",
    "  변경내역 시트: 항상 갱신\n",
    "  저장 검증: 기본 생략 (--verify 사용 시 수행)\n\n",
    "옵션:\n",
    "  -h, --help               도움말\n",
    "  --verify                 저장 후 임시 XLSX를 재열어 검증한 뒤 승격\n",
    "  --version                버전"
);
const MASTER_PATH: &str = "fuel_cost_chungcheong.xlsx";
const RUN_LOCK_PATH: &str = ".fcupdater.lock";
#[cfg(target_os = "windows")]
const RUN_LOCK_SHARE_MODE: u32 = 0x0000_0003;
fn validate_regular_file(file: &File) -> io::Result<Metadata> {
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "경로는 일반 파일이어야 합니다.",
        ));
    }
    Ok(metadata)
}
fn main() -> Result<()> {
    let mut out = stdout();
    let mut raw_args = env::args_os().skip(1);
    let save_verification = match raw_args.next() {
        None => SaveVerification::Skip,
        Some(token) => {
            if token == OsStr::new("-h") || token == OsStr::new("--help") {
                if let Some(extra) = raw_args.next() {
                    return Err(err(format!(
                        "알 수 없는 옵션: {}\n\n{HELP_TEXT}",
                        extra.to_string_lossy()
                    )));
                }
                write_line(&mut out, format_args!("{HELP_TEXT}"))?;
                return Ok(());
            }
            if token == OsStr::new("--version") {
                if let Some(extra) = raw_args.next() {
                    return Err(err(format!(
                        "알 수 없는 --version 옵션: {}\n\n{HELP_TEXT}",
                        extra.to_string_lossy()
                    )));
                }
                write_line(&mut out, format_args!("{APP_NAME} {APP_VERSION}"))?;
                return Ok(());
            }
            if token == OsStr::new("--verify") {
                if let Some(extra) = raw_args.next() {
                    return Err(err(format!(
                        "알 수 없는 옵션: {}\n\n{HELP_TEXT}",
                        extra.to_string_lossy()
                    )));
                }
                SaveVerification::Verify
            } else {
                return Err(err(format!(
                    "알 수 없는 옵션: {}\n\n{HELP_TEXT}",
                    token.to_string_lossy()
                )));
            }
        }
    };
    let mut lock_options = File::options();
    lock_options
        .read(true)
        .write(true)
        .create(true)
        .truncate(false);
    cfg_select! {
        target_os = "windows" => {
            lock_options.share_mode(RUN_LOCK_SHARE_MODE);
        }
        any(target_os = "linux", target_os = "macos") => {
            lock_options.mode(0o600);
        }
    }
    let run_lock = lock_options
        .open(Path::new(RUN_LOCK_PATH))
        .map_err(|source| err_with_source("실행 잠금 파일 열기 실패", source))?;
    validate_regular_file(&run_lock)
        .map_err(|source| err_with_source("실행 잠금 파일 검증 실패", source))?;
    match run_lock.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Err(err("다른 fcupdater 실행이 진행 중입니다.")),
        Err(TryLockError::Error(source)) => {
            return Err(err_with_source("실행 잠금 획득 실패", source));
        }
    }
    UpdateRun {
        master_path: Path::new(MASTER_PATH),
        out: &mut out,
        save_verification,
    }
    .run()
}
fn write_line(output: &mut dyn Write, args: Arguments<'_>) -> io::Result<()> {
    output.write_fmt(args)?;
    output.write_all(b"\n")
}
fn write_line_best_effort(output: &mut dyn Write, args: Arguments<'_>) {
    match write_line(output, args) {
        Ok(()) | Err(_) => {}
    }
}
