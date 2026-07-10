extern crate alloc;
use core::fmt::Arguments;
use diagnostic::{AppError, Result, err, err_with_source};
use std::{
    env,
    ffi::OsStr,
    fs::{File, TryLockError},
    io::{self, Write, stdout},
    path::Path,
};
use update_run::UpdateRun;
mod build_info;
mod change_log;
mod decimal;
mod diagnostic;
mod excel;
mod master_sheet;
mod region;
mod sheet_util;
mod source_download;
mod temp_entry;
mod update_run;
const MASTER_PATH: &str = "fuel_cost_chungcheong.xlsx";
const RUN_LOCK_PATH: &str = ".fcupdater.lock";
#[derive(Debug)]
enum ParseAction {
    Help(String),
    Run { verify_saved_archive: bool },
    Version { verbose: bool },
}
struct RunLock {
    file: File,
}
impl TryFrom<&Path> for RunLock {
    type Error = AppError;
    fn try_from(path: &Path) -> Result<Self> {
        let file = File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(|source| err_with_source("실행 잠금 파일 열기 실패", source))?;
        match file.try_lock() {
            Ok(()) => Ok(Self { file }),
            Err(TryLockError::WouldBlock) => Err(err("다른 fcupdater 실행이 진행 중입니다.")),
            Err(TryLockError::Error(source)) => Err(err_with_source("실행 잠금 획득 실패", source)),
        }
    }
}
impl Drop for RunLock {
    fn drop(&mut self) {
        match self.file.unlock() {
            Ok(()) | Err(_) => {}
        }
    }
}
fn main() -> Result<()> {
    let mut out = stdout();
    let mut raw_args = env::args_os().skip(1);
    let first_arg = raw_args.next();
    let second_arg = raw_args.next();
    let third_arg = raw_args.next();
    let action = match first_arg {
        None => ParseAction::Run {
            verify_saved_archive: false,
        },
        Some(token) => {
            if token == OsStr::new("-h") || token == OsStr::new("--help") {
                if let Some(extra) = second_arg {
                    let usage = usage_text();
                    return Err(err(format!(
                        "알 수 없는 옵션: {}\n\n{usage}",
                        extra.to_string_lossy()
                    )));
                }
                ParseAction::Help(usage_text())
            } else if token == OsStr::new("--version") {
                let verbose = match second_arg {
                    None => false,
                    Some(flag) if flag == OsStr::new("--verbose") => {
                        if let Some(extra) = third_arg {
                            let usage = usage_text();
                            return Err(err(format!(
                                "알 수 없는 --version 옵션: {}\n\n{usage}",
                                extra.to_string_lossy()
                            )));
                        }
                        true
                    }
                    Some(flag) => {
                        let usage = usage_text();
                        return Err(err(format!(
                            "알 수 없는 --version 옵션: {}\n\n{usage}",
                            flag.to_string_lossy()
                        )));
                    }
                };
                ParseAction::Version { verbose }
            } else if token == OsStr::new("--verify") {
                if let Some(extra) = second_arg {
                    let usage = usage_text();
                    return Err(err(format!(
                        "알 수 없는 옵션: {}\n\n{usage}",
                        extra.to_string_lossy()
                    )));
                }
                ParseAction::Run {
                    verify_saved_archive: true,
                }
            } else {
                let usage = usage_text();
                return Err(err(format!(
                    "알 수 없는 옵션: {}\n\n{usage}",
                    token.to_string_lossy()
                )));
            }
        }
    };
    match action {
        ParseAction::Run {
            verify_saved_archive,
        } => {
            let run_lock = RunLock::try_from(Path::new(RUN_LOCK_PATH))?;
            let mut update = UpdateRun {
                master_path: Path::new(MASTER_PATH),
                out: &mut out,
                verify_saved_archive,
            };
            let result = update.run();
            drop(run_lock);
            result
        }
        ParseAction::Help(text) => {
            write_line(&mut out, format_args!("{text}"))?;
            Ok(())
        }
        ParseAction::Version { verbose } => {
            write_line(
                &mut out,
                format_args!("{} {}", build_info::APP_NAME, build_info::APP_VERSION),
            )?;
            if verbose {
                for (label, value) in [
                    ("target", build_info::BUILD_TARGET),
                    ("profile", build_info::BUILD_PROFILE),
                    ("rustc", build_info::BUILD_RUSTC),
                    ("git", build_info::BUILD_GIT_SHA),
                    ("dirty", build_info::BUILD_GIT_DIRTY),
                ] {
                    write_line(&mut out, format_args!("{label}: {value}"))?;
                }
            }
            Ok(())
        }
    }
}
fn usage_text() -> String {
    format!(
        concat!(
            "{} {}\n주유소 가격/정보 현행화 (Excel 미설치 OK)\n\n",
            "사용법:\n  {APP_NAME} [--verify]\n\n",
            "고정 동작:\n",
            "  마스터: fuel_cost_chungcheong.xlsx 직접 현행화\n",
            "  소스: Opinet 현재 판매가격(주유소) 자동 다운로드 .xls\n",
            "  변경내역 시트: 항상 갱신\n",
            "  저장 검증: 기본 생략 (--verify 사용 시 수행)\n\n",
            "옵션:\n",
            "  -h, --help               도움말\n",
            "  --verify                 저장 후 임시 XLSX를 재열어 검증한 뒤 승격\n",
            "  --version                버전\n",
            "  --version --verbose      빌드 메타데이터 포함 버전",
        ),
        build_info::APP_NAME,
        build_info::APP_VERSION,
        APP_NAME = build_info::APP_NAME,
    )
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
