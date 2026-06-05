extern crate alloc;
use core::fmt::Arguments;
use diagnostic::{Result, err};
use std::{
    env,
    ffi::OsStr,
    io::{self, Write, stdout},
    path::Path,
};
use update_run::UpdateRun;
mod change_log;
mod diagnostic;
mod excel;
mod master_sheet;
mod region;
mod rows;
mod sheet_util;
mod source_download;
mod update_run;
const APP_NAME: &str = "fcupdater";
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const MASTER_PATH: &str = "fuel_cost_chungcheong.xlsx";
#[derive(Debug)]
enum ParseAction {
    Help(String),
    Run,
    Version(String),
}
fn main() -> Result<()> {
    let mut out = stdout();
    let mut raw_args = env::args_os().skip(1);
    let first_arg = raw_args.next();
    let has_extra = raw_args.next().is_some();
    let action = match first_arg {
        None => ParseAction::Run,
        Some(token) => {
            if has_extra {
                let usage = usage_text();
                return Err(err(format!(
                    "알 수 없는 옵션: {}\n\n{usage}",
                    token.to_string_lossy()
                )));
            }
            if token == OsStr::new("-h") || token == OsStr::new("--help") {
                ParseAction::Help(usage_text())
            } else if token == OsStr::new("--version") {
                ParseAction::Version(format!("{APP_NAME} {APP_VERSION}"))
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
        ParseAction::Run => {
            let mut update = UpdateRun {
                master_path: Path::new(MASTER_PATH),
                out: &mut out,
            };
            update.run()
        }
        ParseAction::Help(text) | ParseAction::Version(text) => {
            write_line(&mut out, format_args!("{text}"))?;
            Ok(())
        }
    }
}
fn usage_text() -> String {
    format!(
        "{APP_NAME} {APP_VERSION}\n주유소 가격/정보 현행화 (Excel 미설치 OK)\n\n\
사용법:\n  {APP_NAME}\n\n\
고정 동작:\n  마스터: fuel_cost_chungcheong.xlsx 직접 현행화\n  소스: Opinet 현재 판매가격(주유소) 자동 다운로드 .xls\n  변경내역 시트: 항상 갱신\n  저장 검증: 항상 수행\n\n\
옵션:\n  -h, --help               도움말\n  --version                버전"
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
