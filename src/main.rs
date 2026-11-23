extern crate alloc;
use diagnostic::{Result, bail_fmt, ensure_fmt, err, err_src};
use excel::SaveVerification;
use std::{
    env,
    fs::{File, TryLockError},
    io::{Write as _, stdout},
    path::Path,
};
cfg_select! {
    target_os = "windows" => {
        use std::os::windows::fs::OpenOptionsExt as _;
    }
    any(target_os = "linux", target_os = "macos") => {
        use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
    }
}
use temp_entry::{configure_no_follow, validate_regular_file};
use update_run::UpdateRun;
mod change_log;
mod diagnostic;
mod excel;
mod master_sheet;
mod region;
mod source_download;
mod temp_entry;
mod update_run;
const APP_NAME: &str = env!("CARGO_PKG_NAME");
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const HELP_TEXT: &str = concat!(
    env!("CARGO_PKG_NAME"),
    " ",
    env!("CARGO_PKG_VERSION"),
    "\n주유소 가격/정보 현행화 (Excel 미설치 OK)\n\n사용법:\n  ",
    env!("CARGO_PKG_NAME"),
    " [--verify]\n\n고정 동작:\n  마스터: fuel_cost_chungcheong.xlsx 직접 현행화\n  소스: Opinet 현재 판매가격(주유소) 자동 다운로드 .xls\n  변경내역 시트: 항상 갱신\n  저장 검증: 기본 생략 (--verify 사용 시 수행)\n\n옵션:\n  -h, --help               도움말\n  --verify                 저장 후 임시 XLSX를 재열어 검증한 뒤 승격\n  --version                버전"
);
fn main() -> Result<()> {
    let mut out = stdout().lock();
    let mut raw_args = env::args_os().skip(1);
    let save_verification = match raw_args.next() {
        None => SaveVerification::Skip,
        Some(token) => {
            let (is_help, is_ver) = (token == "-h" || token == "--help", token == "--version");
            ensure_fmt!(
                is_help || token == "--verify" || is_ver,
                "알 수 없는 옵션: {}\n\n{HELP_TEXT}",
                token.display()
            );
            if let Some(extra) = raw_args.next() {
                let ctx = if is_ver {
                    "알 수 없는 --version 옵션"
                } else {
                    "알 수 없는 옵션"
                };
                bail_fmt!("{ctx}: {}\n\n{HELP_TEXT}", extra.display());
            }
            if is_help {
                return writeln!(&mut out, "{HELP_TEXT}").map_err(Into::into);
            }
            if is_ver {
                return writeln!(&mut out, "{APP_NAME} {APP_VERSION}").map_err(Into::into);
            }
            SaveVerification::Verify
        }
    };
    let mut lock_opts = File::options();
    lock_opts
        .read(true)
        .write(true)
        .create(true)
        .truncate(false);
    configure_no_follow(&mut lock_opts);
    cfg_select! {
        target_os = "windows" => { lock_opts.share_mode(0x0000_0003); }
        any(target_os = "linux", target_os = "macos") => { lock_opts.mode(0o600); }
    }
    let run_lock = lock_opts
        .open(Path::new(".fcupdater.lock"))
        .map_err(|src| err_src("실행 잠금 파일 열기 실패", src))?;
    let lock_val =
        validate_regular_file(&run_lock).map_err(|src| err_src("실행 잠금 파일 검증 실패", src))?;
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    (lock_val.0.mode() & 0o022 == 0).ok_or("실행 잠금 파일 group/other 쓰기 권한 불가")?;
    #[cfg(target_os = "windows")]
    let (_, _) = lock_val;
    match run_lock.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Err(err("다른 fcupdater 실행이 진행 중입니다.")),
        Err(TryLockError::Error(src)) => return Err(err_src("실행 잠금 획득 실패", src)),
    }
    UpdateRun {
        master_path: Path::new("fuel_cost_chungcheong.xlsx"),
        out: &mut out,
        save_verification,
    }
    .run()
}
