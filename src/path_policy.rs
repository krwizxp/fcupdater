use crate::{
    Result,
    cli::{Args, OutputTarget},
    err,
};
use std::{
    fs,
    io::ErrorKind,
    io::Write,
    path::{Path, PathBuf},
    time::Duration,
};
const RESERVATION_MAGIC: &[u8] = b"FCUPDATER_RESERVED_v1\n";
const STALE_RESERVATION_AGE: Duration = Duration::from_secs(60 * 60);
const MAX_CONFLICT_ATTEMPTS: u32 = 100_000;
pub fn decide_output_path(args: &Args, today: &str, dry_run: bool) -> Result<PathBuf> {
    if matches!(args.output_target, OutputTarget::InPlace) {
        return Ok(args.master.clone());
    }
    let requested = match &args.output_target {
        OutputTarget::Auto => {
            let stem = args
                .master
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("fuel_cost_chungcheong");
            let parent = args.master.parent().unwrap_or_else(|| Path::new("."));
            parent.join(format!("{stem}_updated_{today}.xlsx"))
        }
        OutputTarget::Explicit(path) => path.clone(),
        OutputTarget::InPlace => args.master.clone(),
    };
    if dry_run {
        make_nonconflicting_path(&requested)
    } else {
        reserve_nonconflicting_path(&requested)
    }
}
pub fn reserve_backup_path(master: &Path, today: &str) -> Result<PathBuf> {
    let parent = master.parent().unwrap_or_else(|| Path::new("."));
    let stem = master
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("fuel_cost_chungcheong");
    let base = parent.join(format!("{stem}_backup_{today}.xlsx"));
    reserve_nonconflicting_path(&base)
}
pub fn cleanup_reservation_file(path: &Path) {
    let Ok(content) = fs::read(path) else {
        return;
    };
    if content == RESERVATION_MAGIC {
        let _ = fs::remove_file(path);
    }
}
fn make_nonconflicting_path(path: &Path) -> Result<PathBuf> {
    let mut seq = 0u32;
    loop {
        let candidate = candidate_with_suffix(path, seq);
        if !candidate.exists() {
            return Ok(candidate);
        }
        seq = seq.checked_add(1).ok_or_else(|| {
            err(format!(
                "출력 파일명 시퀀스 계산 overflow: {}",
                path.display()
            ))
        })?;
        if seq > MAX_CONFLICT_ATTEMPTS {
            return Err(err(format!(
                "출력 파일명 충돌이 너무 많아 경로를 확정할 수 없습니다: {}",
                path.display()
            )));
        }
    }
}
fn reserve_nonconflicting_path(path: &Path) -> Result<PathBuf> {
    ensure_parent_dir(path)?;
    let mut seq = 0u32;
    loop {
        let candidate = candidate_with_suffix(path, seq);
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(mut file) => {
                if let Err(e) = file
                    .write_all(RESERVATION_MAGIC)
                    .and_then(|()| file.flush())
                    .and_then(|()| file.sync_all())
                {
                    drop(file);
                    let _ = fs::remove_file(&candidate);
                    return Err(err(format!(
                        "출력 파일 예약 마커 기록 실패: {} ({e})",
                        candidate.display()
                    )));
                }
                return Ok(candidate);
            }
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                if try_remove_stale_reservation(&candidate) {
                    continue;
                }
                seq = seq.checked_add(1).ok_or_else(|| {
                    err(format!(
                        "출력 파일 예약 시퀀스 계산 overflow: {}",
                        path.display()
                    ))
                })?;
                if seq > MAX_CONFLICT_ATTEMPTS {
                    return Err(err(format!(
                        "출력 파일 예약 충돌이 너무 많아 경로를 확정할 수 없습니다: {}",
                        path.display()
                    )));
                }
            }
            Err(e) => {
                return Err(err(format!(
                    "출력 파일 예약 실패: {} ({e})",
                    candidate.display()
                )));
            }
        }
    }
}
fn ensure_parent_dir(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)
        .map_err(|e| err(format!("출력 폴더 생성 실패: {} ({e})", parent.display())))
}
fn try_remove_stale_reservation(path: &Path) -> bool {
    let Ok(meta) = fs::metadata(path) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    let Ok(modified) = meta.modified() else {
        return false;
    };
    let Ok(elapsed) = modified.elapsed() else {
        return false;
    };
    if elapsed < STALE_RESERVATION_AGE {
        return false;
    }
    let Ok(content) = fs::read(path) else {
        return false;
    };
    if content != RESERVATION_MAGIC {
        return false;
    }
    fs::remove_file(path).is_ok()
}
fn candidate_with_suffix(path: &Path, seq: u32) -> PathBuf {
    if seq == 0 {
        return path.to_path_buf();
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("output");
    let ext = path.extension().and_then(|s| s.to_str());
    let file_name = ext.map_or_else(
        || format!("{stem}_{seq}"),
        |ext| format!("{stem}_{seq}.{ext}"),
    );
    parent.join(file_name)
}
