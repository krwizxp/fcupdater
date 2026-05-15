use crate::{Result, err, path_source_message, prefixed_message};
use core::time::Duration;
use std::{
    fs,
    io::{ErrorKind, Read as _, Write as _},
    path::{Path, PathBuf},
};
pub const MAX_CONFLICT_ATTEMPTS: u32 = 100_000;
pub const RESERVATION_MAGIC: &[u8] = b"FCUPDATER_RESERVED_v1\n";
const RESERVATION_MAGIC_LEN: usize = b"FCUPDATER_RESERVED_v1\n".len();
const STALE_RESERVATION_AGE: Duration = Duration::from_hours(1);
pub fn candidate_with_suffix(path: &Path, seq: u32) -> PathBuf {
    if seq == 0 {
        return path.to_path_buf();
    }
    let parent = path_parent_or_current(path);
    let stem = path_file_stem_or(path, "output");
    let ext = path
        .extension()
        .and_then(|extension_os| extension_os.to_str());
    let file_name = ext.map_or_else(
        || format!("{stem}_{seq}"),
        |file_ext| format!("{stem}_{seq}.{file_ext}"),
    );
    parent.join(file_name)
}
pub fn reserve_nonconflicting_path(path: &Path) -> Result<PathBuf> {
    let parent = path_parent_or_current(path);
    fs::create_dir_all(parent).map_err(|source_err| {
        err(path_source_message(
            "출력 폴더 생성 실패",
            parent,
            source_err,
        ))
    })?;
    let mut seq = 0_u32;
    loop {
        let candidate = candidate_with_suffix(path, seq);
        match fs::File::create_new(&candidate) {
            Ok(mut file) => {
                if let Err(write_err) = file
                    .write_all(RESERVATION_MAGIC)
                    .and_then(|()| file.flush())
                    .and_then(|()| file.sync_all())
                {
                    drop(file);
                    match fs::remove_file(&candidate) {
                        Ok(()) | Err(_) => {}
                    }
                    return Err(err(path_source_message(
                        "출력 파일 예약 마커 기록 실패",
                        &candidate,
                        write_err,
                    )));
                }
                return Ok(candidate);
            }
            Err(io_err) if io_err.kind() == ErrorKind::AlreadyExists => {
                let remove_stale = fs::metadata(&candidate)
                    .ok()
                    .filter(fs::Metadata::is_file)
                    .and_then(|meta| meta.modified().ok())
                    .and_then(|modified| modified.elapsed().ok())
                    .filter(|elapsed| *elapsed >= STALE_RESERVATION_AGE)
                    .is_some_and(|_| {
                        file_has_reservation_magic(&candidate)
                            && fs::remove_file(&candidate).is_ok()
                    });
                if remove_stale {
                    continue;
                }
                seq = seq.checked_add(1).ok_or_else(|| {
                    err(prefixed_message(
                        "출력 파일 예약 시퀀스 계산 overflow: ",
                        path.display(),
                    ))
                })?;
                if seq > MAX_CONFLICT_ATTEMPTS {
                    return Err(err(prefixed_message(
                        "출력 파일 예약 충돌이 너무 많아 경로를 확정할 수 없습니다: ",
                        path.display(),
                    )));
                }
            }
            Err(io_err) => {
                return Err(err(path_source_message(
                    "출력 파일 예약 실패",
                    &candidate,
                    io_err,
                )));
            }
        }
    }
}
pub fn path_parent_or_current(path: &Path) -> &Path {
    path.parent().unwrap_or_else(|| Path::new("."))
}
pub fn file_has_reservation_magic(path: &Path) -> bool {
    let Ok(mut file) = fs::File::open(path) else {
        return false;
    };
    let mut magic = [0_u8; RESERVATION_MAGIC_LEN];
    if file.read_exact(&mut magic).is_err() {
        return false;
    }
    let mut extra = [0_u8; 1];
    matches!(file.read(&mut extra), Ok(0)) && magic == RESERVATION_MAGIC
}
pub fn path_file_stem_or<'a>(path: &'a Path, default: &'a str) -> &'a str {
    path.file_stem()
        .and_then(|stem_os| stem_os.to_str())
        .unwrap_or(default)
}
pub fn source_label(path: &Path) -> String {
    path.file_name()
        .and_then(|file_name_os| file_name_os.to_str())
        .map_or_else(|| path.display().to_string(), str::to_owned)
}
