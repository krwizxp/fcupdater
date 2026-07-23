use core::time::Duration;
use std::{
    fs, io,
    path::Path,
    process,
    time::{SystemTime, UNIX_EPOCH},
};
const STALE_TEMP_ENTRY_AGE: Duration = Duration::from_hours(24);
const TEMP_ENTRY_RESERVATION_ATTEMPTS: u32 = 1024;
pub(crate) fn cleanup_stale_temp_files(parent: &Path, prefix: &str) -> io::Result<usize> {
    let now_nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos();
    let mut removed = 0_usize;
    for entry_result in fs::read_dir(parent)? {
        let entry = entry_result?;
        let file_name_os = entry.file_name();
        let Some(file_name) = file_name_os.to_str() else {
            continue;
        };
        let Some(suffix) = file_name.strip_prefix(prefix) else {
            continue;
        };
        let mut fragments = suffix.split('_');
        let (Some(pid), Some(created_at), Some(sequence), None) = (
            fragments.next(),
            fragments.next(),
            fragments.next(),
            fragments.next(),
        ) else {
            continue;
        };
        if pid.parse::<u32>().is_err() || sequence.parse::<u32>().is_err() {
            continue;
        }
        let Ok(created_at_nanos) = created_at.parse::<u128>() else {
            continue;
        };
        let Some(age_nanos) = now_nanos.checked_sub(created_at_nanos) else {
            continue;
        };
        if age_nanos < STALE_TEMP_ENTRY_AGE.as_nanos() {
            continue;
        }
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            continue;
        }
        let path = entry.path();
        match fs::remove_file(&path) {
            Ok(()) => {
                removed = removed.saturating_add(1);
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!("{}: {error}", path.display()),
                ));
            }
        }
    }
    Ok(removed)
}
pub(crate) fn reserve_unique_temp_entry<T>(
    parent: &Path,
    prefix: &str,
    mut create_entry: impl FnMut(&Path) -> io::Result<T>,
) -> io::Result<T> {
    let pid = process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos();
    for sequence in 0..TEMP_ENTRY_RESERVATION_ATTEMPTS {
        let path = parent.join(format!("{prefix}{pid}_{nanos}_{sequence}"));
        match create_entry(&path) {
            Ok(entry) => return Ok(entry),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "임시 항목 이름 충돌이 반복되었습니다. 잠시 후 다시 시도하세요.",
    ))
}
