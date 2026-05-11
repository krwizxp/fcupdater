use super::{
    path_util::path_from_slashes,
    zip_archive::{self, ZipArchiveOpsExt as _},
};
use crate::{
    Result, err, path_pair_source_message, path_source_message, prefixed_message, push_display,
};
use core::{mem, time::Duration};
use std::{
    env, fs,
    io::{self, ErrorKind},
    path::{Component, Path, PathBuf},
    process, thread,
    time::{SystemTime, UNIX_EPOCH},
};
cfg_select! {
    windows => {
        use core::iter::once;
        use core::ptr::{null, null_mut};
        use std::os::windows::ffi::OsStrExt as _;
    }
    _ => {
        use std::io::{stderr, Write as _};
    }
}
#[derive(Debug)]
pub struct XlsxContainer {
    archive_path: PathBuf,
    unpack_dir: PathBuf,
    work_dir: PathBuf,
}
#[derive(Debug)]
struct WorkDirCleanup {
    keep: bool,
    path: PathBuf,
}
struct ArchiveOps;
trait ArchiveOpsExt {
    fn create_archive(&self, unpack_dir: &Path, archive_path: &Path) -> Result<()>;
    fn promote_temp_output(&self, temp_output: &Path, output_xlsx: &Path) -> Result<()>;
    fn verify_saved_archive(&self, saved_archive: &Path) -> Result<()>;
}
impl Drop for WorkDirCleanup {
    fn drop(&mut self) {
        if !self.keep && !self.path.as_os_str().is_empty() {
            match fs::remove_dir_all(&self.path) {
                Ok(()) | Err(_) => {}
            }
        }
    }
}
impl ArchiveOpsExt for ArchiveOps {
    fn create_archive(&self, unpack_dir: &Path, archive_path: &Path) -> Result<()> {
        zip_archive::ZipArchiveOps.create_from_directory(unpack_dir, archive_path)
    }
    fn promote_temp_output(&self, temp_output: &Path, output_xlsx: &Path) -> Result<()> {
        cfg_select! {
            windows => {
                let replacement_w = encode_path_wide(temp_output);
                let destination_w = encode_path_wide(output_xlsx);
                if output_xlsx.try_exists().map_err(|source_err| {
                    err(path_source_message(
                        "대상 파일 경로 확인 실패",
                        output_xlsx,
                        source_err,
                    ))
                })? {
                    // SAFETY: Both UTF-16 path buffers are NUL-terminated and live across the call; optional pointers are intentionally null.
                    let replaced = unsafe {
                        super::windows_api::ReplaceFileW(
                            destination_w.as_ptr(),
                            replacement_w.as_ptr(),
                            null(),
                            super::windows_api::REPLACEFILE_WRITE_THROUGH,
                            null_mut(),
                            null_mut(),
                        )
                    };
                    if replaced != 0_i32 {
                        return Ok(());
                    }
                    // SAFETY: Called immediately after the failing Windows API call on the same thread.
                    let code = unsafe { super::windows_api::GetLastError() };
                    return Err(err(windows_file_op_error_message(
                        "파일 교체 실패(ReplaceFileW): ",
                        output_xlsx,
                        Some(temp_output),
                        " <- ",
                        code,
                    )));
                }
                // SAFETY: Both UTF-16 path buffers are NUL-terminated and valid for the duration of the call.
                let moved = unsafe {
                    super::windows_api::MoveFileExW(
                        replacement_w.as_ptr(),
                        destination_w.as_ptr(),
                        super::windows_api::MOVEFILE_REPLACE_EXISTING
                            | super::windows_api::MOVEFILE_WRITE_THROUGH,
                    )
                };
                if moved != 0_i32 {
                    return Ok(());
                }
                // SAFETY: Called immediately after the failing Windows API call on the same thread.
                let code = unsafe { super::windows_api::GetLastError() };
                Err(err(windows_file_op_error_message(
                    "파일 이동 실패(MoveFileExW): ",
                    temp_output,
                    Some(output_xlsx),
                    " -> ",
                    code,
                )))
            }
            _ => {
                fs::rename(temp_output, output_xlsx).map_err(|source_err| {
                    err(path_pair_source_message(
                        "xlsx 저장 실패",
                        temp_output,
                        output_xlsx,
                        source_err,
                    ))
                })?;
                if let Err(source_err) = fs::OpenOptions::new()
                    .read(true)
                    .open(output_xlsx)
                    .and_then(|file| file.sync_all())
                {
                    if durability_strict_mode() {
                        return Err(err(path_source_message(
                            "xlsx 저장 내구성 동기화 실패(파일)",
                            output_xlsx,
                            source_err,
                        )));
                    }
                    let mut output_path = String::with_capacity(64);
                    push_display(&mut output_path, output_xlsx.display());
                    write_durability_warning("파일", &output_path, &source_err);
                }
                if let Some(parent) = output_xlsx.parent()
                    && let Err(source_err) = fs::File::open(parent).and_then(|dir| dir.sync_all())
                {
                    if durability_strict_mode() {
                        return Err(err(path_source_message(
                            "xlsx 저장 내구성 동기화 실패(폴더)",
                            parent,
                            source_err,
                        )));
                    }
                    let mut parent_path = String::with_capacity(64);
                    push_display(&mut parent_path, parent.display());
                    write_durability_warning("폴더", &parent_path, &source_err);
                }
                Ok(())
            }
        }
    }
    fn verify_saved_archive(&self, saved_archive: &Path) -> Result<()> {
        let verify_work = create_unique_work_dir()?;
        let verify_unpacked = verify_work.join("verify_unpacked");
        create_dir_all_checked(&verify_unpacked, "저장 검증용 임시 폴더 생성 실패")?;
        let verify_result = (|| -> Result<()> {
            for entry_name in Self::list_archive_entries(saved_archive)? {
                if !Self::is_safe_archive_entry_path(&entry_name) {
                    return Err(err(prefixed_message(
                        "허용되지 않은 압축 경로가 포함되어 있습니다: ",
                        entry_name,
                    )));
                }
            }
            extract_archive(saved_archive, &verify_unpacked)?;
            for rel in [
                "[Content_Types].xml",
                "xl/workbook.xml",
                "xl/_rels/workbook.xml.rels",
            ] {
                let path = verify_unpacked.join(path_from_slashes(rel));
                if !path.is_file() {
                    return Err(err(prefixed_message(
                        "저장 검증 실패: 필수 OOXML 파트가 없습니다: ",
                        path.display(),
                    )));
                }
            }
            super::writer::Workbook::open(saved_archive).map_err(|source_err| {
                err(path_source_message(
                    "저장 검증 실패: 저장 직후 재열기 점검에 실패했습니다",
                    saved_archive,
                    source_err,
                ))
            })?;
            Ok(())
        })();
        match fs::remove_dir_all(&verify_work) {
            Ok(()) | Err(_) => {}
        }
        verify_result
    }
}
impl ArchiveOps {
    fn is_safe_archive_entry_path(entry_name: &str) -> bool {
        if entry_name.is_empty() || entry_name.starts_with(['/', '\\']) {
            return false;
        }
        let bytes = entry_name.as_bytes();
        if let Some((&first, &colon)) = bytes.first().zip(bytes.get(1))
            && colon == b':'
            && first.is_ascii_alphabetic()
        {
            return false;
        }
        let mut has_name = false;
        for part in entry_name.split(['/', '\\']) {
            if part.is_empty() || part == "." {
                continue;
            }
            if part == ".." {
                return false;
            }
            has_name = true;
        }
        has_name
    }
    fn list_archive_entries(archive_path: &Path) -> Result<Vec<String>> {
        zip_archive::ZipArchiveOps.list_entries(archive_path)
    }
}
impl XlsxContainer {
    pub fn open_for_update(source_xlsx: &Path) -> Result<Self> {
        if !source_xlsx.try_exists().map_err(|source_err| {
            err(path_source_message(
                "xlsx 파일 경로 확인 실패",
                source_xlsx,
                source_err,
            ))
        })? {
            return Err(err(prefixed_message(
                "xlsx 파일이 없습니다: ",
                source_xlsx.display(),
            )));
        }
        let mut cleanup = WorkDirCleanup {
            path: (create_unique_work_dir())?,
            keep: false,
        };
        let unpack_dir = cleanup.path.join("unzipped");
        let archive_path = cleanup.path.join("workbook.zip");
        create_dir_all_checked(&unpack_dir, "임시 폴더 생성 실패")?;
        fs::copy(source_xlsx, &archive_path).map_err(|source_err| {
            err(path_pair_source_message(
                "xlsx 임시 복사 실패",
                source_xlsx,
                &archive_path,
                source_err,
            ))
        })?;
        for entry_name in ArchiveOps::list_archive_entries(&archive_path)? {
            if !ArchiveOps::is_safe_archive_entry_path(&entry_name) {
                return Err(err(prefixed_message(
                    "허용되지 않은 압축 경로가 포함되어 있습니다: ",
                    entry_name,
                )));
            }
        }
        extract_archive(&archive_path, &unpack_dir)?;
        cleanup.keep = true;
        let work_dir = mem::take(&mut cleanup.path);
        Ok(Self {
            archive_path,
            unpack_dir,
            work_dir,
        })
    }
    pub fn read_text(&self, relative_path: &str) -> Result<String> {
        let path = (self.resolve_relative_path(relative_path))?;
        fs::read_to_string(&path)
            .map_err(|source_err| err(path_source_message("파일 읽기 실패", &path, source_err)))
    }
    pub fn remove_file_if_exists(&self, relative_path: &str) -> Result<()> {
        let path = (self.resolve_relative_path(relative_path))?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(io_err) if io_err.kind() == ErrorKind::NotFound => Ok(()),
            Err(io_err) => Err(err(path_source_message("파일 삭제 실패", &path, io_err))),
        }
    }
    fn resolve_relative_path(&self, relative_path: &str) -> Result<PathBuf> {
        let mut path = PathBuf::new();
        for component in Path::new(relative_path).components() {
            match component {
                Component::CurDir => {}
                Component::Normal(segment) => path.push(segment),
                Component::ParentDir => {
                    return Err(err(relative_path_policy_message(
                        "상위 경로 탐색은 허용되지 않습니다: ",
                        relative_path,
                    )));
                }
                Component::RootDir | Component::Prefix(_) => {
                    return Err(err(relative_path_policy_message(
                        "절대 경로는 허용되지 않습니다: ",
                        relative_path,
                    )));
                }
            }
        }
        if path.as_os_str().is_empty() {
            return Err(err(relative_path_policy_message(
                "상대 경로가 비어 있습니다: ",
                relative_path,
            )));
        }
        Ok(self.unpack_dir.join(path))
    }
    pub fn save_as(&self, output_xlsx: &Path, verify_saved_file: bool) -> Result<()> {
        let archive_ops = ArchiveOps;
        if self.archive_path.try_exists().map_err(|source_err| {
            err(path_source_message(
                "archive 경로 확인 실패",
                &self.archive_path,
                source_err,
            ))
        })? {
            fs::remove_file(&self.archive_path).map_err(|source_err| {
                err(path_source_message(
                    "기존 archive 삭제 실패",
                    &self.archive_path,
                    source_err,
                ))
            })?;
        }
        archive_ops.create_archive(&self.unpack_dir, &self.archive_path)?;
        output_xlsx
            .parent()
            .map(|parent| create_dir_all_checked(parent, "출력 폴더 생성 실패"))
            .transpose()?;
        let parent = output_xlsx.parent().unwrap_or_else(|| Path::new("."));
        let file_name = output_xlsx
            .file_name()
            .and_then(|file_name_os| file_name_os.to_str())
            .unwrap_or("output.xlsx");
        let tmp_output = reserve_unique_temp_entry(
            |pid, nanos, seq| {
                let mut temp_name = String::with_capacity(
                    file_name
                        .len()
                        .saturating_add(40)
                        .saturating_add(".tmp___".len())
                        .saturating_add(1),
                );
                temp_name.push('.');
                temp_name.push_str(file_name);
                temp_name.push_str(".tmp_");
                push_display(&mut temp_name, pid);
                temp_name.push('_');
                push_display(&mut temp_name, nanos);
                temp_name.push('_');
                push_display(&mut temp_name, seq);
                parent.join(temp_name)
            },
            |path| {
                fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .map(|_| ())
            },
            "임시 출력 파일 생성 실패",
            prefixed_message("임시 출력 파일 경로 생성 실패: ", output_xlsx.display()),
        )?;
        let result = (|| -> Result<()> {
            fs::copy(&self.archive_path, &tmp_output).map_err(|source_err| {
                err(path_pair_source_message(
                    "xlsx 임시 저장 실패",
                    &self.archive_path,
                    &tmp_output,
                    source_err,
                ))
            })?;
            if verify_saved_file {
                archive_ops.verify_saved_archive(&tmp_output)?;
            }
            archive_ops.promote_temp_output(&tmp_output, output_xlsx)?;
            Ok(())
        })();
        if result.is_err() {
            match fs::remove_file(&tmp_output) {
                Ok(()) | Err(_) => {}
            }
        }
        result
    }
    pub fn unpack_dir(&self) -> &Path {
        &self.unpack_dir
    }
    pub fn write_text(&self, relative_path: &str, content: &str) -> Result<()> {
        let path = (self.resolve_relative_path(relative_path))?;
        if let Some(parent) = path.parent() {
            create_dir_all_checked(parent, "폴더 생성 실패")?;
        }
        fs::write(&path, content)
            .map_err(|source_err| err(path_source_message("파일 쓰기 실패", &path, source_err)))
    }
}
impl Drop for XlsxContainer {
    fn drop(&mut self) {
        match fs::remove_dir_all(&self.work_dir) {
            Ok(()) | Err(_) => {}
        }
    }
}
#[cfg(not(windows))]
fn write_durability_warning(path_kind: &str, path_text: &str, source_err: &io::Error) {
    let mut err = stderr().lock();
    match writeln!(
        err,
        "경고: 저장 내구성 동기화 실패({path_kind}): {path_text} ({source_err})",
    ) {
        Ok(()) | Err(_) => {}
    }
}
fn create_unique_work_dir() -> Result<PathBuf> {
    let base = env::temp_dir();
    reserve_unique_temp_entry(
        |pid, nanos, seq| {
            let mut dir_name = String::with_capacity("fcupdater__".len().saturating_add(40));
            dir_name.push_str("fcupdater_");
            push_display(&mut dir_name, pid);
            dir_name.push('_');
            push_display(&mut dir_name, nanos);
            dir_name.push('_');
            push_display(&mut dir_name, seq);
            base.join(dir_name)
        },
        |path| fs::create_dir_all(path),
        "임시 작업 폴더 생성 실패",
        "임시 작업 폴더 생성 시도가 모두 실패했습니다. 잠시 후 다시 시도하세요.".into(),
    )
}
fn create_dir_all_checked(path: &Path, failure_label: &str) -> Result<()> {
    fs::create_dir_all(path)
        .map_err(|source_err| err(path_source_message(failure_label, path, source_err)))
}
fn reserve_unique_temp_entry<FBuild, FCreate>(
    build_path: FBuild,
    mut create_entry: FCreate,
    create_failure_label: &str,
    exhausted_message: String,
) -> Result<PathBuf>
where
    FBuild: Fn(u32, u128, u32) -> PathBuf,
    FCreate: FnMut(&Path) -> io::Result<()>,
{
    let pid = process::id();
    for seq in 0..1024_u32 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = build_path(pid, nanos, seq);
        match create_entry(&path) {
            Ok(()) => return Ok(path),
            Err(io_err) if io_err.kind() == ErrorKind::AlreadyExists => {
                thread::sleep(Duration::from_micros(50));
            }
            Err(io_err) => {
                return Err(err(path_source_message(
                    create_failure_label,
                    &path,
                    io_err,
                )));
            }
        }
    }
    Err(err(exhausted_message))
}
fn extract_archive(archive_path: &Path, unpack_dir: &Path) -> Result<()> {
    zip_archive::ZipArchiveOps.extract_to_directory(archive_path, unpack_dir)
}
#[cfg(windows)]
fn encode_path_wide(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(once(0)).collect()
}
fn relative_path_policy_message(prefix: &str, relative_path: &str) -> String {
    let mut out = String::with_capacity(prefix.len().saturating_add(relative_path.len()));
    out.push_str(prefix);
    out.push_str(relative_path);
    out
}
#[cfg(windows)]
fn windows_file_op_error_message(
    prefix: &str,
    from: &Path,
    to: Option<&Path>,
    arrow: &str,
    code: u32,
) -> String {
    let mut out = String::with_capacity(prefix.len().saturating_add(96));
    out.push_str(prefix);
    push_display(&mut out, from.display());
    if let Some(target) = to {
        out.push_str(arrow);
        push_display(&mut out, target.display());
    }
    out.push_str(" (GetLastError=");
    push_display(&mut out, code);
    out.push(')');
    out
}
#[cfg(not(windows))]
fn durability_strict_mode() -> bool {
    env::var("FCUPDATER_DURABILITY_STRICT")
        .ok()
        .is_some_and(|value| {
            let trimmed = value.trim();
            trimmed == "1"
                || trimmed.eq_ignore_ascii_case("true")
                || trimmed.eq_ignore_ascii_case("yes")
                || trimmed.eq_ignore_ascii_case("on")
        })
}
