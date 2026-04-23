use super::path_util::path_from_slashes;
use crate::{
    Result, err, path_pair_source_message, path_source_message, prefixed_message,
    program_source_message,
};
#[cfg(windows)]
use core::iter::once;
#[cfg(windows)]
use core::ptr::{null, null_mut};
use core::{
    fmt::{Display, Write as _},
    mem,
    time::Duration,
};
#[cfg(not(windows))]
use std::io::{Write as _, stderr};
#[cfg(windows)]
use std::os::windows::ffi::OsStrExt as _;
use std::{
    env,
    ffi::OsString,
    fs,
    io::{self, ErrorKind},
    path::{Component, Path, PathBuf},
    process::{self, Stdio},
    sync::OnceLock,
    thread,
    time::{Instant, SystemTime, UNIX_EPOCH},
};
static EXTRACT_TOOLS_READY: OnceLock<()> = OnceLock::new();
static CREATE_TOOLS_READY: OnceLock<()> = OnceLock::new();
#[cfg(not(windows))]
const PYTHON_CREATE_ZIP_SCRIPT: &str = r#"import os
import sys
import zipfile
root, out = sys.argv[1], sys.argv[2]
with zipfile.ZipFile(out, "w", compression=zipfile.ZIP_DEFLATED) as zf:
    for dp, _, files in os.walk(root):
        for name in files:
            p = os.path.join(dp, name)
            arc = os.path.relpath(p, root).replace(os.sep, "/")
            zf.write(p, arc)
"#;
#[cfg(windows)]
const EXTRACT_TOOLS_MISSING_MESSAGE: &str =
    "xlsx 압축 해제를 위한 도구가 없습니다. (PowerShell 또는 tar 필요)";
#[cfg(not(windows))]
const EXTRACT_TOOLS_MISSING_MESSAGE: &str =
    "xlsx 압축 해제를 위한 도구가 없습니다. (unzip 또는 python3/python 필요)";
#[cfg(windows)]
const CREATE_TOOLS_MISSING_MESSAGE: &str =
    "xlsx 압축 생성을 위한 도구가 없습니다. (PowerShell 또는 tar 필요)";
#[cfg(not(windows))]
const CREATE_TOOLS_MISSING_MESSAGE: &str =
    "xlsx 압축 생성을 위한 도구가 없습니다. (zip 또는 python3/python 필요)";
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
        cfg_select! {
            windows => {
                let mut attempts = Vec::with_capacity(2);
                let script = build_powershell_archive_script(
                    "Compress-Archive -Path (Join-Path '",
                    unpack_dir,
                    "' '*') -DestinationPath '",
                    archive_path,
                    "' -Force",
                );
                if let Some(shell_program) = detect_powershell_program() {
                    match run_powershell(shell_program, &script) {
                        Ok(()) => return Ok(()),
                        Err(command_err) => push_attempt(&mut attempts, shell_program, command_err),
                    }
                }
                match run_command(
                    "tar",
                    &[
                        "-a".into(),
                        "-c".into(),
                        "-f".into(),
                        archive_path.as_os_str().to_os_string(),
                        "-C".into(),
                        unpack_dir.as_os_str().to_os_string(),
                        ".".into(),
                    ],
                    None,
                ) {
                    Ok(()) => return Ok(()),
                    Err(command_err) => attempts.push(attempt_source_message("tar", command_err)),
                }
                Err(err(archive_attempts_message(
                    "xlsx 압축 생성 실패",
                    unpack_dir,
                    archive_path,
                    &attempts,
                )))
            }
            _ => {
                let mut attempts = Vec::with_capacity(3);
                match run_command(
                    "zip",
                    &[
                        "-qr".into(),
                        archive_path.as_os_str().to_os_string(),
                        ".".into(),
                    ],
                    Some(unpack_dir),
                ) {
                    Ok(()) => return Ok(()),
                    Err(source_err) => attempts.push(attempt_source_message("zip", source_err)),
                }
                match run_command(
                    "python3",
                    &[
                        "-c".into(),
                        PYTHON_CREATE_ZIP_SCRIPT.into(),
                        unpack_dir.as_os_str().to_os_string(),
                        archive_path.as_os_str().to_os_string(),
                    ],
                    None,
                ) {
                    Ok(()) => return Ok(()),
                    Err(source_err) => {
                        attempts.push(attempt_source_message("python3 -c zipfile", source_err))
                    }
                }
                match run_command(
                    "python",
                    &[
                        "-c".into(),
                        PYTHON_CREATE_ZIP_SCRIPT.into(),
                        unpack_dir.as_os_str().to_os_string(),
                        archive_path.as_os_str().to_os_string(),
                    ],
                    None,
                ) {
                    Ok(()) => return Ok(()),
                    Err(source_err) => {
                        attempts.push(attempt_source_message("python -c zipfile", source_err))
                    }
                }
                Err(err(archive_attempts_message(
                    "xlsx 압축 생성 실패",
                    unpack_dir,
                    archive_path,
                    &attempts,
                )))
            }
        }
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
        cfg_select! {
            windows => {
                let mut attempts = Vec::with_capacity(2);
                if let Some(shell_program) = detect_powershell_program() {
                    let archive_quoted = ps_quote(archive_path);
                    let prefix = "Add-Type -AssemblyName System.IO.Compression.FileSystem; [IO.Compression.ZipFile]::OpenRead('";
                    let suffix = "').Entries | ForEach-Object {$_.FullName}";
                    let mut script = String::with_capacity(
                        prefix
                            .len()
                            .saturating_add(archive_quoted.len())
                            .saturating_add(suffix.len()),
                    );
                    script.push_str(prefix);
                    script.push_str(&archive_quoted);
                    script.push_str(suffix);
                    match Self::run_archive_listing_capture(
                        shell_program,
                        &["-NoProfile".into(), "-Command".into(), script.into()],
                        None,
                    ) {
                        Ok(entries) => return Ok(entries),
                        Err(shell_err) => push_attempt(&mut attempts, shell_program, shell_err),
                    }
                }
                match Self::run_archive_listing_capture(
                    "tar",
                    &["-tf".into(), archive_path.as_os_str().to_os_string()],
                    None,
                ) {
                    Ok(entries) => Ok(entries),
                    Err(tar_err) => {
                        attempts.push(attempt_source_message("tar", tar_err));
                        Err(err(archive_attempts_message(
                            "xlsx 압축 엔트리 목록 확인 실패",
                            archive_path,
                            archive_path,
                            &attempts,
                        )))
                    }
                }
            }
            _ => {
                let mut attempts = Vec::with_capacity(3);
                match Self::run_archive_listing_capture(
                    "unzip",
                    &[
                        "-Z".into(),
                        "-1".into(),
                        archive_path.as_os_str().to_os_string(),
                    ],
                    None,
                ) {
                    Ok(entries) => return Ok(entries),
                    Err(unzip_err) => attempts.push(attempt_source_message("unzip -Z -1", unzip_err)),
                }
                let py_code = "import sys, zipfile\nwith zipfile.ZipFile(sys.argv[1]) as zf:\n    for name in zf.namelist():\n        print(name)";
                for (program, label) in [
                    ("python3", "python3 -c zipfile"),
                    ("python", "python -c zipfile"),
                ] {
                    match Self::run_archive_listing_capture(
                        program,
                        &[
                            "-c".into(),
                            py_code.into(),
                            archive_path.as_os_str().to_os_string(),
                        ],
                        None,
                    ) {
                        Ok(entries) => return Ok(entries),
                        Err(python_err) => attempts.push(attempt_source_message(label, python_err)),
                    }
                }
                Err(err(archive_attempts_message(
                    "xlsx 압축 엔트리 목록 확인 실패",
                    archive_path,
                    archive_path,
                    &attempts,
                )))
            }
        }
    }
    fn run_archive_listing_capture(
        program: &str,
        args: &[OsString],
        current_dir: Option<&Path>,
    ) -> Result<Vec<String>> {
        run_command_capture(program, args, current_dir).map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .lines()
                .map(str::trim)
                .filter(|line| !line.is_empty())
                .map(str::to_owned)
                .collect()
        })
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
        let extract_tools_ready = cfg_select! {
            windows => {
                detect_powershell_program().is_some() || command_exists("tar", &["--version"], None)
            }
            _ => {
                command_exists("unzip", &["-v"], None)
                    || command_exists("python3", &["-c", "import zipfile,sys;sys.exit(0)"], None)
                    || command_exists("python", &["-c", "import zipfile,sys;sys.exit(0)"], None)
            }
        };
        ensure_tools_ready(
            &EXTRACT_TOOLS_READY,
            extract_tools_ready,
            EXTRACT_TOOLS_MISSING_MESSAGE,
        )?;
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
        let create_tools_ready = cfg_select! {
            windows => {
                detect_powershell_program().is_some() || command_exists("tar", &["--version"], None)
            }
            _ => {
                command_exists("zip", &["-v"], None)
                    || command_exists("python3", &["-c", "import zipfile,sys;sys.exit(0)"], None)
                    || command_exists("python", &["-c", "import zipfile,sys;sys.exit(0)"], None)
            }
        };
        ensure_tools_ready(
            &CREATE_TOOLS_READY,
            create_tools_ready,
            CREATE_TOOLS_MISSING_MESSAGE,
        )?;
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
impl Drop for XlsxContainer {
    fn drop(&mut self) {
        match fs::remove_dir_all(&self.work_dir) {
            Ok(()) | Err(_) => {}
        }
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
fn ensure_tools_ready(
    lock: &OnceLock<()>,
    tools_ready: bool,
    missing_message: &'static str,
) -> Result<()> {
    if lock.get().is_none() {
        if !tools_ready {
            return Err(err(missing_message));
        }
        match lock.set(()) {
            Ok(()) | Err(()) => {}
        }
    }
    Ok(())
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
    cfg_select! {
        windows => {
            let mut attempts = Vec::with_capacity(2);
            let script = build_powershell_archive_script(
                "Expand-Archive -LiteralPath '",
                archive_path,
                "' -DestinationPath '",
                unpack_dir,
                "' -Force",
            );
            if let Some(shell_program) = detect_powershell_program() {
                match run_powershell(shell_program, &script) {
                    Ok(()) => return Ok(()),
                    Err(command_err) => push_attempt(&mut attempts, shell_program, command_err),
                }
            }
            match run_command(
                "tar",
                &[
                    "-xf".into(),
                    archive_path.as_os_str().to_os_string(),
                    "-C".into(),
                    unpack_dir.as_os_str().to_os_string(),
                ],
                None,
            ) {
                Ok(()) => return Ok(()),
                Err(command_err) => attempts.push(attempt_source_message("tar", command_err)),
            }
            Err(err(archive_attempts_message(
                "xlsx 압축 해제 실패",
                archive_path,
                unpack_dir,
                &attempts,
            )))
        }
        _ => {
            let mut attempts = Vec::with_capacity(3);
            match run_command(
                "unzip",
                &[
                    "-o".into(),
                    archive_path.as_os_str().to_os_string(),
                    "-d".into(),
                    unpack_dir.as_os_str().to_os_string(),
                ],
                None,
            ) {
                Ok(()) => return Ok(()),
                Err(source_err) => attempts.push(attempt_source_message("unzip", source_err)),
            }
            match run_command(
                "python3",
                &[
                    "-m".into(),
                    "zipfile".into(),
                    "-e".into(),
                    archive_path.as_os_str().to_os_string(),
                    unpack_dir.as_os_str().to_os_string(),
                ],
                None,
            ) {
                Ok(()) => return Ok(()),
                Err(source_err) => {
                    attempts.push(attempt_source_message("python3 -m zipfile", source_err))
                }
            }
            match run_command(
                "python",
                &[
                    "-m".into(),
                    "zipfile".into(),
                    "-e".into(),
                    archive_path.as_os_str().to_os_string(),
                    unpack_dir.as_os_str().to_os_string(),
                ],
                None,
            ) {
                Ok(()) => return Ok(()),
                Err(source_err) => {
                    attempts.push(attempt_source_message("python -m zipfile", source_err))
                }
            }
            Err(err(archive_attempts_message(
                "xlsx 압축 해제 실패",
                archive_path,
                unpack_dir,
                &attempts,
            )))
        }
    }
}
fn archive_attempts_message(label: &str, from: &Path, to: &Path, attempts: &[String]) -> String {
    if attempts.is_empty() {
        path_pair_source_message(label, from, to, "원인 정보 없음")
    } else {
        let mut attempts_len = attempts.len().saturating_sub(1).saturating_mul(3);
        for attempt_text in attempts {
            attempts_len = attempts_len.saturating_add(attempt_text.len());
        }
        let mut attempts_text = String::with_capacity(attempts_len);
        for (index, attempt) in attempts.iter().enumerate() {
            if index > 0 {
                attempts_text.push_str(" / ");
            }
            attempts_text.push_str(attempt);
        }
        path_pair_source_message(label, from, to, attempts_text)
    }
}
#[cfg(windows)]
fn encode_path_wide(path: &Path) -> Vec<u16> {
    path.as_os_str().encode_wide().chain(once(0)).collect()
}
#[cfg(windows)]
fn build_powershell_archive_script(
    prefix: &str,
    first_path: &Path,
    middle: &str,
    second_path: &Path,
    suffix: &str,
) -> String {
    let first_quoted = ps_quote(first_path);
    let second_quoted = ps_quote(second_path);
    let mut script = String::with_capacity(
        prefix
            .len()
            .saturating_add(first_quoted.len())
            .saturating_add(middle.len())
            .saturating_add(second_quoted.len())
            .saturating_add(suffix.len()),
    );
    script.push_str(prefix);
    script.push_str(&first_quoted);
    script.push_str(middle);
    script.push_str(&second_quoted);
    script.push_str(suffix);
    script
}
fn push_display(out: &mut String, value: impl Display) {
    match write!(out, "{value}") {
        Ok(()) | Err(_) => {}
    }
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
fn attempt_source_message(program: &str, source: impl Display) -> String {
    let mut out = String::with_capacity(program.len().saturating_add(64));
    out.push_str(program);
    out.push_str(": ");
    push_display(&mut out, source);
    out
}
#[cfg(windows)]
fn push_attempt(attempts: &mut Vec<String>, program: &str, source: impl Display) {
    attempts.push(attempt_source_message(program, source));
}
fn trimmed_lossy_owned(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).trim().to_owned()
}
fn format_process_failure(program: &str, output: &process::Output) -> String {
    let stderr = trimmed_lossy_owned(&output.stderr);
    let stdout = trimmed_lossy_owned(&output.stdout);
    let detail = if stderr.is_empty() { stdout } else { stderr };
    if detail.is_empty() {
        let mut out = String::with_capacity(
            program
                .len()
                .saturating_add(12)
                .saturating_add(" 비정상 종료(code=)".len()),
        );
        out.push_str(program);
        out.push_str(" 비정상 종료(code=");
        match output.status.code() {
            Some(status_code) => push_display(&mut out, status_code),
            None => out.push_str("None"),
        }
        out.push(')');
        out
    } else {
        let mut out = String::with_capacity(
            program
                .len()
                .saturating_add(12)
                .saturating_add(detail.len())
                .saturating_add(" 비정상 종료(code=): ".len()),
        );
        out.push_str(program);
        out.push_str(" 비정상 종료(code=");
        match output.status.code() {
            Some(status_code) => push_display(&mut out, status_code),
            None => out.push_str("None"),
        }
        out.push_str("): ");
        out.push_str(&detail);
        out
    }
}
fn command_timeout() -> Option<Duration> {
    env::var("FCUPDATER_COMMAND_TIMEOUT_SECS")
        .ok()
        .and_then(|timeout_text| timeout_text.trim().parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map(Duration::from_secs)
}
fn wait_with_optional_timeout(
    mut child: process::Child,
    program: &str,
    timeout: Option<Duration>,
) -> Result<process::Output> {
    let Some(limit) = timeout else {
        return child
            .wait_with_output()
            .map_err(|source_err| err(program_source_message(program, "실행 실패", source_err)));
    };
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                return child.wait_with_output().map_err(|source_err| {
                    err(program_source_message(
                        program,
                        "실행 결과 수집 실패",
                        source_err,
                    ))
                });
            }
            Ok(None) => {
                if start.elapsed() >= limit {
                    let cleanup_diagnostic = stop_process_with_diagnostics(&mut child);
                    let mut message = String::with_capacity(program.len().saturating_add(32));
                    message.push_str(program);
                    message.push_str(" 실행 제한시간 초과: ");
                    push_display(&mut message, limit.as_secs());
                    message.push('초');
                    append_cleanup_diagnostic(&mut message, cleanup_diagnostic.as_deref());
                    return Err(err(message));
                }
                thread::sleep(Duration::from_millis(50));
            }
            Err(source_err) => {
                let cleanup_diagnostic = stop_process_with_diagnostics(&mut child);
                let mut message = program_source_message(program, "상태 확인 실패", source_err);
                append_cleanup_diagnostic(&mut message, cleanup_diagnostic.as_deref());
                return Err(err(message));
            }
        }
    }
}
fn append_cleanup_diagnostic(message: &mut String, cleanup_diagnostic: Option<&str>) {
    if let Some(cleanup) = cleanup_diagnostic {
        message.push_str(" (정리 경고: ");
        message.push_str(cleanup);
        message.push(')');
    }
}
fn stop_process_with_diagnostics(child: &mut process::Child) -> Option<String> {
    let mut diagnostic = String::with_capacity(96);
    match child.try_wait() {
        Ok(Some(_)) => return None,
        Ok(None) => {}
        Err(source_err) => {
            diagnostic.push_str("상태 확인 실패: ");
            push_display(&mut diagnostic, source_err);
        }
    }
    match child.kill() {
        Ok(()) => {}
        Err(source_err) if source_err.kind() == ErrorKind::InvalidInput => {}
        Err(source_err) => {
            if !diagnostic.is_empty() {
                diagnostic.push_str(" / ");
            }
            diagnostic.push_str("종료 실패: ");
            push_display(&mut diagnostic, source_err);
        }
    }
    match child.wait() {
        Ok(_) => {}
        Err(source_err) => {
            if !diagnostic.is_empty() {
                diagnostic.push_str(" / ");
            }
            diagnostic.push_str("대기 실패: ");
            push_display(&mut diagnostic, source_err);
        }
    }
    if diagnostic.is_empty() {
        None
    } else {
        Some(diagnostic)
    }
}
fn run_command_capture(
    program: &str,
    args: &[OsString],
    current_dir: Option<&Path>,
) -> Result<process::Output> {
    let mut cmd = process::Command::new(program);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(dir) = current_dir {
        cmd.current_dir(dir);
    }
    let child = (cmd
        .spawn()
        .map_err(|source_err| err(program_source_message(program, "실행 실패", source_err))))?;
    let output = (wait_with_optional_timeout(child, program, command_timeout()))?;
    if output.status.success() {
        return Ok(output);
    }
    Err(err(format_process_failure(program, &output)))
}
fn run_command(program: &str, args: &[OsString], current_dir: Option<&Path>) -> Result<()> {
    run_command_capture(program, args, current_dir).map(|_| ())
}
fn command_exists(program: &str, args: &[&str], current_dir: Option<&Path>) -> bool {
    let mut cmd = process::Command::new(program);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    if let Some(dir) = current_dir {
        cmd.current_dir(dir);
    }
    cmd.status().is_ok_and(|status| status.success())
}
#[cfg(windows)]
fn detect_powershell_program() -> Option<&'static str> {
    if command_exists("pwsh", &["-NoProfile", "-Command", "exit 0"], None) {
        return Some("pwsh");
    }
    if command_exists("powershell", &["-NoProfile", "-Command", "exit 0"], None) {
        return Some("powershell");
    }
    None
}
#[cfg(windows)]
fn run_powershell(program: &str, script: &str) -> Result<()> {
    let child = (process::Command::new(program)
        .args(["-NoProfile", "-Command", script])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|source_err| err(program_source_message(program, "실행 실패", source_err))))?;
    let output = (wait_with_optional_timeout(child, program, command_timeout()))?;
    if output.status.success() {
        return Ok(());
    }
    Err(err(format_process_failure(program, &output)))
}
#[cfg(windows)]
fn ps_quote(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}
#[cfg(not(windows))]
fn durability_strict_mode() -> bool {
    env::var("FCUPDATER_DURABILITY_STRICT")
        .ok()
        .is_some_and(|value| is_truthy_flag(&value))
}
#[cfg(not(windows))]
fn is_truthy_flag(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed == "1"
        || trimmed.eq_ignore_ascii_case("true")
        || trimmed.eq_ignore_ascii_case("yes")
        || trimmed.eq_ignore_ascii_case("on")
}
