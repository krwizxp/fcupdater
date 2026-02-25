use super::path_util::path_from_slashes;
use crate::{Result, err};
use std::{
    ffi::OsString,
    fs,
    io::ErrorKind,
    path::{Component, Path, PathBuf},
    process::{self, Stdio},
    sync::OnceLock,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
static EXTRACT_TOOLS_READY: OnceLock<()> = OnceLock::new();
static CREATE_TOOLS_READY: OnceLock<()> = OnceLock::new();
#[derive(Debug)]
pub struct XlsxContainer {
    work_dir: PathBuf,
    unpack_dir: PathBuf,
    archive_path: PathBuf,
}
#[derive(Debug)]
struct WorkDirCleanup {
    path: PathBuf,
    keep: bool,
}
impl WorkDirCleanup {
    const fn new(path: PathBuf) -> Self {
        Self { path, keep: false }
    }
    fn path(&self) -> &Path {
        &self.path
    }
    fn into_path(mut self) -> PathBuf {
        self.keep = true;
        std::mem::take(&mut self.path)
    }
}
impl Drop for WorkDirCleanup {
    fn drop(&mut self) {
        if !self.keep && !self.path.as_os_str().is_empty() {
            let _ = fs::remove_dir_all(&self.path);
        }
    }
}
impl XlsxContainer {
    pub fn open_for_update(source_xlsx: &Path) -> Result<Self> {
        if !source_xlsx.exists() {
            return Err(err(format!(
                "xlsx 파일이 없습니다: {}",
                source_xlsx.display()
            )));
        }
        ensure_extract_tools_available()?;
        let cleanup = WorkDirCleanup::new(create_unique_work_dir()?);
        let unpack_dir = cleanup.path().join("unzipped");
        let archive_path = cleanup.path().join("workbook.zip");
        fs::create_dir_all(&unpack_dir).map_err(|e| {
            err(format!(
                "임시 폴더 생성 실패: {} ({e})",
                unpack_dir.display()
            ))
        })?;
        fs::copy(source_xlsx, &archive_path).map_err(|e| {
            err(format!(
                "xlsx 임시 복사 실패: {} -> {} ({e})",
                source_xlsx.display(),
                archive_path.display()
            ))
        })?;
        extract_archive(&archive_path, &unpack_dir)?;
        let work_dir = cleanup.into_path();
        Ok(Self {
            work_dir,
            unpack_dir,
            archive_path,
        })
    }
    pub fn read_text(&self, relative_path: &str) -> Result<String> {
        let path = self.resolve_relative_path(relative_path)?;
        fs::read_to_string(&path)
            .map_err(|e| err(format!("파일 읽기 실패: {} ({e})", path.display())))
    }
    pub fn write_text(&self, relative_path: &str, content: &str) -> Result<()> {
        let path = self.resolve_relative_path(relative_path)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| err(format!("폴더 생성 실패: {} ({e})", parent.display())))?;
        }
        fs::write(&path, content)
            .map_err(|e| err(format!("파일 쓰기 실패: {} ({e})", path.display())))
    }
    pub fn save_as(&self, output_xlsx: &Path, verify_saved_file: bool) -> Result<()> {
        ensure_create_tools_available()?;
        create_archive(&self.unpack_dir, &self.archive_path)?;
        if let Some(parent) = output_xlsx.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| err(format!("출력 폴더 생성 실패: {} ({e})", parent.display())))?;
        }
        let tmp_output = create_unique_temp_output_path(output_xlsx)?;
        let result = (|| -> Result<()> {
            fs::copy(&self.archive_path, &tmp_output).map_err(|e| {
                err(format!(
                    "xlsx 임시 저장 실패: {} -> {} ({e})",
                    self.archive_path.display(),
                    tmp_output.display()
                ))
            })?;
            if verify_saved_file {
                verify_saved_xlsx(&tmp_output)?;
            }
            promote_temp_output(&tmp_output, output_xlsx)?;
            Ok(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&tmp_output);
        }
        result
    }
    pub fn unpack_dir(&self) -> &Path {
        &self.unpack_dir
    }
    fn resolve_relative_path(&self, relative_path: &str) -> Result<PathBuf> {
        let mut path = PathBuf::new();
        for component in Path::new(relative_path).components() {
            match component {
                Component::CurDir => {}
                Component::Normal(segment) => path.push(segment),
                Component::ParentDir => {
                    return Err(err(format!(
                        "상위 경로 탐색은 허용되지 않습니다: {relative_path}"
                    )));
                }
                Component::RootDir | Component::Prefix(_) => {
                    return Err(err(format!(
                        "절대 경로는 허용되지 않습니다: {relative_path}"
                    )));
                }
            }
        }
        if path.as_os_str().is_empty() {
            return Err(err(format!("상대 경로가 비어 있습니다: {relative_path}")));
        }
        Ok(self.unpack_dir.join(path))
    }
}
impl Drop for XlsxContainer {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.work_dir);
    }
}
fn create_unique_work_dir() -> Result<PathBuf> {
    let base = std::env::temp_dir();
    let pid = process::id();
    for seq in 0..1024u32 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let path = base.join(format!("fcupdater_{pid}_{nanos}_{seq}"));
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                thread::sleep(Duration::from_micros(50));
            }
            Err(e) => {
                return Err(err(format!(
                    "임시 작업 폴더 생성 실패: {} ({e})",
                    path.display()
                )));
            }
        }
    }
    Err(err(
        "임시 작업 폴더 생성 시도가 모두 실패했습니다. 잠시 후 다시 시도하세요.",
    ))
}
fn create_unique_temp_output_path(output_xlsx: &Path) -> Result<PathBuf> {
    let parent = output_xlsx.parent().unwrap_or_else(|| Path::new("."));
    let file_name = output_xlsx
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or("output.xlsx");
    let pid = process::id();
    for seq in 0..1024u32 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let candidate = parent.join(format!(".{file_name}.tmp_{pid}_{nanos}_{seq}"));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(_) => return Ok(candidate),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                thread::sleep(Duration::from_micros(50));
            }
            Err(e) => {
                return Err(err(format!(
                    "임시 출력 파일 생성 실패: {} ({e})",
                    candidate.display()
                )));
            }
        }
    }
    Err(err(format!(
        "임시 출력 파일 경로 생성 실패: {}",
        output_xlsx.display()
    )))
}
fn promote_temp_output(temp_output: &Path, output_xlsx: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        super::windows_api::replace_file_atomic(temp_output, output_xlsx)
    }
    #[cfg(not(windows))]
    {
        fs::rename(temp_output, output_xlsx).map_err(|e| {
            err(format!(
                "xlsx 저장 실패: {} -> {} ({e})",
                temp_output.display(),
                output_xlsx.display()
            ))
        })?;
        if let Err(e) = fs::OpenOptions::new()
            .read(true)
            .open(output_xlsx)
            .and_then(|file| file.sync_all())
        {
            if durability_strict_mode() {
                return Err(err(format!(
                    "xlsx 저장 내구성 동기화 실패(파일): {} ({e})",
                    output_xlsx.display()
                )));
            }
            eprintln!(
                "[경고] 저장 내구성 동기화 실패(파일): {} ({e})",
                output_xlsx.display()
            );
        }
        if let Some(parent) = output_xlsx.parent()
            && let Err(e) = fs::File::open(parent).and_then(|dir| dir.sync_all())
        {
            if durability_strict_mode() {
                return Err(err(format!(
                    "xlsx 저장 내구성 동기화 실패(폴더): {} ({e})",
                    parent.display()
                )));
            }
            eprintln!(
                "[경고] 저장 내구성 동기화 실패(폴더): {} ({e})",
                parent.display()
            );
        }
        Ok(())
    }
}
fn ensure_extract_tools_available() -> Result<()> {
    if EXTRACT_TOOLS_READY.get().is_some() {
        return Ok(());
    }
    #[cfg(windows)]
    {
        let has_powershell = detect_powershell_program().is_some();
        let has_tar = command_exists("tar", &["--version"], None);
        if has_powershell || has_tar {
            let _ = EXTRACT_TOOLS_READY.set(());
            return Ok(());
        }
        Err(err(
            "xlsx 압축 해제를 위한 도구가 없습니다. (PowerShell 또는 tar 필요)",
        ))
    }
    #[cfg(not(windows))]
    {
        let has_unzip = command_exists("unzip", &["-v"], None);
        let has_python3_zipfile =
            command_exists("python3", &["-c", "import zipfile,sys;sys.exit(0)"], None);
        let has_python_zipfile =
            command_exists("python", &["-c", "import zipfile,sys;sys.exit(0)"], None);
        if has_unzip || has_python3_zipfile || has_python_zipfile {
            let _ = EXTRACT_TOOLS_READY.set(());
            return Ok(());
        }
        Err(err(
            "xlsx 압축 해제를 위한 도구가 없습니다. (unzip 또는 python3/python 필요)",
        ))
    }
}
fn ensure_create_tools_available() -> Result<()> {
    if CREATE_TOOLS_READY.get().is_some() {
        return Ok(());
    }
    #[cfg(windows)]
    {
        let has_powershell = detect_powershell_program().is_some();
        let has_tar = command_exists("tar", &["--version"], None);
        if has_powershell || has_tar {
            let _ = CREATE_TOOLS_READY.set(());
            return Ok(());
        }
        Err(err(
            "xlsx 압축 생성을 위한 도구가 없습니다. (PowerShell 또는 tar 필요)",
        ))
    }
    #[cfg(not(windows))]
    {
        let has_zip = command_exists("zip", &["-v"], None);
        let has_python3_zipfile =
            command_exists("python3", &["-c", "import zipfile,sys;sys.exit(0)"], None);
        let has_python_zipfile =
            command_exists("python", &["-c", "import zipfile,sys;sys.exit(0)"], None);
        if has_zip || has_python3_zipfile || has_python_zipfile {
            let _ = CREATE_TOOLS_READY.set(());
            return Ok(());
        }
        Err(err(
            "xlsx 압축 생성을 위한 도구가 없습니다. (zip 또는 python3/python 필요)",
        ))
    }
}
fn verify_saved_xlsx(output_xlsx: &Path) -> Result<()> {
    let verify_work = create_unique_work_dir()?;
    let verify_unpacked = verify_work.join("verify_unpacked");
    fs::create_dir_all(&verify_unpacked).map_err(|e| {
        err(format!(
            "저장 검증용 임시 폴더 생성 실패: {} ({e})",
            verify_unpacked.display()
        ))
    })?;
    let result = (|| -> Result<()> {
        extract_archive(output_xlsx, &verify_unpacked)?;
        for rel in [
            "[Content_Types].xml",
            "xl/workbook.xml",
            "xl/_rels/workbook.xml.rels",
        ] {
            let path = verify_unpacked.join(path_from_slashes(rel));
            if !path.is_file() {
                return Err(err(format!(
                    "저장 검증 실패: 필수 OOXML 파트가 없습니다: {}",
                    path.display()
                )));
            }
        }
        verify_saved_workbook_reopen(output_xlsx)?;
        Ok(())
    })();
    let _ = fs::remove_dir_all(&verify_work);
    result
}
fn verify_saved_workbook_reopen(output_xlsx: &Path) -> Result<()> {
    super::writer::Workbook::open(output_xlsx).map_err(|e| {
        err(format!(
            "저장 검증 실패: 저장 직후 재열기 점검에 실패했습니다: {} ({e})",
            output_xlsx.display()
        ))
    })?;
    Ok(())
}
fn extract_archive(archive_path: &Path, unpack_dir: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        let mut attempts = Vec::new();
        let script = format!(
            "Expand-Archive -LiteralPath '{}' -DestinationPath '{}' -Force",
            ps_quote(archive_path),
            ps_quote(unpack_dir),
        );
        if let Some(shell_program) = detect_powershell_program() {
            match run_powershell(shell_program, &script) {
                Ok(()) => return Ok(()),
                Err(e) => attempts.push(format!("{shell_program}: {e}")),
            }
        }
        match run_command(
            "tar",
            &[
                OsString::from("-xf"),
                archive_path.as_os_str().to_os_string(),
                OsString::from("-C"),
                unpack_dir.as_os_str().to_os_string(),
            ],
            None,
        ) {
            Ok(()) => return Ok(()),
            Err(e) => attempts.push(format!("tar: {e}")),
        }
        Err(err(format!(
            "xlsx 압축 해제 실패: {} -> {} ({})",
            archive_path.display(),
            unpack_dir.display(),
            attempts.join(" / ")
        )))
    }
    #[cfg(not(windows))]
    {
        let mut attempts = Vec::new();
        match run_command(
            "unzip",
            &[
                OsString::from("-o"),
                archive_path.as_os_str().to_os_string(),
                OsString::from("-d"),
                unpack_dir.as_os_str().to_os_string(),
            ],
            None,
        ) {
            Ok(()) => return Ok(()),
            Err(e) => attempts.push(format!("unzip: {e}")),
        }
        match run_command(
            "python3",
            &[
                OsString::from("-m"),
                OsString::from("zipfile"),
                OsString::from("-e"),
                archive_path.as_os_str().to_os_string(),
                unpack_dir.as_os_str().to_os_string(),
            ],
            None,
        ) {
            Ok(()) => return Ok(()),
            Err(e) => attempts.push(format!("python3 -m zipfile: {e}")),
        }
        match run_command(
            "python",
            &[
                OsString::from("-m"),
                OsString::from("zipfile"),
                OsString::from("-e"),
                archive_path.as_os_str().to_os_string(),
                unpack_dir.as_os_str().to_os_string(),
            ],
            None,
        ) {
            Ok(()) => return Ok(()),
            Err(e) => attempts.push(format!("python -m zipfile: {e}")),
        }
        Err(err(format!(
            "xlsx 압축 해제 실패: {} -> {} ({})",
            archive_path.display(),
            unpack_dir.display(),
            attempts.join(" / ")
        )))
    }
}
fn create_archive(unpack_dir: &Path, archive_path: &Path) -> Result<()> {
    if archive_path.exists() {
        fs::remove_file(archive_path).map_err(|e| {
            err(format!(
                "기존 archive 삭제 실패: {} ({e})",
                archive_path.display()
            ))
        })?;
    }
    create_archive_impl(unpack_dir, archive_path)
}
#[cfg(windows)]
fn create_archive_impl(unpack_dir: &Path, archive_path: &Path) -> Result<()> {
    let mut attempts = Vec::new();
    let script = format!(
        "Compress-Archive -Path (Join-Path '{}' '*') -DestinationPath '{}' -Force",
        ps_quote(unpack_dir),
        ps_quote(archive_path),
    );
    if let Some(shell_program) = detect_powershell_program() {
        match run_powershell(shell_program, &script) {
            Ok(()) => return Ok(()),
            Err(e) => attempts.push(format!("{shell_program}: {e}")),
        }
    }
    match run_command(
        "tar",
        &[
            OsString::from("-a"),
            OsString::from("-c"),
            OsString::from("-f"),
            archive_path.as_os_str().to_os_string(),
            OsString::from("-C"),
            unpack_dir.as_os_str().to_os_string(),
            OsString::from("."),
        ],
        None,
    ) {
        Ok(()) => return Ok(()),
        Err(e) => attempts.push(format!("tar: {e}")),
    }
    Err(err(format!(
        "xlsx 압축 생성 실패: {} -> {} ({})",
        unpack_dir.display(),
        archive_path.display(),
        attempts.join(" / ")
    )))
}
#[cfg(not(windows))]
fn create_archive_impl(unpack_dir: &Path, archive_path: &Path) -> Result<()> {
    let mut attempts = Vec::new();
    match run_command(
        "zip",
        &[
            OsString::from("-qr"),
            archive_path.as_os_str().to_os_string(),
            OsString::from("."),
        ],
        Some(unpack_dir),
    ) {
        Ok(()) => return Ok(()),
        Err(e) => attempts.push(format!("zip: {e}")),
    }
    let py_script = r#"import os
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
    match run_command(
        "python3",
        &[
            OsString::from("-c"),
            OsString::from(py_script),
            unpack_dir.as_os_str().to_os_string(),
            archive_path.as_os_str().to_os_string(),
        ],
        None,
    ) {
        Ok(()) => return Ok(()),
        Err(e) => attempts.push(format!("python3 -c zipfile: {e}")),
    }
    match run_command(
        "python",
        &[
            OsString::from("-c"),
            OsString::from(py_script),
            unpack_dir.as_os_str().to_os_string(),
            archive_path.as_os_str().to_os_string(),
        ],
        None,
    ) {
        Ok(()) => return Ok(()),
        Err(e) => attempts.push(format!("python -c zipfile: {e}")),
    }
    Err(err(format!(
        "xlsx 압축 생성 실패: {} -> {} ({})",
        unpack_dir.display(),
        archive_path.display(),
        attempts.join(" / ")
    )))
}
fn format_process_failure(program: &str, output: &process::Output) -> String {
    let code = output
        .status
        .code()
        .map_or_else(|| "None".to_string(), |v| v.to_string());
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    let detail = if stderr.is_empty() { stdout } else { stderr };
    if detail.is_empty() {
        format!("{program} 비정상 종료(code={code})")
    } else {
        format!("{program} 비정상 종료(code={code}): {detail}")
    }
}
fn command_timeout() -> Option<Duration> {
    std::env::var("FCUPDATER_COMMAND_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
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
            .map_err(|e| err(format!("{program} 실행 실패: {e}")));
    };
    let start = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(_)) => {
                return child
                    .wait_with_output()
                    .map_err(|e| err(format!("{program} 실행 결과 수집 실패: {e}")));
            }
            Ok(None) => {
                if start.elapsed() >= limit {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(err(format!(
                        "{program} 실행 제한시간 초과: {}초",
                        limit.as_secs()
                    )));
                }
                thread::sleep(Duration::from_millis(50));
            }
            Err(e) => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(err(format!("{program} 상태 확인 실패: {e}")));
            }
        }
    }
}
fn run_command(program: &str, args: &[OsString], current_dir: Option<&Path>) -> Result<()> {
    let mut cmd = process::Command::new(program);
    cmd.args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(dir) = current_dir {
        cmd.current_dir(dir);
    }
    let child = cmd
        .spawn()
        .map_err(|e| err(format!("{program} 실행 실패: {e}")))?;
    let output = wait_with_optional_timeout(child, program, command_timeout())?;
    if output.status.success() {
        return Ok(());
    }
    Err(err(format_process_failure(program, &output)))
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
    let child = process::Command::new(program)
        .args(["-NoProfile", "-Command", script])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| err(format!("{program} 실행 실패: {e}")))?;
    let output = wait_with_optional_timeout(child, program, command_timeout())?;
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
    std::env::var("FCUPDATER_DURABILITY_STRICT")
        .ok()
        .is_some_and(|v| {
            matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
}
