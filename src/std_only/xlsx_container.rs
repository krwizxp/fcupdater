use crate::{Result, err};
use std::{
    fs,
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};
#[derive(Debug)]
pub struct XlsxContainer {
    work_dir: PathBuf,
    unpack_dir: PathBuf,
    archive_path: PathBuf,
}
impl XlsxContainer {
    pub fn open_for_update(source_xlsx: &Path) -> Result<Self> {
        if !source_xlsx.exists() {
            return Err(err(format!(
                "xlsx 파일이 없습니다: {}",
                source_xlsx.display()
            )));
        }
        let work_dir = unique_work_dir();
        let unpack_dir = work_dir.join("unzipped");
        let archive_path = work_dir.join("workbook.zip");
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
    pub fn save_as(&self, output_xlsx: &Path) -> Result<()> {
        create_archive(&self.unpack_dir, &self.archive_path)?;
        if let Some(parent) = output_xlsx.parent() {
            fs::create_dir_all(parent)
                .map_err(|e| err(format!("출력 폴더 생성 실패: {} ({e})", parent.display())))?;
        }
        fs::copy(&self.archive_path, output_xlsx).map_err(|e| {
            err(format!(
                "xlsx 저장 실패: {} -> {} ({e})",
                self.archive_path.display(),
                output_xlsx.display()
            ))
        })?;
        Ok(())
    }
    pub fn unpack_dir(&self) -> &Path {
        &self.unpack_dir
    }
    fn resolve_relative_path(&self, relative_path: &str) -> Result<PathBuf> {
        if relative_path.starts_with('/') {
            return Err(err(format!(
                "절대 경로는 허용되지 않습니다: {relative_path}"
            )));
        }
        let mut path = PathBuf::new();
        for segment in relative_path.split('/') {
            if segment.is_empty() || segment == "." {
                continue;
            }
            if segment == ".." {
                return Err(err(format!(
                    "상위 경로 탐색은 허용되지 않습니다: {relative_path}"
                )));
            }
            path.push(segment);
        }
        Ok(self.unpack_dir.join(path))
    }
}
impl Drop for XlsxContainer {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.work_dir);
    }
}
fn unique_work_dir() -> PathBuf {
    let mut path = std::env::temp_dir();
    let pid = process::id();
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    path.push(format!("fcupdater_stdonly_{pid}_{nanos}"));
    path
}
fn extract_archive(archive_path: &Path, unpack_dir: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        let script = format!(
            "Expand-Archive -LiteralPath '{}' -DestinationPath '{}' -Force",
            ps_quote(archive_path),
            ps_quote(unpack_dir),
        );
        run_powershell(&script)?;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        let status = process::Command::new("unzip")
            .args(["-o", archive_path.to_string_lossy().as_ref(), "-d"])
            .arg(unpack_dir)
            .status()
            .map_err(|e| err(format!("unzip 실행 실패: {e}")))?;
        if !status.success() {
            return Err(err(format!(
                "unzip 실패: {} -> {}",
                archive_path.display(),
                unpack_dir.display()
            )));
        }
        Ok(())
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
    #[cfg(windows)]
    {
        let script = format!(
            "Compress-Archive -Path (Join-Path '{}' '*') -DestinationPath '{}' -Force",
            ps_quote(unpack_dir),
            ps_quote(archive_path),
        );
        run_powershell(&script)?;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        let status = process::Command::new("zip")
            .args(["-qr"])
            .arg(archive_path)
            .arg(".")
            .current_dir(unpack_dir)
            .status()
            .map_err(|e| err(format!("zip 실행 실패: {e}")))?;
        if !status.success() {
            return Err(err(format!(
                "zip 생성 실패: {} -> {}",
                unpack_dir.display(),
                archive_path.display()
            )));
        }
        Ok(())
    }
}
#[cfg(windows)]
fn run_powershell(script: &str) -> Result<()> {
    let output = process::Command::new("powershell")
        .args(["-NoProfile", "-Command", script])
        .output()
        .map_err(|e| err(format!("PowerShell 실행 실패: {e}")))?;
    if !output.status.success() {
        return Err(err(format!(
            "PowerShell 실행 실패: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    Ok(())
}
#[cfg(windows)]
fn ps_quote(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}
