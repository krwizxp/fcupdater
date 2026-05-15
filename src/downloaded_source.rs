use crate::{Result, err, path_source_message, source_download, write_line_ignored};
use core::mem;
use std::{
    fs,
    io::{ErrorKind, Write, stderr},
    path::PathBuf,
};
#[derive(Debug, Default)]
pub struct DownloadedSourceGuard {
    paths: Vec<PathBuf>,
}
impl DownloadedSourceGuard {
    pub fn cleanup(&mut self, out: &mut dyn Write) -> Result<()> {
        self.cleanup_with_message("임시 소스 파일", out)
    }
    fn cleanup_with_message(&mut self, message: &str, out: &mut dyn Write) -> Result<()> {
        let mut removed = 0_usize;
        for path in mem::take(&mut self.paths) {
            if !path
                .file_name()
                .and_then(|text_os| text_os.to_str())
                .is_some_and(|file_name| file_name.contains(source_download::AUTO_SOURCE_MARKER))
            {
                continue;
            }
            match fs::metadata(&path) {
                Ok(metadata) if !metadata.is_file() => continue,
                Ok(_) => {}
                Err(source_err) if source_err.kind() == ErrorKind::NotFound => continue,
                Err(source_err) => {
                    return Err(err(path_source_message(
                        "메타데이터 확인 실패",
                        &path,
                        source_err,
                    )));
                }
            }
            fs::remove_file(&path).map_err(|source_err| {
                err(path_source_message(
                    "자동 소스 파일 삭제 실패",
                    &path,
                    source_err,
                ))
            })?;
            removed = removed.saturating_add(1);
        }
        if removed > 0 {
            write_line_ignored(out, format_args!("{message} {removed}개 정리"));
        }
        Ok(())
    }
    pub fn track(&mut self, paths: Vec<PathBuf>) {
        self.paths = paths;
    }
}
impl Drop for DownloadedSourceGuard {
    fn drop(&mut self) {
        if self.paths.is_empty() {
            return;
        }
        let mut err_out = stderr();
        if let Err(err) = self.cleanup_with_message("종료 중 임시 소스 파일", &mut err_out)
        {
            match writeln!(err_out, "종료 중 임시 소스 파일 정리 실패: {err}") {
                Ok(()) | Err(_) => {}
            }
        }
    }
}
