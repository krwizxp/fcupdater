use crate::diagnostic::{
    AppError as DownloadError, Result as DownloadResult,
    err_with_source as download_error_with_source,
};
use alloc::borrow::Cow;
use core::fmt::Write as FmtWrite;
use std::{
    fs,
    fs::File,
    io::{self, Write},
    path::{Path, PathBuf},
};
cfg_select! {
    any(target_os = "linux", target_os = "macos") => {
        use self::libcurl::Client as PlatformHttpClient;
        mod libcurl;
    }
    windows => {
        use self::winhttp::Client as PlatformHttpClient;
        mod winhttp;
    }
    _ => {
        compile_error!("fcupdater supports only Windows, Linux, and macOS.");
    }
}
mod http_client;
mod workflow;
const HTTP_MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const HTTP_MAX_HEADER_BYTES: usize = 256 * 1024;
const HTTP_ERROR_PREVIEW_BYTES: usize = 512;
const RESPONSE_HEADER_CONTENT_LENGTH: &[u8] = b"Content-Length";
const RESPONSE_HEADER_SET_COOKIE: &[u8] = b"Set-Cookie";
const OLE2_SIGNATURE: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
const _: () = assert!(
    OLE2_SIGNATURE.len() <= HTTP_ERROR_PREVIEW_BYTES,
    "streamed OLE2 signature preview must include the full signature"
);
const OPINET_HOST: &str = "www.opinet.co.kr";
const NETFUNNEL_HOST: &str = "nfl.opinet.co.kr";
const OPDOWNLOAD_PATH: &str = "/user/opdown/opDownload.do";
const OPDOWNLOAD_URL: &str = "https://www.opinet.co.kr/user/opdown/opDownload.do";
const OPDOWNLOAD_LAYOUT_PATH: &str = "/user/main/main_move_price.do";
const OPDOWNLOAD_EXCEL_PATH: &str = "/user/main/main_download_excel.do";
const OIL_PRICE_DOWNLOAD_TAR_URL: &str = "/user/opdown/oil_price_download";
const NETFUNNEL_SERVICE_ID: &str = "service_1";
const NETFUNNEL_ENTRY_ACTION_ID: &str = "B1";
const NETFUNNEL_DOWNLOAD_ACTION_ID: &str = "B7";
const CURRENT_PRICE_PAGE_DIV: &str = "PAGE_DIV_2";
const GAS_STATION_LPG_CODE: &str = "A";
const GAS_STATION_API_GBN: &str = "A";
const DEFAULT_REGION_LABEL: &str = "선택하세요.";
const USER_AGENT: &str = concat!("fcupdater/", env!("CARGO_PKG_VERSION"));
const NETFUNNEL_POLL_LIMIT: usize = 20;
pub(super) struct SourceDownload<'dir, 'out, W: Write + ?Sized> {
    pub dir: &'dir Path,
    pub out: &'out mut W,
}
pub(super) struct TemporarySourceFile {
    file: File,
    path_cleanup: TempFileCleanup,
}
struct TempFileCleanup {
    path: PathBuf,
    remove_on_drop: bool,
}
#[derive(Debug)]
struct StreamedBodySummary {
    bytes_seen: usize,
    preview: Vec<u8>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum HttpHeaderKind {
    ContentLength,
    SetCookie,
}
#[derive(Debug)]
struct HttpHeader {
    kind: HttpHeaderKind,
    value: String,
}
#[derive(Debug)]
struct HttpResponse<B = Vec<u8>> {
    body: B,
    headers: Vec<HttpHeader>,
    status: u32,
}
type HttpStreamResponse = HttpResponse<StreamedBodySummary>;
struct StreamingBodySink<'writer> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    error: Option<DownloadError>,
    limit: usize,
    summary: StreamedBodySummary,
    writer: &'writer mut dyn Write,
}
impl TemporarySourceFile {
    pub(super) fn path(&self) -> &Path {
        &self.path_cleanup.path
    }
    pub(super) fn reader_parts(&mut self) -> (&mut File, &Path) {
        (&mut self.file, &self.path_cleanup.path)
    }
    pub(super) fn remove(self) -> io::Result<()> {
        let Self {
            file,
            mut path_cleanup,
        } = self;
        drop(file);
        path_cleanup.remove()
    }
    fn remove_after_error(self, mut error: DownloadError) -> DownloadError {
        let path = self.path().display().to_string();
        if let Err(remove_error) = self.remove() {
            let cleanup_text = format!("다운로드 임시 파일 삭제 실패: {path} ({remove_error})");
            error.update_message(|message| format!("{message}; {cleanup_text}"));
        }
        error
    }
}
impl TempFileCleanup {
    fn remove(&mut self) -> io::Result<()> {
        self.remove_on_drop = false;
        match fs::remove_file(&self.path) {
            Ok(()) => Ok(()),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(source),
        }
    }
}
impl Drop for TempFileCleanup {
    fn drop(&mut self) {
        if self.remove_on_drop
            && let Err(source) = self.remove()
        {
            let mut error_output = io::stderr().lock();
            match writeln!(
                error_output,
                "경고: 다운로드 임시 파일 삭제 실패: {} ({source})",
                self.path.display()
            ) {
                Ok(()) | Err(_) => {}
            }
        }
    }
}
impl StreamedBodySummary {
    fn preview_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.preview)
    }
}
impl StreamingBodySink<'_> {
    fn append(&mut self, bytes: &[u8]) -> DownloadResult<()> {
        if bytes.len() > self.limit.saturating_sub(self.summary.bytes_seen) {
            return Err(format!(
                "HTTP 응답 본문 크기가 허용 한도({} bytes)를 초과했습니다.",
                self.limit
            )
            .into());
        }
        self.capture_preview(bytes)?;
        self.writer.write_all(bytes).map_err(|source| {
            download_error_with_source("HTTP 응답 본문 파일 쓰기 실패", source)
        })?;
        self.summary.bytes_seen = self.summary.bytes_seen.saturating_add(bytes.len());
        Ok(())
    }
    fn capture_preview(&mut self, bytes: &[u8]) -> DownloadResult<()> {
        let remaining_preview = HTTP_ERROR_PREVIEW_BYTES.saturating_sub(self.summary.preview.len());
        let take = remaining_preview.min(bytes.len());
        if take == 0 {
            return Ok(());
        }
        self.summary
            .preview
            .try_reserve_exact(take)
            .map_err(|source| {
                download_error_with_source("HTTP 응답 본문 preview 메모리 확보 실패", source)
            })?;
        self.summary
            .preview
            .extend(bytes.iter().copied().take(take));
        Ok(())
    }
}
fn push_decimal_fragment(out: &mut String, value: u128) {
    match FmtWrite::write_fmt(out, format_args!("{value}")) {
        Ok(()) | Err(_) => {}
    }
}
cfg_select! {
    windows => {
        fn checked_http_buffer_len(
            label: &str,
            current_len: usize,
            additional_len: usize,
            limit: usize,
        ) -> DownloadResult<usize> {
            let next_len = current_len
                .checked_add(additional_len)
                .ok_or_else(|| format!("HTTP 응답 {label} 크기 계산 실패"))?;
            if next_len > limit {
                Err(format!(
                    "HTTP 응답 {label} 크기가 허용 한도({limit} bytes)를 초과했습니다."
                )
                .into())
            } else {
                Ok(next_len)
            }
        }
    }
    _ => {}
}
fn enforce_http_body_length(actual: usize, expected: Option<usize>) -> DownloadResult<()> {
    if let Some(expected_len) = expected
        && actual != expected_len
    {
        return Err(format!(
            "HTTP 응답 본문 길이가 Content-Length와 다릅니다: expected={expected_len}, actual={actual}"
        )
        .into());
    }
    Ok(())
}
fn validated_http_content_length(
    headers: &[HttpHeader],
    limit: usize,
) -> DownloadResult<Option<usize>> {
    let mut content_length = None;
    for raw_value in headers
        .iter()
        .filter(|header| header.kind == HttpHeaderKind::ContentLength)
        .map(|header| header.value.as_str())
    {
        let value = raw_value.trim_ascii();
        if value.is_empty() {
            return Err("HTTP Content-Length가 음이 아닌 10진수 형식이 아닙니다.".into());
        }
        let mut parsed = 0_usize;
        for byte in value.bytes() {
            if !byte.is_ascii_digit() {
                return Err("HTTP Content-Length가 음이 아닌 10진수 형식이 아닙니다.".into());
            }
            if parsed > limit {
                continue;
            }
            let digit_raw = byte.wrapping_sub(b'0');
            parsed = parsed
                .checked_mul(10)
                .and_then(|scaled| scaled.checked_add(usize::from(digit_raw)))
                .ok_or("HTTP Content-Length 해석 실패")?;
        }
        if parsed > limit {
            return Err(
                format!("HTTP Content-Length가 허용 한도({limit} bytes)를 초과했습니다.").into(),
            );
        }
        if let Some(previous) = content_length {
            if previous != parsed {
                return Err("HTTP Content-Length 헤더 값이 서로 다릅니다.".into());
            }
        } else {
            content_length = Some(parsed);
        }
    }
    Ok(content_length)
}
