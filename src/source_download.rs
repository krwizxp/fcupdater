use crate::diagnostic::{AppError, err, err_with_source, path_source_message};
use alloc::{borrow::Cow, string::String, vec::Vec};
use core::{error::Error, fmt, fmt::Display, result::Result as CoreResult};
use std::{
    fs,
    fs::File,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};
cfg_select! {
    any(target_os = "linux", target_os = "macos") => {
        mod libcurl;
    }
    windows => {
        mod winhttp;
    }
    _ => {}
}
mod http_client;
mod workflow;
const HTTP_MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const HTTP_MAX_HEADER_BYTES: usize = 256 * 1024;
const HTTP_ERROR_PREVIEW_BYTES: usize = 512;
const OLE2_SIGNATURE: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
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
type BoxError = Box<dyn Error + Send + Sync>;
#[derive(Debug)]
struct DownloadError {
    message: Cow<'static, str>,
    source: Option<BoxError>,
}
type DownloadResult<T> = CoreResult<T, DownloadError>;
pub struct SourceDownload<'dir, 'out, W: Write + ?Sized> {
    pub dir: &'dir Path,
    pub out: &'out mut W,
}
#[derive(Debug)]
struct StreamedBodySummary {
    bytes_seen: usize,
    preview: Vec<u8>,
}
#[derive(Debug)]
struct HttpHeader {
    name: String,
    value: String,
}
#[derive(Debug)]
struct HttpResponse {
    body: Vec<u8>,
    headers: Vec<HttpHeader>,
    status: u32,
}
#[derive(Debug)]
struct HttpStreamResponse {
    body: StreamedBodySummary,
    headers: Vec<HttpHeader>,
    status: u32,
}
struct ReservedDownloadFile {
    file: File,
    path: PathBuf,
}
struct StreamingBodySink<'writer> {
    error: Option<DownloadError>,
    limit: usize,
    summary: StreamedBodySummary,
    writer: &'writer mut dyn Write,
}
impl StreamedBodySummary {
    fn preview_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.preview)
    }
    fn starts_with(&self, prefix: &[u8]) -> bool {
        self.bytes_seen >= prefix.len() && self.preview.starts_with(prefix)
    }
}
impl Display for DownloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(source) = self.source.as_ref() {
            write!(f, "{}: {source}", self.message)
        } else {
            f.write_str(self.message.as_ref())
        }
    }
}
impl Error for DownloadError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        self.source.as_deref().map(|source| {
            let source_ref: &(dyn Error + 'static) = source;
            source_ref
        })
    }
}
impl DownloadError {
    fn into_app_error(self) -> AppError {
        match self.source {
            Some(source) => err_with_source(self.message, source),
            None => err(self.message),
        }
    }
}
impl From<Cow<'static, str>> for DownloadError {
    fn from(value: Cow<'static, str>) -> Self {
        Self {
            message: value,
            source: None,
        }
    }
}
impl From<String> for DownloadError {
    fn from(value: String) -> Self {
        Self::from(Cow::Owned(value))
    }
}
impl From<&'static str> for DownloadError {
    fn from(value: &'static str) -> Self {
        Self::from(Cow::Borrowed(value))
    }
}
impl StreamingBodySink<'_> {
    fn append(&mut self, bytes: &[u8]) -> bool {
        let Some(next_len) = self.summary.bytes_seen.checked_add(bytes.len()) else {
            self.error = Some("HTTP 응답 본문 크기 계산 실패".into());
            return false;
        };
        if next_len > self.limit {
            self.error = Some(
                format!(
                    "HTTP 응답 본문 크기가 허용 한도({} bytes)를 초과했습니다.",
                    self.limit
                )
                .into(),
            );
            return false;
        }
        if !self.capture_preview(bytes) {
            return false;
        }
        if let Err(source) = self.writer.write_all(bytes) {
            self.error = Some(download_error_with_source(
                "HTTP 응답 본문 파일 쓰기 실패",
                source,
            ));
            return false;
        }
        self.summary.bytes_seen = next_len;
        true
    }
    fn capture_preview(&mut self, bytes: &[u8]) -> bool {
        let Some(remaining_preview) =
            HTTP_ERROR_PREVIEW_BYTES.checked_sub(self.summary.preview.len())
        else {
            self.error = Some("HTTP 응답 본문 preview 상태가 손상되었습니다.".into());
            return false;
        };
        let take = remaining_preview.min(bytes.len());
        if take == 0 {
            return true;
        }
        if let Err(source) = self.summary.preview.try_reserve(take) {
            self.error = Some(download_error_with_source(
                "HTTP 응답 본문 preview 메모리 확보 실패",
                source,
            ));
            return false;
        }
        let Some(preview) = bytes.get(..take) else {
            self.error = Some("HTTP 응답 본문 preview 범위 계산 실패".into());
            return false;
        };
        self.summary.preview.extend_from_slice(preview);
        true
    }
}
fn download_error_with_source<M, E>(context: M, source: E) -> DownloadError
where
    M: Into<Cow<'static, str>>,
    E: Error + Send + Sync + 'static,
{
    DownloadError {
        message: context.into(),
        source: Some(Box::new(source)),
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
fn enforce_http_content_length_limit(headers: &[HttpHeader], limit: usize) -> DownloadResult<()> {
    for value in headers
        .iter()
        .filter(|header| header.name.eq_ignore_ascii_case("Content-Length"))
        .map(|header| header.value.as_str())
    {
        let parsed = value.trim_ascii().parse::<usize>().map_err(|source| {
            download_error_with_source("HTTP Content-Length 해석 실패", source)
        })?;
        if parsed > limit {
            return Err(
                format!("HTTP Content-Length가 허용 한도({limit} bytes)를 초과했습니다.").into(),
            );
        }
    }
    Ok(())
}
fn attach_remove_file_error(mut error: DownloadError, path: &Path) -> DownloadError {
    match fs::remove_file(path) {
        Ok(()) => error,
        Err(remove_error) if remove_error.kind() == ErrorKind::NotFound => error,
        Err(remove_error) => {
            let cleanup_text =
                path_source_message("다운로드 임시 파일 삭제 실패", path, remove_error);
            match error.message {
                Cow::Borrowed(message) => {
                    error.message = Cow::Owned(format!("{message}; {cleanup_text}"));
                }
                Cow::Owned(mut message) => {
                    let Some(extra_len) = "; ".len().checked_add(cleanup_text.len()) else {
                        message = format!("{message}; {cleanup_text}");
                        error.message = Cow::Owned(message);
                        return error;
                    };
                    if message.try_reserve(extra_len).is_err() {
                        message = format!("{message}; {cleanup_text}");
                    } else {
                        message.push_str("; ");
                        message.push_str(&cleanup_text);
                    }
                    error.message = Cow::Owned(message);
                }
            }
            error
        }
    }
}
