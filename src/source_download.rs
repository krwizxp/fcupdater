use alloc::borrow::Cow;
use core::{error::Error, fmt, fmt::Display, mem::replace, result::Result as CoreResult, str};
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
cfg_select! {
    any(target_os = "linux", target_os = "macos", windows) => {
        const HTTP_MAX_HEADER_BYTES: usize = 256 * 1024;
        const HTTP_ERROR_PREVIEW_BYTES: usize = 512;
        const RESPONSE_HEADER_CONTENT_LENGTH: &[u8] = b"Content-Length";
        const RESPONSE_HEADER_SET_COOKIE: &[u8] = b"Set-Cookie";
    }
    _ => {}
}
const OLE2_SIGNATURE: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
cfg_select! {
    any(target_os = "linux", target_os = "macos", windows) => {
        const _: () = assert!(
            OLE2_SIGNATURE.len() <= HTTP_ERROR_PREVIEW_BYTES,
            "streamed OLE2 signature preview must include the full signature"
        );
    }
    _ => {}
}
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
const U128_DECIMAL_MAX_LEN: usize = 39;
type BoxError = Box<dyn Error + Send + Sync>;
#[derive(Debug)]
struct DownloadError {
    message: Cow<'static, str>,
    source: Option<BoxError>,
}
type DownloadResult<T> = CoreResult<T, DownloadError>;
pub(super) struct SourceDownload<'dir, 'out, W: Write + ?Sized> {
    pub dir: &'dir Path,
    pub out: &'out mut W,
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
cfg_select! {
    any(target_os = "linux", target_os = "macos", windows) => {
        struct StreamingBodySink<'writer> {
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            error: Option<DownloadError>,
            limit: usize,
            summary: StreamedBodySummary,
            writer: &'writer mut dyn Write,
        }
    }
    _ => {}
}
impl StreamedBodySummary {
    fn preview_lossy(&self) -> Cow<'_, str> {
        String::from_utf8_lossy(&self.preview)
    }
    fn starts_with(&self, prefix: &[u8]) -> bool {
        self.preview.starts_with(prefix)
    }
}
impl Display for DownloadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.message.as_ref())
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
cfg_select! {
    any(target_os = "linux", target_os = "macos", windows) => {
        impl StreamingBodySink<'_> {
            fn append(&mut self, bytes: &[u8]) -> DownloadResult<()> {
                let Some(next_len) = self.summary.bytes_seen.checked_add(bytes.len()) else {
                    return Err("HTTP 응답 본문 크기 계산 실패".into());
                };
                if next_len > self.limit {
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
                self.summary.bytes_seen = next_len;
                Ok(())
            }
            fn capture_preview(&mut self, bytes: &[u8]) -> DownloadResult<()> {
                let Some(remaining_preview) =
                    HTTP_ERROR_PREVIEW_BYTES.checked_sub(self.summary.preview.len())
                else {
                    return Err("HTTP 응답 본문 preview 상태가 손상되었습니다.".into());
                };
                let take = remaining_preview.min(bytes.len());
                if take == 0 {
                    return Ok(());
                }
                let Some(next_preview_len) = self.summary.preview.len().checked_add(take) else {
                    return Err("HTTP 응답 본문 preview 길이 계산 실패".into());
                };
                if self.summary.preview.capacity() < next_preview_len
                    && let Err(source) = self.summary.preview.try_reserve_exact(take)
                {
                    return Err(download_error_with_source(
                        "HTTP 응답 본문 preview 메모리 확보 실패",
                        source,
                    ));
                }
                let Some(preview) = bytes.get(..take) else {
                    return Err("HTTP 응답 본문 preview 범위 계산 실패".into());
                };
                self.summary.preview.extend_from_slice(preview);
                Ok(())
            }
        }
    }
    _ => {}
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
fn push_decimal_fragment(
    out: &mut String,
    mut value: u128,
    context: &'static str,
) -> DownloadResult<()> {
    let mut buffer = [0_u8; U128_DECIMAL_MAX_LEN];
    let mut index = buffer.len();
    loop {
        let digit = u8::try_from(value.rem_euclid(10))
            .map_err(|source| download_error_with_source(context, source))?;
        index = index.checked_sub(1).ok_or(context)?;
        let byte = b'0'.checked_add(digit).ok_or(context)?;
        let Some(slot) = buffer.get_mut(index) else {
            return Err(context.into());
        };
        *slot = byte;
        value = value.div_euclid(10);
        if value == 0 {
            break;
        }
    }
    let Some(bytes) = buffer.get(index..) else {
        return Err(context.into());
    };
    let fragment =
        str::from_utf8(bytes).map_err(|source| download_error_with_source(context, source))?;
    out.push_str(fragment);
    Ok(())
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
        let mut over_limit = false;
        for byte in value.bytes() {
            if !byte.is_ascii_digit() {
                return Err("HTTP Content-Length가 음이 아닌 10진수 형식이 아닙니다.".into());
            }
            if over_limit {
                continue;
            }
            let digit_raw = byte.wrapping_sub(b'0');
            parsed = parsed
                .checked_mul(10)
                .and_then(|scaled| scaled.checked_add(usize::from(digit_raw)))
                .ok_or("HTTP Content-Length 해석 실패")?;
            if parsed > limit {
                over_limit = true;
            }
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
fn attach_remove_file_error(mut error: DownloadError, path: &Path) -> DownloadError {
    match fs::remove_file(path) {
        Ok(()) => error,
        Err(remove_error) if remove_error.kind() == ErrorKind::NotFound => error,
        Err(remove_error) => {
            let cleanup_text = format!(
                "다운로드 임시 파일 삭제 실패: {} ({remove_error})",
                path.display()
            );
            append_download_error_message(&mut error, &cleanup_text);
            error
        }
    }
}
fn append_download_error_message(error: &mut DownloadError, additional: &str) {
    const SEPARATOR: &str = "; ";
    let original = replace(&mut error.message, Cow::Borrowed(""));
    match original {
        Cow::Borrowed(existing_message) => {
            if let Some(capacity) = existing_message
                .len()
                .checked_add(SEPARATOR.len())
                .and_then(|value| value.checked_add(additional.len()))
            {
                let mut combined = String::new();
                if combined.try_reserve_exact(capacity).is_ok() {
                    combined.push_str(existing_message);
                    combined.push_str(SEPARATOR);
                    combined.push_str(additional);
                    error.message = Cow::Owned(combined);
                    return;
                }
            }
            error.message = Cow::Borrowed(existing_message);
        }
        Cow::Owned(mut existing_message) => {
            if let Some(extra_len) = SEPARATOR.len().checked_add(additional.len())
                && existing_message.try_reserve_exact(extra_len).is_ok()
            {
                existing_message.push_str(SEPARATOR);
                existing_message.push_str(additional);
            }
            error.message = Cow::Owned(existing_message);
        }
    }
}
