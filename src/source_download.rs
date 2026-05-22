use alloc::{borrow::Cow, string::String, vec::Vec};
use core::result::Result as StdResult;
use std::{io::Write, path::Path};
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
const STREAM_PREFIX_BYTES: usize = 8;
const OPINET_HOST: &str = "www.opinet.co.kr";
const NETFUNNEL_HOST: &str = "nfl.opinet.co.kr";
const OPDOWNLOAD_PATH: &str = "/user/opdown/opDownload.do";
const OPDOWNLOAD_URL: &str = "https://www.opinet.co.kr/user/opdown/opDownload.do";
const OPDOWNLOAD_LAYOUT_PATH: &str = "/user/main/main_move_price.do";
const OPDOWNLOAD_EXCEL_PATH: &str = "/user/main/main_download_excel.do";
const OIL_PRICE_DOWNLOAD_TAR_URL: &str = "/user/opdown/oil_price_download";
const OPINET_KEY: &str = "tNNJ/zjnjSUqxRLpgiO/at1/w4SoJGbzzDOFVmlgEO0=";
const NETFUNNEL_SERVICE_ID: &str = "service_1";
const NETFUNNEL_ENTRY_ACTION_ID: &str = "B1";
const NETFUNNEL_DOWNLOAD_ACTION_ID: &str = "B7";
const CURRENT_PRICE_PAGE_DIV: &str = "PAGE_DIV_2";
const GAS_STATION_LPG_CODE: &str = "A";
const GAS_STATION_API_GBN: &str = "A";
const DEFAULT_REGION_LABEL: &str = "선택하세요.";
const USER_AGENT: &str = concat!("fcupdater/", env!("CARGO_PKG_VERSION"));
const NETFUNNEL_POLL_LIMIT: usize = 20;
pub const TARGET_REGION_KEYS: [&str; 11] = [
    "대전대덕구",
    "대전동구",
    "대전서구",
    "대전유성구",
    "대전중구",
    "세종시",
    "충북청주시",
    "충남공주시",
    "충남보령시",
    "충남아산시",
    "충남천안시",
];
pub struct SourceDownload<'dir, 'out> {
    pub dir: &'dir Path,
    pub out: &'out mut dyn Write,
}
#[derive(Debug)]
struct StreamedBodySummary {
    bytes_seen: usize,
    prefix: Vec<u8>,
    preview: Vec<u8>,
}
struct StreamingBodySink<'writer> {
    error: Option<String>,
    limit: usize,
    summary: StreamedBodySummary,
    writer: &'writer mut dyn Write,
}
impl StreamedBodySummary {
    fn preview_lossy(&self) -> Cow<'_, str> {
        lossy_prefix(&self.preview, self.preview.len())
    }
    fn starts_with(&self, prefix: &[u8]) -> bool {
        self.prefix.as_slice() == prefix && self.bytes_seen >= prefix.len()
    }
}
impl StreamingBodySink<'_> {
    fn append(&mut self, bytes: &[u8]) -> bool {
        let Some(next_len) = self.summary.bytes_seen.checked_add(bytes.len()) else {
            self.error = Some("HTTP 응답 본문 크기 계산 실패".to_owned());
            return false;
        };
        if next_len > self.limit {
            self.error = Some(format!(
                "HTTP 응답 본문 크기가 허용 한도({} bytes)를 초과했습니다.",
                self.limit
            ));
            return false;
        }
        if !self.capture_prefix(bytes) || !self.capture_preview(bytes) {
            return false;
        }
        if let Err(source) = self.writer.write_all(bytes) {
            self.error = Some(format!("HTTP 응답 본문 파일 쓰기 실패: {source}"));
            return false;
        }
        self.summary.bytes_seen = next_len;
        true
    }
    fn capture_prefix(&mut self, bytes: &[u8]) -> bool {
        let take = STREAM_PREFIX_BYTES
            .saturating_sub(self.summary.prefix.len())
            .min(bytes.len());
        if take == 0 {
            return true;
        }
        if let Err(source) = self.summary.prefix.try_reserve(take) {
            self.error = Some(format!("HTTP 응답 본문 prefix 메모리 확보 실패: {source}"));
            return false;
        }
        let Some(prefix) = bytes.get(..take) else {
            self.error = Some("HTTP 응답 본문 prefix 범위 계산 실패".to_owned());
            return false;
        };
        self.summary.prefix.extend_from_slice(prefix);
        true
    }
    fn capture_preview(&mut self, bytes: &[u8]) -> bool {
        let take = HTTP_ERROR_PREVIEW_BYTES
            .saturating_sub(self.summary.preview.len())
            .min(bytes.len());
        if take == 0 {
            return true;
        }
        if let Err(source) = self.summary.preview.try_reserve(take) {
            self.error = Some(format!("HTTP 응답 본문 preview 메모리 확보 실패: {source}"));
            return false;
        }
        let Some(preview) = bytes.get(..take) else {
            self.error = Some("HTTP 응답 본문 preview 범위 계산 실패".to_owned());
            return false;
        };
        self.summary.preview.extend_from_slice(preview);
        true
    }
}
fn lossy_prefix(bytes: &[u8], max_len: usize) -> Cow<'_, str> {
    let prefix_len = bytes.len().min(max_len);
    let Some((prefix, _)) = bytes.split_at_checked(prefix_len) else {
        return String::from_utf8_lossy(bytes);
    };
    String::from_utf8_lossy(prefix)
}
cfg_select! {
    windows => {
        fn checked_http_buffer_len(
            label: &str,
            current_len: usize,
            additional_len: usize,
            limit: usize,
        ) -> StdResult<usize, String> {
            let next_len = current_len
                .checked_add(additional_len)
                .ok_or_else(|| format!("HTTP 응답 {label} 크기 계산 실패"))?;
            if next_len > limit {
                Err(format!(
                    "HTTP 응답 {label} 크기가 허용 한도({limit} bytes)를 초과했습니다."
                ))
            } else {
                Ok(next_len)
            }
        }
    }
    _ => {}
}
fn enforce_http_content_length_limit(
    headers: &[(String, String)],
    limit: usize,
) -> StdResult<(), String> {
    for header in headers {
        let name = &header.0;
        let value = &header.1;
        if !name.eq_ignore_ascii_case("Content-Length") {
            continue;
        }
        let parsed = value
            .trim_ascii()
            .parse::<usize>()
            .map_err(|source| format!("HTTP Content-Length 해석 실패: {source}"))?;
        if parsed > limit {
            return Err(format!(
                "HTTP Content-Length가 허용 한도({limit} bytes)를 초과했습니다."
            ));
        }
    }
    Ok(())
}
