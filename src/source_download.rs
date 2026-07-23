use crate::diagnostic::{Result as DownloadResult, err_with_source as download_error_with_source};
use core::fmt::Write as FmtWrite;
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
const HTTP_MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const HTTP_MAX_HEADER_BYTES: usize = 256 * 1024;
const HTTP_ERROR_PREVIEW_BYTES: usize = 512;
const RESPONSE_HEADER_CONTENT_LENGTH: &[u8] = b"Content-Length";
const RESPONSE_HEADER_SET_COOKIE: &[u8] = b"Set-Cookie";
const OLE2_SIGNATURE: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
const _: () = assert!(
    OLE2_SIGNATURE.len() <= HTTP_ERROR_PREVIEW_BYTES,
    "OLE2 signature preview must include the full signature"
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
#[derive(Default)]
pub(super) struct SourceDownload {
    cookie_header_buffer: String,
    cookie_jars: (http_client::CookieJar, http_client::CookieJar),
    form_body_buffer: String,
    netfunnel_path_buffer: String,
    platform: PlatformHttpClient,
}
#[derive(Debug, Default)]
struct ResponseHeaders {
    content_length: Option<usize>,
    set_cookies: Vec<String>,
}
#[derive(Debug)]
struct HttpResponse {
    body: Vec<u8>,
    headers: ResponseHeaders,
    status: u32,
}
#[derive(Clone, Copy)]
struct RequestHeaders<'header> {
    accept: &'static str,
    content_type: Option<&'static str>,
    cookie: Option<&'header str>,
    referer: Option<&'header str>,
    requested_with: bool,
}
impl RequestHeaders<'_> {
    fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        [
            Some(("User-Agent", USER_AGENT)),
            Some(("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.5,en;q=0.3")),
            Some(("Accept", self.accept)),
            self.content_type.map(|value| ("Content-Type", value)),
            self.requested_with
                .then_some(("X-Requested-With", "XMLHttpRequest")),
            self.referer.map(|value| ("Referer", value)),
            self.cookie.map(|value| ("Cookie", value)),
        ]
        .into_iter()
        .flatten()
    }
}
impl ResponseHeaders {
    fn parse_content_length(&mut self, raw_value: &str, limit: usize) -> DownloadResult<()> {
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
            parsed = parsed
                .checked_mul(10)
                .and_then(|scaled| scaled.checked_add(usize::from(byte.wrapping_sub(b'0'))))
                .ok_or("HTTP Content-Length 해석 실패")?;
        }
        if parsed > limit {
            return Err(
                format!("HTTP Content-Length가 허용 한도({limit} bytes)를 초과했습니다.").into(),
            );
        }
        if self
            .content_length
            .is_some_and(|previous| previous != parsed)
        {
            return Err("HTTP Content-Length 헤더 값이 서로 다릅니다.".into());
        }
        self.content_length = Some(parsed);
        Ok(())
    }
    fn push_set_cookie(&mut self, value: &str) -> DownloadResult<()> {
        self.set_cookies.try_reserve(1).map_err(|source| {
            download_error_with_source("HTTP Set-Cookie 목록 메모리 확보 실패", source)
        })?;
        let mut owned = String::new();
        owned.try_reserve_exact(value.len()).map_err(|source| {
            download_error_with_source("HTTP Set-Cookie 값 메모리 확보 실패", source)
        })?;
        owned.push_str(value);
        self.set_cookies.push(owned);
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
