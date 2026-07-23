use super::{
    CURRENT_PRICE_PAGE_DIV, DEFAULT_REGION_LABEL, DownloadResult, GAS_STATION_API_GBN,
    GAS_STATION_LPG_CODE, HTTP_ERROR_PREVIEW_BYTES, HttpResponse, NETFUNNEL_DOWNLOAD_ACTION_ID,
    NETFUNNEL_ENTRY_ACTION_ID, NETFUNNEL_HOST, NETFUNNEL_POLL_LIMIT, NETFUNNEL_SERVICE_ID,
    OIL_PRICE_DOWNLOAD_TAR_URL, OLE2_SIGNATURE, OPDOWNLOAD_EXCEL_PATH, OPDOWNLOAD_LAYOUT_PATH,
    OPDOWNLOAD_PATH, OPDOWNLOAD_URL, OPINET_HOST, SourceDownload, USER_AGENT,
    download_error_with_source, push_decimal_fragment,
};
use crate::diagnostic::prefixed_message;
use core::{mem, time::Duration};
use std::{
    thread::sleep,
    time::{SystemTime, UNIX_EPOCH},
};
const HTTP_REQUEST_HEADER_CAPACITY: usize = 4;
const HTTP_MERGED_HEADER_CAPACITY: usize = 2 + HTTP_REQUEST_HEADER_CAPACITY + 1;
const MAX_COOKIE_PAIR_BYTES: usize = 4096;
const MAX_COOKIES_PER_HOST: usize = 64;
#[derive(Clone, Copy)]
pub(super) enum PostHeaderProfile {
    Ajax,
    Standard,
}
#[derive(Clone, Copy)]
enum HttpHost {
    Netfunnel,
    Opinet,
}
struct HeaderStack<'header, const CAPACITY: usize> {
    headers: [(&'header str, &'header str); CAPACITY],
    len: usize,
}
struct Cookie {
    name: String,
    value: String,
}
#[derive(Default)]
pub(super) struct CookieJar {
    cookies: Vec<Cookie>,
}
impl<'header, const CAPACITY: usize> HeaderStack<'header, CAPACITY> {
    const fn as_slice(&self) -> &[(&'header str, &'header str)] {
        self.headers.split_at(self.len).0
    }
    fn extend_from_slice(
        &mut self,
        headers: &[(&'header str, &'header str)],
    ) -> DownloadResult<()> {
        for &(name, value) in headers {
            self.push(name, value)?;
        }
        Ok(())
    }
    const fn new() -> Self {
        Self {
            headers: [("", ""); CAPACITY],
            len: 0,
        }
    }
    fn push(&mut self, name: &'header str, value: &'header str) -> DownloadResult<()> {
        let Some(slot) = self.headers.get_mut(self.len) else {
            return Err("HTTP header stack 용량을 초과했습니다.".into());
        };
        *slot = (name, value);
        self.len = self.len.wrapping_add(1);
        Ok(())
    }
}
impl CookieJar {
    fn add_cookie(&mut self, name: &str, value: &str) -> DownloadResult<()> {
        if name.is_empty()
            || name.bytes().any(|byte| {
                !(byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#'
                            | b'$'
                            | b'%'
                            | b'&'
                            | b'\''
                            | b'*'
                            | b'+'
                            | b'-'
                            | b'.'
                            | b'^'
                            | b'_'
                            | b'`'
                            | b'|'
                            | b'~'
                    ))
            })
        {
            return Err("Cookie 이름에 허용되지 않는 문자가 포함되어 있습니다.".into());
        }
        let value_body = if let Some(unquoted) = value.strip_prefix('"') {
            unquoted
                .strip_suffix('"')
                .ok_or("Cookie 값의 quote 형식이 올바르지 않습니다.")?
        } else {
            value
        };
        if value_body.bytes().any(|byte| {
            !matches!(
                byte,
                b'!' | b'#'..=b'+' | b'-'..=b':' | b'<'..=b'[' | b']'..=b'~'
            )
        }) {
            return Err("Cookie 값에 허용되지 않는 문자가 포함되어 있습니다.".into());
        }
        let pair_len = name.len().saturating_add(1).saturating_add(value.len());
        if pair_len > MAX_COOKIE_PAIR_BYTES {
            return Err(format!(
                "Cookie 이름과 값이 허용 한도({MAX_COOKIE_PAIR_BYTES} bytes)를 초과했습니다."
            )
            .into());
        }
        if let Some(cookie) = self.cookies.iter_mut().find(|cookie| cookie.name == name) {
            let additional = value.len().saturating_sub(cookie.value.len());
            cookie
                .value
                .try_reserve_exact(additional)
                .map_err(|source| {
                    download_error_with_source("Cookie 값 메모리 확보 실패", source)
                })?;
            cookie.value.clear();
            cookie.value.push_str(value);
            return Ok(());
        }
        if self.cookies.len() >= MAX_COOKIES_PER_HOST {
            return Err(format!(
                "호스트별 Cookie 수가 허용 한도({MAX_COOKIES_PER_HOST}개)를 초과했습니다."
            )
            .into());
        }
        self.cookies
            .try_reserve(1)
            .map_err(|source| download_error_with_source("Cookie 목록 메모리 확보 실패", source))?;
        let mut cookie = Cookie {
            name: String::new(),
            value: String::new(),
        };
        cookie
            .name
            .try_reserve_exact(name.len())
            .map_err(|source| download_error_with_source("Cookie 이름 메모리 확보 실패", source))?;
        cookie
            .value
            .try_reserve_exact(value.len())
            .map_err(|source| download_error_with_source("Cookie 값 메모리 확보 실패", source))?;
        cookie.name.push_str(name);
        cookie.value.push_str(value);
        self.cookies.push(cookie);
        Ok(())
    }
}
impl SourceDownload {
    fn add_cookie_for_host(
        &mut self,
        host: HttpHost,
        name: &str,
        value: &str,
    ) -> DownloadResult<()> {
        let jar = match host {
            HttpHost::Opinet => &mut self.cookie_jars.0,
            HttpHost::Netfunnel => &mut self.cookie_jars.1,
        };
        jar.add_cookie(name, value)
    }
    fn encode_form_body_into(body: &mut String, form: &[(&str, &str)]) -> DownloadResult<()> {
        let required_capacity = form.iter().fold(0_usize, |sum, &(name, value)| {
            sum.saturating_add(usize::from(sum != 0))
                .saturating_add(Self::percent_encoded_len(name.as_bytes()))
                .saturating_add(Self::percent_encoded_len(value.as_bytes()))
                .saturating_add(1)
        });
        body.clear();
        body.try_reserve_exact(required_capacity)
            .map_err(|source| {
                download_error_with_source("HTTP form body 메모리 확보 실패", source)
            })?;
        for (index, &(name, value)) in form.iter().enumerate() {
            if index != 0 {
                body.push('&');
            }
            Self::push_percent_encoded(&mut *body, name.as_bytes());
            body.push('=');
            Self::push_percent_encoded(&mut *body, value.as_bytes());
        }
        Ok(())
    }
    pub(super) fn fetch_netfunnel_ticket(&mut self, action_id: &str) -> DownloadResult<String> {
        let mut current_key: Option<String> = None;
        let mut current_ttl: Option<u32> = None;
        for _ in 0..NETFUNNEL_POLL_LIMIT {
            let result = self.request_netfunnel(action_id, current_key.as_deref(), current_ttl)?;
            self.add_cookie_for_host(HttpHost::Netfunnel, "NetFunnel_ID", &result)?;
            self.add_cookie_for_host(HttpHost::Opinet, "NetFunnel_ID", &result)?;
            let Some((_opcode, code_tail)) = result.split_once(':') else {
                return Err(prefixed_message("NetFunnel 코드 없음: ", result).into());
            };
            let code_text = split_head_or_all(code_tail, ':');
            let code = parse_netfunnel_u32(code_text, "NetFunnel 코드 파싱 실패")?;
            match code {
                200 | 300 | 303 => {
                    return take_netfunnel_key(result);
                }
                201 | 202 => {
                    current_ttl = result
                        .split_once("ttl=")
                        .map(|(_, ttl_tail)| {
                            let ttl_text = split_head_or_all(ttl_tail, '&');
                            parse_netfunnel_u32(ttl_text, "NetFunnel ttl 파싱 실패")
                        })
                        .transpose()?;
                    let wait_secs = current_ttl.unwrap_or(1).clamp(1, 30);
                    current_key = Some(take_netfunnel_key(result)?);
                    sleep(Duration::from_secs(u64::from(wait_secs)));
                }
                302 => return Err(prefixed_message("NetFunnel IP 차단: ", result).into()),
                _ => return Err(prefixed_message("NetFunnel 응답 오류: ", result).into()),
            }
        }
        Err("NetFunnel 대기 횟수를 초과했습니다.".into())
    }
    fn finish_response(
        &mut self,
        host: HttpHost,
        response: HttpResponse,
    ) -> DownloadResult<HttpResponse> {
        for (cookie_name, cookie_value) in response
            .headers
            .set_cookies
            .iter()
            .filter_map(|value| split_head_or_all(value, ';').split_once('='))
        {
            self.add_cookie_for_host(host, cookie_name.trim_ascii(), cookie_value.trim_ascii())?;
        }
        if !(200..300).contains(&response.status) {
            let body_preview = String::from_utf8_lossy(
                response
                    .body
                    .get(..HTTP_ERROR_PREVIEW_BYTES)
                    .unwrap_or(&response.body),
            );
            let status = response.status;
            return Err(format!("HTTP {status}: {body_preview}").into());
        }
        Ok(response)
    }
    pub(super) fn get_text(&mut self, path: &str, referer: Option<&str>) -> DownloadResult<String> {
        let mut headers = HeaderStack::<HTTP_REQUEST_HEADER_CAPACITY>::new();
        headers.push(
            "Accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        )?;
        if let Some(referer_value) = referer {
            headers.push("Referer", referer_value)?;
        }
        let merged_headers = Self::merged_headers(
            &self.cookie_jars,
            &mut self.cookie_header_buffer,
            HttpHost::Opinet,
            headers.as_slice(),
        )?;
        let raw_response = self
            .platform
            .get(OPINET_HOST, path, merged_headers.as_slice())?;
        let response = self.finish_response(HttpHost::Opinet, raw_response)?;
        String::from_utf8(response.body)
            .map_err(|source| download_error_with_source("HTTP 응답 UTF-8 변환 실패", source))
    }
    fn merged_headers<'request>(
        cookie_jars: &(CookieJar, CookieJar),
        cookie_header: &'request mut String,
        host: HttpHost,
        headers: &[(&'request str, &'request str)],
    ) -> DownloadResult<HeaderStack<'request, HTTP_MERGED_HEADER_CAPACITY>> {
        cookie_header.clear();
        let jar = match host {
            HttpHost::Opinet => &cookie_jars.0,
            HttpHost::Netfunnel => &cookie_jars.1,
        };
        let cookie_text = if jar.cookies.is_empty() {
            None
        } else {
            let separator_capacity = jar.cookies.len().saturating_sub(1).saturating_mul(2);
            let capacity = jar.cookies.iter().fold(separator_capacity, |sum, cookie| {
                sum.saturating_add(cookie.name.len())
                    .saturating_add(1)
                    .saturating_add(cookie.value.len())
            });
            cookie_header
                .try_reserve_exact(capacity)
                .map_err(|source| {
                    download_error_with_source("Cookie header 메모리 확보 실패", source)
                })?;
            for (index, cookie) in jar.cookies.iter().enumerate() {
                if index != 0 {
                    cookie_header.push_str("; ");
                }
                cookie_header.push_str(&cookie.name);
                cookie_header.push('=');
                cookie_header.push_str(&cookie.value);
            }
            Some(cookie_header.as_str())
        };
        let mut merged_headers = HeaderStack::<HTTP_MERGED_HEADER_CAPACITY>::new();
        merged_headers.push("User-Agent", USER_AGENT)?;
        merged_headers.push("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.5,en;q=0.3")?;
        merged_headers.extend_from_slice(headers)?;
        if let Some(value) = cookie_text {
            merged_headers.push("Cookie", value)?;
        }
        Ok(merged_headers)
    }
    fn percent_encoded_len(bytes: &[u8]) -> usize {
        bytes.iter().fold(0_usize, |sum, byte| {
            let byte_len = if is_url_form_literal(*byte) || *byte == b' ' {
                1
            } else {
                3
            };
            sum.saturating_add(byte_len)
        })
    }
    pub(super) fn post_form(
        &mut self,
        path: &str,
        form: &[(&str, &str)],
        referer: Option<&str>,
        profile: PostHeaderProfile,
    ) -> DownloadResult<HttpResponse> {
        let mut body = mem::take(&mut self.form_body_buffer);
        let result = (|| {
            Self::encode_form_body_into(&mut body, form)?;
            let headers = Self::post_headers(referer, profile)?;
            let merged_headers = Self::merged_headers(
                &self.cookie_jars,
                &mut self.cookie_header_buffer,
                HttpHost::Opinet,
                headers.as_slice(),
            )?;
            let response = self.platform.post(
                OPINET_HOST,
                path,
                merged_headers.as_slice(),
                body.as_bytes(),
            )?;
            self.finish_response(HttpHost::Opinet, response)
        })();
        self.form_body_buffer = body;
        result
    }
    pub(super) fn post_form_body(
        &mut self,
        path: &str,
        form: &[(&str, &str)],
        referer: Option<&str>,
        profile: PostHeaderProfile,
    ) -> DownloadResult<Vec<u8>> {
        let mut body = mem::take(&mut self.form_body_buffer);
        let result = (|| {
            Self::encode_form_body_into(&mut body, form)?;
            let headers = Self::post_headers(referer, profile)?;
            let merged_headers = Self::merged_headers(
                &self.cookie_jars,
                &mut self.cookie_header_buffer,
                HttpHost::Opinet,
                headers.as_slice(),
            )?;
            let response = self.platform.post(
                OPINET_HOST,
                path,
                merged_headers.as_slice(),
                body.as_bytes(),
            )?;
            self.finish_response(HttpHost::Opinet, response)
                .map(|finished| finished.body)
        })();
        self.form_body_buffer = body;
        result
    }
    fn post_headers(
        referer: Option<&str>,
        profile: PostHeaderProfile,
    ) -> DownloadResult<HeaderStack<'_, HTTP_REQUEST_HEADER_CAPACITY>> {
        let mut headers = HeaderStack::<HTTP_REQUEST_HEADER_CAPACITY>::new();
        headers.push(
            "Content-Type",
            "application/x-www-form-urlencoded; charset=UTF-8",
        )?;
        headers.push("Accept", "text/html, */*; q=0.01")?;
        if matches!(profile, PostHeaderProfile::Ajax) {
            headers.push("X-Requested-With", "XMLHttpRequest")?;
        }
        if let Some(referer_value) = referer {
            headers.push("Referer", referer_value)?;
        }
        Ok(headers)
    }
    fn push_percent_encoded(out: &mut String, bytes: &[u8]) {
        for byte in bytes {
            match *byte {
                literal if is_url_form_literal(literal) => out.push(char::from(literal)),
                b' ' => out.push('+'),
                other => {
                    let high = other >> 4_u8;
                    let low = other & 0x0F;
                    out.push('%');
                    out.push(char::from(hex_digit(high)));
                    out.push(char::from(hex_digit(low)));
                }
            }
        }
    }
    pub(crate) fn refresh_source(&mut self) -> DownloadResult<Vec<u8>> {
        let result = (|| -> DownloadResult<Vec<u8>> {
            let opdownload_page = self.get_text(OPDOWNLOAD_PATH, None)?;
            let opinet_key = {
                const KEY_ASSIGNMENT_MARKER: &str = "opinet_key.value";
                let Some((_, after_marker)) = opdownload_page.split_once(KEY_ASSIGNMENT_MARKER)
                else {
                    return Err(
                        "Opinet 다운로드 페이지에서 key 할당 구문을 찾지 못했습니다.".into(),
                    );
                };
                let Some((_, raw_value)) = after_marker.split_once('=') else {
                    return Err("Opinet key 할당 구문의 '=' 문자를 찾지 못했습니다.".into());
                };
                let after_eq = raw_value.trim_ascii_start();
                let (quote, value_tail) = if let Some(value_tail) = after_eq.strip_prefix('\'') {
                    ('\'', value_tail)
                } else if let Some(value_tail) = after_eq.strip_prefix('"') {
                    ('"', value_tail)
                } else {
                    return Err("Opinet key 값 quote 문자를 찾지 못했습니다.".into());
                };
                let Some((value, _)) = value_tail.split_once(quote) else {
                    return Err("Opinet key 값 종료 quote를 찾지 못했습니다.".into());
                };
                if value.is_empty() {
                    return Err("Opinet key 값이 비어 있습니다.".into());
                }
                value
            };
            let entry_key = self.fetch_netfunnel_ticket(NETFUNNEL_ENTRY_ACTION_ID)?;
            self.post_form(
                OPDOWNLOAD_PATH,
                &[
                    ("netfunnel_key", entry_key.as_str()),
                    ("opinet_key", opinet_key),
                ],
                Some(OPDOWNLOAD_URL),
                PostHeaderProfile::Standard,
            )?;
            self.post_form(
                OPDOWNLOAD_LAYOUT_PATH,
                &[("tarUrl", OIL_PRICE_DOWNLOAD_TAR_URL)],
                Some(OPDOWNLOAD_URL),
                PostHeaderProfile::Ajax,
            )?;
            let download_key = self.fetch_netfunnel_ticket(NETFUNNEL_DOWNLOAD_ACTION_ID)?;
            let response = self.post_form_body(
                OPDOWNLOAD_EXCEL_PATH,
                &[
                    ("LPG_CD", GAS_STATION_LPG_CODE),
                    ("DATE_DIV_CD", ""),
                    ("PAGE_DIV", CURRENT_PRICE_PAGE_DIV),
                    ("SIDO_NM", DEFAULT_REGION_LABEL),
                    ("SIGUN_NM", DEFAULT_REGION_LABEL),
                    ("API_GBN", GAS_STATION_API_GBN),
                    ("netfunnel_key", download_key.as_str()),
                ],
                Some(OPDOWNLOAD_URL),
                PostHeaderProfile::Standard,
            )?;
            if !response.starts_with(&OLE2_SIGNATURE) {
                let preview_len = response.len().min(HTTP_ERROR_PREVIEW_BYTES);
                let preview =
                    String::from_utf8_lossy(response.get(..preview_len).unwrap_or_default());
                let error_text = prefixed_message(
                    "다운로드 응답이 예상한 OLE2 .xls 파일이 아닙니다: ",
                    preview,
                );
                return Err(error_text.into());
            }
            Ok(response)
        })();
        result.map_err(|mut error| {
            error
                .update_message(|message| prefixed_message("Opinet 자동 다운로드 실패: ", message));
            error
        })
    }
    fn request_netfunnel(
        &mut self,
        action_id: &str,
        key: Option<&str>,
        ttl: Option<u32>,
    ) -> DownloadResult<String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .map_err(|source| download_error_with_source("현재 시간 조회 실패", source))?;
        let opcode = key.map_or("5101", |_| "5002");
        let mut path = mem::take(&mut self.netfunnel_path_buffer);
        let response_result = {
            path.clear();
            path.push_str("/ts.wseq?opcode=");
            path.push_str(opcode);
            if let Some(key_text) = key {
                path.push_str("&key=");
                Self::push_percent_encoded(&mut path, key_text.as_bytes());
            }
            path.push_str("&nfid=0&prefix=NetFunnel.gRtype%3D");
            path.push_str(opcode);
            path.push_str("%3B");
            if let Some(ttl_secs) = ttl {
                path.push_str("&ttl=");
                push_decimal_fragment(&mut path, u128::from(ttl_secs));
            }
            path.push_str("&sid=");
            path.push_str(NETFUNNEL_SERVICE_ID);
            path.push_str("&aid=");
            path.push_str(action_id);
            path.push_str("&js=yes&");
            push_decimal_fragment(&mut path, timestamp);
            let merged_headers = Self::merged_headers(
                &self.cookie_jars,
                &mut self.cookie_header_buffer,
                HttpHost::Netfunnel,
                &[("Accept", "application/javascript,*/*;q=0.8")],
            )?;
            let response = self
                .platform
                .get(NETFUNNEL_HOST, &path, merged_headers.as_slice())?;
            self.finish_response(HttpHost::Netfunnel, response)
        };
        self.netfunnel_path_buffer = path;
        let response = response_result?;
        let mut text = String::from_utf8(response.body).map_err(|source| {
            download_error_with_source("NetFunnel 응답 UTF-8 변환 실패", source)
        })?;
        let Some((_, value_tail)) = text.split_once("result='") else {
            return Err(prefixed_message("NetFunnel result 파싱 실패: ", text).into());
        };
        let Some((value, _)) = value_tail.split_once('\'') else {
            return Err(prefixed_message("NetFunnel result 파싱 실패: ", text).into());
        };
        let value_start = text.len().saturating_sub(value_tail.len());
        let value_end = value_start.saturating_add(value.len());
        text.truncate(value_end);
        text.replace_range(..value_start, "");
        Ok(text)
    }
}
const fn hex_digit(nibble: u8) -> u8 {
    if nibble < 10 {
        b'0'.wrapping_add(nibble)
    } else {
        b'A'.wrapping_add(nibble.saturating_sub(10))
    }
}
const fn is_url_form_literal(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~')
}
fn take_netfunnel_key(mut result: String) -> DownloadResult<String> {
    let Some((_, value_tail)) = result.split_once("key=") else {
        return Err(prefixed_message("NetFunnel key 없음: ", result).into());
    };
    let value = split_head_or_all(value_tail, '&');
    if value.is_empty() {
        return Err(prefixed_message("NetFunnel key 비어 있음: ", result).into());
    }
    let value_start = result.len().saturating_sub(value_tail.len());
    let value_end = value_start.saturating_add(value.len());
    result.truncate(value_end);
    result.replace_range(..value_start, "");
    Ok(result)
}
fn split_head_or_all(value: &str, separator: char) -> &str {
    value.split_once(separator).map_or(value, |(head, _)| head)
}
fn parse_netfunnel_u32(value: &str, context: &'static str) -> DownloadResult<u32> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("{context}: 음이 아닌 10진수 형식이 아닙니다.").into());
    }
    value
        .parse::<u32>()
        .map_err(|source| download_error_with_source(format!("{context} 해석 실패"), source))
}
