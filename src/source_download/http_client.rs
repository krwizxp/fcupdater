use super::{
    CURRENT_PRICE_PAGE_DIV, DEFAULT_REGION_LABEL, DownloadResult, GAS_STATION_API_GBN,
    GAS_STATION_LPG_CODE, HTTP_ERROR_PREVIEW_BYTES, HttpResponse, NETFUNNEL_DOWNLOAD_ACTION_ID,
    NETFUNNEL_ENTRY_ACTION_ID, NETFUNNEL_HOST, NETFUNNEL_POLL_LIMIT, NETFUNNEL_SERVICE_ID,
    OIL_PRICE_DOWNLOAD_TAR_URL, OLE2_SIGNATURE, OPDOWNLOAD_EXCEL_PATH, OPDOWNLOAD_LAYOUT_PATH,
    OPDOWNLOAD_PATH, OPDOWNLOAD_URL, OPINET_HOST, RequestHeaders, SourceDownload,
    download_error_with_source, try_string_with_capacity,
};
use core::{fmt::NumBuffer, mem, time::Duration};
use std::{
    process,
    thread::sleep,
    time::{SystemTime, UNIX_EPOCH},
};
const MAX_COOKIE_PAIR_BYTES: usize = 4096;
const MAX_COOKIES_PER_HOST: usize = 64;
#[derive(Clone, Copy)]
enum PostHeaderProfile {
    Ajax,
    Standard,
}
#[derive(Clone, Copy)]
enum HttpHost {
    Netfunnel,
    Opinet,
}
#[derive(Default)]
pub(super) struct CookieJar {
    cookies: Vec<String>,
}
impl CookieJar {
    fn add_cookie(&mut self, name: &str, value: &str) -> DownloadResult<()> {
        if name.is_empty()
            || name.bytes().any(|byte| {
                !(byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#'..=b'\'' | b'*'..=b'+' | b'-'..=b'.' | b'^'..=b'`' | b'|' | b'~'
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
        let pair_len = name.len().strict_add(1).strict_add(value.len());
        if pair_len > MAX_COOKIE_PAIR_BYTES {
            return Err(format!(
                "Cookie 이름과 값이 허용 한도({MAX_COOKIE_PAIR_BYTES} bytes)를 초과했습니다."
            )
            .into());
        }
        if let Some(cookie) = self.cookies.iter_mut().find(|cookie| {
            cookie
                .strip_prefix(name)
                .is_some_and(|tail| tail.starts_with('='))
        }) {
            let value_start = name.len().strict_add(1);
            let old_value_len = cookie.len().strict_sub(value_start);
            if value.len() > old_value_len {
                cookie
                    .try_reserve_exact(value.len().strict_sub(old_value_len))
                    .map_err(|source| {
                        download_error_with_source("Cookie 값 메모리 확보 실패", source)
                    })?;
            }
            cookie.truncate(value_start);
            cookie.push_str(value);
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
        let mut pair = try_string_with_capacity(pair_len, "Cookie 메모리 확보 실패")?;
        pair.extend([name, "=", value]);
        self.cookies.push(pair);
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
    fn fetch_netfunnel_ticket(&mut self, action_id: &str) -> DownloadResult<String> {
        let mut current_key: Option<String> = None;
        let mut current_ttl: Option<u32> = None;
        for _ in 0..NETFUNNEL_POLL_LIMIT {
            let result = self.request_netfunnel(action_id, current_key.as_deref(), current_ttl)?;
            self.add_cookie_for_host(HttpHost::Netfunnel, "NetFunnel_ID", &result)?;
            self.add_cookie_for_host(HttpHost::Opinet, "NetFunnel_ID", &result)?;
            let Some((_opcode, code_tail)) = result.split_once(':') else {
                return Err(format!("NetFunnel 코드 없음: {result}").into());
            };
            let code_text = split_head_or_all(code_tail, ':');
            let code = parse_netfunnel_u32(code_text, "NetFunnel 코드 파싱 실패")?;
            match code {
                200 | 300 | 303 => {
                    return take_netfunnel_key(result);
                }
                201 | 202 => {
                    let Some((_, ttl_tail)) = result.split_once("ttl=") else {
                        return Err(format!("NetFunnel 대기 응답에 ttl 없음: {result}").into());
                    };
                    let ttl_text = split_head_or_all(ttl_tail, '&');
                    let ttl = parse_netfunnel_u32(ttl_text, "NetFunnel ttl 파싱 실패")?;
                    let wait_secs = ttl.clamp(1, 30);
                    current_ttl = Some(ttl);
                    current_key = Some(take_netfunnel_key(result)?);
                    sleep(Duration::from_secs(u64::from(wait_secs)));
                }
                302 => return Err(format!("NetFunnel IP 차단: {result}").into()),
                _ => return Err(format!("NetFunnel 응답 오류: {result}").into()),
            }
        }
        Err("NetFunnel 대기 횟수를 초과했습니다.".into())
    }
    fn finish_response(
        &mut self,
        host: HttpHost,
        response: HttpResponse,
    ) -> DownloadResult<Vec<u8>> {
        let HttpResponse {
            body,
            headers,
            status,
        } = response;
        for value in &headers.set_cookies {
            let (cookie_name, cookie_value) = split_head_or_all(value, ';')
                .split_once('=')
                .ok_or_else(|| format!("HTTP Set-Cookie 형식이 올바르지 않습니다: {value}"))?;
            self.add_cookie_for_host(host, cookie_name.trim_ascii(), cookie_value.trim_ascii())?;
        }
        if !(200..300).contains(&status) {
            let body_preview =
                String::from_utf8_lossy(body.get(..HTTP_ERROR_PREVIEW_BYTES).unwrap_or(&body));
            return Err(format!("HTTP {status}: {body_preview}").into());
        }
        Ok(body)
    }
    fn post_form(
        &mut self,
        path: &str,
        form: &[(&str, &str)],
        referer: Option<&str>,
        profile: PostHeaderProfile,
    ) -> DownloadResult<Vec<u8>> {
        let mut body = mem::take(&mut self.form_body_buffer);
        let result = (|| {
            let required_capacity = form.iter().fold(0_usize, |sum, &(name, value)| {
                sum.strict_add(usize::from(sum != 0))
                    .strict_add(name.len().strict_add(value.len()).strict_mul(3))
                    .strict_add(1)
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
                Self::push_percent_encoded(&mut body, name.as_bytes());
                body.push('=');
                Self::push_percent_encoded(&mut body, value.as_bytes());
            }
            let headers = Self::request_headers(
                &self.cookie_jars,
                &mut self.cookie_header_buffer,
                HttpHost::Opinet,
                "text/html, */*; q=0.01",
                Some("application/x-www-form-urlencoded; charset=UTF-8"),
                referer,
                matches!(profile, PostHeaderProfile::Ajax),
            )?;
            let response =
                self.platform
                    .request(Some(body.as_bytes()), OPINET_HOST, path, headers)?;
            self.finish_response(HttpHost::Opinet, response)
        })();
        self.form_body_buffer = body;
        result
    }
    fn push_percent_encoded(out: &mut String, bytes: &[u8]) {
        for byte in bytes {
            match *byte {
                literal
                    if literal.is_ascii_alphanumeric()
                        || matches!(literal, b'-' | b'_' | b'.' | b'~') =>
                {
                    out.push(char::from(literal));
                }
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
    pub(crate) fn refresh_source(mut self) -> DownloadResult<Vec<u8>> {
        (|| -> DownloadResult<Vec<u8>> {
            let headers = Self::request_headers(
                &self.cookie_jars,
                &mut self.cookie_header_buffer,
                HttpHost::Opinet,
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                None,
                None,
                false,
            )?;
            let opdownload_response =
                self.platform
                    .request(None, OPINET_HOST, OPDOWNLOAD_PATH, headers)?;
            let body = self.finish_response(HttpHost::Opinet, opdownload_response)?;
            let opdownload_page = String::from_utf8(body).map_err(|source| {
                download_error_with_source("HTTP 응답 UTF-8 변환 실패", source)
            })?;
            let opinet_key = {
                const KEY_ASSIGNMENT_MARKER: &str = "opinet_key.value";
                let Some((_, after_marker)) = opdownload_page.split_once(KEY_ASSIGNMENT_MARKER)
                else {
                    return Err(
                        "Opinet 다운로드 페이지에서 key 할당 구문을 찾지 못했습니다.".into(),
                    );
                };
                let (_, raw_value) = after_marker
                    .split_once('=')
                    .ok_or("Opinet key 할당 구문의 '=' 문자를 찾지 못했습니다.")?;
                let after_eq = raw_value.trim_ascii_start();
                let (quote, value_tail) = if let Some(value_tail) = after_eq.strip_prefix('\'') {
                    ('\'', value_tail)
                } else if let Some(value_tail) = after_eq.strip_prefix('"') {
                    ('"', value_tail)
                } else {
                    return Err("Opinet key 값 quote 문자를 찾지 못했습니다.".into());
                };
                let (value, _) = value_tail
                    .split_once(quote)
                    .ok_or("Opinet key 값 종료 quote를 찾지 못했습니다.")?;
                (!value.is_empty()).ok_or("Opinet key 값이 비어 있습니다.")?;
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
            let response = self.post_form(
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
                let (preview_bytes, _) = response.split_at(preview_len);
                let preview = String::from_utf8_lossy(preview_bytes);
                let error_text =
                    format!("다운로드 응답이 예상한 OLE2 .xls 파일이 아닙니다: {preview}");
                return Err(error_text.into());
            }
            Ok(response)
        })()
        .map_err(|mut error| {
            error.update_message(|message| format!("Opinet 자동 다운로드 실패: {message}"));
            error
        })
    }
    fn request_headers<'request>(
        cookie_jars: &(CookieJar, CookieJar),
        cookie_header: &'request mut String,
        host: HttpHost,
        accept: &'static str,
        content_type: Option<&'static str>,
        referer: Option<&'request str>,
        requested_with: bool,
    ) -> DownloadResult<RequestHeaders<'request>> {
        cookie_header.clear();
        let jar = match host {
            HttpHost::Opinet => &cookie_jars.0,
            HttpHost::Netfunnel => &cookie_jars.1,
        };
        let cookie_text = if jar.cookies.is_empty() {
            None
        } else {
            let separator_capacity = jar.cookies.len().strict_sub(1).strict_mul(2);
            let capacity = jar.cookies.iter().fold(separator_capacity, |sum, cookie| {
                sum.strict_add(cookie.len())
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
                cookie_header.push_str(cookie);
            }
            Some(cookie_header.as_str())
        };
        Ok(RequestHeaders {
            accept,
            content_type,
            cookie: cookie_text,
            referer,
            requested_with,
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
            path.extend(["/ts.wseq?opcode=", opcode]);
            if let Some(key_text) = key {
                path.push_str("&key=");
                Self::push_percent_encoded(&mut path, key_text.as_bytes());
            }
            path.extend(["&nfid=0&prefix=NetFunnel.gRtype%3D", opcode, "%3B"]);
            if let Some(ttl_secs) = ttl {
                let mut ttl_buffer = NumBuffer::new();
                path.extend(["&ttl=", ttl_secs.format_into(&mut ttl_buffer)]);
            }
            path.extend([
                "&sid=",
                NETFUNNEL_SERVICE_ID,
                "&aid=",
                action_id,
                "&js=yes&",
            ]);
            let mut timestamp_buffer = NumBuffer::new();
            path.push_str(timestamp.format_into(&mut timestamp_buffer));
            let headers = Self::request_headers(
                &self.cookie_jars,
                &mut self.cookie_header_buffer,
                HttpHost::Netfunnel,
                "application/javascript,*/*;q=0.8",
                None,
                None,
                false,
            )?;
            let response = self
                .platform
                .request(None, NETFUNNEL_HOST, &path, headers)?;
            self.finish_response(HttpHost::Netfunnel, response)
        };
        self.netfunnel_path_buffer = path;
        let response = response_result?;
        let mut text = String::from_utf8(response).map_err(|source| {
            download_error_with_source("NetFunnel 응답 UTF-8 변환 실패", source)
        })?;
        let Some((_, value_tail)) = text.split_once("result='") else {
            return Err(format!("NetFunnel result 파싱 실패: {text}").into());
        };
        let Some((value, _)) = value_tail.split_once('\'') else {
            return Err(format!("NetFunnel result 파싱 실패: {text}").into());
        };
        let value_range = text.substr_range(value).unwrap_or_else(|| process::abort());
        text.truncate(value_range.end);
        text.replace_range(..value_range.start, "");
        Ok(text)
    }
}
const fn hex_digit(nibble: u8) -> u8 {
    b'0'.strict_add(nibble)
        .strict_add(nibble.strict_add(6).wrapping_shr(4).strict_mul(7))
}
fn take_netfunnel_key(mut result: String) -> DownloadResult<String> {
    let (_, value_tail) = result
        .split_once("key=")
        .ok_or_else(|| format!("NetFunnel key 없음: {result}"))?;
    let value = split_head_or_all(value_tail, '&');
    (!value.is_empty()).ok_or_else(|| format!("NetFunnel key 비어 있음: {result}"))?;
    let value_range = result
        .substr_range(value)
        .unwrap_or_else(|| process::abort());
    result.truncate(value_range.end);
    result.replace_range(..value_range.start, "");
    Ok(result)
}
fn split_head_or_all(value: &str, separator: char) -> &str {
    value.split_once(separator).map_or(value, |(head, _)| head)
}
fn parse_netfunnel_u32(value: &str, context: &'static str) -> DownloadResult<u32> {
    (!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
        .ok_or_else(|| format!("{context}: 음이 아닌 10진수 형식이 아닙니다."))?;
    value
        .parse::<u32>()
        .map_err(|source| download_error_with_source(format!("{context} 해석 실패"), source))
}
