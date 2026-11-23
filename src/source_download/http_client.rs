use super::{
    DownloadResult, HttpResponse, RequestHeaders, SourceDownload, download_error_with_source,
    parse_unsigned_u32, try_push_vec, try_string_with_capacity,
};
use crate::diagnostic::{bail_fmt, ensure, ensure_fmt, fmt_err, map_reserve, or_abort};
use core::{fmt::NumBuffer, mem, range::Range, time::Duration};
use std::{
    thread::sleep,
    time::{SystemTime, UNIX_EPOCH},
};
const HTTP_ERROR_PREVIEW_BYTES: usize = 512;
const MAX_COOKIE_PAIR_BYTES: usize = 4096;
const MAX_COOKIES_PER_HOST: usize = 64;
const OLE2_SIGNATURE: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
const OPDOWNLOAD_PATH: &str = "/user/opdown/opDownload.do";
const OPDOWNLOAD_URL: &str = "https://www.opinet.co.kr/user/opdown/opDownload.do";
const OPINET_HOST: &str = "www.opinet.co.kr";
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
        ensure(
            !name.is_empty() && name.bytes().all(|byte| {
                byte.is_ascii_alphanumeric()
                    || matches!(
                        byte,
                        b'!' | b'#'..=b'\'' | b'*'..=b'+' | b'-'..=b'.' | b'^'..=b'`' | b'|' | b'~'
                    )
            }),
            "Cookie 이름에 허용되지 않는 문자가 포함되어 있습니다.",
        )?;
        let value_body = if value.starts_with('"') {
            value
                .strip_circumfix('"', '"')
                .ok_or("Cookie 값의 quote 형식이 올바르지 않습니다.")?
        } else {
            value
        };
        ensure(
            value_body.bytes().all(|byte| {
                matches!(
                    byte,
                    b'!' | b'#'..=b'+' | b'-'..=b':' | b'<'..=b'[' | b']'..=b'~'
                )
            }),
            "Cookie 값에 허용되지 않는 문자가 포함되어 있습니다.",
        )?;
        let pair_len = name.len().strict_add(1).strict_add(value.len());
        ensure_fmt!(
            pair_len <= MAX_COOKIE_PAIR_BYTES,
            "Cookie 이름과 값이 허용 한도({MAX_COOKIE_PAIR_BYTES} bytes)를 초과했습니다."
        );
        if let Some(cookie) = self.cookies.iter_mut().find(|cookie| {
            cookie
                .strip_prefix(name)
                .is_some_and(|tail| tail.starts_with('='))
        }) {
            if pair_len > cookie.len() {
                map_reserve(
                    cookie.try_reserve_exact(pair_len.strict_sub(cookie.len())),
                    "Cookie 값 메모리 확보 실패",
                )?;
            }
            cookie.truncate(name.len().strict_add(1));
            cookie.push_str(value);
            return Ok(());
        }
        ensure_fmt!(
            self.cookies.len() < MAX_COOKIES_PER_HOST,
            "호스트별 Cookie 수가 허용 한도({MAX_COOKIES_PER_HOST}개)를 초과했습니다."
        );
        let mut pair = try_string_with_capacity(pair_len, "Cookie 메모리 확보 실패")?;
        pair.extend([name, "=", value]);
        try_push_vec(&mut self.cookies, pair, 1, "Cookie 목록 메모리 확보 실패")
    }
}
impl SourceDownload {
    fn add_cookie_for_host(
        &mut self,
        host: HttpHost,
        name: &str,
        value: &str,
    ) -> DownloadResult<()> {
        match host {
            HttpHost::Opinet => self.cookie_jars.0.add_cookie(name, value),
            HttpHost::Netfunnel => self.cookie_jars.1.add_cookie(name, value),
        }
    }
    fn fetch_netfunnel_ticket(&mut self, action_id: &str) -> DownloadResult<String> {
        let (mut current_key, mut current_ttl): (Option<String>, Option<u32>) = (None, None);
        for _ in 0_usize..20_usize {
            let result = self.request_netfunnel(action_id, current_key.as_deref(), current_ttl)?;
            self.add_cookie_for_host(HttpHost::Netfunnel, "NetFunnel_ID", &result)?;
            self.add_cookie_for_host(HttpHost::Opinet, "NetFunnel_ID", &result)?;
            let (_, code_tail) = result
                .split_once(':')
                .ok_or_else(|| fmt_err!("NetFunnel 코드 없음: {result}"))?;
            let code = parse_netfunnel_u32(
                split_head_or_all(code_tail, ':'),
                "NetFunnel 코드 파싱 실패",
            )?;
            match code {
                200 | 300 | 303 => return take_netfunnel_key(result),
                201 | 202 => {
                    let (_, ttl_tail) = result
                        .split_once("ttl=")
                        .ok_or_else(|| fmt_err!("NetFunnel 대기 응답에 ttl 없음: {result}"))?;
                    let ttl = parse_netfunnel_u32(
                        split_head_or_all(ttl_tail, '&'),
                        "NetFunnel ttl 파싱 실패",
                    )?;
                    current_ttl = Some(ttl);
                    current_key = Some(take_netfunnel_key(result)?);
                    sleep(Duration::from_secs(u64::from(ttl.clamp(1, 30))));
                }
                302 => bail_fmt!("NetFunnel IP 차단: {result}"),
                _ => bail_fmt!("NetFunnel 응답 오류: {result}"),
            }
        }
        Err("NetFunnel 대기 횟수를 초과했습니다.".into())
    }
    fn post_form(
        &mut self,
        path: &str,
        form: &[(&str, &str)],
        requested_with: bool,
    ) -> DownloadResult<Vec<u8>> {
        let mut body = mem::take(&mut self.form_body_buffer);
        let result = (|| {
            let required_capacity = form.iter().fold(0_usize, |sum, &(name, value)| {
                sum.strict_add(usize::from(sum != 0))
                    .strict_add(name.len().strict_add(value.len()).strict_mul(3))
                    .strict_add(1)
            });
            body.clear();
            map_reserve(
                body.try_reserve_exact(required_capacity),
                "HTTP form body 메모리 확보 실패",
            )?;
            for (index, &(name, value)) in form.iter().enumerate() {
                if index != 0 {
                    body.push('&');
                }
                Self::push_percent_encoded(&mut body, name.as_bytes());
                body.push('=');
                Self::push_percent_encoded(&mut body, value.as_bytes());
            }
            self.send_request(
                (HttpHost::Opinet, OPINET_HOST, path),
                Some(body.as_bytes()),
                "text/html, */*; q=0.01",
                Some("application/x-www-form-urlencoded; charset=UTF-8"),
                Some(OPDOWNLOAD_URL),
                requested_with,
            )
        })();
        self.form_body_buffer = body;
        result
    }
    fn push_percent_encoded(out: &mut String, bytes: &[u8]) {
        for &byte in bytes {
            match byte {
                literal
                    if literal.is_ascii_alphanumeric()
                        || matches!(literal, b'-' | b'_' | b'.' | b'~') =>
                {
                    out.push(char::from(literal));
                }
                b' ' => out.push('+'),
                other => {
                    out.push('%');
                    out.push(char::from(hex_digit(other >> 4_u8)));
                    out.push(char::from(hex_digit(other & 0x0F)));
                }
            }
        }
    }
    pub(crate) fn refresh_source(mut self) -> DownloadResult<Vec<u8>> {
        (|| -> DownloadResult<Vec<u8>> {
            let body = self.send_request(
                (HttpHost::Opinet, OPINET_HOST, OPDOWNLOAD_PATH),
                None,
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                None,
                None,
                false,
            )?;
            let opdownload_page = String::from_utf8(body).map_err(|source| {
                download_error_with_source("HTTP 응답 UTF-8 변환 실패", source)
            })?;
            let opinet_key = {
                let (_, after_marker) = opdownload_page
                    .split_once("opinet_key.value")
                    .ok_or("Opinet 다운로드 페이지에서 key 할당 구문을 찾지 못했습니다.")?;
                let after_eq = after_marker
                    .split_once('=')
                    .ok_or("Opinet key 할당 구문의 '=' 문자를 찾지 못했습니다.")?
                    .1
                    .trim_ascii_start();
                let (quote, value_tail) = if let Some(tail) = after_eq.strip_prefix('\'') {
                    ('\'', tail)
                } else if let Some(tail) = after_eq.strip_prefix('"') {
                    ('"', tail)
                } else {
                    return Err("Opinet key 값 quote 문자를 찾지 못했습니다.".into());
                };
                let (value, _) = value_tail
                    .split_once(quote)
                    .ok_or("Opinet key 값 종료 quote를 찾지 못했습니다.")?;
                ensure(!value.is_empty(), "Opinet key 값이 비어 있습니다.")?;
                value
            };
            let mut opinet_key_owned =
                try_string_with_capacity(opinet_key.len(), "Opinet key 메모리 확보 실패")?;
            opinet_key_owned.push_str(opinet_key);
            drop(opdownload_page);
            let entry_key = self.fetch_netfunnel_ticket("B1")?;
            self.post_form(
                OPDOWNLOAD_PATH,
                &[
                    ("netfunnel_key", entry_key.as_str()),
                    ("opinet_key", opinet_key_owned.as_str()),
                ],
                false,
            )?;
            self.post_form(
                "/user/main/main_move_price.do",
                &[("tarUrl", "/user/opdown/oil_price_download")],
                true,
            )?;
            let download_key = self.fetch_netfunnel_ticket("B7")?;
            let response = self.post_form(
                "/user/main/main_download_excel.do",
                &[
                    ("LPG_CD", "A"),
                    ("DATE_DIV_CD", ""),
                    ("PAGE_DIV", "PAGE_DIV_2"),
                    ("SIDO_NM", "선택하세요."),
                    ("SIGUN_NM", "선택하세요."),
                    ("API_GBN", "A"),
                    ("netfunnel_key", download_key.as_str()),
                ],
                false,
            )?;
            if !response.starts_with(&OLE2_SIGNATURE) {
                let preview = String::from_utf8_lossy(
                    response
                        .get(..HTTP_ERROR_PREVIEW_BYTES)
                        .unwrap_or(&response),
                );
                bail_fmt!("다운로드 응답이 예상한 OLE2.xls 파일이 아닙니다: {preview}");
            }
            Ok(response)
        })()
        .map_err(|mut error| {
            error.update_message(|message| format!("Opinet 자동 다운로드 실패: {message}"));
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
            path.extend(["/ts.wseq?opcode=", opcode]);
            if let Some(key_text) = key {
                path.push_str("&key=");
                Self::push_percent_encoded(&mut path, key_text.as_bytes());
            }
            path.extend(["&nfid=0&prefix=NetFunnel.gRtype%3D", opcode, "%3B"]);
            if let Some(ttl_secs) = ttl {
                path.extend(["&ttl=", ttl_secs.format_into(&mut NumBuffer::new())]);
            }
            path.extend([
                "&sid=service_1&aid=",
                action_id,
                "&js=yes&",
                timestamp.format_into(&mut NumBuffer::new()),
            ]);
            self.send_request(
                (HttpHost::Netfunnel, "nfl.opinet.co.kr", &path),
                None,
                "application/javascript,*/*;q=0.8",
                None,
                None,
                false,
            )
        };
        self.netfunnel_path_buffer = path;
        let mut text = String::from_utf8(response_result?).map_err(|source| {
            download_error_with_source("NetFunnel 응답 UTF-8 변환 실패", source)
        })?;
        let (value, _) = text
            .split_once("result='")
            .and_then(|(_, tail)| tail.split_once('\''))
            .ok_or_else(|| fmt_err!("NetFunnel result 파싱 실패: {text}"))?;
        let value_range = or_abort(text.substr_range(value));
        retain_substr(&mut text, value_range);
        Ok(text)
    }
    fn send_request(
        &mut self,
        (host, host_name, path): (HttpHost, &'static str, &str),
        request_body: Option<&[u8]>,
        accept: &'static str,
        content_type: Option<&'static str>,
        referer: Option<&str>,
        requested_with: bool,
    ) -> DownloadResult<Vec<u8>> {
        self.cookie_header_buffer.clear();
        let jar = match host {
            HttpHost::Opinet => &self.cookie_jars.0,
            HttpHost::Netfunnel => &self.cookie_jars.1,
        };
        let cookie = if jar.cookies.is_empty() {
            None
        } else {
            let capacity = jar.cookies.iter().fold(
                jar.cookies.len().strict_sub(1).strict_mul(2),
                |sum, item| sum.strict_add(item.len()),
            );
            map_reserve(
                self.cookie_header_buffer.try_reserve_exact(capacity),
                "Cookie header 메모리 확보 실패",
            )?;
            for (index, item) in jar.cookies.iter().enumerate() {
                if index != 0 {
                    self.cookie_header_buffer.push_str("; ");
                }
                self.cookie_header_buffer.push_str(item);
            }
            Some(self.cookie_header_buffer.as_str())
        };
        let HttpResponse {
            body,
            headers,
            status,
        } = self.platform.request(
            request_body,
            host_name,
            path,
            RequestHeaders {
                accept,
                content_type,
                cookie,
                referer,
                requested_with,
            },
        )?;
        if let Some(expected_len) = headers.content_length
            && body.len() != expected_len
        {
            bail_fmt!(
                "HTTP 응답 본문 길이가 Content-Length와 다릅니다: expected={expected_len}, actual={}",
                body.len()
            );
        }
        for value in &headers.set_cookies {
            let (cookie_name, cookie_value) = split_head_or_all(value, ';')
                .split_once('=')
                .ok_or_else(|| fmt_err!("HTTP Set-Cookie 형식이 올바르지 않습니다: {value}"))?;
            self.add_cookie_for_host(host, cookie_name.trim_ascii(), cookie_value.trim_ascii())?;
        }
        if !(200..300).contains(&status) {
            let body_preview =
                String::from_utf8_lossy(body.get(..HTTP_ERROR_PREVIEW_BYTES).unwrap_or(&body));
            bail_fmt!("HTTP {status}: {body_preview}");
        }
        Ok(body)
    }
}
const fn hex_digit(nibble: u8) -> u8 {
    b'0'.strict_add(nibble)
        .strict_add(nibble.strict_add(6).wrapping_shr(4).strict_mul(7))
}
fn parse_netfunnel_u32(value: &str, context: &'static str) -> DownloadResult<u32> {
    parse_unsigned_u32(
        value,
        format_args!("{context}: 음이 아닌 10진수 형식이 아닙니다."),
        format_args!("{context} 해석 실패"),
    )
}
fn retain_substr(text: &mut String, range: Range<usize>) {
    text.truncate(range.end);
    text.replace_range(..range.start, "");
}
fn split_head_or_all(value: &str, separator: char) -> &str {
    value.split_once(separator).map_or(value, |(head, _)| head)
}
fn take_netfunnel_key(mut result: String) -> DownloadResult<String> {
    let (_, value_tail) = result
        .split_once("key=")
        .ok_or_else(|| fmt_err!("NetFunnel key 없음: {result}"))?;
    let value = split_head_or_all(value_tail, '&');
    ensure_fmt!(!value.is_empty(), "NetFunnel key 비어 있음: {result}");
    let value_range = or_abort(result.substr_range(value));
    retain_substr(&mut result, value_range);
    Ok(result)
}
