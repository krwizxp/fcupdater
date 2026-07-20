use super::{
    DownloadResult, HttpHeader, HttpHeaderKind, HttpResponse, HttpStreamResponse, NETFUNNEL_HOST,
    NETFUNNEL_POLL_LIMIT, NETFUNNEL_SERVICE_ID, OPINET_HOST, PlatformHttpClient,
    TemporarySourceFile, USER_AGENT, download_error_with_source, push_decimal_fragment,
};
use crate::diagnostic::prefixed_message;
use core::{mem, time::Duration};
use std::{
    io::Write as IoWrite,
    thread::sleep,
    time::{SystemTime, UNIX_EPOCH},
};
const HTTP_REQUEST_HEADER_CAPACITY: usize = 4;
const HTTP_MERGED_HEADER_CAPACITY: usize = 2 + HTTP_REQUEST_HEADER_CAPACITY + 1;
const MAX_COOKIE_PAIR_BYTES: usize = 4096;
const MAX_COOKIES_PER_HOST: usize = 64;
#[derive(Default)]
pub(super) struct HttpClient {
    cookie_header_buffer: String,
    cookie_jars: Vec<CookieJar>,
    form_body_buffer: String,
    netfunnel_path_buffer: String,
    platform: PlatformHttpClient,
}
#[derive(Clone, Copy)]
pub(super) enum PostHeaderProfile {
    Ajax,
    Standard,
}
#[derive(Clone, Copy)]
pub(super) enum HttpMethod<'body> {
    Get,
    Post(&'body [u8]),
}
struct HeaderStack<'header, const CAPACITY: usize> {
    headers: [(&'header str, &'header str); CAPACITY],
    len: usize,
}
struct Cookie {
    name: String,
    value: String,
}
struct CookieJar {
    cookies: Vec<Cookie>,
    host: String,
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
        if name.is_empty() || name.bytes().any(|byte| !is_http_token_byte(byte)) {
            return Err("HTTP header 이름에 허용되지 않는 문자가 포함되어 있습니다.".into());
        }
        if value
            .bytes()
            .any(|byte| byte.is_ascii_control() && byte != b'\t')
        {
            return Err("HTTP header 값에 허용되지 않는 제어 문자가 포함되어 있습니다.".into());
        }
        let Some(slot) = self.headers.get_mut(self.len) else {
            return Err("HTTP header stack 용량을 초과했습니다.".into());
        };
        *slot = (name, value);
        self.len = self
            .len
            .checked_add(1)
            .ok_or("HTTP header stack 길이 계산 실패")?;
        Ok(())
    }
}
impl CookieJar {
    fn add_cookie(&mut self, name: &str, value: &str) -> DownloadResult<()> {
        if name.is_empty() || name.bytes().any(|byte| !is_http_token_byte(byte)) {
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
        let pair_len = name
            .len()
            .checked_add(1)
            .and_then(|length| length.checked_add(value.len()))
            .ok_or("Cookie 이름과 값의 길이 계산 실패")?;
        if pair_len > MAX_COOKIE_PAIR_BYTES {
            return Err(format!(
                "Cookie 이름과 값이 허용 한도({MAX_COOKIE_PAIR_BYTES} bytes)를 초과했습니다."
            )
            .into());
        }
        if let Some(cookie) = self.cookies.iter_mut().find(|cookie| cookie.name == name) {
            if cookie.value.capacity() < value.len() {
                let additional = value
                    .len()
                    .checked_sub(cookie.value.len())
                    .ok_or("Cookie 값 메모리 용량 계산 실패")?;
                cookie
                    .value
                    .try_reserve_exact(additional)
                    .map_err(|source| {
                        download_error_with_source("Cookie 값 메모리 확보 실패", source)
                    })?;
            }
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
        if self.cookies.len() == self.cookies.capacity() {
            self.cookies.try_reserve(1).map_err(|source| {
                download_error_with_source("Cookie 목록 메모리 확보 실패", source)
            })?;
        }
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
impl HttpClient {
    fn add_cookie_for_host(&mut self, host: &str, name: &str, value: &str) -> DownloadResult<()> {
        if let Some(jar) = self.cookie_jars.iter_mut().find(|jar| jar.host == host) {
            return jar.add_cookie(name, value);
        }
        if self.cookie_jars.len() == self.cookie_jars.capacity() {
            self.cookie_jars.try_reserve(1).map_err(|source| {
                download_error_with_source("Cookie jar 목록 메모리 확보 실패", source)
            })?;
        }
        let mut jar = CookieJar {
            cookies: Vec::new(),
            host: String::new(),
        };
        jar.host.try_reserve_exact(host.len()).map_err(|source| {
            download_error_with_source("Cookie jar host 메모리 확보 실패", source)
        })?;
        jar.host.push_str(host);
        jar.add_cookie(name, value)?;
        self.cookie_jars.push(jar);
        Ok(())
    }
    fn encode_form_body_into(body: &mut String, form: &[(&str, &str)]) -> DownloadResult<()> {
        let body_capacity = form.iter().try_fold(0_usize, |sum, &(name, value)| {
            let encoded_capacity = Self::percent_encoded_len(name.as_bytes())?
                .checked_add(Self::percent_encoded_len(value.as_bytes())?)?
                .checked_add(1)?;
            let separated_capacity = if sum == 0 {
                encoded_capacity
            } else {
                encoded_capacity.checked_add(1)?
            };
            sum.checked_add(separated_capacity)
        });
        body.clear();
        let required_capacity = body_capacity.ok_or("HTTP form body 메모리 용량 계산 실패")?;
        if body.capacity() < required_capacity {
            body.try_reserve_exact(required_capacity)
                .map_err(|source| {
                    download_error_with_source("HTTP form body 메모리 확보 실패", source)
                })?;
        }
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
            self.add_cookie_for_host(NETFUNNEL_HOST, "NetFunnel_ID", &result)?;
            self.add_cookie_for_host(OPINET_HOST, "NetFunnel_ID", &result)?;
            let Some((_opcode, code_tail)) = result.split_once(':') else {
                return Err(prefixed_message("NetFunnel 코드 없음: ", result).into());
            };
            let code_text = split_head_or_all(code_tail, ':');
            let code = parse_netfunnel_u32(code_text, "NetFunnel 코드 파싱 실패")?;
            match code {
                200 | 300 | 303 => {
                    let (key_start, key_end) = netfunnel_key_range(&result)?;
                    return Ok(take_netfunnel_key(result, key_start, key_end));
                }
                201 | 202 => {
                    let (key_start, key_end) = netfunnel_key_range(&result)?;
                    current_ttl = result
                        .split_once("ttl=")
                        .map(|(_, ttl_tail)| {
                            let ttl_text = split_head_or_all(ttl_tail, '&');
                            parse_netfunnel_u32(ttl_text, "NetFunnel ttl 파싱 실패")
                        })
                        .transpose()?;
                    let wait_secs = current_ttl.unwrap_or(1).clamp(1, 30);
                    current_key = Some(take_netfunnel_key(result, key_start, key_end));
                    sleep(Duration::from_secs(u64::from(wait_secs)));
                }
                302 => return Err(prefixed_message("NetFunnel IP 차단: ", result).into()),
                _ => return Err(prefixed_message("NetFunnel 응답 오류: ", result).into()),
            }
        }
        Err("NetFunnel 대기 횟수를 초과했습니다.".into())
    }
    pub(super) fn get_text(
        &mut self,
        host: &str,
        path: &str,
        referer: Option<&str>,
    ) -> DownloadResult<String> {
        let mut headers = HeaderStack::<HTTP_REQUEST_HEADER_CAPACITY>::new();
        headers.push(
            "Accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        )?;
        if let Some(referer_value) = referer {
            headers.push("Referer", referer_value)?;
        }
        let response = self.request(HttpMethod::Get, host, path, headers.as_slice())?;
        String::from_utf8(response.body)
            .map_err(|source| download_error_with_source("HTTP 응답 UTF-8 변환 실패", source))
    }
    fn merged_headers<'request>(
        cookie_jars: &[CookieJar],
        cookie_header: &'request mut String,
        host: &str,
        headers: &[(&'request str, &'request str)],
    ) -> DownloadResult<HeaderStack<'request, HTTP_MERGED_HEADER_CAPACITY>> {
        cookie_header.clear();
        let cookie_text = if let Some(jar) = cookie_jars
            .iter()
            .find(|jar| jar.host == host && !jar.cookies.is_empty())
        {
            let separator_capacity = jar
                .cookies
                .len()
                .checked_sub(1)
                .and_then(|count| count.checked_mul(2))
                .ok_or("Cookie header 용량 계산 실패")?;
            let capacity = jar
                .cookies
                .iter()
                .try_fold(separator_capacity, |sum, cookie| {
                    sum.checked_add(cookie.name.len())?
                        .checked_add(1)?
                        .checked_add(cookie.value.len())
                })
                .ok_or("Cookie header 용량 계산 실패")?;
            if cookie_header.capacity() < capacity {
                cookie_header
                    .try_reserve_exact(capacity)
                    .map_err(|source| {
                        download_error_with_source("Cookie header 메모리 확보 실패", source)
                    })?;
            }
            for (index, cookie) in jar.cookies.iter().enumerate() {
                if index != 0 {
                    cookie_header.push_str("; ");
                }
                cookie_header.push_str(&cookie.name);
                cookie_header.push('=');
                cookie_header.push_str(&cookie.value);
            }
            Some(cookie_header.as_str())
        } else {
            None
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
    fn percent_encoded_len(bytes: &[u8]) -> Option<usize> {
        bytes.iter().try_fold(0_usize, |sum, byte| {
            let byte_len = if is_url_form_literal(*byte) || *byte == b' ' {
                1
            } else {
                3
            };
            sum.checked_add(byte_len)
        })
    }
    pub(super) fn post_form(
        &mut self,
        host: &str,
        path: &str,
        form: &[(&str, &str)],
        referer: Option<&str>,
        profile: PostHeaderProfile,
    ) -> DownloadResult<HttpResponse> {
        let mut body = mem::take(&mut self.form_body_buffer);
        let result = (|| {
            Self::encode_form_body_into(&mut body, form)?;
            let headers = Self::post_headers(referer, profile)?;
            self.request(
                HttpMethod::Post(body.as_bytes()),
                host,
                path,
                headers.as_slice(),
            )
        })();
        self.form_body_buffer = body;
        result
    }
    pub(super) fn post_form_to_file<F>(
        &mut self,
        host: &str,
        path: &str,
        form: &[(&str, &str)],
        referer: Option<&str>,
        profile: PostHeaderProfile,
        reserve_target: F,
    ) -> DownloadResult<(HttpStreamResponse, TemporarySourceFile)>
    where
        F: FnOnce() -> DownloadResult<TemporarySourceFile>,
    {
        let mut body = mem::take(&mut self.form_body_buffer);
        let result = (|| {
            Self::encode_form_body_into(&mut body, form)?;
            let headers = Self::post_headers(referer, profile)?;
            let mut target = reserve_target()?;
            let target_file = &mut target.file;
            let response = match self.request_to_writer(
                HttpMethod::Post(body.as_bytes()),
                host,
                path,
                headers.as_slice(),
                target_file,
            ) {
                Ok(response) => response,
                Err(error) => return Err(target.remove_after_error(error)),
            };
            Ok((response, target))
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
    fn request(
        &mut self,
        method: HttpMethod<'_>,
        host: &str,
        path: &str,
        headers: &[(&str, &str)],
    ) -> DownloadResult<HttpResponse> {
        let response: HttpResponse = {
            let merged_headers = Self::merged_headers(
                &self.cookie_jars,
                &mut self.cookie_header_buffer,
                host,
                headers,
            )?;
            let merged_header_slice = merged_headers.as_slice();
            self.platform
                .request(method, host, path, merged_header_slice)?
        };
        self.store_response_cookies_from_headers(host, &response.headers)?;
        if !(200..300).contains(&response.status) {
            let preview_len = response.body.len().min(512);
            let body_preview = match response.body.get(..preview_len) {
                Some(preview) => String::from_utf8_lossy(preview),
                None => String::from_utf8_lossy(&response.body),
            };
            let status = response.status;
            return Err(format!("HTTP {status}: {body_preview}").into());
        }
        Ok(response)
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
            self.request(
                HttpMethod::Get,
                NETFUNNEL_HOST,
                &path,
                &[("Accept", "application/javascript,*/*;q=0.8")],
            )
        };
        self.netfunnel_path_buffer = path;
        let response = response_result?;
        let mut text = String::from_utf8(response.body).map_err(|source| {
            download_error_with_source("NetFunnel 응답 UTF-8 변환 실패", source)
        })?;
        let Some(value_start) = text
            .find("result='")
            .and_then(|prefix_start| prefix_start.checked_add("result='".len()))
        else {
            return Err(prefixed_message("NetFunnel result 파싱 실패: ", text).into());
        };
        let Some(value_tail) = text.get(value_start..) else {
            return Err(prefixed_message("NetFunnel result 파싱 실패: ", text).into());
        };
        let Some(value_end) = value_tail
            .find('\'')
            .and_then(|value_len| value_start.checked_add(value_len))
        else {
            return Err(prefixed_message("NetFunnel result 파싱 실패: ", text).into());
        };
        text_range_to_owned_value(&mut text, value_start, value_end);
        Ok(text)
    }
    fn request_to_writer(
        &mut self,
        method: HttpMethod<'_>,
        host: &str,
        path: &str,
        headers: &[(&str, &str)],
        writer: &mut dyn IoWrite,
    ) -> DownloadResult<HttpStreamResponse> {
        let response = {
            let merged_headers = Self::merged_headers(
                &self.cookie_jars,
                &mut self.cookie_header_buffer,
                host,
                headers,
            )?;
            let merged_header_slice = merged_headers.as_slice();
            self.platform
                .request_to_writer(method, host, path, merged_header_slice, writer)?
        };
        self.store_response_cookies_from_headers(host, &response.headers)?;
        if !(200..300).contains(&response.status) {
            let body_preview = response.body.preview_lossy();
            let status = response.status;
            return Err(format!("HTTP {status}: {body_preview}").into());
        }
        Ok(response)
    }
    fn store_response_cookies_from_headers(
        &mut self,
        host: &str,
        headers: &[HttpHeader],
    ) -> DownloadResult<()> {
        for (cookie_name, cookie_value) in headers
            .iter()
            .filter(|header| header.kind == HttpHeaderKind::SetCookie)
            .filter_map(|header| split_head_or_all(&header.value, ';').split_once('='))
        {
            self.add_cookie_for_host(host, cookie_name.trim_ascii(), cookie_value.trim_ascii())?;
        }
        Ok(())
    }
}
const fn hex_digit(nibble: u8) -> u8 {
    if nibble < 10 {
        b'0'.wrapping_add(nibble)
    } else {
        b'A'.wrapping_add(nibble.saturating_sub(10))
    }
}
const fn is_http_token_byte(byte: u8) -> bool {
    byte.is_ascii_alphanumeric()
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
        )
}
const fn is_url_form_literal(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~')
}
fn netfunnel_key_range(result: &str) -> DownloadResult<(usize, usize)> {
    let Some(value_start) = result
        .find("key=")
        .and_then(|prefix_start| prefix_start.checked_add("key=".len()))
    else {
        return Err(prefixed_message("NetFunnel key 없음: ", result).into());
    };
    let Some(value_tail) = result.get(value_start..) else {
        return Err(prefixed_message("NetFunnel key 없음: ", result).into());
    };
    let value_len = value_tail.find('&').unwrap_or(value_tail.len());
    if value_len == 0 {
        return Err(prefixed_message("NetFunnel key 비어 있음: ", result).into());
    }
    let value_end = value_start
        .checked_add(value_len)
        .ok_or("NetFunnel key 범위 계산 실패")?;
    Ok((value_start, value_end))
}
fn take_netfunnel_key(mut result: String, value_start: usize, value_end: usize) -> String {
    text_range_to_owned_value(&mut result, value_start, value_end);
    result
}
fn split_head_or_all(value: &str, separator: char) -> &str {
    value.split_once(separator).map_or(value, |(head, _)| head)
}
fn text_range_to_owned_value(text: &mut String, value_start: usize, value_end: usize) {
    text.truncate(value_end);
    text.replace_range(..value_start, "");
}
fn parse_netfunnel_u32(value: &str, context: &'static str) -> DownloadResult<u32> {
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!("{context}: 음이 아닌 10진수 형식이 아닙니다.").into());
    }
    value
        .parse::<u32>()
        .map_err(|source| download_error_with_source(format!("{context} 해석 실패"), source))
}
