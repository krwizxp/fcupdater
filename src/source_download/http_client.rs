use super::{
    HTTP_MAX_BODY_BYTES, NETFUNNEL_HOST, NETFUNNEL_POLL_LIMIT, NETFUNNEL_SERVICE_ID,
    StreamedBodySummary, USER_AGENT, attach_remove_file_error, enforce_http_content_length_limit,
    lossy_prefix,
};
use crate::{path_source_message, prefixed_message};
use alloc::{string::String, vec::Vec};
use core::{fmt::Write as FmtWrite, result::Result as CoreResult, time::Duration};
use std::{
    fs::File,
    io::Write as IoWrite,
    path::Path,
    thread::sleep,
    time::{SystemTime, UNIX_EPOCH},
};
const U32_DECIMAL_MAX_LEN: usize = 10;
const U128_DECIMAL_MAX_LEN: usize = 39;
#[derive(Debug)]
pub(super) struct HttpResponse {
    pub body: Vec<u8>,
    pub headers: Vec<(String, String)>,
    pub status: u32,
}
#[derive(Debug)]
pub(super) struct HttpStreamResponse {
    pub body: StreamedBodySummary,
    pub headers: Vec<(String, String)>,
    pub status: u32,
}
#[derive(Default)]
pub(super) struct HttpClient {
    cookies: Vec<Cookie>,
}
#[derive(Clone)]
struct Cookie {
    name: String,
    value: String,
}
impl HttpClient {
    fn add_cookie(&mut self, name: &str, value: &str) -> CoreResult<(), String> {
        if let Some(cookie) = self.cookies.iter_mut().find(|cookie| cookie.name == name) {
            cookie.value.clear();
            cookie
                .value
                .try_reserve(value.len())
                .map_err(|source| prefixed_message("Cookie 값 메모리 확보 실패: ", source))?;
            cookie.value.push_str(value);
            return Ok(());
        }
        let mut cookie = Cookie {
            name: String::new(),
            value: String::new(),
        };
        cookie
            .name
            .try_reserve(name.len())
            .map_err(|source| prefixed_message("Cookie 이름 메모리 확보 실패: ", source))?;
        cookie
            .value
            .try_reserve(value.len())
            .map_err(|source| prefixed_message("Cookie 값 메모리 확보 실패: ", source))?;
        cookie.name.push_str(name);
        cookie.value.push_str(value);
        self.cookies
            .try_reserve(1)
            .map_err(|source| prefixed_message("Cookie 목록 메모리 확보 실패: ", source))?;
        self.cookies.push(cookie);
        Ok(())
    }
    fn cookie_header(&self) -> CoreResult<Option<String>, String> {
        if self.cookies.is_empty() {
            return Ok(None);
        }
        let capacity = self.cookies.iter().fold(
            self.cookies.len().saturating_sub(1).saturating_mul(2),
            |sum, cookie| {
                sum.saturating_add(cookie.name.len())
                    .saturating_add(1)
                    .saturating_add(cookie.value.len())
            },
        );
        let mut out = String::new();
        out.try_reserve(capacity)
            .map_err(|source| prefixed_message("Cookie header 메모리 확보 실패: ", source))?;
        for cookie in &self.cookies {
            if !out.is_empty() {
                out.push_str("; ");
            }
            out.push_str(&cookie.name);
            out.push('=');
            out.push_str(&cookie.value);
        }
        Ok(Some(out))
    }
    fn encoded_form_body(form: &[(&str, &str)]) -> CoreResult<String, String> {
        let body_capacity = form.iter().try_fold(0_usize, |sum, &(name, value)| {
            let encoded_capacity = name
                .len()
                .checked_add(value.len())?
                .checked_mul(3)?
                .checked_add(1)?;
            let separated_capacity = if sum == 0 {
                encoded_capacity
            } else {
                encoded_capacity.checked_add(1)?
            };
            sum.checked_add(separated_capacity)
        });
        let mut body = String::new();
        body.try_reserve(
            body_capacity.ok_or_else(|| String::from("HTTP form body 메모리 용량 계산 실패"))?,
        )
        .map_err(|source| prefixed_message("HTTP form body 메모리 확보 실패: ", source))?;
        for &(name, value) in form {
            if !body.is_empty() {
                body.push('&');
            }
            Self::push_percent_encoded(&mut body, name.as_bytes());
            body.push('=');
            Self::push_percent_encoded(&mut body, value.as_bytes());
        }
        Ok(body)
    }
    fn extract_netfunnel_key(result: &str) -> CoreResult<String, String> {
        let Some((_, tail)) = result.split_once("key=") else {
            return Err(prefixed_message("NetFunnel key 없음: ", result));
        };
        let value = split_head_or_all(tail, '&');
        if value.is_empty() {
            return Err(prefixed_message("NetFunnel key 비어 있음: ", result));
        }
        Ok(value.to_owned())
    }
    pub(super) fn fetch_netfunnel_ticket(&mut self, action_id: &str) -> CoreResult<String, String> {
        let mut current_key: Option<String> = None;
        for _ in 0..NETFUNNEL_POLL_LIMIT {
            let result = self.request_netfunnel(action_id, current_key.as_deref(), None)?;
            self.add_cookie("NetFunnel_ID", &result)?;
            let mut parts = result.split(':');
            if parts.next().is_none() {
                return Err(prefixed_message("NetFunnel opcode 없음: ", result));
            }
            let Some(code_text) = parts.next() else {
                return Err(prefixed_message("NetFunnel 코드 없음: ", result));
            };
            let code = code_text
                .parse::<u32>()
                .map_err(|source| prefixed_message("NetFunnel 코드 파싱 실패: ", source))?;
            if matches!(code, 200 | 300 | 303) {
                return Self::extract_netfunnel_key(&result);
            }
            if matches!(code, 201 | 202 | 302) {
                current_key = Some(Self::extract_netfunnel_key(&result)?);
                let wait_secs = result
                    .split_once("ttl=")
                    .map(|(_, tail)| split_head_or_all(tail, '&'))
                    .and_then(|ttl_text| ttl_text.parse::<u32>().ok())
                    .unwrap_or(1)
                    .clamp(1, 30);
                sleep(Duration::from_secs(u64::from(wait_secs)));
                continue;
            }
            return Err(prefixed_message("NetFunnel 응답 오류: ", result));
        }
        Err("NetFunnel 대기 횟수를 초과했습니다.".to_owned())
    }
    pub(super) fn get_text(
        &mut self,
        host: &str,
        path: &str,
        referer: Option<&str>,
    ) -> CoreResult<String, String> {
        let mut headers = Vec::new();
        headers
            .try_reserve(3)
            .map_err(|source| prefixed_message("HTTP GET header 메모리 확보 실패: ", source))?;
        headers.push((
            "Accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        ));
        if let Some(referer_value) = referer {
            headers.push(("Referer", referer_value));
        }
        let response = self.request("GET", host, path, None, &headers)?;
        String::from_utf8(response.body)
            .map_err(|source| prefixed_message("HTTP 응답 UTF-8 변환 실패: ", source))
    }
    pub(super) fn post_form(
        &mut self,
        host: &str,
        path: &str,
        form: &[(&str, &str)],
        referer: Option<&str>,
        ajax: bool,
    ) -> CoreResult<HttpResponse, String> {
        let body = Self::encoded_form_body(form)?;
        let headers = Self::post_headers(referer, ajax)?;
        self.request("POST", host, path, Some(body.as_bytes()), &headers)
    }
    pub(super) fn post_form_to_file(
        &mut self,
        host: &str,
        path: &str,
        form: &[(&str, &str)],
        referer: Option<&str>,
        ajax: bool,
        target: &Path,
    ) -> CoreResult<HttpStreamResponse, String> {
        let body = Self::encoded_form_body(form)?;
        let headers = Self::post_headers(referer, ajax)?;
        let mut file = File::create(target).map_err(|source| {
            path_source_message("다운로드 임시 파일 생성 실패", target, source)
        })?;
        let response = match self.request_to_writer(
            "POST",
            host,
            path,
            Some(body.as_bytes()),
            &headers,
            &mut file,
        ) {
            Ok(response) => response,
            Err(error_text) => {
                drop(file);
                return Err(attach_remove_file_error(error_text, target));
            }
        };
        if let Err(source) = IoWrite::flush(&mut file) {
            drop(file);
            let error_text = path_source_message("다운로드 임시 파일 flush 실패", target, source);
            return Err(attach_remove_file_error(error_text, target));
        }
        Ok(response)
    }
    fn post_headers(referer: Option<&str>, ajax: bool) -> CoreResult<Vec<(&str, &str)>, String> {
        let mut headers = Vec::new();
        headers
            .try_reserve(6)
            .map_err(|source| prefixed_message("HTTP POST header 메모리 확보 실패: ", source))?;
        headers.push((
            "Content-Type",
            "application/x-www-form-urlencoded; charset=UTF-8",
        ));
        headers.push(("Accept", "text/html, */*; q=0.01"));
        if ajax {
            headers.push(("X-Requested-With", "XMLHttpRequest"));
        }
        if let Some(referer_value) = referer {
            headers.push(("Referer", referer_value));
        }
        Ok(headers)
    }
    fn push_percent_encoded(out: &mut String, bytes: &[u8]) {
        for byte in bytes {
            match *byte {
                unreserved
                    if unreserved.is_ascii_alphanumeric()
                        || matches!(unreserved, b'-' | b'_' | b'.' | b'~') =>
                {
                    out.push(char::from(unreserved));
                }
                b' ' => out.push('+'),
                other => {
                    let high = other >> 4_u8;
                    let low = other & 0x0F;
                    out.push('%');
                    out.push(match high {
                        0..=9 => char::from(b'0'.saturating_add(high)),
                        10..=15 => char::from(b'A'.saturating_add(high.saturating_sub(10))),
                        _ => '?',
                    });
                    out.push(match low {
                        0..=9 => char::from(b'0'.saturating_add(low)),
                        10..=15 => char::from(b'A'.saturating_add(low.saturating_sub(10))),
                        _ => '?',
                    });
                }
            }
        }
    }
    fn request(
        &mut self,
        method: &str,
        host: &str,
        path: &str,
        body: Option<&[u8]>,
        headers: &[(&str, &str)],
    ) -> CoreResult<HttpResponse, String> {
        let mut merged_headers = Vec::new();
        let merged_header_capacity = headers.len().saturating_add(3);
        merged_headers
            .try_reserve(merged_header_capacity)
            .map_err(|source| prefixed_message("HTTP request header 메모리 확보 실패: ", source))?;
        merged_headers.push(("User-Agent", USER_AGENT));
        merged_headers.push(("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.5,en;q=0.3"));
        for header in headers {
            merged_headers.push(*header);
        }
        let cookie_header = self.cookie_header()?;
        if let Some(cookie_text) = cookie_header.as_deref() {
            merged_headers.push(("Cookie", cookie_text));
        }
        let response = {
            cfg_select! {
                windows => {
                    super::winhttp::CLIENT.request(method, host, path, body, &merged_headers)
                }
                any(target_os = "linux", target_os = "macos") => {
                    super::libcurl::CLIENT.request(method, host, path, body, &merged_headers)
                }
                _ => {
                    let body_len = body.map_or(0, <[u8]>::len);
                    let header_count = headers.len();
                    let merged_header_count = merged_headers.len();
                    Err(format!(
                        "외부 TLS 크레이트 없이 HTTPS 다운로드를 수행하려면 Windows WinHTTP 또는 Linux/macOS libcurl이 필요합니다. 요청: {method} https://{host}{path}, body={body_len} bytes, headers={header_count}/{merged_header_count}"
                    ))
                }
            }
        }?;
        enforce_http_content_length_limit(&response.headers, HTTP_MAX_BODY_BYTES)?;
        self.store_response_cookies(&response)?;
        if !(200..300).contains(&response.status) {
            let body_preview = lossy_prefix(&response.body, 512);
            let status = response.status;
            return Err(format!("HTTP {status}: {body_preview}"));
        }
        Ok(response)
    }
    fn request_netfunnel(
        &mut self,
        action_id: &str,
        key: Option<&str>,
        ttl: Option<u32>,
    ) -> CoreResult<String, String> {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis())
            .map_err(|source| prefixed_message("현재 시간 조회 실패: ", source))?;
        let opcode = if key.is_some() { "5002" } else { "5101" };
        let key_fragment = if let Some(key_value) = key {
            let encoded_capacity = key_value
                .len()
                .checked_mul(3)
                .ok_or_else(|| "NetFunnel key 인코딩 용량 계산 실패".to_owned())?;
            let capacity = encoded_capacity
                .checked_add("&key=".len())
                .ok_or_else(|| "NetFunnel key fragment 용량 계산 실패".to_owned())?;
            let mut fragment = String::new();
            fragment.try_reserve(capacity).map_err(|source| {
                prefixed_message("NetFunnel key fragment 메모리 확보 실패: ", source)
            })?;
            fragment.push_str("&key=");
            Self::push_percent_encoded(&mut fragment, key_value.as_bytes());
            fragment
        } else {
            String::new()
        };
        let ttl_fragment = match ttl {
            Some(ttl_value) => {
                let capacity = U32_DECIMAL_MAX_LEN
                    .checked_add("&ttl=".len())
                    .ok_or_else(|| "NetFunnel ttl fragment 용량 계산 실패".to_owned())?;
                let mut fragment = String::new();
                fragment.try_reserve(capacity).map_err(|source| {
                    prefixed_message("NetFunnel ttl fragment 메모리 확보 실패: ", source)
                })?;
                fragment.push_str("&ttl=");
                FmtWrite::write_fmt(&mut fragment, format_args!("{ttl_value}"))
                    .map_err(|error| format!("NetFunnel ttl fragment 작성 실패: {error}"))?;
                fragment
            }
            None => String::new(),
        };
        let path_capacity = "/ts.wseq?opcode="
            .len()
            .saturating_add(opcode.len())
            .saturating_add(key_fragment.len())
            .saturating_add("&nfid=0&prefix=NetFunnel.gRtype%3D".len())
            .saturating_add(opcode.len())
            .saturating_add("%3B".len())
            .saturating_add(ttl_fragment.len())
            .saturating_add("&sid=".len())
            .saturating_add(NETFUNNEL_SERVICE_ID.len())
            .saturating_add("&aid=".len())
            .saturating_add(action_id.len())
            .saturating_add("&js=yes&".len())
            .saturating_add(U128_DECIMAL_MAX_LEN);
        let mut path = String::new();
        path.try_reserve(path_capacity)
            .map_err(|source| prefixed_message("NetFunnel path 메모리 확보 실패: ", source))?;
        path.push_str("/ts.wseq?opcode=");
        path.push_str(opcode);
        path.push_str(&key_fragment);
        path.push_str("&nfid=0&prefix=NetFunnel.gRtype%3D");
        path.push_str(opcode);
        path.push_str("%3B");
        path.push_str(&ttl_fragment);
        path.push_str("&sid=");
        path.push_str(NETFUNNEL_SERVICE_ID);
        path.push_str("&aid=");
        path.push_str(action_id);
        path.push_str("&js=yes&");
        FmtWrite::write_fmt(&mut path, format_args!("{timestamp}"))
            .map_err(|error| format!("NetFunnel timestamp fragment 작성 실패: {error}"))?;
        let response = self.request(
            "GET",
            NETFUNNEL_HOST,
            &path,
            None,
            &[("Accept", "application/javascript,*/*;q=0.8")],
        )?;
        let text = String::from_utf8(response.body)
            .map_err(|source| prefixed_message("NetFunnel 응답 UTF-8 변환 실패: ", source))?;
        let result = text
            .split_once("result='")
            .and_then(|(_, rest)| rest.split_once('\''))
            .map(|(value, _)| value);
        let Some(value) = result else {
            return Err(prefixed_message("NetFunnel result 파싱 실패: ", text));
        };
        let mut out = String::new();
        out.try_reserve(value.len())
            .map_err(|source| prefixed_message("NetFunnel result 메모리 확보 실패: ", source))?;
        out.push_str(value);
        Ok(out)
    }
    fn request_to_writer(
        &mut self,
        method: &str,
        host: &str,
        path: &str,
        body: Option<&[u8]>,
        headers: &[(&str, &str)],
        writer: &mut dyn IoWrite,
    ) -> CoreResult<HttpStreamResponse, String> {
        let mut merged_headers = Vec::new();
        let merged_header_capacity = headers.len().saturating_add(3);
        merged_headers
            .try_reserve(merged_header_capacity)
            .map_err(|source| prefixed_message("HTTP request header 메모리 확보 실패: ", source))?;
        merged_headers.push(("User-Agent", USER_AGENT));
        merged_headers.push(("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.5,en;q=0.3"));
        for header in headers {
            merged_headers.push(*header);
        }
        let cookie_header = self.cookie_header()?;
        if let Some(cookie_text) = cookie_header.as_deref() {
            merged_headers.push(("Cookie", cookie_text));
        }
        let response = {
            cfg_select! {
                windows => {
                    super::winhttp::CLIENT.request_to_writer(
                        method,
                        host,
                        path,
                        body,
                        &merged_headers,
                        writer,
                    )
                }
                any(target_os = "linux", target_os = "macos") => {
                    super::libcurl::CLIENT.request_to_writer(
                        method,
                        host,
                        path,
                        body,
                        &merged_headers,
                        writer,
                    )
                }
                _ => {
                    let body_len = body.map_or(0, <[u8]>::len);
                    let header_count = headers.len();
                    let merged_header_count = merged_headers.len();
                    let writer_type = core::any::type_name_of_val(writer);
                    Err(format!(
                        "외부 TLS 크레이트 없이 HTTPS 다운로드를 수행하려면 Windows WinHTTP 또는 Linux/macOS libcurl이 필요합니다. 요청: {method} https://{host}{path}, body={body_len} bytes, headers={header_count}/{merged_header_count}, writer={writer_type}"
                    ))
                }
            }
        }?;
        enforce_http_content_length_limit(&response.headers, HTTP_MAX_BODY_BYTES)?;
        self.store_response_cookies_from_headers(&response.headers)?;
        if !(200..300).contains(&response.status) {
            let body_preview = response.body.preview_lossy();
            let status = response.status;
            return Err(format!("HTTP {status}: {body_preview}"));
        }
        Ok(response)
    }
    fn store_response_cookies(&mut self, response: &HttpResponse) -> CoreResult<(), String> {
        self.store_response_cookies_from_headers(&response.headers)
    }
    fn store_response_cookies_from_headers(
        &mut self,
        headers: &[(String, String)],
    ) -> CoreResult<(), String> {
        for header in headers {
            let name = &header.0;
            let value = &header.1;
            if !name.eq_ignore_ascii_case("set-cookie") {
                continue;
            }
            let cookie_pair = split_head_or_all(value, ';');
            let Some((cookie_name, cookie_value)) = cookie_pair.split_once('=') else {
                continue;
            };
            self.add_cookie(cookie_name.trim_ascii(), cookie_value.trim_ascii())?;
        }
        Ok(())
    }
}
fn split_head_or_all(value: &str, separator: char) -> &str {
    value.split_once(separator).map_or(value, |(head, _)| head)
}
