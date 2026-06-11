use super::{
    DownloadResult, HTTP_MAX_BODY_BYTES, HttpHeader, HttpResponse, HttpStreamResponse,
    NETFUNNEL_HOST, NETFUNNEL_POLL_LIMIT, NETFUNNEL_SERVICE_ID, ReservedDownloadFile, USER_AGENT,
    attach_remove_file_error, download_error_with_source, enforce_http_content_length_limit,
};
use crate::diagnostic::{path_context_message, prefixed_message};
use alloc::{string::String, vec::Vec};
use core::{fmt::Write as FmtWrite, time::Duration};
use std::{
    io::Write as IoWrite,
    path::PathBuf,
    thread::sleep,
    time::{SystemTime, UNIX_EPOCH},
};
const U32_DECIMAL_MAX_LEN: usize = 10;
const U128_DECIMAL_MAX_LEN: usize = 39;
cfg_select! {
    windows => {
        type PlatformHttpClient = super::winhttp::Client;
    }
    any(target_os = "linux", target_os = "macos") => {
        type PlatformHttpClient = super::libcurl::Client;
    }
    _ => {
        #[derive(Default)]
        struct PlatformHttpClient;
    }
}
#[derive(Default)]
pub(super) struct HttpClient {
    cookie_header_cache: Option<String>,
    cookie_header_dirty: bool,
    cookies: Vec<Cookie>,
    platform: PlatformHttpClient,
}
#[derive(Clone, Copy)]
pub(super) enum PostHeaderProfile {
    Ajax,
    Standard,
}
pub(super) struct DownloadedFileResponse {
    pub path: PathBuf,
    pub response: HttpStreamResponse,
}
struct Cookie {
    name: String,
    value: String,
}
impl HttpClient {
    fn add_cookie(&mut self, name: &str, value: &str) -> DownloadResult<()> {
        if let Some(cookie) = self.cookies.iter_mut().find(|cookie| cookie.name == name) {
            cookie.value.clear();
            cookie.value.try_reserve(value.len()).map_err(|source| {
                download_error_with_source("Cookie 값 메모리 확보 실패", source)
            })?;
            cookie.value.push_str(value);
            self.cookie_header_dirty = true;
            return Ok(());
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
            .try_reserve(name.len())
            .map_err(|source| download_error_with_source("Cookie 이름 메모리 확보 실패", source))?;
        cookie
            .value
            .try_reserve(value.len())
            .map_err(|source| download_error_with_source("Cookie 값 메모리 확보 실패", source))?;
        cookie.name.push_str(name);
        cookie.value.push_str(value);
        self.cookies.push(cookie);
        self.cookie_header_dirty = true;
        Ok(())
    }
    fn encoded_form_body(form: &[(&str, &str)]) -> DownloadResult<String> {
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
        let mut body = String::new();
        body.try_reserve(body_capacity.ok_or("HTTP form body 메모리 용량 계산 실패")?)
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
        Ok(body)
    }
    fn extract_netfunnel_key(result: &str) -> DownloadResult<String> {
        let Some((_, tail)) = result.split_once("key=") else {
            return Err(prefixed_message("NetFunnel key 없음: ", result).into());
        };
        let value = split_head_or_all(tail, '&');
        if value.is_empty() {
            return Err(prefixed_message("NetFunnel key 비어 있음: ", result).into());
        }
        let mut out = String::new();
        out.try_reserve(value.len()).map_err(|source| {
            download_error_with_source("NetFunnel key 메모리 확보 실패", source)
        })?;
        out.push_str(value);
        Ok(out)
    }
    pub(super) fn fetch_netfunnel_ticket(&mut self, action_id: &str) -> DownloadResult<String> {
        let mut current_key: Option<String> = None;
        for _ in 0..NETFUNNEL_POLL_LIMIT {
            let result = self.request_netfunnel(action_id, current_key.as_deref(), None)?;
            self.add_cookie("NetFunnel_ID", &result)?;
            let Some((_opcode, code_tail)) = result.split_once(':') else {
                return Err(prefixed_message("NetFunnel 코드 없음: ", result).into());
            };
            let code_text = split_head_or_all(code_tail, ':');
            let code = code_text
                .parse::<u32>()
                .map_err(|source| download_error_with_source("NetFunnel 코드 파싱 실패", source))?;
            match code {
                200 | 300 | 303 => return Self::extract_netfunnel_key(&result),
                201 | 202 | 302 => {
                    current_key = Some(Self::extract_netfunnel_key(&result)?);
                    let wait_secs = result
                        .split_once("ttl=")
                        .map(|(_, tail)| split_head_or_all(tail, '&'))
                        .and_then(|ttl_text| ttl_text.parse::<u32>().ok())
                        .unwrap_or(1)
                        .clamp(1, 30);
                    sleep(Duration::from_secs(u64::from(wait_secs)));
                }
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
        let mut headers = Vec::new();
        headers.try_reserve(3).map_err(|source| {
            download_error_with_source("HTTP GET header 메모리 확보 실패", source)
        })?;
        headers.push((
            "Accept",
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        ));
        if let Some(referer_value) = referer {
            headers.push(("Referer", referer_value));
        }
        let response = self.request("GET", host, path, None, &headers)?;
        String::from_utf8(response.body)
            .map_err(|source| download_error_with_source("HTTP 응답 UTF-8 변환 실패", source))
    }
    fn percent_encoded_len(bytes: &[u8]) -> Option<usize> {
        bytes.iter().try_fold(0_usize, |sum, byte| {
            let byte_len = match *byte {
                unreserved
                    if unreserved.is_ascii_alphanumeric()
                        || matches!(unreserved, b'-' | b'_' | b'.' | b'~' | b' ') =>
                {
                    1
                }
                _ => 3,
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
        let body = Self::encoded_form_body(form)?;
        let headers = Self::post_headers(referer, profile)?;
        self.request("POST", host, path, Some(body.as_bytes()), &headers)
    }
    pub(super) fn post_form_to_file(
        &mut self,
        host: &str,
        path: &str,
        form: &[(&str, &str)],
        referer: Option<&str>,
        profile: PostHeaderProfile,
        target: ReservedDownloadFile,
    ) -> DownloadResult<DownloadedFileResponse> {
        let ReservedDownloadFile {
            mut file,
            path: target_path,
        } = target;
        let body = Self::encoded_form_body(form)?;
        let headers = Self::post_headers(referer, profile)?;
        let response = match self.request_to_writer(
            "POST",
            host,
            path,
            Some(body.as_bytes()),
            &headers,
            &mut file,
        ) {
            Ok(response) => response,
            Err(error) => {
                drop(file);
                return Err(attach_remove_file_error(error, &target_path));
            }
        };
        if let Err(source) = IoWrite::flush(&mut file) {
            drop(file);
            let error = download_error_with_source(
                path_context_message("다운로드 임시 파일 flush 실패", &target_path),
                source,
            );
            return Err(attach_remove_file_error(error, &target_path));
        }
        Ok(DownloadedFileResponse {
            path: target_path,
            response,
        })
    }
    fn post_headers(
        referer: Option<&str>,
        profile: PostHeaderProfile,
    ) -> DownloadResult<Vec<(&str, &str)>> {
        let mut headers = Vec::new();
        headers.try_reserve(6).map_err(|source| {
            download_error_with_source("HTTP POST header 메모리 확보 실패", source)
        })?;
        headers.extend_from_slice(&[
            (
                "Content-Type",
                "application/x-www-form-urlencoded; charset=UTF-8",
            ),
            ("Accept", "text/html, */*; q=0.01"),
        ]);
        if matches!(profile, PostHeaderProfile::Ajax) {
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
                    out.push(hex_digit(high));
                    out.push(hex_digit(low));
                }
            }
        }
    }
    fn refresh_cookie_header_cache(&mut self) -> DownloadResult<()> {
        if !self.cookie_header_dirty {
            return Ok(());
        }
        if self.cookies.is_empty() {
            self.cookie_header_cache = None;
            self.cookie_header_dirty = false;
            return Ok(());
        }
        let separator_capacity = self
            .cookies
            .len()
            .checked_sub(1)
            .and_then(|count| count.checked_mul(2))
            .ok_or("Cookie header 용량 계산 실패")?;
        let capacity = self
            .cookies
            .iter()
            .try_fold(separator_capacity, |sum, cookie| {
                sum.checked_add(cookie.name.len())?
                    .checked_add(1)?
                    .checked_add(cookie.value.len())
            })
            .ok_or("Cookie header 용량 계산 실패")?;
        let mut out = String::new();
        out.try_reserve(capacity).map_err(|source| {
            download_error_with_source("Cookie header 메모리 확보 실패", source)
        })?;
        for (index, cookie) in self.cookies.iter().enumerate() {
            if index != 0 {
                out.push_str("; ");
            }
            out.push_str(&cookie.name);
            out.push('=');
            out.push_str(&cookie.value);
        }
        self.cookie_header_cache = Some(out);
        self.cookie_header_dirty = false;
        Ok(())
    }
    fn request(
        &mut self,
        method: &str,
        host: &str,
        path: &str,
        body: Option<&[u8]>,
        headers: &[(&str, &str)],
    ) -> DownloadResult<HttpResponse> {
        self.refresh_cookie_header_cache()?;
        let cookie_header = self.cookie_header_cache.as_deref();
        let mut merged_headers = Vec::new();
        let merged_header_capacity = checked_capacity_sum(
            &[headers.len(), 2, usize::from(cookie_header.is_some())],
            "HTTP request header 용량 계산 실패",
        )?;
        merged_headers
            .try_reserve(merged_header_capacity)
            .map_err(|source| {
                download_error_with_source("HTTP request header 메모리 확보 실패", source)
            })?;
        merged_headers.push(("User-Agent", USER_AGENT));
        merged_headers.push(("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.5,en;q=0.3"));
        merged_headers.extend_from_slice(headers);
        if let Some(cookie_text) = cookie_header {
            merged_headers.push(("Cookie", cookie_text));
        }
        let response = {
            cfg_select! {
                windows => {
                    self.platform.request(method, host, path, body, &merged_headers)
                }
                any(target_os = "linux", target_os = "macos") => {
                    self.platform.request(method, host, path, body, &merged_headers)
                }
                _ => {
                    let body_len = body.map_or(0, <[u8]>::len);
                    let header_count = headers.len();
                    let merged_header_count = merged_headers.len();
                    Err(format!(
                        "외부 TLS 크레이트 없이 HTTPS 다운로드를 수행하려면 Windows WinHTTP 또는 Linux/macOS libcurl이 필요합니다. 요청: {method} https://{host}{path}, body={body_len} bytes, headers={header_count}/{merged_header_count}"
                    )
                    .into())
                }
            }
        }?;
        enforce_http_content_length_limit(&response.headers, HTTP_MAX_BODY_BYTES)?;
        self.store_response_cookies_from_headers(&response.headers)?;
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
        let opcode = if key.is_some() { "5002" } else { "5101" };
        let key_fragment = if let Some(key_value) = key {
            let encoded_capacity = Self::percent_encoded_len(key_value.as_bytes())
                .ok_or("NetFunnel key 인코딩 용량 계산 실패")?;
            let capacity = encoded_capacity
                .checked_add("&key=".len())
                .ok_or("NetFunnel key fragment 용량 계산 실패")?;
            let mut fragment = String::new();
            fragment.try_reserve(capacity).map_err(|source| {
                download_error_with_source("NetFunnel key fragment 메모리 확보 실패", source)
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
                    .ok_or("NetFunnel ttl fragment 용량 계산 실패")?;
                let mut fragment = String::new();
                fragment.try_reserve(capacity).map_err(|source| {
                    download_error_with_source("NetFunnel ttl fragment 메모리 확보 실패", source)
                })?;
                fragment.push_str("&ttl=");
                FmtWrite::write_fmt(&mut fragment, format_args!("{ttl_value}")).map_err(
                    |error| download_error_with_source("NetFunnel ttl fragment 작성 실패", error),
                )?;
                fragment
            }
            None => String::new(),
        };
        let path_capacity = checked_capacity_sum(
            &[
                "/ts.wseq?opcode=".len(),
                opcode.len(),
                key_fragment.len(),
                "&nfid=0&prefix=NetFunnel.gRtype%3D".len(),
                opcode.len(),
                "%3B".len(),
                ttl_fragment.len(),
                "&sid=".len(),
                NETFUNNEL_SERVICE_ID.len(),
                "&aid=".len(),
                action_id.len(),
                "&js=yes&".len(),
                U128_DECIMAL_MAX_LEN,
            ],
            "NetFunnel path 용량 계산 실패",
        )?;
        let mut path = String::new();
        path.try_reserve(path_capacity).map_err(|source| {
            download_error_with_source("NetFunnel path 메모리 확보 실패", source)
        })?;
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
        FmtWrite::write_fmt(&mut path, format_args!("{timestamp}")).map_err(|error| {
            download_error_with_source("NetFunnel timestamp fragment 작성 실패", error)
        })?;
        let response = self.request(
            "GET",
            NETFUNNEL_HOST,
            &path,
            None,
            &[("Accept", "application/javascript,*/*;q=0.8")],
        )?;
        let text = String::from_utf8(response.body).map_err(|source| {
            download_error_with_source("NetFunnel 응답 UTF-8 변환 실패", source)
        })?;
        let result = text
            .split_once("result='")
            .and_then(|(_, rest)| rest.split_once('\''))
            .map(|(value, _)| value);
        let Some(value) = result else {
            return Err(prefixed_message("NetFunnel result 파싱 실패: ", text).into());
        };
        let mut out = String::new();
        out.try_reserve(value.len()).map_err(|source| {
            download_error_with_source("NetFunnel result 메모리 확보 실패", source)
        })?;
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
    ) -> DownloadResult<HttpStreamResponse> {
        self.refresh_cookie_header_cache()?;
        let cookie_header = self.cookie_header_cache.as_deref();
        let mut merged_headers = Vec::new();
        let merged_header_capacity = checked_capacity_sum(
            &[headers.len(), 2, usize::from(cookie_header.is_some())],
            "HTTP request header 용량 계산 실패",
        )?;
        merged_headers
            .try_reserve(merged_header_capacity)
            .map_err(|source| {
                download_error_with_source("HTTP request header 메모리 확보 실패", source)
            })?;
        merged_headers.push(("User-Agent", USER_AGENT));
        merged_headers.push(("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.5,en;q=0.3"));
        merged_headers.extend_from_slice(headers);
        if let Some(cookie_text) = cookie_header {
            merged_headers.push(("Cookie", cookie_text));
        }
        let response = {
            cfg_select! {
                windows => {
                    self.platform.request_to_writer(
                        method,
                        host,
                        path,
                        body,
                        &merged_headers,
                        writer,
                    )
                }
                any(target_os = "linux", target_os = "macos") => {
                    self.platform.request_to_writer(
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
                    )
                    .into())
                }
            }
        }?;
        enforce_http_content_length_limit(&response.headers, HTTP_MAX_BODY_BYTES)?;
        self.store_response_cookies_from_headers(&response.headers)?;
        if !(200..300).contains(&response.status) {
            let body_preview = response.body.preview_lossy();
            let status = response.status;
            return Err(format!("HTTP {status}: {body_preview}").into());
        }
        Ok(response)
    }
    fn store_response_cookies_from_headers(
        &mut self,
        headers: &[HttpHeader],
    ) -> DownloadResult<()> {
        for (cookie_name, cookie_value) in headers
            .iter()
            .filter(|header| header.name.eq_ignore_ascii_case("set-cookie"))
            .filter_map(|header| split_head_or_all(&header.value, ';').split_once('='))
        {
            self.add_cookie(cookie_name.trim_ascii(), cookie_value.trim_ascii())?;
        }
        Ok(())
    }
}
fn checked_capacity_sum(parts: &[usize], context: &'static str) -> DownloadResult<usize> {
    let Some(capacity) = parts
        .iter()
        .try_fold(0_usize, |sum, &part| sum.checked_add(part))
    else {
        return Err(context.into());
    };
    Ok(capacity)
}
const fn hex_digit(nibble: u8) -> char {
    match nibble {
        0 => '0',
        1 => '1',
        2 => '2',
        3 => '3',
        4 => '4',
        5 => '5',
        6 => '6',
        7 => '7',
        8 => '8',
        9 => '9',
        10 => 'A',
        11 => 'B',
        12 => 'C',
        13 => 'D',
        14 => 'E',
        15 => 'F',
        _ => '?',
    }
}
fn split_head_or_all(value: &str, separator: char) -> &str {
    value.split_once(separator).map_or(value, |(head, _)| head)
}
