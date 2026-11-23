use crate::diagnostic::{
    Result as DownloadResult, bail_fmt, ensure, ensure_fmt, err_src as download_error_with_source,
    fmt_err, or_abort, parse_unsigned_u32, push_vec, reserve, str_cap,
};
use core::{fmt::NumBuffer, mem, range::Range, time::Duration};
use std::{
    thread::sleep,
    time::{SystemTime, UNIX_EPOCH},
};
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
const ERR_PREVIEW: usize = 512;
const HTTP_MAX_BODY_BYTES: usize = 32 * 1024 * 1024;
const HTTP_MAX_HEADER_BYTES: usize = 256 * 1024;
const MAX_COOKIE_PAIR: usize = 4096;
const MAX_COOKIES: usize = 64;
const OLE2_SIG: [u8; 8] = [0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1];
const OPDOWN_PATH: &str = "/user/opdown/opDownload.do";
const OPDOWN_URL: &str = "https://www.opinet.co.kr/user/opdown/opDownload.do";
const OPINET_HOST: &str = "www.opinet.co.kr";
const USER_AGENT: &str = concat!("fcupdater/", env!("CARGO_PKG_VERSION"));
#[derive(Clone, Copy)]
enum HttpHost {
    Netfunnel,
    Opinet,
}
#[derive(Default)]
struct CookieJar {
    cookies: Vec<String>,
}
#[derive(Default)]
pub(super) struct SourceDownload {
    cookie_header_buffer: String,
    cookie_jars: (CookieJar, CookieJar),
    form_body_buffer: String,
    netfunnel_path_buffer: String,
    platform: PlatformHttpClient,
}
#[derive(Default)]
struct ResponseHeaders {
    content_length: Option<usize>,
    set_cookies: Vec<String>,
}
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
impl CookieJar {
    fn add_cookie(&mut self, name: &str, value: &str) -> DownloadResult<()> {
        ensure(
            !name.is_empty() && name.bytes().all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, b'!' | b'#'..=b'\'' | b'*'..=b'+' | b'-'..=b'.' | b'^'..=b'`' | b'|' | b'~')),
            "Cookie 이름에 허용되지 않는 문자 포함",
        )?;
        let body = if value.starts_with('"') {
            value
                .strip_circumfix('"', '"')
                .ok_or("Cookie 값 quote 형식 오류")?
        } else {
            value
        };
        ensure(
            body.bytes().all(
                |ch| matches!(ch, b'!' | b'#'..=b'+' | b'-'..=b':' | b'<'..=b'[' | b']'..=b'~'),
            ),
            "Cookie 값에 허용되지 않는 문자 포함",
        )?;
        let pair_len = name.len().strict_add(1).strict_add(value.len());
        ensure_fmt!(
            pair_len <= MAX_COOKIE_PAIR,
            "Cookie 길이 한도({MAX_COOKIE_PAIR}) 초과"
        );
        if let Some(cookie) = self.cookies.iter_mut().find(|ck| {
            ck.strip_prefix(name)
                .is_some_and(|tail| tail.starts_with('='))
        }) {
            if pair_len > cookie.len() {
                reserve(
                    cookie.try_reserve_exact(pair_len.strict_sub(cookie.len())),
                    "Cookie 값",
                )?;
            }
            cookie.truncate(name.len().strict_add(1));
            cookie.push_str(value);
            return Ok(());
        }
        ensure_fmt!(
            self.cookies.len() < MAX_COOKIES,
            "Cookie 수 한도({MAX_COOKIES}) 초과"
        );
        let mut pair = str_cap(pair_len, "Cookie")?;
        pair.extend([name, "=", value]);
        push_vec(&mut self.cookies, pair, 1, "Cookie 목록")
    }
}
impl RequestHeaders<'_> {
    fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        [
            Some(("User-Agent", USER_AGENT)),
            Some(("Accept-Language", "ko-KR,ko;q=0.9,en-US;q=0.5,en;q=0.3")),
            Some(("Accept", self.accept)),
            self.content_type.map(|val| ("Content-Type", val)),
            self.requested_with
                .then_some(("X-Requested-With", "XMLHttpRequest")),
            self.referer.map(|val| ("Referer", val)),
            self.cookie.map(|val| ("Cookie", val)),
        ]
        .into_iter()
        .flatten()
    }
}
impl ResponseHeaders {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    fn parse_content_length(&mut self, raw_value: &str) -> DownloadResult<()> {
        let val = raw_value.trim_ascii();
        (!val.is_empty()).ok_or("HTTP Content-Length 형식 오류")?;
        let mut parsed = 0_usize;
        for byte in val.bytes() {
            byte.is_ascii_digit()
                .ok_or("HTTP Content-Length 형식 오류")?;
            if parsed <= HTTP_MAX_BODY_BYTES {
                parsed = parsed
                    .checked_mul(10)
                    .and_then(|sc| sc.checked_add(usize::from(byte.strict_sub(b'0'))))
                    .ok_or("HTTP Content-Length 해석 실패")?;
            }
        }
        self.set_content_length(parsed)
    }
    fn push_set_cookie(&mut self, value: &str) -> DownloadResult<()> {
        let mut owned = str_cap(value.len(), "HTTP Set-Cookie")?;
        owned.push_str(value);
        push_vec(&mut self.set_cookies, owned, 1, "HTTP Set-Cookie 목록")
    }
    fn set_content_length(&mut self, parsed: usize) -> DownloadResult<()> {
        ensure_fmt!(
            parsed <= HTTP_MAX_BODY_BYTES,
            "HTTP Content-Length 한도({HTTP_MAX_BODY_BYTES}) 초과"
        );
        ensure(
            self.content_length.is_none_or(|prev| prev == parsed),
            "HTTP Content-Length 불일치",
        )?;
        self.content_length = Some(parsed);
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
        match host {
            HttpHost::Opinet => self.cookie_jars.0.add_cookie(name, value),
            HttpHost::Netfunnel => self.cookie_jars.1.add_cookie(name, value),
        }
    }
    fn fetch_netfunnel_ticket(&mut self, action_id: &str) -> DownloadResult<String> {
        let (mut cur_key, mut cur_ttl): (Option<String>, Option<u32>) = (None, None);
        for _ in 0_usize..20_usize {
            let res = self.request_netfunnel(action_id, cur_key.as_deref(), cur_ttl)?;
            self.add_cookie_for_host(HttpHost::Netfunnel, "NetFunnel_ID", &res)?;
            self.add_cookie_for_host(HttpHost::Opinet, "NetFunnel_ID", &res)?;
            let (_, tail) = res
                .split_once(':')
                .ok_or_else(|| fmt_err!("NetFunnel 코드 없음: {res}"))?;
            match parse_nf_u32(split_head(tail, ':'), "NetFunnel 코드 파싱 실패")? {
                200 | 300 | 303 => return take_nf_key(res),
                201 | 202 => {
                    let (_, ttl_tail) = res
                        .split_once("ttl=")
                        .ok_or_else(|| fmt_err!("NetFunnel ttl 없음: {res}"))?;
                    let ttl = parse_nf_u32(split_head(ttl_tail, '&'), "NetFunnel ttl 파싱 실패")?;
                    cur_ttl = Some(ttl);
                    cur_key = Some(take_nf_key(res)?);
                    sleep(Duration::from_secs(u64::from(ttl.clamp(1, 30))));
                }
                302 => bail_fmt!("NetFunnel IP 차단: {res}"),
                _ => bail_fmt!("NetFunnel 응답 오류: {res}"),
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
            let cap = form.iter().fold(0_usize, |sum, &(nm, val)| {
                sum.strict_add(usize::from(sum != 0))
                    .strict_add(nm.len().strict_add(val.len()).strict_mul(3))
                    .strict_add(1)
            });
            body.clear();
            reserve(body.try_reserve_exact(cap), "HTTP form body")?;
            for (idx, &(name, val)) in form.iter().enumerate() {
                if idx != 0 {
                    body.push('&');
                }
                Self::push_percent_encoded(&mut body, name.as_bytes());
                body.push('=');
                Self::push_percent_encoded(&mut body, val.as_bytes());
            }
            self.send_request(
                (HttpHost::Opinet, OPINET_HOST, path),
                Some(body.as_bytes()),
                "text/html, */*; q=0.01",
                Some("application/x-www-form-urlencoded; charset=UTF-8"),
                Some(OPDOWN_URL),
                requested_with,
            )
        })();
        self.form_body_buffer = body;
        result
    }
    fn push_percent_encoded(out: &mut String, bytes: &[u8]) {
        for &byte in bytes {
            match byte {
                lit if lit.is_ascii_alphanumeric() || matches!(lit, b'-' | b'_' | b'.' | b'~') => {
                    out.push(char::from(lit));
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
                (HttpHost::Opinet, OPINET_HOST, OPDOWN_PATH),
                None,
                "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                None,
                None,
                false,
            )?;
            let page = String::from_utf8(body)
                .map_err(|src| download_error_with_source("HTTP UTF-8 실패", src))?;
            let after_eq = page
                .split_once("opinet_key.value")
                .ok_or("Opinet key 할당 구문 없음")?
                .1
                .split_once('=')
                .ok_or("Opinet key '=' 없음")?
                .1
                .trim_ascii_start();
            let (quote, tail) = if let Some(rest) = after_eq.strip_prefix('\'') {
                ('\'', rest)
            } else if let Some(rest) = after_eq.strip_prefix('"') {
                ('"', rest)
            } else {
                return Err("Opinet key quote 없음".into());
            };
            let (opinet_key, _) = tail.split_once(quote).ok_or("Opinet key 종료 quote 없음")?;
            ensure(!opinet_key.is_empty(), "Opinet key 비어 있음")?;
            let mut key_owned = str_cap(opinet_key.len(), "Opinet key")?;
            key_owned.push_str(opinet_key);
            drop(page);
            let entry_key = self.fetch_netfunnel_ticket("B1")?;
            self.post_form(
                OPDOWN_PATH,
                &[("netfunnel_key", &entry_key), ("opinet_key", &key_owned)],
                false,
            )?;
            self.post_form(
                "/user/main/main_move_price.do",
                &[("tarUrl", "/user/opdown/oil_price_download")],
                true,
            )?;
            let dl_key = self.fetch_netfunnel_ticket("B7")?;
            let response = self.post_form(
                "/user/main/main_download_excel.do",
                &[
                    ("LPG_CD", "A"),
                    ("DATE_DIV_CD", ""),
                    ("PAGE_DIV", "PAGE_DIV_2"),
                    ("SIDO_NM", "선택하세요."),
                    ("SIGUN_NM", "선택하세요."),
                    ("API_GBN", "A"),
                    ("netfunnel_key", &dl_key),
                ],
                false,
            )?;
            if !response.starts_with(&OLE2_SIG) {
                let preview =
                    String::from_utf8_lossy(response.get(..ERR_PREVIEW).unwrap_or(&response));
                bail_fmt!("다운로드 응답이 OLE2.xls 파일이 아닙니다: {preview}");
            }
            Ok(response)
        })()
        .map_err(|mut err_val| {
            err_val.update_message(|msg| format!("Opinet 자동 다운로드 실패: {msg}"));
            err_val
        })
    }
    fn request_netfunnel(
        &mut self,
        action_id: &str,
        key: Option<&str>,
        ttl: Option<u32>,
    ) -> DownloadResult<String> {
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|dur| dur.as_millis())
            .map_err(|src| download_error_with_source("현재 시간 조회 실패", src))?;
        let opcode = key.map_or("5101", |_| "5002");
        let mut path = mem::take(&mut self.netfunnel_path_buffer);
        let res = {
            path.clear();
            path.extend(["/ts.wseq?opcode=", opcode]);
            if let Some(key_text) = key {
                path.push_str("&key=");
                Self::push_percent_encoded(&mut path, key_text.as_bytes());
            }
            path.extend(["&nfid=0&prefix=NetFunnel.gRtype%3D", opcode, "%3B"]);
            if let Some(secs) = ttl {
                path.extend(["&ttl=", secs.format_into(&mut NumBuffer::new())]);
            }
            path.extend([
                "&sid=service_1&aid=",
                action_id,
                "&js=yes&",
                ts.format_into(&mut NumBuffer::new()),
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
        let mut text = String::from_utf8(res?)
            .map_err(|src| download_error_with_source("NetFunnel UTF-8 실패", src))?;
        let (val, _) = text
            .split_once("result='")
            .and_then(|(_, tail)| tail.split_once('\''))
            .ok_or_else(|| fmt_err!("NetFunnel result 파싱 실패: {text}"))?;
        let range = or_abort(text.substr_range(val));
        retain_substr(&mut text, range);
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
            let cap = jar.cookies.iter().fold(
                jar.cookies.len().strict_sub(1).strict_mul(2),
                |sum, item| sum.strict_add(item.len()),
            );
            reserve(
                self.cookie_header_buffer.try_reserve_exact(cap),
                "Cookie header",
            )?;
            for (idx, item) in jar.cookies.iter().enumerate() {
                if idx != 0 {
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
        if let Some(exp) = headers.content_length
            && body.len() != exp
        {
            bail_fmt!(
                "HTTP Content-Length 불일치: expected={exp}, actual={}",
                body.len()
            );
        }
        for val in &headers.set_cookies {
            let (name, ck_val) = split_head(val, ';')
                .split_once('=')
                .ok_or_else(|| fmt_err!("HTTP Set-Cookie 형식 오류: {val}"))?;
            self.add_cookie_for_host(host, name.trim_ascii(), ck_val.trim_ascii())?;
        }
        if !(200..300).contains(&status) {
            let preview = String::from_utf8_lossy(body.get(..ERR_PREVIEW).unwrap_or(&body));
            bail_fmt!("HTTP {status}: {preview}");
        }
        Ok(body)
    }
}
fn checked_http_buffer_len(
    label: &str,
    cur: usize,
    add: usize,
    limit: usize,
) -> DownloadResult<usize> {
    let next = cur
        .checked_add(add)
        .ok_or_else(|| fmt_err!("HTTP {label} 크기 계산 실패"))?;
    ensure_fmt!(next <= limit, "HTTP {label} 크기 한도({limit}) 초과");
    Ok(next)
}
const fn hex_digit(nibble: u8) -> u8 {
    b'0'.strict_add(nibble)
        .strict_add(nibble.strict_add(6).wrapping_shr(4).strict_mul(7))
}
fn parse_nf_u32(value: &str, context: &'static str) -> DownloadResult<u32> {
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
fn split_head(value: &str, sep: char) -> &str {
    value.split_once(sep).map_or(value, |(head, _)| head)
}
fn take_nf_key(mut result: String) -> DownloadResult<String> {
    let (_, tail) = result
        .split_once("key=")
        .ok_or_else(|| fmt_err!("NetFunnel key 없음: {result}"))?;
    let val = split_head(tail, '&');
    ensure_fmt!(!val.is_empty(), "NetFunnel key 비어 있음: {result}");
    let range = or_abort(result.substr_range(val));
    retain_substr(&mut result, range);
    Ok(result)
}
