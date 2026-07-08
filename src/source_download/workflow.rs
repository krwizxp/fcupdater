use super::{
    CURRENT_PRICE_PAGE_DIV, DEFAULT_REGION_LABEL, DownloadResult, GAS_STATION_API_GBN,
    GAS_STATION_LPG_CODE, NETFUNNEL_DOWNLOAD_ACTION_ID, NETFUNNEL_ENTRY_ACTION_ID,
    OIL_PRICE_DOWNLOAD_TAR_URL, OLE2_SIGNATURE, OPDOWNLOAD_EXCEL_PATH, OPDOWNLOAD_LAYOUT_PATH,
    OPDOWNLOAD_PATH, OPDOWNLOAD_URL, OPINET_HOST, ReservedDownloadFile, SourceDownload,
    attach_remove_file_error, download_error_with_source,
    http_client::{self, PostHeaderProfile},
    push_decimal_fragment,
};
use crate::diagnostic::{Result, err, err_with_source, path_context_message, prefixed_message};
use core::time::Duration;
use std::{
    fs::{self, File},
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
    process, thread,
    time::{SystemTime, UNIX_EPOCH},
};
const AUTO_SOURCE_OLD_TEMP_FILE_NAME: &str = "fcupdater-opinet-source.tmp";
const AUTO_SOURCE_TEMP_FILE_PREFIX: &str = ".fcupdater-opinet-source.tmp_";
const AUTO_SOURCE_TEMP_FILE_NAME_CAPACITY: usize = 128;
const AUTO_SOURCE_TEMP_FILE_RESERVATION_ATTEMPTS: u32 = 1024;
struct SourceDownloadWorkflow<'dir, 'out, W: Write + ?Sized> {
    dir: &'dir Path,
    out: &'out mut W,
}
struct DownloadNetFunnelKey(String);
struct EntryNetFunnelKey(String);
struct OpinetKey<'page>(&'page str);
impl DownloadNetFunnelKey {
    const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}
impl EntryNetFunnelKey {
    const fn as_str(&self) -> &str {
        self.0.as_str()
    }
}
impl OpinetKey<'_> {
    const fn as_str(&self) -> &str {
        self.0
    }
}
impl<W> SourceDownload<'_, '_, W>
where
    W: Write + ?Sized,
{
    pub fn refresh_source(&mut self) -> Result<PathBuf> {
        fs::create_dir_all(self.dir).map_err(|source_err| {
            err_with_source(
                path_context_message("소스 폴더 생성 실패", self.dir),
                source_err,
            )
        })?;
        SourceDownloadWorkflow {
            dir: self.dir,
            out: &mut *self.out,
        }
        .refresh_source()
    }
}
impl<W> SourceDownloadWorkflow<'_, '_, W>
where
    W: Write + ?Sized,
{
    fn download_nationwide_source_http(&self) -> DownloadResult<PathBuf> {
        let mut client = http_client::HttpClient::default();
        let opdownload_page = client.get_text(OPINET_HOST, OPDOWNLOAD_PATH, None)?;
        let opinet_key = {
            const KEY_ASSIGNMENT_MARKER: &str = "opinet_key.value";
            let Some(marker_start) = opdownload_page.find(KEY_ASSIGNMENT_MARKER) else {
                return Err("Opinet 다운로드 페이지에서 key 할당 구문을 찾지 못했습니다.".into());
            };
            let marker_end = marker_start
                .checked_add(KEY_ASSIGNMENT_MARKER.len())
                .ok_or("Opinet key 할당 구문 위치 계산에 실패했습니다.")?;
            let after_marker = opdownload_page
                .get(marker_end..)
                .ok_or("Opinet key 할당 구문 범위가 손상되었습니다.")?;
            let Some(eq_rel) = after_marker.find('=') else {
                return Err("Opinet key 할당 구문의 '=' 문자를 찾지 못했습니다.".into());
            };
            let after_eq_start = eq_rel
                .checked_add(1)
                .ok_or("Opinet key 값 시작 위치 계산에 실패했습니다.")?;
            let after_eq = after_marker
                .get(after_eq_start..)
                .ok_or("Opinet key 값 시작 범위가 손상되었습니다.")?
                .trim_ascii_start();
            let Some(quote) = after_eq
                .as_bytes()
                .first()
                .copied()
                .filter(|byte| matches!(*byte, b'\'' | b'"'))
            else {
                return Err("Opinet key 값 quote 문자를 찾지 못했습니다.".into());
            };
            let value_tail = after_eq
                .strip_prefix(char::from(quote))
                .ok_or("Opinet key 값 범위가 손상되었습니다.")?;
            let Some(value_end) = value_tail.find(char::from(quote)) else {
                return Err("Opinet key 값 종료 quote를 찾지 못했습니다.".into());
            };
            let value = value_tail
                .get(..value_end)
                .ok_or("Opinet key 값 범위가 손상되었습니다.")?;
            if value.is_empty() {
                return Err("Opinet key 값이 비어 있습니다.".into());
            }
            OpinetKey(value)
        };
        let entry_key =
            EntryNetFunnelKey(client.fetch_netfunnel_ticket(NETFUNNEL_ENTRY_ACTION_ID)?);
        client.post_form(
            OPINET_HOST,
            OPDOWNLOAD_PATH,
            &[
                ("netfunnel_key", entry_key.as_str()),
                ("opinet_key", opinet_key.as_str()),
            ],
            Some(OPDOWNLOAD_URL),
            PostHeaderProfile::Standard,
        )?;
        client.post_form(
            OPINET_HOST,
            OPDOWNLOAD_LAYOUT_PATH,
            &[("tarUrl", OIL_PRICE_DOWNLOAD_TAR_URL)],
            Some(OPDOWNLOAD_URL),
            PostHeaderProfile::Ajax,
        )?;
        let download_key =
            DownloadNetFunnelKey(client.fetch_netfunnel_ticket(NETFUNNEL_DOWNLOAD_ACTION_ID)?);
        let downloaded = client.post_form_to_file(
            OPINET_HOST,
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
            || self.reserve_auto_source_temp_file(),
        )?;
        if !downloaded.response.body.starts_with(&OLE2_SIGNATURE) {
            let preview = downloaded.response.body.preview_lossy();
            let error_text = prefixed_message(
                "다운로드 응답이 예상한 OLE2 .xls 파일이 아닙니다: ",
                preview,
            );
            return Err(attach_remove_file_error(
                error_text.into(),
                &downloaded.path,
            ));
        }
        Ok(downloaded.path)
    }
    fn refresh_source(&mut self) -> Result<PathBuf> {
        let old_temp_path = self.dir.join(AUTO_SOURCE_OLD_TEMP_FILE_NAME);
        let removed_old_temp = match fs::remove_file(&old_temp_path) {
            Ok(()) => true,
            Err(error) if error.kind() == ErrorKind::NotFound => false,
            Err(error) => {
                return Err(err_with_source(
                    path_context_message("기존 자동 소스 정리 실패", &old_temp_path),
                    error,
                ));
            }
        };
        if removed_old_temp {
            match writeln!(self.out, "이전 임시 소스 파일 1개 정리") {
                Ok(()) | Err(_) => {}
            }
        }
        self.download_nationwide_source_http().map_err(|error| {
            let super::DownloadError {
                message: error_message,
                source: error_source,
            } = error;
            let app_message = prefixed_message("Opinet 자동 다운로드 실패: ", error_message);
            match error_source {
                Some(source_error) => err_with_source(app_message, source_error),
                None => err(app_message),
            }
        })
    }
    fn reserve_auto_source_temp_file(&self) -> DownloadResult<ReservedDownloadFile> {
        let pid = process::id();
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|source| {
                download_error_with_source("다운로드 임시 파일 시각 계산 실패", source)
            })?
            .as_nanos();
        let mut file_name = String::new();
        file_name
            .try_reserve_exact(AUTO_SOURCE_TEMP_FILE_NAME_CAPACITY)
            .map_err(|source| {
                download_error_with_source("다운로드 임시 파일명 메모리 확보 실패", source)
            })?;
        let mut path = self.dir.to_path_buf();
        path.try_reserve(AUTO_SOURCE_TEMP_FILE_NAME_CAPACITY)
            .map_err(|source| {
                download_error_with_source("다운로드 임시 파일 경로 메모리 확보 실패", source)
            })?;
        for seq in 0..AUTO_SOURCE_TEMP_FILE_RESERVATION_ATTEMPTS {
            file_name.clear();
            file_name.push_str(AUTO_SOURCE_TEMP_FILE_PREFIX);
            push_decimal_fragment(
                &mut file_name,
                u128::from(pid),
                "다운로드 임시 파일명 작성 실패",
            )?;
            file_name.push('_');
            push_decimal_fragment(&mut file_name, nanos, "다운로드 임시 파일명 작성 실패")?;
            file_name.push('_');
            push_decimal_fragment(
                &mut file_name,
                u128::from(seq),
                "다운로드 임시 파일명 작성 실패",
            )?;
            path.push(file_name.as_str());
            match File::options().write(true).create_new(true).open(&path) {
                Ok(file) => return Ok(ReservedDownloadFile { file, path }),
                Err(error) if error.kind() == ErrorKind::AlreadyExists => {
                    path.pop();
                    thread::sleep(Duration::from_micros(50));
                }
                Err(error) => {
                    return Err(download_error_with_source(
                        path_context_message("다운로드 임시 파일 생성 실패", &path),
                        error,
                    ));
                }
            }
        }
        Err(
            "다운로드 임시 파일 경로 생성 시도가 모두 실패했습니다. 잠시 후 다시 시도하세요."
                .into(),
        )
    }
}
