use super::{
    CURRENT_PRICE_PAGE_DIV, DEFAULT_REGION_LABEL, DownloadResult, GAS_STATION_API_GBN,
    GAS_STATION_LPG_CODE, NETFUNNEL_DOWNLOAD_ACTION_ID, NETFUNNEL_ENTRY_ACTION_ID,
    OIL_PRICE_DOWNLOAD_TAR_URL, OLE2_SIGNATURE, OPDOWNLOAD_EXCEL_PATH, OPDOWNLOAD_LAYOUT_PATH,
    OPDOWNLOAD_PATH, OPDOWNLOAD_URL, OPINET_HOST, SourceDownload, TemporarySourceFile,
    download_error_with_source,
    http_client::{self, PostHeaderProfile},
};
use crate::{
    diagnostic::{Result, err, err_with_source, path_context_message, prefixed_message},
    temp_entry::{TempEntryKind, cleanup_stale_temp_entries, reserve_unique_temp_entry},
};
use std::{
    fs::{self, File},
    io::{self, Write},
};
cfg_select! {
    windows => {
        use std::os::windows::fs::OpenOptionsExt as _;
    }
    any(target_os = "linux", target_os = "macos") => {
        use std::os::unix::fs::OpenOptionsExt as _;
    }
    _ => {}
}
const AUTO_SOURCE_OLD_TEMP_FILE_NAME: &str = "fcupdater-opinet-source.tmp";
const AUTO_SOURCE_TEMP_FILE_PREFIX: &str = ".fcupdater-opinet-source.tmp_";
impl<W> SourceDownload<'_, '_, W>
where
    W: Write + ?Sized,
{
    fn download_nationwide_source_http(&self) -> DownloadResult<TemporarySourceFile> {
        let mut client = http_client::HttpClient::default();
        let opdownload_page = client.get_text(OPINET_HOST, OPDOWNLOAD_PATH, None)?;
        let opinet_key = {
            const KEY_ASSIGNMENT_MARKER: &str = "opinet_key.value";
            let Some((_, after_marker)) = opdownload_page.split_once(KEY_ASSIGNMENT_MARKER) else {
                return Err("Opinet 다운로드 페이지에서 key 할당 구문을 찾지 못했습니다.".into());
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
        let entry_key = client.fetch_netfunnel_ticket(NETFUNNEL_ENTRY_ACTION_ID)?;
        client.post_form(
            OPINET_HOST,
            OPDOWNLOAD_PATH,
            &[
                ("netfunnel_key", entry_key.as_str()),
                ("opinet_key", opinet_key),
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
        let download_key = client.fetch_netfunnel_ticket(NETFUNNEL_DOWNLOAD_ACTION_ID)?;
        let (response, target) = client.post_form_to_file(
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
        if !response.body.preview.starts_with(&OLE2_SIGNATURE) {
            let preview = response.body.preview_lossy();
            let error_text = prefixed_message(
                "다운로드 응답이 예상한 OLE2 .xls 파일이 아닙니다: ",
                preview,
            );
            return Err(target.remove_after_error(error_text.into()));
        }
        Ok(target)
    }
    pub(crate) fn refresh_source(&mut self) -> Result<TemporarySourceFile> {
        fs::create_dir_all(self.dir).map_err(|source_err| {
            err_with_source(
                path_context_message("소스 폴더 생성 실패", self.dir),
                source_err,
            )
        })?;
        let removed_temp_count = self.remove_stale_auto_source_temp_files()?;
        if removed_temp_count != 0 {
            writeln!(self.out, "이전 임시 소스 파일 {removed_temp_count}개 정리")?;
        }
        self.download_nationwide_source_http().map_err(|mut error| {
            error
                .update_message(|message| prefixed_message("Opinet 자동 다운로드 실패: ", message));
            error
        })
    }
    fn remove_stale_auto_source_temp_files(&self) -> Result<usize> {
        let old_temp_path = self.dir.join(AUTO_SOURCE_OLD_TEMP_FILE_NAME);
        let removed_legacy = match fs::remove_file(&old_temp_path) {
            Ok(()) => 1_usize,
            Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
            Err(error) => {
                return Err(err_with_source(
                    path_context_message("기존 자동 소스 정리 실패", &old_temp_path),
                    error,
                ));
            }
        };
        let removed_stale =
            cleanup_stale_temp_entries(self.dir, AUTO_SOURCE_TEMP_FILE_PREFIX, TempEntryKind::File)
                .map_err(|source| {
                    err_with_source(
                        path_context_message("이전 임시 소스 정리 실패", self.dir),
                        source,
                    )
                })?;
        removed_legacy
            .checked_add(removed_stale)
            .ok_or_else(|| err("정리한 임시 소스 파일 수 계산 중 overflow가 발생했습니다."))
    }
    fn reserve_auto_source_temp_file(&self) -> DownloadResult<TemporarySourceFile> {
        reserve_unique_temp_entry(self.dir, AUTO_SOURCE_TEMP_FILE_PREFIX, |path| {
            let mut options = File::options();
            options.read(true).write(true).create_new(true);
            cfg_select! {
                windows => {
                    options.share_mode(0);
                }
                any(target_os = "linux", target_os = "macos") => {
                    options.mode(0o600);
                }
                _ => {}
            }
            options.open(path).map(|file| TemporarySourceFile {
                file: Some(file),
                path: path.to_path_buf(),
            })
        })
        .map_err(|source| {
            download_error_with_source(
                path_context_message("다운로드 임시 파일 생성 실패", self.dir),
                source,
            )
        })
    }
}
