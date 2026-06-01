use super::{
    CURRENT_PRICE_PAGE_DIV, DEFAULT_REGION_LABEL, DownloadResult, GAS_STATION_API_GBN,
    GAS_STATION_LPG_CODE, NETFUNNEL_DOWNLOAD_ACTION_ID, NETFUNNEL_ENTRY_ACTION_ID,
    OIL_PRICE_DOWNLOAD_TAR_URL, OLE2_SIGNATURE, OPDOWNLOAD_EXCEL_PATH, OPDOWNLOAD_LAYOUT_PATH,
    OPDOWNLOAD_PATH, OPDOWNLOAD_URL, OPINET_HOST, OPINET_KEY, attach_remove_file_error,
    http_client::{self, PostHeaderProfile},
};
use crate::{Result, SourceDownload, err, path_source_message, prefixed_message};
use std::{
    fs,
    io::{ErrorKind, Write},
    path::PathBuf,
};
const AUTO_SOURCE_FILE_NAME: &str = "fcupdater-opinet-source.xls";
const AUTO_SOURCE_TEMP_FILE_NAME: &str = "fcupdater-opinet-source.tmp";
struct SourceDownloadWorkflow<'out, W: Write + ?Sized> {
    canonical_dir: PathBuf,
    out: &'out mut W,
}
impl<W> SourceDownload<'_, '_, W>
where
    W: Write + ?Sized,
{
    pub fn refresh_source(&mut self) -> Result<PathBuf> {
        fs::create_dir_all(self.dir).map_err(|source_err| {
            err(path_source_message(
                "소스 폴더 생성 실패",
                self.dir,
                source_err,
            ))
        })?;
        let canonical_dir = self.dir.canonicalize().map_err(|source_err| {
            err(path_source_message(
                "소스 폴더 경로 확인 실패",
                self.dir,
                source_err,
            ))
        })?;
        SourceDownloadWorkflow {
            canonical_dir,
            out: &mut *self.out,
        }
        .refresh_source()
    }
}
impl<W> SourceDownloadWorkflow<'_, W>
where
    W: Write + ?Sized,
{
    fn cleanup_auto_source_files(&self) -> DownloadResult<usize> {
        let mut removed = 0_usize;
        for file_name in [AUTO_SOURCE_FILE_NAME, AUTO_SOURCE_TEMP_FILE_NAME] {
            let path = self.canonical_dir.join(file_name);
            match fs::remove_file(&path) {
                Ok(()) => {
                    removed = removed.saturating_add(1);
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(
                        path_source_message("자동 소스 파일 삭제 실패", &path, error).into(),
                    );
                }
            }
        }
        Ok(removed)
    }
    fn download_nationwide_source_http(&self) -> DownloadResult<PathBuf> {
        let mut client = http_client::HttpClient::default();
        client.get_text(OPINET_HOST, OPDOWNLOAD_PATH, None)?;
        let entry_key = client.fetch_netfunnel_ticket(NETFUNNEL_ENTRY_ACTION_ID)?;
        client.post_form(
            OPINET_HOST,
            OPDOWNLOAD_PATH,
            &[
                ("netfunnel_key", entry_key.as_str()),
                ("opinet_key", OPINET_KEY),
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
        let target = self.canonical_dir.join(AUTO_SOURCE_FILE_NAME);
        let temp = self.canonical_dir.join(AUTO_SOURCE_TEMP_FILE_NAME);
        let response = client.post_form_to_file(
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
            &temp,
        )?;
        if !response.body.starts_with(&OLE2_SIGNATURE) {
            let preview = response.body.preview_lossy();
            let error_text = prefixed_message(
                "다운로드 응답이 예상한 OLE2 .xls 파일이 아닙니다: ",
                preview,
            );
            return Err(attach_remove_file_error(error_text, &temp).into());
        }
        match fs::rename(&temp, &target) {
            Ok(()) => {}
            Err(error) => {
                let error_text =
                    path_source_message("다운로드 파일 이름 변경 실패", &target, error);
                return Err(attach_remove_file_error(error_text, &temp).into());
            }
        }
        Ok(target)
    }
    fn refresh_source(&mut self) -> Result<PathBuf> {
        let removed = self.cleanup_auto_source_files().map_err(|error_text| {
            err(prefixed_message("기존 자동 소스 정리 실패: ", error_text))
        })?;
        if removed > 0 {
            match writeln!(self.out, "이전 임시 소스 파일 {removed}개 정리") {
                Ok(()) | Err(_) => {}
            }
        }
        self.download_nationwide_source_http()
            .map_err(|error_text| err(prefixed_message("Opinet 자동 다운로드 실패: ", error_text)))
    }
}
