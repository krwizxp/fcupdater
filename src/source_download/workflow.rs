use super::{
    CURRENT_PRICE_PAGE_DIV, DEFAULT_REGION_LABEL, GAS_STATION_API_GBN, GAS_STATION_LPG_CODE,
    NETFUNNEL_DOWNLOAD_ACTION_ID, NETFUNNEL_ENTRY_ACTION_ID, OIL_PRICE_DOWNLOAD_TAR_URL,
    OPDOWNLOAD_EXCEL_PATH, OPDOWNLOAD_LAYOUT_PATH, OPDOWNLOAD_PATH, OPDOWNLOAD_URL, OPINET_HOST,
    OPINET_KEY, SourceDownloadOps, http_client, lossy_prefix,
};
use crate::{
    Result, SourceRecord, err, err_with_source, normalize_address_key, path_source_message,
    prefixed_message,
};
use alloc::{string::String, vec::Vec};
use core::result::Result as StdResult;
use std::{
    fs,
    io::{ErrorKind, Write},
    path::{Path, PathBuf},
};
const TARGET_REGION_KEYS: [&str; 11] = [
    "대전대덕구",
    "대전동구",
    "대전서구",
    "대전유성구",
    "대전중구",
    "세종시",
    "충북청주시",
    "충남공주시",
    "충남보령시",
    "충남아산시",
    "충남천안시",
];
const AUTO_SOURCE_FILE_NAME: &str = "fcupdater-opinet-source.xls";
const AUTO_SOURCE_TEMP_FILE_NAME: &str = "fcupdater-opinet-source.tmp";
pub(super) trait SourceDownloadOpsExt {
    fn filter_target_region_records_impl(
        &self,
        records: Vec<SourceRecord>,
    ) -> Result<Vec<SourceRecord>>;
    fn refresh_source_impl(&self, dir: &Path, out: &mut dyn Write) -> Result<PathBuf>;
}
trait SourceDownloadWorkflowExt {
    fn cleanup_auto_source_files(&self, dir: &Path) -> StdResult<usize, String>;
    fn download_nationwide_source_http(&self, dir: &Path) -> StdResult<PathBuf, String>;
    fn record_matches_target_region(&self, record: &SourceRecord) -> bool;
}
impl SourceDownloadWorkflowExt for SourceDownloadOps {
    fn cleanup_auto_source_files(&self, dir: &Path) -> StdResult<usize, String> {
        let mut removed = 0_usize;
        for file_name in [AUTO_SOURCE_FILE_NAME, AUTO_SOURCE_TEMP_FILE_NAME] {
            let path = dir.join(file_name);
            match fs::remove_file(&path) {
                Ok(()) => {
                    removed = removed.saturating_add(1);
                }
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(path_source_message(
                        "자동 소스 파일 삭제 실패",
                        &path,
                        error,
                    ));
                }
            }
        }
        Ok(removed)
    }
    fn download_nationwide_source_http(&self, dir: &Path) -> StdResult<PathBuf, String> {
        let mut client = http_client::HttpClient::default();
        let _gate_html = client.get_text(OPINET_HOST, OPDOWNLOAD_PATH, None)?;
        let entry_key = client.fetch_netfunnel_ticket(NETFUNNEL_ENTRY_ACTION_ID)?;
        let _entry_page = client.post_form(
            OPINET_HOST,
            OPDOWNLOAD_PATH,
            &[
                ("netfunnel_key", entry_key.as_str()),
                ("opinet_key", OPINET_KEY),
            ],
            Some(OPDOWNLOAD_URL),
            false,
        )?;
        let _layout = client.post_form(
            OPINET_HOST,
            OPDOWNLOAD_LAYOUT_PATH,
            &[("tarUrl", OIL_PRICE_DOWNLOAD_TAR_URL)],
            Some(OPDOWNLOAD_URL),
            true,
        )?;
        let download_key = client.fetch_netfunnel_ticket(NETFUNNEL_DOWNLOAD_ACTION_ID)?;
        let response = client.post_form(
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
            false,
        )?;
        if !response
            .body
            .starts_with(&[0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1])
        {
            let preview = lossy_prefix(&response.body, 512);
            return Err(prefixed_message(
                "다운로드 응답이 예상한 OLE2 .xls 파일이 아닙니다: ",
                preview,
            ));
        }
        let target = dir.join(AUTO_SOURCE_FILE_NAME);
        let temp = dir.join(AUTO_SOURCE_TEMP_FILE_NAME);
        fs::write(&temp, &response.body)
            .map_err(|error| path_source_message("다운로드 파일 쓰기 실패", &temp, error))?;
        match fs::rename(&temp, &target) {
            Ok(()) => {}
            Err(error) => {
                let _cleanup_result = fs::remove_file(&temp);
                return Err(path_source_message(
                    "다운로드 파일 이름 변경 실패",
                    &target,
                    error,
                ));
            }
        }
        Ok(target)
    }
    fn record_matches_target_region(&self, record: &SourceRecord) -> bool {
        let region_key = normalize_address_key(&record.region);
        TARGET_REGION_KEYS.contains(&region_key.as_str())
    }
}
impl SourceDownloadOpsExt for SourceDownloadOps {
    fn filter_target_region_records_impl(
        &self,
        records: Vec<SourceRecord>,
    ) -> Result<Vec<SourceRecord>> {
        let mut filtered = Vec::new();
        filtered
            .try_reserve_exact(records.len())
            .map_err(|source| {
                let record_count = records.len();
                err_with_source(
                    format!("필터링 소스 레코드 목록 메모리 확보 실패: {record_count} records"),
                    source,
                )
            })?;
        for record in records {
            if self.record_matches_target_region(&record) {
                filtered.push(record);
            }
        }
        Ok(filtered)
    }
    fn refresh_source_impl(&self, dir: &Path, out: &mut dyn Write) -> Result<PathBuf> {
        fs::create_dir_all(dir).map_err(|source_err| {
            err(path_source_message("소스 폴더 생성 실패", dir, source_err))
        })?;
        let canonical_dir = dir.canonicalize().map_err(|source_err| {
            err(path_source_message(
                "소스 폴더 경로 확인 실패",
                dir,
                source_err,
            ))
        })?;
        let removed = self
            .cleanup_auto_source_files(&canonical_dir)
            .map_err(|error_text| {
                err(prefixed_message("기존 자동 소스 정리 실패: ", error_text))
            })?;
        if removed > 0 {
            let _write_result = writeln!(out, "이전 임시 소스 파일 {removed}개 정리");
        }
        self.download_nationwide_source_http(&canonical_dir)
            .map_err(|error_text| err(prefixed_message("Opinet 자동 다운로드 실패: ", error_text)))
    }
}
