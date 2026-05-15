use super::{
    AUTO_SOURCE_MARKER, CURRENT_PRICE_PAGE_DIV, DEFAULT_REGION_LABEL, GAS_STATION_API_GBN,
    GAS_STATION_LPG_CODE, NETFUNNEL_DOWNLOAD_ACTION_ID, NETFUNNEL_ENTRY_ACTION_ID,
    OIL_PRICE_DOWNLOAD_TAR_URL, OPDOWNLOAD_EXCEL_PATH, OPDOWNLOAD_LAYOUT_PATH, OPDOWNLOAD_PATH,
    OPDOWNLOAD_URL, OPINET_HOST, OPINET_KEY, SourceDownloadOps, contains_ascii_ignore_case,
    http_client, lossy_prefix,
};
use crate::{
    Result, err, err_with_source, has_basic_region_suffix, is_metropolitan_token,
    is_province_token, normalize_address_key, path_source_message, prefixed_message,
    source_sync::SourceRecord, strip_basic_region_suffix,
};
use alloc::{string::String, vec::Vec};
use core::{array::from_fn, result::Result as StdResult};
use std::{
    fs,
    io::Write,
    path::{Path, PathBuf},
    sync::LazyLock,
};
const TASK_COUNT: usize = 11;
const TASK_KEY_CAPACITY: usize = 4;
const TASKS: [Task; TASK_COUNT] = [
    Task {
        sido: "대전광역시",
        sigungu: "대덕구",
    },
    Task {
        sido: "대전광역시",
        sigungu: "동구",
    },
    Task {
        sido: "대전광역시",
        sigungu: "서구",
    },
    Task {
        sido: "대전광역시",
        sigungu: "유성구",
    },
    Task {
        sido: "대전광역시",
        sigungu: "중구",
    },
    Task {
        sido: "세종특별자치시",
        sigungu: "세종시",
    },
    Task {
        sido: "충청북도",
        sigungu: "청주시",
    },
    Task {
        sido: "충청남도",
        sigungu: "공주시",
    },
    Task {
        sido: "충청남도",
        sigungu: "보령시",
    },
    Task {
        sido: "충청남도",
        sigungu: "아산시",
    },
    Task {
        sido: "충청남도",
        sigungu: "천안시",
    },
];
#[derive(Debug, Clone, Copy)]
struct Task {
    sido: &'static str,
    sigungu: &'static str,
}
struct TaskMatcher {
    sido_key: String,
    task_keys: TaskKeys,
}
struct TaskKeys {
    len: usize,
    values: [String; TASK_KEY_CAPACITY],
}
pub(super) trait SourceDownloadOpsExt {
    fn filter_target_region_records_impl(
        &self,
        records: Vec<SourceRecord>,
    ) -> Result<Vec<SourceRecord>>;
    fn refresh_sources_impl(
        &self,
        dir: &Path,
        prefix: &str,
        out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>>;
}
trait SourceDownloadWorkflowExt {
    fn auto_source_name(&self, prefix: &str, extension: &str) -> String;
    fn cleanup_auto_source_files(&self, dir: &Path, prefix: &str) -> StdResult<usize, String>;
    fn download_nationwide_source(&self, dir: &Path, prefix: &str) -> Result<Vec<PathBuf>>;
    fn download_nationwide_source_http(
        &self,
        dir: &Path,
        prefix: &str,
    ) -> StdResult<PathBuf, String>;
    fn record_matches_any_task(&self, record: &SourceRecord, matchers: &[TaskMatcher]) -> bool;
    fn region_has_explicit_sigungu(&self, region: &str) -> bool;
    fn task_match_keys(&self, task: &Task) -> TaskKeys;
    fn task_matchers(&self) -> &'static [TaskMatcher];
}
impl TaskKeys {
    fn push_unique(&mut self, key: String) {
        if key.is_empty()
            || self
                .values
                .iter()
                .take(self.len)
                .any(|existing| existing == &key)
        {
            return;
        }
        let Some(slot) = self.values.get_mut(self.len) else {
            debug_assert!(
                self.len < TASK_KEY_CAPACITY,
                "TASK_KEY_CAPACITY is too small for task aliases"
            );
            return;
        };
        *slot = key;
        self.len = self.len.saturating_add(1);
    }
}
impl SourceDownloadOpsExt for SourceDownloadOps {
    fn filter_target_region_records_impl(
        &self,
        records: Vec<SourceRecord>,
    ) -> Result<Vec<SourceRecord>> {
        let matchers = self.task_matchers();
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
            if self.record_matches_any_task(&record, matchers) {
                filtered.push(record);
            }
        }
        Ok(filtered)
    }
    fn refresh_sources_impl(
        &self,
        dir: &Path,
        prefix: &str,
        out: &mut dyn Write,
    ) -> Result<Vec<PathBuf>> {
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
            .cleanup_auto_source_files(&canonical_dir, prefix)
            .map_err(|error_text| {
                err(prefixed_message("기존 자동 소스 정리 실패: ", error_text))
            })?;
        if removed > 0 {
            let _write_result = writeln!(out, "이전 임시 소스 파일 {removed}개 정리");
        }
        self.download_nationwide_source(&canonical_dir, prefix)
    }
}
impl SourceDownloadWorkflowExt for SourceDownloadOps {
    fn auto_source_name(&self, prefix: &str, extension: &str) -> String {
        format!("{prefix}{AUTO_SOURCE_MARKER}_opdownload_current_price.{extension}")
    }
    fn cleanup_auto_source_files(&self, dir: &Path, prefix: &str) -> StdResult<usize, String> {
        let mut removed = 0_usize;
        let prefix_fold = prefix.to_lowercase();
        let entries =
            fs::read_dir(dir).map_err(|error| path_source_message("폴더 읽기 실패", dir, error))?;
        for entry_result in entries {
            let dir_entry = entry_result
                .map_err(|error| prefixed_message("디렉터리 항목 읽기 실패: ", error))?;
            let path = dir_entry.path();
            if !path.is_file() {
                continue;
            }
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let folded = file_name.to_lowercase();
            if !(folded.starts_with(&prefix_fold) && folded.contains(AUTO_SOURCE_MARKER)) {
                continue;
            }
            fs::remove_file(&path)
                .map_err(|error| path_source_message("자동 소스 파일 삭제 실패", &path, error))?;
            removed = removed.saturating_add(1);
        }
        Ok(removed)
    }
    fn download_nationwide_source(&self, dir: &Path, prefix: &str) -> Result<Vec<PathBuf>> {
        let downloaded =
            self.download_nationwide_source_http(dir, prefix)
                .map_err(|error_text| {
                    err(prefixed_message("Opinet 자동 다운로드 실패: ", error_text))
                })?;
        let mut paths = Vec::new();
        paths
            .try_reserve(1)
            .map_err(|source| err_with_source("다운로드 소스 목록 메모리 확보 실패", source))?;
        paths.push(downloaded);
        Ok(paths)
    }
    fn download_nationwide_source_http(
        &self,
        dir: &Path,
        prefix: &str,
    ) -> StdResult<PathBuf, String> {
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
        let has_excel_signature = response
            .body
            .starts_with(&[0xD0, 0xCF, 0x11, 0xE0, 0xA1, 0xB1, 0x1A, 0xE1])
            || response.body.starts_with(b"PK\x03\x04");
        if !has_excel_signature {
            let preview = lossy_prefix(&response.body, 512);
            return Err(prefixed_message(
                "다운로드 응답이 Excel 파일이 아닙니다: ",
                preview,
            ));
        }
        let mut extension = "xls";
        for header in &response.headers {
            let name = &header.0;
            let value = &header.1;
            if !name.eq_ignore_ascii_case("content-disposition") {
                continue;
            }
            if contains_ascii_ignore_case(value, b".xlsx") {
                extension = "xlsx";
                break;
            }
            if contains_ascii_ignore_case(value, b".xls") {
                break;
            }
        }
        let target = dir.join(self.auto_source_name(prefix, extension));
        let temp = dir.join(self.auto_source_name(prefix, "tmp"));
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
    fn record_matches_any_task(&self, record: &SourceRecord, matchers: &[TaskMatcher]) -> bool {
        let region_key = normalize_address_key(&record.region);
        let region_has_explicit_sigungu =
            !region_key.is_empty() && self.region_has_explicit_sigungu(&record.region);
        let mut combined_key: Option<String> = None;
        matchers.iter().any(|matcher| {
            let matches_task = |value: &str| {
                matcher
                    .task_keys
                    .values
                    .iter()
                    .take(matcher.task_keys.len)
                    .any(|task_key| value.contains(task_key))
            };
            if !region_key.is_empty() {
                if !region_key.contains(&matcher.sido_key) {
                    return false;
                }
                if matches_task(&region_key) {
                    return true;
                }
                if region_has_explicit_sigungu {
                    return false;
                }
            }
            let combined = combined_key.get_or_insert_with(|| {
                let combined_source = format!("{} {}", record.region, record.address);
                normalize_address_key(&combined_source)
            });
            combined.contains(&matcher.sido_key) && matches_task(combined)
        })
    }
    fn region_has_explicit_sigungu(&self, region: &str) -> bool {
        let mut tokens = region.split_whitespace();
        let Some(first_token) = tokens.next() else {
            return false;
        };
        has_basic_region_suffix(first_token)
            || ((is_province_token(first_token) || is_metropolitan_token(first_token))
                && tokens.next().is_some_and(has_basic_region_suffix))
    }
    fn task_match_keys(&self, task: &Task) -> TaskKeys {
        let mut keys = TaskKeys {
            len: 0,
            values: from_fn(|_| String::new()),
        };
        let mut push_alias_key = |alias: &str| {
            let alias_key = normalize_address_key(alias);
            keys.push_unique(alias_key);
            if let Some(stripped_alias) = strip_basic_region_suffix(alias) {
                let stripped = normalize_address_key(stripped_alias);
                keys.push_unique(stripped);
            }
        };
        push_alias_key(task.sigungu);
        if task.sigungu == "세종시" {
            push_alias_key("세종특별자치시");
        }
        keys
    }
    fn task_matchers(&self) -> &'static [TaskMatcher] {
        static TASK_MATCHERS: LazyLock<[TaskMatcher; TASK_COUNT]> = LazyLock::new(|| {
            let ops = SourceDownloadOps;
            TASKS.map(|task| TaskMatcher {
                sido_key: normalize_address_key(task.sido),
                task_keys: ops.task_match_keys(&task),
            })
        });
        &*TASK_MATCHERS
    }
}
