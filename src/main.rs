extern crate alloc;
use crate::{
    change_log::ChangeLogSheetServiceExt as _,
    cli::{Args, OutputTarget, ParseAction},
    excel::{
        source_reader::biff::{SourceReader, SourceReaderApi as _},
        writer::Workbook as StdWorkbook,
    },
    master_sheet::MasterSheetApi as _,
    numeric::round_f64_to_i32,
    source_download::SourceDownloadApi as _,
};
use alloc::collections::BTreeMap;
use core::{
    error::Error,
    fmt::{Arguments, Display, Write as _},
    mem,
    result::Result as StdResult,
    time::Duration,
};
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    env, fs,
    io::{Error as IoError, ErrorKind, Write, stderr, stdout},
    path::{Path, PathBuf},
};
#[cfg(not(windows))]
use std::{
    process::{Command, Output},
    time::{SystemTime, UNIX_EPOCH},
};
pub(crate) mod change_log;
mod cli;
mod excel;
pub(crate) mod master_sheet;
mod numeric;
pub(crate) mod source_download;
pub(crate) mod source_download_opdownload;
mod source_sync;
const MAX_CONFLICT_ATTEMPTS: u32 = 100_000;
const RESERVATION_MAGIC: &[u8] = b"FCUPDATER_RESERVED_v1\n";
const STALE_RESERVATION_AGE_SECS: u64 = 60 * 60;
const ADDRESS_KEY_REPLACEMENTS: [(&str, &str); 4] = [
    ("충청남도", "충남"),
    ("충청북도", "충북"),
    ("대전광역시", "대전"),
    ("세종특별자치시", "세종"),
];
const REGION_LABEL_SUFFIXES: [&str; 3] = ["특별자치시", "광역시", "특별시"];
type BoxError = Box<dyn Error + Send + Sync>;
type Result<T> = StdResult<T, BoxError>;
type SourceScore = (usize, usize, usize);
type SourceIndexEntry = (source_sync::SourceRecord, SourceScore, usize);
#[derive(Debug, Clone)]
struct ChangeRow {
    address: String,
    name: String,
    new_diesel: Option<i32>,
    new_gasoline: Option<i32>,
    new_premium: Option<i32>,
    old_diesel: Option<i32>,
    old_gasoline: Option<i32>,
    old_premium: Option<i32>,
    reason: String,
    region: String,
}
#[derive(Debug, Clone)]
struct StoreRow {
    address: String,
    diesel: Option<i32>,
    gasoline: Option<i32>,
    name: String,
    premium: Option<i32>,
    region: String,
}
#[derive(Debug, Default)]
struct DownloadedSourceGuard {
    paths: Vec<PathBuf>,
}
impl DownloadedSourceGuard {
    fn cleanup(&mut self, out: &mut dyn Write) -> Result<()> {
        self.cleanup_with_message("임시 소스 파일", out)
    }
    fn cleanup_with_message(&mut self, message: &str, out: &mut dyn Write) -> Result<()> {
        let mut removed = 0_usize;
        for path in mem::take(&mut self.paths) {
            if !path
                .file_name()
                .and_then(|text_os| text_os.to_str())
                .is_some_and(|file_name| file_name.contains(source_download::AUTO_SOURCE_MARKER))
            {
                continue;
            }
            match fs::metadata(&path) {
                Ok(metadata) if !metadata.is_file() => continue,
                Ok(_) => {}
                Err(source_err) if source_err.kind() == ErrorKind::NotFound => continue,
                Err(source_err) => {
                    return Err(err(path_source_message(
                        "메타데이터 확인 실패",
                        &path,
                        source_err,
                    )));
                }
            }
            fs::remove_file(&path).map_err(|source_err| {
                err(path_source_message(
                    "자동 소스 파일 삭제 실패",
                    &path,
                    source_err,
                ))
            })?;
            removed = removed.saturating_add(1);
        }
        if removed > 0 {
            write_line_ignored(out, format_args!("{message} {removed}개 정리"));
        }
        Ok(())
    }
    fn track(&mut self, paths: Vec<PathBuf>) {
        self.paths = paths;
    }
}
impl Drop for DownloadedSourceGuard {
    fn drop(&mut self) {
        if self.paths.is_empty() {
            return;
        }
        let mut err_out = stderr();
        if let Err(err) = self.cleanup_with_message("종료 중 임시 소스 파일", &mut err_out)
        {
            match writeln!(err_out, "종료 중 임시 소스 파일 정리 실패: {err}") {
                Ok(()) | Err(_) => {}
            }
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum NaturalPart {
    Number {
        digits_len: usize,
        normalized: String,
        raw_len: usize,
    },
    Text(String),
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SourceFileCandidate {
    natural_key: Vec<NaturalPart>,
    path: PathBuf,
}
struct UpdateSummary<'data> {
    added: &'data [StoreRow],
    args: &'data Args,
    changes: &'data [ChangeRow],
    deleted: &'data [StoreRow],
    out_path: &'data Path,
    source_paths: &'data [PathBuf],
    source_report: &'data source_sync::SourceIndexBuildReport,
}
struct UpdateRunContext<'args, 'out> {
    args: &'args Args,
    out: &'out mut dyn Write,
}
trait UpdateRunContextExt {
    fn build_source_index_and_report(
        &self,
        source_paths: &[PathBuf],
    ) -> Result<(
        HashMap<String, source_sync::SourceRecord>,
        source_sync::SourceIndexBuildReport,
    )>;
    fn build_source_records_from_sheet_xml_streaming(
        &self,
        sheet_xml: &str,
        shared_strings: &[String],
    ) -> Result<Vec<source_sync::SourceRecord>>;
    fn collect_source_paths(&self) -> Result<Vec<PathBuf>>;
    fn determine_output_path(&self, today: &str) -> Result<(PathBuf, bool)>;
    fn load_sources(
        &mut self,
        downloaded_sources: &mut DownloadedSourceGuard,
    ) -> Result<(
        Vec<PathBuf>,
        HashMap<String, source_sync::SourceRecord>,
        source_sync::SourceIndexBuildReport,
    )>;
    fn print_conflict_samples(&mut self, source_report: &source_sync::SourceIndexBuildReport);
    fn print_store_rows(&mut self, title: &str, rows: &[StoreRow]);
    fn print_update_summary(&mut self, summary: &UpdateSummary<'_>);
    fn read_source_records(&self, path: &Path) -> Result<Vec<source_sync::SourceRecord>>;
    fn read_xlsx_source_file(&self, path: &Path) -> Result<Vec<source_sync::SourceRecord>>;
    fn resolve_today(&self) -> Result<String>;
    fn run_update(&mut self) -> Result<()>;
    fn save_book(
        &mut self,
        book: &mut StdWorkbook,
        out_path: &Path,
        reserved_output: bool,
        today: &str,
    ) -> Result<()>;
    fn score_source_record(&self, record: &source_sync::SourceRecord) -> SourceScore;
    fn split_natural_parts(&self, text: &str) -> Vec<NaturalPart>;
}
impl UpdateRunContextExt for UpdateRunContext<'_, '_> {
    fn build_source_index_and_report(
        &self,
        source_paths: &[PathBuf],
    ) -> Result<(
        HashMap<String, source_sync::SourceRecord>,
        source_sync::SourceIndexBuildReport,
    )> {
        let mut map: HashMap<String, SourceIndexEntry> =
            HashMap::with_capacity(source_paths.len().saturating_mul(32));
        let mut report = source_sync::SourceIndexBuildReport::default();
        let mut sampled_keys: HashSet<String> =
            HashSet::with_capacity(source_sync::MAX_CONFLICT_SAMPLES);
        for (file_order, path) in source_paths.iter().enumerate() {
            let records = source_download::SourceDownloadOps
                .filter_target_region_records(self.read_source_records(path)?);
            for record in records {
                let key = normalize_address_key(&record.address);
                let score = self.score_source_record(&record);
                match map.entry(key) {
                    Entry::Vacant(vacant_entry) => {
                        vacant_entry.insert((record, score, file_order));
                    }
                    Entry::Occupied(mut occupied) => {
                        report.duplicate_addresses = report.duplicate_addresses.saturating_add(1);
                        let previous = occupied.get();
                        let prev_score = previous.1;
                        let prev_order = previous.2;
                        if report.samples.len() < source_sync::MAX_CONFLICT_SAMPLES
                            && sampled_keys.insert(occupied.key().clone())
                        {
                            let previous_source = source_paths.get(prev_order).map_or_else(
                                || {
                                    let capacity = 12;
                                    let mut out = String::with_capacity(capacity);
                                    out.push('#');
                                    push_display(&mut out, prev_order);
                                    out
                                },
                                |previous_path| source_label(previous_path),
                            );
                            let incoming_source = source_label(path);
                            let selected_source = if score > prev_score
                                || (score == prev_score && file_order >= prev_order)
                            {
                                incoming_source.clone()
                            } else {
                                previous_source.clone()
                            };
                            report.samples.push(source_sync::SourceConflictSample {
                                address: record.address.clone(),
                                previous_source,
                                incoming_source,
                                selected_source,
                            });
                        }
                        if score > prev_score || (score == prev_score && file_order >= prev_order) {
                            report.replaced_entries = report.replaced_entries.saturating_add(1);
                            occupied.insert((record, score, file_order));
                        }
                    }
                }
            }
        }
        let index = map
            .into_iter()
            .map(|(key, (entry, _score, _order))| (key, entry))
            .collect();
        Ok((index, report))
    }
    fn build_source_records_from_sheet_xml_streaming(
        &self,
        sheet_xml: &str,
        shared_strings: &[String],
    ) -> Result<Vec<source_sync::SourceRecord>> {
        let sheet_data = excel::source_reader::sheet_data_body(sheet_xml)?;
        let mut out = Vec::with_capacity(64);
        let mut cursor = 0_usize;
        let mut next_row_num = 1_usize;
        let mut scanned_rows = 0_usize;
        let header_scan_rows = excel::source_reader::source_header_scan_rows();
        let mut header_indices = None;
        while let Some((parsed_row_num, row_body, next_cursor)) =
            parse_next_sheet_row(sheet_data, cursor)?
        {
            let row_num = if parsed_row_num == 0 {
                next_row_num
            } else {
                parsed_row_num
            };
            if row_num > usize::try_from(excel::source_reader::MAX_XLSX_ROW).unwrap_or(usize::MAX) {
                let capacity = 64;
                let mut message = String::with_capacity(capacity);
                message.push_str("xlsx 행 번호가 비정상적입니다: ");
                push_display(&mut message, row_num);
                message.push_str(" (최대 ");
                push_display(&mut message, excel::source_reader::MAX_XLSX_ROW);
                message.push(')');
                return Err(err(message));
            }
            let row_cells = if let Some(row_xml_body) = row_body {
                excel::source_reader::parse_xlsx_row_cells(row_xml_body, row_num, shared_strings)?
            } else {
                Vec::default()
            };
            next_row_num = row_num.saturating_add(1);
            cursor = next_cursor;
            if parsed_row_num == 0 {
                continue;
            }
            if header_indices.is_none() && scanned_rows < header_scan_rows {
                header_indices = excel::source_reader::parse_source_header_indices(&row_cells);
                scanned_rows = scanned_rows.saturating_add(1);
                continue;
            }
            if let Some(indices) = header_indices
                && let Some(record) =
                    excel::source_reader::build_source_record_from_row(&row_cells, indices)
            {
                out.push(record);
            }
            scanned_rows = scanned_rows.saturating_add(1);
        }
        if header_indices.is_none() {
            return Err(err("헤더 행을 찾지 못했습니다"));
        }
        Ok(out)
    }
    fn collect_source_paths(&self) -> Result<Vec<PathBuf>> {
        let mut auto_candidates = Vec::with_capacity(16);
        let mut manual_candidates = Vec::with_capacity(16);
        let prefix_fold = self.args.sources_prefix.to_lowercase();
        for entry in fs::read_dir(&self.args.sources_dir).map_err(|source_err| {
            err(path_source_message(
                "폴더 읽기 실패",
                &self.args.sources_dir,
                source_err,
            ))
        })? {
            let dir_entry = entry?;
            let path = dir_entry.path();
            if !path.is_file() {
                continue;
            }
            let ext = path
                .extension()
                .and_then(|extension_os| extension_os.to_str())
                .unwrap_or_default();
            if !(ext.eq_ignore_ascii_case("xls") || ext.eq_ignore_ascii_case("xlsx")) {
                continue;
            }
            let Some(file_name) = path
                .file_name()
                .and_then(|file_name_os| file_name_os.to_str())
            else {
                continue;
            };
            let file_name_fold = file_name.to_lowercase();
            if !file_name_fold.starts_with(&prefix_fold) {
                continue;
            }
            let is_auto = file_name_fold.contains(source_download::AUTO_SOURCE_MARKER);
            let natural_key = self.split_natural_parts(&file_name_fold);
            let candidate = SourceFileCandidate { natural_key, path };
            if is_auto {
                auto_candidates.push(candidate);
            } else {
                manual_candidates.push(candidate);
            }
        }
        let mut candidates = if auto_candidates.is_empty() {
            manual_candidates
        } else {
            auto_candidates
        };
        candidates.sort_unstable();
        let mut source_paths = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            source_paths.push(candidate.path);
        }
        if source_paths.is_empty() {
            let capacity = self.args.sources_prefix.len().saturating_add(96);
            let mut out = String::with_capacity(capacity);
            out.push_str("소스 파일을 찾지 못했습니다. 폴더: ");
            push_display(&mut out, self.args.sources_dir.display());
            out.push_str(" / prefix: ");
            out.push_str(&self.args.sources_prefix);
            out.push_str(" / 확장자: .xls,.xlsx");
            return Err(err(out));
        }
        Ok(source_paths)
    }
    fn determine_output_path(&self, today: &str) -> Result<(PathBuf, bool)> {
        let reserved_output = !self.args.save_mode.is_dry_run()
            && !matches!(self.args.output_target, OutputTarget::InPlace);
        let requested = match self.args.output_target.clone() {
            OutputTarget::InPlace => self.args.master.clone(),
            OutputTarget::Auto => {
                let stem = self
                    .args
                    .master
                    .file_stem()
                    .and_then(|stem_os| stem_os.to_str())
                    .unwrap_or("fuel_cost_chungcheong");
                let parent = self.args.master.parent().unwrap_or_else(|| Path::new("."));
                let capacity = stem
                    .len()
                    .saturating_add(today.len())
                    .saturating_add("_updated_.xlsx".len());
                let mut file_name = String::with_capacity(capacity);
                file_name.push_str(stem);
                file_name.push_str("_updated_");
                file_name.push_str(today);
                file_name.push_str(".xlsx");
                parent.join(file_name)
            }
            OutputTarget::Explicit(path) => path,
        };
        let out_path = if self.args.save_mode.is_dry_run() {
            let mut seq = 0_u32;
            loop {
                let candidate = candidate_with_suffix(&requested, seq);
                if !candidate.try_exists().map_err(|source_err| {
                    err(path_source_message(
                        "출력 파일 경로 확인 실패",
                        &candidate,
                        source_err,
                    ))
                })? {
                    break candidate;
                }
                seq = seq.checked_add(1).ok_or_else(|| {
                    err(prefixed_message(
                        "출력 파일명 시퀀스 계산 overflow: ",
                        requested.display(),
                    ))
                })?;
                if seq > MAX_CONFLICT_ATTEMPTS {
                    return Err(err(prefixed_message(
                        "출력 파일명 충돌이 너무 많아 경로를 확정할 수 없습니다: ",
                        requested.display(),
                    )));
                }
            }
        } else {
            reserve_nonconflicting_path(&requested)?
        };
        Ok((out_path, reserved_output))
    }
    fn load_sources(
        &mut self,
        downloaded_sources: &mut DownloadedSourceGuard,
    ) -> Result<(
        Vec<PathBuf>,
        HashMap<String, source_sync::SourceRecord>,
        source_sync::SourceIndexBuildReport,
    )> {
        if !self.args.skip_download {
            let downloaded = source_download::SourceDownloadOps
                .refresh_sources(&self.args.sources_dir, &self.args.sources_prefix, self.out)
                .map_err(|source_err| {
                    let capacity = 128;
                    let mut out = String::with_capacity(capacity);
                    push_display(&mut out, source_err);
                    out.push_str("\n자동 다운로드를 건너뛰려면 --skip-download 를 지정하세요.");
                    err(out)
                })?;
            write_line_ignored(
                self.out,
                format_args!("소스 파일 {}개 준비 완료", downloaded.len()),
            );
            downloaded_sources.track(downloaded);
        }
        let source_paths = self.collect_source_paths()?;
        let (source_index, source_report) = self.build_source_index_and_report(&source_paths)?;
        downloaded_sources.cleanup(self.out)?;
        Ok((source_paths, source_index, source_report))
    }
    fn print_conflict_samples(&mut self, source_report: &source_sync::SourceIndexBuildReport) {
        if source_report.duplicate_addresses == 0 {
            return;
        }
        write_line_ignored(
            self.out,
            format_args!(
                "- 주소 중복 충돌: {}건 (대체 반영 {}건)",
                source_report.duplicate_addresses, source_report.replaced_entries
            ),
        );
        if source_report.samples.is_empty() {
            return;
        }
        write_line_ignored(self.out, format_args!("  충돌 예시:"));
        for (sample_index, sample) in source_report.samples.iter().enumerate() {
            let display_index = sample_index.saturating_add(1);
            write_line_ignored(
                self.out,
                format_args!(
                    "  {display_index}. {address} | 기존:{previous_source} | 신규:{incoming_source} | 선택:{selected_source}",
                    address = sample.address,
                    previous_source = sample.previous_source,
                    incoming_source = sample.incoming_source,
                    selected_source = sample.selected_source
                ),
            );
        }
    }
    fn print_store_rows(&mut self, title: &str, rows: &[StoreRow]) {
        if rows.is_empty() {
            return;
        }
        write_line_ignored(self.out, format_args!("\n{title}"));
        for (item_index, item) in rows.iter().take(20).enumerate() {
            let display_index = item_index.saturating_add(1);
            write_line_ignored(
                self.out,
                format_args!(
                    "  {display_index}. {region} / {name} / {address}",
                    region = item.region,
                    name = item.name,
                    address = item.address
                ),
            );
        }
        if rows.len() > 20 {
            write_line_ignored(
                self.out,
                format_args!("  ... ({}개 중 20개만 표시)", rows.len()),
            );
        }
    }
    fn print_update_summary(&mut self, summary: &UpdateSummary<'_>) {
        let UpdateSummary {
            added,
            args,
            changes,
            deleted,
            out_path,
            source_paths,
            source_report,
        } = *summary;
        write_line_ignored(self.out, format_args!("\n==== 현행화 요약 ===="));
        write_line_ignored(
            self.out,
            format_args!("- 마스터: {}", args.master.display()),
        );
        write_line_ignored(
            self.out,
            format_args!("- 소스 폴더: {}", args.sources_dir.display()),
        );
        write_line_ignored(
            self.out,
            format_args!("- 소스 접두사: {}", args.sources_prefix),
        );
        write_line_ignored(
            self.out,
            format_args!("- 소스 파일 수: {}", source_paths.len()),
        );
        write_line_ignored(
            self.out,
            format_args!("- 기존 업체 변경: {}건", changes.len()),
        );
        write_line_ignored(
            self.out,
            format_args!("- 신규 업체 추가: {}건", added.len()),
        );
        write_line_ignored(
            self.out,
            format_args!("- 폐업 업체 삭제: {}건", deleted.len()),
        );
        self.print_conflict_samples(source_report);
        if args.save_mode.is_dry_run() {
            write_line_ignored(self.out, format_args!("- 출력: 저장 안 함 (--dry-run)"));
        } else {
            write_line_ignored(self.out, format_args!("- 출력: {}", out_path.display()));
            let verify_label = if args.save_mode.verify_saved_file() {
                "사용 (기본)"
            } else {
                "생략 (--fast-save)"
            };
            write_line_ignored(self.out, format_args!("- 저장 검증: {verify_label}"));
        }
        self.print_store_rows("신규 업체 추가 목록 (상위 20개)", added);
        self.print_store_rows("폐업 업체 삭제 목록 (상위 20개)", deleted);
        write_line_ignored(self.out, format_args!("=====================\n"));
    }
    fn read_source_records(&self, path: &Path) -> Result<Vec<source_sync::SourceRecord>> {
        match path
            .extension()
            .and_then(|extension_os| extension_os.to_str())
            .unwrap_or_default()
        {
            ext if ext.eq_ignore_ascii_case("xlsx") => self.read_xlsx_source_file(path),
            ext if ext.eq_ignore_ascii_case("xls") => SourceReader.read_xls_source(path),
            _ => Err(err(prefixed_message(
                "지원하지 않는 소스 확장자입니다: ",
                path.display(),
            ))),
        }
        .map_err(|source_err| err(path_source_message("소스 파일 읽기 실패", path, source_err)))
    }
    fn read_xlsx_source_file(&self, path: &Path) -> Result<Vec<source_sync::SourceRecord>> {
        let container = excel::xlsx_container::XlsxContainer::open_for_update(path)?;
        let catalog = excel::ooxml::load_sheet_catalog(&container)?;
        let shared_strings = excel::ooxml::load_shared_strings(&container)?;
        if catalog.sheet_order.is_empty() {
            return Err(err("xlsx에 시트가 없습니다."));
        }
        let mut all = Vec::with_capacity(catalog.sheet_order.len().saturating_mul(32));
        let mut last_err: Option<BoxError> = None;
        for sheet_name in &catalog.sheet_order {
            let sheet_xml = excel::ooxml::load_sheet_xml(&container, &catalog, sheet_name)?;
            match self.build_source_records_from_sheet_xml_streaming(&sheet_xml, &shared_strings) {
                Ok(records) => {
                    if !records.is_empty() {
                        all.extend(records);
                    }
                }
                Err(stream_err) => {
                    let rows = {
                        let sheet_data = excel::source_reader::sheet_data_body(&sheet_xml)?;
                        let mut rows_map: BTreeMap<usize, Vec<excel::source_reader::CellValue>> =
                            BTreeMap::default();
                        let mut cursor = 0_usize;
                        let mut next_row_num = 1_usize;
                        while let Some((parsed_row_num, row_body, next_cursor)) =
                            parse_next_sheet_row(sheet_data, cursor)?
                        {
                            let row_num = if parsed_row_num == 0 {
                                next_row_num
                            } else {
                                parsed_row_num
                            };
                            let row_cells = if let Some(row_xml_body) = row_body {
                                excel::source_reader::parse_xlsx_row_cells(
                                    row_xml_body,
                                    row_num,
                                    &shared_strings,
                                )?
                            } else {
                                Vec::default()
                            };
                            rows_map.insert(row_num, row_cells);
                            next_row_num = row_num.saturating_add(1);
                            cursor = next_cursor;
                        }
                        rows_map.into_iter().collect::<Vec<_>>()
                    };
                    match excel::source_reader::build_source_records_from_rows(&rows) {
                        Ok(records) => {
                            if !records.is_empty() {
                                all.extend(records);
                            }
                        }
                        Err(legacy_err) => {
                            let capacity = 128;
                            let mut out = String::with_capacity(capacity);
                            out.push_str("스트리밍 파싱 실패: ");
                            push_display(&mut out, stream_err);
                            out.push_str("; 구형 파싱 실패: ");
                            push_display(&mut out, legacy_err);
                            last_err = Some(err(out));
                        }
                    }
                }
            }
        }
        if !all.is_empty() {
            return Ok(all);
        }
        if let Some(source_err) = last_err {
            let capacity = 96;
            let mut out = String::with_capacity(capacity);
            out.push_str("xlsx 시트에서 유효한 소스 데이터를 찾지 못했습니다. (");
            push_display(&mut out, source_err);
            out.push(')');
            return Err(err(out));
        }
        Err(err("xlsx 시트에서 유효한 소스 데이터를 찾지 못했습니다."))
    }
    fn resolve_today(&self) -> Result<String> {
        let is_yyyy_mm_dd = |text: &str| {
            let bytes = text.as_bytes();
            bytes.len() == 10
                && bytes.get(4) == Some(&b'-')
                && bytes.get(7) == Some(&b'-')
                && bytes
                    .iter()
                    .enumerate()
                    .all(|(index, ch)| index == 4 || index == 7 || ch.is_ascii_digit())
        };
        cfg_select! {
            windows => {
                let mut system_time = excel::windows_api::SystemTime {
                    year: 0,
                    month: 0,
                    day_of_week: 0,
                    day: 0,
                    hour: 0,
                    minute: 0,
                    second: 0,
                    milliseconds: 0,
                };
                // SAFETY: `system_time` points to a valid writable `SystemTime` value for the duration of the call.
                unsafe { excel::windows_api::GetLocalTime(&raw mut system_time) };
                if system_time.month == 0
                    || system_time.month > 12
                    || system_time.day == 0
                    || system_time.day > 31
                {
                    return Err(err(format_ymd(
                        "OS 날짜 조회 결과가 비정상적입니다: ",
                        system_time.year,
                        system_time.month,
                        system_time.day,
                    )));
                }
                let today = format_ymd("", system_time.year, system_time.month, system_time.day);
                if !is_yyyy_mm_dd(&today) {
                    return Err(err(prefixed_message(
                        "오늘 날짜 형식이 올바르지 않습니다: ",
                        &today,
                    )));
                }
                Ok(today)
            }
            _ => {
                let mut detected_today = None;
                if let Ok(output) = Command::new("date").args(["+%Y-%m-%d"]).output()
                    && output.status.success()
                {
                    detected_today = valid_today_from_output(&output, is_yyyy_mm_dd);
                }
                if detected_today.is_none() {
                    let script =
                        "from datetime import datetime;print(datetime.now().strftime('%Y-%m-%d'))";
                    for program in ["python3", "python"] {
                        if let Ok(output) = Command::new(program).args(["-c", script]).output()
                            && output.status.success()
                        {
                            detected_today = valid_today_from_output(&output, is_yyyy_mm_dd);
                            if detected_today.is_some() {
                                break;
                            }
                        }
                    }
                }
                if let Some(today) = detected_today {
                    return Ok(today);
                }
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map_err(|source| err(prefixed_message("현재 시간 조회 실패: ", source)))?;
                let days = i64::try_from(now.as_secs() / 86_400)
                    .map_err(|_| err("UTC 날짜 계산 중 일수 변환에 실패했습니다."))?;
                let shifted_days = days + 719_468;
                let era = if shifted_days >= 0 {
                    shifted_days
                } else {
                    shifted_days - 146_096
                } / 146_097;
                let doe = shifted_days - era * 146_097;
                let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
                let year_of_era = yoe + era * 400;
                let day_of_year = doe - (365 * yoe + yoe / 4 - yoe / 100);
                let month_phase = (5 * day_of_year + 2) / 153;
                let day = day_of_year - (153 * month_phase + 2) / 5 + 1;
                let month = month_phase + if month_phase < 10 { 3 } else { -9 };
                let year = (year_of_era + if month <= 2 { 1 } else { 0 }) as i32;
                let today = format_ymd("", year, month, day);
                if !is_yyyy_mm_dd(&today) {
                    return Err(err(prefixed_message(
                        "오늘 날짜 형식이 올바르지 않습니다: ",
                        &today,
                    )));
                }
                Ok(today)
            }
        }
    }
    fn run_update(&mut self) -> Result<()> {
        let master_exists = self.args.master.try_exists().map_err(|source_err| {
            err(path_source_message(
                "마스터 파일 경로 확인 실패",
                &self.args.master,
                source_err,
            ))
        })?;
        if !master_exists {
            return Err(err(prefixed_message(
                "마스터 파일이 없습니다: ",
                format_args!(
                    "{} (같은 폴더에 두거나 --master로 경로를 지정하세요)",
                    self.args.master.display()
                ),
            )));
        }
        let mut downloaded_sources = DownloadedSourceGuard::default();
        let (source_paths, source_index, source_report) =
            self.load_sources(&mut downloaded_sources)?;
        let mut book = StdWorkbook::open(&self.args.master)?;
        let (changes, added, deleted) =
            master_sheet::MasterSheetOps.update_master_sheet(&mut book, &source_index)?;
        let today = self.resolve_today()?;
        if !self.args.no_change_log {
            change_log::ChangeLogSheetService
                .update_change_log_sheet(&mut book, &today, &changes, &added, &deleted)?;
        }
        let (out_path, reserved_output) = self.determine_output_path(&today)?;
        self.save_book(&mut book, &out_path, reserved_output, &today)?;
        self.print_update_summary(&UpdateSummary {
            added: &added,
            args: self.args,
            changes: &changes,
            deleted: &deleted,
            out_path: &out_path,
            source_paths: &source_paths,
            source_report: &source_report,
        });
        Ok(())
    }
    fn save_book(
        &mut self,
        book: &mut StdWorkbook,
        out_path: &Path,
        reserved_output: bool,
        today: &str,
    ) -> Result<()> {
        if self.args.save_mode.is_dry_run() {
            return Ok(());
        }
        if matches!(self.args.output_target, OutputTarget::InPlace) {
            let parent = self.args.master.parent().unwrap_or_else(|| Path::new("."));
            let stem = self
                .args
                .master
                .file_stem()
                .and_then(|stem_os| stem_os.to_str())
                .unwrap_or("fuel_cost_chungcheong");
            let capacity = stem.len().saturating_add(today.len()).saturating_add(13);
            let mut file_name = String::with_capacity(capacity);
            file_name.push_str(stem);
            file_name.push_str("_backup_");
            file_name.push_str(today);
            file_name.push_str(".xlsx");
            let base = parent.join(file_name);
            let backup = reserve_nonconflicting_path(&base)?;
            if let Err(copy_err) = fs::copy(&self.args.master, &backup) {
                match fs::remove_file(&backup) {
                    Ok(()) | Err(_) => {}
                }
                return Err(err(path_pair_source_message(
                    "백업 파일 생성에 실패했습니다",
                    &self.args.master,
                    &backup,
                    copy_err,
                )));
            }
            write_line_ignored(
                self.out,
                format_args!("백업 파일 생성: {}", backup.display()),
            );
        }
        if let Err(save_err) = book.save_as(out_path, self.args.save_mode.verify_saved_file()) {
            if reserved_output
                && let Ok(content) = fs::read(out_path)
                && content == RESERVATION_MAGIC
            {
                match fs::remove_file(out_path) {
                    Ok(()) | Err(_) => {}
                }
            }
            return Err(save_err);
        }
        Ok(())
    }
    fn score_source_record(&self, record: &source_sync::SourceRecord) -> SourceScore {
        (
            [record.gasoline, record.premium, record.diesel]
                .into_iter()
                .flatten()
                .count(),
            {
                let mut non_empty_fields = 0_usize;
                for field_value in [
                    record.region.trim(),
                    record.name.trim(),
                    record.brand.trim(),
                    record.self_yn.trim(),
                    record.address.trim(),
                ] {
                    if !field_value.is_empty() {
                        non_empty_fields = non_empty_fields.saturating_add(1);
                    }
                }
                non_empty_fields
            },
            record
                .region
                .len()
                .saturating_add(record.name.len())
                .saturating_add(record.brand.len())
                .saturating_add(record.self_yn.len())
                .saturating_add(record.address.len()),
        )
    }
    fn split_natural_parts(&self, text: &str) -> Vec<NaturalPart> {
        fn push_part(parts_out: &mut Vec<NaturalPart>, raw: &str, part_is_digit: bool) {
            if part_is_digit {
                let trimmed = raw.trim_start_matches('0');
                let normalized: String = if trimmed.is_empty() {
                    "0".into()
                } else {
                    trimmed.to_owned()
                };
                parts_out.push(NaturalPart::Number {
                    digits_len: normalized.len(),
                    normalized,
                    raw_len: raw.len(),
                });
            } else {
                parts_out.push(NaturalPart::Text(raw.to_owned()));
            }
        }
        let mut out = Vec::with_capacity(text.len().saturating_div(2).saturating_add(1));
        let mut buf = String::with_capacity(text.len());
        let mut digit_mode: Option<bool> = None;
        for ch in text.chars() {
            let is_digit = ch.is_ascii_digit();
            match digit_mode {
                None => {
                    digit_mode = Some(is_digit);
                    buf.push(ch);
                }
                Some(mode) if mode == is_digit => buf.push(ch),
                Some(mode) => {
                    push_part(&mut out, &buf, mode);
                    buf.clear();
                    digit_mode = Some(is_digit);
                    buf.push(ch);
                }
            }
        }
        if let Some(mode) = digit_mode {
            push_part(&mut out, &buf, mode);
        }
        out
    }
}
fn push_display(out: &mut String, value: impl Display) {
    match write!(out, "{value}") {
        Ok(()) | Err(_) => {}
    }
}
#[cfg(not(windows))]
fn valid_today_from_output(
    output: &Output,
    is_yyyy_mm_dd: impl Fn(&str) -> bool,
) -> Option<String> {
    let today = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    is_yyyy_mm_dd(&today).then_some(today)
}
fn err(msg: impl Into<String>) -> BoxError {
    IoError::other(msg.into()).into()
}
fn err_with_source(context: impl Into<String>, source: impl Display) -> BoxError {
    let context_text = context.into();
    let capacity = context_text.len().saturating_add(64);
    let mut message = String::with_capacity(capacity);
    message.push_str(&context_text);
    message.push_str(": ");
    push_display(&mut message, source);
    IoError::other(message).into()
}
pub(crate) fn prefixed_message(prefix: &str, detail: impl Display) -> String {
    let capacity = prefix.len().saturating_add(64);
    let mut out = String::with_capacity(capacity);
    out.push_str(prefix);
    push_display(&mut out, detail);
    out
}
fn xlsx_row_offset_message(prefix: &str, offset: usize) -> String {
    let capacity = prefix.len().saturating_add(16);
    let mut out = String::with_capacity(capacity);
    out.push_str(prefix);
    push_display(&mut out, offset);
    out.push(')');
    out
}
fn xlsx_row_number_message(prefix: &str, row_num: u32) -> String {
    let capacity = prefix.len().saturating_add(16);
    let mut out = String::with_capacity(capacity);
    out.push_str(prefix);
    push_display(&mut out, row_num);
    out.push(')');
    out
}
pub(crate) fn path_source_message(label: &str, path: &Path, source: impl Display) -> String {
    let capacity = label.len().saturating_add(96);
    let mut out = String::with_capacity(capacity);
    out.push_str(label);
    out.push_str(": ");
    push_display(&mut out, path.display());
    out.push_str(" (");
    push_display(&mut out, source);
    out.push(')');
    out
}
pub(crate) fn path_pair_source_message(
    label: &str,
    from: &Path,
    to: &Path,
    source: impl Display,
) -> String {
    let capacity = label.len().saturating_add(128);
    let mut out = String::with_capacity(capacity);
    out.push_str(label);
    out.push_str(": ");
    push_display(&mut out, from.display());
    out.push_str(" -> ");
    push_display(&mut out, to.display());
    out.push_str(" (");
    push_display(&mut out, source);
    out.push(')');
    out
}
pub(crate) fn program_source_message(program: &str, phase: &str, source: impl Display) -> String {
    let capacity = program.len().saturating_add(phase.len()).saturating_add(64);
    let mut out = String::with_capacity(capacity);
    out.push_str(program);
    out.push(' ');
    out.push_str(phase);
    out.push_str(": ");
    push_display(&mut out, source);
    out
}
pub(crate) fn write_line_ignored(output: &mut dyn Write, args: Arguments<'_>) {
    match output.write_fmt(args) {
        Ok(()) | Err(_) => {}
    }
    match output.write_all(b"\n") {
        Ok(()) | Err(_) => {}
    }
}
fn candidate_with_suffix(path: &Path, seq: u32) -> PathBuf {
    if seq == 0 {
        return path.to_path_buf();
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let stem = path
        .file_stem()
        .and_then(|stem_os| stem_os.to_str())
        .unwrap_or("output");
    let ext = path
        .extension()
        .and_then(|extension_os| extension_os.to_str());
    let capacity = stem
        .len()
        .saturating_add(12)
        .saturating_add(ext.map_or(1, |file_ext| file_ext.len().saturating_add(2)));
    let mut file_name = String::with_capacity(capacity);
    file_name.push_str(stem);
    file_name.push('_');
    push_display(&mut file_name, seq);
    if let Some(file_ext) = ext {
        file_name.push('.');
        file_name.push_str(file_ext);
    }
    parent.join(file_name)
}
fn reserve_nonconflicting_path(path: &Path) -> Result<PathBuf> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent).map_err(|source_err| {
        err(path_source_message(
            "출력 폴더 생성 실패",
            parent,
            source_err,
        ))
    })?;
    let mut seq = 0_u32;
    loop {
        let candidate = candidate_with_suffix(path, seq);
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(mut file) => {
                if let Err(write_err) = file
                    .write_all(RESERVATION_MAGIC)
                    .and_then(|()| file.flush())
                    .and_then(|()| file.sync_all())
                {
                    drop(file);
                    match fs::remove_file(&candidate) {
                        Ok(()) | Err(_) => {}
                    }
                    return Err(err(path_source_message(
                        "출력 파일 예약 마커 기록 실패",
                        &candidate,
                        write_err,
                    )));
                }
                return Ok(candidate);
            }
            Err(io_err) if io_err.kind() == ErrorKind::AlreadyExists => {
                let remove_stale = fs::metadata(&candidate)
                    .ok()
                    .filter(fs::Metadata::is_file)
                    .and_then(|meta| meta.modified().ok())
                    .and_then(|modified| modified.elapsed().ok())
                    .filter(|elapsed| *elapsed >= Duration::from_secs(STALE_RESERVATION_AGE_SECS))
                    .and_then(|_| fs::read(&candidate).ok())
                    .is_some_and(|content| {
                        content == RESERVATION_MAGIC && fs::remove_file(&candidate).is_ok()
                    });
                if remove_stale {
                    continue;
                }
                seq = seq.checked_add(1).ok_or_else(|| {
                    err(prefixed_message(
                        "출력 파일 예약 시퀀스 계산 overflow: ",
                        path.display(),
                    ))
                })?;
                if seq > MAX_CONFLICT_ATTEMPTS {
                    return Err(err(prefixed_message(
                        "출력 파일 예약 충돌이 너무 많아 경로를 확정할 수 없습니다: ",
                        path.display(),
                    )));
                }
            }
            Err(io_err) => {
                return Err(err(path_source_message(
                    "출력 파일 예약 실패",
                    &candidate,
                    io_err,
                )));
            }
        }
    }
}
fn source_label(path: &Path) -> String {
    path.file_name()
        .and_then(|file_name_os| file_name_os.to_str())
        .map_or_else(|| prefixed_message("", path.display()), String::from)
}
fn parse_next_sheet_row(
    sheet_data: &str,
    cursor: usize,
) -> Result<Option<(usize, Option<&str>, usize)>> {
    let Some(row_open_rel) = sheet_data.get(cursor..).and_then(|tail| tail.find("<row")) else {
        return Ok(None);
    };
    let row_open =
        excel::source_reader::checked_xml_offset_add(cursor, row_open_rel, "xlsx row 시작")?;
    let Some(row_tag_end_rel) = sheet_data.get(row_open..).and_then(|tail| tail.find('>')) else {
        return Err(err(xlsx_row_offset_message(
            "xlsx row 시작 태그가 손상되었습니다. (offset=",
            row_open,
        )));
    };
    let row_tag_end = excel::source_reader::checked_xml_offset_add(
        row_open,
        row_tag_end_rel,
        "xlsx row 태그 끝",
    )?;
    let row_tag = sheet_data.get(row_open..=row_tag_end).ok_or_else(|| {
        err(xlsx_row_offset_message(
            "xlsx row 태그 범위가 손상되었습니다. (offset=",
            row_open,
        ))
    })?;
    let row_num_u32 = row_tag
        .find("r=\"")
        .and_then(|start| {
            let offset =
                excel::source_reader::checked_xml_offset_add(start, 3, "xlsx row 번호 속성")
                    .ok()?;
            row_tag.get(offset..)
        })
        .and_then(|tail| {
            let end = tail.find('"')?;
            tail.get(..end)
        })
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(0);
    let row_num = usize::try_from(row_num_u32).map_err(|source| {
        err_with_source(
            prefixed_message("xlsx 행 번호 변환 실패: ", row_num_u32),
            source,
        )
    })?;
    if row_tag.ends_with("/>") {
        let next_cursor =
            excel::source_reader::checked_xml_offset_add(row_tag_end, 1, "xlsx row cursor 전진")?;
        return Ok(Some((row_num, None, next_cursor)));
    }
    let row_body_start =
        excel::source_reader::checked_xml_offset_add(row_tag_end, 1, "xlsx row 본문 시작")?;
    let Some(row_close_rel) = sheet_data
        .get(row_body_start..)
        .and_then(|tail| tail.find("</row>"))
    else {
        return Err(err(xlsx_row_number_message(
            "xlsx row 종료 태그를 찾지 못했습니다. (row=",
            row_num_u32,
        )));
    };
    let row_body_end = excel::source_reader::checked_xml_offset_add(
        row_body_start,
        row_close_rel,
        "xlsx row 본문 끝",
    )?;
    let row_body = sheet_data
        .get(row_body_start..row_body_end)
        .ok_or_else(|| {
            err(xlsx_row_number_message(
                "xlsx row 본문 범위가 손상되었습니다. (row=",
                row_num_u32,
            ))
        })?;
    let next_cursor = excel::source_reader::checked_xml_offset_add(
        row_body_end,
        "</row>".len(),
        "xlsx row cursor 전진",
    )?;
    Ok(Some((row_num, Some(row_body), next_cursor)))
}
fn main() -> Result<()> {
    let mut out = stdout();
    let raw_args: Vec<_> = env::args_os().skip(1).collect();
    let action = ParseAction::try_from(raw_args.as_slice())?;
    match action {
        ParseAction::Run(run_args) => {
            let mut context = UpdateRunContext {
                args: &run_args,
                out: &mut out,
            };
            context.run_update()
        }
        ParseAction::Help(text) | ParseAction::Version(text) => {
            write_line_ignored(&mut out, format_args!("{text}"));
            Ok(())
        }
    }
}
fn canon_header(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        if !ch.is_whitespace() {
            out.push(ch);
        }
    }
    out
}
fn same_trimmed(left: &str, right: &str) -> bool {
    left.trim() == right.trim()
}
fn parse_i32_str(text: &str) -> Option<i32> {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "-" {
        return None;
    }
    let normalized = trimmed.replace(',', "");
    normalized.parse::<f64>().ok().and_then(round_f64_to_i32)
}
fn usize_to_u32(value: usize, context: &str) -> Result<u32> {
    u32::try_from(value).map_err(|source| {
        let capacity = context.len().saturating_add(32);
        let mut out = String::with_capacity(capacity);
        out.push_str(context);
        out.push_str(" 값이 너무 큽니다. (value=");
        push_display(&mut out, value);
        out.push(')');
        err_with_source(out, source)
    })
}
fn shift_row(row: u32, increase: u32, decrease: u32) -> u32 {
    if increase > 0 {
        row.saturating_add(increase)
    } else {
        row.saturating_sub(decrease).max(1)
    }
}
fn add_row_offset(base_row: u32, offset: usize, context: &str) -> Result<u32> {
    let offset_u32 = usize_to_u32(offset, context)?;
    base_row.checked_add(offset_u32).ok_or_else(|| {
        let capacity = context.len().saturating_add(48);
        let mut out = String::with_capacity(capacity);
        out.push_str(context);
        out.push_str(" 계산 중 overflow가 발생했습니다. (");
        push_display(&mut out, base_row);
        out.push_str(" + ");
        push_display(&mut out, offset_u32);
        out.push(')');
        err(out)
    })
}
fn format_ymd(prefix: &str, year: impl Display, month: impl Display, day: impl Display) -> String {
    let capacity = prefix.len().saturating_add(10);
    let mut out = String::with_capacity(capacity);
    out.push_str(prefix);
    match write!(&mut out, "{year:04}-{month:02}-{day:02}") {
        Ok(()) | Err(_) => {}
    }
    out
}
fn normalize_address_key(addr: &str) -> String {
    let mut rest = addr.trim();
    let capacity = rest.len();
    let mut out = String::with_capacity(capacity);
    while let Some(ch) = rest.chars().next() {
        if let Some((from, to)) = ADDRESS_KEY_REPLACEMENTS
            .iter()
            .copied()
            .find(|candidate| rest.starts_with(candidate.0))
        {
            out.push_str(to);
            rest = rest.get(from.len()..).unwrap_or_default();
            continue;
        }
        rest = rest.get(ch.len_utf8()..).unwrap_or_default();
        if ch.is_whitespace() {
            continue;
        }
        if matches!(ch, '(' | ')' | '[' | ']' | '{' | '}' | ',' | '.') {
            continue;
        }
        out.push(ch);
    }
    out
}
fn display_region_label_from_source(region: &str, address: &str) -> String {
    if let Some(label) = parse_region_label(region) {
        return label;
    }
    if let Some(label) = parse_region_label(address) {
        return label;
    }
    region.trim().to_owned()
}
fn parse_region_label(text: &str) -> Option<String> {
    let mut tokens = text.split_whitespace().filter_map(|token| {
        let trimmed = token.trim();
        (!trimmed.is_empty()).then_some(trimmed)
    });
    let first = tokens.next()?;
    let second = tokens.next();
    for suffix in REGION_LABEL_SUFFIXES {
        if let Some(label) = first.strip_suffix(suffix)
            && !label.is_empty()
        {
            return Some(label.to_owned());
        }
    }
    if is_province_token(first) {
        return second.map_or_else(
            || None,
            |token| {
                Some(
                    strip_basic_region_suffix(token)
                        .map_or_else(|| token.to_owned(), str::to_owned),
                )
            },
        );
    }
    if is_metropolitan_token(first) {
        return Some(first.to_owned());
    }
    strip_basic_region_suffix(first).map_or_else(
        || (second.is_none()).then(|| first.to_owned()),
        |label| Some(label.to_owned()),
    )
}
fn strip_basic_region_suffix(token: &str) -> Option<&str> {
    token
        .strip_suffix('시')
        .or_else(|| token.strip_suffix('군'))
        .or_else(|| token.strip_suffix('구'))
        .filter(|label| !label.is_empty())
}
fn is_province_token(token: &str) -> bool {
    token.ends_with('도')
        || token.ends_with("특별자치도")
        || matches!(
            token,
            "충남" | "충북" | "경기" | "강원" | "전북" | "전남" | "경북" | "경남" | "제주"
        )
}
fn is_metropolitan_token(token: &str) -> bool {
    matches!(
        token,
        "서울" | "부산" | "대구" | "인천" | "광주" | "대전" | "울산" | "세종"
    )
}
