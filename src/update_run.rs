use crate::{
    BoxError, Result, change_log,
    change_log::ChangeLogSheetServiceExt as _,
    cli::{Args, OutputTarget},
    downloaded_source::DownloadedSourceGuard,
    err, err_with_source,
    excel::{
        self,
        source_reader::biff::{SourceReader, SourceReaderApi as _},
        writer::Workbook as StdWorkbook,
    },
    io_util::write_line_ignored,
    kst_date::{KST_OFFSET, KstDateCalculator, KstDateCalculatorExt as _, SECS_PER_DAY_U64},
    master_sheet,
    master_sheet::MasterSheetApi as _,
    natural_sort,
    output_reservation::{
        MAX_CONFLICT_ATTEMPTS, candidate_with_suffix, file_has_reservation_magic,
        path_file_stem_or, path_parent_or_current, reserve_nonconflicting_path, source_label,
    },
    path_pair_source_message, path_source_message, prefixed_message,
    region::normalize_address_key,
    rows::{ChangeRow, StoreRow},
    source_download, source_sync,
    vec_util::{extend_source_records, try_reserve_vec, try_reserve_vec_exact},
    xlsx_row,
};
use alloc::collections::BTreeMap;
use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    fs,
    io::Write,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};
type SourceScore = (usize, usize, usize);
type SourceIndexEntry = (source_sync::SourceRecord, SourceScore, usize);
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SourceFileCandidate {
    natural_key: natural_sort::NaturalKey,
    path: PathBuf,
}
pub struct UpdateSummary<'data> {
    added: &'data [StoreRow],
    args: &'data Args,
    changes: &'data [ChangeRow],
    deleted: &'data [StoreRow],
    out_path: &'data Path,
    source_paths: &'data [PathBuf],
    source_report: &'data source_sync::SourceIndexBuildReport,
}
pub struct UpdateRunContext<'args, 'out> {
    pub args: &'args Args,
    pub out: &'out mut dyn Write,
}
pub trait UpdateRunContextExt {
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
}
impl UpdateRunContextExt for UpdateRunContext<'_, '_> {
    fn build_source_index_and_report(
        &self,
        source_paths: &[PathBuf],
    ) -> Result<(
        HashMap<String, source_sync::SourceRecord>,
        source_sync::SourceIndexBuildReport,
    )> {
        let source_index_capacity = source_paths.len().saturating_mul(32);
        let mut map: HashMap<String, SourceIndexEntry> = HashMap::new();
        map.try_reserve(source_index_capacity).map_err(|source| {
            err_with_source(
                format!("소스 index 맵 메모리 확보 실패: {source_index_capacity} entries"),
                source,
            )
        })?;
        let mut report = source_sync::SourceIndexBuildReport::default();
        let mut sampled_keys: HashSet<String> = HashSet::new();
        sampled_keys
            .try_reserve(source_sync::MAX_CONFLICT_SAMPLES)
            .map_err(|source| {
                let sample_count = source_sync::MAX_CONFLICT_SAMPLES;
                err_with_source(
                    format!("소스 충돌 sample key 집합 메모리 확보 실패: {sample_count} entries"),
                    source,
                )
            })?;
        for (file_order, path) in source_paths.iter().enumerate() {
            let records = source_download::SourceDownloadOps
                .filter_target_region_records(self.read_source_records(path)?)?;
            map.try_reserve(records.len()).map_err(|source| {
                let record_count = records.len();
                err_with_source(
                    format!("소스 index 맵 추가 메모리 확보 실패: {record_count} entries"),
                    source,
                )
            })?;
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
                                || format!("#{prev_order}"),
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
        let map_len = map.len();
        let mut index: HashMap<String, source_sync::SourceRecord> = HashMap::new();
        index.try_reserve(map_len).map_err(|source| {
            err_with_source(
                format!("최종 소스 index 메모리 확보 실패: {map_len} entries"),
                source,
            )
        })?;
        index.extend(
            map.into_iter()
                .map(|(key, (entry, _score, _order))| (key, entry)),
        );
        Ok((index, report))
    }
    fn build_source_records_from_sheet_xml_streaming(
        &self,
        sheet_xml: &str,
        shared_strings: &[String],
    ) -> Result<Vec<source_sync::SourceRecord>> {
        let sheet_data = excel::source_reader::sheet_data_body(sheet_xml)?;
        let mut out: Vec<source_sync::SourceRecord> = Vec::new();
        try_reserve_vec_exact(&mut out, 64, "스트리밍 소스 레코드 메모리 확보 실패")?;
        let mut cursor = 0_usize;
        let mut next_row_num = 1_usize;
        let mut scanned_rows = 0_usize;
        let header_scan_rows = excel::source_reader::source_header_scan_rows();
        let max_xlsx_row = usize::try_from(excel::source_reader::MAX_XLSX_ROW)
            .map_err(|source| err_with_source("xlsx 최대 행 번호 변환 실패", source))?;
        let mut header_indices = None;
        while let Some((parsed_row_num, row_body, next_cursor)) =
            xlsx_row::parse_next_sheet_row(sheet_data, cursor)?
        {
            let row_num = if parsed_row_num == 0 {
                next_row_num
            } else {
                parsed_row_num
            };
            if row_num > max_xlsx_row {
                return Err(err(format!(
                    "xlsx 행 번호가 비정상적입니다: {row_num} (최대 {})",
                    excel::source_reader::MAX_XLSX_ROW
                )));
            }
            let row_cells = if let Some(row_xml_body) = row_body {
                excel::source_reader::parse_xlsx_row_cells(row_xml_body, row_num, shared_strings)?
            } else {
                Vec::new()
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
                try_reserve_vec(&mut out, 1, "스트리밍 소스 레코드 추가 메모리 확보 실패")?;
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
        let mut auto_candidates: Vec<SourceFileCandidate> = Vec::new();
        let mut manual_candidates: Vec<SourceFileCandidate> = Vec::new();
        try_reserve_vec_exact(
            &mut auto_candidates,
            16,
            "자동 소스 후보 목록 메모리 확보 실패",
        )?;
        try_reserve_vec_exact(
            &mut manual_candidates,
            16,
            "수동 소스 후보 목록 메모리 확보 실패",
        )?;
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
            let Some(ext) = path
                .extension()
                .and_then(|extension_os| extension_os.to_str())
            else {
                continue;
            };
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
            let natural_key = natural_sort::NaturalKey::from(file_name_fold.as_str());
            let candidate = SourceFileCandidate { natural_key, path };
            if is_auto {
                try_reserve_vec(
                    &mut auto_candidates,
                    1,
                    "자동 소스 후보 추가 메모리 확보 실패",
                )?;
                auto_candidates.push(candidate);
            } else {
                try_reserve_vec(
                    &mut manual_candidates,
                    1,
                    "수동 소스 후보 추가 메모리 확보 실패",
                )?;
                manual_candidates.push(candidate);
            }
        }
        let mut candidates = if auto_candidates.is_empty() {
            manual_candidates
        } else {
            auto_candidates
        };
        candidates.sort_unstable();
        let mut source_paths: Vec<PathBuf> = Vec::new();
        try_reserve_vec_exact(
            &mut source_paths,
            candidates.len(),
            "소스 경로 목록 메모리 확보 실패",
        )?;
        source_paths.extend(candidates.into_iter().map(|candidate| candidate.path));
        if source_paths.is_empty() {
            return Err(err(format!(
                "소스 파일을 찾지 못했습니다. 폴더: {} / prefix: {} / 확장자: .xls,.xlsx",
                self.args.sources_dir.display(),
                self.args.sources_prefix
            )));
        }
        Ok(source_paths)
    }
    fn determine_output_path(&self, today: &str) -> Result<(PathBuf, bool)> {
        let reserved_output = !self.args.save_mode.is_dry_run()
            && !matches!(self.args.output_target, OutputTarget::InPlace);
        let requested = match self.args.output_target.clone() {
            OutputTarget::InPlace => self.args.master.clone(),
            OutputTarget::Auto => {
                let stem = path_file_stem_or(&self.args.master, "fuel_cost_chungcheong");
                let parent = path_parent_or_current(&self.args.master);
                parent.join(format!("{stem}_updated_{today}.xlsx"))
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
                    err(format!(
                        "{source_err}\n자동 다운로드를 건너뛰려면 --skip-download 를 지정하세요."
                    ))
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
        let Some(ext) = path
            .extension()
            .and_then(|extension_os| extension_os.to_str())
        else {
            return Err(err(prefixed_message(
                "지원하지 않는 소스 확장자입니다: ",
                path.display(),
            )));
        };
        if ext.eq_ignore_ascii_case("xlsx") {
            self.read_xlsx_source_file(path)
        } else if ext.eq_ignore_ascii_case("xls") {
            SourceReader.read_xls_source(path)
        } else {
            Err(err(prefixed_message(
                "지원하지 않는 소스 확장자입니다: ",
                path.display(),
            )))
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
        let all_capacity = catalog.sheet_order.len().saturating_mul(32);
        let mut all: Vec<source_sync::SourceRecord> = Vec::new();
        try_reserve_vec_exact(
            &mut all,
            all_capacity,
            "xlsx 전체 소스 레코드 메모리 확보 실패",
        )?;
        let mut last_err: Option<BoxError> = None;
        for sheet_name in &catalog.sheet_order {
            let sheet_xml = excel::ooxml::load_sheet_xml(&container, &catalog, sheet_name)?;
            match self.build_source_records_from_sheet_xml_streaming(&sheet_xml, &shared_strings) {
                Ok(records) => {
                    extend_source_records(&mut all, records)?;
                }
                Err(stream_err) => {
                    let rows = {
                        let sheet_data = excel::source_reader::sheet_data_body(&sheet_xml)?;
                        let mut rows_map: BTreeMap<usize, Vec<excel::source_reader::CellValue>> =
                            BTreeMap::new();
                        let mut cursor = 0_usize;
                        let mut next_row_num = 1_usize;
                        while let Some((parsed_row_num, row_body, next_cursor)) =
                            xlsx_row::parse_next_sheet_row(sheet_data, cursor)?
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
                                Vec::new()
                            };
                            rows_map.insert(row_num, row_cells);
                            next_row_num = row_num.saturating_add(1);
                            cursor = next_cursor;
                        }
                        let row_count = rows_map.len();
                        let mut rows = Vec::new();
                        try_reserve_vec_exact(
                            &mut rows,
                            row_count,
                            "xlsx 구형 행 목록 메모리 확보 실패",
                        )?;
                        rows.extend(rows_map);
                        rows
                    };
                    match excel::source_reader::build_source_records_from_rows(&rows) {
                        Ok(records) => {
                            extend_source_records(&mut all, records)?;
                        }
                        Err(row_build_err) => {
                            last_err = Some(err(format!(
                                "스트리밍 파싱 실패: {stream_err}; 구형 파싱 실패: {row_build_err}"
                            )));
                        }
                    }
                }
            }
        }
        if !all.is_empty() {
            return Ok(all);
        }
        if let Some(source_err) = last_err {
            return Err(err(format!(
                "xlsx 시트에서 유효한 소스 데이터를 찾지 못했습니다. ({source_err})"
            )));
        }
        Err(err("xlsx 시트에서 유효한 소스 데이터를 찾지 못했습니다."))
    }
    fn resolve_today(&self) -> Result<String> {
        let is_yyyy_mm_dd = |text: &str| {
            let &[y0, y1, y2, y3, b'-', m0, m1, b'-', d0, d1] = text.as_bytes() else {
                return false;
            };
            [y0, y1, y2, y3, m0, m1, d0, d1]
                .iter()
                .all(u8::is_ascii_digit)
        };
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|source| err(prefixed_message("현재 시간 조회 실패: ", source)))?;
        let kst_secs = since_epoch
            .as_secs()
            .checked_add(KST_OFFSET.as_secs())
            .ok_or_else(|| err("KST 날짜 초 계산 중 범위 오류가 발생했습니다."))?;
        let day_index_i64 = i64::try_from(kst_secs.div_euclid(SECS_PER_DAY_U64))
            .map_err(|source| err_with_source("KST 날짜 일수 변환에 실패했습니다.", source))?;
        let day_index = i32::try_from(day_index_i64)
            .map_err(|source| err_with_source("KST 날짜 범위 변환에 실패했습니다.", source))?;
        let (year, month, day) = KstDateCalculator
            .civil_from_days(day_index)
            .ok_or_else(|| err("KST 날짜 계산 중 범위 오류가 발생했습니다."))?;
        let today = KstDateCalculator.format_ymd("", year, month, day);
        if !is_yyyy_mm_dd(&today) {
            return Err(err(prefixed_message(
                "오늘 날짜 형식이 올바르지 않습니다: ",
                &today,
            )));
        }
        Ok(today)
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
        write_line_ignored(self.out, format_args!("마스터 파일 처리 중..."));
        let mut book = StdWorkbook::open(&self.args.master)?;
        let (changes, added, deleted) =
            master_sheet::MasterSheetOps.update_master_sheet(&mut book, &source_index)?;
        let today = self.resolve_today()?;
        if !self.args.no_change_log {
            change_log::ChangeLogSheetService
                .update_change_log_sheet(&mut book, &today, &changes, &added, &deleted)?;
        }
        let (out_path, reserved_output) = self.determine_output_path(&today)?;
        if !self.args.save_mode.is_dry_run() {
            write_line_ignored(self.out, format_args!("마스터 파일 저장 중..."));
        }
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
            let parent = path_parent_or_current(&self.args.master);
            let stem = path_file_stem_or(&self.args.master, "fuel_cost_chungcheong");
            let base = parent.join(format!("{stem}_backup_{today}.xlsx"));
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
            if reserved_output && file_has_reservation_magic(out_path) {
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
                [
                    record.region.trim(),
                    record.name.trim(),
                    record.brand.trim(),
                    record.self_yn.trim(),
                    record.address.trim(),
                ]
                .into_iter()
                .filter(|field_value| !field_value.is_empty())
                .count()
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
}
