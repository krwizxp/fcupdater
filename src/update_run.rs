use crate::{
    change_log::ChangeLogUpdater,
    diagnostic::{Result, err, err_with_source, path_context_message},
    excel::{SaveVerification, SourceReader, SourceRecord},
    excel::{writer::Workbook as StdWorkbook, xlsx_container::XlsxContainer},
    master_sheet::{
        AddedStoreRow, ChangeRow, MasterSheetUpdateResult, MasterSheetUpdater, StoreRow,
    },
    region::{
        TARGET_REGION_COUNT, TARGET_REGION_LABELS, increment_target_region_count,
        normalize_address_key, target_region_index,
    },
    source_download::SourceDownload,
    write_line, write_line_best_effort,
};
use core::time::Duration;
use std::{
    collections::{HashMap, hash_map::Entry},
    fs,
    io::Write,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
const MAX_DELETED_RATIO_EXCLUSIVE_NUMERATOR: usize = 1;
const MAX_DELETED_RATIO_DENOMINATOR: usize = 2;
const MIN_REGION_RETAIN_DENOMINATOR: usize = 2;
const MIN_REGION_RETAIN_NUMERATOR: usize = 1;
const SOURCE_SAFETY_POLICY: SourceSafetyPolicy = SourceSafetyPolicy {
    max_deleted_ratio_exclusive_numerator: MAX_DELETED_RATIO_EXCLUSIVE_NUMERATOR,
    max_deleted_ratio_denominator: MAX_DELETED_RATIO_DENOMINATOR,
    min_region_retain_denominator: MIN_REGION_RETAIN_DENOMINATOR,
    min_region_retain_numerator: MIN_REGION_RETAIN_NUMERATOR,
};
const DAYS_PER_100_YEARS_I64: i64 = 36_524;
const DAYS_PER_400_YEARS_I64: i64 = 146_097;
const DAYS_PER_4_YEARS_I64: i64 = 1_460;
const DAYS_PER_COMMON_YEAR_I64: i64 = 365;
const DAYS_UNTIL_UNIX_EPOCH_I64: i64 = 719_468;
const KST_OFFSET: Duration = Duration::from_hours(9);
const LEAP_YEAR_CENTURY_DIVISOR_I32: i32 = 100;
const LEAP_YEAR_DIVISOR_I32: i32 = 4;
const LEAP_YEAR_ERA_DIVISOR_I32: i32 = 400;
const MARCH_BASE_MONTH_OFFSET_I64: i64 = 3;
const MARCH_MONTH_THRESHOLD: u32 = 2;
const MONTH_TERM_DIVISOR_I64: i64 = 5;
const MONTH_TERM_MULTIPLIER_I64: i64 = 153;
const MONTH_TERM_OFFSET_I64: i64 = 2;
const PRE_MARCH_MONTH_OFFSET_I64: i64 = 9;
const SECS_PER_DAY_U64: u64 = 86_400;
struct KstDate {
    day: u32,
    month: u32,
    year: i32,
}
struct LoadedSource {
    index: HashMap<String, SourceRecord>,
    name: String,
    region_counts: [usize; TARGET_REGION_COUNT],
}
struct UpdatedWorkbook<'source> {
    book: StdWorkbook,
    master_update: MasterSheetUpdateResult<'source>,
}
struct SourceSafetyPolicy {
    max_deleted_ratio_denominator: usize,
    max_deleted_ratio_exclusive_numerator: usize,
    min_region_retain_denominator: usize,
    min_region_retain_numerator: usize,
}
struct SummaryRowDisplay<'row> {
    address: &'row str,
    name: &'row str,
    region: &'row str,
}
impl SourceSafetyPolicy {
    fn validate_deleted_ratio(&self, existing_count: usize, deleted_count: usize) -> Result<()> {
        if existing_count == 0 {
            return Err(err("현행화 대상 레코드를 찾지 못했습니다."));
        }
        let deleted_scaled = deleted_count
            .checked_mul(self.max_deleted_ratio_denominator)
            .ok_or_else(|| err("폐업 처리 비율 계산 중 overflow가 발생했습니다."))?;
        let limit_scaled = existing_count
            .checked_mul(self.max_deleted_ratio_exclusive_numerator)
            .ok_or_else(|| err("폐업 처리 한도 계산 중 overflow가 발생했습니다."))?;
        if deleted_scaled >= limit_scaled {
            return Err(err(format!(
                "폐업 처리 건수가 비정상적으로 많아 저장을 중단합니다: {deleted_count}건 / {existing_count}건"
            )));
        }
        Ok(())
    }
    fn validate_region_counts(
        &self,
        existing_counts: &[usize; TARGET_REGION_COUNT],
        source_counts: &[usize; TARGET_REGION_COUNT],
    ) -> Result<()> {
        for ((label, existing_count), source_count) in TARGET_REGION_LABELS
            .iter()
            .zip(existing_counts.iter())
            .zip(source_counts.iter())
        {
            if *source_count == 0 {
                return Err(err(format!(
                    "Opinet 소스에서 대상 지역 레코드를 찾지 못했습니다: {label}"
                )));
            }
            if *existing_count == 0 {
                continue;
            }
            let retained_scaled = source_count
                .checked_mul(self.min_region_retain_denominator)
                .ok_or_else(|| err("지역별 소스 건수 감소율 계산 중 overflow가 발생했습니다."))?;
            let required_scaled = existing_count
                .checked_mul(self.min_region_retain_numerator)
                .ok_or_else(|| err("지역별 기존 건수 감소율 계산 중 overflow가 발생했습니다."))?;
            if retained_scaled < required_scaled {
                return Err(err(format!(
                    "대상 지역 소스 건수가 기존 마스터 대비 비정상적으로 적어 저장을 중단합니다: {label} 기존 {existing_count}건 / 소스 {source_count}건"
                )));
            }
        }
        Ok(())
    }
}
pub(super) struct UpdateRun<'out> {
    pub master_path: &'out Path,
    pub out: &'out mut dyn Write,
    pub verify_saved_archive: bool,
}
impl UpdateRun<'_> {
    fn load_source(&mut self) -> Result<LoadedSource> {
        let source_path = SourceDownload {
            dir: Path::new("."),
            out: &mut *self.out,
        }
        .refresh_source()?;
        write_line(self.out, format_args!("Opinet 소스 파일 준비 완료"))?;
        let source_name = source_path
            .file_name()
            .and_then(|name| name.to_str())
            .map_or_else(|| source_path.display().to_string(), str::to_owned);
        let result =
            (|| -> Result<(HashMap<String, SourceRecord>, [usize; TARGET_REGION_COUNT])> {
                let source_records = SourceReader {
                    path: source_path.as_path(),
                }
                .read_xls_source()
                .map_err(|source_err| {
                    err_with_source(
                        path_context_message("소스 xls 파일 읽기 실패", &source_path),
                        source_err,
                    )
                })?;
                let mut map: HashMap<String, SourceRecord> = HashMap::new();
                let mut target_record_count = 0_usize;
                let mut region_counts = [0_usize; TARGET_REGION_COUNT];
                let mut target_region_scratch = String::new();
                for record in source_records {
                    if let Some(region_index) = target_region_index(
                        &record.region,
                        &record.address,
                        &mut target_region_scratch,
                    )? {
                        target_record_count = target_record_count
                            .checked_add(1)
                            .ok_or_else(|| err("대상 지역 소스 레코드 수 계산 실패"))?;
                        increment_target_region_count(
                            &mut region_counts,
                            region_index,
                            "소스 지역별 건수",
                        )?;
                        if map.len() == map.capacity() {
                            map.try_reserve(1).map_err(|source| {
                                err_with_source("소스 index 맵 추가 메모리 확보 실패", source)
                            })?;
                        }
                        let key = normalize_address_key(&record.address)?;
                        match map.entry(key) {
                            Entry::Vacant(vacant_entry) => {
                                vacant_entry.insert(record);
                            }
                            Entry::Occupied(occupied_entry) => {
                                let existing = occupied_entry.get();
                                return Err(err(format!(
                                    "Opinet 소스 주소 중복: address={}, existing={}, incoming={}",
                                    existing.address, existing.name, record.name
                                )));
                            }
                        }
                    }
                }
                if target_record_count == 0 {
                    return Err(err("Opinet 소스에서 대상 지역 레코드를 찾지 못했습니다."));
                }
                for (label, count) in TARGET_REGION_LABELS.iter().zip(region_counts.iter()) {
                    if *count == 0 {
                        return Err(err(format!(
                            "Opinet 소스에서 대상 지역 레코드를 찾지 못했습니다: {label}"
                        )));
                    }
                }
                Ok((map, region_counts))
            })();
        match fs::remove_file(&source_path) {
            Ok(()) => write_line_best_effort(self.out, format_args!("임시 소스 파일 정리 완료")),
            Err(source_err) => write_line_best_effort(
                self.out,
                format_args!(
                    "경고: 임시 소스 파일 정리 실패: {} ({source_err})",
                    source_path.display()
                ),
            ),
        }
        let (index, region_counts) = result?;
        Ok(LoadedSource {
            index,
            name: source_name,
            region_counts,
        })
    }
    fn open_updated_workbook<'source>(
        &mut self,
        loaded_source: &'source LoadedSource,
    ) -> Result<UpdatedWorkbook<'source>> {
        write_line(self.out, format_args!("마스터 파일 처리 중..."))?;
        let container = XlsxContainer::open(self.master_path)?;
        let mut book = StdWorkbook::from_container(container)?;
        let master_update = MasterSheetUpdater {
            source_index: &loaded_source.index,
        }
        .update(&mut book)?;
        self.print_region_count_summary(
            &master_update.existing_region_counts,
            &loaded_source.region_counts,
        )?;
        SOURCE_SAFETY_POLICY.validate_region_counts(
            &master_update.existing_region_counts,
            &loaded_source.region_counts,
        )?;
        SOURCE_SAFETY_POLICY
            .validate_deleted_ratio(master_update.existing_count, master_update.deleted.len())?;
        Ok(UpdatedWorkbook {
            book,
            master_update,
        })
    }
    fn print_added_rows(&mut self, title: &str, rows: &[AddedStoreRow<'_>]) -> Result<()> {
        self.print_summary_rows(
            title,
            rows,
            |item| SummaryRowDisplay {
                address: item.record.address.as_str(),
                name: item.record.name.as_str(),
                region: item.region,
            },
            "신규 업체 표시 번호 계산 실패",
        )
    }
    fn print_region_count_summary(
        &mut self,
        existing_counts: &[usize; TARGET_REGION_COUNT],
        source_counts: &[usize; TARGET_REGION_COUNT],
    ) -> Result<()> {
        write_line(self.out, format_args!("대상 지역별 건수 확인:"))?;
        for ((label, existing_count), source_count) in TARGET_REGION_LABELS
            .iter()
            .zip(existing_counts.iter())
            .zip(source_counts.iter())
        {
            write_line(
                self.out,
                format_args!("  {label}: 기존 {existing_count}건 / 소스 {source_count}건"),
            )?;
        }
        Ok(())
    }
    fn print_store_rows(&mut self, title: &str, rows: &[StoreRow]) -> Result<()> {
        self.print_summary_rows(
            title,
            rows,
            |item| SummaryRowDisplay {
                address: item.address.as_str(),
                name: item.name.as_str(),
                region: item.region.as_str(),
            },
            "폐업 업체 표시 번호 계산 실패",
        )
    }
    fn print_summary_rows<T, F>(
        &mut self,
        title: &str,
        rows: &[T],
        display_row: F,
        display_number_error: &'static str,
    ) -> Result<()>
    where
        F: for<'row> Fn(&'row T) -> SummaryRowDisplay<'row>,
    {
        if rows.is_empty() {
            return Ok(());
        }
        write_line(self.out, format_args!("\n{title}"))?;
        for (item_index, row) in rows.iter().take(20).enumerate() {
            let Some(display_index) = item_index.checked_add(1) else {
                return Err(err(display_number_error));
            };
            let item = display_row(row);
            write_line(
                self.out,
                format_args!(
                    "  {display_index}. {region} / {name} / {address}",
                    region = item.region,
                    name = item.name,
                    address = item.address
                ),
            )?;
        }
        if rows.len() > 20 {
            write_line(
                self.out,
                format_args!("  ... ({}개 중 20개만 표시)", rows.len()),
            )?;
        }
        Ok(())
    }
    fn print_update_summary<'source>(
        &mut self,
        source_name: &str,
        changes: &[ChangeRow<'source>],
        added: &[AddedStoreRow<'source>],
        deleted: &[StoreRow],
    ) -> Result<()> {
        write_line(self.out, format_args!("\n==== 현행화 요약 ===="))?;
        write_line(
            self.out,
            format_args!("- 파일: {}", self.master_path.display()),
        )?;
        write_line(self.out, format_args!("- 소스: {source_name}"))?;
        write_line(
            self.out,
            format_args!("- 기존 업체 변경: {}건", changes.len()),
        )?;
        write_line(
            self.out,
            format_args!("- 신규 업체 추가: {}건", added.len()),
        )?;
        write_line(
            self.out,
            format_args!("- 폐업 업체 삭제: {}건", deleted.len()),
        )?;
        let verification_state = if self.verify_saved_archive {
            "사용"
        } else {
            "생략"
        };
        write_line(self.out, format_args!("- 저장 검증: {verification_state}"))?;
        self.print_added_rows("신규 업체 추가 목록 (상위 20개)", added)?;
        self.print_store_rows("폐업 업체 삭제 목록 (상위 20개)", deleted)?;
        write_line(self.out, format_args!("=====================\n"))?;
        Ok(())
    }
    pub(super) fn run(&mut self) -> Result<()> {
        let loaded_source = self.load_source()?;
        let updated = self.open_updated_workbook(&loaded_source)?;
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|source| err_with_source("현재 시간 조회 실패", source))?;
        let kst_secs = since_epoch
            .as_secs()
            .checked_add(KST_OFFSET.as_secs())
            .ok_or_else(|| err("KST 날짜 초 계산 중 범위 오류가 발생했습니다."))?;
        let day_index_i64 = i64::try_from(kst_secs.div_euclid(SECS_PER_DAY_U64))
            .map_err(|source| err_with_source("KST 날짜 일수 변환에 실패했습니다.", source))?;
        let day_index = i32::try_from(day_index_i64)
            .map_err(|source| err_with_source("KST 날짜 범위 변환에 실패했습니다.", source))?;
        let KstDate { day, month, year } = (|| -> Result<KstDate> {
            let checked_i64 = |value: Option<i64>| {
                value.ok_or_else(|| err("KST 날짜 계산 중 범위 오류가 발생했습니다."))
            };
            let shifted_days =
                checked_i64(i64::from(day_index).checked_add(DAYS_UNTIL_UNIX_EPOCH_I64))?;
            let era = shifted_days.div_euclid(DAYS_PER_400_YEARS_I64);
            let doe = shifted_days.rem_euclid(DAYS_PER_400_YEARS_I64);
            let yoe_after_first =
                checked_i64(doe.checked_sub(checked_i64(doe.checked_div(DAYS_PER_4_YEARS_I64))?))?;
            let yoe_after_second = checked_i64(
                yoe_after_first.checked_add(checked_i64(doe.checked_div(DAYS_PER_100_YEARS_I64))?),
            )?;
            let yoe_numerator = checked_i64(yoe_after_second.checked_sub(checked_i64(
                doe.checked_div(DAYS_PER_400_YEARS_I64 - 1_i64),
            )?))?;
            let yoe = checked_i64(yoe_numerator.checked_div(DAYS_PER_COMMON_YEAR_I64))?;
            let y = checked_i64(yoe.checked_add(checked_i64(
                era.checked_mul(i64::from(LEAP_YEAR_ERA_DIVISOR_I32)),
            )?))?;
            let year_days = checked_i64(DAYS_PER_COMMON_YEAR_I64.checked_mul(yoe))?;
            let leap_days = checked_i64(yoe.checked_div(i64::from(LEAP_YEAR_DIVISOR_I32)))?;
            let skipped_centuries =
                checked_i64(yoe.checked_div(i64::from(LEAP_YEAR_CENTURY_DIVISOR_I32)))?;
            let year_start_days = checked_i64(
                year_days
                    .checked_add(leap_days)
                    .and_then(|value| value.checked_sub(skipped_centuries)),
            )?;
            let doy = checked_i64(doe.checked_sub(year_start_days))?;
            let mp = checked_i64(
                MONTH_TERM_DIVISOR_I64
                    .checked_mul(doy)
                    .and_then(|value| value.checked_add(MONTH_TERM_OFFSET_I64))
                    .and_then(|value| value.checked_div(MONTH_TERM_MULTIPLIER_I64)),
            )?;
            let month_term = checked_i64(
                MONTH_TERM_MULTIPLIER_I64
                    .checked_mul(mp)
                    .and_then(|value| value.checked_add(MONTH_TERM_OFFSET_I64))
                    .and_then(|value| value.checked_div(MONTH_TERM_DIVISOR_I64)),
            )?;
            let day_i64 = checked_i64(
                doy.checked_sub(month_term)
                    .and_then(|value| value.checked_add(1_i64)),
            )?;
            let day = u32::try_from(day_i64)
                .map_err(|source| err_with_source("KST 날짜 일 변환에 실패했습니다.", source))?;
            let raw_month_i64 = if mp < 10_i64 {
                mp.checked_add(MARCH_BASE_MONTH_OFFSET_I64)
            } else {
                mp.checked_sub(PRE_MARCH_MONTH_OFFSET_I64)
            };
            let month_i64 = checked_i64(raw_month_i64)?;
            let month = u32::try_from(month_i64)
                .map_err(|source| err_with_source("KST 날짜 월 변환에 실패했습니다.", source))?;
            let year_adjust = i64::from(month <= MARCH_MONTH_THRESHOLD);
            let year_i64 = checked_i64(y.checked_add(year_adjust))?;
            let year = i32::try_from(year_i64)
                .map_err(|source| err_with_source("KST 날짜 연도 변환에 실패했습니다.", source))?;
            Ok(KstDate { day, month, year })
        })()?;
        let today = format!("{year:04}-{month:02}-{day:02}");
        self.save_workbook_with_change_log(
            &loaded_source,
            &updated.master_update,
            updated.book,
            &today,
        )
    }
    fn save_workbook_with_change_log(
        &mut self,
        loaded_source: &LoadedSource,
        master_update: &MasterSheetUpdateResult<'_>,
        mut book: StdWorkbook,
        today: &str,
    ) -> Result<()> {
        let Some(()) = book.with_sheet_mut(
            "변경내역",
            |worksheet, shared_string_table| -> Result<()> {
                let mut updater = ChangeLogUpdater {
                    added: &master_update.added,
                    changes: &master_update.changes,
                    deleted: &master_update.deleted,
                    shared_string_table,
                    today,
                    worksheet,
                };
                updater.update()
            },
        )?
        else {
            return Err(err("마스터 파일에 '변경내역' 시트가 없습니다"));
        };
        write_line(self.out, format_args!("마스터 파일 저장 중..."))?;
        let save_verification = if self.verify_saved_archive {
            SaveVerification::Verify
        } else {
            SaveVerification::Skip
        };
        book.save(self.master_path, save_verification)?;
        if let Err(summary_err) = self.print_update_summary(
            &loaded_source.name,
            &master_update.changes,
            &master_update.added,
            &master_update.deleted,
        ) {
            write_line_best_effort(
                self.out,
                format_args!(
                    "마스터 파일은 저장됐지만 실행 요약 출력에 실패했습니다: {summary_err}"
                ),
            );
        }
        Ok(())
    }
}
