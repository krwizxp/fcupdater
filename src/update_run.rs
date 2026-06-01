use crate::{
    AddedStoreRow, ChangeLogUpdater, ChangeRow, MasterSheetUpdater, Result, SourceDownload,
    SourceRecord, StoreRow, UpdateRunContext, err, err_with_source,
    excel::{source_reader, writer::Workbook as StdWorkbook},
    io_util::write_line_ignored,
    path_source_message, prefixed_message,
    region::normalize_address_key,
    source_download,
};
use core::{result::Result as CoreResult, time::Duration};
use std::{
    collections::{HashMap, hash_map::Entry},
    fs,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
const MASTER_PATH: &str = "fuel_cost_chungcheong.xlsx";
const MAX_DELETED_RATIO_NUMERATOR: usize = 1;
const MAX_DELETED_RATIO_DENOMINATOR: usize = 2;
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
impl TryFrom<i32> for KstDate {
    type Error = ();
    fn try_from(day_index: i32) -> CoreResult<Self, Self::Error> {
        let Some((year, month, day)) = (|| {
            let shifted_days = i64::from(day_index).checked_add(DAYS_UNTIL_UNIX_EPOCH_I64)?;
            let era = shifted_days.div_euclid(DAYS_PER_400_YEARS_I64);
            let doe = shifted_days.rem_euclid(DAYS_PER_400_YEARS_I64);
            let yoe_after_first = doe.checked_sub(doe.checked_div(DAYS_PER_4_YEARS_I64)?)?;
            let yoe_after_second =
                yoe_after_first.checked_add(doe.checked_div(DAYS_PER_100_YEARS_I64)?)?;
            let yoe_numerator =
                yoe_after_second.checked_sub(doe.checked_div(DAYS_PER_400_YEARS_I64 - 1_i64)?)?;
            let yoe = yoe_numerator.checked_div(DAYS_PER_COMMON_YEAR_I64)?;
            let y = yoe.checked_add(era.checked_mul(i64::from(LEAP_YEAR_ERA_DIVISOR_I32))?)?;
            let year_days = DAYS_PER_COMMON_YEAR_I64.checked_mul(yoe)?;
            let leap_days = yoe.checked_div(i64::from(LEAP_YEAR_DIVISOR_I32))?;
            let skipped_centuries = yoe.checked_div(i64::from(LEAP_YEAR_CENTURY_DIVISOR_I32))?;
            let doy = doe.checked_sub(
                year_days
                    .checked_add(leap_days)?
                    .checked_sub(skipped_centuries)?,
            )?;
            let mp = MONTH_TERM_DIVISOR_I64
                .checked_mul(doy)?
                .checked_add(MONTH_TERM_OFFSET_I64)?
                .checked_div(MONTH_TERM_MULTIPLIER_I64)?;
            let month_term = MONTH_TERM_MULTIPLIER_I64
                .checked_mul(mp)?
                .checked_add(MONTH_TERM_OFFSET_I64)?
                .checked_div(MONTH_TERM_DIVISOR_I64)?;
            let day = u32::try_from(doy.checked_sub(month_term)?.checked_add(1_i64)?).ok()?;
            let month_i64 = if mp < 10_i64 {
                mp.checked_add(MARCH_BASE_MONTH_OFFSET_I64)?
            } else {
                mp.checked_sub(PRE_MARCH_MONTH_OFFSET_I64)?
            };
            let month = u32::try_from(month_i64).ok()?;
            let year_adjust = i64::from(month <= MARCH_MONTH_THRESHOLD);
            let year = i32::try_from(y.checked_add(year_adjust)?).ok()?;
            Some((year, month, day))
        })() else {
            return Err(());
        };
        Ok(Self { day, month, year })
    }
}
impl UpdateRunContext<'_> {
    fn load_source(&mut self) -> Result<(HashMap<String, SourceRecord>, String)> {
        let source_path = SourceDownload {
            dir: Path::new("."),
            out: &mut *self.out,
        }
        .refresh_source()?;
        write_line_ignored(self.out, format_args!("Opinet 소스 파일 준비 완료"));
        let source_name = source_path
            .file_name()
            .and_then(|name| name.to_str())
            .map_or_else(|| source_path.display().to_string(), str::to_owned);
        let result = (|| -> Result<HashMap<String, SourceRecord>> {
            let mut records = source_reader::SourceReader {
                path: source_path.as_path(),
            }
            .read_xls_source()
            .map_err(|source_err| {
                err(path_source_message(
                    "소스 xls 파일 읽기 실패",
                    &source_path,
                    source_err,
                ))
            })?;
            records.retain(|record| {
                let region_key = normalize_address_key(&record.region);
                source_download::TARGET_REGION_KEYS.contains(&region_key.as_str())
            });
            if records.is_empty() {
                return Err(err("Opinet 소스에서 대상 지역 레코드를 찾지 못했습니다."));
            }
            let mut map: HashMap<String, SourceRecord> = HashMap::new();
            map.try_reserve(records.len()).map_err(|source| {
                let record_count = records.len();
                err_with_source(
                    format!("소스 index 맵 메모리 확보 실패: {record_count} entries"),
                    source,
                )
            })?;
            for record in records {
                let key = normalize_address_key(&record.address);
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
            Ok(map)
        })();
        match fs::remove_file(&source_path) {
            Ok(()) => write_line_ignored(self.out, format_args!("임시 소스 파일 정리 완료")),
            Err(source_err) => write_line_ignored(
                self.out,
                format_args!(
                    "경고: 임시 소스 파일 정리 실패: {} ({source_err})",
                    source_path.display()
                ),
            ),
        }
        Ok((result?, source_name))
    }
    fn print_added_rows(&mut self, title: &str, rows: &[AddedStoreRow<'_>]) {
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
                    name = item.record.name,
                    address = item.record.address
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
    fn print_update_summary<'source>(
        &mut self,
        source_name: &str,
        changes: &[ChangeRow<'source>],
        added: &[AddedStoreRow<'source>],
        deleted: &[StoreRow],
    ) {
        write_line_ignored(self.out, format_args!("\n==== 현행화 요약 ===="));
        write_line_ignored(self.out, format_args!("- 파일: {MASTER_PATH}"));
        write_line_ignored(self.out, format_args!("- 소스: {source_name}"));
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
        write_line_ignored(self.out, format_args!("- 저장 검증: 사용"));
        self.print_added_rows("신규 업체 추가 목록 (상위 20개)", added);
        self.print_store_rows("폐업 업체 삭제 목록 (상위 20개)", deleted);
        write_line_ignored(self.out, format_args!("=====================\n"));
    }
    pub(crate) fn run_update(&mut self) -> Result<()> {
        let master_path = Path::new(MASTER_PATH);
        if !master_path.try_exists().map_err(|source_err| {
            err(path_source_message(
                "마스터 파일 경로 확인 실패",
                master_path,
                source_err,
            ))
        })? {
            return Err(err(prefixed_message(
                "마스터 파일이 없습니다: ",
                MASTER_PATH,
            )));
        }
        let (source_index, source_name) = self.load_source()?;
        write_line_ignored(self.out, format_args!("마스터 파일 처리 중..."));
        let mut book = StdWorkbook::open(master_path)?;
        let (changes, added, deleted) = MasterSheetUpdater {
            source_index: &source_index,
        }
        .update(&mut book)?;
        let total_considered = source_index.len().saturating_add(deleted.len());
        if total_considered == 0 {
            return Err(err("현행화 대상 레코드를 찾지 못했습니다."));
        }
        let deleted_scaled = deleted
            .len()
            .checked_mul(MAX_DELETED_RATIO_DENOMINATOR)
            .ok_or_else(|| err("폐업 처리 비율 계산 중 overflow가 발생했습니다."))?;
        let limit_scaled = total_considered
            .checked_mul(MAX_DELETED_RATIO_NUMERATOR)
            .ok_or_else(|| err("폐업 처리 한도 계산 중 overflow가 발생했습니다."))?;
        if deleted_scaled >= limit_scaled {
            return Err(err(format!(
                "폐업 처리 건수가 비정상적으로 많아 저장을 중단합니다: {}건 / {}건",
                deleted.len(),
                total_considered
            )));
        }
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
        let KstDate { day, month, year } = KstDate::try_from(day_index)
            .map_err(|()| err("KST 날짜 계산 중 범위 오류가 발생했습니다."))?;
        let today = format!("{year:04}-{month:02}-{day:02}");
        if !is_yyyy_mm_dd(&today) {
            return Err(err(prefixed_message(
                "오늘 날짜 형식이 올바르지 않습니다: ",
                &today,
            )));
        }
        book.with_sheet_mut(
            "변경내역",
            |worksheet, shared_string_table| -> Result<()> {
                let mut updater = ChangeLogUpdater {
                    added: &added,
                    changes: &changes,
                    deleted: &deleted,
                    shared_string_table,
                    today: &today,
                    worksheet,
                };
                updater.update()
            },
        )
        .ok_or_else(|| err("마스터 파일에 '변경내역' 시트가 없습니다"))??;
        write_line_ignored(self.out, format_args!("마스터 파일 저장 중..."));
        book.save(master_path)?;
        self.print_update_summary(&source_name, &changes, &added, &deleted);
        Ok(())
    }
}
