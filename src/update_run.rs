use crate::{
    change_log::ChangeLogUpdater,
    diagnostic::{Result, err, err_with_source, path_context_message, terminal_safe},
    excel::{SaveVerification, SourceReader, SourceRecord, SourceRecordRef},
    excel::{writer::Workbook as StdWorkbook, xlsx_container::XlsxContainer},
    master_sheet::{ChangeRow, MasterSheetUpdateResult, MasterSheetUpdater, StoreRow},
    region::{
        TARGET_REGION_COUNT, TARGET_REGIONS, TargetRegion, TargetRegionPolicy,
        increment_target_region_count, normalize_address_key, target_region,
    },
    source_download::SourceDownload,
    write_line, write_line_best_effort,
};
use core::time::Duration;
use std::{
    collections::{HashMap, hash_map::Entry},
    io::Write,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
const HALF_COUNT_DIVISOR: usize = 2;
const KST_OFFSET: Duration = Duration::from_hours(9);
const SECS_PER_DAY_U64: u64 = 86_400;
struct LoadedSource {
    index: HashMap<String, SourceRecord>,
    name: String,
    region_counts: [usize; TARGET_REGION_COUNT],
}
#[derive(Default)]
struct SourceIndexBuilder {
    map: HashMap<String, SourceRecord>,
    region_counts: [usize; TARGET_REGION_COUNT],
}
impl SourceIndexBuilder {
    fn finish(self) -> Result<(HashMap<String, SourceRecord>, [usize; TARGET_REGION_COUNT])> {
        let target_record_count = self.map.len();
        if target_record_count == 0 {
            return Err(err("Opinet 소스에서 대상 지역 레코드를 찾지 못했습니다."));
        }
        for (region, count) in TARGET_REGIONS.iter().zip(self.region_counts.iter()) {
            if *count == 0 {
                return Err(err(format!(
                    "Opinet 소스에서 대상 지역 레코드를 찾지 못했습니다: {}",
                    region.label(),
                )));
            }
        }
        let required_populated_count = target_record_count.div_ceil(HALF_COUNT_DIVISOR);
        let validate_field_ratio = |populated_count: usize, label: &'static str| -> Result<()> {
            if populated_count < required_populated_count {
                return Err(err(format!(
                    "Opinet 소스의 대상 지역 {label} 값이 비정상적으로 부족합니다: {populated_count}건 / {target_record_count}건"
                )));
            }
            Ok(())
        };
        let (brand_count, diesel_count, gasoline_count, has_premium) = self.map.values().fold(
            (0_usize, 0_usize, 0_usize, false),
            |(brand_count, diesel_count, gasoline_count, has_premium), record| {
                (
                    brand_count.saturating_add(usize::from(!record.brand.is_empty())),
                    diesel_count.saturating_add(usize::from(record.diesel.is_some())),
                    gasoline_count.saturating_add(usize::from(record.gasoline.is_some())),
                    has_premium || record.premium.is_some(),
                )
            },
        );
        validate_field_ratio(brand_count, "상표")?;
        validate_field_ratio(diesel_count, "경유 가격")?;
        validate_field_ratio(gasoline_count, "휘발유 가격")?;
        if !has_premium {
            return Err(err(
                "Opinet 소스의 대상 지역에서 유효한 고급휘발유 가격을 찾지 못했습니다.",
            ));
        }
        Ok((self.map, self.region_counts))
    }
    fn insert(
        &mut self,
        key: String,
        borrowed_record: SourceRecordRef<'_>,
        region: TargetRegion,
    ) -> Result<()> {
        if self.map.len() == self.map.capacity() {
            self.map
                .try_reserve(1)
                .map_err(|source| err_with_source("소스 index 맵 추가 메모리 확보 실패", source))?;
        }
        match self.map.entry(key) {
            Entry::Vacant(vacant_entry) => {
                vacant_entry.insert(borrowed_record.into_owned_with_region(region.label())?);
                increment_target_region_count(&mut self.region_counts, region, "소스 지역별 건수")
            }
            Entry::Occupied(occupied_entry) => {
                let existing = occupied_entry.get();
                Err(err(format!(
                    "Opinet 소스 주소 중복: address={}, existing={}, incoming={}",
                    existing.address, existing.name, borrowed_record.name
                )))
            }
        }
    }
}
pub(super) struct UpdateRun<'out> {
    pub master_path: &'out Path,
    pub out: &'out mut dyn Write,
    pub save_verification: SaveVerification,
}
impl UpdateRun<'_> {
    fn load_source(&mut self) -> Result<LoadedSource> {
        let mut source_file = SourceDownload {
            dir: Path::new("."),
            out: &mut *self.out,
        }
        .refresh_source()?;
        write_line(self.out, format_args!("Opinet 소스 파일 준비 완료"))?;
        let source_name = source_file
            .path()
            .file_name()
            .and_then(|name| name.to_str())
            .map_or_else(|| source_file.path().display().to_string(), str::to_owned);
        let result =
            (|| -> Result<(HashMap<String, SourceRecord>, [usize; TARGET_REGION_COUNT])> {
                let mut source_index = SourceIndexBuilder::default();
                let mut target_region_scratch = String::new();
                let (source_handle, source_path) =
                    source_file.reader_parts().map_err(|source_err| {
                        err_with_source("다운로드 소스 파일 상태 손상", source_err)
                    })?;
                let source_index_result = SourceReader {
                    file: source_handle,
                    path: source_path,
                }
                .visit_xls_source(|borrowed_record| {
                    if let Some(region) = target_region(
                        borrowed_record.region,
                        borrowed_record.address,
                        &mut target_region_scratch,
                        TargetRegionPolicy::StrictSource,
                    )? {
                        let key = normalize_address_key(borrowed_record.address)?;
                        source_index.insert(key, borrowed_record, region)?;
                    }
                    Ok(())
                })
                .map_err(|source_err| {
                    err_with_source(
                        path_context_message("소스 xls 파일 읽기 실패", source_path),
                        source_err,
                    )
                })?;
                source_index_result?;
                source_index.finish()
            })();
        match source_file.remove() {
            Ok(()) => write_line_best_effort(self.out, format_args!("임시 소스 파일 정리 완료")),
            Err(source_err) => write_line_best_effort(
                self.out,
                format_args!(
                    "경고: 임시 소스 파일 정리 실패: {} ({source_err})",
                    source_file.path().display()
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
    ) -> Result<(StdWorkbook, MasterSheetUpdateResult<'source>)> {
        write_line(self.out, format_args!("마스터 파일 처리 중..."))?;
        let container = XlsxContainer::open(self.master_path)?;
        let mut book = StdWorkbook::from_container(container)?;
        let master_update = MasterSheetUpdater {
            source_index: &loaded_source.index,
        }
        .update(&mut book)?;
        self.print_region_count_summary(
            &master_update.existing_region_counts,
            &master_update.matched_existing_region_counts,
            &loaded_source.region_counts,
        )?;
        for (((region, existing_count), matched_existing_count), source_count) in TARGET_REGIONS
            .iter()
            .zip(master_update.existing_region_counts.iter())
            .zip(master_update.matched_existing_region_counts.iter())
            .zip(loaded_source.region_counts.iter())
        {
            let label = region.label();
            if *existing_count == 0 {
                continue;
            }
            if *matched_existing_count < existing_count.div_ceil(HALF_COUNT_DIVISOR) {
                return Err(err(format!(
                    "대상 지역의 기존 주소 일치 건수가 비정상적으로 적어 저장을 중단합니다: {label} 기존 {existing_count}건 / 기존 주소 일치 {matched_existing_count}건 / 소스 {source_count}건"
                )));
            }
        }
        if master_update.existing_count == 0 {
            return Err(err("현행화 대상 레코드를 찾지 못했습니다."));
        }
        let deleted_count = master_update.deleted.len();
        if deleted_count >= master_update.existing_count.div_ceil(HALF_COUNT_DIVISOR) {
            return Err(err(format!(
                "폐업 처리 건수가 비정상적으로 많아 저장을 중단합니다: {deleted_count}건 / {}건",
                master_update.existing_count
            )));
        }
        Ok((book, master_update))
    }
    fn print_region_count_summary(
        &mut self,
        existing_counts: &[usize; TARGET_REGION_COUNT],
        matched_existing_counts: &[usize; TARGET_REGION_COUNT],
        source_counts: &[usize; TARGET_REGION_COUNT],
    ) -> Result<()> {
        write_line(self.out, format_args!("대상 지역별 건수 확인:"))?;
        for (((region, existing_count), matched_existing_count), source_count) in TARGET_REGIONS
            .iter()
            .zip(existing_counts.iter())
            .zip(matched_existing_counts.iter())
            .zip(source_counts.iter())
        {
            let label = region.label();
            write_line(
                self.out,
                format_args!(
                    "  {label}: 기존 {existing_count}건 / 기존 주소 일치 {matched_existing_count}건 / 소스 {source_count}건"
                ),
            )?;
        }
        Ok(())
    }
    fn print_summary_rows<'row>(
        &mut self,
        title: &str,
        rows: impl ExactSizeIterator<Item = (&'row str, &'row str, &'row str)>,
    ) -> Result<()> {
        let row_count = rows.len();
        if row_count == 0 {
            return Ok(());
        }
        write_line(self.out, format_args!("\n{title}"))?;
        for (display_index, (region, name, address)) in (1_usize..=20).zip(rows) {
            write_line(
                self.out,
                format_args!(
                    "  {display_index}. {region} / {name} / {address}",
                    region = terminal_safe(region),
                    name = terminal_safe(name),
                    address = terminal_safe(address)
                ),
            )?;
        }
        if row_count > 20 {
            write_line(
                self.out,
                format_args!("  ... ({row_count}개 중 20개만 표시)"),
            )?;
        }
        Ok(())
    }
    fn print_update_summary<'source>(
        &mut self,
        source_name: &str,
        changes: &[ChangeRow<'source>],
        added: &[&'source SourceRecord],
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
        let verification_state = match self.save_verification {
            SaveVerification::Verify => "사용",
            SaveVerification::Skip => "생략",
        };
        write_line(self.out, format_args!("- 저장 검증: {verification_state}"))?;
        self.print_summary_rows(
            "신규 업체 추가 목록 (상위 20개)",
            added
                .iter()
                .map(|item| (item.region, item.name.as_str(), item.address.as_str())),
        )?;
        self.print_summary_rows(
            "폐업 업체 삭제 목록 (상위 20개)",
            deleted.iter().map(|item| {
                (
                    item.region.as_str(),
                    item.name.as_str(),
                    item.address.as_str(),
                )
            }),
        )?;
        write_line(self.out, format_args!("=====================\n"))?;
        Ok(())
    }
    pub(super) fn run(&mut self) -> Result<()> {
        let loaded_source = self.load_source()?;
        let (book, master_update) = self.open_updated_workbook(&loaded_source)?;
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|source| err_with_source("현재 시간 조회 실패", source))?;
        let kst_secs = since_epoch
            .as_secs()
            .checked_add(KST_OFFSET.as_secs())
            .ok_or_else(|| err("KST 날짜 초 계산 중 범위 오류가 발생했습니다."))?;
        let shifted_days =
            i128::from(kst_secs.div_euclid(SECS_PER_DAY_U64)).saturating_add(719_468);
        let era = shifted_days.div_euclid(146_097);
        let day_of_era = shifted_days.rem_euclid(146_097);
        let year_of_era = day_of_era
            .saturating_sub(day_of_era.div_euclid(1_460))
            .saturating_add(day_of_era.div_euclid(36_524))
            .saturating_sub(day_of_era.div_euclid(146_096))
            .div_euclid(365);
        let year_base = year_of_era.saturating_add(era.saturating_mul(400));
        let year_start = 365_i128
            .saturating_mul(year_of_era)
            .saturating_add(year_of_era.div_euclid(4))
            .saturating_sub(year_of_era.div_euclid(100));
        let day_of_year = day_of_era.saturating_sub(year_start);
        let march_month = 5_i128
            .saturating_mul(day_of_year)
            .saturating_add(2)
            .div_euclid(153);
        let day = day_of_year
            .saturating_sub(
                153_i128
                    .saturating_mul(march_month)
                    .saturating_add(2)
                    .div_euclid(5),
            )
            .saturating_add(1);
        let month = if march_month < 10 {
            march_month.saturating_add(3)
        } else {
            march_month.saturating_sub(9)
        };
        let year = if month <= 2 {
            year_base.saturating_add(1)
        } else {
            year_base
        };
        let today = format!("{year:04}-{month:02}-{day:02}");
        self.save_workbook_with_change_log(&loaded_source, &master_update, book, &today)
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
        book.save(self.master_path, self.save_verification)?;
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
