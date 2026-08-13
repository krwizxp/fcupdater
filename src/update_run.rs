use crate::{
    change_log::ChangeLogUpdater,
    diagnostic::{Result, err, err_with_source, path_context_message, terminal_safe},
    excel::{SaveVerification, SourceReader, SourceRecord},
    excel::{writer::Workbook as StdWorkbook, xlsx_container::XlsxContainer},
    master_sheet::{ChangeRow, MasterSheetUpdateResult, MasterSheetUpdater, StoreRow},
    region::{
        TARGET_REGION_COUNT, TARGET_REGIONS, TargetRegionPolicy, increment_target_region_count,
        normalize_address_key_into, target_region,
    },
    source_download::SourceDownload,
    temp_entry::open_regular,
    write_line,
};
use core::{mem, time::Duration};
use std::{
    collections::{HashMap, hash_map::Entry},
    io::Write,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
const HALF_COUNT_DIVISOR: usize = 2;
const KST_OFFSET: Duration = Duration::from_hours(9);
const SECS_PER_DAY_U64: u64 = 86_400;
const SOURCE_INDEX_GROWTH: usize = 256;
struct LoadedSource {
    index: HashMap<String, SourceRecord>,
    region_counts: [usize; TARGET_REGION_COUNT],
}
impl LoadedSource {
    fn finish_validation(&self) -> Result<()> {
        let target_record_count = self.index.len();
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
        let (brand_count, diesel_count, gasoline_count, has_premium) = self.index.values().fold(
            (0_usize, 0_usize, 0_usize, false),
            |(brand_count, diesel_count, gasoline_count, has_premium), record| {
                (
                    brand_count.strict_add(usize::from(!record.brand.is_empty())),
                    diesel_count.strict_add(usize::from(record.fuels.diesel.is_some())),
                    gasoline_count.strict_add(usize::from(record.fuels.gasoline.is_some())),
                    has_premium || record.fuels.premium.is_some(),
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
        Ok(())
    }
}
pub(super) struct UpdateRun<'out> {
    pub master_path: &'out Path,
    pub out: &'out mut dyn Write,
    pub save_verification: SaveVerification,
}
impl UpdateRun<'_> {
    fn load_source(&mut self) -> Result<LoadedSource> {
        let source_data = SourceDownload::default().refresh_source()?;
        write_line(self.out, format_args!("Opinet 소스 데이터 준비 완료"))?;
        let mut loaded_source = LoadedSource {
            index: HashMap::new(),
            region_counts: [0; TARGET_REGION_COUNT],
        };
        let mut address_key_scratch = String::new();
        let mut target_region_scratch = String::new();
        SourceReader::from(source_data).visit_rows(|borrowed_record| {
            if let Some(region) = target_region(
                borrowed_record.region,
                borrowed_record.address,
                &mut target_region_scratch,
                TargetRegionPolicy::StrictSource,
            )? {
                normalize_address_key_into(borrowed_record.address, &mut address_key_scratch)?;
                let key = mem::take(&mut address_key_scratch);
                if loaded_source.index.len() == loaded_source.index.capacity() {
                    loaded_source
                        .index
                        .try_reserve(SOURCE_INDEX_GROWTH)
                        .map_err(|source| {
                            err_with_source("소스 index 맵 추가 메모리 확보 실패", source)
                        })?;
                }
                match loaded_source.index.entry(key) {
                    Entry::Vacant(entry) => {
                        entry.insert(borrowed_record.into_owned_with_region(region.label())?);
                        increment_target_region_count(&mut loaded_source.region_counts, region);
                    }
                    Entry::Occupied(entry) => {
                        let existing = entry.get();
                        return Err(err(format!(
                            "Opinet 소스 주소 중복: address={}, existing={}, incoming={}",
                            existing.address, existing.name, borrowed_record.name
                        )));
                    }
                }
            }
            Ok(())
        })?;
        loaded_source.finish_validation()?;
        Ok(loaded_source)
    }
    fn open_updated_workbook<'source>(
        &mut self,
        loaded_source: &'source LoadedSource,
    ) -> Result<(StdWorkbook, MasterSheetUpdateResult<'source>)> {
        write_line(self.out, format_args!("마스터 파일 처리 중..."))?;
        let master_file = open_regular(self.master_path, false).map_err(|source| {
            err_with_source(
                path_context_message("마스터 xlsx 파일 열기 실패", self.master_path),
                source,
            )
        })?;
        let container = XlsxContainer::from_validated_file(master_file, self.master_path)?;
        let mut book = StdWorkbook::from_container(container)?;
        let master_update = MasterSheetUpdater {
            source_index: &loaded_source.index,
        }
        .update(&mut book)?;
        write_line(self.out, format_args!("대상 지역별 건수 확인:"))?;
        let mut region_validation_error = None;
        for (((region, existing_count), matched_existing_count), source_count) in TARGET_REGIONS
            .iter()
            .zip(master_update.existing_region_counts.iter())
            .zip(master_update.matched_existing_region_counts.iter())
            .zip(loaded_source.region_counts.iter())
        {
            let label = region.label();
            write_line(
                self.out,
                format_args!(
                    "  {label}: 기존 {existing_count}건 / 기존 주소 일치 {matched_existing_count}건 / 소스 {source_count}건"
                ),
            )?;
            if region_validation_error.is_none()
                && *existing_count != 0
                && *matched_existing_count < existing_count.div_ceil(HALF_COUNT_DIVISOR)
            {
                region_validation_error = Some(format!(
                    "대상 지역의 기존 주소 일치 건수가 비정상적으로 적어 저장을 중단합니다: {label} 기존 {existing_count}건 / 기존 주소 일치 {matched_existing_count}건 / 소스 {source_count}건"
                ));
            }
        }
        if let Some(message) = region_validation_error {
            return Err(err(message));
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
        changes: &[ChangeRow<'source>],
        added: &[&'source SourceRecord],
        deleted: &[StoreRow],
    ) -> Result<()> {
        write_line(self.out, format_args!("\n==== 현행화 요약 ===="))?;
        write_line(
            self.out,
            format_args!("- 파일: {}", self.master_path.display()),
        )?;
        write_line(self.out, format_args!("- 소스: Opinet 자동 다운로드"))?;
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
        let (mut book, master_update) = self.open_updated_workbook(&loaded_source)?;
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|source| err_with_source("현재 시간 조회 실패", source))?;
        let kst = since_epoch
            .checked_add(KST_OFFSET)
            .ok_or_else(|| err("KST 날짜 초 계산 중 범위 오류가 발생했습니다."))?;
        let shifted_days = kst
            .as_secs()
            .div_euclid(SECS_PER_DAY_U64)
            .strict_add(719_468);
        let era = shifted_days.div_euclid(146_097);
        let day_of_era = shifted_days.rem_euclid(146_097);
        let year_of_era = day_of_era
            .strict_sub(day_of_era.div_euclid(1_460))
            .strict_add(day_of_era.div_euclid(36_524))
            .strict_sub(day_of_era.div_euclid(146_096))
            .div_euclid(365);
        let year_base = year_of_era.strict_add(era.strict_mul(400));
        let day_of_year = day_of_era.strict_sub(
            365_u64
                .strict_mul(year_of_era)
                .strict_add(year_of_era.div_euclid(4))
                .strict_sub(year_of_era.div_euclid(100)),
        );
        let march_month = 5_u64.strict_mul(day_of_year).strict_add(2).div_euclid(153);
        let day = day_of_year
            .strict_sub(153_u64.strict_mul(march_month).strict_add(2).div_euclid(5))
            .strict_add(1);
        let month = if march_month < 10 {
            march_month.strict_add(3)
        } else {
            march_month.strict_sub(9)
        };
        let year = if month <= 2 {
            year_base.strict_add(1)
        } else {
            year_base
        };
        let today = format!("{year:04}-{month:02}-{day:02}");
        let (worksheet, shared_string_table) = book.change_log_sheet_mut();
        let change_log_last_row = ChangeLogUpdater {
            added: &master_update.added,
            changes: &master_update.changes,
            deleted: &master_update.deleted,
            shared_string_table,
            today: &today,
            worksheet,
        }
        .update()?;
        write_line(self.out, format_args!("마스터 파일 저장 중..."))?;
        book.save(
            self.master_path,
            self.save_verification,
            master_update.last_data_row,
            change_log_last_row,
        )?;
        self.print_update_summary(
            &master_update.changes,
            &master_update.added,
            &master_update.deleted,
        )
        .map_err(|source| {
            err_with_source(
                "마스터 파일은 저장됐지만 실행 요약 출력에 실패했습니다.",
                source,
            )
        })
    }
}
