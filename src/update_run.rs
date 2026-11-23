use crate::{
    change_log::ChangeLogUpdater,
    diagnostic::{
        Result, bail_fmt, ensure, ensure_fmt, err_src, fmt_err, path_err, reserve, terminal_safe,
    },
    excel::{
        SaveVerification, SourceReader, SourceRecord, writer::Workbook as StdWorkbook,
        xlsx_container::XlsxContainer,
    },
    master_sheet::{ChangeRow, MasterSheetUpdateResult, MasterSheetUpdater, StoreRow},
    region::{
        TARGET_REGION_COUNT, TARGET_REGIONS, TargetRegionPolicy, increment_target_region_count,
        normalize_address_key_into, target_region,
    },
    source_download::SourceDownload,
    temp_entry::open_regular,
};
use core::time::Duration;
use std::{
    collections::{HashMap, hash_map::Entry},
    io::Write,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
struct LoadedSource {
    index: HashMap<String, SourceRecord>,
    region_counts: [usize; TARGET_REGION_COUNT],
}
pub(super) struct UpdateRun<'out> {
    pub master_path: &'out Path,
    pub out: &'out mut dyn Write,
    pub save_verification: SaveVerification,
}
impl UpdateRun<'_> {
    fn load_source(&mut self) -> Result<LoadedSource> {
        let source_data = SourceDownload::default().refresh_source()?;
        writeln!(self.out, "Opinet 소스 데이터 준비 완료")?;
        let mut loaded = LoadedSource {
            index: HashMap::new(),
            region_counts: [0; TARGET_REGION_COUNT],
        };
        let mut scratch = String::new();
        SourceReader::from(source_data).visit_rows(|record| {
            if let Some(region) = target_region(
                record.region,
                record.address,
                &mut scratch,
                TargetRegionPolicy::StrictSource,
            )? {
                let mut key = String::new();
                normalize_address_key_into(record.address, &mut key)?;
                if loaded.index.len() == loaded.index.capacity() {
                    reserve(loaded.index.try_reserve(256), "소스 index 맵")?;
                }
                match loaded.index.entry(key) {
                    Entry::Vacant(entry) => {
                        entry.insert(record.into_owned_with_region(region.label())?);
                        increment_target_region_count(&mut loaded.region_counts, region);
                    }
                    Entry::Occupied(entry) => {
                        let ex = entry.get();
                        bail_fmt!(
                            "Opinet 소스 주소 중복: address={}, existing={}, incoming={}",
                            ex.address,
                            ex.name,
                            record.name
                        );
                    }
                }
            }
            Ok(())
        })?;
        let total = loaded.index.len();
        ensure(
            total != 0,
            "Opinet 소스에서 대상 지역 레코드를 찾지 못했습니다.",
        )?;
        for (region, &count) in TARGET_REGIONS.iter().zip(&loaded.region_counts) {
            ensure_fmt!(
                count != 0,
                "Opinet 대상 지역 레코드 없음: {}",
                region.label()
            );
        }
        let required = total.div_ceil(2);
        let (brands, diesels, gasolines, has_prem) = loaded.index.values().fold(
            (0_usize, 0_usize, 0_usize, false),
            |(br, ds, gs, pr), rec| {
                (
                    br.strict_add(usize::from(!rec.brand.is_empty())),
                    ds.strict_add(usize::from(rec.fuels.diesel.is_some())),
                    gs.strict_add(usize::from(rec.fuels.gasoline.is_some())),
                    pr || rec.fuels.premium.is_some(),
                )
            },
        );
        for (pop, label) in [
            (brands, "상표"),
            (diesels, "경유 가격"),
            (gasolines, "휘발유 가격"),
        ] {
            ensure_fmt!(
                pop >= required,
                "Opinet 대상 지역 {label} 값 부족: {pop}건 / {total}건"
            );
        }
        ensure(has_prem, "Opinet 대상 지역 유효 고급휘발유 가격 없음")?;
        Ok(loaded)
    }
    fn open_updated_workbook<'source>(
        &mut self,
        loaded: &'source LoadedSource,
    ) -> Result<(StdWorkbook, MasterSheetUpdateResult<'source>)> {
        writeln!(self.out, "마스터 파일 처리 중...")?;
        let master_file = open_regular(self.master_path, false)
            .map_err(|src| path_err("마스터 xlsx 열기 실패", self.master_path, src))?;
        let mut book = StdWorkbook::from_container(XlsxContainer::from_validated_file(
            master_file,
            self.master_path,
        )?)?;
        let update = MasterSheetUpdater {
            source_index: &loaded.index,
        }
        .update(&mut book)?;
        writeln!(self.out, "대상 지역별 건수 확인:")?;
        let mut reg_err = None;
        for (((region, &existing), &matched), &src_cnt) in TARGET_REGIONS
            .iter()
            .zip(&update.existing_region_counts)
            .zip(&update.matched_existing_region_counts)
            .zip(&loaded.region_counts)
        {
            let label = region.label();
            writeln!(
                self.out,
                "  {label}: 기존 {existing}건 / 기존 주소 일치 {matched}건 / 소스 {src_cnt}건"
            )?;
            if reg_err.is_none() && existing != 0 && matched < existing.div_ceil(2) {
                reg_err = Some(fmt_err!(
                    "대상 지역 기존 주소 일치 부족으로 저장 중단: {label} 기존 {existing}건 / 일치 {matched}건 / 소스 {src_cnt}건"
                ));
            }
        }
        if let Some(error) = reg_err {
            return Err(error);
        }
        ensure(update.existing_count != 0, "현행화 대상 레코드 없음")?;
        ensure_fmt!(
            update.deleted.len() < update.existing_count.div_ceil(2),
            "폐업 처리 건수 과다로 저장 중단: {}건 / {}건",
            update.deleted.len(),
            update.existing_count
        );
        Ok((book, update))
    }
    fn print_summary_rows<'row>(
        &mut self,
        title: &str,
        rows: &mut dyn ExactSizeIterator<Item = (&'row str, &'row str, &'row str)>,
    ) -> Result<()> {
        let count = rows.len();
        if count == 0 {
            return Ok(());
        }
        writeln!(self.out, "\n{title}")?;
        for (idx, (reg, name, addr)) in (1_usize..=20).zip(rows) {
            writeln!(
                self.out,
                "  {idx}. {} / {} / {}",
                terminal_safe(reg),
                terminal_safe(name),
                terminal_safe(addr)
            )?;
        }
        if count > 20 {
            writeln!(self.out, "  ... ({count}개 중 20개만 표시)")?;
        }
        Ok(())
    }
    fn print_update_summary<'source>(
        &mut self,
        changes: &[ChangeRow<'source>],
        added: &[&'source SourceRecord],
        deleted: &[StoreRow],
    ) -> Result<()> {
        let verify = match self.save_verification {
            SaveVerification::Verify => "사용",
            SaveVerification::Skip => "생략",
        };
        writeln!(
            self.out,
            "\n==== 현행화 요약 ====\n- 파일: {}\n- 소스: Opinet 자동 다운로드\n- 기존 업체 변경: {}건\n- 신규 업체 추가: {}건\n- 폐업 업체 삭제: {}건\n- 저장 검증: {verify}",
            self.master_path.display(),
            changes.len(),
            added.len(),
            deleted.len(),
        )?;
        self.print_summary_rows(
            "신규 업체 추가 목록 (상위 20개)",
            &mut added
                .iter()
                .map(|it| (it.region, it.name.as_str(), it.address.as_str())),
        )?;
        self.print_summary_rows(
            "폐업 업체 삭제 목록 (상위 20개)",
            &mut deleted
                .iter()
                .map(|it| (it.region.as_str(), it.name.as_str(), it.address.as_str())),
        )?;
        writeln!(self.out, "=====================\n").map_err(Into::into)
    }
    pub(super) fn run(&mut self) -> Result<()> {
        let loaded = self.load_source()?;
        let (mut book, update) = self.open_updated_workbook(&loaded)?;
        let kst = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|src| err_src("현재 시간 조회 실패", src))?
            .checked_add(Duration::from_hours(9))
            .ok_or("KST 날짜 계산 오류")?;
        let shifted = kst.as_secs().div_euclid(86_400).strict_add(719_468);
        let (era, doe) = (shifted.div_euclid(146_097), shifted.rem_euclid(146_097));
        let yoe = doe
            .strict_sub(doe.div_euclid(1_460))
            .strict_add(doe.div_euclid(36_524))
            .strict_sub(doe.div_euclid(146_096))
            .div_euclid(365);
        let doy = doe.strict_sub(
            365_u64
                .strict_mul(yoe)
                .strict_add(yoe.div_euclid(4))
                .strict_sub(yoe.div_euclid(100)),
        );
        let mp = 5_u64.strict_mul(doy).strict_add(2).div_euclid(153);
        let day = doy
            .strict_sub(153_u64.strict_mul(mp).strict_add(2).div_euclid(5))
            .strict_add(1);
        let month = mp.strict_add(2).rem_euclid(12).strict_add(1);
        let year = yoe
            .strict_add(era.strict_mul(400))
            .strict_add(mp.div_euclid(10));
        let today = format!("{year:04}-{month:02}-{day:02}");
        let (worksheet, shared_string_table) = book.change_log_sheet_mut();
        let cl_last = ChangeLogUpdater {
            added: &update.added,
            changes: &update.changes,
            deleted: &update.deleted,
            shared_string_table,
            today: &today,
            worksheet,
        }
        .update()?;
        writeln!(self.out, "마스터 파일 저장 중...")?;
        book.save(
            self.master_path,
            self.save_verification,
            update.last_data_row,
            cl_last,
        )?;
        self.print_update_summary(&update.changes, &update.added, &update.deleted)
            .map_err(|src| err_src("마스터 파일은 저장됐지만 요약 출력 실패", src))
    }
}
