use crate::{
    Result, change_log, err, err_with_source,
    excel::{source_reader, writer::Workbook as StdWorkbook},
    io_util::write_line_ignored,
    kst_date::{KST_OFFSET, KstDateCalculator, SECS_PER_DAY_U64},
    master_sheet, path_source_message, prefixed_message,
    region::normalize_address_key,
    rows::{ChangeRow, SourceRecord, StoreRow},
    source_download,
};
use std::{
    collections::{HashMap, hash_map::Entry},
    fs,
    io::Write,
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
const MASTER_PATH: &str = "fuel_cost_chungcheong.xlsx";
const MAX_DELETED_RATIO_NUMERATOR: usize = 1;
const MAX_DELETED_RATIO_DENOMINATOR: usize = 2;
pub struct UpdateSummary<'data> {
    added: &'data [StoreRow],
    changes: &'data [ChangeRow],
    deleted: &'data [StoreRow],
    source_name: &'data str,
}
pub struct UpdateRunContext<'out> {
    pub out: &'out mut dyn Write,
}
impl UpdateRunContext<'_> {
    fn load_source(&mut self) -> Result<(HashMap<String, SourceRecord>, String)> {
        let source_path = source_download::SourceDownload {
            dir: Path::new("."),
            out: self.out,
        }
        .refresh_source()?;
        write_line_ignored(self.out, format_args!("Opinet 소스 파일 준비 완료"));
        let source_name = source_path
            .file_name()
            .and_then(|name| name.to_str())
            .map_or_else(|| source_path.display().to_string(), str::to_owned);
        let result = (|| -> Result<HashMap<String, SourceRecord>> {
            let raw_records = source_reader::SourceReader { path: &source_path }
                .read_xls_source()
                .map_err(|source_err| {
                    err(path_source_message(
                        "소스 xls 파일 읽기 실패",
                        &source_path,
                        source_err,
                    ))
                })?;
            let mut records = Vec::new();
            records
                .try_reserve_exact(raw_records.len())
                .map_err(|source| {
                    let record_count = raw_records.len();
                    err_with_source(
                        format!("필터링 소스 레코드 목록 메모리 확보 실패: {record_count} records"),
                        source,
                    )
                })?;
            for record in raw_records {
                let region_key = normalize_address_key(&record.region);
                if source_download::TARGET_REGION_KEYS.contains(&region_key.as_str()) {
                    records.push(record);
                }
            }
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
            changes,
            deleted,
            source_name,
        } = *summary;
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
        self.print_store_rows("신규 업체 추가 목록 (상위 20개)", added);
        self.print_store_rows("폐업 업체 삭제 목록 (상위 20개)", deleted);
        write_line_ignored(self.out, format_args!("=====================\n"));
    }
    pub fn run_update(&mut self) -> Result<()> {
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
        let (changes, added, deleted) = master_sheet::MasterSheetUpdate {
            book: &mut book,
            source_index: &source_index,
        }
        .apply()?;
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
        let (year, month, day) = KstDateCalculator { day_index }
            .civil_from_days()
            .ok_or_else(|| err("KST 날짜 계산 중 범위 오류가 발생했습니다."))?;
        let today = format!("{year:04}-{month:02}-{day:02}");
        if !is_yyyy_mm_dd(&today) {
            return Err(err(prefixed_message(
                "오늘 날짜 형식이 올바르지 않습니다: ",
                &today,
            )));
        }
        change_log::ChangeLogSheetService {
            added: &added,
            book: &mut book,
            changes: &changes,
            deleted: &deleted,
            today: &today,
        }
        .update()?;
        write_line_ignored(self.out, format_args!("마스터 파일 저장 중..."));
        book.save(master_path)?;
        self.print_update_summary(&UpdateSummary {
            added: &added,
            changes: &changes,
            deleted: &deleted,
            source_name: &source_name,
        });
        Ok(())
    }
}
