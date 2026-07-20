use self::format::{format_scaled_value, format_unit_price_text, missing_sort_target_row_error};
use crate::{
    diagnostic::{AppError, Result, err, err_with_source},
    excel,
    excel::SourceRecord,
    excel::writer::{Row as StdRow, Workbook as StdWorkbook, col_to_name, remap_formula_rows},
    region::{
        TARGET_REGION_COUNT, TARGET_REGIONS, TargetRegion, TargetRegionPolicy,
        increment_target_region_count, normalize_address_key, target_region,
    },
    sheet_util::{add_row_offset, usize_to_u32},
};
use alloc::{borrow::Cow, collections::BTreeMap};
use core::{fmt::Write as _, range::RangeInclusive};
use std::collections::{HashMap, HashSet};
mod format;
const MASTER_SHEET_NAME: &str = "유류비";
const MASTER_HEADER_SCAN_ROWS: u32 = 200;
const USIZE_DECIMAL_TEXT_MAX_LEN: usize = 20;
const SMART_DISCOUNT_BRAND_KEYWORD: &str = "현대오일뱅크";
const SMART_DISCOUNT_DIRECT_KEYWORD: &str = "직영";
const SMART_DISCOUNT_INPUT_COL: u32 = 2;
const SMART_DISCOUNT_INPUT_ROW: u32 = 13;
const DECIMAL_SCALE: ScaledDecimal = ScaledDecimal(1_000_000);
const DECIMAL_SCALE_SQUARED: ScaledSortKey = ScaledSortKey(1_000_000_000_000);
const DECIMAL_SCALE_CUBED: ScaledSortKey = ScaledSortKey(1_000_000_000_000_000_000);
type RowRange = RangeInclusive<u32>;
pub(super) struct MasterSheetUpdater<'source> {
    pub source_index: &'source HashMap<String, SourceRecord>,
}
#[derive(Debug)]
pub(super) struct ChangeRow<'source> {
    pub old_diesel: Option<i32>,
    pub old_gasoline: Option<i32>,
    pub old_premium: Option<i32>,
    pub reason: String,
    pub record: &'source SourceRecord,
}
#[derive(Debug)]
pub(super) struct StoreRow {
    pub address: String,
    pub diesel: Option<i32>,
    pub gasoline: Option<i32>,
    pub name: String,
    pub old_row: u32,
    pub premium: Option<i32>,
    pub region: String,
}
pub(super) struct MasterSheetUpdateResult<'source> {
    pub added: Vec<&'source SourceRecord>,
    pub changes: Vec<ChangeRow<'source>>,
    pub deleted: Vec<StoreRow>,
    pub existing_count: usize,
    pub existing_region_counts: [usize; TARGET_REGION_COUNT],
    pub matched_existing_region_counts: [usize; TARGET_REGION_COUNT],
}
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ScaledDecimal(i64);
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct ScaledSortKey(i128);
impl ScaledDecimal {
    const ZERO: Self = Self(0);
    fn as_i128(self) -> i128 {
        i128::from(self.0)
    }
    const fn as_i64(self) -> i64 {
        self.0
    }
    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.0.checked_add(rhs.0).map(Self)
    }
    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.0.checked_sub(rhs.0).map(Self)
    }
}
impl ScaledSortKey {
    const MAX: Self = Self(i128::MAX);
    const ZERO: Self = Self(0);
    const fn as_i128(self) -> i128 {
        self.0
    }
    fn checked_add(self, rhs: Self) -> Option<Self> {
        self.0.checked_add(rhs.0).map(Self)
    }
    fn checked_div(self, rhs: Self) -> Option<Self> {
        self.0.checked_div(rhs.0).map(Self)
    }
    fn checked_mul(self, rhs: Self) -> Option<Self> {
        self.0.checked_mul(rhs.0).map(Self)
    }
    fn checked_sub(self, rhs: Self) -> Option<Self> {
        self.0.checked_sub(rhs.0).map(Self)
    }
}
enum MasterRowDecision<'source> {
    Deleted {
        normalized_address: String,
        row: StoreRow,
    },
    Matched {
        change: Option<ChangeRow<'source>>,
        matched_key: &'source str,
        src: &'source SourceRecord,
    },
    Unaddressed,
}
struct MasterRowsRebuilder<'sheet, 'strings, 'deleted, 'kept, 'sources, 'source> {
    deleted: &'deleted [StoreRow],
    kept_source_rows: &'kept [(u32, Option<&'source SourceRecord>)],
    new_sources: &'sources [&'source SourceRecord],
    plan: MasterSheetPlan,
    shared_strings: &'strings [String],
    ws: &'sheet mut excel::writer::Worksheet,
}
struct SortableRankRow {
    key: RankSortKey,
    row: u32,
}
struct RankSortRefresher<'sheet, 'strings> {
    data_rows: RowRange,
    layout: MasterSheetLayout,
    shared_strings: &'strings [String],
    ws: &'sheet mut excel::writer::Worksheet,
}
#[derive(Debug, Clone, Copy)]
struct MasterSheetPlan {
    data_start_row: u32,
    header_row: u32,
    layout: MasterSheetLayout,
}
#[derive(Debug, Clone, Copy)]
struct MasterSheetLayout {
    address: u32,
    adjusted_diesel: Option<u32>,
    adjusted_gasoline: Option<u32>,
    adjusted_premium: Option<u32>,
    brand: u32,
    currency_apply: Option<u32>,
    diesel: u32,
    fuel_total_text: Option<u32>,
    gasoline: u32,
    name: u32,
    premium: u32,
    rank: u32,
    region: u32,
    region_discount: Option<u32>,
    region_rate: Option<u32>,
    regional_total: Option<u32>,
    self_yn: u32,
    smart_discount: Option<u32>,
    sort_key: Option<u32>,
    total_price: Option<u32>,
    unit_price_with_currency: Option<u32>,
    unit_price_without_currency: Option<u32>,
}
struct RankSortContext {
    diesel_qty: ScaledDecimal,
    gasoline_qty: ScaledDecimal,
    premium_qty: ScaledDecimal,
    region_rates: [(TargetRegion, Option<ScaledDecimal>); TARGET_REGION_COUNT],
    smart_discount: ScaledDecimal,
    total_qty: Option<ScaledDecimal>,
}
impl RankSortContext {
    fn region_rate(&self, label: &str) -> Option<ScaledDecimal> {
        for &(region, rate) in &self.region_rates {
            if region.label() == label {
                return rate;
            }
        }
        None
    }
}
struct RankSortKey {
    address: String,
    diesel: ScaledSortKey,
    gasoline: ScaledSortKey,
    name: String,
    premium: ScaledSortKey,
    rank_total: Option<ScaledSortKey>,
    region: String,
}
#[derive(Clone, Copy)]
struct AdjustedFuelPrices {
    diesel: Option<ScaledDecimal>,
    gasoline: Option<ScaledDecimal>,
    premium: Option<ScaledDecimal>,
}
struct ParsedMasterIdentity<'text> {
    address: Cow<'text, str>,
    name: Cow<'text, str>,
    region: Cow<'text, str>,
}
struct ParsedMasterRow<'text> {
    diesel: Option<i32>,
    gasoline: Option<i32>,
    identity: ParsedMasterIdentity<'text>,
    premium: Option<i32>,
    smart_discount_excluded: bool,
}
struct RankRowBase {
    adjusted_prices: AdjustedFuelPrices,
    region_rate: ScaledDecimal,
    smart_discount: ScaledDecimal,
}
struct FilterTarget {
    header_row: u32,
    last_data_row: u32,
    last_filter_col: u32,
}
struct MasterRowEvaluation<'source> {
    changes: Vec<ChangeRow<'source>>,
    deleted: Vec<StoreRow>,
    existing_region_counts: [usize; TARGET_REGION_COUNT],
    kept_source_rows: Vec<(u32, Option<&'source SourceRecord>)>,
    matched_existing_region_counts: [usize; TARGET_REGION_COUNT],
    matched_source_keys: HashSet<&'source str>,
}
struct RowMapper<'deleted> {
    decrease: u32,
    deleted: &'deleted [StoreRow],
    increase: u32,
    old_data_rows: RowRange,
}
impl RowMapper<'_> {
    fn map(&self, old_ref_row: u32) -> Result<u32> {
        if self.old_data_rows.contains(&old_ref_row) {
            let deleted_le = usize_to_u32(
                self.deleted
                    .partition_point(|row| row.old_row <= old_ref_row),
                "삭제 행 누적 수",
            )?;
            return old_ref_row
                .checked_sub(deleted_le)
                .ok_or_else(|| err("행 참조 재배치 중 row 번호가 0 이하가 되었습니다."));
        }
        if old_ref_row > self.old_data_rows.last {
            return shift_row(old_ref_row, self.increase, self.decrease, "행 참조 재배치");
        }
        Ok(old_ref_row)
    }
    fn shift(&self, row: u32) -> Result<u32> {
        shift_row(row, self.increase, self.decrease, "비데이터 행 재배치")
    }
}
impl<'text> ParsedMasterIdentity<'text> {
    fn is_empty(&self) -> bool {
        self.region.is_empty() && self.name.is_empty() && self.address.is_empty()
    }
    fn read(
        ws: &'text excel::writer::Worksheet,
        layout: MasterSheetLayout,
        row: u32,
        shared_strings: &'text [String],
    ) -> Result<Self> {
        Ok(Self {
            address: trim_cow(ws.try_get_display_at(layout.address, row, shared_strings)?),
            name: trim_cow(ws.try_get_display_at(layout.name, row, shared_strings)?),
            region: trim_cow(ws.try_get_display_at(layout.region, row, shared_strings)?),
        })
    }
}
impl<'text> ParsedMasterRow<'text> {
    fn read(
        ws: &'text excel::writer::Worksheet,
        layout: MasterSheetLayout,
        row: u32,
        shared_strings: &'text [String],
    ) -> Result<Self> {
        let identity = ParsedMasterIdentity::read(ws, layout, row, shared_strings)?;
        Self::read_with_identity(identity, ws, layout, row, shared_strings)
    }
    fn read_with_identity(
        identity: ParsedMasterIdentity<'text>,
        ws: &'text excel::writer::Worksheet,
        layout: MasterSheetLayout,
        row: u32,
        shared_strings: &'text [String],
    ) -> Result<Self> {
        let smart_discount_excluded = match layout.smart_discount {
            Some(col)
                if ws.try_get_formula_at(col, row)?.is_none()
                    && MasterSheetUpdater::is_exact_zero_at(ws, col, row, shared_strings)? =>
            {
                true
            }
            Some(_) | None => false,
        };
        Ok(Self {
            diesel: MasterSheetUpdater::normalize_fuel_price(ws.get_i32_at(
                layout.diesel,
                row,
                shared_strings,
            )?),
            gasoline: MasterSheetUpdater::normalize_fuel_price(ws.get_i32_at(
                layout.gasoline,
                row,
                shared_strings,
            )?),
            identity,
            premium: MasterSheetUpdater::normalize_fuel_price(ws.get_i32_at(
                layout.premium,
                row,
                shared_strings,
            )?),
            smart_discount_excluded,
        })
    }
}
impl RankRowBase {
    fn read(
        row: &ParsedMasterRow<'_>,
        currency_apply: bool,
        sort_context: &RankSortContext,
    ) -> Result<Self> {
        let default_smart_discount = if row.identity.name.contains(SMART_DISCOUNT_BRAND_KEYWORD)
            && row.identity.name.contains(SMART_DISCOUNT_DIRECT_KEYWORD)
        {
            sort_context.smart_discount
        } else {
            ScaledDecimal::ZERO
        };
        let smart_discount = if row.smart_discount_excluded {
            ScaledDecimal::ZERO
        } else {
            default_smart_discount
        };
        let adjusted_prices = AdjustedFuelPrices {
            gasoline: MasterSheetUpdater::adjusted_fuel_price(row.gasoline, smart_discount),
            premium: MasterSheetUpdater::adjusted_fuel_price(row.premium, smart_discount),
            diesel: MasterSheetUpdater::adjusted_fuel_price(row.diesel, smart_discount),
        };
        let region_rate = if currency_apply {
            sort_context
                .region_rate(row.identity.region.as_ref())
                .ok_or_else(|| {
                    err(format!(
                        "지역화폐 적용 대상 행의 적용률을 찾지 못했습니다: 지역={}",
                        row.identity.region
                    ))
                })?
        } else {
            ScaledDecimal::ZERO
        };
        Ok(Self {
            adjusted_prices,
            region_rate,
            smart_discount,
        })
    }
    fn regional_discount(
        value: ScaledSortKey,
        region_rate: ScaledDecimal,
    ) -> Option<ScaledSortKey> {
        value
            .checked_mul(ScaledSortKey(region_rate.as_i128()))?
            .checked_div(DECIMAL_SCALE_CUBED)?
            .checked_mul(DECIMAL_SCALE_SQUARED)
    }
}
impl TryFrom<(&excel::writer::Worksheet, &[String], u32, u32)> for MasterSheetLayout {
    type Error = AppError;
    fn try_from(
        (ws, shared_strings, row, max_cols): (&excel::writer::Worksheet, &[String], u32, u32),
    ) -> Result<Self> {
        let optional_header =
            |keys: &[&str]| find_header_column(ws, shared_strings, row, max_cols, keys);
        let required_header = |keys: &[&str], display_name: &str| {
            optional_header(keys)?
                .ok_or_else(|| err(format!("유류비 헤더에 '{display_name}' 컬럼이 없습니다.")))
        };
        Ok(Self {
            address: required_header(&["주소"], "주소")?,
            adjusted_diesel: optional_header(&["조정경유단가(원/L)"])?,
            adjusted_gasoline: optional_header(&["조정휘발유단가(원/L)"])?,
            adjusted_premium: optional_header(&["조정고급유단가(원/L)"])?,
            brand: required_header(&["상표"], "상표")?,
            currency_apply: optional_header(&["지역화폐적용여부"])?,
            diesel: required_header(&["경유", "경유단가(원/L)", "경유단가"], "경유")?,
            fuel_total_text: optional_header(&["유종별총가격(원)"])?,
            gasoline: required_header(
                &["휘발유", "보통휘발유", "휘발유단가(원/L)", "휘발유단가"],
                "휘발유",
            )?,
            name: required_header(&["상호"], "상호")?,
            premium: required_header(
                &["고급유", "고급휘발유", "고급유단가(원/L)", "고급유단가"],
                "고급유",
            )?,
            rank: required_header(&["지역화폐적용순위"], "지역화폐적용순위")?,
            region: required_header(&["지역"], "지역")?,
            region_discount: optional_header(&["지역화폐적립액(원)"])?,
            region_rate: optional_header(&["지역화폐적립율"])?,
            regional_total: optional_header(&["지역화폐적용금액(원)"])?,
            self_yn: required_header(&["셀프여부", "셀프"], "셀프여부")?,
            smart_discount: optional_header(&["스마트주유할인(원/L)"])?,
            sort_key: optional_header(&["정렬키"])?,
            total_price: optional_header(&["총가격(원)"])?,
            unit_price_with_currency: optional_header(&["지역화폐적용단가(원/L)"])?,
            unit_price_without_currency: optional_header(&["지역화폐미적용단가(원/L)"])?,
        })
    }
}
impl MasterRowsRebuilder<'_, '_, '_, '_, '_, '_> {
    fn filter_end_row(&self, final_count: u32) -> Result<u32> {
        let data_start_row = self.plan.data_start_row;
        let Some(row_offset) = final_count.checked_sub(1) else {
            return Ok(data_start_row);
        };
        data_start_row
            .checked_add(row_offset)
            .ok_or_else(|| err("유류비 마지막 행 계산 중 overflow가 발생했습니다."))
    }
    fn finish_rebuild(
        &mut self,
        header_row: u32,
        old_data_rows: RowRange,
        filter_end_row: u32,
    ) -> Result<FilterTarget> {
        let filter_rows = RowRange {
            start: header_row,
            last: filter_end_row,
        };
        let filter_end_col = self.ws.update_auto_filter_ref(filter_rows)?;
        self.ws
            .prune_empty_style_artifacts_after_col(filter_end_col)?;
        self.ws.update_dimension()?;
        let target_cols = [self.plan.layout.rank];
        self.ws.extend_conditional_formats(
            old_data_rows,
            RowRange {
                start: self.plan.data_start_row,
                last: filter_end_row,
            },
            &target_cols,
        )?;
        Ok(FilterTarget {
            header_row,
            last_data_row: filter_end_row,
            last_filter_col: filter_end_col,
        })
    }
    fn rebuild(&mut self, existing_count: usize) -> Result<FilterTarget> {
        let MasterSheetPlan {
            header_row,
            data_start_row,
            layout,
        } = self.plan;
        let last_old_row = self
            .kept_source_rows
            .last()
            .map(|&(row, _)| row)
            .max(self.deleted.last().map(|row| row.old_row));
        let old_end_row = match last_old_row {
            Some(row) => row,
            None => data_start_row
                .checked_sub(1)
                .ok_or_else(|| err("기존 유류비 마지막 행 계산 중 overflow가 발생했습니다."))?,
        };
        let old_data_rows = RowRange {
            start: data_start_row,
            last: old_end_row,
        };
        let original_rows = self.ws.take_rows();
        let final_count = self
            .kept_source_rows
            .len()
            .checked_add(self.new_sources.len())
            .ok_or_else(|| err("최종 마스터 행 수 계산 중 overflow가 발생했습니다."))?;
        let old_count_u32 = usize_to_u32(existing_count, "기존 유류비 행 수")?;
        let final_count_u32 = usize_to_u32(final_count, "최종 유류비 행 수")?;
        let (increase, decrease) = if final_count_u32 >= old_count_u32 {
            (
                final_count_u32
                    .checked_sub(old_count_u32)
                    .ok_or_else(|| err("유류비 행 증가 수 계산에 실패했습니다."))?,
                0_u32,
            )
        } else {
            (
                0_u32,
                old_count_u32
                    .checked_sub(final_count_u32)
                    .ok_or_else(|| err("유류비 행 감소 수 계산에 실패했습니다."))?,
            )
        };
        let row_mapper = RowMapper {
            decrease,
            deleted: self.deleted,
            increase,
            old_data_rows,
        };
        let template_row_num = last_old_row.unwrap_or(data_start_row);
        let new_rows_map =
            self.rebuild_rows(original_rows, &row_mapper, old_data_rows, template_row_num)?;
        self.ws.replace_rows(new_rows_map);
        for (i, &(_, source)) in self.kept_source_rows.iter().enumerate() {
            let new_row = add_row_offset(data_start_row, i, "유류비 기존행 재배치")?;
            if let Some(src) = source {
                MasterSheetUpdater::write_master_row_from_source(
                    self.ws, new_row, src, src.region, layout,
                )?;
            }
        }
        for (i, &source) in self.new_sources.iter().enumerate() {
            let offset = self
                .kept_source_rows
                .len()
                .checked_add(i)
                .ok_or_else(|| err("유류비 신규행 오프셋 계산 중 overflow가 발생했습니다."))?;
            let new_row = add_row_offset(data_start_row, offset, "유류비 신규행 추가")?;
            MasterSheetUpdater::write_master_row_from_source(
                self.ws,
                new_row,
                source,
                source.region,
                layout,
            )?;
            if let Some(col) = layout.smart_discount {
                self.ws.set_i32_at(col, new_row, None)?;
            }
        }
        let filter_end_row = self.filter_end_row(final_count_u32)?;
        if final_count > 0 {
            RankSortRefresher {
                data_rows: RowRange {
                    start: data_start_row,
                    last: filter_end_row,
                },
                layout,
                shared_strings: self.shared_strings,
                ws: self.ws,
            }
            .refresh()?;
        }
        self.finish_rebuild(header_row, old_data_rows, filter_end_row)
    }
    fn rebuild_rows(
        &self,
        mut original_rows: BTreeMap<u32, StdRow>,
        row_mapper: &RowMapper<'_>,
        old_data_rows: RowRange,
        template_row_num: u32,
    ) -> Result<BTreeMap<u32, StdRow>> {
        let data_start_row = self.plan.data_start_row;
        let mut new_rows_map = BTreeMap::new();
        if !self.new_sources.is_empty() {
            let template_row = original_rows.get(&template_row_num).ok_or_else(|| {
                err(format!(
                    "유류비 신규행 template이 없습니다: row={template_row_num}"
                ))
            })?;
            for i in 0..self.new_sources.len() {
                let offset =
                    self.kept_source_rows.len().checked_add(i).ok_or_else(|| {
                        err("유류비 신규행 오프셋 계산 중 overflow가 발생했습니다.")
                    })?;
                let new_row = add_row_offset(data_start_row, offset, "유류비 신규행 추가")?;
                let resolver = |old_ref_row: u32| {
                    if old_ref_row == template_row_num {
                        Ok(new_row)
                    } else {
                        row_mapper.map(old_ref_row)
                    }
                };
                let row_obj = template_row.copy_with_row_mapping(&resolver)?;
                new_rows_map.insert(new_row, row_obj);
            }
        }
        for (row_num, mut row_obj) in
            original_rows.extract_if(.., |row_num, _| !old_data_rows.contains(row_num))
        {
            let new_row_num = if row_num < old_data_rows.start {
                row_num
            } else {
                row_mapper.shift(row_num)?
            };
            remap_formula_rows(&mut row_obj, &|old_ref_row| row_mapper.map(old_ref_row))?;
            new_rows_map.insert(new_row_num, row_obj);
        }
        for (i, &(old_row, _)) in self.kept_source_rows.iter().enumerate() {
            let new_row = add_row_offset(data_start_row, i, "유류비 기존행 재배치")?;
            let mut row_obj = original_rows
                .remove(&old_row)
                .ok_or_else(|| err(format!("유류비 기존행 XML이 없습니다: row={old_row}")))?;
            let resolver = |old_ref_row: u32| {
                if old_ref_row == old_row {
                    Ok(new_row)
                } else {
                    row_mapper.map(old_ref_row)
                }
            };
            remap_formula_rows(&mut row_obj, &resolver)?;
            new_rows_map.insert(new_row, row_obj);
        }
        Ok(new_rows_map)
    }
}
impl RankSortRefresher<'_, '_> {
    fn apply_sorted_rows(
        &mut self,
        data_rows: Vec<SortableRankRow>,
        row_mapping: &[Option<u32>],
    ) -> Result<()> {
        let mut detached_rows: Vec<Option<StdRow>> = Vec::new();
        detached_rows
            .try_reserve_exact(data_rows.len())
            .map_err(|source| {
                let data_row_count = data_rows.len();
                err_with_source(
                    format!("정렬 분리 행 메모리 확보 실패: {data_row_count} entries"),
                    source,
                )
            })?;
        let mut rows = self.ws.take_rows();
        let row_count = row_range_len(self.data_rows, "정렬 분리 행 수")?;
        let mut expected_row = self.data_rows.start;
        for (old_row, row) in rows.extract_if(self.data_rows, |_, _| true) {
            if old_row != expected_row {
                return Err(missing_sort_target_row_error(expected_row));
            }
            detached_rows.push(Some(row));
            if old_row != self.data_rows.last {
                expected_row = old_row
                    .checked_add(1)
                    .ok_or_else(|| err("정렬 분리 행 번호 계산 실패"))?;
            }
        }
        if detached_rows.len() != row_count {
            return Err(missing_sort_target_row_error(expected_row));
        }
        for data_row in data_rows {
            let old_row = data_row.row;
            let Some(new_row) = mapped_contiguous_row(row_mapping, self.data_rows.start, old_row)
            else {
                return Err(err(format!("정렬 후 행 매핑을 찾지 못했습니다: {old_row}")));
            };
            let row_offset = old_row
                .checked_sub(self.data_rows.start)
                .ok_or_else(|| err(format!("정렬 분리 행 offset 계산 실패: {old_row}")))?;
            let index = usize::try_from(row_offset)
                .map_err(|source| err_with_source("정렬 분리 행 index 변환 실패", source))?;
            let mut row = detached_rows
                .get_mut(index)
                .and_then(Option::take)
                .ok_or_else(|| missing_sort_target_row_error(old_row))?;
            remap_formula_rows(&mut row, &|old_ref_row| {
                Ok(
                    mapped_contiguous_row(row_mapping, self.data_rows.start, old_ref_row)
                        .unwrap_or(old_ref_row),
                )
            })?;
            rows.insert(new_row, row);
        }
        self.ws.replace_rows(rows);
        Ok(())
    }
    fn build_and_write_formula_cache(
        &mut self,
        row_num: u32,
        display_total_qty: Option<ScaledDecimal>,
        sort_context: &RankSortContext,
    ) -> Result<Option<ScaledSortKey>> {
        let row = ParsedMasterRow::read(self.ws, self.layout, row_num, self.shared_strings)?;
        let currency_apply =
            MasterSheetUpdater::currency_apply(self.ws, self.layout, row_num, self.shared_strings)?;
        let base = RankRowBase::read(&row, currency_apply, sort_context)?;
        let prices = base.adjusted_prices;
        let total_price = display_total_qty
            .and_then(|_| MasterSheetUpdater::compute_total_price(sort_context, prices));
        let has_total_price = total_price.is_some();
        let region_rate = if has_total_price {
            base.region_rate
        } else {
            ScaledDecimal::ZERO
        };
        let regional_discount =
            total_price.and_then(|value| RankRowBase::regional_discount(value, region_rate));
        let rank_total = total_price
            .zip(regional_discount)
            .and_then(|(total, discount)| total.checked_sub(discount));
        let mut fuel_total_parts = String::new();
        let has_fuel_total_text = display_total_qty.is_some()
            && append_fuel_total_text(
                &mut fuel_total_parts,
                sort_context.gasoline_qty,
                prices.gasoline,
                "휘발유",
            )?
            && append_fuel_total_text(
                &mut fuel_total_parts,
                sort_context.premium_qty,
                prices.premium,
                "고급유",
            )?
            && append_fuel_total_text(
                &mut fuel_total_parts,
                sort_context.diesel_qty,
                prices.diesel,
                "경유",
            )?;
        let fuel_total_text = has_fuel_total_text.then_some(fuel_total_parts);
        let unit_price_with_currency = match rank_total.zip(display_total_qty) {
            Some((value, qty)) => format_unit_price_text(value, qty)?,
            None => None,
        };
        let unit_price_without_currency = match total_price.zip(display_total_qty) {
            Some((value, qty)) => format_unit_price_text(value, qty)?,
            None => None,
        };
        let sort_key = self.layout.sort_key.map(|col| {
            let value = rank_total.map_or(Cow::Borrowed("1000000000000000"), |value| {
                Cow::Owned(format_scaled_value(
                    value.as_i128(),
                    DECIMAL_SCALE_SQUARED.as_i128(),
                ))
            });
            (col, value)
        });
        self.write_decimal_value(
            row_num,
            self.layout.smart_discount,
            Some(base.smart_discount),
        )?;
        self.write_decimal_value(row_num, self.layout.adjusted_gasoline, prices.gasoline)?;
        self.write_decimal_value(row_num, self.layout.adjusted_premium, prices.premium)?;
        self.write_decimal_value(row_num, self.layout.adjusted_diesel, prices.diesel)?;
        self.write_text_value(
            row_num,
            self.layout.fuel_total_text,
            fuel_total_text.as_deref(),
            Some("str"),
        )?;
        self.write_squared_value(row_num, self.layout.total_price, total_price)?;
        self.write_decimal_value(
            row_num,
            self.layout.region_rate,
            has_total_price.then_some(region_rate),
        )?;
        self.write_squared_value(row_num, self.layout.region_discount, regional_discount)?;
        self.write_squared_value(row_num, self.layout.regional_total, rank_total)?;
        if let Some(value) = sort_key.as_ref() {
            self.write_text_value(row_num, Some(value.0), Some(value.1.as_ref()), None)?;
        }
        self.write_text_value(
            row_num,
            self.layout.unit_price_with_currency,
            unit_price_with_currency.as_deref(),
            None,
        )?;
        self.write_text_value(
            row_num,
            self.layout.unit_price_without_currency,
            unit_price_without_currency.as_deref(),
            None,
        )?;
        Ok(rank_total)
    }
    fn build_sort_key(&self, row_num: u32, sort_context: &RankSortContext) -> Result<RankSortKey> {
        let row = ParsedMasterRow::read(self.ws, self.layout, row_num, self.shared_strings)?;
        let currency_apply =
            MasterSheetUpdater::currency_apply(self.ws, self.layout, row_num, self.shared_strings)?;
        let base = RankRowBase::read(&row, currency_apply, sort_context)?;
        let adjusted = base.adjusted_prices;
        let region_rate = base.region_rate;
        let region_multiplier = DECIMAL_SCALE
            .checked_sub(region_rate)
            .ok_or_else(|| err("지역 보정률이 100%를 초과했습니다."))?;
        let regional_adjusted_gasoline = adjusted
            .gasoline
            .and_then(|value| value.as_i128().checked_mul(region_multiplier.as_i128()))
            .map(ScaledSortKey);
        let regional_adjusted_premium = adjusted
            .premium
            .and_then(|value| value.as_i128().checked_mul(region_multiplier.as_i128()))
            .map(ScaledSortKey);
        let regional_adjusted_diesel = adjusted
            .diesel
            .and_then(|value| value.as_i128().checked_mul(region_multiplier.as_i128()))
            .map(ScaledSortKey);
        let rank_total = sort_context.total_qty.and_then(|total_qty| {
            if total_qty == ScaledDecimal::ZERO {
                None
            } else {
                let total_price = MasterSheetUpdater::compute_total_price(sort_context, adjusted)?;
                let discount = RankRowBase::regional_discount(total_price, region_rate)?;
                total_price.checked_sub(discount)
            }
        });
        Ok(RankSortKey {
            rank_total,
            gasoline: MasterSheetUpdater::fuel_sort_value(regional_adjusted_gasoline),
            premium: MasterSheetUpdater::fuel_sort_value(regional_adjusted_premium),
            diesel: MasterSheetUpdater::fuel_sort_value(regional_adjusted_diesel),
            region: row.identity.region.into_owned(),
            name: row.identity.name.into_owned(),
            address: row.identity.address.into_owned(),
        })
    }
    fn collect_rank_totals(
        &mut self,
        display_total_qty: Option<ScaledDecimal>,
        sort_context: &RankSortContext,
    ) -> Result<Vec<(u32, Option<ScaledSortKey>)>> {
        let capacity = row_range_len(self.data_rows, "랭크 캐시 대상 행 수")?;
        let mut rank_totals = Vec::new();
        rank_totals.try_reserve_exact(capacity).map_err(|source| {
            err_with_source(
                format!("랭크 캐시 목록 메모리 확보 실패: {capacity} rows"),
                source,
            )
        })?;
        for row in self.data_rows {
            let rank_total =
                self.build_and_write_formula_cache(row, display_total_qty, sort_context)?;
            rank_totals.push((row, rank_total));
        }
        Ok(rank_totals)
    }
    fn normalize_smart_discount_formulas(&mut self) -> Result<()> {
        let Some(smart_discount_col) = self.layout.smart_discount else {
            return Ok(());
        };
        let name_col = col_to_name(self.layout.name)?;
        let input_col = col_to_name(SMART_DISCOUNT_INPUT_COL)?;
        let mut canonical_formula = String::new();
        for row in self.data_rows {
            canonical_formula.clear();
            write!(
                &mut canonical_formula,
                "IF(AND(IFERROR(SEARCH(\"{SMART_DISCOUNT_BRAND_KEYWORD}\",${name_col}{row}),0)>0,IFERROR(SEARCH(\"{SMART_DISCOUNT_DIRECT_KEYWORD}\",${name_col}{row}),0)>0),${input_col}${SMART_DISCOUNT_INPUT_ROW},0)"
            )
            .map_err(|source| err_with_source("스마트주유 할인 수식 작성 실패", source))?;
            let has_formula = {
                let formula = self.ws.try_get_formula_at(smart_discount_col, row)?;
                if formula.as_deref() == Some(canonical_formula.as_str()) {
                    continue;
                }
                formula.is_some()
            };
            if !has_formula {
                if MasterSheetUpdater::is_exact_zero_at(
                    self.ws,
                    smart_discount_col,
                    row,
                    self.shared_strings,
                )? {
                    self.ws.set_i32_at(smart_discount_col, row, Some(0_i32))?;
                    continue;
                }
                self.ws.set_i32_at(smart_discount_col, row, None)?;
            }
            self.ws
                .set_formula_at(smart_discount_col, row, &canonical_formula)?;
        }
        Ok(())
    }
    fn refresh(&mut self) -> Result<()> {
        self.normalize_smart_discount_formulas()?;
        let sort_context =
            MasterSheetUpdater::build_rank_sort_context(self.ws, self.shared_strings)?;
        self.sort(&sort_context)?;
        self.repair_formulas()?;
        let refreshed_sort_context;
        let cache_sort_context = if (4..=13).any(|row| self.data_rows.contains(&row)) {
            refreshed_sort_context =
                MasterSheetUpdater::build_rank_sort_context(self.ws, self.shared_strings)?;
            &refreshed_sort_context
        } else {
            &sort_context
        };
        self.refresh_caches(cache_sort_context)
    }
    fn refresh_caches(&mut self, sort_context: &RankSortContext) -> Result<()> {
        let display_total_qty = MasterSheetUpdater::get_f64_at(self.ws, 2, 10, self.shared_strings)
            .map(|value| value.filter(|qty| *qty != ScaledDecimal::ZERO))?;
        self.ws
            .clear_formula_cached_values_in_range(self.data_rows)?;
        let rank_totals = self.collect_rank_totals(display_total_qty, sort_context)?;
        let mut visible_rank_totals: Vec<ScaledSortKey> = Vec::new();
        visible_rank_totals
            .try_reserve_exact(rank_totals.len())
            .map_err(|source| {
                err_with_source(
                    format!(
                        "표시 랭크 합계 목록 메모리 확보 실패: {} rows",
                        rank_totals.len()
                    ),
                    source,
                )
            })?;
        visible_rank_totals.extend(rank_totals.iter().filter_map(|&(_, total)| total));
        visible_rank_totals.sort_unstable();
        let mut rank_text = String::new();
        rank_text
            .try_reserve_exact(USIZE_DECIMAL_TEXT_MAX_LEN)
            .map_err(|source| err_with_source("지역화폐 순위 문자열 메모리 확보 실패", source))?;
        for (row, rank_total) in rank_totals {
            let rank_value = if let Some(current) = rank_total {
                let rank = visible_rank_totals
                    .partition_point(|candidate| *candidate < current)
                    .checked_add(1)
                    .ok_or_else(|| err("지역화폐 순위 계산 중 overflow가 발생했습니다."))?;
                rank_text.clear();
                write!(&mut rank_text, "{rank}")
                    .map_err(|source| err_with_source("지역화폐 순위 문자열 작성 실패", source))?;
                Some(rank_text.as_str())
            } else {
                None
            };
            self.ws
                .set_formula_cached_value_at(self.layout.rank, row, rank_value, None)?;
        }
        Ok(())
    }
    fn repair_formulas(&mut self) -> Result<()> {
        let Some(sort_key_col) = self.layout.sort_key else {
            return Ok(());
        };
        let range_rewriter = excel::writer::AbsoluteColumnRangeRewriter::from((
            sort_key_col,
            self.data_rows.start,
            self.data_rows.last,
        ));
        for row in self.data_rows {
            let Some(formula) = self.ws.try_get_formula_at(self.layout.rank, row)? else {
                continue;
            };
            let rewrite_result = range_rewriter.rewrite(formula.as_ref())?;
            if let Some(updated) = rewrite_result {
                self.ws.set_formula_at(self.layout.rank, row, &updated)?;
            }
        }
        Ok(())
    }
    fn row_mapping(&self, data_rows: &[SortableRankRow]) -> Result<Vec<Option<u32>>> {
        let mut row_mapping: Vec<Option<u32>> = Vec::new();
        row_mapping
            .try_reserve_exact(data_rows.len())
            .map_err(|source| {
                let data_row_count = data_rows.len();
                err_with_source(
                    format!("정렬 행 매핑 메모리 확보 실패: {data_row_count} entries"),
                    source,
                )
            })?;
        row_mapping.resize(data_rows.len(), None);
        let mut filled_count = 0_usize;
        for (index, data_row) in data_rows.iter().enumerate() {
            let old_row = data_row.row;
            let new_row = add_row_offset(self.data_rows.start, index, "유류비 정렬 재배치")?;
            let row_offset = old_row
                .checked_sub(self.data_rows.start)
                .ok_or_else(|| err(format!("정렬 행 매핑 offset 계산 실패: {old_row}")))?;
            let mapping_index = usize::try_from(row_offset)
                .map_err(|source| err_with_source("정렬 행 매핑 index 변환 실패", source))?;
            let Some(slot) = row_mapping.get_mut(mapping_index) else {
                return Err(err(format!("정렬 행 매핑 범위를 벗어났습니다: {old_row}")));
            };
            if slot.replace(new_row).is_none() {
                filled_count = filled_count
                    .checked_add(1)
                    .ok_or_else(|| err("정렬 행 매핑 채움 수 계산 실패"))?;
            }
        }
        if filled_count != row_mapping.len() {
            let data_row_count = data_rows.len();
            return Err(err(format!(
                "정렬 행 매핑에 누락된 항목이 있습니다: {data_row_count} entries"
            )));
        }
        Ok(row_mapping)
    }
    fn sort(&mut self, sort_context: &RankSortContext) -> Result<()> {
        if self.data_rows.start >= self.data_rows.last {
            return Ok(());
        }
        let data_rows = self.sorted_data_rows(sort_context)?;
        let row_mapping = self.row_mapping(&data_rows)?;
        self.apply_sorted_rows(data_rows, &row_mapping)
    }
    fn sorted_data_rows(&self, sort_context: &RankSortContext) -> Result<Vec<SortableRankRow>> {
        let row_count = row_range_len(self.data_rows, "정렬 대상 행 수")?;
        let mut data_rows: Vec<SortableRankRow> = Vec::new();
        data_rows.try_reserve_exact(row_count).map_err(|source| {
            err_with_source(
                format!("정렬 대상 행 메모리 확보 실패: {row_count} rows"),
                source,
            )
        })?;
        for row_num in self.data_rows {
            if !self.ws.has_row(row_num) {
                return Err(missing_sort_target_row_error(row_num));
            }
            let sort_key = self.build_sort_key(row_num, sort_context)?;
            data_rows.push(SortableRankRow {
                row: row_num,
                key: sort_key,
            });
        }
        data_rows.sort_by(|left, right| {
            let left_key = &left.key;
            let right_key = &right.key;
            left_key
                .rank_total
                .is_none()
                .cmp(&right_key.rank_total.is_none())
                .then_with(|| left_key.rank_total.cmp(&right_key.rank_total))
                .then_with(|| {
                    left_key
                        .gasoline
                        .cmp(&right_key.gasoline)
                        .then_with(|| left_key.premium.cmp(&right_key.premium))
                        .then_with(|| left_key.diesel.cmp(&right_key.diesel))
                })
                .then_with(|| left_key.region.cmp(&right_key.region))
                .then_with(|| left_key.name.cmp(&right_key.name))
                .then_with(|| left_key.address.cmp(&right_key.address))
        });
        Ok(data_rows)
    }
    fn write_decimal_value(
        &mut self,
        row: u32,
        target_col: Option<u32>,
        value: Option<ScaledDecimal>,
    ) -> Result<()> {
        let Some(col) = target_col else {
            return Ok(());
        };
        let text =
            value.map(|scaled| format_scaled_value(scaled.as_i128(), DECIMAL_SCALE.as_i128()));
        self.write_text_value(row, Some(col), text.as_deref(), None)
    }
    fn write_squared_value(
        &mut self,
        row: u32,
        target_col: Option<u32>,
        value: Option<ScaledSortKey>,
    ) -> Result<()> {
        let Some(col) = target_col else {
            return Ok(());
        };
        let text = value
            .map(|scaled| format_scaled_value(scaled.as_i128(), DECIMAL_SCALE_SQUARED.as_i128()));
        self.write_text_value(row, Some(col), text.as_deref(), None)
    }
    fn write_text_value(
        &mut self,
        row: u32,
        target_col: Option<u32>,
        value: Option<&str>,
        value_type: Option<&'static str>,
    ) -> Result<()> {
        let Some(col) = target_col else {
            return Ok(());
        };
        self.ws
            .set_formula_cached_value_at(col, row, value, value_type)
    }
}
impl<'source> MasterSheetUpdater<'source> {
    fn adjusted_fuel_price(value: Option<i32>, discount: ScaledDecimal) -> Option<ScaledDecimal> {
        let price = value?;
        i64::from(price)
            .checked_mul(DECIMAL_SCALE.as_i64())?
            .checked_add(discount.as_i64())
            .map(ScaledDecimal)
    }
    fn build_rank_sort_context(
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
    ) -> Result<RankSortContext> {
        let gasoline_qty =
            Self::get_f64_at(ws, 2, 4, shared_strings)?.unwrap_or(ScaledDecimal::ZERO);
        let premium_qty =
            Self::get_f64_at(ws, 2, 5, shared_strings)?.unwrap_or(ScaledDecimal::ZERO);
        let diesel_qty = Self::get_f64_at(ws, 2, 6, shared_strings)?.unwrap_or(ScaledDecimal::ZERO);
        let mut region_rates = TARGET_REGIONS.map(|region| (region, None));
        for row in 4..=13 {
            let region_display = ws.try_get_display_at(3, row, shared_strings)?;
            let region = region_display.trim();
            if region.is_empty() {
                continue;
            }
            if let Some(rate) = Self::get_f64_at(ws, 4, row, shared_strings)? {
                for &mut (target, ref mut slot) in &mut region_rates {
                    if target.label() == region {
                        *slot = Some(rate);
                        break;
                    }
                }
            }
        }
        let total_qty = Self::get_f64_at(ws, 2, 10, shared_strings)?
            .filter(|value| *value != ScaledDecimal::ZERO)
            .or_else(|| {
                let derived_total = gasoline_qty
                    .checked_add(premium_qty)?
                    .checked_add(diesel_qty)?;
                (derived_total != ScaledDecimal::ZERO).then_some(derived_total)
            });
        Ok(RankSortContext {
            gasoline_qty,
            premium_qty,
            diesel_qty,
            total_qty,
            smart_discount: Self::get_f64_at(
                ws,
                SMART_DISCOUNT_INPUT_COL,
                SMART_DISCOUNT_INPUT_ROW,
                shared_strings,
            )?
            .unwrap_or(ScaledDecimal::ZERO),
            region_rates,
        })
    }
    fn collect_new_sources(
        &self,
        matched_source_keys: &HashSet<&str>,
    ) -> Result<Vec<&'source SourceRecord>> {
        let mut new_sources: Vec<&'source SourceRecord> = Vec::new();
        new_sources
            .try_reserve_exact(self.source_index.len())
            .map_err(|source| {
                let source_count = self.source_index.len();
                err_with_source(
                    format!("신규 소스 정렬 목록 메모리 확보 실패: {source_count} sources"),
                    source,
                )
            })?;
        new_sources.extend(
            self.source_index
                .iter()
                .filter(|&(key, _rec)| !matched_source_keys.contains(key.as_str()))
                .map(|(_key, rec)| rec),
        );
        new_sources.sort_unstable_by(|left, right| {
            left.region
                .cmp(right.region)
                .then_with(|| left.name.cmp(&right.name))
                .then_with(|| left.address.cmp(&right.address))
        });
        Ok(new_sources)
    }
    fn compute_total_price(
        sort_context: &RankSortContext,
        adjusted: AdjustedFuelPrices,
    ) -> Option<ScaledSortKey> {
        let mut total = ScaledSortKey::ZERO;
        if sort_context.gasoline_qty != ScaledDecimal::ZERO {
            total = total.checked_add(ScaledSortKey(
                sort_context
                    .gasoline_qty
                    .as_i128()
                    .checked_mul(adjusted.gasoline?.as_i128())?,
            ))?;
        }
        if sort_context.premium_qty != ScaledDecimal::ZERO {
            total = total.checked_add(ScaledSortKey(
                sort_context
                    .premium_qty
                    .as_i128()
                    .checked_mul(adjusted.premium?.as_i128())?,
            ))?;
        }
        if sort_context.diesel_qty != ScaledDecimal::ZERO {
            total = total.checked_add(ScaledSortKey(
                sort_context
                    .diesel_qty
                    .as_i128()
                    .checked_mul(adjusted.diesel?.as_i128())?,
            ))?;
        }
        Some(total)
    }
    fn currency_apply(
        ws: &excel::writer::Worksheet,
        layout: MasterSheetLayout,
        row: u32,
        shared_strings: &[String],
    ) -> Result<bool> {
        let Some(col) = layout.currency_apply else {
            return Ok(false);
        };
        Ok(ws
            .try_get_display_at(col, row, shared_strings)?
            .trim()
            .eq_ignore_ascii_case("Y"))
    }
    fn evaluate_master_row(
        &self,
        identity: ParsedMasterIdentity<'_>,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        layout: MasterSheetLayout,
        old_row: u32,
    ) -> Result<MasterRowDecision<'source>> {
        if identity.address.is_empty() {
            return Ok(MasterRowDecision::Unaddressed);
        }
        let key = normalize_address_key(identity.address.as_ref())?;
        let Some((matched_key, src)) = self.source_index.get_key_value(&key) else {
            let row =
                ParsedMasterRow::read_with_identity(identity, ws, layout, old_row, shared_strings)?;
            let ParsedMasterIdentity {
                address,
                name,
                region,
            } = row.identity;
            return Ok(MasterRowDecision::Deleted {
                normalized_address: key,
                row: StoreRow {
                    address: address.into_owned(),
                    diesel: row.diesel,
                    gasoline: row.gasoline,
                    name: name.into_owned(),
                    old_row,
                    premium: row.premium,
                    region: region.into_owned(),
                },
            });
        };
        let row =
            ParsedMasterRow::read_with_identity(identity, ws, layout, old_row, shared_strings)?;
        let old_brand_display = ws.try_get_display_at(layout.brand, old_row, shared_strings)?;
        let old_self_yn_display = ws.try_get_display_at(layout.self_yn, old_row, shared_strings)?;
        let old_brand = old_brand_display.trim();
        let old_name = row.identity.name.as_ref();
        let old_region = row.identity.region.as_ref();
        let old_self_yn = old_self_yn_display.trim();
        let source_region = src.region;
        let region_changed = !same_trimmed(old_region, source_region);
        let name_changed = !same_trimmed(old_name, &src.name);
        let brand_changed = !same_trimmed(old_brand, &src.brand);
        let self_yn_changed = !old_self_yn
            .chars()
            .filter(|ch| !ch.is_whitespace())
            .eq(src.self_yn.chars().filter(|ch| !ch.is_whitespace()));
        let price_changed =
            row.gasoline != src.gasoline || row.premium != src.premium || row.diesel != src.diesel;
        let change =
            (region_changed || name_changed || brand_changed || self_yn_changed || price_changed)
                .then(|| {
                    let mut reason = String::new();
                    for (changed, label) in [
                        (price_changed, "가격변동"),
                        (region_changed, "지역정정"),
                        (name_changed, "상호변경"),
                        (brand_changed, "상표변경"),
                        (self_yn_changed, "셀프여부변경"),
                    ] {
                        if changed {
                            if !reason.is_empty() {
                                reason.push_str(", ");
                            }
                            reason.push_str(label);
                        }
                    }
                    ChangeRow {
                        old_diesel: row.diesel,
                        old_gasoline: row.gasoline,
                        old_premium: row.premium,
                        record: src,
                        reason,
                    }
                });
        Ok(MasterRowDecision::Matched {
            change,
            matched_key: matched_key.as_str(),
            src,
        })
    }
    fn evaluate_master_rows(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        data_start_row: u32,
        layout: MasterSheetLayout,
    ) -> Result<MasterRowEvaluation<'source>> {
        let row_count = ws.row_count();
        let mut matched_source_keys: HashSet<&str> = HashSet::new();
        matched_source_keys
            .try_reserve(row_count)
            .map_err(|source| {
                err_with_source(
                    format!("매칭 소스 키 집합 메모리 확보 실패: {row_count} entries"),
                    source,
                )
            })?;
        let mut master_address_rows: HashMap<Cow<'source, str>, u32> = HashMap::new();
        master_address_rows
            .try_reserve(row_count)
            .map_err(|source| {
                err_with_source(
                    format!("마스터 주소 행 맵 메모리 확보 실패: {row_count} entries"),
                    source,
                )
            })?;
        let mut kept_source_rows = reserved_row_vec(row_count, "유지 행 목록")?;
        let mut changes = reserved_row_vec(row_count, "변경 행 목록")?;
        let mut deleted = reserved_row_vec(row_count, "삭제 행 목록")?;
        let mut existing_region_counts = [0_usize; TARGET_REGION_COUNT];
        let mut matched_existing_region_counts = [0_usize; TARGET_REGION_COUNT];
        let mut target_region_scratch = String::new();
        for old_row in ws.row_numbers_from(data_start_row) {
            let identity = ParsedMasterIdentity::read(ws, layout, old_row, shared_strings)?;
            if identity.is_empty() {
                continue;
            }
            let existing_region = target_region(
                identity.region.as_ref(),
                identity.address.as_ref(),
                &mut target_region_scratch,
                TargetRegionPolicy::Flexible,
            )?;
            increment_optional_target_region_count(
                &mut existing_region_counts,
                existing_region,
                "마스터 지역별 건수",
            )?;
            let decision =
                self.evaluate_master_row(identity, ws, shared_strings, layout, old_row)?;
            let mut record_address = |key: Cow<'source, str>| -> Result<()> {
                if let Some(first_row) = master_address_rows.get(&key) {
                    return Err(err(format!(
                        "마스터 주소 중복: normalized_address={key}, first_row={first_row}, duplicate_row={old_row}",
                    )));
                }
                master_address_rows.insert(key, old_row);
                Ok(())
            };
            match decision {
                MasterRowDecision::Deleted {
                    normalized_address,
                    row,
                } => {
                    record_address(Cow::Owned(normalized_address))?;
                    deleted.push(row);
                }
                MasterRowDecision::Matched {
                    change,
                    matched_key,
                    src,
                } => {
                    record_address(Cow::Borrowed(matched_key))?;
                    matched_source_keys.insert(matched_key);
                    increment_optional_target_region_count(
                        &mut matched_existing_region_counts,
                        existing_region,
                        "마스터 지역별 기존 주소 일치 건수",
                    )?;
                    if let Some(row_change) = change {
                        changes.push(row_change);
                    }
                    kept_source_rows.push((old_row, Some(src)));
                }
                MasterRowDecision::Unaddressed => kept_source_rows.push((old_row, None)),
            }
        }
        Ok(MasterRowEvaluation {
            changes,
            deleted,
            existing_region_counts,
            kept_source_rows,
            matched_existing_region_counts,
            matched_source_keys,
        })
    }
    fn fuel_sort_value(value: Option<ScaledSortKey>) -> ScaledSortKey {
        value.unwrap_or(ScaledSortKey::MAX)
    }
    fn get_f64_at(
        ws: &excel::writer::Worksheet,
        col: u32,
        row: u32,
        shared_strings: &[String],
    ) -> Result<Option<ScaledDecimal>> {
        let display_text = ws.try_get_display_at(col, row, shared_strings)?;
        let trimmed = display_text.trim();
        if trimmed.is_empty() || trimmed == "-" {
            return Ok(None);
        }
        let invalid_value = || {
            err(format!(
                "유류비 숫자 셀 값이 올바르지 않습니다: row={row}, col={col}, value={trimmed}"
            ))
        };
        let (sign, digits) = trimmed.strip_prefix('-').map_or_else(
            || (1_i64, trimmed.strip_prefix('+').unwrap_or(trimmed)),
            |rest| (-1_i64, rest),
        );
        let mut whole = 0_i64;
        let mut fraction = 0_i64;
        let mut fraction_digit_count = 0_u8;
        let mut has_whole_digit = false;
        let mut parsing_fraction = false;
        for digit_byte in digits.bytes() {
            if digit_byte == b',' {
                continue;
            }
            if digit_byte == b'.' {
                if parsing_fraction {
                    return Err(invalid_value());
                }
                parsing_fraction = true;
                continue;
            }
            if !digit_byte.is_ascii_digit() {
                return Err(invalid_value());
            }
            let digit_raw = digit_byte.wrapping_sub(b'0');
            let digit = i64::from(digit_raw);
            if parsing_fraction {
                if fraction_digit_count >= 6 {
                    continue;
                }
                let Some(next_fraction) = fraction
                    .checked_mul(10)
                    .and_then(|value| value.checked_add(digit))
                else {
                    return Err(invalid_value());
                };
                fraction = next_fraction;
                let Some(next_digit_count) = fraction_digit_count.checked_add(1) else {
                    return Err(invalid_value());
                };
                fraction_digit_count = next_digit_count;
            } else {
                has_whole_digit = true;
                let Some(next_whole) = whole
                    .checked_mul(10)
                    .and_then(|value| value.checked_add(digit))
                else {
                    return Err(invalid_value());
                };
                whole = next_whole;
            }
        }
        if !has_whole_digit {
            return Err(invalid_value());
        }
        while fraction_digit_count < 6 {
            let Some(next_fraction) = fraction.checked_mul(10) else {
                return Err(invalid_value());
            };
            fraction = next_fraction;
            let Some(next_digit_count) = fraction_digit_count.checked_add(1) else {
                return Err(invalid_value());
            };
            fraction_digit_count = next_digit_count;
        }
        let Some(whole_scaled) = whole.checked_mul(DECIMAL_SCALE.as_i64()) else {
            return Err(invalid_value());
        };
        let Some(combined) = whole_scaled.checked_add(fraction) else {
            return Err(invalid_value());
        };
        combined
            .checked_mul(sign)
            .map(ScaledDecimal)
            .map(Some)
            .ok_or_else(invalid_value)
    }
    fn is_exact_zero_at(
        ws: &excel::writer::Worksheet,
        col: u32,
        row: u32,
        shared_strings: &[String],
    ) -> Result<bool> {
        let display = ws.try_get_display_at(col, row, shared_strings)?;
        let trimmed = display.trim();
        let digits = trimmed
            .strip_prefix('+')
            .or_else(|| trimmed.strip_prefix('-'))
            .unwrap_or(trimmed);
        let mut has_digit = false;
        let mut has_decimal_point = false;
        for byte in digits.bytes() {
            match byte {
                b'0' => has_digit = true,
                b',' => {}
                b'.' if !has_decimal_point => has_decimal_point = true,
                _ => return Ok(false),
            }
        }
        Ok(has_digit)
    }
    fn normalize_fuel_price(value: Option<i32>) -> Option<i32> {
        value.filter(|price| *price > 0_i32)
    }
    pub(super) fn update(
        &self,
        book: &mut StdWorkbook,
    ) -> Result<MasterSheetUpdateResult<'source>> {
        let Some((result, filter_target)) =
            book.with_sheet_mut(MASTER_SHEET_NAME, |ws, shared_strings| -> Result<_> {
                let max_cols = ws.max_cell_col().clamp(20, 200);
                let mut found_plan = None;
                for row in 1..=MASTER_HEADER_SCAN_ROWS {
                    if find_header_column(
                        ws,
                        shared_strings,
                        row,
                        max_cols,
                        &["지역화폐적용순위"],
                    )?
                    .is_some()
                    {
                        found_plan = Some(MasterSheetPlan {
                            data_start_row: row.checked_add(1).ok_or_else(|| {
                                err("마스터 데이터 시작 행 계산 중 overflow가 발생했습니다.")
                            })?,
                            header_row: row,
                            layout: MasterSheetLayout::try_from((
                                &*ws,
                                shared_strings,
                                row,
                                max_cols,
                            ))?,
                        });
                        break;
                    }
                }
                let plan = found_plan.ok_or_else(|| {
                    err("유류비 시트에서 헤더 행을 찾지 못했습니다. 필수 컬럼(지역화폐적용순위/지역/상호/상표/셀프여부/주소/휘발유/고급유/경유)을 확인하세요.")
                })?;
                let MasterSheetPlan {
                    data_start_row,
                    layout,
                    ..
                } = plan;
                let evaluation =
                    self.evaluate_master_rows(ws, shared_strings, data_start_row, layout)?;
                let added = self.collect_new_sources(&evaluation.matched_source_keys)?;
                let existing_count = evaluation
                    .kept_source_rows
                    .len()
                    .checked_add(evaluation.deleted.len())
                    .ok_or_else(|| err("기존 유류비 행 수 계산 중 overflow가 발생했습니다."))?;
                let filter_target = MasterRowsRebuilder {
                    ws,
                    shared_strings,
                    plan,
                    deleted: &evaluation.deleted,
                    kept_source_rows: &evaluation.kept_source_rows,
                    new_sources: &added,
                }
                .rebuild(existing_count)?;
                Ok((
                    MasterSheetUpdateResult {
                        added,
                        changes: evaluation.changes,
                        deleted: evaluation.deleted,
                        existing_count,
                        existing_region_counts: evaluation.existing_region_counts,
                        matched_existing_region_counts: evaluation
                            .matched_existing_region_counts,
                    },
                    filter_target,
                ))
            })?
        else {
            return Err(err("마스터 파일에 '유류비' 시트가 없습니다"));
        };
        book.update_filter_database_defined_name(
            MASTER_SHEET_NAME,
            filter_target.header_row,
            filter_target.last_data_row,
            filter_target.last_filter_col,
        )?;
        Ok(result)
    }
    fn write_master_row_from_source(
        ws: &mut excel::writer::Worksheet,
        row: u32,
        src: &SourceRecord,
        region_label: &str,
        layout: MasterSheetLayout,
    ) -> Result<()> {
        if !region_label.trim().is_empty() {
            ws.set_string_at(layout.region, row, region_label)?;
        }
        ws.set_string_at(layout.name, row, &src.name)?;
        ws.set_string_at(layout.brand, row, &src.brand)?;
        ws.set_string_at(layout.self_yn, row, &src.self_yn)?;
        ws.set_string_at(layout.address, row, &src.address)?;
        ws.set_i32_at(layout.gasoline, row, src.gasoline)?;
        ws.set_i32_at(layout.premium, row, src.premium)?;
        ws.set_i32_at(layout.diesel, row, src.diesel)?;
        Ok(())
    }
}
fn append_fuel_total_text(
    parts: &mut String,
    quantity: ScaledDecimal,
    price: Option<ScaledDecimal>,
    label: &str,
) -> Result<bool> {
    if quantity == ScaledDecimal::ZERO {
        return Ok(true);
    }
    let Some(price_value) = price else {
        return Ok(false);
    };
    let Some(total) = quantity.as_i128().checked_mul(price_value.as_i128()) else {
        return Ok(false);
    };
    let scaled_total = ScaledSortKey(total);
    let half_scale = ScaledSortKey(DECIMAL_SCALE_SQUARED.as_i128().div_euclid(2));
    let rounded = scaled_total
        .checked_add(half_scale)
        .ok_or_else(|| err("연료비 반올림 계산 중 overflow가 발생했습니다."))?
        .as_i128()
        .div_euclid(DECIMAL_SCALE_SQUARED.as_i128());
    let sign = if rounded < 0 { "-" } else { "" };
    let digits = rounded.unsigned_abs().to_string();
    if !parts.is_empty() {
        parts.push_str(" / ");
    }
    parts.push_str(label);
    parts.push(' ');
    parts.push_str(sign);
    for (index, digit) in digits.chars().enumerate() {
        if index != 0 && digits.len().saturating_sub(index).is_multiple_of(3) {
            parts.push(',');
        }
        parts.push(digit);
    }
    parts.push('원');
    Ok(true)
}
fn find_header_column(
    ws: &excel::writer::Worksheet,
    shared_strings: &[String],
    row: u32,
    max_cols: u32,
    keys: &[&str],
) -> Result<Option<u32>> {
    for key in keys {
        for col in 1..=max_cols {
            let display = ws.try_get_display_at(col, row, shared_strings)?;
            if display
                .chars()
                .filter(|ch| !ch.is_whitespace())
                .eq(key.chars())
            {
                return Ok(Some(col));
            }
        }
    }
    Ok(None)
}
fn row_range_len(rows: RowRange, context: &'static str) -> Result<usize> {
    if rows.start > rows.last {
        return Ok(0);
    }
    let row_count = rows
        .last
        .checked_sub(rows.start)
        .and_then(|count| count.checked_add(1))
        .ok_or_else(|| err(format!("{context} 계산 중 overflow가 발생했습니다.")))?;
    usize::try_from(row_count)
        .map_err(|source| err_with_source(format!("{context} 변환 실패"), source))
}
fn increment_optional_target_region_count(
    counts: &mut [usize; TARGET_REGION_COUNT],
    maybe_region: Option<TargetRegion>,
    context: &'static str,
) -> Result<()> {
    if let Some(region) = maybe_region {
        increment_target_region_count(counts, region, context)?;
    }
    Ok(())
}
fn reserved_row_vec<T>(row_count: usize, label: &str) -> Result<Vec<T>> {
    let mut rows = Vec::new();
    rows.try_reserve_exact(row_count).map_err(|source| {
        err_with_source(
            format!("{label} 메모리 확보 실패: {row_count} rows"),
            source,
        )
    })?;
    Ok(rows)
}
fn mapped_contiguous_row(
    row_mapping: &[Option<u32>],
    data_start_row: u32,
    old_row: u32,
) -> Option<u32> {
    let offset = old_row.checked_sub(data_start_row)?;
    let index = usize::try_from(offset).ok()?;
    row_mapping.get(index).copied().flatten()
}
fn trim_cow(value: Cow<'_, str>) -> Cow<'_, str> {
    match value {
        Cow::Borrowed(text) => Cow::Borrowed(text.trim()),
        Cow::Owned(mut text) => {
            let leading_len = text.len().saturating_sub(text.trim_start().len());
            let trimmed_len = text.trim().len();
            if trimmed_len == 0 {
                text.clear();
            } else {
                let end_len = leading_len.saturating_add(trimmed_len).min(text.len());
                text.truncate(end_len);
                text.replace_range(..leading_len, "");
            }
            Cow::Owned(text)
        }
    }
}
fn same_trimmed(left: &str, right: &str) -> bool {
    left.trim() == right.trim()
}
fn shift_row(row: u32, increase: u32, decrease: u32, context: &str) -> Result<u32> {
    if increase > 0 {
        row.checked_add(increase).ok_or_else(|| {
            err(format!(
                "{context} 중 행 번호 overflow가 발생했습니다. ({row} + {increase})"
            ))
        })
    } else {
        row.checked_sub(decrease)
            .filter(|shifted| *shifted >= 1)
            .ok_or_else(|| {
                err(format!(
                    "{context} 중 행 번호가 1보다 작아졌습니다. ({row} - {decrease})"
                ))
            })
    }
}
