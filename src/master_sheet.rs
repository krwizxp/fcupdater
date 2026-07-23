use self::format::{format_scaled_value, format_unit_price_text, missing_sort_target_row_error};
use crate::{
    diagnostic::{Result, err, err_with_source},
    excel,
    excel::writer::{
        Row as StdRow, SharedStringTable, Workbook as StdWorkbook, remap_formula_rows,
    },
    excel::{FuelValues, SourceRecord},
    region::{
        TARGET_REGION_COUNT, TARGET_REGIONS, TargetRegion, TargetRegionPolicy,
        increment_target_region_count, normalize_address_key_into, target_region,
    },
    sheet_util::{add_row_offset, usize_to_u32},
};
use alloc::{borrow::Cow, collections::BTreeMap};
use core::{fmt::Write as _, mem, range::RangeInclusive};
use std::collections::{HashMap, HashSet};
mod format;
const MASTER_HEADER_ROW: u32 = 14;
const MASTER_DATA_START_ROW: u32 = 15;
const COL_RANK: u32 = 1;
const COL_REGION: u32 = 2;
const COL_NAME: u32 = 3;
const COL_BRAND: u32 = 4;
const COL_SELF_YN: u32 = 5;
const COL_ADDRESS: u32 = 6;
const COL_GASOLINE: u32 = 7;
const COL_PREMIUM: u32 = 8;
const COL_DIESEL: u32 = 10;
const COL_SMART_DISCOUNT: u32 = 11;
const COL_ADJUSTED_GASOLINE: u32 = 12;
const COL_ADJUSTED_PREMIUM: u32 = 13;
const COL_ADJUSTED_DIESEL: u32 = 14;
const COL_FUEL_TOTAL_TEXT: u32 = 15;
const COL_TOTAL_PRICE: u32 = 16;
const COL_CURRENCY_APPLY: u32 = 17;
const COL_REGION_RATE: u32 = 18;
const COL_REGION_DISCOUNT: u32 = 19;
const COL_REGIONAL_TOTAL: u32 = 20;
const COL_UNIT_PRICE_WITH_CURRENCY: u32 = 21;
const COL_UNIT_PRICE_WITHOUT_CURRENCY: u32 = 22;
const COL_SORT_KEY: u32 = 23;
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
    pub old_fuels: FuelValues<Option<i32>>,
    pub reason: String,
    pub record: &'source SourceRecord,
}
#[derive(Debug)]
pub(super) struct StoreRow {
    pub address: String,
    pub fuels: FuelValues<Option<i32>>,
    pub name: String,
    pub old_row: u32,
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
struct SortableRankRow {
    key: RankSortKey,
    row: u32,
}
struct RankSortRefresher<'sheet, 'strings> {
    data_rows: RowRange,
    shared_strings: &'strings [String],
    ws: &'sheet mut excel::writer::Worksheet,
}
struct RankSortContext {
    quantities: FuelValues<ScaledDecimal>,
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
    fuels: FuelValues<ScaledSortKey>,
    name: String,
    rank_total: Option<ScaledSortKey>,
    region: String,
}
type AdjustedFuelPrices = FuelValues<Option<ScaledDecimal>>;
struct ParsedMasterIdentity<'text> {
    address: Cow<'text, str>,
    name: Cow<'text, str>,
    region: Cow<'text, str>,
}
struct ParsedMasterRow<'text> {
    fuels: FuelValues<Option<i32>>,
    identity: ParsedMasterIdentity<'text>,
    smart_discount_excluded: bool,
}
struct RankRowBase {
    adjusted_prices: AdjustedFuelPrices,
    region_rate: ScaledDecimal,
    smart_discount: ScaledDecimal,
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
        row: u32,
        shared_strings: &'text [String],
    ) -> Result<Self> {
        Ok(Self {
            address: trim_cow(ws.try_get_display_at(COL_ADDRESS, row, shared_strings)?),
            name: trim_cow(ws.try_get_display_at(COL_NAME, row, shared_strings)?),
            region: trim_cow(ws.try_get_display_at(COL_REGION, row, shared_strings)?),
        })
    }
}
impl<'text> ParsedMasterRow<'text> {
    fn read(
        ws: &'text excel::writer::Worksheet,
        row: u32,
        shared_strings: &'text [String],
    ) -> Result<Self> {
        let identity = ParsedMasterIdentity::read(ws, row, shared_strings)?;
        Self::read_with_identity(identity, ws, row, shared_strings)
    }
    fn read_with_identity(
        identity: ParsedMasterIdentity<'text>,
        ws: &'text excel::writer::Worksheet,
        row: u32,
        shared_strings: &'text [String],
    ) -> Result<Self> {
        let smart_discount_excluded = ws.try_get_formula_at(COL_SMART_DISCOUNT, row)?.is_none()
            && MasterSheetUpdater::is_exact_zero_at(ws, COL_SMART_DISCOUNT, row, shared_strings)?;
        Ok(Self {
            fuels: FuelValues {
                diesel: MasterSheetUpdater::normalize_fuel_price(ws.get_i32_at(
                    COL_DIESEL,
                    row,
                    shared_strings,
                )?),
                gasoline: MasterSheetUpdater::normalize_fuel_price(ws.get_i32_at(
                    COL_GASOLINE,
                    row,
                    shared_strings,
                )?),
                premium: MasterSheetUpdater::normalize_fuel_price(ws.get_i32_at(
                    COL_PREMIUM,
                    row,
                    shared_strings,
                )?),
            },
            identity,
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
        let adjusted_prices = row.fuels.map(|price| {
            i64::from(price?)
                .checked_mul(DECIMAL_SCALE.as_i64())?
                .checked_add(smart_discount.as_i64())
                .map(ScaledDecimal)
        });
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
impl RankSortRefresher<'_, '_> {
    fn build_and_write_formula_cache(
        &mut self,
        row_num: u32,
        display_total_qty: Option<ScaledDecimal>,
        sort_context: &RankSortContext,
    ) -> Result<Option<ScaledSortKey>> {
        let row = ParsedMasterRow::read(self.ws, row_num, self.shared_strings)?;
        let currency_apply =
            MasterSheetUpdater::currency_apply(self.ws, row_num, self.shared_strings)?;
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
                sort_context.quantities.gasoline,
                prices.gasoline,
                "휘발유",
            )?
            && append_fuel_total_text(
                &mut fuel_total_parts,
                sort_context.quantities.premium,
                prices.premium,
                "고급유",
            )?
            && append_fuel_total_text(
                &mut fuel_total_parts,
                sort_context.quantities.diesel,
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
        let sort_key = rank_total.map_or(Cow::Borrowed("1000000000000000"), |value| {
            Cow::Owned(format_scaled_value(
                value.as_i128(),
                DECIMAL_SCALE_SQUARED.as_i128(),
            ))
        });
        self.write_decimal_value(row_num, COL_SMART_DISCOUNT, Some(base.smart_discount))?;
        self.write_decimal_value(row_num, COL_ADJUSTED_GASOLINE, prices.gasoline)?;
        self.write_decimal_value(row_num, COL_ADJUSTED_PREMIUM, prices.premium)?;
        self.write_decimal_value(row_num, COL_ADJUSTED_DIESEL, prices.diesel)?;
        self.write_text_value(
            row_num,
            COL_FUEL_TOTAL_TEXT,
            fuel_total_text.as_deref(),
            Some("str"),
        )?;
        self.write_squared_value(row_num, COL_TOTAL_PRICE, total_price)?;
        self.write_decimal_value(
            row_num,
            COL_REGION_RATE,
            has_total_price.then_some(region_rate),
        )?;
        self.write_squared_value(row_num, COL_REGION_DISCOUNT, regional_discount)?;
        self.write_squared_value(row_num, COL_REGIONAL_TOTAL, rank_total)?;
        self.write_text_value(row_num, COL_SORT_KEY, Some(sort_key.as_ref()), None)?;
        self.write_text_value(
            row_num,
            COL_UNIT_PRICE_WITH_CURRENCY,
            unit_price_with_currency.as_deref(),
            None,
        )?;
        self.write_text_value(
            row_num,
            COL_UNIT_PRICE_WITHOUT_CURRENCY,
            unit_price_without_currency.as_deref(),
            None,
        )?;
        Ok(rank_total)
    }
    fn build_sort_key(&self, row_num: u32, sort_context: &RankSortContext) -> Result<RankSortKey> {
        let row = ParsedMasterRow::read(self.ws, row_num, self.shared_strings)?;
        let currency_apply =
            MasterSheetUpdater::currency_apply(self.ws, row_num, self.shared_strings)?;
        let base = RankRowBase::read(&row, currency_apply, sort_context)?;
        let adjusted = base.adjusted_prices;
        let region_rate = base.region_rate;
        let region_multiplier = DECIMAL_SCALE
            .checked_sub(region_rate)
            .ok_or_else(|| err("지역 보정률이 100%를 초과했습니다."))?;
        let regional_adjusted = adjusted.map(|price| {
            price
                .and_then(|value| value.as_i128().checked_mul(region_multiplier.as_i128()))
                .map(ScaledSortKey)
        });
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
            fuels: regional_adjusted.map(|value| value.unwrap_or(ScaledSortKey::MAX)),
            region: row.identity.region.into_owned(),
            name: row.identity.name.into_owned(),
            address: row.identity.address.into_owned(),
        })
    }
    fn collect_ranked_rows(
        &mut self,
        display_total_qty: Option<ScaledDecimal>,
        sort_context: &RankSortContext,
    ) -> Result<Vec<(ScaledSortKey, u32)>> {
        let capacity = row_range_len(self.data_rows, "랭크 캐시 대상 행 수")?;
        let mut ranked_rows = Vec::new();
        ranked_rows.try_reserve_exact(capacity).map_err(|source| {
            err_with_source(
                format!("랭크 캐시 목록 메모리 확보 실패: {capacity} rows"),
                source,
            )
        })?;
        for row in self.data_rows {
            if let Some(rank_total) =
                self.build_and_write_formula_cache(row, display_total_qty, sort_context)?
            {
                ranked_rows.push((rank_total, row));
            } else {
                self.ws
                    .set_formula_cached_value_at(COL_RANK, row, None, None)?;
            }
        }
        Ok(ranked_rows)
    }
    fn normalize_smart_discount_formulas(&mut self) -> Result<()> {
        let smart_discount_col = COL_SMART_DISCOUNT;
        for row in self.data_rows {
            let canonical_formula = format!(
                "IF(AND(IFERROR(SEARCH(\"{SMART_DISCOUNT_BRAND_KEYWORD}\",$C{row}),0)>0,IFERROR(SEARCH(\"{SMART_DISCOUNT_DIRECT_KEYWORD}\",$C{row}),0)>0),$B${SMART_DISCOUNT_INPUT_ROW},0)"
            );
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
        let gasoline = MasterSheetUpdater::get_f64_at(self.ws, 2, 4, self.shared_strings)?
            .unwrap_or(ScaledDecimal::ZERO);
        let premium = MasterSheetUpdater::get_f64_at(self.ws, 2, 5, self.shared_strings)?
            .unwrap_or(ScaledDecimal::ZERO);
        let diesel = MasterSheetUpdater::get_f64_at(self.ws, 2, 6, self.shared_strings)?
            .unwrap_or(ScaledDecimal::ZERO);
        let quantities = FuelValues {
            diesel,
            gasoline,
            premium,
        };
        let mut region_rates = TARGET_REGIONS.map(|region| (region, None));
        for row in 4..=13 {
            let region_display = self.ws.try_get_display_at(3, row, self.shared_strings)?;
            let region = region_display.trim();
            if region.is_empty() {
                continue;
            }
            if let Some(rate) =
                MasterSheetUpdater::get_f64_at(self.ws, 4, row, self.shared_strings)?
            {
                for &mut (target, ref mut slot) in &mut region_rates {
                    if target.label() == region {
                        *slot = Some(rate);
                        break;
                    }
                }
            }
        }
        let total_qty = MasterSheetUpdater::get_f64_at(self.ws, 2, 10, self.shared_strings)?
            .filter(|value| *value != ScaledDecimal::ZERO)
            .or_else(|| {
                let derived_total = quantities
                    .gasoline
                    .checked_add(quantities.premium)?
                    .checked_add(quantities.diesel)?;
                (derived_total != ScaledDecimal::ZERO).then_some(derived_total)
            });
        let sort_context = RankSortContext {
            quantities,
            total_qty,
            smart_discount: MasterSheetUpdater::get_f64_at(
                self.ws,
                SMART_DISCOUNT_INPUT_COL,
                SMART_DISCOUNT_INPUT_ROW,
                self.shared_strings,
            )?
            .unwrap_or(ScaledDecimal::ZERO),
            region_rates,
        };
        self.sort(&sort_context)?;
        self.repair_formulas()?;
        self.refresh_caches(&sort_context)
    }
    fn refresh_caches(&mut self, sort_context: &RankSortContext) -> Result<()> {
        let display_total_qty = MasterSheetUpdater::get_f64_at(self.ws, 2, 10, self.shared_strings)
            .map(|value| value.filter(|qty| *qty != ScaledDecimal::ZERO))?;
        self.ws
            .clear_formula_cached_values_in_range(self.data_rows)?;
        let mut ranked_rows = self.collect_ranked_rows(display_total_qty, sort_context)?;
        ranked_rows.sort_unstable();
        let mut rank_text = String::new();
        rank_text
            .try_reserve_exact(USIZE_DECIMAL_TEXT_MAX_LEN)
            .map_err(|source| err_with_source("지역화폐 순위 문자열 메모리 확보 실패", source))?;
        let mut previous_total = None;
        for (index, (current, row)) in ranked_rows.into_iter().enumerate() {
            if previous_total != Some(current) {
                let rank = index
                    .checked_add(1)
                    .ok_or_else(|| err("지역화폐 순위 계산 중 overflow가 발생했습니다."))?;
                rank_text.clear();
                write!(&mut rank_text, "{rank}")
                    .map_err(|source| err_with_source("지역화폐 순위 문자열 작성 실패", source))?;
                previous_total = Some(current);
            }
            self.ws
                .set_formula_cached_value_at(COL_RANK, row, Some(rank_text.as_str()), None)?;
        }
        Ok(())
    }
    fn repair_formulas(&mut self) -> Result<()> {
        let range_rewriter = excel::writer::AbsoluteColumnRangeRewriter::from((
            COL_SORT_KEY,
            self.data_rows.start,
            self.data_rows.last,
        ));
        for row in self.data_rows {
            let Some(formula) = self.ws.try_get_formula_at(COL_RANK, row)? else {
                continue;
            };
            let rewrite_result = range_rewriter.rewrite(formula.as_ref())?;
            if let Some(updated) = rewrite_result {
                self.ws.set_formula_at(COL_RANK, row, &updated)?;
            }
        }
        Ok(())
    }
    fn sort(&mut self, sort_context: &RankSortContext) -> Result<()> {
        if self.data_rows.start == self.data_rows.last {
            return Ok(());
        }
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
                        .fuels
                        .gasoline
                        .cmp(&right_key.fuels.gasoline)
                        .then_with(|| left_key.fuels.premium.cmp(&right_key.fuels.premium))
                        .then_with(|| left_key.fuels.diesel.cmp(&right_key.fuels.diesel))
                })
                .then_with(|| left_key.region.cmp(&right_key.region))
                .then_with(|| left_key.name.cmp(&right_key.name))
                .then_with(|| left_key.address.cmp(&right_key.address))
        });
        let mut row_mapping = Vec::new();
        row_mapping.try_reserve_exact(row_count).map_err(|source| {
            err_with_source(
                format!("정렬 행 매핑 메모리 확보 실패: {row_count} entries"),
                source,
            )
        })?;
        row_mapping.resize(row_count, 0_u32);
        for (index, data_row) in data_rows.iter().enumerate() {
            let old_row = data_row.row;
            let new_row = add_row_offset(self.data_rows.start, index, "유류비 정렬 재배치")?;
            let row_offset = old_row
                .checked_sub(self.data_rows.start)
                .ok_or_else(|| err(format!("정렬 행 매핑 offset 계산 실패: {old_row}")))?;
            let mapping_index = usize::try_from(row_offset)
                .map_err(|source| err_with_source("정렬 행 매핑 index 변환 실패", source))?;
            let slot = row_mapping
                .get_mut(mapping_index)
                .ok_or_else(|| err(format!("정렬 행 매핑 범위를 벗어났습니다: {old_row}")))?;
            *slot = new_row;
        }
        let mut rows = self.ws.take_rows();
        let mut sorted_rows: Vec<StdRow> = Vec::new();
        sorted_rows.try_reserve_exact(row_count).map_err(|source| {
            err_with_source(
                format!("정렬 분리 행 메모리 확보 실패: {row_count} entries"),
                source,
            )
        })?;
        for data_row in &data_rows {
            sorted_rows.push(
                rows.remove(&data_row.row)
                    .ok_or_else(|| missing_sort_target_row_error(data_row.row))?,
            );
        }
        for (index, mut row) in sorted_rows.into_iter().enumerate() {
            let new_row = add_row_offset(self.data_rows.start, index, "유류비 정렬 재배치")?;
            remap_formula_rows(&mut row, &|old_ref_row| {
                let mapped_row = old_ref_row
                    .checked_sub(self.data_rows.start)
                    .and_then(|offset| usize::try_from(offset).ok())
                    .and_then(|mapping_index| row_mapping.get(mapping_index).copied())
                    .unwrap_or(old_ref_row);
                Ok(mapped_row)
            })?;
            rows.insert(new_row, row);
        }
        self.ws.replace_rows(rows);
        Ok(())
    }
    fn write_decimal_value(
        &mut self,
        row: u32,
        col: u32,
        value: Option<ScaledDecimal>,
    ) -> Result<()> {
        let text =
            value.map(|scaled| format_scaled_value(scaled.as_i128(), DECIMAL_SCALE.as_i128()));
        self.write_text_value(row, col, text.as_deref(), None)
    }
    fn write_squared_value(
        &mut self,
        row: u32,
        col: u32,
        value: Option<ScaledSortKey>,
    ) -> Result<()> {
        let text = value
            .map(|scaled| format_scaled_value(scaled.as_i128(), DECIMAL_SCALE_SQUARED.as_i128()));
        self.write_text_value(row, col, text.as_deref(), None)
    }
    fn write_text_value(
        &mut self,
        row: u32,
        col: u32,
        value: Option<&str>,
        value_type: Option<&'static str>,
    ) -> Result<()> {
        self.ws
            .set_formula_cached_value_at(col, row, value, value_type)
    }
}
impl<'source> MasterSheetUpdater<'source> {
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
        for (quantity, price) in [
            (sort_context.quantities.gasoline, adjusted.gasoline),
            (sort_context.quantities.premium, adjusted.premium),
            (sort_context.quantities.diesel, adjusted.diesel),
        ] {
            if quantity != ScaledDecimal::ZERO {
                total = total.checked_add(ScaledSortKey(
                    quantity.as_i128().checked_mul(price?.as_i128())?,
                ))?;
            }
        }
        Some(total)
    }
    fn currency_apply(
        ws: &excel::writer::Worksheet,
        row: u32,
        shared_strings: &[String],
    ) -> Result<bool> {
        Ok(ws
            .try_get_display_at(COL_CURRENCY_APPLY, row, shared_strings)?
            .trim()
            .eq_ignore_ascii_case("Y"))
    }
    fn evaluate_master_row(
        &self,
        identity: ParsedMasterIdentity<'_>,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        old_row: u32,
        address_key_scratch: &mut String,
    ) -> Result<MasterRowDecision<'source>> {
        if identity.address.is_empty() {
            return Ok(MasterRowDecision::Unaddressed);
        }
        normalize_address_key_into(identity.address.as_ref(), address_key_scratch)?;
        let Some((matched_key, src)) = self
            .source_index
            .get_key_value(address_key_scratch.as_str())
        else {
            let row = ParsedMasterRow::read_with_identity(identity, ws, old_row, shared_strings)?;
            let ParsedMasterIdentity {
                address,
                name,
                region,
            } = row.identity;
            return Ok(MasterRowDecision::Deleted {
                normalized_address: mem::take(address_key_scratch),
                row: StoreRow {
                    address: address.into_owned(),
                    fuels: row.fuels,
                    name: name.into_owned(),
                    old_row,
                    region: region.into_owned(),
                },
            });
        };
        let row = ParsedMasterRow::read_with_identity(identity, ws, old_row, shared_strings)?;
        let old_brand_display = ws.try_get_display_at(COL_BRAND, old_row, shared_strings)?;
        let old_self_yn_display = ws.try_get_display_at(COL_SELF_YN, old_row, shared_strings)?;
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
        let price_changed = row.fuels != src.fuels;
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
                        old_fuels: row.fuels,
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
        for old_row in ws.row_numbers_from(MASTER_DATA_START_ROW) {
            let identity = ParsedMasterIdentity::read(ws, old_row, shared_strings)?;
            if identity.is_empty() {
                continue;
            }
            let existing_region = target_region(
                identity.region.as_ref(),
                identity.address.as_ref(),
                &mut target_region_scratch,
                TargetRegionPolicy::Flexible,
            )?;
            increment_optional_target_region_count(&mut existing_region_counts, existing_region);
            let decision = self.evaluate_master_row(
                identity,
                ws,
                shared_strings,
                old_row,
                &mut target_region_scratch,
            )?;
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
                    );
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
                fraction = fraction.saturating_mul(10).saturating_add(digit);
                fraction_digit_count = fraction_digit_count.saturating_add(1);
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
            fraction = fraction.saturating_mul(10);
            fraction_digit_count = fraction_digit_count.saturating_add(1);
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
        let (ws, shared_strings) = book.master_sheet_mut();
        let evaluation = self.evaluate_master_rows(ws, shared_strings.values())?;
        let added = self.collect_new_sources(&evaluation.matched_source_keys)?;
        let existing_count = evaluation
            .kept_source_rows
            .len()
            .checked_add(evaluation.deleted.len())
            .ok_or_else(|| err("기존 유류비 행 수 계산 중 overflow가 발생했습니다."))?;
        let last_old_row = evaluation
            .kept_source_rows
            .last()
            .map(|&(row, _)| row)
            .max(evaluation.deleted.last().map(|row| row.old_row));
        let old_data_rows = RowRange {
            start: MASTER_DATA_START_ROW,
            last: last_old_row.unwrap_or(MASTER_HEADER_ROW),
        };
        let mut original_rows = ws.take_rows();
        let final_count = evaluation
            .kept_source_rows
            .len()
            .checked_add(added.len())
            .ok_or_else(|| err("최종 마스터 행 수 계산 중 overflow가 발생했습니다."))?;
        let old_count_u32 = usize_to_u32(existing_count, "기존 유류비 행 수")?;
        let final_count_u32 = usize_to_u32(final_count, "최종 유류비 행 수")?;
        let row_mapper = RowMapper {
            decrease: old_count_u32.saturating_sub(final_count_u32),
            deleted: &evaluation.deleted,
            increase: final_count_u32.saturating_sub(old_count_u32),
            old_data_rows,
        };
        let template_row_num = last_old_row.unwrap_or(MASTER_DATA_START_ROW);
        let mut new_rows_map = BTreeMap::new();
        if !added.is_empty() {
            let template_row = original_rows.get(&template_row_num).ok_or_else(|| {
                err(format!(
                    "유류비 신규행 template이 없습니다: row={template_row_num}"
                ))
            })?;
            for i in 0..added.len() {
                let offset = evaluation
                    .kept_source_rows
                    .len()
                    .checked_add(i)
                    .ok_or_else(|| err("유류비 신규행 오프셋 계산 중 overflow가 발생했습니다."))?;
                let new_row = add_row_offset(MASTER_DATA_START_ROW, offset, "유류비 신규행 추가")?;
                let resolver = |old_ref_row: u32| {
                    if old_ref_row == template_row_num {
                        Ok(new_row)
                    } else {
                        row_mapper.map(old_ref_row)
                    }
                };
                new_rows_map.insert(new_row, template_row.copy_with_row_mapping(&resolver)?);
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
        for (i, &(old_row, _)) in evaluation.kept_source_rows.iter().enumerate() {
            let new_row = add_row_offset(MASTER_DATA_START_ROW, i, "유류비 기존행 재배치")?;
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
        ws.replace_rows(new_rows_map);
        for (i, &(_, source)) in evaluation.kept_source_rows.iter().enumerate() {
            let new_row = add_row_offset(MASTER_DATA_START_ROW, i, "유류비 기존행 재배치")?;
            if let Some(src) = source {
                Self::write_master_row_from_source(ws, shared_strings, new_row, src)?;
            }
        }
        for (i, &source) in added.iter().enumerate() {
            let offset = evaluation
                .kept_source_rows
                .len()
                .checked_add(i)
                .ok_or_else(|| err("유류비 신규행 오프셋 계산 중 overflow가 발생했습니다."))?;
            let new_row = add_row_offset(MASTER_DATA_START_ROW, offset, "유류비 신규행 추가")?;
            Self::write_master_row_from_source(ws, shared_strings, new_row, source)?;
            ws.set_i32_at(COL_SMART_DISCOUNT, new_row, None)?;
        }
        let last_data_row = MASTER_DATA_START_ROW
            .checked_add(final_count_u32.saturating_sub(1))
            .ok_or_else(|| err("유류비 마지막 행 계산 중 overflow가 발생했습니다."))?;
        if final_count > 0 {
            RankSortRefresher {
                data_rows: RowRange {
                    start: MASTER_DATA_START_ROW,
                    last: last_data_row,
                },
                shared_strings: shared_strings.values(),
                ws,
            }
            .refresh()?;
        }
        ws.update_auto_filter_ref(last_data_row)?;
        ws.prune_empty_style_artifacts_after_col(COL_SORT_KEY)?;
        ws.update_dimension()?;
        ws.extend_conditional_formats(
            old_data_rows,
            RowRange {
                start: MASTER_DATA_START_ROW,
                last: last_data_row,
            },
            &[COL_RANK],
        )?;
        book.update_filter_database_defined_name(last_data_row)?;
        Ok(MasterSheetUpdateResult {
            added,
            changes: evaluation.changes,
            deleted: evaluation.deleted,
            existing_count,
            existing_region_counts: evaluation.existing_region_counts,
            matched_existing_region_counts: evaluation.matched_existing_region_counts,
        })
    }
    fn write_master_row_from_source(
        ws: &mut excel::writer::Worksheet,
        shared_strings: &mut SharedStringTable,
        row: u32,
        src: &SourceRecord,
    ) -> Result<()> {
        for (col, value) in [
            (COL_REGION, src.region),
            (COL_NAME, src.name.as_str()),
            (COL_BRAND, src.brand.as_str()),
            (COL_SELF_YN, src.self_yn.as_str()),
            (COL_ADDRESS, src.address.as_str()),
        ] {
            shared_strings.set_cell(ws, col, row, value)?;
        }
        ws.set_i32_at(COL_GASOLINE, row, src.fuels.gasoline)?;
        ws.set_i32_at(COL_PREMIUM, row, src.fuels.premium)?;
        ws.set_i32_at(COL_DIESEL, row, src.fuels.diesel)?;
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
fn row_range_len(rows: RowRange, context: &'static str) -> Result<usize> {
    let row_count = rows
        .last
        .checked_sub(rows.start)
        .and_then(|count| count.checked_add(1))
        .ok_or_else(|| err(format!("{context} 계산 중 overflow가 발생했습니다.")))?;
    usize::try_from(row_count)
        .map_err(|source| err_with_source(format!("{context} 변환 실패"), source))
}
const fn increment_optional_target_region_count(
    counts: &mut [usize; TARGET_REGION_COUNT],
    maybe_region: Option<TargetRegion>,
) {
    if let Some(region) = maybe_region {
        increment_target_region_count(counts, region);
    }
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
