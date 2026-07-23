use self::format::{format_scaled_value, format_unit_price_text};
use crate::{
    diagnostic::{Result, err, err_with_source},
    excel,
    excel::writer::{Row as StdRow, SharedStringTable, Workbook as StdWorkbook},
    excel::{FuelValues, SourceRecord},
    region::{
        TARGET_REGION_COUNT, TARGET_REGIONS, TargetRegion, TargetRegionPolicy,
        increment_target_region_count, normalize_address_key_into, target_region,
    },
    sheet_util::{add_row_offset, usize_to_u32},
};
use alloc::{borrow::Cow, rc::Rc};
use core::{
    fmt::{Arguments, Write as _},
    mem,
    range::RangeInclusive,
};
use std::collections::HashMap;
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
const MASTER_FORMULA_BUFFER_CAPACITY: usize = 512;
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
    base: RankRowBase,
    key: RankSortKey,
    row: u32,
    smart_discount_excluded: bool,
}
struct RankSortRefresher<'sheet, 'strings> {
    data_rows: RowRange,
    shared_strings: &'strings [Rc<str>],
    ws: &'sheet mut excel::writer::Worksheet,
}
struct RankSortContext {
    display_total_qty: Option<ScaledDecimal>,
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
#[derive(Clone, Copy)]
struct RankRowBase {
    adjusted_prices: AdjustedFuelPrices,
    region_rate: ScaledDecimal,
    smart_discount: ScaledDecimal,
}
struct MasterRowEvaluation<'source> {
    changes: Vec<ChangeRow<'source>>,
    deleted: Vec<StoreRow>,
    existing_address_rows: HashMap<Cow<'source, str>, u32>,
    existing_region_counts: [usize; TARGET_REGION_COUNT],
    kept_source_rows: Vec<(u32, Option<&'source SourceRecord>)>,
    matched_existing_region_counts: [usize; TARGET_REGION_COUNT],
}
impl<'text> ParsedMasterIdentity<'text> {
    fn is_empty(&self) -> bool {
        self.region.is_empty() && self.name.is_empty() && self.address.is_empty()
    }
    fn read(
        ws: &'text excel::writer::Worksheet,
        row: u32,
        shared_strings: &'text [Rc<str>],
    ) -> Result<Self> {
        Ok(Self {
            address: trim_cow(ws.try_get_display_at(COL_ADDRESS, row, shared_strings)?),
            name: trim_cow(ws.try_get_display_at(COL_NAME, row, shared_strings)?),
            region: trim_cow(ws.try_get_display_at(COL_REGION, row, shared_strings)?),
        })
    }
}
impl<'text> ParsedMasterRow<'text> {
    fn read_with_identity(
        identity: ParsedMasterIdentity<'text>,
        ws: &'text excel::writer::Worksheet,
        row: u32,
        shared_strings: &'text [Rc<str>],
    ) -> Result<Self> {
        let smart_discount_excluded = if ws.try_get_formula_at(COL_SMART_DISCOUNT, row)?.is_some() {
            false
        } else {
            let display = ws.try_get_display_at(COL_SMART_DISCOUNT, row, shared_strings)?;
            let trimmed = display.trim();
            let digits = trimmed
                .strip_prefix('+')
                .or_else(|| trimmed.strip_prefix('-'))
                .unwrap_or(trimmed);
            let mut has_digit = false;
            let mut has_decimal_point = false;
            let mut is_zero = true;
            for byte in digits.bytes() {
                match byte {
                    b'0' => has_digit = true,
                    b',' => {}
                    b'.' if !has_decimal_point => has_decimal_point = true,
                    _ => {
                        is_zero = false;
                        break;
                    }
                }
            }
            is_zero && has_digit
        };
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
        base: RankRowBase,
        sort_context: &RankSortContext,
    ) -> Result<Option<ScaledSortKey>> {
        let display_total_qty = sort_context.display_total_qty;
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
            true,
        )?;
        self.write_squared_value(row_num, COL_TOTAL_PRICE, total_price)?;
        self.write_decimal_value(
            row_num,
            COL_REGION_RATE,
            has_total_price.then_some(region_rate),
        )?;
        self.write_squared_value(row_num, COL_REGION_DISCOUNT, regional_discount)?;
        self.write_squared_value(row_num, COL_REGIONAL_TOTAL, rank_total)?;
        self.write_text_value(row_num, COL_SORT_KEY, Some(sort_key.as_ref()), false)?;
        self.write_text_value(
            row_num,
            COL_UNIT_PRICE_WITH_CURRENCY,
            unit_price_with_currency.as_deref(),
            false,
        )?;
        self.write_text_value(
            row_num,
            COL_UNIT_PRICE_WITHOUT_CURRENCY,
            unit_price_without_currency.as_deref(),
            false,
        )?;
        Ok(rank_total)
    }
    fn build_sort_plan(
        &self,
        row_num: u32,
        sort_context: &RankSortContext,
    ) -> Result<SortableRankRow> {
        let identity = ParsedMasterIdentity::read(self.ws, row_num, self.shared_strings)?;
        let row =
            ParsedMasterRow::read_with_identity(identity, self.ws, row_num, self.shared_strings)?;
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
        let currency_apply = self
            .ws
            .try_get_display_at(COL_CURRENCY_APPLY, row_num, self.shared_strings)?
            .trim()
            .eq_ignore_ascii_case("Y");
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
        let base = RankRowBase {
            adjusted_prices,
            region_rate,
            smart_discount,
        };
        let region_multiplier = DECIMAL_SCALE
            .checked_sub(region_rate)
            .ok_or_else(|| err("지역 보정률이 100%를 초과했습니다."))?;
        let regional_adjusted = adjusted_prices.map(|price| {
            price
                .and_then(|value| value.as_i128().checked_mul(region_multiplier.as_i128()))
                .map(ScaledSortKey)
        });
        let rank_total = sort_context.total_qty.and_then(|total_qty| {
            if total_qty == ScaledDecimal::ZERO {
                None
            } else {
                let total_price =
                    MasterSheetUpdater::compute_total_price(sort_context, adjusted_prices)?;
                let discount = RankRowBase::regional_discount(total_price, region_rate)?;
                total_price.checked_sub(discount)
            }
        });
        Ok(SortableRankRow {
            base,
            row: row_num,
            smart_discount_excluded: row.smart_discount_excluded,
            key: RankSortKey {
                rank_total,
                fuels: regional_adjusted.map(|value| value.unwrap_or(ScaledSortKey::MAX)),
                region: row.identity.region.into_owned(),
                name: row.identity.name.into_owned(),
                address: row.identity.address.into_owned(),
            },
        })
    }
    fn collect_ranked_rows(
        &mut self,
        row_plans: &[SortableRankRow],
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
        for (row, plan) in self.data_rows.into_iter().zip(row_plans) {
            if let Some(rank_total) =
                self.build_and_write_formula_cache(row, plan.base, sort_context)?
            {
                ranked_rows.push((rank_total, row));
            } else {
                self.ws
                    .set_formula_cached_value_at(COL_RANK, row, None, false)?;
            }
        }
        Ok(ranked_rows)
    }
    fn normalize_formulas(&mut self, row_plans: &[SortableRankRow]) -> Result<()> {
        let mut formula = String::new();
        formula
            .try_reserve_exact(MASTER_FORMULA_BUFFER_CAPACITY)
            .map_err(|source| err_with_source("마스터 수식 메모리 확보 실패", source))?;
        for (row, plan) in self.data_rows.into_iter().zip(row_plans) {
            let mut set_formula = |col: u32, args: Arguments<'_>| -> Result<()> {
                formula.clear();
                formula
                    .write_fmt(args)
                    .map_err(|source| err_with_source("마스터 수식 작성 실패", source))?;
                self.ws.set_formula_at(col, row, &formula)
            };
            set_formula(
                COL_RANK,
                format_args!(
                    r#"IF($T{row}="","",1+COUNTIF($W${}:$W${},"<"&W{row}))"#,
                    self.data_rows.start, self.data_rows.last,
                ),
            )?;
            if !plan.smart_discount_excluded {
                set_formula(
                    COL_SMART_DISCOUNT,
                    format_args!(
                        r#"IF(AND(IFERROR(SEARCH("{SMART_DISCOUNT_BRAND_KEYWORD}",$C{row}),0)>0,IFERROR(SEARCH("{SMART_DISCOUNT_DIRECT_KEYWORD}",$C{row}),0)>0),$B${SMART_DISCOUNT_INPUT_ROW},0)"#
                    ),
                )?;
            }
            set_formula(
                COL_ADJUSTED_GASOLINE,
                format_args!(r#"IF($G{row}="","",$G{row}+$K{row})"#),
            )?;
            set_formula(
                COL_ADJUSTED_PREMIUM,
                format_args!(r#"IF($H{row}="","",$H{row}+$K{row})"#),
            )?;
            set_formula(
                COL_ADJUSTED_DIESEL,
                format_args!(r#"IF($J{row}="","",$J{row}+$K{row})"#),
            )?;
            set_formula(
                COL_FUEL_TOTAL_TEXT,
                format_args!(
                    r##"IF($B$10=0,"",IFERROR(IF($B$4>0,"휘발유 "&TEXT(IF($L{row}="",1/0,$L{row}*$B$4),"#,##0")&"원","")&IF($B$5>0,IF($B$4>0," / ","")&"고급유 "&TEXT(IF($M{row}="",1/0,$M{row}*$B$5),"#,##0")&"원","")&IF($B$6>0,IF(OR($B$4>0,$B$5>0)," / ","")&"경유 "&TEXT(IF($N{row}="",1/0,$N{row}*$B$6),"#,##0")&"원",""),""))"##
                ),
            )?;
            set_formula(
                COL_TOTAL_PRICE,
                format_args!(
                    r#"IF($B$10=0,"",IFERROR(IF($B$4>0,IF($L{row}="",1/0,$L{row}*$B$4),0)+IF($B$5>0,IF($M{row}="",1/0,$M{row}*$B$5),0)+IF($B$6>0,IF($N{row}="",1/0,$N{row}*$B$6),0),""))"#
                ),
            )?;
            set_formula(
                COL_REGION_RATE,
                format_args!(
                    r#"IF($P{row}="","",IF($Q{row}="Y",IFERROR(VLOOKUP($B{row},$C$4:$D$13,2,FALSE()),0),0))"#
                ),
            )?;
            set_formula(
                COL_REGION_DISCOUNT,
                format_args!(r#"IF($P{row}="","",ROUNDDOWN($P{row}*$R{row},0))"#),
            )?;
            set_formula(
                COL_REGIONAL_TOTAL,
                format_args!(r#"IF($P{row}="","",$P{row}-$S{row})"#),
            )?;
            set_formula(
                COL_UNIT_PRICE_WITH_CURRENCY,
                format_args!(r#"IF($T{row}="","",IF($B$10=0,"",$T{row}/$B$10))"#),
            )?;
            set_formula(
                COL_UNIT_PRICE_WITHOUT_CURRENCY,
                format_args!(r#"IF($P{row}="","",IF($B$10=0,"",$P{row}/$B$10))"#),
            )?;
            set_formula(
                COL_SORT_KEY,
                format_args!(r#"IF($T{row}="",1000000000000000,$T{row})"#),
            )?;
        }
        Ok(())
    }
    fn refresh(&mut self) -> Result<()> {
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
        let display_total_qty =
            MasterSheetUpdater::get_f64_at(self.ws, 2, 10, self.shared_strings)?
                .filter(|value| *value != ScaledDecimal::ZERO);
        let total_qty = display_total_qty.or_else(|| {
            let derived_total = quantities
                .gasoline
                .checked_add(quantities.premium)?
                .checked_add(quantities.diesel)?;
            (derived_total != ScaledDecimal::ZERO).then_some(derived_total)
        });
        let sort_context = RankSortContext {
            display_total_qty,
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
        let row_plans = self.sort(&sort_context)?;
        self.normalize_formulas(&row_plans)?;
        self.refresh_caches(&sort_context, &row_plans)
    }
    fn refresh_caches(
        &mut self,
        sort_context: &RankSortContext,
        row_plans: &[SortableRankRow],
    ) -> Result<()> {
        let mut ranked_rows = self.collect_ranked_rows(row_plans, sort_context)?;
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
                .set_formula_cached_value_at(COL_RANK, row, Some(rank_text.as_str()), false)?;
        }
        Ok(())
    }
    fn sort(&mut self, sort_context: &RankSortContext) -> Result<Vec<SortableRankRow>> {
        let row_count = row_range_len(self.data_rows, "정렬 대상 행 수")?;
        let mut data_rows: Vec<SortableRankRow> = Vec::new();
        data_rows.try_reserve_exact(row_count).map_err(|source| {
            err_with_source(
                format!("정렬 대상 행 메모리 확보 실패: {row_count} rows"),
                source,
            )
        })?;
        for row_num in self.data_rows {
            data_rows.push(self.build_sort_plan(row_num, sort_context)?);
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
        let mut rows = self.ws.take_rows();
        let data_start_index = usize::try_from(self.data_rows.start.saturating_sub(1))
            .map_err(|source| err_with_source("정렬 시작 row 변환 실패", source))?;
        let data_end_index = usize::try_from(self.data_rows.last)
            .map_err(|source| err_with_source("정렬 종료 row 변환 실패", source))?;
        if data_start_index > data_end_index || data_end_index > rows.len() {
            return Err(err("정렬 대상 row 범위가 worksheet를 벗어났습니다."));
        }
        let trailing_rows = rows.split_off(data_end_index);
        let source_rows = rows.split_off(data_start_index);
        let mut available_rows = Vec::new();
        available_rows
            .try_reserve_exact(source_rows.len())
            .map_err(|source| {
                err_with_source(
                    format!("정렬 원본 행 메모리 확보 실패: {row_count} entries"),
                    source,
                )
            })?;
        available_rows.extend(source_rows.into_iter().map(Some));
        rows.try_reserve(available_rows.len().saturating_add(trailing_rows.len()))
            .map_err(|source| {
                err_with_source(
                    format!("정렬 결과 행 메모리 확보 실패: {row_count} entries"),
                    source,
                )
            })?;
        for data_row in &data_rows {
            rows.push(take_row(
                &mut available_rows,
                data_row.row,
                self.data_rows.start,
                "유류비 정렬 원본",
            )?);
        }
        rows.extend(trailing_rows);
        self.ws.replace_rows(rows);
        Ok(data_rows)
    }
    fn write_decimal_value(
        &mut self,
        row: u32,
        col: u32,
        value: Option<ScaledDecimal>,
    ) -> Result<()> {
        let text =
            value.map(|scaled| format_scaled_value(scaled.as_i128(), DECIMAL_SCALE.as_i128()));
        self.write_text_value(row, col, text.as_deref(), false)
    }
    fn write_squared_value(
        &mut self,
        row: u32,
        col: u32,
        value: Option<ScaledSortKey>,
    ) -> Result<()> {
        let text = value
            .map(|scaled| format_scaled_value(scaled.as_i128(), DECIMAL_SCALE_SQUARED.as_i128()));
        self.write_text_value(row, col, text.as_deref(), false)
    }
    fn write_text_value(
        &mut self,
        row: u32,
        col: u32,
        value: Option<&str>,
        string_value: bool,
    ) -> Result<()> {
        self.ws
            .set_formula_cached_value_at(col, row, value, string_value)
    }
}
impl<'source> MasterSheetUpdater<'source> {
    fn collect_new_sources(
        &self,
        existing_address_rows: &HashMap<Cow<'source, str>, u32>,
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
                .filter(|&(key, _rec)| !existing_address_rows.contains_key(key.as_str()))
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
    fn evaluate_master_row(
        &self,
        identity: ParsedMasterIdentity<'_>,
        ws: &excel::writer::Worksheet,
        shared_strings: &[Rc<str>],
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
        shared_strings: &[Rc<str>],
    ) -> Result<MasterRowEvaluation<'source>> {
        let row_count = ws.row_count();
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
        for old_row in ws.row_numbers_from(MASTER_DATA_START_ROW)? {
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
            existing_address_rows: master_address_rows,
            existing_region_counts,
            kept_source_rows,
            matched_existing_region_counts,
        })
    }
    fn get_f64_at(
        ws: &excel::writer::Worksheet,
        col: u32,
        row: u32,
        shared_strings: &[Rc<str>],
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
    fn normalize_fuel_price(value: Option<i32>) -> Option<i32> {
        value.filter(|price| *price > 0_i32)
    }
    pub(super) fn update(
        &self,
        book: &mut StdWorkbook,
    ) -> Result<MasterSheetUpdateResult<'source>> {
        let (ws, shared_strings) = book.master_sheet_mut();
        let evaluation = self.evaluate_master_rows(ws, shared_strings.values())?;
        let added = self.collect_new_sources(&evaluation.existing_address_rows)?;
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
        let final_count = evaluation
            .kept_source_rows
            .len()
            .checked_add(added.len())
            .ok_or_else(|| err("최종 마스터 행 수 계산 중 overflow가 발생했습니다."))?;
        let final_count_u32 = usize_to_u32(final_count, "최종 유류비 행 수")?;
        let mut original_rows = ws.take_rows();
        let template_row_num = last_old_row.unwrap_or(MASTER_DATA_START_ROW);
        let mut added_template_rows = Vec::new();
        if !added.is_empty() {
            let template_index = usize::try_from(template_row_num.saturating_sub(1))
                .map_err(|source| err_with_source("유류비 template row 변환 실패", source))?;
            let template_row = original_rows.get(template_index).ok_or_else(|| {
                err(format!(
                    "유류비 신규행 template이 없습니다: row={template_row_num}"
                ))
            })?;
            added_template_rows
                .try_reserve_exact(added.len())
                .map_err(|source| err_with_source("유류비 신규행 메모리 확보 실패", source))?;
            for _ in &added {
                added_template_rows.push(template_row.try_copy()?);
            }
        }
        let data_start_index = usize::try_from(MASTER_HEADER_ROW)
            .map_err(|source| err_with_source("유류비 데이터 시작 index 변환 실패", source))?;
        let trailing_start_index = usize::try_from(last_old_row.unwrap_or(MASTER_HEADER_ROW))
            .map_err(|source| {
                err_with_source("유류비 trailing row 시작 index 변환 실패", source)
            })?;
        if data_start_index > trailing_start_index || trailing_start_index > original_rows.len() {
            return Err(err(
                "유류비 기존 데이터 row 범위가 worksheet를 벗어났습니다.",
            ));
        }
        let trailing_rows = original_rows.split_off(trailing_start_index);
        let source_rows = original_rows.split_off(data_start_index);
        let mut available_rows = Vec::new();
        available_rows
            .try_reserve_exact(source_rows.len())
            .map_err(|source| err_with_source("유류비 기존행 메모리 확보 실패", source))?;
        available_rows.extend(source_rows.into_iter().map(Some));
        original_rows
            .try_reserve(
                evaluation
                    .kept_source_rows
                    .len()
                    .saturating_add(added_template_rows.len())
                    .saturating_add(trailing_rows.len()),
            )
            .map_err(|source| err_with_source("유류비 결과행 메모리 확보 실패", source))?;
        for &(old_row, _) in &evaluation.kept_source_rows {
            original_rows.push(take_row(
                &mut available_rows,
                old_row,
                MASTER_DATA_START_ROW,
                "유류비 기존행",
            )?);
        }
        original_rows.extend(added_template_rows);
        original_rows.extend(trailing_rows);
        ws.replace_rows(original_rows);
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
fn take_row(
    rows: &mut [Option<StdRow>],
    row: u32,
    first_row: u32,
    context: &str,
) -> Result<StdRow> {
    let index = row
        .checked_sub(first_row)
        .and_then(|offset| usize::try_from(offset).ok())
        .ok_or_else(|| err(format!("{context} row 번호가 범위를 벗어났습니다: {row}")))?;
    rows.get_mut(index)
        .and_then(Option::take)
        .ok_or_else(|| err(format!("{context} XML이 없습니다: row={row}")))
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
