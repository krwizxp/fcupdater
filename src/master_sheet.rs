use self::format::format_scaled_value_into;
use crate::{
    diagnostic::{
        Result, append_fmt, err, err_with_source, try_string_with_capacity, try_vec_with_capacity,
    },
    excel,
    excel::writer::{SharedStringTable, Workbook as StdWorkbook, format_excel_ratio_into},
    excel::{FuelValues, SourceRecord},
    region::{
        TARGET_REGION_COUNT, TargetRegion, TargetRegionPolicy, increment_target_region_count,
        normalize_address_key_into, target_region,
    },
    sheet_util::{add_row_offset, usize_to_u32},
    u32_to_usize,
};
use alloc::borrow::Cow;
use core::{
    fmt::{Arguments, NumBuffer},
    mem,
};
use std::{
    collections::{HashMap, hash_map::Entry},
    process,
};
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
pub(super) struct MasterSheetUpdater<'source> {
    pub source_index: &'source HashMap<String, SourceRecord>,
}
pub(super) struct ChangeRow<'source> {
    pub old_fuels: FuelValues<Option<i32>>,
    pub reason: String,
    pub record: &'source SourceRecord,
}
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
    pub last_data_row: u32,
    pub matched_existing_region_counts: [usize; TARGET_REGION_COUNT],
}
#[derive(Clone, Copy, Eq, PartialEq)]
struct ScaledDecimal(i64);
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
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
    fn regional_discount(self, region_rate: ScaledDecimal) -> Option<Self> {
        self.checked_mul(Self(region_rate.as_i128()))?
            .checked_div(DECIMAL_SCALE_CUBED)?
            .checked_mul(DECIMAL_SCALE_SQUARED)
    }
}
struct SortableRankRow<'text> {
    address: &'text str,
    adjusted_prices: AdjustedFuelPrices,
    fuels: FuelValues<ScaledSortKey>,
    name: &'text str,
    rank_total: Option<ScaledSortKey>,
    region: &'text str,
    region_rate: ScaledDecimal,
    smart_discount: ScaledDecimal,
    smart_discount_excluded: bool,
    source_index: usize,
}
struct RankSortRefresher<'sheet, 'strings> {
    data_last_row: u32,
    shared_strings: &'strings SharedStringTable,
    ws: &'sheet mut excel::writer::Worksheet,
}
struct RankSortContext {
    quantities: FuelValues<ScaledDecimal>,
    region_rates: [Option<ScaledDecimal>; TARGET_REGION_COUNT],
    smart_discount: ScaledDecimal,
    total_qty: Option<ScaledDecimal>,
}
struct FormulaBuffers {
    cache: String,
    formula: String,
}
type AdjustedFuelPrices = FuelValues<Option<ScaledDecimal>>;
struct MasterRowEvaluation<'source> {
    added: Vec<&'source SourceRecord>,
    changes: Vec<ChangeRow<'source>>,
    deleted: Vec<StoreRow>,
    existing_region_counts: [usize; TARGET_REGION_COUNT],
    kept_source_rows: Vec<(u32, Option<&'source SourceRecord>)>,
    matched_existing_region_counts: [usize; TARGET_REGION_COUNT],
}
impl<'strings> RankSortRefresher<'_, 'strings> {
    fn apply_formula_cache(
        &mut self,
        row: u32,
        col: u32,
        formula_args: Arguments<'_>,
        value: Option<&str>,
        string_value: bool,
        formula: &mut String,
    ) -> Result<()> {
        formula.clear();
        append_fmt(formula, formula_args);
        self.ws
            .set_formula_at_with_cache(col, row, formula, value, string_value)
    }
    fn apply_numeric_formula_cache(
        &mut self,
        row: u32,
        col: u32,
        formula_args: Arguments<'_>,
        value: Option<i128>,
        scale: i128,
        buffers: &mut FormulaBuffers,
    ) -> Result<()> {
        if let Some(scaled) = value {
            format_scaled_value_into(&mut buffers.cache, scaled, scale);
            let cached = buffers.cache.as_str();
            self.apply_formula_cache(
                row,
                col,
                formula_args,
                Some(cached),
                false,
                &mut buffers.formula,
            )
        } else {
            self.apply_formula_cache(row, col, formula_args, None, false, &mut buffers.formula)
        }
    }
    fn apply_row_formulas_and_caches(
        &mut self,
        row_num: u32,
        plan: &SortableRankRow<'_>,
        sort_context: &RankSortContext,
        rank_cache: Option<&str>,
        buffers: &mut FormulaBuffers,
    ) -> Result<()> {
        let mut row_buffer = NumBuffer::new();
        let row_text = row_num.format_into(&mut row_buffer);
        let total_qty = sort_context.total_qty;
        let prices = plan.adjusted_prices;
        let total_price =
            total_qty.and_then(|_| MasterSheetUpdater::compute_total_price(sort_context, prices));
        let has_total_price = total_price.is_some();
        let region_rate = if has_total_price {
            plan.region_rate
        } else {
            ScaledDecimal::ZERO
        };
        let regional_discount = total_price.and_then(|value| value.regional_discount(region_rate));
        let rank_total = total_qty.and(plan.rank_total);
        let data_start = MASTER_DATA_START_ROW;
        let data_last = self.data_last_row;
        self.apply_formula_cache(
            row_num,
            COL_RANK,
            format_args!(
                r#"IF($T{row_text}="","",1+COUNTIF($W${data_start}:$W${data_last},"<"&W{row_text}))"#
            ),
            rank_cache,
            false,
            &mut buffers.formula,
        )?;
        buffers.cache.clear();
        let has_fuel_total_text = total_qty.is_some()
            && append_fuel_total_text(
                &mut buffers.cache,
                sort_context.quantities.gasoline,
                prices.gasoline,
                "휘발유",
            )?
            && append_fuel_total_text(
                &mut buffers.cache,
                sort_context.quantities.premium,
                prices.premium,
                "고급유",
            )?
            && append_fuel_total_text(
                &mut buffers.cache,
                sort_context.quantities.diesel,
                prices.diesel,
                "경유",
            )?;
        let fuel_total_cache = has_fuel_total_text.then_some(buffers.cache.as_str());
        self.apply_formula_cache(
            row_num,
            COL_FUEL_TOTAL_TEXT,
            format_args!(
                r##"IF($B$10=0,"",IFERROR(IF($B$4>0,"휘발유 "&TEXT(IF($L{row_text}="",1/0,$L{row_text}*$B$4),"#,##0")&"원","")&IF($B$5>0,IF($B$4>0," / ","")&"고급유 "&TEXT(IF($M{row_text}="",1/0,$M{row_text}*$B$5),"#,##0")&"원","")&IF($B$6>0,IF(OR($B$4>0,$B$5>0)," / ","")&"경유 "&TEXT(IF($N{row_text}="",1/0,$N{row_text}*$B$6),"#,##0")&"원",""),""))"##
            ),
            fuel_total_cache,
            true,
            &mut buffers.formula,
        )?;
        format_scaled_value_into(
            &mut buffers.cache,
            plan.smart_discount.as_i128(),
            DECIMAL_SCALE.as_i128(),
        );
        if plan.smart_discount_excluded {
            self.ws.set_formula_cached_value_at(
                COL_SMART_DISCOUNT,
                row_num,
                Some(buffers.cache.as_str()),
                false,
            )?;
        } else {
            let smart_discount_cache = buffers.cache.as_str();
            self.apply_formula_cache(
                row_num,
                COL_SMART_DISCOUNT,
                format_args!(
                    r#"IF(AND(IFERROR(SEARCH("{SMART_DISCOUNT_BRAND_KEYWORD}",$C{row_text}),0)>0,IFERROR(SEARCH("{SMART_DISCOUNT_DIRECT_KEYWORD}",$C{row_text}),0)>0),$B${SMART_DISCOUNT_INPUT_ROW},0)"#
                ),
                Some(smart_discount_cache),
                false,
                &mut buffers.formula,
            )?;
        }
        for (col, source_col, price) in [
            (COL_ADJUSTED_GASOLINE, "G", prices.gasoline),
            (COL_ADJUSTED_PREMIUM, "H", prices.premium),
            (COL_ADJUSTED_DIESEL, "J", prices.diesel),
        ] {
            self.apply_numeric_formula_cache(
                row_num,
                col,
                format_args!(
                    r#"IF(${source_col}{row_text}="","",${source_col}{row_text}+$K{row_text})"#
                ),
                price.map(|ScaledDecimal(value)| i128::from(value)),
                DECIMAL_SCALE.as_i128(),
                buffers,
            )?;
        }
        self.apply_numeric_formula_cache(
            row_num,
            COL_TOTAL_PRICE,
            format_args!(
                r#"IF($B$10=0,"",IFERROR(IF($B$4>0,IF($L{row_text}="",1/0,$L{row_text}*$B$4),0)+IF($B$5>0,IF($M{row_text}="",1/0,$M{row_text}*$B$5),0)+IF($B$6>0,IF($N{row_text}="",1/0,$N{row_text}*$B$6),0),""))"#
            ),
            total_price.map(ScaledSortKey::as_i128),
            DECIMAL_SCALE_SQUARED.as_i128(),
            buffers,
        )?;
        self.apply_numeric_formula_cache(
            row_num,
            COL_REGION_RATE,
            format_args!(
                r#"IF($P{row_text}="","",IF($Q{row_text}="Y",IFERROR(VLOOKUP($B{row_text},$C$4:$D$13,2,FALSE()),0),0))"#
            ),
            has_total_price.then_some(region_rate.as_i128()),
            DECIMAL_SCALE.as_i128(),
            buffers,
        )?;
        self.apply_numeric_formula_cache(
            row_num,
            COL_REGION_DISCOUNT,
            format_args!(r#"IF($P{row_text}="","",ROUNDDOWN($P{row_text}*$R{row_text},0))"#),
            regional_discount.map(ScaledSortKey::as_i128),
            DECIMAL_SCALE_SQUARED.as_i128(),
            buffers,
        )?;
        self.apply_numeric_formula_cache(
            row_num,
            COL_REGIONAL_TOTAL,
            format_args!(r#"IF($P{row_text}="","",$P{row_text}-$S{row_text})"#),
            rank_total.map(ScaledSortKey::as_i128),
            DECIMAL_SCALE_SQUARED.as_i128(),
            buffers,
        )?;
        if let Some(value) = rank_total {
            format_scaled_value_into(
                &mut buffers.cache,
                value.as_i128(),
                DECIMAL_SCALE_SQUARED.as_i128(),
            );
        } else {
            buffers.cache.clear();
            buffers.cache.push_str("1000000000000000");
        }
        let sort_key_cache = buffers.cache.as_str();
        self.apply_formula_cache(
            row_num,
            COL_SORT_KEY,
            format_args!(r#"IF($T{row_text}="",1000000000000000,$T{row_text})"#),
            Some(sort_key_cache),
            false,
            &mut buffers.formula,
        )?;
        for (col, total_col, value) in [
            (COL_UNIT_PRICE_WITH_CURRENCY, "T", rank_total),
            (COL_UNIT_PRICE_WITHOUT_CURRENCY, "P", total_price),
        ] {
            buffers.cache.clear();
            let has_value = if let Some((total, qty)) = value.zip(total_qty)
                && qty != ScaledDecimal::ZERO
            {
                let denominator = qty
                    .as_i128()
                    .checked_mul(DECIMAL_SCALE.as_i128())
                    .ok_or_else(|| err("단가 분모 계산 중 overflow가 발생했습니다."))?;
                format_excel_ratio_into(&mut buffers.cache, total.as_i128(), denominator)?;
                true
            } else {
                false
            };
            self.apply_formula_cache(
                row_num,
                col,
                format_args!(
                    r#"IF(${total_col}{row_text}="","",IF($B$10=0,"",${total_col}{row_text}/$B$10))"#
                ),
                has_value.then_some(buffers.cache.as_str()),
                false,
                &mut buffers.formula,
            )?;
        }
        Ok(())
    }
    fn build_sort_plan(
        &self,
        source_index: usize,
        row_num: u32,
        sort_context: &RankSortContext,
    ) -> Result<SortableRankRow<'strings>> {
        let address = self
            .ws
            .try_get_fixed_text_at(COL_ADDRESS, row_num, self.shared_strings)?
            .trim();
        let name = self
            .ws
            .try_get_fixed_text_at(COL_NAME, row_num, self.shared_strings)?
            .trim();
        let region = self
            .ws
            .try_get_fixed_text_at(COL_REGION, row_num, self.shared_strings)?
            .trim();
        let fuels = read_master_fuels(self.ws, row_num, self.shared_strings)?;
        let smart_discount_excluded = self
            .ws
            .try_get_formula_at(COL_SMART_DISCOUNT, row_num)?
            .is_none()
            && MasterSheetUpdater::get_f64_at(
                self.ws,
                COL_SMART_DISCOUNT,
                row_num,
                self.shared_strings,
            )? == Some(ScaledDecimal::ZERO);
        let default_smart_discount = if name.contains(SMART_DISCOUNT_BRAND_KEYWORD)
            && name.contains(SMART_DISCOUNT_DIRECT_KEYWORD)
        {
            sort_context.smart_discount
        } else {
            ScaledDecimal::ZERO
        };
        let smart_discount = if smart_discount_excluded {
            ScaledDecimal::ZERO
        } else {
            default_smart_discount
        };
        let adjusted_prices = fuels.map(|price| {
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
            TargetRegion::from_label(region)
                .and_then(|target| target.value(&sort_context.region_rates))
                .ok_or_else(|| {
                    err(format!(
                        "지역화폐 적용 대상 행의 적용률을 찾지 못했습니다: 지역={region}"
                    ))
                })?
        } else {
            ScaledDecimal::ZERO
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
                let discount = total_price.regional_discount(region_rate)?;
                total_price.checked_sub(discount)
            }
        });
        Ok(SortableRankRow {
            address,
            adjusted_prices,
            fuels: regional_adjusted.map(|value| value.unwrap_or(ScaledSortKey::MAX)),
            name,
            rank_total,
            region,
            region_rate,
            smart_discount,
            smart_discount_excluded,
            source_index,
        })
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
        let mut region_rates = [None; TARGET_REGION_COUNT];
        for row in 4..=13 {
            let region_display = self.ws.try_get_display_at(3, row, self.shared_strings)?;
            let region = region_display.trim();
            if region.is_empty() {
                continue;
            }
            if let Some(rate) =
                MasterSheetUpdater::get_f64_at(self.ws, 4, row, self.shared_strings)?
                && let Some(target) = TargetRegion::from_label(region)
            {
                *target.value_mut(&mut region_rates) = Some(rate);
            }
        }
        let derived_total_qty = quantities
            .gasoline
            .checked_add(quantities.premium)
            .and_then(|total| total.checked_add(quantities.diesel))
            .ok_or_else(|| err("유류비 고정 입력값 합계가 허용 범위를 초과했습니다."))?;
        let total_qty = (derived_total_qty != ScaledDecimal::ZERO).then_some(derived_total_qty);
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
        let row_count = u32_to_usize(
            self.data_last_row
                .strict_sub(MASTER_DATA_START_ROW)
                .strict_add(1),
        );
        let mut row_plans: Vec<SortableRankRow<'strings>> =
            try_vec_with_capacity(row_count, "정렬 대상 행 메모리 확보 실패")?;
        for (source_index, row_num) in (MASTER_DATA_START_ROW..=self.data_last_row).enumerate() {
            row_plans.push(self.build_sort_plan(source_index, row_num, &sort_context)?);
        }
        row_plans.sort_unstable_by(|left, right| {
            left.rank_total
                .is_none()
                .cmp(&right.rank_total.is_none())
                .then_with(|| left.rank_total.cmp(&right.rank_total))
                .then_with(|| {
                    left.fuels
                        .gasoline
                        .cmp(&right.fuels.gasoline)
                        .then_with(|| left.fuels.premium.cmp(&right.fuels.premium))
                        .then_with(|| left.fuels.diesel.cmp(&right.fuels.diesel))
                })
                .then_with(|| left.region.cmp(right.region))
                .then_with(|| left.name.cmp(right.name))
                .then_with(|| left.address.cmp(right.address))
                .then_with(|| left.source_index.cmp(&right.source_index))
        });
        let mut rows = self.ws.take_rows();
        let data_start_index = u32_to_usize(MASTER_DATA_START_ROW.strict_sub(1));
        let data_end_index = u32_to_usize(self.data_last_row);
        (data_start_index <= data_end_index && data_end_index <= rows.len())
            .ok_or_else(|| err("정렬 대상 row 범위가 worksheet를 벗어났습니다."))?;
        let trailing_rows = rows.split_off(data_end_index);
        let mut source_rows = rows.split_off(data_start_index);
        let additional = source_rows.len().strict_add(trailing_rows.len());
        rows.try_reserve(additional)
            .map_err(|source| err_with_source("정렬 결과 행 메모리 확보 실패", source))?;
        for row_plan in &row_plans {
            let source_row = source_rows
                .get_mut(row_plan.source_index)
                .ok_or_else(|| err("유류비 정렬 원본 XML index가 범위를 벗어났습니다."))?;
            rows.push(mem::take(source_row));
        }
        rows.extend(trailing_rows);
        self.ws.replace_rows(rows);
        let mut buffers = FormulaBuffers {
            cache: String::new(),
            formula: try_string_with_capacity(
                MASTER_FORMULA_BUFFER_CAPACITY,
                "마스터 수식 메모리 확보 실패",
            )?,
        };
        let mut rank_text = try_string_with_capacity(
            USIZE_DECIMAL_TEXT_MAX_LEN,
            "지역화폐 순위 문자열 메모리 확보 실패",
        )?;
        let mut rank_buffer = NumBuffer::new();
        let ranking_enabled = sort_context.total_qty.is_some();
        let mut ranked_count = 0_usize;
        let mut previous_total = None;
        for (row, plan) in (MASTER_DATA_START_ROW..=self.data_last_row).zip(&row_plans) {
            let rank_cache = if ranking_enabled && let Some(current) = plan.rank_total {
                ranked_count = ranked_count.strict_add(1);
                if previous_total != Some(current) {
                    rank_text.clear();
                    rank_text.push_str(ranked_count.format_into(&mut rank_buffer));
                    previous_total = Some(current);
                }
                Some(rank_text.as_str())
            } else {
                None
            };
            self.apply_row_formulas_and_caches(row, plan, &sort_context, rank_cache, &mut buffers)?;
        }
        Ok(())
    }
}
impl<'source> MasterSheetUpdater<'source> {
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
    fn evaluate_master_rows(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &SharedStringTable,
    ) -> Result<MasterRowEvaluation<'source>> {
        let row_count = ws.row_count();
        let mut master_address_rows: HashMap<Cow<'source, str>, u32> = HashMap::new();
        master_address_rows
            .try_reserve(row_count)
            .map_err(|source| err_with_source("마스터 주소 행 맵 메모리 확보 실패", source))?;
        let mut kept_source_rows = try_vec_with_capacity(row_count, "행 목록 메모리 확보 실패")?;
        let mut changes = Vec::new();
        let mut deleted = Vec::new();
        let mut existing_region_counts = [0_usize; TARGET_REGION_COUNT];
        let mut matched_existing_region_counts = [0_usize; TARGET_REGION_COUNT];
        let mut target_region_scratch = String::new();
        for old_row in ws.row_numbers_from(MASTER_DATA_START_ROW) {
            let address = trim_cow(ws.try_get_display_at(COL_ADDRESS, old_row, shared_strings)?);
            let name = trim_cow(ws.try_get_display_at(COL_NAME, old_row, shared_strings)?);
            let region = trim_cow(ws.try_get_display_at(COL_REGION, old_row, shared_strings)?);
            if region.is_empty() && name.is_empty() && address.is_empty() {
                continue;
            }
            let existing_region = target_region(
                region.as_ref(),
                address.as_ref(),
                &mut target_region_scratch,
                TargetRegionPolicy::Flexible,
            )?;
            if let Some(target) = existing_region {
                increment_target_region_count(&mut existing_region_counts, target);
            }
            if address.is_empty() {
                kept_source_rows.push((old_row, None));
                continue;
            }
            normalize_address_key_into(address.as_ref(), &mut target_region_scratch)?;
            let matched = self
                .source_index
                .get_key_value(target_region_scratch.as_str());
            let mut record_address = |key: Cow<'source, str>| -> Result<()> {
                match master_address_rows.entry(key) {
                    Entry::Occupied(entry) => Err(err(format!(
                        "마스터 주소 중복: normalized_address={}, first_row={}, duplicate_row={old_row}",
                        entry.key(),
                        entry.get(),
                    ))),
                    Entry::Vacant(entry) => {
                        entry.insert(old_row);
                        Ok(())
                    }
                }
            };
            let Some((matched_key, src)) = matched else {
                record_address(Cow::Owned(mem::take(&mut target_region_scratch)))?;
                let row = StoreRow {
                    address: address.into_owned(),
                    fuels: read_master_fuels(ws, old_row, shared_strings)?,
                    name: name.into_owned(),
                    old_row,
                    region: region.into_owned(),
                };
                try_push_row(&mut deleted, row_count, row)?;
                continue;
            };
            record_address(Cow::Borrowed(matched_key.as_str()))?;
            if let Some(target) = existing_region {
                increment_target_region_count(&mut matched_existing_region_counts, target);
            }
            let fuels = read_master_fuels(ws, old_row, shared_strings)?;
            let old_brand_display = ws.try_get_display_at(COL_BRAND, old_row, shared_strings)?;
            let old_self_yn_display =
                ws.try_get_display_at(COL_SELF_YN, old_row, shared_strings)?;
            let region_changed = region.as_ref() != src.region.trim();
            let name_changed = name.as_ref() != src.name.trim();
            let brand_changed = old_brand_display.trim() != src.brand.trim();
            let self_yn_changed = !old_self_yn_display
                .trim()
                .chars()
                .filter(|ch| !ch.is_whitespace())
                .eq(src.service.label().chars());
            let price_changed = fuels != src.fuels;
            if region_changed || name_changed || brand_changed || self_yn_changed || price_changed {
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
                let row = ChangeRow {
                    old_fuels: fuels,
                    reason,
                    record: src,
                };
                try_push_row(&mut changes, row_count, row)?;
            }
            kept_source_rows.push((old_row, Some(src)));
        }
        let mut added: Vec<&'source SourceRecord> = try_vec_with_capacity(
            self.source_index
                .len()
                .strict_add(deleted.len())
                .strict_sub(master_address_rows.len()),
            "신규 소스 정렬 목록 메모리 확보 실패",
        )?;
        added.extend(
            self.source_index
                .iter()
                .filter(|&(key, _rec)| !master_address_rows.contains_key(key.as_str()))
                .map(|(_key, rec)| rec),
        );
        added.sort_unstable_by(|left, right| {
            left.region
                .cmp(right.region)
                .then_with(|| left.name.cmp(&right.name))
                .then_with(|| left.address.cmp(&right.address))
        });
        Ok(MasterRowEvaluation {
            added,
            changes,
            deleted,
            existing_region_counts,
            kept_source_rows,
            matched_existing_region_counts,
        })
    }
    fn get_f64_at(
        ws: &excel::writer::Worksheet,
        col: u32,
        row: u32,
        shared_strings: &SharedStringTable,
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
                (!parsing_fraction).ok_or_else(&invalid_value)?;
                parsing_fraction = true;
                continue;
            }
            digit_byte.is_ascii_digit().ok_or_else(&invalid_value)?;
            let digit_raw = digit_byte.strict_sub(b'0');
            let digit = i64::from(digit_raw);
            if parsing_fraction {
                if fraction_digit_count >= 6 {
                    (digit == 0).ok_or_else(&invalid_value)?;
                    continue;
                }
                fraction = fraction.strict_mul(10).strict_add(digit);
                fraction_digit_count = fraction_digit_count.strict_add(1);
            } else {
                has_whole_digit = true;
                let next_whole = whole
                    .checked_mul(10)
                    .and_then(|value| value.checked_add(digit))
                    .ok_or_else(&invalid_value)?;
                whole = next_whole;
            }
        }
        has_whole_digit.ok_or_else(&invalid_value)?;
        while fraction_digit_count < 6 {
            fraction = fraction.strict_mul(10);
            fraction_digit_count = fraction_digit_count.strict_add(1);
        }
        let whole_scaled = whole
            .checked_mul(DECIMAL_SCALE.as_i64())
            .ok_or_else(&invalid_value)?;
        let combined = whole_scaled
            .checked_add(fraction)
            .ok_or_else(&invalid_value)?;
        combined
            .checked_mul(sign)
            .map(ScaledDecimal)
            .map(Some)
            .ok_or_else(invalid_value)
    }
    pub(super) fn update(
        &self,
        book: &mut StdWorkbook,
    ) -> Result<MasterSheetUpdateResult<'source>> {
        let (ws, shared_strings) = book.master_sheet_mut();
        let MasterRowEvaluation {
            added,
            changes,
            deleted,
            existing_region_counts,
            kept_source_rows,
            matched_existing_region_counts,
        } = self.evaluate_master_rows(ws, shared_strings)?;
        let kept_count = kept_source_rows.len();
        let existing_count = kept_count.strict_add(deleted.len());
        let last_old_row = kept_source_rows
            .last()
            .map(|&(row, _)| row)
            .max(deleted.last().map(|row| row.old_row));
        let old_data_last_row = last_old_row.unwrap_or(MASTER_HEADER_ROW);
        let final_count = kept_count.strict_add(added.len());
        let final_count_u32 = usize_to_u32(final_count, "최종 유류비 행 수")?;
        let mut original_rows = ws.take_rows();
        let template_row_num = last_old_row.unwrap_or(MASTER_DATA_START_ROW);
        let added_template_row = if added.is_empty() {
            None
        } else {
            let template_index = u32_to_usize(template_row_num.strict_sub(1));
            let template_row = original_rows.get(template_index).ok_or_else(|| {
                err(format!(
                    "유류비 신규행 template이 없습니다: row={template_row_num}"
                ))
            })?;
            Some(template_row.try_copy()?)
        };
        let data_start_index = u32_to_usize(MASTER_HEADER_ROW);
        let trailing_start_index = u32_to_usize(last_old_row.unwrap_or(MASTER_HEADER_ROW));
        (data_start_index <= trailing_start_index && trailing_start_index <= original_rows.len())
            .ok_or_else(|| err("유류비 기존 데이터 row 범위가 worksheet를 벗어났습니다."))?;
        let trailing_rows = original_rows.split_off(trailing_start_index);
        let source_rows = original_rows.split_off(data_start_index);
        let additional = kept_count
            .strict_add(added.len())
            .strict_add(trailing_rows.len());
        original_rows
            .try_reserve(additional)
            .map_err(|source| err_with_source("유류비 결과행 메모리 확보 실패", source))?;
        let mut kept_rows = kept_source_rows.iter();
        let mut next_kept_row = kept_rows.next();
        for (old_row, source_row) in (MASTER_DATA_START_ROW..=old_data_last_row).zip(source_rows) {
            if next_kept_row.is_some_and(|&(kept_row, _)| kept_row == old_row) {
                original_rows.push(source_row);
                next_kept_row = kept_rows.next();
            }
        }
        if let Some(&(old_row, _)) = next_kept_row {
            return Err(err(format!("유류비 기존행 XML이 없습니다: row={old_row}")));
        }
        if let Some(template_row) = added_template_row {
            for _ in 1..added.len() {
                original_rows.push(template_row.try_copy()?);
            }
            original_rows.push(template_row);
        }
        original_rows.extend(trailing_rows);
        ws.replace_rows(original_rows);
        for (i, (_, source)) in kept_source_rows.into_iter().enumerate() {
            let new_row = add_row_offset(MASTER_DATA_START_ROW, i, "유류비 기존행 재배치")?;
            if let Some(src) = source {
                Self::write_master_row_from_source(ws, shared_strings, new_row, src)?;
            }
        }
        for (i, &source) in added.iter().enumerate() {
            let offset = kept_count.strict_add(i);
            let new_row = add_row_offset(MASTER_DATA_START_ROW, offset, "유류비 신규행 추가")?;
            Self::write_master_row_from_source(ws, shared_strings, new_row, source)?;
            ws.set_i32_at(COL_SMART_DISCOUNT, new_row, None)?;
        }
        let last_data_row = MASTER_DATA_START_ROW.strict_add(final_count_u32.strict_sub(1));
        RankSortRefresher {
            data_last_row: last_data_row,
            shared_strings,
            ws,
        }
        .refresh()?;
        Ok(MasterSheetUpdateResult {
            added,
            changes,
            deleted,
            existing_count,
            existing_region_counts,
            last_data_row,
            matched_existing_region_counts,
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
            (COL_SELF_YN, src.service.label()),
            (COL_ADDRESS, src.address.as_str()),
        ] {
            shared_strings.set_cell(ws, col, row, value)?;
        }
        ws.set_i32_at(COL_GASOLINE, row, src.fuels.gasoline)?;
        ws.set_i32_at(COL_PREMIUM, row, src.fuels.premium)?;
        ws.set_i32_at(COL_DIESEL, row, src.fuels.diesel)
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
    let rounded = ScaledSortKey(total)
        .checked_add(ScaledSortKey(DECIMAL_SCALE_SQUARED.as_i128().div_euclid(2)))
        .ok_or_else(|| err("연료비 반올림 계산 중 overflow가 발생했습니다."))?
        .as_i128()
        .div_euclid(DECIMAL_SCALE_SQUARED.as_i128());
    if !parts.is_empty() {
        parts.push_str(" / ");
    }
    parts.extend([label, " "]);
    if rounded < 0 {
        parts.push('-');
    }
    let mut number_buffer = NumBuffer::new();
    let digits = rounded.unsigned_abs().format_into(&mut number_buffer);
    let mut group_remaining = match digits.len().rem_euclid(3) {
        0 => 3,
        remainder => remainder,
    };
    for byte in digits.bytes() {
        if group_remaining == 0 {
            parts.push(',');
            group_remaining = 3;
        }
        parts.push(char::from(byte));
        group_remaining = group_remaining.strict_sub(1);
    }
    parts.push('원');
    Ok(true)
}
fn read_master_fuels(
    ws: &excel::writer::Worksheet,
    row: u32,
    shared_strings: &SharedStringTable,
) -> Result<FuelValues<Option<i32>>> {
    Ok(FuelValues {
        diesel: ws
            .get_i32_at(COL_DIESEL, row, shared_strings)?
            .filter(|price| *price > 0_i32),
        gasoline: ws
            .get_i32_at(COL_GASOLINE, row, shared_strings)?
            .filter(|price| *price > 0_i32),
        premium: ws
            .get_i32_at(COL_PREMIUM, row, shared_strings)?
            .filter(|price| *price > 0_i32),
    })
}
fn try_push_row<T>(rows: &mut Vec<T>, max_len: usize, row: T) -> Result<()> {
    if rows.len() == rows.capacity() {
        rows.try_reserve_exact(rows.capacity().max(1).min(max_len.strict_sub(rows.len())))
            .map_err(|source| err_with_source("행 목록 메모리 확보 실패", source))?;
    }
    rows.push(row);
    Ok(())
}
fn trim_cow(value: Cow<'_, str>) -> Cow<'_, str> {
    match value {
        Cow::Borrowed(text) => Cow::Borrowed(text.trim()),
        Cow::Owned(mut text) => {
            let range = text
                .substr_range(text.trim())
                .unwrap_or_else(|| process::abort());
            text.truncate(range.end);
            text.replace_range(..range.start, "");
            Cow::Owned(text)
        }
    }
}
