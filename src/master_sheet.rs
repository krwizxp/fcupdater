use crate::{
    diagnostic::{
        Result, add_row_offset, append_fmt, bail_fmt, ensure, err, err_with_source, fmt_err,
        map_reserve, or_abort, try_push_vec, try_string_with_capacity, try_vec_with_capacity,
        usize_to_u32,
    },
    excel,
    excel::writer::{
        SharedStringTable, Workbook as StdWorkbook, format_excel_ratio_into,
        is_shared_formula_follower, parse_decimal_scaled,
    },
    excel::{FuelValues, SourceRecord},
    region::{
        TARGET_REGION_COUNT, TargetRegion, TargetRegionPolicy, increment_target_region_count,
        target_region,
    },
};
use alloc::borrow::Cow;
use core::{
    cmp::Ordering,
    fmt::{Arguments, NumBuffer},
    mem,
};
use std::{
    collections::{HashMap, hash_map::Entry},
    process,
};
const MASTER_HEADER_ROW: u32 = 14;
const MASTER_DATA_START_ROW: u32 = 15;
const COL_REGION: u32 = 2;
const COL_NAME: u32 = 3;
const COL_ADDRESS: u32 = 6;
const COL_GASOLINE: u32 = 7;
const COL_PREMIUM: u32 = 8;
const COL_DIESEL: u32 = 10;
const COL_SMART_DISCOUNT: u32 = 11;
const COL_SORT_KEY: u32 = 23;
const SMART_DISCOUNT_BRAND_KEYWORD: &str = "현대오일뱅크";
const SMART_DISCOUNT_DIRECT_KEYWORD: &str = "직영";
pub(super) const DECIMAL_SCALE: i64 = 1_000_000;
const DECIMAL_SCALE_I128: i128 = 1_000_000;
const DECIMAL_SCALE_SQUARED: i128 = 1_000_000_000_000;
const DECIMAL_SCALE_CUBED: i128 = 1_000_000_000_000_000_000;
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
struct SortableRankRow<'text> {
    address: &'text str,
    adjusted_prices: FuelValues<Option<i64>>,
    fuels: FuelValues<i128>,
    name: &'text str,
    rank_total: Option<i128>,
    rank_value: Option<f64>,
    region: &'text str,
    region_rate: i64,
    smart_discount: i64,
    smart_discount_excluded: bool,
    source_index: usize,
    total_price: Option<i128>,
}
struct RankSortRefresher<'sheet, 'strings> {
    data_last_row: u32,
    shared_strings: &'strings SharedStringTable,
    ws: &'sheet mut excel::writer::Worksheet,
}
struct RankSortContext {
    quantities: FuelValues<i64>,
    region_rates: [Option<i64>; TARGET_REGION_COUNT],
    smart_discount: i64,
    unit_price_denominator: Option<i128>,
}
struct FormulaBuffers {
    cache: String,
    formula: String,
}
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
        (row, col): (u32, u32),
        is_shared_follower: bool,
        formula_args: Arguments<'_>,
        value: Option<&str>,
        string_value: bool,
        formula: &mut String,
    ) -> Result<()> {
        let formula_text = (!is_shared_follower).then(|| {
            formula.clear();
            append_fmt(formula, formula_args);
            formula.as_str()
        });
        self.ws
            .set_formula_at_with_cache(col, row, formula_text, value, string_value)
    }
    fn apply_numeric_formula_cache(
        &mut self,
        (row, col): (u32, u32),
        is_shared_follower: bool,
        formula_args: Arguments<'_>,
        value: Option<i128>,
        scale: i128,
        buffers: &mut FormulaBuffers,
    ) -> Result<()> {
        let cached = value.map(|scaled| {
            format_scaled_value_into(&mut buffers.cache, scaled, scale);
            buffers.cache.as_str()
        });
        self.apply_formula_cache(
            (row, col),
            is_shared_follower,
            formula_args,
            cached,
            false,
            &mut buffers.formula,
        )
    }
    fn apply_row_formulas_and_caches(
        &mut self,
        row_num: u32,
        plan: &SortableRankRow<'_>,
        sort_context: &RankSortContext,
        rank_cache: Option<&str>,
        smart_discount_follower: bool,
        buffers: &mut FormulaBuffers,
    ) -> Result<()> {
        let mut row_buffer = NumBuffer::new();
        let row_text = row_num.format_into(&mut row_buffer);
        let (prices, total_price, rank_total) =
            (plan.adjusted_prices, plan.total_price, plan.rank_total);
        let regional_discount = total_price
            .zip(rank_total)
            .map(|(price, ranked)| price.strict_sub(ranked));
        let (data_start, data_last) = (MASTER_DATA_START_ROW, self.data_last_row);
        let is_shared_follower = is_shared_formula_follower(
            row_num.strict_sub(data_start) as usize,
            data_last.strict_sub(data_start).strict_add(1) as usize,
        );
        self.apply_formula_cache(
            (row_num, 1),
            is_shared_follower,
            format_args!(
                r#"IF($T{row_text}="","",RANK(T{row_text},$T${data_start}:$T${data_last},1))"#
            ),
            rank_cache,
            false,
            &mut buffers.formula,
        )?;
        buffers.cache.clear();
        let qty = sort_context.quantities;
        let has_fuel_total = sort_context.unit_price_denominator.is_some()
            && append_fuel_total_text(&mut buffers.cache, qty.gasoline, prices.gasoline, "휘발유")?
            && append_fuel_total_text(&mut buffers.cache, qty.premium, prices.premium, "고급유")?
            && append_fuel_total_text(&mut buffers.cache, qty.diesel, prices.diesel, "경유")?;
        self.apply_formula_cache(
            (row_num, 15),
            is_shared_follower,
            format_args!(
                r##"IF($B$10=0,"",IFERROR(IF($B$4>0,"휘발유 "&TEXT(IF($L{row_text}="",1/0,$L{row_text}*$B$4),"#,##0")&"원","")&IF($B$5>0,IF($B$4>0," / ","")&"고급유 "&TEXT(IF($M{row_text}="",1/0,$M{row_text}*$B$5),"#,##0")&"원","")&IF($B$6>0,IF(OR($B$4>0,$B$5>0)," / ","")&"경유 "&TEXT(IF($N{row_text}="",1/0,$N{row_text}*$B$6),"#,##0")&"원",""),""))"##
            ),
            has_fuel_total.then_some(buffers.cache.as_str()),
            true,
            &mut buffers.formula,
        )?;
        if plan.smart_discount_excluded {
            self.ws
                .set_i32_at(COL_SMART_DISCOUNT, row_num, Some(0_i32))?;
        } else {
            self.apply_numeric_formula_cache(
                (row_num, COL_SMART_DISCOUNT),
                smart_discount_follower,
                format_args!(
                    r#"IF(AND(IFERROR(SEARCH("{SMART_DISCOUNT_BRAND_KEYWORD}",$C{row_text}),0)>0,IFERROR(SEARCH("{SMART_DISCOUNT_DIRECT_KEYWORD}",$C{row_text}),0)>0),$B$13,0)"#
                ),
                Some(i128::from(plan.smart_discount)),
                DECIMAL_SCALE_I128,
                buffers,
            )?;
        }
        for (col, source_col, price) in [
            (12, "G", prices.gasoline),
            (13, "H", prices.premium),
            (14, "J", prices.diesel),
        ] {
            self.apply_numeric_formula_cache(
                (row_num, col),
                is_shared_follower,
                format_args!(
                    r#"IF(${source_col}{row_text}="","",${source_col}{row_text}+$K{row_text})"#
                ),
                price.map(i128::from),
                DECIMAL_SCALE_I128,
                buffers,
            )?;
        }
        macro_rules! num_cell {
            ($col:expr, $fmt:literal, $val:expr, $scale:expr) => {
                self.apply_numeric_formula_cache(
                    (row_num, $col),
                    is_shared_follower,
                    format_args!($fmt),
                    $val,
                    $scale,
                    buffers,
                )?;
            };
        }
        num_cell!(
            16,
            r#"IF($B$10=0,"",IFERROR(IF($B$4>0,IF($L{row_text}="",1/0,$L{row_text}*$B$4),0)+IF($B$5>0,IF($M{row_text}="",1/0,$M{row_text}*$B$5),0)+IF($B$6>0,IF($N{row_text}="",1/0,$N{row_text}*$B$6),0),""))"#,
            total_price,
            DECIMAL_SCALE_SQUARED
        );
        num_cell!(
            18,
            r#"IF($P{row_text}="","",IF($Q{row_text}="Y",IFERROR(VLOOKUP($B{row_text},$C$4:$D$13,2,FALSE()),0),0))"#,
            total_price.map(|_| i128::from(plan.region_rate)),
            DECIMAL_SCALE_I128
        );
        num_cell!(
            19,
            r#"IF($P{row_text}="","",ROUNDDOWN($P{row_text}*$R{row_text},0))"#,
            regional_discount,
            DECIMAL_SCALE_SQUARED
        );
        num_cell!(
            20,
            r#"IF($P{row_text}="","",$P{row_text}-$S{row_text})"#,
            rank_total,
            DECIMAL_SCALE_SQUARED
        );
        if rank_total.is_none() {
            buffers.cache.clear();
            buffers.cache.push_str("1000000000000000");
        }
        self.apply_formula_cache(
            (row_num, COL_SORT_KEY),
            is_shared_follower,
            format_args!(r#"IF($T{row_text}="",1000000000000000,$T{row_text})"#),
            Some(buffers.cache.as_str()),
            false,
            &mut buffers.formula,
        )?;
        for (col, total_col, value) in [(21, "T", rank_total), (22, "P", total_price)] {
            buffers.cache.clear();
            let cached =
                value
                    .zip(sort_context.unit_price_denominator)
                    .map(|(total, denominator)| {
                        format_excel_ratio_into(&mut buffers.cache, total, denominator);
                        buffers.cache.as_str()
                    });
            self.apply_formula_cache(
                (row_num, col),
                is_shared_follower,
                format_args!(
                    r#"IF(${total_col}{row_text}="","",IF($B$10=0,"",${total_col}{row_text}/$B$10))"#
                ),
                cached,
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
        rank_buffer: &mut String,
    ) -> Result<SortableRankRow<'strings>> {
        let fixed_text = |col| -> Result<&'strings str> {
            Ok(self
                .ws
                .try_get_fixed_text_at(col, row_num, self.shared_strings)?
                .trim())
        };
        let (address, name, region) = (
            fixed_text(COL_ADDRESS)?,
            fixed_text(COL_NAME)?,
            fixed_text(COL_REGION)?,
        );
        let fuels = read_master_fuels(self.ws, row_num, self.shared_strings)?;
        let smart_discount_excluded = !self.ws.has_formula_at(COL_SMART_DISCOUNT, row_num)?
            && MasterSheetUpdater::get_f64_at(
                self.ws,
                COL_SMART_DISCOUNT,
                row_num,
                self.shared_strings,
            )? == Some(0_i64);
        let smart_discount = if !smart_discount_excluded
            && name.contains(SMART_DISCOUNT_BRAND_KEYWORD)
            && name.contains(SMART_DISCOUNT_DIRECT_KEYWORD)
        {
            sort_context.smart_discount
        } else {
            0_i64
        };
        let adjust = |price: Option<i32>| -> Result<Option<i64>> {
            price
                .map(|fuel_price| {
                    i64::from(fuel_price)
                        .strict_mul(DECIMAL_SCALE)
                        .checked_add(smart_discount)
                        .ok_or_else(|| err("할인 적용 가격 계산 중 overflow가 발생했습니다."))
                })
                .transpose()
        };
        let adjusted_prices = FuelValues {
            gasoline: adjust(fuels.gasoline)?,
            premium: adjust(fuels.premium)?,
            diesel: adjust(fuels.diesel)?,
        };
        let currency_apply = self
            .ws
            .try_get_display_at(17, row_num, self.shared_strings)?
            .eq_ignore_ascii_case("Y");
        let region_rate = if currency_apply {
            TargetRegion::from_label(region)
                .and_then(|target| target.value(&sort_context.region_rates))
                .unwrap_or(0_i64)
        } else {
            0_i64
        };
        let region_multiplier = DECIMAL_SCALE
            .checked_sub(region_rate)
            .ok_or("지역 보정률이 100%를 초과했습니다.")?;
        let total_price = if sort_context.unit_price_denominator.is_some() {
            let mut total = 0_i128;
            let mut complete = true;
            for (quantity, price) in [
                (sort_context.quantities.gasoline, adjusted_prices.gasoline),
                (sort_context.quantities.premium, adjusted_prices.premium),
                (sort_context.quantities.diesel, adjusted_prices.diesel),
            ] {
                if quantity > 0 {
                    let Some(fuel_price) = price else {
                        complete = false;
                        break;
                    };
                    total = i128::from(quantity)
                        .checked_mul(i128::from(fuel_price))
                        .and_then(|value| total.checked_add(value))
                        .ok_or("유류비 합계 계산 중 overflow가 발생했습니다.")?;
                }
            }
            complete.then_some(total)
        } else {
            None
        };
        let rank_total = total_price
            .map(|price| {
                let discount = price
                    .checked_mul(i128::from(region_rate))
                    .and_then(|value| value.checked_div(DECIMAL_SCALE_CUBED))
                    .and_then(|value| value.checked_mul(DECIMAL_SCALE_SQUARED))
                    .ok_or("지역화폐 할인 계산 중 overflow가 발생했습니다.")?;
                price
                    .checked_sub(discount)
                    .ok_or_else(|| err("지역화폐 적용 금액 계산 중 overflow가 발생했습니다."))
            })
            .transpose()?;
        let rank_value = rank_total
            .map(|scaled| {
                if scaled.rem_euclid(DECIMAL_SCALE_SQUARED) == 0
                    && let Ok(integer) = i32::try_from(scaled.div_euclid(DECIMAL_SCALE_SQUARED))
                {
                    Ok(f64::from(integer))
                } else {
                    format_scaled_value_into(rank_buffer, scaled, DECIMAL_SCALE_SQUARED);
                    rank_buffer
                        .parse::<f64>()
                        .map_err(|source| err_with_source("순위 금액 변환 실패", source))
                }
            })
            .transpose()?;
        Ok(SortableRankRow {
            address,
            adjusted_prices,
            fuels: adjusted_prices.map(|price| {
                price
                    .and_then(|value| i128::from(value).checked_mul(i128::from(region_multiplier)))
                    .unwrap_or(i128::MAX)
            }),
            name,
            rank_total,
            rank_value,
            region,
            region_rate,
            smart_discount,
            smart_discount_excluded,
            source_index,
            total_price,
        })
    }
    fn refresh(&mut self) -> Result<()> {
        let read_decimal = |col, row| {
            MasterSheetUpdater::get_f64_at(self.ws, col, row, self.shared_strings)
                .map(Option::unwrap_or_default)
        };
        let quantities = FuelValues {
            gasoline: read_decimal(2, 4)?,
            premium: read_decimal(2, 5)?,
            diesel: read_decimal(2, 6)?,
        };
        let mut region_rates = [None; TARGET_REGION_COUNT];
        for row in 4..=13 {
            let region_display = self.ws.try_get_display_at(3, row, self.shared_strings)?;
            if let Some(target) = TargetRegion::from_label(region_display.as_ref())
                && target.value(&region_rates).is_none()
            {
                *target.value_mut(&mut region_rates) = Some(read_decimal(4, row)?);
            }
        }
        let derived_total_qty = quantities
            .gasoline
            .checked_add(quantities.premium)
            .and_then(|total| total.checked_add(quantities.diesel))
            .ok_or("유류비 고정 입력값 합계가 허용 범위를 초과했습니다.")?;
        let unit_price_denominator = (derived_total_qty != 0_i64)
            .then(|| {
                i128::from(derived_total_qty)
                    .checked_mul(DECIMAL_SCALE_I128)
                    .ok_or_else(|| err("단가 분모 계산 중 overflow가 발생했습니다."))
            })
            .transpose()?;
        let sort_context = RankSortContext {
            quantities,
            region_rates,
            smart_discount: read_decimal(2, 13)?,
            unit_price_denominator,
        };
        let row_count = self
            .data_last_row
            .strict_sub(MASTER_DATA_START_ROW)
            .strict_add(1) as usize;
        let mut row_plans = try_vec_with_capacity(row_count, "정렬 대상 행 메모리 확보 실패")?;
        let mut rank_value_buffer = String::new();
        for (source_index, row_num) in (MASTER_DATA_START_ROW..=self.data_last_row).enumerate() {
            row_plans.push(self.build_sort_plan(
                source_index,
                row_num,
                &sort_context,
                &mut rank_value_buffer,
            )?);
        }
        row_plans.sort_unstable_by(|left, right| {
            left.rank_total
                .is_none()
                .cmp(&right.rank_total.is_none())
                .then_with(|| {
                    left.rank_value
                        .zip(right.rank_value)
                        .map_or(Ordering::Equal, |(lhs, rhs)| lhs.total_cmp(&rhs))
                })
                .then_with(|| {
                    (
                        left.fuels.into_array(),
                        left.region,
                        left.name,
                        left.address,
                        left.source_index,
                    )
                        .cmp(&(
                            right.fuels.into_array(),
                            right.region,
                            right.name,
                            right.address,
                            right.source_index,
                        ))
                })
        });
        let mut rows = self.ws.take_rows();
        let data_end_index = self.data_last_row as usize;
        ensure(
            data_end_index <= rows.len(),
            "정렬 대상 row 범위가 worksheet를 벗어났습니다.",
        )?;
        let trailing_rows = rows.split_off(data_end_index);
        let mut source_rows = rows.split_off(MASTER_DATA_START_ROW.strict_sub(1) as usize);
        map_reserve(
            rows.try_reserve(source_rows.len().strict_add(trailing_rows.len())),
            "정렬 결과 행 메모리 확보 실패",
        )?;
        for row_plan in &row_plans {
            rows.push(mem::take(or_abort(
                source_rows.get_mut(row_plan.source_index),
            )));
        }
        rows.extend(trailing_rows);
        self.ws.replace_rows(rows);
        let mut buffers = FormulaBuffers {
            cache: String::new(),
            formula: try_string_with_capacity(512, "마스터 수식 메모리 확보 실패")?,
        };
        let mut rank_buffer = NumBuffer::new();
        let (mut ranked_count, mut current_rank, mut previous_total) = (0_usize, 0_usize, None);
        let (mut smart_group_end, mut smart_seen_gap, mut smart_prev_row) = (0_u32, false, None);
        for (index, (row, plan)) in (MASTER_DATA_START_ROW..=self.data_last_row)
            .zip(&row_plans)
            .enumerate()
        {
            let smart_discount_follower = if plan.smart_discount_excluded {
                false
            } else if row <= smart_group_end {
                true
            } else {
                let interrupted = smart_seen_gap
                    || smart_prev_row.is_some_and(|prev: u32| row > prev.strict_add(1));
                let run_len = or_abort(row_plans.get(index..))
                    .iter()
                    .take(if interrupted { 32_usize } else { 64_usize })
                    .take_while(|candidate| !candidate.smart_discount_excluded)
                    .count();
                let last_row = row.strict_add(
                    u32::try_from(run_len.strict_sub(1)).unwrap_or_else(|_| process::abort()),
                );
                (smart_seen_gap, smart_prev_row, smart_group_end) = (
                    interrupted,
                    Some(last_row),
                    if run_len >= 6 { last_row } else { 0 },
                );
                false
            };
            let rank_cache = plan.rank_value.map(|current| {
                ranked_count = ranked_count.strict_add(1);
                if previous_total != Some(current.to_bits()) {
                    current_rank = ranked_count;
                    previous_total = Some(current.to_bits());
                }
                current_rank.format_into(&mut rank_buffer)
            });
            self.apply_row_formulas_and_caches(
                row,
                plan,
                &sort_context,
                rank_cache,
                smart_discount_follower,
                &mut buffers,
            )?;
        }
        Ok(())
    }
}
impl<'source> MasterSheetUpdater<'source> {
    fn evaluate_master_rows(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &SharedStringTable,
    ) -> Result<MasterRowEvaluation<'source>> {
        let row_count = ws.row_count();
        let mut master_address_rows: HashMap<Cow<'source, str>, u32> = HashMap::new();
        map_reserve(
            master_address_rows.try_reserve(row_count),
            "마스터 주소 행 맵 메모리 확보 실패",
        )?;
        let mut kept_source_rows = try_vec_with_capacity(row_count, "행 목록 메모리 확보 실패")?;
        let (mut changes, mut deleted) = (Vec::new(), Vec::new());
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
            let matched = self
                .source_index
                .get_key_value(target_region_scratch.as_str());
            let key = matched.map_or_else(
                || Cow::Owned(mem::take(&mut target_region_scratch)),
                |(matched_key, _)| Cow::Borrowed(matched_key.as_str()),
            );
            match master_address_rows.entry(key) {
                Entry::Occupied(entry) => {
                    bail_fmt!(
                        "마스터 주소 중복: normalized_address={}, first_row={}, duplicate_row={old_row}",
                        entry.key(),
                        entry.get(),
                    );
                }
                Entry::Vacant(entry) => {
                    entry.insert(old_row);
                }
            }
            let fuels = read_master_fuels(ws, old_row, shared_strings)?;
            let Some((_, src)) = matched else {
                let row = StoreRow {
                    address: address.into_owned(),
                    fuels,
                    name: name.into_owned(),
                    old_row,
                    region: region.into_owned(),
                };
                let growth = row_count.saturating_sub(deleted.len()).clamp(1, 64);
                try_push_vec(&mut deleted, row, growth, "변경 행 목록 메모리 확보 실패")?;
                continue;
            };
            if let Some(target) = existing_region {
                increment_target_region_count(&mut matched_existing_region_counts, target);
            }
            let old_brand = ws.try_get_display_at(4, old_row, shared_strings)?;
            let old_self_yn = ws.try_get_display_at(5, old_row, shared_strings)?;
            let flags = [
                (fuels != src.fuels, "가격변동"),
                (region.as_ref() != src.region, "지역정정"),
                (name.as_ref() != src.name, "상호변경"),
                (old_brand.trim() != src.brand, "상표변경"),
                (
                    !old_self_yn
                        .chars()
                        .filter(|ch| !ch.is_whitespace())
                        .eq(src.service.label().chars()),
                    "셀프여부변경",
                ),
            ];
            let mut reason = String::new();
            for (changed, label) in flags {
                if changed {
                    if !reason.is_empty() {
                        reason.push_str(", ");
                    }
                    reason.push_str(label);
                }
            }
            if !reason.is_empty() {
                let growth = row_count.saturating_sub(changes.len()).clamp(1, 64);
                try_push_vec(
                    &mut changes,
                    ChangeRow {
                        old_fuels: fuels,
                        reason,
                        record: src,
                    },
                    growth,
                    "변경 행 목록 메모리 확보 실패",
                )?;
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
                .filter(|&(key, _)| !master_address_rows.contains_key(key.as_str()))
                .map(|(_, rec)| rec),
        );
        added.sort_unstable_by(|left, right| {
            (left.region, &left.name, &left.address).cmp(&(
                right.region,
                &right.name,
                &right.address,
            ))
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
    pub(super) fn get_f64_at(
        ws: &excel::writer::Worksheet,
        col: u32,
        row: u32,
        shared_strings: &SharedStringTable,
    ) -> Result<Option<i64>> {
        let display_text = ws.try_get_display_at(col, row, shared_strings)?;
        let trimmed = display_text.trim();
        if trimmed.is_empty() || trimmed == "-" {
            return Ok(None);
        }
        parse_decimal_scaled(trimmed, 6, true, false)
            .map(Some)
            .ok_or_else(|| {
                fmt_err!(
                    "유류비 숫자 셀 값이 올바르지 않습니다: row={row}, col={col}, value={trimmed}"
                )
            })
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
        let final_count_u32 =
            usize_to_u32(kept_count.strict_add(added.len()), "최종 유류비 행 수")?;
        let mut original_rows = ws.take_rows();
        let added_template_row = (!added.is_empty())
            .then(|| {
                let template_row_num = last_old_row.unwrap_or(MASTER_DATA_START_ROW);
                original_rows
                    .get(template_row_num.strict_sub(1) as usize)
                    .ok_or_else(|| {
                        fmt_err!("유류비 신규행 template이 없습니다: row={template_row_num}")
                    })?
                    .try_copy_style(COL_SORT_KEY)
            })
            .transpose()?;
        let trailing_start_index = old_data_last_row as usize;
        (trailing_start_index <= original_rows.len())
            .ok_or("유류비 기존 데이터 row 범위가 worksheet를 벗어났습니다.")?;
        let trailing_rows = original_rows.split_off(trailing_start_index);
        let source_rows = original_rows.split_off(MASTER_HEADER_ROW as usize);
        map_reserve(
            original_rows.try_reserve(
                kept_count
                    .strict_add(added.len())
                    .strict_add(trailing_rows.len()),
            ),
            "유류비 결과행 메모리 확보 실패",
        )?;
        let mut kept_rows = kept_source_rows.iter();
        let mut next_kept_row = kept_rows.next();
        for (old_row, source_row) in (MASTER_DATA_START_ROW..=old_data_last_row).zip(source_rows) {
            if next_kept_row.is_some_and(|&(kept_row, _)| kept_row == old_row) {
                original_rows.push(source_row);
                next_kept_row = kept_rows.next();
            }
        }
        if let Some(&(old_row, _)) = next_kept_row {
            bail_fmt!("유류비 기존행 XML이 없습니다: row={old_row}");
        }
        if let Some(template_row) = added_template_row {
            for _ in 1..added.len() {
                original_rows.push(template_row.try_copy_style(COL_SORT_KEY)?);
            }
            original_rows.push(template_row);
        }
        original_rows.extend(trailing_rows);
        ws.replace_rows(original_rows);
        for (index, (_, source)) in kept_source_rows.into_iter().enumerate() {
            let new_row = add_row_offset(MASTER_DATA_START_ROW, index, "유류비 기존행 재배치")?;
            if let Some(src) = source {
                Self::write_master_row_from_source(ws, shared_strings, new_row, src)?;
            }
        }
        for (index, &source) in added.iter().enumerate() {
            let new_row = add_row_offset(
                MASTER_DATA_START_ROW,
                kept_count.strict_add(index),
                "유류비 신규행 추가",
            )?;
            Self::write_master_row_from_source(ws, shared_strings, new_row, source)?;
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
            (4, src.brand.as_str()),
            (5, src.service.label()),
            (COL_ADDRESS, src.address.as_str()),
        ] {
            shared_strings.set_cell(ws, col, row, value)?;
        }
        for (col, value) in [COL_GASOLINE, COL_PREMIUM, COL_DIESEL]
            .into_iter()
            .zip(src.fuels.into_array())
        {
            ws.set_i32_at(col, row, value)?;
        }
        Ok(())
    }
}
fn append_fuel_total_text(
    parts: &mut String,
    quantity: i64,
    price: Option<i64>,
    label: &str,
) -> Result<bool> {
    if quantity <= 0 {
        return Ok(true);
    }
    let Some(total) = price.and_then(|value| i128::from(quantity).checked_mul(i128::from(value)))
    else {
        return Ok(false);
    };
    let rounding_offset = DECIMAL_SCALE_SQUARED
        .div_euclid(2)
        .strict_sub(i128::from(total < 0));
    let rounded = total
        .checked_add(rounding_offset)
        .ok_or("연료비 반올림 계산 중 overflow가 발생했습니다.")?
        .div_euclid(DECIMAL_SCALE_SQUARED);
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
fn format_scaled_value_into(text: &mut String, value: i128, scale: i128) {
    text.clear();
    let (abs, scale_abs) = (value.unsigned_abs(), scale.unsigned_abs());
    if value < 0 {
        text.push('-');
    }
    let (whole, frac) =
        if let (Ok(abs_u64), Ok(scale_u64)) = (u64::try_from(abs), u64::try_from(scale_abs)) {
            (
                u128::from(abs_u64.div_euclid(scale_u64)),
                u128::from(abs_u64.rem_euclid(scale_u64)),
            )
        } else {
            (abs.div_euclid(scale_abs), abs.rem_euclid(scale_abs))
        };
    let mut buffer = NumBuffer::new();
    text.push_str(whole.format_into(&mut buffer));
    if frac == 0 {
        return;
    }
    text.push('.');
    let frac_text = frac.format_into(&mut buffer);
    for _ in frac_text.len()..(scale_abs.ilog10() as usize) {
        text.push('0');
    }
    text.push_str(frac_text);
    text.truncate(text.trim_end_matches('0').len());
}
fn read_master_fuels(
    ws: &excel::writer::Worksheet,
    row: u32,
    shared_strings: &SharedStringTable,
) -> Result<FuelValues<Option<i32>>> {
    let read_price = |col| {
        ws.get_i32_at(col, row, shared_strings)
            .map(|value| value.filter(|price| *price > 0_i32))
    };
    Ok(FuelValues {
        diesel: read_price(COL_DIESEL)?,
        gasoline: read_price(COL_GASOLINE)?,
        premium: read_price(COL_PREMIUM)?,
    })
}
fn trim_cow(value: Cow<'_, str>) -> Cow<'_, str> {
    match value {
        Cow::Borrowed(text) => Cow::Borrowed(text.trim()),
        Cow::Owned(mut text) => {
            let range = or_abort(text.substr_range(text.trim()));
            text.truncate(range.end);
            text.replace_range(..range.start, "");
            Cow::Owned(text)
        }
    }
}
