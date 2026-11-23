use crate::{
    diagnostic::{
        Result, add_row_offset, append_fmt, bail_fmt, ensure, err, err_src, fmt_err, or_abort,
        push_vec, reserve, str_cap, usize_to_u32, vec_cap,
    },
    excel::{
        self, FuelValues, SourceRecord,
        writer::{
            SharedStringTable, Workbook as StdWorkbook, format_excel_ratio_into,
            is_shared_formula_follower, parse_decimal_scaled,
        },
    },
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
const HEADER_ROW: u32 = 14;
const DATA_START: u32 = 15;
const COL_SORT: u32 = 23;
const BRAND_KW: &str = "현대오일뱅크";
const DIRECT_KW: &str = "직영";
pub(super) const DECIMAL_SCALE: i64 = 1_000_000;
const SCALE_I128: i128 = 1_000_000;
const SCALE_SQ: i128 = 1_000_000_000_000;
const SCALE_CUBE: i128 = 1_000_000_000_000_000_000;
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
        follower: bool,
        formula_args: Arguments<'_>,
        value: Option<&str>,
        string_value: bool,
        formula: &mut String,
    ) -> Result<()> {
        let text = (!follower).then(|| {
            formula.clear();
            append_fmt(formula, formula_args);
            formula.as_str()
        });
        self.ws
            .set_formula_at_with_cache(col, row, text, value, string_value)
    }
    fn apply_num_cache(
        &mut self,
        pos: (u32, u32),
        follower: bool,
        formula_args: Arguments<'_>,
        value: Option<i128>,
        scale: i128,
        bufs: &mut FormulaBuffers,
    ) -> Result<()> {
        let cached = value.map(|scaled| {
            format_scaled_value_into(&mut bufs.cache, scaled, scale);
            bufs.cache.as_str()
        });
        self.apply_formula_cache(
            pos,
            follower,
            formula_args,
            cached,
            false,
            &mut bufs.formula,
        )
    }
    fn apply_row_formulas(
        &mut self,
        row: u32,
        plan: &SortableRankRow<'_>,
        ctx: &RankSortContext,
        rank_cache: Option<&str>,
        smart_follower: bool,
        bufs: &mut FormulaBuffers,
    ) -> Result<()> {
        let mut row_buf = NumBuffer::new();
        let rt = row.format_into(&mut row_buf);
        let (prices, tot_p, rk_tot) = (plan.adjusted_prices, plan.total_price, plan.rank_total);
        let reg_disc = tot_p.zip(rk_tot).map(|(pr, rk)| pr.strict_sub(rk));
        let (ds, dl) = (DATA_START, self.data_last_row);
        let follow = is_shared_formula_follower(
            row.strict_sub(ds) as usize,
            dl.strict_sub(ds).strict_add(1) as usize,
        );
        self.apply_formula_cache(
            (row, 1),
            follow,
            format_args!(r#"IF($T{rt}="","",RANK(T{rt},$T${ds}:$T${dl},1))"#),
            rank_cache,
            false,
            &mut bufs.formula,
        )?;
        bufs.cache.clear();
        let qty = ctx.quantities;
        let has_total = ctx.unit_price_denominator.is_some()
            && append_fuel_total(&mut bufs.cache, qty.gasoline, prices.gasoline, "휘발유")?
            && append_fuel_total(&mut bufs.cache, qty.premium, prices.premium, "고급유")?
            && append_fuel_total(&mut bufs.cache, qty.diesel, prices.diesel, "경유")?;
        self.apply_formula_cache(
            (row, 15),
            follow,
            format_args!(r##"IF($B$10=0,"",IFERROR(IF($B$4>0,"휘발유 "&TEXT(IF($L{rt}="",1/0,$L{rt}*$B$4),"#,##0")&"원","")&IF($B$5>0,IF($B$4>0," / ","")&"고급유 "&TEXT(IF($M{rt}="",1/0,$M{rt}*$B$5),"#,##0")&"원","")&IF($B$6>0,IF(OR($B$4>0,$B$5>0)," / ","")&"경유 "&TEXT(IF($N{rt}="",1/0,$N{rt}*$B$6),"#,##0")&"원",""),""))"##),
            has_total.then_some(bufs.cache.as_str()),
            true,
            &mut bufs.formula,
        )?;
        if plan.smart_discount_excluded {
            self.ws.set_i32_at(11, row, Some(0_i32))?;
        } else {
            self.apply_num_cache(
                (row, 11),
                smart_follower,
                format_args!(r#"IF(AND(IFERROR(SEARCH("{BRAND_KW}",$C{rt}),0)>0,IFERROR(SEARCH("{DIRECT_KW}",$C{rt}),0)>0),$B$13,0)"#),
                Some(i128::from(plan.smart_discount)),
                SCALE_I128,
                bufs,
            )?;
        }
        for (col, sc, price) in [
            (12, "G", prices.gasoline),
            (13, "H", prices.premium),
            (14, "J", prices.diesel),
        ] {
            self.apply_num_cache(
                (row, col),
                follow,
                format_args!(r#"IF(${sc}{rt}="","",${sc}{rt}+$K{rt})"#),
                price.map(i128::from),
                SCALE_I128,
                bufs,
            )?;
        }
        macro_rules! nc {
            ($c:expr, $f:literal, $v:expr, $s:expr) => {
                self.apply_num_cache((row, $c), follow, format_args!($f), $v, $s, bufs)?;
            };
        }
        nc!(
            16,
            r#"IF($B$10=0,"",IFERROR(IF($B$4>0,IF($L{rt}="",1/0,$L{rt}*$B$4),0)+IF($B$5>0,IF($M{rt}="",1/0,$M{rt}*$B$5),0)+IF($B$6>0,IF($N{rt}="",1/0,$N{rt}*$B$6),0),""))"#,
            tot_p,
            SCALE_SQ
        );
        nc!(
            18,
            r#"IF($P{rt}="","",IF($Q{rt}="Y",IFERROR(VLOOKUP($B{rt},$C$4:$D$13,2,FALSE()),0),0))"#,
            tot_p.map(|_| i128::from(plan.region_rate)),
            SCALE_I128
        );
        nc!(
            19,
            r#"IF($P{rt}="","",ROUNDDOWN($P{rt}*$R{rt},0))"#,
            reg_disc,
            SCALE_SQ
        );
        nc!(20, r#"IF($P{rt}="","",$P{rt}-$S{rt})"#, rk_tot, SCALE_SQ);
        if rk_tot.is_none() {
            bufs.cache.clear();
            bufs.cache.push_str("1000000000000000");
        }
        self.apply_formula_cache(
            (row, COL_SORT),
            follow,
            format_args!(r#"IF($T{rt}="",1000000000000000,$T{rt})"#),
            Some(bufs.cache.as_str()),
            false,
            &mut bufs.formula,
        )?;
        for (col, tc, value) in [(21, "T", rk_tot), (22, "P", tot_p)] {
            bufs.cache.clear();
            let cached = value.zip(ctx.unit_price_denominator).map(|(tot, den)| {
                format_excel_ratio_into(&mut bufs.cache, tot, den);
                bufs.cache.as_str()
            });
            self.apply_formula_cache(
                (row, col),
                follow,
                format_args!(r#"IF(${tc}{rt}="","",IF($B$10=0,"",${tc}{rt}/$B$10))"#),
                cached,
                false,
                &mut bufs.formula,
            )?;
        }
        Ok(())
    }
    fn build_sort_plan(
        &self,
        source_index: usize,
        row_num: u32,
        ctx: &RankSortContext,
        rank_buf: &mut String,
    ) -> Result<SortableRankRow<'strings>> {
        let fixed = |col| -> Result<&'strings str> {
            Ok(self
                .ws
                .try_get_fixed_text_at(col, row_num, self.shared_strings)?
                .trim())
        };
        let (address, name, region) = (fixed(6)?, fixed(3)?, fixed(2)?);
        let fuels = read_master_fuels(self.ws, row_num, self.shared_strings)?;
        let smart_discount_excluded = !self.ws.has_formula_at(11, row_num)?
            && MasterSheetUpdater::get_f64_at(self.ws, 11, row_num, self.shared_strings)?
                == Some(0_i64);
        let smart_discount =
            if !smart_discount_excluded && name.contains(BRAND_KW) && name.contains(DIRECT_KW) {
                ctx.smart_discount
            } else {
                0_i64
            };
        let adjust = |price: Option<i32>| -> Result<Option<i64>> {
            price
                .map(|val| {
                    i64::from(val)
                        .strict_mul(DECIMAL_SCALE)
                        .checked_add(smart_discount)
                        .ok_or_else(|| err("할인 적용 가격 overflow"))
                })
                .transpose()
        };
        let adjusted_prices = FuelValues {
            gasoline: adjust(fuels.gasoline)?,
            premium: adjust(fuels.premium)?,
            diesel: adjust(fuels.diesel)?,
        };
        let region_rate = if self
            .ws
            .try_get_display_at(17, row_num, self.shared_strings)?
            .eq_ignore_ascii_case("Y")
        {
            TargetRegion::from_label(region)
                .and_then(|target| target.value(&ctx.region_rates))
                .unwrap_or(0_i64)
        } else {
            0_i64
        };
        let region_mul = DECIMAL_SCALE
            .checked_sub(region_rate)
            .ok_or("지역 보정률 100% 초과")?;
        let total_price = if ctx.unit_price_denominator.is_some() {
            let (mut total, mut complete) = (0_i128, true);
            for (qty, price) in [
                (ctx.quantities.gasoline, adjusted_prices.gasoline),
                (ctx.quantities.premium, adjusted_prices.premium),
                (ctx.quantities.diesel, adjusted_prices.diesel),
            ] {
                if qty > 0 {
                    let Some(fp) = price else {
                        complete = false;
                        break;
                    };
                    total = i128::from(qty)
                        .checked_mul(i128::from(fp))
                        .and_then(|val| total.checked_add(val))
                        .ok_or("유류비 합계 overflow")?;
                }
            }
            complete.then_some(total)
        } else {
            None
        };
        let rank_total = total_price
            .map(|price| {
                let disc = price
                    .checked_mul(i128::from(region_rate))
                    .and_then(|val| val.checked_div(SCALE_CUBE))
                    .and_then(|val| val.checked_mul(SCALE_SQ))
                    .ok_or("지역화폐 할인 overflow")?;
                price
                    .checked_sub(disc)
                    .ok_or_else(|| err("지역화폐 적용 금액 overflow"))
            })
            .transpose()?;
        let rank_value = rank_total
            .map(|scaled| {
                if scaled.rem_euclid(SCALE_SQ) == 0
                    && let Ok(int_val) = i32::try_from(scaled.div_euclid(SCALE_SQ))
                {
                    Ok(f64::from(int_val))
                } else {
                    format_scaled_value_into(rank_buf, scaled, SCALE_SQ);
                    rank_buf
                        .parse::<f64>()
                        .map_err(|src| err_src("순위 금액 변환 실패", src))
                }
            })
            .transpose()?;
        Ok(SortableRankRow {
            address,
            adjusted_prices,
            fuels: adjusted_prices.map(|price| {
                price
                    .and_then(|val| i128::from(val).checked_mul(i128::from(region_mul)))
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
        let read_dec = |col, row| {
            MasterSheetUpdater::get_f64_at(self.ws, col, row, self.shared_strings)
                .map(Option::unwrap_or_default)
        };
        let quantities = FuelValues {
            gasoline: read_dec(2, 4)?,
            premium: read_dec(2, 5)?,
            diesel: read_dec(2, 6)?,
        };
        let mut region_rates = [None; TARGET_REGION_COUNT];
        for row in 4..=13 {
            let disp = self.ws.try_get_display_at(3, row, self.shared_strings)?;
            if let Some(target) = TargetRegion::from_label(&disp)
                && target.value(&region_rates).is_none()
            {
                *target.value_mut(&mut region_rates) = Some(read_dec(4, row)?);
            }
        }
        let total_qty = quantities
            .gasoline
            .checked_add(quantities.premium)
            .and_then(|sum| sum.checked_add(quantities.diesel))
            .ok_or("유류비 고정 입력값 합계 초과")?;
        let unit_price_denominator = (total_qty != 0_i64)
            .then(|| {
                i128::from(total_qty)
                    .checked_mul(SCALE_I128)
                    .ok_or_else(|| err("단가 분모 overflow"))
            })
            .transpose()?;
        let ctx = RankSortContext {
            quantities,
            region_rates,
            smart_discount: read_dec(2, 13)?,
            unit_price_denominator,
        };
        let mut plans = vec_cap(
            self.data_last_row.strict_sub(DATA_START).strict_add(1) as usize,
            "정렬 대상 행",
        )?;
        let mut rank_buf = String::new();
        for (idx, row_num) in (DATA_START..=self.data_last_row).enumerate() {
            plans.push(self.build_sort_plan(idx, row_num, &ctx, &mut rank_buf)?);
        }
        plans.sort_unstable_by(|lhs, rhs| {
            lhs.rank_total
                .is_none()
                .cmp(&rhs.rank_total.is_none())
                .then_with(|| {
                    lhs.rank_value
                        .zip(rhs.rank_value)
                        .map_or(Ordering::Equal, |(lv, rv)| lv.total_cmp(&rv))
                })
                .then_with(|| {
                    (
                        lhs.fuels.into_array(),
                        lhs.region,
                        lhs.name,
                        lhs.address,
                        lhs.source_index,
                    )
                        .cmp(&(
                            rhs.fuels.into_array(),
                            rhs.region,
                            rhs.name,
                            rhs.address,
                            rhs.source_index,
                        ))
                })
        });
        let mut rows = self.ws.take_rows();
        let end_idx = self.data_last_row as usize;
        ensure(end_idx <= rows.len(), "정렬 대상 row 범위 초과")?;
        let trailing = rows.split_off(end_idx);
        let mut src_rows = rows.split_off(DATA_START.strict_sub(1) as usize);
        reserve(
            rows.try_reserve(src_rows.len().strict_add(trailing.len())),
            "정렬 결과 행",
        )?;
        for plan in &plans {
            rows.push(mem::take(or_abort(src_rows.get_mut(plan.source_index))));
        }
        rows.extend(trailing);
        self.ws.replace_rows(rows);
        let mut bufs = FormulaBuffers {
            cache: String::new(),
            formula: str_cap(512, "마스터 수식")?,
        };
        let mut num_buf = NumBuffer::new();
        let (mut ranked, mut cur_rank, mut prev_tot) = (0_usize, 0_usize, None);
        let (mut grp_end, mut seen_gap, mut prev_row) = (0_u32, false, None);
        for (idx, (row, plan)) in (DATA_START..=self.data_last_row).zip(&plans).enumerate() {
            let smart_follower = if plan.smart_discount_excluded {
                false
            } else if row <= grp_end {
                true
            } else {
                let gap = seen_gap || prev_row.is_some_and(|pr: u32| row > pr.strict_add(1));
                let run = or_abort(plans.get(idx..))
                    .iter()
                    .take(if gap { 32 } else { 64 })
                    .take_while(|cand| !cand.smart_discount_excluded)
                    .count();
                let last = row.strict_add(
                    u32::try_from(run.strict_sub(1)).unwrap_or_else(|_| process::abort()),
                );
                (seen_gap, prev_row, grp_end) = (gap, Some(last), if run >= 6 { last } else { 0 });
                false
            };
            let rank_cache = plan.rank_value.map(|cur| {
                ranked = ranked.strict_add(1);
                if prev_tot != Some(cur.to_bits()) {
                    cur_rank = ranked;
                    prev_tot = Some(cur.to_bits());
                }
                cur_rank.format_into(&mut num_buf)
            });
            self.apply_row_formulas(row, plan, &ctx, rank_cache, smart_follower, &mut bufs)?;
        }
        Ok(())
    }
}
impl<'source> MasterSheetUpdater<'source> {
    fn evaluate_master_rows(
        &self,
        ws: &excel::writer::Worksheet,
        sst: &SharedStringTable,
    ) -> Result<MasterRowEvaluation<'source>> {
        let row_count = ws.row_count();
        let mut addr_rows: HashMap<Cow<'source, str>, u32> = HashMap::new();
        reserve(addr_rows.try_reserve(row_count), "마스터 주소 행 맵")?;
        let mut kept = vec_cap(row_count, "행 목록")?;
        let (mut changes, mut deleted) = (Vec::new(), Vec::new());
        let (mut existing_counts, mut matched_counts) = (
            [0_usize; TARGET_REGION_COUNT],
            [0_usize; TARGET_REGION_COUNT],
        );
        let mut scratch = String::new();
        for old_row in ws.row_numbers_from(DATA_START) {
            let address = trim_cow(ws.try_get_display_at(6, old_row, sst)?);
            let name = trim_cow(ws.try_get_display_at(3, old_row, sst)?);
            let region = trim_cow(ws.try_get_display_at(2, old_row, sst)?);
            if region.is_empty() && name.is_empty() && address.is_empty() {
                continue;
            }
            let existing_reg = target_region(
                &region,
                &address,
                &mut scratch,
                TargetRegionPolicy::Flexible,
            )?;
            if let Some(target) = existing_reg {
                increment_target_region_count(&mut existing_counts, target);
            }
            if address.is_empty() {
                kept.push((old_row, None));
                continue;
            }
            let matched = self.source_index.get_key_value(scratch.as_str());
            let key = matched.map_or_else(
                || Cow::Owned(mem::take(&mut scratch)),
                |(mk, _)| Cow::Borrowed(mk.as_str()),
            );
            match addr_rows.entry(key) {
                Entry::Occupied(entry) => bail_fmt!(
                    "마스터 주소 중복: normalized_address={}, first_row={}, duplicate_row={old_row}",
                    entry.key(),
                    entry.get()
                ),
                Entry::Vacant(entry) => {
                    entry.insert(old_row);
                }
            }
            let fuels = read_master_fuels(ws, old_row, sst)?;
            let Some((_, src)) = matched else {
                let row = StoreRow {
                    address: address.into_owned(),
                    fuels,
                    name: name.into_owned(),
                    old_row,
                    region: region.into_owned(),
                };
                let grow = row_count.saturating_sub(deleted.len()).clamp(1, 64);
                push_vec(&mut deleted, row, grow, "변경 행 목록")?;
                continue;
            };
            if let Some(target) = existing_reg {
                increment_target_region_count(&mut matched_counts, target);
            }
            let (old_brand, old_self) = (
                ws.try_get_display_at(4, old_row, sst)?,
                ws.try_get_display_at(5, old_row, sst)?,
            );
            let mut reason = String::new();
            for (changed, label) in [
                (fuels != src.fuels, "가격변동"),
                (region.as_ref() != src.region, "지역정정"),
                (name.as_ref() != src.name, "상호변경"),
                (old_brand.trim() != src.brand, "상표변경"),
                (
                    !old_self
                        .chars()
                        .filter(|ch| !ch.is_whitespace())
                        .eq(src.service.label().chars()),
                    "셀프여부변경",
                ),
            ] {
                if changed {
                    if !reason.is_empty() {
                        reason.push_str(", ");
                    }
                    reason.push_str(label);
                }
            }
            if !reason.is_empty() {
                let grow = row_count.saturating_sub(changes.len()).clamp(1, 64);
                push_vec(
                    &mut changes,
                    ChangeRow {
                        old_fuels: fuels,
                        reason,
                        record: src,
                    },
                    grow,
                    "변경 행 목록",
                )?;
            }
            kept.push((old_row, Some(src)));
        }
        let mut added: Vec<&'source SourceRecord> = vec_cap(
            self.source_index
                .len()
                .strict_add(deleted.len())
                .strict_sub(addr_rows.len()),
            "신규 소스 정렬 목록",
        )?;
        added.extend(
            self.source_index
                .iter()
                .filter(|&(key, _)| !addr_rows.contains_key(key.as_str()))
                .map(|(_, rec)| rec),
        );
        added.sort_unstable_by(|lhs, rhs| {
            (lhs.region, &lhs.name, &lhs.address).cmp(&(rhs.region, &rhs.name, &rhs.address))
        });
        Ok(MasterRowEvaluation {
            added,
            changes,
            deleted,
            existing_region_counts: existing_counts,
            kept_source_rows: kept,
            matched_existing_region_counts: matched_counts,
        })
    }
    pub(super) fn get_f64_at(
        ws: &excel::writer::Worksheet,
        col: u32,
        row: u32,
        sst: &SharedStringTable,
    ) -> Result<Option<i64>> {
        let display = ws.try_get_display_at(col, row, sst)?;
        let trimmed = display.trim();
        if trimmed.is_empty() || trimmed == "-" {
            return Ok(None);
        }
        parse_decimal_scaled(trimmed, 6, true, false)
            .map(Some)
            .ok_or_else(|| fmt_err!("유류비 숫자 셀 오류: row={row}, col={col}, value={trimmed}"))
    }
    pub(super) fn update(
        &self,
        book: &mut StdWorkbook,
    ) -> Result<MasterSheetUpdateResult<'source>> {
        let (ws, sst) = book.master_sheet_mut();
        let MasterRowEvaluation {
            added,
            changes,
            deleted,
            existing_region_counts,
            kept_source_rows,
            matched_existing_region_counts,
        } = self.evaluate_master_rows(ws, sst)?;
        let kept_count = kept_source_rows.len();
        let existing_count = kept_count.strict_add(deleted.len());
        let last_old = kept_source_rows
            .last()
            .map(|&(row, _)| row)
            .max(deleted.last().map(|row| row.old_row));
        let old_last = last_old.unwrap_or(HEADER_ROW);
        let final_u32 = usize_to_u32(kept_count.strict_add(added.len()), "최종 유류비 행 수")?;
        let mut orig_rows = ws.take_rows();
        let template = (!added.is_empty())
            .then(|| {
                let tpl_row = last_old.unwrap_or(DATA_START);
                orig_rows
                    .get(tpl_row.strict_sub(1) as usize)
                    .ok_or_else(|| fmt_err!("유류비 신규행 template 없음: row={tpl_row}"))?
                    .try_copy_style(COL_SORT)
            })
            .transpose()?;
        let trailing_start = old_last as usize;
        (trailing_start <= orig_rows.len()).ok_or("유류비 기존 데이터 row 범위 초과")?;
        let trailing = orig_rows.split_off(trailing_start);
        let src_rows = orig_rows.split_off(HEADER_ROW as usize);
        reserve(
            orig_rows.try_reserve(
                kept_count
                    .strict_add(added.len())
                    .strict_add(trailing.len()),
            ),
            "유류비 결과행",
        )?;
        let mut kept_iter = kept_source_rows.iter();
        let mut next_kept = kept_iter.next();
        for (old_row, src_row) in (DATA_START..=old_last).zip(src_rows) {
            if next_kept.is_some_and(|&(kr, _)| kr == old_row) {
                orig_rows.push(src_row);
                next_kept = kept_iter.next();
            }
        }
        if let Some(&(old_row, _)) = next_kept {
            bail_fmt!("유류비 기존행 XML 없음: row={old_row}");
        }
        if let Some(tpl) = template {
            for _ in 1..added.len() {
                orig_rows.push(tpl.try_copy_style(COL_SORT)?);
            }
            orig_rows.push(tpl);
        }
        orig_rows.extend(trailing);
        ws.replace_rows(orig_rows);
        for (idx, (_, src)) in kept_source_rows.into_iter().enumerate() {
            let new_row = add_row_offset(DATA_START, idx, "유류비 기존행 재배치")?;
            if let Some(rec) = src {
                Self::write_row(ws, sst, new_row, rec)?;
            }
        }
        for (idx, &src) in added.iter().enumerate() {
            let new_row =
                add_row_offset(DATA_START, kept_count.strict_add(idx), "유류비 신규행 추가")?;
            Self::write_row(ws, sst, new_row, src)?;
        }
        let last_data_row = DATA_START.strict_add(final_u32.strict_sub(1));
        RankSortRefresher {
            data_last_row: last_data_row,
            shared_strings: sst,
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
    fn write_row(
        ws: &mut excel::writer::Worksheet,
        sst: &mut SharedStringTable,
        row: u32,
        src: &SourceRecord,
    ) -> Result<()> {
        for (col, val) in [
            (2, src.region),
            (3, src.name.as_str()),
            (4, src.brand.as_str()),
            (5, src.service.label()),
            (6, src.address.as_str()),
        ] {
            sst.set_cell(ws, col, row, val)?;
        }
        for (col, val) in [7, 8, 10].into_iter().zip(src.fuels.into_array()) {
            ws.set_i32_at(col, row, val)?;
        }
        Ok(())
    }
}
fn append_fuel_total(
    parts: &mut String,
    qty: i64,
    price: Option<i64>,
    label: &str,
) -> Result<bool> {
    if qty <= 0 {
        return Ok(true);
    }
    let Some(total) = price.and_then(|val| i128::from(qty).checked_mul(i128::from(val))) else {
        return Ok(false);
    };
    let rounded = total
        .checked_add(SCALE_SQ.div_euclid(2).strict_sub(i128::from(total < 0)))
        .ok_or("연료비 반올림 overflow")?
        .div_euclid(SCALE_SQ);
    if !parts.is_empty() {
        parts.push_str(" / ");
    }
    parts.extend([label, " "]);
    if rounded < 0 {
        parts.push('-');
    }
    let mut num_buf = NumBuffer::new();
    let digits = rounded.unsigned_abs().format_into(&mut num_buf);
    let mut grp = match digits.len().rem_euclid(3) {
        0 => 3,
        rem => rem,
    };
    for byte in digits.bytes() {
        if grp == 0 {
            parts.push(',');
            grp = 3;
        }
        parts.push(char::from(byte));
        grp = grp.strict_sub(1);
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
    let (whole, frac) = if let (Ok(au), Ok(su)) = (u64::try_from(abs), u64::try_from(scale_abs)) {
        (u128::from(au.div_euclid(su)), u128::from(au.rem_euclid(su)))
    } else {
        (abs.div_euclid(scale_abs), abs.rem_euclid(scale_abs))
    };
    let mut buf = NumBuffer::new();
    text.push_str(whole.format_into(&mut buf));
    if frac == 0 {
        return;
    }
    text.push('.');
    let frac_text = frac.format_into(&mut buf);
    for _ in frac_text.len()..(scale_abs.ilog10() as usize) {
        text.push('0');
    }
    text.push_str(frac_text);
    text.truncate(text.trim_end_matches('0').len());
}
fn read_master_fuels(
    ws: &excel::writer::Worksheet,
    row: u32,
    sst: &SharedStringTable,
) -> Result<FuelValues<Option<i32>>> {
    let read = |col| {
        ws.get_i32_at(col, row, sst)
            .map(|val| val.filter(|pr| *pr > 0_i32))
    };
    Ok(FuelValues {
        diesel: read(10)?,
        gasoline: read(7)?,
        premium: read(8)?,
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
