use crate::{
    ChangeRow, Result, StoreRow, add_row_offset, canon_header, display_region_label_from_source,
    err, excel,
    excel::writer::{Row as StdRow, Workbook as StdWorkbook, remap_row_numbers},
    normalize_address_key, push_display, same_trimmed, shift_row,
    source_sync::SourceRecord,
    usize_to_u32,
};
use alloc::collections::BTreeMap;
use core::{cmp::Ordering, error::Error, mem};
use std::{
    collections::{HashMap, HashSet},
    env,
};
const DECIMAL_SCALE: ScaledDecimal = 1_000_000;
const DECIMAL_SCALE_SQUARED: ScaledSortKey = 1_000_000_000_000;
const DECIMAL_SCALE_CUBED: ScaledSortKey = 1_000_000_000_000_000_000;
type ScaledDecimal = i64;
type ScaledSortKey = i128;
struct MasterRowDecision {
    change: Option<ChangeRow>,
    deleted: Option<StoreRow>,
    matched_key: Option<String>,
    src: Option<SourceRecord>,
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
    region_rates: HashMap<String, ScaledDecimal>,
    smart_discount: ScaledDecimal,
    total_qty: Option<ScaledDecimal>,
}
struct RankSortKey {
    address: String,
    diesel: ScaledSortKey,
    gasoline: ScaledSortKey,
    has_rank_total: bool,
    name: String,
    premium: ScaledSortKey,
    rank_total: ScaledSortKey,
    region: String,
}
struct RankFormulaCache {
    adjusted_diesel: Option<ScaledDecimal>,
    adjusted_gasoline: Option<ScaledDecimal>,
    adjusted_premium: Option<ScaledDecimal>,
    fuel_total_text: Option<String>,
    rank_total: Option<ScaledSortKey>,
    region_rate: Option<ScaledDecimal>,
    regional_discount: Option<ScaledSortKey>,
    smart_discount: ScaledDecimal,
    total_price: Option<ScaledSortKey>,
    unit_price_with_currency: Option<String>,
    unit_price_without_currency: Option<String>,
}
struct ExistingMasterRow<'row> {
    brand: &'row str,
    diesel: Option<i32>,
    gasoline: Option<i32>,
    name: &'row str,
    premium: Option<i32>,
    region: &'row str,
    self_yn: &'row str,
}
#[derive(Debug, Clone)]
struct KeptMasterRow {
    new_row: u32,
    src: Option<SourceRecord>,
}
struct NewSourcePlacementPlan<'rows> {
    data_start_row: u32,
    kept_count: usize,
    new_rows_map: &'rows mut BTreeMap<u32, StdRow>,
    new_sources: &'rows [SourceRecord],
    row_mapper: &'rows RowMapper,
    template_row: &'rows StdRow,
    template_row_num: u32,
}
struct MasterRowEvaluation {
    changes: Vec<ChangeRow>,
    deleted: Vec<StoreRow>,
    kept_source_rows: Vec<(u32, Option<SourceRecord>)>,
    matched_source_keys: HashSet<String>,
}
struct RowMapper {
    data_start_row: u32,
    decrease: u32,
    deleted_rows: Vec<u32>,
    has_old_rows: bool,
    increase: u32,
    old_count_u32: u32,
    old_end_row: u32,
}
pub struct MasterSheetOps;
pub trait MasterSheetApi {
    fn update_master_sheet(
        &self,
        book: &mut StdWorkbook,
        source_index: &HashMap<String, SourceRecord>,
    ) -> Result<(Vec<ChangeRow>, Vec<StoreRow>, Vec<StoreRow>)>;
}
trait MasterSheetOpsExt {
    fn build_change_row_if_needed(
        &self,
        old: &ExistingMasterRow<'_>,
        src: &SourceRecord,
    ) -> Option<ChangeRow>;
    fn build_deleted_rows(
        &self,
        old_rows: &[u32],
        kept_source_rows: &[(u32, Option<SourceRecord>)],
    ) -> Vec<u32>;
    fn build_rank_formula_cache(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        row: u32,
        layout: MasterSheetLayout,
        sort_context: &RankSortContext,
        display_total_qty: Option<ScaledDecimal>,
    ) -> RankFormulaCache;
    fn build_rank_sort_context(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
    ) -> RankSortContext;
    fn build_rebased_non_data_rows(
        &self,
        original_rows: &BTreeMap<u32, StdRow>,
        data_start_row: u32,
        old_end_row: u32,
        row_mapper: &RowMapper,
    ) -> BTreeMap<u32, StdRow>;
    fn collect_master_data_rows(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        data_start_row: u32,
        layout: MasterSheetLayout,
    ) -> Vec<u32>;
    fn collect_new_sources(
        &self,
        source_index: &HashMap<String, SourceRecord>,
        matched_source_keys: &HashSet<String>,
    ) -> Vec<SourceRecord>;
    fn compare_out_of_rank_fuels(&self, left: &RankSortKey, right: &RankSortKey) -> Ordering;
    fn compare_rank_sort_key(&self, left: &RankSortKey, right: &RankSortKey) -> Ordering;
    fn compute_rank_sort_key(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        row: u32,
        layout: MasterSheetLayout,
        sort_context: &RankSortContext,
    ) -> RankSortKey;
    fn compute_total_price(
        &self,
        gasoline_qty: ScaledDecimal,
        adjusted_gasoline: Option<ScaledDecimal>,
        premium_qty: ScaledDecimal,
        adjusted_premium: Option<ScaledDecimal>,
        diesel_qty: ScaledDecimal,
        adjusted_diesel: Option<ScaledDecimal>,
    ) -> Option<ScaledSortKey>;
    fn default_row(&self, row_num: u32) -> StdRow;
    fn evaluate_master_row(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        old_row: u32,
        source_index: &HashMap<String, SourceRecord>,
        layout: MasterSheetLayout,
    ) -> MasterRowDecision;
    fn evaluate_master_rows(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        old_rows: &[u32],
        source_index: &HashMap<String, SourceRecord>,
        layout: MasterSheetLayout,
    ) -> MasterRowEvaluation;
    fn find_master_sheet_layout(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
    ) -> Result<(u32, MasterSheetLayout)>;
    fn fuel_sort_value(&self, value: Option<ScaledSortKey>) -> ScaledSortKey;
    fn get_f64_at(
        &self,
        ws: &excel::writer::Worksheet,
        col: u32,
        row: u32,
        shared_strings: &[String],
    ) -> Option<ScaledDecimal>;
    fn get_master_header_col_optional(
        &self,
        headers: &HashMap<String, u32>,
        keys: &[&str],
    ) -> Option<u32>;
    fn get_master_header_col_required(
        &self,
        headers: &HashMap<String, u32>,
        keys: &[&str],
        display_name: &str,
    ) -> Result<u32>;
    fn is_zero(&self, value: ScaledDecimal) -> bool;
    fn master_header_scan_cols(&self, ws: &excel::writer::Worksheet) -> u32;
    fn master_header_scan_rows(&self) -> u32;
    fn normalize_fuel_price(&self, value: Option<i32>) -> Option<i32>;
    fn parse_f64_text(&self, text: &str) -> Option<ScaledDecimal>;
    fn place_kept_rows(
        &self,
        new_rows_map: &mut BTreeMap<u32, StdRow>,
        original_rows: &BTreeMap<u32, StdRow>,
        kept_source_rows: &[(u32, Option<SourceRecord>)],
        data_start_row: u32,
        row_mapper: &RowMapper,
    ) -> Result<Vec<KeptMasterRow>>;
    fn place_new_source_rows(
        &self,
        plan: NewSourcePlacementPlan<'_>,
    ) -> Result<Vec<(u32, SourceRecord)>>;
    fn rebuild_master_rows(
        &self,
        ws: &mut excel::writer::Worksheet,
        shared_strings: &[String],
        plan: MasterSheetPlan,
        old_rows: &[u32],
        kept_source_rows: &[(u32, Option<SourceRecord>)],
        new_sources: &[SourceRecord],
    ) -> Result<(u32, u32)>;
    fn refresh_rank_formula_caches(
        &self,
        ws: &mut excel::writer::Worksheet,
        shared_strings: &[String],
        data_start_row: u32,
        data_end_row: u32,
        layout: MasterSheetLayout,
    );
    fn repair_rank_formulas(
        &self,
        ws: &mut excel::writer::Worksheet,
        data_start_row: u32,
        data_end_row: u32,
        layout: MasterSheetLayout,
    );
    fn rewrite_rank_formula_range(
        &self,
        formula: &str,
        sort_key_col: u32,
        data_start_row: u32,
        data_end_row: u32,
    ) -> String;
    fn rows_from_sources(&self, new_sources: &[SourceRecord]) -> Vec<StoreRow>;
    fn sort_master_rows_by_rank(
        &self,
        ws: &mut excel::writer::Worksheet,
        shared_strings: &[String],
        data_start_row: u32,
        data_end_row: u32,
        layout: MasterSheetLayout,
    ) -> Result<()>;
    fn sort_refresh_master_rows(
        &self,
        ws: &mut excel::writer::Worksheet,
        shared_strings: &[String],
        data_start_row: u32,
        data_end_row: u32,
        layout: MasterSheetLayout,
    ) -> Result<()>;
    fn update_filter_database_defined_name(
        &self,
        workbook_xml: &mut String,
        data_start_row: u32,
        data_end_row: u32,
        data_end_col: u32,
    );
    fn update_master_sheet_impl(
        &self,
        book: &mut StdWorkbook,
        source_index: &HashMap<String, SourceRecord>,
    ) -> Result<(Vec<ChangeRow>, Vec<StoreRow>, Vec<StoreRow>)>;
    fn write_master_row_from_source(
        &self,
        ws: &mut excel::writer::Worksheet,
        row: u32,
        src: &SourceRecord,
        layout: MasterSheetLayout,
    );
    fn write_rank_formula_cache(
        &self,
        ws: &mut excel::writer::Worksheet,
        row: u32,
        layout: MasterSheetLayout,
        cache: &RankFormulaCache,
    );
    fn write_source_rows_to_master(
        &self,
        ws: &mut excel::writer::Worksheet,
        kept_rows: &[KeptMasterRow],
        new_rows_from_sources: &[(u32, SourceRecord)],
        layout: MasterSheetLayout,
    );
}
impl MasterSheetApi for MasterSheetOps {
    fn update_master_sheet(
        &self,
        book: &mut StdWorkbook,
        source_index: &HashMap<String, SourceRecord>,
    ) -> Result<(Vec<ChangeRow>, Vec<StoreRow>, Vec<StoreRow>)> {
        self.update_master_sheet_impl(book, source_index)
    }
}
impl RowMapper {
    fn map(&self, old_ref_row: u32) -> u32 {
        if self.has_old_rows
            && old_ref_row >= self.data_start_row
            && old_ref_row <= self.old_end_row
        {
            let deleted_le = u32::try_from(match self.deleted_rows.binary_search(&old_ref_row) {
                Ok(idx) => idx.saturating_add(1),
                Err(idx) => idx,
            })
            .unwrap_or(self.old_count_u32);
            return old_ref_row.saturating_sub(deleted_le);
        }
        if old_ref_row > self.old_end_row {
            return shift_row(old_ref_row, self.increase, self.decrease);
        }
        old_ref_row
    }
    fn shift(&self, row: u32) -> u32 {
        shift_row(row, self.increase, self.decrease)
    }
}
impl MasterSheetOpsExt for MasterSheetOps {
    fn build_change_row_if_needed(
        &self,
        old: &ExistingMasterRow<'_>,
        src: &SourceRecord,
    ) -> Option<ChangeRow> {
        let name_changed = !same_trimmed(old.name, &src.name);
        let brand_changed = !same_trimmed(old.brand, &src.brand);
        let self_yn_changed = canon_header(old.self_yn) != canon_header(&src.self_yn);
        let gas_changed = old.gasoline != src.gasoline;
        let premium_changed = old.premium != src.premium;
        let diesel_changed = old.diesel != src.diesel;
        if !(name_changed
            || brand_changed
            || self_yn_changed
            || gas_changed
            || premium_changed
            || diesel_changed)
        {
            return None;
        }
        let capacity = 32;
        let mut reasons = String::with_capacity(capacity);
        let mut push_reason = |reason: &str| {
            if !reasons.is_empty() {
                reasons.push_str(", ");
            }
            reasons.push_str(reason);
        };
        if gas_changed || premium_changed || diesel_changed {
            push_reason("가격변동");
        }
        if name_changed {
            push_reason("상호변경");
        }
        if brand_changed {
            push_reason("상표변경");
        }
        if self_yn_changed {
            push_reason("셀프여부변경");
        }
        Some(ChangeRow {
            reason: reasons,
            region: old.region.to_owned(),
            name: src.name.clone(),
            address: src.address.clone(),
            old_gasoline: old.gasoline,
            new_gasoline: src.gasoline,
            old_premium: old.premium,
            new_premium: src.premium,
            old_diesel: old.diesel,
            new_diesel: src.diesel,
        })
    }
    fn build_deleted_rows(
        &self,
        old_rows: &[u32],
        kept_source_rows: &[(u32, Option<SourceRecord>)],
    ) -> Vec<u32> {
        let kept_old_rows: HashSet<u32> = kept_source_rows.iter().map(|entry| entry.0).collect();
        let mut deleted_rows: Vec<u32> = old_rows
            .iter()
            .copied()
            .filter(|row_num| !kept_old_rows.contains(row_num))
            .collect();
        deleted_rows.sort_unstable();
        deleted_rows
    }
    fn build_rank_formula_cache(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        row: u32,
        layout: MasterSheetLayout,
        sort_context: &RankSortContext,
        display_total_qty: Option<ScaledDecimal>,
    ) -> RankFormulaCache {
        let region = ws.get_display_at(layout.region, row, shared_strings);
        let name = ws.get_display_at(layout.name, row, shared_strings);
        let gasoline =
            self.normalize_fuel_price(ws.get_i32_at(layout.gasoline, row, shared_strings));
        let premium = self.normalize_fuel_price(ws.get_i32_at(layout.premium, row, shared_strings));
        let diesel = self.normalize_fuel_price(ws.get_i32_at(layout.diesel, row, shared_strings));
        let smart_discount = if name.contains("현대오일뱅크") && name.contains("직영") {
            sort_context.smart_discount
        } else {
            0
        };
        let adjusted_gasoline = gasoline.and_then(|value| {
            i64::from(value)
                .checked_mul(DECIMAL_SCALE)?
                .checked_add(smart_discount)
        });
        let adjusted_premium = premium.and_then(|value| {
            i64::from(value)
                .checked_mul(DECIMAL_SCALE)?
                .checked_add(smart_discount)
        });
        let adjusted_diesel = diesel.and_then(|value| {
            i64::from(value)
                .checked_mul(DECIMAL_SCALE)?
                .checked_add(smart_discount)
        });
        let total_price = display_total_qty.and_then(|_| {
            self.compute_total_price(
                sort_context.gasoline_qty,
                adjusted_gasoline,
                sort_context.premium_qty,
                adjusted_premium,
                sort_context.diesel_qty,
                adjusted_diesel,
            )
        });
        let currency_apply = layout
            .currency_apply
            .map(|col| ws.get_display_at(col, row, shared_strings))
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("Y"));
        let region_rate = if total_price.is_some() && currency_apply {
            sort_context
                .region_rates
                .get(region.trim())
                .copied()
                .unwrap_or(0)
        } else {
            0
        };
        let regional_discount = total_price.and_then(|value| {
            value
                .checked_mul(i128::from(region_rate))?
                .checked_div(DECIMAL_SCALE_CUBED)?
                .checked_mul(DECIMAL_SCALE_SQUARED)
        });
        let rank_total = total_price
            .zip(regional_discount)
            .and_then(|(total, discount)| total.checked_sub(discount));
        let fuel_total_text = display_total_qty.and_then(|_| {
            let mut parts = Vec::with_capacity(3);
            if sort_context.gasoline_qty > 0 {
                let total = i128::from(sort_context.gasoline_qty)
                    .checked_mul(i128::from(adjusted_gasoline?))?;
                parts.push(format_fuel_price_text("휘발유", total));
            }
            if sort_context.premium_qty > 0 {
                let total = i128::from(sort_context.premium_qty)
                    .checked_mul(i128::from(adjusted_premium?))?;
                parts.push(format_fuel_price_text("고급유", total));
            }
            if sort_context.diesel_qty > 0 {
                let total = i128::from(sort_context.diesel_qty)
                    .checked_mul(i128::from(adjusted_diesel?))?;
                parts.push(format_fuel_price_text("경유", total));
            }
            Some(parts.join(" / "))
        });
        RankFormulaCache {
            adjusted_diesel,
            adjusted_gasoline,
            adjusted_premium,
            fuel_total_text,
            rank_total,
            region_rate: total_price.map(|_| region_rate),
            regional_discount,
            smart_discount,
            total_price,
            unit_price_with_currency: rank_total
                .zip(display_total_qty)
                .and_then(|(value, qty)| format_unit_price_text(value, qty)),
            unit_price_without_currency: total_price
                .zip(display_total_qty)
                .and_then(|(value, qty)| format_unit_price_text(value, qty)),
        }
    }
    fn build_rank_sort_context(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
    ) -> RankSortContext {
        let gasoline_qty = self.get_f64_at(ws, 2, 4, shared_strings).unwrap_or(0);
        let premium_qty = self.get_f64_at(ws, 2, 5, shared_strings).unwrap_or(0);
        let diesel_qty = self.get_f64_at(ws, 2, 6, shared_strings).unwrap_or(0);
        let mut region_rates = HashMap::with_capacity(10);
        for row in 4..=13 {
            let region = ws.get_display_at(3, row, shared_strings).trim().to_owned();
            if region.is_empty() {
                continue;
            }
            if let Some(rate) = self.get_f64_at(ws, 4, row, shared_strings) {
                region_rates.insert(region, rate);
            }
        }
        let total_qty = self
            .get_f64_at(ws, 2, 10, shared_strings)
            .filter(|value| !self.is_zero(*value))
            .or_else(|| {
                let derived_total = gasoline_qty
                    .checked_add(premium_qty)?
                    .checked_add(diesel_qty)?;
                (!self.is_zero(derived_total)).then_some(derived_total)
            });
        RankSortContext {
            gasoline_qty,
            premium_qty,
            diesel_qty,
            total_qty,
            smart_discount: self.get_f64_at(ws, 2, 13, shared_strings).unwrap_or(0),
            region_rates,
        }
    }
    fn build_rebased_non_data_rows(
        &self,
        original_rows: &BTreeMap<u32, StdRow>,
        data_start_row: u32,
        old_end_row: u32,
        row_mapper: &RowMapper,
    ) -> BTreeMap<u32, StdRow> {
        let mut new_rows_map = BTreeMap::default();
        for (row_ref, row_obj) in original_rows {
            let row_num = *row_ref;
            if row_mapper.has_old_rows && row_num >= data_start_row && row_num <= old_end_row {
                continue;
            }
            let mut cloned_row = row_obj.clone();
            if row_num < data_start_row {
                remap_row_numbers(&mut cloned_row, row_num, &|old_ref_row| {
                    row_mapper.map(old_ref_row)
                });
                new_rows_map.insert(row_num, cloned_row);
            } else {
                let shifted = row_mapper.shift(row_num);
                remap_row_numbers(&mut cloned_row, shifted, &|old_ref_row| {
                    row_mapper.map(old_ref_row)
                });
                new_rows_map.insert(shifted, cloned_row);
            }
        }
        new_rows_map
    }
    fn collect_master_data_rows(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        data_start_row: u32,
        layout: MasterSheetLayout,
    ) -> Vec<u32> {
        let mut rows = Vec::with_capacity(ws.rows.len());
        for row in ws.rows.range(data_start_row..).map(|(row, _)| *row) {
            let region = ws.get_display_at(layout.region, row, shared_strings);
            let name = ws.get_display_at(layout.name, row, shared_strings);
            let addr = ws.get_display_at(layout.address, row, shared_strings);
            if region.trim().is_empty() && name.trim().is_empty() && addr.trim().is_empty() {
                continue;
            }
            rows.push(row);
        }
        rows
    }
    fn collect_new_sources(
        &self,
        source_index: &HashMap<String, SourceRecord>,
        matched_source_keys: &HashSet<String>,
    ) -> Vec<SourceRecord> {
        let mut new_sources: Vec<(String, &SourceRecord)> = source_index
            .iter()
            .filter(|entry| !matched_source_keys.contains(entry.0.as_str()))
            .map(|entry| {
                let rec = entry.1;
                (
                    display_region_label_from_source(&rec.region, &rec.address),
                    rec,
                )
            })
            .collect();
        new_sources.sort_by(|left, right| {
            left.0
                .cmp(&right.0)
                .then_with(|| left.1.name.cmp(&right.1.name))
                .then_with(|| left.1.address.cmp(&right.1.address))
        });
        new_sources
            .into_iter()
            .map(|(_, rec)| rec.clone())
            .collect()
    }
    fn compare_out_of_rank_fuels(&self, left: &RankSortKey, right: &RankSortKey) -> Ordering {
        left.gasoline
            .cmp(&right.gasoline)
            .then_with(|| left.premium.cmp(&right.premium))
            .then_with(|| left.diesel.cmp(&right.diesel))
    }
    fn compare_rank_sort_key(&self, left: &RankSortKey, right: &RankSortKey) -> Ordering {
        right
            .has_rank_total
            .cmp(&left.has_rank_total)
            .then_with(|| left.rank_total.cmp(&right.rank_total))
            .then_with(|| self.compare_out_of_rank_fuels(left, right))
            .then_with(|| left.region.cmp(&right.region))
            .then_with(|| left.name.cmp(&right.name))
            .then_with(|| left.address.cmp(&right.address))
    }
    fn compute_rank_sort_key(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        row: u32,
        layout: MasterSheetLayout,
        sort_context: &RankSortContext,
    ) -> RankSortKey {
        let region = ws.get_display_at(layout.region, row, shared_strings);
        let name = ws.get_display_at(layout.name, row, shared_strings);
        let address = ws.get_display_at(layout.address, row, shared_strings);
        let gasoline =
            self.normalize_fuel_price(ws.get_i32_at(layout.gasoline, row, shared_strings));
        let premium = self.normalize_fuel_price(ws.get_i32_at(layout.premium, row, shared_strings));
        let diesel = self.normalize_fuel_price(ws.get_i32_at(layout.diesel, row, shared_strings));
        let premium_qty = sort_context.premium_qty;
        let gasoline_qty = sort_context.gasoline_qty;
        let diesel_qty = sort_context.diesel_qty;
        let is_direct_hyundai = name.contains("현대오일뱅크") && name.contains("직영");
        let discount = if is_direct_hyundai {
            sort_context.smart_discount
        } else {
            0
        };
        let adjusted_gasoline = gasoline.and_then(|value| {
            i64::from(value)
                .checked_mul(DECIMAL_SCALE)?
                .checked_add(discount)
        });
        let adjusted_premium = premium.and_then(|value| {
            i64::from(value)
                .checked_mul(DECIMAL_SCALE)?
                .checked_add(discount)
        });
        let adjusted_diesel = diesel.and_then(|value| {
            i64::from(value)
                .checked_mul(DECIMAL_SCALE)?
                .checked_add(discount)
        });
        let currency_apply = layout
            .currency_apply
            .map(|col| ws.get_display_at(col, row, shared_strings))
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("Y"));
        let region_rate = if currency_apply {
            sort_context
                .region_rates
                .get(region.trim())
                .copied()
                .unwrap_or(0)
        } else {
            0
        };
        let region_multiplier = DECIMAL_SCALE.saturating_sub(region_rate);
        let regional_adjusted_gasoline = adjusted_gasoline
            .and_then(|value| i128::from(value).checked_mul(i128::from(region_multiplier)));
        let regional_adjusted_premium = adjusted_premium
            .and_then(|value| i128::from(value).checked_mul(i128::from(region_multiplier)));
        let regional_adjusted_diesel = adjusted_diesel
            .and_then(|value| i128::from(value).checked_mul(i128::from(region_multiplier)));
        let rank_total = sort_context.total_qty.and_then(|total_qty| {
            (!self.is_zero(total_qty))
                .then_some(total_qty)
                .and_then(|_| {
                    self.compute_total_price(
                        gasoline_qty,
                        adjusted_gasoline,
                        premium_qty,
                        adjusted_premium,
                        diesel_qty,
                        adjusted_diesel,
                    )
                })
                .and_then(|total_price| {
                    let discount_numerator = total_price.checked_mul(i128::from(region_rate))?;
                    let discount_floor = discount_numerator.checked_div(DECIMAL_SCALE_CUBED)?;
                    let discount_value = discount_floor.checked_mul(DECIMAL_SCALE_SQUARED)?;
                    total_price.checked_sub(discount_value)
                })
                .filter(|value| *value != 0)
        });
        RankSortKey {
            has_rank_total: rank_total.is_some(),
            rank_total: rank_total.unwrap_or(i128::MAX),
            gasoline: self.fuel_sort_value(regional_adjusted_gasoline),
            premium: self.fuel_sort_value(regional_adjusted_premium),
            diesel: self.fuel_sort_value(regional_adjusted_diesel),
            region,
            name,
            address,
        }
    }
    fn compute_total_price(
        &self,
        gasoline_qty: ScaledDecimal,
        adjusted_gasoline: Option<ScaledDecimal>,
        premium_qty: ScaledDecimal,
        adjusted_premium: Option<ScaledDecimal>,
        diesel_qty: ScaledDecimal,
        adjusted_diesel: Option<ScaledDecimal>,
    ) -> Option<ScaledSortKey> {
        let mut total = 0_i128;
        if gasoline_qty > 0 {
            total = total.checked_add(
                i128::from(gasoline_qty).checked_mul(i128::from(adjusted_gasoline?))?,
            )?;
        }
        if premium_qty > 0 {
            total = total
                .checked_add(i128::from(premium_qty).checked_mul(i128::from(adjusted_premium?))?)?;
        }
        if diesel_qty > 0 {
            total = total
                .checked_add(i128::from(diesel_qty).checked_mul(i128::from(adjusted_diesel?))?)?;
        }
        Some(total)
    }
    fn default_row(&self, row_num: u32) -> StdRow {
        StdRow {
            attrs: vec![("r".into(), {
                let capacity = 12;
                let mut row_text = String::with_capacity(capacity);
                push_display(&mut row_text, row_num);
                row_text
            })],
            cells: BTreeMap::default(),
        }
    }
    fn evaluate_master_row(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        old_row: u32,
        source_index: &HashMap<String, SourceRecord>,
        layout: MasterSheetLayout,
    ) -> MasterRowDecision {
        let region = ws
            .get_display_at(layout.region, old_row, shared_strings)
            .trim()
            .to_owned();
        let name = ws
            .get_display_at(layout.name, old_row, shared_strings)
            .trim()
            .to_owned();
        let addr = ws
            .get_display_at(layout.address, old_row, shared_strings)
            .trim()
            .to_owned();
        if addr.is_empty() {
            return MasterRowDecision {
                src: None,
                matched_key: None,
                change: None,
                deleted: None,
            };
        }
        let key = normalize_address_key(&addr);
        let Some(src) = source_index.get(&key) else {
            return MasterRowDecision {
                src: None,
                matched_key: None,
                change: None,
                deleted: Some(StoreRow {
                    region,
                    name,
                    address: addr,
                    gasoline: self.normalize_fuel_price(ws.get_i32_at(
                        layout.gasoline,
                        old_row,
                        shared_strings,
                    )),
                    premium: self.normalize_fuel_price(ws.get_i32_at(
                        layout.premium,
                        old_row,
                        shared_strings,
                    )),
                    diesel: self.normalize_fuel_price(ws.get_i32_at(
                        layout.diesel,
                        old_row,
                        shared_strings,
                    )),
                }),
            };
        };
        let old_brand = ws
            .get_display_at(layout.brand, old_row, shared_strings)
            .trim()
            .to_owned();
        let old_self_yn = ws
            .get_display_at(layout.self_yn, old_row, shared_strings)
            .trim()
            .to_owned();
        let old_gas =
            self.normalize_fuel_price(ws.get_i32_at(layout.gasoline, old_row, shared_strings));
        let old_premium =
            self.normalize_fuel_price(ws.get_i32_at(layout.premium, old_row, shared_strings));
        let old_diesel =
            self.normalize_fuel_price(ws.get_i32_at(layout.diesel, old_row, shared_strings));
        let old = ExistingMasterRow {
            brand: &old_brand,
            premium: old_premium,
            diesel: old_diesel,
            gasoline: old_gas,
            name: &name,
            region: &region,
            self_yn: &old_self_yn,
        };
        let change = self.build_change_row_if_needed(&old, src);
        MasterRowDecision {
            src: Some(src.clone()),
            matched_key: Some(key),
            change,
            deleted: None,
        }
    }
    fn evaluate_master_rows(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        old_rows: &[u32],
        source_index: &HashMap<String, SourceRecord>,
        layout: MasterSheetLayout,
    ) -> MasterRowEvaluation {
        let mut matched_source_keys = HashSet::with_capacity(old_rows.len());
        let mut kept_source_rows = Vec::with_capacity(old_rows.len());
        let mut changes = Vec::with_capacity(old_rows.len());
        let mut deleted = Vec::with_capacity(old_rows.len());
        for old_row in old_rows.iter().copied() {
            let MasterRowDecision {
                src,
                matched_key,
                change,
                deleted: deleted_row,
            } = self.evaluate_master_row(ws, shared_strings, old_row, source_index, layout);
            if let Some(row) = deleted_row {
                deleted.push(row);
                continue;
            }
            if let Some(key) = matched_key {
                matched_source_keys.insert(key);
            }
            if let Some(row_change) = change {
                changes.push(row_change);
            }
            kept_source_rows.push((old_row, src));
        }
        MasterRowEvaluation {
            changes,
            deleted,
            kept_source_rows,
            matched_source_keys,
        }
    }
    fn find_master_sheet_layout(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
    ) -> Result<(u32, MasterSheetLayout)> {
        let max_cols = self.master_header_scan_cols(ws);
        for row in 1..=self.master_header_scan_rows() {
            let mut headers: HashMap<String, u32> =
                HashMap::with_capacity(usize::try_from(max_cols).unwrap_or(0));
            for col in 1..=max_cols {
                let key = canon_header(ws.get_display_at(col, row, shared_strings).trim());
                if key.is_empty() {
                    continue;
                }
                headers.entry(key).or_insert(col);
            }
            if headers.is_empty()
                || self
                    .get_master_header_col_optional(&headers, &["지역화폐적용순위"])
                    .is_none()
            {
                continue;
            }
            let layout = MasterSheetLayout {
                address: self.get_master_header_col_required(&headers, &["주소"], "주소")?,
                adjusted_diesel: self
                    .get_master_header_col_optional(&headers, &["조정경유단가(원/L)"]),
                adjusted_gasoline: self
                    .get_master_header_col_optional(&headers, &["조정휘발유단가(원/L)"]),
                adjusted_premium: self
                    .get_master_header_col_optional(&headers, &["조정고급유단가(원/L)"]),
                brand: self.get_master_header_col_required(&headers, &["상표"], "상표")?,
                self_yn: self.get_master_header_col_required(
                    &headers,
                    &["셀프여부", "셀프"],
                    "셀프여부",
                )?,
                gasoline: self.get_master_header_col_required(
                    &headers,
                    &["휘발유", "보통휘발유", "휘발유단가(원/L)", "휘발유단가"],
                    "휘발유",
                )?,
                fuel_total_text: self
                    .get_master_header_col_optional(&headers, &["유종별 총가격(원)"]),
                name: self.get_master_header_col_required(&headers, &["상호"], "상호")?,
                premium: self.get_master_header_col_required(
                    &headers,
                    &["고급유", "고급휘발유", "고급유단가(원/L)", "고급유단가"],
                    "고급유",
                )?,
                rank: self.get_master_header_col_required(
                    &headers,
                    &["지역화폐적용순위"],
                    "지역화폐적용순위",
                )?,
                region: self.get_master_header_col_required(&headers, &["지역"], "지역")?,
                region_discount: self
                    .get_master_header_col_optional(&headers, &["지역화폐적립액(원)"]),
                region_rate: self.get_master_header_col_optional(&headers, &["지역화폐적립율"]),
                regional_total: self
                    .get_master_header_col_optional(&headers, &["지역화폐적용금액(원)"]),
                diesel: self.get_master_header_col_required(
                    &headers,
                    &["경유", "경유단가(원/L)", "경유단가"],
                    "경유",
                )?,
                currency_apply: self.get_master_header_col_optional(
                    &headers,
                    &["지역화폐적용여부", "지역화폐 적용여부"],
                ),
                smart_discount: self
                    .get_master_header_col_optional(&headers, &["스마트주유 할인(원/L)"]),
                sort_key: self.get_master_header_col_optional(&headers, &["정렬키"]),
                total_price: self.get_master_header_col_optional(&headers, &["총가격(원)"]),
                unit_price_with_currency: self
                    .get_master_header_col_optional(&headers, &["지역화폐 적용단가(원/L)"]),
                unit_price_without_currency: self
                    .get_master_header_col_optional(&headers, &["지역화폐 미적용 단가(원/L)"]),
            };
            return Ok((row, layout));
        }
        Err(err(
            "유류비 시트에서 헤더 행을 찾지 못했습니다. 필수 컬럼(지역화폐적용순위/지역/상호/상표/셀프여부/주소/휘발유/고급유/경유)을 확인하세요.",
        ))
    }
    fn fuel_sort_value(&self, value: Option<ScaledSortKey>) -> ScaledSortKey {
        value.unwrap_or(i128::MAX)
    }
    fn get_f64_at(
        &self,
        ws: &excel::writer::Worksheet,
        col: u32,
        row: u32,
        shared_strings: &[String],
    ) -> Option<ScaledDecimal> {
        self.parse_f64_text(&ws.get_display_at(col, row, shared_strings))
    }
    fn get_master_header_col_optional(
        &self,
        headers: &HashMap<String, u32>,
        keys: &[&str],
    ) -> Option<u32> {
        for key in keys {
            let canon = canon_header(key);
            if let Some(col) = headers.get(&canon) {
                return Some(*col);
            }
        }
        None
    }
    fn get_master_header_col_required(
        &self,
        headers: &HashMap<String, u32>,
        keys: &[&str],
        display_name: &str,
    ) -> Result<u32> {
        self.get_master_header_col_optional(headers, keys)
            .ok_or_else(|| {
                let capacity = display_name.len().saturating_add(32);
                let mut message = String::with_capacity(capacity);
                message.push_str("유류비 헤더에 '");
                message.push_str(display_name);
                message.push_str("' 컬럼이 없습니다.");
                err(message)
            })
    }
    fn is_zero(&self, value: ScaledDecimal) -> bool {
        value == 0
    }
    fn master_header_scan_cols(&self, ws: &excel::writer::Worksheet) -> u32 {
        ws.max_cell_col().clamp(20, 200)
    }
    fn master_header_scan_rows(&self) -> u32 {
        env::var("FCUPDATER_MASTER_HEADER_SCAN_ROWS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| *value > 0)
            .map_or(200, |value| value.min(20_000))
    }
    fn normalize_fuel_price(&self, value: Option<i32>) -> Option<i32> {
        value.filter(|price| *price > 0_i32)
    }
    fn parse_f64_text(&self, text: &str) -> Option<ScaledDecimal> {
        let trimmed = text.trim();
        if trimmed.is_empty() || trimmed == "-" {
            return None;
        }
        let normalized_storage;
        let normalized = if trimmed.contains(',') {
            normalized_storage = trimmed.replace(',', "");
            normalized_storage.as_str()
        } else {
            trimmed
        };
        let (sign, digits) = normalized
            .strip_prefix('-')
            .map_or((1_i64, normalized), |rest| (-1_i64, rest));
        let (whole_text, fraction_text) = digits.split_once('.').unwrap_or((digits, ""));
        let whole = whole_text.parse::<i64>().ok()?;
        let mut fraction = 0_i64;
        let mut fraction_digit_count = 0_u8;
        for ch in fraction_text.chars().filter(char::is_ascii_digit).take(6) {
            let digit = ch.to_digit(10)?;
            fraction = fraction.checked_mul(10)?.checked_add(i64::from(digit))?;
            fraction_digit_count = fraction_digit_count.checked_add(1)?;
        }
        while fraction_digit_count < 6 {
            fraction = fraction.checked_mul(10)?;
            fraction_digit_count = fraction_digit_count.checked_add(1)?;
        }
        let whole_scaled = whole.checked_mul(DECIMAL_SCALE)?;
        let combined = whole_scaled.checked_add(fraction)?;
        combined.checked_mul(sign)
    }
    fn place_kept_rows(
        &self,
        new_rows_map: &mut BTreeMap<u32, StdRow>,
        original_rows: &BTreeMap<u32, StdRow>,
        kept_source_rows: &[(u32, Option<SourceRecord>)],
        data_start_row: u32,
        row_mapper: &RowMapper,
    ) -> Result<Vec<KeptMasterRow>> {
        let mut kept_rows: Vec<KeptMasterRow> = Vec::with_capacity(kept_source_rows.len());
        for (i, kept_source_row) in kept_source_rows.iter().enumerate() {
            let old_row = kept_source_row.0;
            let src = &kept_source_row.1;
            let new_row = add_row_offset(data_start_row, i, "유류비 기존행 재배치")?;
            let mut row_obj = original_rows
                .get(&old_row)
                .cloned()
                .unwrap_or_else(|| self.default_row(old_row));
            let old_row_value = old_row;
            let resolver = |old_ref_row: u32| {
                if old_ref_row == old_row_value {
                    new_row
                } else {
                    row_mapper.map(old_ref_row)
                }
            };
            remap_row_numbers(&mut row_obj, new_row, &resolver);
            new_rows_map.insert(new_row, row_obj);
            kept_rows.push(KeptMasterRow {
                new_row,
                src: src.clone(),
            });
        }
        Ok(kept_rows)
    }
    fn place_new_source_rows(
        &self,
        plan: NewSourcePlacementPlan<'_>,
    ) -> Result<Vec<(u32, SourceRecord)>> {
        let NewSourcePlacementPlan {
            data_start_row,
            kept_count,
            new_rows_map,
            new_sources,
            row_mapper,
            template_row,
            template_row_num,
        } = plan;
        let mut new_rows_from_sources = Vec::with_capacity(new_sources.len());
        for (i, src) in new_sources.iter().cloned().enumerate() {
            let offset = kept_count
                .checked_add(i)
                .ok_or_else(|| err("유류비 신규행 오프셋 계산 중 overflow가 발생했습니다."))?;
            let new_row = add_row_offset(data_start_row, offset, "유류비 신규행 추가")?;
            let mut row_obj = template_row.clone();
            let resolver = |old_ref_row: u32| {
                if old_ref_row == template_row_num {
                    new_row
                } else {
                    row_mapper.map(old_ref_row)
                }
            };
            remap_row_numbers(&mut row_obj, new_row, &resolver);
            new_rows_map.insert(new_row, row_obj);
            new_rows_from_sources.push((new_row, src));
        }
        Ok(new_rows_from_sources)
    }
    fn rebuild_master_rows(
        &self,
        ws: &mut excel::writer::Worksheet,
        shared_strings: &[String],
        plan: MasterSheetPlan,
        old_rows: &[u32],
        kept_source_rows: &[(u32, Option<SourceRecord>)],
        new_sources: &[SourceRecord],
    ) -> Result<(u32, u32)> {
        let MasterSheetPlan {
            header_row,
            data_start_row,
            layout,
        } = plan;
        let old_count = old_rows.len();
        let old_end_row = old_rows
            .last()
            .copied()
            .unwrap_or_else(|| data_start_row.saturating_sub(1));
        let original_rows = mem::take(&mut ws.rows);
        let deleted_rows = self.build_deleted_rows(old_rows, kept_source_rows);
        let final_count = kept_source_rows.len().saturating_add(new_sources.len());
        let old_count_u32 = usize_to_u32(old_count, "기존 유류비 행 수")?;
        let final_count_u32 = usize_to_u32(final_count, "최종 유류비 행 수")?;
        let row_mapper = RowMapper {
            has_old_rows: old_count > 0,
            data_start_row,
            old_end_row,
            deleted_rows,
            old_count_u32,
            increase: final_count_u32.saturating_sub(old_count_u32),
            decrease: old_count_u32.saturating_sub(final_count_u32),
        };
        let rebuilt = (|| -> Result<_> {
            let template_row_num = old_rows.last().copied().unwrap_or(data_start_row);
            let template_row = original_rows
                .get(&template_row_num)
                .cloned()
                .unwrap_or_else(|| self.default_row(template_row_num));
            let mut new_rows_map = self.build_rebased_non_data_rows(
                &original_rows,
                data_start_row,
                old_end_row,
                &row_mapper,
            );
            let kept_rows = self.place_kept_rows(
                &mut new_rows_map,
                &original_rows,
                kept_source_rows,
                data_start_row,
                &row_mapper,
            )?;
            let new_rows_from_sources = self.place_new_source_rows(NewSourcePlacementPlan {
                new_rows_map: &mut new_rows_map,
                template_row: &template_row,
                template_row_num,
                kept_count: kept_source_rows.len(),
                new_sources,
                data_start_row,
                row_mapper: &row_mapper,
            })?;
            Ok((new_rows_map, kept_rows, new_rows_from_sources))
        })();
        let (new_rows_map, kept_rows, new_rows_from_sources) = match rebuilt {
            Ok(values) => values,
            Err(error) => {
                ws.rows = original_rows;
                return Err(error);
            }
        };
        ws.rows = new_rows_map;
        self.write_source_rows_to_master(ws, &kept_rows, &new_rows_from_sources, layout);
        let filter_end_row = if final_count == 0 {
            data_start_row
        } else {
            data_start_row
                .checked_add(final_count_u32.saturating_sub(1))
                .ok_or_else(|| err("유류비 마지막 행 계산 중 overflow가 발생했습니다."))?
        };
        if final_count > 0 {
            self.sort_refresh_master_rows(
                ws,
                shared_strings,
                data_start_row,
                filter_end_row,
                layout,
            )?;
        }
        if let Err(error) = ws.update_auto_filter_ref(header_row, filter_end_row) {
            ws.rows = original_rows;
            return Err(error);
        }
        let filter_end_col = ws
            .rows
            .get(&header_row)
            .and_then(|row| row.cells.last_key_value().map(|(&col, _)| col))
            .unwrap_or(1)
            .max(ws.max_cell_col());
        if let Err(error) = ws.update_dimension() {
            ws.rows = original_rows;
            return Err(error);
        }
        Ok((filter_end_row, filter_end_col))
    }
    fn refresh_rank_formula_caches(
        &self,
        ws: &mut excel::writer::Worksheet,
        shared_strings: &[String],
        data_start_row: u32,
        data_end_row: u32,
        layout: MasterSheetLayout,
    ) {
        let display_total_qty = self
            .get_f64_at(ws, 2, 10, shared_strings)
            .filter(|value| !self.is_zero(*value));
        let sort_context = self.build_rank_sort_context(ws, shared_strings);
        ws.clear_formula_cached_values_in_range(data_start_row, data_end_row);
        let capacity_for_rank_totals_init = usize::try_from(
            data_end_row
                .saturating_sub(data_start_row)
                .saturating_add(1),
        )
        .unwrap_or(0);
        let mut rank_totals = Vec::with_capacity(capacity_for_rank_totals_init);
        for row in data_start_row..=data_end_row {
            let cache = self.build_rank_formula_cache(
                ws,
                shared_strings,
                row,
                layout,
                &sort_context,
                display_total_qty,
            );
            self.write_rank_formula_cache(ws, row, layout, &cache);
            rank_totals.push((row, cache.rank_total));
        }
        let mut visible_rank_totals: Vec<ScaledSortKey> =
            rank_totals.iter().filter_map(|entry| entry.1).collect();
        visible_rank_totals.sort_unstable();
        for (row, rank_total) in rank_totals {
            let rank_text = rank_total.map(|current| {
                let rank = visible_rank_totals
                    .partition_point(|value| *value < current)
                    .saturating_add(1);
                rank.to_string()
            });
            ws.set_formula_cached_value_at(layout.rank, row, rank_text.as_deref(), None);
        }
    }
    fn repair_rank_formulas(
        &self,
        ws: &mut excel::writer::Worksheet,
        data_start_row: u32,
        data_end_row: u32,
        layout: MasterSheetLayout,
    ) {
        let Some(sort_key_col) = layout.sort_key else {
            return;
        };
        for row in data_start_row..=data_end_row {
            let Some(formula) = ws.get_formula_at(layout.rank, row) else {
                continue;
            };
            let updated = self.rewrite_rank_formula_range(
                &formula,
                sort_key_col,
                data_start_row,
                data_end_row,
            );
            if updated != formula {
                ws.set_formula_at(layout.rank, row, &updated);
            }
        }
    }
    fn rewrite_rank_formula_range(
        &self,
        formula: &str,
        sort_key_col: u32,
        data_start_row: u32,
        data_end_row: u32,
    ) -> String {
        let sort_key_col_name = excel::writer::col_to_name(sort_key_col);
        let capacity = sort_key_col_name.len().saturating_add(2);
        let mut range_marker = String::with_capacity(capacity);
        range_marker.push('$');
        range_marker.push_str(&sort_key_col_name);
        range_marker.push('$');
        let Some(first_col_pos) = formula.find(&range_marker) else {
            return formula.to_owned();
        };
        let Some(start_digits_start) = first_col_pos.checked_add(range_marker.len()) else {
            return formula.to_owned();
        };
        let Some(start_digits_tail) = formula.get(start_digits_start..) else {
            return formula.to_owned();
        };
        let start_digits_len = start_digits_tail
            .chars()
            .take_while(char::is_ascii_digit)
            .count();
        if start_digits_len == 0 {
            return formula.to_owned();
        }
        let Some(second_col_pos) = start_digits_start
            .checked_add(start_digits_len)
            .and_then(|value| value.checked_add(1))
        else {
            return formula.to_owned();
        };
        if !formula
            .get(second_col_pos..)
            .is_some_and(|tail| tail.starts_with(&range_marker))
        {
            return formula.to_owned();
        }
        let Some(end_digits_start) = second_col_pos.checked_add(range_marker.len()) else {
            return formula.to_owned();
        };
        let Some(end_digits_tail) = formula.get(end_digits_start..) else {
            return formula.to_owned();
        };
        let end_digits_len = end_digits_tail
            .chars()
            .take_while(char::is_ascii_digit)
            .count();
        if end_digits_len == 0 {
            return formula.to_owned();
        }
        let Some(end_digits_end) = end_digits_start.checked_add(end_digits_len) else {
            return formula.to_owned();
        };
        let Some(prefix) = formula.get(..first_col_pos) else {
            return formula.to_owned();
        };
        let Some(suffix) = formula.get(end_digits_end..) else {
            return formula.to_owned();
        };
        let mut out = String::with_capacity(
            prefix
                .len()
                .saturating_add(suffix.len())
                .saturating_add(sort_key_col_name.len().saturating_mul(2))
                .saturating_add(24)
                .saturating_add(7),
        );
        out.push_str(prefix);
        out.push('$');
        out.push_str(&sort_key_col_name);
        out.push('$');
        push_display(&mut out, data_start_row);
        out.push(':');
        out.push('$');
        out.push_str(&sort_key_col_name);
        out.push('$');
        push_display(&mut out, data_end_row);
        out.push_str(suffix);
        out
    }
    fn rows_from_sources(&self, new_sources: &[SourceRecord]) -> Vec<StoreRow> {
        new_sources
            .iter()
            .map(|src| StoreRow {
                region: display_region_label_from_source(&src.region, &src.address),
                name: src.name.clone(),
                address: src.address.clone(),
                gasoline: src.gasoline,
                premium: src.premium,
                diesel: src.diesel,
            })
            .collect()
    }
    fn sort_master_rows_by_rank(
        &self,
        ws: &mut excel::writer::Worksheet,
        shared_strings: &[String],
        data_start_row: u32,
        data_end_row: u32,
        layout: MasterSheetLayout,
    ) -> Result<()> {
        if data_end_row <= data_start_row {
            return Ok(());
        }
        let sort_context = self.build_rank_sort_context(ws, shared_strings);
        let row_count = usize::try_from(
            data_end_row
                .saturating_sub(data_start_row)
                .saturating_add(1),
        )
        .unwrap_or(0);
        let mut data_rows = Vec::with_capacity(row_count);
        for row_num in data_start_row..=data_end_row {
            if !ws.rows.contains_key(&row_num) {
                return Err(missing_sort_target_row_error(row_num));
            }
            let sort_key =
                self.compute_rank_sort_key(ws, shared_strings, row_num, layout, &sort_context);
            data_rows.push((row_num, sort_key));
        }
        data_rows.sort_by(|left, right| self.compare_rank_sort_key(&left.1, &right.1));
        let mut row_mapping = HashMap::with_capacity(data_rows.len());
        for (index, data_row) in data_rows.iter().enumerate() {
            let old_row = data_row.0;
            let new_row = add_row_offset(data_start_row, index, "유류비 정렬 재배치")?;
            row_mapping.insert(old_row, new_row);
        }
        let mut detached_rows = HashMap::with_capacity(data_rows.len());
        for old_row in data_start_row..=data_end_row {
            let row = ws
                .rows
                .remove(&old_row)
                .ok_or_else(|| missing_sort_target_row_error(old_row))?;
            detached_rows.insert(old_row, row);
        }
        for (old_row, _) in data_rows {
            let Some(&new_row) = row_mapping.get(&old_row) else {
                let capacity = 48;
                let mut out = String::with_capacity(capacity);
                out.push_str("정렬 후 행 매핑을 찾지 못했습니다: ");
                push_display(&mut out, old_row);
                return Err(err(out));
            };
            let mut row = detached_rows
                .remove(&old_row)
                .ok_or_else(|| missing_sort_target_row_error(old_row))?;
            remap_row_numbers(&mut row, new_row, &|old_ref_row| {
                row_mapping
                    .get(&old_ref_row)
                    .copied()
                    .unwrap_or(old_ref_row)
            });
            ws.rows.insert(new_row, row);
        }
        Ok(())
    }
    fn sort_refresh_master_rows(
        &self,
        ws: &mut excel::writer::Worksheet,
        shared_strings: &[String],
        data_start_row: u32,
        data_end_row: u32,
        layout: MasterSheetLayout,
    ) -> Result<()> {
        self.sort_master_rows_by_rank(ws, shared_strings, data_start_row, data_end_row, layout)?;
        self.repair_rank_formulas(ws, data_start_row, data_end_row, layout);
        self.refresh_rank_formula_caches(ws, shared_strings, data_start_row, data_end_row, layout);
        Ok(())
    }
    fn update_filter_database_defined_name(
        &self,
        workbook_xml: &mut String,
        data_start_row: u32,
        data_end_row: u32,
        data_end_col: u32,
    ) {
        let end_col = excel::writer::col_to_name(data_end_col.max(1));
        let mut replacement = String::with_capacity(
            "유류비!$A$:$$"
                .len()
                .saturating_add(end_col.len())
                .saturating_add(24),
        );
        replacement.push_str("유류비!$A$");
        push_display(&mut replacement, data_start_row);
        replacement.push(':');
        replacement.push_str(&end_col);
        replacement.push('$');
        push_display(&mut replacement, data_end_row);
        let marker = "_xlnm._FilterDatabase";
        let marker_attr_double_capacity = marker.len().saturating_add(7);
        let mut marker_attr_double = String::with_capacity(marker_attr_double_capacity);
        marker_attr_double.push_str("name=\"");
        marker_attr_double.push_str(marker);
        marker_attr_double.push('"');
        let marker_attr_single_capacity = marker.len().saturating_add(7);
        let mut marker_attr_single = String::with_capacity(marker_attr_single_capacity);
        marker_attr_single.push_str("name='");
        marker_attr_single.push_str(marker);
        marker_attr_single.push('\'');
        let sheet_ref_plain = "유류비!";
        let sheet_ref_quoted = "'유류비'!";
        let mut cursor = 0_usize;
        while let Some(open_rel) = workbook_xml
            .get(cursor..)
            .and_then(|tail| tail.find("<definedName"))
        {
            let open_pos = cursor.saturating_add(open_rel);
            let Some(open_end_rel) = workbook_xml.get(open_pos..).and_then(|tail| tail.find('>'))
            else {
                break;
            };
            let open_end = open_pos.saturating_add(open_end_rel);
            let Some(open_tag) = workbook_xml.get(open_pos..=open_end) else {
                break;
            };
            if !open_tag.contains(&marker_attr_double) && !open_tag.contains(&marker_attr_single) {
                cursor = open_end.saturating_add(1);
                continue;
            }
            let content_start = open_end.saturating_add(1);
            let Some(close_rel) = workbook_xml
                .get(content_start..)
                .and_then(|tail| tail.find("</definedName>"))
            else {
                break;
            };
            let content_end = content_start.saturating_add(close_rel);
            let Some(content) = workbook_xml.get(content_start..content_end) else {
                break;
            };
            if content.contains(sheet_ref_plain) || content.contains(sheet_ref_quoted) {
                workbook_xml.replace_range(content_start..content_end, &replacement);
                return;
            }
            cursor = content_end.saturating_add("</definedName>".len());
        }
    }
    fn update_master_sheet_impl(
        &self,
        book: &mut StdWorkbook,
        source_index: &HashMap<String, SourceRecord>,
    ) -> Result<(Vec<ChangeRow>, Vec<StoreRow>, Vec<StoreRow>)> {
        let (changes, added, deleted, filter_start_row, filter_end_row, filter_end_col) = book
            .with_sheet_mut("유류비", |ws, shared_strings| -> Result<_> {
                let (header_row, layout) = self.find_master_sheet_layout(ws, shared_strings)?;
                let data_start_row = header_row.saturating_add(1);
                let plan = MasterSheetPlan {
                    data_start_row,
                    header_row,
                    layout,
                };
                let old_rows =
                    self.collect_master_data_rows(ws, shared_strings, data_start_row, layout);
                let evaluation =
                    self.evaluate_master_rows(ws, shared_strings, &old_rows, source_index, layout);
                let new_sources =
                    self.collect_new_sources(source_index, &evaluation.matched_source_keys);
                let added = self.rows_from_sources(&new_sources);
                let (filter_end_row, filter_end_col) = self.rebuild_master_rows(
                    ws,
                    shared_strings,
                    plan,
                    &old_rows,
                    &evaluation.kept_source_rows,
                    &new_sources,
                )?;
                Ok((
                    evaluation.changes,
                    added,
                    evaluation.deleted,
                    data_start_row,
                    filter_end_row,
                    filter_end_col,
                ))
            })
            .ok_or_else(|| err("마스터 파일에 '유류비' 시트가 없습니다"))
            .flatten()?;
        if filter_start_row > 0 && filter_end_row > 0 && filter_end_col > 0 {
            self.update_filter_database_defined_name(
                book.workbook_xml_mut(),
                filter_start_row,
                filter_end_row,
                filter_end_col,
            );
        }
        Ok((changes, added, deleted))
    }
    fn write_master_row_from_source(
        &self,
        ws: &mut excel::writer::Worksheet,
        row: u32,
        src: &SourceRecord,
        layout: MasterSheetLayout,
    ) {
        ws.set_string_at(layout.name, row, &src.name);
        ws.set_string_at(layout.brand, row, &src.brand);
        ws.set_string_at(layout.self_yn, row, &src.self_yn);
        ws.set_string_at(layout.address, row, &src.address);
        ws.set_i32_at(layout.gasoline, row, src.gasoline);
        ws.set_i32_at(layout.premium, row, src.premium);
        ws.set_i32_at(layout.diesel, row, src.diesel);
    }
    fn write_rank_formula_cache(
        &self,
        ws: &mut excel::writer::Worksheet,
        row: u32,
        layout: MasterSheetLayout,
        cache: &RankFormulaCache,
    ) {
        if let Some(col) = layout.smart_discount {
            let smart_discount_text =
                format_scaled_value(i128::from(cache.smart_discount), i128::from(DECIMAL_SCALE));
            ws.set_formula_cached_value_at(col, row, Some(&smart_discount_text), None);
        }
        if let Some(col) = layout.adjusted_gasoline {
            let text = cache
                .adjusted_gasoline
                .map(|value| format_scaled_value(i128::from(value), i128::from(DECIMAL_SCALE)));
            ws.set_formula_cached_value_at(col, row, text.as_deref(), None);
        }
        if let Some(col) = layout.adjusted_premium {
            let text = cache
                .adjusted_premium
                .map(|value| format_scaled_value(i128::from(value), i128::from(DECIMAL_SCALE)));
            ws.set_formula_cached_value_at(col, row, text.as_deref(), None);
        }
        if let Some(col) = layout.adjusted_diesel {
            let text = cache
                .adjusted_diesel
                .map(|value| format_scaled_value(i128::from(value), i128::from(DECIMAL_SCALE)));
            ws.set_formula_cached_value_at(col, row, text.as_deref(), None);
        }
        if let Some(col) = layout.fuel_total_text {
            ws.set_formula_cached_value_at(col, row, cache.fuel_total_text.as_deref(), Some("str"));
        }
        if let Some(col) = layout.total_price {
            let text = cache
                .total_price
                .map(|value| format_scaled_value(value, DECIMAL_SCALE_SQUARED));
            ws.set_formula_cached_value_at(col, row, text.as_deref(), None);
        }
        if let Some(col) = layout.region_rate {
            let text = cache
                .region_rate
                .map(|value| format_scaled_value(i128::from(value), i128::from(DECIMAL_SCALE)));
            ws.set_formula_cached_value_at(col, row, text.as_deref(), None);
        }
        if let Some(col) = layout.region_discount {
            let text = cache
                .regional_discount
                .map(|value| format_scaled_value(value, DECIMAL_SCALE_SQUARED));
            ws.set_formula_cached_value_at(col, row, text.as_deref(), None);
        }
        if let Some(col) = layout.regional_total {
            let text = cache
                .rank_total
                .map(|value| format_scaled_value(value, DECIMAL_SCALE_SQUARED));
            ws.set_formula_cached_value_at(col, row, text.as_deref(), None);
        }
        if let Some(col) = layout.sort_key {
            let text = cache.rank_total.map_or_else(
                || "1000000000000000".to_owned(),
                |value| format_scaled_value(value, DECIMAL_SCALE_SQUARED),
            );
            ws.set_formula_cached_value_at(col, row, Some(&text), None);
        }
        if let Some(col) = layout.unit_price_with_currency {
            ws.set_formula_cached_value_at(
                col,
                row,
                cache.unit_price_with_currency.as_deref(),
                None,
            );
        }
        if let Some(col) = layout.unit_price_without_currency {
            ws.set_formula_cached_value_at(
                col,
                row,
                cache.unit_price_without_currency.as_deref(),
                None,
            );
        }
    }
    fn write_source_rows_to_master(
        &self,
        ws: &mut excel::writer::Worksheet,
        kept_rows: &[KeptMasterRow],
        new_rows_from_sources: &[(u32, SourceRecord)],
        layout: MasterSheetLayout,
    ) {
        for plan in kept_rows {
            if let Some(src) = plan.src.as_ref() {
                self.write_master_row_from_source(ws, plan.new_row, src, layout);
            }
        }
        for source_row in new_rows_from_sources {
            let new_row = source_row.0;
            let src = &source_row.1;
            self.write_master_row_from_source(ws, new_row, src, layout);
            let region_label = display_region_label_from_source(&src.region, &src.address);
            if !region_label.trim().is_empty() {
                ws.set_string_at(layout.region, new_row, &region_label);
            }
        }
    }
}
fn format_fuel_price_text(label: &str, total: ScaledSortKey) -> String {
    let rounded = total
        .checked_add(DECIMAL_SCALE_SQUARED.div_euclid(2))
        .unwrap_or(total)
        .div_euclid(DECIMAL_SCALE_SQUARED);
    let raw = rounded.to_string();
    let (sign, digits) = raw
        .strip_prefix('-')
        .map_or(("", raw.as_str()), |rest| ("-", rest));
    let groups = digits.len().saturating_sub(1).div_euclid(3);
    let mut amount = String::with_capacity(
        sign.len()
            .saturating_add(digits.len())
            .saturating_add(groups),
    );
    amount.push_str(sign);
    for (index, ch) in digits.chars().enumerate() {
        if index != 0 && digits.len().saturating_sub(index).rem_euclid(3) == 0 {
            amount.push(',');
        }
        amount.push(ch);
    }
    let capacity = label.len().saturating_add(amount.len()).saturating_add(2);
    let mut out = String::with_capacity(capacity);
    out.push_str(label);
    out.push(' ');
    out.push_str(&amount);
    out.push('원');
    out
}
fn format_scaled_value(value: i128, scale: i128) -> String {
    let sign = if value < 0 { "-" } else { "" };
    let abs = value.abs();
    let whole = abs.div_euclid(scale);
    let frac = abs.rem_euclid(scale);
    if frac == 0 {
        let capacity = sign.len().saturating_add(whole.to_string().len());
        let mut out = String::with_capacity(capacity);
        out.push_str(sign);
        push_display(&mut out, whole);
        return out;
    }
    let mut frac_text = frac.to_string();
    let width = scale.to_string().len().saturating_sub(1);
    while frac_text.len() < width {
        frac_text.insert(0, '0');
    }
    while frac_text.ends_with('0') {
        frac_text.pop();
    }
    let capacity = sign
        .len()
        .saturating_add(whole.to_string().len())
        .saturating_add(frac_text.len())
        .saturating_add(1);
    let mut out = String::with_capacity(capacity);
    out.push_str(sign);
    push_display(&mut out, whole);
    out.push('.');
    out.push_str(&frac_text);
    out
}
fn format_unit_price_text(total: ScaledSortKey, qty: ScaledDecimal) -> Option<String> {
    if qty == 0 {
        return None;
    }
    let denominator = i128::from(qty).checked_mul(i128::from(DECIMAL_SCALE))?;
    let sign = if total < 0 { "-" } else { "" };
    let abs = total.abs();
    let whole = abs.div_euclid(denominator);
    let mut remainder = abs.rem_euclid(denominator);
    if remainder == 0 {
        let capacity = sign.len().saturating_add(whole.to_string().len());
        let mut out = String::with_capacity(capacity);
        out.push_str(sign);
        push_display(&mut out, whole);
        return Some(out);
    }
    let mut frac_text = String::with_capacity(15);
    while frac_text.len() < 15 && remainder != 0 {
        remainder = remainder.checked_mul(10)?;
        let digit = remainder.div_euclid(denominator);
        let digit_u8 = u8::try_from(digit).ok()?;
        frac_text.push(char::from(b'0'.saturating_add(digit_u8)));
        remainder = remainder.rem_euclid(denominator);
    }
    while frac_text.ends_with('0') {
        frac_text.pop();
    }
    let capacity = sign
        .len()
        .saturating_add(whole.to_string().len())
        .saturating_add(frac_text.len())
        .saturating_add(1);
    let mut out = String::with_capacity(capacity);
    out.push_str(sign);
    push_display(&mut out, whole);
    out.push('.');
    out.push_str(&frac_text);
    Some(out)
}
fn missing_sort_target_row_error(row_num: u32) -> Box<dyn Error + Send + Sync> {
    let capacity = 40;
    let mut out = String::with_capacity(capacity);
    out.push_str("정렬 대상 행을 찾지 못했습니다: ");
    push_display(&mut out, row_num);
    err(out)
}
