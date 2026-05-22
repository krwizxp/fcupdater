use self::filter::FilterDatabaseDefinedNameUpdater;
use self::format::{
    format_fuel_price_text, format_scaled_value, format_unit_price_text,
    missing_sort_target_row_error, split_negative_prefix,
};
use crate::{
    ChangeRow, Result, SourceRecord, StoreRow, add_row_offset, canon_header, err, err_with_source,
    excel,
    excel::writer::{Row as StdRow, Workbook as StdWorkbook, remap_row_numbers},
    normalize_address_key, parse_region_label, same_trimmed, shift_row, usize_to_u32,
};
use alloc::collections::BTreeMap;
use core::mem;
use std::collections::{HashMap, HashSet};
mod filter;
mod format;
const MASTER_HEADER_SCAN_ROWS: u32 = 200;
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
struct ChangeRowBuilder<'row, 'source> {
    old: &'row ExistingMasterRow<'row>,
    src: &'source SourceRecord,
}
struct DeletedRowsBuilder<'old, 'kept> {
    kept_source_rows: &'kept [(u32, Option<SourceRecord>)],
    old_rows: &'old [u32],
}
struct MasterDataRowsCollector<'sheet, 'strings> {
    data_start_row: u32,
    layout: MasterSheetLayout,
    shared_strings: &'strings [String],
    ws: &'sheet excel::writer::Worksheet,
}
struct MasterHeaderResolver<'headers> {
    headers: &'headers HashMap<String, u32>,
}
struct MasterSheetLayoutFinder<'sheet, 'strings> {
    shared_strings: &'strings [String],
    ws: &'sheet excel::writer::Worksheet,
}
struct MasterRowEvaluator<'sheet, 'strings, 'source> {
    layout: MasterSheetLayout,
    old_row: u32,
    shared_strings: &'strings [String],
    source_index: &'source HashMap<String, SourceRecord>,
    ws: &'sheet excel::writer::Worksheet,
}
struct MasterRowsRebuilder<'sheet, 'strings, 'old, 'sources, 'source> {
    kept_source_rows: &'source [(u32, Option<SourceRecord>)],
    new_sources: &'sources [NewSourceRef<'source>],
    old_rows: &'old [u32],
    plan: MasterSheetPlan,
    shared_strings: &'strings [String],
    ws: &'sheet mut excel::writer::Worksheet,
}
struct RankFormulaCacheBuilder<'sheet, 'strings, 'context> {
    display_total_qty: Option<ScaledDecimal>,
    layout: MasterSheetLayout,
    row: u32,
    shared_strings: &'strings [String],
    sort_context: &'context RankSortContext,
    ws: &'sheet excel::writer::Worksheet,
}
struct RankFormulaCacheWriter<'sheet, 'cache> {
    cache: &'cache RankFormulaCache,
    layout: MasterSheetLayout,
    row: u32,
    ws: &'sheet mut excel::writer::Worksheet,
}
struct RankFormulaRangeRewriter<'formula> {
    data_end_row: u32,
    data_start_row: u32,
    formula: &'formula str,
    sort_key_col: u32,
}
struct RankFormulaRefresher<'sheet, 'strings> {
    data_end_row: u32,
    data_start_row: u32,
    layout: MasterSheetLayout,
    shared_strings: &'strings [String],
    ws: &'sheet mut excel::writer::Worksheet,
}
struct RankRowsSorter<'sheet, 'strings> {
    data_end_row: u32,
    data_start_row: u32,
    layout: MasterSheetLayout,
    shared_strings: &'strings [String],
    ws: &'sheet mut excel::writer::Worksheet,
}
struct RankSortKeyBuilder<'sheet, 'strings, 'context> {
    layout: MasterSheetLayout,
    row: u32,
    shared_strings: &'strings [String],
    sort_context: &'context RankSortContext,
    ws: &'sheet excel::writer::Worksheet,
}
struct RankSortRefresher<'sheet, 'strings> {
    data_end_row: u32,
    data_start_row: u32,
    layout: MasterSheetLayout,
    shared_strings: &'strings [String],
    ws: &'sheet mut excel::writer::Worksheet,
}
struct RebasedNonDataRowsBuilder<'rows, 'mapper> {
    data_start_row: u32,
    old_end_row: u32,
    original_rows: &'rows BTreeMap<u32, StdRow>,
    row_mapper: &'mapper RowMapper,
}
struct SourceRowsWriter<'sheet, 'kept, 'new_rows, 'rows, 'source> {
    kept_rows: &'kept [KeptMasterRow<'source>],
    layout: MasterSheetLayout,
    new_rows_from_sources: &'new_rows [NewSourcePlacement<'rows, 'source>],
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
#[derive(Clone, Copy)]
struct AdjustedFuelPrices {
    diesel: Option<ScaledDecimal>,
    gasoline: Option<ScaledDecimal>,
    premium: Option<ScaledDecimal>,
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
#[derive(Debug, Clone, Copy)]
struct KeptMasterRow<'source> {
    new_row: u32,
    src: Option<&'source SourceRecord>,
}
struct NewSourceRef<'source> {
    display_region: String,
    record: &'source SourceRecord,
}
struct NewSourcePlacement<'rows, 'source> {
    new_row: u32,
    source: &'rows NewSourceRef<'source>,
}
type RebuiltMasterRows<'rows, 'source> = (
    BTreeMap<u32, StdRow>,
    Vec<KeptMasterRow<'source>>,
    Vec<NewSourcePlacement<'rows, 'source>>,
);
struct NewSourcePlacementPlan<'work, 'sources, 'source> {
    data_start_row: u32,
    kept_count: usize,
    new_rows_map: &'work mut BTreeMap<u32, StdRow>,
    new_sources: &'sources [NewSourceRef<'source>],
    row_mapper: &'work RowMapper,
    template_row: &'work StdRow,
    template_row_num: u32,
}
struct KeptRowsPlacer<'work, 'source> {
    data_start_row: u32,
    kept_source_rows: &'source [(u32, Option<SourceRecord>)],
    new_rows_map: &'work mut BTreeMap<u32, StdRow>,
    original_rows: &'work BTreeMap<u32, StdRow>,
    row_mapper: &'work RowMapper,
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
impl RowMapper {
    fn map(&self, old_ref_row: u32) -> u32 {
        if self.has_old_rows
            && old_ref_row >= self.data_start_row
            && old_ref_row <= self.old_end_row
        {
            let deleted_le = u32::try_from(
                self.deleted_rows
                    .partition_point(|deleted_row| *deleted_row <= old_ref_row),
            )
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
struct MasterSheetOps<'source> {
    source_index: &'source HashMap<String, SourceRecord>,
}
pub struct MasterSheetUpdate<'book, 'source> {
    pub book: &'book mut StdWorkbook,
    pub source_index: &'source HashMap<String, SourceRecord>,
}
impl MasterSheetUpdate<'_, '_> {
    pub fn apply(&mut self) -> Result<(Vec<ChangeRow>, Vec<StoreRow>, Vec<StoreRow>)> {
        MasterSheetOps {
            source_index: self.source_index,
        }
        .update_master_sheet_impl(self.book)
    }
}
impl ChangeRowBuilder<'_, '_> {
    fn build(&self) -> Option<ChangeRow> {
        let name_changed = !same_trimmed(self.old.name, &self.src.name);
        let brand_changed = !same_trimmed(self.old.brand, &self.src.brand);
        let self_yn_changed = canon_header(self.old.self_yn) != canon_header(&self.src.self_yn);
        let gas_changed = self.old.gasoline != self.src.gasoline;
        let premium_changed = self.old.premium != self.src.premium;
        let diesel_changed = self.old.diesel != self.src.diesel;
        (name_changed
            || brand_changed
            || self_yn_changed
            || gas_changed
            || premium_changed
            || diesel_changed)
            .then(|| {
                let mut reason = String::new();
                if gas_changed || premium_changed || diesel_changed {
                    push_joined_text(&mut reason, ", ", "가격변동");
                }
                if name_changed {
                    push_joined_text(&mut reason, ", ", "상호변경");
                }
                if brand_changed {
                    push_joined_text(&mut reason, ", ", "상표변경");
                }
                if self_yn_changed {
                    push_joined_text(&mut reason, ", ", "셀프여부변경");
                }
                ChangeRow {
                    reason,
                    region: self.old.region.to_owned(),
                    name: self.src.name.clone(),
                    address: self.src.address.clone(),
                    old_gasoline: self.old.gasoline,
                    new_gasoline: self.src.gasoline,
                    old_premium: self.old.premium,
                    new_premium: self.src.premium,
                    old_diesel: self.old.diesel,
                    new_diesel: self.src.diesel,
                }
            })
    }
}
impl DeletedRowsBuilder<'_, '_> {
    fn build(&self) -> Result<Vec<u32>> {
        let mut deleted_rows: Vec<u32> = Vec::new();
        deleted_rows
            .try_reserve(self.old_rows.len())
            .map_err(|source| {
                let row_count = self.old_rows.len();
                err_with_source(
                    format!("삭제 행 목록 메모리 확보 실패: {row_count} rows"),
                    source,
                )
            })?;
        let mut kept_iter = self.kept_source_rows.iter().map(|entry| entry.0).peekable();
        for row_num in self.old_rows.iter().copied() {
            while kept_iter.peek().is_some_and(|kept_row| *kept_row < row_num) {
                kept_iter.next();
            }
            if kept_iter
                .peek()
                .is_some_and(|kept_row| *kept_row == row_num)
            {
                kept_iter.next();
            } else {
                deleted_rows.push(row_num);
            }
        }
        Ok(deleted_rows)
    }
}
impl MasterDataRowsCollector<'_, '_> {
    fn collect(&self) -> Result<Vec<u32>> {
        let mut rows: Vec<u32> = Vec::new();
        rows.try_reserve_exact(self.ws.rows.len())
            .map_err(|source| {
                let row_count = self.ws.rows.len();
                err_with_source(
                    format!("마스터 데이터 행 목록 메모리 확보 실패: {row_count} rows"),
                    source,
                )
            })?;
        for row in self
            .ws
            .rows
            .range(self.data_start_row..)
            .map(|(row, _)| *row)
        {
            let region = self
                .ws
                .get_display_at(self.layout.region, row, self.shared_strings);
            let name = self
                .ws
                .get_display_at(self.layout.name, row, self.shared_strings);
            let addr = self
                .ws
                .get_display_at(self.layout.address, row, self.shared_strings);
            if region.trim().is_empty() && name.trim().is_empty() && addr.trim().is_empty() {
                continue;
            }
            rows.push(row);
        }
        Ok(rows)
    }
}
impl MasterRowEvaluator<'_, '_, '_> {
    fn evaluate(&self) -> MasterRowDecision {
        let region = self
            .ws
            .get_display_at(self.layout.region, self.old_row, self.shared_strings)
            .trim()
            .to_owned();
        let name = self
            .ws
            .get_display_at(self.layout.name, self.old_row, self.shared_strings)
            .trim()
            .to_owned();
        let addr = self
            .ws
            .get_display_at(self.layout.address, self.old_row, self.shared_strings)
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
        let Some(src) = self.source_index.get(&key) else {
            return MasterRowDecision {
                src: None,
                matched_key: None,
                change: None,
                deleted: Some(StoreRow {
                    region,
                    name,
                    address: addr,
                    gasoline: MasterSheetOps::normalize_fuel_price(self.ws.get_i32_at(
                        self.layout.gasoline,
                        self.old_row,
                        self.shared_strings,
                    )),
                    premium: MasterSheetOps::normalize_fuel_price(self.ws.get_i32_at(
                        self.layout.premium,
                        self.old_row,
                        self.shared_strings,
                    )),
                    diesel: MasterSheetOps::normalize_fuel_price(self.ws.get_i32_at(
                        self.layout.diesel,
                        self.old_row,
                        self.shared_strings,
                    )),
                }),
            };
        };
        let old_brand = self
            .ws
            .get_display_at(self.layout.brand, self.old_row, self.shared_strings)
            .trim()
            .to_owned();
        let old_self_yn = self
            .ws
            .get_display_at(self.layout.self_yn, self.old_row, self.shared_strings)
            .trim()
            .to_owned();
        let old_gas = MasterSheetOps::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.gasoline,
            self.old_row,
            self.shared_strings,
        ));
        let old_premium = MasterSheetOps::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.premium,
            self.old_row,
            self.shared_strings,
        ));
        let old_diesel = MasterSheetOps::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.diesel,
            self.old_row,
            self.shared_strings,
        ));
        let old = ExistingMasterRow {
            brand: &old_brand,
            premium: old_premium,
            diesel: old_diesel,
            gasoline: old_gas,
            name: &name,
            region: &region,
            self_yn: &old_self_yn,
        };
        let change = ChangeRowBuilder { old: &old, src }.build();
        MasterRowDecision {
            src: Some(src.clone()),
            matched_key: Some(key),
            change,
            deleted: None,
        }
    }
}
impl RebasedNonDataRowsBuilder<'_, '_> {
    fn build(&self) -> BTreeMap<u32, StdRow> {
        let mut new_rows_map = BTreeMap::new();
        for (row_ref, row_obj) in self.original_rows {
            let row_num = *row_ref;
            if self.row_mapper.has_old_rows
                && row_num >= self.data_start_row
                && row_num <= self.old_end_row
            {
                continue;
            }
            let mut cloned_row = row_obj.clone();
            if row_num < self.data_start_row {
                remap_row_numbers(&mut cloned_row, row_num, &|old_ref_row| {
                    self.row_mapper.map(old_ref_row)
                });
                new_rows_map.insert(row_num, cloned_row);
            } else {
                let shifted = self.row_mapper.shift(row_num);
                remap_row_numbers(&mut cloned_row, shifted, &|old_ref_row| {
                    self.row_mapper.map(old_ref_row)
                });
                new_rows_map.insert(shifted, cloned_row);
            }
        }
        new_rows_map
    }
}
impl<'source> KeptRowsPlacer<'_, 'source> {
    fn place(&mut self) -> Result<Vec<KeptMasterRow<'source>>> {
        let mut kept_rows: Vec<KeptMasterRow<'source>> = Vec::new();
        kept_rows
            .try_reserve_exact(self.kept_source_rows.len())
            .map_err(|source| {
                let kept_row_count = self.kept_source_rows.len();
                err_with_source(
                    format!("유지 마스터 행 메모리 확보 실패: {kept_row_count} rows"),
                    source,
                )
            })?;
        for (i, kept_source_row) in self.kept_source_rows.iter().enumerate() {
            let old_row = kept_source_row.0;
            let src = kept_source_row.1.as_ref();
            let new_row = add_row_offset(self.data_start_row, i, "유류비 기존행 재배치")?;
            let mut row_obj = self
                .original_rows
                .get(&old_row)
                .cloned()
                .unwrap_or_else(|| MasterSheetOps::default_row(old_row));
            let old_row_value = old_row;
            let resolver = |old_ref_row: u32| {
                if old_ref_row == old_row_value {
                    new_row
                } else {
                    self.row_mapper.map(old_ref_row)
                }
            };
            remap_row_numbers(&mut row_obj, new_row, &resolver);
            self.new_rows_map.insert(new_row, row_obj);
            kept_rows.push(KeptMasterRow { new_row, src });
        }
        Ok(kept_rows)
    }
}
impl<'sources, 'source> NewSourcePlacementPlan<'_, 'sources, 'source> {
    fn place(self) -> Result<Vec<NewSourcePlacement<'sources, 'source>>> {
        let NewSourcePlacementPlan {
            data_start_row,
            kept_count,
            new_rows_map,
            new_sources,
            row_mapper,
            template_row,
            template_row_num,
        } = self;
        let mut new_rows_from_sources: Vec<NewSourcePlacement<'sources, 'source>> = Vec::new();
        new_rows_from_sources
            .try_reserve_exact(new_sources.len())
            .map_err(|source| {
                let new_source_count = new_sources.len();
                err_with_source(
                    format!("신규 소스 행 메모리 확보 실패: {new_source_count} rows"),
                    source,
                )
            })?;
        for (i, source) in new_sources.iter().enumerate() {
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
            new_rows_from_sources.push(NewSourcePlacement { new_row, source });
        }
        Ok(new_rows_from_sources)
    }
}
impl SourceRowsWriter<'_, '_, '_, '_, '_> {
    fn write(&mut self) {
        for plan in self.kept_rows {
            if let Some(src) = plan.src {
                MasterSheetOps::write_master_row_from_source(
                    self.ws,
                    plan.new_row,
                    src,
                    self.layout,
                );
            }
        }
        for source_row in self.new_rows_from_sources {
            let new_row = source_row.new_row;
            let src = source_row.source.record;
            MasterSheetOps::write_master_row_from_source(self.ws, new_row, src, self.layout);
            let region_label = &source_row.source.display_region;
            if !region_label.trim().is_empty() {
                self.ws
                    .set_string_at(self.layout.region, new_row, region_label);
            }
        }
    }
}
impl RankFormulaRangeRewriter<'_> {
    fn rewrite(&self) -> String {
        let sort_key_col_name = excel::writer::col_to_name(self.sort_key_col);
        let range_marker = format!("${sort_key_col_name}$");
        let Some(first_col_pos) = self.formula.find(&range_marker) else {
            return self.formula.to_owned();
        };
        let Some(start_digits_start) = first_col_pos.checked_add(range_marker.len()) else {
            return self.formula.to_owned();
        };
        let Some(start_digits_tail) = self.formula.get(start_digits_start..) else {
            return self.formula.to_owned();
        };
        let start_digits_len = start_digits_tail
            .chars()
            .take_while(char::is_ascii_digit)
            .count();
        if start_digits_len == 0 {
            return self.formula.to_owned();
        }
        let Some(second_col_pos) = start_digits_start
            .checked_add(start_digits_len)
            .and_then(|value| value.checked_add(1))
        else {
            return self.formula.to_owned();
        };
        if !self
            .formula
            .get(second_col_pos..)
            .is_some_and(|tail| tail.starts_with(&range_marker))
        {
            return self.formula.to_owned();
        }
        let Some(end_digits_start) = second_col_pos.checked_add(range_marker.len()) else {
            return self.formula.to_owned();
        };
        let Some(end_digits_tail) = self.formula.get(end_digits_start..) else {
            return self.formula.to_owned();
        };
        let end_digits_len = end_digits_tail
            .chars()
            .take_while(char::is_ascii_digit)
            .count();
        if end_digits_len == 0 {
            return self.formula.to_owned();
        }
        let Some(end_digits_end) = end_digits_start.checked_add(end_digits_len) else {
            return self.formula.to_owned();
        };
        let Some((prefix, _range_start)) = self.formula.split_at_checked(first_col_pos) else {
            return self.formula.to_owned();
        };
        let Some((_formula_head, suffix)) = self.formula.split_at_checked(end_digits_end) else {
            return self.formula.to_owned();
        };
        let data_start_row = self.data_start_row;
        let data_end_row = self.data_end_row;
        format!(
            "{prefix}${sort_key_col_name}${data_start_row}:${sort_key_col_name}${data_end_row}{suffix}"
        )
    }
}
impl MasterHeaderResolver<'_> {
    fn layout(&self) -> Result<MasterSheetLayout> {
        Ok(MasterSheetLayout {
            address: self.required(&["주소"], "주소")?,
            adjusted_diesel: self.optional(&["조정경유단가(원/L)"]),
            adjusted_gasoline: self.optional(&["조정휘발유단가(원/L)"]),
            adjusted_premium: self.optional(&["조정고급유단가(원/L)"]),
            brand: self.required(&["상표"], "상표")?,
            self_yn: self.required(&["셀프여부", "셀프"], "셀프여부")?,
            gasoline: self.required(
                &["휘발유", "보통휘발유", "휘발유단가(원/L)", "휘발유단가"],
                "휘발유",
            )?,
            fuel_total_text: self.optional(&["유종별 총가격(원)"]),
            name: self.required(&["상호"], "상호")?,
            premium: self.required(
                &["고급유", "고급휘발유", "고급유단가(원/L)", "고급유단가"],
                "고급유",
            )?,
            rank: self.required(&["지역화폐적용순위"], "지역화폐적용순위")?,
            region: self.required(&["지역"], "지역")?,
            region_discount: self.optional(&["지역화폐적립액(원)"]),
            region_rate: self.optional(&["지역화폐적립율"]),
            regional_total: self.optional(&["지역화폐적용금액(원)"]),
            diesel: self.required(&["경유", "경유단가(원/L)", "경유단가"], "경유")?,
            currency_apply: self.optional(&["지역화폐적용여부", "지역화폐 적용여부"]),
            smart_discount: self.optional(&["스마트주유 할인(원/L)"]),
            sort_key: self.optional(&["정렬키"]),
            total_price: self.optional(&["총가격(원)"]),
            unit_price_with_currency: self.optional(&["지역화폐 적용단가(원/L)"]),
            unit_price_without_currency: self.optional(&["지역화폐 미적용 단가(원/L)"]),
        })
    }
    fn optional(&self, keys: &[&str]) -> Option<u32> {
        for key in keys {
            let canon = canon_header(key);
            if let Some(col) = self.headers.get(&canon) {
                return Some(*col);
            }
        }
        None
    }
    fn required(&self, keys: &[&str], display_name: &str) -> Result<u32> {
        self.optional(keys)
            .ok_or_else(|| err(format!("유류비 헤더에 '{display_name}' 컬럼이 없습니다.")))
    }
}
impl MasterSheetLayoutFinder<'_, '_> {
    fn collect_row_headers(&self, row: u32, max_cols: u32, headers: &mut HashMap<String, u32>) {
        headers.clear();
        for col in 1..=max_cols {
            let key = canon_header(self.ws.get_display_at(col, row, self.shared_strings).trim());
            if key.is_empty() {
                continue;
            }
            headers.entry(key).or_insert(col);
        }
    }
    fn find(&self) -> Result<(u32, MasterSheetLayout)> {
        let max_cols = self.ws.max_cell_col().clamp(20, 200);
        let header_capacity = usize::try_from(max_cols)
            .map_err(|source| err_with_source("마스터 헤더 열 수 변환 실패", source))?;
        let mut headers: HashMap<String, u32> = HashMap::new();
        headers.try_reserve(header_capacity).map_err(|source| {
            err_with_source(
                format!("마스터 헤더 맵 메모리 확보 실패: {header_capacity} entries"),
                source,
            )
        })?;
        for row in 1..=MASTER_HEADER_SCAN_ROWS {
            self.collect_row_headers(row, max_cols, &mut headers);
            let resolver = MasterHeaderResolver { headers: &headers };
            if headers.is_empty() || resolver.optional(&["지역화폐적용순위"]).is_none() {
                continue;
            }
            return Ok((row, resolver.layout()?));
        }
        Err(err(
            "유류비 시트에서 헤더 행을 찾지 못했습니다. 필수 컬럼(지역화폐적용순위/지역/상호/상표/셀프여부/주소/휘발유/고급유/경유)을 확인하세요.",
        ))
    }
}
impl<'sources, 'source> MasterRowsRebuilder<'_, '_, '_, 'sources, 'source> {
    fn filter_end_row(&self, final_count: usize, final_count_u32: u32) -> Result<u32> {
        let data_start_row = self.plan.data_start_row;
        if final_count == 0 {
            return Ok(data_start_row);
        }
        data_start_row
            .checked_add(final_count_u32.saturating_sub(1))
            .ok_or_else(|| err("유류비 마지막 행 계산 중 overflow가 발생했습니다."))
    }
    fn finish_rebuild(
        &mut self,
        header_row: u32,
        filter_end_row: u32,
        original_rows: BTreeMap<u32, StdRow>,
    ) -> Result<(u32, u32)> {
        if let Err(error) = self.ws.update_auto_filter_ref(header_row, filter_end_row) {
            self.ws.rows = original_rows;
            return Err(error);
        }
        let filter_end_col = self
            .ws
            .rows
            .get(&header_row)
            .and_then(|row| row.cells.last_key_value())
            .map_or(1, |(&col, _)| col)
            .max(self.ws.max_cell_col());
        if let Err(error) = self.ws.update_dimension() {
            self.ws.rows = original_rows;
            return Err(error);
        }
        Ok((filter_end_row, filter_end_col))
    }
    fn rebuild(&mut self) -> Result<(u32, u32)> {
        let MasterSheetPlan {
            header_row,
            data_start_row,
            layout,
        } = self.plan;
        let old_count = self.old_rows.len();
        let old_end_row = self
            .old_rows
            .last()
            .copied()
            .unwrap_or_else(|| data_start_row.saturating_sub(1));
        let original_rows = mem::take(&mut self.ws.rows);
        let final_count = self
            .kept_source_rows
            .len()
            .saturating_add(self.new_sources.len());
        let row_mapper = self.row_mapper(old_count, old_end_row, final_count)?;
        let rebuilt = self.rebuild_rows(&original_rows, &row_mapper, old_end_row);
        let (new_rows_map, kept_rows, new_rows_from_sources) = match rebuilt {
            Ok(values) => values,
            Err(error) => {
                self.ws.rows = original_rows;
                return Err(error);
            }
        };
        self.ws.rows = new_rows_map;
        SourceRowsWriter {
            kept_rows: &kept_rows,
            layout,
            new_rows_from_sources: &new_rows_from_sources,
            ws: self.ws,
        }
        .write();
        let final_count_u32 = usize_to_u32(final_count, "최종 유류비 행 수")?;
        let filter_end_row = self.filter_end_row(final_count, final_count_u32)?;
        if final_count > 0 {
            RankSortRefresher {
                data_start_row,
                data_end_row: filter_end_row,
                layout,
                shared_strings: self.shared_strings,
                ws: self.ws,
            }
            .refresh()?;
        }
        self.finish_rebuild(header_row, filter_end_row, original_rows)
    }
    fn rebuild_rows(
        &self,
        original_rows: &BTreeMap<u32, StdRow>,
        row_mapper: &RowMapper,
        old_end_row: u32,
    ) -> Result<RebuiltMasterRows<'sources, 'source>> {
        let data_start_row = self.plan.data_start_row;
        let template_row_num = self.old_rows.last().copied().unwrap_or(data_start_row);
        let template_row = original_rows
            .get(&template_row_num)
            .cloned()
            .unwrap_or_else(|| MasterSheetOps::default_row(template_row_num));
        let mut new_rows_map = RebasedNonDataRowsBuilder {
            data_start_row,
            old_end_row,
            original_rows,
            row_mapper,
        }
        .build();
        let kept_rows = KeptRowsPlacer {
            data_start_row,
            kept_source_rows: self.kept_source_rows,
            new_rows_map: &mut new_rows_map,
            original_rows,
            row_mapper,
        }
        .place()?;
        let new_rows_from_sources = NewSourcePlacementPlan {
            new_rows_map: &mut new_rows_map,
            template_row: &template_row,
            template_row_num,
            kept_count: self.kept_source_rows.len(),
            new_sources: self.new_sources,
            data_start_row,
            row_mapper,
        }
        .place()?;
        Ok((new_rows_map, kept_rows, new_rows_from_sources))
    }
    fn row_mapper(
        &self,
        old_count: usize,
        old_end_row: u32,
        final_count: usize,
    ) -> Result<RowMapper> {
        let old_count_u32 = usize_to_u32(old_count, "기존 유류비 행 수")?;
        let final_count_u32 = usize_to_u32(final_count, "최종 유류비 행 수")?;
        Ok(RowMapper {
            has_old_rows: old_count > 0,
            data_start_row: self.plan.data_start_row,
            old_end_row,
            deleted_rows: DeletedRowsBuilder {
                kept_source_rows: self.kept_source_rows,
                old_rows: self.old_rows,
            }
            .build()?,
            old_count_u32,
            increase: final_count_u32.saturating_sub(old_count_u32),
            decrease: old_count_u32.saturating_sub(final_count_u32),
        })
    }
}
impl RankFormulaCacheBuilder<'_, '_, '_> {
    fn adjusted_prices(&self, smart_discount: ScaledDecimal) -> AdjustedFuelPrices {
        let gasoline = MasterSheetOps::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.gasoline,
            self.row,
            self.shared_strings,
        ));
        let premium = MasterSheetOps::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.premium,
            self.row,
            self.shared_strings,
        ));
        let diesel = MasterSheetOps::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.diesel,
            self.row,
            self.shared_strings,
        ));
        AdjustedFuelPrices {
            gasoline: gasoline.and_then(|value| {
                i64::from(value)
                    .checked_mul(DECIMAL_SCALE)?
                    .checked_add(smart_discount)
            }),
            premium: premium.and_then(|value| {
                i64::from(value)
                    .checked_mul(DECIMAL_SCALE)?
                    .checked_add(smart_discount)
            }),
            diesel: diesel.and_then(|value| {
                i64::from(value)
                    .checked_mul(DECIMAL_SCALE)?
                    .checked_add(smart_discount)
            }),
        }
    }
    fn build(&self) -> RankFormulaCache {
        let region = self
            .ws
            .get_display_at(self.layout.region, self.row, self.shared_strings);
        let name = self
            .ws
            .get_display_at(self.layout.name, self.row, self.shared_strings);
        let smart_discount = if name.contains("현대오일뱅크") && name.contains("직영") {
            self.sort_context.smart_discount
        } else {
            0
        };
        let prices = self.adjusted_prices(smart_discount);
        let total_price = self.total_price(prices);
        let region_rate = self.region_rate(&region, total_price);
        let regional_discount = total_price.and_then(|value| {
            value
                .checked_mul(i128::from(region_rate))?
                .checked_div(DECIMAL_SCALE_CUBED)?
                .checked_mul(DECIMAL_SCALE_SQUARED)
        });
        let rank_total = total_price
            .zip(regional_discount)
            .and_then(|(total, discount)| total.checked_sub(discount));
        RankFormulaCache {
            adjusted_diesel: prices.diesel,
            adjusted_gasoline: prices.gasoline,
            adjusted_premium: prices.premium,
            fuel_total_text: self.fuel_total_text(prices),
            rank_total,
            region_rate: total_price.is_some().then_some(region_rate),
            regional_discount,
            smart_discount,
            total_price,
            unit_price_with_currency: rank_total
                .zip(self.display_total_qty)
                .and_then(|(value, qty)| format_unit_price_text(value, qty)),
            unit_price_without_currency: total_price
                .zip(self.display_total_qty)
                .and_then(|(value, qty)| format_unit_price_text(value, qty)),
        }
    }
    fn fuel_total_text(&self, prices: AdjustedFuelPrices) -> Option<String> {
        self.display_total_qty?;
        let mut parts = String::new();
        if self.sort_context.gasoline_qty > 0 {
            let total = i128::from(self.sort_context.gasoline_qty)
                .checked_mul(i128::from(prices.gasoline?))?;
            push_joined_text(&mut parts, " / ", &format_fuel_price_text("휘발유", total));
        }
        if self.sort_context.premium_qty > 0 {
            let total = i128::from(self.sort_context.premium_qty)
                .checked_mul(i128::from(prices.premium?))?;
            push_joined_text(&mut parts, " / ", &format_fuel_price_text("고급유", total));
        }
        if self.sort_context.diesel_qty > 0 {
            let total =
                i128::from(self.sort_context.diesel_qty).checked_mul(i128::from(prices.diesel?))?;
            push_joined_text(&mut parts, " / ", &format_fuel_price_text("경유", total));
        }
        Some(parts)
    }
    fn region_rate(&self, region: &str, total_price: Option<ScaledSortKey>) -> ScaledDecimal {
        let currency_apply = self
            .layout
            .currency_apply
            .map(|col| self.ws.get_display_at(col, self.row, self.shared_strings))
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("Y"));
        if currency_apply && total_price.is_some() {
            self.sort_context
                .region_rates
                .get(region.trim())
                .copied()
                .unwrap_or_default()
        } else {
            0
        }
    }
    fn total_price(&self, prices: AdjustedFuelPrices) -> Option<ScaledSortKey> {
        self.display_total_qty?;
        MasterSheetOps::compute_total_price(
            self.sort_context.gasoline_qty,
            prices.gasoline,
            self.sort_context.premium_qty,
            prices.premium,
            self.sort_context.diesel_qty,
            prices.diesel,
        )
    }
}
impl RankFormulaCacheWriter<'_, '_> {
    fn write(self) {
        let decimal_scale = i128::from(DECIMAL_SCALE);
        let smart_discount_text =
            format_scaled_value(i128::from(self.cache.smart_discount), decimal_scale);
        let adjusted_gasoline = self
            .cache
            .adjusted_gasoline
            .map(|value| format_scaled_value(i128::from(value), decimal_scale));
        let adjusted_premium = self
            .cache
            .adjusted_premium
            .map(|value| format_scaled_value(i128::from(value), decimal_scale));
        let adjusted_diesel = self
            .cache
            .adjusted_diesel
            .map(|value| format_scaled_value(i128::from(value), decimal_scale));
        let total_price = self
            .cache
            .total_price
            .map(|value| format_scaled_value(value, DECIMAL_SCALE_SQUARED));
        let region_rate = self
            .cache
            .region_rate
            .map(|value| format_scaled_value(i128::from(value), decimal_scale));
        let region_discount = self
            .cache
            .regional_discount
            .map(|value| format_scaled_value(value, DECIMAL_SCALE_SQUARED));
        let regional_total = self
            .cache
            .rank_total
            .map(|value| format_scaled_value(value, DECIMAL_SCALE_SQUARED));
        let sort_key = self.cache.rank_total.map_or_else(
            || "1000000000000000".to_owned(),
            |value| format_scaled_value(value, DECIMAL_SCALE_SQUARED),
        );
        for (formula_col, value, value_type) in [
            (
                self.layout.smart_discount,
                Some(smart_discount_text.as_str()),
                None,
            ),
            (
                self.layout.adjusted_gasoline,
                adjusted_gasoline.as_deref(),
                None,
            ),
            (
                self.layout.adjusted_premium,
                adjusted_premium.as_deref(),
                None,
            ),
            (
                self.layout.adjusted_diesel,
                adjusted_diesel.as_deref(),
                None,
            ),
            (
                self.layout.fuel_total_text,
                self.cache.fuel_total_text.as_deref(),
                Some("str"),
            ),
            (self.layout.total_price, total_price.as_deref(), None),
            (self.layout.region_rate, region_rate.as_deref(), None),
            (
                self.layout.region_discount,
                region_discount.as_deref(),
                None,
            ),
            (self.layout.regional_total, regional_total.as_deref(), None),
            (self.layout.sort_key, Some(sort_key.as_str()), None),
            (
                self.layout.unit_price_with_currency,
                self.cache.unit_price_with_currency.as_deref(),
                None,
            ),
            (
                self.layout.unit_price_without_currency,
                self.cache.unit_price_without_currency.as_deref(),
                None,
            ),
        ] {
            let Some(formula_col_num) = formula_col else {
                continue;
            };
            self.ws
                .set_formula_cached_value_at(formula_col_num, self.row, value, value_type);
        }
    }
}
impl RankFormulaRefresher<'_, '_> {
    fn collect_rank_totals(
        &mut self,
        display_total_qty: Option<ScaledDecimal>,
        sort_context: &RankSortContext,
    ) -> Result<Vec<(u32, Option<ScaledSortKey>)>> {
        let capacity = usize::try_from(
            self.data_end_row
                .saturating_sub(self.data_start_row)
                .saturating_add(1),
        )
        .unwrap_or_default();
        let mut rank_totals: Vec<(u32, Option<ScaledSortKey>)> = Vec::new();
        rank_totals.try_reserve_exact(capacity).map_err(|source| {
            err_with_source(
                format!("랭크 캐시 목록 메모리 확보 실패: {capacity} rows"),
                source,
            )
        })?;
        for row in self.data_start_row..=self.data_end_row {
            let cache = {
                let ws = &*self.ws;
                RankFormulaCacheBuilder {
                    ws,
                    shared_strings: self.shared_strings,
                    row,
                    layout: self.layout,
                    sort_context,
                    display_total_qty,
                }
                .build()
            };
            RankFormulaCacheWriter {
                ws: self.ws,
                row,
                layout: self.layout,
                cache: &cache,
            }
            .write();
            rank_totals.push((row, cache.rank_total));
        }
        Ok(rank_totals)
    }
    fn refresh_caches(&mut self) -> Result<()> {
        let display_total_qty = MasterSheetOps::get_f64_at(self.ws, 2, 10, self.shared_strings)
            .filter(|value| !MasterSheetOps::is_zero(*value));
        let sort_context = MasterSheetOps::build_rank_sort_context(self.ws, self.shared_strings)?;
        self.ws
            .clear_formula_cached_values_in_range(self.data_start_row, self.data_end_row);
        let rank_totals = self.collect_rank_totals(display_total_qty, &sort_context)?;
        let mut visible_rank_totals: Vec<ScaledSortKey> = Vec::new();
        visible_rank_totals
            .try_reserve(rank_totals.len())
            .map_err(|source| {
                let row_count = rank_totals.len();
                err_with_source(
                    format!("표시 랭크 합계 목록 메모리 확보 실패: {row_count} rows"),
                    source,
                )
            })?;
        visible_rank_totals.extend(rank_totals.iter().filter_map(|entry| entry.1));
        visible_rank_totals.sort_unstable();
        for (row, rank_total) in rank_totals {
            let rank_text = rank_total.map(|current| {
                let rank = visible_rank_totals
                    .partition_point(|value| *value < current)
                    .saturating_add(1);
                rank.to_string()
            });
            self.ws
                .set_formula_cached_value_at(self.layout.rank, row, rank_text.as_deref(), None);
        }
        Ok(())
    }
    fn repair_formulas(&mut self) {
        let Some(sort_key_col) = self.layout.sort_key else {
            return;
        };
        for row in self.data_start_row..=self.data_end_row {
            let Some(formula) = self.ws.get_formula_at(self.layout.rank, row) else {
                continue;
            };
            let updated = RankFormulaRangeRewriter {
                formula: &formula,
                sort_key_col,
                data_start_row: self.data_start_row,
                data_end_row: self.data_end_row,
            }
            .rewrite();
            if updated != formula {
                self.ws.set_formula_at(self.layout.rank, row, &updated);
            }
        }
    }
}
impl RankRowsSorter<'_, '_> {
    fn apply_sorted_rows(
        &mut self,
        data_rows: Vec<(u32, RankSortKey)>,
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
        for old_row in self.data_start_row..=self.data_end_row {
            let row = self
                .ws
                .rows
                .remove(&old_row)
                .ok_or_else(|| missing_sort_target_row_error(old_row))?;
            detached_rows.push(Some(row));
        }
        for (old_row, _) in data_rows {
            let Some(new_row) = mapped_contiguous_row(row_mapping, self.data_start_row, old_row)
            else {
                return Err(err(format!("정렬 후 행 매핑을 찾지 못했습니다: {old_row}")));
            };
            let row_offset = old_row
                .checked_sub(self.data_start_row)
                .ok_or_else(|| err(format!("정렬 분리 행 offset 계산 실패: {old_row}")))?;
            let index = usize::try_from(row_offset)
                .map_err(|source| err_with_source("정렬 분리 행 index 변환 실패", source))?;
            let mut row = detached_rows
                .get_mut(index)
                .and_then(Option::take)
                .ok_or_else(|| missing_sort_target_row_error(old_row))?;
            remap_row_numbers(&mut row, new_row, &|old_ref_row| {
                mapped_contiguous_row(row_mapping, self.data_start_row, old_ref_row)
                    .unwrap_or(old_ref_row)
            });
            self.ws.rows.insert(new_row, row);
        }
        Ok(())
    }
    fn row_mapping(&self, data_rows: &[(u32, RankSortKey)]) -> Result<Vec<Option<u32>>> {
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
        for (index, data_row) in data_rows.iter().enumerate() {
            let old_row = data_row.0;
            let new_row = add_row_offset(self.data_start_row, index, "유류비 정렬 재배치")?;
            let row_offset = old_row
                .checked_sub(self.data_start_row)
                .ok_or_else(|| err(format!("정렬 행 매핑 offset 계산 실패: {old_row}")))?;
            let mapping_index = usize::try_from(row_offset)
                .map_err(|source| err_with_source("정렬 행 매핑 index 변환 실패", source))?;
            let Some(slot) = row_mapping.get_mut(mapping_index) else {
                return Err(err(format!("정렬 행 매핑 범위를 벗어났습니다: {old_row}")));
            };
            *slot = Some(new_row);
        }
        if row_mapping.iter().any(Option::is_none) {
            let data_row_count = data_rows.len();
            return Err(err(format!(
                "정렬 행 매핑에 누락된 항목이 있습니다: {data_row_count} entries"
            )));
        }
        Ok(row_mapping)
    }
    fn sort(&mut self) -> Result<()> {
        if self.data_end_row <= self.data_start_row {
            return Ok(());
        }
        let sort_context = MasterSheetOps::build_rank_sort_context(self.ws, self.shared_strings)?;
        let data_rows = self.sorted_data_rows(&sort_context)?;
        let row_mapping = self.row_mapping(&data_rows)?;
        self.apply_sorted_rows(data_rows, &row_mapping)
    }
    fn sorted_data_rows(&self, sort_context: &RankSortContext) -> Result<Vec<(u32, RankSortKey)>> {
        let row_count = usize::try_from(
            self.data_end_row
                .saturating_sub(self.data_start_row)
                .saturating_add(1),
        )
        .unwrap_or_default();
        let mut data_rows: Vec<(u32, RankSortKey)> = Vec::new();
        data_rows.try_reserve_exact(row_count).map_err(|source| {
            err_with_source(
                format!("정렬 대상 행 메모리 확보 실패: {row_count} rows"),
                source,
            )
        })?;
        for row_num in self.data_start_row..=self.data_end_row {
            if !self.ws.rows.contains_key(&row_num) {
                return Err(missing_sort_target_row_error(row_num));
            }
            let sort_key = RankSortKeyBuilder {
                ws: self.ws,
                shared_strings: self.shared_strings,
                row: row_num,
                layout: self.layout,
                sort_context,
            }
            .build();
            data_rows.push((row_num, sort_key));
        }
        data_rows.sort_by(|left, right| {
            let left_key = &left.1;
            let right_key = &right.1;
            right_key
                .has_rank_total
                .cmp(&left_key.has_rank_total)
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
}
impl RankSortKeyBuilder<'_, '_, '_> {
    fn build(&self) -> RankSortKey {
        let region = self
            .ws
            .get_display_at(self.layout.region, self.row, self.shared_strings);
        let name = self
            .ws
            .get_display_at(self.layout.name, self.row, self.shared_strings);
        let address = self
            .ws
            .get_display_at(self.layout.address, self.row, self.shared_strings);
        let gasoline = MasterSheetOps::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.gasoline,
            self.row,
            self.shared_strings,
        ));
        let premium = MasterSheetOps::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.premium,
            self.row,
            self.shared_strings,
        ));
        let diesel = MasterSheetOps::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.diesel,
            self.row,
            self.shared_strings,
        ));
        let is_direct_hyundai = name.contains("현대오일뱅크") && name.contains("직영");
        let discount = if is_direct_hyundai {
            self.sort_context.smart_discount
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
        let currency_apply = self
            .layout
            .currency_apply
            .map(|col| self.ws.get_display_at(col, self.row, self.shared_strings))
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("Y"));
        let region_rate = if currency_apply {
            self.sort_context
                .region_rates
                .get(region.trim())
                .copied()
                .unwrap_or_default()
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
        let rank_total = self.sort_context.total_qty.and_then(|total_qty| {
            if MasterSheetOps::is_zero(total_qty) {
                None
            } else {
                MasterSheetOps::compute_total_price(
                    self.sort_context.gasoline_qty,
                    adjusted_gasoline,
                    self.sort_context.premium_qty,
                    adjusted_premium,
                    self.sort_context.diesel_qty,
                    adjusted_diesel,
                )
                .and_then(|total_price| {
                    let discount_numerator = total_price.checked_mul(i128::from(region_rate))?;
                    let discount_floor = discount_numerator.checked_div(DECIMAL_SCALE_CUBED)?;
                    let discount_value = discount_floor.checked_mul(DECIMAL_SCALE_SQUARED)?;
                    total_price.checked_sub(discount_value)
                })
                .filter(|value| *value != 0)
            }
        });
        RankSortKey {
            has_rank_total: rank_total.is_some(),
            rank_total: rank_total.unwrap_or(i128::MAX),
            gasoline: MasterSheetOps::fuel_sort_value(regional_adjusted_gasoline),
            premium: MasterSheetOps::fuel_sort_value(regional_adjusted_premium),
            diesel: MasterSheetOps::fuel_sort_value(regional_adjusted_diesel),
            region,
            name,
            address,
        }
    }
}
impl RankSortRefresher<'_, '_> {
    fn refresh(&mut self) -> Result<()> {
        RankRowsSorter {
            data_start_row: self.data_start_row,
            data_end_row: self.data_end_row,
            layout: self.layout,
            shared_strings: self.shared_strings,
            ws: self.ws,
        }
        .sort()?;
        let mut formula_refresher = RankFormulaRefresher {
            data_start_row: self.data_start_row,
            data_end_row: self.data_end_row,
            layout: self.layout,
            shared_strings: self.shared_strings,
            ws: self.ws,
        };
        formula_refresher.repair_formulas();
        formula_refresher.refresh_caches()
    }
}
impl MasterSheetOps<'_> {
    fn build_rank_sort_context(
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
    ) -> Result<RankSortContext> {
        let gasoline_qty = Self::get_f64_at(ws, 2, 4, shared_strings).unwrap_or_default();
        let premium_qty = Self::get_f64_at(ws, 2, 5, shared_strings).unwrap_or_default();
        let diesel_qty = Self::get_f64_at(ws, 2, 6, shared_strings).unwrap_or_default();
        let mut region_rates = HashMap::new();
        region_rates.try_reserve(10).map_err(|source| {
            err_with_source("지역 보정률 맵 메모리 확보 실패: 10 regions", source)
        })?;
        for row in 4..=13 {
            let region = ws.get_display_at(3, row, shared_strings).trim().to_owned();
            if region.is_empty() {
                continue;
            }
            if let Some(rate) = Self::get_f64_at(ws, 4, row, shared_strings) {
                region_rates.insert(region, rate);
            }
        }
        let total_qty = Self::get_f64_at(ws, 2, 10, shared_strings)
            .filter(|value| !Self::is_zero(*value))
            .or_else(|| {
                let derived_total = gasoline_qty
                    .checked_add(premium_qty)?
                    .checked_add(diesel_qty)?;
                (!Self::is_zero(derived_total)).then_some(derived_total)
            });
        Ok(RankSortContext {
            gasoline_qty,
            premium_qty,
            diesel_qty,
            total_qty,
            smart_discount: Self::get_f64_at(ws, 2, 13, shared_strings).unwrap_or_default(),
            region_rates,
        })
    }
    fn collect_new_sources(
        &self,
        matched_source_keys: &HashSet<String>,
    ) -> Result<Vec<NewSourceRef<'_>>> {
        let mut new_sources: Vec<NewSourceRef<'_>> = Vec::new();
        new_sources
            .try_reserve(self.source_index.len())
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
                .map(|(_key, rec)| NewSourceRef {
                    display_region: parse_region_label(&rec.region)
                        .or_else(|| parse_region_label(&rec.address))
                        .unwrap_or_else(|| rec.region.trim().to_owned()),
                    record: rec,
                }),
        );
        new_sources.sort_unstable_by(|left, right| {
            left.display_region
                .cmp(&right.display_region)
                .then_with(|| left.record.name.cmp(&right.record.name))
                .then_with(|| left.record.address.cmp(&right.record.address))
        });
        Ok(new_sources)
    }
    fn compute_total_price(
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
    fn default_row(row_num: u32) -> StdRow {
        StdRow {
            attrs: vec![("r".into(), { row_num.to_string() })],
            cells: BTreeMap::new(),
        }
    }
    fn evaluate_master_rows(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        old_rows: &[u32],
        layout: MasterSheetLayout,
    ) -> Result<MasterRowEvaluation> {
        let mut matched_source_keys: HashSet<String> = HashSet::new();
        matched_source_keys
            .try_reserve(old_rows.len())
            .map_err(|source| {
                let old_row_count = old_rows.len();
                err_with_source(
                    format!("매칭 소스 키 집합 메모리 확보 실패: {old_row_count} entries"),
                    source,
                )
            })?;
        let mut kept_source_rows: Vec<(u32, Option<SourceRecord>)> = Vec::new();
        kept_source_rows
            .try_reserve_exact(old_rows.len())
            .map_err(|source| {
                let old_row_count = old_rows.len();
                err_with_source(
                    format!("유지 행 목록 메모리 확보 실패: {old_row_count} rows"),
                    source,
                )
            })?;
        let mut changes: Vec<ChangeRow> = Vec::new();
        changes
            .try_reserve_exact(old_rows.len())
            .map_err(|source| {
                let old_row_count = old_rows.len();
                err_with_source(
                    format!("변경 행 목록 메모리 확보 실패: {old_row_count} rows"),
                    source,
                )
            })?;
        let mut deleted: Vec<StoreRow> = Vec::new();
        deleted
            .try_reserve_exact(old_rows.len())
            .map_err(|source| {
                let old_row_count = old_rows.len();
                err_with_source(
                    format!("삭제 행 목록 메모리 확보 실패: {old_row_count} rows"),
                    source,
                )
            })?;
        for old_row in old_rows.iter().copied() {
            let MasterRowDecision {
                src,
                matched_key,
                change,
                deleted: deleted_row,
            } = MasterRowEvaluator {
                layout,
                old_row,
                shared_strings,
                source_index: self.source_index,
                ws,
            }
            .evaluate();
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
        Ok(MasterRowEvaluation {
            changes,
            deleted,
            kept_source_rows,
            matched_source_keys,
        })
    }
    fn fuel_sort_value(value: Option<ScaledSortKey>) -> ScaledSortKey {
        value.unwrap_or(i128::MAX)
    }
    fn get_f64_at(
        ws: &excel::writer::Worksheet,
        col: u32,
        row: u32,
        shared_strings: &[String],
    ) -> Option<ScaledDecimal> {
        let display_text = ws.get_display_at(col, row, shared_strings);
        let trimmed = display_text.trim();
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
        let (sign, digits) = split_negative_prefix(normalized, 1_i64, -1_i64);
        let (whole_text, fraction_text) = digits.split_once('.').unwrap_or((digits, ""));
        let whole = whole_text.parse::<i64>().ok()?;
        let mut fraction = 0_i64;
        let mut fraction_digit_count = 0_u8;
        for digit_byte in fraction_text.bytes().filter(u8::is_ascii_digit).take(6) {
            let digit = i64::from(digit_byte.checked_sub(b'0')?);
            fraction = fraction.checked_mul(10)?.checked_add(digit)?;
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
    const fn is_zero(value: ScaledDecimal) -> bool {
        value == 0
    }
    fn normalize_fuel_price(value: Option<i32>) -> Option<i32> {
        value.filter(|price| *price > 0_i32)
    }
    fn update_master_sheet_impl(
        &self,
        book: &mut StdWorkbook,
    ) -> Result<(Vec<ChangeRow>, Vec<StoreRow>, Vec<StoreRow>)> {
        let (changes, added, deleted, filter_start_row, filter_end_row, filter_end_col) = book
            .with_sheet_mut("유류비", |ws, shared_strings| -> Result<_> {
                let (header_row, layout) = MasterSheetLayoutFinder { shared_strings, ws }.find()?;
                let data_start_row = header_row.saturating_add(1);
                let plan = MasterSheetPlan {
                    data_start_row,
                    header_row,
                    layout,
                };
                let old_rows = MasterDataRowsCollector {
                    data_start_row,
                    layout,
                    shared_strings,
                    ws,
                }
                .collect()?;
                let evaluation =
                    self.evaluate_master_rows(ws, shared_strings, &old_rows, layout)?;
                let new_sources = self.collect_new_sources(&evaluation.matched_source_keys)?;
                let mut added = Vec::new();
                added
                    .try_reserve_exact(new_sources.len())
                    .map_err(|source| {
                        let new_source_count = new_sources.len();
                        err_with_source(
                            format!("신규 StoreRow 목록 메모리 확보 실패: {new_source_count} rows"),
                            source,
                        )
                    })?;
                for source in &new_sources {
                    let src = source.record;
                    added.push(StoreRow {
                        region: source.display_region.clone(),
                        name: src.name.clone(),
                        address: src.address.clone(),
                        gasoline: src.gasoline,
                        premium: src.premium,
                        diesel: src.diesel,
                    });
                }
                let (filter_end_row, filter_end_col) = MasterRowsRebuilder {
                    ws,
                    shared_strings,
                    plan,
                    old_rows: &old_rows,
                    kept_source_rows: &evaluation.kept_source_rows,
                    new_sources: &new_sources,
                }
                .rebuild()?;
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
            FilterDatabaseDefinedNameUpdater {
                workbook_xml: book.workbook_xml_mut(),
                data_start_row: filter_start_row,
                data_end_row: filter_end_row,
                data_end_col: filter_end_col,
            }
            .update();
        }
        Ok((changes, added, deleted))
    }
    fn write_master_row_from_source(
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
fn push_joined_text(out: &mut String, separator: &str, value: &str) {
    if !out.is_empty() {
        out.push_str(separator);
    }
    out.push_str(value);
}
