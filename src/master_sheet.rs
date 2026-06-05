use self::format::{
    format_fuel_price_text, format_scaled_value, format_unit_price_text,
    missing_sort_target_row_error, split_negative_prefix,
};
use crate::{
    diagnostic::{Result, err, err_with_source},
    excel,
    excel::writer::{Row as StdRow, Workbook as StdWorkbook, remap_row_numbers},
    region::normalize_address_key,
    rows::{AddedStoreRow, ChangeRow, MasterSheetUpdateResult, SourceRecord, StoreRow},
    sheet_util::{add_row_offset, usize_to_u32},
};
use alloc::{borrow::Cow, collections::BTreeMap};
use core::range::RangeInclusive;
use std::collections::{HashMap, HashSet};
mod filter;
mod format;
const MASTER_HEADER_SCAN_ROWS: u32 = 200;
const REGION_LABEL_SUFFIXES: [&str; 3] = ["특별자치시", "광역시", "특별시"];
const DECIMAL_SCALE: ScaledDecimal = ScaledDecimal(1_000_000);
const DECIMAL_SCALE_SQUARED: ScaledSortKey = ScaledSortKey(1_000_000_000_000);
const DECIMAL_SCALE_CUBED: ScaledSortKey = ScaledSortKey(1_000_000_000_000_000_000);
type RowRange = RangeInclusive<u32>;
pub struct MasterSheetUpdater<'source> {
    pub source_index: &'source HashMap<String, SourceRecord>,
}
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
struct ScaledDecimal(i64);
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd)]
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
    const fn is_zero(self) -> bool {
        self.0 == 0
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
struct MasterRowDecision<'source> {
    change: Option<ChangeRow<'source>>,
    deleted: Option<StoreRow>,
    matched_key: Option<&'source str>,
    src: Option<&'source SourceRecord>,
}
struct ChangeRowBuilder<'row, 'source> {
    old: &'row ExistingMasterRow<'row>,
    src: &'source SourceRecord,
}
struct DeletedRowsBuilder<'old, 'kept, 'source> {
    kept_source_rows: &'kept [KeptSourceRow<'source>],
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
struct FilterDatabaseDefinedNameUpdater<'xml> {
    data_end_col: u32,
    data_rows: RowRange,
    workbook_xml: &'xml mut String,
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
struct MasterRowsRebuilder<'sheet, 'strings, 'old, 'kept, 'sources, 'source> {
    kept_source_rows: &'kept [KeptSourceRow<'source>],
    new_sources: &'sources [AddedStoreRow<'source>],
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
struct FormulaCacheWrite<'value> {
    col: Option<u32>,
    value: Option<&'value str>,
    value_type: Option<&'static str>,
}
struct RankFormulaRangeRewriter<'formula> {
    data_rows: RowRange,
    formula: &'formula str,
    sort_key_col: u32,
}
struct RankFormulaRefresher<'sheet, 'strings> {
    data_rows: RowRange,
    layout: MasterSheetLayout,
    shared_strings: &'strings [String],
    ws: &'sheet mut excel::writer::Worksheet,
}
struct RankTotalRow {
    row: u32,
    total: Option<ScaledSortKey>,
}
struct RankRowsSorter<'sheet, 'strings> {
    data_rows: RowRange,
    layout: MasterSheetLayout,
    shared_strings: &'strings [String],
    ws: &'sheet mut excel::writer::Worksheet,
}
struct SortableRankRow {
    key: RankSortKey,
    row: u32,
}
struct RankSortKeyBuilder<'sheet, 'strings, 'context> {
    layout: MasterSheetLayout,
    row: u32,
    shared_strings: &'strings [String],
    sort_context: &'context RankSortContext,
    ws: &'sheet excel::writer::Worksheet,
}
struct RankSortRefresher<'sheet, 'strings> {
    data_rows: RowRange,
    layout: MasterSheetLayout,
    shared_strings: &'strings [String],
    ws: &'sheet mut excel::writer::Worksheet,
}
struct RebasedNonDataRowsBuilder<'rows, 'mapper> {
    old_data_rows: RowRange,
    original_rows: &'rows mut BTreeMap<u32, StdRow>,
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
struct KeptSourceRow<'source> {
    old_row: u32,
    source: Option<&'source SourceRecord>,
}
struct NewSourcePlacement<'rows, 'source> {
    new_row: u32,
    source: &'rows AddedStoreRow<'source>,
}
struct RebuiltMasterRows<'rows, 'source> {
    kept_rows: Vec<KeptMasterRow<'source>>,
    new_rows_from_sources: Vec<NewSourcePlacement<'rows, 'source>>,
    new_rows_map: BTreeMap<u32, StdRow>,
}
struct FilterTarget {
    data_end_col: u32,
    data_rows: RowRange,
}
struct MasterSheetUpdateOutcome<'source> {
    added: Vec<AddedStoreRow<'source>>,
    changes: Vec<ChangeRow<'source>>,
    deleted: Vec<StoreRow>,
    filter_target: FilterTarget,
}
struct NewSourcePlacementPlan<'work, 'sources, 'source> {
    data_start_row: u32,
    kept_count: usize,
    new_rows_map: &'work mut BTreeMap<u32, StdRow>,
    new_sources: &'sources [AddedStoreRow<'source>],
    row_mapper: &'work RowMapper,
    template_row: &'work StdRow,
    template_row_num: u32,
}
struct KeptRowsPlacer<'work, 'kept, 'source> {
    data_start_row: u32,
    kept_source_rows: &'kept [KeptSourceRow<'source>],
    new_rows_map: &'work mut BTreeMap<u32, StdRow>,
    original_rows: &'work mut BTreeMap<u32, StdRow>,
    row_mapper: &'work RowMapper,
}
struct MasterRowEvaluation<'source> {
    changes: Vec<ChangeRow<'source>>,
    deleted: Vec<StoreRow>,
    kept_source_rows: Vec<KeptSourceRow<'source>>,
    matched_source_keys: HashSet<&'source str>,
}
struct RowMapper {
    decrease: u32,
    deleted_rows: Vec<u32>,
    increase: u32,
    old_count_u32: u32,
    old_data_rows: RowRange,
}
impl RowMapper {
    fn map(&self, old_ref_row: u32) -> Result<u32> {
        if self.old_data_rows.contains(&old_ref_row) {
            let deleted_le = u32::try_from(
                self.deleted_rows
                    .partition_point(|deleted_row| *deleted_row <= old_ref_row),
            )
            .unwrap_or(self.old_count_u32);
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
impl<'source> ChangeRowBuilder<'_, 'source> {
    fn build(&self) -> Result<Option<ChangeRow<'source>>> {
        let name_changed = !same_trimmed(self.old.name, &self.src.name);
        let brand_changed = !same_trimmed(self.old.brand, &self.src.brand);
        let self_yn_changed = canon_header(self.old.self_yn)? != canon_header(&self.src.self_yn)?;
        let gas_changed = self.old.gasoline != self.src.gasoline;
        let premium_changed = self.old.premium != self.src.premium;
        let diesel_changed = self.old.diesel != self.src.diesel;
        Ok((name_changed
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
                    name: &self.src.name,
                    address: &self.src.address,
                    old_gasoline: self.old.gasoline,
                    new_gasoline: self.src.gasoline,
                    old_premium: self.old.premium,
                    new_premium: self.src.premium,
                    old_diesel: self.old.diesel,
                    new_diesel: self.src.diesel,
                }
            }))
    }
}
impl DeletedRowsBuilder<'_, '_, '_> {
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
        let mut kept_iter = self
            .kept_source_rows
            .iter()
            .map(|entry| entry.old_row)
            .peekable();
        for row_num in self.old_rows.iter().copied() {
            while kept_iter.next_if(|kept_row| *kept_row < row_num).is_some() {}
            if kept_iter.next_if_eq(&row_num).is_none() {
                deleted_rows.push(row_num);
            }
        }
        Ok(deleted_rows)
    }
}
impl MasterDataRowsCollector<'_, '_> {
    fn collect(&self) -> Result<Vec<u32>> {
        let mut rows: Vec<u32> = Vec::new();
        rows.try_reserve_exact(self.ws.row_count())
            .map_err(|source| {
                let row_count = self.ws.row_count();
                err_with_source(
                    format!("마스터 데이터 행 목록 메모리 확보 실패: {row_count} rows"),
                    source,
                )
            })?;
        for row in self.ws.row_numbers_from(self.data_start_row) {
            let region =
                self.ws
                    .try_get_display_at(self.layout.region, row, self.shared_strings)?;
            let name = self
                .ws
                .try_get_display_at(self.layout.name, row, self.shared_strings)?;
            let addr = self
                .ws
                .try_get_display_at(self.layout.address, row, self.shared_strings)?;
            if !(region.trim().is_empty() && name.trim().is_empty() && addr.trim().is_empty()) {
                rows.push(row);
            }
        }
        Ok(rows)
    }
}
impl<'source> MasterRowEvaluator<'_, '_, 'source> {
    fn evaluate(&self) -> Result<MasterRowDecision<'source>> {
        let region_display =
            self.ws
                .try_get_display_at(self.layout.region, self.old_row, self.shared_strings)?;
        let name_display =
            self.ws
                .try_get_display_at(self.layout.name, self.old_row, self.shared_strings)?;
        let addr_display =
            self.ws
                .try_get_display_at(self.layout.address, self.old_row, self.shared_strings)?;
        let region = region_display.trim();
        let name = name_display.trim();
        let addr = addr_display.trim();
        if addr.is_empty() {
            return Ok(MasterRowDecision {
                src: None,
                matched_key: None,
                change: None,
                deleted: None,
            });
        }
        let key = normalize_address_key(addr)?;
        let Some((matched_key, src)) = self.source_index.get_key_value(&key) else {
            return Ok(MasterRowDecision {
                src: None,
                matched_key: None,
                change: None,
                deleted: Some(StoreRow {
                    region: region.to_owned(),
                    name: name.to_owned(),
                    address: addr.to_owned(),
                    gasoline: MasterSheetUpdater::normalize_fuel_price(self.ws.get_i32_at(
                        self.layout.gasoline,
                        self.old_row,
                        self.shared_strings,
                    )?),
                    premium: MasterSheetUpdater::normalize_fuel_price(self.ws.get_i32_at(
                        self.layout.premium,
                        self.old_row,
                        self.shared_strings,
                    )?),
                    diesel: MasterSheetUpdater::normalize_fuel_price(self.ws.get_i32_at(
                        self.layout.diesel,
                        self.old_row,
                        self.shared_strings,
                    )?),
                }),
            });
        };
        let old_brand_display =
            self.ws
                .try_get_display_at(self.layout.brand, self.old_row, self.shared_strings)?;
        let old_self_yn_display =
            self.ws
                .try_get_display_at(self.layout.self_yn, self.old_row, self.shared_strings)?;
        let old_gas = MasterSheetUpdater::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.gasoline,
            self.old_row,
            self.shared_strings,
        )?);
        let old_premium = MasterSheetUpdater::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.premium,
            self.old_row,
            self.shared_strings,
        )?);
        let old_diesel = MasterSheetUpdater::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.diesel,
            self.old_row,
            self.shared_strings,
        )?);
        let old = ExistingMasterRow {
            brand: old_brand_display.trim(),
            premium: old_premium,
            diesel: old_diesel,
            gasoline: old_gas,
            name,
            region,
            self_yn: old_self_yn_display.trim(),
        };
        let change = ChangeRowBuilder { old: &old, src }.build()?;
        Ok(MasterRowDecision {
            src: Some(src),
            matched_key: Some(matched_key.as_str()),
            change,
            deleted: None,
        })
    }
}
impl RebasedNonDataRowsBuilder<'_, '_> {
    fn move_rows_into(&mut self, new_rows_map: &mut BTreeMap<u32, StdRow>) -> Result<()> {
        let mut row_numbers: Vec<u32> = Vec::new();
        row_numbers
            .try_reserve_exact(self.original_rows.len())
            .map_err(|source| {
                let row_count = self.original_rows.len();
                err_with_source(
                    format!("비데이터 행 번호 목록 메모리 확보 실패: {row_count} rows"),
                    source,
                )
            })?;
        row_numbers.extend(self.original_rows.keys().copied());
        for row_num in row_numbers {
            if self.old_data_rows.contains(&row_num) {
                continue;
            }
            let mut row_obj = self
                .original_rows
                .remove(&row_num)
                .ok_or_else(|| err(format!("비데이터 원본 행을 찾지 못했습니다: {row_num}")))?;
            if row_num < self.old_data_rows.start {
                remap_row_numbers(&mut row_obj, row_num, &|old_ref_row| {
                    self.row_mapper.map(old_ref_row)
                })?;
                new_rows_map.insert(row_num, row_obj);
            } else {
                let shifted = self.row_mapper.shift(row_num)?;
                remap_row_numbers(&mut row_obj, shifted, &|old_ref_row| {
                    self.row_mapper.map(old_ref_row)
                })?;
                new_rows_map.insert(shifted, row_obj);
            }
        }
        Ok(())
    }
}
impl<'source> KeptRowsPlacer<'_, '_, 'source> {
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
            let old_row = kept_source_row.old_row;
            let src = kept_source_row.source;
            let new_row = add_row_offset(self.data_start_row, i, "유류비 기존행 재배치")?;
            let mut row_obj = self
                .original_rows
                .remove(&old_row)
                .unwrap_or_else(|| MasterSheetUpdater::default_row(old_row));
            let old_row_value = old_row;
            let resolver = |old_ref_row: u32| {
                if old_ref_row == old_row_value {
                    Ok(new_row)
                } else {
                    self.row_mapper.map(old_ref_row)
                }
            };
            remap_row_numbers(&mut row_obj, new_row, &resolver)?;
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
            let resolver = |old_ref_row: u32| {
                if old_ref_row == template_row_num {
                    Ok(new_row)
                } else {
                    row_mapper.map(old_ref_row)
                }
            };
            let row_obj = template_row.copy_for_row(new_row, &resolver)?;
            new_rows_map.insert(new_row, row_obj);
            new_rows_from_sources.push(NewSourcePlacement { new_row, source });
        }
        Ok(new_rows_from_sources)
    }
}
impl SourceRowsWriter<'_, '_, '_, '_, '_> {
    fn write(&mut self) -> Result<()> {
        for plan in self.kept_rows {
            if let Some(src) = plan.src {
                MasterSheetUpdater::write_master_row_from_source(
                    self.ws,
                    plan.new_row,
                    src,
                    self.layout,
                )?;
            }
        }
        for source_row in self.new_rows_from_sources {
            let new_row = source_row.new_row;
            let src = source_row.source.record;
            MasterSheetUpdater::write_master_row_from_source(self.ws, new_row, src, self.layout)?;
            let region_label = source_row.source.region;
            if !region_label.trim().is_empty() {
                self.ws
                    .set_string_at(self.layout.region, new_row, region_label)?;
            }
        }
        Ok(())
    }
}
impl RankFormulaRangeRewriter<'_> {
    fn rewrite(&self) -> Result<Option<String>> {
        let sort_key_col_name = excel::writer::col_to_name(self.sort_key_col)?;
        let range_marker = format!("${sort_key_col_name}$");
        let Some(first_col_pos) = self.formula.find(&range_marker) else {
            return Ok(None);
        };
        let Some(start_digits_start) = first_col_pos.checked_add(range_marker.len()) else {
            return Ok(None);
        };
        let Some(start_digits_tail) = self.formula.get(start_digits_start..) else {
            return Ok(None);
        };
        let start_digits_len = start_digits_tail
            .chars()
            .take_while(char::is_ascii_digit)
            .count();
        if start_digits_len == 0 {
            return Ok(None);
        }
        let Some(second_col_pos) = start_digits_start
            .checked_add(start_digits_len)
            .and_then(|value| value.checked_add(1))
        else {
            return Ok(None);
        };
        let Some(second_tail) = self.formula.get(second_col_pos..) else {
            return Ok(None);
        };
        let Some(end_digits_start) = second_col_pos.checked_add(range_marker.len()) else {
            return Ok(None);
        };
        let Some(end_digits_tail) = second_tail.strip_prefix(range_marker.as_str()) else {
            return Ok(None);
        };
        let end_digits_len = end_digits_tail
            .chars()
            .take_while(char::is_ascii_digit)
            .count();
        if end_digits_len == 0 {
            return Ok(None);
        }
        let Some(end_digits_end) = end_digits_start.checked_add(end_digits_len) else {
            return Ok(None);
        };
        let Some(prefix) = self.formula.get(..first_col_pos) else {
            return Ok(None);
        };
        let Some(suffix) = self.formula.get(end_digits_end..) else {
            return Ok(None);
        };
        let data_start_row = self.data_rows.start;
        let data_end_row = self.data_rows.last;
        let updated = format!(
            "{prefix}${sort_key_col_name}${data_start_row}:${sort_key_col_name}${data_end_row}{suffix}"
        );
        Ok((updated != self.formula).then_some(updated))
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
            let Ok(canon) = canon_header(key) else {
                return None;
            };
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
    fn collect_row_headers(
        &self,
        row: u32,
        max_cols: u32,
        headers: &mut HashMap<String, u32>,
    ) -> Result<()> {
        headers.clear();
        for col in 1..=max_cols {
            let display = self.ws.try_get_display_at(col, row, self.shared_strings)?;
            let key = canon_header(display.trim())?;
            if key.is_empty() {
                continue;
            }
            headers.entry(key).or_insert(col);
        }
        Ok(())
    }
    fn find(&self) -> Result<MasterSheetPlan> {
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
            self.collect_row_headers(row, max_cols, &mut headers)?;
            let resolver = MasterHeaderResolver { headers: &headers };
            if headers.is_empty() || resolver.optional(&["지역화폐적용순위"]).is_none() {
                continue;
            }
            return Ok(MasterSheetPlan {
                data_start_row: row
                    .checked_add(1)
                    .ok_or_else(|| err("마스터 데이터 시작 행 계산 중 overflow가 발생했습니다."))?,
                header_row: row,
                layout: resolver.layout()?,
            });
        }
        Err(err(
            "유류비 시트에서 헤더 행을 찾지 못했습니다. 필수 컬럼(지역화폐적용순위/지역/상호/상표/셀프여부/주소/휘발유/고급유/경유)을 확인하세요.",
        ))
    }
}
impl<'sources, 'source> MasterRowsRebuilder<'_, '_, '_, '_, 'sources, 'source> {
    fn filter_end_row(&self, final_count: usize, final_count_u32: u32) -> Result<u32> {
        let data_start_row = self.plan.data_start_row;
        if final_count == 0 {
            return Ok(data_start_row);
        }
        let row_offset = final_count_u32
            .checked_sub(1)
            .ok_or_else(|| err("유류비 마지막 행 offset 계산 중 overflow가 발생했습니다."))?;
        data_start_row
            .checked_add(row_offset)
            .ok_or_else(|| err("유류비 마지막 행 계산 중 overflow가 발생했습니다."))
    }
    fn finish_rebuild(&mut self, header_row: u32, filter_end_row: u32) -> Result<FilterTarget> {
        self.ws.update_auto_filter_ref(RowRange {
            start: header_row,
            last: filter_end_row,
        })?;
        let filter_end_col = self
            .ws
            .max_col_in_row(header_row)
            .unwrap_or(1)
            .max(self.ws.max_cell_col());
        self.ws.update_dimension()?;
        Ok(FilterTarget {
            data_end_col: filter_end_col,
            data_rows: RowRange {
                start: self.plan.data_start_row,
                last: filter_end_row,
            },
        })
    }
    fn rebuild(&mut self) -> Result<FilterTarget> {
        let MasterSheetPlan {
            header_row,
            data_start_row,
            layout,
        } = self.plan;
        let old_count = self.old_rows.len();
        let old_end_row = match self.old_rows.last().copied() {
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
        let row_mapper = self.row_mapper(old_count, old_data_rows, final_count)?;
        let rebuilt = self.rebuild_rows(original_rows, &row_mapper, old_data_rows)?;
        self.ws.replace_rows(rebuilt.new_rows_map);
        SourceRowsWriter {
            kept_rows: &rebuilt.kept_rows,
            layout,
            new_rows_from_sources: &rebuilt.new_rows_from_sources,
            ws: self.ws,
        }
        .write()?;
        let final_count_u32 = usize_to_u32(final_count, "최종 유류비 행 수")?;
        let filter_end_row = self.filter_end_row(final_count, final_count_u32)?;
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
        self.finish_rebuild(header_row, filter_end_row)
    }
    fn rebuild_rows(
        &self,
        mut original_rows: BTreeMap<u32, StdRow>,
        row_mapper: &RowMapper,
        old_data_rows: RowRange,
    ) -> Result<RebuiltMasterRows<'sources, 'source>> {
        let data_start_row = self.plan.data_start_row;
        let template_row_num = self.old_rows.last().copied().unwrap_or(data_start_row);
        let fallback_template = (!original_rows.contains_key(&template_row_num))
            .then(|| MasterSheetUpdater::default_row(template_row_num));
        let template_row = original_rows
            .get(&template_row_num)
            .or(fallback_template.as_ref())
            .ok_or_else(|| err("유류비 신규행 template row 준비에 실패했습니다."))?;
        let mut new_rows_map = BTreeMap::new();
        let new_rows_from_sources = {
            NewSourcePlacementPlan {
                new_rows_map: &mut new_rows_map,
                template_row,
                template_row_num,
                kept_count: self.kept_source_rows.len(),
                new_sources: self.new_sources,
                data_start_row,
                row_mapper,
            }
            .place()?
        };
        RebasedNonDataRowsBuilder {
            old_data_rows,
            original_rows: &mut original_rows,
            row_mapper,
        }
        .move_rows_into(&mut new_rows_map)?;
        let kept_rows = KeptRowsPlacer {
            data_start_row,
            kept_source_rows: self.kept_source_rows,
            new_rows_map: &mut new_rows_map,
            original_rows: &mut original_rows,
            row_mapper,
        }
        .place()?;
        Ok(RebuiltMasterRows {
            kept_rows,
            new_rows_from_sources,
            new_rows_map,
        })
    }
    fn row_mapper(
        &self,
        old_count: usize,
        old_data_rows: RowRange,
        final_count: usize,
    ) -> Result<RowMapper> {
        let old_count_u32 = usize_to_u32(old_count, "기존 유류비 행 수")?;
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
        Ok(RowMapper {
            old_data_rows,
            deleted_rows: DeletedRowsBuilder {
                kept_source_rows: self.kept_source_rows,
                old_rows: self.old_rows,
            }
            .build()?,
            old_count_u32,
            increase,
            decrease,
        })
    }
}
impl RankFormulaCacheBuilder<'_, '_, '_> {
    fn adjusted_prices(&self, smart_discount: ScaledDecimal) -> Result<AdjustedFuelPrices> {
        let gasoline = MasterSheetUpdater::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.gasoline,
            self.row,
            self.shared_strings,
        )?);
        let premium = MasterSheetUpdater::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.premium,
            self.row,
            self.shared_strings,
        )?);
        let diesel = MasterSheetUpdater::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.diesel,
            self.row,
            self.shared_strings,
        )?);
        Ok(AdjustedFuelPrices {
            gasoline: MasterSheetUpdater::adjusted_fuel_price(gasoline, smart_discount),
            premium: MasterSheetUpdater::adjusted_fuel_price(premium, smart_discount),
            diesel: MasterSheetUpdater::adjusted_fuel_price(diesel, smart_discount),
        })
    }
    fn build(&self) -> Result<RankFormulaCache> {
        let region =
            self.ws
                .try_get_display_at(self.layout.region, self.row, self.shared_strings)?;
        let name = self
            .ws
            .try_get_display_at(self.layout.name, self.row, self.shared_strings)?;
        let smart_discount = if name.contains("현대오일뱅크") && name.contains("직영") {
            self.sort_context.smart_discount
        } else {
            ScaledDecimal::ZERO
        };
        let prices = self.adjusted_prices(smart_discount)?;
        let total_price = self.total_price(prices);
        let region_rate = self.region_rate(&region, total_price)?;
        let regional_discount = total_price.and_then(|value| {
            value
                .checked_mul(ScaledSortKey(region_rate.as_i128()))?
                .checked_div(DECIMAL_SCALE_CUBED)?
                .checked_mul(DECIMAL_SCALE_SQUARED)
        });
        let rank_total = total_price
            .zip(regional_discount)
            .and_then(|(total, discount)| total.checked_sub(discount));
        Ok(RankFormulaCache {
            adjusted_diesel: prices.diesel,
            adjusted_gasoline: prices.gasoline,
            adjusted_premium: prices.premium,
            fuel_total_text: self.fuel_total_text(prices)?,
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
        })
    }
    fn fuel_total_text(&self, prices: AdjustedFuelPrices) -> Result<Option<String>> {
        if self.display_total_qty.is_none() {
            return Ok(None);
        }
        let mut parts = String::new();
        if !self.sort_context.gasoline_qty.is_zero() {
            let Some(gasoline) = prices.gasoline else {
                return Ok(None);
            };
            let Some(total) = self
                .sort_context
                .gasoline_qty
                .as_i128()
                .checked_mul(gasoline.as_i128())
            else {
                return Ok(None);
            };
            push_joined_text(
                &mut parts,
                " / ",
                &format_fuel_price_text("휘발유", ScaledSortKey(total))?,
            );
        }
        if !self.sort_context.premium_qty.is_zero() {
            let Some(premium) = prices.premium else {
                return Ok(None);
            };
            let Some(total) = self
                .sort_context
                .premium_qty
                .as_i128()
                .checked_mul(premium.as_i128())
            else {
                return Ok(None);
            };
            push_joined_text(
                &mut parts,
                " / ",
                &format_fuel_price_text("고급유", ScaledSortKey(total))?,
            );
        }
        if !self.sort_context.diesel_qty.is_zero() {
            let Some(diesel) = prices.diesel else {
                return Ok(None);
            };
            let Some(total) = self
                .sort_context
                .diesel_qty
                .as_i128()
                .checked_mul(diesel.as_i128())
            else {
                return Ok(None);
            };
            push_joined_text(
                &mut parts,
                " / ",
                &format_fuel_price_text("경유", ScaledSortKey(total))?,
            );
        }
        Ok(Some(parts))
    }
    fn region_rate(
        &self,
        region: &str,
        total_price: Option<ScaledSortKey>,
    ) -> Result<ScaledDecimal> {
        let currency_apply = if let Some(col) = self.layout.currency_apply {
            self.ws
                .try_get_display_at(col, self.row, self.shared_strings)?
                .trim()
                .eq_ignore_ascii_case("Y")
        } else {
            false
        };
        Ok(if currency_apply && total_price.is_some() {
            self.sort_context
                .region_rates
                .get(region.trim())
                .copied()
                .unwrap_or_default()
        } else {
            ScaledDecimal::ZERO
        })
    }
    fn total_price(&self, prices: AdjustedFuelPrices) -> Option<ScaledSortKey> {
        self.display_total_qty?;
        MasterSheetUpdater::compute_total_price(
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
    fn write(self) -> Result<()> {
        let decimal_scale = DECIMAL_SCALE.as_i128();
        let squared_scale = DECIMAL_SCALE_SQUARED.as_i128();
        let format_decimal =
            |value: ScaledDecimal| format_scaled_value(value.as_i128(), decimal_scale);
        let format_squared =
            |value: ScaledSortKey| format_scaled_value(value.as_i128(), squared_scale);
        let smart_discount_text = format_decimal(self.cache.smart_discount);
        let adjusted_gasoline = self.cache.adjusted_gasoline.map(format_decimal);
        let adjusted_premium = self.cache.adjusted_premium.map(format_decimal);
        let adjusted_diesel = self.cache.adjusted_diesel.map(format_decimal);
        let total_price = self.cache.total_price.map(format_squared);
        let region_rate = self.cache.region_rate.map(format_decimal);
        let region_discount = self.cache.regional_discount.map(format_squared);
        let regional_total = self.cache.rank_total.map(format_squared);
        let sort_key = self.cache.rank_total.map_or_else(
            || Cow::Borrowed("1000000000000000"),
            |value| Cow::Owned(format_squared(value)),
        );
        for item in [
            FormulaCacheWrite {
                col: self.layout.smart_discount,
                value: Some(smart_discount_text.as_str()),
                value_type: None,
            },
            FormulaCacheWrite {
                col: self.layout.adjusted_gasoline,
                value: adjusted_gasoline.as_deref(),
                value_type: None,
            },
            FormulaCacheWrite {
                col: self.layout.adjusted_premium,
                value: adjusted_premium.as_deref(),
                value_type: None,
            },
            FormulaCacheWrite {
                col: self.layout.adjusted_diesel,
                value: adjusted_diesel.as_deref(),
                value_type: None,
            },
            FormulaCacheWrite {
                col: self.layout.fuel_total_text,
                value: self.cache.fuel_total_text.as_deref(),
                value_type: Some("str"),
            },
            FormulaCacheWrite {
                col: self.layout.total_price,
                value: total_price.as_deref(),
                value_type: None,
            },
            FormulaCacheWrite {
                col: self.layout.region_rate,
                value: region_rate.as_deref(),
                value_type: None,
            },
            FormulaCacheWrite {
                col: self.layout.region_discount,
                value: region_discount.as_deref(),
                value_type: None,
            },
            FormulaCacheWrite {
                col: self.layout.regional_total,
                value: regional_total.as_deref(),
                value_type: None,
            },
            FormulaCacheWrite {
                col: self.layout.sort_key,
                value: Some(sort_key.as_ref()),
                value_type: None,
            },
            FormulaCacheWrite {
                col: self.layout.unit_price_with_currency,
                value: self.cache.unit_price_with_currency.as_deref(),
                value_type: None,
            },
            FormulaCacheWrite {
                col: self.layout.unit_price_without_currency,
                value: self.cache.unit_price_without_currency.as_deref(),
                value_type: None,
            },
        ] {
            let Some(formula_col_num) = item.col else {
                continue;
            };
            self.ws.set_formula_cached_value_at(
                formula_col_num,
                self.row,
                item.value,
                item.value_type,
            )?;
        }
        Ok(())
    }
}
impl RankFormulaRefresher<'_, '_> {
    fn collect_rank_totals(
        &mut self,
        display_total_qty: Option<ScaledDecimal>,
        sort_context: &RankSortContext,
    ) -> Result<Vec<RankTotalRow>> {
        let capacity = row_range_len(self.data_rows, "랭크 캐시 대상 행 수")?;
        let mut rank_totals: Vec<RankTotalRow> = Vec::new();
        rank_totals.try_reserve_exact(capacity).map_err(|source| {
            err_with_source(
                format!("랭크 캐시 목록 메모리 확보 실패: {capacity} rows"),
                source,
            )
        })?;
        for row in self.data_rows {
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
                .build()?
            };
            RankFormulaCacheWriter {
                ws: self.ws,
                row,
                layout: self.layout,
                cache: &cache,
            }
            .write()?;
            rank_totals.push(RankTotalRow {
                row,
                total: cache.rank_total,
            });
        }
        Ok(rank_totals)
    }
    fn refresh_caches(&mut self) -> Result<()> {
        let display_total_qty = MasterSheetUpdater::get_f64_at(self.ws, 2, 10, self.shared_strings)
            .map(|value| value.filter(|qty| !MasterSheetUpdater::is_zero(*qty)))?;
        let sort_context =
            MasterSheetUpdater::build_rank_sort_context(self.ws, self.shared_strings)?;
        self.ws
            .clear_formula_cached_values_in_range(self.data_rows)?;
        let rank_totals = self.collect_rank_totals(display_total_qty, &sort_context)?;
        let mut visible_rank_totals: Vec<ScaledSortKey> = Vec::new();
        let visible_rank_count = rank_totals
            .iter()
            .filter(|entry| entry.total.is_some())
            .count();
        visible_rank_totals
            .try_reserve_exact(visible_rank_count)
            .map_err(|source| {
                err_with_source(
                    format!("표시 랭크 합계 목록 메모리 확보 실패: {visible_rank_count} rows"),
                    source,
                )
            })?;
        visible_rank_totals.extend(rank_totals.iter().filter_map(|entry| entry.total));
        visible_rank_totals.sort_unstable();
        for entry in rank_totals {
            let rank_text = if let Some(current) = entry.total {
                let rank = visible_rank_totals
                    .partition_point(|value| *value < current)
                    .checked_add(1)
                    .ok_or_else(|| err("지역화폐 순위 계산 중 overflow가 발생했습니다."))?;
                Some(rank.to_string())
            } else {
                None
            };
            self.ws.set_formula_cached_value_at(
                self.layout.rank,
                entry.row,
                rank_text.as_deref(),
                None,
            )?;
        }
        Ok(())
    }
    fn repair_formulas(&mut self) -> Result<()> {
        let Some(sort_key_col) = self.layout.sort_key else {
            return Ok(());
        };
        for row in self.data_rows {
            let Some(formula) = self.ws.try_get_formula_at(self.layout.rank, row)? else {
                continue;
            };
            let rewrite_result = RankFormulaRangeRewriter {
                formula: formula.as_ref(),
                sort_key_col,
                data_rows: self.data_rows,
            }
            .rewrite()?;
            if let Some(updated) = rewrite_result {
                self.ws.set_formula_at(self.layout.rank, row, &updated)?;
            }
        }
        Ok(())
    }
}
impl RankRowsSorter<'_, '_> {
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
        for old_row in self.data_rows {
            let row = self
                .ws
                .remove_row(old_row)
                .ok_or_else(|| missing_sort_target_row_error(old_row))?;
            detached_rows.push(Some(row));
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
            remap_row_numbers(&mut row, new_row, &|old_ref_row| {
                Ok(
                    mapped_contiguous_row(row_mapping, self.data_rows.start, old_ref_row)
                        .unwrap_or(old_ref_row),
                )
            })?;
            self.ws.insert_row(new_row, row);
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
            *slot = Some(new_row);
        }
        if row_mapping.contains(&None) {
            let data_row_count = data_rows.len();
            return Err(err(format!(
                "정렬 행 매핑에 누락된 항목이 있습니다: {data_row_count} entries"
            )));
        }
        Ok(row_mapping)
    }
    fn sort(&mut self) -> Result<()> {
        if self.data_rows.start >= self.data_rows.last {
            return Ok(());
        }
        let sort_context =
            MasterSheetUpdater::build_rank_sort_context(self.ws, self.shared_strings)?;
        let data_rows = self.sorted_data_rows(&sort_context)?;
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
            let sort_key = RankSortKeyBuilder {
                ws: self.ws,
                shared_strings: self.shared_strings,
                row: row_num,
                layout: self.layout,
                sort_context,
            }
            .build()?;
            data_rows.push(SortableRankRow {
                row: row_num,
                key: sort_key,
            });
        }
        data_rows.sort_by(|left, right| {
            let left_key = &left.key;
            let right_key = &right.key;
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
    fn build(&self) -> Result<RankSortKey> {
        let region =
            self.ws
                .try_get_display_at(self.layout.region, self.row, self.shared_strings)?;
        let name = self
            .ws
            .try_get_display_at(self.layout.name, self.row, self.shared_strings)?;
        let address =
            self.ws
                .try_get_display_at(self.layout.address, self.row, self.shared_strings)?;
        let gasoline = MasterSheetUpdater::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.gasoline,
            self.row,
            self.shared_strings,
        )?);
        let premium = MasterSheetUpdater::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.premium,
            self.row,
            self.shared_strings,
        )?);
        let diesel = MasterSheetUpdater::normalize_fuel_price(self.ws.get_i32_at(
            self.layout.diesel,
            self.row,
            self.shared_strings,
        )?);
        let is_direct_hyundai = name.contains("현대오일뱅크") && name.contains("직영");
        let discount = if is_direct_hyundai {
            self.sort_context.smart_discount
        } else {
            ScaledDecimal::ZERO
        };
        let adjusted_gasoline = MasterSheetUpdater::adjusted_fuel_price(gasoline, discount);
        let adjusted_premium = MasterSheetUpdater::adjusted_fuel_price(premium, discount);
        let adjusted_diesel = MasterSheetUpdater::adjusted_fuel_price(diesel, discount);
        let currency_apply = if let Some(col) = self.layout.currency_apply {
            self.ws
                .try_get_display_at(col, self.row, self.shared_strings)?
                .trim()
                .eq_ignore_ascii_case("Y")
        } else {
            false
        };
        let region_rate = if currency_apply {
            self.sort_context
                .region_rates
                .get(region.trim())
                .copied()
                .unwrap_or_default()
        } else {
            ScaledDecimal::ZERO
        };
        let region_multiplier = DECIMAL_SCALE
            .checked_sub(region_rate)
            .ok_or_else(|| err("지역 보정률이 100%를 초과했습니다."))?;
        let regional_adjusted_gasoline = adjusted_gasoline
            .and_then(|value| value.as_i128().checked_mul(region_multiplier.as_i128()))
            .map(ScaledSortKey);
        let regional_adjusted_premium = adjusted_premium
            .and_then(|value| value.as_i128().checked_mul(region_multiplier.as_i128()))
            .map(ScaledSortKey);
        let regional_adjusted_diesel = adjusted_diesel
            .and_then(|value| value.as_i128().checked_mul(region_multiplier.as_i128()))
            .map(ScaledSortKey);
        let rank_total = self.sort_context.total_qty.and_then(|total_qty| {
            if MasterSheetUpdater::is_zero(total_qty) {
                None
            } else {
                MasterSheetUpdater::compute_total_price(
                    self.sort_context.gasoline_qty,
                    adjusted_gasoline,
                    self.sort_context.premium_qty,
                    adjusted_premium,
                    self.sort_context.diesel_qty,
                    adjusted_diesel,
                )
                .and_then(|total_price| {
                    let discount_numerator =
                        total_price.checked_mul(ScaledSortKey(region_rate.as_i128()))?;
                    let discount_floor = discount_numerator.checked_div(DECIMAL_SCALE_CUBED)?;
                    let discount_value = discount_floor.checked_mul(DECIMAL_SCALE_SQUARED)?;
                    total_price.checked_sub(discount_value)
                })
                .filter(|value| *value != ScaledSortKey::ZERO)
            }
        });
        Ok(RankSortKey {
            has_rank_total: rank_total.is_some(),
            rank_total: rank_total.unwrap_or(ScaledSortKey::MAX),
            gasoline: MasterSheetUpdater::fuel_sort_value(regional_adjusted_gasoline),
            premium: MasterSheetUpdater::fuel_sort_value(regional_adjusted_premium),
            diesel: MasterSheetUpdater::fuel_sort_value(regional_adjusted_diesel),
            region: region.into_owned(),
            name: name.into_owned(),
            address: address.into_owned(),
        })
    }
}
impl RankSortRefresher<'_, '_> {
    fn refresh(&mut self) -> Result<()> {
        RankRowsSorter {
            data_rows: self.data_rows,
            layout: self.layout,
            shared_strings: self.shared_strings,
            ws: self.ws,
        }
        .sort()?;
        let mut formula_refresher = RankFormulaRefresher {
            data_rows: self.data_rows,
            layout: self.layout,
            shared_strings: self.shared_strings,
            ws: self.ws,
        };
        formula_refresher.repair_formulas()?;
        formula_refresher.refresh_caches()
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
        let gasoline_qty = Self::get_f64_at(ws, 2, 4, shared_strings)?.unwrap_or_default();
        let premium_qty = Self::get_f64_at(ws, 2, 5, shared_strings)?.unwrap_or_default();
        let diesel_qty = Self::get_f64_at(ws, 2, 6, shared_strings)?.unwrap_or_default();
        let mut region_rates = HashMap::new();
        region_rates.try_reserve(10).map_err(|source| {
            err_with_source("지역 보정률 맵 메모리 확보 실패: 10 regions", source)
        })?;
        for row in 4..=13 {
            let region_display = ws.try_get_display_at(3, row, shared_strings)?;
            let region = region_display.trim();
            if region.is_empty() {
                continue;
            }
            if let Some(rate) = Self::get_f64_at(ws, 4, row, shared_strings)? {
                region_rates.insert(region.to_owned(), rate);
            }
        }
        let total_qty = Self::get_f64_at(ws, 2, 10, shared_strings)?
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
            smart_discount: Self::get_f64_at(ws, 2, 13, shared_strings)?.unwrap_or_default(),
            region_rates,
        })
    }
    fn collect_new_sources(
        &self,
        matched_source_keys: &HashSet<&str>,
    ) -> Result<Vec<AddedStoreRow<'source>>> {
        let mut new_sources: Vec<AddedStoreRow<'source>> = Vec::new();
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
                .map(|(_key, rec)| AddedStoreRow {
                    region: parse_region_label(&rec.region)
                        .or_else(|| parse_region_label(&rec.address))
                        .unwrap_or_else(|| rec.region.trim()),
                    record: rec,
                }),
        );
        new_sources.sort_unstable_by(|left, right| {
            left.region
                .cmp(right.region)
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
        let mut total = ScaledSortKey::ZERO;
        if !gasoline_qty.is_zero() {
            total = total.checked_add(ScaledSortKey(
                gasoline_qty
                    .as_i128()
                    .checked_mul(adjusted_gasoline?.as_i128())?,
            ))?;
        }
        if !premium_qty.is_zero() {
            total = total.checked_add(ScaledSortKey(
                premium_qty
                    .as_i128()
                    .checked_mul(adjusted_premium?.as_i128())?,
            ))?;
        }
        if !diesel_qty.is_zero() {
            total = total.checked_add(ScaledSortKey(
                diesel_qty
                    .as_i128()
                    .checked_mul(adjusted_diesel?.as_i128())?,
            ))?;
        }
        Some(total)
    }
    fn default_row(row_num: u32) -> StdRow {
        StdRow::numbered(row_num)
    }
    fn evaluate_master_rows(
        &self,
        ws: &excel::writer::Worksheet,
        shared_strings: &[String],
        old_rows: &[u32],
        layout: MasterSheetLayout,
    ) -> Result<MasterRowEvaluation<'source>> {
        let mut matched_source_keys: HashSet<&str> = HashSet::new();
        matched_source_keys
            .try_reserve(old_rows.len())
            .map_err(|source| {
                let old_row_count = old_rows.len();
                err_with_source(
                    format!("매칭 소스 키 집합 메모리 확보 실패: {old_row_count} entries"),
                    source,
                )
            })?;
        let mut kept_source_rows: Vec<KeptSourceRow<'source>> = Vec::new();
        kept_source_rows
            .try_reserve_exact(old_rows.len())
            .map_err(|source| {
                let old_row_count = old_rows.len();
                err_with_source(
                    format!("유지 행 목록 메모리 확보 실패: {old_row_count} rows"),
                    source,
                )
            })?;
        let mut changes: Vec<ChangeRow<'source>> = Vec::new();
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
            .evaluate()?;
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
            kept_source_rows.push(KeptSourceRow {
                old_row,
                source: src,
            });
        }
        Ok(MasterRowEvaluation {
            changes,
            deleted,
            kept_source_rows,
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
        let normalized_storage;
        let normalized = if trimmed.contains(',') {
            normalized_storage = trimmed.replace(',', "");
            normalized_storage.as_str()
        } else {
            trimmed
        };
        let (sign, digits) = split_negative_prefix(normalized, 1_i64, -1_i64);
        let (whole_text, fraction_text) = digits.split_once('.').unwrap_or((digits, ""));
        let Some(whole) = whole_text.parse::<i64>().ok() else {
            return Ok(None);
        };
        let mut fraction = 0_i64;
        let mut fraction_digit_count = 0_u8;
        for digit_byte in fraction_text.bytes().filter(u8::is_ascii_digit).take(6) {
            let Some(digit_raw) = digit_byte.checked_sub(b'0') else {
                return Ok(None);
            };
            let digit = i64::from(digit_raw);
            let Some(next_fraction) = fraction
                .checked_mul(10)
                .and_then(|value| value.checked_add(digit))
            else {
                return Ok(None);
            };
            fraction = next_fraction;
            let Some(next_digit_count) = fraction_digit_count.checked_add(1) else {
                return Ok(None);
            };
            fraction_digit_count = next_digit_count;
        }
        while fraction_digit_count < 6 {
            let Some(next_fraction) = fraction.checked_mul(10) else {
                return Ok(None);
            };
            fraction = next_fraction;
            let Some(next_digit_count) = fraction_digit_count.checked_add(1) else {
                return Ok(None);
            };
            fraction_digit_count = next_digit_count;
        }
        let Some(whole_scaled) = whole.checked_mul(DECIMAL_SCALE.as_i64()) else {
            return Ok(None);
        };
        let Some(combined) = whole_scaled.checked_add(fraction) else {
            return Ok(None);
        };
        Ok(combined.checked_mul(sign).map(ScaledDecimal))
    }
    const fn is_zero(value: ScaledDecimal) -> bool {
        value.is_zero()
    }
    fn normalize_fuel_price(value: Option<i32>) -> Option<i32> {
        value.filter(|price| *price > 0_i32)
    }
    pub fn update(&self, book: &mut StdWorkbook) -> Result<MasterSheetUpdateResult<'source>> {
        let Some(outcome_result) =
            book.with_sheet_mut("유류비", |ws, shared_strings| -> Result<_> {
                let plan = MasterSheetLayoutFinder { shared_strings, ws }.find()?;
                let MasterSheetPlan {
                    data_start_row,
                    layout,
                    ..
                } = plan;
                let old_rows = MasterDataRowsCollector {
                    data_start_row,
                    layout,
                    shared_strings,
                    ws,
                }
                .collect()?;
                let evaluation =
                    self.evaluate_master_rows(ws, shared_strings, &old_rows, layout)?;
                let added = self.collect_new_sources(&evaluation.matched_source_keys)?;
                let filter_target = MasterRowsRebuilder {
                    ws,
                    shared_strings,
                    plan,
                    old_rows: &old_rows,
                    kept_source_rows: &evaluation.kept_source_rows,
                    new_sources: &added,
                }
                .rebuild()?;
                Ok(MasterSheetUpdateOutcome {
                    added,
                    changes: evaluation.changes,
                    deleted: evaluation.deleted,
                    filter_target,
                })
            })
        else {
            return Err(err("마스터 파일에 '유류비' 시트가 없습니다"));
        };
        let outcome = outcome_result?;
        let FilterTarget {
            data_end_col,
            data_rows,
        } = outcome.filter_target;
        FilterDatabaseDefinedNameUpdater {
            workbook_xml: book.workbook_xml_mut(),
            data_rows,
            data_end_col,
        }
        .update()?;
        Ok(MasterSheetUpdateResult {
            added: outcome.added,
            changes: outcome.changes,
            deleted: outcome.deleted,
        })
    }
    fn write_master_row_from_source(
        ws: &mut excel::writer::Worksheet,
        row: u32,
        src: &SourceRecord,
        layout: MasterSheetLayout,
    ) -> Result<()> {
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
fn mapped_contiguous_row(
    row_mapping: &[Option<u32>],
    data_start_row: u32,
    old_row: u32,
) -> Option<u32> {
    let offset = old_row.checked_sub(data_start_row)?;
    let index = usize::try_from(offset).ok()?;
    row_mapping.get(index).copied().flatten()
}
fn canon_header(text: &str) -> Result<String> {
    let mut out = String::new();
    out.try_reserve(text.len())
        .map_err(|source| err_with_source("header 정규화 메모리 확보 실패", source))?;
    out.extend(text.chars().filter(|ch| !ch.is_whitespace()));
    Ok(out)
}
fn parse_region_label(text: &str) -> Option<&str> {
    let mut tokens = text.split_whitespace();
    let first = tokens.next()?;
    let second = tokens.next();
    if let Some(label) = REGION_LABEL_SUFFIXES
        .iter()
        .filter_map(|suffix| first.strip_suffix(suffix))
        .find(|label| !label.is_empty())
    {
        return Some(label);
    }
    if first.ends_with('도')
        || matches!(
            first,
            "충남" | "충북" | "경기" | "강원" | "전북" | "전남" | "경북" | "경남" | "제주"
        )
    {
        return second.map(|token| strip_basic_region_suffix(token).unwrap_or(token));
    }
    if matches!(
        first,
        "서울" | "부산" | "대구" | "인천" | "광주" | "대전" | "울산" | "세종"
    ) {
        return Some(first);
    }
    match strip_basic_region_suffix(first) {
        Some(label) => Some(label),
        None if second.is_none() => Some(first),
        None => None,
    }
}
fn push_joined_text(out: &mut String, separator: &str, value: &str) {
    if !out.is_empty() {
        out.push_str(separator);
    }
    out.push_str(value);
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
fn strip_basic_region_suffix(token: &str) -> Option<&str> {
    token
        .strip_suffix(['시', '군', '구'])
        .filter(|label| !label.is_empty())
}
