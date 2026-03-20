use crate::excel::writer::{Row as StdRow, Workbook as StdWorkbook, remap_row_numbers};
use crate::{
    ChangeRow, Result, StoreRow, add_row_offset, canon_header, defined_name, err, excel,
    normalize_address_key, same_self_yn, same_trimmed, shift_row, source_sync::SourceRecord,
    usize_to_u32,
};
use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashMap, HashSet},
    env,
};
struct MasterRowDecision {
    src: Option<SourceRecord>,
    matched_key: Option<String>,
    change: Option<ChangeRow>,
    deleted: Option<StoreRow>,
}
#[derive(Debug, Clone, Copy)]
struct MasterSheetPlan {
    header_row: u32,
    data_start_row: u32,
    layout: MasterSheetLayout,
}
#[derive(Debug, Clone, Copy)]
struct MasterSheetLayout {
    rank: u32,
    region: u32,
    name: u32,
    brand: u32,
    self_yn: u32,
    address: u32,
    gasoline: u32,
    premium: u32,
    diesel: u32,
    currency_apply: Option<u32>,
    sort_key: Option<u32>,
}
struct RankSortContext {
    gasoline_qty: f64,
    premium_qty: f64,
    diesel_qty: f64,
    total_qty: Option<f64>,
    smart_discount: f64,
    region_rates: HashMap<String, f64>,
}
struct RankSortKey {
    has_rank_total: bool,
    rank_total: f64,
    gasoline: f64,
    premium: f64,
    diesel: f64,
    region: String,
    name: String,
    address: String,
}
fn normalize_fuel_price(value: Option<i32>) -> Option<i32> {
    value.filter(|v| *v > 0)
}
fn evaluate_master_row(
    ws: &excel::writer::Worksheet,
    shared_strings: &[String],
    old_row: u32,
    source_index: &HashMap<String, SourceRecord>,
    layout: MasterSheetLayout,
) -> MasterRowDecision {
    let region = ws
        .get_display_at(layout.region, old_row, shared_strings)
        .trim()
        .to_string();
    let name = ws
        .get_display_at(layout.name, old_row, shared_strings)
        .trim()
        .to_string();
    let addr = ws
        .get_display_at(layout.address, old_row, shared_strings)
        .trim()
        .to_string();
    if addr.is_empty() {
        return MasterRowDecision {
            src: None,
            matched_key: None,
            change: None,
            deleted: None,
        };
    }
    let key = normalize_address_key(&addr);
    let Some(src) = source_index.get(&key).cloned() else {
        return MasterRowDecision {
            src: None,
            matched_key: None,
            change: None,
            deleted: Some(StoreRow {
                region,
                name,
                address: addr,
                gasoline: normalize_fuel_price(ws.get_i32_at(
                    layout.gasoline,
                    old_row,
                    shared_strings,
                )),
                premium: normalize_fuel_price(ws.get_i32_at(
                    layout.premium,
                    old_row,
                    shared_strings,
                )),
                diesel: normalize_fuel_price(ws.get_i32_at(layout.diesel, old_row, shared_strings)),
            }),
        };
    };
    let old_brand = ws
        .get_display_at(layout.brand, old_row, shared_strings)
        .trim()
        .to_string();
    let old_self_yn = ws
        .get_display_at(layout.self_yn, old_row, shared_strings)
        .trim()
        .to_string();
    let old_gas = normalize_fuel_price(ws.get_i32_at(layout.gasoline, old_row, shared_strings));
    let old_premium = normalize_fuel_price(ws.get_i32_at(layout.premium, old_row, shared_strings));
    let old_diesel = normalize_fuel_price(ws.get_i32_at(layout.diesel, old_row, shared_strings));
    let old = ExistingMasterRow {
        region: &region,
        name: &name,
        brand: &old_brand,
        self_yn: &old_self_yn,
        gasoline: old_gas,
        premium: old_premium,
        diesel: old_diesel,
    };
    let change = build_change_row_if_needed(&old, &src);
    MasterRowDecision {
        src: Some(src),
        matched_key: Some(key),
        change,
        deleted: None,
    }
}
struct ExistingMasterRow<'a> {
    region: &'a str,
    name: &'a str,
    brand: &'a str,
    self_yn: &'a str,
    gasoline: Option<i32>,
    premium: Option<i32>,
    diesel: Option<i32>,
}
fn build_change_row_if_needed(
    old: &ExistingMasterRow<'_>,
    src: &SourceRecord,
) -> Option<ChangeRow> {
    let name_changed = !same_trimmed(old.name, &src.name);
    let brand_changed = !same_trimmed(old.brand, &src.brand);
    let self_yn_changed = !same_self_yn(old.self_yn, &src.self_yn);
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
    let mut reasons: Vec<&str> = vec![];
    if gas_changed || premium_changed || diesel_changed {
        reasons.push("가격변동");
    }
    if name_changed {
        reasons.push("상호변경");
    }
    if brand_changed {
        reasons.push("상표변경");
    }
    if self_yn_changed {
        reasons.push("셀프여부변경");
    }
    Some(ChangeRow {
        reason: reasons.join(", "),
        region: old.region.to_string(),
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
#[derive(Debug, Clone)]
struct KeptMasterRow {
    new_row: u32,
    src: Option<SourceRecord>,
}
struct MasterRowEvaluation {
    kept_source_rows: Vec<(u32, Option<SourceRecord>)>,
    matched_source_keys: HashSet<String>,
    changes: Vec<ChangeRow>,
    deleted: Vec<StoreRow>,
}
struct RowMapper {
    has_old_rows: bool,
    data_start_row: u32,
    old_end_row: u32,
    deleted_rows: Vec<u32>,
    old_count_u32: u32,
    increase: u32,
    decrease: u32,
}
impl RowMapper {
    fn map(&self, old_ref_row: u32) -> u32 {
        if self.has_old_rows
            && old_ref_row >= self.data_start_row
            && old_ref_row <= self.old_end_row
        {
            let deleted_le = u32::try_from(count_deleted_le(&self.deleted_rows, old_ref_row))
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
pub fn update_master_sheet(
    book: &mut StdWorkbook,
    source_index: &HashMap<String, SourceRecord>,
) -> Result<(Vec<ChangeRow>, Vec<StoreRow>, Vec<StoreRow>)> {
    let (changes, added, deleted, filter_start_row, filter_end_row, filter_end_col) = book
        .with_sheet_mut("유류비", |ws, shared_strings| -> Result<_> {
            let (header_row, layout) = find_master_sheet_layout(ws, shared_strings)?;
            let data_start_row = header_row + 1;
            let plan = MasterSheetPlan {
                header_row,
                data_start_row,
                layout,
            };
            let old_rows = collect_master_data_rows(ws, shared_strings, data_start_row, layout);
            let evaluation =
                evaluate_master_rows(ws, shared_strings, &old_rows, source_index, layout);
            let new_sources = collect_new_sources(source_index, &evaluation.matched_source_keys);
            let added = rows_from_sources(&new_sources);
            let (filter_end_row, filter_end_col) = rebuild_master_rows(
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
        defined_name::update_filter_database_defined_name(
            book.workbook_xml_mut(),
            filter_start_row,
            filter_end_row,
            filter_end_col,
        );
    }
    Ok((changes, added, deleted))
}
fn evaluate_master_rows(
    ws: &excel::writer::Worksheet,
    shared_strings: &[String],
    old_rows: &[u32],
    source_index: &HashMap<String, SourceRecord>,
    layout: MasterSheetLayout,
) -> MasterRowEvaluation {
    let mut matched_source_keys = HashSet::new();
    let mut kept_source_rows = Vec::new();
    let mut changes = Vec::new();
    let mut deleted = Vec::new();
    for old_row in old_rows.iter().copied() {
        let MasterRowDecision {
            src,
            matched_key,
            change,
            deleted: deleted_row,
        } = evaluate_master_row(ws, shared_strings, old_row, source_index, layout);
        if let Some(row) = deleted_row {
            deleted.push(row);
            continue;
        }
        if let Some(key) = matched_key {
            matched_source_keys.insert(key);
        }
        if let Some(change) = change {
            changes.push(change);
        }
        kept_source_rows.push((old_row, src));
    }
    MasterRowEvaluation {
        kept_source_rows,
        matched_source_keys,
        changes,
        deleted,
    }
}
fn collect_new_sources(
    source_index: &HashMap<String, SourceRecord>,
    matched_source_keys: &HashSet<String>,
) -> Vec<SourceRecord> {
    let mut new_sources: Vec<(String, SourceRecord)> = source_index
        .iter()
        .filter(|(key, _)| !matched_source_keys.contains(*key))
        .map(|(_, rec)| {
            (
                crate::display_region_label_from_source(&rec.region, &rec.address),
                rec.clone(),
            )
        })
        .collect();
    new_sources.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.1.name.cmp(&b.1.name))
            .then_with(|| a.1.address.cmp(&b.1.address))
    });
    new_sources.into_iter().map(|(_, rec)| rec).collect()
}
fn rows_from_sources(new_sources: &[SourceRecord]) -> Vec<StoreRow> {
    new_sources
        .iter()
        .map(|src| StoreRow {
            region: crate::display_region_label_from_source(&src.region, &src.address),
            name: src.name.clone(),
            address: src.address.clone(),
            gasoline: src.gasoline,
            premium: src.premium,
            diesel: src.diesel,
        })
        .collect()
}
fn rebuild_master_rows(
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
    let original_rows = std::mem::take(&mut ws.rows);
    let deleted_rows = build_deleted_rows(old_rows, kept_source_rows);
    let final_count = kept_source_rows.len() + new_sources.len();
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
            .unwrap_or_else(|| default_row(template_row_num));
        let mut new_rows_map =
            build_rebased_non_data_rows(&original_rows, data_start_row, old_end_row, &row_mapper);
        let kept_rows = place_kept_rows(
            &mut new_rows_map,
            &original_rows,
            kept_source_rows,
            data_start_row,
            &row_mapper,
        )?;
        let new_rows_from_sources = place_new_source_rows(
            &mut new_rows_map,
            &template_row,
            template_row_num,
            kept_source_rows.len(),
            new_sources,
            data_start_row,
            &row_mapper,
        )?;
        Ok((new_rows_map, kept_rows, new_rows_from_sources))
    })();
    let (new_rows_map, kept_rows, new_rows_from_sources) = match rebuilt {
        Ok(values) => values,
        Err(e) => {
            ws.rows = original_rows;
            return Err(e);
        }
    };
    ws.rows = new_rows_map;
    write_source_rows_to_master(ws, &kept_rows, &new_rows_from_sources, layout);
    let filter_end_row = if final_count == 0 {
        data_start_row
    } else {
        data_start_row
            .checked_add(final_count_u32.saturating_sub(1))
            .ok_or_else(|| err("유류비 마지막 행 계산 중 overflow가 발생했습니다."))?
    };
    if final_count > 0 {
        sort_master_rows_by_rank(ws, shared_strings, data_start_row, filter_end_row, layout)?;
        repair_rank_formulas(ws, data_start_row, filter_end_row, layout);
    }
    ws.clear_formula_cached_values();
    let filter_end_col = ws
        .rows
        .get(&header_row)
        .and_then(|row| row.cells.keys().copied().max())
        .unwrap_or(1)
        .max(ws.max_cell_col());
    if let Err(e) = ws.update_dimension() {
        ws.rows = original_rows;
        return Err(e);
    }
    Ok((filter_end_row, filter_end_col))
}
fn build_deleted_rows(
    old_rows: &[u32],
    kept_source_rows: &[(u32, Option<SourceRecord>)],
) -> Vec<u32> {
    let kept_old_rows: HashSet<u32> = kept_source_rows.iter().map(|(r, _)| *r).collect();
    let mut deleted_rows: Vec<u32> = old_rows
        .iter()
        .copied()
        .filter(|r| !kept_old_rows.contains(r))
        .collect();
    deleted_rows.sort_unstable();
    deleted_rows
}
fn build_rebased_non_data_rows(
    original_rows: &BTreeMap<u32, StdRow>,
    data_start_row: u32,
    old_end_row: u32,
    row_mapper: &RowMapper,
) -> BTreeMap<u32, StdRow> {
    let mut new_rows_map = BTreeMap::new();
    for (r, row_obj) in original_rows {
        let row_num = *r;
        if row_mapper.has_old_rows && row_num >= data_start_row && row_num <= old_end_row {
            continue;
        }
        let mut row_obj = row_obj.clone();
        if row_num < data_start_row {
            remap_row_numbers(&mut row_obj, row_num, &|old_ref_row| {
                row_mapper.map(old_ref_row)
            });
            new_rows_map.insert(row_num, row_obj);
        } else {
            let shifted = row_mapper.shift(row_num);
            remap_row_numbers(&mut row_obj, shifted, &|old_ref_row| {
                row_mapper.map(old_ref_row)
            });
            new_rows_map.insert(shifted, row_obj);
        }
    }
    new_rows_map
}
fn place_kept_rows(
    new_rows_map: &mut BTreeMap<u32, StdRow>,
    original_rows: &BTreeMap<u32, StdRow>,
    kept_source_rows: &[(u32, Option<SourceRecord>)],
    data_start_row: u32,
    row_mapper: &RowMapper,
) -> Result<Vec<KeptMasterRow>> {
    let mut kept_rows: Vec<KeptMasterRow> = vec![];
    for (i, (old_row, src)) in kept_source_rows.iter().enumerate() {
        let new_row = add_row_offset(data_start_row, i, "유류비 기존행 재배치")?;
        let mut row_obj = original_rows
            .get(old_row)
            .cloned()
            .unwrap_or_else(|| default_row(*old_row));
        let old_row_value = *old_row;
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
    new_rows_map: &mut BTreeMap<u32, StdRow>,
    template_row: &StdRow,
    template_row_num: u32,
    kept_count: usize,
    new_sources: &[SourceRecord],
    data_start_row: u32,
    row_mapper: &RowMapper,
) -> Result<Vec<(u32, SourceRecord)>> {
    let mut new_rows_from_sources = vec![];
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
fn write_source_rows_to_master(
    ws: &mut excel::writer::Worksheet,
    kept_rows: &[KeptMasterRow],
    new_rows_from_sources: &[(u32, SourceRecord)],
    layout: MasterSheetLayout,
) {
    for plan in kept_rows {
        if let Some(src) = &plan.src {
            write_master_row_from_source(ws, plan.new_row, src, layout);
        }
    }
    for (new_row, src) in new_rows_from_sources {
        write_master_row_from_source(ws, *new_row, src, layout);
        let region_label = crate::display_region_label_from_source(&src.region, &src.address);
        if !region_label.trim().is_empty() {
            ws.set_string_at(layout.region, *new_row, &region_label);
        }
    }
}
fn sort_master_rows_by_rank(
    ws: &mut excel::writer::Worksheet,
    shared_strings: &[String],
    data_start_row: u32,
    data_end_row: u32,
    layout: MasterSheetLayout,
) -> Result<()> {
    if data_end_row <= data_start_row {
        return Ok(());
    }
    let sort_context = build_rank_sort_context(ws, shared_strings);
    let mut data_rows = Vec::new();
    for row_num in data_start_row..=data_end_row {
        if !ws.rows.contains_key(&row_num) {
            return Err(err(format!("정렬 대상 행을 찾지 못했습니다: {row_num}")));
        }
        let sort_key = compute_rank_sort_key(ws, shared_strings, row_num, layout, &sort_context);
        data_rows.push((row_num, sort_key));
    }
    data_rows.sort_by(|a, b| compare_rank_sort_key(&a.1, &b.1));
    let mut row_mapping = HashMap::with_capacity(data_rows.len());
    for (index, (old_row, _)) in data_rows.iter().enumerate() {
        let new_row = add_row_offset(data_start_row, index, "유류비 정렬 재배치")?;
        row_mapping.insert(*old_row, new_row);
    }
    let mut detached_rows = HashMap::with_capacity(data_rows.len());
    for old_row in data_start_row..=data_end_row {
        let row = ws
            .rows
            .remove(&old_row)
            .ok_or_else(|| err(format!("정렬 대상 행을 찾지 못했습니다: {old_row}")))?;
        detached_rows.insert(old_row, row);
    }
    for (old_row, _) in data_rows {
        let Some(&new_row) = row_mapping.get(&old_row) else {
            return Err(err(format!("정렬 후 행 매핑을 찾지 못했습니다: {old_row}")));
        };
        let mut row = detached_rows
            .remove(&old_row)
            .ok_or_else(|| err(format!("정렬 대상 행을 찾지 못했습니다: {old_row}")))?;
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
fn build_rank_sort_context(
    ws: &excel::writer::Worksheet,
    shared_strings: &[String],
) -> RankSortContext {
    let gasoline_qty = get_f64_at(ws, 2, 4, shared_strings).unwrap_or(0.0);
    let premium_qty = get_f64_at(ws, 2, 5, shared_strings).unwrap_or(0.0);
    let diesel_qty = get_f64_at(ws, 2, 6, shared_strings).unwrap_or(0.0);
    let mut region_rates = HashMap::new();
    for row in 4..=13 {
        let region = ws.get_display_at(3, row, shared_strings).trim().to_string();
        if region.is_empty() {
            continue;
        }
        if let Some(rate) = get_f64_at(ws, 4, row, shared_strings) {
            region_rates.insert(region, rate);
        }
    }
    let total_qty = get_f64_at(ws, 2, 10, shared_strings)
        .filter(|value| !is_zero(*value))
        .or_else(|| {
            let derived_total = gasoline_qty + premium_qty + diesel_qty;
            (!is_zero(derived_total)).then_some(derived_total)
        });
    RankSortContext {
        gasoline_qty,
        premium_qty,
        diesel_qty,
        total_qty,
        smart_discount: get_f64_at(ws, 2, 13, shared_strings).unwrap_or(0.0),
        region_rates,
    }
}
fn compute_rank_sort_key(
    ws: &excel::writer::Worksheet,
    shared_strings: &[String],
    row: u32,
    layout: MasterSheetLayout,
    sort_context: &RankSortContext,
) -> RankSortKey {
    let region = ws.get_display_at(layout.region, row, shared_strings);
    let name = ws.get_display_at(layout.name, row, shared_strings);
    let address = ws.get_display_at(layout.address, row, shared_strings);
    let gasoline = normalize_fuel_price(ws.get_i32_at(layout.gasoline, row, shared_strings));
    let premium = normalize_fuel_price(ws.get_i32_at(layout.premium, row, shared_strings));
    let diesel = normalize_fuel_price(ws.get_i32_at(layout.diesel, row, shared_strings));
    let premium_qty = sort_context.premium_qty;
    let gasoline_qty = sort_context.gasoline_qty;
    let diesel_qty = sort_context.diesel_qty;
    let is_direct_hyundai = name.contains("현대오일뱅크") && name.contains("직영");
    let discount = if is_direct_hyundai {
        sort_context.smart_discount
    } else {
        0.0
    };
    let adjusted_gasoline = gasoline.map(f64::from).map(|value| value + discount);
    let adjusted_premium = premium.map(f64::from).map(|value| value + discount);
    let adjusted_diesel = diesel.map(f64::from).map(|value| value + discount);
    let currency_apply = layout
        .currency_apply
        .map(|col| ws.get_display_at(col, row, shared_strings))
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("Y"));
    let region_rate = if currency_apply {
        sort_context
            .region_rates
            .get(region.trim())
            .copied()
            .unwrap_or(0.0)
    } else {
        0.0
    };
    let regional_adjusted_gasoline = adjusted_gasoline.map(|value| value * (1.0 - region_rate));
    let regional_adjusted_premium = adjusted_premium.map(|value| value * (1.0 - region_rate));
    let regional_adjusted_diesel = adjusted_diesel.map(|value| value * (1.0 - region_rate));
    let rank_total = sort_context.total_qty.and_then(|total_qty| {
        (!is_zero(total_qty))
            .then_some(total_qty)
            .and_then(|_| {
                compute_total_price(
                    gasoline_qty,
                    adjusted_gasoline,
                    premium_qty,
                    adjusted_premium,
                    diesel_qty,
                    adjusted_diesel,
                )
            })
            .map(|total_price| total_price - (total_price * region_rate).floor())
            .filter(|value| !is_zero(*value))
    });
    RankSortKey {
        has_rank_total: rank_total.is_some(),
        rank_total: rank_total.unwrap_or(f64::INFINITY),
        gasoline: fuel_sort_value(regional_adjusted_gasoline),
        premium: fuel_sort_value(regional_adjusted_premium),
        diesel: fuel_sort_value(regional_adjusted_diesel),
        region,
        name,
        address,
    }
}
fn compare_rank_sort_key(a: &RankSortKey, b: &RankSortKey) -> Ordering {
    b.has_rank_total
        .cmp(&a.has_rank_total)
        .then_with(|| a.rank_total.total_cmp(&b.rank_total))
        .then_with(|| {
            if !a.has_rank_total && !b.has_rank_total {
                compare_out_of_rank_fuels(a, b)
            } else {
                Ordering::Equal
            }
        })
        .then_with(|| a.region.cmp(&b.region))
        .then_with(|| a.name.cmp(&b.name))
        .then_with(|| a.address.cmp(&b.address))
}
fn compare_out_of_rank_fuels(a: &RankSortKey, b: &RankSortKey) -> Ordering {
    a.gasoline
        .total_cmp(&b.gasoline)
        .then_with(|| a.premium.total_cmp(&b.premium))
        .then_with(|| a.diesel.total_cmp(&b.diesel))
}
fn compute_total_price(
    gasoline_qty: f64,
    adjusted_gasoline: Option<f64>,
    premium_qty: f64,
    adjusted_premium: Option<f64>,
    diesel_qty: f64,
    adjusted_diesel: Option<f64>,
) -> Option<f64> {
    let mut total = 0.0;
    if gasoline_qty > 0.0 {
        total += gasoline_qty * adjusted_gasoline?;
    }
    if premium_qty > 0.0 {
        total += premium_qty * adjusted_premium?;
    }
    if diesel_qty > 0.0 {
        total += diesel_qty * adjusted_diesel?;
    }
    Some(total)
}
fn get_f64_at(
    ws: &excel::writer::Worksheet,
    col: u32,
    row: u32,
    shared_strings: &[String],
) -> Option<f64> {
    parse_f64_text(&ws.get_display_at(col, row, shared_strings))
}
fn parse_f64_text(text: &str) -> Option<f64> {
    let trimmed = text.trim();
    if trimmed.is_empty() || trimmed == "-" {
        return None;
    }
    trimmed.replace(',', "").parse::<f64>().ok()
}
fn is_zero(value: f64) -> bool {
    value.abs() <= f64::EPSILON
}
fn fuel_sort_value(value: Option<f64>) -> f64 {
    value.unwrap_or(f64::INFINITY)
}
fn master_header_scan_rows() -> u32 {
    env::var("FCUPDATER_MASTER_HEADER_SCAN_ROWS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .map_or(200, |v| v.min(20_000))
}
fn master_header_scan_cols(ws: &excel::writer::Worksheet) -> u32 {
    ws.max_cell_col().clamp(20, 200)
}
fn find_master_sheet_layout(
    ws: &excel::writer::Worksheet,
    shared_strings: &[String],
) -> Result<(u32, MasterSheetLayout)> {
    let max_cols = master_header_scan_cols(ws);
    for row in 1..=master_header_scan_rows() {
        let mut headers: HashMap<String, u32> = HashMap::new();
        for col in 1..=max_cols {
            let key = canon_header(ws.get_display_at(col, row, shared_strings).trim());
            if key.is_empty() {
                continue;
            }
            headers.entry(key).or_insert(col);
        }
        if headers.is_empty()
            || get_master_header_col_optional(&headers, &["지역화폐적용순위"]).is_none()
        {
            continue;
        }
        let layout = MasterSheetLayout {
            rank: get_master_header_col_required(
                &headers,
                &["지역화폐적용순위"],
                "지역화폐적용순위",
            )?,
            region: get_master_header_col_required(&headers, &["지역"], "지역")?,
            name: get_master_header_col_required(&headers, &["상호"], "상호")?,
            brand: get_master_header_col_required(&headers, &["상표"], "상표")?,
            self_yn: get_master_header_col_required(&headers, &["셀프여부", "셀프"], "셀프여부")?,
            address: get_master_header_col_required(&headers, &["주소"], "주소")?,
            gasoline: get_master_header_col_required(
                &headers,
                &["휘발유", "보통휘발유", "휘발유단가(원/L)", "휘발유단가"],
                "휘발유",
            )?,
            premium: get_master_header_col_required(
                &headers,
                &["고급유", "고급휘발유", "고급유단가(원/L)", "고급유단가"],
                "고급유",
            )?,
            diesel: get_master_header_col_required(
                &headers,
                &["경유", "경유단가(원/L)", "경유단가"],
                "경유",
            )?,
            currency_apply: get_master_header_col_optional(
                &headers,
                &["지역화폐적용여부", "지역화폐 적용여부"],
            ),
            sort_key: get_master_header_col_optional(&headers, &["정렬키"]),
        };
        return Ok((row, layout));
    }
    Err(err(
        "유류비 시트에서 헤더 행을 찾지 못했습니다. 필수 컬럼(지역화폐적용순위/지역/상호/상표/셀프여부/주소/휘발유/고급유/경유)을 확인하세요.",
    ))
}
fn get_master_header_col_required(
    headers: &HashMap<String, u32>,
    keys: &[&str],
    display_name: &str,
) -> Result<u32> {
    get_master_header_col_optional(headers, keys)
        .ok_or_else(|| err(format!("유류비 헤더에 '{display_name}' 컬럼이 없습니다.")))
}
fn get_master_header_col_optional(headers: &HashMap<String, u32>, keys: &[&str]) -> Option<u32> {
    for key in keys {
        let canon = canon_header(key);
        if let Some(col) = headers.get(&canon) {
            return Some(*col);
        }
    }
    None
}
fn collect_master_data_rows(
    ws: &excel::writer::Worksheet,
    shared_strings: &[String],
    data_start_row: u32,
    layout: MasterSheetLayout,
) -> Vec<u32> {
    let mut rows = Vec::new();
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
fn count_deleted_le(sorted_deleted_rows: &[u32], row: u32) -> usize {
    match sorted_deleted_rows.binary_search(&row) {
        Ok(idx) => idx + 1,
        Err(idx) => idx,
    }
}
fn default_row(row_num: u32) -> StdRow {
    StdRow {
        attrs: vec![("r".to_string(), row_num.to_string())],
        cells: std::collections::BTreeMap::new(),
    }
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
fn repair_rank_formulas(
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
        let updated =
            rewrite_rank_formula_range(&formula, sort_key_col, data_start_row, data_end_row);
        if updated != formula {
            ws.set_formula_at(layout.rank, row, &updated);
        }
    }
}
fn rewrite_rank_formula_range(
    formula: &str,
    sort_key_col: u32,
    data_start_row: u32,
    data_end_row: u32,
) -> String {
    let sort_key_col_name = excel::writer::col_to_name(sort_key_col);
    let range_marker = format!("${sort_key_col_name}$");
    let Some(first_col_pos) = formula.find(&range_marker) else {
        return formula.to_string();
    };
    let start_digits_start = first_col_pos + range_marker.len();
    let start_digits_len = formula[start_digits_start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .count();
    if start_digits_len == 0 {
        return formula.to_string();
    }
    let second_col_pos = start_digits_start + start_digits_len + 1;
    if !formula
        .get(second_col_pos..)
        .is_some_and(|tail| tail.starts_with(&range_marker))
    {
        return formula.to_string();
    }
    let end_digits_start = second_col_pos + range_marker.len();
    let end_digits_len = formula[end_digits_start..]
        .chars()
        .take_while(char::is_ascii_digit)
        .count();
    if end_digits_len == 0 {
        return formula.to_string();
    }
    let end_digits_end = end_digits_start + end_digits_len;
    format!(
        "{}${}${}:$${}${}{}",
        &formula[..first_col_pos],
        sort_key_col_name,
        data_start_row,
        sort_key_col_name,
        data_end_row,
        &formula[end_digits_end..]
    )
    .replace("$$", "$")
}
