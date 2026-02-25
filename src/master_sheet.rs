use crate::excel::writer::{Row as StdRow, Workbook as StdWorkbook, remap_row_numbers};
use crate::{
    ChangeRow, Result, StoreRow, add_row_offset, defined_name, err, excel, normalize_address_key,
    same_phone, same_self_yn, same_trimmed, shift_row, source_sync::SourceRecord, usize_to_u32,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    env,
};
struct MasterRowDecision {
    src: Option<SourceRecord>,
    matched_key: Option<String>,
    change: Option<ChangeRow>,
    deleted: Option<StoreRow>,
}
fn evaluate_master_row(
    ws: &excel::writer::Worksheet,
    shared_strings: &[String],
    old_row: u32,
    source_index: &HashMap<String, SourceRecord>,
) -> MasterRowDecision {
    let region = ws
        .get_display_at(2, old_row, shared_strings)
        .trim()
        .to_string();
    let name = ws
        .get_display_at(3, old_row, shared_strings)
        .trim()
        .to_string();
    let addr = ws
        .get_display_at(6, old_row, shared_strings)
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
                gasoline: ws.get_i32_at(8, old_row, shared_strings),
                premium: ws.get_i32_at(9, old_row, shared_strings),
                diesel: ws.get_i32_at(11, old_row, shared_strings),
            }),
        };
    };
    let old_brand = ws
        .get_display_at(4, old_row, shared_strings)
        .trim()
        .to_string();
    let old_self_yn = ws
        .get_display_at(5, old_row, shared_strings)
        .trim()
        .to_string();
    let old_phone = ws
        .get_display_at(7, old_row, shared_strings)
        .trim()
        .to_string();
    let old_gas = ws.get_i32_at(8, old_row, shared_strings);
    let old_premium = ws.get_i32_at(9, old_row, shared_strings);
    let old_diesel = ws.get_i32_at(11, old_row, shared_strings);
    let old = ExistingMasterRow {
        region: &region,
        name: &name,
        address: &addr,
        brand: &old_brand,
        self_yn: &old_self_yn,
        phone: &old_phone,
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
    address: &'a str,
    brand: &'a str,
    self_yn: &'a str,
    phone: &'a str,
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
    let address_changed = normalize_address_key(old.address) != normalize_address_key(&src.address);
    let phone_changed = !same_phone(old.phone, &src.phone);
    let gas_changed = old.gasoline != src.gasoline;
    let premium_changed = old.premium != src.premium;
    let diesel_changed = old.diesel != src.diesel;
    if !(name_changed
        || brand_changed
        || self_yn_changed
        || address_changed
        || phone_changed
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
    if address_changed {
        reasons.push("주소변경");
    }
    if phone_changed {
        reasons.push("전화번호변경");
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
            let header_row = find_master_header_row(ws, shared_strings)?;
            let data_start_row = header_row + 1;
            let old_rows = collect_master_data_rows(ws, shared_strings, data_start_row);
            let evaluation = evaluate_master_rows(ws, shared_strings, &old_rows, source_index);
            let new_sources = collect_new_sources(source_index, &evaluation.matched_source_keys);
            let added = rows_from_sources(&new_sources);
            let (filter_end_row, filter_end_col) = rebuild_master_rows(
                ws,
                shared_strings,
                header_row,
                data_start_row,
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
        .ok_or_else(|| err("마스터 파일에 '유류비' 시트가 없습니다"))??;
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
        } = evaluate_master_row(ws, shared_strings, old_row, source_index);
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
    let mut new_sources: Vec<SourceRecord> = source_index
        .iter()
        .filter_map(|(key, rec)| (!matched_source_keys.contains(key)).then_some(rec.clone()))
        .collect();
    new_sources.sort_by(|a, b| {
        a.region
            .cmp(&b.region)
            .then(a.name.cmp(&b.name))
            .then(a.address.cmp(&b.address))
    });
    new_sources
}
fn rows_from_sources(new_sources: &[SourceRecord]) -> Vec<StoreRow> {
    new_sources
        .iter()
        .map(|src| StoreRow {
            region: src.region.clone(),
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
    header_row: u32,
    data_start_row: u32,
    old_rows: &[u32],
    kept_source_rows: &[(u32, Option<SourceRecord>)],
    new_sources: &[SourceRecord],
) -> Result<(u32, u32)> {
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
    write_source_rows_to_master(ws, &kept_rows, &new_rows_from_sources, shared_strings);
    let filter_end_row = if final_count == 0 {
        data_start_row
    } else {
        data_start_row
            .checked_add(final_count_u32.saturating_sub(1))
            .ok_or_else(|| err("유류비 마지막 행 계산 중 overflow가 발생했습니다."))?
    };
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
        let r = *r;
        if row_mapper.has_old_rows && r >= data_start_row && r <= old_end_row {
            continue;
        }
        let mut row_obj = row_obj.clone();
        if r < data_start_row {
            remap_row_numbers(&mut row_obj, r, &|old_ref_row| row_mapper.map(old_ref_row));
            new_rows_map.insert(r, row_obj);
        } else {
            let shifted = row_mapper.shift(r);
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
    shared_strings: &[String],
) {
    for plan in kept_rows {
        if let Some(src) = &plan.src {
            write_master_row_from_source(ws, plan.new_row, src);
        }
    }
    for (new_row, src) in new_rows_from_sources {
        write_master_row_from_source(ws, *new_row, src);
        let region_cell = ws.get_display_at(2, *new_row, shared_strings);
        if region_cell.trim().is_empty() && !src.region.trim().is_empty() {
            ws.set_string_at(2, *new_row, &src.region);
        }
    }
}
fn find_master_header_row(ws: &excel::writer::Worksheet, shared_strings: &[String]) -> Result<u32> {
    for row in 1..=master_header_scan_rows() {
        if ws.get_display_at(1, row, shared_strings).trim() == "지역화폐적용순위" {
            return Ok(row);
        }
    }
    Err(err("유류비 시트에서 헤더 행을 찾지 못했습니다"))
}
fn master_header_scan_rows() -> u32 {
    env::var("FCUPDATER_MASTER_HEADER_SCAN_ROWS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .map_or(200, |v| v.min(20_000))
}
fn collect_master_data_rows(
    ws: &excel::writer::Worksheet,
    shared_strings: &[String],
    data_start_row: u32,
) -> Vec<u32> {
    let mut rows = Vec::new();
    for row in ws.rows.range(data_start_row..).map(|(row, _)| *row) {
        let region = ws.get_display_at(2, row, shared_strings);
        let name = ws.get_display_at(3, row, shared_strings);
        let addr = ws.get_display_at(6, row, shared_strings);
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
fn write_master_row_from_source(ws: &mut excel::writer::Worksheet, row: u32, src: &SourceRecord) {
    ws.set_string_at(3, row, &src.name);
    ws.set_string_at(4, row, &src.brand);
    ws.set_string_at(5, row, &src.self_yn);
    ws.set_string_at(6, row, &src.address);
    ws.set_string_at(7, row, &src.phone);
    ws.set_i32_at(8, row, src.gasoline);
    ws.set_i32_at(9, row, src.premium);
    ws.set_i32_at(11, row, src.diesel);
}
