use crate::{
    ChangeRow, Result, StoreRow, add_row_offset, canon_header, err,
    excel::writer::{Workbook as StdWorkbook, Worksheet, col_to_name},
    push_display,
};
use std::{collections::HashMap, env};
const HEADER_KEYS_REGION: [&str; 1] = ["지역"];
const HEADER_KEYS_NAME: [&str; 1] = ["상호"];
const HEADER_KEYS_ADDRESS: [&str; 1] = ["주소"];
const HEADER_KEYS_REASON: [&str; 3] = ["변경내용", "변경내역", "변경사유"];
const HEADER_KEYS_OLD_GAS: [&str; 2] = ["휘발유(이전)", "휘발유이전"];
const HEADER_KEYS_NEW_GAS: [&str; 2] = ["휘발유(신규)", "휘발유신규"];
const HEADER_KEYS_OLD_PREMIUM: [&str; 2] = ["고급유(이전)", "고급유이전"];
const HEADER_KEYS_NEW_PREMIUM: [&str; 2] = ["고급유(신규)", "고급유신규"];
const HEADER_KEYS_OLD_DIESEL: [&str; 2] = ["경유(이전)", "경유이전"];
const HEADER_KEYS_NEW_DIESEL: [&str; 2] = ["경유(신규)", "경유신규"];
const HEADER_KEYS_DELTA_GAS: [&str; 4] = ["휘발유Δ", "휘발유△", "휘발유증감", "휘발유차이"];
const HEADER_KEYS_DELTA_PREMIUM: [&str; 4] = ["고급유Δ", "고급유△", "고급유증감", "고급유차이"];
const HEADER_KEYS_DELTA_DIESEL: [&str; 4] = ["경유Δ", "경유△", "경유증감", "경유차이"];
struct ChangeLogLayout {
    col_address: u32,
    col_delta_diesel: Option<u32>,
    col_delta_gas: Option<u32>,
    col_delta_premium: Option<u32>,
    col_name: u32,
    col_new_diesel: u32,
    col_new_gas: u32,
    col_new_premium: u32,
    col_old_diesel: u32,
    col_old_gas: u32,
    col_old_premium: u32,
    col_reason: u32,
    col_region: u32,
    data_start_row: u32,
    max_col: u32,
}
#[derive(Debug, Clone)]
struct ChangeLogEntry {
    address: String,
    name: String,
    new_diesel: Option<i32>,
    new_gasoline: Option<i32>,
    new_premium: Option<i32>,
    old_diesel: Option<i32>,
    old_gasoline: Option<i32>,
    old_premium: Option<i32>,
    reason: String,
    region: String,
}
struct ChangeLogUpdater<'sheet, 'shared> {
    shared_string_table: &'shared [String],
    worksheet: &'sheet mut Worksheet,
}
pub struct ChangeLogSheetService;
pub trait ChangeLogSheetServiceExt {
    fn update_change_log_sheet(
        &self,
        book: &mut StdWorkbook,
        today: &str,
        changes: &[ChangeRow],
        added: &[StoreRow],
        deleted: &[StoreRow],
    ) -> Result<()>;
}
trait ChangeLogUpdaterExt {
    fn build_entries(
        &self,
        changes: &[ChangeRow],
        added: &[StoreRow],
        deleted: &[StoreRow],
    ) -> Vec<ChangeLogEntry>;
    fn clear_existing_rows(&mut self, layout: &ChangeLogLayout);
    fn find_layout(&self) -> Result<ChangeLogLayout>;
    fn select_style_template_row(&self, layout: &ChangeLogLayout) -> u32;
    fn update(
        &mut self,
        today: &str,
        changes: &[ChangeRow],
        added: &[StoreRow],
        deleted: &[StoreRow],
    ) -> Result<()>;
    fn write_entries(
        &mut self,
        layout: &ChangeLogLayout,
        style_template_row: u32,
        entries: &[ChangeLogEntry],
    ) -> Result<()>;
}
impl ChangeLogSheetServiceExt for ChangeLogSheetService {
    fn update_change_log_sheet(
        &self,
        book: &mut StdWorkbook,
        today: &str,
        changes: &[ChangeRow],
        added: &[StoreRow],
        deleted: &[StoreRow],
    ) -> Result<()> {
        book.with_sheet_mut("변경내역", |ws, shared_strings| -> Result<()> {
            let mut updater = ChangeLogUpdater {
                shared_string_table: shared_strings,
                worksheet: ws,
            };
            updater.update(today, changes, added, deleted)
        })
        .ok_or_else(|| err("마스터 파일에 '변경내역' 시트가 없습니다"))?
    }
}
impl ChangeLogUpdaterExt for ChangeLogUpdater<'_, '_> {
    fn build_entries(
        &self,
        changes: &[ChangeRow],
        added: &[StoreRow],
        deleted: &[StoreRow],
    ) -> Vec<ChangeLogEntry> {
        let mut entries = Vec::with_capacity(
            changes
                .len()
                .saturating_add(added.len())
                .saturating_add(deleted.len()),
        );
        for change in changes {
            entries.push(ChangeLogEntry {
                reason: change.reason.clone(),
                region: change.region.clone(),
                name: change.name.clone(),
                address: change.address.clone(),
                old_gasoline: change.old_gasoline,
                new_gasoline: change.new_gasoline,
                old_premium: change.old_premium,
                new_premium: change.new_premium,
                old_diesel: change.old_diesel,
                new_diesel: change.new_diesel,
            });
        }
        for item in added {
            entries.push(ChangeLogEntry {
                reason: String::from("신규"),
                region: item.region.clone(),
                name: item.name.clone(),
                address: item.address.clone(),
                old_gasoline: None,
                new_gasoline: item.gasoline,
                old_premium: None,
                new_premium: item.premium,
                old_diesel: None,
                new_diesel: item.diesel,
            });
        }
        for item in deleted {
            entries.push(ChangeLogEntry {
                reason: String::from("폐업"),
                region: item.region.clone(),
                name: item.name.clone(),
                address: item.address.clone(),
                old_gasoline: item.gasoline,
                new_gasoline: None,
                old_premium: item.premium,
                new_premium: None,
                old_diesel: item.diesel,
                new_diesel: None,
            });
        }
        entries
    }
    fn clear_existing_rows(&mut self, layout: &ChangeLogLayout) {
        let cols = [
            layout.col_region,
            layout.col_name,
            layout.col_address,
            layout.col_reason,
            layout.col_old_gas,
            layout.col_new_gas,
            layout.col_old_premium,
            layout.col_new_premium,
            layout.col_old_diesel,
            layout.col_new_diesel,
        ];
        let mut last_row = None;
        for row in self
            .worksheet
            .rows
            .range(layout.data_start_row..)
            .map(|(row, _)| *row)
        {
            if self
                .worksheet
                .row_has_any_data(row, &cols, self.shared_string_table)
            {
                last_row = Some(row);
            }
        }
        if let Some(last_data_row) = last_row {
            let estimated_rows = last_data_row
                .saturating_sub(layout.data_start_row)
                .saturating_add(1);
            let mut cells_to_clear = Vec::with_capacity(
                usize::try_from(estimated_rows)
                    .unwrap_or(0)
                    .saturating_mul(8),
            );
            for (row, row_obj) in self
                .worksheet
                .rows
                .range(layout.data_start_row..=last_data_row)
            {
                for entry in row_obj.cells.range(..=layout.max_col) {
                    cells_to_clear.push((*entry.0, *row));
                }
            }
            for (col, row) in cells_to_clear {
                self.worksheet.clear_cell_if_exists(col, row);
            }
        }
    }
    fn find_layout(&self) -> Result<ChangeLogLayout> {
        let max_rows = change_log_env_u32("FCUPDATER_CHANGELOG_HEADER_SCAN_ROWS", 30, Some(1_000));
        let max_cols = change_log_env_u32("FCUPDATER_CHANGELOG_HEADER_SCAN_COLS", 60, Some(500));
        for row in 1..=max_rows {
            let mut headers = HashMap::with_capacity(usize::try_from(max_cols).unwrap_or(0));
            for col in 1..=max_cols {
                let key = canon_header(
                    self.worksheet
                        .get_display_at(col, row, self.shared_string_table)
                        .trim(),
                );
                if !key.is_empty() {
                    headers.entry(key).or_insert(col);
                }
            }
            if headers.is_empty() {
                continue;
            }
            let get_header_col_optional = |keys: &[&str]| {
                for key in keys {
                    let canon = canon_header(key);
                    if let Some(col) = headers.get(&canon) {
                        return Some(*col);
                    }
                }
                None
            };
            let required_col = |keys: &[&str], display_name: &str| {
                get_header_col_optional(keys).ok_or_else(|| {
                    let mut message = String::with_capacity(display_name.len().saturating_add(36));
                    message.push_str("변경내역 헤더에 '");
                    message.push_str(display_name);
                    message.push_str("' 컬럼이 없습니다.");
                    err(message)
                })
            };
            let Some(col_region) = get_header_col_optional(&HEADER_KEYS_REGION) else {
                continue;
            };
            let Some(col_name) = get_header_col_optional(&HEADER_KEYS_NAME) else {
                continue;
            };
            let Some(col_address) = get_header_col_optional(&HEADER_KEYS_ADDRESS) else {
                continue;
            };
            let Some(col_reason) = get_header_col_optional(&HEADER_KEYS_REASON) else {
                continue;
            };
            let col_old_gas = required_col(&HEADER_KEYS_OLD_GAS, "휘발유(이전)")?;
            let col_new_gas = required_col(&HEADER_KEYS_NEW_GAS, "휘발유(신규)")?;
            let col_old_premium = required_col(&HEADER_KEYS_OLD_PREMIUM, "고급유(이전)")?;
            let col_new_premium = required_col(&HEADER_KEYS_NEW_PREMIUM, "고급유(신규)")?;
            let col_old_diesel = required_col(&HEADER_KEYS_OLD_DIESEL, "경유(이전)")?;
            let col_new_diesel = required_col(&HEADER_KEYS_NEW_DIESEL, "경유(신규)")?;
            let col_delta_gas = get_header_col_optional(&HEADER_KEYS_DELTA_GAS);
            let col_delta_premium = get_header_col_optional(&HEADER_KEYS_DELTA_PREMIUM);
            let col_delta_diesel = get_header_col_optional(&HEADER_KEYS_DELTA_DIESEL);
            let mut max_col = [
                col_region,
                col_name,
                col_address,
                col_reason,
                col_old_gas,
                col_new_gas,
                col_old_premium,
                col_new_premium,
                col_old_diesel,
                col_new_diesel,
            ]
            .into_iter()
            .fold(0, u32::max);
            for col in [col_delta_gas, col_delta_premium, col_delta_diesel]
                .into_iter()
                .flatten()
            {
                max_col = max_col.max(col);
            }
            let data_start_row = row
                .checked_add(1)
                .ok_or_else(|| err("변경내역 데이터 시작 행 계산 중 범위 오류"))?;
            return Ok(ChangeLogLayout {
                col_address,
                col_delta_diesel,
                col_delta_gas,
                col_delta_premium,
                col_name,
                col_new_diesel,
                col_new_gas,
                col_new_premium,
                col_old_diesel,
                col_old_gas,
                col_old_premium,
                col_reason,
                col_region,
                data_start_row,
                max_col,
            });
        }
        Err(err(
            "변경내역 시트에서 헤더 행을 찾지 못했습니다. 필수 컬럼(지역/상호/주소/변경내용/휘발유(이전)/휘발유(신규)/고급유(이전)/고급유(신규)/경유(이전)/경유(신규))을 확인하세요.",
        ))
    }
    fn select_style_template_row(&self, layout: &ChangeLogLayout) -> u32 {
        let preferred_row = change_log_env_u32("FCUPDATER_CHANGELOG_STYLE_TEMPLATE_ROW", 243, None);
        if preferred_row >= layout.data_start_row
            && self
                .worksheet
                .has_any_row_format(preferred_row, layout.max_col)
        {
            return preferred_row;
        }
        let end = if preferred_row > layout.data_start_row {
            preferred_row
        } else {
            layout.data_start_row.saturating_add(1)
        };
        (layout.data_start_row..end)
            .rev()
            .find(|row| self.worksheet.has_any_row_format(*row, layout.max_col))
            .unwrap_or(layout.data_start_row)
    }
    fn update(
        &mut self,
        today: &str,
        changes: &[ChangeRow],
        added: &[StoreRow],
        deleted: &[StoreRow],
    ) -> Result<()> {
        let mut date_text = String::with_capacity(today.len().saturating_add(16));
        date_text.push_str("현행화 일자: ");
        date_text.push_str(today);
        self.worksheet.set_string_at(1, 2, &date_text);
        let layout = self.find_layout()?;
        let style_template_row = self.select_style_template_row(&layout);
        self.clear_existing_rows(&layout);
        let entries = self.build_entries(changes, added, deleted);
        self.write_entries(&layout, style_template_row, &entries)?;
        self.worksheet.update_dimension()?;
        Ok(())
    }
    fn write_entries(
        &mut self,
        layout: &ChangeLogLayout,
        style_template_row: u32,
        entries: &[ChangeLogEntry],
    ) -> Result<()> {
        let old_gas_col = col_to_name(layout.col_old_gas);
        let new_gas_col = col_to_name(layout.col_new_gas);
        let old_premium_col = col_to_name(layout.col_old_premium);
        let new_premium_col = col_to_name(layout.col_new_premium);
        let old_diesel_col = col_to_name(layout.col_old_diesel);
        let new_diesel_col = col_to_name(layout.col_new_diesel);
        for (index, entry) in entries.iter().enumerate() {
            let row = add_row_offset(layout.data_start_row, index, "변경내역 데이터 쓰기")?;
            if row > style_template_row {
                self.worksheet
                    .clone_row_style(style_template_row, row, layout.max_col);
            }
            self.worksheet
                .set_string_at(layout.col_region, row, &entry.region);
            self.worksheet
                .set_string_at(layout.col_name, row, &entry.name);
            self.worksheet
                .set_string_at(layout.col_address, row, &entry.address);
            self.worksheet
                .set_string_at(layout.col_reason, row, &entry.reason);
            self.worksheet
                .set_i32_at(layout.col_old_gas, row, entry.old_gasoline);
            self.worksheet
                .set_i32_at(layout.col_new_gas, row, entry.new_gasoline);
            self.worksheet
                .set_i32_at(layout.col_old_premium, row, entry.old_premium);
            self.worksheet
                .set_i32_at(layout.col_new_premium, row, entry.new_premium);
            self.worksheet
                .set_i32_at(layout.col_old_diesel, row, entry.old_diesel);
            self.worksheet
                .set_i32_at(layout.col_new_diesel, row, entry.new_diesel);
            if let Some(col) = layout.col_delta_gas {
                self.worksheet.set_formula_at(
                    col,
                    row,
                    &delta_formula(&old_gas_col, &new_gas_col, row),
                );
            }
            if let Some(col) = layout.col_delta_premium {
                self.worksheet.set_formula_at(
                    col,
                    row,
                    &delta_formula(&old_premium_col, &new_premium_col, row),
                );
            }
            if let Some(col) = layout.col_delta_diesel {
                self.worksheet.set_formula_at(
                    col,
                    row,
                    &delta_formula(&old_diesel_col, &new_diesel_col, row),
                );
            }
        }
        if entries.is_empty() {
            return Ok(());
        }
        let last_change_row = add_row_offset(
            layout.data_start_row,
            entries.len().saturating_sub(1),
            "변경내역 마지막 행 계산",
        )?;
        let mut target_cols = Vec::with_capacity(3);
        if let Some(col) = layout.col_delta_gas {
            target_cols.push(col);
        }
        if let Some(col) = layout.col_delta_premium {
            target_cols.push(col);
        }
        if let Some(col) = layout.col_delta_diesel {
            target_cols.push(col);
        }
        self.worksheet.extend_conditional_formats(
            last_change_row,
            &target_cols,
            layout.data_start_row,
        )
    }
}
fn change_log_env_u32(name: &str, default: u32, max: Option<u32>) -> u32 {
    env::var(name)
        .ok()
        .and_then(|parsed_value| parsed_value.parse::<u32>().ok())
        .filter(|parsed_value| *parsed_value > 0)
        .map_or(default, |parsed_value| {
            max.map_or(parsed_value, |max_value| parsed_value.min(max_value))
        })
}
fn delta_formula(old_col: &str, new_col: &str, row: u32) -> String {
    let mut out = String::with_capacity(
        old_col
            .len()
            .saturating_add(new_col.len())
            .saturating_mul(2)
            .saturating_add(48),
    );
    out.push_str("IF(OR(");
    out.push_str(old_col);
    push_display(&mut out, row);
    out.push_str("=\"\",");
    out.push_str(new_col);
    push_display(&mut out, row);
    out.push_str("=\"\"),\"\",");
    out.push_str(new_col);
    push_display(&mut out, row);
    out.push('-');
    out.push_str(old_col);
    push_display(&mut out, row);
    out.push(')');
    out
}
