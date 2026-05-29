use crate::{
    ChangeRow, Result, StoreRow, add_row_offset, err, err_with_source,
    excel::writer::{Workbook as StdWorkbook, Worksheet, col_to_name},
};
use core::range::RangeInclusive;
const CHANGELOG_HEADER_ROW: u32 = 3;
const CHANGELOG_DATA_START_ROW: u32 = 4;
const CHANGELOG_STYLE_TEMPLATE_ROW: u32 = 243;
const CHANGELOG_COL_REGION: u32 = 1;
const CHANGELOG_COL_NAME: u32 = 2;
const CHANGELOG_COL_ADDRESS: u32 = 3;
const CHANGELOG_COL_REASON: u32 = 4;
const CHANGELOG_COL_OLD_GAS: u32 = 5;
const CHANGELOG_COL_NEW_GAS: u32 = 6;
const CHANGELOG_COL_DELTA_GAS: u32 = 7;
const CHANGELOG_COL_OLD_PREMIUM: u32 = 8;
const CHANGELOG_COL_NEW_PREMIUM: u32 = 9;
const CHANGELOG_COL_DELTA_PREMIUM: u32 = 10;
const CHANGELOG_COL_OLD_DIESEL: u32 = 11;
const CHANGELOG_COL_NEW_DIESEL: u32 = 12;
const CHANGELOG_COL_DELTA_DIESEL: u32 = 13;
struct ChangeLogLayout {
    col_address: u32,
    col_delta_diesel: u32,
    col_delta_gas: u32,
    col_delta_premium: u32,
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
struct ChangeLogUpdater<'sheet, 'shared, 'data> {
    added: &'data [StoreRow],
    changes: &'data [ChangeRow],
    deleted: &'data [StoreRow],
    shared_string_table: &'shared [String],
    today: &'data str,
    worksheet: &'sheet mut Worksheet,
}
pub struct ChangeLogSheetService<'book, 'data> {
    pub added: &'data [StoreRow],
    pub book: &'book mut StdWorkbook,
    pub changes: &'data [ChangeRow],
    pub deleted: &'data [StoreRow],
    pub today: &'data str,
}
impl ChangeLogSheetService<'_, '_> {
    pub fn update(&mut self) -> Result<()> {
        self.book
            .with_sheet_mut("변경내역", |ws, shared_strings| -> Result<()> {
                let mut updater = ChangeLogUpdater {
                    added: self.added,
                    changes: self.changes,
                    deleted: self.deleted,
                    shared_string_table: shared_strings,
                    today: self.today,
                    worksheet: ws,
                };
                updater.update()
            })
            .ok_or_else(|| err("마스터 파일에 '변경내역' 시트가 없습니다"))?
    }
}
impl ChangeLogUpdater<'_, '_, '_> {
    fn build_entries(&self) -> Result<Vec<ChangeLogEntry>> {
        let entry_capacity = self
            .changes
            .len()
            .saturating_add(self.added.len())
            .saturating_add(self.deleted.len());
        let mut entries: Vec<ChangeLogEntry> = Vec::new();
        entries
            .try_reserve_exact(entry_capacity)
            .map_err(|source| {
                err_with_source(
                    format!("변경내역 entry 메모리 확보 실패: {entry_capacity} entries"),
                    source,
                )
            })?;
        for change in self.changes {
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
        for item in self.added {
            entries.push(ChangeLogEntry {
                reason: "신규".to_owned(),
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
        for item in self.deleted {
            entries.push(ChangeLogEntry {
                reason: "폐업".to_owned(),
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
        Ok(entries)
    }
    fn clear_existing_rows(&mut self, layout: &ChangeLogLayout) -> Result<()> {
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
        let last_row = self
            .worksheet
            .rows
            .range(layout.data_start_row..)
            .rev()
            .map(|(row, _)| *row)
            .find(|&row| {
                self.worksheet
                    .row_has_any_data(row, &cols, self.shared_string_table)
            });
        if let Some(last_data_row) = last_row {
            let estimated_rows = last_data_row
                .saturating_sub(layout.data_start_row)
                .saturating_add(1);
            let estimated_rows_usize = usize::try_from(estimated_rows)
                .map_err(|source| err_with_source("변경내역 clear 행 수 변환 실패", source))?;
            let max_col_usize = usize::try_from(layout.max_col)
                .map_err(|source| err_with_source("변경내역 clear 열 수 변환 실패", source))?;
            let cell_capacity =
                estimated_rows_usize
                    .checked_mul(max_col_usize)
                    .ok_or_else(|| {
                        err("변경내역 clear 대상 cell 수 계산 중 overflow가 발생했습니다.")
                    })?;
            let mut cells_to_clear: Vec<(u32, u32)> = Vec::new();
            cells_to_clear
                .try_reserve_exact(cell_capacity)
                .map_err(|source| {
                    err_with_source(
                        format!("변경내역 clear 대상 메모리 확보 실패: {cell_capacity} cells"),
                        source,
                    )
                })?;
            for (row, row_obj) in self
                .worksheet
                .rows
                .range(layout.data_start_row..=last_data_row)
            {
                for (&col, _) in row_obj.cells.range(..=layout.max_col) {
                    cells_to_clear.push((col, *row));
                }
            }
            for (col, row) in cells_to_clear {
                self.worksheet.clear_cell_if_exists(col, row);
            }
        }
        Ok(())
    }
    fn find_layout(&self) -> Result<ChangeLogLayout> {
        self.validate_fixed_header()?;
        Ok(ChangeLogLayout {
            col_address: CHANGELOG_COL_ADDRESS,
            col_delta_diesel: CHANGELOG_COL_DELTA_DIESEL,
            col_delta_gas: CHANGELOG_COL_DELTA_GAS,
            col_delta_premium: CHANGELOG_COL_DELTA_PREMIUM,
            col_name: CHANGELOG_COL_NAME,
            col_new_diesel: CHANGELOG_COL_NEW_DIESEL,
            col_new_gas: CHANGELOG_COL_NEW_GAS,
            col_new_premium: CHANGELOG_COL_NEW_PREMIUM,
            col_old_diesel: CHANGELOG_COL_OLD_DIESEL,
            col_old_gas: CHANGELOG_COL_OLD_GAS,
            col_old_premium: CHANGELOG_COL_OLD_PREMIUM,
            col_reason: CHANGELOG_COL_REASON,
            col_region: CHANGELOG_COL_REGION,
            data_start_row: CHANGELOG_DATA_START_ROW,
            max_col: CHANGELOG_COL_DELTA_DIESEL,
        })
    }
    fn select_style_template_row(&self, layout: &ChangeLogLayout) -> u32 {
        if CHANGELOG_STYLE_TEMPLATE_ROW >= layout.data_start_row
            && self
                .worksheet
                .has_any_row_format(CHANGELOG_STYLE_TEMPLATE_ROW, layout.max_col)
        {
            return CHANGELOG_STYLE_TEMPLATE_ROW;
        }
        let end = if CHANGELOG_STYLE_TEMPLATE_ROW > layout.data_start_row {
            CHANGELOG_STYLE_TEMPLATE_ROW
        } else {
            layout.data_start_row.saturating_add(1)
        };
        (layout.data_start_row..end)
            .rev()
            .find(|row| self.worksheet.has_any_row_format(*row, layout.max_col))
            .unwrap_or(layout.data_start_row)
    }
    fn update(&mut self) -> Result<()> {
        let date_text = format!("현행화 일자: {}", self.today);
        self.worksheet.set_string_at(1, 2, &date_text);
        let layout = self.find_layout()?;
        let style_template_row = self.select_style_template_row(&layout);
        self.clear_existing_rows(&layout)?;
        let entries = self.build_entries()?;
        self.write_entries(&layout, style_template_row, &entries)?;
        self.worksheet.update_dimension()?;
        Ok(())
    }
    fn validate_fixed_header(&self) -> Result<()> {
        let expected_headers = [
            (CHANGELOG_COL_REGION, "지역"),
            (CHANGELOG_COL_NAME, "상호"),
            (CHANGELOG_COL_ADDRESS, "주소"),
            (CHANGELOG_COL_REASON, "변경내용"),
            (CHANGELOG_COL_OLD_GAS, "휘발유(이전)"),
            (CHANGELOG_COL_NEW_GAS, "휘발유(신규)"),
            (CHANGELOG_COL_DELTA_GAS, "휘발유 Δ"),
            (CHANGELOG_COL_OLD_PREMIUM, "고급유(이전)"),
            (CHANGELOG_COL_NEW_PREMIUM, "고급유(신규)"),
            (CHANGELOG_COL_DELTA_PREMIUM, "고급유 Δ"),
            (CHANGELOG_COL_OLD_DIESEL, "경유(이전)"),
            (CHANGELOG_COL_NEW_DIESEL, "경유(신규)"),
            (CHANGELOG_COL_DELTA_DIESEL, "경유 Δ"),
        ];
        for (col, expected) in expected_headers {
            let actual =
                self.worksheet
                    .get_display_at(col, CHANGELOG_HEADER_ROW, self.shared_string_table);
            let trimmed = actual.trim();
            if trimmed != expected {
                return Err(err(format!(
                    "변경내역 헤더가 예상과 다릅니다: row={CHANGELOG_HEADER_ROW}, col={col}, expected={expected}, actual={trimmed}"
                )));
            }
        }
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
        let delta_columns = [
            (
                layout.col_delta_gas,
                old_gas_col.as_str(),
                new_gas_col.as_str(),
            ),
            (
                layout.col_delta_premium,
                old_premium_col.as_str(),
                new_premium_col.as_str(),
            ),
            (
                layout.col_delta_diesel,
                old_diesel_col.as_str(),
                new_diesel_col.as_str(),
            ),
        ];
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
            for (delta_col, old_col, new_col) in delta_columns {
                let formula = format!(
                    "IF(OR({old_col}{row}=\"\",{new_col}{row}=\"\"),\"\",{new_col}{row}-{old_col}{row})"
                );
                self.worksheet.set_formula_at(delta_col, row, &formula);
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
        let target_cols = [
            layout.col_delta_gas,
            layout.col_delta_premium,
            layout.col_delta_diesel,
        ];
        self.worksheet.extend_conditional_formats(
            RangeInclusive {
                start: layout.data_start_row,
                last: last_change_row,
            },
            &target_cols,
        )
    }
}
