use crate::{
    diagnostic::{Result, err, err_with_source},
    excel::writer::{Worksheet, col_to_name},
    rows::{AddedStoreRow, ChangeRow, StoreRow},
    sheet_util::add_row_offset,
};
use core::{fmt::Write as _, range::RangeInclusive};
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
const ROW_DECIMAL_TEXT_MAX_LEN: usize = 10;
pub struct ChangeLogUpdater<'sheet, 'shared, 'data, 'source> {
    pub added: &'data [AddedStoreRow<'source>],
    pub changes: &'data [ChangeRow<'source>],
    pub deleted: &'data [StoreRow],
    pub shared_string_table: &'shared [String],
    pub today: &'data str,
    pub worksheet: &'sheet mut Worksheet,
}
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
struct ChangeLogRowValues<'row> {
    address: &'row str,
    name: &'row str,
    new_diesel: Option<i32>,
    new_gasoline: Option<i32>,
    new_premium: Option<i32>,
    old_diesel: Option<i32>,
    old_gasoline: Option<i32>,
    old_premium: Option<i32>,
    reason: &'row str,
    region: &'row str,
}
struct DeltaFormulaColumn<'col> {
    formula_capacity: usize,
    new_ref: &'col str,
    old_ref: &'col str,
    target_col: u32,
}
struct OptionalIntCell {
    col: u32,
    value: Option<i32>,
}
struct TextCell<'text> {
    col: u32,
    value: &'text str,
}
impl<'col> DeltaFormulaColumn<'col> {
    fn new(new_ref: &'col str, old_ref: &'col str, target_col: u32) -> Result<Self> {
        let Some(formula_capacity) = "IF(OR("
            .len()
            .checked_add(old_ref.len())
            .and_then(|len| len.checked_add(ROW_DECIMAL_TEXT_MAX_LEN))
            .and_then(|len| len.checked_add("=\"\",".len()))
            .and_then(|len| len.checked_add(new_ref.len()))
            .and_then(|len| len.checked_add(ROW_DECIMAL_TEXT_MAX_LEN))
            .and_then(|len| len.checked_add("=\"\"),\"\",".len()))
            .and_then(|len| len.checked_add(new_ref.len()))
            .and_then(|len| len.checked_add(ROW_DECIMAL_TEXT_MAX_LEN))
            .and_then(|len| len.checked_add("-".len()))
            .and_then(|len| len.checked_add(old_ref.len()))
            .and_then(|len| len.checked_add(ROW_DECIMAL_TEXT_MAX_LEN))
            .and_then(|len| len.checked_add(")".len()))
        else {
            return Err(err("변경내역 delta formula 용량 계산 실패"));
        };
        Ok(Self {
            formula_capacity,
            new_ref,
            old_ref,
            target_col,
        })
    }
}
impl ChangeLogRowValues<'_> {
    fn write_to(
        &self,
        worksheet: &mut Worksheet,
        layout: &ChangeLogLayout,
        row: u32,
        delta_columns: &[DeltaFormulaColumn<'_>],
        formula_buffer: &mut String,
    ) -> Result<()> {
        for cell in [
            TextCell {
                col: layout.col_region,
                value: self.region,
            },
            TextCell {
                col: layout.col_name,
                value: self.name,
            },
            TextCell {
                col: layout.col_address,
                value: self.address,
            },
            TextCell {
                col: layout.col_reason,
                value: self.reason,
            },
        ] {
            worksheet.set_string_at(cell.col, row, cell.value)?;
        }
        for cell in [
            OptionalIntCell {
                col: layout.col_old_gas,
                value: self.old_gasoline,
            },
            OptionalIntCell {
                col: layout.col_new_gas,
                value: self.new_gasoline,
            },
            OptionalIntCell {
                col: layout.col_old_premium,
                value: self.old_premium,
            },
            OptionalIntCell {
                col: layout.col_new_premium,
                value: self.new_premium,
            },
            OptionalIntCell {
                col: layout.col_old_diesel,
                value: self.old_diesel,
            },
            OptionalIntCell {
                col: layout.col_new_diesel,
                value: self.new_diesel,
            },
        ] {
            worksheet.set_i32_at(cell.col, row, cell.value)?;
        }
        for column in delta_columns {
            let old_col = column.old_ref;
            let new_col = column.new_ref;
            formula_buffer.clear();
            write!(
                formula_buffer,
                "IF(OR({old_col}{row}=\"\",{new_col}{row}=\"\"),\"\",{new_col}{row}-{old_col}{row})"
            )
            .map_err(|source| err_with_source("변경내역 delta formula 작성 실패", source))?;
            worksheet.set_formula_at(column.target_col, row, formula_buffer)?;
        }
        Ok(())
    }
}
impl ChangeLogUpdater<'_, '_, '_, '_> {
    fn clear_existing_rows(&mut self, layout: &ChangeLogLayout) -> Result<RangeInclusive<u32>> {
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
        for row in self.worksheet.row_numbers_from(layout.data_start_row).rev() {
            if self
                .worksheet
                .row_has_any_data(row, &cols, self.shared_string_table)?
            {
                last_row = Some(row);
                break;
            }
        }
        let old_data_rows = if let Some(last_data_row) = last_row {
            self.worksheet.clear_cells_in_rows_through_col(
                RangeInclusive {
                    start: layout.data_start_row,
                    last: last_data_row,
                },
                layout.max_col,
            );
            RangeInclusive {
                start: layout.data_start_row,
                last: last_data_row,
            }
        } else {
            RangeInclusive {
                start: layout.data_start_row,
                last: layout.data_start_row.checked_sub(1).ok_or_else(|| {
                    err("변경내역 기존 데이터 범위 계산 중 overflow가 발생했습니다.")
                })?,
            }
        };
        Ok(old_data_rows)
    }
    fn extend_entry_conditional_formats(
        &mut self,
        layout: &ChangeLogLayout,
        old_data_rows: RangeInclusive<u32>,
        entry_count: usize,
    ) -> Result<u32> {
        let last_change_row = if entry_count == 0 {
            layout.data_start_row
        } else {
            let last_entry_index = entry_count
                .checked_sub(1)
                .ok_or_else(|| err("변경내역 마지막 entry index 계산 실패"))?;
            add_row_offset(
                layout.data_start_row,
                last_entry_index,
                "변경내역 마지막 행 계산",
            )?
        };
        let target_cols = [
            layout.col_delta_gas,
            layout.col_delta_premium,
            layout.col_delta_diesel,
        ];
        self.worksheet.extend_conditional_formats(
            old_data_rows,
            RangeInclusive {
                start: layout.data_start_row,
                last: last_change_row,
            },
            &target_cols,
        )?;
        Ok(last_change_row)
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
    fn select_style_template_row(&self, layout: &ChangeLogLayout) -> Result<u32> {
        if CHANGELOG_STYLE_TEMPLATE_ROW >= layout.data_start_row
            && self
                .worksheet
                .has_any_row_format(CHANGELOG_STYLE_TEMPLATE_ROW, layout.max_col)
        {
            return Ok(CHANGELOG_STYLE_TEMPLATE_ROW);
        }
        let end = if CHANGELOG_STYLE_TEMPLATE_ROW > layout.data_start_row {
            CHANGELOG_STYLE_TEMPLATE_ROW
        } else {
            layout.data_start_row.checked_add(1).ok_or_else(|| {
                err("변경 이력 style template 행 계산 중 overflow가 발생했습니다.")
            })?
        };
        Ok((layout.data_start_row..end)
            .rev()
            .find(|row| self.worksheet.has_any_row_format(*row, layout.max_col))
            .unwrap_or(layout.data_start_row))
    }
    pub fn update(&mut self) -> Result<()> {
        let date_text = format!("현행화 일자: {}", self.today);
        self.worksheet.set_string_at(1, 2, &date_text)?;
        let layout = self.find_layout()?;
        let style_template_row = self.select_style_template_row(&layout)?;
        let old_data_rows = self.clear_existing_rows(&layout)?;
        self.write_entries(&layout, style_template_row, old_data_rows)?;
        self.worksheet.update_dimension()?;
        Ok(())
    }
    fn validate_fixed_header(&self) -> Result<()> {
        struct HeaderExpectation {
            col: u32,
            text: &'static str,
        }
        let expected_headers = [
            HeaderExpectation {
                col: CHANGELOG_COL_REGION,
                text: "지역",
            },
            HeaderExpectation {
                col: CHANGELOG_COL_NAME,
                text: "상호",
            },
            HeaderExpectation {
                col: CHANGELOG_COL_ADDRESS,
                text: "주소",
            },
            HeaderExpectation {
                col: CHANGELOG_COL_REASON,
                text: "변경내용",
            },
            HeaderExpectation {
                col: CHANGELOG_COL_OLD_GAS,
                text: "휘발유(이전)",
            },
            HeaderExpectation {
                col: CHANGELOG_COL_NEW_GAS,
                text: "휘발유(신규)",
            },
            HeaderExpectation {
                col: CHANGELOG_COL_DELTA_GAS,
                text: "휘발유 Δ",
            },
            HeaderExpectation {
                col: CHANGELOG_COL_OLD_PREMIUM,
                text: "고급유(이전)",
            },
            HeaderExpectation {
                col: CHANGELOG_COL_NEW_PREMIUM,
                text: "고급유(신규)",
            },
            HeaderExpectation {
                col: CHANGELOG_COL_DELTA_PREMIUM,
                text: "고급유 Δ",
            },
            HeaderExpectation {
                col: CHANGELOG_COL_OLD_DIESEL,
                text: "경유(이전)",
            },
            HeaderExpectation {
                col: CHANGELOG_COL_NEW_DIESEL,
                text: "경유(신규)",
            },
            HeaderExpectation {
                col: CHANGELOG_COL_DELTA_DIESEL,
                text: "경유 Δ",
            },
        ];
        for expected in expected_headers {
            let actual = self.worksheet.try_get_display_at(
                expected.col,
                CHANGELOG_HEADER_ROW,
                self.shared_string_table,
            )?;
            let trimmed = actual.trim();
            if trimmed != expected.text {
                let col = expected.col;
                let expected_text = expected.text;
                return Err(err(format!(
                    "변경내역 헤더가 예상과 다릅니다: row={CHANGELOG_HEADER_ROW}, col={col}, expected={expected_text}, actual={trimmed}"
                )));
            }
        }
        Ok(())
    }
    fn write_entries(
        &mut self,
        layout: &ChangeLogLayout,
        style_template_row: u32,
        old_data_rows: RangeInclusive<u32>,
    ) -> Result<()> {
        let entry_count = self
            .changes
            .len()
            .checked_add(self.added.len())
            .and_then(|count| count.checked_add(self.deleted.len()))
            .ok_or_else(|| err("변경내역 entry 수 계산 중 overflow가 발생했습니다."))?;
        if entry_count == 0 {
            let last_change_row =
                self.extend_entry_conditional_formats(layout, old_data_rows, entry_count)?;
            return self
                .worksheet
                .truncate_rows_after(last_change_row.max(style_template_row));
        }
        let change_entries = self.changes.iter().map(|change| ChangeLogRowValues {
            address: change.address,
            name: change.name,
            new_diesel: change.new_diesel,
            new_gasoline: change.new_gasoline,
            new_premium: change.new_premium,
            old_diesel: change.old_diesel,
            old_gasoline: change.old_gasoline,
            old_premium: change.old_premium,
            reason: change.reason,
            region: change.region.as_ref(),
        });
        let added_entries = self.added.iter().map(|item| ChangeLogRowValues {
            address: item.record.address.as_str(),
            name: item.record.name.as_str(),
            new_diesel: item.record.diesel,
            new_gasoline: item.record.gasoline,
            new_premium: item.record.premium,
            old_diesel: None,
            old_gasoline: None,
            old_premium: None,
            reason: "신규",
            region: item.region,
        });
        let deleted_entries = self.deleted.iter().map(|item| ChangeLogRowValues {
            address: item.address.as_str(),
            name: item.name.as_str(),
            new_diesel: None,
            new_gasoline: None,
            new_premium: None,
            old_diesel: item.diesel,
            old_gasoline: item.gasoline,
            old_premium: item.premium,
            reason: "폐업",
            region: item.region.as_str(),
        });
        let old_gas_col = col_to_name(layout.col_old_gas)?;
        let new_gas_col = col_to_name(layout.col_new_gas)?;
        let old_premium_col = col_to_name(layout.col_old_premium)?;
        let new_premium_col = col_to_name(layout.col_new_premium)?;
        let old_diesel_col = col_to_name(layout.col_old_diesel)?;
        let new_diesel_col = col_to_name(layout.col_new_diesel)?;
        let delta_columns = [
            DeltaFormulaColumn::new(
                new_gas_col.as_str(),
                old_gas_col.as_str(),
                layout.col_delta_gas,
            )?,
            DeltaFormulaColumn::new(
                new_premium_col.as_str(),
                old_premium_col.as_str(),
                layout.col_delta_premium,
            )?,
            DeltaFormulaColumn::new(
                new_diesel_col.as_str(),
                old_diesel_col.as_str(),
                layout.col_delta_diesel,
            )?,
        ];
        let mut formula_buffer = String::new();
        let formula_capacity = delta_columns.iter().fold(0_usize, |max_capacity, column| {
            max_capacity.max(column.formula_capacity)
        });
        formula_buffer
            .try_reserve_exact(formula_capacity)
            .map_err(|source| err_with_source("변경내역 delta formula 메모리 확보 실패", source))?;
        let worksheet = &mut *self.worksheet;
        for (index, values) in change_entries
            .chain(added_entries)
            .chain(deleted_entries)
            .enumerate()
        {
            let row = add_row_offset(layout.data_start_row, index, "변경내역 데이터 쓰기")?;
            if row > style_template_row {
                worksheet.copy_row_style(style_template_row, row, layout.max_col)?;
            }
            values.write_to(worksheet, layout, row, &delta_columns, &mut formula_buffer)?;
        }
        let last_change_row =
            self.extend_entry_conditional_formats(layout, old_data_rows, entry_count)?;
        self.worksheet
            .truncate_rows_after(last_change_row.max(style_template_row))
    }
}
