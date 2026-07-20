use crate::{
    diagnostic::{Result, err, err_with_source},
    excel::{
        SourceRecord,
        writer::{Worksheet, col_to_name},
    },
    master_sheet::{ChangeRow, StoreRow},
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
pub(super) struct ChangeLogUpdater<'sheet, 'shared, 'data, 'source> {
    pub added: &'data [&'source SourceRecord],
    pub changes: &'data [ChangeRow<'source>],
    pub deleted: &'data [StoreRow],
    pub shared_string_table: &'shared [String],
    pub today: &'data str,
    pub worksheet: &'sheet mut Worksheet,
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
        row: u32,
        delta_columns: &[DeltaFormulaColumn<'_>],
        formula_buffer: &mut String,
    ) -> Result<()> {
        for (col, value) in [
            (CHANGELOG_COL_REGION, self.region),
            (CHANGELOG_COL_NAME, self.name),
            (CHANGELOG_COL_ADDRESS, self.address),
            (CHANGELOG_COL_REASON, self.reason),
        ] {
            worksheet.set_string_at(col, row, value)?;
        }
        for (col, value) in [
            (CHANGELOG_COL_OLD_GAS, self.old_gasoline),
            (CHANGELOG_COL_NEW_GAS, self.new_gasoline),
            (CHANGELOG_COL_OLD_PREMIUM, self.old_premium),
            (CHANGELOG_COL_NEW_PREMIUM, self.new_premium),
            (CHANGELOG_COL_OLD_DIESEL, self.old_diesel),
            (CHANGELOG_COL_NEW_DIESEL, self.new_diesel),
        ] {
            worksheet.set_i32_at(col, row, value)?;
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
    fn clear_existing_rows(&mut self) -> Result<RangeInclusive<u32>> {
        let cols = [
            CHANGELOG_COL_REGION,
            CHANGELOG_COL_NAME,
            CHANGELOG_COL_ADDRESS,
            CHANGELOG_COL_REASON,
            CHANGELOG_COL_OLD_GAS,
            CHANGELOG_COL_NEW_GAS,
            CHANGELOG_COL_OLD_PREMIUM,
            CHANGELOG_COL_NEW_PREMIUM,
            CHANGELOG_COL_OLD_DIESEL,
            CHANGELOG_COL_NEW_DIESEL,
        ];
        let mut last_row = None;
        for row in self
            .worksheet
            .row_numbers_from(CHANGELOG_DATA_START_ROW)
            .rev()
        {
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
                    start: CHANGELOG_DATA_START_ROW,
                    last: last_data_row,
                },
                CHANGELOG_COL_DELTA_DIESEL,
            );
            RangeInclusive {
                start: CHANGELOG_DATA_START_ROW,
                last: last_data_row,
            }
        } else {
            RangeInclusive {
                start: CHANGELOG_DATA_START_ROW,
                last: CHANGELOG_HEADER_ROW,
            }
        };
        Ok(old_data_rows)
    }
    fn extend_entry_conditional_formats(
        &mut self,
        old_data_rows: RangeInclusive<u32>,
        entry_count: usize,
    ) -> Result<u32> {
        let last_change_row = if let Some(last_entry_index) = entry_count.checked_sub(1) {
            add_row_offset(
                CHANGELOG_DATA_START_ROW,
                last_entry_index,
                "변경내역 마지막 행 계산",
            )?
        } else {
            CHANGELOG_DATA_START_ROW
        };
        let target_cols = [
            CHANGELOG_COL_DELTA_GAS,
            CHANGELOG_COL_DELTA_PREMIUM,
            CHANGELOG_COL_DELTA_DIESEL,
        ];
        self.worksheet.extend_conditional_formats(
            old_data_rows,
            RangeInclusive {
                start: CHANGELOG_DATA_START_ROW,
                last: last_change_row,
            },
            &target_cols,
        )?;
        Ok(last_change_row)
    }
    fn select_style_template_row(&self) -> Result<u32> {
        if self
            .worksheet
            .has_any_row_format(CHANGELOG_STYLE_TEMPLATE_ROW, CHANGELOG_COL_DELTA_DIESEL)
        {
            return Ok(CHANGELOG_STYLE_TEMPLATE_ROW);
        }
        (CHANGELOG_DATA_START_ROW..CHANGELOG_STYLE_TEMPLATE_ROW)
            .rev()
            .find(|row| {
                self.worksheet
                    .has_any_row_format(*row, CHANGELOG_COL_DELTA_DIESEL)
            })
            .ok_or_else(|| err("변경내역 style template 행을 찾지 못했습니다."))
    }
    pub(super) fn update(&mut self) -> Result<()> {
        let date_text = format!("현행화 일자: {}", self.today);
        self.worksheet.set_string_at(1, 2, &date_text)?;
        self.validate_fixed_header()?;
        let style_template_row = self.select_style_template_row()?;
        let old_data_rows = self.clear_existing_rows()?;
        self.write_entries(style_template_row, old_data_rows)?;
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
        for (col, expected_text) in expected_headers {
            let actual = self.worksheet.try_get_display_at(
                col,
                CHANGELOG_HEADER_ROW,
                self.shared_string_table,
            )?;
            let trimmed = actual.trim();
            if trimmed != expected_text {
                return Err(err(format!(
                    "변경내역 헤더가 예상과 다릅니다: row={CHANGELOG_HEADER_ROW}, col={col}, expected={expected_text}, actual={trimmed}"
                )));
            }
        }
        Ok(())
    }
    fn write_entries(
        &mut self,
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
                self.extend_entry_conditional_formats(old_data_rows, entry_count)?;
            return self
                .worksheet
                .truncate_rows_after(last_change_row.max(style_template_row));
        }
        let change_entries = self.changes.iter().map(|change| ChangeLogRowValues {
            address: &change.record.address,
            name: &change.record.name,
            new_diesel: change.record.diesel,
            new_gasoline: change.record.gasoline,
            new_premium: change.record.premium,
            old_diesel: change.old_diesel,
            old_gasoline: change.old_gasoline,
            old_premium: change.old_premium,
            reason: &change.reason,
            region: change.record.region,
        });
        let added_entries = self.added.iter().map(|item| ChangeLogRowValues {
            address: &item.address,
            name: &item.name,
            new_diesel: item.diesel,
            new_gasoline: item.gasoline,
            new_premium: item.premium,
            old_diesel: None,
            old_gasoline: None,
            old_premium: None,
            reason: "신규",
            region: item.region,
        });
        let deleted_entries = self.deleted.iter().map(|item| ChangeLogRowValues {
            address: &item.address,
            name: &item.name,
            new_diesel: None,
            new_gasoline: None,
            new_premium: None,
            old_diesel: item.diesel,
            old_gasoline: item.gasoline,
            old_premium: item.premium,
            reason: "폐업",
            region: &item.region,
        });
        let old_gas_col = col_to_name(CHANGELOG_COL_OLD_GAS)?;
        let new_gas_col = col_to_name(CHANGELOG_COL_NEW_GAS)?;
        let old_premium_col = col_to_name(CHANGELOG_COL_OLD_PREMIUM)?;
        let new_premium_col = col_to_name(CHANGELOG_COL_NEW_PREMIUM)?;
        let old_diesel_col = col_to_name(CHANGELOG_COL_OLD_DIESEL)?;
        let new_diesel_col = col_to_name(CHANGELOG_COL_NEW_DIESEL)?;
        let delta_columns = [
            DeltaFormulaColumn::new(
                new_gas_col.as_str(),
                old_gas_col.as_str(),
                CHANGELOG_COL_DELTA_GAS,
            )?,
            DeltaFormulaColumn::new(
                new_premium_col.as_str(),
                old_premium_col.as_str(),
                CHANGELOG_COL_DELTA_PREMIUM,
            )?,
            DeltaFormulaColumn::new(
                new_diesel_col.as_str(),
                old_diesel_col.as_str(),
                CHANGELOG_COL_DELTA_DIESEL,
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
            let row = add_row_offset(CHANGELOG_DATA_START_ROW, index, "변경내역 데이터 쓰기")?;
            if row > style_template_row {
                worksheet.copy_row_style(style_template_row, row, CHANGELOG_COL_DELTA_DIESEL)?;
            }
            values.write_to(worksheet, row, &delta_columns, &mut formula_buffer)?;
        }
        let last_change_row = self.extend_entry_conditional_formats(old_data_rows, entry_count)?;
        self.worksheet
            .truncate_rows_after(last_change_row.max(style_template_row))
    }
}
