use crate::{
    diagnostic::{Result, err, err_with_source},
    excel::{
        FuelValues, SourceRecord,
        writer::{SharedStringTable, Worksheet},
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
const DELTA_FORMULA_COLUMNS: [(&str, &str, u32); 3] = [
    ("E", "F", CHANGELOG_COL_DELTA_GAS),
    ("H", "I", CHANGELOG_COL_DELTA_PREMIUM),
    ("K", "L", CHANGELOG_COL_DELTA_DIESEL),
];
pub(super) struct ChangeLogUpdater<'sheet, 'shared, 'data, 'source> {
    pub added: &'data [&'source SourceRecord],
    pub changes: &'data [ChangeRow<'source>],
    pub deleted: &'data [StoreRow],
    pub shared_string_table: &'shared mut SharedStringTable,
    pub today: &'data str,
    pub worksheet: &'sheet mut Worksheet,
}
struct ChangeLogRowValues<'row> {
    address: &'row str,
    name: &'row str,
    new_fuels: FuelValues<Option<i32>>,
    old_fuels: FuelValues<Option<i32>>,
    reason: &'row str,
    region: &'row str,
}
impl ChangeLogRowValues<'_> {
    fn write_to(
        &self,
        worksheet: &mut Worksheet,
        shared_strings: &mut SharedStringTable,
        row: u32,
        formula_buffer: &mut String,
    ) -> Result<()> {
        for (col, value) in [
            (CHANGELOG_COL_REGION, self.region),
            (CHANGELOG_COL_NAME, self.name),
            (CHANGELOG_COL_ADDRESS, self.address),
            (CHANGELOG_COL_REASON, self.reason),
        ] {
            shared_strings.set_cell(worksheet, col, row, value)?;
        }
        for (col, value) in [
            (CHANGELOG_COL_OLD_GAS, self.old_fuels.gasoline),
            (CHANGELOG_COL_NEW_GAS, self.new_fuels.gasoline),
            (CHANGELOG_COL_OLD_PREMIUM, self.old_fuels.premium),
            (CHANGELOG_COL_NEW_PREMIUM, self.new_fuels.premium),
            (CHANGELOG_COL_OLD_DIESEL, self.old_fuels.diesel),
            (CHANGELOG_COL_NEW_DIESEL, self.new_fuels.diesel),
        ] {
            worksheet.set_i32_at(col, row, value)?;
        }
        for &(old_col, new_col, target_col) in &DELTA_FORMULA_COLUMNS {
            formula_buffer.clear();
            write!(
                formula_buffer,
                "IF(OR({old_col}{row}=\"\",{new_col}{row}=\"\"),\"\",{new_col}{row}-{old_col}{row})"
            )
            .map_err(|source| err_with_source("변경내역 delta formula 작성 실패", source))?;
            worksheet.set_formula_at(target_col, row, formula_buffer)?;
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
            .row_numbers_from(CHANGELOG_DATA_START_ROW)?
            .into_iter()
            .rev()
        {
            if self
                .worksheet
                .row_has_any_data(row, &cols, self.shared_string_table.values())?
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
        let last_change_row = add_row_offset(
            CHANGELOG_DATA_START_ROW,
            entry_count.saturating_sub(1),
            "변경내역 마지막 행 계산",
        )?;
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
    pub(super) fn update(&mut self) -> Result<()> {
        if !self
            .worksheet
            .has_any_row_format(CHANGELOG_STYLE_TEMPLATE_ROW, CHANGELOG_COL_DELTA_DIESEL)
        {
            return Err(err("변경내역 243행에 고정 style template이 없습니다."));
        }
        let date_text = format!("현행화 일자: {}", self.today);
        self.shared_string_table
            .set_cell(self.worksheet, 1, 2, &date_text)?;
        let old_data_rows = self.clear_existing_rows()?;
        self.write_entries(CHANGELOG_STYLE_TEMPLATE_ROW, old_data_rows)?;
        self.worksheet.update_dimension()?;
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
            .saturating_add(self.added.len())
            .saturating_add(self.deleted.len());
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
            new_fuels: change.record.fuels,
            old_fuels: change.old_fuels,
            reason: &change.reason,
            region: change.record.region,
        });
        let added_entries = self.added.iter().map(|item| ChangeLogRowValues {
            address: &item.address,
            name: &item.name,
            new_fuels: item.fuels,
            old_fuels: FuelValues::default(),
            reason: "신규",
            region: item.region,
        });
        let deleted_entries = self.deleted.iter().map(|item| ChangeLogRowValues {
            address: &item.address,
            name: &item.name,
            new_fuels: FuelValues::default(),
            old_fuels: item.fuels,
            reason: "폐업",
            region: &item.region,
        });
        let mut formula_buffer = String::new();
        let formula_capacity = ROW_DECIMAL_TEXT_MAX_LEN
            .saturating_mul(4)
            .saturating_add("IF(OR(E=\"\",F=\"\"),\"\",F-E)".len());
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
            values.write_to(
                worksheet,
                self.shared_string_table,
                row,
                &mut formula_buffer,
            )?;
        }
        let last_change_row = self.extend_entry_conditional_formats(old_data_rows, entry_count)?;
        self.worksheet
            .truncate_rows_after(last_change_row.max(style_template_row))
    }
}
