use crate::{
    diagnostic::{Result, append_fmt, try_string_with_capacity},
    excel::{
        FuelValues, SourceRecord,
        writer::{SharedStringTable, Worksheet, is_shared_formula_follower},
    },
    master_sheet::{ChangeRow, StoreRow},
    sheet_util::add_row_offset,
};
use core::fmt::NumBuffer;
const CHANGELOG_DATA_START_ROW: u32 = 4;
const CHANGELOG_STYLE_TEMPLATE_ROW: u32 = 243;
const CHANGELOG_COL_REGION: u32 = 1;
const CHANGELOG_COL_NAME: u32 = 2;
const CHANGELOG_COL_ADDRESS: u32 = 3;
const CHANGELOG_COL_REASON: u32 = 4;
const CHANGELOG_COL_OLD_GAS: u32 = 5;
const CHANGELOG_COL_DELTA_DIESEL: u32 = 13;
const ROW_DECIMAL_TEXT_MAX_LEN: usize = 10;
const FUEL_COLUMNS: [(u32, u32, u32, &str, &str); 3] = [
    (5, 6, 7, "E", "F"),
    (8, 9, 10, "H", "I"),
    (11, 12, 13, "K", "L"),
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
        is_shared_follower: bool,
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
        let mut row_buffer = NumBuffer::new();
        let row_text = row.format_into(&mut row_buffer);
        let mut cache_buffer = NumBuffer::new();
        for (&(old_col_num, new_col_num, target_col, old_col, new_col), (old_value, new_value)) in
            FUEL_COLUMNS.iter().zip(
                self.old_fuels
                    .into_array()
                    .into_iter()
                    .zip(self.new_fuels.into_array()),
            )
        {
            worksheet.set_i32_at(old_col_num, row, old_value)?;
            worksheet.set_i32_at(new_col_num, row, new_value)?;
            let cached_value = old_value
                .zip(new_value)
                .map(|(old, new)| new.strict_sub(old));
            let cached_text = cached_value.map(|value| value.format_into(&mut cache_buffer));
            let formula = (!is_shared_follower).then(|| {
                formula_buffer.clear();
                append_fmt(
                    formula_buffer,
                    format_args!(
                        "IF(OR({old_col}{row_text}=\"\",{new_col}{row_text}=\"\"),\"\",{new_col}{row_text}-{old_col}{row_text})"
                    ),
                );
                formula_buffer.as_str()
            });
            worksheet.set_formula_at_with_cache(target_col, row, formula, cached_text, false)?;
        }
        Ok(())
    }
}
impl ChangeLogUpdater<'_, '_, '_, '_> {
    pub(super) fn update(&mut self) -> Result<u32> {
        self.worksheet
            .has_any_row_format(CHANGELOG_STYLE_TEMPLATE_ROW, CHANGELOG_COL_DELTA_DIESEL)
            .ok_or("변경내역 243행에 고정 style template이 없습니다.")?;
        let date_text = format!("현행화 일자: {}", self.today);
        self.shared_string_table
            .set_cell(self.worksheet, 1, 2, &date_text)?;
        let last_data_row = self
            .worksheet
            .row_numbers_from(CHANGELOG_DATA_START_ROW)
            .last;
        self.worksheet.clear_cells_in_rows_through_col(
            CHANGELOG_DATA_START_ROW,
            last_data_row,
            CHANGELOG_COL_DELTA_DIESEL,
        );
        let entry_count = self
            .changes
            .len()
            .strict_add(self.added.len())
            .strict_add(self.deleted.len());
        if entry_count == 0 {
            self.worksheet
                .truncate_rows_after(CHANGELOG_STYLE_TEMPLATE_ROW);
            return Ok(CHANGELOG_DATA_START_ROW);
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
        let formula_capacity = ROW_DECIMAL_TEXT_MAX_LEN
            .strict_mul(4)
            .strict_add("IF(OR(E=\"\",F=\"\"),\"\",F-E)".len());
        let mut formula_buffer =
            try_string_with_capacity(formula_capacity, "변경내역 delta formula 메모리 확보 실패")?;
        for (index, values) in change_entries
            .chain(added_entries)
            .chain(deleted_entries)
            .enumerate()
        {
            let row = add_row_offset(CHANGELOG_DATA_START_ROW, index, "변경내역 데이터 쓰기")?;
            if row > CHANGELOG_STYLE_TEMPLATE_ROW {
                self.worksheet.copy_row_style(
                    CHANGELOG_STYLE_TEMPLATE_ROW,
                    row,
                    CHANGELOG_COL_DELTA_DIESEL,
                )?;
            }
            self.worksheet.set_existing_cell_style_in_range(
                row,
                CHANGELOG_COL_OLD_GAS,
                CHANGELOG_COL_DELTA_DIESEL,
                26,
            )?;
            let is_shared_follower = is_shared_formula_follower(index, entry_count);
            values.write_to(
                self.worksheet,
                self.shared_string_table,
                row,
                is_shared_follower,
                &mut formula_buffer,
            )?;
        }
        let last_change_row = add_row_offset(
            CHANGELOG_DATA_START_ROW,
            entry_count.strict_sub(1),
            "변경내역 마지막 행 계산",
        )?;
        self.worksheet
            .truncate_rows_after(last_change_row.max(CHANGELOG_STYLE_TEMPLATE_ROW));
        Ok(last_change_row)
    }
}
