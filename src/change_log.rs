use crate::{
    diagnostic::{Result, add_row_offset, append_fmt, ensure, try_string_with_capacity},
    excel::{
        FuelValues, SourceRecord,
        writer::{SharedStringTable, Worksheet, is_shared_formula_follower},
    },
    master_sheet::{ChangeRow, StoreRow},
};
use core::fmt::NumBuffer;
const CHANGELOG_DATA_START_ROW: u32 = 4;
const CHANGELOG_STYLE_TEMPLATE_ROW: u32 = 243;
const CHANGELOG_COL_DELTA_DIESEL: u32 = 13;
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
impl ChangeLogUpdater<'_, '_, '_, '_> {
    pub(super) fn update(&mut self) -> Result<u32> {
        ensure(
            self.worksheet
                .has_any_row_format(CHANGELOG_STYLE_TEMPLATE_ROW, CHANGELOG_COL_DELTA_DIESEL),
            "변경내역 243행에 고정 style template이 없습니다.",
        )?;
        self.shared_string_table.set_cell(
            self.worksheet,
            1,
            2,
            &format!("현행화 일자: {}", self.today),
        )?;
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
        let rows = self
            .changes
            .iter()
            .map(|change| {
                (
                    change.record.region,
                    change.record.name.as_str(),
                    change.record.address.as_str(),
                    change.reason.as_str(),
                    change.old_fuels,
                    change.record.fuels,
                )
            })
            .chain(self.added.iter().map(|item| {
                (
                    item.region,
                    item.name.as_str(),
                    item.address.as_str(),
                    "신규",
                    FuelValues::default(),
                    item.fuels,
                )
            }))
            .chain(self.deleted.iter().map(|item| {
                (
                    item.region.as_str(),
                    item.name.as_str(),
                    item.address.as_str(),
                    "폐업",
                    item.fuels,
                    FuelValues::default(),
                )
            }));
        let mut formula_buffer =
            try_string_with_capacity(64, "변경내역 delta formula 메모리 확보 실패")?;
        let (mut row_buffer, mut cache_buffer) = (NumBuffer::new(), NumBuffer::new());
        for (index, (region, name, address, reason, old_fuels, new_fuels)) in rows.enumerate() {
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
                5,
                CHANGELOG_COL_DELTA_DIESEL,
                26,
            )?;
            for (col, value) in [(1, region), (2, name), (3, address), (4, reason)] {
                self.shared_string_table
                    .set_cell(self.worksheet, col, row, value)?;
            }
            let row_text = row.format_into(&mut row_buffer);
            let is_shared_follower = is_shared_formula_follower(index, entry_count);
            for (&(old_col_num, new_col_num, target_col, old_col, new_col), (old_val, new_val)) in
                FUEL_COLUMNS.iter().zip(
                    old_fuels
                        .into_array()
                        .into_iter()
                        .zip(new_fuels.into_array()),
                )
            {
                self.worksheet.set_i32_at(old_col_num, row, old_val)?;
                self.worksheet.set_i32_at(new_col_num, row, new_val)?;
                let cached = old_val
                    .zip(new_val)
                    .map(|(old, new)| new.strict_sub(old).format_into(&mut cache_buffer));
                let formula = (!is_shared_follower).then(|| {
                    formula_buffer.clear();
                    append_fmt(
                        &mut formula_buffer,
                        format_args!(
                            "IF(OR({old_col}{row_text}=\"\",{new_col}{row_text}=\"\"),\"\",{new_col}{row_text}-{old_col}{row_text})"
                        ),
                    );
                    formula_buffer.as_str()
                });
                self.worksheet
                    .set_formula_at_with_cache(target_col, row, formula, cached, false)?;
            }
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
