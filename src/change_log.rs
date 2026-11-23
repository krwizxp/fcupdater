use crate::{
    diagnostic::{Result, add_row_offset, append_fmt, ensure, str_cap},
    excel::{
        FuelValues, SourceRecord,
        writer::{SharedStringTable, Worksheet, is_shared_formula_follower},
    },
    master_sheet::{ChangeRow, StoreRow},
};
use core::fmt::NumBuffer;
const START_ROW: u32 = 4;
const TPL_ROW: u32 = 243;
const LAST_COL: u32 = 13;
const FUEL_COLS: [(u32, u32, u32, &str, &str); 3] = [
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
            self.worksheet.has_any_row_format(TPL_ROW, LAST_COL),
            "변경내역 243행 고정 style template 없음",
        )?;
        self.shared_string_table.set_cell(
            self.worksheet,
            1,
            2,
            &format!("현행화 일자: {}", self.today),
        )?;
        let last_row = self.worksheet.row_numbers_from(START_ROW).last;
        self.worksheet
            .clear_cells_in_rows_through_col(START_ROW, last_row, LAST_COL);
        let count = self
            .changes
            .len()
            .strict_add(self.added.len())
            .strict_add(self.deleted.len());
        if count == 0 {
            self.worksheet.truncate_rows_after(TPL_ROW);
            return Ok(START_ROW);
        }
        let rows = self
            .changes
            .iter()
            .map(|ch| {
                (
                    ch.record.region,
                    ch.record.name.as_str(),
                    ch.record.address.as_str(),
                    ch.reason.as_str(),
                    ch.old_fuels,
                    ch.record.fuels,
                )
            })
            .chain(self.added.iter().map(|it| {
                (
                    it.region,
                    it.name.as_str(),
                    it.address.as_str(),
                    "신규",
                    FuelValues::default(),
                    it.fuels,
                )
            }))
            .chain(self.deleted.iter().map(|it| {
                (
                    it.region.as_str(),
                    it.name.as_str(),
                    it.address.as_str(),
                    "폐업",
                    it.fuels,
                    FuelValues::default(),
                )
            }));
        let mut formula_buf = str_cap(64, "변경내역 delta formula")?;
        let (mut row_buf, mut cache_buf) = (NumBuffer::new(), NumBuffer::new());
        for (idx, (reg, name, addr, reason, old_f, new_f)) in rows.enumerate() {
            let row = add_row_offset(START_ROW, idx, "변경내역 쓰기")?;
            if row > TPL_ROW {
                self.worksheet.copy_row_style(TPL_ROW, row, LAST_COL)?;
            }
            self.worksheet
                .set_existing_cell_style_in_range(row, 5, LAST_COL, 26)?;
            for (col, val) in [(1, reg), (2, name), (3, addr), (4, reason)] {
                self.shared_string_table
                    .set_cell(self.worksheet, col, row, val)?;
            }
            let rt = row.format_into(&mut row_buf);
            let follower = is_shared_formula_follower(idx, count);
            for (&(oc, nc, tc, o_col, n_col), (ov, nv)) in FUEL_COLS
                .iter()
                .zip(old_f.into_array().into_iter().zip(new_f.into_array()))
            {
                self.worksheet.set_i32_at(oc, row, ov)?;
                self.worksheet.set_i32_at(nc, row, nv)?;
                let cached = ov
                    .zip(nv)
                    .map(|(old, new)| new.strict_sub(old).format_into(&mut cache_buf));
                let formula = (!follower).then(|| {
                    formula_buf.clear();
                    append_fmt(
                        &mut formula_buf,
                        format_args!(
                            "IF(OR({o_col}{rt}=\"\",{n_col}{rt}=\"\"),\"\",{n_col}{rt}-{o_col}{rt})"
                        ),
                    );
                    formula_buf.as_str()
                });
                self.worksheet
                    .set_formula_at_with_cache(tc, row, formula, cached, false)?;
            }
        }
        let last_change = add_row_offset(START_ROW, count.strict_sub(1), "변경내역 마지막 행")?;
        self.worksheet.truncate_rows_after(last_change.max(TPL_ROW));
        Ok(last_change)
    }
}
