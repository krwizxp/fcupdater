use crate::{
    ChangeRow, Result, StoreRow, add_row_offset, canon_header, err,
    excel::writer::{Workbook as StdWorkbook, Worksheet, col_to_name},
};
use std::collections::HashMap;
struct ChangeLogLayout {
    data_start_row: u32,
    col_region: u32,
    col_name: u32,
    col_address: u32,
    col_reason: u32,
    col_old_gas: u32,
    col_new_gas: u32,
    col_delta_gas: Option<u32>,
    col_old_premium: u32,
    col_new_premium: u32,
    col_delta_premium: Option<u32>,
    col_old_diesel: u32,
    col_new_diesel: u32,
    col_delta_diesel: Option<u32>,
    max_col: u32,
}
#[derive(Debug, Clone)]
struct ChangeLogEntry {
    reason: String,
    region: String,
    name: String,
    address: String,
    old_gasoline: Option<i32>,
    new_gasoline: Option<i32>,
    old_premium: Option<i32>,
    new_premium: Option<i32>,
    old_diesel: Option<i32>,
    new_diesel: Option<i32>,
}
pub fn update_change_log_sheet(
    book: &mut StdWorkbook,
    today: &str,
    changes: &[ChangeRow],
    added: &[StoreRow],
    deleted: &[StoreRow],
) -> Result<()> {
    book.with_sheet_mut("변경내역", |ws, shared_strings| -> Result<()> {
        ws.set_string_at(1, 2, &format!("현행화 일자: {today}"));
        let layout = find_change_log_layout(ws, shared_strings)?;
        let style_template_row =
            pick_change_log_style_template_row(ws, layout.max_col, layout.data_start_row);
        if let Some(last_row) =
            find_last_change_log_data_row(ws, layout.data_start_row, &layout, shared_strings)
        {
            let mut cells_to_clear = Vec::new();
            for (row, row_obj) in ws.rows.range(layout.data_start_row..=last_row) {
                for col in row_obj.cells.keys().copied() {
                    if col <= layout.max_col {
                        cells_to_clear.push((col, *row));
                    }
                }
            }
            for (col, row) in cells_to_clear {
                ws.clear_cell_if_exists(col, row);
            }
        }
        let entries = build_change_log_entries(changes, added, deleted);
        let old_gas_col = col_to_name(layout.col_old_gas);
        let new_gas_col = col_to_name(layout.col_new_gas);
        let old_premium_col = col_to_name(layout.col_old_premium);
        let new_premium_col = col_to_name(layout.col_new_premium);
        let old_diesel_col = col_to_name(layout.col_old_diesel);
        let new_diesel_col = col_to_name(layout.col_new_diesel);
        for (i, entry) in entries.iter().enumerate() {
            let row = add_row_offset(layout.data_start_row, i, "변경내역 데이터 쓰기")?;
            if row > style_template_row {
                ws.clone_row_style(style_template_row, row, layout.max_col);
            }
            ws.set_string_at(layout.col_region, row, &entry.region);
            ws.set_string_at(layout.col_name, row, &entry.name);
            ws.set_string_at(layout.col_address, row, &entry.address);
            ws.set_string_at(layout.col_reason, row, &entry.reason);
            ws.set_i32_at(layout.col_old_gas, row, entry.old_gasoline);
            ws.set_i32_at(layout.col_new_gas, row, entry.new_gasoline);
            ws.set_i32_at(layout.col_old_premium, row, entry.old_premium);
            ws.set_i32_at(layout.col_new_premium, row, entry.new_premium);
            ws.set_i32_at(layout.col_old_diesel, row, entry.old_diesel);
            ws.set_i32_at(layout.col_new_diesel, row, entry.new_diesel);
            if let Some(col) = layout.col_delta_gas {
                ws.set_formula_at(
                    col,
                    row,
                    &format!(
                        "IF(OR({old_gas_col}{row}=\"\",{new_gas_col}{row}=\"\"),\"\",{new_gas_col}{row}-{old_gas_col}{row})"
                    ),
                );
            }
            if let Some(col) = layout.col_delta_premium {
                ws.set_formula_at(
                    col,
                    row,
                    &format!(
                        "IF(OR({old_premium_col}{row}=\"\",{new_premium_col}{row}=\"\"),\"\",{new_premium_col}{row}-{old_premium_col}{row})"
                    ),
                );
            }
            if let Some(col) = layout.col_delta_diesel {
                ws.set_formula_at(
                    col,
                    row,
                    &format!(
                        "IF(OR({old_diesel_col}{row}=\"\",{new_diesel_col}{row}=\"\"),\"\",{new_diesel_col}{row}-{old_diesel_col}{row})"
                    ),
                );
            }
        }
        if !entries.is_empty() {
            let last_change_row = add_row_offset(
                layout.data_start_row,
                entries.len().saturating_sub(1),
                "변경내역 마지막 행 계산",
            )?;
            let mut target_cols = Vec::new();
            if let Some(col) = layout.col_delta_gas {
                target_cols.push(col);
            }
            if let Some(col) = layout.col_delta_premium {
                target_cols.push(col);
            }
            if let Some(col) = layout.col_delta_diesel {
                target_cols.push(col);
            }
            ws.extend_conditional_formats(last_change_row, &target_cols, layout.data_start_row)?;
        }
        ws.update_dimension()?;
        Ok(())
    })
    .ok_or_else(|| err("마스터 파일에 '변경내역' 시트가 없습니다"))?
}
fn build_change_log_entries(
    changes: &[ChangeRow],
    added: &[StoreRow],
    deleted: &[StoreRow],
) -> Vec<ChangeLogEntry> {
    let mut out = Vec::with_capacity(changes.len() + added.len() + deleted.len());
    for ch in changes {
        out.push(ChangeLogEntry {
            reason: ch.reason.clone(),
            region: ch.region.clone(),
            name: ch.name.clone(),
            address: ch.address.clone(),
            old_gasoline: ch.old_gasoline,
            new_gasoline: ch.new_gasoline,
            old_premium: ch.old_premium,
            new_premium: ch.new_premium,
            old_diesel: ch.old_diesel,
            new_diesel: ch.new_diesel,
        });
    }
    for item in added {
        out.push(ChangeLogEntry {
            reason: "신규".to_string(),
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
        out.push(ChangeLogEntry {
            reason: "폐업".to_string(),
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
    out
}
fn find_change_log_layout(ws: &Worksheet, shared_strings: &[String]) -> Result<ChangeLogLayout> {
    let max_rows = change_log_header_scan_rows();
    let max_cols = change_log_header_scan_cols();
    for row in 1..=max_rows {
        let mut headers: HashMap<String, u32> = HashMap::new();
        for col in 1..=max_cols {
            let key = canon_header(ws.get_display_at(col, row, shared_strings).trim());
            if key.is_empty() {
                continue;
            }
            headers.entry(key).or_insert(col);
        }
        if headers.is_empty() {
            continue;
        }
        let Some(col_region) = get_header_col_optional(&headers, &["지역"]) else {
            continue;
        };
        let Some(col_name) = get_header_col_optional(&headers, &["상호"]) else {
            continue;
        };
        let Some(col_address) = get_header_col_optional(&headers, &["주소"]) else {
            continue;
        };
        let Some(col_reason) =
            get_header_col_optional(&headers, &["변경내용", "변경내역", "변경사유"])
        else {
            continue;
        };
        let col_old_gas =
            get_header_col_required(&headers, &["휘발유(이전)", "휘발유이전"], "휘발유(이전)")?;
        let col_new_gas =
            get_header_col_required(&headers, &["휘발유(신규)", "휘발유신규"], "휘발유(신규)")?;
        let col_old_premium =
            get_header_col_required(&headers, &["고급유(이전)", "고급유이전"], "고급유(이전)")?;
        let col_new_premium =
            get_header_col_required(&headers, &["고급유(신규)", "고급유신규"], "고급유(신규)")?;
        let col_old_diesel =
            get_header_col_required(&headers, &["경유(이전)", "경유이전"], "경유(이전)")?;
        let col_new_diesel =
            get_header_col_required(&headers, &["경유(신규)", "경유신규"], "경유(신규)")?;
        let col_delta_gas = get_header_col_optional(
            &headers,
            &["휘발유Δ", "휘발유△", "휘발유증감", "휘발유차이"],
        );
        let col_delta_premium = get_header_col_optional(
            &headers,
            &["고급유Δ", "고급유△", "고급유증감", "고급유차이"],
        );
        let col_delta_diesel =
            get_header_col_optional(&headers, &["경유Δ", "경유△", "경유증감", "경유차이"]);
        let mut max_col = col_region
            .max(col_name)
            .max(col_address)
            .max(col_reason)
            .max(col_old_gas)
            .max(col_new_gas)
            .max(col_old_premium)
            .max(col_new_premium)
            .max(col_old_diesel)
            .max(col_new_diesel);
        if let Some(col) = col_delta_gas {
            max_col = max_col.max(col);
        }
        if let Some(col) = col_delta_premium {
            max_col = max_col.max(col);
        }
        if let Some(col) = col_delta_diesel {
            max_col = max_col.max(col);
        }
        return Ok(ChangeLogLayout {
            data_start_row: row + 1,
            col_region,
            col_name,
            col_address,
            col_reason,
            col_old_gas,
            col_new_gas,
            col_delta_gas,
            col_old_premium,
            col_new_premium,
            col_delta_premium,
            col_old_diesel,
            col_new_diesel,
            col_delta_diesel,
            max_col,
        });
    }
    Err(err(
        "변경내역 시트에서 헤더 행을 찾지 못했습니다. 필수 컬럼(지역/상호/주소/변경내용/휘발유(이전)/휘발유(신규)/고급유(이전)/고급유(신규)/경유(이전)/경유(신규))을 확인하세요.",
    ))
}
fn get_header_col_required(
    headers: &HashMap<String, u32>,
    keys: &[&str],
    display_name: &str,
) -> Result<u32> {
    get_header_col_optional(headers, keys)
        .ok_or_else(|| err(format!("변경내역 헤더에 '{display_name}' 컬럼이 없습니다.")))
}
fn get_header_col_optional(headers: &HashMap<String, u32>, keys: &[&str]) -> Option<u32> {
    for key in keys {
        let canon = canon_header(key);
        if let Some(col) = headers.get(&canon) {
            return Some(*col);
        }
    }
    None
}
fn row_has_change_log_data(
    ws: &Worksheet,
    row: u32,
    layout: &ChangeLogLayout,
    shared_strings: &[String],
) -> bool {
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
    ws.row_has_any_data(row, &cols, shared_strings)
}
fn find_last_change_log_data_row(
    ws: &Worksheet,
    data_start_row: u32,
    layout: &ChangeLogLayout,
    shared_strings: &[String],
) -> Option<u32> {
    let mut last = None;
    for row in ws.rows.range(data_start_row..).map(|(row, _)| *row) {
        if row_has_change_log_data(ws, row, layout, shared_strings) {
            last = Some(row);
        }
    }
    last
}
fn pick_change_log_style_template_row(ws: &Worksheet, max_col: u32, data_start_row: u32) -> u32 {
    let preferred_row = change_log_style_template_row();
    if preferred_row >= data_start_row && ws.has_any_row_format(preferred_row, max_col) {
        return preferred_row;
    }
    let end = if preferred_row > data_start_row {
        preferred_row
    } else {
        data_start_row + 1
    };
    for row in (data_start_row..end).rev() {
        if ws.has_any_row_format(row, max_col) {
            return row;
        }
    }
    data_start_row
}
fn change_log_header_scan_rows() -> u32 {
    std::env::var("FCUPDATER_CHANGELOG_HEADER_SCAN_ROWS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .map_or(30, |v| v.min(1_000))
}
fn change_log_header_scan_cols() -> u32 {
    std::env::var("FCUPDATER_CHANGELOG_HEADER_SCAN_COLS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .map_or(60, |v| v.min(500))
}
fn change_log_style_template_row() -> u32 {
    std::env::var("FCUPDATER_CHANGELOG_STYLE_TEMPLATE_ROW")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(243)
}
