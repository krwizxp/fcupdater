use crate::{Result, err_with_source, source_sync};
pub fn extend_source_records(
    out: &mut Vec<source_sync::SourceRecord>,
    records: Vec<source_sync::SourceRecord>,
) -> Result<()> {
    if !records.is_empty() {
        try_reserve_vec(
            out,
            records.len(),
            "xlsx 전체 소스 레코드 추가 메모리 확보 실패",
        )?;
        out.extend(records);
    }
    Ok(())
}
pub fn try_reserve_vec<T>(values: &mut Vec<T>, additional: usize, context: &str) -> Result<()> {
    values
        .try_reserve(additional)
        .map_err(|source| err_with_source(format!("{context}: {additional} entries"), source))
}
pub fn try_reserve_vec_exact<T>(
    values: &mut Vec<T>,
    additional: usize,
    context: &str,
) -> Result<()> {
    values
        .try_reserve_exact(additional)
        .map_err(|source| err_with_source(format!("{context}: {additional} entries"), source))
}
