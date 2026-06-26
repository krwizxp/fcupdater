use crate::region::TARGET_REGION_COUNT;
#[derive(Debug)]
pub struct SourceRecord {
    pub address: String,
    pub brand: String,
    pub diesel: Option<i32>,
    pub gasoline: Option<i32>,
    pub name: String,
    pub premium: Option<i32>,
    pub region: String,
    pub self_yn: String,
}
#[derive(Debug)]
pub struct AddedStoreRow<'source> {
    pub record: &'source SourceRecord,
    pub region: &'source str,
}
#[derive(Debug)]
pub struct ChangeRow<'source> {
    pub address: &'source str,
    pub name: &'source str,
    pub new_diesel: Option<i32>,
    pub new_gasoline: Option<i32>,
    pub new_premium: Option<i32>,
    pub old_diesel: Option<i32>,
    pub old_gasoline: Option<i32>,
    pub old_premium: Option<i32>,
    pub reason: String,
    pub region: String,
}
#[derive(Debug)]
pub struct StoreRow {
    pub address: String,
    pub diesel: Option<i32>,
    pub gasoline: Option<i32>,
    pub name: String,
    pub premium: Option<i32>,
    pub region: String,
}
pub struct MasterSheetUpdateResult<'source> {
    pub added: Vec<AddedStoreRow<'source>>,
    pub changes: Vec<ChangeRow<'source>>,
    pub deleted: Vec<StoreRow>,
    pub existing_count: usize,
    pub existing_region_counts: [usize; TARGET_REGION_COUNT],
}
