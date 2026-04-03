pub const MAX_CONFLICT_SAMPLES: usize = 10;
#[derive(Debug, Clone)]
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
#[derive(Debug, Clone)]
pub struct SourceConflictSample {
    pub address: String,
    pub incoming_source: String,
    pub previous_source: String,
    pub selected_source: String,
}
#[derive(Debug, Clone, Default)]
pub struct SourceIndexBuildReport {
    pub duplicate_addresses: usize,
    pub replaced_entries: usize,
    pub samples: Vec<SourceConflictSample>,
}
