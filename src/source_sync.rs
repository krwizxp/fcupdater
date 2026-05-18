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
