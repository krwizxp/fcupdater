#[derive(Debug, Clone)]
pub struct ChangeRow {
    pub address: String,
    pub name: String,
    pub new_diesel: Option<i32>,
    pub new_gasoline: Option<i32>,
    pub new_premium: Option<i32>,
    pub old_diesel: Option<i32>,
    pub old_gasoline: Option<i32>,
    pub old_premium: Option<i32>,
    pub reason: String,
    pub region: String,
}
#[derive(Debug, Clone)]
pub struct StoreRow {
    pub address: String,
    pub diesel: Option<i32>,
    pub gasoline: Option<i32>,
    pub name: String,
    pub premium: Option<i32>,
    pub region: String,
}
