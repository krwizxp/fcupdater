#[path = "source_reader_biff.rs"]
pub mod biff;
const MAX_SOURCE_ROW: u32 = 200_000;
const MAX_SOURCE_COL: usize = 1_024;
const SOURCE_DATA_START_ROW: usize = 5;
const COL_REGION: usize = 1;
const COL_NAME: usize = 2;
const COL_ADDRESS: usize = 3;
const COL_BRAND: usize = 4;
const COL_SELF_YN: usize = 5;
const COL_PREMIUM: usize = 6;
const COL_GASOLINE: usize = 7;
const COL_DIESEL: usize = 8;
