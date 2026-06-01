use std::env;
pub const APP_NAME: &str = "fcupdater";
pub const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
#[derive(Debug)]
pub enum ParseAction {
    Help(String),
    Run,
    Version(String),
}
pub fn usage_text() -> String {
    format!(
        "{APP_NAME} {APP_VERSION}\n주유소 가격/정보 현행화 (Excel 미설치 OK)\n\n\
사용법:\n  {APP_NAME}\n\n\
고정 동작:\n  마스터: fuel_cost_chungcheong.xlsx 직접 현행화\n  소스: Opinet 현재 판매가격(주유소) 자동 다운로드 .xls\n  변경내역 시트: 항상 갱신\n  저장 검증: 항상 수행\n\n\
옵션:\n  -h, --help               도움말\n  --version                버전\n\n\
설정값은 고정되어 있습니다."
    )
}
