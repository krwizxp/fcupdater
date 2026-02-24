use std::{
    collections::{HashMap, HashSet},
    env,
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
    process,
};
mod std_only;
use std_only::writer::{Row as StdRow, Workbook as StdWorkbook, col_to_name, remap_row_numbers};
type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Result<T> = std::result::Result<T, BoxError>;
const APP_NAME: &str = "fcupdater";
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
fn err(msg: impl Into<String>) -> BoxError {
    std::io::Error::other(msg.into()).into()
}
#[derive(Debug, Clone)]
struct Args {
    master: PathBuf,
    sources_dir: PathBuf,
    sources_prefix: String,
    output: Option<PathBuf>,
    in_place: bool,
    no_change_log: bool,
    dry_run: bool,
}
impl Default for Args {
    fn default() -> Self {
        Self {
            master: PathBuf::from("fuel_cost_chungcheong.xlsx"),
            sources_dir: PathBuf::from("."),
            sources_prefix: "지역_위치별(주유소)".to_string(),
            output: None,
            in_place: false,
            no_change_log: false,
            dry_run: false,
        }
    }
}
impl Args {
    fn parse() -> Result<Self> {
        parse_args(env::args_os().skip(1).collect())
    }
}
fn parse_args(raw_args: Vec<OsString>) -> Result<Args> {
    let mut args = Args::default();
    let mut i = 0usize;
    while i < raw_args.len() {
        let token = raw_args[i].to_string_lossy().to_string();
        match token.as_str() {
            "-h" | "--help" => {
                println!("{}", usage_text());
                process::exit(0);
            }
            "--version" => {
                println!("{APP_NAME} {APP_VERSION}");
                process::exit(0);
            }
            "--in-place" => args.in_place = true,
            "--no-change-log" => args.no_change_log = true,
            "--dry-run" => args.dry_run = true,
            "--master" => {
                let value = take_option_value(&raw_args, &mut i, "--master")?;
                args.master = PathBuf::from(value);
            }
            "--sources-dir" => {
                let value = take_option_value(&raw_args, &mut i, "--sources-dir")?;
                args.sources_dir = PathBuf::from(value);
            }
            "--sources-prefix" => {
                let value = take_option_value(&raw_args, &mut i, "--sources-prefix")?;
                args.sources_prefix = value.to_string_lossy().to_string();
            }
            "--output" => {
                let value = take_option_value(&raw_args, &mut i, "--output")?;
                args.output = Some(PathBuf::from(value));
            }
            _ => {
                if let Some(v) = token.strip_prefix("--master=") {
                    args.master = PathBuf::from(v);
                } else if let Some(v) = token.strip_prefix("--sources-dir=") {
                    args.sources_dir = PathBuf::from(v);
                } else if let Some(v) = token.strip_prefix("--sources-prefix=") {
                    args.sources_prefix = v.to_string();
                } else if let Some(v) = token.strip_prefix("--output=") {
                    args.output = Some(PathBuf::from(v));
                } else {
                    return Err(err(format!("알 수 없는 옵션: {token}\n\n{}", usage_text())));
                }
            }
        }
        i += 1;
    }
    if args.in_place && args.output.is_some() {
        return Err(err("--in-place 와 --output 은 동시에 사용할 수 없습니다."));
    }
    Ok(args)
}
fn take_option_value(raw_args: &[OsString], i: &mut usize, opt_name: &str) -> Result<OsString> {
    *i += 1;
    let Some(value) = raw_args.get(*i) else {
        return Err(err(format!("{opt_name} 옵션에 값이 필요합니다.")));
    };
    if value.to_string_lossy().starts_with("--") {
        return Err(err(format!(
            "{opt_name} 옵션에 값이 필요합니다. (다음 토큰: {})",
            value.to_string_lossy()
        )));
    }
    Ok(value.clone())
}
fn usage_text() -> String {
    format!(
        "{APP_NAME} {APP_VERSION}\n주유소 가격/정보 현행화 (Excel 미설치 OK)\n\n\
사용법:\n  {APP_NAME} [OPTIONS]\n\n\
옵션:\n  --master <PATH>          마스터 파일 경로 (기본: fuel_cost_chungcheong.xlsx)\n  --sources-dir <PATH>     소스 폴더 (기본: .)\n  --sources-prefix <TEXT>  소스 파일 prefix (기본: 지역_위치별(주유소))\n  --output <PATH>          출력 파일 경로\n  --in-place               마스터 파일 덮어쓰기(백업 생성)\n  --no-change-log          변경내역 시트 갱신 안 함\n  --dry-run                파일 저장 없이 요약만 출력\n  -h, --help               도움말\n  --version                버전"
    )
}
fn local_today_yyyy_mm_dd() -> Result<String> {
    #[cfg(windows)]
    let output = process::Command::new("powershell")
        .args([
            "-NoProfile",
            "-Command",
            "(Get-Date).ToString('yyyy-MM-dd')",
        ])
        .output()
        .map_err(|e| err(format!("오늘 날짜 조회 실패: {e}")))?;
    #[cfg(not(windows))]
    let output = process::Command::new("date")
        .arg("+%F")
        .output()
        .map_err(|e| err(format!("오늘 날짜 조회 실패: {e}")))?;
    if !output.status.success() {
        return Err(err(format!(
            "오늘 날짜 조회 실패: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
    }
    let s = String::from_utf8(output.stdout)
        .map_err(|e| err(format!("오늘 날짜 결과 파싱 실패: {e}")))?;
    let today = s.trim();
    if !is_yyyy_mm_dd(today) {
        return Err(err(format!("오늘 날짜 형식이 올바르지 않습니다: {today}")));
    }
    Ok(today.to_string())
}
fn is_yyyy_mm_dd(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 10 {
        return false;
    }
    if b[4] != b'-' || b[7] != b'-' {
        return false;
    }
    b.iter().enumerate().all(|(i, ch)| {
        if i == 4 || i == 7 {
            true
        } else {
            ch.is_ascii_digit()
        }
    })
}
#[derive(Debug, Clone)]
struct SourceRecord {
    region: String,
    name: String,
    brand: String,
    self_yn: String,
    address: String,
    phone: String,
    gasoline: Option<i32>,
    premium: Option<i32>,
    diesel: Option<i32>,
}
#[derive(Debug, Clone)]
struct ChangeRow {
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
#[derive(Debug, Clone)]
struct StoreRow {
    region: String,
    name: String,
    address: String,
    gasoline: Option<i32>,
    premium: Option<i32>,
    diesel: Option<i32>,
}
trait UpdaterBackend {
    type Workbook;
    fn build_source_index(&self, paths: &[PathBuf]) -> Result<HashMap<String, SourceRecord>>;
    fn read_master(&self, path: &Path) -> Result<Self::Workbook>;
    fn update_master_sheet(
        &self,
        book: &mut Self::Workbook,
        source_index: &HashMap<String, SourceRecord>,
    ) -> Result<(Vec<ChangeRow>, Vec<StoreRow>, Vec<StoreRow>)>;
    fn update_change_log_sheet(
        &self,
        book: &mut Self::Workbook,
        today: &str,
        changes: &[ChangeRow],
        added: &[StoreRow],
        deleted: &[StoreRow],
    ) -> Result<()>;
    fn write_master(&self, book: &mut Self::Workbook, path: &Path) -> Result<()>;
}
struct StdBackend;
impl UpdaterBackend for StdBackend {
    type Workbook = StdWorkbook;
    fn build_source_index(&self, paths: &[PathBuf]) -> Result<HashMap<String, SourceRecord>> {
        build_source_index(paths)
    }
    fn read_master(&self, path: &Path) -> Result<Self::Workbook> {
        StdWorkbook::open(path)
    }
    fn update_master_sheet(
        &self,
        book: &mut Self::Workbook,
        source_index: &HashMap<String, SourceRecord>,
    ) -> Result<(Vec<ChangeRow>, Vec<StoreRow>, Vec<StoreRow>)> {
        update_master_sheet(book, source_index)
    }
    fn update_change_log_sheet(
        &self,
        book: &mut Self::Workbook,
        today: &str,
        changes: &[ChangeRow],
        added: &[StoreRow],
        deleted: &[StoreRow],
    ) -> Result<()> {
        update_change_log_sheet(book, today, changes, added, deleted)
    }
    fn write_master(&self, book: &mut Self::Workbook, path: &Path) -> Result<()> {
        book.save_as(path)
    }
}
fn main() -> Result<()> {
    let args = Args::parse()?;
    run(args)
}
fn run(args: Args) -> Result<()> {
    let backend = StdBackend;
    run_with_backend(args, &backend)
}
fn run_with_backend<B: UpdaterBackend>(args: Args, backend: &B) -> Result<()> {
    if !args.master.exists() {
        return Err(err(format!(
            "마스터 파일이 없습니다: {} (같은 폴더에 두거나 --master로 경로를 지정하세요)",
            args.master.display()
        )));
    }
    let source_paths = find_source_files(&args.sources_dir, &args.sources_prefix)?;
    if source_paths.is_empty() {
        return Err(err(format!(
            "소스 파일을 찾지 못했습니다. 폴더: {} / prefix: {} / 확장자: .xls,.xlsx",
            args.sources_dir.display(),
            args.sources_prefix
        )));
    }
    let source_index = backend.build_source_index(&source_paths)?;
    let mut book = backend.read_master(&args.master)?;
    let (changes, added, deleted) = backend.update_master_sheet(&mut book, &source_index)?;
    let today = local_today_yyyy_mm_dd()?;
    if !args.no_change_log {
        backend.update_change_log_sheet(&mut book, &today, &changes, &added, &deleted)?;
    }
    let out_path = decide_output_path(&args, &today)?;
    if !args.dry_run {
        if args.in_place {
            let backup = make_backup_path(&args.master, &today);
            fs::copy(&args.master, &backup).map_err(|e| {
                err(format!(
                    "백업 파일 생성에 실패했습니다: {} -> {} ({e})",
                    args.master.display(),
                    backup.display()
                ))
            })?;
            eprintln!("[백업 생성] {}", backup.display());
        }
        backend.write_master(&mut book, &out_path)?;
    }
    print_summary(
        &args,
        &out_path,
        source_paths.len(),
        &changes,
        &added,
        &deleted,
    );
    Ok(())
}
fn decide_output_path(args: &Args, today: &str) -> Result<PathBuf> {
    if args.in_place {
        return Ok(args.master.clone());
    }
    if let Some(p) = &args.output {
        return Ok(p.clone());
    }
    let stem = args
        .master
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("fuel_cost_chungcheong");
    let parent = args.master.parent().unwrap_or(Path::new("."));
    Ok(parent.join(format!("{stem}_updated_{today}.xlsx")))
}
fn make_backup_path(master: &Path, today: &str) -> PathBuf {
    let parent = master.parent().unwrap_or(Path::new("."));
    let stem = master
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("fuel_cost_chungcheong");
    parent.join(format!("{stem}_backup_{today}.xlsx"))
}
fn find_source_files(dir: &Path, prefix: &str) -> Result<Vec<PathBuf>> {
    let mut out = vec![];
    for entry in
        fs::read_dir(dir).map_err(|e| err(format!("폴더 읽기 실패: {} ({e})", dir.display())))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let file_name = match path.file_name().and_then(|s| s.to_str()) {
            Some(v) => v,
            None => continue,
        };
        if !file_name.starts_with(prefix) {
            continue;
        }
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("");
        if ext.eq_ignore_ascii_case("xls") || ext.eq_ignore_ascii_case("xlsx") {
            out.push(path);
        }
    }
    out.sort();
    Ok(out)
}
fn build_source_index(paths: &[PathBuf]) -> Result<HashMap<String, SourceRecord>> {
    let mut map: HashMap<String, SourceRecord> = HashMap::new();
    for path in paths {
        let recs = read_source_file(path)
            .map_err(|e| err(format!("소스 파일 읽기 실패: {} ({e})", path.display())))?;
        for rec in recs {
            let key = normalize_address_key(&rec.address);
            match map.get(&key) {
                None => {
                    map.insert(key, rec);
                }
                Some(existing) => {
                    let score = count_prices(&rec);
                    let prev = count_prices(existing);
                    if score > prev {
                        map.insert(key, rec);
                    }
                }
            }
        }
    }
    Ok(map)
}
fn count_prices(rec: &SourceRecord) -> usize {
    [rec.gasoline, rec.premium, rec.diesel]
        .iter()
        .filter(|v| v.is_some())
        .count()
}
fn read_source_file(path: &Path) -> Result<Vec<SourceRecord>> {
    std_only::source_reader::read_source_file(path)
}
#[derive(Debug, Clone)]
struct KeptMasterRow {
    new_row: u32,
    src: Option<SourceRecord>,
}
fn update_master_sheet(
    book: &mut StdWorkbook,
    source_index: &HashMap<String, SourceRecord>,
) -> Result<(Vec<ChangeRow>, Vec<StoreRow>, Vec<StoreRow>)> {
    let shared_strings = book.shared_strings().to_vec();
    let mut changes: Vec<ChangeRow> = vec![];
    let mut added: Vec<StoreRow> = vec![];
    let mut deleted: Vec<StoreRow> = vec![];
    let (filter_start_row, filter_end_row) = {
        let ws = book
            .sheet_mut("유류비")
            .ok_or_else(|| err("마스터 파일에 '유류비' 시트가 없습니다"))?;
        let mut header_row: Option<u32> = None;
        for r in 1..=200u32 {
            let a = ws.get_display_at(1, r, &shared_strings).trim().to_string();
            if a == "지역화폐적용순위" {
                header_row = Some(r);
                break;
            }
        }
        let header_row =
            header_row.ok_or_else(|| err("유류비 시트에서 헤더 행을 찾지 못했습니다"))?;
        let data_start_row = header_row + 1;
        let mut old_rows: Vec<u32> = vec![];
        let mut row = data_start_row;
        loop {
            let region = ws
                .get_display_at(2, row, &shared_strings)
                .trim()
                .to_string();
            let name = ws
                .get_display_at(3, row, &shared_strings)
                .trim()
                .to_string();
            let addr = ws
                .get_display_at(6, row, &shared_strings)
                .trim()
                .to_string();
            if region.is_empty() && name.is_empty() && addr.is_empty() {
                break;
            }
            old_rows.push(row);
            row += 1;
            if row > 20000 {
                return Err(err("유류비 데이터 행 범위가 비정상적으로 큽니다."));
            }
        }
        let old_end_row = old_rows
            .last()
            .copied()
            .unwrap_or(data_start_row.saturating_sub(1));
        let filter_start_row = data_start_row;
        let original_rows = ws.rows.clone();
        let mut matched_source_keys: HashSet<String> = HashSet::new();
        let mut kept_source_rows: Vec<(u32, Option<SourceRecord>)> = Vec::new();
        for old_row in &old_rows {
            let old_row = *old_row;
            let region = ws
                .get_display_at(2, old_row, &shared_strings)
                .trim()
                .to_string();
            let name = ws
                .get_display_at(3, old_row, &shared_strings)
                .trim()
                .to_string();
            let addr = ws
                .get_display_at(6, old_row, &shared_strings)
                .trim()
                .to_string();
            if addr.is_empty() {
                kept_source_rows.push((old_row, None));
                continue;
            }
            let key = normalize_address_key(&addr);
            let Some(src) = source_index.get(&key).cloned() else {
                deleted.push(StoreRow {
                    region,
                    name,
                    address: addr,
                    gasoline: ws.get_i32_at(8, old_row, &shared_strings),
                    premium: ws.get_i32_at(9, old_row, &shared_strings),
                    diesel: ws.get_i32_at(11, old_row, &shared_strings),
                });
                continue;
            };
            matched_source_keys.insert(key);
            let old_brand = ws
                .get_display_at(4, old_row, &shared_strings)
                .trim()
                .to_string();
            let old_self_yn = ws
                .get_display_at(5, old_row, &shared_strings)
                .trim()
                .to_string();
            let old_phone = ws
                .get_display_at(7, old_row, &shared_strings)
                .trim()
                .to_string();
            let old_gas = ws.get_i32_at(8, old_row, &shared_strings);
            let old_premium = ws.get_i32_at(9, old_row, &shared_strings);
            let old_diesel = ws.get_i32_at(11, old_row, &shared_strings);
            let name_changed = !same_trimmed(&name, &src.name);
            let brand_changed = !same_trimmed(&old_brand, &src.brand);
            let self_yn_changed = !same_self_yn(&old_self_yn, &src.self_yn);
            let address_changed =
                normalize_address_key(&addr) != normalize_address_key(&src.address);
            let phone_changed = !same_phone(&old_phone, &src.phone);
            let gas_changed = old_gas != src.gasoline;
            let premium_changed = old_premium != src.premium;
            let diesel_changed = old_diesel != src.diesel;
            if name_changed
                || brand_changed
                || self_yn_changed
                || address_changed
                || phone_changed
                || gas_changed
                || premium_changed
                || diesel_changed
            {
                let mut reasons: Vec<&str> = vec![];
                if gas_changed || premium_changed || diesel_changed {
                    reasons.push("가격변동");
                }
                if name_changed {
                    reasons.push("상호변경");
                }
                if brand_changed {
                    reasons.push("상표변경");
                }
                if self_yn_changed {
                    reasons.push("셀프여부변경");
                }
                if address_changed {
                    reasons.push("주소변경");
                }
                if phone_changed {
                    reasons.push("전화번호변경");
                }
                changes.push(ChangeRow {
                    reason: reasons.join(", "),
                    region,
                    name: src.name.clone(),
                    address: src.address.clone(),
                    old_gasoline: old_gas,
                    new_gasoline: src.gasoline,
                    old_premium,
                    new_premium: src.premium,
                    old_diesel,
                    new_diesel: src.diesel,
                });
            }
            kept_source_rows.push((old_row, Some(src)));
        }
        let mut new_sources: Vec<SourceRecord> = source_index
            .iter()
            .filter_map(|(key, rec)| (!matched_source_keys.contains(key)).then_some(rec.clone()))
            .collect();
        new_sources.sort_by(|a, b| {
            a.region
                .cmp(&b.region)
                .then(a.name.cmp(&b.name))
                .then(a.address.cmp(&b.address))
        });
        for src in &new_sources {
            added.push(StoreRow {
                region: src.region.clone(),
                name: src.name.clone(),
                address: src.address.clone(),
                gasoline: src.gasoline,
                premium: src.premium,
                diesel: src.diesel,
            });
        }
        let kept_old_rows: HashSet<u32> = kept_source_rows.iter().map(|(r, _)| *r).collect();
        let mut deleted_rows: Vec<u32> = old_rows
            .iter()
            .copied()
            .filter(|r| !kept_old_rows.contains(r))
            .collect();
        deleted_rows.sort();
        let old_count = old_rows.len();
        let final_count = kept_source_rows.len() + new_sources.len();
        let delta = final_count as i64 - old_count as i64;
        let generic_map = |old_ref_row: u32| -> u32 {
            if old_count > 0 && old_ref_row >= data_start_row && old_ref_row <= old_end_row {
                let deleted_le = count_deleted_le(&deleted_rows, old_ref_row) as u32;
                return old_ref_row.saturating_sub(deleted_le);
            }
            if old_ref_row > old_end_row {
                let shifted = old_ref_row as i64 + delta;
                return if shifted < 1 { 1 } else { shifted as u32 };
            }
            old_ref_row
        };
        let template_row_num = old_rows.last().copied().unwrap_or(data_start_row);
        let template_row = original_rows
            .get(&template_row_num)
            .cloned()
            .unwrap_or_else(|| default_row(template_row_num));
        let mut new_rows_map = std::collections::BTreeMap::new();
        for (r, row_obj) in &original_rows {
            let r = *r;
            if old_count > 0 && r >= data_start_row && r <= old_end_row {
                continue;
            }
            let mut row_obj = row_obj.clone();
            if r < data_start_row {
                remap_row_numbers(&mut row_obj, r, &generic_map);
                new_rows_map.insert(r, row_obj);
            } else {
                let shifted = (r as i64 + delta).max(1) as u32;
                remap_row_numbers(&mut row_obj, shifted, &generic_map);
                new_rows_map.insert(shifted, row_obj);
            }
        }
        let mut kept_rows: Vec<KeptMasterRow> = vec![];
        for (i, (old_row, src)) in kept_source_rows.iter().enumerate() {
            let new_row = data_start_row + i as u32;
            let mut row_obj = original_rows
                .get(old_row)
                .cloned()
                .unwrap_or_else(|| default_row(*old_row));
            let old_row_value = *old_row;
            let resolver = |old_ref_row: u32| {
                if old_ref_row == old_row_value {
                    new_row
                } else {
                    generic_map(old_ref_row)
                }
            };
            remap_row_numbers(&mut row_obj, new_row, &resolver);
            new_rows_map.insert(new_row, row_obj);
            kept_rows.push(KeptMasterRow {
                new_row,
                src: src.clone(),
            });
        }
        let mut new_rows_from_sources: Vec<(u32, SourceRecord)> = vec![];
        for (i, src) in new_sources.iter().cloned().enumerate() {
            let new_row = data_start_row + kept_source_rows.len() as u32 + i as u32;
            let mut row_obj = template_row.clone();
            let resolver = |old_ref_row: u32| {
                if old_ref_row == template_row_num {
                    new_row
                } else {
                    generic_map(old_ref_row)
                }
            };
            remap_row_numbers(&mut row_obj, new_row, &resolver);
            new_rows_map.insert(new_row, row_obj);
            new_rows_from_sources.push((new_row, src));
        }
        ws.rows = new_rows_map;
        for plan in &kept_rows {
            if let Some(src) = &plan.src {
                write_master_row_from_source(ws, plan.new_row, src);
            }
        }
        for (new_row, src) in &new_rows_from_sources {
            write_master_row_from_source(ws, *new_row, src);
            let region_cell = ws.get_display_at(2, *new_row, &shared_strings);
            if region_cell.trim().is_empty() && !src.region.trim().is_empty() {
                ws.set_string_at(2, *new_row, &src.region);
            }
        }
        let computed_end = if final_count == 0 {
            data_start_row
        } else {
            data_start_row + final_count as u32 - 1
        };
        let filter_end_row = computed_end;
        ws.update_dimension();
        (filter_start_row, filter_end_row)
    };
    if filter_start_row > 0 && filter_end_row > 0 {
        update_filter_database_defined_name(
            book.workbook_xml_mut(),
            filter_start_row,
            filter_end_row,
        );
    }
    Ok((changes, added, deleted))
}
fn count_deleted_le(sorted_deleted_rows: &[u32], row: u32) -> usize {
    match sorted_deleted_rows.binary_search(&row) {
        Ok(idx) => idx + 1,
        Err(idx) => idx,
    }
}
fn default_row(row_num: u32) -> StdRow {
    StdRow {
        attrs: vec![("r".to_string(), row_num.to_string())],
        cells: std::collections::BTreeMap::new(),
    }
}
fn update_filter_database_defined_name(
    workbook_xml: &mut String,
    data_start_row: u32,
    data_end_row: u32,
) {
    let marker = "_xlnm._FilterDatabase";
    let Some(marker_pos) = workbook_xml.find(marker) else {
        return;
    };
    let after_marker = &workbook_xml[marker_pos..];
    let Some(open_end_rel) = after_marker.find('>') else {
        return;
    };
    let content_start = marker_pos + open_end_rel + 1;
    let Some(close_rel) = workbook_xml[content_start..].find("</definedName>") else {
        return;
    };
    let content_end = content_start + close_rel;
    let replacement = format!("유류비!$A${}:$W${}", data_start_row, data_end_row);
    workbook_xml.replace_range(content_start..content_end, &replacement);
}
fn write_master_row_from_source(
    ws: &mut std_only::writer::Worksheet,
    row: u32,
    src: &SourceRecord,
) {
    ws.set_string_at(3, row, &src.name);
    ws.set_string_at(4, row, &src.brand);
    ws.set_string_at(5, row, &src.self_yn);
    ws.set_string_at(6, row, &src.address);
    ws.set_string_at(7, row, &src.phone);
    ws.set_i32_at(8, row, src.gasoline);
    ws.set_i32_at(9, row, src.premium);
    ws.set_i32_at(11, row, src.diesel);
}
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
fn update_change_log_sheet(
    book: &mut StdWorkbook,
    today: &str,
    changes: &[ChangeRow],
    added: &[StoreRow],
    deleted: &[StoreRow],
) -> Result<()> {
    let shared_strings = book.shared_strings().to_vec();
    let ws = book
        .sheet_mut("변경내역")
        .ok_or_else(|| err("마스터 파일에 '변경내역' 시트가 없습니다"))?;
    ws.set_string_at(1, 2, &format!("현행화 일자: {today}"));
    let layout = find_change_log_layout(ws, &shared_strings)?;
    let style_template_row =
        pick_change_log_style_template_row(ws, layout.max_col, layout.data_start_row);
    let mut r = layout.data_start_row;
    loop {
        if !row_has_change_log_data(ws, r, &layout, &shared_strings) {
            break;
        }
        for col in 1..=layout.max_col {
            ws.set_blank_at(col, r);
        }
        r += 1;
        if r > 5000 {
            break;
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
        let row = layout.data_start_row + i as u32;
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
        let last_change_row = layout.data_start_row + entries.len() as u32 - 1;
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
        ws.extend_conditional_formats(last_change_row, &target_cols, layout.data_start_row);
    }
    ws.update_dimension();
    Ok(())
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
fn find_change_log_layout(
    ws: &std_only::writer::Worksheet,
    shared_strings: &[String],
) -> Result<ChangeLogLayout> {
    for row in 1..=30u32 {
        let mut headers: HashMap<String, u32> = HashMap::new();
        for col in 1..=60u32 {
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
    get_header_col_optional(headers, keys).ok_or_else(|| {
        err(format!(
            "변경내역 헤더에 '{}' 컬럼이 없습니다.",
            display_name
        ))
    })
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
    ws: &std_only::writer::Worksheet,
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
fn pick_change_log_style_template_row(
    ws: &std_only::writer::Worksheet,
    max_col: u32,
    data_start_row: u32,
) -> u32 {
    let preferred_row = 243u32;
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
fn print_summary(
    args: &Args,
    out_path: &Path,
    source_files: usize,
    changes: &[ChangeRow],
    added: &[StoreRow],
    deleted: &[StoreRow],
) {
    println!("\n==== 현행화 요약 ====");
    println!("- 마스터: {}", args.master.display());
    println!("- 소스 폴더: {}", args.sources_dir.display());
    println!("- 소스 prefix: {}", args.sources_prefix);
    println!("- 소스 파일 수: {source_files}");
    println!("- 기존 업체 변경 건수(가격/정보): {}", changes.len());
    println!("- 신규 업체 추가: {}", added.len());
    println!("- 폐업 업체 삭제: {}", deleted.len());
    if args.dry_run {
        println!("- 출력: (dry-run) 파일 저장 안 함");
    } else {
        println!("- 출력: {}", out_path.display());
    }
    if !added.is_empty() {
        println!("\n[신규 업체 추가 목록(상위 20개)]");
        for (i, item) in added.iter().take(20).enumerate() {
            println!(
                "  {}. {} / {} / {}",
                i + 1,
                item.region,
                item.name,
                item.address
            );
        }
        if added.len() > 20 {
            println!("  ... ({}개 중 20개만 표시)", added.len());
        }
    }
    if !deleted.is_empty() {
        println!("\n[폐업 업체 삭제 목록(상위 20개)]");
        for (i, item) in deleted.iter().take(20).enumerate() {
            println!(
                "  {}. {} / {} / {}",
                i + 1,
                item.region,
                item.name,
                item.address
            );
        }
        if deleted.len() > 20 {
            println!("  ... ({}개 중 20개만 표시)", deleted.len());
        }
    }
    println!("=====================\n");
}
fn canon_header(s: &str) -> String {
    s.trim().replace(' ', "")
}
fn same_trimmed(a: &str, b: &str) -> bool {
    a.trim() == b.trim()
}
fn normalize_phone(s: &str) -> String {
    s.chars().filter(|ch| ch.is_ascii_digit()).collect()
}
fn same_phone(a: &str, b: &str) -> bool {
    let na = normalize_phone(a);
    let nb = normalize_phone(b);
    if !na.is_empty() || !nb.is_empty() {
        na == nb
    } else {
        same_trimmed(a, b)
    }
}
fn same_self_yn(a: &str, b: &str) -> bool {
    canon_header(a) == canon_header(b)
}
fn parse_i32_str(s: &str) -> Option<i32> {
    let t = s.trim();
    if t.is_empty() || t == "-" {
        return None;
    }
    let t = t.replace(',', "");
    t.parse::<f64>().ok().map(|v| v.round() as i32)
}
fn normalize_address_key(addr: &str) -> String {
    let mut s = addr.trim().to_string();
    let replacements = [
        ("충청남도", "충남"),
        ("충청북도", "충북"),
        ("대전광역시", "대전"),
        ("세종특별자치시", "세종"),
    ];
    for (from, to) in replacements {
        s = s.replace(from, to);
    }
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        if ch.is_whitespace() {
            continue;
        }
        if matches!(ch, '(' | ')' | '[' | ']' | '{' | '}' | ',' | '.') {
            continue;
        }
        out.push(ch);
    }
    out
}
