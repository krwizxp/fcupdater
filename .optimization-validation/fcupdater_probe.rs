fn probe_book(path: &Path) -> Result<excel::writer::Workbook> {
    let file = temp_entry::open_regular(path, false)?;
    let container = excel::xlsx_container::XlsxContainer::from_validated_file(file, path)?;
    excel::writer::Workbook::from_container(container)
}
fn probe_source(book: &mut excel::writer::Workbook) -> Result<std::collections::HashMap<String, excel::SourceRecord>> {
    use excel::source_reader::StationService;
    let (sheet, strings) = book.master_sheet_mut();
    let mut source = std::collections::HashMap::new();
    for row in sheet.row_numbers_from(15) {
        let address = sheet.try_get_display_at(6, row, strings)?.trim().to_owned();
        let name = sheet.try_get_display_at(3, row, strings)?.trim().to_owned();
        if address.is_empty() || name.is_empty() { continue; }
        let raw_region = sheet.try_get_display_at(2, row, strings)?;
        let region = region::TARGET_REGIONS.iter().find(|r| r.label() == raw_region.trim()).unwrap().label();
        let brand = sheet.try_get_display_at(4, row, strings)?.into_owned();
        let service = if sheet.try_get_display_at(5, row, strings)?.trim() == "셀프" { StationService::SelfService } else { StationService::General };
        let fuels = excel::FuelValues {
            gasoline: sheet.get_i32_at(7, row, strings)?, premium: sheet.get_i32_at(8, row, strings)?, diesel: sheet.get_i32_at(10, row, strings)?,
        };
        let mut key = String::new();
        region::target_region(region, &address, &mut key, region::TargetRegionPolicy::Flexible)?;
        source.insert(key, excel::SourceRecord { address, name, brand, region, service, fuels });
    }
    Ok(source)
}
fn main() -> Result<()> {
    let args: Vec<_> = env::args().collect();
    if args[1].starts_with("intern-") {
        let escaping = args[1] == "intern-escaped";
        let corpus: Vec<_> = (0..5000).map(|i| if escaping { format!(" {i} 주유소 & <상호> \r주소 ") } else { format!("{i} 충남 천안시 주유소 주소 테스트") }).collect();
        let start = std::time::Instant::now();
        let mut bytes = 0;
        for _ in 0..8 { bytes += std::hint::black_box(excel::writer::probe_intern_xml(std::hint::black_box(&corpus))?).len(); }
        println!("{} {}", start.elapsed().as_nanos(), bytes);
        return Ok(());
    }

    if args[1] == "numbers" || args[1] == "integers" || args[1] == "errors" {
        let mut book = probe_book(Path::new(&args[2]))?;
        let (sheet, strings) = book.master_sheet_mut();
        for line in io::BufRead::lines(io::stdin().lock()) {
            let text = line?;
            sheet.set_formula_at_with_cache(4, 4, "1", Some(&text), false)?;
            if args[1] == "integers" {
                match sheet.get_i32_at(4, 4, strings)? {
                    Some(value) => println!("{value}"), None => println!("NONE"),
                }
                continue;
            }
            match master_sheet::MasterSheetUpdater::get_f64_at(sheet, 4, 4, strings) {
                Ok(Some(value)) => println!("{}", value.as_i128()),
                Ok(None) => println!("NONE"),
                Err(error) if args[1] == "errors" => println!("ERROR {error}"),
                Err(_) => println!("ERROR"),
            }
        }
        return Ok(());
    }
    let path = Path::new(&args[2]);

    if args[1] == "parse-bench" {
        let start = std::time::Instant::now();
        for _ in 0..8 { std::hint::black_box(probe_book(path)?); }
        println!("{} 8",start.elapsed().as_nanos());
        return Ok(());
    }
    if args[1] == "roundtrip" {
        let output = Path::new(&args[3]);
        let mut nanos = 0;
        for _ in 0..4 {
            std::fs::copy(path, output)?;
            let start = std::time::Instant::now();
            let mut book = probe_book(output)?;
            let source = probe_source(&mut book)?;
            let result = master_sheet::MasterSheetUpdater { source_index: &source }.update(&mut book)?;
            let change_last = book.change_log_sheet_mut().0.row_count() as u32;
            book.save(output, SaveVerification::Verify, result.last_data_row, change_last)?;
            nanos += start.elapsed().as_nanos();
        }
        println!("{nanos} 4");
        return Ok(());
    }
    if args[1] == "parse" {
        match probe_book(path) { Ok(_) => println!("OK"), Err(e) => println!("ERROR {e}") }
        return Ok(());
    }
    let mut book = probe_book(path)?;
    let source = probe_source(&mut book)?;
    if args[1] == "save" {
        let result = master_sheet::MasterSheetUpdater { source_index: &source }.update(&mut book)?;
        let change_log_last = book.change_log_sheet_mut().0.row_count() as u32;
        println!("{} {} {} {} {}", source.len(), result.existing_count, result.added.len(), result.changes.len(), result.deleted.len());
        book.save(Path::new(&args[3]), SaveVerification::Verify, result.last_data_row, change_log_last)?;
        return Ok(());
    }
    let mut total = 0;
    let mut nanos = 0;
    for _ in 0..8 {
        let mut book = probe_book(path)?;
        let start = std::time::Instant::now();
        let result = master_sheet::MasterSheetUpdater { source_index: std::hint::black_box(&source) }.update(&mut book)?;
        nanos += start.elapsed().as_nanos();
        total += std::hint::black_box(result.last_data_row);
    }
    println!("{nanos} {total}");
    Ok(())
}
