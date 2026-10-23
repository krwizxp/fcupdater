#[cfg(test)]
mod octane_regression_validation {
    use super::*;
    use std::path::Path;

    fn cached_value(book: &Workbook, row_index: usize) -> Result<Option<f64>> {
        let row = book
            .master_sheet
            .rows
            .get(row_index)
            .ok_or_else(|| err("Octane cache validation row is missing"))?;
        let cell = row
            .cells
            .iter()
            .find(|cell| cell.col == 2)
            .ok_or_else(|| err("Octane cache validation cell is missing"))?;
        if cell.inner_xml.contains("<v/>") {
            return Ok(None);
        }
        let text = cell
            .inner_xml
            .split_once("<v>")
            .and_then(|(_, suffix)| suffix.split_once("</v>").map(|(value, _)| value))
            .ok_or_else(|| err("Octane cache validation cannot find the cached value"))?;
        text.parse::<f64>()
            .map(Some)
            .map_err(|source| err_with_source("Invalid numeric octane cache", source))
    }

    #[test]
    fn octane_formulas_match_independent_weighted_expectations() -> Result<()> {
        let fixtures = [
            ("default.xlsx", Some(95.0), Some(95.88888888888889)),
            ("premium.xlsx", Some(100.0), Some(102.0)),
            ("gasoline.xlsx", Some(91.0), Some(91.0)),
            ("zero.xlsx", None, None),
            ("large.xlsx", Some(95.5), Some(96.5)),
        ];
        for (filename, avg, ultra) in fixtures {
            let path = Path::new(".validation/fixtures").join(filename);
            let input = crate::temp_entry::open_regular(&path, false)?;
            let container = XlsxContainer::from_validated_file(input, &path)?;
            let mut workbook = Workbook::from_container(container)?;
            workbook.request_recalculation_caches()?;
            for (row_index, expected) in [(10, avg), (11, ultra)] {
                let actual = cached_value(&workbook, row_index)?;
                let matched = match (actual, expected) {
                    (Some(a), Some(b)) => (a - b).abs() < 0.000000001,
                    (None, None) => true,
                    _ => false,
                };
                if !matched {
                    return Err(err(format!(
                        "Weighted octane cache mismatch for {filename}, row {}, actual {actual:?}, expected {expected:?}",
                        row_index
                    )));
                }
            }
        }
        Ok(())
    }

    #[test]
    fn invalid_fixed_header_formula_is_rejected() -> Result<()> {
        let path = Path::new(".validation/fixtures/badformula.xlsx");
        let input = crate::temp_entry::open_regular(path, false)?;
        let container = XlsxContainer::from_validated_file(input, path)?;
        if Workbook::from_container(container).is_ok() {
            return Err(err("Unsupported fixed-header formula was accepted"));
        }
        Ok(())
    }

    #[test]
    fn malformed_zip_is_rejected() -> Result<()> {
        let path = Path::new(".validation/fixtures/badzip.xlsx");
        let input = crate::temp_entry::open_regular(path, false)?;
        if XlsxContainer::from_validated_file(input, path).is_ok() {
            return Err(err("Malformed XLSX container was accepted"));
        }
        Ok(())
    }

    #[cfg(any(target_os = "linux", target_os = "macos"))]
    #[test]
    fn symlinked_workbook_is_rejected() -> Result<()> {
        let path = Path::new(".validation/fixtures/symlink.xlsx");
        if crate::temp_entry::open_regular(path, false).is_ok() {
            return Err(err("Symlinked XLSX was accepted"));
        }
        Ok(())
    }
}