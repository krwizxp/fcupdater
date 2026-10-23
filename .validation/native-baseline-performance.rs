#[cfg(test)]
mod exact_baseline_performance {
    use super::*;
    use std::{path::Path, time::Instant};

    #[test]
    fn workbook_octane_cache_path() -> Result<()> {
        let fixture = Path::new(".validation/fixtures/native-benchmark.xlsx");
        let input = crate::temp_entry::open_regular(fixture, false)?;
        let container = XlsxContainer::from_validated_file(input, fixture)?;
        let mut book = Workbook::from_container(container)?;
        for _ in 0..3_000 {
            book.request_recalculation_caches()?;
        }
        let mut elapsed = Vec::with_capacity(9);
        for _ in 0..9 {
            let start = Instant::now();
            for _ in 0..20_000 {
                std::hint::black_box(book.request_recalculation_caches()?);
            }
            elapsed.push(start.elapsed().as_nanos() as f64 / 20_000.0);
        }
        elapsed.sort_unstable_by(f64::total_cmp);
        println!(
            "EXACT_BASELINE_CORE_MEDIAN_NS {:.3} P95_NS {:.3} FASTEST_NS {:.3}",
            elapsed[4], elapsed[8], elapsed[0]
        );
        Ok(())
    }
}
