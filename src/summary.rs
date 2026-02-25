use crate::{ChangeRow, StoreRow, cli::Args, source_sync::SourceIndexBuildReport};
use std::path::Path;
pub fn print_summary(
    args: &Args,
    out_path: &Path,
    source_files: usize,
    source_report: &SourceIndexBuildReport,
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
    if source_report.duplicate_address_conflicts > 0 {
        println!(
            "- 주소 중복 충돌: {}건 (대체 반영: {}건)",
            source_report.duplicate_address_conflicts, source_report.overwritten_conflicts
        );
        if !source_report.sample_conflicts.is_empty() {
            println!("  충돌 상세 예시:");
            for (i, sample) in source_report.sample_conflicts.iter().enumerate() {
                println!(
                    "  {}. {} | 기존:{} | 신규:{} | 선택:{}",
                    i + 1,
                    sample.address,
                    sample.previous_source,
                    sample.incoming_source,
                    sample.selected_source
                );
            }
        } else if !source_report.sample_conflict_addresses.is_empty() {
            println!("  충돌 주소 예시:");
            for (i, addr) in source_report.sample_conflict_addresses.iter().enumerate() {
                println!("  {}. {}", i + 1, addr);
            }
        }
    }
    if args.save_mode.is_dry_run() {
        println!("- 출력: (dry-run) 파일 저장 안 함");
    } else {
        println!("- 출력: {}", out_path.display());
        if args.save_mode.verify_saved_file() {
            println!("- 저장 검증: 사용(기본)");
        } else {
            println!("- 저장 검증: 생략(--fast-save)");
        }
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
