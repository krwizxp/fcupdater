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
    println!(
        "- 마스터: {master_path}",
        master_path = args.master.display()
    );
    println!(
        "- 소스 폴더: {sources_dir}",
        sources_dir = args.sources_dir.display()
    );
    println!(
        "- 소스 접두어: {sources_prefix}",
        sources_prefix = args.sources_prefix
    );
    println!("- 소스 파일 수: {source_files}");
    println!(
        "- 기존 업체 변경: {change_count}건",
        change_count = changes.len()
    );
    println!(
        "- 신규 업체 추가: {added_count}건",
        added_count = added.len()
    );
    println!(
        "- 폐업 업체 삭제: {deleted_count}건",
        deleted_count = deleted.len()
    );
    if source_report.duplicate_addresses > 0 {
        println!(
            "- 주소 중복 충돌: {duplicate_count}건 (대체 반영 {replaced_count}건)",
            duplicate_count = source_report.duplicate_addresses,
            replaced_count = source_report.replaced_entries
        );
        if !source_report.samples.is_empty() {
            println!("  충돌 예시:");
            for (i, sample) in source_report.samples.iter().enumerate() {
                println!(
                    "  {index}. {address} | 기존:{previous_source} | 신규:{incoming_source} | 선택:{selected_source}",
                    index = i + 1,
                    address = sample.address,
                    previous_source = sample.previous_source,
                    incoming_source = sample.incoming_source,
                    selected_source = sample.selected_source
                );
            }
        }
    }
    if args.save_mode.is_dry_run() {
        println!("- 출력: 저장 안 함 (--dry-run)");
    } else {
        println!("- 출력: {output_path}", output_path = out_path.display());
        if args.save_mode.verify_saved_file() {
            println!("- 저장 검증: 사용 (기본)");
        } else {
            println!("- 저장 검증: 생략(--fast-save)");
        }
    }
    if !added.is_empty() {
        println!("\n신규 업체 추가 목록 (상위 20개)");
        for (i, item) in added.iter().take(20).enumerate() {
            println!(
                "  {index}. {region} / {name} / {address}",
                index = i + 1,
                region = item.region,
                name = item.name,
                address = item.address
            );
        }
        if added.len() > 20 {
            println!(
                "  ... ({added_count}개 중 20개만 표시)",
                added_count = added.len()
            );
        }
    }
    if !deleted.is_empty() {
        println!("\n폐업 업체 삭제 목록 (상위 20개)");
        for (i, item) in deleted.iter().take(20).enumerate() {
            println!(
                "  {index}. {region} / {name} / {address}",
                index = i + 1,
                region = item.region,
                name = item.name,
                address = item.address
            );
        }
        if deleted.len() > 20 {
            println!(
                "  ... ({deleted_count}개 중 20개만 표시)",
                deleted_count = deleted.len()
            );
        }
    }
    println!("=====================\n");
}
