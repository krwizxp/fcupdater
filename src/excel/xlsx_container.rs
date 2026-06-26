use super::{
    SaveVerification, SheetInfo, ZipArchiveBuilder, ZipArchiveExtractor,
    path_util::path_to_slashes,
    xml::{
        decode_xml_entities, extract_all_tag_text, extract_attr, extract_first_tag_text,
        find_end_tag, find_start_tag, find_tag_end,
    },
};
use crate::diagnostic::{
    Result, err, err_with_source, path_context_message, path_pair_context_message,
    path_source_message, prefixed_message,
};
use alloc::borrow::Cow;
use core::{mem, range::Range, time::Duration};
use std::{
    collections::{HashMap, HashSet, hash_map::Entry as HashEntry},
    env, fs,
    io::{self, ErrorKind, Read as _},
    path::{Component, Path, PathBuf},
    process, thread,
    time::{SystemTime, UNIX_EPOCH},
};
cfg_select! {
    any(target_os = "linux", target_os = "macos") => {
        use std::io::{Write as IoWrite, stderr};
    }
    _ => {}
}
const TEMP_ARCHIVE_PROMOTION_ATTEMPTS: u32 = 5;
const TEMP_ARCHIVE_PROMOTION_RETRY_DELAY: Duration = Duration::from_millis(50);
const MAX_XLSX_TEXT_PART_BYTES: u64 = 64 * 1024 * 1024;
const CHANGELOG_DATA_START_ROW: u32 = 4;
const FAR_ARTIFACT_MIN_COL: u32 = 16_382;
const MASTER_FILTER_DATA_START_ROW: u32 = 15;
const MASTER_FILTER_HEADER_ROW: u32 = 14;
const MASTER_FILTER_START_COL: u32 = 1;
const WORKSHEET_REL_TYPE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet";
#[derive(Debug)]
pub struct XlsxContainer {
    unpack_dir: PathBuf,
    work_dir: PathBuf,
}
#[derive(Debug)]
struct WorkDirCleanup {
    keep: bool,
    path: PathBuf,
}
impl Drop for WorkDirCleanup {
    fn drop(&mut self) {
        if !self.keep && !self.path.as_os_str().is_empty() {
            match fs::remove_dir_all(&self.path) {
                Ok(()) | Err(_) => {}
            }
        }
    }
}
struct SavedArchiveVerifier<'path> {
    saved_archive: &'path Path,
}
struct ArchiveSemanticVerifier<'container> {
    container: &'container XlsxContainer,
    workbook_xml: &'container str,
}
struct ArchiveSemanticSummary {
    master_auto_filter_ref: String,
}
struct CellPosition {
    col: u32,
    row: u32,
}
struct CellBounds {
    max: CellPosition,
    min: CellPosition,
}
enum FilterRefPolicy {
    AnyA1,
    RequireAbsolute,
}
struct SharedStringsSummary {
    declared_total: Option<usize>,
    declared_unique: Option<usize>,
    present: bool,
    unique_entries: usize,
}
struct WorksheetSemanticSummary {
    master_auto_filter_ref: Option<String>,
    shared_ref_count: usize,
}
struct WorkbookRelationship<'xml> {
    target: Cow<'xml, str>,
    target_mode: Option<Cow<'xml, str>>,
    type_: Cow<'xml, str>,
}
impl WorkbookRelationship<'_> {
    fn internal_worksheet_target(&self, rid: &str) -> Result<&str> {
        if self.type_.as_ref() != WORKSHEET_REL_TYPE {
            return Err(err(format!(
                "workbook.xml sheet 관계 Type이 worksheet가 아닙니다: rid={rid}, type={}",
                self.type_.as_ref()
            )));
        }
        if self
            .target_mode
            .as_ref()
            .is_some_and(|mode| mode.as_ref() != "Internal")
        {
            return Err(err(format!(
                "workbook.xml sheet 관계 TargetMode는 External일 수 없습니다: rid={rid}"
            )));
        }
        Ok(self.target.as_ref())
    }
}
impl SavedArchiveVerifier<'_> {
    fn trailing_row_num(cell_ref: &str) -> Result<u32> {
        let digits_len = cell_ref
            .bytes()
            .rev()
            .take_while(u8::is_ascii_digit)
            .count();
        if digits_len == 0 {
            return Err(err(format!(
                "저장 검증 실패: filter cell reference에 row 번호가 없습니다: {cell_ref}"
            )));
        }
        let digit_start = cell_ref
            .len()
            .checked_sub(digits_len)
            .ok_or_else(|| err("저장 검증 실패: filter row 번호 위치 계산 실패"))?;
        let row_text = cell_ref
            .get(digit_start..)
            .ok_or_else(|| err("저장 검증 실패: filter row 번호 범위가 손상되었습니다."))?;
        row_text
            .parse::<u32>()
            .map_err(|source| err_with_source("저장 검증 실패: filter row 번호 해석 실패", source))
    }
    fn verify(&self) -> Result<()> {
        let container = XlsxContainer::open(self.saved_archive).map_err(|source_err| {
            err_with_source(
                path_context_message(
                    "저장 검증 실패: 저장 직후 압축 해제 점검에 실패했습니다",
                    self.saved_archive,
                ),
                source_err,
            )
        })?;
        container
            .read_text("[Content_Types].xml")
            .map_err(|source_err| {
                err_with_source(
                    path_context_message(
                        "저장 검증 실패: 필수 OOXML 파트 읽기 실패",
                        self.saved_archive,
                    ),
                    source_err,
                )
            })?;
        let workbook_xml = container
            .read_text("xl/workbook.xml")
            .map_err(|source_err| {
                err_with_source(
                    path_context_message(
                        "저장 검증 실패: workbook.xml 읽기 실패",
                        self.saved_archive,
                    ),
                    source_err,
                )
            })?;
        let summary = ArchiveSemanticVerifier {
            container: &container,
            workbook_xml: &workbook_xml,
        }
        .verify()?;
        let workbook =
            super::writer::Workbook::from_container(container).map_err(|source_err| {
                err_with_source(
                    path_context_message(
                        "저장 검증 실패: 저장 직후 재열기 점검에 실패했습니다",
                        self.saved_archive,
                    ),
                    source_err,
                )
            })?;
        let normalized = ArchiveSemanticVerifier::normalize_filter_ref(
            &summary.master_auto_filter_ref,
            &FilterRefPolicy::AnyA1,
        )?;
        let (start_ref, end_ref) = normalized.split_once(':').map_or(
            (normalized.as_str(), normalized.as_str()),
            |(start, end)| (start, end),
        );
        let header_row = Self::trailing_row_num(start_ref)?;
        let filter_end_row = Self::trailing_row_num(end_ref)?;
        workbook.verify_sheet_address_data_end_row("유류비", header_row, filter_end_row)?;
        Ok(())
    }
}
impl ArchiveSemanticVerifier<'_> {
    fn cell_body_span(sheet_xml: &str, cell_open_end: usize) -> Result<Range<usize>> {
        let body_start = cell_open_end
            .checked_add(1)
            .ok_or_else(|| err("저장 검증 실패: cell 본문 시작 계산 실패"))?;
        let cell_close = find_end_tag(sheet_xml, "c", body_start)
            .ok_or_else(|| err("저장 검증 실패: cell 종료 태그가 없습니다."))?;
        Ok(Range {
            start: body_start,
            end: cell_close,
        })
    }
    fn cell_cursor_after(
        sheet_xml: &str,
        cell_open_end: usize,
        cell_self_closing: bool,
    ) -> Result<usize> {
        if cell_self_closing {
            return cell_open_end
                .checked_add(1)
                .ok_or_else(|| err("저장 검증 실패: cell cursor 계산 실패"));
        }
        let cell_close_start = Self::cell_body_span(sheet_xml, cell_open_end)?.end;
        let cell_close_end = find_tag_end(sheet_xml, cell_close_start)
            .ok_or_else(|| err("저장 검증 실패: cell 종료 태그가 손상되었습니다."))?;
        cell_close_end
            .checked_add(1)
            .ok_or_else(|| err("저장 검증 실패: cell cursor 계산 실패"))
    }
    fn filter_database_ref(&self, sheets: &[SheetInfo]) -> Result<String> {
        let master_sheet_index = super::workbook_sheet_index_by_name(self.workbook_xml, "유류비")?;
        if sheets.iter().filter(|sheet| sheet.name == "유류비").count() != 1 {
            return Err(err(
                "저장 검증 실패: workbook에 유류비 시트가 중복되어 있습니다.",
            ));
        }
        let mut quoted_sheet = String::new();
        quoted_sheet
            .try_reserve("'유류비'!".len())
            .map_err(|source| {
                err_with_source("저장 검증 실패: sheet 이름 메모리 확보 실패", source)
            })?;
        quoted_sheet.push('\'');
        quoted_sheet.push_str("유류비");
        quoted_sheet.push_str("'!");
        let plain_sheet = "유류비!";
        let span = super::workbook_defined_name_content_span(
            self.workbook_xml,
            "_xlnm._FilterDatabase",
            master_sheet_index,
            &quoted_sheet,
            plain_sheet,
        )?;
        let raw_ref = self
            .workbook_xml
            .get(span)
            .ok_or_else(|| err("저장 검증 실패: definedName 본문 범위가 손상되었습니다."))?;
        Ok(decode_xml_entities(raw_ref)?.into_owned())
    }
    fn normalize_filter_ref(ref_text: &str, policy: &FilterRefPolicy) -> Result<String> {
        let range_text = ref_text
            .rsplit_once('!')
            .map_or(ref_text, |(_, range)| range)
            .trim();
        if matches!(policy, FilterRefPolicy::RequireAbsolute) {
            let (start_ref, end_ref) = range_text.split_once(':').ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: _FilterDatabase 범위가 올바르지 않습니다: {ref_text}"
                ))
            })?;
            let validate_absolute = |cell_ref: &str| -> Result<()> {
                let bytes = cell_ref.as_bytes();
                let mut cursor = 0_usize;
                if bytes.get(cursor) != Some(&b'$') {
                    return Err(err(format!(
                        "저장 검증 실패: _FilterDatabase가 절대참조가 아닙니다: {ref_text}"
                    )));
                }
                cursor = cursor.checked_add(1).ok_or_else(|| {
                    err("저장 검증 실패: _FilterDatabase column cursor 계산 실패")
                })?;
                let col_start = cursor;
                while bytes.get(cursor).is_some_and(u8::is_ascii_alphabetic) {
                    cursor = cursor.checked_add(1).ok_or_else(|| {
                        err("저장 검증 실패: _FilterDatabase column cursor 계산 실패")
                    })?;
                }
                if cursor == col_start || bytes.get(cursor) != Some(&b'$') {
                    return Err(err(format!(
                        "저장 검증 실패: _FilterDatabase가 절대참조가 아닙니다: {ref_text}"
                    )));
                }
                cursor = cursor
                    .checked_add(1)
                    .ok_or_else(|| err("저장 검증 실패: _FilterDatabase row cursor 계산 실패"))?;
                let row_start = cursor;
                while bytes.get(cursor).is_some_and(u8::is_ascii_digit) {
                    cursor = cursor.checked_add(1).ok_or_else(|| {
                        err("저장 검증 실패: _FilterDatabase row cursor 계산 실패")
                    })?;
                }
                if cursor == row_start || cursor != bytes.len() {
                    return Err(err(format!(
                        "저장 검증 실패: _FilterDatabase가 절대참조가 아닙니다: {ref_text}"
                    )));
                }
                Ok(())
            };
            validate_absolute(start_ref)?;
            validate_absolute(end_ref)?;
        }
        let mut normalized = String::new();
        normalized.try_reserve(range_text.len()).map_err(|source| {
            err_with_source(
                "저장 검증 실패: filter 범위 정규화 메모리 확보 실패",
                source,
            )
        })?;
        normalized.extend(range_text.chars().filter(|ch| *ch != '$' && *ch != '\''));
        Ok(normalized)
    }
    fn optional_usize_attr(tag: &str, attr_name: &str, context: &str) -> Result<Option<usize>> {
        extract_attr(tag, attr_name)?
            .map(|value| {
                value.parse::<usize>().map_err(|source| {
                    err_with_source(format!("{context}: {attr_name} 해석 실패"), source)
                })
            })
            .transpose()
    }
    fn parse_cell_bounds(range_ref: &str, context: &str) -> Result<CellBounds> {
        let (start_ref, end_ref) = range_ref
            .split_once(':')
            .map_or((range_ref, range_ref), |(start, end)| (start, end));
        let min = Self::parse_cell_position(start_ref, context)?;
        let max = Self::parse_cell_position(end_ref, context)?;
        if min.col > max.col || min.row > max.row {
            return Err(err(format!(
                "{context}: cell range 순서가 올바르지 않습니다."
            )));
        }
        Ok(CellBounds { max, min })
    }
    fn parse_cell_position(cell_ref: &str, context: &str) -> Result<CellPosition> {
        let bytes = cell_ref.as_bytes();
        let mut cursor = 0_usize;
        if bytes.get(cursor) == Some(&b'$') {
            cursor = cursor
                .checked_add(1)
                .ok_or_else(|| err(format!("{context}: cell reference cursor 계산 실패")))?;
        }
        let mut col = 0_u32;
        let mut saw_col = false;
        while let Some(&byte) = bytes.get(cursor) {
            let upper = byte.to_ascii_uppercase();
            if !upper.is_ascii_uppercase() {
                break;
            }
            let digit = upper
                .checked_sub(b'A')
                .and_then(|value| value.checked_add(1))
                .map(u32::from)
                .ok_or_else(|| err(format!("{context}: column 계산에 실패했습니다.")))?;
            col = col
                .checked_mul(26)
                .and_then(|value| value.checked_add(digit))
                .ok_or_else(|| {
                    err(format!(
                        "{context}: column 계산 중 overflow가 발생했습니다."
                    ))
                })?;
            saw_col = true;
            cursor = cursor
                .checked_add(1)
                .ok_or_else(|| err(format!("{context}: column cursor 계산 실패")))?;
        }
        if !saw_col {
            return Err(err(format!(
                "{context}: cell reference에 column이 없습니다."
            )));
        }
        if bytes.get(cursor) == Some(&b'$') {
            cursor = cursor
                .checked_add(1)
                .ok_or_else(|| err(format!("{context}: row cursor 계산 실패")))?;
        }
        let row_text = cell_ref
            .get(cursor..)
            .ok_or_else(|| err(format!("{context}: row 범위가 손상되었습니다.")))?;
        if row_text.is_empty() || !row_text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(err(format!(
                "{context}: cell reference에 row 번호가 없습니다."
            )));
        }
        let row = row_text
            .parse::<u32>()
            .map_err(|source| err_with_source(format!("{context}: row 번호 해석 실패"), source))?;
        if row == 0 || col == 0 {
            return Err(err(format!(
                "{context}: cell reference는 1 이상이어야 합니다."
            )));
        }
        Ok(CellPosition { col, row })
    }
    fn shared_strings_summary(&self) -> Result<SharedStringsSummary> {
        let shared_strings_path = self
            .container
            .resolve_relative_path("xl/sharedStrings.xml")?;
        if !shared_strings_path.try_exists().map_err(|source_err| {
            err_with_source(
                path_context_message("sharedStrings.xml 존재 확인 실패", &shared_strings_path),
                source_err,
            )
        })? {
            return Ok(SharedStringsSummary {
                declared_total: None,
                declared_unique: None,
                present: false,
                unique_entries: 0,
            });
        }
        let shared_xml = self.container.read_text("xl/sharedStrings.xml")?;
        let sst_start = find_start_tag(&shared_xml, "sst", 0)
            .ok_or_else(|| err("저장 검증 실패: sharedStrings.xml에 <sst>가 없습니다."))?;
        let sst_end = find_tag_end(&shared_xml, sst_start).ok_or_else(|| {
            err("저장 검증 실패: sharedStrings.xml의 <sst> 태그가 손상되었습니다.")
        })?;
        let sst_tag = shared_xml.get(sst_start..=sst_end).ok_or_else(|| {
            err("저장 검증 실패: sharedStrings.xml의 <sst> 태그 범위가 손상되었습니다.")
        })?;
        let declared_total =
            Self::optional_usize_attr(sst_tag, "count", "저장 검증 실패: sharedStrings count")?;
        let declared_unique = Self::optional_usize_attr(
            sst_tag,
            "uniqueCount",
            "저장 검증 실패: sharedStrings uniqueCount",
        )?;
        let mut unique_entries = 0_usize;
        let mut cursor = 0_usize;
        while let Some(si_start) = find_start_tag(&shared_xml, "si", cursor) {
            let si_open_end = find_tag_end(&shared_xml, si_start)
                .ok_or_else(|| err("저장 검증 실패: sharedStrings <si> 태그가 손상되었습니다."))?;
            let si_open_tag = shared_xml.get(si_start..=si_open_end).ok_or_else(|| {
                err("저장 검증 실패: sharedStrings <si> 태그 범위가 손상되었습니다.")
            })?;
            unique_entries = unique_entries
                .checked_add(1)
                .ok_or_else(|| err("저장 검증 실패: sharedStrings <si> 수 계산 실패"))?;
            cursor = if si_open_tag.trim_ascii_end().ends_with("/>") {
                si_open_end
                    .checked_add(1)
                    .ok_or_else(|| err("저장 검증 실패: sharedStrings <si> cursor 계산 실패"))?
            } else {
                let si_body_start = si_open_end
                    .checked_add(1)
                    .ok_or_else(|| err("저장 검증 실패: sharedStrings <si> 본문 시작 계산 실패"))?;
                let si_close_start = find_end_tag(&shared_xml, "si", si_body_start)
                    .ok_or_else(|| err("저장 검증 실패: sharedStrings </si> 태그가 없습니다."))?;
                let si_close_end = find_tag_end(&shared_xml, si_close_start).ok_or_else(|| {
                    err("저장 검증 실패: sharedStrings </si> 태그가 손상되었습니다.")
                })?;
                si_close_end
                    .checked_add(1)
                    .ok_or_else(|| err("저장 검증 실패: sharedStrings <si> cursor 계산 실패"))?
            };
        }
        Ok(SharedStringsSummary {
            declared_total,
            declared_unique,
            present: true,
            unique_entries,
        })
    }
    fn verify(&self) -> Result<ArchiveSemanticSummary> {
        let sheets = self.container.load_sheet_catalog(self.workbook_xml)?;
        let shared_summary = self.shared_strings_summary()?;
        let shared_entry_count = shared_summary
            .present
            .then_some(shared_summary.unique_entries);
        let worksheet_summary = self.worksheet_summary(&sheets, shared_entry_count)?;
        let filter_database_ref = self.filter_database_ref(&sheets)?;
        let Some(auto_filter_ref) = worksheet_summary.master_auto_filter_ref else {
            return Err(err(
                "저장 검증 실패: _FilterDatabase definedName이 있지만 autoFilter가 없습니다.",
            ));
        };
        let normalized_auto_filter =
            Self::normalize_filter_ref(&auto_filter_ref, &FilterRefPolicy::AnyA1)?;
        let normalized_database =
            Self::normalize_filter_ref(&filter_database_ref, &FilterRefPolicy::RequireAbsolute)?;
        if normalized_auto_filter != normalized_database {
            return Err(err(format!(
                "저장 검증 실패: autoFilter 범위와 _FilterDatabase 범위가 다릅니다: autoFilter={auto_filter_ref}, definedName={filter_database_ref}"
            )));
        }
        if worksheet_summary.shared_ref_count > 0 && !shared_summary.present {
            return Err(err(
                "저장 검증 실패: shared string 참조가 있지만 sharedStrings.xml이 없습니다.",
            ));
        }
        if let Some(declared_count) = shared_summary.declared_total
            && declared_count != worksheet_summary.shared_ref_count
        {
            return Err(err(format!(
                "저장 검증 실패: sharedStrings count가 실제 참조 수와 다릅니다: declared={declared_count}, actual={}",
                worksheet_summary.shared_ref_count
            )));
        }
        if let Some(declared_unique) = shared_summary.declared_unique
            && declared_unique != shared_summary.unique_entries
        {
            return Err(err(format!(
                "저장 검증 실패: sharedStrings uniqueCount가 실제 <si> 수와 다릅니다: declared={declared_unique}, actual={}",
                shared_summary.unique_entries
            )));
        }
        Ok(ArchiveSemanticSummary {
            master_auto_filter_ref: auto_filter_ref,
        })
    }
    fn worksheet_bounds_and_shared_refs(
        sheet_xml: &str,
        sheet_name: &str,
        shared_entry_count: Option<usize>,
    ) -> Result<(Option<CellBounds>, usize)> {
        let mut bounds: Option<CellBounds> = None;
        let mut shared_ref_count = 0_usize;
        let mut cursor = 0_usize;
        while let Some(cell_start) = find_start_tag(sheet_xml, "c", cursor) {
            let cell_open_end = find_tag_end(sheet_xml, cell_start).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: cell 태그가 손상되었습니다: {sheet_name}"
                ))
            })?;
            let cell_tag = sheet_xml.get(cell_start..=cell_open_end).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: cell 태그 범위가 손상되었습니다: {sheet_name}"
                ))
            })?;
            let Some(cell_ref) = extract_attr(cell_tag, "r")? else {
                return Err(err(format!(
                    "저장 검증 실패: cell에 r 속성이 없습니다: {sheet_name}"
                )));
            };
            let cell =
                Self::parse_cell_position(cell_ref.as_ref(), "저장 검증 실패: cell reference")?;
            if let Some(existing) = bounds.as_mut() {
                existing.min.col = existing.min.col.min(cell.col);
                existing.min.row = existing.min.row.min(cell.row);
                existing.max.col = existing.max.col.max(cell.col);
                existing.max.row = existing.max.row.max(cell.row);
            } else {
                let col = cell.col;
                let row = cell.row;
                bounds = Some(CellBounds {
                    max: CellPosition { col, row },
                    min: CellPosition { col, row },
                });
            }
            let cell_self_closing = cell_tag.trim_ascii_end().ends_with("/>");
            if extract_attr(cell_tag, "t")?.as_deref() == Some("s") {
                let Some(shared_count) = shared_entry_count else {
                    return Err(err(
                        "저장 검증 실패: shared string 참조가 있지만 sharedStrings.xml이 없습니다.",
                    ));
                };
                if cell_self_closing {
                    return Err(err(format!(
                        "저장 검증 실패: shared string cell에 <v>가 없습니다: {sheet_name}!{}",
                        cell_ref.as_ref()
                    )));
                }
                let body_span = Self::cell_body_span(sheet_xml, cell_open_end)?;
                let cell_body = sheet_xml
                    .get(body_span)
                    .ok_or_else(|| err("저장 검증 실패: cell 본문 범위가 손상되었습니다."))?;
                let v_start = find_start_tag(cell_body, "v", 0).ok_or_else(|| {
                    err(format!(
                        "저장 검증 실패: shared string cell에 <v>가 없습니다: {sheet_name}!{}",
                        cell_ref.as_ref()
                    ))
                })?;
                let v_open_end = find_tag_end(cell_body, v_start).ok_or_else(|| {
                    err("저장 검증 실패: shared string <v> 태그가 손상되었습니다.")
                })?;
                let v_body_start = v_open_end
                    .checked_add(1)
                    .ok_or_else(|| err("저장 검증 실패: shared string <v> 본문 시작 계산 실패"))?;
                let v_close = find_end_tag(cell_body, "v", v_body_start)
                    .ok_or_else(|| err("저장 검증 실패: shared string </v> 태그가 없습니다."))?;
                let raw_index = cell_body.get(v_body_start..v_close).ok_or_else(|| {
                    err("저장 검증 실패: shared string index 범위가 손상되었습니다.")
                })?;
                let decoded_index = decode_xml_entities(raw_index)?;
                let trimmed_index = decoded_index.trim();
                if trimmed_index.is_empty()
                    || trimmed_index.bytes().any(|byte| !byte.is_ascii_digit())
                {
                    return Err(err(format!(
                        "저장 검증 실패: shared string index가 음이 아닌 정수가 아닙니다: {sheet_name}!{}",
                        cell_ref.as_ref()
                    )));
                }
                let index = trimmed_index.parse::<usize>().map_err(|source| {
                    err_with_source("저장 검증 실패: shared string index 해석 실패", source)
                })?;
                if index >= shared_count {
                    return Err(err(format!(
                        "저장 검증 실패: shared string index가 범위를 벗어났습니다: {sheet_name}!{} index={index}, uniqueCount={shared_count}",
                        cell_ref.as_ref()
                    )));
                }
                shared_ref_count = shared_ref_count
                    .checked_add(1)
                    .ok_or_else(|| err("저장 검증 실패: shared string 참조 수 계산 실패"))?;
            }
            cursor = Self::cell_cursor_after(sheet_xml, cell_open_end, cell_self_closing)?;
        }
        Ok((bounds, shared_ref_count))
    }
    fn worksheet_cell_refs_unique(sheet_xml: &str, sheet_name: &str) -> Result<()> {
        let mut seen_cells: HashSet<u64> = HashSet::new();
        let mut cursor = 0_usize;
        while let Some(cell_start) = find_start_tag(sheet_xml, "c", cursor) {
            let cell_open_end = find_tag_end(sheet_xml, cell_start)
                .ok_or_else(|| err("저장 검증 실패: cell 태그가 손상되었습니다."))?;
            let cell_tag = sheet_xml
                .get(cell_start..=cell_open_end)
                .ok_or_else(|| err("저장 검증 실패: cell 태그 범위가 손상되었습니다."))?;
            let Some(cell_ref) = extract_attr(cell_tag, "r")? else {
                return Err(err(format!(
                    "저장 검증 실패: cell에 r 속성이 없습니다: {sheet_name}"
                )));
            };
            let cell =
                Self::parse_cell_position(cell_ref.as_ref(), "저장 검증 실패: cell reference")?;
            if cell.col >= FAR_ARTIFACT_MIN_COL {
                return Err(err(format!(
                    "저장 검증 실패: worksheet에 원거리 cell artifact가 있습니다: {sheet_name}!{}",
                    cell_ref.as_ref()
                )));
            }
            let cell_key = (u64::from(cell.row) << 32_u32) | u64::from(cell.col);
            if !seen_cells.insert(cell_key) {
                return Err(err(format!(
                    "저장 검증 실패: worksheet에 중복 cell reference가 있습니다: {sheet_name}!{}",
                    cell_ref.as_ref()
                )));
            }
            let cell_self_closing = cell_tag.trim_ascii_end().ends_with("/>");
            cursor = Self::cell_cursor_after(sheet_xml, cell_open_end, cell_self_closing)?;
        }
        Ok(())
    }
    fn worksheet_filter_semantics(
        sheet_xml: &str,
        sheet_name: &str,
        actual_bounds: Option<&CellBounds>,
    ) -> Result<Option<String>> {
        if sheet_name == "유류비" {
            let mut cursor = 0_usize;
            let mut found_ref = None;
            while let Some(filter_start) = find_start_tag(sheet_xml, "autoFilter", cursor) {
                let filter_end = find_tag_end(sheet_xml, filter_start)
                    .ok_or_else(|| err("저장 검증 실패: autoFilter 태그가 손상되었습니다."))?;
                let filter_tag = sheet_xml
                    .get(filter_start..=filter_end)
                    .ok_or_else(|| err("저장 검증 실패: autoFilter 태그 범위가 손상되었습니다."))?;
                let Some(filter_ref) = extract_attr(filter_tag, "ref")? else {
                    return Err(err(
                        "저장 검증 실패: 유류비 autoFilter ref 속성이 없습니다.",
                    ));
                };
                let filter_ref_text = filter_ref.as_ref();
                let filter_bounds =
                    Self::parse_cell_bounds(filter_ref_text, "저장 검증 실패: autoFilter ref")?;
                if filter_bounds.min.col != MASTER_FILTER_START_COL
                    || filter_bounds.min.row != MASTER_FILTER_HEADER_ROW
                {
                    return Err(err(format!(
                        "저장 검증 실패: 유류비 autoFilter 시작 범위가 예상과 다릅니다: {filter_ref_text}"
                    )));
                }
                let expected_last_row = Self::worksheet_meaningful_row_bound(
                    sheet_xml,
                    MASTER_FILTER_DATA_START_ROW,
                    filter_bounds.max.col,
                )?
                .ok_or_else(|| {
                    err("저장 검증 실패: 유류비 autoFilter 데이터 행을 찾지 못했습니다.")
                })?;
                if filter_bounds.max.row != expected_last_row {
                    return Err(err(format!(
                        "저장 검증 실패: 유류비 autoFilter 마지막 행이 실제 데이터 마지막 행과 다릅니다: filter={filter_ref_text}, actual={expected_last_row}"
                    )));
                }
                if actual_bounds.is_some_and(|bounds| bounds.max.col != filter_bounds.max.col) {
                    return Err(err(
                        "저장 검증 실패: 유류비 worksheet의 실제 마지막 열과 autoFilter 마지막 열이 다릅니다.",
                    ));
                }
                if found_ref.replace(filter_ref.into_owned()).is_some() {
                    return Err(err(
                        "저장 검증 실패: 유류비 worksheet에 autoFilter가 중복되어 있습니다.",
                    ));
                }
                cursor = filter_end
                    .checked_add(1)
                    .ok_or_else(|| err("저장 검증 실패: autoFilter cursor 계산 실패"))?;
            }
            return Ok(found_ref);
        }
        if sheet_name != "변경내역" {
            return Ok(None);
        }
        let expected_last_row =
            Self::worksheet_meaningful_row_bound(sheet_xml, CHANGELOG_DATA_START_ROW, 13)?
                .map_or(CHANGELOG_DATA_START_ROW, |row| row);
        let mut delta_mask = 0_u8;
        let mut cursor = 0_usize;
        while let Some(cf_start) = find_start_tag(sheet_xml, "conditionalFormatting", cursor) {
            let cf_end = find_tag_end(sheet_xml, cf_start).ok_or_else(|| {
                err("저장 검증 실패: 변경내역 conditionalFormatting 태그가 손상되었습니다.")
            })?;
            let cf_tag = sheet_xml.get(cf_start..=cf_end).ok_or_else(|| {
                err("저장 검증 실패: 변경내역 conditionalFormatting 태그 범위가 손상되었습니다.")
            })?;
            if let Some(sqref) = extract_attr(cf_tag, "sqref")? {
                for token in sqref.split_whitespace() {
                    let bounds = Self::parse_cell_bounds(
                        token,
                        "저장 검증 실패: 변경내역 조건부 서식 sqref",
                    )?;
                    if bounds.min.row == CHANGELOG_DATA_START_ROW
                        && bounds.max.row == expected_last_row
                        && bounds.min.col == bounds.max.col
                    {
                        delta_mask |= match bounds.min.col {
                            7 => 1,
                            10 => 2,
                            13 => 4,
                            _ => 0,
                        };
                    }
                }
            }
            cursor = cf_end.checked_add(1).ok_or_else(|| {
                err("저장 검증 실패: 변경내역 conditionalFormatting cursor 계산 실패")
            })?;
        }
        for (bit, name) in [(1, "G"), (2, "J"), (4, "M")] {
            if delta_mask & bit == 0 {
                return Err(err(format!(
                    "저장 검증 실패: 변경내역 {name}열 조건부 서식 기준 범위가 없습니다."
                )));
            }
        }
        Ok(None)
    }
    fn worksheet_meaningful_row_bound(
        sheet_xml: &str,
        min_row: u32,
        max_col: u32,
    ) -> Result<Option<u32>> {
        let mut last_row = None;
        let mut cursor = 0_usize;
        while let Some(cell_start) = find_start_tag(sheet_xml, "c", cursor) {
            let cell_open_end = find_tag_end(sheet_xml, cell_start)
                .ok_or_else(|| err("저장 검증 실패: cell 태그가 손상되었습니다."))?;
            let cell_tag = sheet_xml
                .get(cell_start..=cell_open_end)
                .ok_or_else(|| err("저장 검증 실패: cell 태그 범위가 손상되었습니다."))?;
            let Some(cell_ref) = extract_attr(cell_tag, "r")? else {
                return Err(err("저장 검증 실패: cell에 r 속성이 없습니다."));
            };
            let cell =
                Self::parse_cell_position(cell_ref.as_ref(), "저장 검증 실패: cell reference")?;
            let cell_self_closing = cell_tag.trim_ascii_end().ends_with("/>");
            let cursor_after_cell =
                Self::cell_cursor_after(sheet_xml, cell_open_end, cell_self_closing)?;
            if cell.row >= min_row && cell.col <= max_col && !cell_self_closing {
                let body_span = Self::cell_body_span(sheet_xml, cell_open_end)?;
                let cell_body = sheet_xml
                    .get(body_span)
                    .ok_or_else(|| err("저장 검증 실패: cell 본문 범위가 손상되었습니다."))?;
                let has_payload = if find_start_tag(cell_body, "f", 0).is_some() {
                    true
                } else if let Some(raw_value) = extract_first_tag_text(cell_body, "v")? {
                    let value = decode_xml_entities(raw_value)?;
                    !value.trim().is_empty()
                } else {
                    extract_all_tag_text(cell_body, "t")?.is_some_and(|text| !text.is_empty())
                };
                if !has_payload {
                    cursor = cursor_after_cell;
                    continue;
                }
                last_row = Some(last_row.map_or(cell.row, |row: u32| row.max(cell.row)));
            }
            cursor = cursor_after_cell;
        }
        Ok(last_row)
    }
    fn worksheet_range_dimension_matches(
        sheet_xml: &str,
        sheet_name: &str,
        actual_bounds: &CellBounds,
    ) -> Result<()> {
        let dim_start = find_start_tag(sheet_xml, "dimension", 0).ok_or_else(|| {
            err(format!(
                "저장 검증 실패: worksheet dimension 태그가 없습니다: {sheet_name}"
            ))
        })?;
        let dim_end = find_tag_end(sheet_xml, dim_start).ok_or_else(|| {
            err(format!(
                "저장 검증 실패: worksheet dimension 태그가 손상되었습니다: {sheet_name}"
            ))
        })?;
        let dim_tag = sheet_xml.get(dim_start..=dim_end).ok_or_else(|| {
            err(format!(
                "저장 검증 실패: worksheet dimension 태그 범위가 손상되었습니다: {sheet_name}"
            ))
        })?;
        let Some(dim_ref) = extract_attr(dim_tag, "ref")? else {
            return Err(err(format!(
                "저장 검증 실패: worksheet dimension ref 속성이 없습니다: {sheet_name}"
            )));
        };
        let declared_bounds =
            Self::parse_cell_bounds(dim_ref.as_ref(), "저장 검증 실패: worksheet dimension ref")?;
        if declared_bounds.min.col == actual_bounds.min.col
            && declared_bounds.min.row == actual_bounds.min.row
            && declared_bounds.max.col == actual_bounds.max.col
            && declared_bounds.max.row == actual_bounds.max.row
        {
            return Ok(());
        }
        Err(err(format!(
            "저장 검증 실패: worksheet dimension이 실제 cell 범위와 다릅니다: {sheet_name}, declared={}, actual=col {} row {}:col {} row {}",
            dim_ref.as_ref(),
            actual_bounds.min.col,
            actual_bounds.min.row,
            actual_bounds.max.col,
            actual_bounds.max.row
        )))
    }
    fn worksheet_ref_formula_refs(sheet_xml: &str, sheet_name: &str) -> Result<()> {
        let mut cursor = 0_usize;
        while let Some(formula_start) = find_start_tag(sheet_xml, "f", cursor) {
            let open_end = find_tag_end(sheet_xml, formula_start).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: formula 태그가 손상되었습니다: {sheet_name}"
                ))
            })?;
            let formula_tag = sheet_xml.get(formula_start..=open_end).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: formula 태그 범위가 손상되었습니다: {sheet_name}"
                ))
            })?;
            if formula_tag.trim_ascii_end().ends_with("/>") {
                cursor = open_end
                    .checked_add(1)
                    .ok_or_else(|| err("저장 검증 실패: formula cursor 계산 실패"))?;
                continue;
            }
            let body_start = open_end
                .checked_add(1)
                .ok_or_else(|| err("저장 검증 실패: formula 본문 시작 계산 실패"))?;
            let formula_end = find_end_tag(sheet_xml, "f", body_start).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: formula 종료 태그가 없습니다: {sheet_name}"
                ))
            })?;
            let formula_raw = sheet_xml.get(body_start..formula_end).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: formula 본문 범위가 손상되었습니다: {sheet_name}"
                ))
            })?;
            if decode_xml_entities(formula_raw)?.contains("#REF!") {
                return Err(err(format!(
                    "저장 검증 실패: worksheet에 #REF! 수식이 있습니다: {sheet_name}"
                )));
            }
            let formula_close_end = find_tag_end(sheet_xml, formula_end).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: formula 종료 태그가 손상되었습니다: {sheet_name}"
                ))
            })?;
            cursor = formula_close_end
                .checked_add(1)
                .ok_or_else(|| err("저장 검증 실패: formula cursor 계산 실패"))?;
        }
        Ok(())
    }
    fn worksheet_row_and_col_refs_valid(sheet_xml: &str, sheet_name: &str) -> Result<()> {
        let mut seen_rows: HashSet<u32> = HashSet::new();
        let mut cursor = 0_usize;
        while let Some(row_start) = find_start_tag(sheet_xml, "row", cursor) {
            let row_end = find_tag_end(sheet_xml, row_start)
                .ok_or_else(|| err("저장 검증 실패: row 태그가 손상되었습니다."))?;
            let row_tag = sheet_xml
                .get(row_start..=row_end)
                .ok_or_else(|| err("저장 검증 실패: row 태그 범위가 손상되었습니다."))?;
            let Some(row_ref) = extract_attr(row_tag, "r")? else {
                return Err(err(format!(
                    "저장 검증 실패: worksheet row에 r 속성이 없습니다: {sheet_name}"
                )));
            };
            let row_num = row_ref.parse::<u32>().map_err(|source| {
                err_with_source("저장 검증 실패: row reference 해석 실패", source)
            })?;
            if !seen_rows.insert(row_num) {
                return Err(err(format!(
                    "저장 검증 실패: worksheet에 중복 row reference가 있습니다: {sheet_name}!{row_num}"
                )));
            }
            cursor = row_end
                .checked_add(1)
                .ok_or_else(|| err("저장 검증 실패: row cursor 계산 실패"))?;
        }
        let mut col_cursor = 0_usize;
        while let Some(col_start) = find_start_tag(sheet_xml, "col", col_cursor) {
            let col_end = find_tag_end(sheet_xml, col_start)
                .ok_or_else(|| err("저장 검증 실패: col 태그가 손상되었습니다."))?;
            let col_tag = sheet_xml
                .get(col_start..=col_end)
                .ok_or_else(|| err("저장 검증 실패: col 태그 범위가 손상되었습니다."))?;
            let Some(min_col_text) = extract_attr(col_tag, "min")? else {
                return Err(err("저장 검증 실패: col min 속성이 없습니다."));
            };
            let Some(max_col_text) = extract_attr(col_tag, "max")? else {
                return Err(err("저장 검증 실패: col max 속성이 없습니다."));
            };
            let min_col = min_col_text.parse::<u32>().map_err(|source| {
                err_with_source("저장 검증 실패: col min reference 해석 실패", source)
            })?;
            let max_col = max_col_text.parse::<u32>().map_err(|source| {
                err_with_source("저장 검증 실패: col max reference 해석 실패", source)
            })?;
            if max_col < min_col {
                return Err(err(format!(
                    "저장 검증 실패: worksheet col 범위 순서가 올바르지 않습니다: {sheet_name}, min={min_col}, max={max_col}"
                )));
            }
            if max_col >= FAR_ARTIFACT_MIN_COL {
                return Err(err(format!(
                    "저장 검증 실패: worksheet에 원거리 col artifact가 있습니다: {sheet_name}, min={min_col}, max={max_col}"
                )));
            }
            col_cursor = col_end
                .checked_add(1)
                .ok_or_else(|| err("저장 검증 실패: col cursor 계산 실패"))?;
        }
        Ok(())
    }
    fn worksheet_summary(
        &self,
        sheets: &[SheetInfo],
        shared_entry_count: Option<usize>,
    ) -> Result<WorksheetSemanticSummary> {
        let mut shared_ref_count = 0_usize;
        let mut master_auto_filter_ref = None;
        for sheet in sheets {
            let sheet_xml = self.container.read_text(&sheet.path)?;
            if sheet.name == "유류비" {
                Self::worksheet_cell_refs_unique(&sheet_xml, &sheet.name)?;
                Self::worksheet_row_and_col_refs_valid(&sheet_xml, &sheet.name)?;
                Self::worksheet_ref_formula_refs(&sheet_xml, &sheet.name)?;
                let (bounds, count) = Self::worksheet_bounds_and_shared_refs(
                    &sheet_xml,
                    &sheet.name,
                    shared_entry_count,
                )?;
                if let Some(actual_bounds) = bounds.as_ref() {
                    Self::worksheet_range_dimension_matches(
                        &sheet_xml,
                        &sheet.name,
                        actual_bounds,
                    )?;
                }
                master_auto_filter_ref =
                    Self::worksheet_filter_semantics(&sheet_xml, &sheet.name, bounds.as_ref())?;
                shared_ref_count = shared_ref_count
                    .checked_add(count)
                    .ok_or_else(|| err("저장 검증 실패: shared string 참조 수 계산 실패"))?;
                continue;
            }
            Self::worksheet_cell_refs_unique(&sheet_xml, &sheet.name)?;
            Self::worksheet_row_and_col_refs_valid(&sheet_xml, &sheet.name)?;
            Self::worksheet_ref_formula_refs(&sheet_xml, &sheet.name)?;
            let (bounds, count) = Self::worksheet_bounds_and_shared_refs(
                &sheet_xml,
                &sheet.name,
                shared_entry_count,
            )?;
            if let Some(actual_bounds) = bounds.as_ref() {
                Self::worksheet_range_dimension_matches(&sheet_xml, &sheet.name, actual_bounds)?;
            }
            Self::worksheet_filter_semantics(&sheet_xml, &sheet.name, bounds.as_ref())?;
            shared_ref_count = shared_ref_count
                .checked_add(count)
                .ok_or_else(|| err("저장 검증 실패: shared string 참조 수 계산 실패"))?;
        }
        Ok(WorksheetSemanticSummary {
            master_auto_filter_ref,
            shared_ref_count,
        })
    }
}
struct TempArchivePromotion<'path> {
    target_xlsx: &'path Path,
    temp_archive: &'path Path,
}
impl TempArchivePromotion<'_> {
    fn promote(&self) -> Result<()> {
        let mut last_error = None;
        for attempt in 1..=TEMP_ARCHIVE_PROMOTION_ATTEMPTS {
            match fs::rename(self.temp_archive, self.target_xlsx) {
                Ok(()) => {
                    cfg_select! {
                        any(target_os = "linux", target_os = "macos") => {
                            if let Err(source_err) = fs::OpenOptions::new()
                                .read(true)
                                .open(self.target_xlsx)
                                .and_then(|file| file.sync_all())
                            {
                                let target_path = self.target_xlsx.display().to_string();
                                write_durability_warning("파일", &target_path, &source_err);
                            }
                            let parent = self
                                .target_xlsx
                                .parent()
                                .filter(|path| !path.as_os_str().is_empty())
                                .map_or_else(|| Path::new("."), |path| path);
                            if let Err(source_err) =
                                fs::File::open(parent).and_then(|dir| dir.sync_all())
                            {
                                let parent_path = parent.display().to_string();
                                write_durability_warning("폴더", &parent_path, &source_err);
                            }
                        }
                        _ => {}
                    }
                    return Ok(());
                }
                Err(source_err) => {
                    last_error = Some(source_err);
                    if attempt < TEMP_ARCHIVE_PROMOTION_ATTEMPTS {
                        thread::sleep(TEMP_ARCHIVE_PROMOTION_RETRY_DELAY);
                    }
                }
            }
        }
        let Some(source_err) = last_error else {
            return Err(err("xlsx 저장 시도 횟수가 비정상적으로 비어 있습니다."));
        };
        Err(err_with_source(
            path_pair_context_message("xlsx 저장 실패", self.temp_archive, self.target_xlsx),
            source_err,
        ))
    }
}
impl XlsxContainer {
    pub(super) fn load_sheet_catalog(&self, workbook_xml: &str) -> Result<Vec<SheetInfo>> {
        let rels_xml = self.read_text("xl/_rels/workbook.xml.rels")?;
        let relationship_count = rels_xml.matches("<Relationship").count();
        let mut rid_to_rel: HashMap<Cow<'_, str>, WorkbookRelationship<'_>> = HashMap::new();
        rid_to_rel
            .try_reserve(relationship_count)
            .map_err(|source| {
                err_with_source(
                    format!("workbook 관계 맵 메모리 확보 실패: {relationship_count} entries"),
                    source,
                )
            })?;
        let mut rels_cursor = 0_usize;
        while let Some(rel_start) = find_start_tag(&rels_xml, "Relationship", rels_cursor) {
            let Some(rel_end) = find_tag_end(&rels_xml, rel_start) else {
                return Err(err("workbook Relationship 시작 태그가 손상되었습니다."));
            };
            let Some(tag) = rels_xml.get(rel_start..=rel_end) else {
                return Err(err("workbook Relationship 태그 범위가 손상되었습니다."));
            };
            let id = extract_attr(tag, "Id")?
                .ok_or_else(|| err("workbook.xml.rels의 Relationship에 Id 속성이 없습니다."))?;
            let target = extract_attr(tag, "Target")?
                .ok_or_else(|| err("workbook.xml.rels의 Relationship에 Target 속성이 없습니다."))?;
            let type_ = extract_attr(tag, "Type")?
                .ok_or_else(|| err("workbook.xml.rels의 Relationship에 Type 속성이 없습니다."))?;
            let target_mode = extract_attr(tag, "TargetMode")?;
            let relationship = WorkbookRelationship {
                target,
                target_mode,
                type_,
            };
            let HashEntry::Vacant(entry) = rid_to_rel.entry(id) else {
                return Err(err("workbook.xml.rels에 중복 Relationship Id가 있습니다."));
            };
            entry.insert(relationship);
            rels_cursor = rel_end
                .checked_add(1)
                .ok_or_else(|| err("다음 workbook Relationship 위치 계산에 실패했습니다."))?;
        }
        let sheet_count = workbook_xml.matches("<sheet").count();
        let mut sheets = Vec::new();
        sheets.try_reserve_exact(sheet_count).map_err(|source| {
            err_with_source(
                format!("시트 순서 목록 메모리 확보 실패: {sheet_count} sheets"),
                source,
            )
        })?;
        let mut sheet_cursor = 0_usize;
        while let Some(sheet_start) = find_start_tag(workbook_xml, "sheet", sheet_cursor) {
            let Some(sheet_end) = find_tag_end(workbook_xml, sheet_start) else {
                return Err(err("workbook.xml의 sheet 시작 태그가 손상되었습니다."));
            };
            let Some(tag) = workbook_xml.get(sheet_start..=sheet_end) else {
                return Err(err("workbook.xml의 sheet 태그 범위가 손상되었습니다."));
            };
            let Some(name) = extract_attr(tag, "name")? else {
                return Err(err("workbook.xml의 sheet에 name 속성이 없습니다."));
            };
            let Some(rid) = extract_attr(tag, "r:id")? else {
                return Err(err("workbook.xml의 sheet에 r:id 속성이 없습니다."));
            };
            let Some(relationship) = rid_to_rel.get(rid.as_ref()) else {
                return Err(err(format!(
                    "workbook.xml.rels에서 sheet 관계 target을 찾지 못했습니다: {}",
                    rid.as_ref()
                )));
            };
            let target_text = relationship.internal_worksheet_target(rid.as_ref())?;
            let resolved = if target_text.starts_with('/') {
                return Err(err(format!(
                    "sheet 관계 target에 절대 경로는 허용되지 않습니다: {target_text}"
                )));
            } else {
                let mut combined: PathBuf = "xl".into();
                for segment in target_text.split('/').filter(|segment| !segment.is_empty()) {
                    combined.push(segment);
                }
                let normalized = normalize_safe_relative_path(&combined, target_text)?;
                let resolved_path = path_to_slashes(&normalized, target_text)?;
                if resolved_path.is_empty() {
                    return Err(err(format!(
                        "sheet 관계 target 정규화 결과가 비어 있습니다: {target_text}"
                    )));
                }
                resolved_path
            };
            sheets.push(SheetInfo {
                name: name.into_owned(),
                path: resolved,
            });
            sheet_cursor = sheet_end
                .checked_add(1)
                .ok_or_else(|| err("workbook.xml의 다음 sheet 위치 계산에 실패했습니다."))?;
        }
        if sheets.is_empty() {
            return Err(err("workbook에서 시트 정보를 찾지 못했습니다."));
        }
        Ok(sheets)
    }
    pub fn open(source_xlsx: &Path) -> Result<Self> {
        if !source_xlsx.try_exists().map_err(|source_err| {
            err_with_source(
                path_context_message("xlsx 파일 경로 확인 실패", source_xlsx),
                source_err,
            )
        })? {
            return Err(err(prefixed_message(
                "xlsx 파일이 없습니다: ",
                source_xlsx.display(),
            )));
        }
        let base = env::temp_dir();
        let mut cleanup = WorkDirCleanup {
            path: reserve_unique_temp_entry(
                |pid, nanos, seq| base.join(format!("fcupdater_{pid}_{nanos}_{seq}")),
                |path| fs::DirBuilder::new().create(path),
                "임시 작업 폴더 생성 실패",
                "임시 작업 폴더 생성 시도가 모두 실패했습니다. 잠시 후 다시 시도하세요.".into(),
            )?,
            keep: false,
        };
        let unpack_dir = cleanup.path.join("unzipped");
        create_dir_all_checked(&unpack_dir, "임시 폴더 생성 실패")?;
        ZipArchiveExtractor {
            archive_path: source_xlsx,
            unpack_dir: unpack_dir.as_path(),
        }
        .extract()?;
        cleanup.keep = true;
        let work_dir = mem::take(&mut cleanup.path);
        Ok(Self {
            unpack_dir,
            work_dir,
        })
    }
    pub(super) fn read_shared_strings_text(&self) -> Result<Option<String>> {
        let path = self.resolve_relative_path("xl/sharedStrings.xml")?;
        let file = match fs::File::open(&path) {
            Ok(file) => file,
            Err(io_err) if io_err.kind() == ErrorKind::NotFound => return Ok(None),
            Err(io_err) => {
                return Err(err_with_source(
                    path_context_message("파일 열기 실패", &path),
                    io_err,
                ));
            }
        };
        Self::read_text_from_file(&path, file).map(Some)
    }
    pub fn read_text(&self, relative_path: &str) -> Result<String> {
        let path = self.resolve_relative_path(relative_path)?;
        let file = fs::File::open(&path).map_err(|source_err| {
            err_with_source(path_context_message("파일 열기 실패", &path), source_err)
        })?;
        Self::read_text_from_file(&path, file)
    }
    fn read_text_from_file(path: &Path, file: fs::File) -> Result<String> {
        let file_size = file
            .metadata()
            .map_err(|source_err| {
                err_with_source(
                    path_context_message("파일 메타데이터 조회 실패", path),
                    source_err,
                )
            })?
            .len();
        if file_size > MAX_XLSX_TEXT_PART_BYTES {
            return Err(err(format!(
                "xlsx XML part가 너무 큽니다: {} ({file_size} bytes, 최대 {MAX_XLSX_TEXT_PART_BYTES} bytes)",
                path.display()
            )));
        }
        let data_len = usize::try_from(file_size)
            .map_err(|source| err_with_source("xlsx XML part 크기 변환 실패", source))?;
        let read_limit = MAX_XLSX_TEXT_PART_BYTES
            .checked_add(1)
            .ok_or_else(|| err("xlsx XML part 읽기 한도 계산 실패"))?;
        let mut reader = file.take(read_limit);
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(data_len)
            .map_err(|source| err_with_source("xlsx XML part 메모리 확보 실패", source))?;
        reader.read_to_end(&mut bytes).map_err(|source_err| {
            err_with_source(path_context_message("파일 읽기 실패", path), source_err)
        })?;
        if u64::try_from(bytes.len()).is_ok_and(|actual| actual > MAX_XLSX_TEXT_PART_BYTES) {
            return Err(err(format!(
                "xlsx XML part가 너무 큽니다: {} (최대 {MAX_XLSX_TEXT_PART_BYTES} bytes)",
                path.display()
            )));
        }
        String::from_utf8(bytes).map_err(|source| {
            err_with_source(path_context_message("파일 UTF-8 해석 실패", path), source)
        })
    }
    pub(super) fn remove_calc_chain_if_exists(&self) -> Result<()> {
        let path = self.resolve_relative_path("xl/calcChain.xml")?;
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(io_err) if io_err.kind() == ErrorKind::NotFound => Ok(()),
            Err(io_err) => Err(err_with_source(
                path_context_message("파일 삭제 실패", &path),
                io_err,
            )),
        }
    }
    fn resolve_relative_path(&self, relative_path: &str) -> Result<PathBuf> {
        let path = normalize_safe_relative_path(Path::new(relative_path), relative_path)?;
        Ok(self.unpack_dir.join(path))
    }
    pub fn save(&self, target_xlsx: &Path, verification: &SaveVerification) -> Result<()> {
        let parent = if let Some(parent) = target_xlsx.parent() {
            create_dir_all_checked(parent, "저장 폴더 생성 실패")?;
            parent
        } else {
            Path::new(".")
        };
        let file_name = target_xlsx
            .file_name()
            .and_then(|file_name_os| file_name_os.to_str())
            .map_or("workbook.xlsx", |name| name);
        let tmp_archive = reserve_unique_temp_entry(
            |pid, nanos, seq| parent.join(format!(".{file_name}.tmp_{pid}_{nanos}_{seq}")),
            |path| {
                fs::File::create_new(path)?;
                Ok(())
            },
            "임시 저장 파일 생성 실패",
            prefixed_message("임시 저장 파일 경로 생성 실패: ", target_xlsx.display()),
        )?;
        let result = (|| -> Result<()> {
            ZipArchiveBuilder {
                archive_path: tmp_archive.as_path(),
                root: self.unpack_dir.as_path(),
            }
            .create()?;
            match *verification {
                SaveVerification::Skip => {}
                SaveVerification::Verify => {
                    SavedArchiveVerifier {
                        saved_archive: &tmp_archive,
                    }
                    .verify()?;
                }
            }
            TempArchivePromotion {
                target_xlsx,
                temp_archive: &tmp_archive,
            }
            .promote()?;
            Ok(())
        })();
        match result {
            Ok(()) => Ok(()),
            Err(source) => match fs::remove_file(&tmp_archive) {
                Ok(()) => Err(source),
                Err(error) if error.kind() == ErrorKind::NotFound => Err(source),
                Err(error) => Err(err_with_source(
                    path_source_message("xlsx 임시 저장 파일 삭제 실패", &tmp_archive, error),
                    source,
                )),
            },
        }
    }
    pub fn write_text(&self, relative_path: &str, content: &str) -> Result<()> {
        let path = self.resolve_relative_path(relative_path)?;
        if let Some(parent) = path.parent() {
            create_dir_all_checked(parent, "폴더 생성 실패")?;
        }
        fs::write(&path, content).map_err(|source_err| {
            err_with_source(path_context_message("파일 쓰기 실패", &path), source_err)
        })
    }
}
impl Drop for XlsxContainer {
    fn drop(&mut self) {
        match fs::remove_dir_all(&self.work_dir) {
            Ok(()) | Err(_) => {}
        }
    }
}
cfg_select! {
    any(target_os = "linux", target_os = "macos") => {
        fn write_durability_warning(path_kind: &str, path_text: &str, source_err: &io::Error) {
            let mut err = stderr().lock();
            match IoWrite::write_fmt(
                &mut err,
                format_args!("경고: 저장 내구성 동기화 실패({path_kind}): {path_text} ({source_err})\n"),
            ) {
                Ok(()) | Err(_) => {}
            }
        }
    }
    _ => {}
}
fn create_dir_all_checked(path: &Path, failure_label: &str) -> Result<()> {
    fs::create_dir_all(path).map_err(|source_err| {
        err_with_source(path_context_message(failure_label, path), source_err)
    })
}
fn normalize_safe_relative_path(path: &Path, relative_path: &str) -> Result<PathBuf> {
    let mut normalized = PathBuf::new();
    for component in path.components() {
        match component {
            Component::CurDir => {}
            Component::Normal(segment) => normalized.push(segment),
            Component::ParentDir => {
                return Err(err(relative_path_policy_message(
                    "상위 경로 탐색은 허용되지 않습니다: ",
                    relative_path,
                )));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(err(relative_path_policy_message(
                    "절대 경로는 허용되지 않습니다: ",
                    relative_path,
                )));
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        return Err(err(relative_path_policy_message(
            "상대 경로가 비어 있습니다: ",
            relative_path,
        )));
    }
    Ok(normalized)
}
fn reserve_unique_temp_entry<FBuild, FCreate>(
    build_path: FBuild,
    mut create_entry: FCreate,
    create_failure_label: &str,
    exhausted_message: String,
) -> Result<PathBuf>
where
    FBuild: Fn(u32, u128, u32) -> PathBuf,
    FCreate: FnMut(&Path) -> io::Result<()>,
{
    let pid = process::id();
    for seq in 0..1024_u32 {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|source| err_with_source("임시 xlsx 경로 시각 계산 실패", source))?
            .as_nanos();
        let path = build_path(pid, nanos, seq);
        match create_entry(&path) {
            Ok(()) => return Ok(path),
            Err(io_err) if io_err.kind() == ErrorKind::AlreadyExists => {
                thread::sleep(Duration::from_micros(50));
            }
            Err(io_err) => {
                return Err(err_with_source(
                    path_context_message(create_failure_label, &path),
                    io_err,
                ));
            }
        }
    }
    Err(err(exhausted_message))
}
fn relative_path_policy_message(prefix: &str, relative_path: &str) -> String {
    format!("{prefix}{relative_path}")
}
