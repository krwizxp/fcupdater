use super::path_util::reject_windows_special_component;
use super::{
    ArchiveFingerprint, CHANGE_LOG_SHEET_NAME, CHANGE_LOG_SHEET_PATH, MASTER_SHEET_NAME,
    MASTER_SHEET_PATH, SPREADSHEETML_NAMESPACE, SaveVerification, ZipArchiveBuilder,
    ZipArchiveExtractor,
    path_util::path_to_slashes,
    writer::parse_ref_with_locks,
    xml::{
        XmlAttrScanner, XmlScanner, XmlTag, decode_xml_entities, extract_all_tag_text,
        extract_attr, extract_first_tag_text, find_end_tag, find_start_tag, find_tag_end,
    },
    zip_archive::scan_open_archive,
};
use crate::diagnostic::{
    AppError, Result, err, err_with_source, path_context_message, path_pair_context_message,
};
use crate::temp_entry::{TempEntryKind, cleanup_stale_temp_entries, reserve_unique_temp_entry};
use crate::validate_regular_file;
use alloc::borrow::Cow;
use core::{iter, num::NonZeroU32, range::Range, str};
use std::{
    collections::{HashMap, HashSet, hash_map::Entry as HashEntry},
    env, fs,
    io::{self, Read as _, Write as _, stderr},
    path::{Component, Path, PathBuf},
};
cfg_select! {
    any(target_os = "linux", target_os = "macos") => {
        use std::os::unix::fs::{DirBuilderExt as _, OpenOptionsExt as _};
    }
    _ => {}
}
mod atomic_replace;
const WORK_DIR_PREFIX: &str = "fcupdater_";
const MAX_XLSX_TEXT_PART_BYTES: u64 = 64 * 1024 * 1024;
const CHANGELOG_DATA_START_ROW: u32 = 4;
const FAR_ARTIFACT_MIN_COL: u32 = 16_382;
const MASTER_FILTER_DATA_START_ROW: u32 = 15;
const MASTER_FILTER_HEADER_ROW: u32 = 14;
const MASTER_FILTER_START_COL: u32 = 1;
const CONTENT_TYPES_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/package/2006/content-types";
const OFFICE_DOCUMENT_REL_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
const OFFICE_DOCUMENT_REL_TYPE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument";
const PACKAGE_RELATIONSHIPS_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/package/2006/relationships";
const WORKBOOK_CONTENT_TYPE: &str =
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml";
const WORKBOOK_PART_NAME: &str = "/xl/workbook.xml";
const WORKBOOK_REL_TARGET: &str = "xl/workbook.xml";
const WORKSHEET_REL_TYPE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet";
#[derive(Debug)]
pub(crate) struct XlsxContainer {
    _work_dir: WorkDirCleanup,
    source_fingerprint: ArchiveFingerprint,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    source_permissions: fs::Permissions,
    unpack_dir: PathBuf,
}
#[derive(Debug)]
struct WorkDirCleanup {
    path: PathBuf,
}
struct ReservedTempArchive {
    file: Option<fs::File>,
    path: PathBuf,
    remove_on_drop: bool,
}
impl Drop for WorkDirCleanup {
    fn drop(&mut self) {
        if let Err(source) = fs::remove_dir_all(&self.path)
            && source.kind() != io::ErrorKind::NotFound
        {
            write_path_warning("xlsx 작업 폴더 정리 실패", &self.path, &source);
        }
    }
}
impl ReservedTempArchive {
    const fn disable_drop_cleanup(&mut self) {
        self.remove_on_drop = false;
    }
    fn path(&self) -> &Path {
        &self.path
    }
    fn verify_saved_archive(&self) -> Result<()> {
        let saved_archive = self.path();
        let saved_container = XlsxContainer::open(saved_archive).map_err(|source_err| {
            err_with_source(
                path_context_message(
                    "저장 검증 실패: 저장 직후 압축 해제 점검에 실패했습니다",
                    saved_archive,
                ),
                source_err,
            )
        })?;
        let workbook_xml = saved_container
            .read_text("xl/workbook.xml")
            .map_err(|source_err| {
                err_with_source(
                    path_context_message("저장 검증 실패: workbook.xml 읽기 실패", saved_archive),
                    source_err,
                )
            })?;
        let master_auto_filter_ref = ArchiveSemanticVerifier {
            container: &saved_container,
            workbook_xml: &workbook_xml,
        }
        .verify()?;
        let saved_workbook =
            super::writer::Workbook::from_container(saved_container).map_err(|source_err| {
                err_with_source(
                    path_context_message(
                        "저장 검증 실패: 저장 직후 재열기 점검에 실패했습니다",
                        saved_archive,
                    ),
                    source_err,
                )
            })?;
        let normalized = ArchiveSemanticVerifier::normalize_filter_ref(
            &master_auto_filter_ref,
            &FilterRefPolicy::AnyA1,
        )?;
        let (start_ref, end_ref) = normalized.split_once(':').map_or(
            (normalized.as_str(), normalized.as_str()),
            |(start, end)| (start, end),
        );
        let header_row = saved_archive_trailing_row_num(start_ref)?;
        if header_row != 14 {
            return Err(err(format!(
                "저장 검증 실패: 유류비 autoFilter 시작 행이 고정 헤더 행과 다릅니다: {header_row}"
            )));
        }
        let filter_end_row = saved_archive_trailing_row_num(end_ref)?;
        saved_workbook.verify_master_address_data_end_row(filter_end_row)?;
        Ok(())
    }
    fn write_archive_from(
        &mut self,
        root: &Path,
        #[cfg(any(target_os = "linux", target_os = "macos"))] permissions: fs::Permissions,
    ) -> Result<()> {
        let Some(file) = self.file.take() else {
            return Err(err("xlsx 임시 저장 파일 handle이 이미 닫혔습니다."));
        };
        ZipArchiveBuilder {
            archive_path: self.path(),
            file,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            permissions,
            root,
        }
        .create()
    }
}
impl Drop for ReservedTempArchive {
    fn drop(&mut self) {
        drop(self.file.take());
        if self.remove_on_drop
            && let Err(source) = fs::remove_file(&self.path)
            && source.kind() != io::ErrorKind::NotFound
        {
            write_path_warning("xlsx 임시 저장 파일 정리 실패", &self.path, &source);
        }
    }
}
struct ArchiveSemanticVerifier<'container> {
    container: &'container XlsxContainer,
    workbook_xml: &'container str,
}
struct WorksheetSemanticVerifier<'xml> {
    name: &'xml str,
    shared_entry_count: usize,
    xml: &'xml str,
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
    unique_entries: usize,
}
struct WorksheetNamespaceElement<'xml> {
    local_name: &'xml str,
    name: &'xml str,
    raw: &'xml str,
}
struct DirectXmlChild<'xml> {
    local_name: &'xml str,
    raw: &'xml str,
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
        if self.target.is_empty() {
            return Err(err(format!(
                "workbook.xml sheet 관계 Target이 비어 있습니다: rid={rid}"
            )));
        }
        let target = self.target.as_ref();
        if target.contains('\\') {
            return Err(err(format!(
                "workbook.xml sheet 관계 Target에는 백슬래시를 사용할 수 없습니다: rid={rid}, target={target}"
            )));
        }
        if target.bytes().any(|byte| matches!(byte, b'?' | b'#')) {
            return Err(err(format!(
                "workbook.xml sheet 관계 Target에는 query/fragment를 사용할 수 없습니다: rid={rid}, target={target}"
            )));
        }
        if target
            .split('/')
            .next()
            .is_some_and(|segment| segment.contains(':'))
        {
            return Err(err(format!(
                "workbook.xml sheet 관계 Target에는 URI scheme을 사용할 수 없습니다: rid={rid}, target={target}"
            )));
        }
        Ok(target)
    }
}
impl ArchiveSemanticVerifier<'_> {
    fn filter_database_ref(&self) -> Result<Cow<'_, str>> {
        let quoted_sheet = "'유류비'!";
        let plain_sheet = "유류비!";
        let span = super::workbook_defined_name_content_span(
            self.workbook_xml,
            "_xlnm._FilterDatabase",
            0,
            quoted_sheet,
            plain_sheet,
        )?;
        let raw_ref = self
            .workbook_xml
            .get(span)
            .ok_or_else(|| err("저장 검증 실패: definedName 본문 범위가 손상되었습니다."))?;
        decode_xml_entities(raw_ref)
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
        normalized
            .try_reserve_exact(range_text.len())
            .map_err(|source| {
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
                if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err(err(format!(
                        "{context}: {attr_name}가 음이 아닌 10진수 형식이 아닙니다."
                    )));
                }
                value.parse::<usize>().map_err(|source| {
                    err_with_source(format!("{context}: {attr_name} 해석 실패"), source)
                })
            })
            .transpose()
    }
    fn shared_strings_summary(&self) -> Result<SharedStringsSummary> {
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
            unique_entries,
        })
    }
    fn verify(&self) -> Result<String> {
        self.container
            .ensure_fixed_sheet_catalog(self.workbook_xml)?;
        let shared_summary = self.shared_strings_summary()?;
        let shared_entry_count = shared_summary.unique_entries;
        let (master_auto_filter_ref, shared_ref_count) =
            self.worksheet_summary(shared_entry_count)?;
        let filter_database_ref = self.filter_database_ref()?;
        let Some(auto_filter_ref) = master_auto_filter_ref else {
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
        if let Some(declared_count) = shared_summary.declared_total
            && declared_count != shared_ref_count
        {
            return Err(err(format!(
                "저장 검증 실패: sharedStrings count가 실제 참조 수와 다릅니다: declared={declared_count}, actual={shared_ref_count}"
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
        Ok(auto_filter_ref)
    }
    fn worksheet_summary(&self, shared_entry_count: usize) -> Result<(Option<String>, usize)> {
        let summarize = |name: &str, path: &str| -> Result<(Option<String>, usize)> {
            let sheet_xml = self.container.read_text(path)?;
            validate_worksheet_core_namespaces(&sheet_xml, name)?;
            let verifier = WorksheetSemanticVerifier {
                name,
                shared_entry_count,
                xml: &sheet_xml,
            };
            verifier.summarize()
        };
        let (master_auto_filter_ref, master_shared_ref_count) =
            summarize(MASTER_SHEET_NAME, MASTER_SHEET_PATH)?;
        let (_, change_log_shared_ref_count) =
            summarize(CHANGE_LOG_SHEET_NAME, CHANGE_LOG_SHEET_PATH)?;
        let shared_ref_count = master_shared_ref_count
            .checked_add(change_log_shared_ref_count)
            .ok_or_else(|| err("저장 검증 실패: shared string 참조 수 계산 실패"))?;
        Ok((master_auto_filter_ref, shared_ref_count))
    }
}
impl WorksheetSemanticVerifier<'_> {
    fn bounds_and_shared_refs(&self) -> Result<(Option<CellBounds>, usize)> {
        let Self {
            name,
            shared_entry_count,
            xml,
        } = *self;
        let mut bounds: Option<CellBounds> = None;
        let mut shared_ref_count = 0_usize;
        let mut cursor = 0_usize;
        while let Some(cell_start) = find_start_tag(xml, "c", cursor) {
            let cell_open_end = find_tag_end(xml, cell_start).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: cell 태그가 손상되었습니다: {name}"
                ))
            })?;
            let cell_tag = xml.get(cell_start..=cell_open_end).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: cell 태그 범위가 손상되었습니다: {name}"
                ))
            })?;
            let Some(cell_ref) = extract_attr(cell_tag, "r")? else {
                return Err(err(format!(
                    "저장 검증 실패: cell에 r 속성이 없습니다: {name}"
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
                if cell_self_closing {
                    return Err(err(format!(
                        "저장 검증 실패: shared string cell에 <v>가 없습니다: {name}!{}",
                        cell_ref.as_ref()
                    )));
                }
                let body_span = Self::cell_body_span(xml, cell_open_end)?;
                let cell_body = xml
                    .get(body_span)
                    .ok_or_else(|| err("저장 검증 실패: cell 본문 범위가 손상되었습니다."))?;
                let Some(raw_index) = extract_first_tag_text(cell_body, "v")? else {
                    return Err(err(format!(
                        "저장 검증 실패: shared string cell에 <v>가 없습니다: {name}!{}",
                        cell_ref.as_ref()
                    )));
                };
                let decoded_index = decode_xml_entities(raw_index)?;
                let trimmed_index = decoded_index.trim();
                if trimmed_index.is_empty()
                    || !trimmed_index.bytes().all(|byte| byte.is_ascii_digit())
                {
                    return Err(err(format!(
                        "저장 검증 실패: shared string index가 음이 아닌 정수가 아닙니다: {name}!{}",
                        cell_ref.as_ref()
                    )));
                }
                let index = trimmed_index.parse::<usize>().map_err(|source| {
                    err_with_source("저장 검증 실패: shared string index 해석 실패", source)
                })?;
                if index >= shared_entry_count {
                    return Err(err(format!(
                        "저장 검증 실패: shared string index가 범위를 벗어났습니다: {name}!{} index={index}, uniqueCount={shared_entry_count}",
                        cell_ref.as_ref()
                    )));
                }
                shared_ref_count = shared_ref_count
                    .checked_add(1)
                    .ok_or_else(|| err("저장 검증 실패: shared string 참조 수 계산 실패"))?;
            }
            cursor = Self::cell_cursor_after(xml, cell_open_end, cell_self_closing)?;
        }
        Ok((bounds, shared_ref_count))
    }
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
    fn cell_refs_unique(&self) -> Result<()> {
        let Self { name, xml, .. } = *self;
        let mut seen_cells: HashSet<u64> = HashSet::new();
        let mut cursor = 0_usize;
        while let Some(cell_start) = find_start_tag(xml, "c", cursor) {
            let cell_open_end = find_tag_end(xml, cell_start)
                .ok_or_else(|| err("저장 검증 실패: cell 태그가 손상되었습니다."))?;
            let cell_tag = xml
                .get(cell_start..=cell_open_end)
                .ok_or_else(|| err("저장 검증 실패: cell 태그 범위가 손상되었습니다."))?;
            let Some(cell_ref) = extract_attr(cell_tag, "r")? else {
                return Err(err(format!(
                    "저장 검증 실패: cell에 r 속성이 없습니다: {name}"
                )));
            };
            let cell =
                Self::parse_cell_position(cell_ref.as_ref(), "저장 검증 실패: cell reference")?;
            if cell.col >= FAR_ARTIFACT_MIN_COL {
                return Err(err(format!(
                    "저장 검증 실패: worksheet에 원거리 cell artifact가 있습니다: {name}!{}",
                    cell_ref.as_ref()
                )));
            }
            let cell_key = (u64::from(cell.row) << 32_u32) | u64::from(cell.col);
            if seen_cells.len() == seen_cells.capacity() {
                seen_cells.try_reserve(1).map_err(|source| {
                    err_with_source(
                        "저장 검증 실패: cell reference 집합 메모리 확보 실패",
                        source,
                    )
                })?;
            }
            if !seen_cells.insert(cell_key) {
                return Err(err(format!(
                    "저장 검증 실패: worksheet에 중복 cell reference가 있습니다: {name}!{}",
                    cell_ref.as_ref()
                )));
            }
            let cell_self_closing = cell_tag.trim_ascii_end().ends_with("/>");
            cursor = Self::cell_cursor_after(xml, cell_open_end, cell_self_closing)?;
        }
        Ok(())
    }
    fn filter_semantics(&self, actual_bounds: Option<&CellBounds>) -> Result<Option<String>> {
        let Self { name, xml, .. } = *self;
        if name == "유류비" {
            let mut cursor = 0_usize;
            let mut found_ref = None;
            while let Some(filter_start) = find_start_tag(xml, "autoFilter", cursor) {
                let filter_end = find_tag_end(xml, filter_start)
                    .ok_or_else(|| err("저장 검증 실패: autoFilter 태그가 손상되었습니다."))?;
                let filter_tag = xml
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
                let expected_last_row = self
                    .meaningful_row_bound(MASTER_FILTER_DATA_START_ROW, filter_bounds.max.col)?
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
        if name != "변경내역" {
            return Ok(None);
        }
        let expected_last_row = self
            .meaningful_row_bound(CHANGELOG_DATA_START_ROW, 13)?
            .unwrap_or(CHANGELOG_DATA_START_ROW);
        let mut delta_mask = 0_u8;
        let mut cursor = 0_usize;
        while let Some(cf_start) = find_start_tag(xml, "conditionalFormatting", cursor) {
            let cf_end = find_tag_end(xml, cf_start).ok_or_else(|| {
                err("저장 검증 실패: 변경내역 conditionalFormatting 태그가 손상되었습니다.")
            })?;
            let cf_tag = xml.get(cf_start..=cf_end).ok_or_else(|| {
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
        for (bit, column) in [(1, "G"), (2, "J"), (4, "M")] {
            if delta_mask & bit == 0 {
                return Err(err(format!(
                    "저장 검증 실패: 변경내역 {column}열 조건부 서식 기준 범위가 없습니다."
                )));
            }
        }
        Ok(None)
    }
    fn meaningful_row_bound(&self, min_row: u32, max_col: u32) -> Result<Option<u32>> {
        let xml = self.xml;
        let mut last_row = None;
        let mut cursor = 0_usize;
        while let Some(cell_start) = find_start_tag(xml, "c", cursor) {
            let cell_open_end = find_tag_end(xml, cell_start)
                .ok_or_else(|| err("저장 검증 실패: cell 태그가 손상되었습니다."))?;
            let cell_tag = xml
                .get(cell_start..=cell_open_end)
                .ok_or_else(|| err("저장 검증 실패: cell 태그 범위가 손상되었습니다."))?;
            let Some(cell_ref) = extract_attr(cell_tag, "r")? else {
                return Err(err("저장 검증 실패: cell에 r 속성이 없습니다."));
            };
            let cell =
                Self::parse_cell_position(cell_ref.as_ref(), "저장 검증 실패: cell reference")?;
            let cell_self_closing = cell_tag.trim_ascii_end().ends_with("/>");
            let cursor_after_cell = Self::cell_cursor_after(xml, cell_open_end, cell_self_closing)?;
            if cell.row >= min_row && cell.col <= max_col && !cell_self_closing {
                let body_span = Self::cell_body_span(xml, cell_open_end)?;
                let cell_body = xml
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
                last_row = Some(last_row.unwrap_or(cell.row).max(cell.row));
            }
            cursor = cursor_after_cell;
        }
        Ok(last_row)
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
        let reference = parse_ref_with_locks(cell_ref)
            .ok_or_else(|| err(format!("{context}: 유효한 A1 cell reference가 아닙니다.")))?;
        Ok(CellPosition {
            col: reference.col,
            row: reference.row,
        })
    }
    fn parse_positive_u32(text: &str, context: &'static str) -> Result<u32> {
        if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err(err(format!("{context}: 양의 10진수 형식이 아닙니다.")));
        }
        let parsed = text
            .parse::<u32>()
            .map_err(|source| err_with_source(context, source))?;
        let Some(non_zero) = NonZeroU32::new(parsed) else {
            return Err(err(format!("{context}는 1 이상이어야 합니다.")));
        };
        Ok(non_zero.get())
    }
    fn range_dimension_matches(&self, actual_bounds: &CellBounds) -> Result<()> {
        let Self { name, xml, .. } = *self;
        let dim_start = find_start_tag(xml, "dimension", 0).ok_or_else(|| {
            err(format!(
                "저장 검증 실패: worksheet dimension 태그가 없습니다: {name}"
            ))
        })?;
        let dim_end = find_tag_end(xml, dim_start).ok_or_else(|| {
            err(format!(
                "저장 검증 실패: worksheet dimension 태그가 손상되었습니다: {name}"
            ))
        })?;
        let dim_tag = xml.get(dim_start..=dim_end).ok_or_else(|| {
            err(format!(
                "저장 검증 실패: worksheet dimension 태그 범위가 손상되었습니다: {name}"
            ))
        })?;
        let Some(dim_ref) = extract_attr(dim_tag, "ref")? else {
            return Err(err(format!(
                "저장 검증 실패: worksheet dimension ref 속성이 없습니다: {name}"
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
            "저장 검증 실패: worksheet dimension이 실제 cell 범위와 다릅니다: {name}, declared={}, actual=col {} row {}:col {} row {}",
            dim_ref.as_ref(),
            actual_bounds.min.col,
            actual_bounds.min.row,
            actual_bounds.max.col,
            actual_bounds.max.row
        )))
    }
    fn ref_formula_refs(&self) -> Result<()> {
        let Self { name, xml, .. } = *self;
        let mut cursor = 0_usize;
        while let Some(formula_start) = find_start_tag(xml, "f", cursor) {
            let open_end = find_tag_end(xml, formula_start).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: formula 태그가 손상되었습니다: {name}"
                ))
            })?;
            let formula_tag = xml.get(formula_start..=open_end).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: formula 태그 범위가 손상되었습니다: {name}"
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
            let formula_end = find_end_tag(xml, "f", body_start).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: formula 종료 태그가 없습니다: {name}"
                ))
            })?;
            let formula_raw = xml.get(body_start..formula_end).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: formula 본문 범위가 손상되었습니다: {name}"
                ))
            })?;
            if decode_xml_entities(formula_raw)?.contains("#REF!") {
                return Err(err(format!(
                    "저장 검증 실패: worksheet에 #REF! 수식이 있습니다: {name}"
                )));
            }
            let formula_close_end = find_tag_end(xml, formula_end).ok_or_else(|| {
                err(format!(
                    "저장 검증 실패: formula 종료 태그가 손상되었습니다: {name}"
                ))
            })?;
            cursor = formula_close_end
                .checked_add(1)
                .ok_or_else(|| err("저장 검증 실패: formula cursor 계산 실패"))?;
        }
        Ok(())
    }
    fn row_and_col_refs_valid(&self) -> Result<()> {
        let Self { name, xml, .. } = *self;
        let mut seen_rows: HashSet<u32> = HashSet::new();
        let mut cursor = 0_usize;
        while let Some(row_start) = find_start_tag(xml, "row", cursor) {
            let row_end = find_tag_end(xml, row_start)
                .ok_or_else(|| err("저장 검증 실패: row 태그가 손상되었습니다."))?;
            let row_tag = xml
                .get(row_start..=row_end)
                .ok_or_else(|| err("저장 검증 실패: row 태그 범위가 손상되었습니다."))?;
            let Some(row_ref) = extract_attr(row_tag, "r")? else {
                return Err(err(format!(
                    "저장 검증 실패: worksheet row에 r 속성이 없습니다: {name}"
                )));
            };
            let row_num = Self::parse_positive_u32(
                row_ref.as_ref(),
                "저장 검증 실패: row reference 해석 실패",
            )?;
            if seen_rows.len() == seen_rows.capacity() {
                seen_rows.try_reserve(1).map_err(|source| {
                    err_with_source(
                        "저장 검증 실패: row reference 집합 메모리 확보 실패",
                        source,
                    )
                })?;
            }
            if !seen_rows.insert(row_num) {
                return Err(err(format!(
                    "저장 검증 실패: worksheet에 중복 row reference가 있습니다: {name}!{row_num}"
                )));
            }
            cursor = row_end
                .checked_add(1)
                .ok_or_else(|| err("저장 검증 실패: row cursor 계산 실패"))?;
        }
        let mut col_cursor = 0_usize;
        while let Some(col_start) = find_start_tag(xml, "col", col_cursor) {
            let col_end = find_tag_end(xml, col_start)
                .ok_or_else(|| err("저장 검증 실패: col 태그가 손상되었습니다."))?;
            let col_tag = xml
                .get(col_start..=col_end)
                .ok_or_else(|| err("저장 검증 실패: col 태그 범위가 손상되었습니다."))?;
            let Some(min_col_text) = extract_attr(col_tag, "min")? else {
                return Err(err("저장 검증 실패: col min 속성이 없습니다."));
            };
            let Some(max_col_text) = extract_attr(col_tag, "max")? else {
                return Err(err("저장 검증 실패: col max 속성이 없습니다."));
            };
            let min_col = Self::parse_positive_u32(
                min_col_text.as_ref(),
                "저장 검증 실패: col min reference 해석 실패",
            )?;
            let max_col = Self::parse_positive_u32(
                max_col_text.as_ref(),
                "저장 검증 실패: col max reference 해석 실패",
            )?;
            if max_col < min_col {
                return Err(err(format!(
                    "저장 검증 실패: worksheet col 범위 순서가 올바르지 않습니다: {name}, min={min_col}, max={max_col}"
                )));
            }
            if max_col >= FAR_ARTIFACT_MIN_COL {
                return Err(err(format!(
                    "저장 검증 실패: worksheet에 원거리 col artifact가 있습니다: {name}, min={min_col}, max={max_col}"
                )));
            }
            col_cursor = col_end
                .checked_add(1)
                .ok_or_else(|| err("저장 검증 실패: col cursor 계산 실패"))?;
        }
        Ok(())
    }
    fn summarize(&self) -> Result<(Option<String>, usize)> {
        self.cell_refs_unique()?;
        self.row_and_col_refs_valid()?;
        self.ref_formula_refs()?;
        let (bounds, count) = self.bounds_and_shared_refs()?;
        if let Some(actual_bounds) = bounds.as_ref() {
            self.range_dimension_matches(actual_bounds)?;
        }
        let auto_filter_ref = self.filter_semantics(bounds.as_ref())?;
        Ok((auto_filter_ref, count))
    }
}
struct TempArchivePromotion<'path> {
    backup_archive: &'path mut ReservedTempArchive,
    expected_fingerprint: ArchiveFingerprint,
    target_xlsx: &'path Path,
    temp_archive: &'path mut ReservedTempArchive,
}
impl TempArchivePromotion<'_> {
    fn cleanup_displaced_original(
        &self,
        displaced_file: atomic_replace::DisplacedFile,
    ) -> Result<()> {
        let captured_original = self.displaced_path(displaced_file);
        fs::remove_file(captured_original).map_err(|source| {
            err_with_source(
                path_context_message("교체된 원본 xlsx 정리 실패", captured_original),
                source,
            )
        })?;
        if matches!(displaced_file, atomic_replace::DisplacedFile::Replacement) {
            let backup_archive_path = self.backup_archive.path();
            fs::remove_file(backup_archive_path).map_err(|source| {
                err_with_source(
                    path_context_message("xlsx 교체 예약 파일 정리 실패", backup_archive_path),
                    source,
                )
            })?;
        }
        Ok(())
    }
    fn displaced_path(&self, displaced: atomic_replace::DisplacedFile) -> &Path {
        match displaced {
            #[cfg(target_os = "windows")]
            atomic_replace::DisplacedFile::Backup => self.backup_archive.path(),
            atomic_replace::DisplacedFile::Replacement => self.temp_archive.path(),
        }
    }
    fn preserve_recovery_archives(
        &mut self,
        context: &str,
        source: atomic_replace::ReplaceFailure,
    ) -> Result<()> {
        let message = format!(
            "{context}; 자동 복구 실패 후 수동 복구를 위해 현재 경로 상태를 보존했습니다: target={}, replacement={}, backup={}",
            self.target_xlsx.display(),
            self.temp_archive.path().display(),
            self.backup_archive.path().display(),
        );
        self.temp_archive.disable_drop_cleanup();
        self.backup_archive.disable_drop_cleanup();
        Err(err_with_source(message, source))
    }
    fn promote(&mut self) -> Result<()> {
        let replace_result = atomic_replace::replace_files(
            self.target_xlsx,
            self.temp_archive.path(),
            self.backup_archive.path(),
            false,
        );
        let displaced_file = match replace_result {
            Ok(displaced) => displaced,
            Err(atomic_replace::ReplaceFilesError::Failed(source)) => {
                return Err(err_with_source(
                    path_pair_context_message(
                        "xlsx 저장 실패",
                        self.temp_archive.path(),
                        self.target_xlsx,
                    ),
                    source,
                ));
            }
            #[cfg(target_os = "windows")]
            Err(atomic_replace::ReplaceFilesError::Restored(source)) => {
                return Err(err_with_source(
                    path_pair_context_message(
                        "xlsx 저장 실패 후 원본 대상 파일 자동 복원 완료",
                        self.temp_archive.path(),
                        self.target_xlsx,
                    ),
                    source,
                ));
            }
            #[cfg(target_os = "windows")]
            Err(atomic_replace::ReplaceFilesError::RecoveryRequired(source)) => {
                let context = path_pair_context_message(
                    "xlsx 저장 중 원본 대상 파일 자동 복구 실패",
                    self.temp_archive.path(),
                    self.target_xlsx,
                );
                return self.preserve_recovery_archives(&context, source);
            }
        };
        if let Err(validation_error) = self.validate_displaced_original(displaced_file) {
            return self.rollback_after_validation_failure(validation_error);
        }
        if let Err(cleanup_error) = self.cleanup_displaced_original(displaced_file) {
            let mut error_output = stderr().lock();
            match writeln!(
                &mut error_output,
                "경고: xlsx 저장은 완료됐지만 교체된 원본 정리에 실패했습니다: {cleanup_error}"
            ) {
                Ok(()) | Err(_) => {}
            }
        }
        cfg_select! {
            any(target_os = "linux", target_os = "macos") => {
                if let Err(source_err) = fs::OpenOptions::new()
                    .read(true)
                    .open(self.target_xlsx)
                    .and_then(|file| file.sync_all())
                {
                    write_path_warning(
                        "저장 내구성 동기화 실패(파일)",
                        self.target_xlsx,
                        &source_err,
                    );
                }
                let parent = self
                    .target_xlsx
                    .parent()
                    .filter(|path| !path.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
                if let Err(source_err) = fs::File::open(parent).and_then(|dir| dir.sync_all()) {
                    write_path_warning("저장 내구성 동기화 실패(폴더)", parent, &source_err);
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn rollback_after_validation_failure(&mut self, validation_error: AppError) -> Result<()> {
        let rollback_error = match atomic_replace::replace_files(
            self.target_xlsx,
            self.temp_archive.path(),
            self.backup_archive.path(),
            true,
        ) {
            Ok(_) => return Err(validation_error),
            #[cfg(target_os = "windows")]
            Err(atomic_replace::ReplaceFilesError::Restored(_)) => return Err(validation_error),
            #[cfg(target_os = "windows")]
            Err(atomic_replace::ReplaceFilesError::RecoveryRequired(source)) => source,
            Err(atomic_replace::ReplaceFilesError::Failed(source)) => source,
        };
        let context = format!("원본 xlsx 검증 실패 후 복구 실패 ({validation_error})");
        self.preserve_recovery_archives(&context, rollback_error)
    }
    fn validate_displaced_original(
        &self,
        displaced_file: atomic_replace::DisplacedFile,
    ) -> Result<()> {
        let captured_original = self.displaced_path(displaced_file);
        let captured_file = fs::File::open(captured_original).map_err(|source| {
            err_with_source(
                path_context_message("교체된 원본 xlsx 열기 실패", captured_original),
                source,
            )
        })?;
        let fingerprint =
            scan_open_archive(&captured_file, captured_original, None).map_err(|source| {
                err_with_source(
                    path_context_message("교체된 원본 xlsx 검증 실패", captured_original),
                    source,
                )
            })?;
        if fingerprint != self.expected_fingerprint {
            return Err(err(format!(
                "원본 xlsx가 실행 중 변경되어 저장을 중단했습니다: {}",
                self.target_xlsx.display()
            )));
        }
        Ok(())
    }
}
impl XlsxContainer {
    pub(super) fn ensure_fixed_sheet_catalog(&self, workbook_xml: &str) -> Result<()> {
        validate_unprefixed_spreadsheet_part(
            workbook_xml,
            "workbook",
            &["workbook", "sheets", "sheet"],
            "workbook.xml",
        )?;
        let rels_xml = self.read_text("xl/_rels/workbook.xml.rels")?;
        let mut relationship_map: HashMap<Cow<'_, str>, WorkbookRelationship<'_>> = HashMap::new();
        let mut relationship_scanner = XmlScanner::new(&rels_xml);
        while let Some(relationship_tag) = relationship_scanner.next_start_named("Relationship") {
            let tag = relationship_tag.raw();
            let id = required_xml_attr(tag, "Id", "workbook.xml.rels Relationship")?;
            let target = required_xml_attr(tag, "Target", "workbook.xml.rels Relationship")?;
            let type_ = required_xml_attr(tag, "Type", "workbook.xml.rels Relationship")?;
            let relationship = WorkbookRelationship {
                target,
                target_mode: extract_attr(tag, "TargetMode")?,
                type_,
            };
            relationship_map.try_reserve(1).map_err(|source| {
                err_with_source("workbook 관계 맵 추가 메모리 확보 실패", source)
            })?;
            let HashEntry::Vacant(entry) = relationship_map.entry(id) else {
                return Err(err("workbook.xml.rels Relationship Id가 중복됩니다."));
            };
            entry.insert(relationship);
        }
        let mut workbook_scanner = XmlScanner::new(workbook_xml);
        let workbook_tag = workbook_scanner
            .next_start_named("workbook")
            .ok_or_else(|| err("workbook.xml의 workbook 시작 태그를 찾지 못했습니다."))?;
        let workbook_open_tag = workbook_tag.raw();
        if required_xml_attr(workbook_open_tag, "xmlns:r", "workbook.xml workbook")?.as_ref()
            != OFFICE_DOCUMENT_REL_NAMESPACE
        {
            return Err(err("workbook.xml의 xmlns:r namespace가 올바르지 않습니다."));
        }
        workbook_scanner
            .next_start_named("sheets")
            .ok_or_else(|| err("workbook.xml의 sheets 시작 태그를 찾지 못했습니다."))?;
        for (expected_name, expected_path) in [
            (MASTER_SHEET_NAME, MASTER_SHEET_PATH),
            (CHANGE_LOG_SHEET_NAME, CHANGE_LOG_SHEET_PATH),
        ] {
            let sheet_tag = workbook_scanner
                .next_start_named("sheet")
                .ok_or_else(|| err("workbook sheet 수가 고정 스키마의 2개보다 적습니다."))?;
            let tag = sheet_tag.raw();
            let name = required_xml_attr(tag, "name", "workbook.xml sheet")?;
            let rid = required_xml_attr(tag, "r:id", "workbook.xml sheet")?;
            let relationship = relationship_map.get(rid.as_ref()).ok_or_else(|| {
                err(format!(
                    "workbook.xml.rels에서 sheet 관계 target을 찾지 못했습니다: {rid}"
                ))
            })?;
            let target_text = relationship.internal_worksheet_target(rid.as_ref())?;
            if target_text.starts_with('/') {
                return Err(err(format!(
                    "sheet 관계 target에 절대 경로는 허용되지 않습니다: {target_text}"
                )));
            }
            let mut combined: PathBuf = "xl".into();
            for segment in target_text.split('/').filter(|segment| !segment.is_empty()) {
                combined.push(segment);
            }
            let normalized = normalize_safe_relative_path(&combined, target_text)?;
            let resolved = path_to_slashes(&normalized, target_text)?;
            if resolved.is_empty() {
                return Err(err(format!(
                    "sheet 관계 target 정규화 결과가 비어 있습니다: {target_text}"
                )));
            }
            if name != expected_name || resolved != expected_path {
                return Err(err(format!(
                    "workbook sheet가 고정 스키마와 다릅니다: expected={expected_name}({expected_path}), actual={name}({resolved})"
                )));
            }
        }
        if workbook_scanner.next_start_named("sheet").is_some() {
            return Err(err("workbook sheet 수가 고정 스키마의 2개보다 많습니다."));
        }
        Ok(())
    }
    pub(crate) fn open(source_xlsx: &Path) -> Result<Self> {
        let mut source_options = fs::File::options();
        source_options.read(true);
        let source_file = source_options.open(source_xlsx).map_err(|source_err| {
            err_with_source(
                path_context_message("마스터 xlsx 파일 열기 실패", source_xlsx),
                source_err,
            )
        })?;
        let source_metadata = validate_regular_file(&source_file).map_err(|source_err| {
            err_with_source(
                path_context_message("마스터 xlsx 파일 검증 실패", source_xlsx),
                source_err,
            )
        });
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let source_permissions = source_metadata?.permissions();
        #[cfg(target_os = "windows")]
        source_metadata?;
        let base = env::temp_dir();
        if let Err(source) =
            cleanup_stale_temp_entries(&base, WORK_DIR_PREFIX, TempEntryKind::Directory)
        {
            write_path_warning("이전 xlsx 작업 폴더 정리 실패", &base, &source);
        }
        let work_dir = WorkDirCleanup {
            path: reserve_unique_temp_entry(&base, WORK_DIR_PREFIX, |path| {
                cfg_select! {
                    any(target_os = "linux", target_os = "macos") => {
                        let mut builder = fs::DirBuilder::new();
                        builder.mode(0o700);
                        builder.create(path)?;
                    }
                    _ => {
                        fs::DirBuilder::new().create(path)?;
                    }
                }
                Ok(path.to_path_buf())
            })
            .map_err(|source| err_with_source("임시 작업 폴더 생성 실패", source))?,
        };
        let unpack_dir = work_dir.path.join("unzipped");
        create_dir_all_checked(&unpack_dir, "임시 폴더 생성 실패")?;
        let source_fingerprint = ZipArchiveExtractor {
            archive_file: source_file,
            archive_path: source_xlsx,
            unpack_dir: unpack_dir.as_path(),
        }
        .extract()?;
        let container = Self {
            _work_dir: work_dir,
            source_fingerprint,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            source_permissions,
            unpack_dir,
        };
        container.validate_content_types()?;
        container.validate_root_relationships()?;
        Ok(container)
    }
    pub(super) fn read_shared_strings_text(&self) -> Result<String> {
        let path = self.resolve_relative_path("xl/sharedStrings.xml")?;
        let file = fs::File::open(&path).map_err(|source_err| {
            err_with_source(path_context_message("파일 열기 실패", &path), source_err)
        })?;
        let xml = Self::read_text_from_file(&path, file)?;
        validate_unprefixed_spreadsheet_part(
            &xml,
            "sst",
            &["sst", "si", "t", "r", "rPr", "rPh", "phoneticPr"],
            "sharedStrings.xml",
        )?;
        Ok(xml)
    }
    pub(super) fn read_text(&self, relative_path: &str) -> Result<String> {
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
        if bytes.len() != data_len {
            return Err(err(format!(
                "xlsx XML part가 읽는 중 변경되었습니다: {}",
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
            Err(io_err) if io_err.kind() == io::ErrorKind::NotFound => Ok(()),
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
    pub(super) fn save(self, target_xlsx: &Path, verification: SaveVerification) -> Result<()> {
        let parent = target_xlsx
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        create_dir_all_checked(parent, "저장 폴더 생성 실패")?;
        let target_file_name = target_xlsx
            .file_name()
            .and_then(|file_name_os| file_name_os.to_str())
            .unwrap_or("workbook.xlsx");
        let temp_archive_prefix = format!(".{target_file_name}.tmp_");
        let backup_archive_prefix = format!(".{target_file_name}.backup_");
        if let Err(source) =
            cleanup_stale_temp_entries(parent, &temp_archive_prefix, TempEntryKind::File)
        {
            write_path_warning("이전 xlsx 임시 저장 파일 정리 실패", parent, &source);
        }
        if let Err(source) =
            cleanup_stale_temp_entries(parent, &backup_archive_prefix, TempEntryKind::File)
        {
            write_path_warning("이전 xlsx 교체 예약 파일 정리 실패", parent, &source);
        }
        let mut tmp_archive =
            reserve_unique_temp_entry(parent, &temp_archive_prefix, create_reserved_temp_archive)
                .map_err(|source| {
                err_with_source(
                    path_context_message("임시 저장 파일 생성 실패", target_xlsx),
                    source,
                )
            })?;
        let mut backup_archive =
            reserve_unique_temp_entry(parent, &backup_archive_prefix, create_reserved_temp_archive)
                .map_err(|source| {
                    err_with_source(
                        path_context_message("교체 예약 파일 생성 실패", target_xlsx),
                        source,
                    )
                })?;
        backup_archive.file = None;
        let result = (|| -> Result<()> {
            cfg_select! {
                any(target_os = "linux", target_os = "macos") => {
                    tmp_archive.write_archive_from(
                        self.unpack_dir.as_path(),
                        self.source_permissions,
                    )?;
                }
                target_os = "windows" => {
                    tmp_archive.write_archive_from(self.unpack_dir.as_path())?;
                }
            }
            match verification {
                SaveVerification::Skip => {}
                SaveVerification::Verify => {
                    tmp_archive.verify_saved_archive()?;
                }
            }
            TempArchivePromotion {
                backup_archive: &mut backup_archive,
                expected_fingerprint: self.source_fingerprint,
                target_xlsx,
                temp_archive: &mut tmp_archive,
            }
            .promote()
        })();
        match result {
            Ok(()) => Ok(()),
            Err(source) => {
                if !tmp_archive.remove_on_drop {
                    return Err(source);
                }
                tmp_archive.disable_drop_cleanup();
                let tmp_archive_path = tmp_archive.path();
                match fs::remove_file(tmp_archive_path) {
                    Ok(()) => Err(source),
                    Err(error) if error.kind() == io::ErrorKind::NotFound => Err(source),
                    Err(error) => Err(err_with_source(
                        format!(
                            "xlsx 임시 저장 파일 삭제 실패: {} ({error})",
                            tmp_archive_path.display(),
                        ),
                        source,
                    )),
                }
            }
        }
    }
    fn validate_content_types(&self) -> Result<()> {
        let content_types_xml = self.read_text("[Content_Types].xml")?;
        let children = direct_xml_children(
            &content_types_xml,
            "Types",
            CONTENT_TYPES_NAMESPACE,
            "[Content_Types].xml",
        )?;
        let mut seen_part_names = HashSet::new();
        seen_part_names
            .try_reserve(children.len())
            .map_err(|source| {
                err_with_source("content type part name 집합 메모리 확보 실패", source)
            })?;
        let mut workbook_override = None;
        let mut xml_default = None;
        for child in children {
            match child.local_name {
                "Default" => {
                    let extension =
                        required_xml_attr(child.raw, "Extension", "[Content_Types].xml Default")?;
                    let content_type =
                        required_xml_attr(child.raw, "ContentType", "[Content_Types].xml Default")?;
                    if extension.eq_ignore_ascii_case("xml")
                        && xml_default.replace(content_type).is_some()
                    {
                        return Err(err(
                            "[Content_Types].xml에 중복 XML Default 항목이 있습니다.",
                        ));
                    }
                }
                "Override" => {
                    let part_name =
                        required_xml_attr(child.raw, "PartName", "[Content_Types].xml Override")?;
                    let content_type = required_xml_attr(
                        child.raw,
                        "ContentType",
                        "[Content_Types].xml Override",
                    )?;
                    let is_workbook_part = part_name.as_ref() == WORKBOOK_PART_NAME;
                    if !seen_part_names.insert(part_name) {
                        return Err(err(
                            "[Content_Types].xml에 중복 Override PartName이 있습니다.",
                        ));
                    }
                    if is_workbook_part {
                        workbook_override = Some(content_type);
                    }
                }
                _ => {
                    return Err(err(format!(
                        "[Content_Types].xml에 알 수 없는 child 태그가 있습니다: {}",
                        child.local_name
                    )));
                }
            }
        }
        let workbook_content_type = workbook_override.or(xml_default).ok_or_else(|| {
            err("[Content_Types].xml에서 workbook content type을 찾지 못했습니다.")
        })?;
        if workbook_content_type.as_ref() != WORKBOOK_CONTENT_TYPE {
            return Err(err(format!(
                "workbook content type이 올바르지 않습니다: {}",
                workbook_content_type.as_ref()
            )));
        }
        Ok(())
    }
    fn validate_root_relationships(&self) -> Result<()> {
        let relationships_xml = self.read_text("_rels/.rels")?;
        let children = direct_xml_children(
            &relationships_xml,
            "Relationships",
            PACKAGE_RELATIONSHIPS_NAMESPACE,
            "_rels/.rels",
        )?;
        let mut seen_ids = HashSet::new();
        seen_ids.try_reserve(children.len()).map_err(|source| {
            err_with_source("package relationship id 집합 메모리 확보 실패", source)
        })?;
        let mut office_document_seen = false;
        for child in children {
            if child.local_name != "Relationship" {
                return Err(err(format!(
                    "_rels/.rels에 알 수 없는 child 태그가 있습니다: {}",
                    child.local_name
                )));
            }
            let id = required_xml_attr(child.raw, "Id", "_rels/.rels Relationship")?;
            let target = required_xml_attr(child.raw, "Target", "_rels/.rels Relationship")?;
            let type_ = required_xml_attr(child.raw, "Type", "_rels/.rels Relationship")?;
            let target_mode = unique_xml_attr(child.raw, "TargetMode", "_rels/.rels Relationship")?;
            if !seen_ids.insert(id) {
                return Err(err("_rels/.rels에 중복 Relationship Id가 있습니다."));
            }
            if type_.as_ref() != OFFICE_DOCUMENT_REL_TYPE {
                continue;
            }
            if office_document_seen {
                return Err(err(
                    "_rels/.rels에 officeDocument Relationship이 여러 개 있습니다.",
                ));
            }
            if target_mode
                .as_ref()
                .is_some_and(|mode| mode.as_ref() != "Internal")
            {
                return Err(err(
                    "_rels/.rels의 officeDocument Relationship은 External일 수 없습니다.",
                ));
            }
            if target.as_ref() != WORKBOOK_REL_TARGET {
                return Err(err(format!(
                    "_rels/.rels의 officeDocument target이 올바르지 않습니다: {}",
                    target.as_ref()
                )));
            }
            office_document_seen = true;
        }
        if !office_document_seen {
            return Err(err("_rels/.rels에 officeDocument Relationship이 없습니다."));
        }
        Ok(())
    }
    pub(super) fn write_text(&self, relative_path: &str, content: &str) -> Result<()> {
        let path = self.resolve_relative_path(relative_path)?;
        if let Some(parent) = path.parent() {
            create_dir_all_checked(parent, "폴더 생성 실패")?;
        }
        fs::write(&path, content).map_err(|source_err| {
            err_with_source(path_context_message("파일 쓰기 실패", &path), source_err)
        })
    }
}
fn saved_archive_trailing_row_num(cell_ref: &str) -> Result<u32> {
    let row_text = cell_ref
        .rsplit_once(|ch: char| !ch.is_ascii_digit())
        .map_or(cell_ref, |(_, row)| row);
    if row_text.is_empty() {
        return Err(err(format!(
            "저장 검증 실패: filter cell reference에 row 번호가 없습니다: {cell_ref}"
        )));
    }
    row_text
        .parse::<u32>()
        .map_err(|source| err_with_source("저장 검증 실패: filter row 번호 해석 실패", source))
}
pub(super) fn validate_worksheet_core_namespaces(sheet_xml: &str, sheet_name: &str) -> Result<()> {
    let context = format!("worksheet XML namespace 검증: {sheet_name}");
    let mut scanner = XmlScanner::new(sheet_xml);
    let root = scanner
        .next_tag()
        .ok_or_else(|| err(format!("{context}에 root 태그가 없습니다.")))?;
    if !root.is_start()
        || root.name() != "worksheet"
        || root.local_name() != "worksheet"
        || root.self_closing()
    {
        return Err(err(format!("{context}의 root 태그가 올바르지 않습니다.")));
    }
    if resolved_element_namespace(&root, &[], &context)?.as_deref() != Some(SPREADSHEETML_NAMESPACE)
    {
        return Err(err(format!(
            "{context}의 worksheet namespace가 올바르지 않습니다."
        )));
    }
    let mut ancestors = Vec::new();
    ancestors
        .try_reserve_exact(8)
        .map_err(|source| err_with_source(format!("{context} stack 메모리 확보 실패"), source))?;
    ancestors.push(WorksheetNamespaceElement {
        local_name: root.local_name(),
        name: root.name(),
        raw: root.raw(),
    });
    while let Some(tag) = scanner.next_tag() {
        if ancestors.is_empty() {
            return Err(err(format!("{context}에 root 밖의 XML 요소가 있습니다.")));
        }
        if tag.is_start() {
            let parent_name = ancestors.last().map(|element| element.local_name);
            let is_core_element = matches!(
                (parent_name, tag.local_name()),
                (Some("worksheet"), "sheetData")
                    | (Some("sheetData"), "row")
                    | (Some("row"), "c")
                    | (Some("c"), "f" | "is" | "v")
                    | (Some("is"), "r" | "t")
                    | (Some("r"), "t")
            );
            if is_core_element && tag.name() != tag.local_name() {
                return Err(err(format!(
                    "{context}의 prefixed core element는 지원하지 않습니다: {}",
                    tag.name()
                )));
            }
            if is_core_element
                && resolved_element_namespace(&tag, &ancestors, &context)?.as_deref()
                    != Some(SPREADSHEETML_NAMESPACE)
            {
                return Err(err(format!(
                    "{context}의 {} namespace가 올바르지 않습니다.",
                    tag.name()
                )));
            }
            if !tag.self_closing() {
                if ancestors.len() == ancestors.capacity() {
                    ancestors.try_reserve(1).map_err(|source| {
                        err_with_source(format!("{context} stack 메모리 확보 실패"), source)
                    })?;
                }
                ancestors.push(WorksheetNamespaceElement {
                    local_name: tag.local_name(),
                    name: tag.name(),
                    raw: tag.raw(),
                });
            }
            continue;
        }
        let open = ancestors
            .pop()
            .ok_or_else(|| err(format!("{context}의 종료 태그 순서가 올바르지 않습니다.")))?;
        if open.name != tag.name() {
            return Err(err(format!(
                "{context}의 XML 태그 쌍이 일치하지 않습니다: {} / {}",
                open.name,
                tag.name()
            )));
        }
    }
    if !ancestors.is_empty() {
        return Err(err(format!("{context}에 닫히지 않은 XML 요소가 있습니다.")));
    }
    Ok(())
}
fn validate_unprefixed_spreadsheet_part(
    xml: &str,
    root_name: &str,
    core_names: &[&str],
    context: &str,
) -> Result<()> {
    let mut scanner = XmlScanner::new(xml);
    let root = scanner
        .next_tag()
        .ok_or_else(|| err(format!("{context}에 root 태그가 없습니다.")))?;
    if !root.is_start() || root.name() != root_name || root.self_closing() {
        return Err(err(format!("{context}의 root 형식이 올바르지 않습니다.")));
    }
    if resolved_element_namespace(&root, &[], context)?.as_deref() != Some(SPREADSHEETML_NAMESPACE)
    {
        return Err(err(format!(
            "{context}의 root namespace가 올바르지 않습니다."
        )));
    }
    while let Some(tag) = scanner.next_tag() {
        if core_names.contains(&tag.local_name()) && tag.name() != tag.local_name() {
            return Err(err(format!(
                "{context}의 prefixed core element는 지원하지 않습니다: {}",
                tag.name()
            )));
        }
    }
    Ok(())
}
fn create_reserved_temp_archive(path: &Path) -> io::Result<ReservedTempArchive> {
    let mut options = fs::File::options();
    options.write(true).create_new(true);
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    options.mode(0o600);
    let file = options.open(path)?;
    Ok(ReservedTempArchive {
        file: Some(file),
        path: path.to_path_buf(),
        remove_on_drop: true,
    })
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
            Component::Normal(segment) => {
                let Some(text) = segment.to_str() else {
                    return Err(err(format!(
                        "상대 경로 component가 UTF-8이 아닙니다: {relative_path}"
                    )));
                };
                reject_windows_special_component(text, &relative_path)?;
                normalized.push(segment);
            }
            Component::ParentDir => {
                return Err(err(format!(
                    "상위 경로 탐색은 허용되지 않습니다: {relative_path}"
                )));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(err(format!(
                    "절대 경로는 허용되지 않습니다: {relative_path}"
                )));
            }
        }
    }
    if normalized.as_os_str().is_empty() {
        return Err(err(format!("상대 경로가 비어 있습니다: {relative_path}")));
    }
    Ok(normalized)
}
fn xml_misc_only(mut xml: &str, allow_bom: bool) -> bool {
    if allow_bom && let Some(without_bom) = xml.strip_prefix('\u{feff}') {
        xml = without_bom;
    }
    loop {
        xml = xml.trim_start();
        if xml.is_empty() {
            return true;
        }
        let terminator = if xml.starts_with("<!--") {
            "-->"
        } else if xml.starts_with("<?") {
            "?>"
        } else {
            return false;
        };
        let Some(end) = xml.find(terminator) else {
            return false;
        };
        let Some(next) = end.checked_add(terminator.len()) else {
            return false;
        };
        let Some(remaining) = xml.get(next..) else {
            return false;
        };
        xml = remaining;
    }
}
fn unique_xml_attr<'tag>(
    tag: &'tag str,
    attr_name: &str,
    context: &str,
) -> Result<Option<Cow<'tag, str>>> {
    let mut value = None;
    let mut attributes = XmlAttrScanner::new(tag)?;
    while let Some((name, attr_value)) = attributes.next()? {
        if name == attr_name && value.replace(attr_value).is_some() {
            return Err(err(format!(
                "{context}에 중복 {attr_name} 속성이 있습니다."
            )));
        }
    }
    Ok(value)
}
fn required_xml_attr<'tag>(
    tag: &'tag str,
    attr_name: &str,
    context: &str,
) -> Result<Cow<'tag, str>> {
    unique_xml_attr(tag, attr_name, context)?
        .ok_or_else(|| err(format!("{context}에 {attr_name} 속성이 없습니다.")))
}
fn declared_element_namespace<'tag>(
    tag: &'tag str,
    qualified_name: &str,
    context: &str,
) -> Result<Option<Cow<'tag, str>>> {
    let prefix = match qualified_name.split_once(':') {
        Some((prefix, local_name))
            if !prefix.is_empty() && !local_name.is_empty() && !local_name.contains(':') =>
        {
            Some(prefix)
        }
        Some(_) => {
            return Err(err(format!(
                "{context}의 XML qualified name이 잘못되었습니다."
            )));
        }
        None => None,
    };
    let mut namespace = None;
    let mut attributes = XmlAttrScanner::new(tag)?;
    while let Some((name, value)) = attributes.next()? {
        let matches = (prefix.is_none() && name == "xmlns")
            || prefix.is_some_and(|namespace_prefix| {
                name.strip_prefix("xmlns:") == Some(namespace_prefix)
            });
        if matches && namespace.replace(value).is_some() {
            return Err(err(format!(
                "{context}에 중복 XML namespace 선언이 있습니다."
            )));
        }
    }
    Ok(namespace)
}
fn resolved_element_namespace<'xml>(
    tag: &XmlTag<'xml>,
    ancestors: &[WorksheetNamespaceElement<'xml>],
    context: &str,
) -> Result<Option<Cow<'xml, str>>> {
    for declaration in
        iter::once(tag.raw()).chain(ancestors.iter().rev().map(|element| element.raw))
    {
        if let Some(namespace) = declared_element_namespace(declaration, tag.name(), context)? {
            return Ok(Some(namespace));
        }
    }
    Ok(None)
}
fn validate_element_namespace(
    tag: &XmlTag<'_>,
    root_tag: &XmlTag<'_>,
    expected_namespace: &str,
    context: &str,
) -> Result<()> {
    let own_namespace = declared_element_namespace(tag.raw(), tag.name(), context)?;
    let namespace = match own_namespace {
        Some(namespace) => Some(namespace),
        None => declared_element_namespace(root_tag.raw(), tag.name(), context)?,
    };
    if namespace.as_deref() != Some(expected_namespace) {
        return Err(err(format!(
            "{context}의 XML namespace가 올바르지 않습니다."
        )));
    }
    Ok(())
}
fn direct_xml_children<'xml>(
    xml: &'xml str,
    root_local_name: &str,
    expected_namespace: &str,
    context: &str,
) -> Result<Vec<DirectXmlChild<'xml>>> {
    let mut scanner = XmlScanner::new(xml);
    let root_tag = scanner
        .next_tag()
        .ok_or_else(|| err(format!("{context}의 XML root 태그가 없습니다.")))?;
    if !root_tag.is_start() || root_tag.local_name() != root_local_name {
        return Err(err(format!(
            "{context}의 XML root 태그가 올바르지 않습니다."
        )));
    }
    if root_tag.self_closing() {
        return Err(err(format!("{context}의 XML root 태그가 비어 있습니다.")));
    }
    let leading = xml
        .get(..root_tag.start())
        .ok_or_else(|| err(format!("{context}의 XML root 범위가 손상되었습니다.")))?;
    if !xml_misc_only(leading, true) {
        return Err(err(format!(
            "{context}의 XML root 앞 내용이 올바르지 않습니다."
        )));
    }
    validate_element_namespace(
        &root_tag,
        &root_tag,
        expected_namespace,
        &format!("{context} root"),
    )?;
    let root_name = root_tag.name();
    let mut open_child_name = None;
    let mut children = Vec::new();
    let mut root_closed = false;
    while let Some(tag) = scanner.next_tag() {
        if root_closed {
            return Err(err(format!(
                "{context}에 XML root 태그가 여러 개 있습니다."
            )));
        }
        if tag.is_start() {
            if open_child_name.is_some() {
                return Err(err(format!(
                    "{context}의 XML child 태그는 중첩될 수 없습니다."
                )));
            }
            validate_element_namespace(&tag, &root_tag, expected_namespace, context)?;
            if children.len() == children.capacity() {
                children.try_reserve(1).map_err(|source| {
                    err_with_source(format!("{context} child 목록 메모리 확보 실패"), source)
                })?;
            }
            if !tag.self_closing() {
                open_child_name = Some(tag.name());
            }
            children.push(DirectXmlChild {
                local_name: tag.local_name(),
                raw: tag.raw(),
            });
        } else if let Some(child_name) = open_child_name {
            if tag.name() != child_name {
                return Err(err(format!(
                    "{context}의 XML child 종료 태그가 일치하지 않습니다."
                )));
            }
            open_child_name = None;
        } else {
            if tag.name() != root_name {
                return Err(err(format!(
                    "{context}의 XML root 종료 태그가 일치하지 않습니다."
                )));
            }
            let trailing_start = tag
                .end()
                .checked_add(1)
                .ok_or_else(|| err(format!("{context}의 XML root 범위가 손상되었습니다.")))?;
            let trailing = xml
                .get(trailing_start..)
                .ok_or_else(|| err(format!("{context}의 XML root 범위가 손상되었습니다.")))?;
            if !xml_misc_only(trailing, false) {
                return Err(err(format!(
                    "{context}의 XML root 뒤 내용이 올바르지 않습니다."
                )));
            }
            root_closed = true;
            break;
        }
    }
    if open_child_name.is_some() || !root_closed {
        return Err(err(format!("{context}의 XML 종료 태그가 없습니다.")));
    }
    Ok(children)
}
fn write_path_warning(context: &str, path: &Path, source: &io::Error) {
    let mut error_output = stderr().lock();
    match writeln!(
        &mut error_output,
        "경고: {context}: {} ({source})",
        path.display(),
    ) {
        Ok(()) | Err(_) => {}
    }
}
