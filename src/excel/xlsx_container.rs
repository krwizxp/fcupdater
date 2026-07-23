use super::{
    ArchiveFingerprint, CHANGE_LOG_SHEET_NAME, MASTER_SHEET_NAME, PackagePart,
    SPREADSHEETML_NAMESPACE, SaveVerification, XLSX_PART_NAMES, ZipArchiveBuilder,
    ZipPackageReader,
    xml::{XmlAttrScanner, XmlScanner},
    zip_archive::scan_open_archive,
};
use crate::diagnostic::{AppError, Result, err, err_with_source, path_context_message};
use crate::temp_entry::{cleanup_stale_temp_files, reserve_unique_temp_entry};
use crate::validate_regular_file;
use alloc::borrow::Cow;
use core::{mem, str};
use std::{
    fs,
    io::{self, Write as _, stderr},
    path::{Path, PathBuf},
};
cfg_select! {
    any(target_os = "linux", target_os = "macos") => {
        use std::os::unix::fs::OpenOptionsExt as _;
    }
    _ => {}
}
mod atomic_replace;
const MAX_XLSX_TEXT_PART_BYTES: usize = 64 * 1024 * 1024;
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
const WORKBOOK_LOEXT_FRAGMENT: &str = concat!(
    "<extLst><ext xmlns:loext=\"http://schemas.libreoffice.org/\" ",
    "uri=\"{7626C862-2A13-11E5-B345-FEFF819CDC9F}\">",
    "<loext:extCalcPr stringRefSyntax=\"CalcA1ExcelA1\"/></ext></extLst>",
);
const WORKBOOK_LOEXT_OPEN_TAG: &str = concat!(
    "<ext xmlns:loext=\"http://schemas.libreoffice.org/\" ",
    "uri=\"{7626C862-2A13-11E5-B345-FEFF819CDC9F}\">",
);
const WORKBOOK_LOEXT_VALUE_TAG: &str = "<loext:extCalcPr stringRefSyntax=\"CalcA1ExcelA1\"/>";
const CONTENT_TYPE_DEFAULTS: [(&str, &str); 5] = [
    ("fntdata", "application/x-fontdata"),
    ("jpeg", "image/jpeg"),
    ("png", "image/png"),
    (
        "rels",
        "application/vnd.openxmlformats-package.relationships+xml",
    ),
    ("xml", "application/xml"),
];
const CONTENT_TYPE_OVERRIDES: [(&str, &str); 10] = [
    (
        "/docProps/custom.xml",
        "application/vnd.openxmlformats-officedocument.custom-properties+xml",
    ),
    (
        "/docProps/core.xml",
        "application/vnd.openxmlformats-package.core-properties+xml",
    ),
    (
        "/docProps/app.xml",
        "application/vnd.openxmlformats-officedocument.extended-properties+xml",
    ),
    (
        "/xl/worksheets/sheet1.xml",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml",
    ),
    (
        "/xl/worksheets/sheet2.xml",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml",
    ),
    (
        "/xl/drawings/drawing1.xml",
        "application/vnd.openxmlformats-officedocument.drawing+xml",
    ),
    (
        "/xl/styles.xml",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml",
    ),
    (WORKBOOK_PART_NAME, WORKBOOK_CONTENT_TYPE),
    (
        "/xl/theme/theme1.xml",
        "application/vnd.openxmlformats-officedocument.theme+xml",
    ),
    (
        "/xl/sharedStrings.xml",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml",
    ),
];
const ROOT_RELATIONSHIPS: [(&str, &str, &str); 4] = [
    ("rId1", OFFICE_DOCUMENT_REL_TYPE, WORKBOOK_REL_TARGET),
    (
        "rId2",
        "http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties",
        "docProps/core.xml",
    ),
    (
        "rId3",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties",
        "docProps/app.xml",
    ),
    (
        "rId4",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/custom-properties",
        "docProps/custom.xml",
    ),
];
const WORKBOOK_RELATIONSHIPS: [(&str, &str, &str); 5] = [
    (
        "rId1",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme",
        "theme/theme1.xml",
    ),
    (
        "rId2",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles",
        "styles.xml",
    ),
    ("rId3", WORKSHEET_REL_TYPE, "worksheets/sheet1.xml"),
    ("rId4", WORKSHEET_REL_TYPE, "worksheets/sheet2.xml"),
    (
        "rId5",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings",
        "sharedStrings.xml",
    ),
];
#[derive(Debug)]
pub(crate) struct XlsxContainer {
    parts: [PackagePart; XLSX_PART_NAMES.len()],
    source_bytes: Vec<u8>,
    source_fingerprint: ArchiveFingerprint,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    source_permissions: fs::Permissions,
}
struct ReservedTempArchive {
    file: Option<fs::File>,
    path: PathBuf,
    remove_on_drop: bool,
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
                    "저장 검증 실패: 저장 직후 ZIP package 점검에 실패했습니다",
                    saved_archive,
                ),
                source_err,
            )
        })?;
        super::writer::Workbook::from_container(saved_container)
            .map(|_| ())
            .map_err(|source_err| {
                err_with_source(
                    path_context_message(
                        "저장 검증 실패: 저장 직후 재열기 점검에 실패했습니다",
                        saved_archive,
                    ),
                    source_err,
                )
            })
    }
    fn write_archive_from(
        &mut self,
        parts: &[PackagePart],
        source_bytes: &[u8],
        #[cfg(any(target_os = "linux", target_os = "macos"))] permissions: fs::Permissions,
    ) -> Result<()> {
        let Some(file) = self.file.take() else {
            return Err(err("xlsx 임시 저장 파일 handle이 이미 닫혔습니다."));
        };
        ZipArchiveBuilder {
            archive_path: self.path(),
            file,
            parts,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            permissions,
            source_bytes,
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
struct DirectXmlChild<'xml> {
    local_name: &'xml str,
    raw: &'xml str,
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
                    format!(
                        "xlsx 저장 실패: {} -> {}",
                        self.temp_archive.path().display(),
                        self.target_xlsx.display(),
                    ),
                    source,
                ));
            }
            #[cfg(target_os = "windows")]
            Err(atomic_replace::ReplaceFilesError::Restored(source)) => {
                return Err(err_with_source(
                    format!(
                        "xlsx 저장 실패 후 원본 대상 파일 자동 복원 완료: {} -> {}",
                        self.temp_archive.path().display(),
                        self.target_xlsx.display(),
                    ),
                    source,
                ));
            }
            #[cfg(target_os = "windows")]
            Err(atomic_replace::ReplaceFilesError::RecoveryRequired(source)) => {
                let context = format!(
                    "xlsx 저장 중 원본 대상 파일 자동 복구 실패: {} -> {}",
                    self.temp_archive.path().display(),
                    self.target_xlsx.display(),
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
    pub(super) fn ensure_fixed_sheet_catalog(&mut self, workbook_xml: &str) -> Result<()> {
        if workbook_xml.match_indices(WORKBOOK_LOEXT_FRAGMENT).count() != 1 {
            return Err(err(
                "workbook.xml의 LibreOffice 확장 표현이 고정 스키마와 다릅니다.",
            ));
        }
        let mut namespace_scanner = XmlScanner::new(workbook_xml);
        let root = namespace_scanner
            .next_tag()
            .ok_or_else(|| err("workbook.xml에 root 태그가 없습니다."))?;
        if !root.is_start() || root.name() != "workbook" || root.self_closing() {
            return Err(err("workbook.xml의 root 형식이 올바르지 않습니다."));
        }
        if required_xml_attr(root.raw(), "xmlns", "workbook.xml")?.as_ref()
            != SPREADSHEETML_NAMESPACE
        {
            return Err(err("workbook.xml의 root namespace가 올바르지 않습니다."));
        }
        let mut extension_open_seen = false;
        let mut extension_value_seen = false;
        while let Some(tag) = namespace_scanner.next_tag() {
            if !tag.is_start() {
                continue;
            }
            if tag.raw() == WORKBOOK_LOEXT_OPEN_TAG {
                if mem::replace(&mut extension_open_seen, true) {
                    return Err(err(
                        "workbook.xml에 LibreOffice namespace 선언이 여러 개 있습니다.",
                    ));
                }
                continue;
            }
            if tag.raw() == WORKBOOK_LOEXT_VALUE_TAG {
                if mem::replace(&mut extension_value_seen, true) {
                    return Err(err(
                        "workbook.xml에 LibreOffice 확장 값이 여러 개 있습니다.",
                    ));
                }
                continue;
            }
            if tag.name() != tag.local_name() {
                return Err(err(format!(
                    "workbook.xml의 prefixed core element는 지원하지 않습니다: {}",
                    tag.name()
                )));
            }
            reject_namespace_declaration(tag.raw(), "workbook.xml")?;
        }
        if !extension_open_seen || !extension_value_seen {
            return Err(err(
                "workbook.xml의 LibreOffice namespace 표현이 고정 스키마와 다릅니다.",
            ));
        }
        if XmlScanner::new(workbook_xml)
            .next_start_named("fileRecoveryPr")
            .is_some()
        {
            return Err(err(
                "workbook.xml의 fileRecoveryPr 복구 표현은 지원하지 않습니다.",
            ));
        }
        let workbook_relationships = self.take_text("xl/_rels/workbook.xml.rels")?;
        validate_relationship_catalog(
            &workbook_relationships,
            &WORKBOOK_RELATIONSHIPS,
            "workbook.xml.rels",
        )?;
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
        for (expected_name, expected_sheet_id, expected_rid) in [
            (MASTER_SHEET_NAME, "1", "rId3"),
            (CHANGE_LOG_SHEET_NAME, "2", "rId4"),
        ] {
            let sheet_tag = workbook_scanner
                .next_start_named("sheet")
                .ok_or_else(|| err("workbook sheet 수가 고정 스키마의 2개보다 적습니다."))?;
            let tag = sheet_tag.raw();
            validate_exact_attrs(
                tag,
                &[
                    ("name", expected_name),
                    ("sheetId", expected_sheet_id),
                    ("state", "visible"),
                    ("r:id", expected_rid),
                ],
                "workbook.xml sheet",
            )?;
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
        let (source_fingerprint, source_bytes, parts) = ZipPackageReader {
            archive_file: source_file,
            archive_path: source_xlsx,
        }
        .read()?;
        let mut container = Self {
            parts,
            source_bytes,
            source_fingerprint,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            source_permissions,
        };
        container.validate_content_types()?;
        container.validate_root_relationships()?;
        container.part_mut("[Content_Types].xml")?.bytes = Vec::new();
        container.part_mut("_rels/.rels")?.bytes = Vec::new();
        Ok(container)
    }
    fn part(&self, name: &str) -> Result<&PackagePart> {
        XLSX_PART_NAMES
            .iter()
            .position(|part_name| *part_name == name)
            .and_then(|index| self.parts.get(index))
            .ok_or_else(|| err(format!("xlsx part를 찾지 못했습니다: {name}")))
    }
    fn part_mut(&mut self, name: &str) -> Result<&mut PackagePart> {
        XLSX_PART_NAMES
            .iter()
            .zip(&mut self.parts)
            .find_map(|(part_name, part)| (*part_name == name).then_some(part))
            .ok_or_else(|| err(format!("xlsx part를 찾지 못했습니다: {name}")))
    }
    pub(super) fn put_text(&mut self, name: &str, content: String) -> Result<()> {
        if content.len() > MAX_XLSX_TEXT_PART_BYTES {
            return Err(err(format!(
                "xlsx XML part가 너무 큽니다: {name} ({} bytes, 최대 {MAX_XLSX_TEXT_PART_BYTES} bytes)",
                content.len()
            )));
        }
        let part = self.part_mut(name)?;
        part.bytes = content.into_bytes();
        part.changed = true;
        Ok(())
    }
    pub(super) fn save(self, target_xlsx: &Path, verification: SaveVerification) -> Result<()> {
        let parent = target_xlsx
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(|source_err| {
            err_with_source(
                path_context_message("저장 폴더 생성 실패", parent),
                source_err,
            )
        })?;
        let target_file_name = crate::MASTER_PATH;
        let temp_archive_prefix = format!(".{target_file_name}.tmp_");
        let backup_archive_prefix = format!(".{target_file_name}.backup_");
        if let Err(source) = cleanup_stale_temp_files(parent, &temp_archive_prefix) {
            write_path_warning("이전 xlsx 임시 저장 파일 정리 실패", parent, &source);
        }
        if let Err(source) = cleanup_stale_temp_files(parent, &backup_archive_prefix) {
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
                        &self.parts,
                        &self.source_bytes,
                        self.source_permissions,
                    )?;
                }
                target_os = "windows" => {
                    tmp_archive.write_archive_from(&self.parts, &self.source_bytes)?;
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
    pub(super) fn take_shared_strings_text(&mut self) -> Result<String> {
        let xml = self.take_text("xl/sharedStrings.xml")?;
        let mut scanner = XmlScanner::new(&xml);
        let root = scanner
            .next_tag()
            .ok_or_else(|| err("sharedStrings.xml에 root 태그가 없습니다."))?;
        if !root.is_start() || root.name() != "sst" || root.self_closing() {
            return Err(err("sharedStrings.xml의 root 형식이 올바르지 않습니다."));
        }
        if required_xml_attr(root.raw(), "xmlns", "sharedStrings.xml")?.as_ref()
            != SPREADSHEETML_NAMESPACE
        {
            return Err(err(
                "sharedStrings.xml의 root namespace가 올바르지 않습니다.",
            ));
        }
        while let Some(tag) = scanner.next_tag() {
            if tag.name() != tag.local_name() {
                return Err(err(format!(
                    "sharedStrings.xml의 prefixed core element는 지원하지 않습니다: {}",
                    tag.name()
                )));
            }
            if tag.is_start() {
                reject_namespace_declaration(tag.raw(), "sharedStrings.xml")?;
            }
        }
        Ok(xml)
    }
    pub(super) fn take_text(&mut self, name: &str) -> Result<String> {
        let bytes = mem::take(&mut self.part_mut(name)?.bytes);
        if bytes.len() > MAX_XLSX_TEXT_PART_BYTES {
            return Err(err(format!(
                "xlsx XML part가 너무 큽니다: {name} ({} bytes, 최대 {MAX_XLSX_TEXT_PART_BYTES} bytes)",
                bytes.len()
            )));
        }
        String::from_utf8(bytes)
            .map_err(|source| err_with_source(format!("xlsx part UTF-8 해석 실패: {name}"), source))
    }
    fn text(&self, name: &str) -> Result<&str> {
        let part = self.part(name)?;
        if part.bytes.len() > MAX_XLSX_TEXT_PART_BYTES {
            return Err(err(format!(
                "xlsx XML part가 너무 큽니다: {name} ({} bytes, 최대 {MAX_XLSX_TEXT_PART_BYTES} bytes)",
                part.bytes.len()
            )));
        }
        str::from_utf8(&part.bytes)
            .map_err(|source| err_with_source(format!("xlsx part UTF-8 해석 실패: {name}"), source))
    }
    fn validate_content_types(&self) -> Result<()> {
        let content_types_xml = self.text("[Content_Types].xml")?;
        let children = direct_xml_children(
            content_types_xml,
            "Types",
            CONTENT_TYPES_NAMESPACE,
            "[Content_Types].xml",
        )?;
        let mut child_iter = children.iter();
        for (extension, content_type) in CONTENT_TYPE_DEFAULTS {
            let child = child_iter
                .next()
                .ok_or_else(|| err("[Content_Types].xml Default 항목이 부족합니다."))?;
            if child.local_name != "Default" {
                return Err(err("[Content_Types].xml Default 순서가 올바르지 않습니다."));
            }
            validate_exact_attrs(
                child.raw,
                &[("Extension", extension), ("ContentType", content_type)],
                "[Content_Types].xml Default",
            )?;
        }
        for (part_name, content_type) in CONTENT_TYPE_OVERRIDES {
            let child = child_iter
                .next()
                .ok_or_else(|| err("[Content_Types].xml Override 항목이 부족합니다."))?;
            if child.local_name != "Override" {
                return Err(err(
                    "[Content_Types].xml Override 순서가 올바르지 않습니다.",
                ));
            }
            validate_exact_attrs(
                child.raw,
                &[("PartName", part_name), ("ContentType", content_type)],
                "[Content_Types].xml Override",
            )?;
        }
        if child_iter.next().is_some() {
            return Err(err("[Content_Types].xml에 고정 스키마 외 항목이 있습니다."));
        }
        Ok(())
    }
    fn validate_root_relationships(&self) -> Result<()> {
        validate_relationship_catalog(
            self.text("_rels/.rels")?,
            &ROOT_RELATIONSHIPS,
            "_rels/.rels",
        )
    }
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
    if required_xml_attr(root.raw(), "xmlns", &context)?.as_ref() != SPREADSHEETML_NAMESPACE {
        return Err(err(format!(
            "{context}의 worksheet namespace가 올바르지 않습니다."
        )));
    }
    let mut ancestors = Vec::new();
    ancestors
        .try_reserve_exact(8)
        .map_err(|source| err_with_source(format!("{context} stack 메모리 확보 실패"), source))?;
    ancestors.push(root.name());
    while let Some(tag) = scanner.next_tag() {
        if ancestors.is_empty() {
            return Err(err(format!("{context}에 root 밖의 XML 요소가 있습니다.")));
        }
        if tag.is_start() {
            if tag.name() != tag.local_name() {
                return Err(err(format!(
                    "{context}의 prefixed core element는 지원하지 않습니다: {}",
                    tag.name()
                )));
            }
            reject_namespace_declaration(tag.raw(), &context)?;
            if !tag.self_closing() {
                if ancestors.len() == ancestors.capacity() {
                    ancestors.try_reserve(1).map_err(|source| {
                        err_with_source(format!("{context} stack 메모리 확보 실패"), source)
                    })?;
                }
                ancestors.push(tag.name());
            }
            continue;
        }
        let open = ancestors
            .pop()
            .ok_or_else(|| err(format!("{context}의 종료 태그 순서가 올바르지 않습니다.")))?;
        if open != tag.name() {
            return Err(err(format!(
                "{context}의 XML 태그 쌍이 일치하지 않습니다: {} / {}",
                open,
                tag.name()
            )));
        }
    }
    if !ancestors.is_empty() {
        return Err(err(format!("{context}에 닫히지 않은 XML 요소가 있습니다.")));
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
fn validate_exact_attrs(tag: &str, expected: &[(&str, &str)], context: &str) -> Result<()> {
    let mut count = 0_usize;
    let mut attributes = XmlAttrScanner::new(tag)?;
    while let Some((name, _value)) = attributes.next()? {
        if !expected.iter().any(|candidate| name == candidate.0) {
            return Err(err(format!(
                "{context}에 알 수 없는 {name} 속성이 있습니다."
            )));
        }
        count = count
            .checked_add(1)
            .ok_or_else(|| err(format!("{context} 속성 수 계산 실패")))?;
    }
    if count != expected.len() {
        return Err(err(format!("{context} 속성 수가 고정 스키마와 다릅니다.")));
    }
    for &(name, expected_value) in expected {
        let actual = required_xml_attr(tag, name, context)?;
        if actual.as_ref() != expected_value {
            return Err(err(format!(
                "{context}의 {name} 값이 고정 스키마와 다릅니다."
            )));
        }
    }
    Ok(())
}
fn validate_relationship_catalog(
    xml: &str,
    expected: &[(&str, &str, &str)],
    context: &str,
) -> Result<()> {
    let children = direct_xml_children(
        xml,
        "Relationships",
        PACKAGE_RELATIONSHIPS_NAMESPACE,
        context,
    )?;
    if children.len() != expected.len() {
        return Err(err(format!("{context} 관계 수가 고정 스키마와 다릅니다.")));
    }
    for (child, &(id, type_, target)) in children.iter().zip(expected) {
        if child.local_name != "Relationship" {
            return Err(err(format!("{context} 관계 태그가 올바르지 않습니다.")));
        }
        validate_exact_attrs(
            child.raw,
            &[("Id", id), ("Type", type_), ("Target", target)],
            context,
        )?;
    }
    Ok(())
}
fn required_xml_attr<'tag>(
    tag: &'tag str,
    attr_name: &str,
    context: &str,
) -> Result<Cow<'tag, str>> {
    let mut value = None;
    let mut attributes = XmlAttrScanner::new(tag)?;
    while let Some((name, attr_value)) = attributes.next()? {
        if name == attr_name && value.replace(attr_value).is_some() {
            return Err(err(format!(
                "{context}에 중복 {attr_name} 속성이 있습니다."
            )));
        }
    }
    value.ok_or_else(|| err(format!("{context}에 {attr_name} 속성이 없습니다.")))
}
fn reject_namespace_declaration(tag: &str, context: &str) -> Result<()> {
    let mut attributes = XmlAttrScanner::new(tag)?;
    while let Some((name, _)) = attributes.next()? {
        if name == "xmlns" || name.starts_with("xmlns:") {
            return Err(err(format!(
                "{context}의 descendant namespace 재정의는 지원하지 않습니다."
            )));
        }
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
    if !root_tag.is_start() || root_tag.name() != root_local_name {
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
    validate_exact_attrs(
        root_tag.raw(),
        &[("xmlns", expected_namespace)],
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
            if tag.name() != tag.local_name() {
                return Err(err(format!(
                    "{context}의 prefixed child element는 지원하지 않습니다: {}",
                    tag.name()
                )));
            }
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
