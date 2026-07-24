use super::{
    ArchiveFingerprint, CALC_CHAIN_PATH, CHANGE_LOG_SHEET_NAME, EXCEL_XLSX_PART_NAMES,
    MASTER_SHEET_NAME, PackagePart, SPREADSHEETML_NAMESPACE, SaveVerification, XlsxPackageKind,
    ZipArchiveBuilder, ZipPackageReader,
    xml::{XmlAttrScanner, XmlScanner},
    zip_archive::scan_open_archive,
};
use crate::diagnostic::{AppError, Result, err, err_with_source, path_context_message};
use crate::temp_entry::{cleanup_stale_temp_files, reserve_unique_temp_entry};
use crate::validate_regular_file;
use alloc::borrow::Cow;
use core::{mem, range::Range, str};
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
const RICH_DATA2_NAMESPACE: &str =
    "http://schemas.microsoft.com/office/spreadsheetml/2017/richdata2";
const WORKBOOK_LOEXT_VALUE_TAG: &str = "<loext:extCalcPr stringRefSyntax=\"CalcA1ExcelA1\"/>";
const EXCEL_CONTENT_TYPE_DEFAULTS: [(&str, &str); 3] = [
    ("emf", "image/x-emf"),
    (
        "rels",
        "application/vnd.openxmlformats-package.relationships+xml",
    ),
    ("xml", "application/xml"),
];
const EXCEL_CONTENT_TYPE_OVERRIDES: [(&str, &str); 9] = [
    (WORKBOOK_PART_NAME, WORKBOOK_CONTENT_TYPE),
    (
        "/xl/worksheets/sheet1.xml",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml",
    ),
    (
        "/xl/worksheets/sheet2.xml",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml",
    ),
    (
        "/xl/theme/theme1.xml",
        "application/vnd.openxmlformats-officedocument.theme+xml",
    ),
    (
        "/xl/styles.xml",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml",
    ),
    (
        "/xl/sharedStrings.xml",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml",
    ),
    (
        "/xl/calcChain.xml",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.calcChain+xml",
    ),
    (
        "/docProps/core.xml",
        "application/vnd.openxmlformats-package.core-properties+xml",
    ),
    (
        "/docProps/app.xml",
        "application/vnd.openxmlformats-officedocument.extended-properties+xml",
    ),
];
const EXCEL_ROOT_RELATIONSHIPS: [(&str, &str, &str); 4] = [
    (
        "rId3",
        "http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties",
        "docProps/core.xml",
    ),
    (
        "rId2",
        "http://schemas.openxmlformats.org/package/2006/relationships/metadata/thumbnail",
        "docProps/thumbnail.emf",
    ),
    ("rId1", OFFICE_DOCUMENT_REL_TYPE, WORKBOOK_REL_TARGET),
    (
        "rId4",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties",
        "docProps/app.xml",
    ),
];
const EXCEL_WORKBOOK_RELATIONSHIPS: [(&str, &str, &str); 6] = [
    (
        "rId3",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme",
        "theme/theme1.xml",
    ),
    ("rId2", WORKSHEET_REL_TYPE, "worksheets/sheet2.xml"),
    ("rId1", WORKSHEET_REL_TYPE, "worksheets/sheet1.xml"),
    (
        "rId6",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/calcChain",
        "calcChain.xml",
    ),
    (
        "rId5",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings",
        "sharedStrings.xml",
    ),
    (
        "rId4",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles",
        "styles.xml",
    ),
];
const LIBREOFFICE_CONTENT_TYPE_DEFAULTS: [(&str, &str); 5] = [
    ("fntdata", "application/x-fontdata"),
    ("jpeg", "image/jpeg"),
    ("png", "image/png"),
    (
        "rels",
        "application/vnd.openxmlformats-package.relationships+xml",
    ),
    ("xml", "application/xml"),
];
const LIBREOFFICE_CONTENT_TYPE_OVERRIDES: [(&str, &str); 10] = [
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
const LIBREOFFICE_ROOT_RELATIONSHIPS: [(&str, &str, &str); 4] = [
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
const LIBREOFFICE_WORKBOOK_RELATIONSHIPS: [(&str, &str, &str); 5] = [
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
const LIBREOFFICE_SHEET_RELATIONSHIPS: [(&str, &str, &str); 1] = [(
    "rId1",
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/drawing",
    "../drawings/drawing1.xml",
)];
const EXCEL_CONTENT_TYPES_XML: &str = concat!(
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n",
    "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">",
    "<Default Extension=\"emf\" ContentType=\"image/x-emf\"/>",
    "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>",
    "<Default Extension=\"xml\" ContentType=\"application/xml\"/>",
    "<Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>",
    "<Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>",
    "<Override PartName=\"/xl/worksheets/sheet2.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>",
    "<Override PartName=\"/xl/theme/theme1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.theme+xml\"/>",
    "<Override PartName=\"/xl/styles.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml\"/>",
    "<Override PartName=\"/xl/sharedStrings.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml\"/>",
    "<Override PartName=\"/xl/calcChain.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.calcChain+xml\"/>",
    "<Override PartName=\"/docProps/core.xml\" ContentType=\"application/vnd.openxmlformats-package.core-properties+xml\"/>",
    "<Override PartName=\"/docProps/app.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.extended-properties+xml\"/>",
    "</Types>",
);
const EXCEL_ROOT_RELATIONSHIPS_XML: &str = concat!(
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n",
    "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">",
    "<Relationship Id=\"rId3\" Type=\"http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties\" Target=\"docProps/core.xml\"/>",
    "<Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/package/2006/relationships/metadata/thumbnail\" Target=\"docProps/thumbnail.emf\"/>",
    "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"xl/workbook.xml\"/>",
    "<Relationship Id=\"rId4\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties\" Target=\"docProps/app.xml\"/>",
    "</Relationships>",
);
const EXCEL_WORKBOOK_RELATIONSHIPS_XML: &str = concat!(
    "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n",
    "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">",
    "<Relationship Id=\"rId3\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme\" Target=\"theme/theme1.xml\"/>",
    "<Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet2.xml\"/>",
    "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet1.xml\"/>",
    "<Relationship Id=\"rId6\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/calcChain\" Target=\"calcChain.xml\"/>",
    "<Relationship Id=\"rId5\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings\" Target=\"sharedStrings.xml\"/>",
    "<Relationship Id=\"rId4\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles\" Target=\"styles.xml\"/>",
    "</Relationships>",
);
const BLANK_EXCEL_THUMBNAIL_DWORDS: [u32; 32] = [
    1,
    108,
    0,
    0,
    1,
    1,
    0,
    0,
    26,
    26,
    0x464d_4520,
    0x0001_0000,
    128,
    2,
    1,
    0,
    0,
    0,
    96,
    96,
    25,
    25,
    0,
    0,
    0,
    25_000,
    25_000,
    14,
    20,
    0,
    0,
    20,
];
#[derive(Debug)]
pub(crate) struct XlsxContainer {
    package_kind: XlsxPackageKind,
    parts: Vec<PackagePart>,
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
        self.temp_archive.disable_drop_cleanup();
        self.backup_archive.disable_drop_cleanup();
        self.cleanup_displaced_original(displaced_file)?;
        cfg_select! {
            any(target_os = "linux", target_os = "macos") => {
                sync_saved_path(
                    self.target_xlsx,
                    "xlsx 저장 완료 후 파일 내구성 동기화 실패",
                )?;
                let parent = self
                    .target_xlsx
                    .parent()
                    .filter(|path| !path.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
                sync_saved_path(parent, "xlsx 저장 완료 후 폴더 내구성 동기화 실패")?;
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
    pub(super) fn ensure_fixed_sheet_catalog(
        &mut self,
        workbook_xml: &mut String,
    ) -> Result<Option<String>> {
        if workbook_xml.match_indices(WORKBOOK_LOEXT_VALUE_TAG).count() != 1 {
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
        if required_xml_attr(root.raw(), "xmlns:r", "workbook.xml")?.as_ref()
            != OFFICE_DOCUMENT_REL_NAMESPACE
        {
            return Err(err("workbook.xml의 xmlns:r namespace가 올바르지 않습니다."));
        }
        for (tag_name, message) in [
            (
                "fileRecoveryPr",
                "workbook.xml의 fileRecoveryPr 복구 표현은 지원하지 않습니다.",
            ),
            (
                "externalReferences",
                "workbook.xml의 외부 workbook 관계는 지원하지 않습니다.",
            ),
            (
                "connections",
                "workbook.xml의 외부 데이터 연결은 지원하지 않습니다.",
            ),
        ] {
            if XmlScanner::new(workbook_xml)
                .next_start_named(tag_name)
                .is_some()
            {
                return Err(err(message));
            }
        }
        let workbook_relationships = self.take_text("xl/_rels/workbook.xml.rels")?;
        let workbook_relationship_catalog = match self.package_kind {
            XlsxPackageKind::Excel => EXCEL_WORKBOOK_RELATIONSHIPS.as_slice(),
            XlsxPackageKind::LibreOffice => LIBREOFFICE_WORKBOOK_RELATIONSHIPS.as_slice(),
        };
        validate_relationship_catalog(
            &workbook_relationships,
            workbook_relationship_catalog,
            "workbook.xml.rels",
        )?;
        let calc_chain_xml = if self.package_kind == XlsxPackageKind::Excel {
            let xml = self.take_text(CALC_CHAIN_PATH)?;
            let children =
                direct_xml_children(&xml, "calcChain", SPREADSHEETML_NAMESPACE, "calcChain.xml")?;
            if children.is_empty() {
                return Err(err("calcChain.xml에 formula cell이 없습니다."));
            }
            if children.iter().any(|child| child.local_name != "c") {
                return Err(err("calcChain.xml에 고정 스키마 외 요소가 있습니다."));
            }
            Some(xml)
        } else {
            None
        };
        let mut workbook_scanner = XmlScanner::new(workbook_xml);
        workbook_scanner
            .next_start_named("sheets")
            .ok_or_else(|| err("workbook.xml의 sheets 시작 태그를 찾지 못했습니다."))?;
        let sheet_ids = match self.package_kind {
            XlsxPackageKind::Excel => ["rId1", "rId2"],
            XlsxPackageKind::LibreOffice => ["rId3", "rId4"],
        };
        for ((expected_name, expected_sheet_id), expected_rid) in
            [(MASTER_SHEET_NAME, "1"), (CHANGE_LOG_SHEET_NAME, "2")]
                .into_iter()
                .zip(sheet_ids)
        {
            let sheet_tag = workbook_scanner
                .next_start_named("sheet")
                .ok_or_else(|| err("workbook sheet 수가 고정 스키마의 2개보다 적습니다."))?;
            let tag = sheet_tag.raw();
            match self.package_kind {
                XlsxPackageKind::Excel => validate_exact_attrs(
                    tag,
                    &[
                        ("name", expected_name),
                        ("sheetId", expected_sheet_id),
                        ("r:id", expected_rid),
                    ],
                    "workbook.xml sheet",
                )?,
                XlsxPackageKind::LibreOffice => validate_exact_attrs(
                    tag,
                    &[
                        ("name", expected_name),
                        ("sheetId", expected_sheet_id),
                        ("state", "visible"),
                        ("r:id", expected_rid),
                    ],
                    "workbook.xml sheet",
                )?,
            }
        }
        if workbook_scanner.next_start_named("sheet").is_some() {
            return Err(err("workbook sheet 수가 고정 스키마의 2개보다 많습니다."));
        }
        if self.package_kind == XlsxPackageKind::LibreOffice {
            replace_single_self_closing_tag(
                workbook_xml,
                "fileVersion",
                "<fileVersion appName=\"xl\" lastEdited=\"7\" lowestEdited=\"7\" rupBuild=\"27932\"/>",
            )?;
            replace_single_self_closing_tag(workbook_xml, "workbookPr", "<workbookPr/>")?;
            let mut defined_name_scanner = XmlScanner::new(workbook_xml);
            let defined_name = defined_name_scanner
                .next_start_named("definedName")
                .filter(|tag| tag.name() == "definedName" && !tag.self_closing())
                .ok_or_else(|| {
                    err("LibreOffice workbook의 _FilterDatabase 태그가 올바르지 않습니다.")
                })?;
            validate_exact_attrs(
                defined_name.raw(),
                &[
                    ("function", "false"),
                    ("hidden", "true"),
                    ("localSheetId", "0"),
                    ("name", "_xlnm._FilterDatabase"),
                    ("vbProcedure", "false"),
                ],
                "LibreOffice workbook _FilterDatabase",
            )?;
            let defined_name_span = defined_name.start()
                ..defined_name
                    .end()
                    .checked_add(1)
                    .ok_or_else(|| err("LibreOffice _FilterDatabase 태그 끝 계산 실패"))?;
            workbook_xml.replace_range(
                defined_name_span,
                "<definedName name=\"_xlnm._FilterDatabase\" localSheetId=\"0\" hidden=\"1\">",
            );
            let mut sheet_scanner = XmlScanner::new(workbook_xml);
            let mut sheet_spans = [0..0, 0..0];
            for span in &mut sheet_spans {
                let tag = sheet_scanner
                    .next_start_named("sheet")
                    .ok_or_else(|| err("LibreOffice workbook sheet 태그가 없습니다."))?;
                if !tag.self_closing() {
                    return Err(err("LibreOffice workbook sheet 태그가 올바르지 않습니다."));
                }
                *span = tag.start()
                    ..tag
                        .end()
                        .checked_add(1)
                        .ok_or_else(|| err("workbook sheet 태그 끝 계산 실패"))?;
            }
            let [master_sheet_span, change_log_sheet_span] = sheet_spans;
            workbook_xml.replace_range(
                change_log_sheet_span,
                "<sheet name=\"변경내역\" sheetId=\"2\" r:id=\"rId2\"/>",
            );
            workbook_xml.replace_range(
                master_sheet_span,
                "<sheet name=\"유류비\" sheetId=\"1\" r:id=\"rId1\"/>",
            );
        }
        Ok(calc_chain_xml)
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
        let (source_fingerprint, source_bytes, package_kind, parts) = ZipPackageReader {
            archive_file: source_file,
            archive_path: source_xlsx,
        }
        .read()?;
        let mut container = Self {
            package_kind,
            parts,
            source_bytes,
            source_fingerprint,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            source_permissions,
        };
        container.validate_content_types()?;
        container.validate_root_relationships()?;
        container.validate_libreoffice_companion_parts()?;
        container.part_mut("[Content_Types].xml")?.bytes = Vec::new();
        container.part_mut("_rels/.rels")?.bytes = Vec::new();
        Ok(container)
    }
    pub(crate) fn package_prepare_excel_output(&mut self) -> Result<()> {
        if self.package_kind == XlsxPackageKind::Excel {
            return Ok(());
        }
        let mut source_parts = mem::take(&mut self.parts);
        let mut output_parts = Vec::new();
        output_parts
            .try_reserve_exact(EXCEL_XLSX_PART_NAMES.len())
            .map_err(|source| {
                err_with_source("Excel package part 목록 메모리 확보 실패", source)
            })?;
        for name in EXCEL_XLSX_PART_NAMES {
            let bytes = match name {
                "[Content_Types].xml" => EXCEL_CONTENT_TYPES_XML.as_bytes().to_vec(),
                "_rels/.rels" => EXCEL_ROOT_RELATIONSHIPS_XML.as_bytes().to_vec(),
                "xl/_rels/workbook.xml.rels" => {
                    EXCEL_WORKBOOK_RELATIONSHIPS_XML.as_bytes().to_vec()
                }
                "docProps/thumbnail.emf" => {
                    let thumbnail_len = BLANK_EXCEL_THUMBNAIL_DWORDS
                        .len()
                        .checked_mul(size_of::<u32>())
                        .ok_or_else(|| err("Excel thumbnail 크기 계산 실패"))?;
                    let mut bytes = Vec::new();
                    bytes.try_reserve_exact(thumbnail_len).map_err(|source| {
                        err_with_source("Excel thumbnail 메모리 확보 실패", source)
                    })?;
                    for value in BLANK_EXCEL_THUMBNAIL_DWORDS {
                        bytes.extend_from_slice(&value.to_le_bytes());
                    }
                    bytes
                }
                CALC_CHAIN_PATH => Vec::new(),
                _ => {
                    let part = source_parts
                        .iter_mut()
                        .find(|part| part.name == name)
                        .ok_or_else(|| {
                            err(format!(
                                "LibreOffice 입력에서 Excel 공통 part를 찾지 못했습니다: {name}"
                            ))
                        })?;
                    let bytes = mem::take(&mut part.bytes);
                    if name == "docProps/app.xml" {
                        let mut xml = String::from_utf8(bytes).map_err(|source_error| {
                            err_with_source("LibreOffice app.xml UTF-8 해석 실패", source_error)
                        })?;
                        replace_exact_element_text(&mut xml, "Application", "Microsoft Excel")?;
                        replace_exact_element_text(&mut xml, "AppVersion", "16.0300")?;
                        xml.into_bytes()
                    } else {
                        bytes
                    }
                }
            };
            output_parts.push(PackagePart {
                bytes,
                central_record: Range { start: 0, end: 0 },
                changed: true,
                local_record: Range { start: 0, end: 0 },
                name,
            });
        }
        self.parts = output_parts;
        self.package_kind = XlsxPackageKind::Excel;
        Ok(())
    }
    fn part(&self, name: &str) -> Result<&PackagePart> {
        self.parts
            .iter()
            .find(|part| part.name == name)
            .ok_or_else(|| err(format!("xlsx part를 찾지 못했습니다: {name}")))
    }
    fn part_mut(&mut self, name: &str) -> Result<&mut PackagePart> {
        self.parts
            .iter_mut()
            .find(|part| part.name == name)
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
                validate_descendant_namespace_declaration(
                    tag.name(),
                    tag.raw(),
                    "sharedStrings.xml",
                )?;
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
    pub(super) fn take_worksheet_text(&mut self, name: &str, sheet_name: &str) -> Result<String> {
        let mut xml = self.take_text(name)?;
        if self.package_kind == XlsxPackageKind::LibreOffice && name == super::MASTER_SHEET_PATH {
            let mut scanner = XmlScanner::new(&xml);
            let drawing = scanner
                .next_start_named("drawing")
                .filter(|tag| tag.name() == "drawing" && tag.self_closing())
                .ok_or_else(|| {
                    err("LibreOffice sheet1.xml의 빈 drawing 참조를 찾지 못했습니다.")
                })?;
            validate_exact_attrs(
                drawing.raw(),
                &[("r:id", "rId1")],
                "LibreOffice sheet1.xml drawing",
            )?;
            let span = drawing.start()
                ..drawing
                    .end()
                    .checked_add(1)
                    .ok_or_else(|| err("LibreOffice drawing 참조 끝 계산 실패"))?;
            scanner.skip_to(span.end);
            if scanner.next_start_named("drawing").is_some() {
                return Err(err(
                    "LibreOffice sheet1.xml에 drawing 참조가 여러 개 있습니다.",
                ));
            }
            xml.replace_range(span, "");
        }
        let context = format!("worksheet XML namespace 검증: {sheet_name}");
        let mut scanner = XmlScanner::new(&xml);
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
        ancestors.try_reserve_exact(8).map_err(|source| {
            err_with_source(format!("{context} stack 메모리 확보 실패"), source)
        })?;
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
                validate_descendant_namespace_declaration(tag.name(), tag.raw(), &context)?;
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
        Ok(xml)
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
        let (defaults, overrides) = match self.package_kind {
            XlsxPackageKind::Excel => (
                EXCEL_CONTENT_TYPE_DEFAULTS.as_slice(),
                EXCEL_CONTENT_TYPE_OVERRIDES.as_slice(),
            ),
            XlsxPackageKind::LibreOffice => (
                LIBREOFFICE_CONTENT_TYPE_DEFAULTS.as_slice(),
                LIBREOFFICE_CONTENT_TYPE_OVERRIDES.as_slice(),
            ),
        };
        let children = direct_xml_children(
            content_types_xml,
            "Types",
            CONTENT_TYPES_NAMESPACE,
            "[Content_Types].xml",
        )?;
        let mut child_iter = children.iter();
        for &(extension, content_type) in defaults {
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
        for &(part_name, content_type) in overrides {
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
    fn validate_libreoffice_companion_parts(&mut self) -> Result<()> {
        if self.package_kind != XlsxPackageKind::LibreOffice {
            return Ok(());
        }
        validate_relationship_catalog(
            self.text("xl/worksheets/_rels/sheet1.xml.rels")?,
            &LIBREOFFICE_SHEET_RELATIONSHIPS,
            "sheet1.xml.rels",
        )?;
        validate_empty_xml_root(
            self.text("docProps/custom.xml")?,
            "Properties",
            &[
                (
                    "xmlns",
                    "http://schemas.openxmlformats.org/officeDocument/2006/custom-properties",
                ),
                (
                    "xmlns:vt",
                    "http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes",
                ),
            ],
            "custom.xml",
        )?;
        validate_empty_xml_root(
            self.text("xl/drawings/drawing1.xml")?,
            "xdr:wsDr",
            &[
                (
                    "xmlns:xdr",
                    "http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing",
                ),
                (
                    "xmlns:a",
                    "http://schemas.openxmlformats.org/drawingml/2006/main",
                ),
                (
                    "xmlns:r",
                    "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
                ),
            ],
            "drawing1.xml",
        )?;
        for name in [
            "docProps/custom.xml",
            "xl/worksheets/_rels/sheet1.xml.rels",
            "xl/drawings/drawing1.xml",
        ] {
            self.part_mut(name)?.bytes = Vec::new();
        }
        Ok(())
    }
    fn validate_root_relationships(&self) -> Result<()> {
        let catalog = match self.package_kind {
            XlsxPackageKind::Excel => EXCEL_ROOT_RELATIONSHIPS.as_slice(),
            XlsxPackageKind::LibreOffice => LIBREOFFICE_ROOT_RELATIONSHIPS.as_slice(),
        };
        validate_relationship_catalog(self.text("_rels/.rels")?, catalog, "_rels/.rels")
    }
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
fn validate_empty_xml_root(
    xml: &str,
    expected_name: &str,
    expected_attrs: &[(&str, &str)],
    context: &str,
) -> Result<()> {
    let mut scanner = XmlScanner::new(xml);
    let root = scanner
        .next_tag()
        .ok_or_else(|| err(format!("{context}의 XML root 태그가 없습니다.")))?;
    if !root.is_start() || root.name() != expected_name {
        return Err(err(format!(
            "{context}의 XML root 태그가 올바르지 않습니다."
        )));
    }
    let leading = xml
        .get(..root.start())
        .ok_or_else(|| err(format!("{context}의 XML root 범위가 손상되었습니다.")))?;
    if !xml_misc_only(leading, true) {
        return Err(err(format!(
            "{context}의 XML root 앞 내용이 올바르지 않습니다."
        )));
    }
    validate_exact_attrs(root.raw(), expected_attrs, context)?;
    let root_end = root
        .end()
        .checked_add(1)
        .ok_or_else(|| err(format!("{context}의 XML root 끝 계산 실패")))?;
    let document_end = if root.self_closing() {
        root_end
    } else {
        let close = scanner
            .next_tag()
            .filter(|tag| !tag.is_start() && tag.name() == expected_name)
            .ok_or_else(|| err(format!("{context}의 XML root가 비어 있지 않습니다.")))?;
        close
            .end()
            .checked_add(1)
            .ok_or_else(|| err(format!("{context}의 XML root 종료 계산 실패")))?
    };
    if scanner.next_tag().is_some()
        || !xml
            .get(document_end..)
            .is_some_and(|trailing| xml_misc_only(trailing, false))
    {
        return Err(err(format!(
            "{context}의 XML root 뒤 내용이 올바르지 않습니다."
        )));
    }
    Ok(())
}
fn replace_exact_element_text(xml: &mut String, name: &str, replacement: &str) -> Result<()> {
    let open = format!("<{name}>");
    let close = format!("</{name}>");
    let body_start = xml
        .find(&open)
        .and_then(|start| start.checked_add(open.len()))
        .ok_or_else(|| err(format!("app.xml의 {name} 태그를 찾지 못했습니다.")))?;
    let relative_end = xml
        .get(body_start..)
        .and_then(|tail| tail.find(&close))
        .ok_or_else(|| err(format!("app.xml의 {name} 종료 태그를 찾지 못했습니다.")))?;
    let body_end = body_start
        .checked_add(relative_end)
        .ok_or_else(|| err(format!("app.xml의 {name} 범위 계산 실패")))?;
    let trailing = body_end
        .checked_add(close.len())
        .and_then(|after| xml.get(after..))
        .ok_or_else(|| err(format!("app.xml의 {name} 종료 범위 계산 실패")))?;
    if trailing.contains(&open) {
        return Err(err(format!("app.xml에 {name} 태그가 여러 개 있습니다.")));
    }
    xml.replace_range(body_start..body_end, replacement);
    Ok(())
}
fn replace_single_self_closing_tag(xml: &mut String, name: &str, replacement: &str) -> Result<()> {
    let mut scanner = XmlScanner::new(xml);
    let tag = scanner
        .next_start_named(name)
        .filter(|tag| tag.name() == name && tag.self_closing())
        .ok_or_else(|| {
            err(format!(
                "LibreOffice workbook의 {name} 태그가 올바르지 않습니다."
            ))
        })?;
    let span = tag.start()
        ..tag
            .end()
            .checked_add(1)
            .ok_or_else(|| err(format!("LibreOffice workbook의 {name} 태그 끝 계산 실패")))?;
    scanner.skip_to(span.end);
    if scanner.next_start_named(name).is_some() {
        return Err(err(format!(
            "LibreOffice workbook에 {name} 태그가 여러 개 있습니다."
        )));
    }
    xml.replace_range(span, replacement);
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
fn validate_descendant_namespace_declaration(
    tag_name: &str,
    tag: &str,
    context: &str,
) -> Result<()> {
    let mut attributes = XmlAttrScanner::new(tag)?;
    while let Some((name, value)) = attributes.next()? {
        if name == "xmlns" || name.starts_with("xmlns:") {
            if tag_name == "sortState"
                && name == "xmlns:xlrd2"
                && value.as_ref() == RICH_DATA2_NAMESPACE
            {
                continue;
            }
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
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn sync_saved_path(path: &Path, context: &str) -> Result<()> {
    fs::File::open(path)
        .and_then(|file| file.sync_all())
        .map_err(|source| err_with_source(path_context_message(context, path), source))
}
