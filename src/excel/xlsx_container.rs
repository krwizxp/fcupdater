use super::{
    ArchiveFingerprint, CALC_CHAIN_PATH, CHANGE_LOG_SHEET_NAME, CanonicalStyleMap,
    MASTER_SHEET_NAME, PackagePart, PartRole, SPREADSHEETML_NAMESPACE, SaveVerification,
    XLSX_PARTS, ZipArchiveBuilder, ZipPackageReader,
    xml::{
        MAX_XML_NESTING_DEPTH, XmlAttrScanner, XmlScanner, XmlTag, decode_xml_entities,
        xml_misc_only,
    },
    zip_archive::scan_open_archive,
};
use crate::diagnostic::{
    AppError, Result, err, err_with_source, path_context_message, terminal_safe,
    try_string_with_capacity, try_vec_with_capacity,
};
#[cfg(target_os = "windows")]
use crate::temp_entry::configure_replaceable_file;
use crate::temp_entry::{
    FileIdentity, ValidatedFile, configure_no_follow, open_regular, validate_open_file,
    validate_regular_file,
};
use alloc::borrow::Cow;
use core::{array, mem, str};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::fs::OpenOptionsExt as _;
use std::{
    fs,
    io::{self, Seek as _, Write as _, stderr},
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};
mod atomic_replace;
const CONTENT_TYPES_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/package/2006/content-types";
const OFFICE_DOCUMENT_REL_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
const OFFICE_DOCUMENT_REL_TYPE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument";
const PACKAGE_RELATIONSHIPS_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/package/2006/relationships";
const WORKBOOK_REL_TARGET: &str = "xl/workbook.xml";
const WORKSHEET_REL_TYPE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet";
const RICH_DATA2_NAMESPACE: &str =
    "http://schemas.microsoft.com/office/spreadsheetml/2017/richdata2";
const CONTENT_TYPE_DEFAULTS: [(&str, &str, PartRole); 6] = [
    ("emf", "image/x-emf", PartRole::OptionalInput),
    (
        "rels",
        "application/vnd.openxmlformats-package.relationships+xml",
        PartRole::Required,
    ),
    ("xml", "application/xml", PartRole::Required),
    ("fntdata", "application/x-fontdata", PartRole::InputOnly),
    ("jpeg", "image/jpeg", PartRole::InputOnly),
    ("png", "image/png", PartRole::InputOnly),
];
const ROOT_RELATIONSHIPS: [RelationshipSpec; 5] = [
    RelationshipSpec {
        optional_part: None,
        target: "docProps/core.xml",
        type_: "http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties",
    },
    RelationshipSpec {
        optional_part: Some("docProps/thumbnail.emf"),
        target: "docProps/thumbnail.emf",
        type_: "http://schemas.openxmlformats.org/package/2006/relationships/metadata/thumbnail",
    },
    RelationshipSpec {
        optional_part: None,
        target: WORKBOOK_REL_TARGET,
        type_: OFFICE_DOCUMENT_REL_TYPE,
    },
    RelationshipSpec {
        optional_part: None,
        target: "docProps/app.xml",
        type_: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties",
    },
    RelationshipSpec {
        optional_part: Some("docProps/custom.xml"),
        target: "docProps/custom.xml",
        type_: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/custom-properties",
    },
];
const WORKBOOK_RELATIONSHIPS: [RelationshipSpec; 6] = [
    RelationshipSpec {
        optional_part: None,
        target: "theme/theme1.xml",
        type_: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme",
    },
    RelationshipSpec {
        optional_part: None,
        target: "worksheets/sheet2.xml",
        type_: WORKSHEET_REL_TYPE,
    },
    RelationshipSpec {
        optional_part: None,
        target: "worksheets/sheet1.xml",
        type_: WORKSHEET_REL_TYPE,
    },
    RelationshipSpec {
        optional_part: None,
        target: "sharedStrings.xml",
        type_: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings",
    },
    RelationshipSpec {
        optional_part: None,
        target: "styles.xml",
        type_: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles",
    },
    RelationshipSpec {
        optional_part: Some(CALC_CHAIN_PATH),
        target: "calcChain.xml",
        type_: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/calcChain",
    },
];
const SHEET_RELATIONSHIPS: [RelationshipSpec; 1] = [RelationshipSpec {
    optional_part: None,
    target: "../drawings/drawing1.xml",
    type_: "http://schemas.openxmlformats.org/officeDocument/2006/relationships/drawing",
}];
const EXCEL_STYLES_XML: &str = include_str!("excel_styles.xml");
const EXCEL_THEME_XML: &str = include_str!("excel_theme.xml");
const EXCEL_CONTENT_TYPES_XML: &str = include_str!("excel_content_types.xml");
const EXCEL_ROOT_RELS_XML: &str = include_str!("excel_root_rels.xml");
const EXCEL_WORKBOOK_RELS_XML: &str = include_str!("excel_workbook_rels.xml");
const LIBREOFFICE_CELL_XFS_XML: &str = include_str!("libreoffice_cell_xfs.xml");
const LIBREOFFICE_STYLE_MAP: [u32; 26] = [
    0, 4, 5, 24, 2, 1, 25, 3, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 19, 27, 20, 21, 22, 23,
];
const EXCEL_CORE_PROPERTIES: [(&str, &str, &str); 9] = [
    ("dc:title", "<dc:title>", "</dc:title>"),
    ("dc:subject", "<dc:subject>", "</dc:subject>"),
    ("dc:creator", "<dc:creator>", "</dc:creator>"),
    ("dc:description", "<dc:description>", "</dc:description>"),
    (
        "cp:lastModifiedBy",
        "<cp:lastModifiedBy>",
        "</cp:lastModifiedBy>",
    ),
    ("cp:revision", "<cp:revision>", "</cp:revision>"),
    (
        "dcterms:created",
        "<dcterms:created xsi:type=\"dcterms:W3CDTF\">",
        "</dcterms:created>",
    ),
    (
        "dcterms:modified",
        "<dcterms:modified xsi:type=\"dcterms:W3CDTF\">",
        "</dcterms:modified>",
    ),
    ("dc:language", "<dc:language>", "</dc:language>"),
];
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
#[derive(Clone, Copy)]
struct RelationshipSpec {
    optional_part: Option<&'static str>,
    target: &'static str,
    type_: &'static str,
}
pub(crate) struct XlsxContainer {
    parts: Vec<PackagePart>,
    source_fingerprint: ArchiveFingerprint,
    source_identity: FileIdentity,
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    source_permissions: fs::Permissions,
}
struct ReservedTempArchive {
    file: Option<fs::File>,
    identity: FileIdentity,
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
    fn remove_owned(&mut self) -> io::Result<()> {
        self.remove_on_drop = false;
        match open_regular(&self.path, false) {
            Ok(file) if file.identity == self.identity => {
                drop(file.file);
                drop(self.file.take());
                fs::remove_file(&self.path)
            }
            Ok(_) => Err(io::Error::other(
                "임시 저장 경로의 파일 identity가 생성 시점과 다릅니다.",
            )),
            Err(source) if source.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(source) => Err(source),
        }
    }
    fn validate_path_identity(&self) -> Result<()> {
        let file = open_regular(&self.path, false).map_err(|source| {
            err_with_source(
                path_context_message("xlsx 임시 파일 identity 검증 실패", &self.path),
                source,
            )
        })?;
        (file.identity == self.identity).ok_or_else(|| {
            err(format!(
                "xlsx 임시 파일 identity가 실행 중 변경되었습니다: {}",
                self.path.display()
            ))
        })
    }
    fn verify_saved_archive(&mut self) -> Result<()> {
        let mut saved_handle = self.file.take().unwrap_or_else(|| process::abort());
        let saved_archive = self.path();
        saved_handle.rewind().map_err(|source| {
            err_with_source(
                path_context_message("저장 검증용 xlsx seek 실패", saved_archive),
                source,
            )
        })?;
        let saved_container = (|| {
            let saved_file = validate_open_file(saved_handle).map_err(|source_err| {
                err_with_source(
                    path_context_message("마스터 xlsx 파일 검증 실패", saved_archive),
                    source_err,
                )
            })?;
            XlsxContainer::from_validated_file(saved_file, saved_archive)
        })()
        .map_err(|source_err| {
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
        #[cfg(any(target_os = "linux", target_os = "macos"))] permissions: fs::Permissions,
    ) -> Result<()> {
        let file = self.file.as_mut().unwrap_or_else(|| process::abort());
        ZipArchiveBuilder {
            archive_path: self.path.as_path(),
            file,
            parts,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            permissions,
        }
        .create()
    }
}
impl Drop for ReservedTempArchive {
    fn drop(&mut self) {
        if self.remove_on_drop
            && let Err(source) = self.remove_owned()
        {
            let mut error_output = stderr().lock();
            let path_display = self.path.display();
            match writeln!(
                &mut error_output,
                "경고: xlsx 임시 저장 파일 정리 실패: {} ({})",
                terminal_safe(&path_display),
                terminal_safe(&source),
            ) {
                Ok(()) | Err(_) => {}
            }
        }
    }
}
struct TempArchivePromotion<'path> {
    #[cfg(target_os = "windows")]
    backup_archive: &'path mut ReservedTempArchive,
    expected_fingerprint: ArchiveFingerprint,
    expected_identity: FileIdentity,
    target_xlsx: &'path Path,
    temp_archive: &'path mut ReservedTempArchive,
}
impl TempArchivePromotion<'_> {
    fn displaced_original_path(&self) -> &Path {
        cfg_select! {
            target_os = "windows" => {
                self.backup_archive.path()
            }
            any(target_os = "linux", target_os = "macos") => {
                self.temp_archive.path()
            }
            _ => {
                compile_error!("fcupdater archive promotion supports only Windows, Linux, and macOS.")
            }
        }
    }
    fn preserve_recovery_archives(
        &mut self,
        context: &str,
        source: atomic_replace::ReplaceFailure,
    ) -> Result<()> {
        let message = cfg_select! {
            target_os = "windows" => {
                format!(
                    "{context}; 자동 복구 실패 후 수동 복구를 위해 현재 경로 상태를 보존했습니다: target={}, replacement={}, backup={}",
                    self.target_xlsx.display(),
                    self.temp_archive.path().display(),
                    self.backup_archive.path().display(),
                )
            }
            any(target_os = "linux", target_os = "macos") => {
                format!(
                    "{context}; 자동 복구 실패 후 수동 복구를 위해 현재 경로 상태를 보존했습니다: target={}, replacement={}",
                    self.target_xlsx.display(),
                    self.temp_archive.path().display(),
                )
            }
            _ => {
                compile_error!("fcupdater archive promotion supports only Windows, Linux, and macOS.")
            }
        };
        self.temp_archive.disable_drop_cleanup();
        #[cfg(target_os = "windows")]
        self.backup_archive.disable_drop_cleanup();
        Err(err_with_source(message, source))
    }
    fn promote(mut self) -> Result<()> {
        self.temp_archive.validate_path_identity()?;
        #[cfg(target_os = "windows")]
        self.backup_archive.validate_path_identity()?;
        let target_file = open_regular(self.target_xlsx, false).map_err(|source| {
            err_with_source(
                path_context_message("저장 직전 원본 xlsx identity 검증 실패", self.target_xlsx),
                source,
            )
        })?;
        if target_file.identity != self.expected_identity {
            return Err(err(format!(
                "원본 xlsx identity가 실행 중 변경되어 저장을 중단했습니다: {}",
                self.target_xlsx.display()
            )));
        }
        #[cfg(target_os = "windows")]
        {
            drop(self.temp_archive.file.take());
            drop(self.backup_archive.file.take());
        }
        let replace_result = cfg_select! {
            target_os = "windows" => {
                atomic_replace::replace_files(
                    self.target_xlsx,
                    self.temp_archive.path(),
                    self.backup_archive.path(),
                    self.backup_archive.path(),
                )
            }
            any(target_os = "linux", target_os = "macos") => {
                atomic_replace::exchange_files(self.target_xlsx, self.temp_archive.path())
            }
            _ => { compile_error!("fcupdater archive promotion supports only Windows, Linux, and macOS.") }
        };
        match replace_result {
            Ok(()) => {}
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
        }
        if let Err(validation_error) = self.validate_displaced_original() {
            return self.rollback_after_validation_failure(validation_error);
        }
        let saved_validation = (|| {
            let saved_file = open_regular(self.target_xlsx, true).map_err(|source| {
                err_with_source(
                    path_context_message("저장된 xlsx identity 검증 실패", self.target_xlsx),
                    source,
                )
            })?;
            if saved_file.identity != self.temp_archive.identity {
                return Err(err("저장된 xlsx identity가 임시 archive와 다릅니다."));
            }
            saved_file.file.sync_all().map_err(|source| {
                err_with_source(
                    path_context_message(
                        "xlsx 저장 완료 후 파일 내구성 동기화 실패",
                        self.target_xlsx,
                    ),
                    source,
                )
            })
        })();
        if let Err(validation_error) = saved_validation {
            return self.rollback_after_validation_failure(validation_error);
        }
        self.temp_archive.disable_drop_cleanup();
        #[cfg(target_os = "windows")]
        self.backup_archive.disable_drop_cleanup();
        let captured_original = self.displaced_original_path();
        fs::remove_file(captured_original).map_err(|source| {
            err_with_source(
                path_context_message("교체된 원본 xlsx 정리 실패", captured_original),
                source,
            )
        })?;
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let parent = self
                .target_xlsx
                .parent()
                .filter(|path| !path.is_empty())
                .unwrap_or_else(|| Path::new("."));
            fs::File::open(parent)
                .and_then(|file| file.sync_all())
                .map_err(|source| {
                    err_with_source(
                        path_context_message("xlsx 저장 완료 후 폴더 내구성 동기화 실패", parent),
                        source,
                    )
                })?;
        }
        Ok(())
    }
    fn rollback_after_validation_failure(&mut self, validation_error: AppError) -> Result<()> {
        let replace_result = cfg_select! {
            target_os = "windows" => {
                atomic_replace::replace_files(
                    self.target_xlsx,
                    self.backup_archive.path(),
                    self.temp_archive.path(),
                    self.backup_archive.path(),
                )
            }
            any(target_os = "linux", target_os = "macos") => {
                atomic_replace::exchange_files(self.target_xlsx, self.temp_archive.path())
            }
            _ => { compile_error!("fcupdater archive rollback supports only Windows, Linux, and macOS.") }
        };
        let rollback_error = match replace_result {
            Ok(()) => return Err(validation_error),
            #[cfg(target_os = "windows")]
            Err(atomic_replace::ReplaceFilesError::Restored(_)) => return Err(validation_error),
            #[cfg(target_os = "windows")]
            Err(atomic_replace::ReplaceFilesError::RecoveryRequired(source)) => source,
            Err(atomic_replace::ReplaceFilesError::Failed(source)) => source,
        };
        let context = format!("원본 xlsx 검증 실패 후 복구 실패 ({validation_error})");
        self.preserve_recovery_archives(&context, rollback_error)
    }
    fn validate_displaced_original(&self) -> Result<()> {
        let captured_original = self.displaced_original_path();
        let captured_file = open_regular(captured_original, false).map_err(|source| {
            err_with_source(
                path_context_message("교체된 원본 xlsx 열기 실패", captured_original),
                source,
            )
        })?;
        if captured_file.identity != self.expected_identity {
            return Err(err(format!(
                "교체된 원본 xlsx identity가 실행 시작 시점과 다릅니다: {}",
                captured_original.display()
            )));
        }
        let fingerprint =
            scan_open_archive(&captured_file.file, captured_original, None).map_err(|source| {
                err_with_source(
                    path_context_message("교체된 원본 xlsx 검증 실패", captured_original),
                    source,
                )
            })?;
        (fingerprint == self.expected_fingerprint).ok_or_else(|| {
            err(format!(
                "원본 xlsx가 실행 중 변경되어 저장을 중단했습니다: {}",
                self.target_xlsx.display()
            ))
        })
    }
}
impl XlsxContainer {
    pub(super) fn ensure_supported_workbook(&mut self) -> Result<()> {
        let workbook_text = self.take_text("xl/workbook.xml")?;
        let root =
            validate_spreadsheet_xml_document(&workbook_text, "workbook", "workbook.xml", true)?;
        if required_xml_attr(root.raw, "xmlns:r", "workbook.xml")?.as_ref()
            != OFFICE_DOCUMENT_REL_NAMESPACE
        {
            return Err(err("workbook.xml의 xmlns:r namespace가 올바르지 않습니다."));
        }
        let workbook_xml = workbook_text.as_str();
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
            (
                "workbookProtection",
                "workbook.xml의 보호 설정은 정규 저장으로 보존할 수 없습니다.",
            ),
        ] {
            if XmlScanner::new(workbook_xml)
                .next_start_named(tag_name)
                .is_some()
            {
                return Err(err(message));
            }
        }
        self.take_workbook_dependencies(workbook_xml)?;
        let mut properties_scanner = XmlScanner::new(workbook_xml);
        if let Some(properties) = properties_scanner.next_element_named("workbookPr")? {
            if properties.opening.name != "workbookPr" || !properties.body.trim().is_empty() {
                return Err(err("workbookPr 요소가 올바르지 않습니다."));
            }
            let mut date_system = None;
            let mut attributes = XmlAttrScanner::new(properties.opening.raw)?;
            while let Some((name, value)) = attributes.next()? {
                if name == "date1904" && date_system.replace(value).is_some() {
                    return Err(err("workbookPr에 date1904 속성이 중복되었습니다."));
                }
            }
            if date_system
                .as_deref()
                .is_some_and(|value| !matches!(value, "0" | "false"))
            {
                return Err(err("workbookPr의 1904 날짜 체계는 지원하지 않습니다."));
            }
            if properties_scanner.next_start_named("workbookPr").is_some() {
                return Err(err("workbook에 workbookPr 요소가 여러 개 있습니다."));
            }
        }
        Ok(())
    }
    pub(crate) fn from_validated_file(
        source_file: ValidatedFile,
        source_xlsx: &Path,
    ) -> Result<Self> {
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        let source_permissions = source_file.permissions;
        let source_identity = source_file.identity;
        let (source_fingerprint, parts) = ZipPackageReader {
            archive_file: source_file.file,
            archive_path: source_xlsx,
        }
        .read()?;
        let mut container = Self {
            parts,
            source_fingerprint,
            source_identity,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            source_permissions,
        };
        container.text("xl/theme/theme1.xml")?;
        container.validate_content_types()?;
        validate_relationship_set(
            container.text("_rels/.rels")?,
            "_rels/.rels",
            &ROOT_RELATIONSHIPS,
            &container,
        )?;
        if container.has_part("docProps/custom.xml") {
            validate_empty_xml_root(
                container.text("docProps/custom.xml")?,
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
        }
        container.part_mut("[Content_Types].xml").bytes = Vec::new();
        container.part_mut("_rels/.rels").bytes = Vec::new();
        Ok(container)
    }
    fn has_part(&self, name: &str) -> bool {
        self.parts.iter().any(|part| part.name == name)
    }
    pub(super) fn package_prepare_excel_output(&mut self) -> Result<CanonicalStyleMap> {
        let source_styles = self.text("xl/styles.xml")?;
        let source_xfs = style_entries(source_styles, "cellXfs", "xf")?;
        let source_fonts = style_entries(source_styles, "fonts", "font")?;
        let excel_xfs = style_entries(EXCEL_STYLES_XML, "cellXfs", "xf")?;
        let excel_fonts = style_entries(EXCEL_STYLES_XML, "fonts", "font")?;
        let libreoffice_xfs = style_entries(LIBREOFFICE_CELL_XFS_XML, "cellXfs", "xf")?;
        if libreoffice_xfs.len() != LIBREOFFICE_STYLE_MAP.len() {
            return Err(err(
                "내장 LibreOffice style mapping 수가 올바르지 않습니다.",
            ));
        }
        let mut input_styles =
            try_vec_with_capacity(source_xfs.len(), "입력 style mapping 메모리 확보 실패")?;
        for source_xf in source_xfs {
            let canonical = match find_equivalent_xf(
                source_xf,
                &excel_xfs,
                Some((&source_fonts, &excel_fonts)),
            )? {
                Some(index) => Some(
                    u32::try_from(index)
                        .map_err(|error| err_with_source("Excel style index 변환 실패", error))?,
                ),
                None => find_equivalent_xf(source_xf, &libreoffice_xfs, None)?
                    .and_then(|index| LIBREOFFICE_STYLE_MAP.get(index))
                    .copied(),
            };
            input_styles.push(canonical);
        }
        let source_core = self.part_mut("docProps/core.xml");
        let source_core_xml = str::from_utf8(&source_core.bytes)
            .map_err(|source| err_with_source("core.xml UTF-8 해석 실패", source))?;
        let mut core_xml = try_string_with_capacity(
            source_core_xml.len().strict_add(1),
            "Excel core.xml 메모리 확보 실패",
        )?;
        core_xml.push_str(concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n",
            "<cp:coreProperties xmlns:cp=\"http://schemas.openxmlformats.org/package/2006/metadata/core-properties\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:dcmitype=\"http://purl.org/dc/dcmitype/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">",
        ));
        let mut core_values = [None; EXCEL_CORE_PROPERTIES.len()];
        let (mut core_scanner, core_root) =
            scan_xml_root(source_core_xml, "cp:coreProperties", "core.xml")?;
        while let Some(element) =
            core_scanner.next_direct_element_until(core_root.name, "core.xml")?
        {
            let Some((property, slot)) = EXCEL_CORE_PROPERTIES
                .iter()
                .zip(&mut core_values)
                .find(|item| element.opening.name == item.0.0)
            else {
                continue;
            };
            let qualified = property.0;
            if slot.is_some() {
                return Err(err(format!(
                    "core.xml에 {qualified} 요소가 여러 개 있습니다."
                )));
            }
            let body = (!element.body.contains('<'))
                .then_some(element.body)
                .ok_or_else(|| err(format!("core.xml의 {qualified} 본문이 올바르지 않습니다.")))?;
            decode_xml_entities(body).map_err(|source| {
                err_with_source(
                    format!("core.xml의 {qualified} 본문이 올바르지 않습니다."),
                    source,
                )
            })?;
            *slot = Some(body);
        }
        for ((qualified, opening, closing), body_value) in
            EXCEL_CORE_PROPERTIES.into_iter().zip(core_values)
        {
            let body = body_value
                .ok_or_else(|| err(format!("core.xml의 {qualified} 요소를 찾지 못했습니다.")))?;
            core_xml.extend([opening, body, closing]);
        }
        core_xml.push_str("</cp:coreProperties>");
        source_core.bytes = core_xml.into_bytes();
        let source_app = self.part_mut("docProps/app.xml");
        let source_app_xml = str::from_utf8(&source_app.bytes)
            .map_err(|source_error| err_with_source("app.xml UTF-8 해석 실패", source_error))?;
        let (mut app_scanner, app_root) = scan_xml_root(source_app_xml, "Properties", "app.xml")?;
        let mut total_time_body = None;
        while let Some(element) = app_scanner.next_direct_element_until(app_root.name, "app.xml")? {
            if element.opening.name == "TotalTime"
                && total_time_body.replace(element.body).is_some()
            {
                return Err(err("app.xml에 TotalTime 요소가 여러 개 있습니다."));
            }
        }
        let total_time = total_time_body
            .filter(|value| !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
            .ok_or_else(|| err("app.xml의 TotalTime 형식이 올바르지 않습니다."))?;
        let mut app_xml = try_string_with_capacity(
            960_usize.strict_add(total_time.len()),
            "Excel app.xml 메모리 확보 실패",
        )?;
        app_xml.push_str(concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n",
            "<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\" xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\"><Template></Template><TotalTime>",
        ));
        app_xml.extend([total_time, "</TotalTime><Pages>2</Pages><Words>0</Words><Characters>0</Characters><Application>Microsoft Excel</Application><DocSecurity>0</DocSecurity><Paragraphs>0</Paragraphs><ScaleCrop>false</ScaleCrop><HeadingPairs><vt:vector size=\"2\" baseType=\"variant\"><vt:variant><vt:lpstr>워크시트</vt:lpstr></vt:variant><vt:variant><vt:i4>2</vt:i4></vt:variant></vt:vector></HeadingPairs><TitlesOfParts><vt:vector size=\"2\" baseType=\"lpstr\"><vt:lpstr>유류비</vt:lpstr><vt:lpstr>변경내역</vt:lpstr></vt:vector></TitlesOfParts><LinksUpToDate>false</LinksUpToDate><CharactersWithSpaces>0</CharactersWithSpaces><SharedDoc>false</SharedDoc><HyperlinksChanged>false</HyperlinksChanged><AppVersion>16.0300</AppVersion></Properties>"]);
        source_app.bytes = app_xml.into_bytes();
        let mut source_parts = mem::take(&mut self.parts);
        let mut output_parts =
            try_vec_with_capacity(XLSX_PARTS.len(), "Excel package part 목록 메모리 확보 실패")?;
        for (name, role, _) in XLSX_PARTS {
            if role == PartRole::InputOnly {
                continue;
            }
            let bytes = match name {
                "[Content_Types].xml" => excel_static_xml(EXCEL_CONTENT_TYPES_XML),
                "_rels/.rels" => excel_static_xml(EXCEL_ROOT_RELS_XML),
                "xl/_rels/workbook.xml.rels" => excel_static_xml(EXCEL_WORKBOOK_RELS_XML),
                "xl/styles.xml" => excel_static_xml(EXCEL_STYLES_XML),
                "xl/theme/theme1.xml" => excel_static_xml(EXCEL_THEME_XML),
                "docProps/thumbnail.emf" => {
                    let words = BLANK_EXCEL_THUMBNAIL_DWORDS.map(u32::to_le_bytes);
                    let thumbnail = words.as_flattened();
                    let mut bytes =
                        try_vec_with_capacity(thumbnail.len(), "Excel thumbnail 메모리 확보 실패")?;
                    bytes.extend_from_slice(thumbnail);
                    bytes
                }
                _ => {
                    let index = source_parts
                        .iter()
                        .position(|part| part.name == name)
                        .unwrap_or_else(|| process::abort());
                    source_parts.swap_remove(index).bytes
                }
            };
            output_parts.push(PackagePart { bytes, name });
        }
        self.parts = output_parts;
        Ok(input_styles)
    }
    fn part(&self, name: &str) -> &PackagePart {
        self.parts
            .iter()
            .find(|part| part.name == name)
            .unwrap_or_else(|| process::abort())
    }
    fn part_mut(&mut self, name: &str) -> &mut PackagePart {
        self.parts
            .iter_mut()
            .find(|part| part.name == name)
            .unwrap_or_else(|| process::abort())
    }
    pub(super) fn put_text(&mut self, name: &str, content: String) {
        self.part_mut(name).bytes = content.into_bytes();
    }
    pub(super) fn save(self, target_xlsx: &Path, verification: SaveVerification) -> Result<()> {
        let parent = target_xlsx
            .parent()
            .filter(|path| !path.is_empty())
            .unwrap_or_else(|| Path::new("."));
        let target_file_name = crate::MASTER_PATH;
        let temp_archive_prefix = format!(".{target_file_name}.tmp_");
        #[cfg(target_os = "windows")]
        let backup_archive_prefix = format!(".{target_file_name}.backup_");
        let reserve_archive = |prefix: &str| -> io::Result<ReservedTempArchive> {
            const ATTEMPTS: u32 = 1024;
            let pid = process::id();
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(io::Error::other)?
                .as_nanos();
            for sequence in 0..ATTEMPTS {
                let path = parent.join(format!("{prefix}{pid}_{nanos}_{sequence}"));
                let mut options = fs::File::options();
                options.read(true).write(true).create_new(true);
                configure_no_follow(&mut options);
                #[cfg(target_os = "windows")]
                configure_replaceable_file(&mut options);
                #[cfg(any(target_os = "linux", target_os = "macos"))]
                options.mode(0o600);
                match options.open(&path) {
                    Ok(file) => {
                        let (_, identity) = validate_regular_file(&file)?;
                        return Ok(ReservedTempArchive {
                            file: Some(file),
                            identity,
                            path,
                            remove_on_drop: true,
                        });
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error),
                }
            }
            Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "임시 항목 이름 충돌이 반복되었습니다. 잠시 후 다시 시도하세요.",
            ))
        };
        let mut tmp_archive = reserve_archive(&temp_archive_prefix).map_err(|source| {
            err_with_source(
                path_context_message("임시 저장 파일 생성 실패", target_xlsx),
                source,
            )
        })?;
        #[cfg(target_os = "windows")]
        let mut backup_archive = reserve_archive(&backup_archive_prefix).map_err(|source| {
            err_with_source(
                path_context_message("교체 예약 파일 생성 실패", target_xlsx),
                source,
            )
        })?;
        let result = (|| -> Result<()> {
            cfg_select! {
                any(target_os = "linux", target_os = "macos") => {
                    tmp_archive.write_archive_from(&self.parts, self.source_permissions)?;
                }
                target_os = "windows" => {
                    tmp_archive.write_archive_from(&self.parts)?;
                }
            }
            match verification {
                SaveVerification::Skip => {}
                SaveVerification::Verify => {
                    tmp_archive.verify_saved_archive()?;
                }
            }
            TempArchivePromotion {
                #[cfg(target_os = "windows")]
                backup_archive: &mut backup_archive,
                expected_fingerprint: self.source_fingerprint,
                expected_identity: self.source_identity,
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
                match tmp_archive.remove_owned() {
                    Ok(()) => Err(source),
                    Err(error) => Err(err_with_source(
                        format!(
                            "xlsx 임시 저장 파일 삭제 실패: {} ({error})",
                            tmp_archive.path().display(),
                        ),
                        source,
                    )),
                }
            }
        }
    }
    pub(super) fn take_shared_strings_text(&mut self) -> Result<String> {
        let xml = self.take_text("xl/sharedStrings.xml")?;
        validate_spreadsheet_xml_document(&xml, "sst", "sharedStrings.xml", false)?;
        Ok(xml)
    }
    pub(super) fn take_text(&mut self, name: &str) -> Result<String> {
        let bytes = mem::take(&mut self.part_mut(name).bytes);
        String::from_utf8(bytes)
            .map_err(|source| err_with_source(format!("xlsx part UTF-8 해석 실패: {name}"), source))
    }
    fn take_workbook_dependencies(&mut self, workbook_xml: &str) -> Result<()> {
        let workbook_relationships = self.take_text("xl/_rels/workbook.xml.rels")?;
        let relationship_ids = validate_relationship_set(
            &workbook_relationships,
            "workbook.xml.rels",
            &WORKBOOK_RELATIONSHIPS,
            self,
        )?;
        if self.has_part(CALC_CHAIN_PATH) {
            let xml = self.take_text(CALC_CHAIN_PATH)?;
            let child_count = visit_direct_xml_children(
                &xml,
                "calcChain",
                SPREADSHEETML_NAMESPACE,
                "calcChain.xml",
                |local_name, _raw| {
                    if local_name != "c" {
                        return Err(err("calcChain.xml에 고정 스키마 외 요소가 있습니다."));
                    }
                    Ok(())
                },
            )?;
            if child_count == 0 {
                return Err(err("calcChain.xml에 formula cell이 없습니다."));
            }
        }
        let [_, change_log_rid, master_rid, _, _, _] = relationship_ids;
        let sheet_ids = [
            master_rid
                .as_deref()
                .ok_or_else(|| err("유류비 worksheet relationship Id가 없습니다."))?,
            change_log_rid
                .as_deref()
                .ok_or_else(|| err("변경내역 worksheet relationship Id가 없습니다."))?,
        ];
        let mut workbook_scanner = XmlScanner::new(workbook_xml);
        workbook_scanner
            .next_start_named("sheets")
            .ok_or_else(|| err("workbook.xml의 sheets 시작 태그를 찾지 못했습니다."))?;
        for ((expected_name, expected_sheet_id), expected_rid) in
            [(MASTER_SHEET_NAME, "1"), (CHANGE_LOG_SHEET_NAME, "2")]
                .into_iter()
                .zip(sheet_ids)
        {
            let sheet_tag = workbook_scanner
                .next_start_named("sheet")
                .filter(|tag| tag.name == "sheet" && tag.self_closing)
                .ok_or_else(|| err("workbook sheet 태그가 올바르지 않습니다."))?;
            let [name, sheet_id, state, rid] = parse_attrs(
                sheet_tag.raw,
                ["name", "sheetId", "state", "r:id"],
                "workbook.xml sheet",
            )?;
            if name.as_deref() != Some(expected_name)
                || sheet_id.as_deref() != Some(expected_sheet_id)
                || rid.as_deref() != Some(expected_rid)
                || state.as_deref().is_some_and(|value| value != "visible")
            {
                return Err(err("workbook.xml sheet 구성이 고정 스키마와 다릅니다."));
            }
        }
        if workbook_scanner.next_start_named("sheet").is_some() {
            return Err(err("workbook sheet 수가 고정 스키마의 2개보다 많습니다."));
        }
        Ok(())
    }
    pub(super) fn take_worksheet_text(&mut self, name: &str, sheet_name: &str) -> Result<String> {
        let drawing_rid = if name == super::MASTER_SHEET_PATH {
            let has_relationships = self.has_part("xl/worksheets/_rels/sheet1.xml.rels");
            let has_drawing = self.has_part("xl/drawings/drawing1.xml");
            if has_relationships != has_drawing {
                return Err(err(
                    "worksheet drawing 관계와 drawing part는 함께 존재해야 합니다.",
                ));
            }
            if has_relationships {
                let relationships_xml = self.take_text("xl/worksheets/_rels/sheet1.xml.rels")?;
                let mut relationships = validate_relationship_set(
                    &relationships_xml,
                    "sheet1.xml.rels",
                    &SHEET_RELATIONSHIPS,
                    self,
                )?;
                let rid = relationships
                    .first_mut()
                    .and_then(Option::take)
                    .ok_or_else(|| err("worksheet drawing relationship Id가 없습니다."))?
                    .into_owned();
                validate_empty_xml_root(
                    &self.take_text("xl/drawings/drawing1.xml")?,
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
                Some(rid)
            } else {
                None
            }
        } else {
            None
        };
        let mut xml = self.take_text(name)?;
        let mut drawing_scanner = XmlScanner::new(&xml);
        let drawing = drawing_scanner.next_start_named("drawing");
        match (drawing_rid, drawing) {
            (Some(expected_rid), Some(tag)) if tag.name == "drawing" && tag.self_closing => {
                validate_exact_attrs(
                    tag.raw,
                    &[("r:id", expected_rid.as_str())],
                    "sheet1.xml drawing",
                )?;
                let span = tag.start..tag.end.strict_add(1);
                drawing_scanner.skip_to(span.end);
                if drawing_scanner.next_start_named("drawing").is_some() {
                    return Err(err("sheet1.xml에 drawing 참조가 여러 개 있습니다."));
                }
                xml.replace_range(span, "");
            }
            (Some(_), None) => {
                return Err(err("sheet1.xml의 빈 drawing 참조를 찾지 못했습니다."));
            }
            (None | Some(_), Some(_)) => {
                return Err(err(
                    "worksheet에 대응하는 drawing part 없는 참조가 있습니다.",
                ));
            }
            (None, None) => {}
        }
        let context = format!("worksheet XML namespace 검증: {sheet_name}");
        validate_spreadsheet_xml_document(&xml, "worksheet", &context, false)?;
        Ok(xml)
    }
    fn text(&self, name: &str) -> Result<&str> {
        let part = self.part(name);
        str::from_utf8(&part.bytes)
            .map_err(|source| err_with_source(format!("xlsx part UTF-8 해석 실패: {name}"), source))
    }
    fn validate_content_types(&self) -> Result<()> {
        let content_types_xml = self.text("[Content_Types].xml")?;
        let mut seen_defaults = [false; CONTENT_TYPE_DEFAULTS.len()];
        let mut seen_overrides = [false; XLSX_PARTS.len()];
        visit_direct_xml_children(
            content_types_xml,
            "Types",
            CONTENT_TYPES_NAMESPACE,
            "[Content_Types].xml",
            |local_name, raw| {
                let (is_default, key_name) = match local_name {
                    "Default" => (true, "Extension"),
                    "Override" => (false, "PartName"),
                    _ => {
                        return Err(err(format!(
                            "[Content_Types].xml에 알 수 없는 {local_name} 항목이 있습니다."
                        )));
                    }
                };
                let [key_attr, content_type_attr] =
                    parse_attrs(raw, [key_name, "ContentType"], "[Content_Types].xml entry")?;
                let key = key_attr.ok_or_else(|| {
                    err(format!(
                        "[Content_Types].xml entry에 {key_name} 속성이 없습니다."
                    ))
                })?;
                let content_type = content_type_attr.ok_or_else(|| {
                    err("[Content_Types].xml entry에 ContentType 속성이 없습니다.")
                })?;
                let matching_entry = if is_default {
                    CONTENT_TYPE_DEFAULTS
                        .iter()
                        .zip(&mut seen_defaults)
                        .find_map(|(&(candidate, value, _), present)| {
                            (candidate == key && value == content_type).then_some(present)
                        })
                } else {
                    XLSX_PARTS.iter().zip(&mut seen_overrides).find_map(
                        |(&(candidate, _, value), present)| {
                            (key.strip_prefix('/') == Some(candidate)
                                && value == Some(content_type.as_ref()))
                            .then_some(present)
                        },
                    )
                };
                let Some(seen_entry) = matching_entry else {
                    return Err(err(format!(
                        "[Content_Types].xml에 지원하지 않는 {local_name} 항목이 있습니다: {key}"
                    )));
                };
                if !is_default
                    && !key
                        .strip_prefix('/')
                        .is_some_and(|name| self.has_part(name))
                {
                    return Err(err(format!(
                        "[Content_Types].xml Override 대상 part가 없습니다: {key}"
                    )));
                }
                if mem::replace(seen_entry, true) {
                    return Err(err(format!(
                        "[Content_Types].xml 항목이 중복되었습니다: {key}"
                    )));
                }
                Ok(())
            },
        )?;
        for (&(key, _, role), present) in CONTENT_TYPE_DEFAULTS.iter().zip(seen_defaults) {
            let required = role == PartRole::Required
                || role == PartRole::OptionalInput && self.has_part("docProps/thumbnail.emf");
            if required && !present {
                return Err(err(format!(
                    "[Content_Types].xml 필수 항목이 없습니다: {key}"
                )));
            }
        }
        for (&(part, _, content_type), present) in XLSX_PARTS.iter().zip(seen_overrides) {
            if content_type.is_some() && self.has_part(part) && !present {
                return Err(err(format!(
                    "[Content_Types].xml 필수 항목이 없습니다: /{part}"
                )));
            }
        }
        Ok(())
    }
}
fn style_entries<'text>(
    styles_xml: &'text str,
    group_name: &str,
    entry_name: &str,
) -> Result<Vec<&'text str>> {
    let mut scanner = XmlScanner::new(styles_xml);
    let element = scanner
        .next_element_named(group_name)?
        .filter(|element| element.opening.name == group_name && !element.opening.self_closing)
        .ok_or_else(|| {
            err(format!(
                "styles.xml의 {group_name} 시작 태그가 올바르지 않습니다."
            ))
        })?;
    let declared_count = required_xml_attr(element.opening.raw, "count", group_name)?
        .parse::<usize>()
        .map_err(|source| {
            err_with_source(format!("styles.xml {group_name} count 해석 실패"), source)
        })?;
    let mut entries =
        try_vec_with_capacity(declared_count, "styles.xml 항목 목록 메모리 확보 실패")?;
    let mut child_scanner = XmlScanner::new(element.body);
    while let Some(entry) = child_scanner.next_direct_element(group_name)? {
        if entry.opening.name != entry_name {
            return Err(err(format!(
                "styles.xml {group_name}에 알 수 없는 {} 요소가 있습니다.",
                entry.opening.name
            )));
        }
        entries.push(
            element
                .body
                .get(entry.span)
                .ok_or_else(|| err("styles.xml 항목 범위가 손상되었습니다."))?,
        );
    }
    if scanner.next_start_named(group_name).is_some() {
        return Err(err(format!(
            "styles.xml에 {group_name} 태그가 여러 개 있습니다."
        )));
    }
    if entries.len() != declared_count {
        return Err(err(format!(
            "styles.xml {group_name} count가 실제 {entry_name} 수와 다릅니다: declared={declared_count}, actual={}",
            entries.len()
        )));
    }
    Ok(entries)
}
fn find_equivalent_xf(
    source: &str,
    catalog: &[&str],
    fonts: Option<(&[&str], &[&str])>,
) -> Result<Option<usize>> {
    const BOOLEAN_ATTRS: [&str; 13] = [
        "applyAlignment",
        "applyBorder",
        "applyFill",
        "applyFont",
        "applyNumberFormat",
        "applyProtection",
        "hidden",
        "justifyLastLine",
        "locked",
        "pivotButton",
        "quotePrefix",
        "shrinkToFit",
        "wrapText",
    ];
    if let Some(index) = catalog.iter().position(|&candidate| candidate == source) {
        return Ok(Some(index));
    }
    for (index, candidate) in catalog.iter().enumerate() {
        let mut source_scanner = XmlScanner::new(source);
        let mut candidate_scanner = XmlScanner::new(candidate);
        let mut equivalent = true;
        loop {
            let (source_tag, candidate_tag) =
                match (source_scanner.next_tag(), candidate_scanner.next_tag()) {
                    (Some(source_entry), Some(candidate_entry)) => (source_entry, candidate_entry),
                    (None, None) => break,
                    (None | Some(_), None | Some(_)) => {
                        equivalent = false;
                        break;
                    }
                };
            if source_tag.is_start != candidate_tag.is_start
                || source_tag.name != candidate_tag.name
                || source_tag.self_closing != candidate_tag.self_closing
            {
                equivalent = false;
                break;
            }
            if !source_tag.is_start {
                continue;
            }
            let mut candidate_attr_count = 0_usize;
            let mut candidate_attrs = XmlAttrScanner::new(candidate_tag.raw)?;
            while let Some((name, candidate_value)) = candidate_attrs.next()? {
                candidate_attr_count = candidate_attr_count.strict_add(1);
                let mut source_match = None;
                let mut source_attrs = XmlAttrScanner::new(source_tag.raw)?;
                while let Some((source_name, source_value)) = source_attrs.next()? {
                    if source_name == name && source_match.replace(source_value).is_some() {
                        equivalent = false;
                        break;
                    }
                }
                let Some(source_value) = source_match else {
                    equivalent = false;
                    break;
                };
                let values_match = if name == "fontId" {
                    fonts.map_or_else(
                        || source_value == candidate_value,
                        |(source_fonts, candidate_fonts)| {
                            source_value
                                .parse::<usize>()
                                .ok()
                                .and_then(|font_index| source_fonts.get(font_index))
                                .zip(
                                    candidate_value
                                        .parse::<usize>()
                                        .ok()
                                        .and_then(|font_index| candidate_fonts.get(font_index)),
                                )
                                .is_some_and(|(source_font, candidate_font)| {
                                    source_font == candidate_font
                                })
                        },
                    )
                } else if BOOLEAN_ATTRS.contains(&name) {
                    matches!(
                        (source_value.as_ref(), candidate_value.as_ref()),
                        ("0" | "false", "0" | "false") | ("1" | "true", "1" | "true")
                    )
                } else {
                    source_value == candidate_value
                };
                if !values_match {
                    equivalent = false;
                    break;
                }
            }
            if !equivalent {
                break;
            }
            let mut source_attr_count = 0_usize;
            let mut source_attrs = XmlAttrScanner::new(source_tag.raw)?;
            while source_attrs.next()?.is_some() {
                source_attr_count = source_attr_count.strict_add(1);
            }
            if source_attr_count != candidate_attr_count {
                equivalent = false;
                break;
            }
        }
        if equivalent {
            return Ok(Some(index));
        }
    }
    Ok(None)
}
fn excel_static_xml(source: &str) -> Vec<u8> {
    source
        .strip_suffix('\n')
        .unwrap_or(source)
        .replacen('\n', "\r\n", 1)
        .into_bytes()
}
fn parse_attrs<'tag, const N: usize>(
    tag: &'tag str,
    names: [&str; N],
    context: &str,
) -> Result<[Option<Cow<'tag, str>>; N]> {
    let mut values = array::repeat(None);
    let mut attributes = XmlAttrScanner::new(tag)?;
    while let Some((name, value)) = attributes.next()? {
        let Some((_, slot)) = names
            .iter()
            .zip(&mut values)
            .find(|&(candidate, _)| *candidate == name)
        else {
            return Err(err(format!(
                "{context}에 알 수 없는 {name} 속성이 있습니다."
            )));
        };
        if slot.replace(value).is_some() {
            return Err(err(format!("{context}에 {name} 속성이 중복되었습니다.")));
        }
    }
    Ok(values)
}
fn validate_exact_attrs<const N: usize>(
    tag: &str,
    expected: &[(&str, &str); N],
    context: &str,
) -> Result<()> {
    let values = parse_attrs(tag, expected.map(|(name, _)| name), context)?;
    values
        .iter()
        .zip(expected)
        .all(|(value, expected_attr)| value.as_deref() == Some(expected_attr.1))
        .ok_or_else(|| err(format!("{context} 속성이 고정 스키마와 다릅니다.")))
}
fn validate_relationship_set<'xml, const N: usize>(
    xml: &'xml str,
    context: &str,
    expected: &[RelationshipSpec; N],
    container: &XlsxContainer,
) -> Result<[Option<Cow<'xml, str>>; N]> {
    let mut ids: [Option<Cow<'xml, str>>; N] = array::repeat(None);
    visit_direct_xml_children(
        xml,
        "Relationships",
        PACKAGE_RELATIONSHIPS_NAMESPACE,
        context,
        |local_name, raw| {
            if local_name != "Relationship" {
                return Err(err(format!("{context} 관계 태그가 올바르지 않습니다.")));
            }
            let [id_attr, type_attr, target_attr] =
                parse_attrs(raw, ["Id", "Type", "Target"], context)?;
            let id = id_attr.ok_or_else(|| err(format!("{context} 관계에 Id가 없습니다.")))?;
            if id.is_empty() {
                return Err(err(format!("{context} 관계 Id가 비어 있습니다.")));
            }
            let type_ =
                type_attr.ok_or_else(|| err(format!("{context} 관계에 Type이 없습니다.")))?;
            let target =
                target_attr.ok_or_else(|| err(format!("{context} 관계에 Target이 없습니다.")))?;
            if ids
                .iter()
                .flatten()
                .any(|candidate| candidate == id.as_ref())
            {
                return Err(err(format!("{context} 관계 Id가 중복되었습니다: {id}")));
            }
            let Some((candidate, slot)) = expected
                .iter()
                .zip(ids.iter_mut())
                .find(|item| type_ == item.0.type_ && target == item.0.target)
            else {
                return Err(err(format!(
                    "{context}에 지원하지 않는 관계가 있습니다: {type_} -> {target}"
                )));
            };
            if candidate
                .optional_part
                .is_some_and(|part| !container.has_part(part))
            {
                return Err(err(format!(
                    "{context} 관계 대상 part가 없습니다: {target}"
                )));
            }
            if slot.replace(id).is_some() {
                return Err(err(format!("{context} 관계가 중복되었습니다: {target}")));
            }
            Ok(())
        },
    )?;
    for (candidate, id) in expected.iter().zip(&ids) {
        let required = candidate
            .optional_part
            .is_none_or(|part| container.has_part(part));
        if required != id.is_some() {
            return Err(err(format!(
                "{context} 필수 관계 구성이 올바르지 않습니다: {}",
                candidate.target
            )));
        }
    }
    Ok(ids)
}
fn scan_xml_root<'xml>(
    xml: &'xml str,
    expected_name: &str,
    context: &str,
) -> Result<(XmlScanner<'xml>, XmlTag<'xml>)> {
    let mut scanner = XmlScanner::new(xml);
    let root = scanner
        .next_tag()
        .filter(|tag| tag.is_start && tag.name == expected_name)
        .ok_or_else(|| err(format!("{context}의 XML root 태그가 올바르지 않습니다.")))?;
    let leading = xml
        .get(..root.start)
        .ok_or_else(|| err(format!("{context}의 XML root 범위가 손상되었습니다.")))?;
    if !xml_misc_only(leading, true) {
        return Err(err(format!(
            "{context}의 XML root 앞 내용이 올바르지 않습니다."
        )));
    }
    Ok((scanner, root))
}
fn validate_spreadsheet_xml_document<'xml>(
    xml: &'xml str,
    expected_root: &str,
    context: &str,
    allow_extensions: bool,
) -> Result<XmlTag<'xml>> {
    let (mut scanner, root) = scan_xml_root(xml, expected_root, context)?;
    if root.self_closing {
        return Err(err(format!("{context}의 XML root 태그가 비어 있습니다.")));
    }
    if required_xml_attr(root.raw, "xmlns", context)?.as_ref() != SPREADSHEETML_NAMESPACE {
        return Err(err(format!(
            "{context}의 root namespace가 올바르지 않습니다."
        )));
    }
    let mut ancestors = [root.name; MAX_XML_NESTING_DEPTH];
    let mut depth = 1_usize;
    let mut sheet_data_seen = false;
    while let Some(tag) = scanner.next_tag() {
        if tag.is_start {
            if !allow_extensions && tag.name != tag.local_name {
                return Err(err(format!(
                    "{context}의 prefixed core element는 지원하지 않습니다: {}",
                    tag.name
                )));
            }
            let mut attributes = XmlAttrScanner::new(tag.raw)?;
            while let Some((name, value)) = attributes.next()? {
                if name == "xmlns" || !allow_extensions && name.starts_with("xmlns:") {
                    if tag.name == "sortState"
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
            if expected_root == "worksheet" && tag.local_name == "sheetData" {
                if depth != 1 || sheet_data_seen {
                    return Err(err(format!(
                        "{context}의 sheetData 위치 또는 개수가 올바르지 않습니다."
                    )));
                }
                sheet_data_seen = true;
            }
            if !tag.self_closing {
                let slot = ancestors
                    .get_mut(depth)
                    .ok_or_else(|| err(format!("{context}의 XML 중첩 깊이가 너무 큽니다.")))?;
                *slot = tag.name;
                depth = depth.strict_add(1);
            }
            continue;
        }
        depth = depth
            .checked_sub(1)
            .ok_or_else(|| err(format!("{context}의 종료 태그 순서가 올바르지 않습니다.")))?;
        let open = ancestors
            .get(depth)
            .copied()
            .ok_or_else(|| err(format!("{context}의 XML 중첩 깊이가 손상되었습니다.")))?;
        if open != tag.name {
            return Err(err(format!(
                "{context}의 XML 태그 쌍이 일치하지 않습니다: {open} / {}",
                tag.name
            )));
        }
        if depth == 0 {
            if expected_root == "worksheet" && !sheet_data_seen {
                return Err(err(format!("{context}에 sheetData가 없습니다.")));
            }
            let document_end = tag.end.strict_add(1);
            if scanner.next_tag().is_some()
                || !xml
                    .get(document_end..)
                    .is_some_and(|trailing| xml_misc_only(trailing, false))
            {
                return Err(err(format!(
                    "{context}의 XML root 뒤 내용이 올바르지 않습니다."
                )));
            }
            return Ok(root);
        }
    }
    Err(err(format!("{context}에 닫히지 않은 XML 요소가 있습니다.")))
}
fn validate_empty_xml_root<const N: usize>(
    xml: &str,
    expected_name: &str,
    expected_attrs: &[(&str, &str); N],
    context: &str,
) -> Result<()> {
    let (mut scanner, root) = scan_xml_root(xml, expected_name, context)?;
    validate_exact_attrs(root.raw, expected_attrs, context)?;
    let empty = if root.self_closing {
        let trailing = xml
            .get(root.end.strict_add(1)..)
            .ok_or_else(|| err(format!("{context}의 XML root 범위가 손상되었습니다.")))?;
        xml_misc_only(trailing, false)
    } else {
        scanner
            .next_direct_element_until(root.name, context)?
            .is_none()
    };
    empty.ok_or_else(|| err(format!("{context}의 XML root가 비어 있지 않습니다.")))
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
fn visit_direct_xml_children<'xml>(
    xml: &'xml str,
    root_local_name: &str,
    expected_namespace: &str,
    context: &str,
    mut visit: impl FnMut(&'xml str, &'xml str) -> Result<()>,
) -> Result<usize> {
    let (mut scanner, root) = scan_xml_root(xml, root_local_name, context)?;
    if root.self_closing {
        return Err(err(format!("{context}의 XML root 태그가 비어 있습니다.")));
    }
    validate_exact_attrs(
        root.raw,
        &[("xmlns", expected_namespace)],
        &format!("{context} root"),
    )?;
    let mut child_count = 0_usize;
    while let Some(child) = scanner.next_direct_element_until(root.name, context)? {
        let tag = child.opening;
        if tag.name != tag.local_name {
            return Err(err(format!(
                "{context}의 prefixed child element는 지원하지 않습니다: {}",
                tag.name
            )));
        }
        if !xml_misc_only(child.body, false) {
            return Err(err(format!(
                "{context}의 XML child 태그는 중첩되거나 text를 포함할 수 없습니다."
            )));
        }
        visit(tag.local_name, tag.raw)?;
        child_count = child_count.strict_add(1);
    }
    Ok(child_count)
}
