use super::{
    ArchiveFingerprint, CALC_CHAIN_PATH, CHANGE_LOG_SHEET_NAME, CanonicalStyleMap,
    MASTER_SHEET_NAME, PackagePart, SPREADSHEETML_NAMESPACE, SaveVerification, XLSX_PARTS,
    XlsxPartRole, ZipArchiveBuilder, ZipPackageReader,
    xml::{XmlAttrScanner, XmlScanner, XmlTag, decode_xml_entities},
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
use core::{array, convert::identity, mem, str};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::fs::OpenOptionsExt as _;
use std::{
    fs,
    io::{self, Seek as _, SeekFrom, Write as _, stderr},
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};
mod atomic_replace;
const MAX_XML_NESTING_DEPTH: usize = 64;
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
const INPUT_ROOT_RELATIONSHIPS: [(&str, &str, Option<&str>); 5] = [
    (OFFICE_DOCUMENT_REL_TYPE, WORKBOOK_REL_TARGET, None),
    (
        "http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties",
        "docProps/core.xml",
        None,
    ),
    (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties",
        "docProps/app.xml",
        None,
    ),
    (
        "http://schemas.openxmlformats.org/package/2006/relationships/metadata/thumbnail",
        "docProps/thumbnail.emf",
        Some("docProps/thumbnail.emf"),
    ),
    (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/custom-properties",
        "docProps/custom.xml",
        Some("docProps/custom.xml"),
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
const INPUT_WORKBOOK_RELATIONSHIPS: [(&str, &str, Option<&str>); 6] = [
    (WORKSHEET_REL_TYPE, "worksheets/sheet1.xml", None),
    (WORKSHEET_REL_TYPE, "worksheets/sheet2.xml", None),
    (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme",
        "theme/theme1.xml",
        None,
    ),
    (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles",
        "styles.xml",
        None,
    ),
    (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings",
        "sharedStrings.xml",
        None,
    ),
    (
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/calcChain",
        "calcChain.xml",
        Some(CALC_CHAIN_PATH),
    ),
];
const ADDITIONAL_INPUT_CONTENT_TYPE_DEFAULTS: [(&str, &str); 3] = [
    ("fntdata", "application/x-fontdata"),
    ("jpeg", "image/jpeg"),
    ("png", "image/png"),
];
const ADDITIONAL_INPUT_CONTENT_TYPE_OVERRIDES: [(&str, &str); 2] = [
    (
        "/docProps/custom.xml",
        "application/vnd.openxmlformats-officedocument.custom-properties+xml",
    ),
    (
        "/xl/drawings/drawing1.xml",
        "application/vnd.openxmlformats-officedocument.drawing+xml",
    ),
];
const INPUT_SHEET_RELATIONSHIPS: [(&str, &str, Option<&str>); 1] = [(
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/drawing",
    "../drawings/drawing1.xml",
    None,
)];
const EXCEL_STYLES_XML: &str = include_str!("excel_styles.xml");
const EXCEL_THEME_XML: &str = include_str!("excel_theme.xml");
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
pub(crate) struct XlsxContainer {
    drawing_rid: Option<String>,
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
                fs::remove_file(&self.path)?;
                Ok(())
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
        if file.identity != self.identity {
            return Err(err(format!(
                "xlsx 임시 파일 identity가 실행 중 변경되었습니다: {}",
                self.path.display()
            )));
        }
        Ok(())
    }
    fn verify_saved_archive(&self) -> Result<()> {
        let saved_archive = self.path();
        let mut saved_handle = self
            .file
            .as_ref()
            .ok_or_else(|| err("저장 검증용 xlsx handle이 닫혀 있습니다."))?
            .try_clone()
            .map_err(|source| {
                err_with_source(
                    path_context_message("저장 검증용 xlsx handle 복제 실패", saved_archive),
                    source,
                )
            })?;
        saved_handle.seek(SeekFrom::Start(0)).map_err(|source| {
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
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| err("xlsx 임시 저장 파일 handle이 닫혀 있습니다."))?;
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
            })?;
            Ok(())
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
                .filter(|path| !path.as_os_str().is_empty())
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
        replace_single_self_closing_tag(
            workbook_xml,
            "loext:extCalcPr",
            WORKBOOK_LOEXT_VALUE_TAG,
            |tag| {
                validate_exact_attrs(
                    tag,
                    &[("stringRefSyntax", "CalcA1ExcelA1")],
                    "workbook loext:extCalcPr",
                )
            },
        )?;
        let mut namespace_scanner = XmlScanner::new(workbook_xml);
        let root = namespace_scanner
            .next_tag()
            .ok_or_else(|| err("workbook.xml에 root 태그가 없습니다."))?;
        if !root.is_start || root.name != "workbook" || root.self_closing {
            return Err(err("workbook.xml의 root 형식이 올바르지 않습니다."));
        }
        if required_xml_attr(root.raw, "xmlns", "workbook.xml")?.as_ref() != SPREADSHEETML_NAMESPACE
        {
            return Err(err("workbook.xml의 root namespace가 올바르지 않습니다."));
        }
        if required_xml_attr(root.raw, "xmlns:r", "workbook.xml")?.as_ref()
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
        let calc_chain_xml = self.take_workbook_dependencies(workbook_xml)?;
        replace_single_self_closing_tag(
            workbook_xml,
            "fileVersion",
            "<fileVersion appName=\"xl\" lastEdited=\"7\" lowestEdited=\"7\" rupBuild=\"27932\"/>",
            |tag| {
                let [app_name, last_edited, lowest_edited, build] = parse_attrs(
                    tag,
                    ["appName", "lastEdited", "lowestEdited", "rupBuild"],
                    "workbook fileVersion",
                )?;
                if app_name.as_deref().is_none_or(str::is_empty)
                    || [last_edited, lowest_edited, build]
                        .into_iter()
                        .flatten()
                        .any(|value| {
                            value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit())
                        })
                {
                    return Err(err("workbook fileVersion 속성이 올바르지 않습니다."));
                }
                Ok(())
            },
        )?;
        replace_single_self_closing_tag(workbook_xml, "workbookPr", "<workbookPr/>", |tag| {
            let [backup, objects, date_system] =
                parse_attrs(tag, ["backupFile", "showObjects", "date1904"], "workbookPr")?;
            if backup
                .as_deref()
                .is_some_and(|value| !matches!(value, "0" | "false"))
                || objects.as_deref().is_some_and(|value| value != "all")
                || date_system
                    .as_deref()
                    .is_some_and(|value| !matches!(value, "0" | "false"))
            {
                return Err(err("workbookPr 속성이 지원하는 의미와 다릅니다."));
            }
            Ok(())
        })?;
        let mut defined_name_scanner = XmlScanner::new(workbook_xml);
        let defined_name = defined_name_scanner
            .next_start_named("definedName")
            .filter(|tag| tag.name == "definedName" && !tag.self_closing)
            .ok_or_else(|| err("workbook의 _FilterDatabase 태그가 올바르지 않습니다."))?;
        let defined_name_span = defined_name.start
            ..defined_name
                .end
                .checked_add(1)
                .ok_or_else(|| err("_FilterDatabase 태그 끝 계산 실패"))?;
        let [function, hidden, sheet_id, name, procedure] = parse_attrs(
            defined_name.raw,
            ["function", "hidden", "localSheetId", "name", "vbProcedure"],
            "_FilterDatabase",
        )?;
        if name.as_deref() != Some("_xlnm._FilterDatabase")
            || sheet_id.as_deref() != Some("0")
            || !hidden
                .as_deref()
                .is_some_and(|value| matches!(value, "1" | "true"))
            || function
                .as_deref()
                .is_some_and(|value| !matches!(value, "0" | "false"))
            || procedure
                .as_deref()
                .is_some_and(|value| !matches!(value, "0" | "false"))
        {
            return Err(err("_FilterDatabase 속성 구성이 올바르지 않습니다."));
        }
        defined_name_scanner.skip_to(defined_name_span.end);
        if defined_name_scanner
            .next_start_named("definedName")
            .is_some()
        {
            return Err(err("workbook에 definedName이 여러 개 있습니다."));
        }
        workbook_xml.replace_range(
            defined_name_span,
            "<definedName name=\"_xlnm._FilterDatabase\" localSheetId=\"0\" hidden=\"1\">",
        );
        let mut sheet_scanner = XmlScanner::new(workbook_xml);
        let mut sheet_spans = [0..0, 0..0];
        for span in &mut sheet_spans {
            let tag = sheet_scanner
                .next_start_named("sheet")
                .filter(|tag| tag.name == "sheet" && tag.self_closing)
                .ok_or_else(|| err("workbook sheet 태그가 올바르지 않습니다."))?;
            *span = tag.start
                ..tag
                    .end
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
        Ok(calc_chain_xml)
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
            drawing_rid: None,
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
            &INPUT_ROOT_RELATIONSHIPS,
            &container,
        )?;
        container.prepare_companion_parts()?;
        container.part_mut("[Content_Types].xml")?.bytes = Vec::new();
        container.part_mut("_rels/.rels")?.bytes = Vec::new();
        Ok(container)
    }
    fn has_part(&self, name: &str) -> bool {
        self.parts.iter().any(|part| part.name == name)
    }
    pub(super) fn package_prepare_excel_output(&mut self) -> Result<CanonicalStyleMap> {
        let source_xfs = cell_xf_entries(self.text("xl/styles.xml")?)?;
        let excel_xfs = cell_xf_entries(EXCEL_STYLES_XML)?;
        let libreoffice_xfs = cell_xf_entries(LIBREOFFICE_CELL_XFS_XML)?;
        if libreoffice_xfs.len() != LIBREOFFICE_STYLE_MAP.len() {
            return Err(err(
                "내장 LibreOffice style mapping 수가 올바르지 않습니다.",
            ));
        }
        let mut entries =
            try_vec_with_capacity(source_xfs.len(), "입력 style mapping 메모리 확보 실패")?;
        for source_xf in source_xfs {
            let canonical = match find_equivalent_xf(source_xf, &excel_xfs)? {
                Some(index) => Some(
                    u32::try_from(index)
                        .map_err(|error| err_with_source("Excel style index 변환 실패", error))?,
                ),
                None => find_equivalent_xf(source_xf, &libreoffice_xfs)?
                    .and_then(|index| LIBREOFFICE_STYLE_MAP.get(index))
                    .copied(),
            };
            entries.push(canonical);
        }
        let input_styles = CanonicalStyleMap { entries };
        let mut source_parts = mem::take(&mut self.parts);
        let source_core = source_parts
            .iter_mut()
            .find(|part| part.name == "docProps/core.xml")
            .ok_or_else(|| err("Excel core.xml 원본 part를 찾지 못했습니다."))?;
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
        let mut core_scanner = XmlScanner::new(source_core_xml);
        while let Some(tag) = core_scanner.next_tag() {
            if !tag.is_start {
                continue;
            }
            let Some((property, slot)) = EXCEL_CORE_PROPERTIES
                .iter()
                .zip(&mut core_values)
                .find(|item| tag.name == item.0.0)
            else {
                continue;
            };
            let qualified = property.0;
            if slot.is_some() {
                return Err(err(format!(
                    "core.xml에 {qualified} 요소가 여러 개 있습니다."
                )));
            }
            let body = if tag.self_closing {
                ""
            } else {
                let body_start = tag
                    .end
                    .checked_add(1)
                    .ok_or_else(|| err("core.xml 요소 본문 시작 계산 실패"))?;
                let closing = core_scanner
                    .next_tag()
                    .filter(|closing| !closing.is_start && closing.name == qualified)
                    .ok_or_else(|| err(format!("core.xml의 {qualified} 종료 태그가 없습니다.")))?;
                source_core_xml
                    .get(body_start..closing.start)
                    .filter(|value| !value.contains('<'))
                    .ok_or_else(|| {
                        err(format!("core.xml의 {qualified} 본문이 올바르지 않습니다."))
                    })?
            };
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
            core_xml.push_str(opening);
            core_xml.push_str(body);
            core_xml.push_str(closing);
        }
        core_xml.push_str("</cp:coreProperties>");
        source_core.bytes = core_xml.into_bytes();
        let source_app = source_parts
            .iter_mut()
            .find(|part| part.name == "docProps/app.xml")
            .ok_or_else(|| err("Excel app.xml 원본 part를 찾지 못했습니다."))?;
        let source_app_xml = str::from_utf8(&source_app.bytes)
            .map_err(|source_error| err_with_source("app.xml UTF-8 해석 실패", source_error))?;
        let (_, total_time_tail) = source_app_xml
            .split_once("<TotalTime>")
            .ok_or_else(|| err("app.xml의 TotalTime 태그를 찾지 못했습니다."))?;
        let (total_time, after_total_time) = total_time_tail
            .split_once("</TotalTime>")
            .ok_or_else(|| err("app.xml의 TotalTime 종료 태그를 찾지 못했습니다."))?;
        if after_total_time.contains("<TotalTime>")
            || total_time.is_empty()
            || !total_time.bytes().all(|byte| byte.is_ascii_digit())
        {
            return Err(err("app.xml의 TotalTime 형식이 올바르지 않습니다."));
        }
        let mut app_xml = try_string_with_capacity(
            960_usize.strict_add(total_time.len()),
            "Excel app.xml 메모리 확보 실패",
        )?;
        app_xml.push_str(concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n",
            "<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\" xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\"><Template></Template><TotalTime>",
        ));
        app_xml.push_str(total_time);
        app_xml.push_str("</TotalTime><Pages>2</Pages><Words>0</Words><Characters>0</Characters><Application>Microsoft Excel</Application><DocSecurity>0</DocSecurity><Paragraphs>0</Paragraphs><ScaleCrop>false</ScaleCrop><HeadingPairs><vt:vector size=\"2\" baseType=\"variant\"><vt:variant><vt:lpstr>워크시트</vt:lpstr></vt:variant><vt:variant><vt:i4>2</vt:i4></vt:variant></vt:vector></HeadingPairs><TitlesOfParts><vt:vector size=\"2\" baseType=\"lpstr\"><vt:lpstr>유류비</vt:lpstr><vt:lpstr>변경내역</vt:lpstr></vt:vector></TitlesOfParts><LinksUpToDate>false</LinksUpToDate><CharactersWithSpaces>0</CharactersWithSpaces><SharedDoc>false</SharedDoc><HyperlinksChanged>false</HyperlinksChanged><AppVersion>16.0300</AppVersion></Properties>");
        source_app.bytes = app_xml.into_bytes();
        let mut output_parts =
            try_vec_with_capacity(XLSX_PARTS.len(), "Excel package part 목록 메모리 확보 실패")?;
        for (name, role) in XLSX_PARTS {
            if role == XlsxPartRole::InputOnly {
                continue;
            }
            let bytes = match name {
                "[Content_Types].xml" => {
                    let mut xml = excel_catalog_xml(
                        "Types",
                        CONTENT_TYPES_NAMESPACE,
                        "Excel content types 메모리 확보 실패",
                    )?;
                    for (extension, content_type) in EXCEL_CONTENT_TYPE_DEFAULTS {
                        push_empty_xml_element(
                            &mut xml,
                            "Default",
                            [("Extension", extension), ("ContentType", content_type)],
                        );
                    }
                    for (part_name, content_type) in EXCEL_CONTENT_TYPE_OVERRIDES {
                        push_empty_xml_element(
                            &mut xml,
                            "Override",
                            [("PartName", part_name), ("ContentType", content_type)],
                        );
                    }
                    xml.extend_from_slice(b"</Types>");
                    xml
                }
                "_rels/.rels" => excel_relationships_xml(&EXCEL_ROOT_RELATIONSHIPS)?,
                "xl/_rels/workbook.xml.rels" => {
                    excel_relationships_xml(&EXCEL_WORKBOOK_RELATIONSHIPS)?
                }
                "xl/styles.xml" => excel_static_xml(EXCEL_STYLES_XML),
                "xl/theme/theme1.xml" => excel_static_xml(EXCEL_THEME_XML),
                "docProps/thumbnail.emf" => {
                    let thumbnail_len = BLANK_EXCEL_THUMBNAIL_DWORDS
                        .len()
                        .checked_mul(size_of::<u32>())
                        .ok_or_else(|| err("Excel thumbnail 크기 계산 실패"))?;
                    let mut bytes =
                        try_vec_with_capacity(thumbnail_len, "Excel thumbnail 메모리 확보 실패")?;
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
                                "입력에서 Excel 공통 part를 찾지 못했습니다: {name}"
                            ))
                        })?;
                    mem::take(&mut part.bytes)
                }
            };
            output_parts.push(PackagePart { bytes, name });
        }
        self.parts = output_parts;
        Ok(input_styles)
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
    fn prepare_companion_parts(&mut self) -> Result<()> {
        let has_sheet_relationships = self.has_part("xl/worksheets/_rels/sheet1.xml.rels");
        let has_drawing = self.has_part("xl/drawings/drawing1.xml");
        if has_sheet_relationships != has_drawing {
            return Err(err(
                "worksheet drawing 관계와 drawing part는 함께 존재해야 합니다.",
            ));
        }
        if has_sheet_relationships {
            let mut relationships = validate_relationship_set(
                self.text("xl/worksheets/_rels/sheet1.xml.rels")?,
                "sheet1.xml.rels",
                &INPUT_SHEET_RELATIONSHIPS,
                self,
            )?;
            self.drawing_rid = Some(
                relationships
                    .get_mut(0)
                    .and_then(Option::take)
                    .ok_or_else(|| err("worksheet drawing relationship Id가 없습니다."))?
                    .into_owned(),
            );
        }
        if self.has_part("docProps/custom.xml") {
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
        }
        if has_drawing {
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
        }
        for name in [
            "docProps/custom.xml",
            "xl/worksheets/_rels/sheet1.xml.rels",
            "xl/drawings/drawing1.xml",
        ] {
            if self.has_part(name) {
                self.part_mut(name)?.bytes = Vec::new();
            }
        }
        Ok(())
    }
    pub(super) fn put_text(&mut self, name: &str, content: String) -> Result<()> {
        let part = self.part_mut(name)?;
        part.bytes = content.into_bytes();
        Ok(())
    }
    pub(super) fn save(self, target_xlsx: &Path, verification: SaveVerification) -> Result<()> {
        let parent = target_xlsx
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
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
        validate_spreadsheet_xml_document(&xml, "sst", "sharedStrings.xml")?;
        Ok(xml)
    }
    pub(super) fn take_text(&mut self, name: &str) -> Result<String> {
        let bytes = mem::take(&mut self.part_mut(name)?.bytes);
        String::from_utf8(bytes)
            .map_err(|source| err_with_source(format!("xlsx part UTF-8 해석 실패: {name}"), source))
    }
    fn take_workbook_dependencies(&mut self, workbook_xml: &str) -> Result<Option<String>> {
        let workbook_relationships = self.take_text("xl/_rels/workbook.xml.rels")?;
        let relationship_ids = validate_relationship_set(
            &workbook_relationships,
            "workbook.xml.rels",
            &INPUT_WORKBOOK_RELATIONSHIPS,
            self,
        )?;
        let calc_chain_xml = if self.has_part(CALC_CHAIN_PATH) {
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
            Some(xml)
        } else {
            None
        };
        let [master_rid, change_log_rid, _, _, _, _] = relationship_ids;
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
                .ok_or_else(|| err("workbook sheet 수가 고정 스키마의 2개보다 적습니다."))?;
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
        Ok(calc_chain_xml)
    }
    pub(super) fn take_worksheet_text(&mut self, name: &str, sheet_name: &str) -> Result<String> {
        let drawing_rid = if name == super::MASTER_SHEET_PATH {
            self.drawing_rid.take()
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
                let span = tag.start
                    ..tag
                        .end
                        .checked_add(1)
                        .ok_or_else(|| err("drawing 참조 끝 계산 실패"))?;
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
        validate_spreadsheet_xml_document(&xml, "worksheet", &context)?;
        Ok(xml)
    }
    fn text(&self, name: &str) -> Result<&str> {
        let part = self.part(name)?;
        str::from_utf8(&part.bytes)
            .map_err(|source| err_with_source(format!("xlsx part UTF-8 해석 실패: {name}"), source))
    }
    fn validate_content_types(&self) -> Result<()> {
        let content_types_xml = self.text("[Content_Types].xml")?;
        let mut seen_defaults = [false;
            EXCEL_CONTENT_TYPE_DEFAULTS
                .len()
                .strict_add(ADDITIONAL_INPUT_CONTENT_TYPE_DEFAULTS.len())];
        let mut seen_overrides = [false;
            EXCEL_CONTENT_TYPE_OVERRIDES
                .len()
                .strict_add(ADDITIONAL_INPUT_CONTENT_TYPE_OVERRIDES.len())];
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
                    EXCEL_CONTENT_TYPE_DEFAULTS
                        .iter()
                        .chain(&ADDITIONAL_INPUT_CONTENT_TYPE_DEFAULTS)
                        .zip(&mut seen_defaults)
                        .find_map(|(&(candidate, value), present)| {
                            (candidate == key && value == content_type).then_some(present)
                        })
                } else {
                    EXCEL_CONTENT_TYPE_OVERRIDES
                        .iter()
                        .chain(&ADDITIONAL_INPUT_CONTENT_TYPE_OVERRIDES)
                        .zip(&mut seen_overrides)
                        .find_map(|(&(candidate, value), present)| {
                            (candidate == key && value == content_type).then_some(present)
                        })
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
        for (&(key, _), present) in EXCEL_CONTENT_TYPE_DEFAULTS
            .iter()
            .chain(&ADDITIONAL_INPUT_CONTENT_TYPE_DEFAULTS)
            .zip(seen_defaults)
        {
            let required = matches!(key, "rels" | "xml")
                || key == "emf" && self.has_part("docProps/thumbnail.emf");
            if required && !present {
                return Err(err(format!(
                    "[Content_Types].xml 필수 항목이 없습니다: {key}"
                )));
            }
        }
        for (&(key, _), present) in EXCEL_CONTENT_TYPE_OVERRIDES
            .iter()
            .chain(&ADDITIONAL_INPUT_CONTENT_TYPE_OVERRIDES)
            .zip(seen_overrides)
        {
            let part = key.trim_start_matches('/');
            if self.has_part(part) && !present {
                return Err(err(format!(
                    "[Content_Types].xml 필수 항목이 없습니다: {key}"
                )));
            }
        }
        Ok(())
    }
}
fn cell_xf_entries(styles_xml: &str) -> Result<Vec<&str>> {
    let mut scanner = XmlScanner::new(styles_xml);
    let element = scanner
        .next_element_named("cellXfs")?
        .filter(|element| element.opening.name == "cellXfs" && !element.opening.self_closing)
        .ok_or_else(|| err("styles.xml의 cellXfs 시작 태그가 올바르지 않습니다."))?;
    let opening = element.opening;
    let declared_count = required_xml_attr(opening.raw, "count", "styles.xml cellXfs")?
        .parse::<usize>()
        .map_err(|source| err_with_source("styles.xml cellXfs count 해석 실패", source))?;
    let body_start = element.body_span.start;
    let closing_start = element.body_span.end;
    if styles_xml.get(closing_start..element.span.end) != Some("</cellXfs>") {
        return Err(err(
            "styles.xml의 cellXfs 종료 태그는 unprefixed여야 합니다.",
        ));
    }
    scanner.skip_to(body_start);
    let mut entries =
        try_vec_with_capacity(declared_count, "styles.xml cellXfs 목록 메모리 확보 실패")?;
    let mut stack = try_vec_with_capacity(3, "styles.xml cellXfs stack 메모리 확보 실패")?;
    let mut entry_start = None;
    let mut consumed = body_start;
    while let Some(tag) = scanner.next_tag() {
        if tag.start >= closing_start {
            break;
        }
        if !styles_xml
            .get(consumed..tag.start)
            .is_some_and(|between| between.trim().is_empty())
        {
            return Err(err("styles.xml cellXfs에 알 수 없는 text가 있습니다."));
        }
        if tag.is_start {
            if stack.is_empty() {
                if tag.name != "xf" {
                    return Err(err(format!(
                        "styles.xml cellXfs에 알 수 없는 {} 요소가 있습니다.",
                        tag.name
                    )));
                }
                entry_start = Some(tag.start);
            }
            consumed = tag
                .end
                .checked_add(1)
                .ok_or_else(|| err("styles.xml cellXfs 요소 끝 계산 실패"))?;
            if !tag.self_closing {
                stack.push(tag.name);
                continue;
            }
        } else {
            let open = stack
                .pop()
                .ok_or_else(|| err("styles.xml cellXfs 종료 태그가 중복되었습니다."))?;
            if open != tag.name {
                return Err(err(format!(
                    "styles.xml cellXfs 태그 쌍이 일치하지 않습니다: {open} / {}",
                    tag.name
                )));
            }
            consumed = tag
                .end
                .checked_add(1)
                .ok_or_else(|| err("styles.xml cellXfs 종료 범위 계산 실패"))?;
        }
        if !stack.is_empty() {
            continue;
        }
        entries.push(
            styles_xml
                .get(
                    entry_start
                        .take()
                        .ok_or_else(|| err("cellXf 시작 위치가 없습니다."))?
                        ..consumed,
                )
                .ok_or_else(|| err("cellXf 범위가 손상되었습니다."))?,
        );
    }
    if !stack.is_empty()
        || entry_start.is_some()
        || !styles_xml
            .get(consumed..closing_start)
            .is_some_and(|trailing| trailing.trim().is_empty())
    {
        return Err(err("styles.xml cellXfs 요소 구조가 올바르지 않습니다."));
    }
    scanner.skip_to(element.span.end);
    if scanner.next_start_named("cellXfs").is_some() {
        return Err(err("styles.xml에 cellXfs 태그가 여러 개 있습니다."));
    }
    if entries.len() != declared_count {
        return Err(err(format!(
            "styles.xml cellXfs count가 실제 xf 수와 다릅니다: declared={declared_count}, actual={}",
            entries.len()
        )));
    }
    Ok(entries)
}
fn find_equivalent_xf(source: &str, catalog: &[&str]) -> Result<Option<usize>> {
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
                let values_match = if BOOLEAN_ATTRS.contains(&name) {
                    let source_bool = match source_value.as_ref() {
                        "0" | "false" => Some(false),
                        "1" | "true" => Some(true),
                        _ => None,
                    };
                    let candidate_bool = match candidate_value.as_ref() {
                        "0" | "false" => Some(false),
                        "1" | "true" => Some(true),
                        _ => None,
                    };
                    source_bool.is_some() && source_bool == candidate_bool
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
fn excel_catalog_xml(
    root: &str,
    namespace: &str,
    allocation_error: &'static str,
) -> Result<Vec<u8>> {
    let mut xml = try_vec_with_capacity(4 * 1024, allocation_error)?;
    xml.extend_from_slice(b"<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n<");
    xml.extend_from_slice(root.as_bytes());
    xml.extend_from_slice(b" xmlns=\"");
    xml.extend_from_slice(namespace.as_bytes());
    xml.extend_from_slice(b"\">");
    Ok(xml)
}
fn push_empty_xml_element<const N: usize>(xml: &mut Vec<u8>, name: &str, attrs: [(&str, &str); N]) {
    xml.push(b'<');
    xml.extend_from_slice(name.as_bytes());
    for (attr_name, value) in attrs {
        xml.push(b' ');
        xml.extend_from_slice(attr_name.as_bytes());
        xml.extend_from_slice(b"=\"");
        xml.extend_from_slice(value.as_bytes());
        xml.push(b'"');
    }
    xml.extend_from_slice(b"/>");
}
fn excel_relationships_xml(relationships: &[(&str, &str, &str)]) -> Result<Vec<u8>> {
    let mut xml = excel_catalog_xml(
        "Relationships",
        PACKAGE_RELATIONSHIPS_NAMESPACE,
        "Excel relationship 메모리 확보 실패",
    )?;
    for &(id, type_, target) in relationships {
        push_empty_xml_element(
            &mut xml,
            "Relationship",
            [("Id", id), ("Type", type_), ("Target", target)],
        );
    }
    xml.extend_from_slice(b"</Relationships>");
    Ok(xml)
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
fn parse_attrs<'tag, const N: usize>(
    tag: &'tag str,
    names: [&str; N],
    context: &str,
) -> Result<[Option<Cow<'tag, str>>; N]> {
    let mut values = array::from_fn(|_| None);
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
    let mut seen = [false; N];
    let mut attributes = XmlAttrScanner::new(tag)?;
    while let Some((name, value)) = attributes.next()? {
        let mut matched = false;
        for (expected_attr, was_seen) in expected.iter().zip(&mut seen) {
            if name != expected_attr.0 {
                continue;
            }
            if *was_seen {
                return Err(err(format!("{context}에 {name} 속성이 중복되었습니다.")));
            }
            *was_seen = true;
            if value.as_ref() != expected_attr.1 {
                return Err(err(format!(
                    "{context}의 {name} 값이 고정 스키마와 다릅니다."
                )));
            }
            matched = true;
            break;
        }
        if !matched {
            return Err(err(format!(
                "{context}에 알 수 없는 {name} 속성이 있습니다."
            )));
        }
    }
    if seen.into_iter().all(identity) {
        Ok(())
    } else {
        Err(err(format!("{context} 속성 수가 고정 스키마와 다릅니다.")))
    }
}
fn validate_relationship_set<'xml, const N: usize>(
    xml: &'xml str,
    context: &str,
    expected: &[(&str, &str, Option<&str>); N],
    container: &XlsxContainer,
) -> Result<[Option<Cow<'xml, str>>; N]> {
    let mut ids: [Option<Cow<'xml, str>>; N] = array::from_fn(|_| None);
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
                .find(|item| type_ == item.0.0 && target == item.0.1)
            else {
                return Err(err(format!(
                    "{context}에 지원하지 않는 관계가 있습니다: {type_} -> {target}"
                )));
            };
            if candidate.2.is_some_and(|part| !container.has_part(part)) {
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
        let required = candidate.2.is_none_or(|part| container.has_part(part));
        if required != id.is_some() {
            return Err(err(format!(
                "{context} 필수 관계 구성이 올바르지 않습니다: {}",
                candidate.1
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
        .ok_or_else(|| err(format!("{context}의 XML root 태그가 없습니다.")))?;
    if !root.is_start || root.name != expected_name {
        return Err(err(format!(
            "{context}의 XML root 태그가 올바르지 않습니다."
        )));
    }
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
fn validate_spreadsheet_xml_document(xml: &str, expected_root: &str, context: &str) -> Result<()> {
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
    while let Some(tag) = scanner.next_tag() {
        if tag.is_start {
            if tag.name != tag.local_name {
                return Err(err(format!(
                    "{context}의 prefixed core element는 지원하지 않습니다: {}",
                    tag.name
                )));
            }
            let mut attributes = XmlAttrScanner::new(tag.raw)?;
            while let Some((name, value)) = attributes.next()? {
                if name == "xmlns" || name.starts_with("xmlns:") {
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
            let document_end = tag
                .end
                .checked_add(1)
                .ok_or_else(|| err(format!("{context}의 XML root 끝 계산 실패")))?;
            if scanner.next_tag().is_some()
                || !xml
                    .get(document_end..)
                    .is_some_and(|trailing| xml_misc_only(trailing, false))
            {
                return Err(err(format!(
                    "{context}의 XML root 뒤 내용이 올바르지 않습니다."
                )));
            }
            return Ok(());
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
    let root_end = root
        .end
        .checked_add(1)
        .ok_or_else(|| err(format!("{context}의 XML root 끝 계산 실패")))?;
    let document_end = if root.self_closing {
        root_end
    } else {
        let close = scanner
            .next_tag()
            .filter(|tag| !tag.is_start && tag.name == expected_name)
            .ok_or_else(|| err(format!("{context}의 XML root가 비어 있지 않습니다.")))?;
        close
            .end
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
fn replace_single_self_closing_tag(
    xml: &mut String,
    name: &str,
    replacement: &str,
    validate: impl FnOnce(&str) -> Result<()>,
) -> Result<()> {
    let mut scanner = XmlScanner::new(xml);
    let tag = scanner
        .next_start_named(name)
        .filter(|tag| tag.name == name && tag.self_closing)
        .ok_or_else(|| err(format!("workbook의 {name} 태그가 올바르지 않습니다.")))?;
    validate(tag.raw)?;
    if scanner.next_start_named(name).is_some() {
        return Err(err(format!("workbook에 {name} 태그가 여러 개 있습니다.")));
    }
    let span = tag.start
        ..tag
            .end
            .checked_add(1)
            .ok_or_else(|| err(format!("workbook의 {name} 태그 끝 계산 실패")))?;
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
fn visit_direct_xml_children<'xml>(
    xml: &'xml str,
    root_local_name: &str,
    expected_namespace: &str,
    context: &str,
    mut visit: impl FnMut(&'xml str, &'xml str) -> Result<()>,
) -> Result<usize> {
    let (mut scanner, root_tag) = scan_xml_root(xml, root_local_name, context)?;
    if root_tag.self_closing {
        return Err(err(format!("{context}의 XML root 태그가 비어 있습니다.")));
    }
    validate_exact_attrs(
        root_tag.raw,
        &[("xmlns", expected_namespace)],
        &format!("{context} root"),
    )?;
    let root_name = root_tag.name;
    let mut open_child_name = None;
    let mut child_count = 0_usize;
    let mut content_start = root_tag
        .end
        .checked_add(1)
        .ok_or_else(|| err(format!("{context}의 XML root 범위가 손상되었습니다.")))?;
    while let Some(tag) = scanner.next_tag() {
        let between = xml
            .get(content_start..tag.start)
            .ok_or_else(|| err(format!("{context}의 XML child 범위가 손상되었습니다.")))?;
        if !xml_misc_only(between, false) {
            return Err(err(format!(
                "{context}의 XML 요소 사이 내용이 올바르지 않습니다."
            )));
        }
        content_start = tag
            .end
            .checked_add(1)
            .ok_or_else(|| err(format!("{context}의 XML child 범위가 손상되었습니다.")))?;
        if tag.is_start {
            if open_child_name.is_some() {
                return Err(err(format!(
                    "{context}의 XML child 태그는 중첩될 수 없습니다."
                )));
            }
            if tag.name != tag.local_name {
                return Err(err(format!(
                    "{context}의 prefixed child element는 지원하지 않습니다: {}",
                    tag.name
                )));
            }
            if !tag.self_closing {
                open_child_name = Some(tag.name);
            }
            visit(tag.local_name, tag.raw)?;
            child_count = child_count.strict_add(1);
        } else if let Some(child_name) = open_child_name {
            if tag.name != child_name {
                return Err(err(format!(
                    "{context}의 XML child 종료 태그가 일치하지 않습니다."
                )));
            }
            open_child_name = None;
        } else {
            if tag.name != root_name {
                return Err(err(format!(
                    "{context}의 XML root 종료 태그가 일치하지 않습니다."
                )));
            }
            let trailing_start = tag
                .end
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
            return Ok(child_count);
        }
    }
    Err(err(format!("{context}의 XML 종료 태그가 없습니다.")))
}
