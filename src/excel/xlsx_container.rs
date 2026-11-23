use super::{
    ArchiveFingerprint, CALC_CHAIN_PATH, CHANGE_LOG_SHEET_NAME, CanonicalStyleMap,
    MASTER_SHEET_NAME, PackagePart, PartRole, SPREADSHEETML_NAMESPACE, SaveVerification,
    XLSX_PARTS, ZipArchiveBuilder, ZipPackageReader, canonical_excel_fragment,
    xml::{
        MAX_XML_NESTING_DEPTH, XmlAttrScanner, XmlScanner, XmlTag, decode_xml_entities,
        xml_misc_only,
    },
    zip_archive::{ZIP_FINGERPRINT_BUFFER_BYTES, archive_file_len, crc32_update},
};
use crate::diagnostic::{
    AppError, Result, append_fmt, bail, bail_fmt, ensure, ensure_fmt, err, err_with_source,
    fmt_err, fmt_src, or_abort, path_err_with_source, terminal_safe, try_push_vec,
    try_string_with_capacity, try_vec_with_capacity,
};
#[cfg(target_os = "windows")]
use crate::temp_entry::configure_replaceable_file;
use crate::temp_entry::{
    FileIdentity, ValidatedFile, configure_no_follow, open_regular, validate_open_file,
    validate_regular_file,
};
use alloc::borrow::Cow;
use core::{array, fmt::Arguments, mem, ops::Range, str};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::fs::OpenOptionsExt as _;
use std::{
    fs,
    io::{self, Read as _, Seek as _, Write as _, stderr},
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};
mod atomic_replace;
const OFFICE_DOCUMENT_REL_NAMESPACE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
const WORKSHEET_REL_TYPE: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet";
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
    (
        None,
        "docProps/core.xml",
        "http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties",
    ),
    (
        Some("docProps/thumbnail.emf"),
        "docProps/thumbnail.emf",
        "http://schemas.openxmlformats.org/package/2006/relationships/metadata/thumbnail",
    ),
    (
        None,
        "xl/workbook.xml",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument",
    ),
    (
        None,
        "docProps/app.xml",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties",
    ),
    (
        Some("docProps/custom.xml"),
        "docProps/custom.xml",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/custom-properties",
    ),
];
const WORKBOOK_RELATIONSHIPS: [RelationshipSpec; 6] = [
    (
        None,
        "theme/theme1.xml",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/theme",
    ),
    (None, "worksheets/sheet2.xml", WORKSHEET_REL_TYPE),
    (None, "worksheets/sheet1.xml", WORKSHEET_REL_TYPE),
    (
        None,
        "sharedStrings.xml",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings",
    ),
    (
        None,
        "styles.xml",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles",
    ),
    (
        Some(CALC_CHAIN_PATH),
        "calcChain.xml",
        "http://schemas.openxmlformats.org/officeDocument/2006/relationships/calcChain",
    ),
];
const SHEET_RELATIONSHIPS: [RelationshipSpec; 1] = [(
    None,
    "../drawings/drawing1.xml",
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/drawing",
)];
const EXCEL_STYLES_XML: &str = include_str!("excel_styles.xml");
const EXCEL_THEME_XML: &str = include_str!("excel_theme.xml");
const EXCEL_CONTENT_TYPES_XML: &str = include_str!("excel_content_types.xml");
const EXCEL_ROOT_RELS_XML: &str = include_str!("excel_root_rels.xml");
const EXCEL_WORKBOOK_RELS_XML: &str = include_str!("excel_workbook_rels.xml");
const LIBREOFFICE_CELL_XFS: [[u8; 6]; 26] = [
    [164, 0, 0, 0, 0, 0],
    [164, 24, 31, 10, 0, 111],
    [164, 25, 0, 10, 0, 175],
    [164, 26, 0, 10, 0, 175],
    [164, 7, 0, 0, 64, 31],
    [164, 7, 0, 0, 63, 13],
    [164, 27, 0, 10, 0, 175],
    [164, 0, 0, 11, 0, 31],
    [164, 27, 0, 10, 0, 111],
    [169, 27, 0, 10, 0, 111],
    [164, 7, 0, 0, 46, 29],
    [171, 27, 0, 10, 0, 111],
    [164, 24, 31, 10, 63, 111],
    [164, 24, 31, 12, 0, 111],
    [164, 0, 0, 10, 0, 111],
    [166, 0, 0, 10, 63, 111],
    [169, 0, 0, 10, 63, 111],
    [166, 0, 0, 10, 63, 47],
    [164, 28, 0, 10, 0, 111],
    [164, 7, 0, 0, 63, 29],
    [166, 0, 0, 12, 63, 47],
    [164, 26, 0, 0, 0, 143],
    [164, 26, 0, 0, 63, 29],
    [164, 0, 0, 10, 0, 79],
    [164, 0, 0, 10, 0, 175],
    [172, 7, 0, 10, 0, 79],
];
const LIBREOFFICE_STYLE_MAP: [u8; 26] = [
    0, 3, 4, 24, 2, 1, 22, 0, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 23, 18, 19, 20, 21,
];
const EXCEL_CORE_PROPERTIES: [(&str, &str); 9] = [
    ("dc:title", ""),
    ("dc:subject", ""),
    ("dc:creator", ""),
    ("dc:description", ""),
    ("cp:lastModifiedBy", ""),
    ("cp:revision", ""),
    ("dcterms:created", " xsi:type=\"dcterms:W3CDTF\""),
    ("dcterms:modified", " xsi:type=\"dcterms:W3CDTF\""),
    ("dc:language", ""),
];
type RelationshipSpec = (Option<&'static str>, &'static str, &'static str);
type PackageParts = [Option<Vec<u8>>; XLSX_PARTS.len()];
type StyleResources<'text> = (
    &'text [&'text str],
    &'text [&'text str],
    &'text [&'text str],
    &'text [&'text str],
);
pub(crate) struct XlsxContainer {
    parts: PackageParts,
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
            path_err_with_source("xlsx 임시 파일 identity 검증 실패", &self.path, source)
        })?;
        (file.identity == self.identity).ok_or_else(|| {
            fmt_err!(
                "xlsx 임시 파일 identity가 실행 중 변경되었습니다: {}",
                self.path.display()
            )
        })
    }
    fn verify_saved_archive(&mut self) -> Result<()> {
        let mut saved_handle = or_abort(self.file.take());
        let saved_archive = self.path.as_path();
        saved_handle.rewind().map_err(|source| {
            path_err_with_source("저장 검증용 xlsx seek 실패", saved_archive, source)
        })?;
        let saved_container = validate_open_file(saved_handle)
            .map_err(|source| {
                path_err_with_source("마스터 xlsx 파일 검증 실패", saved_archive, source)
            })
            .and_then(|saved_file| XlsxContainer::from_validated_file(saved_file, saved_archive))
            .map_err(|source| {
                path_err_with_source(
                    "저장 검증 실패: 저장 직후 ZIP package 점검에 실패했습니다",
                    saved_archive,
                    source,
                )
            })?;
        super::writer::Workbook::from_container(saved_container)
            .map(|_| ())
            .map_err(|source| {
                path_err_with_source(
                    "저장 검증 실패: 저장 직후 재열기 점검에 실패했습니다",
                    saved_archive,
                    source,
                )
            })
    }
}
impl Drop for ReservedTempArchive {
    fn drop(&mut self) {
        if self.remove_on_drop
            && let Err(source) = self.remove_owned()
        {
            let mut error_output = stderr().lock();
            match writeln!(
                &mut error_output,
                "경고: xlsx 임시 저장 파일 정리 실패: {} ({})",
                terminal_safe(&self.path.display()),
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
            target_os = "windows" => self.backup_archive.path.as_path(),
            any(target_os = "linux", target_os = "macos") => self.temp_archive.path.as_path(),
            _ => compile_error!("fcupdater archive promotion supports only Windows, Linux, and macOS."),
        }
    }
    fn preserve_recovery_archives(
        &mut self,
        context: &str,
        source: atomic_replace::ReplaceFailure,
    ) -> Result<()> {
        let backup_suffix = cfg_select! {
            target_os = "windows" => format!(", backup={}", self.backup_archive.path.display()),
            any(target_os = "linux", target_os = "macos") => "",
            _ => compile_error!("fcupdater archive promotion supports only Windows, Linux, and macOS."),
        };
        let message = format!(
            "{context}; 자동 복구 실패 후 수동 복구를 위해 현재 경로 상태를 보존했습니다: target={}, replacement={}{backup_suffix}",
            self.target_xlsx.display(),
            self.temp_archive.path.display(),
        );
        self.temp_archive.remove_on_drop = false;
        #[cfg(target_os = "windows")]
        {
            self.backup_archive.remove_on_drop = false;
        }
        Err(err_with_source(message, source))
    }
    fn promote(mut self) -> Result<()> {
        self.temp_archive.validate_path_identity()?;
        #[cfg(target_os = "windows")]
        self.backup_archive.validate_path_identity()?;
        let target_file = open_regular(self.target_xlsx, false).map_err(|source| {
            path_err_with_source(
                "저장 직전 원본 xlsx identity 검증 실패",
                self.target_xlsx,
                source,
            )
        })?;
        ensure_fmt!(
            target_file.identity == self.expected_identity,
            "원본 xlsx identity가 실행 중 변경되어 저장을 중단했습니다: {}",
            self.target_xlsx.display()
        );
        #[cfg(target_os = "windows")]
        {
            drop(self.temp_archive.file.take());
            drop(self.backup_archive.file.take());
        }
        if let Err(replace_error) = atomic_replace::swap_files(
            self.target_xlsx,
            &self.temp_archive.path,
            #[cfg(target_os = "windows")]
            &self.backup_archive.path,
            #[cfg(target_os = "windows")]
            false,
        ) {
            let format_context = |label: &str| {
                format!(
                    "{label}: {} -> {}",
                    self.temp_archive.path.display(),
                    self.target_xlsx.display(),
                )
            };
            return match replace_error {
                atomic_replace::ReplaceFilesError::Failed(source) => {
                    Err(err_with_source(format_context("xlsx 저장 실패"), source))
                }
                #[cfg(target_os = "windows")]
                atomic_replace::ReplaceFilesError::Restored(source) => Err(err_with_source(
                    format_context("xlsx 저장 실패 후 원본 대상 파일 자동 복원 완료"),
                    source,
                )),
                #[cfg(target_os = "windows")]
                atomic_replace::ReplaceFilesError::RecoveryRequired(source) => self
                    .preserve_recovery_archives(
                        &format_context("xlsx 저장 중 원본 대상 파일 자동 복구 실패"),
                        source,
                    ),
            };
        }
        let saved_validation = self.validate_displaced_original().and_then(|()| {
            let saved_file = open_regular(self.target_xlsx, true).map_err(|source| {
                path_err_with_source("저장된 xlsx identity 검증 실패", self.target_xlsx, source)
            })?;
            (saved_file.identity == self.temp_archive.identity)
                .ok_or("저장된 xlsx identity가 임시 archive와 다릅니다.")?;
            saved_file.file.sync_all().map_err(|source| {
                path_err_with_source(
                    "xlsx 저장 완료 후 파일 내구성 동기화 실패",
                    self.target_xlsx,
                    source,
                )
            })
        });
        if let Err(validation_error) = saved_validation {
            return self.rollback_after_validation_failure(validation_error);
        }
        self.temp_archive.remove_on_drop = false;
        #[cfg(target_os = "windows")]
        {
            self.backup_archive.remove_on_drop = false;
        }
        let captured_original = self.displaced_original_path();
        fs::remove_file(captured_original).map_err(|source| {
            path_err_with_source("교체된 원본 xlsx 정리 실패", captured_original, source)
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
                    path_err_with_source(
                        "xlsx 저장 완료 후 폴더 내구성 동기화 실패",
                        parent,
                        source,
                    )
                })?;
        }
        Ok(())
    }
    fn rollback_after_validation_failure(&mut self, validation_error: AppError) -> Result<()> {
        let replace_result = atomic_replace::swap_files(
            self.target_xlsx,
            &self.temp_archive.path,
            #[cfg(target_os = "windows")]
            &self.backup_archive.path,
            #[cfg(target_os = "windows")]
            true,
        );
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
            path_err_with_source("교체된 원본 xlsx 열기 실패", captured_original, source)
        })?;
        ensure_fmt!(
            captured_file.identity == self.expected_identity,
            "교체된 원본 xlsx identity가 실행 시작 시점과 다릅니다: {}",
            captured_original.display()
        );
        let archive_len = archive_file_len(&captured_file.file, captured_original)?;
        let mut limited = (&captured_file.file).take((archive_len as u64).strict_add(1));
        let mut buffer = vec![0_u8; ZIP_FINGERPRINT_BUFFER_BYTES];
        let (mut crc, mut bytes_read) = (u32::MAX, 0_usize);
        loop {
            let read_len = limited.read(buffer.as_mut()).map_err(|source| {
                path_err_with_source("교체된 원본 xlsx 검증 실패", captured_original, source)
            })?;
            if read_len == 0 {
                break;
            }
            bytes_read = bytes_read.strict_add(read_len);
            crc = crc32_update(crc, buffer.split_at(read_len).0);
        }
        ensure_fmt!(
            bytes_read == archive_len,
            "교체된 원본 xlsx가 검증 중 변경되었습니다: {}",
            captured_original.display()
        );
        let fingerprint = ArchiveFingerprint {
            crc32: !crc,
            len: bytes_read,
        };
        (fingerprint == self.expected_fingerprint).ok_or_else(|| {
            fmt_err!(
                "원본 xlsx가 실행 중 변경되어 저장을 중단했습니다: {}",
                self.target_xlsx.display()
            )
        })
    }
}
impl XlsxContainer {
    pub(super) fn ensure_supported_workbook(&mut self) -> Result<()> {
        let workbook_text = self.take_text("xl/workbook.xml")?;
        let root = validate_spreadsheet_xml_document(
            &workbook_text,
            "workbook",
            "workbook.xml",
            true,
            None,
        )?;
        (required_xml_attr(root.raw, "xmlns:r", "workbook.xml")?.as_ref()
            == OFFICE_DOCUMENT_REL_NAMESPACE)
            .ok_or("workbook.xml의 xmlns:r namespace가 올바르지 않습니다.")?;
        let workbook_xml = workbook_text.as_str();
        let mut scanner = XmlScanner::new(workbook_xml);
        scanner.skip_to(root.end.strict_add(1));
        let (mut forbidden, mut empty_protection_seen) = (None, false);
        while let Some(element) = scanner.next_direct_element_until(&root, "workbook.xml")? {
            let tag = element.opening;
            let candidate = match tag.local_name {
                "fileRecoveryPr" => (
                    0_u8,
                    "workbook.xml의 fileRecoveryPr 복구 표현은 지원하지 않습니다.",
                ),
                "externalReferences" => {
                    (1, "workbook.xml의 외부 workbook 관계는 지원하지 않습니다.")
                }
                "connections" => (2, "workbook.xml의 외부 데이터 연결은 지원하지 않습니다."),
                "workbookProtection" => {
                    if tag.name == "workbookProtection"
                        && !empty_protection_seen
                        && XmlAttrScanner::from_tag(&tag).next()?.is_none()
                        && xml_misc_only(element.body, false)
                    {
                        empty_protection_seen = true;
                        continue;
                    }
                    (
                        3,
                        "workbook.xml의 보호 설정은 정규 저장으로 보존할 수 없습니다.",
                    )
                }
                _ => continue,
            };
            if forbidden.is_none_or(|current: (u8, &str)| candidate.0 < current.0) {
                forbidden = Some(candidate);
            }
        }
        if let Some((_, message)) = forbidden {
            return Err(err(message));
        }
        self.take_workbook_dependencies(workbook_xml)?;
        let mut properties_scanner = XmlScanner::new(workbook_xml);
        if let Some(properties) = properties_scanner.next_element_named("workbookPr")? {
            ensure(
                properties.opening.name == "workbookPr" && properties.body.trim().is_empty(),
                "workbookPr 요소가 올바르지 않습니다.",
            )?;
            let mut date_system = None;
            let mut attributes = XmlAttrScanner::from_tag(&properties.opening);
            while let Some((name, value)) = attributes.next()? {
                ensure(
                    name == "date1904" && date_system.replace(value).is_none(),
                    "workbookPr에 date1904 속성이 중복되었습니다.",
                )?;
            }
            ensure(
                date_system
                    .as_deref()
                    .is_none_or(|value| matches!(value, "0" | "false")),
                "workbookPr의 1904 날짜 체계는 지원하지 않습니다.",
            )?;
            ensure(
                properties_scanner.next_start_named("workbookPr").is_none(),
                "workbook에 workbookPr 요소가 여러 개 있습니다.",
            )?;
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
        let (source_fingerprint, source_parts) = ZipPackageReader {
            archive_file: source_file.file,
            archive_path: source_xlsx,
        }
        .read()?;
        let mut parts: PackageParts = array::repeat(None);
        for PackagePart { bytes, name } in source_parts {
            if or_abort(parts.get_mut(part_index(name)))
                .replace(bytes)
                .is_some()
            {
                process::abort();
            }
        }
        let mut container = Self {
            parts,
            source_fingerprint,
            source_identity,
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            source_permissions,
        };
        container.validate_content_types()?;
        let mut root_ids = [const { None }; ROOT_RELATIONSHIPS.len()];
        validate_relationship_set(
            container.text("_rels/.rels")?,
            "_rels/.rels",
            &ROOT_RELATIONSHIPS,
            &mut root_ids,
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
        container.part_mut("[Content_Types].xml").clear();
        container.part_mut("_rels/.rels").clear();
        Ok(container)
    }
    fn has_part(&self, name: &str) -> bool {
        self.parts
            .get(part_index(name))
            .is_some_and(Option::is_some)
    }
    pub(super) fn package_prepare_excel_output(&mut self) -> Result<CanonicalStyleMap> {
        let source_styles = self.text("xl/styles.xml")?;
        let canonical_styles = EXCEL_STYLES_XML
            .trim_suffix('\n')
            .split_once('\n')
            .is_some_and(|(declaration, style_sheet)| {
                source_styles
                    .strip_prefix(declaration)
                    .and_then(|tail| tail.strip_prefix("\r\n"))
                    == Some(style_sheet)
            });
        let input_styles = if canonical_styles {
            let mut mapping =
                try_vec_with_capacity(27_u32 as usize, "입력 style mapping 메모리 확보 실패")?;
            mapping.extend((0..27_u32).map(Some));
            mapping
        } else {
            let (excel_xfs, source_xfs, source_fonts, source_borders, excel_fonts, excel_borders) = (
                style_entries(EXCEL_STYLES_XML, "cellXfs", "xf")?,
                style_entries(source_styles, "cellXfs", "xf")?,
                style_entries(source_styles, "fonts", "font")?,
                style_entries(source_styles, "borders", "border")?,
                style_entries(EXCEL_STYLES_XML, "fonts", "font")?,
                style_entries(EXCEL_STYLES_XML, "borders", "border")?,
            );
            let mut libreoffice_xml =
                try_string_with_capacity(8192, "LibreOffice style XML 메모리 확보 실패")?;
            libreoffice_xml.push_str("<cellXfs count=\"26\">");
            for &[num_fmt, font, fill, border, xf_id, flags] in &LIBREOFFICE_CELL_XFS {
                let horiz = match flags >> 6_u8 {
                    1 => "center",
                    2 => "left",
                    _ => "general",
                };
                let vert = if flags & 16 != 0 { "bottom" } else { "center" };
                let bool_str = |mask: u8| if flags & mask != 0 { "true" } else { "false" };
                append_fmt(
                    &mut libreoffice_xml,
                    format_args!(
                        "<xf numFmtId=\"{num_fmt}\" fontId=\"{font}\" fillId=\"{fill}\" borderId=\"{border}\" xfId=\"{xf_id}\" applyFont=\"{}\" applyBorder=\"{}\" applyAlignment=\"{}\" applyProtection=\"{}\"><alignment horizontal=\"{horiz}\" vertical=\"{vert}\" textRotation=\"0\" wrapText=\"{}\" indent=\"0\" shrinkToFit=\"false\"/><protection locked=\"true\" hidden=\"false\"/></xf>",
                        bool_str(1),
                        bool_str(2),
                        bool_str(4),
                        bool_str(8),
                        bool_str(32),
                    ),
                );
            }
            libreoffice_xml.push_str("</cellXfs>");
            let libreoffice_xfs = style_entries(&libreoffice_xml, "cellXfs", "xf")?;
            ensure(
                libreoffice_xfs.len() == LIBREOFFICE_STYLE_MAP.len(),
                "내장 LibreOffice style mapping 수가 올바르지 않습니다.",
            )?;
            let mut mapping =
                try_vec_with_capacity(source_xfs.len(), "입력 style mapping 메모리 확보 실패")?;
            for source_xf in source_xfs {
                let canonical =
                    match find_equivalent_xf(
                        source_xf,
                        &excel_xfs,
                        Some((&source_fonts, &excel_fonts, &source_borders, &excel_borders)),
                    )? {
                        Some(index) => Some(u32::try_from(index).map_err(|error| {
                            err_with_source("Excel style index 변환 실패", error)
                        })?),
                        None => find_equivalent_xf(source_xf, &libreoffice_xfs, None)?
                            .filter(|&index| index != 7)
                            .and_then(|index| LIBREOFFICE_STYLE_MAP.get(index))
                            .copied()
                            .map(u32::from),
                    };
                mapping.push(canonical);
            }
            mapping
        };
        let source_core = self.part_mut("docProps/core.xml");
        let source_core_xml = str::from_utf8(source_core)
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
        while let Some(element) = core_scanner.next_direct_element_until(&core_root, "core.xml")? {
            let Some((&(qualified, _), slot)) = EXCEL_CORE_PROPERTIES
                .iter()
                .zip(&mut core_values)
                .find(|&(&(name, _), _)| element.opening.name == name)
            else {
                continue;
            };
            ensure_fmt!(
                slot.is_none(),
                "core.xml에 {qualified} 요소가 여러 개 있습니다."
            );
            ensure_fmt!(
                !element.body.contains('<'),
                "core.xml의 {qualified} 본문이 올바르지 않습니다."
            );
            decode_xml_entities(element.body).map_err(|source| {
                fmt_src!(source, "core.xml의 {qualified} 본문이 올바르지 않습니다.")
            })?;
            *slot = Some(element.body);
        }
        for ((name, extra), body_value) in EXCEL_CORE_PROPERTIES.into_iter().zip(core_values) {
            if let Some(body) = body_value {
                core_xml.extend(["<", name, extra, ">", body, "</", name, ">"]);
            }
        }
        core_xml.push_str("</cp:coreProperties>");
        *source_core = core_xml.into_bytes();
        let source_app = self.part_mut("docProps/app.xml");
        let source_app_xml = str::from_utf8(source_app)
            .map_err(|source| err_with_source("app.xml UTF-8 해석 실패", source))?;
        let (mut app_scanner, app_root) = scan_xml_root(source_app_xml, "Properties", "app.xml")?;
        let mut total_time_body = None;
        while let Some(element) = app_scanner.next_direct_element_until(&app_root, "app.xml")? {
            ensure(
                element.opening.name != "TotalTime"
                    || total_time_body.replace(element.body).is_none(),
                "app.xml에 TotalTime 요소가 여러 개 있습니다.",
            )?;
        }
        let mut app_xml = try_string_with_capacity(
            960_usize.strict_add(total_time_body.map_or(0, str::len)),
            "Excel app.xml 메모리 확보 실패",
        )?;
        app_xml.push_str(concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n",
            "<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\" xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\">",
        ));
        if let Some(body) = total_time_body {
            let value = decode_xml_entities(body)?
                .trim_matches([' ', '\t', '\r', '\n'])
                .parse::<i32>()
                .map_err(|source| err_with_source("app.xml의 TotalTime 해석 실패", source))?;
            ensure(value >= 0_i32, "app.xml의 TotalTime은 음수일 수 없습니다.")?;
            app_xml.extend(["<TotalTime>", body, "</TotalTime>"]);
        }
        app_xml.extend(["<Application>", crate::APP_NAME, "</Application><HeadingPairs><vt:vector size=\"2\" baseType=\"variant\"><vt:variant><vt:lpstr>워크시트</vt:lpstr></vt:variant><vt:variant><vt:i4>2</vt:i4></vt:variant></vt:vector></HeadingPairs><TitlesOfParts><vt:vector size=\"2\" baseType=\"lpstr\"><vt:lpstr>유류비</vt:lpstr><vt:lpstr>변경내역</vt:lpstr></vt:vector></TitlesOfParts></Properties>"]);
        *source_app = app_xml.into_bytes();
        for (index, (name, role, _)) in XLSX_PARTS.into_iter().enumerate() {
            let slot = or_abort(self.parts.get_mut(index));
            if role == PartRole::InputOnly {
                *slot = None;
                continue;
            }
            let canonical_xml = match name {
                "[Content_Types].xml" => Some(EXCEL_CONTENT_TYPES_XML),
                "_rels/.rels" => Some(EXCEL_ROOT_RELS_XML),
                "xl/_rels/workbook.xml.rels" => Some(EXCEL_WORKBOOK_RELS_XML),
                "xl/styles.xml" if !canonical_styles => Some(EXCEL_STYLES_XML),
                "xl/theme/theme1.xml" => Some(EXCEL_THEME_XML),
                _ => None,
            };
            match canonical_xml {
                Some(xml) => *slot = Some(canonical_excel_fragment(xml)?.into_bytes()),
                None if slot.is_none() => process::abort(),
                None => {}
            }
        }
        Ok(input_styles)
    }
    fn part_mut(&mut self, name: &str) -> &mut Vec<u8> {
        or_abort(
            self.parts
                .get_mut(part_index(name))
                .and_then(Option::as_mut),
        )
    }
    pub(super) fn put_text(&mut self, name: &str, content: String) {
        *self.part_mut(name) = content.into_bytes();
    }
    pub(super) fn save(mut self, target_xlsx: &Path, verification: SaveVerification) -> Result<()> {
        let mut archive_parts =
            try_vec_with_capacity(XLSX_PARTS.len(), "Excel package part 목록 메모리 확보 실패")?;
        for ((name, _, _), slot) in XLSX_PARTS.into_iter().zip(&mut self.parts) {
            if let Some(bytes) = slot.take() {
                archive_parts.push(PackagePart { bytes, name });
            }
        }
        let parent = target_xlsx
            .parent()
            .filter(|path| !path.is_empty())
            .unwrap_or_else(|| Path::new("."));
        let reserve_archive = |prefix: &str, label: &str| -> Result<ReservedTempArchive> {
            let pid = process::id();
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|source| {
                    path_err_with_source(label, target_xlsx, io::Error::other(source))
                })?
                .as_nanos();
            for sequence in 0_u32..1024_u32 {
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
                        let (_, identity) = validate_regular_file(&file)
                            .map_err(|source| path_err_with_source(label, target_xlsx, source))?;
                        return Ok(ReservedTempArchive {
                            file: Some(file),
                            identity,
                            path,
                            remove_on_drop: true,
                        });
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(path_err_with_source(label, target_xlsx, error)),
                }
            }
            Err(path_err_with_source(
                label,
                target_xlsx,
                io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "임시 항목 이름 충돌이 반복되었습니다. 잠시 후 다시 시도하세요.",
                ),
            ))
        };
        let mut tmp_archive = reserve_archive(
            ".fuel_cost_chungcheong.xlsx.tmp_",
            "임시 저장 파일 생성 실패",
        )?;
        #[cfg(target_os = "windows")]
        let mut backup_archive = reserve_archive(
            ".fuel_cost_chungcheong.xlsx.backup_",
            "교체 예약 파일 생성 실패",
        )?;
        let result = (|| -> Result<()> {
            ZipArchiveBuilder {
                archive_path: tmp_archive.path.as_path(),
                file: or_abort(tmp_archive.file.as_mut()),
                parts: &archive_parts,
                #[cfg(any(target_os = "linux", target_os = "macos"))]
                permissions: self.source_permissions,
            }
            .create()?;
            match verification {
                SaveVerification::Skip => {}
                SaveVerification::Verify => tmp_archive.verify_saved_archive()?,
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
        let Err(source) = result else {
            return Ok(());
        };
        if tmp_archive.remove_on_drop
            && let Err(error) = tmp_archive.remove_owned()
        {
            return Err(fmt_src!(
                source,
                "xlsx 임시 저장 파일 삭제 실패: {} ({error})",
                tmp_archive.path.display()
            ));
        }
        Err(source)
    }
    pub(super) fn take_shared_strings_text(&mut self) -> Result<String> {
        let xml = self.take_text("xl/sharedStrings.xml")?;
        validate_spreadsheet_xml_document(&xml, "sst", "sharedStrings.xml", false, None)?;
        Ok(xml)
    }
    fn take_text(&mut self, name: &str) -> Result<String> {
        String::from_utf8(mem::take(self.part_mut(name)))
            .map_err(|source| fmt_src!(source, "xlsx part UTF-8 해석 실패: {name}"))
    }
    fn take_workbook_dependencies(&mut self, workbook_xml: &str) -> Result<()> {
        let workbook_relationships = self.take_text("xl/_rels/workbook.xml.rels")?;
        let mut relationship_ids = [const { None }; WORKBOOK_RELATIONSHIPS.len()];
        validate_relationship_set(
            &workbook_relationships,
            "workbook.xml.rels",
            &WORKBOOK_RELATIONSHIPS,
            &mut relationship_ids,
            self,
        )?;
        let [_, change_log_rid, master_rid, _, _, _] = relationship_ids;
        let sheet_ids = [
            master_rid
                .as_deref()
                .ok_or("유류비 worksheet relationship Id가 없습니다.")?,
            change_log_rid
                .as_deref()
                .ok_or("변경내역 worksheet relationship Id가 없습니다.")?,
        ];
        let mut workbook_scanner = XmlScanner::new(workbook_xml);
        workbook_scanner
            .next_start_named("sheets")
            .ok_or("workbook.xml의 sheets 시작 태그를 찾지 못했습니다.")?;
        for ((expected_name, expected_sheet_id), expected_rid) in
            [(MASTER_SHEET_NAME, "1"), (CHANGE_LOG_SHEET_NAME, "2")]
                .into_iter()
                .zip(sheet_ids)
        {
            let sheet_tag = workbook_scanner
                .next_start_named("sheet")
                .filter(|tag| tag.name == "sheet" && tag.self_closing)
                .ok_or("workbook sheet 태그가 올바르지 않습니다.")?;
            let mut sheet_attrs = [None, None, None, None];
            parse_attrs_into(
                sheet_tag.raw,
                &["name", "sheetId", "state", "r:id"],
                &mut sheet_attrs,
                "workbook.xml sheet",
            )?;
            let [name, sheet_id, state, rid] = sheet_attrs;
            (name.as_deref() == Some(expected_name)
                && sheet_id.as_deref() == Some(expected_sheet_id)
                && rid.as_deref() == Some(expected_rid)
                && state.as_deref().is_none_or(|value| value == "visible"))
            .ok_or("workbook.xml sheet 구성이 고정 스키마와 다릅니다.")?;
        }
        ensure(
            workbook_scanner.next_start_named("sheet").is_none(),
            "workbook sheet 수가 고정 스키마의 2개보다 많습니다.",
        )
    }
    pub(super) fn take_worksheet_text(&mut self, name: &str) -> Result<String> {
        let drawing_rid = if name == super::MASTER_SHEET_PATH {
            let has_relationships = self.has_part("xl/worksheets/_rels/sheet1.xml.rels");
            ensure(
                has_relationships == self.has_part("xl/drawings/drawing1.xml"),
                "worksheet drawing 관계와 drawing part는 함께 존재해야 합니다.",
            )?;
            if has_relationships {
                let relationships_xml = self.take_text("xl/worksheets/_rels/sheet1.xml.rels")?;
                let mut relationships = [const { None }; SHEET_RELATIONSHIPS.len()];
                validate_relationship_set(
                    &relationships_xml,
                    "sheet1.xml.rels",
                    &SHEET_RELATIONSHIPS,
                    &mut relationships,
                    self,
                )?;
                let rid = relationships
                    .first_mut()
                    .and_then(Option::take)
                    .ok_or("worksheet drawing relationship Id가 없습니다.")?
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
        if drawing_rid.is_none() && !xml.contains("drawing") {
            return Ok(xml);
        }
        let mut drawing_scanner = XmlScanner::new(&xml);
        match (drawing_rid, drawing_scanner.next_start_named("drawing")) {
            (Some(expected_rid), Some(tag)) if tag.name == "drawing" && tag.self_closing => {
                validate_exact_attrs(
                    tag.raw,
                    &[("r:id", expected_rid.as_str())],
                    format_args!("sheet1.xml drawing"),
                )?;
                let span = tag.start..tag.end.strict_add(1);
                drawing_scanner.skip_to(span.end);
                ensure(
                    drawing_scanner.next_start_named("drawing").is_none(),
                    "sheet1.xml에 drawing 참조가 여러 개 있습니다.",
                )?;
                xml.replace_range(span, "");
            }
            (Some(_), None) => bail!("sheet1.xml의 빈 drawing 참조를 찾지 못했습니다."),
            (None | Some(_), Some(_)) => {
                bail!("worksheet에 대응하는 drawing part 없는 참조가 있습니다.");
            }
            (None, None) => {}
        }
        Ok(xml)
    }
    fn text(&self, name: &str) -> Result<&str> {
        let bytes = or_abort(self.parts.get(part_index(name)).and_then(Option::as_deref));
        str::from_utf8(bytes)
            .map_err(|source| fmt_src!(source, "xlsx part UTF-8 해석 실패: {name}"))
    }
    fn validate_content_types(&self) -> Result<()> {
        let content_types_xml = self.text("[Content_Types].xml")?;
        let mut seen_defaults = [false; CONTENT_TYPE_DEFAULTS.len()];
        let mut seen_overrides = [false; XLSX_PARTS.len()];
        visit_direct_xml_children(
            content_types_xml,
            "Types",
            "http://schemas.openxmlformats.org/package/2006/content-types",
            "[Content_Types].xml",
            &mut |local_name, raw| {
                let (is_default, key_name) = match local_name {
                    "Default" => (true, "Extension"),
                    "Override" => (false, "PartName"),
                    _ => {
                        bail_fmt!("[Content_Types].xml에 알 수 없는 {local_name} 항목이 있습니다.")
                    }
                };
                let mut entry_attrs = [None, None];
                parse_attrs_into(
                    raw,
                    &[key_name, "ContentType"],
                    &mut entry_attrs,
                    "[Content_Types].xml entry",
                )?;
                let [key_attr, content_type_attr] = entry_attrs;
                let key = key_attr.ok_or_else(|| {
                    fmt_err!("[Content_Types].xml entry에 {key_name} 속성이 없습니다.")
                })?;
                let content_type = content_type_attr
                    .ok_or("[Content_Types].xml entry에 ContentType 속성이 없습니다.")?;
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
                    bail_fmt!(
                        "[Content_Types].xml에 지원하지 않는 {local_name} 항목이 있습니다: {key}"
                    );
                };
                ensure_fmt!(
                    is_default
                        || key
                            .strip_prefix('/')
                            .is_some_and(|name| self.has_part(name)),
                    "[Content_Types].xml Override 대상 part가 없습니다: {key}"
                );
                ensure_fmt!(
                    !mem::replace(seen_entry, true),
                    "[Content_Types].xml 항목이 중복되었습니다: {key}"
                );
                Ok(())
            },
        )?;
        for (&(key, _, role), present) in CONTENT_TYPE_DEFAULTS.iter().zip(seen_defaults) {
            let required = role == PartRole::Required
                || role == PartRole::OptionalInput && self.has_part("docProps/thumbnail.emf");
            ensure_fmt!(
                !required || present,
                "[Content_Types].xml 필수 항목이 없습니다: {key}"
            );
        }
        for (&(part, _, content_type), present) in XLSX_PARTS.iter().zip(seen_overrides) {
            ensure_fmt!(
                content_type.is_none() || !self.has_part(part) || present,
                "[Content_Types].xml 필수 항목이 없습니다: /{part}"
            );
        }
        Ok(())
    }
}
fn find_equivalent_xf(
    source: &str,
    catalog: &[&str],
    resources: Option<StyleResources<'_>>,
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
    let mut source_attrs = try_vec_with_capacity(4, "style source 속성 목록 메모리 확보 실패")?;
    'candidate: for (index, candidate) in catalog.iter().enumerate() {
        let (mut source_scanner, mut candidate_scanner) =
            (XmlScanner::new(source), XmlScanner::new(candidate));
        loop {
            let (source_tag, candidate_tag) =
                match (source_scanner.next_tag(), candidate_scanner.next_tag()) {
                    (Some(src), Some(cand)) => (src, cand),
                    (None, None) => return Ok(Some(index)),
                    (None | Some(_), None | Some(_)) => continue 'candidate,
                };
            if (
                source_tag.is_start,
                source_tag.name,
                source_tag.self_closing,
            ) != (
                candidate_tag.is_start,
                candidate_tag.name,
                candidate_tag.self_closing,
            ) {
                continue 'candidate;
            }
            if !source_tag.is_start {
                continue;
            }
            source_attrs.clear();
            let mut source_attr_scanner = XmlAttrScanner::from_tag(&source_tag);
            while let Some((name, value)) = source_attr_scanner.next()? {
                try_push_vec(
                    &mut source_attrs,
                    (name, value),
                    1,
                    "style source 속성 목록 추가 메모리 확보 실패",
                )?;
            }
            let mut candidate_attr_count = 0_usize;
            let mut candidate_attrs = XmlAttrScanner::from_tag(&candidate_tag);
            while let Some((name, candidate_value)) = candidate_attrs.next()? {
                candidate_attr_count = candidate_attr_count.strict_add(1);
                let mut source_matches = source_attrs.iter().filter(|entry| entry.0 == name);
                let (Some(source_entry), None) = (source_matches.next(), source_matches.next())
                else {
                    continue 'candidate;
                };
                let source_value = source_entry.1.as_ref();
                let indexed_resources = resources.and_then(|(sf, cf, sb, cb)| match name {
                    "fontId" => Some((sf, cf)),
                    "borderId" => Some((sb, cb)),
                    _ => None,
                });
                let values_match = if let Some((src_list, cand_list)) = indexed_resources {
                    source_value
                        .parse::<usize>()
                        .ok()
                        .and_then(|idx| src_list.get(idx))
                        .zip(
                            candidate_value
                                .parse::<usize>()
                                .ok()
                                .and_then(|idx| cand_list.get(idx)),
                        )
                        .is_some_and(|(lhs, rhs)| lhs == rhs)
                } else if BOOLEAN_ATTRS.contains(&name) {
                    matches!(
                        (source_value, candidate_value.as_ref()),
                        ("0" | "false", "0" | "false") | ("1" | "true", "1" | "true")
                    )
                } else {
                    source_value == candidate_value.as_ref()
                };
                if !values_match {
                    continue 'candidate;
                }
            }
            if source_attrs.len() != candidate_attr_count {
                continue 'candidate;
            }
        }
    }
    Ok(None)
}
fn parse_attrs_into<'tag>(
    tag: &'tag str,
    names: &[&str],
    values: &mut [Option<Cow<'tag, str>>],
    context: &str,
) -> Result<()> {
    let mut attributes = XmlAttrScanner::new(tag)?;
    while let Some((name, value)) = attributes.next()? {
        let Some((_, slot)) = names
            .iter()
            .zip(values.iter_mut())
            .find(|&(candidate, _)| *candidate == name)
        else {
            bail_fmt!("{context}에 알 수 없는 {name} 속성이 있습니다.");
        };
        ensure_fmt!(
            slot.replace(value).is_none(),
            "{context}에 {name} 속성이 중복되었습니다."
        );
    }
    Ok(())
}
fn part_index(name: &str) -> usize {
    or_abort(
        XLSX_PARTS
            .iter()
            .position(|&(candidate, _, _)| candidate == name),
    )
}
fn required_xml_attr<'tag>(
    tag: &'tag str,
    attr_name: &str,
    context: &str,
) -> Result<Cow<'tag, str>> {
    let mut value = None;
    let mut attributes = XmlAttrScanner::new(tag)?;
    while let Some((name, attr_value)) = attributes.next()? {
        ensure_fmt!(
            name != attr_name || value.replace(attr_value).is_none(),
            "{context}에 중복 {attr_name} 속성이 있습니다."
        );
    }
    value.ok_or_else(|| fmt_err!("{context}에 {attr_name} 속성이 없습니다."))
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
        .ok_or_else(|| fmt_err!("{context}의 XML root 태그가 올바르지 않습니다."))?;
    ensure_fmt!(
        xml_misc_only(or_abort(xml.get(..root.start)), true),
        "{context}의 XML root 앞 내용이 올바르지 않습니다."
    );
    Ok((scanner, root))
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
        .ok_or_else(|| fmt_err!("styles.xml의 {group_name} 시작 태그가 올바르지 않습니다."))?;
    let declared_count = required_xml_attr(element.opening.raw, "count", group_name)?
        .parse::<usize>()
        .map_err(|source| fmt_src!(source, "styles.xml {group_name} count 해석 실패"))?;
    let capacity = declared_count.min(
        element
            .body
            .len()
            .div_euclid(entry_name.len().strict_add(3)),
    );
    let mut entries = try_vec_with_capacity(capacity, "styles.xml 항목 목록 메모리 확보 실패")?;
    let mut child_scanner = XmlScanner::new(element.body);
    while let Some(entry) = child_scanner.next_direct_element(group_name)? {
        ensure_fmt!(
            entry.opening.name == entry_name,
            "styles.xml {group_name}에 알 수 없는 {} 요소가 있습니다.",
            entry.opening.name
        );
        try_push_vec(
            &mut entries,
            or_abort(element.body.get(entry.span)),
            1,
            "styles.xml 항목 목록 메모리 확보 실패",
        )?;
    }
    ensure_fmt!(
        scanner.next_start_named(group_name).is_none(),
        "styles.xml에 {group_name} 태그가 여러 개 있습니다."
    );
    ensure_fmt!(
        entries.len() == declared_count,
        "styles.xml {group_name} count가 실제 {entry_name} 수와 다릅니다: declared={declared_count}, actual={}",
        entries.len()
    );
    Ok(entries)
}
fn validate_empty_xml_root(
    xml: &str,
    expected_name: &str,
    expected_attrs: &[(&str, &str)],
    context: &str,
) -> Result<()> {
    let (mut scanner, root) = scan_xml_root(xml, expected_name, context)?;
    validate_exact_attrs(root.raw, expected_attrs, format_args!("{context}"))?;
    scanner
        .next_direct_element_until(&root, context)?
        .is_none()
        .ok_or_else(|| fmt_err!("{context}의 XML root가 비어 있지 않습니다."))
}
fn validate_exact_attrs(
    tag: &str,
    expected: &[(&str, &str)],
    context: Arguments<'_>,
) -> Result<()> {
    let mut seen = 0_u32;
    let mut attributes = XmlAttrScanner::new(tag)?;
    while let Some((name, value)) = attributes.next()? {
        let Some((index, &(_, expected_value))) = expected
            .iter()
            .enumerate()
            .find(|&(_, &(candidate, _))| candidate == name)
        else {
            bail_fmt!("{context}에 알 수 없는 {name} 속성이 있습니다.");
        };
        let bit = 1_u32
            .checked_shl(u32::try_from(index).unwrap_or(u32::MAX))
            .unwrap_or(0);
        ensure_fmt!(seen & bit == 0, "{context}에 {name} 속성이 중복되었습니다.");
        seen |= bit;
        ensure_fmt!(
            value.as_ref() == expected_value,
            "{context} 속성이 고정 스키마와 다릅니다."
        );
    }
    let expected_mask = 1_u32
        .checked_shl(u32::try_from(expected.len()).unwrap_or(u32::MAX))
        .unwrap_or(0)
        .strict_sub(1);
    (seen == expected_mask).ok_or_else(|| fmt_err!("{context} 속성이 고정 스키마와 다릅니다."))
}
fn validate_relationship_set<'xml>(
    xml: &'xml str,
    context: &str,
    expected: &[RelationshipSpec],
    ids: &mut [Option<Cow<'xml, str>>],
    container: &XlsxContainer,
) -> Result<()> {
    visit_direct_xml_children(
        xml,
        "Relationships",
        "http://schemas.openxmlformats.org/package/2006/relationships",
        context,
        &mut |local_name, raw| {
            ensure_fmt!(
                local_name == "Relationship",
                "{context} 관계 태그가 올바르지 않습니다."
            );
            let mut rel_attrs = [None, None, None];
            parse_attrs_into(raw, &["Id", "Type", "Target"], &mut rel_attrs, context)?;
            let [id_attr, type_attr, target_attr] = rel_attrs;
            let id = id_attr.ok_or_else(|| fmt_err!("{context} 관계에 Id가 없습니다."))?;
            ensure_fmt!(!id.is_empty(), "{context} 관계 Id가 비어 있습니다.");
            let type_ = type_attr.ok_or_else(|| fmt_err!("{context} 관계에 Type이 없습니다."))?;
            let target =
                target_attr.ok_or_else(|| fmt_err!("{context} 관계에 Target이 없습니다."))?;
            ensure_fmt!(
                !ids.iter()
                    .flatten()
                    .any(|candidate| candidate == id.as_ref()),
                "{context} 관계 Id가 중복되었습니다: {id}"
            );
            let Some((&(optional_part, _, _), slot)) = expected
                .iter()
                .zip(ids.iter_mut())
                .find(|&(&(_, exp_target, exp_type), _)| type_ == exp_type && target == exp_target)
            else {
                bail_fmt!("{context}에 지원하지 않는 관계가 있습니다: {type_} -> {target}");
            };
            ensure_fmt!(
                optional_part.is_none_or(|part| container.has_part(part)),
                "{context} 관계 대상 part가 없습니다: {target}"
            );
            ensure_fmt!(
                slot.replace(id).is_none(),
                "{context} 관계가 중복되었습니다: {target}"
            );
            Ok(())
        },
    )?;
    for (&(optional_part, target, _), id) in expected.iter().zip(ids.iter()) {
        ensure_fmt!(
            optional_part.is_none_or(|part| container.has_part(part)) == id.is_some(),
            "{context} 필수 관계 구성이 올바르지 않습니다: {target}"
        );
    }
    Ok(())
}
pub(super) fn validate_spreadsheet_xml_document<'xml>(
    xml: &'xml str,
    expected_root: &str,
    context: &str,
    allow_extensions: bool,
    parsed_sheet_data: Option<&Range<usize>>,
) -> Result<XmlTag<'xml>> {
    let (mut scanner, root) = scan_xml_root(xml, expected_root, context)?;
    ensure_fmt!(
        !root.self_closing,
        "{context}의 XML root 태그가 비어 있습니다."
    );
    ensure_fmt!(
        required_xml_attr(root.raw, "xmlns", context)?.as_ref() == SPREADSHEETML_NAMESPACE,
        "{context}의 root namespace가 올바르지 않습니다."
    );
    let mut ancestors = [root.name; MAX_XML_NESTING_DEPTH];
    let (mut depth, mut sheet_data_seen) = (1_usize, false);
    while let Some(tag) = scanner.next_tag() {
        if tag.is_start {
            ensure_fmt!(
                allow_extensions || tag.name == tag.local_name,
                "{context}의 prefixed core element는 지원하지 않습니다: {}",
                tag.name
            );
            let mut attributes = XmlAttrScanner::from_tag(&tag);
            while let Some((name, value)) = attributes.next()? {
                if (name == "xmlns" || !allow_extensions && name.starts_with("xmlns:"))
                    && !(tag.name == "sortState"
                        && name == "xmlns:xlrd2"
                        && value.as_ref()
                            == "http://schemas.microsoft.com/office/spreadsheetml/2017/richdata2")
                {
                    bail_fmt!("{context}의 descendant namespace 재정의는 지원하지 않습니다.");
                }
            }
            if expected_root == "worksheet" && tag.local_name == "sheetData" {
                ensure_fmt!(
                    depth == 1 && !sheet_data_seen,
                    "{context}의 sheetData 위치 또는 개수가 올바르지 않습니다."
                );
                sheet_data_seen = true;
                if let Some(parsed_span) = parsed_sheet_data {
                    ensure_fmt!(
                        !tag.self_closing
                            && tag.start == parsed_span.start
                            && parsed_span.end > tag.end
                            && xml.get(parsed_span.start..parsed_span.end).is_some(),
                        "{context}의 사전 해석된 sheetData 범위가 올바르지 않습니다."
                    );
                    scanner.skip_to(parsed_span.end);
                    continue;
                }
            }
            if !tag.self_closing {
                *ancestors
                    .get_mut(depth)
                    .ok_or_else(|| fmt_err!("{context}의 XML 중첩 깊이가 너무 큽니다."))? =
                    tag.name;
                depth = depth.strict_add(1);
            }
            continue;
        }
        depth = depth.strict_sub(1);
        let open = or_abort(ancestors.get(depth).copied());
        ensure_fmt!(
            open == tag.name,
            "{context}의 XML 태그 쌍이 일치하지 않습니다: {open} / {}",
            tag.name
        );
        if depth == 0 {
            ensure_fmt!(
                expected_root != "worksheet" || sheet_data_seen,
                "{context}에 sheetData가 없습니다."
            );
            ensure_fmt!(
                xml.get(tag.end.strict_add(1)..)
                    .is_some_and(|trailing| xml_misc_only(trailing, false)),
                "{context}의 XML root 뒤 내용이 올바르지 않습니다."
            );
            return Ok(root);
        }
    }
    Err(fmt_err!("{context}에 닫히지 않은 XML 요소가 있습니다."))
}
fn visit_direct_xml_children<'xml>(
    xml: &'xml str,
    root_local_name: &str,
    expected_namespace: &str,
    context: &str,
    visit: &mut dyn FnMut(&'xml str, &'xml str) -> Result<()>,
) -> Result<()> {
    let (mut scanner, root) = scan_xml_root(xml, root_local_name, context)?;
    ensure_fmt!(
        !root.self_closing,
        "{context}의 XML root 태그가 비어 있습니다."
    );
    validate_exact_attrs(
        root.raw,
        &[("xmlns", expected_namespace)],
        format_args!("{context} root"),
    )?;
    while let Some(child) = scanner.next_direct_element_until(&root, context)? {
        let tag = child.opening;
        ensure_fmt!(
            tag.name == tag.local_name,
            "{context}의 prefixed child element는 지원하지 않습니다: {}",
            tag.name
        );
        ensure_fmt!(
            xml_misc_only(child.body, false),
            "{context}의 XML child 태그는 중첩되거나 text를 포함할 수 없습니다."
        );
        visit(tag.local_name, tag.raw)?;
    }
    Ok(())
}
