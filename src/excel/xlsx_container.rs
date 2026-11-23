use super::{
    ArchiveFingerprint, CALC_CHAIN_PATH, CHANGE_LOG_SHEET_NAME, CanonicalStyleMap,
    MASTER_SHEET_NAME, PackagePart, PartRole, SPREADSHEETML_NAMESPACE, SaveVerification,
    XLSX_PARTS, canonical_excel_fragment,
    xml::{
        MAX_XML_NESTING_DEPTH, XmlAttrScanner, XmlScanner, XmlTag, decode_xml_entities,
        xml_misc_only,
    },
    zip_archive::{ZIP_FINGERPRINT_BUFFER_BYTES, ZipOut, ZipPkg, archive_file_len, crc32_update},
};
use crate::diagnostic::{
    AppError, Result, bail, bail_fmt, ensure, ensure_fmt, err, err_src, fmt_err, fmt_src, or_abort,
    path_err, push_vec, str_cap, terminal_safe, vec_cap,
};
#[cfg(target_os = "windows")]
use crate::temp_entry::configure_replaceable_file;
use crate::temp_entry::{
    FileIdentity, ValidatedFile, configure_no_follow, open_regular, validate_open_file,
    validate_regular_file,
};
use alloc::borrow::Cow;
use core::{
    array,
    error::Error,
    fmt::{self, Arguments},
    mem,
    ops::Range,
    result::Result as CoreResult,
    str,
};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use std::os::unix::fs::OpenOptionsExt as _;
use std::{
    fs,
    io::{self, Read as _, Seek as _, Write as _, stderr},
    path::{Path, PathBuf},
    process,
    time::{SystemTime, UNIX_EPOCH},
};
cfg_select! {
    target_os = "windows" => {
        use core::{ffi::c_void, ptr::null};
        use std::{os::windows::ffi::OsStrExt as _, path::{Component, Prefix, absolute}};
    }
    any(target_os = "linux", target_os = "macos") => {
        use alloc::ffi::CString;
        use core::ffi::{c_char, c_int, c_uint};
        use std::os::unix::ffi::OsStrExt as _;
    }
    _ => {}
}
cfg_select! {
    target_os = "windows" => {
        #[link(name = "kernel32")]
        unsafe extern "system" {
            fn ReplaceFileW(replaced: *const u16, replacement: *const u16, backup: *const u16, flags: u32, exclude: *const c_void, reserved: *const c_void) -> i32;
        }
    }
    target_os = "linux" => {
        unsafe extern "C" {
            fn renameat2(old_dir_fd: c_int, old_path: *const c_char, new_dir_fd: c_int, new_path: *const c_char, flags: c_uint) -> c_int;
        }
    }
    target_os = "macos" => {
        unsafe extern "C" {
            fn renamex_np(old_path: *const c_char, new_path: *const c_char, flags: c_uint) -> c_int;
        }
    }
    _ => {}
}
macro_rules! rel_s {
    ($opt:expr, $t:expr, o $ns:expr) => {
        (
            $opt,
            $t,
            concat!(
                "http://schemas.openxmlformats.org/officeDocument/2006/relationships/",
                $ns
            ),
        )
    };
    ($opt:expr, $t:expr, p $ns:expr) => {
        (
            $opt,
            $t,
            concat!(
                "http://schemas.openxmlformats.org/package/2006/relationships/metadata/",
                $ns
            ),
        )
    };
}
const OFFICE_DOC_REL_NS: &str =
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships";
const CT_DEFAULTS: [(&str, &str, PartRole); 6] = [
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
const ROOT_RELS: [RelSpec; 5] = [
    rel_s!(None, "docProps/core.xml", p "core-properties"),
    rel_s!(Some("docProps/thumbnail.emf"), "docProps/thumbnail.emf", p "thumbnail"),
    rel_s!(None, "xl/workbook.xml", o "officeDocument"),
    rel_s!(None, "docProps/app.xml", o "extended-properties"),
    rel_s!(Some("docProps/custom.xml"), "docProps/custom.xml", o "custom-properties"),
];
const WB_RELS: [RelSpec; 6] = [
    rel_s!(None, "theme/theme1.xml", o "theme"),
    rel_s!(None, "worksheets/sheet2.xml", o "worksheet"),
    rel_s!(None, "worksheets/sheet1.xml", o "worksheet"),
    rel_s!(None, "sharedStrings.xml", o "sharedStrings"),
    rel_s!(None, "styles.xml", o "styles"),
    rel_s!(Some(CALC_CHAIN_PATH), "calcChain.xml", o "calcChain"),
];
const SHEET_RELS: [RelSpec; 1] = [rel_s!(None, "../drawings/drawing1.xml", o "drawing")];
const EXCEL_STYLES_XML: &str = include_str!("excel_styles.xml");
const EXCEL_THEME_XML: &str = include_str!("excel_theme.xml");
const EXCEL_CT_XML: &str = include_str!("excel_content_types.xml");
const EXCEL_ROOT_RELS_XML: &str = include_str!("excel_root_rels.xml");
const EXCEL_WB_RELS_XML: &str = include_str!("excel_workbook_rels.xml");
const LO_XFS: [u8; 156] = [
    164, 0, 0, 0, 0, 0, 164, 24, 31, 10, 0, 111, 164, 25, 0, 10, 0, 175, 164, 26, 0, 10, 0, 175,
    164, 7, 0, 0, 64, 31, 164, 7, 0, 0, 63, 13, 164, 27, 0, 10, 0, 175, 164, 0, 0, 11, 0, 31, 164,
    27, 0, 10, 0, 111, 169, 27, 0, 10, 0, 111, 164, 7, 0, 0, 46, 29, 171, 27, 0, 10, 0, 111, 164,
    24, 31, 10, 63, 111, 164, 24, 31, 12, 0, 111, 164, 0, 0, 10, 0, 111, 166, 0, 0, 10, 63, 111,
    169, 0, 0, 10, 63, 111, 166, 0, 0, 10, 63, 47, 164, 28, 0, 10, 0, 111, 164, 7, 0, 0, 63, 29,
    166, 0, 0, 12, 63, 47, 164, 26, 0, 0, 0, 143, 164, 26, 0, 0, 63, 29, 164, 0, 0, 10, 0, 79, 164,
    0, 0, 10, 0, 175, 172, 7, 0, 10, 0, 79,
];
const LO_STYLE_MAP: [u8; 26] = [
    0, 3, 4, 24, 2, 1, 22, 0, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 23, 18, 19, 20, 21,
];
const CORE_PROPS: [&str; 9] = [
    "dc:title",
    "dc:subject",
    "dc:creator",
    "dc:description",
    "cp:lastModifiedBy",
    "cp:revision",
    "dcterms:created",
    "dcterms:modified",
    "dc:language",
];
type RelSpec = (Option<&'static str>, &'static str, &'static str);
type PackageParts = [Option<Vec<u8>>; XLSX_PARTS.len()];
#[derive(Debug)]
struct ReplaceFailure {
    replace: io::Error,
    #[cfg(target_os = "windows")]
    restore: Option<io::Error>,
}
enum ReplaceFilesError {
    Failed(ReplaceFailure),
    #[cfg(target_os = "windows")]
    RecoveryRequired(ReplaceFailure),
    #[cfg(target_os = "windows")]
    Restored(ReplaceFailure),
}
impl From<io::Error> for ReplaceFilesError {
    fn from(replace: io::Error) -> Self {
        Self::Failed(ReplaceFailure {
            replace,
            #[cfg(target_os = "windows")]
            restore: None,
        })
    }
}
impl fmt::Display for ReplaceFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(target_os = "windows")]
        if let Some(restore) = self.restore.as_ref() {
            return write!(f, "{}; 자동 복원 실패: {restore}", self.replace);
        }
        fmt::Display::fmt(&self.replace, f)
    }
}
impl Error for ReplaceFailure {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(&self.replace)
    }
}
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
            Ok(_) => Err(io::Error::other("임시 파일 identity 불일치")),
            Err(src) if src.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(src) => Err(src),
        }
    }
    fn validate_path_identity(&self) -> Result<()> {
        let file = open_regular(&self.path, false)
            .map_err(|src| path_err("임시 파일 검증 실패", &self.path, src))?;
        (file.identity == self.identity)
            .ok_or_else(|| fmt_err!("임시 파일 identity 변경됨: {}", self.path.display()))
    }
    fn verify_saved_archive(&mut self) -> Result<()> {
        let mut handle = or_abort(self.file.take());
        let path = self.path.as_path();
        handle
            .rewind()
            .map_err(|src| path_err("저장 검증 seek 실패", path, src))?;
        let container = validate_open_file(handle)
            .map_err(|src| path_err("xlsx 검증 실패", path, src))
            .and_then(|file| XlsxContainer::from_validated_file(file, path))
            .map_err(|src| path_err("저장 검증 ZIP 점검 실패", path, src))?;
        super::writer::Workbook::from_container(container)
            .map(|_| ())
            .map_err(|src| path_err("저장 검증 재열기 실패", path, src))
    }
}
impl Drop for ReservedTempArchive {
    fn drop(&mut self) {
        if self.remove_on_drop
            && let Err(src) = self.remove_owned()
        {
            let mut out = stderr().lock();
            match writeln!(
                &mut out,
                "경고: xlsx 임시 파일 정리 실패: {} ({})",
                terminal_safe(&self.path.display()),
                terminal_safe(&src)
            ) {
                Ok(()) | Err(_) => {}
            }
        }
    }
}
struct StyleCatalog<'xml> {
    ex_borders: Vec<&'xml str>,
    ex_fonts: Vec<&'xml str>,
    ex_xfs: Vec<&'xml str>,
    src_borders: Vec<&'xml str>,
    src_fonts: Vec<&'xml str>,
}
impl StyleCatalog<'_> {
    fn match_excel_xf(&self, source: &str) -> Result<Option<u32>> {
        if let Some(idx) = self.ex_xfs.iter().position(|&cand| cand == source) {
            return Ok(Some(
                u32::try_from(idx).map_err(|src| err_src("style index", src))?,
            ));
        }
        let mut src_attrs = vec_cap(4, "style 속성")?;
        'cand: for (idx, cand) in self.ex_xfs.iter().enumerate() {
            let (mut src_sc, mut cand_sc) = (XmlScanner::new(source), XmlScanner::new(cand));
            loop {
                let (s_tag, c_tag) = match (src_sc.next_tag(), cand_sc.next_tag()) {
                    (Some(src), Some(cnd)) => (src, cnd),
                    (None, None) => {
                        return Ok(Some(
                            u32::try_from(idx).map_err(|src| err_src("style index", src))?,
                        ));
                    }
                    (None | Some(_), None | Some(_)) => continue 'cand,
                };
                if (s_tag.is_start, s_tag.name, s_tag.self_closing)
                    != (c_tag.is_start, c_tag.name, c_tag.self_closing)
                {
                    continue 'cand;
                }
                if !s_tag.is_start {
                    continue;
                }
                src_attrs.clear();
                let mut s_sc = XmlAttrScanner::from_tag(&s_tag);
                while let Some((nm, val)) = s_sc.next()? {
                    push_vec(&mut src_attrs, (nm, val), 1, "style 속성")?;
                }
                let (mut c_cnt, mut c_sc) = (0_usize, XmlAttrScanner::from_tag(&c_tag));
                while let Some((nm, c_val)) = c_sc.next()? {
                    c_cnt = c_cnt.strict_add(1);
                    let mut found = src_attrs.iter().filter(|entry| entry.0 == nm);
                    let (Some(s_en), None) = (found.next(), found.next()) else {
                        continue 'cand;
                    };
                    let s_val = s_en.1.as_ref();
                    let ok = match nm {
                        "fontId" | "borderId" => {
                            let (sl, cl) = if nm == "fontId" {
                                (&self.src_fonts, &self.ex_fonts)
                            } else {
                                (&self.src_borders, &self.ex_borders)
                            };
                            s_val
                                .parse::<usize>()
                                .ok()
                                .and_then(|pos| sl.get(pos))
                                .zip(c_val.parse::<usize>().ok().and_then(|pos| cl.get(pos)))
                                .is_some_and(|(lhs, rhs)| lhs == rhs)
                        }
                        "applyAlignment" | "applyBorder" | "applyFill" | "applyFont"
                        | "applyNumberFormat" | "applyProtection" | "hidden"
                        | "justifyLastLine" | "locked" | "pivotButton" | "quotePrefix"
                        | "shrinkToFit" | "wrapText" => parse_bool_attr(s_val)
                            .is_some_and(|flag| Some(flag) == parse_bool_attr(&c_val)),
                        _ => s_val == c_val.as_ref(),
                    };
                    if !ok {
                        continue 'cand;
                    }
                }
                if src_attrs.len() != c_cnt {
                    continue 'cand;
                }
            }
        }
        Ok(None)
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
            _ => compile_error!("unsupported OS"),
        }
    }
    fn preserve_recovery_archives(&mut self, context: &str, source: ReplaceFailure) -> Result<()> {
        let backup = cfg_select! {
            target_os = "windows" => format!(", backup={}", self.backup_archive.path.display()),
            any(target_os = "linux", target_os = "macos") => "",
            _ => compile_error!("unsupported OS"),
        };
        let msg = format!(
            "{context}; 수동 복구 보존: target={}, replacement={}{backup}",
            self.target_xlsx.display(),
            self.temp_archive.path.display()
        );
        self.temp_archive.remove_on_drop = false;
        #[cfg(target_os = "windows")]
        {
            self.backup_archive.remove_on_drop = false;
        }
        Err(err_src(msg, source))
    }
    fn promote(mut self) -> Result<()> {
        self.temp_archive.validate_path_identity()?;
        #[cfg(target_os = "windows")]
        self.backup_archive.validate_path_identity()?;
        let target_file = open_regular(self.target_xlsx, false)
            .map_err(|src| path_err("원본 xlsx 검증 실패", self.target_xlsx, src))?;
        ensure_fmt!(
            target_file.identity == self.expected_identity,
            "원본 xlsx identity 변경됨"
        );
        #[cfg(target_os = "windows")]
        {
            drop(self.temp_archive.file.take());
            drop(self.backup_archive.file.take());
        }
        if let Err(replace_err) = swap_files(
            self.target_xlsx,
            &self.temp_archive.path,
            #[cfg(target_os = "windows")]
            &self.backup_archive.path,
            #[cfg(target_os = "windows")]
            false,
        ) {
            let ctx = |label: &str| {
                format!(
                    "{label}: {} -> {}",
                    self.temp_archive.path.display(),
                    self.target_xlsx.display()
                )
            };
            return match replace_err {
                ReplaceFilesError::Failed(src) => Err(err_src(ctx("xlsx 저장 실패"), src)),
                #[cfg(target_os = "windows")]
                ReplaceFilesError::Restored(src) => Err(err_src(ctx("저장 실패 후 복원"), src)),
                #[cfg(target_os = "windows")]
                ReplaceFilesError::RecoveryRequired(src) => {
                    self.preserve_recovery_archives(&ctx("복구 실패"), src)
                }
            };
        }
        let saved_ok = self.validate_displaced_original().and_then(|()| {
            let saved = open_regular(self.target_xlsx, true)
                .map_err(|src| path_err("저장된 xlsx 검증 실패", self.target_xlsx, src))?;
            (saved.identity == self.temp_archive.identity).ok_or("저장된 xlsx identity 불일치")?;
            saved
                .file
                .sync_all()
                .map_err(|src| path_err("xlsx sync 실패", self.target_xlsx, src))
        });
        if let Err(val_err) = saved_ok {
            return self.rollback_after_validation_failure(val_err);
        }
        self.temp_archive.remove_on_drop = false;
        #[cfg(target_os = "windows")]
        {
            self.backup_archive.remove_on_drop = false;
        }
        let captured = self.displaced_original_path();
        fs::remove_file(captured)
            .map_err(|src| path_err("교체된 원본 정리 실패", captured, src))?;
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        {
            let parent = self
                .target_xlsx
                .parent()
                .filter(|path| !path.is_empty())
                .unwrap_or_else(|| Path::new("."));
            fs::File::open(parent)
                .and_then(|file| file.sync_all())
                .map_err(|src| path_err("폴더 sync 실패", parent, src))?;
        }
        Ok(())
    }
    fn rollback_after_validation_failure(&mut self, val_err: AppError) -> Result<()> {
        let res = swap_files(
            self.target_xlsx,
            &self.temp_archive.path,
            #[cfg(target_os = "windows")]
            &self.backup_archive.path,
            #[cfg(target_os = "windows")]
            true,
        );
        let rollback_err = match res {
            Ok(()) => return Err(val_err),
            #[cfg(target_os = "windows")]
            Err(ReplaceFilesError::Restored(_)) => return Err(val_err),
            #[cfg(target_os = "windows")]
            Err(ReplaceFilesError::RecoveryRequired(src)) => src,
            Err(ReplaceFilesError::Failed(src)) => src,
        };
        self.preserve_recovery_archives(
            &format!("원본 검증 실패 후 복구 실패 ({val_err})"),
            rollback_err,
        )
    }
    fn validate_displaced_original(&self) -> Result<()> {
        let captured = self.displaced_original_path();
        let file = open_regular(captured, false)
            .map_err(|src| path_err("교체된 원본 열기 실패", captured, src))?;
        ensure_fmt!(
            file.identity == self.expected_identity,
            "교체된 원본 identity 불일치"
        );
        let archive_len = archive_file_len(&file.file, captured)?;
        let mut limited = (&file.file).take((archive_len as u64).strict_add(1));
        let mut buf = vec![0_u8; ZIP_FINGERPRINT_BUFFER_BYTES];
        let (mut crc, mut total) = (u32::MAX, 0_usize);
        loop {
            let count = limited
                .read(buf.as_mut())
                .map_err(|src| path_err("원본 검증 실패", captured, src))?;
            if count == 0 {
                break;
            }
            total = total.strict_add(count);
            crc = crc32_update(crc, buf.split_at(count).0);
        }
        ensure_fmt!(total == archive_len, "교체된 원본 검증 중 변경됨");
        (ArchiveFingerprint {
            crc32: !crc,
            len: total,
        } == self.expected_fingerprint)
            .ok_or_else(|| {
                fmt_err!(
                    "원본 xlsx 변경으로 저장 중단: {}",
                    self.target_xlsx.display()
                )
            })
    }
}
impl XlsxContainer {
    pub(super) fn ensure_supported_workbook(&mut self) -> Result<()> {
        let wb_text = self.take_text("xl/workbook.xml")?;
        let root =
            validate_spreadsheet_xml_document(&wb_text, "workbook", "workbook.xml", true, None)?;
        (required_xml_attr(root.raw, "xmlns:r", "workbook.xml")?.as_ref() == OFFICE_DOC_REL_NS)
            .ok_or("workbook.xml xmlns:r 오류")?;
        let wb_xml = wb_text.as_str();
        let mut sc = XmlScanner::new(wb_xml);
        sc.skip_to(root.end.strict_add(1));
        let (mut forbidden, mut empty_prot) = (None, false);
        while let Some(el) = sc.next_direct_element_until(&root, "workbook.xml")? {
            let tag = el.opening;
            let cand = match tag.local_name {
                "fileRecoveryPr" => (0_u8, "workbook.xml fileRecoveryPr 미지원"),
                "externalReferences" => (1, "workbook.xml 외부 관계 미지원"),
                "connections" => (2, "workbook.xml 외부 연결 미지원"),
                "workbookProtection" => {
                    if tag.name == "workbookProtection"
                        && !empty_prot
                        && XmlAttrScanner::from_tag(&tag).next()?.is_none()
                        && xml_misc_only(el.body, false)
                    {
                        empty_prot = true;
                        continue;
                    }
                    (3, "workbook.xml 보호 설정 미지원")
                }
                _ => continue,
            };
            if forbidden.is_none_or(|cur: (u8, &str)| cand.0 < cur.0) {
                forbidden = Some(cand);
            }
        }
        if let Some((_, msg)) = forbidden {
            return Err(err(msg));
        }
        self.take_workbook_dependencies(wb_xml)?;
        let mut pr_sc = XmlScanner::new(wb_xml);
        if let Some(props) = pr_sc.next_element_named("workbookPr")? {
            ensure(
                props.opening.name == "workbookPr" && props.body.trim().is_empty(),
                "workbookPr 오류",
            )?;
            let mut date1904 = [None];
            XmlAttrScanner::parse_into(
                props.opening.raw,
                &["date1904"],
                &mut date1904,
                "workbookPr",
            )?;
            ensure(
                date1904[0]
                    .as_deref()
                    .is_none_or(|val| matches!(val, "0" | "false")),
                "workbookPr 1904 날짜 체계 미지원",
            )?;
            ensure(
                pr_sc.next_start_named("workbookPr").is_none(),
                "workbookPr 중복",
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
        let (source_fingerprint, source_parts) = ZipPkg(source_file.file, source_xlsx).read()?;
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
        let mut root_ids = [const { None }; ROOT_RELS.len()];
        validate_relationship_set(
            container.text("_rels/.rels")?,
            "_rels/.rels",
            &ROOT_RELS,
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
        let (input_styles, canonical_styles) = self.prepare_style_map()?;
        let source_core = self.part_mut("docProps/core.xml");
        let core_in =
            str::from_utf8(source_core).map_err(|src| err_src("core.xml UTF-8 실패", src))?;
        let mut core_xml = str_cap(core_in.len().strict_add(1), "core.xml")?;
        core_xml.push_str(concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n",
            "<cp:coreProperties xmlns:cp=\"http://schemas.openxmlformats.org/package/2006/metadata/core-properties\" xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" xmlns:dcmitype=\"http://purl.org/dc/dcmitype/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">",
        ));
        let mut core_vals = [None; CORE_PROPS.len()];
        let (mut core_sc, core_root) = scan_xml_root(core_in, "cp:coreProperties", "core.xml")?;
        while let Some(el) = core_sc.next_direct_element_until(&core_root, "core.xml")? {
            if let Some((&qual, slot)) = CORE_PROPS
                .iter()
                .zip(&mut core_vals)
                .find(|&(&nm, _)| el.opening.name == nm)
            {
                ensure_fmt!(slot.is_none(), "core.xml {qual} 중복");
                ensure_fmt!(!el.body.contains('<'), "core.xml {qual} 본문 오류");
                decode_xml_entities(el.body)
                    .map_err(|src| fmt_src!(src, "core.xml {qual} 본문 오류"))?;
                *slot = Some(el.body);
            }
        }
        for (name, val) in CORE_PROPS.into_iter().zip(core_vals) {
            if let Some(body) = val {
                let extra = if name.starts_with("dcterms:") {
                    " xsi:type=\"dcterms:W3CDTF\""
                } else {
                    ""
                };
                core_xml.extend(["<", name, extra, ">", body, "</", name, ">"]);
            }
        }
        core_xml.push_str("</cp:coreProperties>");
        *source_core = core_xml.into_bytes();
        let source_app = self.part_mut("docProps/app.xml");
        let app_in =
            str::from_utf8(source_app).map_err(|src| err_src("app.xml UTF-8 실패", src))?;
        let (mut app_sc, app_root) = scan_xml_root(app_in, "Properties", "app.xml")?;
        let mut total_time = None;
        while let Some(el) = app_sc.next_direct_element_until(&app_root, "app.xml")? {
            ensure(
                el.opening.name != "TotalTime" || total_time.replace(el.body).is_none(),
                "TotalTime 중복",
            )?;
        }
        let mut app_xml = str_cap(
            960_usize.strict_add(total_time.map_or(0, str::len)),
            "app.xml",
        )?;
        app_xml.push_str(concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\r\n",
            "<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\" xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\">",
        ));
        if let Some(body) = total_time {
            let val = decode_xml_entities(body)?
                .trim_matches([' ', '\t', '\r', '\n'])
                .parse::<i32>()
                .map_err(|src| err_src("TotalTime 해석 실패", src))?;
            ensure(val >= 0_i32, "TotalTime 음수 불가")?;
            app_xml.extend(["<TotalTime>", body, "</TotalTime>"]);
        }
        app_xml.extend(["<Application>", crate::APP_NAME, "</Application><HeadingPairs><vt:vector size=\"2\" baseType=\"variant\"><vt:variant><vt:lpstr>워크시트</vt:lpstr></vt:variant><vt:variant><vt:i4>2</vt:i4></vt:variant></vt:vector></HeadingPairs><TitlesOfParts><vt:vector size=\"2\" baseType=\"lpstr\"><vt:lpstr>유류비</vt:lpstr><vt:lpstr>변경내역</vt:lpstr></vt:vector></TitlesOfParts></Properties>"]);
        *source_app = app_xml.into_bytes();
        for (idx, (name, role, _)) in XLSX_PARTS.into_iter().enumerate() {
            let slot = or_abort(self.parts.get_mut(idx));
            if role == PartRole::InputOnly {
                *slot = None;
                continue;
            }
            let canon = match name {
                "[Content_Types].xml" => Some(EXCEL_CT_XML),
                "_rels/.rels" => Some(EXCEL_ROOT_RELS_XML),
                "xl/_rels/workbook.xml.rels" => Some(EXCEL_WB_RELS_XML),
                "xl/styles.xml" if !canonical_styles => Some(EXCEL_STYLES_XML),
                "xl/theme/theme1.xml" => Some(EXCEL_THEME_XML),
                _ => None,
            };
            match canon {
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
    fn prepare_style_map(&self) -> Result<(CanonicalStyleMap, bool)> {
        let src_styles = self.text("xl/styles.xml")?;
        if EXCEL_STYLES_XML
            .trim_suffix('\n')
            .split_once('\n')
            .is_some_and(|(decl, sheet)| {
                src_styles
                    .strip_prefix(decl)
                    .and_then(|tail| tail.strip_prefix("\r\n"))
                    == Some(sheet)
            })
        {
            let mut mapping = vec_cap(27, "style mapping")?;
            mapping.extend((0..27_u32).map(Some));
            return Ok((mapping, true));
        }
        validate_spreadsheet_xml_document(src_styles, "styleSheet", "styles.xml", true, None)?;
        let src_xfs = style_entries(src_styles, "cellXfs", "xf")?;
        let cat = StyleCatalog {
            ex_borders: style_entries(EXCEL_STYLES_XML, "borders", "border")?,
            ex_fonts: style_entries(EXCEL_STYLES_XML, "fonts", "font")?,
            ex_xfs: style_entries(EXCEL_STYLES_XML, "cellXfs", "xf")?,
            src_borders: style_entries(src_styles, "borders", "border")?,
            src_fonts: style_entries(src_styles, "fonts", "font")?,
        };
        let mut mapping = vec_cap(src_xfs.len(), "style mapping")?;
        for source in src_xfs {
            let resolved = cat.match_excel_xf(source)?.or_else(|| {
                let mut sc = XmlScanner::new(source);
                let mut key = [0_u8; 6];
                let (xf, al, pr, cl) = (
                    sc.next_tag()?,
                    sc.next_tag()?,
                    sc.next_tag()?,
                    sc.next_tag()?,
                );
                if !xf.is_start
                    || xf.name != "xf"
                    || xf.self_closing
                    || !al.is_start
                    || al.name != "alignment"
                    || !al.self_closing
                    || !pr.is_start
                    || pr.name != "protection"
                    || !pr.self_closing
                    || cl.is_start
                    || cl.name != "xf"
                    || sc.next_tag().is_some()
                {
                    return None;
                }
                let (mut xa, mut aa, mut pa) = (
                    [const { None }; 9],
                    [const { None }; 6],
                    [const { None }; 2],
                );
                XmlAttrScanner::parse_into(
                    xf.raw,
                    &[
                        "numFmtId",
                        "fontId",
                        "fillId",
                        "borderId",
                        "xfId",
                        "applyFont",
                        "applyBorder",
                        "applyAlignment",
                        "applyProtection",
                    ],
                    &mut xa,
                    "",
                )
                .ok()?;
                XmlAttrScanner::parse_into(
                    al.raw,
                    &[
                        "horizontal",
                        "vertical",
                        "textRotation",
                        "wrapText",
                        "indent",
                        "shrinkToFit",
                    ],
                    &mut aa,
                    "",
                )
                .ok()?;
                XmlAttrScanner::parse_into(pr.raw, &["locked", "hidden"], &mut pa, "").ok()?;
                for (slot, val) in key.iter_mut().take(5).zip(&xa) {
                    let tx = val.as_deref()?;
                    if tx.len() > 1 && tx.starts_with('0') {
                        return None;
                    }
                    *slot = tx.parse::<u8>().ok()?;
                }
                for (shift, val) in xa.get(5..9)?.iter().enumerate() {
                    key[5] |= u8::from(parse_bool_attr(val.as_deref()?)?)
                        << or_abort(u8::try_from(shift).ok());
                }
                key[5] |= match aa[0].as_deref()? {
                    "general" => 0,
                    "center" => 64,
                    "left" => 128,
                    _ => return None,
                };
                key[5] |= match aa[1].as_deref()? {
                    "center" => 0,
                    "bottom" => 16,
                    _ => return None,
                };
                key[5] |= u8::from(parse_bool_attr(aa[3].as_deref()?)?) << 5_u8;
                if aa[2].as_deref() != Some("0")
                    || aa[4].as_deref() != Some("0")
                    || parse_bool_attr(aa[5].as_deref()?) != Some(false)
                    || parse_bool_attr(pa[0].as_deref()?) != Some(true)
                    || parse_bool_attr(pa[1].as_deref()?) != Some(false)
                {
                    return None;
                }
                LO_XFS
                    .as_chunks::<6>()
                    .0
                    .iter()
                    .position(|cand| cand == &key)
                    .filter(|&idx| idx != 7)
                    .and_then(|idx| LO_STYLE_MAP.get(idx))
                    .copied()
                    .map(u32::from)
            });
            mapping.push(resolved);
        }
        Ok((mapping, false))
    }
    pub(super) fn put_text(&mut self, name: &str, content: String) {
        *self.part_mut(name) = content.into_bytes();
    }
    pub(super) fn save(mut self, target_xlsx: &Path, verification: SaveVerification) -> Result<()> {
        let mut archive_parts = vec_cap(XLSX_PARTS.len(), "package part 목록")?;
        for ((name, _, _), slot) in XLSX_PARTS.into_iter().zip(&mut self.parts) {
            if let Some(bytes) = slot.take() {
                archive_parts.push(PackagePart { bytes, name });
            }
        }
        let parent = target_xlsx
            .parent()
            .filter(|path| !path.is_empty())
            .unwrap_or_else(|| Path::new("."));
        let reserve = |prefix: &str, label: &str| -> Result<ReservedTempArchive> {
            let pid = process::id();
            let nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|src| path_err(label, target_xlsx, io::Error::other(src)))?
                .as_nanos();
            for seq in 0_u32..1024_u32 {
                let path = parent.join(format!("{prefix}{pid}_{nanos}_{seq}"));
                let mut opts = fs::File::options();
                opts.read(true).write(true).create_new(true);
                configure_no_follow(&mut opts);
                #[cfg(target_os = "windows")]
                configure_replaceable_file(&mut opts);
                #[cfg(any(target_os = "linux", target_os = "macos"))]
                opts.mode(0o600);
                match opts.open(&path) {
                    Ok(file) => {
                        let (_, identity) = validate_regular_file(&file)
                            .map_err(|src| path_err(label, target_xlsx, src))?;
                        return Ok(ReservedTempArchive {
                            file: Some(file),
                            identity,
                            path,
                            remove_on_drop: true,
                        });
                    }
                    Err(err_io) if err_io.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(err_io) => return Err(path_err(label, target_xlsx, err_io)),
                }
            }
            Err(path_err(
                label,
                target_xlsx,
                io::Error::new(io::ErrorKind::AlreadyExists, "임시 이름 충돌 반복"),
            ))
        };
        let mut tmp_archive = reserve(
            ".fuel_cost_chungcheong.xlsx.tmp_",
            "임시 저장 파일 생성 실패",
        )?;
        #[cfg(target_os = "windows")]
        let mut backup_archive = reserve(
            ".fuel_cost_chungcheong.xlsx.backup_",
            "교체 예약 파일 생성 실패",
        )?;
        let result = (|| -> Result<()> {
            ZipOut(
                tmp_archive.path.as_path(),
                or_abort(tmp_archive.file.as_mut()),
                &archive_parts,
                #[cfg(any(target_os = "linux", target_os = "macos"))]
                self.source_permissions,
            )
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
            && let Err(rm_err) = tmp_archive.remove_owned()
        {
            return Err(fmt_src!(
                source,
                "xlsx 임시 파일 삭제 실패: {} ({rm_err})",
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
            .map_err(|src| fmt_src!(src, "xlsx part UTF-8 실패: {name}"))
    }
    fn take_workbook_dependencies(&mut self, workbook_xml: &str) -> Result<()> {
        let wb_rels = self.take_text("xl/_rels/workbook.xml.rels")?;
        let mut rel_ids = [const { None }; WB_RELS.len()];
        validate_relationship_set(&wb_rels, "workbook.xml.rels", &WB_RELS, &mut rel_ids, self)?;
        let [_, cl_rid, ms_rid, _, _, _] = rel_ids;
        let sheet_ids = [
            ms_rid.as_deref().ok_or("유류비 relationship Id 없음")?,
            cl_rid.as_deref().ok_or("변경내역 relationship Id 없음")?,
        ];
        let mut sc = XmlScanner::new(workbook_xml);
        sc.next_start_named("sheets")
            .ok_or("workbook.xml sheets 태그 없음")?;
        for ((exp_name, exp_id), exp_rid) in
            [(MASTER_SHEET_NAME, "1"), (CHANGE_LOG_SHEET_NAME, "2")]
                .into_iter()
                .zip(sheet_ids)
        {
            let tag = sc
                .next_start_named("sheet")
                .filter(|tg| tg.name == "sheet" && tg.self_closing)
                .ok_or("workbook sheet 태그 오류")?;
            let mut attrs = [None, None, None, None];
            XmlAttrScanner::parse_into(
                tag.raw,
                &["name", "sheetId", "state", "r:id"],
                &mut attrs,
                "sheet",
            )?;
            let [name, sheet_id, state, rid] = attrs;
            (name.as_deref() == Some(exp_name)
                && sheet_id.as_deref() == Some(exp_id)
                && rid.as_deref() == Some(exp_rid)
                && state.as_deref().is_none_or(|val| val == "visible"))
            .ok_or("workbook.xml sheet 구성 불일치")?;
        }
        ensure(
            sc.next_start_named("sheet").is_none(),
            "workbook sheet 수 초과",
        )
    }
    pub(super) fn take_worksheet_text(&mut self, name: &str) -> Result<String> {
        let drawing_rid = if name == super::MASTER_SHEET_PATH {
            let has_rels = self.has_part("xl/worksheets/_rels/sheet1.xml.rels");
            ensure(
                has_rels == self.has_part("xl/drawings/drawing1.xml"),
                "drawing 관계 불일치",
            )?;
            if has_rels {
                let rels_xml = self.take_text("xl/worksheets/_rels/sheet1.xml.rels")?;
                let mut rels = [const { None }; SHEET_RELS.len()];
                validate_relationship_set(
                    &rels_xml,
                    "sheet1.xml.rels",
                    &SHEET_RELS,
                    &mut rels,
                    self,
                )?;
                let rid = rels
                    .first_mut()
                    .and_then(Option::take)
                    .ok_or("drawing Id 없음")?
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
                        ("xmlns:r", OFFICE_DOC_REL_NS),
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
        let mut sc = XmlScanner::new(&xml);
        match (drawing_rid, sc.next_start_named("drawing")) {
            (Some(exp_rid), Some(tag)) if tag.name == "drawing" && tag.self_closing => {
                validate_exact_attrs(
                    tag.raw,
                    &[("r:id", exp_rid.as_str())],
                    format_args!("drawing"),
                )?;
                let span = tag.start..tag.end.strict_add(1);
                sc.skip_to(span.end);
                ensure(
                    sc.next_start_named("drawing").is_none(),
                    "drawing 참조 중복",
                )?;
                xml.replace_range(span, "");
            }
            (Some(_), None) => bail!("sheet1.xml 빈 drawing 참조 없음"),
            (None | Some(_), Some(_)) => bail!("대응하는 drawing part 없는 참조"),
            (None, None) => {}
        }
        Ok(xml)
    }
    fn text(&self, name: &str) -> Result<&str> {
        let bytes = or_abort(self.parts.get(part_index(name)).and_then(Option::as_deref));
        str::from_utf8(bytes).map_err(|src| fmt_src!(src, "xlsx part UTF-8 실패: {name}"))
    }
    fn validate_content_types(&self) -> Result<()> {
        let ct_xml = self.text("[Content_Types].xml")?;
        let (mut seen_def, mut seen_ovr) = ([false; CT_DEFAULTS.len()], [false; XLSX_PARTS.len()]);
        visit_direct_xml_children(
            ct_xml,
            "Types",
            "http://schemas.openxmlformats.org/package/2006/content-types",
            "[Content_Types].xml",
            &mut |local, raw| {
                let (is_def, key_name) = match local {
                    "Default" => (true, "Extension"),
                    "Override" => (false, "PartName"),
                    _ => bail_fmt!("[Content_Types].xml 미지원 항목: {local}"),
                };
                let mut attrs = [None, None];
                XmlAttrScanner::parse_into(
                    raw,
                    &[key_name, "ContentType"],
                    &mut attrs,
                    "Content_Types",
                )?;
                let [key_a, ct_a] = attrs;
                let key = key_a.ok_or_else(|| fmt_err!("Content_Types {key_name} 없음"))?;
                let ct = ct_a.ok_or("Content_Types ContentType 없음")?;
                let slot = if is_def {
                    CT_DEFAULTS
                        .iter()
                        .zip(&mut seen_def)
                        .find_map(|(&(cand, val, _), flag)| {
                            (cand == key && val == ct).then_some(flag)
                        })
                } else {
                    XLSX_PARTS
                        .iter()
                        .zip(&mut seen_ovr)
                        .find_map(|(&(cand, _, val), flag)| {
                            (key.strip_prefix('/') == Some(cand) && val == Some(ct.as_ref()))
                                .then_some(flag)
                        })
                };
                let Some(seen) = slot else {
                    bail_fmt!("[Content_Types].xml 미지원 {local}: {key}");
                };
                ensure_fmt!(
                    is_def || key.strip_prefix('/').is_some_and(|nm| self.has_part(nm)),
                    "[Content_Types].xml Override 대상 없음: {key}"
                );
                ensure_fmt!(!mem::replace(seen, true), "[Content_Types].xml 중복: {key}");
                Ok(())
            },
        )?;
        for (&(key, _, role), present) in CT_DEFAULTS.iter().zip(seen_def) {
            let req = role == PartRole::Required
                || role == PartRole::OptionalInput && self.has_part("docProps/thumbnail.emf");
            ensure_fmt!(!req || present, "[Content_Types].xml 필수 항목 없음: {key}");
        }
        for (&(part, _, ct), present) in XLSX_PARTS.iter().zip(seen_ovr) {
            ensure_fmt!(
                ct.is_none() || !self.has_part(part) || present,
                "[Content_Types].xml 필수 없음: /{part}"
            );
        }
        Ok(())
    }
}
fn parse_bool_attr(value: &str) -> Option<bool> {
    match value {
        "0" | "false" => Some(false),
        "1" | "true" => Some(true),
        _ => None,
    }
}
fn part_index(name: &str) -> usize {
    or_abort(XLSX_PARTS.iter().position(|&(cand, _, _)| cand == name))
}
#[cfg(any(target_os = "linux", target_os = "macos"))]
fn path_to_c_string(path: &Path) -> io::Result<CString> {
    CString::new(path.as_os_str().as_bytes())
        .map_err(|src| io::Error::new(io::ErrorKind::InvalidInput, src))
}
#[cfg(target_os = "windows")]
fn path_to_wide(path: &Path) -> io::Result<Vec<u16>> {
    let abs = absolute(path)?;
    let invalid = |msg| io::Error::new(io::ErrorKind::InvalidInput, msg);
    let (prefix, skip) = match abs.components().next() {
        Some(Component::Prefix(comp)) => match comp.kind() {
            Prefix::Disk(_) => (r"\\?\", 0_usize),
            Prefix::UNC(_, _) => (r"\\?\UNC\", 2_usize),
            Prefix::Verbatim(_)
            | Prefix::VerbatimUNC(_, _)
            | Prefix::VerbatimDisk(_)
            | Prefix::DeviceNS(_) => ("", 0_usize),
        },
        _ => return Err(invalid("Windows file path is not absolute")),
    };
    let cap = prefix
        .len()
        .checked_add(abs.as_os_str().len())
        .and_then(|len| len.checked_add(1))
        .ok_or_else(|| invalid("Windows file path length overflow"))?;
    let mut wide = Vec::new();
    wide.try_reserve_exact(cap).map_err(io::Error::other)?;
    wide.extend(prefix.encode_utf16());
    let mut seen = 0_usize;
    for unit in abs.as_os_str().encode_wide() {
        (unit != 0).ok_or_else(|| invalid("Windows file path contains a NUL character"))?;
        if seen >= skip {
            wide.push(unit);
        }
        seen = seen.strict_add(1);
    }
    (seen >= skip).ok_or_else(|| invalid("Windows UNC path is invalid"))?;
    wide.push(0);
    Ok(wide)
}
fn required_xml_attr<'tag>(
    tag: &'tag str,
    attr_name: &str,
    context: &str,
) -> Result<Cow<'tag, str>> {
    let mut value = None;
    let mut attrs = XmlAttrScanner::new(tag)?;
    while let Some((name, val)) = attrs.next()? {
        ensure_fmt!(
            name != attr_name || value.replace(val).is_none(),
            "{context} 중복 {attr_name} 속성"
        );
    }
    value.ok_or_else(|| fmt_err!("{context} {attr_name} 속성 없음"))
}
fn scan_xml_root<'xml>(
    xml: &'xml str,
    expected_name: &str,
    context: &str,
) -> Result<(XmlScanner<'xml>, XmlTag<'xml>)> {
    let mut sc = XmlScanner::new(xml);
    let root = sc
        .next_tag()
        .filter(|tag| tag.is_start && tag.name == expected_name)
        .ok_or_else(|| fmt_err!("{context} XML root 태그 오류"))?;
    ensure_fmt!(
        xml_misc_only(or_abort(xml.get(..root.start)), true),
        "{context} XML root 앞 내용 오류"
    );
    Ok((sc, root))
}
fn style_entries<'text>(
    styles_xml: &'text str,
    group_name: &str,
    entry_name: &str,
) -> Result<Vec<&'text str>> {
    let mut sc = XmlScanner::new(styles_xml);
    let el = sc
        .next_element_named(group_name)?
        .filter(|item| item.opening.name == group_name && !item.opening.self_closing)
        .ok_or_else(|| fmt_err!("styles.xml {group_name} 태그 오류"))?;
    let declared = required_xml_attr(el.opening.raw, "count", group_name)?
        .parse::<usize>()
        .map_err(|src| fmt_src!(src, "styles.xml {group_name} count 해석 실패"))?;
    let cap = declared.min(el.body.len().div_euclid(entry_name.len().strict_add(3)));
    let mut entries = vec_cap(cap, "styles.xml 항목")?;
    let mut child_sc = XmlScanner::new(el.body);
    while let Some(entry) = child_sc.next_direct_element(group_name)? {
        ensure_fmt!(
            entry.opening.name == entry_name,
            "styles.xml {group_name} 미지원 요소"
        );
        push_vec(
            &mut entries,
            or_abort(el.body.get(entry.span)),
            1,
            "styles.xml 항목",
        )?;
    }
    ensure_fmt!(
        sc.next_start_named(group_name).is_none(),
        "styles.xml {group_name} 중복"
    );
    ensure_fmt!(
        entries.len() == declared,
        "styles.xml {group_name} count 불일치"
    );
    Ok(entries)
}
cfg_select! {
    target_os = "windows" => {
        fn swap_files(target: &Path, temp: &Path, backup: &Path, rollback: bool) -> CoreResult<(), ReplaceFilesError> {
            let (incoming, backup_out) = if rollback { (backup, temp) } else { (temp, backup) };
            let (target_w, incoming_w, backup_w) = (path_to_wide(target)?, path_to_wide(incoming)?, path_to_wide(backup_out)?);
            // SAFETY: all three paths are valid NUL-terminated UTF-16 buffers and reserved pointers
            // are null as required by ReplaceFileW.
            let status = unsafe { ReplaceFileW(target_w.as_ptr(), incoming_w.as_ptr(), backup_w.as_ptr(), 0, null(), null()) };
            if status != 0_i32 {
                return Ok(());
            }
            let replace = io::Error::last_os_error();
            if replace.raw_os_error() != Some(1_177_i32) {
                return Err(replace.into());
            }
            match fs::rename(backup, target) {
                Ok(()) => Err(ReplaceFilesError::Restored(ReplaceFailure { replace, restore: None })),
                Err(restore) => Err(ReplaceFilesError::RecoveryRequired(ReplaceFailure { replace, restore: Some(restore) })),
            }
        }
    }
    any(target_os = "linux", target_os = "macos") => {
        fn swap_files(target: &Path, replacement: &Path) -> CoreResult<(), ReplaceFilesError> {
            let (target_c, repl_c) = (path_to_c_string(target)?, path_to_c_string(replacement)?);
            let status = cfg_select! {
                target_os = "linux" => {
                    // SAFETY: both paths are valid NUL-terminated byte strings and AT_FDCWD selects
                    // the current process path resolution context.
                    unsafe { renameat2(-100, target_c.as_ptr(), -100, repl_c.as_ptr(), 2) }
                }
                target_os = "macos" => {
                    // SAFETY: both paths are valid NUL-terminated byte strings and RENAME_SWAP
                    // requests an atomic exchange of two existing paths.
                    unsafe { renamex_np(target_c.as_ptr(), repl_c.as_ptr(), 2) }
                }
            };
            (status == 0_i32).ok_or_else(|| io::Error::last_os_error().into())
        }
    }
    _ => {
        compile_error!("unsupported OS");
    }
}
fn validate_empty_xml_root(
    xml: &str,
    expected_name: &str,
    expected_attrs: &[(&str, &str)],
    context: &str,
) -> Result<()> {
    let (mut sc, root) = scan_xml_root(xml, expected_name, context)?;
    validate_exact_attrs(root.raw, expected_attrs, format_args!("{context}"))?;
    sc.next_direct_element_until(&root, context)?
        .is_none()
        .ok_or_else(|| fmt_err!("{context} XML root 비어 있지 않음"))
}
fn validate_exact_attrs(
    tag: &str,
    expected: &[(&str, &str)],
    context: Arguments<'_>,
) -> Result<()> {
    let (mut seen, mut attrs) = (0_u32, XmlAttrScanner::new(tag)?);
    while let Some((name, val)) = attrs.next()? {
        let Some((idx, &(_, exp_val))) = expected
            .iter()
            .enumerate()
            .find(|&(_, &(cand, _))| cand == name)
        else {
            bail_fmt!("{context} 알 수 없는 {name} 속성");
        };
        let bit = 1_u32
            .checked_shl(u32::try_from(idx).unwrap_or(u32::MAX))
            .unwrap_or(0);
        ensure_fmt!(seen & bit == 0, "{context} {name} 속성 중복");
        seen |= bit;
        ensure_fmt!(val.as_ref() == exp_val, "{context} 속성 불일치");
    }
    let exp_mask = 1_u32
        .checked_shl(u32::try_from(expected.len()).unwrap_or(u32::MAX))
        .unwrap_or(0)
        .strict_sub(1);
    (seen == exp_mask).ok_or_else(|| fmt_err!("{context} 속성 불일치"))
}
fn validate_relationship_set<'xml>(
    xml: &'xml str,
    context: &str,
    expected: &[RelSpec],
    ids: &mut [Option<Cow<'xml, str>>],
    container: &XlsxContainer,
) -> Result<()> {
    visit_direct_xml_children(
        xml,
        "Relationships",
        "http://schemas.openxmlformats.org/package/2006/relationships",
        context,
        &mut |local, raw| {
            ensure_fmt!(local == "Relationship", "{context} 관계 태그 오류");
            let mut attrs = [None, None, None];
            XmlAttrScanner::parse_into(raw, &["Id", "Type", "Target"], &mut attrs, context)?;
            let [id_a, type_a, target_a] = attrs;
            let id = id_a.ok_or_else(|| fmt_err!("{context} 관계 Id 없음"))?;
            ensure_fmt!(!id.is_empty(), "{context} 관계 Id 비어 있음");
            let rtype = type_a.ok_or_else(|| fmt_err!("{context} 관계 Type 없음"))?;
            let target = target_a.ok_or_else(|| fmt_err!("{context} 관계 Target 없음"))?;
            ensure_fmt!(
                !ids.iter().flatten().any(|cand| cand == id.as_ref()),
                "{context} Id 중복: {id}"
            );
            let Some((&(opt_part, _, _), slot)) = expected
                .iter()
                .zip(ids.iter_mut())
                .find(|&(&(_, exp_t, exp_ty), _)| rtype == exp_ty && target == exp_t)
            else {
                bail_fmt!("{context} 미지원 관계: {rtype} -> {target}");
            };
            ensure_fmt!(
                opt_part.is_none_or(|part| container.has_part(part)),
                "{context} 대상 없음: {target}"
            );
            ensure_fmt!(slot.replace(id).is_none(), "{context} 관계 중복: {target}");
            Ok(())
        },
    )?;
    for (&(opt_part, target, _), id) in expected.iter().zip(ids.iter()) {
        ensure_fmt!(
            opt_part.is_none_or(|part| container.has_part(part)) == id.is_some(),
            "{context} 필수 관계 구성 오류: {target}"
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
    let (mut sc, root) = scan_xml_root(xml, expected_root, context)?;
    ensure_fmt!(!root.self_closing, "{context} XML root 비어 있음");
    ensure_fmt!(
        required_xml_attr(root.raw, "xmlns", context)?.as_ref() == SPREADSHEETML_NAMESPACE,
        "{context} root namespace 오류"
    );
    let mut ancestors = [root.name; MAX_XML_NESTING_DEPTH];
    let (mut depth, mut sd_seen) = (1_usize, false);
    while let Some(tag) = sc.next_tag() {
        if tag.is_start {
            ensure_fmt!(
                allow_extensions || tag.name == tag.local_name,
                "{context} prefixed element 미지원"
            );
            let mut attrs = XmlAttrScanner::from_tag(&tag);
            while let Some((name, val)) = attrs.next()? {
                if (name == "xmlns" || !allow_extensions && name.starts_with("xmlns:"))
                    && !(tag.name == "sortState"
                        && name == "xmlns:xlrd2"
                        && val.as_ref()
                            == "http://schemas.microsoft.com/office/spreadsheetml/2017/richdata2")
                {
                    bail_fmt!("{context} descendant namespace 재정의 미지원");
                }
            }
            if expected_root == "worksheet" && tag.local_name == "sheetData" {
                ensure_fmt!(depth == 1 && !sd_seen, "{context} sheetData 위치/개수 오류");
                sd_seen = true;
                if let Some(span) = parsed_sheet_data {
                    ensure_fmt!(
                        !tag.self_closing
                            && tag.start == span.start
                            && span.end > tag.end
                            && xml.get(span.start..span.end).is_some(),
                        "{context} sheetData 범위 오류"
                    );
                    sc.skip_to(span.end);
                    continue;
                }
            }
            if !tag.self_closing {
                *ancestors
                    .get_mut(depth)
                    .ok_or_else(|| fmt_err!("{context} XML 중첩 깊이 초과"))? = tag.name;
                depth = depth.strict_add(1);
            }
            continue;
        }
        depth = depth.strict_sub(1);
        let open = or_abort(ancestors.get(depth).copied());
        ensure_fmt!(
            open == tag.name,
            "{context} XML 태그 불일치: {open} / {}",
            tag.name
        );
        if depth == 0 {
            ensure_fmt!(
                expected_root != "worksheet" || sd_seen,
                "{context} sheetData 없음"
            );
            ensure_fmt!(
                xml.get(tag.end.strict_add(1)..)
                    .is_some_and(|tail| xml_misc_only(tail, false)),
                "{context} XML root 뒤 내용 오류"
            );
            return Ok(root);
        }
    }
    Err(fmt_err!("{context} 닫히지 않은 XML 요소"))
}
fn visit_direct_xml_children<'xml>(
    xml: &'xml str,
    root_local_name: &str,
    expected_namespace: &str,
    context: &str,
    visit: &mut dyn FnMut(&'xml str, &'xml str) -> Result<()>,
) -> Result<()> {
    let (mut sc, root) = scan_xml_root(xml, root_local_name, context)?;
    ensure_fmt!(!root.self_closing, "{context} XML root 비어 있음");
    validate_exact_attrs(
        root.raw,
        &[("xmlns", expected_namespace)],
        format_args!("{context} root"),
    )?;
    while let Some(child) = sc.next_direct_element_until(&root, context)? {
        let tag = child.opening;
        ensure_fmt!(
            tag.name == tag.local_name,
            "{context} prefixed child 미지원: {}",
            tag.name
        );
        ensure_fmt!(
            xml_misc_only(child.body, false),
            "{context} XML child 내용 오류"
        );
        visit(tag.local_name, tag.raw)?;
    }
    Ok(())
}
