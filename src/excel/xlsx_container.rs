use super::{
    SheetInfo, ZipArchiveBuilder, ZipArchiveExtractor,
    path_util::path_to_slashes,
    xml::{extract_attr, find_start_tag, find_tag_end},
};
use crate::diagnostic::{
    Result, err, err_with_source, path_context_message, path_pair_context_message,
    path_source_message, prefixed_message,
};
use alloc::borrow::Cow;
use core::{mem, time::Duration};
use std::{
    collections::{HashMap, hash_map::Entry as HashEntry},
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
impl SavedArchiveVerifier<'_> {
    fn verify(&self) -> Result<()> {
        let container = XlsxContainer::open(self.saved_archive).map_err(|source_err| {
            source_err.prepend_context(path_context_message(
                "저장 검증 실패: 저장 직후 압축 해제 점검에 실패했습니다",
                self.saved_archive,
            ))
        })?;
        container
            .read_text("[Content_Types].xml")
            .map_err(|source_err| {
                source_err.prepend_context(path_context_message(
                    "저장 검증 실패: 필수 OOXML 파트 읽기 실패",
                    self.saved_archive,
                ))
            })?;
        super::writer::Workbook::from_container(container).map_err(|source_err| {
            source_err.prepend_context(path_context_message(
                "저장 검증 실패: 저장 직후 재열기 점검에 실패했습니다",
                self.saved_archive,
            ))
        })?;
        Ok(())
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
                                .unwrap_or_else(|| Path::new("."));
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
        let mut rid_to_target: HashMap<Cow<'_, str>, Cow<'_, str>> = HashMap::new();
        rid_to_target
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
            match rid_to_target.entry(id) {
                HashEntry::Vacant(entry) => {
                    entry.insert(target);
                }
                HashEntry::Occupied(entry) => {
                    return Err(err(format!(
                        "workbook.xml.rels에 중복 Relationship Id가 있습니다: {}",
                        entry.key().as_ref()
                    )));
                }
            }
            let Some(next_cursor) = rel_end.checked_add(1) else {
                return Err(err("다음 workbook Relationship 위치 계산에 실패했습니다."));
            };
            rels_cursor = next_cursor;
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
            let Some(target) = rid_to_target.get(rid.as_ref()) else {
                return Err(err(format!(
                    "workbook.xml.rels에서 sheet 관계 target을 찾지 못했습니다: {}",
                    rid.as_ref()
                )));
            };
            let target_text = target.as_ref();
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
            let Some(next_cursor) = sheet_end.checked_add(1) else {
                return Err(err("workbook.xml의 다음 sheet 위치 계산에 실패했습니다."));
            };
            sheet_cursor = next_cursor;
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
    pub(super) fn read_text(&self, relative_path: &str) -> Result<String> {
        let path = self.resolve_relative_path(relative_path)?;
        let file = fs::File::open(&path).map_err(|source_err| {
            err_with_source(path_context_message("파일 열기 실패", &path), source_err)
        })?;
        let file_size = file
            .metadata()
            .map_err(|source_err| {
                err_with_source(
                    path_context_message("파일 메타데이터 조회 실패", &path),
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
            err_with_source(path_context_message("파일 읽기 실패", &path), source_err)
        })?;
        if u64::try_from(bytes.len()).is_ok_and(|actual| actual > MAX_XLSX_TEXT_PART_BYTES) {
            return Err(err(format!(
                "xlsx XML part가 너무 큽니다: {} (최대 {MAX_XLSX_TEXT_PART_BYTES} bytes)",
                path.display()
            )));
        }
        String::from_utf8(bytes).map_err(|source| {
            err_with_source(path_context_message("파일 UTF-8 해석 실패", &path), source)
        })
    }
    pub(super) fn remove_file_if_exists(&self, relative_path: &str) -> Result<()> {
        let path = self.resolve_relative_path(relative_path)?;
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
    pub(super) fn save(&self, target_xlsx: &Path) -> Result<()> {
        let parent = if let Some(parent) = target_xlsx.parent() {
            create_dir_all_checked(parent, "저장 폴더 생성 실패")?;
            parent
        } else {
            Path::new(".")
        };
        let file_name = target_xlsx
            .file_name()
            .and_then(|file_name_os| file_name_os.to_str())
            .unwrap_or("workbook.xlsx");
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
            SavedArchiveVerifier {
                saved_archive: &tmp_archive,
            }
            .verify()?;
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
                Err(error) => Err(source.prepend_context(path_source_message(
                    "xlsx 임시 저장 파일 삭제 실패",
                    &tmp_archive,
                    error,
                ))),
            },
        }
    }
    pub(super) fn unpack_dir(&self) -> &Path {
        &self.unpack_dir
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
