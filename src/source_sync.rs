use crate::{Result, err};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet, hash_map::Entry},
    fs,
    path::{Path, PathBuf},
};
#[derive(Debug, Clone)]
pub struct SourceRecord {
    pub region: String,
    pub name: String,
    pub brand: String,
    pub self_yn: String,
    pub address: String,
    pub gasoline: Option<i32>,
    pub premium: Option<i32>,
    pub diesel: Option<i32>,
}
type SourcePriority = (usize, usize, usize);
type SourceIndexEntry = (SourceRecord, SourcePriority, usize);
const MAX_CONFLICT_SAMPLES: usize = 10;
#[derive(Debug, Clone)]
pub struct SourceConflictSample {
    pub address: String,
    pub previous_source: String,
    pub incoming_source: String,
    pub selected_source: String,
}
#[derive(Debug, Clone, Default)]
pub struct SourceIndexBuildReport {
    pub duplicate_addresses: usize,
    pub replaced_entries: usize,
    pub samples: Vec<SourceConflictSample>,
}
#[derive(Debug, Clone)]
pub struct SourceIndexBuildResult {
    pub index: HashMap<String, SourceRecord>,
    pub report: SourceIndexBuildReport,
}
pub fn find_source_files(dir: &Path, prefix: &str) -> Result<Vec<PathBuf>> {
    let mut auto_candidates = Vec::new();
    let mut manual_candidates = Vec::new();
    let prefix_fold = prefix.to_lowercase();
    for entry in
        fs::read_dir(dir).map_err(|e| err(format!("폴더 읽기 실패: {} ({e})", dir.display())))?
    {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let ext = path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or_default();
        if !(ext.eq_ignore_ascii_case("xls") || ext.eq_ignore_ascii_case("xlsx")) {
            continue;
        }
        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        let file_name_fold = file_name.to_lowercase();
        if !file_name_fold.starts_with(&prefix_fold) {
            continue;
        }
        let is_auto =
            crate::source_download::is_auto_source_file_name_folded(file_name, &prefix_fold);
        let natural_key = split_natural_parts(&file_name_fold);
        let candidate = SourceFileCandidate { path, natural_key };
        if is_auto {
            auto_candidates.push(candidate);
        } else {
            manual_candidates.push(candidate);
        }
    }
    let mut candidates = if auto_candidates.is_empty() {
        manual_candidates
    } else {
        auto_candidates
    };
    candidates.sort_by(|a, b| {
        compare_natural_parts(&a.natural_key, &b.natural_key).then_with(|| a.path.cmp(&b.path))
    });
    Ok(candidates.into_iter().map(|v| v.path).collect())
}
pub fn build_source_index_with_report(paths: &[PathBuf]) -> Result<SourceIndexBuildResult> {
    let mut map: HashMap<String, SourceIndexEntry> = HashMap::new();
    let mut report = SourceIndexBuildReport::default();
    let mut sampled_keys: HashSet<String> = HashSet::new();
    for (file_order, path) in paths.iter().enumerate() {
        let recs = crate::source_download::filter_target_region_records(
            read_source_file(path)
                .map_err(|e| err(format!("소스 파일 읽기 실패: {} ({e})", path.display())))?,
        );
        for rec in recs {
            let key = crate::normalize_address_key(&rec.address);
            let score = source_priority(&rec);
            match map.entry(key) {
                Entry::Vacant(vacant) => {
                    vacant.insert((rec, score, file_order));
                }
                Entry::Occupied(mut occupied) => {
                    let previous = occupied.get();
                    let prev_score = previous.1;
                    let prev_order = previous.2;
                    report.duplicate_addresses = report.duplicate_addresses.saturating_add(1);
                    if report.samples.len() < MAX_CONFLICT_SAMPLES
                        && sampled_keys.insert(occupied.key().clone())
                    {
                        let previous_source = paths
                            .get(prev_order)
                            .map_or_else(|| format!("#{prev_order}"), |p| source_label(p));
                        let incoming_source = source_label(path);
                        let selected_source = if score > prev_score
                            || (score == prev_score && file_order >= prev_order)
                        {
                            incoming_source.clone()
                        } else {
                            previous_source.clone()
                        };
                        report.samples.push(SourceConflictSample {
                            address: rec.address.clone(),
                            previous_source,
                            incoming_source,
                            selected_source,
                        });
                    }
                    if score > prev_score || (score == prev_score && file_order >= prev_order) {
                        report.replaced_entries = report.replaced_entries.saturating_add(1);
                        occupied.insert((rec, score, file_order));
                    }
                }
            }
        }
    }
    let index = map
        .into_iter()
        .map(|(k, (v, _score, _order))| (k, v))
        .collect();
    Ok(SourceIndexBuildResult { index, report })
}
struct SourceFileCandidate {
    path: PathBuf,
    natural_key: Vec<NaturalPart>,
}
fn compare_natural_parts(a_parts: &[NaturalPart], b_parts: &[NaturalPart]) -> Ordering {
    for (a_part, b_part) in a_parts.iter().zip(b_parts) {
        let ord = compare_natural_part(a_part, b_part);
        if ord != Ordering::Equal {
            return ord;
        }
    }
    a_parts.len().cmp(&b_parts.len())
}
#[derive(Debug, Clone, PartialEq, Eq)]
enum NaturalPart {
    Number { normalized: String, raw_len: usize },
    Text(String),
}
impl NaturalPart {
    #[expect(
        clippy::ref_patterns,
        reason = "borrowed enum fields need explicit reference patterns to satisfy pattern_type_mismatch"
    )]
    fn as_number(&self) -> Option<(&str, usize)> {
        match *self {
            Self::Number {
                ref normalized,
                raw_len,
            } => Some((normalized, raw_len)),
            Self::Text(_) => None,
        }
    }
    #[expect(
        clippy::ref_patterns,
        reason = "borrowed enum fields need explicit reference patterns to satisfy pattern_type_mismatch"
    )]
    fn as_text(&self) -> Option<&str> {
        match *self {
            Self::Text(ref text) => Some(text),
            Self::Number { .. } => None,
        }
    }
}
fn split_natural_parts(s: &str) -> Vec<NaturalPart> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut digit_mode: Option<bool> = None;
    for ch in s.chars() {
        let is_digit = ch.is_ascii_digit();
        match digit_mode {
            None => {
                digit_mode = Some(is_digit);
                buf.push(ch);
            }
            Some(mode) if mode == is_digit => buf.push(ch),
            Some(mode) => {
                push_natural_part(&mut out, &buf, mode);
                buf.clear();
                digit_mode = Some(is_digit);
                buf.push(ch);
            }
        }
    }
    if let Some(mode) = digit_mode {
        push_natural_part(&mut out, &buf, mode);
    }
    out
}
fn push_natural_part(out: &mut Vec<NaturalPart>, raw: &str, digit_mode: bool) {
    if digit_mode {
        let trimmed = raw.trim_start_matches('0');
        let normalized = if trimmed.is_empty() {
            "0".to_owned()
        } else {
            trimmed.to_owned()
        };
        out.push(NaturalPart::Number {
            normalized,
            raw_len: raw.len(),
        });
    } else {
        out.push(NaturalPart::Text(raw.to_owned()));
    }
}
fn compare_natural_part(a: &NaturalPart, b: &NaturalPart) -> Ordering {
    if let (Some((a_num, a_raw)), Some((b_num, b_raw))) = (a.as_number(), b.as_number()) {
        a_num
            .len()
            .cmp(&b_num.len())
            .then_with(|| a_num.cmp(b_num))
            .then_with(|| a_raw.cmp(&b_raw))
    } else if let (Some(a_text), Some(b_text)) = (a.as_text(), b.as_text()) {
        a_text.cmp(b_text)
    } else if a.as_number().is_some() {
        Ordering::Less
    } else {
        Ordering::Greater
    }
}
fn source_priority(rec: &SourceRecord) -> SourcePriority {
    let price_count = [rec.gasoline, rec.premium, rec.diesel]
        .into_iter()
        .flatten()
        .count();
    let text_field_count = [
        rec.region.trim(),
        rec.name.trim(),
        rec.brand.trim(),
        rec.self_yn.trim(),
        rec.address.trim(),
    ]
    .iter()
    .filter(|v| !v.is_empty())
    .count();
    let text_len =
        rec.region.len() + rec.name.len() + rec.brand.len() + rec.self_yn.len() + rec.address.len();
    (price_count, text_field_count, text_len)
}
fn read_source_file(path: &Path) -> Result<Vec<SourceRecord>> {
    crate::excel::source_reader::read_source_file(path)
}
fn source_label(path: &Path) -> String {
    path.file_name()
        .and_then(|s| s.to_str())
        .map_or_else(|| path.display().to_string(), ToString::to_string)
}
