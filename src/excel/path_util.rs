use std::path::PathBuf;
pub(in crate::excel) fn path_from_slashes(path: &str) -> PathBuf {
    let mut out = PathBuf::new();
    for segment in path.split('/').filter(|segment| !segment.is_empty()) {
        out.push(segment);
    }
    out
}
