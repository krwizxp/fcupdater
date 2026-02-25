use std::path::PathBuf;
pub(super) fn path_from_slashes(path: &str) -> PathBuf {
    let mut out = PathBuf::new();
    for segment in path.split('/') {
        if segment.is_empty() {
            continue;
        }
        out.push(segment);
    }
    out
}
