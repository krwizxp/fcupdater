use std::{
    env,
    ffi::OsStr,
    fs,
    path::{Path, PathBuf},
    process::Command,
};
const UNKNOWN: &str = "unknown";
fn main() {
    let dot_git = Path::new(".git");
    let has_git_metadata = fs::exists(dot_git).is_ok_and(|exists| exists);
    if has_git_metadata {
        let git_head = git_path("HEAD");
        if let Some(path) = git_head.as_deref() {
            emit_rerun_if_changed(path);
            if let Ok(head) = fs::read_to_string(path)
                && let Some(ref_name) = head.trim().strip_prefix("ref: ")
                && let Some(ref_path) = git_path(ref_name)
            {
                emit_rerun_if_changed(ref_path.as_path());
            }
        }
        for name in ["packed-refs", "index"] {
            if let Some(path) = git_path(name) {
                emit_rerun_if_changed(path.as_path());
            }
        }
    }
    if has_git_metadata
        && let Some(paths) = git_command_stdout(&[
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
            "--",
        ])
    {
        for path in paths.split_terminator('\0') {
            println!("cargo::rerun-if-changed={path}");
        }
    }
    println!("cargo::rerun-if-env-changed=GITHUB_SHA");
    let git_sha = env::var("GITHUB_SHA")
        .ok()
        .and_then(|sha| short_git_sha(sha.as_str()))
        .or_else(|| {
            if !has_git_metadata {
                return None;
            }
            let sha = git_command_stdout(&["rev-parse", "--short=12", "HEAD"])?;
            short_git_sha(sha.as_str())
        })
        .unwrap_or_else(|| UNKNOWN.to_owned());
    let git_dirty = if has_git_metadata {
        git_command_stdout(&["status", "--porcelain", "--untracked-files=normal"]).map_or(
            UNKNOWN,
            |text| {
                if text.trim().is_empty() {
                    "false"
                } else {
                    "true"
                }
            },
        )
    } else {
        UNKNOWN
    };
    println!(
        "cargo::rustc-env=BUILD_TARGET={}",
        env::var("TARGET").unwrap_or_else(|_| UNKNOWN.to_owned()),
    );
    println!(
        "cargo::rustc-env=BUILD_PROFILE={}",
        env::var("PROFILE").unwrap_or_else(|_| UNKNOWN.to_owned()),
    );
    let rustc = env::var_os("RUSTC");
    println!(
        "cargo::rustc-env=BUILD_RUSTC={}",
        command_stdout(
            rustc.as_deref().unwrap_or_else(|| OsStr::new("rustc")),
            &["--version"],
        )
        .map_or_else(
            || UNKNOWN.to_owned(),
            |text| {
                let trimmed = text.trim();
                if trimmed.is_empty() {
                    UNKNOWN.to_owned()
                } else {
                    trimmed.to_owned()
                }
            },
        ),
    );
    println!("cargo::rustc-env=BUILD_GIT_SHA={git_sha}");
    println!("cargo::rustc-env=BUILD_GIT_DIRTY={git_dirty}");
}
fn short_git_sha(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || !trimmed.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(trimmed.bytes().take(12).map(char::from).collect())
}
fn git_command_stdout(args: &[&str]) -> Option<String> {
    command_stdout(OsStr::new("git"), args).or_else(|| {
        [
            r"C:\Program Files\Git\cmd\git.exe",
            r"C:\Program Files\Git\bin\git.exe",
            r"C:\Program Files (x86)\Git\cmd\git.exe",
        ]
        .into_iter()
        .find_map(|candidate| command_stdout(OsStr::new(candidate), args))
    })
}
fn git_path(name: &str) -> Option<PathBuf> {
    let raw_path =
        git_command_stdout(&["rev-parse", "--path-format=absolute", "--git-path", name])?;
    let trimmed_path = raw_path.trim();
    if trimmed_path.is_empty() {
        None
    } else {
        Some(PathBuf::from(trimmed_path))
    }
}
fn command_stdout(program: &OsStr, args: &[&str]) -> Option<String> {
    let Ok(output) = Command::new(program).args(args).output() else {
        return None;
    };
    if !output.status.success() {
        return None;
    }
    let Ok(text) = String::from_utf8(output.stdout) else {
        return None;
    };
    Some(text)
}
fn emit_rerun_if_changed(path: &Path) {
    if fs::exists(path).is_ok_and(|exists| exists) {
        println!("cargo::rerun-if-changed={}", path.display());
    }
}
