use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};
const UNKNOWN: &str = "unknown";
fn main() {
    let dot_git = Path::new(".git");
    let has_git_metadata = fs::exists(dot_git).is_ok_and(|exists| exists);
    let git_dir = if dot_git.is_dir() {
        dot_git.to_path_buf()
    } else if let Ok(text) = fs::read_to_string(dot_git) {
        text.trim().strip_prefix("gitdir: ").map_or_else(
            || dot_git.to_path_buf(),
            |raw_path| {
                let git_path = PathBuf::from(raw_path);
                if git_path.is_absolute() {
                    git_path
                } else {
                    dot_git
                        .parent()
                        .unwrap_or_else(|| Path::new("."))
                        .join(git_path)
                }
            },
        )
    } else {
        dot_git.to_path_buf()
    };
    let git_head = git_dir.join("HEAD");
    emit_rerun_if_changed(git_head.as_path());
    emit_rerun_if_changed(git_dir.join("packed-refs").as_path());
    emit_rerun_if_changed(git_dir.join("index").as_path());
    if let Ok(head) = fs::read_to_string(&git_head)
        && let Some(ref_name) = head.trim().strip_prefix("ref: ")
    {
        emit_rerun_if_changed(git_dir.join(ref_name).as_path());
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
    println!(
        "cargo::rustc-env=BUILD_RUSTC={}",
        command_stdout("rustc", &["--version"]).map_or_else(
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
    command_stdout("git", args).or_else(|| {
        [
            r"C:\Program Files\Git\cmd\git.exe",
            r"C:\Program Files\Git\bin\git.exe",
            r"C:\Program Files (x86)\Git\cmd\git.exe",
        ]
        .into_iter()
        .find_map(|candidate| command_stdout(candidate, args))
    })
}
fn command_stdout(program: &str, args: &[&str]) -> Option<String> {
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
