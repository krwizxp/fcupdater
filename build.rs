use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};
const UNKNOWN: &str = "unknown";
fn main() {
    let dot_git = Path::new(".git");
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
    println!("cargo:rerun-if-changed={}", git_head.display());
    println!(
        "cargo:rerun-if-changed={}",
        git_dir.join("packed-refs").display(),
    );
    if let Ok(head) = fs::read_to_string(&git_head)
        && let Some(ref_name) = head.trim().strip_prefix("ref: ")
    {
        println!(
            "cargo:rerun-if-changed={}",
            git_dir.join(ref_name).display()
        );
    }
    println!("cargo:rerun-if-env-changed=GITHUB_SHA");
    println!("cargo:rerun-if-env-changed=TARGET");
    println!("cargo:rerun-if-env-changed=PROFILE");
    let git_sha = env::var("GITHUB_SHA")
        .ok()
        .and_then(|sha| {
            let trimmed = sha.trim();
            (!trimmed.is_empty()).then(|| trimmed.chars().take(12).collect())
        })
        .unwrap_or_else(|| command_text("git", &["rev-parse", "--short=12", "HEAD"]));
    println!(
        "cargo:rustc-env=BUILD_TARGET={}",
        env::var("TARGET").unwrap_or_else(|_| UNKNOWN.to_owned()),
    );
    println!(
        "cargo:rustc-env=BUILD_PROFILE={}",
        env::var("PROFILE").unwrap_or_else(|_| UNKNOWN.to_owned()),
    );
    println!(
        "cargo:rustc-env=BUILD_RUSTC={}",
        command_text("rustc", &["--version"]),
    );
    println!("cargo:rustc-env=BUILD_GIT_SHA={git_sha}");
}
fn command_text(program: &str, args: &[&str]) -> String {
    if let Some(text) = command_output(program, args) {
        return text;
    }
    if program == "git" {
        for candidate in [
            r"C:\Program Files\Git\cmd\git.exe",
            r"C:\Program Files\Git\bin\git.exe",
            r"C:\Program Files (x86)\Git\cmd\git.exe",
        ] {
            if let Some(text) = command_output(candidate, args) {
                return text;
            }
        }
    }
    UNKNOWN.to_owned()
}
fn command_output(program: &str, args: &[&str]) -> Option<String> {
    let Ok(output) = Command::new(program).args(args).output() else {
        return None;
    };
    if !output.status.success() {
        return None;
    }
    let Ok(text) = String::from_utf8(output.stdout) else {
        return None;
    };
    let trimmed = text.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_owned())
    }
}
