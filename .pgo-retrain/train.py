#!/usr/bin/env python3
"""Temporary native PGO retraining for fcupdater. Requires Rust 1.99.1."""
import hashlib
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile
import xml.etree.ElementTree as ET

# Keep native Windows runner output encoding independent from the console code page.
sys.stdout.reconfigure(encoding="utf-8", errors="backslashreplace")
sys.stderr.reconfigure(encoding="utf-8", errors="backslashreplace")

VERSION = "1.99.1"
ROOT = Path.cwd().resolve()
TARGET = sys.argv[1]
TMP = Path(os.environ["RUNNER_TEMP"]).resolve() / ("fcupdater-pgo-" + TARGET)
RAW = TMP / "raw"
OUT = ROOT / ".pgo-retrain" / "out"
WORKBOOK = ROOT / "fuel_cost_chungcheong.xlsx"
EXE = "fcupdater.exe" if os.name == "nt" else "fcupdater"
ROOT_PROFILE = ROOT / "pgo" / (TARGET + ".profdata")
BASE_ENV = dict(os.environ)
for key in ("RUSTFLAGS", "CARGO_ENCODED_RUSTFLAGS", "CARGO_TARGET_DIR", "LLVM_PROFILE_FILE"):
    BASE_ENV.pop(key, None)
BASE_ENV["CARGO_INCREMENTAL"] = "0"
TMP.mkdir(parents=True, exist_ok=False)
RAW.mkdir()
OUT.mkdir(parents=True, exist_ok=True)

def run(argv, *, cwd=ROOT, env=None, ok=(0,), timeout=480):
    args = list(map(str, argv))
    print("$ " + " ".join(args), flush=True)
    completed = subprocess.run(args, cwd=cwd, env=env or BASE_ENV,
                               capture_output=True, text=True, errors="replace",
                               timeout=timeout)
    if completed.stdout:
        print(completed.stdout[-1600:], flush=True)
    if completed.stderr:
        print(completed.stderr[-2400:], file=sys.stderr, flush=True)
    if completed.returncode not in ok:
        raise RuntimeError("command exited " + str(completed.returncode) + ": " + " ".join(args))
    return completed

def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()

def check_xlsx(path):
    ns = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
    with zipfile.ZipFile(path) as archive:
        assert archive.testzip() is None, "XLSX ZIP CRC failure"
        required = ("xl/workbook.xml", "xl/styles.xml", "xl/sharedStrings.xml",
                    "xl/worksheets/sheet1.xml", "xl/worksheets/sheet2.xml")
        assert all(name in archive.namelist() for name in required), "Missing XLSX parts"
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        sheets = [node.attrib["name"] for node in workbook.iter(ns + "sheet")]
        assert sheets == ["유류비", "변경내역"], "Unexpected workbook sheet structure"
        sheet = ET.fromstring(archive.read("xl/worksheets/sheet1.xml"))
        assert sum(1 for _ in sheet.iter(ns + "row")) >= 800, "Master rows missing"
        assert any(True for _ in sheet.iter(ns + "f")), "Workbook formulas missing"
        ET.fromstring(archive.read("xl/worksheets/sheet2.xml"))
        ET.fromstring(archive.read("xl/sharedStrings.xml"))
        ET.fromstring(archive.read("xl/styles.xml"))

version = run(["rustc", "+" + VERSION, "-Vv"]).stdout
assert version.startswith("rustc 1.99.1 "), version
print("Compiler:\n" + version)
original_sha = sha256(WORKBOOK)

# A strict TLS connectivity check is diagnostic only; live training still uses
# the application's actual platform transport and certificate validation.
try:
    control = "curl.exe" if os.name == "nt" else "curl"
    preflight = run([control, "--silent", "--show-error", "--location", "--head",
                     "--max-time", "20", "https://nfl.opinet.co.kr/"],
                    timeout=30, ok=tuple(range(256)))
    print("HTTPS preflight exit:", preflight.returncode)
except (FileNotFoundError, subprocess.TimeoutExpired) as error:
    print("HTTPS preflight unavailable:", type(error).__name__)

instrumented_target = TMP / "instrumented-target"
generate_env = BASE_ENV | {
    "RUSTFLAGS": "-Cprofile-generate=" + str(RAW),
    "CARGO_TARGET_DIR": str(instrumented_target),
    "LLVM_PROFILE_FILE": str(RAW / "cli-%p-%m.profraw"),
}
run(["cargo", "+" + VERSION, "build", "--release", "--frozen", "--bin",
     "fcupdater", "--target", TARGET], env=generate_env, timeout=1200)
binary = instrumented_target / TARGET / "release" / EXE
assert binary.is_file()
for args, rc in [(["--help"], (0,)), (["--version"], (0,)),
                 (["--invalid"], (1,))]:
    run([binary, *args], env=generate_env, ok=rc, timeout=30)

# Exercise the real source-download/update/save/verify path on a disposable
# copy. Never modify the tracked workbook during training.
live_dir = TMP / "live"
live_dir.mkdir()
live_book = live_dir / WORKBOOK.name
shutil.copyfile(WORKBOOK, live_book)
live_result = subprocess.run([str(binary), "--verify"], cwd=live_dir, env=generate_env,
                             capture_output=True, text=True, errors="replace", timeout=180)
print("Live --verify exit:", live_result.returncode)
print("Live stdout:", live_result.stdout[-1200:])
print("Live stderr:", live_result.stderr[-1200:])
if live_result.returncode == 0:
    check_xlsx(live_book)
if TARGET == "x86_64-unknown-linux-gnu":
    assert live_result.returncode == 0, "Authoritative Opinet update failed on Linux"
    assert live_result.stdout.count("Opinet 소스 데이터 준비 완료") == 1
    assert sha256(live_book) != original_sha, "Refreshed workbook unexpectedly identical"
    shutil.copyfile(live_book, OUT / WORKBOOK.name)
    assert sha256(OUT / WORKBOOK.name) == sha256(live_book)
    print("Refreshed workbook SHA-256:", sha256(live_book))
assert sha256(WORKBOOK) == original_sha, "Tracked workbook changed"

# Train the substantial workbook computation without external I/O using the
# previously validated project-local diagnostic harness, applied ONLY to a
# disposable checkout copy (never to release/source files).
probe_src = TMP / "probe-src"
shutil.copytree(ROOT, probe_src, ignore=shutil.ignore_patterns(".git", "target", "artifacts", "out"))
main = probe_src / "src" / "main.rs"
original_main = main.read_text(encoding="utf-8")
marker = "fn main() -> Result<()> {"
assert original_main.count(marker) == 1
main.write_text(original_main.replace(marker, "fn production_main() -> Result<()> {", 1)
                + "\n" + (ROOT / ".pgo-retrain" / "probe.rs").read_text(encoding="utf-8"),
                encoding="utf-8")
excel = probe_src / "src" / "excel.rs"
excel_text = excel.read_text(encoding="utf-8")
assert excel_text.count("mod source_reader;") == 1
excel.write_text(excel_text.replace("mod source_reader;", "pub(crate) mod source_reader;", 1),
                 encoding="utf-8")
writer = probe_src / "src" / "excel" / "writer.rs"
with writer.open("a", encoding="utf-8") as output:
    output.write("\n")
    output.write((ROOT / ".pgo-retrain" / "writer_probe.rs").read_text(encoding="utf-8"))
probe_target = TMP / "probe-target"
probe_env = generate_env | {
    "CARGO_TARGET_DIR": str(probe_target),
    "LLVM_PROFILE_FILE": str(RAW / "probe-%p-%m.profraw"),
}
run(["cargo", "+" + VERSION, "build", "--release", "--frozen", "--bin",
     "fcupdater", "--target", TARGET], cwd=probe_src, env=probe_env, timeout=1200)
probe = probe_target / TARGET / "release" / EXE
assert probe.is_file()
for name in ("bench", "parse-bench", "intern-plain", "intern-escaped"):
    for _ in range(3):
        run([probe, name, WORKBOOK], cwd=probe_src, env=probe_env, timeout=120)
saved = TMP / "offline-trained.xlsx"
# The workbook writer intentionally protects the input file identity. A
# disposable workbook must exist first, then be updated in place.
shutil.copyfile(WORKBOOK, saved)
run([probe, "save", saved, saved], cwd=probe_src, env=probe_env, timeout=180)
check_xlsx(saved)
raw = sorted(RAW.glob("*.profraw"))
assert len(raw) >= 7, "Insufficient nonempty training samples"
assert all(p.stat().st_size > 0 for p in raw), "Empty profiling sample"
sysroot = Path(run(["rustc", "+" + VERSION, "--print", "sysroot"]).stdout.strip())
profdata_exe = "llvm-profdata.exe" if os.name == "nt" else "llvm-profdata"
profdata = sysroot / "lib" / "rustlib" / TARGET / "bin" / profdata_exe
assert profdata.is_file(), "Rust 1.99.1 llvm-tools-preview missing"
run([profdata, "merge", "-o", ROOT_PROFILE, *raw], timeout=240)
assert ROOT_PROFILE.stat().st_size > 25000, "PGO profile unusually small"
profile_show = run([profdata, "show", "--all-functions", "--counts", ROOT_PROFILE], timeout=180)
assert len(profile_show.stdout) > 1000, "PGO profile has no useful counters"
text = profile_show.stdout.lower()
assert "function" in text, "PGO profile missing function data"
profile_sha = sha256(ROOT_PROFILE)

# Build the exact shipping profile with the just-merged, compiler-matched
# .profdata. Do not fall back to a non-PGO build.
release_env = BASE_ENV | {"CARGO_TARGET_DIR": str(TMP / "pgo-release-target")}
release = run(["cargo", "+" + VERSION, "build-pgo"], env=release_env, timeout=1200)
diagnostics = release.stderr.lower()
for diagnostic in ("function control flow change detected",
                   "hash mismatch", "profile data may be out of date"):
    assert diagnostic not in diagnostics, "PGO mismatch: " + diagnostic

# LLVM can emit warnings for compiler-generated, never-executed helper
# functions with exactly zero counters discarded. Do not ignore any other
# diagnostics or silently drop useful application profile information.
warnings = [line for line in release.stderr.splitlines()
            if line.startswith("warning:") and not re.search(r"generated \d+ warnings?$", line)]
zero_count_helpers = [line for line in warnings
                      if "no profile data available for function" in line
                      and "up to 0 count discarded" in line]
assert len(warnings) == len(zero_count_helpers), "Unexpected PGO warning: " + repr(warnings)
assert len(zero_count_helpers) <= 32, "Unusually many profile-absent functions"
assert all("RankSortRefresher" not in line and "SharedStringTable" not in line
           for line in zero_count_helpers), "Hot application functions lack PGO counters"
print("Zero-count LLVM helper warnings:", len(zero_count_helpers))
release_binary = TMP / "pgo-release-target" / TARGET / "release" / EXE
assert release_binary.is_file(), "PGO release binary missing"
run([release_binary, "--help"], env=BASE_ENV, timeout=30)
run([release_binary, "--version"], env=BASE_ENV, timeout=30)
run([release_binary, "--invalid"], env=BASE_ENV, ok=(1,), timeout=30)
assert sha256(WORKBOOK) == original_sha, "Source workbook changed during validation"
shutil.copyfile(ROOT_PROFILE, OUT / ROOT_PROFILE.name)
result = {
    "target": TARGET, "rust": version.splitlines()[0], "sha256": profile_sha,
    "bytes": ROOT_PROFILE.stat().st_size, "raw_count": len(raw),
    "live_verify_exit": live_result.returncode,
    "offline_workbook_valid": True, "release_pgo_build": "success",
    "zero_count_pgo_helpers": len(zero_count_helpers),
    "source_workbook_sha256": original_sha,
    "updated_workbook_sha256": sha256(OUT / WORKBOOK.name) if TARGET == "x86_64-unknown-linux-gnu" else None,
}
print("PGO RESULT:", json.dumps(result, sort_keys=True))
with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as summary:
    summary.write(f"### {TARGET}\n\n")
    summary.write(f"- Compiler: {result['rust']}\n")
    summary.write(f"- PGO SHA-256: {profile_sha}\n")
    summary.write(f"- Samples: {len(raw)}; profile bytes: {ROOT_PROFILE.stat().st_size}\n")
    summary.write(f"- Zero-count LLVM helper warnings: {len(zero_count_helpers)}\n")
    summary.write(f"- Real CLI --verify exit: {live_result.returncode} (offline probe also executed)\n")
    summary.write("- Instrumented workload, XLSX CRC, PGO release build and CLI: passed\n")
