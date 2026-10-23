import json
import os
import random
import re
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

import psutil

BASELINE_SHA = "68b774e838693f4a7226d272859fdea773b8814d"
MAIN_SHA = "6d267214b1feae5cb4cd53b4d539291766293753"
ROOT = Path.cwd().resolve()
TEMP = Path(os.environ["RUNNER_TEMP"]).resolve()
TARGET = os.environ["NATIVE_TARGET"]
EXE = ".exe" if sys.platform == "win32" else ""
SOURCE = ROOT / "fuel_cost_chungcheong.xlsx"
PERFORMANCE_SOURCE = ROOT / ".validation/native-baseline-performance.rs"
VARIANTS = ("baseline", "main", "candidate")
N = 24
MARGIN_MEDIAN = 0.05
MARGIN_TAIL = 0.10
MARGIN_PACKAGE = 0.03
MARGIN_MEMORY = 0.10
FIXTURE = Path(".validation/fixtures/native-benchmark.xlsx")

def run(cmd, cwd, env=None, timeout=360):
    result = subprocess.run(
        cmd, cwd=cwd, env=env, capture_output=True, text=True,
        errors="replace", timeout=timeout
    )
    if result.returncode:
        print("COMMAND_FAILED", cmd, str(cwd), result.returncode)
        print("STDOUT", result.stdout[-4500:])
        print("STDERR", result.stderr[-6500:])
        raise RuntimeError("Failed command on " + str(cwd))
    return result

def toolchain():
    p = run(["rustc", "+stable", "-Vv"], ROOT)
    print("LATEST_STABLE_COMPILER", p.stdout.strip().replace("\n", " | "))
    return p.stdout.strip()

def setup():
    entries = {
        "baseline": (TEMP / "exact-fcupdater-baseline", BASELINE_SHA),
        "main": (TEMP / "exact-fcupdater-main", MAIN_SHA),
        "candidate": (ROOT, os.environ["GITHUB_SHA"]),
    }
    for name in ("baseline", "main"):
        path, commit = entries[name]
        run(["git", "worktree", "add", "--detach", str(path), commit], ROOT)
    for name, (path, commit) in entries.items():
        actual = run(["git", "rev-parse", "HEAD"], path).stdout.strip()
        if name != "candidate":
            assert actual == commit, (name, actual, commit)
        fixture = path / FIXTURE
        fixture.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(SOURCE, fixture)
        file = path / "src/excel/writer.rs"
        with file.open("a", encoding="utf-8", newline="\n") as stream:
            stream.write('\ninclude!("../../.validation/native-baseline-performance.rs");\n')
        dst = path / ".validation/native-baseline-performance.rs"
        if dst != PERFORMANCE_SOURCE:
            shutil.copyfile(PERFORMANCE_SOURCE, dst)
        print("PINNED_REVISION", name, actual, "fixture_bytes", fixture.stat().st_size)
    return {name: path for name, (path, _) in entries.items()}

def build_one(name, directory):
    d = directory
    env = os.environ.copy()
    build = run(["cargo", "+stable", "build-pgo"], d, env=env, timeout=450)
    targetbin = d / "target" / TARGET / "release" / ("fcupdater" + EXE)
    assert targetbin.exists(), str(targetbin)
    build_report = {
        "native_release_bytes": targetbin.stat().st_size,
        "profile_missing_warnings": build.stderr.count("no profile data available"),
        "profile_hash_warnings": build.stderr.count("control flow change detected"),
        "compiler_warning_count": build.stderr.count("warning:"),
    }
    report = run(["cargo", "+stable", "run", "--release", "--frozen", "--example", "package_artifact",
                  "--", "fcupdater-performance-" + name], d,
                 env={**env, "CARGO_TARGET_DIR":str(d / "target" / TARGET)}, timeout=180)
    artifact = d / "artifacts" / ("fcupdater-performance-" + name + (".exe" if EXE else ".tar"))
    assert artifact.is_file(), str(artifact)
    build_report["native_distribution_bytes"] = artifact.stat().st_size
    work = TEMP / ("native-bench-test-" + name)
    work.mkdir(exist_ok=True)
    test_env = {**env, "CARGO_TARGET_DIR":str(work)}
    testbuild = run(["cargo", "+stable", "test", "--release", "--frozen",
                     "--config", ".cargo/pgo.toml", "--no-run"],
                    d, env=test_env, timeout=450)
    possible = list(work.rglob("fcupdater-*" + EXE))
    possible = [p for p in possible if p.is_file() and (EXE or os.access(p, os.X_OK))]
    assert len(possible) == 1, [str(p) for p in possible]
    print("NATIVE_BUILD", name, json.dumps(build_report, sort_keys=True))
    return build_report, possible[0]

def timed_test(path, directory):
    begin = time.perf_counter()
    proc = subprocess.Popen(
        [str(path), "exact_baseline_performance::workbook_octane_cache_path", "--nocapture"],
        cwd=directory, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    monitored = psutil.Process(proc.pid)
    memory = 0
    while proc.poll() is None:
        try:
            memory = max(memory, monitored.memory_info().rss)
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
        time.sleep(0.005)
    out, err = proc.communicate(timeout=8)
    text = out.decode("utf-8", errors="replace")
    log = err.decode("utf-8", errors="replace")
    if proc.returncode:
        raise RuntimeError("Bench failed "+text[-1600:]+log[-1600:])
    matches = re.search(
        r"EXACT_BASELINE_CORE_MEDIAN_NS\s+([\d.]+)\s+P95_NS\s+([\d.]+)\s+FASTEST_NS\s+([\d.]+)",
        text
    )
    assert matches, text
    return {
        "core_ns": float(matches.group(1)),
        "within_process_max_ns": float(matches.group(2)),
        "within_process_min_ns": float(matches.group(3)),
        "peak_rss_bytes": memory,
        "process_elapsed_ms": 1000 * (time.perf_counter() - begin),
    }

def median(values):
    return statistics.median(values)

def summaries(samples):
    result = {}
    for name, rows in samples.items():
        result[name] = {}
        for field in rows[0]:
            values = [row[field] for row in rows]
            result[name][field] = {
                "median": round(median(values), 3),
                "p95": round(sorted(values)[int((len(values) - 1) * 0.95)], 3),
                "spread": round(max(values) - min(values), 3),
                "individual": values,
            }
    return result

def bootstrap_upper_ratio(before, after):
    rng = random.Random(20261023)
    sampled = []
    for _ in range(1600):
        old = median([before[rng.randrange(len(before))] for _ in before])
        new = median([after[rng.randrange(len(after))] for _ in after])
        sampled.append(new / old - 1)
    return sorted(sampled)[int(len(sampled) * 0.95)]

def validate(results, builds, samples):
    old, candidate = results["baseline"], results["candidate"]
    checks = {}
    for field, limit in [("core_ns",MARGIN_MEDIAN),("process_elapsed_ms",MARGIN_TAIL)]:
        baseline = old[field]["median"]
        new = candidate[field]["median"]
        upper = bootstrap_upper_ratio(
            [x[field] for x in samples["baseline"]],
            [x[field] for x in samples["candidate"]],
        )
        allowed = 1 + limit
        checks[field] = {
            "relative_change_percent": round(100*(new/baseline-1),3),
            "upper_95pct_bootstrap_percent": round(100*upper,3),
            "within_limit": new/baseline <= allowed and upper <= limit,
            "limit_percent": 100*limit,
        }
    b = old["peak_rss_bytes"]["median"]
    c = candidate["peak_rss_bytes"]["median"]
    checks["peak_memory"] = {
        "relative_change_percent":round(100*(c/b-1),3),
        "within_limit": c <= b*(1+MARGIN_MEMORY) + 8_000_000,
        "limit_percent":100*MARGIN_MEMORY,
    }
    before = builds["baseline"]["native_distribution_bytes"]
    after = builds["candidate"]["native_distribution_bytes"]
    checks["distribution"] = {
        "relative_change_percent":round(100*(after/before-1),3),
        "within_limit":after <= before*(1+MARGIN_PACKAGE),
        "limit_percent":100*MARGIN_PACKAGE,
    }
    print("EXACT_BASELINE_COMPARISON",TARGET,json.dumps(checks,sort_keys=True))
    assert all(value["within_limit"] for value in checks.values()),checks
    return checks

def main():
    info = toolchain()
    paths = setup()
    builds = {}
    binaries = {}
    for name in VARIANTS:
        builds[name], binaries[name] = build_one(name, paths[name])
    for name in VARIANTS:
        timed_test(binaries[name], paths[name])
    samples = {name:[] for name in VARIANTS}
    for round in range(N):
        sequence = VARIANTS if round%2 == 0 else tuple(reversed(VARIANTS))
        for name in sequence:
            point = timed_test(binaries[name], paths[name])
            point["round"] = round
            samples[name].append(point)
            print("NATIVE_SAMPLE",TARGET,name,round,json.dumps(point,sort_keys=True))
    for points in samples.values():
        for value in points:
            value.pop("round",None)
    summary = summaries(samples)
    checks = validate(summary,builds,samples)
    report = {
        "target":TARGET,"rustc":info,"baseline_sha":BASELINE_SHA,
        "main_sha":MAIN_SHA,
        "candidate_sha":os.environ["GITHUB_SHA"],
        "distribution_builds":builds,
        "summary":summary,"samples":samples,"acceptance":checks,
    }
    out = ROOT / ".validation" / ("native-baseline-"+TARGET+".json")
    out.write_text(json.dumps(report,ensure_ascii=False,indent=2),encoding="utf8")
    print("EXACT_BASELINE_NATIVE_PERFORMANCE_PASS",TARGET)
if __name__ == "__main__":
    main()
