import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

import round3_verify as validate

root = Path.cwd()
work = Path(os.environ["RUNNER_TEMP"]) / "goal-round3-workbook"
work.mkdir()
source, real_binaries = validate.product_builds(root, sys.argv[1], "fcupdater", work)
fixture = work / "opinet-source.xls"
original = (root / "fuel_cost_chungcheong.xlsx").read_bytes()
replays = []
replacement = '''let source_data = if let Some(path) = std::env::var_os("GOAL_REPLAY_SOURCE") {
            std::fs::read(path).map_err(|source| err_with_source("probe source read", source))?
        } else {
            let data = SourceDownload::default().refresh_source()?;
            if let Some(path) = std::env::var_os("GOAL_CAPTURE_SOURCE") {
                std::fs::write(path, &data).map_err(|source| err_with_source("probe source capture", source))?;
            }
            data
        };'''
for name, base in (("before", source), ("after", root)):
    probe = work / (name + "-source")
    shutil.copytree(base, probe, ignore=shutil.ignore_patterns(".git", ".github", "target"))
    path = probe / "src/update_run.rs"
    text = path.read_text()
    marker = "let source_data = SourceDownload::default().refresh_source()?;"
    assert text.count(marker) == 1
    path.write_text(text.replace(marker, replacement))
    target = work / (name + "-target")
    validate.run(["cargo", "+1.98.1", "build", "--release", "--frozen"], cwd=probe,
                 env={**os.environ, "CARGO_TARGET_DIR": str(target)})
    replays.append(target / "release/fcupdater")

capture = work / "capture"
capture.mkdir()
(capture / "fuel_cost_chungcheong.xlsx").write_bytes(original)
for attempt in range(3):
    result = subprocess.run([str(replays[1]), "--verify"], cwd=capture, capture_output=True,
                            env={**os.environ, "GOAL_CAPTURE_SOURCE": str(fixture)}, timeout=180)
    if result.returncode == 0:
        break
    assert (capture / "fuel_cost_chungcheong.xlsx").read_bytes() == original
    print("CAPTURE_RETRY", attempt + 1, result.stderr.decode(), flush=True)
else:
    raise RuntimeError("Live Opinet capture failed after bounded retries")
print("SOURCE", fixture.stat().st_size, hashlib.sha256(fixture.read_bytes()).hexdigest(), flush=True)
os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
directories = [work / "run-before", work / "run-after"]
for directory in directories:
    directory.mkdir()
environment = {**os.environ, "GOAL_REPLAY_SOURCE": str(fixture)}
for mode in ([], ["--verify"]):
    samples = [[], []]
    rss = [[], []]
    for pair in range(validate.PAIRS):
        outputs = [None, None]
        for side in ([0, 1] if pair % 2 == 0 else [1, 0]):
            master = directories[side] / "fuel_cost_chungcheong.xlsx"
            master.write_bytes(original)
            usage = directories[side] / "usage.txt"
            started = time.perf_counter_ns()
            result = subprocess.run(["/usr/bin/time", "-f", "%U %S %M", "-o", str(usage), str(replays[side]), *mode],
                                    cwd=directories[side], env=environment, capture_output=True, timeout=30)
            elapsed = time.perf_counter_ns() - started
            assert result.returncode == 0, result.stderr.decode()
            samples[side].append(elapsed)
            rss[side].append(int(usage.read_text().split()[2]))
            outputs[side] = master.read_bytes()
        assert outputs[0] == outputs[1], "Fixed-source workbook bytes differ"
    upper = validate.paired_bounds(*samples)
    print("WORKBOOK_PERFORMANCE " + json.dumps({"mode": mode or ["default"], "pairs": validate.PAIRS,
          "before_ns": samples[0], "after_ns": samples[1], "upper95_ratio": upper, "limit": validate.LIMIT,
          "rss_kib": rss, "pass": upper <= validate.LIMIT, "network_excluded": True}), flush=True)
    assert upper <= validate.LIMIT

for scenario in ("locked", "corrupt", "symlink"):
    observations = []
    for side in (0, 1):
        directory = work / f"reject-{scenario}-{side}"
        directory.mkdir()
        master = directory / "fuel_cost_chungcheong.xlsx"
        lock = None
        if scenario == "corrupt":
            master.write_bytes(b"X" + original[1:])
        elif scenario == "symlink":
            protected = directory / "original.xlsx"
            protected.write_bytes(original)
            master.symlink_to(protected)
        else:
            master.write_bytes(original)
            lock = (directory / ".fcupdater.lock").open("wb")
            os.chmod(lock.name, 0o600)
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        before = master.read_bytes()
        result = subprocess.run([str(replays[side]), "--verify"], cwd=directory,
                                env=environment, capture_output=True, timeout=30)
        assert result.returncode != 0 and master.read_bytes() == before
        observations.append((result.returncode, result.stdout, result.stderr))
        if lock:
            lock.close()
    assert observations[0] == observations[1]
    print("REJECTION", scenario, "equivalent; workbook preserved", flush=True)

master = work / "fuel_cost_chungcheong.xlsx"
master.write_bytes(original)
for attempt in range(3):
    result = subprocess.run(["/usr/bin/time", "-v", str(real_binaries[1]), "--verify"], cwd=work,
                            capture_output=True, timeout=180)
    if result.returncode == 0:
        break
    assert master.read_bytes() == original
    print("REFRESH_RETRY", attempt + 1, result.stderr.decode(), flush=True)
else:
    raise RuntimeError("Final real updater refresh failed")
(work / "workbook-refresh.log").write_bytes(result.stdout + result.stderr)
print(result.stdout.decode(), flush=True)
print(result.stderr.decode(), flush=True)
print("WORKBOOK", master.stat().st_size, hashlib.sha256(master.read_bytes()).hexdigest(), flush=True)
