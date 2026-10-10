"""Direct old SRG PGO vs same-source ordinary release benchmark on Windows."""
from pathlib import Path
import os, json, random, hashlib, statistics, platform, time
import patterns as p

ROOT = Path(__file__).resolve().parent
PREPARED = ROOT / "prepared"
OUTPUT = ROOT / "direct-results"
COUNT = 8_145_060
PAIRS_PER_SHARD = 4
SHARDS = 4
SEED = 2026101026

def verified_inputs():
    base = json.loads((PREPARED / "results.json").read_text(encoding="utf-8"))
    app = base["apps"]["srg"]
    checks = {}
    for name in ["old.exe", "normal.exe"]:
        path = PREPARED / name
        actual = hashlib.sha256(path.read_bytes()).hexdigest()
        expected = app["prepared_sha256"][name]
        assert actual == expected, (name, actual, expected)
        checks[name] = actual
    assert app["binary_bytes"]["old"] != 0
    assert app["source_sha256"]
    return checks, app["source_sha256"], app["binary_bytes"]

def timed_run(exe, env):
    seconds, checks = p.menu_bulk(exe, COUNT, env, full_check=True)
    assert checks["records"] == COUNT + 1
    assert checks["lines"] == (COUNT + 1) * 17
    return seconds, checks

def measure(shard):
    assert 0 <= shard < SHARDS
    hashes, sources, binary_bytes = verified_inputs()
    rng = random.Random(SEED + shard)
    out = OUTPUT / ("shard-" + str(shard))
    out.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    env.pop("LLVM_PROFILE_FILE", None)
    env.pop("RUSTFLAGS", None)
    for key in ["old", "normal"]:
        p.menu_bulk((PREPARED / (key + ".exe")).resolve(), 1024, env)
    result = {
        "shard": shard, "seed": SEED + shard,
        "source_commit": "5be8df3c960ebc601c7bfb815d2aae7c90911088",
        "product_source_checksums": sources, "binary_sha256": hashes,
        "binary_bytes": binary_bytes, "record_count_input": COUNT,
        "record_count_output": COUNT + 1, "expected_lines": (COUNT + 1) * 17,
        "pairs": [], "environment": {
            "platform": platform.platform(),
            "processor": os.environ.get("PROCESSOR_IDENTIFIER"),
            "image": os.environ.get("ImageVersion"),
            "runner": os.environ.get("RUNNER_NAME"),
            "arch": os.environ.get("RUNNER_ARCH")
        },
        "definitions": {
            "normal": "same-source ordinary --release (Rust 1.99.0)",
            "old": "previous product PGO (Rust 1.99.0)",
            "timer": "perf_counter covers menu4 generation; independent file validation occurs after timer"
        }
    }
    target = out / "results.json"
    target.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    for i in range(PAIRS_PER_SHARD):
        order = ["normal", "old"]
        rng.shuffle(order)
        values = {}
        print("BEGIN shard", shard, "pair", i + 1, "/", PAIRS_PER_SHARD, "order", order, flush=True)
        for variant in order:
            start = time.monotonic()
            seconds, checks = timed_run((PREPARED / (variant + ".exe")).resolve(), env)
            values[variant] = {"seconds": seconds, "integrity": checks}
            print("SAMPLE", variant, "generation_s", round(seconds, 6),
                  "total_s", round(time.monotonic() - start, 3), flush=True)
        for key in ["records", "lines"]:
            assert values["normal"]["integrity"][key] == values["old"]["integrity"][key]
        result["pairs"].append({"order": order, "normal": values["normal"], "old": values["old"]})
        target.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print("DONE shard", shard, "paired", len(result["pairs"]), flush=True)

def bootstrap(pairs, seed=SEED, draws=10000):
    rng = random.Random(seed)
    ratios = [r["old"]["seconds"] / r["normal"]["seconds"] for r in pairs]
    boots = sorted(statistics.median(rng.choices(ratios, k=len(ratios))) - 1 for _ in range(draws))
    return {
        "paired_median_change": statistics.median(ratios) - 1,
        "ci95": [boots[int(draws * .025)], boots[int(draws * .975) - 1]],
        "pairs": len(ratios),
        "ordinary_release_median_seconds": statistics.median(x["normal"]["seconds"] for x in pairs),
        "existing_pgo_median_seconds": statistics.median(x["old"]["seconds"] for x in pairs)
    }

def combine():
    records = []
    for shard in range(SHARDS):
        path = OUTPUT / ("srg-direct-old-vs-release-shard-" + str(shard)) / "results.json"
        if not path.exists():
            path = OUTPUT / ("shard-" + str(shard)) / "results.json"
        record = json.loads(path.read_text(encoding="utf-8"))
        assert record["shard"] == shard and len(record["pairs"]) == PAIRS_PER_SHARD
        assert record["record_count_input"] == COUNT and record["expected_lines"] == 138466037
        records.append(record)
    first = records[0]
    for r in records[1:]:
        for field in ["binary_sha256", "binary_bytes", "source_commit", "product_source_checksums"]:
            assert r[field] == first[field], (field, r["shard"])
    pairs = [pair for rec in records for pair in rec["pairs"]]
    assert len(pairs) == SHARDS * PAIRS_PER_SHARD
    # Account for different native runner CPUs: resample within each runner.
    rng = random.Random(SEED)
    draws = 20000
    groups = [[pair["old"]["seconds"] / pair["normal"]["seconds"] for pair in r["pairs"]] for r in records]
    all_ratios = [x for g in groups for x in g]
    boot = sorted(statistics.median([x for group in groups for x in rng.choices(group, k=len(group))]) - 1
                  for _ in range(draws))
    paired_median_change = statistics.median(all_ratios) - 1
    pooled = {
        "paired_median_change": paired_median_change,
        "stratified_bootstrap_ci95": [boot[int(draws * .025)], boot[int(draws * .975) - 1]],
        "pairs": len(pairs),
        "ordinary_release_median_seconds": statistics.median(x["normal"]["seconds"] for x in pairs),
        "existing_pgo_median_seconds": statistics.median(x["old"]["seconds"] for x in pairs),
        "sampling": "stratified by native Windows runner, 20,000 bootstrap replicates, deterministic seed"
    }
    lo, hi = pooled["stratified_bootstrap_ci95"]
    interpretation = ("Existing PGO faster (95% upper < 0)" if hi < 0 else
                      "Existing PGO slower (95% lower > 0)" if lo > 0 else
                      "Inconclusive (95% CI includes zero)")
    summary = {
        "question": "Does the existing SRG PGO outperform ordinary release on Windows menu 4 for 8,145,060 records?",
        "ratio_definition": "existing PGO time / ordinary release time - 1; negative = existing PGO faster",
        "pooled": pooled, "interpretation": interpretation,
        "by_runner": [{"shard": r["shard"], "environment": r["environment"],
                       "result": bootstrap(r["pairs"], SEED + r["shard"])} for r in records],
        "source_commit": first["source_commit"],
        "binary_sha256": first["binary_sha256"],
        "product_source_checksums": first["product_source_checksums"],
        "binary_bytes": first["binary_bytes"], "total_runs": len(pairs) * 2,
        "raw_shards": records
    }
    OUTPUT.mkdir(parents=True, exist_ok=True)
    (OUTPUT / "final_results.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print("FINAL", json.dumps({"interpretation": interpretation, **pooled}, ensure_ascii=False), flush=True)

if __name__ == "__main__":
    try:
        phase = os.environ["SRG_COMPARE_PHASE"]
        if phase == "measure":
            measure(int(os.environ["SRG_COMPARE_SHARD"]))
        elif phase == "combine":
            combine()
        else:
            raise ValueError(phase)
    finally:
        p.SERVER.shutdown()
        p.SERVER.server_close()
