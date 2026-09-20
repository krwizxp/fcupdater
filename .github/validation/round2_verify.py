import argparse
import datetime
import hashlib
import io
import json
import os
from pathlib import Path
import random
import shutil
import statistics
import struct
import subprocess
import tarfile
import tempfile
import zlib

PAIRS = 31
LIMIT = 1.05
SEED = 20260920

def run(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)

def block(text, marker):
    start = text.index(marker)
    brace = text.index("{", start)
    depth = 1
    end = brace + 1
    while depth:
        depth += (text[end] == "{") - (text[end] == "}")
        end += 1
    return text[start:end]

def executable_sections(data):
    sections = {}
    if data[:4] == b"\x7fELF":
        offset = struct.unpack_from("<Q", data, 40)[0]
        width, count, names_index = struct.unpack_from("<HHH", data, 58)
        names_header = offset + width * names_index
        names_offset, names_size = struct.unpack_from("<QQ", data, names_header + 24)
        names = data[names_offset:names_offset + names_size]
        for index in range(count):
            header = offset + width * index
            name_offset = struct.unpack_from("<I", data, header)[0]
            flags = struct.unpack_from("<Q", data, header + 8)[0]
            start, size = struct.unpack_from("<QQ", data, header + 24)
            if flags & 4:
                name = names[name_offset:].split(b"\0", 1)[0].decode()
                sections[name] = data[start:start + size]
    elif data[:2] == b"MZ":
        pe = struct.unpack_from("<I", data, 60)[0]
        count = struct.unpack_from("<H", data, pe + 6)[0]
        optional_size = struct.unpack_from("<H", data, pe + 20)[0]
        for index in range(count):
            header = pe + 24 + optional_size + 40 * index
            name = data[header:header + 8].rstrip(b"\0").decode()
            size, start = struct.unpack_from("<II", data, header + 16)
            flags = struct.unpack_from("<I", data, header + 36)[0]
            if flags & 0x20000000:
                sections[name] = data[start:start + size]
    elif struct.unpack_from("<I", data)[0] == 0xFEEDFACF:
        count = struct.unpack_from("<I", data, 16)[0]
        offset = 32
        for _ in range(count):
            command, size = struct.unpack_from("<II", data, offset)
            if command == 0x19:
                section_count = struct.unpack_from("<I", data, offset + 64)[0]
                for index in range(section_count):
                    header = offset + 72 + 80 * index
                    name = data[header:header + 16].rstrip(b"\0").decode()
                    length = struct.unpack_from("<Q", data, header + 40)[0]
                    start = struct.unpack_from("<I", data, header + 48)[0]
                    if name == "__text":
                        sections[name] = data[start:start + length]
            offset += size
    assert sections, "No executable sections found"
    return sections

def product_builds(root, base, role, work):
    source = work / "product"
    source.mkdir()
    archive = subprocess.check_output(["git", "archive", base], cwd=root)
    with tarfile.open(fileobj=io.BytesIO(archive)) as contents:
        contents.extractall(source, filter="data")
    baseline_source = work / "baseline-source"
    shutil.copytree(source, baseline_source)
    target = work / "target"
    cargo = os.environ.get("GOAL_CARGO", "cargo")
    binaries = []
    for phase in ("baseline", "candidate"):
        if phase == "candidate":
            tracked = subprocess.check_output(["git", "ls-files", "-z"], cwd=root)
            for path in tracked.decode().split("\0"):
                if path and not path.startswith(".github/"):
                    destination = source / path
                    destination.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copyfile(root / path, destination)
        run([cargo, "+1.98.1", "build", "--release", "--frozen"], cwd=source,
            env={**os.environ, "CARGO_TARGET_DIR": str(target)})
        executable = target / "release" / (role + (".exe" if os.name == "nt" else ""))
        saved = work / (phase + executable.suffix)
        shutil.copy2(executable, saved)
        binaries.append(saved)
    return baseline_source, binaries

def probe_source(root, role):
    if role == "fcupdater":
        source = (root / "src/excel/zip_archive.rs").read_text()
        pieces = ["use std::process;", block(source, "const CRC32_TABLES:") + ";"]
        if "const fn crc32_advance_table(" in source:
            pieces.append(block(source, "const fn crc32_advance_table("))
        pieces.extend(block(source, marker) for marker in
                      ("fn crc32_table_value(", "pub(super) fn crc32_update("))
        pieces.append('''
pub(super) fn value(data: &[u8]) -> u32 {
    let seed = u32::from_le_bytes(data[..4].try_into().unwrap());
    !crc32_update(!seed, &data[4..])
}
pub(super) fn check(data: &[u8]) -> String { value(data).to_string() }
pub(super) fn measure(data: &[u8]) { std::hint::black_box(value(data)); }
pub(super) fn tables() {
    use std::io::Write;
    let mut out = std::io::stdout().lock();
    for table in CRC32_TABLES { for value in table { out.write_all(&value.to_le_bytes()).unwrap(); } }
}
''')
    else:
        source = (root / "src/time.rs").read_text()
        pieces = ['''
use alloc::borrow::Cow;
use core::{error::Error, fmt, result::Result as CoreResult};
type BoxError = Box<dyn Error + Send + Sync>;
type Result<T> = CoreResult<T, TimeError>;
''']
        for marker, derive in (("enum TimeErrorKind", "#[derive(Clone, Copy, Debug)]"),
                               ("pub(super) struct TimeError", "#[derive(Debug)]"),
                               ("struct CivilDate", ""), ("impl TimeError {", ""),
                               ("impl fmt::Display for TimeError", ""),
                               ("impl Error for TimeError", "")):
            pieces.append(derive + "\n" + block(source, marker))
        pieces.append("mod util {" + (root / "src/time/util.rs").read_text() + "}")
        pieces.append("mod http_date {" + (root / "src/time/http_date.rs").read_text() + "}")
        pieces.append('''
pub(super) fn check(data: &[u8]) -> String {
    match std::str::from_utf8(data).unwrap().parse::<http_date::HttpDate>() {
        Ok(http_date::HttpDate(time)) => match time.duration_since(std::time::UNIX_EPOCH) {
            Ok(value) => format!("O:{}", value.as_secs()),
            Err(value) => format!("O:-{}", value.duration().as_secs()),
        },
        Err(error) => format!("E:{error}"),
    }
}
pub(super) fn measure(data: &[u8]) {
    drop(std::hint::black_box(std::str::from_utf8(data).unwrap().parse::<http_date::HttpDate>()));
}
pub(super) fn tables() {}
''')
    return "extern crate alloc;\nmod engine {\n" + "\n".join(pieces) + "\n}\n" + '''
fn main() {
    use std::io::Write;
    let args: Vec<String> = std::env::args().collect();
    if args[1] == "tables" { engine::tables(); return; }
    let bytes = std::fs::read(&args[2]).unwrap();
    let mut remaining = bytes.as_slice();
    let mut cases = Vec::new();
    while !remaining.is_empty() {
        let len = u32::from_le_bytes(remaining[..4].try_into().unwrap()) as usize;
        cases.push(&remaining[4..4 + len]);
        remaining = &remaining[4 + len..];
    }
    if args[1] == "check" {
        let mut out = std::io::BufWriter::new(std::io::stdout().lock());
        for data in cases { writeln!(out, "{}", engine::check(data)).unwrap(); }
    } else {
        let loops: usize = args[3].parse().unwrap();
        let start = std::time::Instant::now();
        for _ in 0..loops { for data in &cases { engine::measure(std::hint::black_box(data)); } }
        println!("{}", start.elapsed().as_nanos());
    }
}
'''

def corpus(role):
    rng = random.Random(SEED)
    if role == "fcupdater":
        cases, expected = [], []
        for length in list(range(273)) + [511, 512, 513, 4095, 4096, 4097, 65535, 65536, 65537]:
            data = rng.randbytes(length)
            for seed in (0, 0xFFFFFFFF, rng.getrandbits(32)):
                cases.append(struct.pack("<I", seed) + data)
                expected.append(str(zlib.crc32(data, seed)))
        bench = [struct.pack("<I", 0) + rng.randbytes(length) for length in (16, 8192)]
        return cases, expected, bench, 20000
    origin = datetime.datetime(1994, 11, 6, tzinfo=datetime.timezone.utc)
    cases, expected = [], []
    for second in range(86400):
        date = origin + datetime.timedelta(seconds=second)
        cases.append(date.strftime("%a, %d %b %Y %H:%M:%S GMT").encode())
        expected.append("O:" + str(int(date.timestamp())))
    for date in [datetime.datetime(year, month, day, 12, 34, 56, tzinfo=datetime.timezone.utc)
                 for year, month, day in [(1970, 1, 1), (2000, 2, 29), (2024, 2, 29), (2026, 9, 20), (2038, 1, 19)]]:
        for fmt in ("%a, %d %b %Y %H:%M:%S GMT", "%A, %d-%b-%y %H:%M:%S GMT", "%a %b %d %H:%M:%S %Y"):
            # RFC850 resolves 70 against the current century; omit that ambiguous fixture.
            if date.year == 1970 and "%y" in fmt:
                continue
            cases.append(date.strftime(fmt).encode())
            expected.append("O:" + str(int(date.timestamp())))
    bench = cases[::677][:128]
    for length in range(17):
        cases.append(b"Sun, 06 Nov 1994 " + b"0" * length + b" GMT")
        expected.append(None)
    for index in range(8):
        for byte in range(128):
            token = bytearray(b"08:49:37")
            token[index] = byte
            cases.append(b"Sun, 06 Nov 1994 " + bytes(token) + b" GMT")
            expected.append(None)
    for token in ("24:00:00", "23:60:00", "23:59:60", "０８:４９:３７", "08／49／37", ""):
        cases.append(("Sun, 06 Nov 1994 " + token + " GMT").encode())
        expected.append(None)
    bench += cases[-32:]
    return cases, expected, bench, 3000

def write_corpus(path, cases):
    with path.open("wb") as output:
        for data in cases:
            output.write(struct.pack("<I", len(data)))
            output.write(data)

def paired_bounds(before, after):
    ratios = [new / old for old, new in zip(before, after)]
    rng = random.Random(SEED)
    samples = sorted(statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(10000))
    return samples[9500]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=["srg", "fcupdater"])
    parser.add_argument("baseline")
    parser.add_argument("--root", default=".")
    parser.add_argument("--native-only", action="store_true")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    work = Path(tempfile.mkdtemp(prefix="goal-round2-", dir=os.environ.get("RUNNER_TEMP")))
    baseline_source, binaries = product_builds(root, args.baseline, args.role, work)
    images = [path.read_bytes() for path in binaries]
    code = [executable_sections(data) for data in images]
    native = {"platform": os.sys.platform, "sizes": [len(data) for data in images],
              "sha256": [hashlib.sha256(data).hexdigest() for data in images],
              "executable_sections_equal": code[0] == code[1]}
    print("NATIVE " + json.dumps(native), flush=True)
    assert native["executable_sections_equal"], "Investigate native instruction difference"
    for arguments in (["--help"], ["--version"], ["--unknown"], ["--help", "extra"]):
        observed = []
        for executable in binaries:
            result = subprocess.run([str(executable), *arguments], cwd=work, capture_output=True)
            observed.append((result.returncode, result.stdout, result.stderr))
        assert observed[0] == observed[1], arguments
    if args.native_only:
        return
    if hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
    probes = []
    rustc = os.environ.get("GOAL_RUSTC", "rustc")
    for name, source in (("before", baseline_source), ("after", root)):
        path = work / (name + ".rs")
        path.write_text(probe_source(source, args.role))
        executable = work / name
        run([rustc, "+1.98.1", "--edition=2024", "-C", "opt-level=3", "-C", "lto=fat",
             "-C", "codegen-units=1", "-C", "panic=abort", str(path), "-o", str(executable)])
        probes.append(executable)
    cases, expected, bench, loops = corpus(args.role)
    fixtures, bench_file = work / "cases.bin", work / "bench.bin"
    write_corpus(fixtures, cases)
    write_corpus(bench_file, bench)
    observations = [subprocess.check_output([str(executable), "check", str(fixtures)]).decode().splitlines()
                    for executable in probes]
    assert observations[0] == observations[1]
    assert len(observations[0]) == len(expected)
    for got, wanted in zip(observations[1], expected):
        if wanted is not None:
            assert got == wanted, (got, wanted)
    if args.role == "fcupdater":
        tables = [subprocess.check_output([str(executable), "tables"]) for executable in probes]
        wanted = b"".join(struct.pack("<I", ~zlib.crc32(bytes([byte]) + bytes(index), 0xFFFFFFFF) & 0xFFFFFFFF)
                          for index in range(16) for byte in range(256))
        assert tables[0] == tables[1] == wanted
        print("TABLE " + hashlib.sha256(wanted).hexdigest(), flush=True)
    print("EQUIVALENCE " + json.dumps({"cases": len(cases), "independent_expected": sum(x is not None for x in expected)}), flush=True)
    measure = lambda executable: int(subprocess.check_output([str(executable), "bench", str(bench_file), str(loops)]))
    for executable in probes * 2:
        measure(executable)
    samples = [[], []]
    for pair in range(PAIRS):
        for side in ([0, 1] if pair % 2 == 0 else [1, 0]):
            samples[side].append(measure(probes[side]))
    upper = paired_bounds(*samples)
    result = {"pairs": PAIRS, "seed": SEED, "before_ns": samples[0], "after_ns": samples[1],
              "median_ns": [statistics.median(x) for x in samples], "upper95_ratio": upper,
              "limit": LIMIT, "pass": upper <= LIMIT,
              "scope": "fixed parser/CRC inputs; input setup, network and output excluded"}
    print("PERFORMANCE " + json.dumps(result), flush=True)
    assert result["pass"], "Noninferiority remains unresolved"

if __name__ == "__main__":
    main()
