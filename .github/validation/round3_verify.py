import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import random
import re
import shutil
import statistics
import struct
import subprocess
import tarfile
import tempfile

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

def probe_source(root, role):
    if role == "fcupdater":
        source = (root / "src/excel/writer/cell_ref.rs").read_text()
        writer = (root / "src/excel/writer.rs").read_text()
        pieces = [block(writer, "struct CellReference {"),
                  "const MAX_A1_COLUMN_LETTERS: usize = 3;",
                  "const MAX_A1_COL: u32 = 0x4000;",
                  "const MAX_A1_ROW: u32 = 0x0010_0000;",
                  block(source, "pub(super) fn parse_ref_with_locks(")]
        pieces.append("""
pub(super) fn check(data: &[u8]) -> String {
    match parse_ref_with_locks(std::str::from_utf8(data).unwrap()) {
        Some(value) => format!("{}:{}:{}:{}", value.col, value.row, value.col_locked, value.row_locked),
        None => "N".into(),
    }
}
pub(super) fn measure(data: &[u8], buffer: &mut [u8; 32]) {
    std::hint::black_box(parse_ref_with_locks(std::str::from_utf8(data).unwrap()));
    std::hint::black_box(buffer);
}
""")
        prefix = ""
    else:
        source = (root / "src/output.rs").read_text()
        pieces = ["use crate::buffmt::{ByteCursor, digit_byte, two_digits};", "use std::process;",
                  'static HEX_DIGITS: &[u8; 16] = b"0123456789ABCDEF";',
                  "const U8_THREE_DIGIT_THRESHOLD: u8 = 100;",
                  "const U8_TWO_DIGIT_THRESHOLD: u8 = 10;",
                  "const TWO_DIGIT_WIDTH: usize = 2;"]
        pieces.extend(block(source, marker) for marker in
                      ("fn hex_byte(", "fn buf_write_u8_dec(", "fn buf_write_prefixed_hex24("))
        pieces.append("""
fn render(data: &[u8], buffer: &mut [u8; 32]) -> usize {
    let mut cursor = ByteCursor::new(buffer);
    if data.len() == 1 {
        buf_write_u8_dec(&mut cursor, data[0]);
    } else {
        buf_write_prefixed_hex24(&mut cursor, &data[3..], data[0], data[1], data[2]);
    }
    cursor.written_len()
}
pub(super) fn check(data: &[u8]) -> String {
    let mut buffer = [0; 32];
    let len = render(data, &mut buffer);
    String::from_utf8(buffer[..len].to_vec()).unwrap()
}
pub(super) fn measure(data: &[u8], buffer: &mut [u8; 32]) {
    let len = render(data, buffer);
    std::hint::black_box(&buffer[..len]);
}
""")
        prefix = "mod buffmt {" + (root / "src/buffmt.rs").read_text() + "}\n"
    return prefix + "mod engine {\n" + "\n".join(pieces) + "\n}\n" + """
fn main() {
    use std::io::Write;
    let args: Vec<String> = std::env::args().collect();
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
        let warm: usize = args[4].parse().unwrap();
        let mut buffer = [0; 32];
        for _ in 0..warm {
            for data in &cases { engine::measure(std::hint::black_box(data), &mut buffer); }
        }
        let start = std::time::Instant::now();
        for _ in 0..loops {
            for data in &cases { engine::measure(std::hint::black_box(data), &mut buffer); }
        }
        println!("{}", start.elapsed().as_nanos());
    }
}
"""

def corpus(role):
    rng = random.Random(SEED)
    if role == "srg":
        decimal = [bytes([n]) for n in range(256)]
        rgb = [bytes([a, b, c]) + prefix.encode() for prefix in ("", "#", "m#")
               for a, b, c in [(n, 0, 255) for n in range(256)]
               + [(0, n, 255) for n in range(256)]
               + [(0, 255, n) for n in range(256)]]
        rgb += [rng.randbytes(3) + b"#" for _ in range(4096)]
        cases = decimal + rgb
        expected = [str(data[0]) if len(data) == 1 else data[3:].decode() + data[:3].hex().upper()
                    for data in cases]
        return cases, expected, {"decimal-all-u8": (decimal, 50000), "hex24-mixed": (rgb, 2000)}
    cases = []
    for width in range(1, 4):
        import itertools
        for letters in itertools.product("ABCDEFGHIJKLMNOPQRSTUVWXYZ", repeat=width):
            name = "".join(letters)
            for locks in ("{}", "$" + "{}", "{}$", "$" + "{}$"):
                col = locks.format(name)
                cases.extend([col + "1", col.lower() + "1048576"])
    for name in ("A", "XFD", "XFE", "ZZZ", "AAAA", "", "é", "Ａ"):
        for row in ("", "0", "0001", "1048575", "1048576", "1048577", "4294967295",
                    "999999999999999999999", "0" * 256 + "1", "+1", "-1", "１", "1x", " 1", "1 "):
            cases.append(name + row)
    cases += ["$", "$$", "A$$1", "A1$", "A:1", "A1:B2", "A\0" + "1", "\nA1"]
    for _ in range(5000):
        cases.append("".join(rng.choices("ABCxyz$0123456789+- :;\n", k=rng.randrange(1, 20))))
    def expected(text):
        match = re.fullmatch(r"(\$?)([A-Za-z]{1,3})(\$?)([0-9]+)", text)
        if not match:
            return "N"
        col = 0
        for letter in match[2].upper():
            col = col * 26 + ord(letter) - 64
        row = int(match[4])
        if not (1 <= col <= 16384 and 1 <= row <= 1048576):
            return "N"
        return f"{col}:{row}:{str(bool(match[1])).lower()}:{str(bool(match[3])).lower()}"
    wanted = [expected(text) for text in cases]
    encoded = [text.encode() for text in cases]
    small = [text.encode() for text in ("A1", "$A1", "XFD1048576", "$XFD$1048576",
             "aa1", "ABC123", "ZZZ1", "AAAA1", "A0", "XFE1", "A1048577", "A-1",
             "$a$0001", "Z15", "W883", "A1x")]
    return encoded, wanted, {"short-boundaries": (small, 100000), "mixed-4096": (encoded[::37][:4096], 500)}

def measure(executable, fixture, loops, warm):
    args = [str(executable), "bench", str(fixture), str(loops), str(warm)]
    if Path("/usr/bin/time").exists():
        result = subprocess.run(["/usr/bin/time", "-f", "RSS:%M", *args], check=True, capture_output=True, text=True)
        return int(result.stdout), int(result.stderr.strip().removeprefix("RSS:"))
    return int(subprocess.check_output(args)), None

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("role", choices=["srg", "fcupdater"])
    parser.add_argument("baseline")
    parser.add_argument("--root", default=".")
    parser.add_argument("--native-only", action="store_true")
    parser.add_argument("--unchanged-source", action="store_true")
    args = parser.parse_args()
    root = Path(args.root).resolve()
    work = Path(tempfile.mkdtemp(prefix="goal-round3-", dir=os.environ.get("RUNNER_TEMP")))
    baseline_source, binaries = product_builds(root, args.baseline, args.role, work)
    images = [path.read_bytes() for path in binaries]
    native = {"platform": os.sys.platform, "sizes": [len(data) for data in images],
              "sha256": [hashlib.sha256(data).hexdigest() for data in images],
              "executable_sections_equal": executable_sections(images[0]) == executable_sections(images[1])}
    print("NATIVE " + json.dumps(native), flush=True)
    assert len(images[1]) <= len(images[0]), "Release size increased"
    for arguments in (["--help"], ["--version"], ["--unknown"], ["--help", "extra"]):
        observed = []
        for executable in binaries:
            result = subprocess.run([str(executable), *arguments], cwd=work, capture_output=True)
            observed.append((result.returncode, result.stdout, result.stderr))
        assert observed[0] == observed[1], arguments
    if args.unchanged_source:
        paths = subprocess.check_output(["git", "ls-files", "*.rs"], cwd=root, text=True).splitlines()
        assert all((root / path).read_bytes() == (baseline_source / path).read_bytes() for path in paths)
        print("UNCHANGED_SOURCE " + json.dumps({"rust_files": len(paths), "existing_behavior_evidence_reused": True}), flush=True)
        return
    probes = []
    rustc = os.environ.get("GOAL_RUSTC", "rustc")
    for name, source in (("before", baseline_source), ("after", root)):
        path = work / (name + ".rs")
        path.write_text(probe_source(source, args.role))
        executable = work / (name + (".exe" if os.name == "nt" else ""))
        run([rustc, "+1.98.1", "--edition=2024", "-C", "opt-level=3", "-C", "lto=fat",
             "-C", "codegen-units=1", "-C", "panic=abort", str(path), "-o", str(executable)])
        probes.append(executable)
    cases, expected, groups = corpus(args.role)
    fixture = work / "cases.bin"
    write_corpus(fixture, cases)
    observations = [subprocess.check_output([str(executable), "check", str(fixture)]).decode().splitlines()
                    for executable in probes]
    assert observations[0] == observations[1] == expected
    print("EQUIVALENCE " + json.dumps({"cases": len(cases), "independent_expected": len(expected),
          "corpus_sha256": hashlib.sha256(fixture.read_bytes()).hexdigest()}), flush=True)
    if args.native_only:
        return
    if hasattr(os, "sched_setaffinity"):
        os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
    for label, (inputs, loops) in groups.items():
        fixture = work / (label + ".bin")
        write_corpus(fixture, inputs)
        for warm in (0, 100):
            samples, rss = [[], []], [[], []]
            for pair in range(PAIRS):
                for side in ([0, 1] if pair % 2 == 0 else [1, 0]):
                    elapsed, peak = measure(probes[side], fixture, loops, warm)
                    samples[side].append(elapsed)
                    rss[side].append(peak)
            upper = paired_bounds(*samples)
            memory_upper = paired_bounds(*rss) if rss[0][0] is not None else None
            result = {"group": label, "warmup_passes": warm, "pairs": PAIRS, "seed": SEED,
                      "before_ns": samples[0], "after_ns": samples[1],
                      "median_ns": list(map(statistics.median, samples)),
                      "p95_ns": [sorted(values)[29] for values in samples],
                      "upper95_ratio": upper, "limit": LIMIT, "pass": upper <= LIMIT,
                      "rss_kib": rss if memory_upper else None, "rss_upper95": memory_upper,
                      "scope": "extracted actual source; input setup and process startup excluded"}
            print("PERFORMANCE " + json.dumps(result), flush=True)
            assert upper <= LIMIT, "Noninferiority unresolved"
            if memory_upper:
                assert memory_upper <= LIMIT, "Memory noninferiority unresolved"

if __name__ == "__main__":
    main()
