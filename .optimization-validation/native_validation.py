"""Task-owned native validation; never installed in the product path."""
import argparse
from decimal import Decimal
import hashlib
import http.server
import threading
import json
import os
from pathlib import Path
import random
import re
import shutil
import statistics
import subprocess
import sys
import tarfile
import time
import xml.etree.ElementTree as ET
import zipfile

VERSION = '1.99.0'
BASE = {'srg': 'b9933fd84f3e696a15f2a2880bd4093be87b587d',
        'fcupdater': '7efaa72ed632cfa4b4eb1aec4c050345786cafa5'}
NS = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}


class NativeValidation:
    def __init__(self, repo, target, name):
        self.repo, self.target, self.name = repo, target, name
        self.selector = VERSION + '-' + target
        self.checkout = Path.cwd()
        self.tools = self.checkout / '.optimization-validation'
        # Sibling checkouts prevent Cargo from merging parent array-valued aliases.
        self.root = self.checkout.parent / ('optimization-native-work-' + repo)
        self.out = self.checkout / 'optimization-native-results'
        self.root.mkdir(exist_ok=False)
        self.out.mkdir(exist_ok=False)
        self.result = {'repo': repo, 'target': target, 'baseline_sha': BASE[repo],
                       'validation_sha': os.environ.get('GITHUB_SHA'),
                       'compiler': '', 'commands': [], 'variants': {}, 'timings': []}
        self.serial = 0
        self.base_env = dict(os.environ)
        self.base_env.pop('RUSTFLAGS', None)
        self.base_env.pop('CARGO_ENCODED_RUSTFLAGS', None)
        self.base_env['CARGO_INCREMENTAL'] = '0'

    def save(self):
        (self.out / 'result.json').write_text(json.dumps(self.result, indent=2), encoding='utf-8')

    def run(self, args, cwd=None, env=None, expected=(0,), timeout=300, label=None):
        self.serial += 1
        label = label or str(args[0])
        log = self.out / f'{self.serial:03d}-{label}.log'
        started = time.monotonic()
        completed = subprocess.run(list(map(str, args)), cwd=cwd, env=env or self.base_env,
                                   capture_output=True, timeout=timeout)
        log.write_bytes(completed.stdout + b'\n--- stderr ---\n' + completed.stderr)
        self.result['commands'].append({'args': list(map(str, args)), 'cwd': str(cwd),
                                       'status': completed.returncode, 'log': log.name,
                                       'seconds': time.monotonic() - started})
        self.save()
        if completed.returncode not in expected:
            raise RuntimeError(f'{label} failed ({completed.returncode}): '
                               f'{completed.stderr.decode(errors="replace")[-5000:]}')
        return completed

    def snapshot(self, variant, ref):
        dest = self.root / variant
        archive = self.root / f'{variant}.tar'
        self.run(['git', 'archive', '--format=tar', '-o', archive, ref], label=f'{variant}-archive')
        dest.mkdir()
        with tarfile.open(archive) as contents:
            contents.extractall(dest, filter='data')
        archive.unlink()
        return dest

    def checks(self, variant, checkout):
        env = self.base_env | {'CARGO_TARGET_DIR': str(self.root / 'checks' / variant)}
        for label, args in [('fmt', ['fmt', '--all', '--', '--check']),
                            ('clippy', ['clippy', '--all-targets', '--frozen', '--target',
                                        self.target, '--', '-D', 'warnings']),
                            ('test', ['test', '--all-targets', '--frozen', '--target', self.target])]:
            self.run(['cargo', f'+{self.selector}', *args], cwd=checkout, env=env,
                     label=f'{variant}-{label}')

    def binary(self, directory):
        return directory / self.target / 'release' / (self.repo + ('.exe' if os.name == 'nt' else ''))

    def package(self, variant, checkout, directory):
        binary = self.binary(directory)
        entry_name = self.name + '-' + variant
        env = self.base_env | {'CARGO_TARGET_DIR': str(directory / self.target), 'RUSTFLAGS': ''}
        self.run(['cargo', f'+{self.selector}', 'run', '--frozen', '--example', 'package_artifact',
                  '--', entry_name], cwd=checkout, env=env, label=f'{variant}-package')
        artifact = checkout / 'artifacts' / (entry_name + ('.exe' if os.name == 'nt' else '.tar'))
        data = binary.read_bytes()
        if os.name == 'nt':
            assert artifact.read_bytes() == data
        else:
            with tarfile.open(artifact) as archive:
                entries = archive.getmembers()
                assert len(entries) == 1 and entries[0].name == entry_name
                assert archive.extractfile(entries[0]).read() == data
        shutil.copyfile(artifact, self.out / artifact.name)
        return {'binary_bytes': len(data), 'binary_sha256': hashlib.sha256(data).hexdigest(),
                'package_bytes': artifact.stat().st_size, 'package_name': artifact.name}

    def cli(self, binary):
        cases = [['--help'], ['-h'], ['--version'], ['--bad'], ['--version', '--bad'],
                 ['--help', '--bad']]
        if self.repo == 'srg':
            cases += [['generate'], ['generate', '0'], ['generate', '-1'], ['generate', 'abc'],
                      ['random-integer', '5', '3'], ['random-float', 'nan', '1'],
                      ['time-observe', '', '0'], ['ladder', '', '']]
        empty = self.root / 'empty'
        empty.mkdir(exist_ok=True)
        values = []
        for args in cases:
            r = self.run([binary, *args], cwd=empty, expected=(0, 1), label='cli')
            values.append({'args': args, 'status': r.returncode,
                           'stdout': r.stdout.hex(), 'stderr': r.stderr.hex()})
        return values

    def make_probe(self, checkout, allocation=False):
        main = checkout / 'src/main.rs'
        original = main.read_text(encoding='utf-8')
        marker = 'fn main() -> Result<()> {'
        assert original.count(marker) == 1
        original = original.replace(marker, 'fn production_main() -> Result<()> {', 1)
        harness = (self.tools / (self.repo + '_probe.rs')).read_text(encoding='utf-8')
        if allocation:
            harness = (self.tools / 'allocation_counter.rs').read_text(encoding='utf-8') + '\n' + harness
            harness = harness.replace('let start=Instant::now();', 'COUNT_ALLOC.store(true, Ordering::Relaxed);\n    let start=Instant::now();')
            harness = harness.replace('println!("{}",start.elapsed().as_nanos());', 'COUNT_ALLOC.store(false, Ordering::Relaxed);\n    println!("{} {} {}",start.elapsed().as_nanos(), ALLOC_CALLS.load(Ordering::Relaxed), ALLOC_BYTES.load(Ordering::Relaxed));')
            harness = harness.replace('let start = std::time::Instant::now();',
                                      'COUNT_ALLOC.store(true, Ordering::Relaxed);\n'
                                      '        let start = std::time::Instant::now();', 1)
            harness = harness.replace('println!("{} {}", start.elapsed().as_nanos(), bytes);',
                                      'COUNT_ALLOC.store(false, Ordering::Relaxed);\n'
                                      '        println!("{} {} {} {}", start.elapsed().as_nanos(), bytes, '
                                      'ALLOC_CALLS.load(Ordering::Relaxed), ALLOC_BYTES.load(Ordering::Relaxed));')
            harness = harness.replace('let result = master_sheet::MasterSheetUpdater { source_index: std::hint::black_box(&source) }.update(&mut book)?;',
                                      'COUNT_ALLOC.store(true, Ordering::Relaxed);\n'
                                      '        let result = master_sheet::MasterSheetUpdater { source_index: std::hint::black_box(&source) }.update(&mut book)?;\n'
                                      '        COUNT_ALLOC.store(false, Ordering::Relaxed);')
            harness = harness.replace('println!("{nanos} {total}");',
                                      'println!("{} {} {} {}", nanos, total, ALLOC_CALLS.load(Ordering::Relaxed), ALLOC_BYTES.load(Ordering::Relaxed));')
        main.write_text(original + '\n' + harness, encoding='utf-8')
        if self.repo == 'fcupdater':
            excel = checkout / 'src/excel.rs'
            excel.write_text(excel.read_text(encoding='utf-8').replace(
                'mod source_reader;', 'pub(crate) mod source_reader;', 1), encoding='utf-8')
            writer = checkout / 'src/excel/writer.rs'
            writer.write_text(writer.read_text(encoding='utf-8') + '\n' +
                              (self.tools / 'fc_writer_probe.rs').read_text(encoding='utf-8'), encoding='utf-8')

    def build_probe(self, variant, checkout, flags=None):
        directory = self.root / 'probe-build' / variant
        env = self.base_env | {'CARGO_TARGET_DIR': str(directory)}
        if flags is not None:
            env['RUSTFLAGS'] = flags
        r = self.run(['cargo', f'+{self.selector}', 'build', '--release', '--frozen', '--bin', self.repo,
                      '--target', self.target], cwd=checkout, env=env, label=f'{variant}-probe-build')
        return self.binary(directory), r

    def timed(self, binary, mode, workbook):
        args = [binary, mode]
        if workbook:
            args.append(workbook)
        if mode == 'roundtrip':
            args.append(self.root / (binary.parent.parent.parent.name + '-roundtrip.xlsx'))
        r = subprocess.run(list(map(str, args)), capture_output=True, timeout=120, env=self.base_env)
        if r.returncode:
            raise RuntimeError(r.stderr.decode(errors='replace'))
        return int(r.stdout.split()[0])

    def paired(self, candidate, old, new, mode, workbook=None):
        def series(samples):
            for _ in range(3):
                self.timed(old, mode, workbook)
                self.timed(new, mode, workbook)
            raw = []
            for i in range(samples):
                order = [('baseline', old), ('candidate', new)]
                if i % 2 == 0:
                    order.reverse()
                raw.append({name: self.timed(exe, mode, workbook) for name, exe in order})
            ratios = [r['candidate'] / r['baseline'] for r in raw]
            rng = random.Random(20261011)
            boot = sorted(statistics.median(rng.choices(ratios, k=len(ratios))) for _ in range(10000))
            return {'samples': samples, 'raw_nanoseconds': raw,
                    'baseline_median_ms': statistics.median(r['baseline'] for r in raw) / 1e6,
                    'candidate_median_ms': statistics.median(r['candidate'] for r in raw) / 1e6,
                    'paired_ratio_median': statistics.median(ratios),
                    'ratio_95pct_bootstrap': [boot[250], boot[9749]]}
        measured = {'candidate': candidate, 'mode': mode, 'first': series(32)}
        if measured['first']['ratio_95pct_bootstrap'][1] > 1.05:
            measured['bounded_repeat'] = series(64)
        final = measured.get('bounded_repeat', measured['first'])
        measured['protected_runtime_pass'] = final['ratio_95pct_bootstrap'][1] <= 1.05
        self.result['timings'].append(measured)
        self.save()
        print(json.dumps(measured), flush=True)

    def semantic_xlsx(self, path):
        with zipfile.ZipFile(path) as archive:
            assert archive.testzip() is None
            # Independent stdlib XML parser verifies all XML parts and decompressed content.
            parts = {}
            for name in sorted(archive.namelist()):
                data = archive.read(name)
                if name.endswith(('.xml', '.rels')):
                    ET.fromstring(data)
                parts[name] = hashlib.sha256(data).hexdigest()
            sheet = ET.fromstring(archive.read('xl/worksheets/sheet1.xml'))
            rank_rows = []
            cells = {c.get('r'): c for c in sheet.findall('.//s:c', NS)}
            for cell in cells.values():
                match = re.fullmatch(r'A(\d+)', cell.get('r', ''))
                if match and int(match[1]) >= 15 and cell.find('s:f', NS) is not None:
                    rank_rows.append(int(match[1]))
            rank_rows.sort()
            assert rank_rows and rank_rows == list(range(15, 15 + len(rank_rows)))
            ranks, values, blank_rows = [], [], []
            for row in rank_rows:
                rank = cells['A' + str(row)].findtext('s:v', namespaces=NS)
                ranks.append(rank)
                total = cells.get('T' + str(row))
                value = total.findtext('s:v', namespaces=NS) if total is not None else None
                if value is not None and value != '':
                    key = cells['W' + str(row)].findtext('s:v', namespaces=NS)
                    assert key is not None and key != ''
                    values.append((row, Decimal(key)))
                else:
                    assert rank is None or rank == ''
                    blank_rows.append(row)
            for row, key in values:
                actual = cells['A' + str(row)].findtext('s:v', namespaces=NS)
                assert int(actual) == 1 + sum(other < key for _, other in values)
            return {'parts': parts, 'rank_cache_count': len(ranks),
                    'independent_competition_rank_checks': len(values),
                    'independent_blank_rank_checks': len(blank_rows),
                    'rank_cache_sha256': hashlib.sha256(json.dumps(ranks).encode()).hexdigest()}

    def train_fc(self, variant, checkout):
        raw = self.root / 'raw' / variant
        raw.mkdir(parents=True)
        directory = self.root / 'generate-build' / variant
        flags = f'-Cprofile-generate={raw}'
        env = self.base_env | {'CARGO_TARGET_DIR': str(directory), 'RUSTFLAGS': flags,
                               'LLVM_PROFILE_FILE': str(raw / 'cli-%p-%m.profraw')}
        self.run(['cargo', f'+{self.selector}', 'build', '--release', '--frozen', '--bin', self.repo,
                  '--target', self.target], cwd=checkout, env=env, label=f'{variant}-generate')
        binary = self.binary(directory)
        for args in [['--help'], ['--version'], ['--bad']]:
            self.run([binary, *args], env=env, expected=(0, 1), label=f'{variant}-cli-training')
        live = self.root / 'live' / variant
        live.mkdir(parents=True)
        master = checkout / 'fuel_cost_chungcheong.xlsx'
        shutil.copyfile(master, live / master.name)
        r = self.run([binary, '--verify'], cwd=live, env=env, label=f'{variant}-live-training', timeout=180)
        self.result['variants'][variant]['live_verify'] = {'status': r.returncode,
            'workbook': self.semantic_xlsx(live / master.name)}
        probe_checkout = self.root / (variant + '-probe')
        shutil.copytree(checkout, probe_checkout, ignore=shutil.ignore_patterns('target', 'artifacts', '.optimization-validation'))
        self.make_probe(probe_checkout)
        probe_binary, _ = self.build_probe(variant + '-generate', probe_checkout, flags)
        probe_env = self.base_env | {'LLVM_PROFILE_FILE': str(raw / 'probe-%p-%m.profraw')}
        workbook = checkout / master.name
        for mode in ['bench', 'parse-bench', 'intern-plain', 'intern-escaped']:
            for _ in range(3):
                self.run([probe_binary, mode, workbook], env=probe_env, label=f'{variant}-probe-training')
        training_output = self.root / f'{variant}-training.xlsx'
        shutil.copyfile(workbook, training_output)
        self.run([probe_binary, 'save', training_output, training_output],
                 env=probe_env, label=f'{variant}-save-training')
        raw_files = sorted(raw.glob('*.profraw'))
        assert len(raw_files) >= 7
        sysroot = self.run(['rustc', f'+{self.selector}', '--print', 'sysroot'], label='sysroot').stdout.decode().strip()
        profiler = Path(sysroot) / 'lib/rustlib' / self.target / 'bin' / ('llvm-profdata.exe' if os.name == 'nt' else 'llvm-profdata')
        profile = checkout / 'pgo' / f'{self.target}.profdata'
        self.run([profiler, 'merge', '-o', profile, *raw_files], label=f'{variant}-profile-merge')
        self.run([profiler, 'show', '--all-functions', '--counts', profile], label=f'{variant}-profile-show')
        shutil.copyfile(profile, self.out / f'{variant}-{self.target}.profdata')
        self.result['variants'][variant]['profile'] = {'raw_count': len(raw_files),
            'bytes': profile.stat().st_size, 'sha256': hashlib.sha256(profile.read_bytes()).hexdigest(),
            'training': 'original CLI live --verify + fixed repository workbook + 5000 plain/escaped strings; 3 repetitions'}
        shutil.copyfile(profile, probe_checkout / 'pgo' / profile.name)
        profile_flags = f'-Cprofile-use={profile} -Cllvm-args=-pgo-warn-missing-function'
        probe_binary, diagnostic = self.build_probe(variant + '-pgo', probe_checkout, profile_flags)
        pgo_diagnostics = diagnostic.stderr.decode(errors='replace')
        assert 'function control flow change detected' not in pgo_diagnostics
        assert 'hash mismatch' not in pgo_diagnostics
        missing = [line for line in pgo_diagnostics.splitlines() if 'no profile data available' in line]
        # Profiling changes LLVM inlining and can expose fresh compiler-generated helpers.
        # Retain those diagnostic warnings; never accept a lost nonzero count or affected function.
        assert all('up to 0 count discarded' in line for line in missing)
        assert all('RankSortRefresher' not in line and 'SharedStringTable' not in line for line in missing)
        self.result['variants'][variant]['diagnostic_probe_missing_profiles'] = missing
        return probe_binary, profile

    def release(self, variant, checkout, pgo=False):
        directory = self.root / 'release-build' / variant
        env = self.base_env | {'CARGO_TARGET_DIR': str(directory)}
        args = ['build-pgo'] if pgo else ['build', '--release', '--frozen', '--bin', self.repo]
        completed = self.run(['cargo', f'+{self.selector}', *args, '--target', self.target],
                             cwd=checkout, env=env, timeout=600, label=f'{variant}-release')
        return self.binary(directory), self.package(variant, checkout, directory), completed.stderr.decode(errors='replace')

    def numbers(self, binary, mode, workbook, values):
        completed = subprocess.run([str(binary), mode, str(workbook)], input='\n'.join(values)+'\n',
                                   text=True, capture_output=True, timeout=60, env=self.base_env)
        assert completed.returncode == 0, completed.stderr
        return completed.stdout.splitlines()

    def fc_contracts(self, old, new, workbook):
        original_hash = hashlib.sha256(workbook.read_bytes()).hexdigest()
        rng = random.Random(426013)
        micro = [rng.randrange(-10**12, 10**12) for _ in range(1000)] + [0, 1, -1, 9223372036854775807]
        plain = [('-' if n < 0 else '') + str(abs(n)//10**6) + '.' + str(abs(n)%10**6).zfill(6) for n in micro]
        expected = list(map(str, micro))
        assert self.numbers(old, 'numbers', workbook, plain) == expected
        assert self.numbers(new, 'numbers', workbook, plain) == expected
        assert self.numbers(new, 'numbers', workbook, [str(n)+'e-6' for n in micro]) == expected
        extra = ['100e-8', '1'+'0'*100+'e-106', '.5', '1e-7', 'NaN', 'INF', '1e', '1e+', '1e--2', '1.2.3', '1e19', '9223372036854.775808', '0.00000001', '0e-10000']
        assert self.numbers(new, 'numbers', workbook, extra) == ['1','1','500000','ERROR','ERROR','ERROR','ERROR','ERROR','ERROR','ERROR','ERROR','ERROR','ERROR','0']
        ints = ['1.5e0','-1.5E0','1.234e3','2147483647e0','-2147483648e0','2147483648e0','1e309','1e-9999','1e','1,000e0']
        assert self.numbers(new, 'integers', workbook, ints) == ['2','-2','1234','2147483647','-2147483648','NONE','NONE','0','NONE','NONE']
        assert self.numbers(old, 'integers', workbook, ['1500','1.5e3']) == ['1500','NONE']
        assert self.numbers(new, 'integers', workbook, ['1500','1.5e3']) == ['1500','1500']
        assert self.numbers(old, 'numbers', workbook, ['1E-6']) == ['ERROR']
        assert self.numbers(new, 'numbers', workbook, ['1E-6']) == ['1']
        raw = workbook.read_bytes()
        for i in range(16):
            bad = bytearray(raw)
            bad[rng.randrange(30, len(raw)-100)] ^= 0x80
            path = self.root / f'corrupt-{i}.xlsx'
            path.write_bytes(bad)
            for binary in [old, new]:
                completed = self.run([binary, 'parse', path], label='zip-rejection')
                assert completed.stdout.startswith(b'ERROR ')
            assert path.read_bytes() == bad
        outputs = {}
        for variant, binary in [('baseline',old), ('candidate',new)]:
            path = self.root / f'contract-{variant}.xlsx'
            self.run([binary, 'roundtrip', workbook, path], label=f'{variant}-roundtrip')
            outputs[variant] = self.semantic_xlsx(path)
        assert outputs['baseline'] == outputs['candidate'], 'normal workbook parts/cache differ'
        scientific = self.root / 'scientific.xlsx'
        with zipfile.ZipFile(workbook) as archive, zipfile.ZipFile(scientific, 'w') as output:
            for entry in archive.infolist():
                data = archive.read(entry.filename)
                if entry.filename == 'xl/worksheets/sheet1.xml':
                    text = data.decode('utf-8')
                    match = re.search(r'<c\b[^>]*\br="D4"[^>]*>.*?</c>', text, re.S)
                    assert match
                    replaced, count = re.subn(r'<v>[^<]*</v>', '<v>1E-6</v>', match.group(), count=1)
                    assert count == 1
                    data = (text[:match.start()]+replaced+text[match.end():]).encode('utf-8')
                output.writestr(entry, data)
        path = self.root / 'scientific-roundtrip.xlsx'
        self.run([new, 'roundtrip', scientific, path], label='scientific-roundtrip')
        scientific_output = self.semantic_xlsx(path)
        assert hashlib.sha256(workbook.read_bytes()).hexdigest() == original_hash
        self.result['behavior'] = {'numeric_plain_cases':len(plain), 'numeric_scientific_cases':len(micro),
            'numeric_edge_cases':len(extra), 'scientific_integer_cases':len(ints), 'corrupt_zip_rejections':16,
            'ordinary_roundtrip':outputs, 'scientific_roundtrip':scientific_output,
            'original_workbook_sha256':original_hash, 'original_workbook_unchanged':True}
        self.save()

    def peak_memory(self, variant, binary, workbook):
        args = [str(binary), 'roundtrip', str(workbook), str(self.root/f'memory-{variant}.xlsx')]
        if sys.platform.startswith('linux'):
            output = self.out/f'{variant}-rss.txt'
            self.run(['/usr/bin/time','-f','%M','-o',output,*args], label=f'{variant}-peak-memory')
            measurement = {'peak_rss_bytes':int(output.read_text().strip())*1024, 'method':'GNU time maximum RSS'}
        elif sys.platform == 'darwin':
            r = self.run(['/usr/bin/time','-l',*args], label=f'{variant}-peak-memory')
            match = re.search(r'(\d+)\s+maximum resident set size', r.stderr.decode())
            assert match, r.stderr
            measurement = {'peak_rss_bytes':int(match[1]), 'method':'BSD time maximum RSS'}
        else:
            import ctypes
            from ctypes import wintypes
            class Counters(ctypes.Structure):
                _fields_ = [('cb',wintypes.DWORD),('PageFaultCount',wintypes.DWORD)]+[(name,ctypes.c_size_t) for name in ['PeakWorkingSetSize','WorkingSetSize','QuotaPeakPagedPoolUsage','QuotaPagedPoolUsage','QuotaPeakNonPagedPoolUsage','QuotaNonPagedPoolUsage','PagefileUsage','PeakPagefileUsage']]
            get_memory = ctypes.windll.psapi.GetProcessMemoryInfo
            get_memory.argtypes = [wintypes.HANDLE,ctypes.POINTER(Counters),wintypes.DWORD]
            get_memory.restype = wintypes.BOOL
            peak, samples = 0, 0
            child = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=self.base_env)
            while child.poll() is None:
                counters = Counters(); counters.cb = ctypes.sizeof(counters)
                if get_memory(wintypes.HANDLE(child._handle),ctypes.byref(counters),counters.cb):
                    peak = max(peak,counters.PeakWorkingSetSize);samples += 1
                time.sleep(.002)
            stdout, stderr = child.communicate(timeout=30)
            assert child.returncode == 0, stderr
            assert peak > 0 and samples > 0
            measurement = {'peak_working_set_bytes':peak,'samples':samples,'method':'Windows GetProcessMemoryInfo peak sampled until exit'}
        self.result['variants'][variant]['memory'] = measurement
        self.save()

    def local_http(self, binaries):
        cases = []
        for variant, binary in binaries.items():
            for case in ['fresh', 'cached', 'duplicate']:
                class Handler(http.server.BaseHTTPRequestHandler):
                    calls = 0
                    def log_message(self, *args): pass
                    def do_HEAD(self):
                        type(self).calls += 1
                        self.send_response(200)
                        self.send_header('Age', '1' if case == 'cached' else '0')
                        if case == 'duplicate': self.send_header('Date', self.date_time_string())
                        self.send_header('Cache-Control', 'no-store')
                        self.end_headers()
                server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
                thread = threading.Thread(target=server.serve_forever, daemon=True)
                thread.start()
                try:
                    observed = self.run([binary, 'time-observe', f'127.0.0.1:{server.server_port}', '1'], label=f'{variant}-http-{case}')
                    stdout = observed.stdout.decode()
                    assert Handler.calls > 0
                    if case == 'fresh': assert '샘플 수집 실패' not in stdout
                    if case == 'cached': assert 'Age 헤더가 0보다 커' in stdout
                    if case == 'duplicate': assert 'Date 헤더가 여러 개' in stdout
                    cases.append({'variant':variant,'case':case,'requests':Handler.calls,'stdout':stdout})
                finally:
                    server.shutdown(); server.server_close(); thread.join(timeout=5)
        self.result['native_http'] = cases
        self.save()

    def srg(self):
        binaries, probes, floats = {}, {}, {}
        for variant, ref in [('baseline',BASE[self.repo]),('candidate','HEAD')]:
            checkout = self.snapshot(variant, ref)
            self.result['variants'][variant] = {}
            self.checks(variant, checkout)
            binary, package, warnings = self.release(variant, checkout)
            self.result['variants'][variant]['release'] = package
            self.result['variants'][variant]['cli'] = self.cli(binary)
            binaries[variant] = binary
            probe_checkout = self.root/(variant+'-probe')
            shutil.copytree(checkout,probe_checkout,ignore=shutil.ignore_patterns('target','artifacts','.optimization-validation'))
            self.make_probe(probe_checkout)
            probe, _ = self.build_probe(variant,probe_checkout)
            probes[variant] = probe
            allocation_checkout = self.root/(variant+'-allocation')
            shutil.copytree(checkout,allocation_checkout,ignore=shutil.ignore_patterns('target','artifacts','.optimization-validation'))
            self.make_probe(allocation_checkout,True)
            allocation_binary,_ = self.build_probe(variant+'-allocation',allocation_checkout)
            allocation = self.run([allocation_binary],label=f'{variant}-allocation').stdout.decode().split()
            assert len(allocation) == 3,allocation
            self.result['variants'][variant]['allocations'] = {'count':int(allocation[1]),'allocated_bytes':int(allocation[2]),'workload':'491520 reused-buffer record formats'}
            dumped = self.run([probe,'dump'],label=f'{variant}-format-dump').stdout
            self.result['variants'][variant]['format_dump'] = {'bytes':len(dumped),'sha256':hashlib.sha256(dumped).hexdigest()}
            source = (self.tools/'float_probe.rs').read_text().replace('../srg/src/random_number.rs', (checkout/'src/random_number.rs').as_posix())
            path = self.root/f'{variant}-float.rs';path.write_text(source)
            exe = self.root/(variant+'-float'+('.exe' if os.name=='nt' else ''))
            self.run(['rustc',f'+{self.selector}','--edition=2024','-O','--crate-name',variant+'_float',path,'-o',exe],label=f'{variant}-float-build')
            observed = self.run([exe],label=f'{variant}-float-boundary').stdout.decode()
            assert 'out_of_range='+('2' if variant=='baseline' else '0') in observed, observed
            self.result['variants'][variant]['float_boundary'] = observed
            floats[variant] = exe
        assert self.result['variants']['baseline']['cli'] == self.result['variants']['candidate']['cli']
        assert self.result['variants']['baseline']['format_dump'] == self.result['variants']['candidate']['format_dump']
        self.local_http(binaries)
        self.paired('candidate',probes['baseline'],probes['candidate'],'bench')
        self.paired('float-boundary-fix',floats['baseline'],floats['candidate'],'bench')
        self.result['arm_float_probe_scope'] = 'diagnostic source probe only; production random-float remains x86_64 only'
        self.save()

    def fcupdater(self):
        stored, fresh, workbooks = {}, {}, {}
        configured = self.target in ['x86_64-unknown-linux-gnu','x86_64-pc-windows-msvc','x86_64-apple-darwin','aarch64-apple-darwin']
        for variant, ref in [('baseline',BASE[self.repo]),('candidate','HEAD')]:
            checkout = self.snapshot(variant,ref)
            self.result['variants'][variant] = {}
            workbook = checkout/'fuel_cost_chungcheong.xlsx'
            workbooks[variant] = workbook
            self.checks(variant,checkout)
            binary, package, diagnostics = self.release(variant+'-stored',checkout,True)
            self.result['variants'][variant]['stored_profile_release'] = package
            self.result['variants'][variant]['stored_profile_diagnostics'] = diagnostics
            self.result['variants'][variant]['cli'] = self.cli(binary)
            probe_checkout = self.root/(variant+'-plain-probe')
            shutil.copytree(checkout,probe_checkout,ignore=shutil.ignore_patterns('target','artifacts','.optimization-validation'))
            self.make_probe(probe_checkout)
            stored[variant],_ = self.build_probe(variant+'-plain',probe_checkout)
            self.peak_memory(variant,stored[variant],workbook)
            alloc_checkout = self.root/(variant+'-allocation')
            shutil.copytree(checkout,alloc_checkout,ignore=shutil.ignore_patterns('target','artifacts','.optimization-validation'))
            self.make_probe(alloc_checkout,True)
            alloc_binary,_ = self.build_probe(variant+'-allocation',alloc_checkout)
            allocation = self.run([alloc_binary,'bench',workbook],label=f'{variant}-allocation').stdout.decode().split()
            assert len(allocation) == 4, allocation
            self.result['variants'][variant]['allocations'] = {'count':int(allocation[2]),'allocated_bytes':int(allocation[3]),'workload':'eight master-sheet updates excluding parse'}
            fresh[variant],profile = self.train_fc(variant,checkout)
            if configured:
                binary,package,diagnostics = self.release(variant+'-refreshed',checkout,True)
                assert 'function control flow change detected' not in diagnostics
                assert 'hash mismatch' not in diagnostics
                missing = [line for line in diagnostics.splitlines() if 'no profile data available' in line]
                assert all('up to 0 count discarded' in line for line in missing), missing
                self.result['variants'][variant]['refreshed_profile_release'] = package
                self.result['variants'][variant]['refreshed_profile_diagnostics'] = diagnostics
            self.result['variants'][variant]['profile_used_by_shipping_configuration'] = configured
        assert self.result['variants']['baseline']['cli'] == self.result['variants']['candidate']['cli']
        self.fc_contracts(stored['baseline'],stored['candidate'],workbooks['baseline'])
        self.paired('source-only-unprofiled',stored['baseline'],stored['candidate'],'roundtrip',workbooks['baseline'])
        self.paired('source-only-fresh-profile-both',fresh['baseline'],fresh['candidate'],'roundtrip',workbooks['baseline'])
        self.result['profiles_are_diagnostic_until_measured'] = True
        self.save()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo',choices=list(BASE),required=True)
    parser.add_argument('--target',required=True)
    parser.add_argument('--name',required=True)
    args = parser.parse_args()
    validator = NativeValidation(args.repo,args.target,args.name)
    try:
        compiler = validator.run(['rustc',f'+{validator.selector}','-vV'],label='compiler').stdout.decode()
        validator.result['compiler'] = compiler
        assert 'release: '+VERSION in compiler and 'host: '+args.target in compiler,compiler
        getattr(validator,args.repo)()
        validator.result['status'] = 'passed' if all(t['protected_runtime_pass'] for t in validator.result['timings']) else 'runtime-bound-unresolved'
        validator.save()
        if validator.result['status'] != 'passed':raise RuntimeError('5% protected runtime bound unresolved; see paired measurements')
    except BaseException as error:
        validator.result['error'] = str(error)
        validator.result.setdefault('status','failed')
        validator.save()
        raise

if __name__ == '__main__':
    main()
