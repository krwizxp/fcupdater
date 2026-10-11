"""Task-owned native validation; never installed in the product path."""
import argparse
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import random
import shutil
import statistics
import subprocess
import sys
import tarfile
import time
import xml.etree.ElementTree as ET
import zipfile

VERSION = '1.99.0'
BASE = {'srg': '5b2feee723ddb5d143dd539b77359cc5b815964d',
        'fcupdater': '6efe2f1f280dc007f3fa7c1efcfb013d0c90a824'}
NS = {'s': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}


class NativeValidation:
    def __init__(self, repo, target, name):
        self.repo, self.target, self.name = repo, target, name
        self.checkout = Path.cwd()
        self.tools = self.checkout / '.optimization-validation'
        self.root = self.checkout / 'optimization-native-work'
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
            self.run(['cargo', f'+{VERSION}', *args], cwd=checkout, env=env,
                     label=f'{variant}-{label}')

    def binary(self, directory):
        return directory / self.target / 'release' / (self.repo + ('.exe' if os.name == 'nt' else ''))

    def package(self, variant, checkout, directory):
        binary = self.binary(directory)
        entry_name = self.name + '-' + variant
        env = self.base_env | {'CARGO_TARGET_DIR': str(directory / self.target), 'RUSTFLAGS': ''}
        self.run(['cargo', f'+{VERSION}', 'run', '--frozen', '--example', 'package_artifact',
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
            harness = harness.replace('let start = std::time::Instant::now();',
                                      'COUNT_ALLOC.store(true, Ordering::Relaxed);\n'
                                      '        let start = std::time::Instant::now();', 1)
            harness = harness.replace('println!("{} {}", start.elapsed().as_nanos(), bytes);',
                                      'COUNT_ALLOC.store(false, Ordering::Relaxed);\n'
                                      '        println!("{} {} {} {}", start.elapsed().as_nanos(), bytes, '
                                      'ALLOC_CALLS.load(Ordering::Relaxed), ALLOC_BYTES.load(Ordering::Relaxed));')
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
        r = self.run(['cargo', f'+{VERSION}', 'build', '--release', '--frozen', '--bin', self.repo,
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
                    'ratio_90pct_bootstrap': [boot[500], boot[9499]]}
        measured = {'candidate': candidate, 'mode': mode, 'first': series(21)}
        if measured['first']['ratio_90pct_bootstrap'][1] > 1.05:
            measured['bounded_repeat'] = series(41)
        final = measured.get('bounded_repeat', measured['first'])
        measured['protected_runtime_pass'] = final['ratio_90pct_bootstrap'][1] <= 1.05
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
            ranks = [c.findtext('s:v', namespaces=NS) for c in sheet.findall('.//s:c', NS)
                     if c.get('r', '').startswith('I') and c.find('s:f', NS) is not None]
            cells = {c.get('r'): c for c in sheet.findall('.//s:c', NS)}
            values = []
            for row in range(15, 15 + len(ranks)):
                total = cells.get('T' + str(row))
                value = total.findtext('s:v', namespaces=NS) if total is not None else None
                if value is not None and value != '':
                    key = cells['W' + str(row)].findtext('s:v', namespaces=NS)
                    values.append((row, Decimal(key)))
            for row, key in values:
                actual = cells['I' + str(row)].findtext('s:v', namespaces=NS)
                assert int(actual) == 1 + sum(other < key for _, other in values)
            return {'parts': parts, 'rank_cache_count': len(ranks),
                    'independent_competition_rank_checks': len(values),
                    'rank_cache_sha256': hashlib.sha256(json.dumps(ranks).encode()).hexdigest()}

    def srg(self):
        probes = {}
        outputs = {}
        cli = {}
        for variant, ref in [('baseline', BASE[self.repo]), ('candidate', 'HEAD')]:
            checkout = self.snapshot(variant, ref)
            self.checks(variant, checkout)
            directory = self.root / 'release-build' / variant
            env = self.base_env | {'CARGO_TARGET_DIR': str(directory)}
            self.run(['cargo', f'+{VERSION}', 'build', '--release', '--frozen', '--bin', self.repo,
                      '--target', self.target], cwd=checkout, env=env, label=f'{variant}-release')
            self.result['variants'][variant] = self.package(variant, checkout, directory)
            cli[variant] = self.cli(self.binary(directory))
            self.make_probe(checkout)
            probes[variant], _ = self.build_probe(variant, checkout)
            outputs[variant] = self.run([probes[variant], 'dump'], label=f'{variant}-dump').stdout
        assert outputs['baseline'] == outputs['candidate']
        assert cli['baseline'] == cli['candidate']
        self.result['equivalence'] = {'records': 8192, 'bytes': len(outputs['candidate']),
                                      'sha256': hashlib.sha256(outputs['candidate']).hexdigest(),
                                      'cli_cases': len(cli['baseline'])}
        self.paired('candidate', probes['baseline'], probes['candidate'], 'generate')

    def train_fc(self, variant, checkout):
        raw = self.root / 'raw' / variant
        raw.mkdir(parents=True)
        directory = self.root / 'generate-build' / variant
        flags = f'-Cprofile-generate={raw}'
        env = self.base_env | {'CARGO_TARGET_DIR': str(directory), 'RUSTFLAGS': flags,
                               'LLVM_PROFILE_FILE': str(raw / 'cli-%p-%m.profraw')}
        self.run(['cargo', f'+{VERSION}', 'build', '--release', '--frozen', '--bin', self.repo,
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
        self.run([probe_binary, 'save', workbook, self.root / f'{variant}-training.xlsx'],
                 env=probe_env, label=f'{variant}-save-training')
        raw_files = sorted(raw.glob('*.profraw'))
        assert len(raw_files) >= 7
        sysroot = self.run(['rustc', f'+{VERSION}', '--print', 'sysroot'], label='sysroot').stdout.decode().strip()
        profiler = Path(sysroot) / 'lib/rustlib' / self.target / 'bin' / ('llvm-profdata.exe' if os.name == 'nt' else 'llvm-profdata')
        profile = checkout / 'pgo' / f'{self.target}.profdata'
        self.run([profiler, 'merge', '-o', profile, *raw_files], label=f'{variant}-profile-merge')
        self.run([profiler, 'show', '--all-functions', '--counts', profile], label=f'{variant}-profile-show')
        shutil.copyfile(profile, self.out / f'{variant}-{self.target}.profdata')
        self.result['variants'][variant]['profile'] = {'raw_count': len(raw_files),
            'bytes': profile.stat().st_size(), 'sha256': hashlib.sha256(profile.read_bytes()).hexdigest(),
            'training': 'original CLI live --verify + fixed 864-station workbook + 5000 plain/escaped strings; 3 repetitions'}
        shutil.copyfile(profile, probe_checkout / 'pgo' / profile.name)
        profile_flags = f'-Cprofile-use={profile} -Cllvm-args=-pgo-warn-missing-function'
        probe_binary, diagnostic = self.build_probe(variant + '-pgo', probe_checkout, profile_flags)
        pgo_diagnostics = diagnostic.stderr.decode(errors='replace')
        assert 'function control flow change detected' not in pgo_diagnostics
        assert 'no profile data available' not in pgo_diagnostics
        assert 'hash mismatch' not in pgo_diagnostics
        return probe_binary, profile

    def fcupdater(self):
        probes, cli, outputs = {}, {}, {}
        for variant in ['baseline', 'rank', 'xml-fast']:
            checkout = self.snapshot(variant, BASE[self.repo])
            if variant != 'baseline':
                patch = self.tools / f'fcupdater-{variant}-experiment.patch'
                self.run(['git', 'apply', '--check', patch], cwd=checkout, label=f'{variant}-patch-check')
                self.run(['git', 'apply', patch], cwd=checkout, label=f'{variant}-patch')
            self.checks(variant, checkout)
            self.result['variants'][variant] = {}
            probes[variant], profile = self.train_fc(variant, checkout)
            directory = self.root / 'release-build' / variant
            env = self.base_env | {'CARGO_TARGET_DIR': str(directory)}
            release = self.run(['cargo', f'+{VERSION}', 'build-pgo'], cwd=checkout, env=env,
                               label=f'{variant}-release')
            diagnostics = release.stderr.decode(errors='replace')
            assert 'warning:' not in diagnostics, diagnostics
            self.result['variants'][variant].update(self.package(variant, checkout, directory))
            self.result['variants'][variant]['release_pgo_warnings'] = 0
            cli[variant] = self.cli(self.binary(directory))
            workbook = checkout / 'fuel_cost_chungcheong.xlsx'
            saved = self.out / f'{variant}-fixed-output.xlsx'
            self.run([probes[variant], 'save', workbook, saved], label=f'{variant}-fixed-save')
            outputs[variant] = self.semantic_xlsx(saved)
            # Allocation instrumentation is kept in a separate diagnostic binary.
            alloc_checkout = self.root / (variant + '-alloc')
            shutil.copytree(checkout, alloc_checkout, ignore=shutil.ignore_patterns('target', 'artifacts', '.optimization-validation'))
            self.make_probe(alloc_checkout, allocation=True)
            allocator, _ = self.build_probe(variant + '-alloc', alloc_checkout)
            allocation = {}
            for mode in ['intern-plain', 'intern-escaped']:
                r = self.run([allocator, mode, workbook], label=f'{variant}-{mode}-alloc')
                nanos, byte_count, calls, requested = map(int, r.stdout.split())
                allocation[mode] = {'calls': calls, 'requested_bytes': requested, 'xml_bytes': byte_count}
            self.result['variants'][variant]['allocations'] = allocation
            self.save()
        for variant in ['rank', 'xml-fast']:
            assert cli['baseline'] == cli[variant]
            assert outputs['baseline'] == outputs[variant]
            for mode in ['bench', 'parse-bench', 'intern-plain', 'intern-escaped', 'roundtrip']:
                self.paired(variant, probes['baseline'], probes[variant], mode,
                            self.root / 'baseline/fuel_cost_chungcheong.xlsx')
        self.result['equivalence'] = {'fixed_workbook_xml_parts_equal': True,
                                     'independent_parser': 'Python stdlib zipfile/ElementTree',
                                     'rank_cache': outputs['baseline'], 'cli_cases': len(cli['baseline'])}

    def main(self):
        compiler = self.run(['rustc', f'+{VERSION}', '-Vv'], label='compiler').stdout.decode()
        self.result['compiler'] = compiler
        assert f'host: {self.target}' in compiler
        (self.srg if self.repo == 'srg' else self.fcupdater)()
        self.result['status'] = 'passed'
        self.save()
        print(json.dumps(self.result, indent=2), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--repo', choices=list(BASE), required=True)
    parser.add_argument('--target', required=True)
    parser.add_argument('--binary-name', required=True)
    args = parser.parse_args()
    validation = NativeValidation(args.repo, args.target, args.binary_name)
    try:
        validation.main()
    except BaseException as error:
        validation.result['status'] = 'failed'
        validation.result['error'] = str(error)
        validation.save()
        raise


if __name__ == '__main__':
    main()
