"""Rebuild and exercise the exact final product source/profiles on each native runner."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil

from native_validation import NativeValidation, VERSION


class FinalValidation(NativeValidation):
    def main(self):
        self.result['compiler'] = self.run(['rustc',f'+{VERSION}','-Vv'],label='compiler').stdout.decode()
        assert f'host: {self.target}' in self.result['compiler']
        assert 'LLVM version: 23.1.1' in self.result['compiler']
        expected = json.loads((self.tools/'final-manifest.json').read_text())[self.target]
        candidate = self.snapshot('candidate','HEAD')
        source = candidate/'src/master_sheet.rs'
        assert hashlib.sha256(source.read_bytes()).hexdigest() == '337fd663631e593559cf969ea5cfd30bc27619e3ed169ff0435a7bd68d600580'
        profile = candidate/'pgo'/f'{self.target}.profdata'
        profile_hash = hashlib.sha256(profile.read_bytes()).hexdigest()
        assert profile_hash == expected['profile']['sha256']
        self.result['profile_provenance'] = expected['profile']
        self.result['measured_validation_sha'] = expected['validation_sha']
        self.checks('candidate',candidate)
        directory = self.root/'release-build'/'candidate'
        release = self.run(['cargo',f'+{VERSION}','build-pgo'],cwd=candidate,
                           env=self.base_env|{'CARGO_TARGET_DIR':str(directory)},label='candidate-release')
        diagnostic = release.stderr.decode(errors='replace')
        warnings = [line for line in diagnostic.splitlines()
                    if line.startswith('warning:') and not re.search(r'generated \d+ warnings?$',line)]
        assert all('no profile data available' in line and 'up to 0 count discarded' in line for line in warnings)
        assert all('RankSortRefresher' not in line and 'SharedStringTable' not in line for line in warnings)
        normalized = sorted(re.search(r'function (.+)',line).group(1) for line in warnings)
        assert normalized == expected['release_pgo_zero_count_helpers']
        assert 'function control flow change detected' not in diagnostic and 'hash mismatch' not in diagnostic
        package = self.package('candidate',candidate,directory)
        assert package['binary_bytes'] == expected['expected_binary_bytes']
        assert package['package_bytes'] == expected['expected_package_bytes']
        self.result['variants']['candidate'] = package|{'release_pgo_zero_count_helpers':normalized,
                                                        'profile_sha256':profile_hash}
        binary = self.binary(directory)
        assert self.cli(binary) == expected['baseline_cli']
        self.result['equivalence'] = {'cli_cases':6,'baseline_cli_bytes_equal':True}
        # Both public modes exercise the actual PGO release binary with live original source.
        self.result['live'] = []
        for mode,args in [('verify',['--verify']),('default',[])]:
            live = self.root/('live-'+mode)
            live.mkdir()
            master = candidate/'fuel_cost_chungcheong.xlsx'
            shutil.copyfile(master,live/master.name)
            self.run([binary,*args],cwd=live,label='release-live-'+mode,timeout=180)
            self.result['live'].append({'mode':mode,'status':0,'workbook':self.semantic_xlsx(live/master.name)})
            shutil.copyfile(live/master.name,self.out/(mode+'-live-output.xlsx'))
        # The fixed workbook path checks full output equality and a second update/save.
        probe_checkout = self.root/'candidate-probe'
        shutil.copytree(candidate,probe_checkout,ignore=shutil.ignore_patterns('target','artifacts','.optimization-validation'))
        self.make_probe(probe_checkout)
        flags = f'-Cprofile-use={profile} -Cllvm-args=-pgo-warn-missing-function'
        probe,compiled = self.build_probe('final-pgo',probe_checkout,flags)
        text = compiled.stderr.decode(errors='replace')
        assert 'function control flow change detected' not in text and 'hash mismatch' not in text
        missing = [line for line in text.splitlines() if 'no profile data available' in line]
        assert all('up to 0 count discarded' in line for line in missing)
        assert all('RankSortRefresher' not in line and 'SharedStringTable' not in line for line in missing)
        fixed = self.out/'candidate-fixed-output.xlsx'
        shutil.copyfile(candidate/'fuel_cost_chungcheong.xlsx',fixed)
        self.run([probe,'save',fixed,fixed],label='candidate-fixed-save')
        first = self.semantic_xlsx(fixed)
        assert first['parts'] == expected['fixed_parts']
        assert first['rank_cache_count']==864 and first['independent_competition_rank_checks']==166
        assert first['independent_blank_rank_checks']==698
        self.run([probe,'save',fixed,fixed],label='candidate-idempotent-save')
        second = self.semantic_xlsx(fixed)
        assert first==second
        self.result['equivalence'].update({'fixed_workbook':first,'fixed_all_parts_equal':True,'second_update_save_identical':True})
        self.result['status']='passed'
        self.save()
        print(json.dumps(self.result,indent=2),flush=True)


if __name__ == '__main__':
    import argparse
    parser=argparse.ArgumentParser()
    parser.add_argument('--target',required=True)
    parser.add_argument('--binary-name',required=True)
    args=parser.parse_args()
    validation=FinalValidation('fcupdater',args.target,args.binary_name)
    try:
        validation.main()
    except BaseException as error:
        validation.result['status']='failed'
        validation.result['error']=str(error)
        validation.save()
        raise
