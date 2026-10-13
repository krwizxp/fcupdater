"""Rebuild the committed distribution profiles without regenerating them."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
from native_validation import BASE, VERSION, NativeValidation


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--target',required=True)
    args=parser.parse_args()
    v=NativeValidation('fcupdater',args.target,'fcupdater-final-'+args.target)
    configured=args.target in ['x86_64-unknown-linux-gnu','x86_64-pc-windows-msvc','x86_64-apple-darwin','aarch64-apple-darwin']
    binaries,probes,workbooks={}, {}, {}
    blocked=[]
    try:
        compiler=v.run(['rustc','+'+v.selector,'-vV'],label='compiler').stdout.decode()
        assert 'release: '+VERSION in compiler and 'host: '+v.target in compiler
        v.result['compiler']=compiler
        for variant,ref in [('baseline',BASE['fcupdater']),('candidate','HEAD')]:
            checkout=v.snapshot(variant,ref)
            v.result['variants'][variant]={}
            v.checks(variant,checkout)
            binary,package,diagnostics=v.release(variant,checkout,True)
            binaries[variant]=binary
            v.result['variants'][variant]['release']=package
            v.result['variants'][variant]['distribution_diagnostics']=diagnostics
            v.result['variants'][variant]['cli']=v.cli(binary)
            if variant=='candidate' and configured:
                assert 'function control flow change detected' not in diagnostics
                assert 'hash mismatch' not in diagnostics
                missing=[line for line in diagnostics.splitlines() if 'no profile data available' in line]
                assert all('up to 0 count discarded' in line for line in missing),missing
                profile=checkout/'pgo'/(args.target+'.profdata')
                v.result['committed_profile_sha256']=hashlib.sha256(profile.read_bytes()).hexdigest()
            workbook=checkout/'fuel_cost_chungcheong.xlsx'
            workbooks[variant]=workbook
            probe_checkout=v.root/(variant+'-probe')
            shutil.copytree(checkout,probe_checkout,ignore=shutil.ignore_patterns('target','artifacts','.optimization-validation'))
            v.make_probe(probe_checkout)
            flags='-Cprofile-use='+str(checkout/'pgo'/(args.target+'.profdata'))+' -Cllvm-args=-pgo-warn-missing-function' if configured else None
            probes[variant],diagnostic=v.build_probe(variant,probe_checkout,flags)
            v.result['variants'][variant]['probe_diagnostics']=diagnostic.stderr.decode(errors='replace')
            if variant=='candidate' and configured:
                assert 'function control flow change detected' not in v.result['variants'][variant]['probe_diagnostics']
                assert 'hash mismatch' not in v.result['variants'][variant]['probe_diagnostics']
            live=v.root/'live'/variant;live.mkdir(parents=True)
            live_book=live/workbook.name;shutil.copyfile(workbook,live_book)
            original=hashlib.sha256(live_book.read_bytes()).hexdigest()
            completed=v.run([binary,'--verify'],cwd=live,expected=(0,1),label=variant+'-live-final',timeout=180)
            if completed.returncode:
                # A separate Windows 11 x64 environment must establish the successful live path.
                # Keep the default Server transport failure visible; never bypass TLS validation.
                assert os.name=='nt' and v.target=='x86_64-pc-windows-msvc' and b'12175' in completed.stderr
                assert hashlib.sha256(live_book.read_bytes()).hexdigest()==original
                blocked.append({'variant':variant,'error':completed.stderr.decode(),'workbook_unchanged':True})
            else:
                v.result['variants'][variant]['live_verify']={'status':0,'workbook':v.semantic_xlsx(live_book)}
        assert v.result['variants']['baseline']['cli']==v.result['variants']['candidate']['cli']
        v.fc_contracts(probes['baseline'],probes['candidate'],workbooks['baseline'])
        v.paired('final-source-and-profile-vs-original-cycle',probes['baseline'],probes['candidate'],'roundtrip',workbooks['baseline'])
        v.result['default_host_live_blockers']=blocked
        v.result['status']='passed' if not blocked and all(t['protected_runtime_pass'] for t in v.result['timings']) else 'passed-offline-live-path-blocked' if blocked and all(t['protected_runtime_pass'] for t in v.result['timings']) else 'runtime-bound-unresolved'
        v.save()
        if v.result['status']=='runtime-bound-unresolved':raise RuntimeError('protected runtime bound unresolved')
        print(json.dumps({'status':v.result['status'],'target':v.target,'live_blockers':blocked}),flush=True)
    except BaseException as error:
        v.result['error']=str(error);v.result.setdefault('status','failed');v.save();raise

if __name__=='__main__':main()
