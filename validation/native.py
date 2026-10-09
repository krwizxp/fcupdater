from pathlib import Path
import subprocess,os,json,shutil,gzip,sys,tempfile,random,statistics,hashlib,threading,time,email.utils
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
root=Path.cwd();v=root/'validation';app=os.environ['VALIDATION_APP'];target=os.environ['VALIDATION_TARGET'];base_sha=os.environ['VALIDATION_BASE'];win=sys.platform=='win32';suffix='.exe' if win else '';rng=random.Random(20261010);report={'app':app,'target':target,'run':os.environ.get('GITHUB_RUN_ID'),'checks':{}}
def command(args,cwd=root,env=None,check=True):
 p=subprocess.run(args,cwd=cwd,env=env,capture_output=True,text=True,timeout=300)
 if p.stdout:print(p.stdout,flush=True)
 if p.stderr:print(p.stderr,flush=True)
 if check and p.returncode:raise RuntimeError((args,p.returncode))
 return p
def cargo(args,cwd=root,env=None):return command(['cargo','+1.99.0',*args],cwd,env)
def binary(folder,td='target/release'):return (folder/td/(app+suffix)).resolve()
baseline=root.parent/(app+'-release-baseline');command(['git','worktree','add',str(baseline),base_sha])
for label,folder in [('baseline',baseline),('candidate',root)]:
 cargo(['fmt','--all','--','--check'],folder)
 cargo(['clippy','--release','--frozen','--all-targets','--target',target,'--','-D','warnings'],folder)
 cargo(['build','--release','--frozen'],folder)
 env=dict(os.environ,CARGO_TARGET_DIR=str(folder/'target'))
 cargo(['run','--frozen','--example','package_artifact','--',app+'-'+target],folder,env)
report['binary']=[binary(f).stat().st_size for f in [baseline,root]]
report['package']=[next((f/'artifacts').glob(app+'-'+target+'.*')).stat().st_size for f in [baseline,root]]
smokes=[['--version'],['--help'],['--bad-option']]
smokes+=([['generate','0'],['random-integer','2','1'],['random-float','NaN','1'],['time-observe','host:+80','1']] if app=='srg' else [['--verify','extra']])
for args in smokes:
 ps=[command([str(binary(f)),*args],check=False) for f in [baseline,root]]
 assert (ps[0].returncode,ps[0].stdout,ps[0].stderr)==(ps[1].returncode,ps[1].stdout,ps[1].stderr)
report['checks']['cli_smokes']=len(smokes)
command([sys.executable,str(v/'corpora.py')],v)
corpus=v/'corpus'
probes={}
for label,folder in [('baseline',baseline),('candidate',root)]:
 probe=v/('probe-'+label);shutil.copytree(folder,probe,ignore=shutil.ignore_patterns('.git','target','artifacts','validation'),dirs_exist_ok=True)
 p=probe/'src/main.rs';s=p.read_text();hook='''
 if let Ok(mode)=env::var("API_AUDIT_MODE") {let path=env::var("API_AUDIT_PATH").unwrap();let loops=env::var("API_AUDIT_LOOPS").unwrap_or_else(|_|"0".into()).parse().unwrap();HOOK}
 '''
 if app=='srg':
  hook=hook.replace('HOOK','if mode=="output"{output::api_audit(&path,loops);}else{time::api_audit(&mode,&path,loops);}return Ok(());')
  for rel,name in [('src/time.rs','time-probe.rs'),('src/output.rs','output-probe.rs')]:
   q=probe/rel;q.write_text(q.read_text()+(v/name).read_text())
 else:
  hook=hook.replace('HOOK','return api_audit(&mode,&path,loops);');s+='\n'+(v/'fc-probe.rs').read_text()
 marker='fn main() -> Result<()> {';assert s.count(marker)==1;s=s.replace(marker,marker+hook);s+='\n'+(v/'allocator.rs').read_text();p.write_text(s)
 cargo(['build','--release','--frozen'],probe);probes[label]=binary(probe)
source=v/'source.xls';source.write_bytes(gzip.decompress((v/'source.xls.gz').read_bytes()));replay=v/('winhttp.dll' if win else 'replay.dylib' if sys.platform=='darwin' else 'replay.so')
command(['clang','-shared','-O2',str(v/'replay.c'),'-o',str(replay)] if win else ['clang','-dynamiclib','-O2',str(v/'replay.c'),'-o',str(replay)] if sys.platform=='darwin' else ['gcc','-shared','-fPIC','-O2',str(v/'replay.c'),'-o',str(replay)])
replayenv=dict(os.environ,PGO_XLS=str(source.resolve()))
if win:
 for exe in [*probes.values(),binary(baseline),binary(root)]:shutil.copy2(replay,exe.parent/'winhttp.dll')
elif sys.platform=='darwin':replayenv.update(DYLD_INSERT_LIBRARIES=str(replay.resolve()),DYLD_FORCE_FLAT_NAMESPACE='1')
else:replayenv['LD_PRELOAD']=str(replay.resolve())
def probe_run(label,mode,path,loops=0):
 if mode=='update':
  dest=v/('input-'+label+'.xlsx');shutil.copy2(path,dest);path=dest
 e=dict(replayenv,API_AUDIT_MODE=mode,API_AUDIT_PATH=str(path),API_AUDIT_LOOPS=str(loops));return subprocess.run([str(probes[label])],env=e,capture_output=True,timeout=120)
import zipfile,xml.etree.ElementTree as ET
def canonical(path):
 with zipfile.ZipFile(path) as z:
  assert z.testzip() is None
  result={}
  for name in sorted(z.namelist()):
   b=z.read(name)
   if name=='docProps/core.xml':
    tree=ET.fromstring(b)
    for e in tree.iter():
     if e.tag.endswith('}modified'):e.text='NORMALIZED'
    b=ET.tostring(tree)
   result[name]=b
  return result
for fixture in [root/'fuel_cost_chungcheong.xlsx',v/'before.xlsx',v/'valid.xlsx']:
 results=[]
 for folder in [baseline,root]:
  with tempfile.TemporaryDirectory(prefix='real-release-equivalence-') as scratch:
   work=Path(scratch);shutil.copy2(fixture,work/'fuel_cost_chungcheong.xlsx');p=command([str(binary(folder)),'--verify'],work,replayenv);results.append((p.stdout,p.stderr,canonical(work/'fuel_cost_chungcheong.xlsx')))
 assert results[0]==results[1],fixture
report['checks']['actual_product_full_workbook_equivalence']=3
if app=='srg':
 import re
 for mode in ['time','address','date']:
  ps=[probe_run(label,mode,corpus/(mode+'.txt')) for label in probes];assert all(p.returncode==0 for p in ps);assert ps[0].stdout==ps[1].stdout
  values=ps[0].stdout.decode().splitlines();inputs=(corpus/(mode+'.txt')).read_text().splitlines()
  if mode=='time':
   for s,value in zip(inputs,values,strict=True):
    m=re.fullmatch(r'([0-9]{2}):([0-9]{2}):([0-9]{2})',s);valid=bool(m) and int(m[1])<24 and int(m[2])<60 and int(m[3])<60
    assert value.startswith('ERR:')!=valid
    if valid:assert int(value)==int(m[1])*3600+int(m[2])*60+int(m[3])
  else:
   expected=json.loads((corpus/(mode+'-expected.json')).read_text())
   for (s,expected),value in zip(expected.items(),values,strict=True):
    if mode=='address':assert (value=='OK')==expected,(s,value)
    elif expected is None:assert value.startswith('ERR:'),(s,value)
    else:assert int(value)==expected,(s,value,expected)
  report['checks'][mode]=len(values)
 ps=[probe_run(label,'output',corpus/'octal.txt') for label in probes];assert ps[0].returncode==ps[1].returncode==0;assert ps[0].stdout==ps[1].stdout
 values=re.findall('8진수: ([0-7]+)'.encode(),ps[0].stdout)
 for n,value in zip((corpus/'octal.txt').read_text().splitlines(),values,strict=True):assert value.decode()==format(int(n),'o')
 report['checks']['octal_full_records']=len(values)
else:
 for path in corpus.glob('*.xlsx'):
  ps=[probe_run(label,'xlsx',path) for label in probes];assert (ps[0].returncode,ps[0].stderr)==(ps[1].returncode,ps[1].stderr),(path,[p.stderr for p in ps]);assert (ps[0].returncode==0)==(path.stem=='valid'),(path,ps[0].stderr)
 report['checks']['xlsx']=len(list(corpus.glob('*.xlsx')))
# Native runtime samples for platform code generation; same predeclared 3% upper bound.
cases=([('time','time-valid.txt',200),('output','output-short.txt',500),('output','output-wide.txt',500)] if app=='srg' else [('xlsx','valid.xlsx',20),('update','valid.xlsx',1)])
report['runtime']={}
for mode,path,loops in cases:
 pairs=[]
 for _ in range(96):
  labels=list(probes);rng.shuffle(labels);ps={label:json.loads(probe_run(label,mode,corpus/path,loops).stdout) for label in labels};a,b=ps['baseline'],ps['candidate'];assert a['checksum']==b['checksum'];assert b['allocations']<=a['allocations'] and b['allocation_bytes']<=a['allocation_bytes'];pairs.append([a,b])
 ratios=[b['nanos']/a['nanos'] for a,b in pairs];boot=sorted(statistics.median(rng.choices(ratios,k=len(ratios))) for _ in range(5000));summary={'median_change':statistics.median(ratios)-1,'ci95':[boot[125]-1,boot[4874]-1],'allocation_count':[pairs[0][0]['allocations'],pairs[0][1]['allocations']],'allocation_bytes':[pairs[0][0]['allocation_bytes'],pairs[0][1]['allocation_bytes']]};report['runtime'][mode+'-'+path]={'summary':summary,'raw':pairs};print(json.dumps(summary),flush=True)
report['size_gate']={'binary':report['binary'][1]<=report['binary'][0],'package':report['package'][1]<=report['package'][0]}
(v/'native-release-results.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report['size_gate']),flush=True)
assert all(report['size_gate'].values()),report['size_gate']
assert all(x['summary']['ci95'][1]<=.03 for x in report['runtime'].values()),'native runtime gate'
assert report['runtime']['update-valid.xlsx']['summary']['allocation_count'][1]<report['runtime']['update-valid.xlsx']['summary']['allocation_count'][0],'expected update allocation reduction'
# Build profiles from the unchanged actual product binary, never from probes.
if sys.platform in ['linux','win32']:
 profiles=v/'profiles';profiles.mkdir(exist_ok=True);raw=profiles/'raw';raw.mkdir(exist_ok=True)
 env=dict(os.environ,RUSTFLAGS='-Cprofile-generate='+str(raw.resolve()),CARGO_TARGET_DIR=str(root/'target-train'))
 cargo(['build','--release','--frozen','--target',target],root,env)
 exe=binary(root,'target-train/'+target+'/release');trainenv=dict(os.environ,LLVM_PROFILE_FILE=str(raw.resolve()/'%p-%m.profraw'))
 if app=='fcupdater':
  source=profiles/'source.xls';source.write_bytes(gzip.decompress((v/'source.xls.gz').read_bytes()));trainenv['PGO_XLS']=str(source.resolve())
  replay=profiles/('winhttp.dll' if win else 'replay.so')
  if win:
   command(['clang','-shared','-O2',str(v/'replay.c'),'-o',str(replay)]);shutil.copy2(replay,exe.parent/'winhttp.dll')
  else:
   command(['gcc','-shared','-fPIC','-O2',str(v/'replay.c'),'-o',str(replay)]);trainenv['LD_PRELOAD']=str(replay.resolve())
 with tempfile.TemporaryDirectory(prefix='api-product-training-') as scratch:
  work=Path(scratch)
  for args in smokes:command([str(exe),*args],work,trainenv,False)
  if app=='srg':
   for _ in range(6):
    for args in [['generate','1'],['generate','8'],['generate','256'],['generate','2048'],['random-integer','42','42'],['random-integer','-100','100'],['random-float','1.25','1.25'],['random-float','-100','100'],['ladder','a,b,c,d','1,2,3,4']]:command([str(exe),*args],work,trainenv,False)
   class Handler(BaseHTTPRequestHandler):
    def do_HEAD(self):
     self.send_response_only(200);self.send_header('Date',email.utils.formatdate(usegmt=True));self.send_header('Content-Length','0');self.send_header('Connection','close');self.end_headers()
    def log_message(self,*args):pass
   server=ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start()
   for _ in range(4):command([str(exe),'time-observe','http://127.0.0.1:'+str(server.server_port),'2'],work,trainenv)
   server.shutdown();server.server_close()
  else:
   for i in range(16):
    source=v/('before.xlsx' if i%2 else 'valid.xlsx');shutil.copy2(source,work/'fuel_cost_chungcheong.xlsx');command([str(exe),*(['--verify'] if i%3 else [])],work,trainenv)
   for name in ['split-zip.xlsx','row-1-0.xlsx','truncated.xlsx']:
    shutil.copy2(corpus/name,work/'fuel_cost_chungcheong.xlsx');command([str(exe),'--verify'],work,trainenv,False)
 sysroot=Path(command(['rustc','+1.99.0','--print','sysroot']).stdout.strip());host=next(line.split(': ',1)[1] for line in command(['rustc','+1.99.0','-Vv']).stdout.splitlines() if line.startswith('host:'))
 llvm=sysroot/'lib/rustlib'/host/'bin'/('llvm-profdata'+suffix);profile=profiles/(target+'.profdata');command([str(llvm),'merge','-o',str(profile),*map(str,raw.glob('*.profraw'))])
 shutil.copy2(profile,root/'pgo'/profile.name)
 result=cargo(['build-pgo']);assert 'hash mismatch' not in result.stderr and 'no profile data available' not in result.stderr,result.stderr
 pgo=binary(root,'target/'+target+'/release');report['pgo']={'bytes':pgo.stat().st_size,'profile_bytes':profile.stat().st_size,'profile_sha256':hashlib.sha256(profile.read_bytes()).hexdigest()}
 if win and app=='fcupdater':shutil.copy2(replay,pgo.parent/'winhttp.dll')
 env=dict(trainenv);env.pop('LLVM_PROFILE_FILE',None)
 with tempfile.TemporaryDirectory(prefix='api-product-heldout-') as scratch:
  work=Path(scratch)
  for args in smokes:command([str(pgo),*args],work,env,False)
  if app=='srg':command([str(pgo),'generate','17'],work,env)
  else:shutil.copy2(v/'valid.xlsx',work/'fuel_cost_chungcheong.xlsx');command([str(pgo),'--verify'],work,env)
 (profiles/'manifest.json').write_text(json.dumps({'source_commit':os.environ.get('GITHUB_SHA'),'rustc':'1.99.0','LLVM':'23.1.1','target':target,'training':'actual product CLI; probes excluded','run':os.environ.get('GITHUB_RUN_ID'),**report['pgo']},indent=2)+'\n')
(v/'native-release-results.json').write_text(json.dumps(report,indent=2)+'\n')
assert all(report['size_gate'].values()),report['size_gate']
assert all(x['summary']['ci95'][1]<=.03 for x in report['runtime'].values()),'native performance non-regression not established'
print('Required native checks passed',flush=True)
