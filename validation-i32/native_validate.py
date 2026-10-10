"""Task-only proposed native validation; never installs profiles in product pgo/."""
from pathlib import Path
import atexit, ctypes, gzip, hashlib, json, os, platform, random, shutil
import statistics, subprocess, sys, tarfile, tempfile, time, zipfile
import xml.etree.ElementTree as ET
import openpyxl, psutil
ROOT=Path.cwd(); TASK=ROOT/'validation-i32'; TARGET=os.environ['OPT_TARGET']
OUT=TASK/'evidence'/TARGET; OUT.mkdir(parents=True,exist_ok=True)
VERSION='1.99.0'; BASE_SHA='4a0df6f79f5ab812d699af99d8dd5dbe4a219900'
BASE=Path(os.environ['RUNNER_TEMP'])/'fcupdater-i32-baseline'
WIN=sys.platform=='win32'; MAC=sys.platform=='darwin'; EXT='.exe' if WIN else ''
rng=random.Random(202610110145)
criteria={'pairs':192,'warmups':3,'CLI_launches':8,'bootstrap':10000,
          'each_wall_CPU_CI95_upper':.03,'executable_growth_bytes':0,
          'memory_growth':'baseline median + max(2MiB,5%)'}
report={'target':TARGET,'base':BASE_SHA,'candidate':os.environ.get('GITHUB_SHA'),
        'run':os.environ.get('GITHUB_RUN_ID'),'image':os.environ.get('ImageVersion'),
        'criteria':criteria,'cases':{},'checks':[],'logs':[],'complete':False}
def save(): (OUT/'results.json').write_text(json.dumps(report,indent=2)+'\n')
atexit.register(save)
def digest(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def cmd(args,cwd=ROOT,env=None,expected=0,timeout=1200):
 p=subprocess.run(list(map(str,args)),cwd=cwd,env=env,capture_output=True,timeout=timeout)
 if expected is not None and p.returncode!=expected:
  raise RuntimeError((args,p.returncode,p.stdout[-2000:],p.stderr[-8000:]))
 return p
clean=dict(os.environ)
for k in ['RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','LD_PRELOAD','DYLD_INSERT_LIBRARIES',
          'DYLD_FORCE_FLAT_NAMESPACE','LLVM_PROFILE_FILE','PGO_XLS','PGO_SCENARIO']:
 clean.pop(k,None)
def cargo(args,folder,label,flags=None):
 env=dict(clean,CARGO_TARGET_DIR=str(TASK/('build-'+label)))
 if flags: env['RUSTFLAGS']=flags
 p=cmd(['cargo','+'+VERSION,*args],folder,env)
 log=p.stderr.decode(errors='replace'); report['logs'].append({'label':label,'args':args,'stderr':log});save()
 return p
def exe(label): return TASK/('build-'+label)/TARGET/'release'/('fcupdater'+EXT)
def records(profile):
 s=cmd([llvm,'show','--all-functions','--counts','--text',profile]).stdout.decode()
 s=s.replace('\r\n','\n').removeprefix(':ir\n').strip()
 return {r.splitlines()[0]:r for r in s.split('\n\n')}
def func_hash(record):
 s=record.splitlines();return s[s.index('# Func Hash:')+1]
def canonical(path):
 with zipfile.ZipFile(path) as z:
  assert z.testzip() is None
  parts={}
  for n in sorted(z.namelist()):
   b=z.read(n)
   if n=='docProps/core.xml':
    t=ET.fromstring(b)
    for e in t.iter():
     if e.tag.endswith('}modified'):e.text='NORMALIZED'
    b=ET.tostring(t)
   parts[n]=hashlib.sha256(b).hexdigest()
  return parts
def timed(args,cwd,useenv):
 # Unix: child-only rusage. Windows: retained native process handle CPU time.
 if not WIN:
  import resource
  before=resource.getrusage(resource.RUSAGE_CHILDREN)
 st=time.perf_counter();p=subprocess.Popen(list(map(str,args)),cwd=cwd,env=useenv,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
 stdout,stderr=p.communicate(timeout=120);elapsed=time.perf_counter()-st
 if WIN:
  from ctypes import wintypes
  values=[wintypes.FILETIME() for _ in range(4)]
  gettimes=ctypes.WinDLL('kernel32',use_last_error=True).GetProcessTimes
  gettimes.argtypes=[wintypes.HANDLE,*([ctypes.POINTER(wintypes.FILETIME)]*4)]
  gettimes.restype=wintypes.BOOL
  assert gettimes(wintypes.HANDLE(int(p._handle)),*[ctypes.byref(x) for x in values])
  cpu=sum((x.dwHighDateTime<<32)+x.dwLowDateTime for x in values[2:])*1e-7
 else:
  after=resource.getrusage(resource.RUSAGE_CHILDREN)
  cpu=after.ru_utime-before.ru_utime+after.ru_stime-before.ru_stime
 assert p.returncode==0,(p.returncode,stderr)
 return elapsed,cpu,(p.returncode,stdout,stderr)
def sample(path,fixture,verify,loops=8,useenv=None):
 total=cpu=0
 with tempfile.TemporaryDirectory() as d:
  p=Path(d)/'fuel_cost_chungcheong.xlsx'
  for _ in range(loops):
   shutil.copy2(fixture,p)
   wall,c,signature=timed([path,*(['--verify'] if verify else [])],d,useenv)
   total+=wall;cpu+=c;signature=(*signature,canonical(p))
 return total/loops,cpu,signature
def summary(rows):
 ratios=[b/a-1 for a,b in rows]
 boots=sorted(statistics.median(rng.choices(ratios,k=len(rows))) for _ in range(10000))
 return {'median':[statistics.median(r[i] for r in rows) for i in (0,1)],
         'p95':[sorted(r[i] for r in rows)[int(len(rows)*.95)] for i in (0,1)],
         'paired_change':statistics.median(ratios),'ci95':[boots[250],boots[9749]],
         'passes':boots[9749]<=.03}
def measure(name,functions):
 for _ in range(3):
  for k in functions:functions[k]()
 raw=[];cpuraw=[];orders=[]
 for i in range(192):
  order=list(functions);rng.shuffle(order);values={k:functions[k]() for k in order}
  assert values['baseline'][2]==values['candidate'][2],(name,i)
  raw.append([values[k][0] for k in functions]);cpuraw.append([values[k][1] for k in functions]);orders.append(order)
 report['cases'][name]={'raw':raw,'CPUraw':cpuraw,'order':orders,
                       'wall':summary(raw),'CPU':summary(cpuraw)}
 save();print(name,report['cases'][name]['wall'],report['cases'][name]['CPU'],flush=True)
cmd(['git','worktree','add','--detach',BASE,BASE_SHA])
assert digest(ROOT/'src/excel/writer/cell_ref.rs')=='e20129d649ce64dae14e6234f4cb710a5e0d3a7e7a7552a80438866b42be5f7b'
changed_paths=cmd(['git','diff','--name-only',BASE_SHA,'--']).stdout.decode().splitlines()
assert all(p=='src/excel/writer/cell_ref.rs' or p=='.github/workflows/i32-native-validation.yml' or p.startswith('validation-i32/') for p in changed_paths),changed_paths

rust=cmd(['rustc','+'+VERSION,'-Vv']).stdout.decode()
assert 'commit-hash: b940084d7eb6a299eb4bfeb8e34901bc051e7ac4' in rust
assert 'host: '+TARGET in rust
report['environment']={'rustc':rust,'platform':platform.platform()}
report['protected_inputs']={str(p.relative_to(ROOT)):digest(p) for p in
                           [ROOT/'Cargo.lock',ROOT/'fuel_cost_chungcheong.xlsx',*sorted((ROOT/'pgo').glob('*.profdata'))]}
save()
for variant,folder in [('baseline',BASE),('candidate',ROOT)]:
 cargo(['fmt','--all','--','--check'],folder,'fmt-'+variant)
 cargo(['clippy','--release','--frozen','--all-targets','--target',TARGET,'--','-D','warnings'],folder,'lint-'+variant)
 cargo(['build','--release','--frozen','--target',TARGET],folder,'normal-'+variant)
report['normal_bytes']={k:exe('normal-'+k).stat().st_size for k in ['baseline','candidate']}
assert report['normal_bytes']['candidate']<=report['normal_bytes']['baseline']
stocklog=cargo(['build-pgo'],BASE,'stock').stderr.decode(errors='replace')
host=TARGET;sysroot=Path(cmd(['rustc','+'+VERSION,'--print','sysroot']).stdout.decode().strip())
llvm=sysroot/'lib/rustlib'/host/'bin'/('llvm-profdata'+EXT)
source=TASK/'source.xls';source.write_bytes(gzip.decompress((TASK/'source.xls.gz').read_bytes()))
replay=TASK/('winhttp.dll' if WIN else 'replay.dylib' if MAC else 'replay.so')
cmd(['clang','-shared','-O2',TASK/'replay.c','-o',replay] if WIN else
    ['clang','-dynamiclib','-O2',TASK/'replay.c','-lcurl','-o',replay] if MAC else
    ['gcc','-shared','-fPIC','-O2',TASK/'replay.c','-o',replay])
env=dict(clean,PGO_XLS=str(source))
if MAC:env.update(DYLD_INSERT_LIBRARIES=str(replay),DYLD_FORCE_FLAT_NAMESPACE='1')
elif not WIN:env['LD_PRELOAD']=str(replay)
if WIN:shutil.copy2(replay,exe('stock').parent/'winhttp.dll')
fixtures={'current':ROOT/'fuel_cost_chungcheong.xlsx','prior':TASK/'before.xlsx','valid':TASK/'valid.xlsx'}
fresh={};original=records(ROOT/'pgo'/(TARGET+'.profdata'))
for variant,folder in [('baseline',BASE),('candidate',ROOT)]:
 raw=TASK/('raw-'+variant);raw.mkdir()
 cargo(['build','--release','--frozen','--target',TARGET],folder,'train-'+variant,'-Cprofile-generate='+str(raw))
 path=exe('train-'+variant)
 if WIN:shutil.copy2(replay,path.parent/'winhttp.dll')
 trainenv=dict(env,LLVM_PROFILE_FILE=str(raw/'%p-%m.profraw'))
 for i in range(40):sample(path,fixtures['prior' if i%2 else 'valid'],i<32,1,trainenv)
 for args in [['--help'],['--version'],['--bad-option']]:
  with tempfile.TemporaryDirectory() as d:cmd([path,*args],d,trainenv,0 if args[0]!='--bad-option' else 1)
 with tempfile.TemporaryDirectory() as d:
  Path(d,'fuel_cost_chungcheong.xlsx').write_bytes(b'not an XLSX')
  cmd([path,'--verify'],d,trainenv,1)
 profile=OUT/('fresh-'+variant+'.profdata')
 cmd([llvm,'merge','-o',profile,*sorted(raw.glob('*.profraw'))]);fresh[variant]=(profile,records(profile))
changed=sorted(k for k in original.keys()&fresh['candidate'][1].keys() if func_hash(original[k])!=func_hash(fresh['candidate'][1][k]))
affected=sorted(k for k in original if 'parse_ref_with_locks' in k)
assert len(affected)==1 and set(changed)<=set(affected),changed
report['changed_CFG_hashes']=changed
assert not (fresh['candidate'][1].keys()-original.keys())
report['profiles']={}
for variant in ['baseline','candidate']:
 key=affected[0]; assert key in fresh[variant][1]
 pattern=key.split(';')[-1];retained=OUT/(variant+'-retained.profdata');selected=OUT/(variant+'-selected.profdata');profile=OUT/(variant+'.profdata')
 cmd([llvm,'merge','--no-function='+pattern,ROOT/'pgo'/(TARGET+'.profdata'),'-o',retained])
 cmd([llvm,'merge','--function='+pattern,fresh[variant][0],'-o',selected])
 cmd([llvm,'merge',retained,selected,'-o',profile]);final=records(profile)
 assert original.keys()==final.keys()
 assert all(original[k]==final[k] for k in original if k!=key)
 folder=BASE if variant=='baseline' else ROOT
 log=cargo(['build','--release','--frozen','--target',TARGET],folder,'pgo-'+variant,
           '-Cprofile-use='+str(profile)+' -Cllvm-args=-pgo-warn-missing-function').stderr.decode(errors='replace')
 assert 'hash mismatch' not in log
 warnings={line.strip() for line in log.splitlines() if 'warning:' in line}
 stockwarnings={line.strip() for line in stocklog.splitlines() if 'warning:' in line}
 assert warnings<=stockwarnings,(warnings,stockwarnings)
 report['profiles'][variant]={'refreshed':key,'unaffected_records_exact':len(original)-1,
                              'sha256':digest(profile),'bytes':profile.stat().st_size,'diagnostics':sorted(warnings)}
 if WIN:shutil.copy2(replay,exe('pgo-'+variant).parent/'winhttp.dll')
report['executable_bytes']={k:exe(k).stat().st_size for k in ['stock','pgo-baseline','pgo-candidate']}
assert report['executable_bytes']['pgo-candidate']<=min(report['executable_bytes']['stock'],report['executable_bytes']['pgo-baseline'])
# Generate both diagnostic parsers from actual native source, retaining the independent oracle.
for variant,folder in [('baseline',BASE),('candidate',ROOT)]:
 dst=TASK/('fc-'+variant)/'src/excel/writer';dst.mkdir(parents=True)
 shutil.copy2(folder/'src/excel/writer/cell_ref.rs',dst/'cell_ref.rs')
cmd([sys.executable,TASK/'prepare_a1_probes.py'])
probes={}
for variant in ['baseline','candidate']:
 path=TASK/('i32-'+variant+EXT);probes[variant]=path
 cmd(['rustc','+'+VERSION,'--edition=2024','-Copt-level=3','-Clto=fat','-Ccodegen-units=1',
      '-Cpanic=abort',TASK/('i32-'+variant+'.rs'),'-o',path])
 assert cmd([path,'check',TASK/'oracle-input.bin']).stdout==(TASK/'oracle-expected.bin').read_bytes()
def kernel(k):
 _,cpu,signature=timed([probes[k],'measure',TASK/'kernel-input.bin'],ROOT,clean)
 wall,sink=signature[1].decode().strip().split(',')
 return float(wall),cpu,sink
measure('kernel',{k:(lambda k=k:kernel(k)) for k in ['baseline','candidate']})
for comparison,basepath in [('controlled',exe('pgo-baseline')),('stock',exe('stock'))]:
 for fixture in ['current','prior']:
  for verify in [True,False]:
   measure(comparison+'-'+fixture+('-verify' if verify else '-default'),
           {k:(lambda path=path,f=fixtures[fixture],v=verify:sample(path,f,v,useenv=env))
            for k,path in [('baseline',basepath),('candidate',exe('pgo-candidate'))]})
for args in [['--help'],['--version'],['--bad-option'],['--verify','--bad-option']]:
 signatures=[]
 for path in [exe('stock'),exe('pgo-baseline'),exe('pgo-candidate')]:
  with tempfile.TemporaryDirectory() as d:
   p=cmd([path,*args],d,env,expected=None);signatures.append((p.returncode,p.stdout,p.stderr))
 assert signatures.count(signatures[0])==3
for data in [b'not an XLSX',b'PK\x03\x04truncated',fixtures['current'].read_bytes()[:-20]]:
 signatures=[]
 for path in [exe('stock'),exe('pgo-baseline'),exe('pgo-candidate')]:
  with tempfile.TemporaryDirectory() as d:
   p=Path(d,'fuel_cost_chungcheong.xlsx');p.write_bytes(data)
   r=cmd([path,'--verify'],d,env,expected=None)
   assert r.returncode!=0 and p.read_bytes()==data
   signatures.append((r.returncode,r.stdout,r.stderr))
 assert signatures.count(signatures[0])==3
report['memory']={}
for label in ['stock','pgo-baseline','pgo-candidate']:
 peaks=[]
 for _ in range(8):
  with tempfile.TemporaryDirectory() as d:
   shutil.copy2(fixtures['current'],Path(d,'fuel_cost_chungcheong.xlsx'))
   p=psutil.Popen([str(exe(label)),'--verify'],cwd=d,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
   peak=0
   while p.poll() is None:
    try:peak=max(peak,p.memory_info().rss)
    except psutil.NoSuchProcess:pass
    time.sleep(.0005)
   stderr=p.communicate()[1];assert p.returncode==0 and peak>0,stderr;peaks.append(peak)
 report['memory'][label]={'samples':peaks,'median':statistics.median(peaks)}
candidate_memory=report['memory']['pgo-candidate']['median']
for label in ['stock','pgo-baseline']:
 old=report['memory'][label]['median']
 assert candidate_memory<=old+max(2*1024*1024,.05*old)
# Ship only clean copies, with actual project packaging; no replay component next to them.
report['packages']={}
for label in ['stock','pgo-baseline','pgo-candidate']:
 td=TASK/('package-'+label);p=td/'release'/('fcupdater'+EXT);p.parent.mkdir(parents=True)
 shutil.copy2(exe(label),p);name='fcupdater-'+TARGET+'-'+label
 cmd(['cargo','+'+VERSION,'run','--frozen','--example','package_artifact','--',name],ROOT,
     dict(clean,CARGO_TARGET_DIR=str(td)))
 artifact=next((ROOT/'artifacts').glob(name+'.*'))
 if WIN:assert artifact.read_bytes()==p.read_bytes()
 else:
  with tarfile.open(artifact) as t:
   members=t.getmembers();assert len(members)==1 and members[0].mode==0o755 and members[0].name==name
   assert t.extractfile(members[0]).read()==p.read_bytes()
 report['packages'][label]={'bytes':artifact.stat().st_size,'executable_sha256':digest(p)}
 if label=='pgo-candidate':
  with tempfile.TemporaryDirectory() as d:
   workbook=Path(d,'fuel_cost_chungcheong.xlsx');shutil.copy2(fixtures['current'],workbook)
   r=cmd([p,'--verify'],d,clean,timeout=180);assert not r.stderr
   canonical(workbook);book=openpyxl.load_workbook(workbook,data_only=False)
   assert book.sheetnames==['유류비','변경내역'];book.close()
   report['live']={'exit':r.returncode,'stderr':r.stderr.decode(),'independent_XLSX_reopen':True}
assert report['packages']['pgo-candidate']['bytes']<=min(report['packages'][k]['bytes'] for k in ['stock','pgo-baseline'])
assert all(digest(ROOT/p)==sha for p,sha in report['protected_inputs'].items())
report['checks']=['two native strict lint/fmt/normal releases','independent A1 oracle covers every16384 column and4 lock combinations per variant',
 'affected-only PGO refresh; other records exact','same192-pair wall+CPU performance gates',
 'all output archives independently CRC checked; final block signatures equal',
 'CLI/malformed errors equivalent without mutation','bounded sampled RSS comparison',
 'actual packaging and clean live native update; product workbook/profiles preserved']
report['accepted']=all(c['wall']['passes'] and c['CPU']['passes'] for c in report['cases'].values())
report['complete']=True;save();assert report['accepted'],report['cases']
