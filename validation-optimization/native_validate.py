from pathlib import Path
import os,sys,json,subprocess,shutil,gzip,tempfile,zipfile,hashlib,random,statistics,time,platform,xml.etree.ElementTree as ET
import openpyxl,psutil,tarfile
ROOT=Path.cwd();TASK=ROOT/'validation-optimization';TARGET=os.environ['OPT_TARGET'];WIN=sys.platform=='win32';MAC=sys.platform=='darwin';EXT='.exe' if WIN else '';VERSION='1.99.0';OUT=TASK/'evidence'/TARGET;OUT.mkdir(parents=True,exist_ok=True)
BASE_SHA='4a0df6f79f5ab812d699af99d8dd5dbe4a219900';BASE=TASK/'baseline';rng=random.Random(2026101059)
report={'target':TARGET,'base_commit':BASE_SHA,'candidate_commit':os.environ.get('GITHUB_SHA'),'run':os.environ.get('GITHUB_RUN_ID'),'runner_image':os.environ.get('ImageVersion'),'criteria':{'pairs':48,'loops':3,'bootstrap':10000,'runtime_upper_bound':.03,'shipped_size_growth':0,'memory_upper_bound':'baseline median + max(2MiB,5%)'},'cases':{},'checks':[],'build_diagnostics':[]}
def save():(OUT/'results.json').write_text(json.dumps(report,ensure_ascii=False,indent=2)+'\n',encoding='utf-8')
def digest(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def cmd(args,cwd=ROOT,env=None,expected=0,timeout=600):
 p=subprocess.run(list(map(str,args)),cwd=cwd,env=env,capture_output=True,timeout=timeout)
 if expected is not None and p.returncode!=expected:raise RuntimeError((args,p.returncode,p.stdout[-2500:],p.stderr[-8000:]))
 return p
clean=dict(os.environ)
for k in ['RUSTFLAGS','CARGO_ENCODED_RUSTFLAGS','LD_PRELOAD','DYLD_INSERT_LIBRARIES','DYLD_FORCE_FLAT_NAMESPACE','LLVM_PROFILE_FILE']:clean.pop(k,None)
def cargo(args,td,folder=ROOT,flags=None):
 env=dict(clean,CARGO_TARGET_DIR=str(td))
 if flags:env['RUSTFLAGS']=flags
 p=cmd(['cargo','+'+VERSION,*args],folder,env);log=p.stderr.decode('utf-8','replace');report['build_diagnostics'].append({'args':args,'folder':str(folder),'log':log});save()
 assert 'hash mismatch' not in log,log
 return p
cmd(['git','worktree','add','--detach',BASE,BASE_SHA]);report['environment']={'rust':cmd(['rustc','+'+VERSION,'-Vv']).stdout.decode(),'platform':platform.platform(),'cpu':platform.processor()};report['sources']={str(p.relative_to(ROOT)):digest(p) for p in (ROOT/'src').rglob('*') if p.is_file()};report['lock']=digest(ROOT/'Cargo.lock');report['workbook']=digest(ROOT/'fuel_cost_chungcheong.xlsx');save()
normal=TASK/'build-baseline';train=TASK/'build-train';final=TASK/'build-final';raw=TASK/'raw';raw.mkdir(exist_ok=True)
cargo(['fmt','--all','--','--check'],final);cargo(['clippy','--release','--frozen','--all-targets','--target',TARGET,'--','-D','warnings'],final);report['checks']+=['format','native lint all targets'];cargo(['build-pgo'],normal,BASE);cargo(['build','--release','--frozen','--target',TARGET],train,flags='-Cprofile-generate='+str(raw))
source=TASK/'source.xls';source.write_bytes(gzip.decompress((TASK/'source.xls.gz').read_bytes()))
replay=TASK/('winhttp.dll' if WIN else 'replay.dylib' if MAC else 'replay.so')
cmd(['clang','-shared','-O2',TASK/'replay.c','-o',replay] if WIN else ['clang','-dynamiclib','-O2',TASK/'replay.c','-lcurl','-o',replay] if MAC else ['gcc','-shared','-fPIC','-O2',TASK/'replay.c','-o',replay])
exes={'baseline':normal/TARGET/'release'/('fcupdater'+EXT),'train':train/TARGET/'release'/('fcupdater'+EXT)}
env=dict(clean,PGO_XLS=str(source))
if MAC:env.update(DYLD_INSERT_LIBRARIES=str(replay),DYLD_FORCE_FLAT_NAMESPACE='1')
elif not WIN:env['LD_PRELOAD']=str(replay)
if WIN:
 for exe in exes.values():shutil.copy2(replay,exe.parent/'winhttp.dll')
trainenv=dict(env,LLVM_PROFILE_FILE=str(raw/'%p-%m.profraw'))
def canonical(p):
 with zipfile.ZipFile(p) as z:
  assert z.testzip() is None
  parts={}
  for n in sorted(z.namelist()):
   b=z.read(n)
   if n=='docProps/core.xml':
    doc=ET.fromstring(b)
    for e in doc.iter():
     if e.tag.endswith('}modified'):e.text='NORMALIZED'
    b=ET.tostring(doc)
   parts[n]=hashlib.sha256(b).hexdigest()
  return parts
fixtures={'current':ROOT/'fuel_cost_chungcheong.xlsx','prior':TASK/'before.xlsx','valid':TASK/'valid.xlsx'}
def sample(label,fixture,verify,loops=3,useenv=env):
 elapsed=0;sig=None
 with tempfile.TemporaryDirectory(prefix='fc-opt-') as d:
  p=Path(d)/'fuel_cost_chungcheong.xlsx'
  for _ in range(loops):
   shutil.copy2(fixture,p);st=time.perf_counter();r=cmd([exes[label],*(['--verify'] if verify else [])],d,useenv);elapsed+=time.perf_counter()-st
   sig=(r.returncode,r.stdout.hex(),r.stderr.hex(),canonical(p))
 return elapsed/loops,sig
for i in range(40):sample('train',fixtures['prior' if i%2 else 'valid'],i<32,1,trainenv)
for args in [['--help'],['--version'],['--bad-option']]:
 with tempfile.TemporaryDirectory() as d:cmd([exes['train'],*args],d,trainenv,0 if args[0]!='--bad-option' else 1)
with tempfile.TemporaryDirectory() as d:
 Path(d,'fuel_cost_chungcheong.xlsx').write_bytes(b'not an XLSX');assert cmd([exes['train'],'--verify'],d,trainenv,expected=None).returncode!=0
host=next(l.split(': ',1)[1] for l in report['environment']['rust'].splitlines() if l.startswith('host:'));sysroot=Path(cmd(['rustc','+'+VERSION,'--print','sysroot']).stdout.decode().strip());llvm=sysroot/'lib/rustlib'/host/'bin'/('llvm-profdata'+EXT);prof=OUT/(TARGET+'.profdata');cmd([llvm,'merge','-o',prof,*sorted(raw.glob('*.profraw'))])
fresh=OUT/'full-fresh.profdata';shutil.copy2(prof,fresh)
retained=TASK/'retained.profdata';crc_only=TASK/'crc-only.profdata';pattern='11zip_archive12crc32_update'
cmd([llvm,'merge','--no-function='+pattern,ROOT/'pgo'/(TARGET+'.profdata'),'-o',retained]);cmd([llvm,'merge','--function='+pattern,fresh,'-o',crc_only]);cmd([llvm,'merge',retained,crc_only,'-o',prof])
def profile_records(p):
 text=cmd([llvm,'show','--all-functions','--counts','--text',p]).stdout.decode().removeprefix(':ir\n').strip()
 return {record.splitlines()[0]:record for record in text.split('\n\n')}
old_records=profile_records(ROOT/'pgo'/(TARGET+'.profdata'));new_records=profile_records(prof);diff=[k for k in old_records.keys()|new_records.keys() if old_records.get(k)!=new_records.get(k)];assert len(diff)==1 and pattern in diff[0],diff
report['profile_refresh']={'strategy':'refresh only changed CRC function from actual current native instrumentation; preserve all unchanged function records byte-for-byte','changed_records':diff,'unchanged_records':len(old_records)-1}
report['training']={'updates':40,'verify_updates':32,'CLI':3,'malformed':1,'raw_profiles':len(list(raw.glob('*.profraw'))),'profile':{'bytes':prof.stat().st_size,'sha256':digest(prof)}};save()
cargo(['build','--release','--frozen','--target',TARGET],final,flags='-Cprofile-use='+str(prof)+' -Cllvm-args=-pgo-warn-missing-function');exes['candidate']=final/TARGET/'release'/('fcupdater'+EXT)
if WIN:shutil.copy2(replay,exes['candidate'].parent/'winhttp.dll')
report['binary']={k:{'bytes':p.stat().st_size,'sha256':digest(p)} for k,p in exes.items() if k!='train'}
def summary(raw):
 ratios=[b/a-1 for a,b in raw];boots=sorted(statistics.median(rng.choices(ratios,k=len(ratios))) for _ in range(10000));return {'median_seconds':[statistics.median(r[i] for r in raw) for i in (0,1)],'p95_seconds':[sorted(r[i] for r in raw)[45] for i in (0,1)],'paired_median_change':statistics.median(ratios),'ci95':[boots[250],boots[9749]]}
for fixture in ('current','prior'):
 for verify in (True,False):
  name=fixture+('-verify' if verify else '-skip');rawsamples=[]
  for _ in range(3):
   for k in ('baseline','candidate'):sample(k,fixtures[fixture],verify)
  for i in range(48):
   order=['baseline','candidate'];rng.shuffle(order);vals={k:sample(k,fixtures[fixture],verify) for k in order};assert vals['baseline'][1]==vals['candidate'][1],name;rawsamples.append([vals[k][0] for k in ('baseline','candidate')])
  report['cases'][name]={'raw':rawsamples,'summary':summary(rawsamples)};save();print(name,report['cases'][name]['summary'],flush=True)
for args in [['--help'],['--version'],['--bad-option'],['--verify','--bad-option']]:
 vals=[]
 for label in ('baseline','candidate'):
  with tempfile.TemporaryDirectory() as d:
   p=cmd([exes[label],*args],d,env,expected=None);vals.append((p.returncode,p.stdout,p.stderr))
 assert vals[0]==vals[1]
with zipfile.ZipFile(fixtures['current']) as z:
 info=z.infolist()[0];bad=bytearray(fixtures['current'].read_bytes());pos=info.header_offset+30+len(info.filename.encode())+len(info.extra)+max(0,info.compress_size//2);bad[pos]^=0x80
for data in [b'not an XLSX',b'PK\x03\x04truncated',bytes(bad)]:
 vals=[]
 for label in ('baseline','candidate'):
  with tempfile.TemporaryDirectory() as d:
   p=Path(d)/'fuel_cost_chungcheong.xlsx';p.write_bytes(data);r=cmd([exes[label],'--verify'],d,env,expected=None);assert r.returncode!=0 and p.read_bytes()==data;vals.append((r.returncode,r.stdout,r.stderr))
 assert vals[0]==vals[1]
report['checks']+=['all paired XLSX ZIP parts and CLI bytes equivalent','CRC corrupted and truncated XLSX rejected unchanged','CLI diagnostics equivalent']
report['memory']={}
for label in ('baseline','candidate'):
 peaks=[]
 for _ in range(8):
  with tempfile.TemporaryDirectory() as d:
   shutil.copy2(fixtures['current'],Path(d)/'fuel_cost_chungcheong.xlsx');child=psutil.Popen([str(exes[label]),'--verify'],cwd=d,env=env,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE);peak=0
   while child.poll() is None:
    try:peak=max(peak,child.memory_info().rss)
    except psutil.NoSuchProcess:pass
    time.sleep(.0005)
   stderr=child.communicate()[1];assert child.returncode==0,stderr;assert peak>0;peaks.append(peak)
 report['memory'][label]={'peak_rss_bytes':peaks,'median':statistics.median(peaks),'sampling_seconds':.0005}
save()

# Independent workbook reader, true live HTTP, and original packaging with no replay DLL shipped.
package=TASK/'package';exe=package/TARGET/'release'/('fcupdater'+EXT);exe.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(exes['candidate'],exe)
with tempfile.TemporaryDirectory() as d:
 p=Path(d)/'fuel_cost_chungcheong.xlsx';shutil.copy2(fixtures['current'],p);r=cmd([exe,'--verify'],d,clean,timeout=180);(OUT/'live.stdout.txt').write_bytes(r.stdout);(OUT/'live.stderr.txt').write_bytes(r.stderr);assert zipfile.ZipFile(p).testzip() is None;book=openpyxl.load_workbook(p,data_only=False);assert book.sheetnames==['유류비','변경내역'];book.close();report['checks'].append('live native updater --verify + independent XLSX reopen')
for label in ('baseline','candidate'):
 td=TASK/('package-'+label);p=td/TARGET/'release'/('fcupdater'+EXT);p.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(exes[label],p);name='fcupdater-'+TARGET+'-'+label;penv=dict(clean,CARGO_TARGET_DIR=str(td/TARGET));cmd(['cargo','+'+VERSION,'run','--frozen','--example','package_artifact','--',name],ROOT,penv);artifact=next((ROOT/'artifacts').glob(name+'.*'));report['binary'][label]['package_bytes']=artifact.stat().st_size;shutil.copy2(artifact,OUT/artifact.name)
 if WIN:assert artifact.read_bytes()==p.read_bytes()
 else:
  with tarfile.open(artifact) as archive:
   members=archive.getmembers();assert len(members)==1 and members[0].name==name;assert archive.extractfile(members[0]).read()==p.read_bytes()
report['checks']+=['native packaging and no replay component shipped'];report['accepted']=report['binary']['candidate']['bytes']<=report['binary']['baseline']['bytes'] and all(c['summary']['ci95'][1]<=.03 for c in report['cases'].values());report['accepted']=report['accepted'] and report['memory']['candidate']['median']<=report['memory']['baseline']['median']+max(2*1024*1024,.05*report['memory']['baseline']['median']);report['complete']=True;save();print('Accepted:',report['accepted'],flush=True)
