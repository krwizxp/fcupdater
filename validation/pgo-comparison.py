from pathlib import Path
import os,sys,subprocess,shutil,tempfile,gzip,random,statistics,json,time,zipfile,xml.etree.ElementTree as ET
root=Path.cwd();v=root/'validation';target=os.environ['VALIDATION_TARGET'];win=sys.platform=='win32';ext='.exe' if win else '';rng=random.Random(20261010);report={'target':target,'baseline_commit':'8ecd1f17b46290e092e81872008d01185793be8e','source_commit':os.environ['GITHUB_SHA'],'criteria':{'upper95':.03,'pairs':128,'invocations':4},'cases':{}}
def command(args,cwd=root,env=None):
 p=subprocess.run(args,cwd=cwd,env=env,capture_output=True,text=True,encoding='utf-8',timeout=300)
 assert p.returncode==0,(args,p.stderr)
 assert 'hash mismatch' not in p.stderr and 'no profile data available' not in p.stderr,p.stderr
 return p
baseline=root.parent/'pgo-comparison-baseline';command(['git','worktree','add',str(baseline),report['baseline_commit']])
for folder in [baseline,root]:command(['cargo','+1.99.0','build-pgo'],folder)
command(['cargo','+1.99.0','build','--release','--frozen'],root)
executables={'old':(baseline/'target'/target/'release'/('fcupdater'+ext)).resolve(),'new':(root/'target'/target/'release'/('fcupdater'+ext)).resolve(),'normal':(root/'target/release'/('fcupdater'+ext)).resolve()};report['binary_bytes']={k:p.stat().st_size for k,p in executables.items()}
source=v/'pgo-source.xls';source.write_bytes(gzip.decompress((v/'source.xls.gz').read_bytes()));env=dict(os.environ,PGO_XLS=str(source.resolve()));dll=v/('winhttp.dll' if win else 'pgo-replay.so');command(['clang','-shared','-O2',str(v/'replay.c'),'-o',str(dll)] if win else ['gcc','-shared','-fPIC','-O2',str(v/'replay.c'),'-o',str(dll)])
if win:
 for exe in executables.values():shutil.copy2(dll,exe.parent/'winhttp.dll')
else:env['LD_PRELOAD']=str(dll.resolve())
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
def sample(binary,name):
 with tempfile.TemporaryDirectory(prefix='real-cli-pgo-') as scratch:
  work=Path(scratch);elapsed=0.;output=None;result=None
  for _ in range(4):
   shutil.copy2(v/('valid.xlsx' if name=='updated' else 'before.xlsx'),work/'fuel_cost_chungcheong.xlsx')
   started=time.perf_counter();p=command([str(binary),'--verify'],work,env);elapsed+=time.perf_counter()-started;output=p.stdout;result=canonical(work/'fuel_cost_chungcheong.xlsx')
  return elapsed/4,output,result
for comparison,labels in [('oldPGO-newPGO',['old','new']),('same-source-release-PGO',['normal','new'])]:
 for name in ['updated','prior']:
  for label in labels:sample(executables[label],name)
  pairs=[]
  for i in range(128):
   order=labels.copy();rng.shuffle(order);values={label:sample(executables[label],name) for label in order};assert values[labels[0]][1:]==values[labels[1]][1:];pairs.append([values[labels[0]][0],values[labels[1]][0]])
  ratios=[b/a for a,b in pairs];boot=sorted(statistics.median(rng.choices(ratios,k=len(ratios))) for _ in range(5000));summary={'median_seconds':[statistics.median(a for a,b in pairs),statistics.median(b for a,b in pairs)],'median_change':statistics.median(ratios)-1,'ci95':[boot[125]-1,boot[4874]-1]};report['cases'][comparison+'-'+name]={'summary':summary,'raw':pairs};print(comparison,name,json.dumps(summary),flush=True);(v/'pgo-comparison-results.json').write_text(json.dumps(report,indent=2)+'\n')
assert all(x['summary']['ci95'][1]<=.03 for x in report['cases'].values()),'PGO runtime non-regression not established'
print('All PGO comparisons passed',flush=True)
