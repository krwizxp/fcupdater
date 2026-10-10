"""Task-only PGO training from unchanged actual product binaries."""
from pathlib import Path
import os, sys, subprocess, tempfile, shutil, time, threading, json, hashlib, gzip
import random, statistics, re, zipfile, xml.etree.ElementTree as ET, email.utils
import faulthandler
faulthandler.enable()
faulthandler.dump_traceback_later(60, repeat=True)
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler

WIN = sys.platform == 'win32'
EXT = '.exe' if WIN else ''
TARGET = os.environ.get('PATTERN_TARGET', 'x86_64-unknown-linux-gnu')
ROOT = Path.cwd()
TASK = ROOT / 'validation-patterns'
OUT = TASK / 'evidence' / TARGET
OUT.mkdir(parents=True, exist_ok=True)
RNG = random.Random(2026101001)
REPORT = {'target': TARGET, 'run': os.environ.get('GITHUB_RUN_ID'),
          'rust': '1.99.0', 'user_pattern': {'srg': 'Windows PC menu 4: 8145060 records; GitHub Actions generate-multiple default: 10',
                                          'fcupdater': 'Windows PC and Update Master Excel Actions: --verify'},
          'criteria': {'non_regression_upper95': .03, 'main_improvement_upper95': 0,
                       'main_median_improvement': -.01, 'executable_growth': 0,
                       'selection_pairs': 32, 'final_pairs': 64, 'bulk_full_pairs': 4,
                       'maximum_profiles': 3}, 'apps': {}}

def save():
    (OUT / 'results.json').write_text(json.dumps(REPORT, ensure_ascii=False, indent=2)+'\n', encoding='utf-8')

def cmd(args, cwd=ROOT, env=None, timeout=600, check=True):
    p = subprocess.run(list(map(str,args)), cwd=cwd, env=env, capture_output=True, timeout=timeout)
    if check and p.returncode:
        raise RuntimeError((list(map(str,args)),p.returncode,p.stdout[-2000:],p.stderr[-8000:]))
    return p

def cargo(folder, td, flags=None):
    env = dict(os.environ, CARGO_TARGET_DIR=str(td))
    env.pop('CARGO_ENCODED_RUSTFLAGS', None)
    env.pop('RUSTFLAGS', None)
    if flags: env['RUSTFLAGS'] = flags
    p=cmd(['cargo','+1.99.0','build','--release','--frozen','--target',TARGET], folder, env)
    stderr=p.stderr.decode('utf-8','replace')
    assert 'hash mismatch' not in stderr and 'no profile data available' not in stderr, stderr
    return (td / TARGET / 'release' / (folder.name if False else 'unused'))

class Terminal:
    def __init__(self, exe, cwd, env):
        self.text=''; self.lock=threading.Lock(); self.ended=threading.Event()
        if WIN:
            from winpty import PtyProcess
            self.p=PtyProcess.spawn([str(exe)],cwd=str(cwd),env=env,dimensions=(40,160))
        else:
            import pty, termios, fcntl, struct
            self.master,slave=pty.openpty()
            fcntl.ioctl(slave,termios.TIOCSWINSZ,struct.pack('HHHH',40,160,0,0))
            def setup():
                os.setsid(); fcntl.ioctl(slave,termios.TIOCSCTTY,0)
            self.p=subprocess.Popen([str(exe)],cwd=cwd,env=env,stdin=slave,stdout=slave,stderr=slave,preexec_fn=setup)
            os.close(slave)
        self.reader=threading.Thread(target=self.drain,daemon=True); self.reader.start()
    def drain(self):
        try:
            while True:
                s=self.p.read(65536) if WIN else os.read(self.master,65536).decode('utf-8','replace')
                if not s: break
                with self.lock: self.text=(self.text+s)[-500000:]
        except (OSError,EOFError): pass
        finally: self.ended.set()
    def send(self,s):
        if WIN: self.p.write(s.replace('\n','\r\n'))
        else: os.write(self.master,s.encode())
    def wait(self,needle,timeout=600):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            with self.lock: value=self.text
            if needle in value: return value
            if self.ended.is_set(): raise RuntimeError(('terminal closed',needle,value[-4000:]))
            time.sleep(.001)
        raise TimeoutError((needle,self.text[-4000:]))
    def clear(self):
        with self.lock:self.text=''
    def close(self):
        self.send('0\n')
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            alive=self.p.isalive() if WIN else self.p.poll() is None
            if not alive: break
            time.sleep(.01)
        if WIN:
            status=self.p.exitstatus;self.p.close(force=True)
        else:
            if self.p.poll() is None:self.p.kill()
            status=self.p.wait();os.close(self.master)
        assert status==0, ('terminal exit',status,self.text[-4000:])

def inspect_records(path,n):
    assert path.stat().st_size > n*500
    lines=0
    with path.open('rb') as f:
        first=f.read(65536);assert first.startswith(b'\xef\xbb\xbf64')
        lines+=first.count(b'\n')
        while b:=f.read(32*1024*1024):lines+=b.count(b'\n')
        f.seek(max(0,path.stat().st_size-4096));tail=f.read()
    assert lines==17*n,(n,lines)
    assert 'NMS 은하 좌표: '.encode() in tail
    for match in re.finditer(rb'64[^\n]*?: ([0-9]+) ',first): assert int(match[1])<=2**64-1
    return {'records':n,'lines':lines,'bytes':path.stat().st_size}

def menu_bulk(exe,n,env,cancel=False,full_check=False):
    with tempfile.TemporaryDirectory(prefix='srg-pattern-terminal-') as d:
        cwd=Path(d);t=Terminal(exe,cwd,env);t.wait('선택해 주세요: ');t.clear();t.send('4\n')
        t.wait('생성할 데이터 개수');t.clear();start=time.perf_counter();t.send(str(n)+'\n')
        if cancel:
            t.wait('생성 중 Enter');time.sleep(.12);t.send('\n');text=t.wait('Enter 입력으로 작업을 중단했습니다.')
        else:text=t.wait(f'총 {n}건 생성 완료')
        elapsed=time.perf_counter()-start;t.wait('선택해 주세요: ');t.close()
        path=cwd/'random_data.txt'
        checks=inspect_records(path,n+1) if full_check and not cancel else {'bytes':path.stat().st_size}
        if cancel:
            match=re.search(r'총 (\d+)건이',text);assert match and int(match[1])<n
            checks['cancelled_count']=int(match[1])
        return elapsed,checks

class Handler(BaseHTTPRequestHandler):
    def do_HEAD(self):
        self.send_response_only(200);self.send_header('Date',email.utils.formatdate(usegmt=True))
        self.send_header('Content-Length','0');self.send_header('Connection','close');self.end_headers()
    def log_message(self,*args): pass

SERVER=ThreadingHTTPServer(('127.0.0.1',0),Handler)
threading.Thread(target=SERVER.serve_forever,daemon=True).start()
HOST='http://127.0.0.1:'+str(SERVER.server_port)

def manual_input(n=200):
    rng=random.Random(290731)
    chunks=[]
    for _ in range(n):
        chunks.extend(['7',str(rng.getrandbits(64))])
        # Surplus supplemental lines are harmless menu exits, so drive this via terminal prompts instead.
    return chunks

def manual_terminal(exe,env,n=24):
    with tempfile.TemporaryDirectory(prefix='srg-manual-pattern-') as d:
        t=Terminal(exe,Path(d),env);t.wait('선택해 주세요: ')
        start=time.perf_counter();rng=random.Random(290731);record=[]
        for _ in range(n):
            t.clear();t.send('7\n');t.wait('num_64를 입력해 주세요');t.clear();t.send(str(rng.getrandbits(64))+'\n')
            while True:
                deadline=time.monotonic()+15
                while time.monotonic()<deadline:
                    with t.lock:s=t.text
                    if '선택해 주세요: ' in s:break
                    if 'supp 값 #' in s and '입력 (' in s: t.clear();t.send(str(rng.getrandbits(64))+'\n')
                    time.sleep(.001)
                else:raise TimeoutError(t.text[-4000:])
                if '선택해 주세요: ' in s:break
        elapsed=time.perf_counter()-start;t.close()
        data=Path(d,'random_data.txt').read_bytes()
        # First record is nondeterministic initialization. Remaining conversion records are deterministic.
        data=b'\n'.join(data.split(b'\n')[17:])
        return elapsed,hashlib.sha256(data).hexdigest()

def srg_cli(exe,case,env,loops=1):
    with tempfile.TemporaryDirectory(prefix='srg-pattern-cli-') as d:
        elapsed=0.; sig=None
        for _ in range(loops):
            path=Path(d,'random_data.txt');path.unlink(missing_ok=True)
            args={'help':['--help'],'version':['--version'],'invalid':['generate','0'],
                  'integer':['random-integer','-9223372036854775807','9223372036854775807'],
                  'float':['random-float','-1e150','1e150'],
                  'ladder':['ladder',','.join('p'+str(i) for i in range(64)),','.join('r'+str(i) for i in range(64))],
                  'single':['generate','1'],'small':['generate','8'],'actions-default':['generate','10'],'medium':['generate','2048'],
                  'time':['time-observe',HOST,'1']}[case]
            start=time.perf_counter();p=cmd([exe,*args],d,env,check=False);elapsed+=time.perf_counter()-start
            assert (p.returncode==0)==(case!='invalid'),(case,p.stderr)
            if case in ['help','version','invalid']:sig=(p.returncode,p.stdout.hex(),p.stderr.hex())
            elif case=='integer':
                match=re.search(rb': (-?\d+) \(0x',p.stdout);assert match and abs(int(match[1]))<=2**63-1
            elif case=='float':
                match=re.search(rb': ([+-]?[0-9.eE+-]+)',p.stdout);assert match and -1e150<=float(match[1])<=1e150
            elif case=='ladder':
                text=p.stdout.decode('utf-8');assert all('p'+str(i) in text for i in range(64))
                assert all('r'+str(i) in text for i in range(64))
            elif case in ['single','small','actions-default','medium']:inspect_records(path,int(args[1]))
        return elapsed/loops,sig

def canonical(path):
    with zipfile.ZipFile(path) as z:
        assert z.testzip() is None
        output={}
        for name in sorted(z.namelist()):
            b=z.read(name)
            if name=='docProps/core.xml':
                tree=ET.fromstring(b)
                for e in tree.iter():
                    if e.tag.endswith('}modified'):e.text='NORMALIZED'
                b=ET.tostring(tree)
            output[name]=hashlib.sha256(b).hexdigest()
        return output

def fc_cli(exe,case,env,loops=1):
    with tempfile.TemporaryDirectory(prefix='fc-pattern-cli-') as d:
        elapsed=0.;sig=None
        for _ in range(loops):
            workbook=Path(d,'fuel_cost_chungcheong.xlsx')
            if case.startswith(('verify','skip')):
                fixture=TASK/('before.xlsx' if 'prior' in case else 'valid.xlsx')
                shutil.copy2(fixture,workbook)
                args=['--verify'] if case.startswith('verify') else []
            elif case=='malformed':workbook.write_bytes(b'not an XLSX');args=['--verify']
            else:args={'help':['--help'],'version':['--version'],'invalid':['--bad-option']}[case]
            start=time.perf_counter();p=cmd([exe,*args],d,env,check=False);elapsed+=time.perf_counter()-start
            assert (p.returncode==0)==(case not in ['invalid','malformed']),(case,p.stderr)
            sig=(p.returncode,p.stdout.hex(),p.stderr.hex(),canonical(workbook) if case.startswith(('verify','skip')) else None)
        return elapsed/loops,sig

def workflow_cli(wrapper,exe,env,loops=1):
    with tempfile.TemporaryDirectory(prefix='real-srg-workflow-') as d:
        e=dict(env,SRG_ACTION='generate-multiple',SRG_COUNT='10',CARGO_TARGET_DIR=str(exe.parent.parent))
        elapsed=0.
        for _ in range(loops):
            start=time.perf_counter();p=cmd([wrapper],d,e);elapsed+=time.perf_counter()-start
            inspect_records(Path(d,'artifacts/srg-result-random_data.txt'),10)
            assert '총 10건 생성 완료'.encode() in p.stdout
        return elapsed/loops,None

def interval(pairs):
    ratios=[b/a for a,b in pairs];rng=random.Random(199017)
    boot=sorted(statistics.median(rng.choices(ratios,k=len(ratios))) for _ in range(5000))
    return {'median_seconds':[statistics.median(x[i] for x in pairs) for i in [0,1]],
            'median_change':statistics.median(ratios)-1,'ci95':[boot[125]-1,boot[4874]-1],
            'p95_seconds':[sorted(x[i] for x in pairs)[min(len(pairs)-1,int(len(pairs)*.95))] for i in [0,1]]}

def pairs(sample,a,b,n):
    values=[]
    for i in range(n):
        if i % 8 == 0: print("paired sample", a, b, i, n, flush=True)
        order=[a,b];RNG.shuffle(order);v={label:sample(label) for label in order}
        if v[a][1] is not None and v[b][1] is not None:assert v[a][1]==v[b][1],('behavior',a,b,v[a][1],v[b][1])
        values.append([v[a][0],v[b][0]])
    return {'summary':interval(values),'raw':values}

def executable(td,app):return (td/TARGET/'release'/(app+EXT)).resolve()

def train(app,folder,trainexe,raw,env,wrapper=None):
    groups=['main','actions','features','errors']
    for group in groups:(raw/group).mkdir(parents=True,exist_ok=True)
    def e(group):return dict(env,LLVM_PROFILE_FILE=str(raw/group/'%p-%m.profraw'))
    coverage={}
    if app=='srg':
        elapsed,check=menu_bulk(trainexe,8145060,e('main'),full_check=True)
        coverage['menu4-real-count']={'seconds':elapsed,**check};print('Trained actual SRG menu 8145060',elapsed,flush=True)
        workflow_cli(wrapper,trainexe,e('actions'),100)
        coverage['actions-default']='Run SRG Manually generate-multiple default 10, real CLI and workflow wrapper'
        for name in ['single','small','medium','integer','float','ladder','time']:
            srg_cli(trainexe,name,e('features'),3 if name!='time' else 2)
        manual_terminal(trainexe,e('features'),40)
        elapsed,check=menu_bulk(trainexe,10000000,e('features'),cancel=True)
        coverage['enter-cancellation']=check
        menu_bulk(trainexe,32768,e('features'),full_check=True)
        with tempfile.TemporaryDirectory(prefix='srg-other-menus-') as d:
            p=cmd([trainexe],d,e('features'),check=False,timeout=30) if False else subprocess.run(
                [str(trainexe)],cwd=d,env=e('features'),input='1\na,b,c,d\n1,2,3,4\n2\n1\n-100\n100\n2\n2\n-10\n10\n3\n6\n0\n'.encode(),capture_output=True,timeout=30)
            assert p.returncode==0,p.stderr
        for name in ['help','version','invalid']:srg_cli(trainexe,name,e('errors'),4)
        coverage['normal_paths']=['menu1 ladder','menu2 integer/float','menu3 single','menu4 bulk+cancel',
                                  'menu6 clear','menu7 manual all record fields','CLI time-observe native loopback HTTP',
                                  'CLI generate small/medium','CLI integer/float/ladder','help/version']
        coverage['not_profiled']=['scheduled mouse-click/F5 successful OS input delivery','GUI-specific Linux portal permissions']
    else:
        for _ in range(24):
            for name in ['verify-current','verify-prior']:fc_cli(trainexe,name,e('main'))
        for _ in range(12):fc_cli(trainexe,'verify-current',e('actions'))
        for _ in range(6):
            for name in ['skip-current','skip-prior']:fc_cli(trainexe,name,e('features'))
        for name in ['help','version','invalid','malformed']:fc_cli(trainexe,name,e('errors'),4)
        coverage['normal_paths']=['verify current+prior workbook','default update current+prior','help/version','invalid option','malformed XLSX']
        coverage['network']='captured public Opinet response replay; external adapter outside product'
    return coverage

def run_app(app,folder,llvm):
    print('Starting',app,TARGET,flush=True)
    work=OUT/app;work.mkdir(exist_ok=True)
    data={'source_sha256':{},'coverage':{},'selection':{},'comparisons':{}}
    for path in sorted((folder/'src').rglob('*.rs')):data['source_sha256'][str(path.relative_to(folder))]=hashlib.sha256(path.read_bytes()).hexdigest()
    REPORT['apps'][app]=data;save()
    cmd(['cargo','+1.99.0','fmt','--all','--','--check'],folder)
    cmd(['cargo','+1.99.0','clippy','--release','--frozen','--all-targets','--target',TARGET,'--','-D','warnings'],folder)
    builds=work/'builds';builds.mkdir(exist_ok=True)
    wrapper=None
    if app=='srg':
        helper=builds/'workflow-helper'
        cmd(['cargo','+1.99.0','build','--release','--frozen','--example','manual_workflow'],folder,dict(os.environ,CARGO_TARGET_DIR=str(helper)))
        wrapper=(helper/'release/examples'/('manual_workflow'+EXT)).resolve()
    exes={}
    for label,flags in [('normal',None),('old','-Cprofile-use='+str(folder/'pgo'/(TARGET+'.profdata'))+' -Cllvm-args=-pgo-warn-missing-function')]:
        td=builds/label;cargo(folder,td,flags);exes[label]=executable(td,app)
    env=dict(os.environ)
    if app=='fcupdater':
        source=work/'source.xls';source.write_bytes(gzip.decompress((TASK/'source.xls.gz').read_bytes()))
        env['PGO_XLS']=str(source);dll=work/('winhttp.dll' if WIN else 'replay.so')
        cmd(['clang','-shared','-O2',TASK/'replay.c','-o',dll] if WIN else ['gcc','-shared','-fPIC','-O2',TASK/'replay.c','-o',dll])
        if WIN:
            for exe in exes.values():shutil.copy2(dll,exe.parent/'winhttp.dll')
        else:env['LD_PRELOAD']=str(dll)
    raw=work/'raw';cargo(folder,builds/'train','-Cprofile-generate='+str(raw))
    trainexe=executable(builds/'train',app)
    if app=='fcupdater' and WIN:shutil.copy2(dll,trainexe.parent/'winhttp.dll')
    data['coverage']=train(app,folder,trainexe,raw,env,wrapper);save()
    weights=[(1,1,1,1),(1,10000,100,1),(1,100000,1000,1)] if app=='srg' else [(16,16,1,1),(4,4,1,1),(1,1,1,1)]
    for i,ws in enumerate(weights):
        profile=work/('candidate-'+str(i)+'.profdata')
        args=[llvm,'merge','-o',profile]
        for group,weight in zip(['main','actions','features','errors'],ws):
            files=list((raw/group).glob('*.profraw'));assert files,group
            args.extend('--weighted-input='+str(weight)+','+str(f) for f in files)
        cmd(args);label='candidate-'+str(i);td=builds/label
        cargo(folder,td,'-Cprofile-use='+str(profile)+' -Cllvm-args=-pgo-warn-missing-function')
        exes[label]=executable(td,app)
        if app=='fcupdater' and WIN:shutil.copy2(dll,exes[label].parent/'winhttp.dll')
        data['selection'][label]={'weights':ws,'binary_bytes':exes[label].stat().st_size,'profile_sha256':hashlib.sha256(profile.read_bytes()).hexdigest()}
        main=lambda name: (menu_bulk(exes[name],250000,env)[0],None) if app=='srg' else fc_cli(exes[name],'verify-current',env,4)
        data['selection'][label]['vs_old']=pairs(main,'old',label,32)
        if app=='srg':data['selection'][label]['actions_vs_old']=pairs(lambda name:workflow_cli(wrapper,exes[name],env,16),'old',label,32)
        print(app,label,'selection',data['selection'][label]['vs_old']['summary'],flush=True);save()
    # Selection is followed by fresh final randomized samples; never use selection samples as acceptance evidence.
    selected=min(data['selection'],key=lambda k:max(data['selection'][k]['vs_old']['summary']['median_change'],data['selection'][k].get('actions_vs_old',data['selection'][k]['vs_old'])['summary']['median_change']))
    data['selected']=selected;profile=work/(selected+'.profdata');data['binary_bytes']={k:exes[k].stat().st_size for k in ['normal','old',selected]}
    shutil.copy2(profile, OUT/(app+'-'+TARGET+'.profdata'))
    cases=(['menu-bulk','actions-default','workflow-default','single','small','medium','integer','float','ladder','manual','time','help','version','invalid'] if app=='srg'
           else ['verify-current','verify-prior','skip-current','skip-prior','help','version','invalid','malformed'])
    for case in cases:
        def sample(label):
            if app=='fcupdater':return fc_cli(exes[label],case,env,4)
            if case=='workflow-default':return workflow_cli(wrapper,exes[label],env,8)
            if case=='menu-bulk':return menu_bulk(exes[label],250000,env)[0],None
            if case=='manual':return manual_terminal(exes[label],env,16)
            return srg_cli(exes[label],case,env,4 if case!='time' else 1)
        n=16 if case in ['manual','time'] else 64
        for a,b in [('old',selected),('normal',selected)]:
            name=a+'-'+case;print(app, 'BEGIN', name, flush=True);data['comparisons'][name]=pairs(sample,a,b,n)
            print(app,name,data['comparisons'][name]['summary'],flush=True);save()
    if app=='srg':
        for a in ['old','normal']:
            samples=[];checks=[]
            for _ in range(4):
                labels=[a,selected];RNG.shuffle(labels);v={}
                for label in labels:
                    elapsed,check=menu_bulk(exes[label],8145060,env,full_check=True);v[label]=elapsed;checks.append({'label':label,**check})
                samples.append([v[a],v[selected]])
            data['comparisons'][a+'-menu-8145060']={'summary':interval(samples),'raw':samples,'records':checks}
            print(app,a,'full count',data['comparisons'][a+'-menu-8145060']['summary'],flush=True);save()
        data['final_cancellation']=menu_bulk(exes[selected],10000000,env,cancel=True)[1]
    profile_output=OUT/(app+'-'+TARGET+'.profdata');shutil.copy2(profile,profile_output)
    data['candidate_profile']={'sha256':hashlib.sha256(profile_output.read_bytes()).hexdigest(),'bytes':profile_output.stat().st_size}
    data['size_pass']=data['binary_bytes'][selected]<=data['binary_bytes']['old']
    data['non_regression_pass']=all(v['summary']['ci95'][1]<=.03 for v in data['comparisons'].values())
    mains=['old-menu-bulk','normal-menu-bulk','old-workflow-default','normal-workflow-default'] if app=='srg' else ['old-verify-current','normal-verify-current','old-verify-prior','normal-verify-prior']
    data['main_improvement_pass']=all(data['comparisons'][key]['summary']['ci95'][1]<0 and data['comparisons'][key]['summary']['median_change']<=-.01 for key in mains)
    data['every_measured_feature_faster']=all(v['summary']['ci95'][1]<0 for v in data['comparisons'].values())
    data['accepted']=data['size_pass'] and data['non_regression_pass'] and data['main_improvement_pass']
    data['limitations']=['All inputs cannot be exhaustively timed; throughput depends on CPU, disk, entropy and network.',
                         'Time-observe contains deliberate wait; no universal latency improvement claimed.',
                         'Live successful OS click/F5 delivery not included in PGO training or timed comparison.'] if app=='srg' else ['Captured HTTP replay isolates computation; real service latency is not a PGO speed claim.']
    # Ship no replay library: pack via the project's authoritative example using a separate clean binary directory.
    package_td=work/'package-target';clean_exe=package_td/TARGET/'release'/(app+EXT);clean_exe.parent.mkdir(parents=True,exist_ok=True)
    shutil.copy2(exes[selected],clean_exe)
    penv=dict(os.environ,CARGO_TARGET_DIR=str(package_td/TARGET))
    cmd(['cargo','+1.99.0','run','--frozen','--example','package_artifact','--',app+'-'+TARGET],folder,penv)
    package=next((folder/'artifacts').glob(app+'-'+TARGET+'.*'));data['package_bytes']=package.stat().st_size
    shutil.copy2(package,OUT/package.name)
    print(app,'FINAL',json.dumps({k:data[k] for k in ['size_pass','non_regression_pass','main_improvement_pass','accepted','every_measured_feature_faster']}),flush=True);save()

def main():
    sysroot=Path(cmd(['rustc','+1.99.0','--print','sysroot']).stdout.decode().strip())
    host=next(l.split(': ',1)[1] for l in cmd(['rustc','+1.99.0','-Vv']).stdout.decode().splitlines() if l.startswith('host:'))
    llvm=sysroot/'lib/rustlib'/host/'bin'/('llvm-profdata'+EXT)
    for app,folder in [('fcupdater',ROOT),('srg',ROOT.parent/'srg')]:run_app(app,folder.resolve(),llvm)
    SERVER.shutdown();SERVER.server_close();save()

if __name__=='__main__':
    try:main()
    except BaseException as e:
        REPORT['error']=repr(e);save();raise
