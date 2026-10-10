"""Task-only Windows orchestration; all timed operations use patterns.py unchanged."""
import os, json, shutil, hashlib, platform
from pathlib import Path
import patterns as p

FOLDER = p.ROOT.parent/'srg'
PREPARED = p.TASK/'prepared'

def prepared():
    p.REPORT = json.loads((PREPARED/'results.json').read_text(encoding='utf-8'))
    p.REPORT['training_run'] = p.REPORT['run']
    p.REPORT['run'] = os.environ.get('GITHUB_RUN_ID')
    data = p.REPORT['apps']['srg']
    for name, digest in data['prepared_sha256'].items():
        assert hashlib.sha256((PREPARED/name).read_bytes()).hexdigest() == digest, name
    selected = data['selected']
    exes = {}
    for name,file in [('normal','normal'),('old','old'),(selected,'candidate')]:
        dest=PREPARED/'runtime'/name/'release'/('srg'+p.EXT)
        dest.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(PREPARED/(file+p.EXT),dest);exes[name]=dest.resolve()
    return data, selected, exes, (PREPARED/('wrapper'+p.EXT)).resolve()

def measure(group):
    data, selected, exes, wrapper = prepared()
    data['comparisons'] = {}
    env = dict(os.environ)
    # Independent streams per job; fixed before the split run, never selected by timing.
    seed = 2026101001 + ['features','menu','full-0','full-1','full-2','full-3'].index(group)
    p.RNG.seed(seed)
    p.REPORT['measurement_group'] = group
    p.REPORT['measurement_seed'] = seed
    p.REPORT['environment'] = {'platform':platform.platform(), 'processor_identifier':os.environ.get('PROCESSOR_IDENTIFIER'), 'runner_image':os.environ.get('ImageVersion'), 'runner_arch':os.environ.get('RUNNER_ARCH')}
    if group.startswith('full-'):
        for a in ['old','normal']:
            labels = [a,selected];p.RNG.shuffle(labels);v={};checks=[]
            for label in labels:
                print('BEGIN full count',group,a,label,flush=True)
                elapsed,check=p.menu_bulk(exes[label],8145060,env,full_check=True)
                v[label]=elapsed;checks.append({'label':label,**check})
            raw=[[v[a],v[selected]]]
            data['comparisons'][a+'-menu-8145060']={'raw':raw,'records':checks}
            p.save()
        return
    cases = ['menu-bulk'] if group == 'menu' else [
        'actions-default','workflow-default','single','small','medium','integer',
        'float','ladder','manual','time','help','version','invalid']
    for case in cases:
        def sample(label):
            if case=='workflow-default':return p.workflow_cli(wrapper,exes[label],env,8)
            if case=='menu-bulk':return p.menu_bulk(exes[label],250000,env)[0],None
            if case=='manual':return p.manual_terminal(exes[label],env,16)
            return p.srg_cli(exes[label],case,env,4 if case!='time' else 1)
        n=16 if case in ['manual','time'] else 64
        for a,b in [('old',selected),('normal',selected)]:
            name=a+'-'+case;print('BEGIN',group,name,flush=True)
            data['comparisons'][name]=p.pairs(sample,a,b,n)
            print(name,data['comparisons'][name]['summary'],flush=True);p.save()
    if group=='features':
        data['final_cancellation']=p.menu_bulk(exes[selected],10000000,env,cancel=True)[1]
    p.save()

def combine():
    data,selected,exes,wrapper=prepared()
    data['comparisons']={};data['measurement_jobs']=[]
    reports=list((p.TASK/'parts').rglob('results.json'));assert len(reports)==6,len(reports)
    for path in reports:
        report=json.loads(path.read_text(encoding='utf-8'));assert 'error' not in report,report.get('error')
        other=report['apps']['srg'];assert other['prepared_sha256']==data['prepared_sha256']
        data['measurement_jobs'].append({'group':report['measurement_group'],'seed':report['measurement_seed'],'environment':report.get('environment')})
        for key,value in other['comparisons'].items():
            if key.endswith('-menu-8145060'):
                dest=data['comparisons'].setdefault(key,{'raw':[],'records':[]})
                dest['raw'].extend(value['raw']);dest['records'].extend(value['records'])
            else:
                assert key not in data['comparisons'];data['comparisons'][key]=value
        if 'final_cancellation' in other:data['final_cancellation']=other['final_cancellation']
    assert len(data['comparisons'])==30,len(data['comparisons'])
    for key,value in data['comparisons'].items():
        n=4 if key.endswith('-menu-8145060') else 16 if key.endswith(('-manual','-time')) else 64
        assert len(value['raw'])==n,(key,len(value['raw']),n)
        value['summary']=p.interval(value['raw'])
    profile=p.OUT/('srg-'+p.TARGET+'.profdata');shutil.copy2(PREPARED/'candidate.profdata',profile)
    data['candidate_profile']={'sha256':hashlib.sha256(profile.read_bytes()).hexdigest(),'bytes':profile.stat().st_size}
    data['size_pass']=data['binary_bytes'][selected]<=data['binary_bytes']['old']
    data['non_regression_pass']=all(v['summary']['ci95'][1]<=.03 for v in data['comparisons'].values())
    mains=['old-menu-bulk','normal-menu-bulk','old-workflow-default','normal-workflow-default']
    data['main_improvement_pass']=all(data['comparisons'][k]['summary']['ci95'][1]<0 and data['comparisons'][k]['summary']['median_change']<=-.01 for k in mains)
    data['every_measured_feature_faster']=all(v['summary']['ci95'][1]<0 for v in data['comparisons'].values())
    data['accepted']=data['size_pass'] and data['non_regression_pass'] and data['main_improvement_pass']
    data['limitations']=['Six native Windows jobs use byte-identical artifacts; each old/new pair runs on the same host; between-job hardware variation is not attributed to PGO.',
                        'Live successful OS click/F5 delivery is not trained or timed.',
                        'User physical Windows PC hardware is not measured.']
    work=p.OUT/'srg';work.mkdir(exist_ok=True)
    package_td=work/'package-target';clean_exe=package_td/p.TARGET/'release'/('srg'+p.EXT)
    clean_exe.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(exes[selected],clean_exe)
    penv=dict(os.environ,CARGO_TARGET_DIR=str(package_td/p.TARGET))
    p.cmd(['cargo','+1.99.0','run','--frozen','--example','package_artifact','--','srg-'+p.TARGET],FOLDER,penv)
    package=next((FOLDER/'artifacts').glob('srg-'+p.TARGET+'.*'));data['package_bytes']=package.stat().st_size
    shutil.copy2(package,p.OUT/package.name)
    p.save();print('FINAL',json.dumps({k:data[k] for k in ['size_pass','non_regression_pass','main_improvement_pass','accepted']}),flush=True)

if __name__=='__main__':
    try:
        phase=os.environ['PATTERN_PHASE']
        if phase=='train':
            os.environ['PATTERN_PREPARE_ONLY']='1'
            sysroot=Path(p.cmd(['rustc','+1.99.0','--print','sysroot']).stdout.decode().strip())
            llvm=sysroot/'lib/rustlib'/p.TARGET/'bin'/('llvm-profdata'+p.EXT)
            p.run_app('srg',FOLDER.resolve(),llvm)
        elif phase=='measure':measure(os.environ['PATTERN_GROUP'])
        elif phase=='combine':combine()
        else:raise ValueError(phase)
    except BaseException as exc:
        p.REPORT['error']=repr(exc);p.save();raise
    finally:
        p.SERVER.shutdown();p.SERVER.server_close()
