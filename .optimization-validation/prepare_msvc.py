"""Load the supported ARM64-host/x64-target MSVC developer environment."""
import os
from pathlib import Path
import subprocess


def environment():
    installer = Path(os.environ['ProgramFiles(x86)'])/'Microsoft Visual Studio/Installer/vswhere.exe'
    result = subprocess.run([installer,'-latest','-products','*','-property','installationPath'],capture_output=True,text=True,check=True)
    vcvars = Path(result.stdout.strip())/'VC/Auxiliary/Build/vcvarsall.bat'
    assert vcvars.is_file(),vcvars
    failures = []
    for architecture in ['arm64_amd64','amd64','x64']:
        command = 'call "'+str(vcvars)+'" '+architecture+' >nul && set'
        prepared = subprocess.run(['cmd.exe','/s','/c',command],capture_output=True,text=True)
        if prepared.returncode:
            failures.append(architecture+': '+prepared.stderr+' '+prepared.stdout)
            continue
        values = {key.upper():value for line in prepared.stdout.splitlines() if '=' in line and not line.startswith('=') for key,value in [line.split('=',1)]}
        assert values.get('VSCMD_ARG_TGT_ARCH') in ['x64','amd64'], 'MSVC environment is not x64'
        assert values.get('LIB') and values.get('PATH')
        break
    else:
        raise RuntimeError('MSVC environment setup failed: '+'; '.join(failures))
    keys = ['PATH','LIB','INCLUDE','LIBPATH','VSCMD_ARG_TGT_ARCH','VCINSTALLDIR','VCToolsInstallDir','WindowsSdkDir','WindowsSDKVersion']
    return {key:values[key] for key in keys if key in values}
