"""Load the supported ARM64-host/x64-target MSVC developer environment."""
import os
from pathlib import Path
import subprocess


def environment():
    installer = Path(os.environ['ProgramFiles(x86)'])/'Microsoft Visual Studio/Installer/vswhere.exe'
    result = subprocess.run([installer,'-latest','-products','*','-property','installationPath'],capture_output=True,text=True,check=True)
    vcvars = Path(result.stdout.strip())/'VC/Auxiliary/Build/vcvarsall.bat'
    assert vcvars.is_file(),vcvars
    command = 'call "'+str(vcvars)+'" arm64_x64 >nul && set'
    prepared = subprocess.run(['cmd.exe','/s','/c',command],capture_output=True,text=True,check=True)
    values = {key.upper():value for line in prepared.stdout.splitlines() if '=' in line and not line.startswith('=') for key,value in [line.split('=',1)]}
    keys = ['PATH','LIB','INCLUDE','LIBPATH','VSCMD_ARG_TGT_ARCH','VCINSTALLDIR','VCToolsInstallDir','WindowsSdkDir','WindowsSDKVersion']
    return {key:values[key] for key in keys if key in values}
