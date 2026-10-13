"""Observe strict WinHTTP certificate failures without ignoring TLS validation."""
import ctypes
from ctypes import wintypes as W
import json
import os
from pathlib import Path
import re
import sys

if os.name != 'nt': sys.exit(0)
headers = sorted(Path(os.environ['ProgramFiles(x86)']).glob('Windows Kits/10/Include/*/um/winhttp.h'))
assert headers, 'Windows SDK winhttp.h not found'
header = headers[-1].read_text(errors='replace')
def constant(name):
    match = re.search(r'^\s*#define\s+'+name+r'\s+\(?\s*(0x[0-9A-Fa-f]+|\d+)',header,re.M)
    assert match,name
    return int(match[1],0)
api = ctypes.WinDLL('winhttp',use_last_error=True)
P = ctypes.c_void_p
for name, restype, argtypes in [
    ('WinHttpOpen',P,[W.LPCWSTR,W.DWORD,W.LPCWSTR,W.LPCWSTR,W.DWORD]),
    ('WinHttpConnect',P,[P,W.LPCWSTR,W.WORD,W.DWORD]),
    ('WinHttpOpenRequest',P,[P,W.LPCWSTR,W.LPCWSTR,W.LPCWSTR,W.LPCWSTR,P,W.DWORD]),
    ('WinHttpSetOption',W.BOOL,[P,W.DWORD,P,W.DWORD]),
    ('WinHttpSetTimeouts',W.BOOL,[P,ctypes.c_int,ctypes.c_int,ctypes.c_int,ctypes.c_int]),
    ('WinHttpSetStatusCallback',P,[P,P,W.DWORD,ctypes.c_size_t]),
    ('WinHttpSendRequest',W.BOOL,[P,W.LPCWSTR,W.DWORD,P,W.DWORD,W.DWORD,ctypes.c_size_t]),
    ('WinHttpReceiveResponse',W.BOOL,[P,P]),
    ('WinHttpCloseHandle',W.BOOL,[P])]:
    function = getattr(api,name);function.restype=restype;function.argtypes=argtypes
failure = constant('WINHTTP_CALLBACK_STATUS_SECURE_FAILURE')
flag_names = ['CERT_REV_FAILED','INVALID_CERT','CERT_REVOKED','INVALID_CA','CERT_CN_INVALID','CERT_DATE_INVALID','SECURITY_CHANNEL_ERROR']
flags = {name:constant('WINHTTP_CALLBACK_STATUS_FLAG_'+name) for name in flag_names}
records = []
for host in ['nfl.opinet.co.kr','www.opinet.co.kr']:
    events = []
    callback_type=ctypes.WINFUNCTYPE(None,P,ctypes.c_size_t,W.DWORD,P,W.DWORD)
    @callback_type
    def callback(handle,context,status,information,length):
        if status==failure and information and length==ctypes.sizeof(W.DWORD):
            value=ctypes.cast(information,ctypes.POINTER(W.DWORD)).contents.value
            events.append({'value':value,'flags':[name for name,flag in flags.items() if value&flag]})
    handles=[]
    try:
        session=api.WinHttpOpen('optimization-validation',4,None,None,0);assert session;handles.append(session)
        assert api.WinHttpSetTimeouts(session,30000,30000,30000,30000)
        protocols=W.DWORD(0x800|0x2000)
        assert api.WinHttpSetOption(session,84,ctypes.byref(protocols),ctypes.sizeof(protocols))
        connection=api.WinHttpConnect(session,host,443,0);assert connection;handles.append(connection)
        request=api.WinHttpOpenRequest(connection,'HEAD','/',None,None,None,0x800000);assert request;handles.append(request)
        revocation=W.DWORD(1)
        assert api.WinHttpSetOption(request,79,ctypes.byref(revocation),ctypes.sizeof(revocation))
        previous=api.WinHttpSetStatusCallback(request,ctypes.cast(callback,P),failure,0)
        assert previous!=ctypes.c_void_p(-1).value
        sent=api.WinHttpSendRequest(request,None,0,None,0,0,1)
        error=ctypes.get_last_error() if not sent else 0
        received=api.WinHttpReceiveResponse(request,None) if sent else False
        if sent and not received:error=ctypes.get_last_error()
        records.append({'host':host,'sent':bool(sent),'received':bool(received),'error':error,'secure_failure_events':events})
    finally:
        for handle in reversed(handles):api.WinHttpCloseHandle(handle)
output=Path('optimization-native-results');output.mkdir(exist_ok=True)
(output/'windows-tls.json').write_text(json.dumps({'sdk':str(headers[-1]),'records':records},indent=2))
print(json.dumps(records))
