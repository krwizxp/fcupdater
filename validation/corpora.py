from pathlib import Path
import random,zipfile,re,json,datetime,calendar,zlib
r=Path(__file__).resolve().parent;c=r/'corpus';c.mkdir(exist_ok=True);rng=random.Random(20261009)
times=[f'{h:02}:{m:02}:{s:02}' for h in range(100) for m in range(100) for s in range(100)]
times+=['00:00','00:00:00:00','0:00:00','00:0:00','00:00:0','+0:00:00',' 0:00:00','０0:00:00','00:00:00 ','-0:00:00','ab:00:00','00::00','00:00:','00:00:60','24:00:00']
(c/'time.txt').write_text('\n'.join(times)+'\n')
(c/'time-valid.txt').write_text('\n'.join(times[h*10000+m*100+s] for h in range(24) for m in range(60) for s in range(60))+'\n')
(c/'time-invalid.txt').write_text('\n'.join(times[-15:])*200+'\n')
values=list(range(65536))
for b in range(0,64,3):
 for delta in [-1,0,1]:
  n=(1<<b)+delta
  if 0<=n<=2**64-1:values.append(n)
values += [2**64-1]+[rng.getrandbits(64) for _ in range(10000)]
(c/'octal.txt').write_text('\n'.join(map(str,values))+'\n')
for kind,vs in [('short',list(range(4096))),('wide',[rng.getrandbits(64) for _ in range(4096)])]:(c/f'output-{kind}.txt').write_text('\n'.join(map(str,vs))+'\n')
addresses={'localhost':True,'http://example.com':True,'https://example.com:443':True,'http://127.0.0.1:1':True,'[::1]:65535':True,'::1':True,'https://[::1]:443':True,' ':False,'http://':False,'host:0':False,'host:65536':False,'host:+80':False,'host:-1':False,'host/path':False,'host?x':False,'host#x':False,'user@host':False,'[invalid]':False,'[127.0.0.1]':False,'host:1:2':False,'[::1':False,'[::1]:':False}
(c/'address.txt').write_text('\n'.join(addresses)+'\n');(c/'address-expected.json').write_text(json.dumps(addresses))
dates={}
for y in [1970,1994,2000,2024,2026,2100]:
 for m,d in [(1,1),(2,28),(3,1),(12,31)]:
  dt=datetime.datetime(y,m,d,23,59,59,tzinfo=datetime.timezone.utc)
  dates[dt.strftime('%a, %d %b %Y %H:%M:%S GMT')]=int(dt.timestamp())
for s in ['Sun, 06 Nov 1994 08:49:37 GMT','Sunday, 06-Nov-94 08:49:37 GMT','Sun Nov  6 08:49:37 1994']:dates[s]=784111777
for s in ['Mon, 06 Nov 1994 08:49:37 GMT','Sun, 00 Nov 1994 08:49:37 GMT','Sun, 31 Feb 2026 08:49:37 GMT','Sun, 06 Nov 1994 24:49:37 GMT','Sun, 06 Nov 1994 08:60:37 GMT','Sun, 06 Nov 1994 08:49:60 GMT','Sun, 06 Nov 1994 08:49:37 UTC','bogus']:dates[s]=None
(c/'date.txt').write_text('\n'.join(dates)+'\n');(c/'date-expected.json').write_text(json.dumps(dates))
original=(r/'valid.xlsx').read_bytes();(c/'valid.xlsx').write_bytes(original)
def mutate(name,part,fn):
 with zipfile.ZipFile(c/'valid.xlsx') as zin,zipfile.ZipFile(c/f'{name}.xlsx','w',compression=zipfile.ZIP_DEFLATED,compresslevel=6) as zout:
  for info in zin.infolist():
   data=zin.read(info.filename)
   if info.filename==part:data=fn(data)
   zout.writestr(info,data)
for part in ['xl/worksheets/sheet1.xml','xl/worksheets/sheet2.xml']:
 for value in ['0','+1','1048577']:
  mutate('row-'+part[-5]+'-'+value.replace('+','plus'),part,lambda s,value=value:re.sub(rb'<row r="[0-9]+"',b'<row r="'+value.encode()+b'"',s,count=1))
mutate('hidden-master','xl/workbook.xml',lambda s:s.replace(b'sheetId="1"',b'state="hidden" sheetId="1"',1))
mutate('bad-xml','xl/worksheets/sheet1.xml',lambda s:s[:-19])
mutate('bad-shared-index','xl/worksheets/sheet1.xml',lambda s:re.sub(rb't="s"><v>[0-9]+</v>',b't="s"><v>9999999</v>',s,count=1))
mutate('workbook-body','xl/workbook.xml',lambda s:s.replace(b'<workbookPr/>',b'<workbookPr>bad</workbookPr>',1))
data=bytearray(original);offset=data.index(b'PK\x01\x02');data[offset+34:offset+36]=(1).to_bytes(2,'little');(c/'split-zip.xlsx').write_bytes(data)
data=bytearray(original);data[offset]=0;(c/'bad-central.xlsx').write_bytes(data)
(c/'truncated.xlsx').write_bytes(original[:-20]);(c/'not-zip.xlsx').write_bytes(b'not an XLSX')
print('Prepared time 1,000,015; octal',len(values),'address',len(addresses),'date',len(dates),'XLSX',len(list(c.glob('*.xlsx'))))
