from pathlib import Path
import json,random,re,struct
from openpyxl.utils.cell import column_index_from_string,get_column_letter
ROOT=Path(__file__).resolve().parent
rng=random.Random(202610110145)
cases=[]
for col in range(1,16385):
 name=get_column_letter(col)
 for col_lock,row_lock in [('',''),('$',''),('','$'),('$','$')]:
  row=rng.choice([1,2,15,1048575,1048576])
  cases.append(col_lock+(name.lower() if col%3==0 else name)+row_lock+str(row))
edges=['',' ','$','A','A0','A0001','A1048576','A1048577','XFD1','XFE1','ZZZ1','AAAA1','@1','[1','a1','A$1','$A$1','$$A1','A$$1','A+1','A-1','A1.0','A1e0','A１','Å1','A1 ',' A1','A1\n','A\n1','A1\x00','A0'*4]
cases+=edges
for col in ['A','Z','AA','XFD','XFE','ZZZ','AAAA']:
 for row in ['','0','00','0001','1048575','1048576','1048577','4294967295','4294967296','9'*128,'0'*1024+'1','0'*1024]:
  for lock in ['','$']:cases.append(col+lock+row)
alphabet='ABCxyz$0123456789+-._ \n\r\tÅ'
for _ in range(8192):
 cases.append(''.join(rng.choice(alphabet) for _ in range(rng.randrange(18))))
pattern=re.compile(r'(\$?)([A-Za-z]{1,3})(\$?)([0-9]+)')
def expected(text):
 m=pattern.fullmatch(text)
 if m is None:return None
 col=column_index_from_string(m[2]);row=int(m[4])
 if not (1<=col<=16384 and 1<=row<=1048576):return None
 return col,row,bool(m[1]),bool(m[3])
def encoded(items):
 return b''.join(struct.pack('<I',len(x.encode()))+x.encode() for x in items)
(ROOT/'oracle-input.bin').write_bytes(encoded(cases))
(ROOT/'oracle-expected.bin').write_bytes(b''.join(b'\0'*11 if expected(s) is None else b'\1'+struct.pack('<IIBB',*expected(s)) for s in cases))
kernel=[cases[rng.randrange(65536)] for _ in range(768)]
kernel += [cases[65536+rng.randrange(len(cases)-65536)] for _ in range(256)]
(ROOT/'kernel-input.bin').write_bytes(encoded(kernel))
(ROOT/'corpus.json').write_text(json.dumps({'seed':202610110145,'oracle_cases':len(cases),'kernel_cases':1024,'kernel_passes':1024,
 'oracle':'independent ASCII regex grammar, openpyxl column index lookup, arbitrary precision Python row/lock fields',
 'valid_column_coverage':16384,'lock_combinations':4,'edges':edges},ensure_ascii=False,indent=2)+'\n')
template=r'''
use std::{io::Write,hint::black_box};
const MAX_A1_COLUMN_LETTERS: usize = 3;
const MAX_A1_COL: u32 = 0x4000;
const MAX_A1_ROW: u32 = 0x0010_0000;
struct CellReference { col:u32, col_locked:bool, row:u32, row_locked:bool }
BODY
fn main() {
 let args:Vec<String>=std::env::args().collect();
 let raw=std::fs::read(&args[2]).unwrap();let mut pos=0;let mut texts=Vec::new();
 while pos<raw.len(){let n=u32::from_le_bytes(raw[pos..pos+4].try_into().unwrap()) as usize;pos+=4;texts.push(std::str::from_utf8(&raw[pos..pos+n]).unwrap());pos+=n;}
 if args[1]=="check" {
  let mut out=std::io::stdout().lock();
  for text in texts {match parse_ref_with_locks(text) {
   Some(v)=>{out.write_all(&[1]).unwrap();out.write_all(&v.col.to_le_bytes()).unwrap();out.write_all(&v.row.to_le_bytes()).unwrap();out.write_all(&[u8::from(v.col_locked),u8::from(v.row_locked)]).unwrap();},
   None=>out.write_all(&[0;11]).unwrap()}}
 } else {
  let t=std::time::Instant::now();let mut sink=0_u64;
  for _ in 0..1024 {for text in &texts {let v=parse_ref_with_locks(black_box(text));sink=sink.wrapping_add(v.map_or(1,|v|u64::from(v.col)+u64::from(v.row)+u64::from(v.col_locked)+u64::from(v.row_locked)));}}
  println!("{},{}",t.elapsed().as_secs_f64(),black_box(sink));
 }
}
'''
for label in ['baseline','candidate']:
 source=(ROOT/f'fc-{label}/src/excel/writer/cell_ref.rs').read_text()
 start=source.index('pub(super) fn parse_ref_with_locks')
 end=source.index('pub(super) fn with_unlocked_ref_parts',start)
 body=source[start:end].replace('pub(super) fn','fn',1)
 (ROOT/f'i32-{label}.rs').write_text(template.replace('BODY',body))
print('independent A1 oracle cases',len(cases))
