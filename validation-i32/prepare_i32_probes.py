from pathlib import Path
import json, random, decimal, re, struct
ROOT=Path(__file__).resolve().parent
rng=random.Random(202610110015)
edges=['',' ','+', '-', '.', ',', ',,', '+,.5','-.5','.49','.5','.50','.4999','-.4999','1,2,3.4','1,,2','1,','1.','1.0','2147483647','2147483647.4999','2147483647.5','-2147483648','-2147483648.4999','-2147483648.5','9223372036854775807','9223372036854775807.5','-9223372036854775808','000001.50000','1e3','NaN','Infinity','1.2.3','1.2,3','--1','+-1','１２３','١٢٣','1 2','\x00','1\n2','1\r2']
cases=list(edges)
for space in ['\t','\r','\n','\u0085','\u00a0','\u1680','\u2000','\u2028','\u2029','\u202f','\u205f','\u3000']:
 for value in ['123.5','-2147483648.49','',',.5']:cases.append(space+value+space)
for e in range(65):
 for delta in range(-3,4):
  value=max(0,(1<<e)+delta)
  for sign in ['','-']:
   for suffix in ['','.49','.5','.500000000000000000001']:
    cases.append(sign+str(value)+suffix)
for _ in range(8192):
 n=rng.randrange(-4000000000,4000000000)
 value=str(n) if rng.randrange(2) else f'{n:,}'
 if rng.randrange(3):value+='.'+str(rng.randrange(1000000)).zfill(6)
 if rng.randrange(4)==0:value=value[:rng.randrange(len(value)+1)]+rng.choice(['+',',','.','x',' ','\u00a0'])+value[rng.randrange(len(value)+1):]
 cases.append(value)
cases += ['0'*4096+'1.5','9'*4096, '-'+'0'*4096+'2147483648.4999', '0.'+'0'*4096+'9', '1.'+'9'*4096]
decimal.getcontext().prec=8192
WHITE='\t\n\v\f\r \u0085\u00a0\u1680' + ''.join(chr(c) for c in range(0x2000,0x200b)) + '\u2028\u2029\u202f\u205f\u3000'
def expected(text):
 s=text.strip(WHITE)
 if not re.fullmatch(r'[+-]?[0-9,]*(?:\.[0-9]*)?',s) or not re.search('[0-9]',s):return None
 value=decimal.Decimal(s.replace(',','')).to_integral_value(rounding=decimal.ROUND_HALF_UP)
 return int(value) if -2147483648<=value<=2147483647 else None
def encoded(items):
 return b''.join(struct.pack('<I',len(x.encode()))+x.encode() for x in items)
(ROOT/'oracle-input.bin').write_bytes(encoded(cases))
(ROOT/'oracle-expected.bin').write_bytes(b''.join(b'\0'*5 if expected(s) is None else b'\1'+struct.pack('<i',expected(s)) for s in cases))
kernel=[str(rng.randrange(0,3000))+rng.choice(['','.0','.5',',','.49']) for _ in range(512)]
kernel += [cases[rng.randrange(len(cases)-5)] for _ in range(512)]
(ROOT/'kernel-input.bin').write_bytes(encoded(kernel))
(ROOT/'corpus.json').write_text(json.dumps({'seed':202610110015,'oracle_cases':len(cases),'kernel_cases':len(kernel),'kernel_passes':1024,'oracle':'independent ASCII grammar + Python Decimal ROUND_HALF_UP; Rust Unicode White_Space trim explicitly enumerated','edges':edges},ensure_ascii=False,indent=2)+'\n')
template=r'''
use std::{io::Write, hint::black_box};
fn parse_input(text: &str) -> Option<i32> {
BODY
}
fn main() {
 let args:Vec<String>=std::env::args().collect();
 let raw=std::fs::read(&args[2]).unwrap();let mut pos=0;let mut texts=Vec::new();
 while pos<raw.len() {let n=u32::from_le_bytes(raw[pos..pos+4].try_into().unwrap()) as usize;pos+=4;texts.push(std::str::from_utf8(&raw[pos..pos+n]).unwrap());pos+=n;}
 if args[1]=="check" {
  let mut out=std::io::stdout().lock();
  for text in texts {match parse_input(text) {Some(v)=> {out.write_all(&[1]).unwrap();out.write_all(&v.to_le_bytes()).unwrap();},None=>out.write_all(&[0;5]).unwrap()}}
 } else {
  let t=std::time::Instant::now();let mut sink=0_u64;
  for _ in 0..1024 {for text in &texts {let v=parse_input(black_box(text));sink=sink.wrapping_add(v.map_or(1,|v|u64::from(v.unsigned_abs())));}}
  println!("{},{}",t.elapsed().as_secs_f64(),black_box(sink));
 }
}
'''
for label in ['baseline','candidate']:
 source=(ROOT/f'fc-{label}/src/excel/writer.rs').read_text()
 start=source.index('        let trimmed = text.trim();',source.index('pub(crate) fn get_i32_at'))
 end=source.index('    fn get_or_create_cell_mut',start)
 body=source[start:end].rsplit('\n    }',1)[0].replace('return Ok(None)','return None')
 lines=body.splitlines();last=lines[-1].strip();assert last.startswith('Ok(') and last.endswith(')')
 lines[-1]='        '+last[3:-1];body='\n'.join(lines)
 (ROOT/f'i32-{label}.rs').write_text(template.replace('BODY',body))
print('oracle cases',len(cases))
