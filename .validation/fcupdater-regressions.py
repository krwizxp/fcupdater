import hashlib
import os
import re
import shutil
import subprocess
import tempfile
import time
from fractions import Fraction
from pathlib import Path
from zipfile import ZipFile
import xml.etree.ElementTree as ET

APP = Path(os.environ["FCUPDATER_RELEASE_BINARY"]).resolve()
MASTER = Path("fuel_cost_chungcheong.xlsx").resolve()
NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
FORMULA_11 = 'IF(B4+B5=0,"",(B4*B7+B5*B8)/(B4+B5))'
FORMULA_12 = 'IF(B4+B5=0,"",(B4*B7+B5*B9)/(B4+B5))'

def fingerprint(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def invoke(workdir, *args, timeout=250):
    begin = time.perf_counter()
    proc = subprocess.run([str(APP), *args], cwd=workdir, capture_output=True, text=True, errors="replace", timeout=timeout)
    duration = time.perf_counter() - begin
    print("CLI",args,"exit",proc.returncode,"seconds",round(duration,3))
    if proc.returncode:
        print("stderr",proc.stderr[-1100:])
    return proc, duration

def inspect(path, expected=None):
    with ZipFile(path) as z:
        assert z.testzip() is None, "ZIP CRC error"
        shared = ET.fromstring(z.read("xl/sharedStrings.xml"))
        strings = ["".join(t.text or "" for t in item.iter(NS+"t")) for item in shared.findall(NS+"si")]
        root = ET.fromstring(z.read("xl/worksheets/sheet1.xml"))
        assert "xl/worksheets/sheet2.xml" in z.namelist()
        assert "xl/styles.xml" in z.namelist()
        cells = {cell.get("r"):cell for cell in root.findall(".//"+NS+"c")}
        def text_at(pos):
            cell=cells.get(pos)
            if cell is None:return None
            node=cell.find(NS+"v")
            if node is None:return None
            value=node.text
            if cell.get("t") == "s":
                return strings[int(value)]
            return value
        def formula_at(pos):
            cell=cells.get(pos)
            node=cell.find(NS+"f") if cell is not None else None
            return None if node is None else node.text
        assert formula_at("B11")==FORMULA_11,(formula_at("B11"),FORMULA_11)
        assert formula_at("B12")==FORMULA_12,(formula_at("B12"),FORMULA_12)
        assert len(root.findall(".//"+NS+"row"))>=100
        matches=[]
        for row in root.findall(".//"+NS+"row"):
            rowcells={c.get("r","").rstrip("0123456789"):c for c in row.findall(NS+"c")}
            def val(col):
                cell=rowcells.get(col)
                if cell is None:return None
                v=cell.find(NS+"v")
                if v is None:return None
                return strings[int(v.text)] if cell.get("t")=="s" else v.text
            if val("C")=="한솔주유소" and val("B")=="대전":
                matches.append((val("F"),val("Q")))
        assert len(matches)==1 and matches[0][1]=="Y",matches
        gasoline=int(float(text_at("B4")))
        premium=int(float(text_at("B5")))
        total=gasoline+premium
        octave=[text_at("B11"),text_at("B12")]
        octane=[int(float(text_at(x))) for x in ("B7","B8","B9")]
        if not total:
            assert octave==[None,None],octave
        else:
            for raw,weight in zip(octave,octane[1:]):
                assert raw is not None
                expected_ratio=float(Fraction(gasoline*octane[0]+premium*weight,total))
                assert abs(float(raw)-expected_ratio)<1e-9,(float(raw),expected_ratio)
        if expected is not None:
            assert (gasoline,premium)==expected,((gasoline,premium),expected)
        print("Workbook",path.stat().st_size,"bytes","octane caches",octave,"Hansol",matches)
        return {"rows":len(root.findall(".//"+NS+"row")), "octane":octave,"station":matches[0]}

def change_controls(source,destination,controls=None,invalid_formula=False):
    with ZipFile(source) as src,ZipFile(destination,"w") as dst:
        for info in src.infolist():
            data=src.read(info.filename)
            if info.filename=="xl/worksheets/sheet1.xml":
                if controls:
                    for coord,value in controls.items():
                        pattern=rb'(<c r="'+coord.encode()+rb'"[^>]*><v>)([^<]+)(</v></c>)'
                        data,n=re.subn(pattern,lambda m:m.group(1)+str(value).encode()+m.group(3),data)
                        assert n==1,coord
                if invalid_formula:
                    old=FORMULA_12.encode()
                    new=b'IF(B4+B5=0,"",(B4*B7+B5*B8)/(B4+B5))'
                    assert data.count(old)==1
                    data=data.replace(old,new)
            dst.writestr(info,data)

def valid_scenario(name,expected,controls=None):
    with tempfile.TemporaryDirectory(prefix="fcupdater-"+name+"-") as tmp:
        work=Path(tmp)
        target=work/MASTER.name
        if controls:
            change_controls(MASTER,target,controls)
        else:
            shutil.copy2(MASTER,target)
        before=fingerprint(target)
        result,duration=invoke(work,"--verify")
        assert result.returncode==0,(name,result.stderr[-1000:])
        assert "Opinet" in result.stdout and "마스터 파일" in result.stdout,result.stdout[-1400:]
        inspect(target,expected)
        assert fingerprint(target)!=before,(name,"output not refreshed")
        assert not list(work.glob("*.tmp")),(name,"stale output temp")
        return duration

def rejection_scenario(name,mode,expected_fragment):
    with tempfile.TemporaryDirectory(prefix="fcupdater-"+name+"-") as tmp:
        work=Path(tmp)
        target=work/MASTER.name
        if mode=="missing":
            pass
        elif mode=="invalid_zip":
            target.write_bytes(b"not a valid xlsx container")
        elif mode=="invalid_formula":
            change_controls(MASTER,target,invalid_formula=True)
        elif mode=="symlink":
            real=work/"backing.xlsx"
            shutil.copy2(MASTER,real)
            target.symlink_to(real)
        before=fingerprint(target) if target.exists() else None
        result,_=invoke(work,"--verify")
        assert result.returncode!=0,(name,"unexpected acceptance")
        assert expected_fragment in (result.stdout+result.stderr),(name,result.stdout[-400:],result.stderr[-400:])
        assert before==(fingerprint(target) if target.exists() else None),(name,"rejected input modified")

def main():
    assert APP.is_file()
    print("Release binary",APP,APP.stat().st_size,"bytes")
    with tempfile.TemporaryDirectory(prefix="fcupdater-cli-") as tmp:
        for args,okay,marker in [
            (("--help",),True,"--verify"),
            (("--version",),True,"fcupdater 1.1.0"),
            (("--unknown-option",),False,"알 수 없는 옵션"),
            (("--verify","--version"),False,"알 수 없는 옵션")
        ]:
            result,_=invoke(tmp,*args)
            assert (result.returncode==0)==okay,(args,result.stderr)
            assert marker in result.stdout+result.stderr,(args,result.stdout,result.stderr)
        assert not (Path(tmp)/MASTER.name).exists()
    inspect(MASTER,(25,20))
    duration = valid_scenario("default",(25,20))
    print("Single live Opinet success path (seconds):",round(duration,3))

if __name__=="__main__":
    main()
