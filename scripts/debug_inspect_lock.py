import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
bronze = str(
    ROOT
    / "sampledata"
    / "bronze_samples"
    / "sample=pattern3_scd_state"
    / "system=retail_demo"
    / "table=orders"
    / "dt=2025-11-28"
)
silver_tmp = str(ROOT / "output" / "silver_tmp_debug2")
os.makedirs(silver_tmp, exist_ok=True)
config = str(ROOT / "docs" / "examples" / "configs" / "patterns" / "pattern_current_history.yaml")
procs=[]
for i in range(3):
    tag=f'tag{i}'
    cmd=[sys.executable,str(ROOT/'silver_extract.py'),'--config',config,'--bronze-path',bronze,'--silver-base',silver_tmp,'--write-parquet','--artifact-writer','transactional','--chunk-tag',tag,'--use-locks','--verbose','--lock-timeout','5']
    p=subprocess.Popen(cmd,cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    procs.append((tag,p))
    time.sleep(0.2)
# Monitor lock file and processes
lock_path=Path(silver_tmp)/'domain=retail_demo'/'entity=orders'/'v1'/'load_date=2025-11-28'/'.silver.lock'
print('Lock path',lock_path)
for i in range(60):
    msg=[]
    if lock_path.exists():
        try:
            text=lock_path.read_text()
        except Exception as e:
            text=str(e)
        msg.append('LOCK:'+text.strip())
    else:
        msg.append('NO LOCK')
    procs_state=[(tag,p.poll()) for tag,p in procs]
    msg.append(str(procs_state))
    print(i, ' '.join(msg))
    if all(p.poll() is not None for _,p in procs):
        break
    time.sleep(1)
for tag,p in procs:
    try:
        out,err=p.communicate(timeout=20)
    except Exception:
        p.kill()
        out,err=p.communicate()
    print('TAG',tag,'RC',p.returncode)
    print('STDOUT:',out[:1000])
    print('STDERR:',err[:1000])
