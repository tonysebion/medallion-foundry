import subprocess,sys,uuid,time
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1]
bronze=str(ROOT/'sampledata'/'bronze_samples'/'sample=pattern3_scd_state'/'system=retail_demo'/'table=orders'/'dt=2025-11-28')
silver_tmp=str(ROOT/'output'/'silver_tmp_test')
import os
os.makedirs(silver_tmp,exist_ok=True)
config=str(ROOT/'docs'/'examples'/'configs'/'patterns'/'pattern_current_history.yaml')
procs=[]
for i in range(3):
    tag=f'tag{i}'
    cmd=[sys.executable,str(ROOT/'silver_extract.py'),'--config',config,'--bronze-path',bronze,'--silver-base',silver_tmp,'--write-parquet','--artifact-writer','transactional','--chunk-tag',tag,'--use-locks','--verbose','--lock-timeout','10']
    p=subprocess.Popen(cmd,cwd=ROOT,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
    procs.append((tag,p))
    time.sleep(0.2)
for tag,p in procs:
    try:
        out,err=p.communicate(timeout=60)
    except subprocess.TimeoutExpired:
        p.kill()
        out,err=p.communicate()
    print('TAG',tag,'RC',p.returncode)
    print('STDOUT:',out[:2000])
    print('STDERR:',err[:2000])
