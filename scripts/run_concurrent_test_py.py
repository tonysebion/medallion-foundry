import shutil
import subprocess
import sys
import time
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
bronze = ROOT/'sampledata'/'bronze_samples'/'sample=pattern3_scd_state'/'system=retail_demo'/'table=orders'/'dt=2025-11-28'
config = ROOT/'docs'/'examples'/'configs'/'patterns'/'pattern_current_history.yaml'
silver_tmp = ROOT/'output'/'silver_tmp_run_test'
if silver_tmp.exists():
    shutil.rmtree(silver_tmp)
silver_tmp.mkdir(parents=True, exist_ok=True)

# clean stale locks
for f in silver_tmp.rglob('.silver.lock'):
    try:
        f.unlink()
    except Exception:
        pass

# spawn processes
procs=[]
tags=[f'lock-{uuid.uuid4().hex[:6]}' for _ in range(3)]
for t in tags:
    stdout_path = silver_tmp / f"{t}.out"
    stderr_path = silver_tmp / f"{t}.err"
    cmd=[sys.executable, str(ROOT/'silver_extract.py'), '--config', str(config), '--bronze-path', str(bronze), '--silver-base', str(silver_tmp), '--write-parquet', '--artifact-writer', 'transactional', '--chunk-tag', t, '--use-locks', '--lock-timeout', '10', '--verbose']
    p = subprocess.Popen(cmd, cwd=ROOT, stdout=open(stdout_path, 'w', encoding='utf-8'), stderr=open(stderr_path, 'w', encoding='utf-8'), text=True)
    procs.append((t, p))
    time.sleep(0.2)

# wait
# Wait for processes to complete, with periodic diagnostics
start = time.time()
timeout = 120
while True:
    states = [(t, p.poll()) for t, p in procs]
    print('States:', states)
    if all(s is not None for _, s in states):
        break
    if time.time() - start > timeout:
        print('Timeout reached; killing remaining processes')
        for t, p in procs:
            if p.poll() is None:
                p.kill()
        break
    time.sleep(1)

# read outputs
for t,p in procs:
    out = ''
    err = ''
    stdout_path = silver_tmp / f"{t}.out"
    stderr_path = silver_tmp / f"{t}.err"
    if stdout_path.exists():
        out = stdout_path.read_text(encoding='utf-8')
    if stderr_path.exists():
        err = stderr_path.read_text(encoding='utf-8')
    print('TAG', t, 'RC', p.returncode)
    print('OUT', out[:1000])
    print('ERR', err[:1000])

# consolidate
subprocess.run([sys.executable, str(ROOT/'scripts'/'silver_consolidate.py'), '--silver-base', str(silver_tmp)], check=True, cwd=ROOT)

print('Done')
