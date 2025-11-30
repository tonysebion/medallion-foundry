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
silver_tmp = str(ROOT / "output" / "silver_tmp_simple")
os.makedirs(silver_tmp, exist_ok=True)
config = str(
    ROOT / "docs" / "examples" / "configs" / "patterns" / "pattern_current_history.yaml"
)
procs = []
for i in range(3):
    tag = f"tag{i}"
    cmd = [
        sys.executable,
        str(ROOT / "silver_extract.py"),
        "--config",
        config,
        "--bronze-path",
        bronze,
        "--silver-base",
        silver_tmp,
        "--write-parquet",
        "--artifact-writer",
        "transactional",
        "--chunk-tag",
        tag,
        "--use-locks",
        "--verbose",
        "--lock-timeout",
        "1",
    ]
    p = subprocess.Popen(
        cmd, cwd=ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
    )
    procs.append((tag, p))
    time.sleep(0.2)
results = []
for tag, p in procs:
    try:
        out, err = p.communicate(timeout=20)
    except subprocess.TimeoutExpired:
        p.kill()
        out, err = p.communicate()
        results.append((tag, p.returncode, out, err, "timeout"))
        continue
    results.append((tag, p.returncode, out, err, None))
with open("scripts/debug_concurrent_simple_results.txt", "w", encoding="utf-8") as f:
    for r in results:
        tag, rc, out, err, k = r
        f.write(f"TAG {tag} RC {rc} REASON {k}\n")
        f.write("STDOUT:\n")
        f.write(out[:500] + "\n")
        f.write("STDERR:\n")
        f.write(err[:500] + "\n")
        f.write("----\n")
