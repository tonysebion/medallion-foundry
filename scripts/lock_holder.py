import sys, time
from pathlib import Path
PROJECT_ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT_ROOT))
from core.storage.locks import file_lock

silver_partition = Path(sys.argv[1])
name = sys.argv[2] if len(sys.argv) > 2 else 'holder'
with file_lock(silver_partition, timeout=10):
    print(f"{name} acquired lock")
    time.sleep(5)
    print(f"{name} releasing lock")
