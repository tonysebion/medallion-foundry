"""Local filesystem storage backend."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any, Dict, List

from core.storage.backend import StorageBackend

logger = logging.getLogger(__name__)


class LocalStorage(StorageBackend):
    """Simple filesystem backend used for tests and local samples."""

    def __init__(self, config: Dict[str, Any]) -> None:
        bronze_cfg = config.get("bronze", {})
        root = bronze_cfg.get("local_root")
        self.base_dir = Path(root).expanduser().resolve() if root else Path(".").resolve()

    def _resolve_path(self, remote_path: str) -> Path:
        candidate = Path(remote_path)
        if not candidate.is_absolute():
            candidate = self.base_dir / candidate
        return candidate.resolve()

    def upload_file(self, local_path: str, remote_path: str) -> bool:
        dest = self._resolve_path(remote_path)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(local_path, dest)
        return True

    def download_file(self, remote_path: str, local_path: str) -> bool:
        src = self._resolve_path(remote_path)
        if not src.exists():
            raise FileNotFoundError(f"Local file {src} not found")
        Path(local_path).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(src, local_path)
        return True

    def list_files(self, prefix: str) -> List[str]:
        resolved = self._resolve_path(prefix)
        if resolved.is_file():
            return [str(resolved)]
        files: List[str] = []
        if resolved.exists():
            for path in resolved.rglob("*"):
                if path.is_file():
                    files.append(str(path))
        return files

    def delete_file(self, remote_path: str) -> bool:
        target = self._resolve_path(remote_path)
        if target.exists():
            if target.is_file():
                target.unlink()
                return True
            shutil.rmtree(target)
            return True
        return False

    def get_backend_type(self) -> str:
        return "local"
