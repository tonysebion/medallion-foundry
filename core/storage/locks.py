"""Simple cross-platform file lock for local filesystem operations.

This utility implements a minimal advisory lock based on creating a lockfile
with O_EXCL semantics. It is intended for local filesystem coordination and
is not a distributed lock for cloud object stores.
"""
from __future__ import annotations

import os
import time
import errno
import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional


class LockAcquireError(Exception):
    pass


@contextmanager
def file_lock(dir_path: Path, lock_name: str = ".silver.lock", timeout: float = 30.0, poll_interval: float = 0.2) -> Iterator[None]:
    """Context manager for a lock on a directory.

    Args:
        dir_path: Directory to place the lock file in.
        lock_name: File name of the lock file (defaults to '.silver.lock').
        timeout: Maximum seconds to wait for a lock before raising.
        poll_interval: Poll interval while waiting.
    """
    lock_path = dir_path / lock_name
    start = time.time()
    fd = None
    try:
        while True:
            try:
                # Use os.O_CREAT | os.O_EXCL to create atomically
                fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                # Write pid to help debugging
                with os.fdopen(fd, "w") as f:
                    f.write(str(os.getpid()))
                fd = None
                logging.getLogger(__name__).debug("Acquired lock %s by pid %s", lock_path, os.getpid())
                break
            except FileExistsError:
                # If the existing lock file looks stale (pid not running), remove it and retry
                try:
                    text = lock_path.read_text(encoding="utf-8")
                    pid = int(text.strip())
                except Exception:
                    # Lock file has invalid contents -> treat as stale and remove
                    pid = None
                    try:
                        lock_path.unlink()
                    except Exception:
                        pass
                    continue
                if pid is not None:
                    try:
                        os.kill(pid, 0)
                        # Process exists, so wait
                    except OSError as exc:
                        # Only treat the lock as stale if the underlying error indicates
                        # that the process does not exist (ESRCH). Other errors such as
                        # EPERM (permission denied) or platform specific errors should be
                        # treated as 'process exists' to avoid incorrectly removing the
                        # lock while the owning process is still running.
                        if getattr(exc, "errno", None) == errno.ESRCH:
                            try:
                                lock_path.unlink()
                            except Exception:
                                pass
                            continue
                        # otherwise, don't remove lock; just wait for the poll interval
                        logging.getLogger(__name__).debug(
                            "Lock file %s appears to be held by PID %s; os.kill raised %s",
                            lock_path,
                            pid,
                            exc,
                        )
                if time.time() - start >= timeout:
                    raise LockAcquireError(f"Unable to acquire lock {lock_path} after {timeout}s")
                time.sleep(poll_interval)
        yield
    finally:
        if fd is not None:
            try:
                os.close(fd)
            except Exception:
                pass
        try:
            lock_path.unlink()
            logging.getLogger(__name__).debug("Released lock %s by pid %s", lock_path, os.getpid())
        except FileNotFoundError:
            pass
        except Exception:
            pass
