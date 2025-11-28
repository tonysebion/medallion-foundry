"""Utility script to clean generated artifacts such as site/ and htmlcov/.

This helps contributors remove accidentally committed build artifacts.
"""

from pathlib import Path
import shutil


def rm_dir(path: Path) -> None:
    if path.exists():
        print(f"Removing {path}")
        shutil.rmtree(path)
    else:
        print(f"Not found: {path}")


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    rm_dir(repo_root / "site")
    rm_dir(repo_root / "htmlcov")


if __name__ == "__main__":
    main()
