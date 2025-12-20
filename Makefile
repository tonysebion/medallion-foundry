# Makefile for medallion-foundry development tasks
# Requires: Python 3.9+, venv created at .venv/

PYTHON ?= python
PIP ?= $(PYTHON) -m pip
PYTEST ?= $(PYTHON) -m pytest

.PHONY: help install test test-fast test-smoke test-cov lint format type-check docs clean

help:  ## Show this help message
	@echo "Available targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-18s %s\n", $$1, $$2}'

install:  ## Install dependencies
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements-dev.txt
	$(PIP) install -e .

install-all:  ## Install all dependencies (including Azure, DB extras)
	$(PIP) install -r requirements.txt
	$(PIP) install -r requirements-dev.txt
	$(PIP) install -r requirements-azure.txt
	$(PIP) install -r requirements-db.txt
	$(PIP) install -e .

test:  ## Run all tests
	$(PYTEST) -v

test-fast:  ## Run tests (excluding slow integration tests)
	$(PYTEST) -v -m "not integration"

test-smoke:  ## Run smoke tests
	RUN_SMOKE=1 $(PYTEST) -v -m integration

test-cov:  ## Run tests with coverage report
	$(PYTEST) --cov=pipelines --cov-report=html --cov-report=term

lint:  ## Run linters (ruff check)
	$(PYTHON) -m ruff check .

format:  ## Format code (ruff format)
	$(PYTHON) -m ruff format .

fix:  ## Fix linting issues automatically
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

type-check:  ## Run type checker (mypy)
	$(PYTEST) --mypy --mypy-config-file=mypy.ini -k mypy

docs:  ## Build documentation
	$(PYTHON) -m mkdocs build --strict

docs-serve:  ## Serve documentation locally
	$(PYTHON) -m mkdocs serve

clean:  ## Clean temporary files and caches
	# Cross-platform Python-based cleanup
	$(PYTHON) -c "import shutil; import pathlib; [p.unlink() for p in pathlib.Path('.').rglob('*.pyc')]" || true
	$(PYTHON) -c "import shutil; import pathlib; [shutil.rmtree(d, ignore_errors=True) for d in ['__pycache__', '.pytest_cache', '.mypy_cache', '.ruff_cache', 'htmlcov', 'site']]" || true
	$(PYTHON) -c "import pathlib; p = pathlib.Path('.coverage'); p.unlink() if p.exists() else None" || true
	$(PYTHON) -c "import shutil; import pathlib; [shutil.rmtree(str(d), ignore_errors=True) for d in pathlib.Path('.').rglob('__pycache__')]" || true

clean-output:  ## Clean output directories
	$(PYTHON) -c "import shutil; [shutil.rmtree(d, ignore_errors=True) for d in ['output', 'silver_output']]" || true

pre-commit-install:  ## Install pre-commit hooks
	$(PIP) install pre-commit
	pre-commit install

pre-commit-run:  ## Run pre-commit on all files
	pre-commit run --all-files

validate-config:  ## Validate sample configs
	$(PYTHON) bronze_extract.py --config config/sample_api.yaml --validate-only

.PHONY: all
all: install lint type-check test  ## Run install, lint, type-check, and test
