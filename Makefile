# Makefile for medallion-foundry development tasks
# Requires: Python 3.8+, venv created at .venv/

PYTHON := .venv/Scripts/python.exe
PIP := .venv/Scripts/pip.exe
PYTEST := .venv/Scripts/pytest.exe

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
	set RUN_SMOKE=1 && $(PYTEST) -v -m integration

test-cov:  ## Run tests with coverage report
	$(PYTEST) --cov=core --cov=bronze_extract --cov=silver_extract --cov-report=html --cov-report=term

lint:  ## Run linters (ruff check)
	$(PYTHON) -m ruff check .

format:  ## Format code (ruff format)
	$(PYTHON) -m ruff format .

fix:  ## Fix linting issues automatically
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

type-check:  ## Run type checker (mypy)
	$(PYTHON) -m mypy --config-file=mypy.ini core/ bronze_extract.py silver_extract.py

docs:  ## Build documentation
	$(PYTHON) -m mkdocs build --strict

docs-serve:  ## Serve documentation locally
	$(PYTHON) -m mkdocs serve

clean:  ## Clean temporary files and caches
	-del /q /s *.pyc 2>nul
	-rmdir /s /q __pycache__ .pytest_cache .mypy_cache .ruff_cache .coverage htmlcov 2>nul
	-rmdir /s /q site 2>nul

clean-output:  ## Clean output directories
	-rmdir /s /q output silver_output 2>nul

pre-commit-install:  ## Install pre-commit hooks
	$(PIP) install pre-commit
	.venv/Scripts/pre-commit install

pre-commit-run:  ## Run pre-commit on all files
	.venv/Scripts/pre-commit run --all-files

validate-config:  ## Validate sample configs
	$(PYTHON) bronze_extract.py --config config/sample_api.yaml --validate-only

.PHONY: all
all: install lint type-check test  ## Run install, lint, type-check, and test
