# dev.ps1 - Development commands for OpenMetadata Custom Connectors
# Usage: .\dev.ps1 <command>
# Example: .\dev.ps1 test

param(
    [Parameter(Position=0)]
    [string]$Command = "help",
    
    [Parameter(ValueFromRemainingArguments=$true)]
    [string[]]$Args
)

# Activate virtual environment if it exists
$venvPath = Join-Path $PSScriptRoot ".venv\Scripts\Activate.ps1"
if (Test-Path $venvPath) {
    . $venvPath
}

function Show-Help {
    Write-Host ""
    Write-Host "OpenMetadata Custom Connectors - Development Commands" -ForegroundColor Cyan
    Write-Host "=" * 60
    Write-Host ""
    Write-Host "Setup Commands:" -ForegroundColor Yellow
    Write-Host "  install           Install package dependencies"
    Write-Host "  install-dev       Install package with dev dependencies"
    Write-Host "  install-hooks     Install pre-commit hooks"
    Write-Host ""
    Write-Host "Testing Commands:" -ForegroundColor Yellow
    Write-Host "  test              Run all tests"
    Write-Host "  test-cov          Run tests with coverage report"
    Write-Host "  test-specific     Run specific test (use: .\dev.ps1 test-specific tests\test_common.py)"
    Write-Host ""
    Write-Host "Code Quality Commands:" -ForegroundColor Yellow
    Write-Host "  format            Auto-format code with ruff"
    Write-Host "  format-check      Check if code needs formatting (don't modify)"
    Write-Host "  lint              Run ruff linting"
    Write-Host "  type-check        Run mypy type checking"
    Write-Host "  security          Run bandit and pip-audit security checks"
    Write-Host "  quality           Run all quality checks (lint, format-check, type-check)"
    Write-Host "  all               Format, lint, type-check, and test"
    Write-Host ""
    Write-Host "Build Commands:" -ForegroundColor Yellow
    Write-Host "  clean             Clean up generated files"
    Write-Host "  build             Build distribution packages"
    Write-Host ""
    Write-Host "Connector Commands:" -ForegroundColor Yellow
    Write-Host "  create            Create new connector (requires NAME and TYPE params)"
    Write-Host "                    Example: .\dev.ps1 create salesforce storage"
    Write-Host ""
    Write-Host "Examples:" -ForegroundColor Green
    Write-Host "  .\dev.ps1 install-dev"
    Write-Host "  .\dev.ps1 test"
    Write-Host "  .\dev.ps1 format"
    Write-Host "  .\dev.ps1 quality"
    Write-Host "  .\dev.ps1 create salesforce storage"
    Write-Host ""
}

function Invoke-Install {
    Write-Host "Installing package dependencies..." -ForegroundColor Cyan
    pip install -e .
}

function Invoke-InstallDev {
    Write-Host "Installing package with dev dependencies..." -ForegroundColor Cyan
    pip install -e ".[dev,identity,secrets-all]"
}

function Invoke-InstallHooks {
    Write-Host "Installing pre-commit hooks..." -ForegroundColor Cyan
    pip install pre-commit
    pre-commit install
}

function Invoke-Test {
    Write-Host "Running tests..." -ForegroundColor Cyan
    pytest
}

function Invoke-TestCov {
    Write-Host "Running tests with coverage..." -ForegroundColor Cyan
    pytest --cov=om_connectors --cov-report=html --cov-report=term
    Write-Host ""
    Write-Host "Coverage report saved to: htmlcov/index.html" -ForegroundColor Green
}

function Invoke-TestSpecific {
    if ($Args.Count -eq 0) {
        Write-Host "Error: Please specify test file" -ForegroundColor Red
        Write-Host "Example: .\dev.ps1 test-specific tests\test_common.py"
        exit 1
    }
    Write-Host "Running specific test: $($Args[0])..." -ForegroundColor Cyan
    pytest $Args[0] -v
}

function Invoke-Format {
    Write-Host "Formatting code with ruff..." -ForegroundColor Cyan
    python -m ruff format pipelines/ tests/ scripts/
    Write-Host "Code formatted successfully!" -ForegroundColor Green
}

function Invoke-FormatCheck {
    Write-Host "Checking code formatting..." -ForegroundColor Cyan
    python -m ruff format --check pipelines/ tests/ scripts/
}

function Invoke-Lint {
    Write-Host "Running ruff linting..." -ForegroundColor Cyan
    python -m ruff check pipelines/ tests/ scripts/
}

function Invoke-TypeCheck {
    Write-Host "Running mypy type checking..." -ForegroundColor Cyan
    python -m mypy pipelines/
}

function Invoke-Security {
    Write-Host "Running security checks..." -ForegroundColor Cyan
    Write-Host "Running bandit..." -ForegroundColor Yellow
    bandit -r src/om_connectors -c pyproject.toml
    Write-Host ""
    Write-Host "Running pip-audit..." -ForegroundColor Yellow
    pip-audit
}

function Invoke-Quality {
    Write-Host "Running all quality checks..." -ForegroundColor Cyan
    Invoke-Lint
    Invoke-FormatCheck
    Invoke-TypeCheck
}

function Invoke-All {
    Write-Host "Running complete workflow..." -ForegroundColor Cyan
    Invoke-Format
    Invoke-Lint
    Invoke-TypeCheck
    Invoke-Test
}

function Invoke-Clean {
    Write-Host "Cleaning up generated files..." -ForegroundColor Cyan
    Remove-Item -Path "build", "dist", "*.egg-info", ".coverage", "htmlcov", ".pytest_cache", ".mypy_cache" -Recurse -Force -ErrorAction SilentlyContinue
    Get-ChildItem -Path . -Filter "__pycache__" -Recurse -Directory | Remove-Item -Recurse -Force
    Get-ChildItem -Path . -Filter "*.pyc" -Recurse -File | Remove-Item -Force
    Write-Host "Cleanup complete!" -ForegroundColor Green
}

function Invoke-Build {
    Write-Host "Building distribution packages..." -ForegroundColor Cyan
    python -m build
}

function Invoke-CreateConnector {
    if ($Args.Count -lt 2) {
        Write-Host "Error: Please specify connector name and type" -ForegroundColor Red
        Write-Host "Example: .\dev.ps1 create salesforce storage"
        Write-Host "Types: storage, database, mlmodel, identity"
        exit 1
    }
    $name = $Args[0]
    $type = $Args[1]
    Write-Host "Creating $type connector: $name..." -ForegroundColor Cyan
    python scripts/create_connector.py --name $name --type $type
}

# Main command dispatcher
switch ($Command.ToLower()) {
    "help" { Show-Help }
    "install" { Invoke-Install }
    "install-dev" { Invoke-InstallDev }
    "install-hooks" { Invoke-InstallHooks }
    "test" { Invoke-Test }
    "test-cov" { Invoke-TestCov }
    "test-specific" { Invoke-TestSpecific }
    "format" { Invoke-Format }
    "format-check" { Invoke-FormatCheck }
    "lint" { Invoke-Lint }
    "type-check" { Invoke-TypeCheck }
    "security" { Invoke-Security }
    "quality" { Invoke-Quality }
    "all" { Invoke-All }
    "clean" { Invoke-Clean }
    "build" { Invoke-Build }
    "create" { Invoke-CreateConnector }
    default {
        Write-Host "Unknown command: $Command" -ForegroundColor Red
        Write-Host ""
        Show-Help
        exit 1
    }
}
