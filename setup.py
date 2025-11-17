"""Setup configuration for medallion-foundry package."""

from setuptools import setup, find_packages
from pathlib import Path

# Read the README file
readme_file = Path(__file__).parent / "README.md"
long_description = (
    readme_file.read_text(encoding="utf-8") if readme_file.exists() else ""
)

setup(
    name="medallion-foundry",
    version="1.0.0",
    description="Config-driven Python framework for landing data into Bronze layer with pluggable storage backends",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Tony Sebion",
    url="https://github.com/tonysebion/medallion-foundry",
    packages=find_packages(exclude=["tests", "tests.*", "docs", "docs.*"]),
    python_requires=">=3.8",
    install_requires=[
        "boto3>=1.26.0",
        "pandas>=1.5.0",
        "pyarrow>=10.0.0",  # Required for parquet support
        "pyodbc>=4.0.0",
        "pyyaml>=6.0",
        "requests>=2.28.0",
        "tenacity>=8.0.0",  # For retry logic
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=0.990",
        ],
    },
    entry_points={
        "console_scripts": [
            "bronze-extract=bronze_extract:main",
            "silver-extract=silver_extract:main",
            "bronze-config-doctor=scripts.bronze_config_doctor:main",
        ],
    },
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
    ],
    keywords="data-engineering bronze-layer medallion-architecture etl data-pipeline storage-backends",
    project_urls={
        "Documentation": "https://github.com/tonysebion/medallion-foundry/blob/main/README.md",
        "Bug Reports": "https://github.com/tonysebion/medallion-foundry/issues",
        "Source": "https://github.com/tonysebion/medallion-foundry",
        "Changelog": "https://github.com/tonysebion/medallion-foundry/blob/main/CHANGELOG.md",
    },
)
