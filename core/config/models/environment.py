"""Environment-wide storage and connection configuration.

This module provides configuration for storage backends (S3, Azure, etc.)
that can be shared across multiple pattern configurations.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from core.config.placeholders import resolve_env_vars

logger = logging.getLogger(__name__)


@dataclass
class S3ConnectionConfig:
    """S3 connection configuration.

    Attributes:
        endpoint_url: Optional custom S3 endpoint (e.g., MinIO, LocalStack)
        access_key_id: AWS access key ID
        secret_access_key: AWS secret access key
        region: AWS region name
        buckets: Named bucket references (name -> actual bucket)
    """

    endpoint_url: Optional[str] = None
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    region: str = "us-east-1"
    buckets: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "S3ConnectionConfig":
        """Create S3 config from dictionary with environment variable resolution.

        Args:
            data: Configuration dictionary

        Returns:
            S3ConnectionConfig instance with resolved environment variables
        """
        if not isinstance(data, dict):
            raise ValueError("S3 configuration must be a dictionary")

        # Resolve environment variables
        data = resolve_env_vars(data)

        return cls(
            endpoint_url=data.get("endpoint_url"),
            access_key_id=data.get("access_key_id"),
            secret_access_key=data.get("secret_access_key"),
            region=data.get("region", "us-east-1"),
            buckets=data.get("buckets", {}),
        )

    def get_bucket(self, name: str) -> str:
        """Get bucket name by reference, or return the name itself.

        Args:
            name: Bucket reference name or actual bucket name

        Returns:
            Actual bucket name

        Example:
            >>> config = S3ConnectionConfig(buckets={"data": "my-data-bucket"})
            >>> config.get_bucket("data")
            'my-data-bucket'
            >>> config.get_bucket("some-actual-bucket")
            'some-actual-bucket'
        """
        return self.buckets.get(name, name)


@dataclass
class EnvironmentConfig:
    """Environment-wide storage and connection settings.

    Attributes:
        name: Environment name (e.g., "dev", "prod", "staging")
        s3: Optional S3 connection configuration
        azure: Optional Azure Blob Storage configuration (future)
    """

    name: str
    s3: Optional[S3ConnectionConfig] = None
    azure: Optional[Dict[str, Any]] = None  # Future implementation

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "EnvironmentConfig":
        """Create environment config from dictionary.

        Args:
            data: Configuration dictionary

        Returns:
            EnvironmentConfig instance
        """
        if not isinstance(data, dict):
            raise ValueError("Environment configuration must be a dictionary")

        name = data.get("name")
        if not name:
            raise ValueError("Environment configuration must have a 'name' field")

        s3_data = data.get("s3")
        s3_config = S3ConnectionConfig.from_dict(s3_data) if s3_data else None

        azure_data = data.get("azure")

        return cls(name=name, s3=s3_config, azure=azure_data)

    @classmethod
    def from_yaml(cls, path: Path) -> "EnvironmentConfig":
        """Load environment config from YAML file.

        Args:
            path: Path to YAML configuration file

        Returns:
            EnvironmentConfig instance

        Raises:
            FileNotFoundError: If config file doesn't exist
            ValueError: If config file is invalid
        """
        if not path.exists():
            raise FileNotFoundError(f"Environment config file not found: {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)

        if not data:
            raise ValueError(f"Environment config file is empty: {path}")

        return cls.from_dict(data)
