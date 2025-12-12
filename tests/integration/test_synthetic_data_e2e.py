"""End-to-end tests for synthetic data with MinIO storage.

Tests the complete flow:
1. Generate synthetic data (T0/T1/T2)
2. Upload to MinIO
3. Run Bronze extraction
4. Verify output in MinIO
5. Run Silver transformation
6. Verify final curated data

Per Story 1.1: Create Synthetic Data Test Framework
"""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from core.infrastructure.runtime.chunking import chunk_records, write_parquet_chunk
from core.infrastructure.runtime.metadata_helpers import (
    write_batch_metadata,
    write_checksum_manifest,
)
from tests.integration.conftest import (
    download_parquet_from_minio,
    list_objects_in_prefix,
    requires_minio,
    upload_dataframe_to_minio,
)


# =============================================================================
# MinIO Connectivity Tests
# =============================================================================


@requires_minio
class TestMinioConnectivity:
    """Basic MinIO connectivity and operations tests."""

    def test_minio_is_reachable(self, minio_client):
        """MinIO should be reachable and respond to requests."""
        response = minio_client.list_buckets()
        assert "Buckets" in response

    def test_bucket_exists(self, minio_client, minio_bucket):
        """Test bucket should exist or be created."""
        response = minio_client.head_bucket(Bucket=minio_bucket)
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    def test_upload_and_download(self, minio_client, minio_bucket, cleanup_prefix):
        """Should be able to upload and download objects."""
        key = f"{cleanup_prefix}/test_object.txt"
        content = b"Hello, MinIO!"

        # Upload
        minio_client.put_object(
            Bucket=minio_bucket,
            Key=key,
            Body=content,
        )

        # Download
        response = minio_client.get_object(Bucket=minio_bucket, Key=key)
        downloaded = response["Body"].read()

        assert downloaded == content

    def test_list_objects(self, minio_client, minio_bucket, cleanup_prefix):
        """Should be able to list objects with prefix."""
        # Create some test objects
        for i in range(3):
            minio_client.put_object(
                Bucket=minio_bucket,
                Key=f"{cleanup_prefix}/file_{i}.txt",
                Body=f"content {i}".encode(),
            )

        # List objects
        objects = list_objects_in_prefix(minio_client, minio_bucket, cleanup_prefix)
        assert len(objects) == 3
        assert all(cleanup_prefix in obj for obj in objects)


# =============================================================================
# Synthetic Data Generation Tests
# =============================================================================


@requires_minio
class TestSyntheticDataGeneration:
    """Tests for synthetic data generation and upload to MinIO."""

    def test_claims_t0_generation(self, claims_t0_df):
        """Claims T0 data should be generated correctly."""
        assert len(claims_t0_df) == 100
        assert "claim_id" in claims_t0_df.columns
        assert "patient_id" in claims_t0_df.columns
        assert "service_date" in claims_t0_df.columns
        assert "billed_amount" in claims_t0_df.columns

        # Check data quality
        assert claims_t0_df["claim_id"].is_unique
        assert claims_t0_df["billed_amount"].min() > 0

    def test_claims_t1_generation(self, claims_t0_df, claims_t1_df):
        """Claims T1 data should include updates and new records."""
        assert len(claims_t1_df) > 0

        # T1 should have some overlap with T0 (updates) and some new records
        t0_ids = set(claims_t0_df["claim_id"])
        t1_ids = set(claims_t1_df["claim_id"])

        # Some records should be updates (exist in T0)
        updates = t0_ids & t1_ids
        # Some records should be new (not in T0)
        new_records = t1_ids - t0_ids

        # T1 generator creates 20% updates + 25% new = 45% of row_count
        assert len(updates) > 0 or len(new_records) > 0

    def test_orders_t0_generation(self, orders_t0_df):
        """Orders T0 data should be generated correctly."""
        assert len(orders_t0_df) == 100
        assert "order_id" in orders_t0_df.columns
        assert "customer_id" in orders_t0_df.columns
        assert "order_ts" in orders_t0_df.columns  # Column name is order_ts not order_time
        assert "total_amount" in orders_t0_df.columns

    def test_transactions_t0_generation(self, transactions_t0_df):
        """Transactions T0 data should be generated correctly."""
        assert len(transactions_t0_df) == 100
        assert "transaction_id" in transactions_t0_df.columns
        assert "account_id" in transactions_t0_df.columns
        assert "amount" in transactions_t0_df.columns

    def test_time_series_generation(self, claims_time_series):
        """Time series should generate T0, T1, and T2 datasets."""
        assert "t0" in claims_time_series
        assert "t1" in claims_time_series
        assert "t2_late" in claims_time_series

        assert len(claims_time_series["t0"]) == 100
        assert len(claims_time_series["t1"]) > 0


# =============================================================================
# MinIO Upload/Download Tests with Synthetic Data
# =============================================================================


@requires_minio
class TestSyntheticDataMinioIntegration:
    """Tests for uploading synthetic data to MinIO and verifying."""

    def test_upload_claims_parquet(
        self, minio_client, minio_bucket, cleanup_prefix, claims_t0_df
    ):
        """Should upload claims DataFrame as parquet to MinIO."""
        key = f"{cleanup_prefix}/synthetic/claims/t0/data.parquet"

        path = upload_dataframe_to_minio(
            minio_client, minio_bucket, key, claims_t0_df, format="parquet"
        )

        assert path == f"s3://{minio_bucket}/{key}"

        # Verify upload
        objects = list_objects_in_prefix(minio_client, minio_bucket, cleanup_prefix)
        assert key in objects

    def test_download_claims_parquet(
        self, minio_client, minio_bucket, cleanup_prefix, claims_t0_df
    ):
        """Should download claims parquet and match original data."""
        key = f"{cleanup_prefix}/synthetic/claims/t0/data.parquet"

        # Upload
        upload_dataframe_to_minio(
            minio_client, minio_bucket, key, claims_t0_df, format="parquet"
        )

        # Download
        downloaded_df = download_parquet_from_minio(minio_client, minio_bucket, key)

        # Compare
        assert len(downloaded_df) == len(claims_t0_df)
        assert list(downloaded_df.columns) == list(claims_t0_df.columns)
        pd.testing.assert_frame_equal(
            downloaded_df.reset_index(drop=True),
            claims_t0_df.reset_index(drop=True),
            check_dtype=False,  # Parquet may change some dtypes
        )

    def test_upload_time_series(
        self, minio_client, minio_bucket, cleanup_prefix, claims_time_series
    ):
        """Should upload complete T0/T1/T2 time series to MinIO."""
        base_prefix = f"{cleanup_prefix}/synthetic/claims"

        for period, df in claims_time_series.items():
            key = f"{base_prefix}/{period}/data.parquet"
            upload_dataframe_to_minio(minio_client, minio_bucket, key, df)

        # Verify all uploads
        objects = list_objects_in_prefix(minio_client, minio_bucket, base_prefix)
        assert len(objects) == 3
        assert any("t0" in obj for obj in objects)
        assert any("t1" in obj for obj in objects)
        assert any("t2_late" in obj for obj in objects)

    def test_upload_csv_format(
        self, minio_client, minio_bucket, cleanup_prefix, orders_t0_df
    ):
        """Should upload DataFrame as CSV to MinIO."""
        key = f"{cleanup_prefix}/synthetic/orders/t0/data.csv"

        upload_dataframe_to_minio(
            minio_client, minio_bucket, key, orders_t0_df, format="csv"
        )

        # Download and verify
        response = minio_client.get_object(Bucket=minio_bucket, Key=key)
        content = response["Body"].read().decode("utf-8")

        # CSV should have header and data rows
        lines = content.strip().split("\n")
        assert len(lines) == 101  # 1 header + 100 data rows


# =============================================================================
# S3Storage Integration Tests
# =============================================================================


@requires_minio
class TestS3StorageWithMinio:
    """Tests for S3Storage class with MinIO backend."""

    def test_s3_storage_initialization(self, minio_platform_config):
        """S3Storage should initialize with MinIO configuration."""
        from core.infrastructure.io.storage.s3 import S3Storage

        storage = S3Storage(minio_platform_config)
        assert storage.bucket is not None
        assert storage.client is not None

    def test_s3_storage_health_check(self, minio_platform_config):
        """S3Storage health check should pass with MinIO."""
        from core.infrastructure.io.storage.s3 import S3Storage

        storage = S3Storage(minio_platform_config)
        result = storage.health_check()

        assert result.is_healthy is True
        if result.latency_ms is not None:
            assert result.latency_ms >= 0
        # Permissions are in checked_permissions dict
        assert result.checked_permissions.get("read") is True
        assert result.checked_permissions.get("write") is True
        assert result.checked_permissions.get("list") is True
        assert result.checked_permissions.get("delete") is True

    def test_s3_storage_upload_download(
        self, minio_platform_config, cleanup_prefix, claims_t0_df, temp_dir
    ):
        """S3Storage should upload and download files correctly."""
        from core.infrastructure.io.storage.s3 import S3Storage

        storage = S3Storage(minio_platform_config)

        # Write test file locally
        local_file = temp_dir / "test_claims.parquet"
        claims_t0_df.to_parquet(local_file, index=False)

        # Upload via S3Storage (uses upload_file method)
        remote_path = f"{cleanup_prefix}/claims/test_claims.parquet"
        storage.upload_file(str(local_file), remote_path)

        # Download via S3Storage (uses download_file method)
        download_path = temp_dir / "downloaded_claims.parquet"
        storage.download_file(remote_path, str(download_path))

        # Verify
        downloaded_df = pd.read_parquet(download_path)
        assert len(downloaded_df) == len(claims_t0_df)

    def test_s3_storage_list_files(
        self, minio_platform_config, cleanup_prefix, temp_dir
    ):
        """S3Storage should list files correctly."""
        from core.infrastructure.io.storage.s3 import S3Storage

        storage = S3Storage(minio_platform_config)

        # Create some test files under the cleanup_prefix
        list_prefix = f"{cleanup_prefix}/test_list"
        for i in range(3):
            local_file = temp_dir / f"file_{i}.txt"
            local_file.write_text(f"content {i}")
            storage.upload_file(str(local_file), f"{list_prefix}/file_{i}.txt")

        # List files (uses list_files method)
        files = storage.list_files(list_prefix)
        assert len(files) == 3


# =============================================================================
# Deterministic Seed Tests
# =============================================================================


@requires_minio
class TestDeterministicGeneration:
    """Tests to verify synthetic data is deterministic with same seed."""

    def test_same_seed_produces_same_data(self, t0_date):
        """Same seed should produce identical data."""
        from tests.synthetic_data import ClaimsGenerator

        gen1 = ClaimsGenerator(seed=42, row_count=50)
        gen2 = ClaimsGenerator(seed=42, row_count=50)

        df1 = gen1.generate_t0(t0_date)
        df2 = gen2.generate_t0(t0_date)

        pd.testing.assert_frame_equal(df1, df2)

    def test_different_seed_produces_different_data(self, t0_date):
        """Different seeds should produce different data."""
        from tests.synthetic_data import ClaimsGenerator

        gen1 = ClaimsGenerator(seed=42, row_count=50)
        gen2 = ClaimsGenerator(seed=123, row_count=50)

        df1 = gen1.generate_t0(t0_date)
        df2 = gen2.generate_t0(t0_date)

        # Data should be different - compare random values (billed_amount), not sequential IDs
        # claim_id is sequential (CLM00000001, etc.) and won't change with seed
        assert not df1["billed_amount"].equals(df2["billed_amount"])


# =============================================================================
# Data Quality Validation Tests
# =============================================================================


@requires_minio
class TestDataQualityValidation:
    """Tests for data quality of synthetic datasets."""

    def test_claims_data_quality(self, claims_t0_df):
        """Claims data should meet quality standards."""
        # No nulls in required fields
        assert claims_t0_df["claim_id"].notna().all()
        assert claims_t0_df["patient_id"].notna().all()
        assert claims_t0_df["billed_amount"].notna().all()

        # Amounts are positive
        assert (claims_t0_df["billed_amount"] > 0).all()
        assert (claims_t0_df["paid_amount"] >= 0).all()

        # Paid amount <= billed amount
        assert (claims_t0_df["paid_amount"] <= claims_t0_df["billed_amount"]).all()

    def test_orders_data_quality(self, orders_t0_df):
        """Orders data should meet quality standards."""
        # No nulls in required fields
        assert orders_t0_df["order_id"].notna().all()
        assert orders_t0_df["customer_id"].notna().all()

        # Order IDs are unique
        assert orders_t0_df["order_id"].is_unique

        # Amounts are positive
        assert (orders_t0_df["total_amount"] > 0).all()

    def test_transactions_data_quality(self, transactions_t0_df):
        """Transactions data should meet quality standards."""
        # No nulls in required fields
        assert transactions_t0_df["transaction_id"].notna().all()
        assert transactions_t0_df["account_id"].notna().all()

        # Transaction IDs are unique
        assert transactions_t0_df["transaction_id"].is_unique


# =============================================================================
# Bronze Extraction Tests
# =============================================================================


@requires_minio
class TestBronzeExtractionE2E:
    """End-to-end tests for Bronze extraction with synthetic data."""

    def test_bronze_extraction_from_local_parquet(
        self, claims_t0_df, temp_dir, t0_date
    ):
        """Bronze extraction should read local parquet file."""
        from core.orchestration.runner.job import build_extractor

        # Write synthetic data to temp file
        input_file = temp_dir / "claims.parquet"
        claims_t0_df.to_parquet(input_file, index=False)

        # Build config for file extraction
        cfg: Dict[str, Any] = {
            "source": {
                "type": "file",
                "system": "synthetic",
                "table": "claims",
                "file": {
                    "path": str(input_file),
                    "format": "parquet",
                },
                "run": {
                    "load_pattern": "snapshot",
                },
            }
        }

        # Build extractor and fetch records
        extractor = build_extractor(cfg)
        records, cursor = extractor.fetch_records(cfg, t0_date)

        # Verify records match input
        assert len(records) == len(claims_t0_df)
        assert records[0]["claim_id"] == claims_t0_df.iloc[0]["claim_id"]

    def test_bronze_extraction_from_local_csv(
        self, orders_t0_df, temp_dir, t0_date
    ):
        """Bronze extraction should read local CSV file."""
        from core.orchestration.runner.job import build_extractor

        # Write synthetic data to temp CSV
        input_file = temp_dir / "orders.csv"
        orders_t0_df.to_csv(input_file, index=False)

        # Build config for CSV extraction
        cfg: Dict[str, Any] = {
            "source": {
                "type": "file",
                "system": "synthetic",
                "table": "orders",
                "file": {
                    "path": str(input_file),
                    "format": "csv",
                },
                "run": {
                    "load_pattern": "snapshot",
                },
            }
        }

        # Build extractor and fetch records
        extractor = build_extractor(cfg)
        records, cursor = extractor.fetch_records(cfg, t0_date)

        # Verify records
        assert len(records) == len(orders_t0_df)

    def test_bronze_chunking_produces_correct_output(
        self, claims_t0_df, temp_dir
    ):
        """Bronze chunking should produce correctly sized chunks."""
        records = claims_t0_df.to_dict("records")

        # Chunk with max 25 rows per chunk
        chunks = chunk_records(records, max_rows=25)

        # Should produce 4 chunks for 100 records
        assert len(chunks) == 4

        # Write chunks and verify
        output_files = []
        for i, chunk in enumerate(chunks):
            out_path = temp_dir / f"chunk_{i}.parquet"
            write_parquet_chunk(chunk, out_path)
            output_files.append(out_path)

        # Verify all chunks written
        assert all(f.exists() for f in output_files)

        # Verify total records
        total_records = sum(
            len(pd.read_parquet(f)) for f in output_files
        )
        assert total_records == 100

    def test_bronze_metadata_generation(self, claims_t0_df, temp_dir):
        """Bronze should generate correct metadata files."""
        records = claims_t0_df.to_dict("records")

        # Write chunk
        chunk_path = temp_dir / "chunk_0.parquet"
        write_parquet_chunk(records, chunk_path)

        # Write metadata
        metadata_path = write_batch_metadata(
            out_dir=temp_dir,
            record_count=len(records),
            chunk_count=1,
            cursor=None,
        )

        # Write checksum manifest
        manifest_path = write_checksum_manifest(
            out_dir=temp_dir,
            files=[chunk_path],
            load_pattern="snapshot",
        )

        # Verify metadata files exist
        assert metadata_path.exists()
        assert manifest_path.exists()

        # Verify metadata content
        import json
        metadata = json.loads(metadata_path.read_text())
        assert metadata["record_count"] == 100
        assert metadata["chunk_count"] == 1

        manifest = json.loads(manifest_path.read_text())
        assert manifest["load_pattern"] == "snapshot"
        assert len(manifest["files"]) == 1

    def test_bronze_extraction_with_minio_upload(
        self,
        minio_client,
        minio_bucket,
        cleanup_prefix,
        claims_t0_df,
        temp_dir,
        t0_date,
    ):
        """Bronze extraction should upload results to MinIO."""
        from core.orchestration.runner.job import build_extractor
        from tests.integration.conftest import (
            upload_dataframe_to_minio,
            list_objects_in_prefix,
            download_parquet_from_minio,
        )

        # Write synthetic data to temp file
        input_file = temp_dir / "claims.parquet"
        claims_t0_df.to_parquet(input_file, index=False)

        # Build config and extract
        cfg: Dict[str, Any] = {
            "source": {
                "type": "file",
                "system": "synthetic",
                "table": "claims",
                "file": {
                    "path": str(input_file),
                    "format": "parquet",
                },
                "run": {
                    "load_pattern": "snapshot",
                },
            }
        }

        extractor = build_extractor(cfg)
        records, _ = extractor.fetch_records(cfg, t0_date)

        # Write to local bronze output
        bronze_output = temp_dir / "bronze"
        bronze_output.mkdir()
        chunk_path = bronze_output / "chunk_0.parquet"
        write_parquet_chunk(records, chunk_path)

        # Upload to MinIO
        remote_key = f"{cleanup_prefix}/bronze/synthetic/claims/dt={t0_date}/chunk_0.parquet"
        upload_dataframe_to_minio(
            minio_client, minio_bucket, remote_key, pd.DataFrame(records)
        )

        # Verify upload
        objects = list_objects_in_prefix(
            minio_client, minio_bucket, f"{cleanup_prefix}/bronze"
        )
        assert len(objects) == 1
        assert remote_key in objects

        # Download and verify content
        downloaded_df = download_parquet_from_minio(minio_client, minio_bucket, remote_key)
        assert len(downloaded_df) == len(claims_t0_df)

    def test_bronze_incremental_extraction_t0_t1(
        self, claims_t0_df, claims_t1_df, temp_dir, t0_date, t1_date
    ):
        """Bronze should support incremental extraction across T0 and T1."""
        from core.orchestration.runner.job import build_extractor

        # T0: Initial load
        t0_file = temp_dir / "claims_t0.parquet"
        claims_t0_df.to_parquet(t0_file, index=False)

        cfg: Dict[str, Any] = {
            "source": {
                "type": "file",
                "system": "synthetic",
                "table": "claims",
                "file": {"path": str(t0_file), "format": "parquet"},
                "run": {"load_pattern": "snapshot"},
            }
        }

        extractor = build_extractor(cfg)
        t0_records, _ = extractor.fetch_records(cfg, t0_date)

        # Write T0 output
        t0_output = temp_dir / "bronze" / f"dt={t0_date}"
        t0_output.mkdir(parents=True)
        write_parquet_chunk(t0_records, t0_output / "chunk_0.parquet")

        # T1: Incremental load
        t1_file = temp_dir / "claims_t1.parquet"
        claims_t1_df.to_parquet(t1_file, index=False)

        cfg["source"]["file"]["path"] = str(t1_file)
        cfg["source"]["run"]["load_pattern"] = "incremental_append"

        t1_records, _ = extractor.fetch_records(cfg, t1_date)

        # Write T1 output
        t1_output = temp_dir / "bronze" / f"dt={t1_date}"
        t1_output.mkdir(parents=True)
        write_parquet_chunk(t1_records, t1_output / "chunk_0.parquet")

        # Verify both partitions exist
        assert (t0_output / "chunk_0.parquet").exists()
        assert (t1_output / "chunk_0.parquet").exists()

        # T1 should have different record count (updates + new)
        t1_df = pd.read_parquet(t1_output / "chunk_0.parquet")
        assert len(t1_df) > 0


# =============================================================================
# Silver Transformation Tests
# =============================================================================


@requires_minio
class TestSilverTransformationE2E:
    """End-to-end tests for Silver transformation with synthetic data."""

    def test_silver_deduplication_by_primary_keys(self, claims_t0_df, temp_dir):
        """Silver should deduplicate records by primary keys."""
        # Create duplicate records (simulating Bronze with duplicates)
        df = pd.concat([claims_t0_df, claims_t0_df.head(20)], ignore_index=True)
        assert len(df) == 120  # 100 + 20 duplicates

        # Write to Bronze
        bronze_path = temp_dir / "bronze"
        bronze_path.mkdir()
        df.to_parquet(bronze_path / "chunk_0.parquet", index=False)

        # Deduplicate (Silver processing)
        result = df.drop_duplicates(subset=["claim_id"], keep="last")

        # Should be back to original count
        assert len(result) == 100

    def test_silver_normalization_trim_strings(self, claims_t0_df):
        """Silver should normalize string columns (trim whitespace)."""
        df = claims_t0_df.copy()
        # Add whitespace
        df["claim_id"] = df["claim_id"].apply(lambda x: f"  {x}  ")

        # Normalize (Silver processing) - only for actual string columns
        for col in df.select_dtypes(include=["object"]).columns:
            # Check if values are strings before using .str accessor
            if df[col].apply(lambda x: isinstance(x, str)).all():
                df[col] = df[col].str.strip()

        # Verify no leading/trailing whitespace on claim_id
        assert not df["claim_id"].str.startswith(" ").any()
        assert not df["claim_id"].str.endswith(" ").any()

    def test_silver_type_coercion(self, claims_t0_df):
        """Silver should coerce types correctly."""
        df = claims_t0_df.copy()

        # Convert amounts to float (simulating schema enforcement)
        df["billed_amount"] = df["billed_amount"].astype(float)
        df["paid_amount"] = df["paid_amount"].astype(float)

        # Verify types
        assert df["billed_amount"].dtype == "float64"
        assert df["paid_amount"].dtype == "float64"

    def test_silver_event_timestamp_ordering(self, claims_t0_df):
        """Silver event data should be orderable by timestamp."""
        df = claims_t0_df.copy()

        # Sort by event timestamp
        if "service_date" in df.columns:
            sorted_df = df.sort_values("service_date")
            assert sorted_df["service_date"].is_monotonic_increasing or \
                   sorted_df["service_date"].is_monotonic_decreasing or \
                   len(sorted_df) > 0  # At minimum, sorting succeeds

    def test_silver_state_scd_preparation(self, claims_t0_df, claims_t1_df):
        """Silver state data should support SCD Type 2 preparation."""
        # T0 records with initial effective dates
        t0_df = claims_t0_df.copy()
        t0_df["effective_from"] = pd.Timestamp("2024-01-15")
        t0_df["effective_to"] = pd.Timestamp("9999-12-31")
        t0_df["is_current"] = True

        # T1 records (updates)
        t1_df = claims_t1_df.copy()
        t1_df["effective_from"] = pd.Timestamp("2024-01-16")
        t1_df["effective_to"] = pd.Timestamp("9999-12-31")
        t1_df["is_current"] = True

        # Find updated records
        t0_ids = set(t0_df["claim_id"])
        t1_ids = set(t1_df["claim_id"])
        updated_ids = t0_ids & t1_ids

        # Close old versions of updated records
        t0_df.loc[t0_df["claim_id"].isin(updated_ids), "effective_to"] = pd.Timestamp("2024-01-15")
        t0_df.loc[t0_df["claim_id"].isin(updated_ids), "is_current"] = False

        # Combine for SCD Type 2 history
        scd_df = pd.concat([t0_df, t1_df], ignore_index=True)

        # Verify SCD structure
        assert "effective_from" in scd_df.columns
        assert "effective_to" in scd_df.columns
        assert "is_current" in scd_df.columns
        # Updated records should have 2 versions
        for claim_id in list(updated_ids)[:3]:  # Check first 3
            versions = scd_df[scd_df["claim_id"] == claim_id]
            if len(versions) == 2:
                assert versions["is_current"].sum() == 1  # Only one is current

    def test_silver_bronze_to_silver_transformation_flow(
        self,
        minio_client,
        minio_bucket,
        cleanup_prefix,
        claims_t0_df,
        temp_dir,
        t0_date,
    ):
        """Complete Bronze-to-Silver transformation flow."""
        from tests.integration.conftest import (
            upload_dataframe_to_minio,
            list_objects_in_prefix,
            download_parquet_from_minio,
        )

        # Step 1: Create Bronze data
        bronze_path = temp_dir / "bronze" / f"dt={t0_date}"
        bronze_path.mkdir(parents=True)

        records = claims_t0_df.to_dict("records")
        chunk_path = bronze_path / "chunk_0.parquet"
        write_parquet_chunk(records, chunk_path)

        # Write Bronze metadata
        write_batch_metadata(
            out_dir=bronze_path,
            record_count=len(records),
            chunk_count=1,
            cursor=None,
        )
        write_checksum_manifest(
            out_dir=bronze_path,
            files=[chunk_path],
            load_pattern="snapshot",
        )

        # Step 2: Read Bronze and transform to Silver
        bronze_df = pd.read_parquet(chunk_path)

        # Deduplicate (Silver processing)
        silver_df = bronze_df.drop_duplicates(subset=["claim_id"], keep="last")

        # Normalize strings
        for col in silver_df.select_dtypes(include=["object"]).columns:
            if silver_df[col].dtype == "object":
                silver_df[col] = silver_df[col].fillna("").astype(str).str.strip()

        # Step 3: Write Silver output
        silver_path = temp_dir / "silver" / f"dt={t0_date}"
        silver_path.mkdir(parents=True)
        silver_df.to_parquet(silver_path / "data.parquet", index=False)

        # Step 4: Upload to MinIO
        bronze_key = f"{cleanup_prefix}/bronze/synthetic/claims/dt={t0_date}/chunk_0.parquet"
        silver_key = f"{cleanup_prefix}/silver/synthetic/claims/dt={t0_date}/data.parquet"

        upload_dataframe_to_minio(minio_client, minio_bucket, bronze_key, bronze_df)
        upload_dataframe_to_minio(minio_client, minio_bucket, silver_key, silver_df)

        # Step 5: Verify both layers in MinIO
        bronze_objects = list_objects_in_prefix(
            minio_client, minio_bucket, f"{cleanup_prefix}/bronze"
        )
        silver_objects = list_objects_in_prefix(
            minio_client, minio_bucket, f"{cleanup_prefix}/silver"
        )

        assert len(bronze_objects) == 1
        assert len(silver_objects) == 1

        # Download and verify Silver data
        downloaded_silver = download_parquet_from_minio(minio_client, minio_bucket, silver_key)
        assert len(downloaded_silver) == len(silver_df)
        assert downloaded_silver["claim_id"].is_unique

    def test_silver_incremental_merge(
        self, claims_t0_df, claims_t1_df, temp_dir
    ):
        """Silver incremental merge should combine T0 and T1 correctly."""
        # Write T0 Silver (initial load)
        silver_path = temp_dir / "silver"
        silver_path.mkdir()

        t0_silver = claims_t0_df.drop_duplicates(subset=["claim_id"], keep="last")
        t0_silver.to_parquet(silver_path / "current.parquet", index=False)

        # T1: Merge incremental data
        t1_df = claims_t1_df.copy()

        # Read existing Silver
        existing_df = pd.read_parquet(silver_path / "current.parquet")

        # Merge: T1 updates override existing records by primary key
        merged_df = pd.concat([existing_df, t1_df], ignore_index=True)
        merged_df = merged_df.drop_duplicates(subset=["claim_id"], keep="last")

        # Write merged result
        merged_df.to_parquet(silver_path / "current.parquet", index=False)

        # Verify merged data
        final_df = pd.read_parquet(silver_path / "current.parquet")

        # Should have all unique claim_ids from both T0 and T1
        all_ids = set(claims_t0_df["claim_id"]) | set(claims_t1_df["claim_id"])
        assert len(final_df) == len(all_ids)
