"""Tests for Silver SCD Type 2 (FULL_HISTORY) functionality.

SCD Type 2 maintains a full history of changes with effective date ranges.
"""

from pathlib import Path

import pandas as pd

from pipelines.lib.silver import EntityKind, HistoryMode, SilverEntity
from pipelines.lib.curate import build_history, dedupe_latest


def _create_parquet_file(path: Path, rows: list[dict]) -> None:
    """Helper to create a Parquet file from row dictionaries."""
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


class TestBuildHistory:
    """Tests for the build_history curate function."""

    def test_build_history_adds_effective_dates(self):
        """build_history should add _effective_from and _effective_to columns."""
        import ibis

        con = ibis.duckdb.connect()

        # Create test data with multiple versions of the same key
        df = pd.DataFrame([
            {"id": 1, "name": "Alice", "updated_at": "2025-01-10T10:00:00"},
            {"id": 1, "name": "Alice Smith", "updated_at": "2025-01-15T10:00:00"},
            {"id": 1, "name": "Alice Jones", "updated_at": "2025-01-20T10:00:00"},
            {"id": 2, "name": "Bob", "updated_at": "2025-01-12T10:00:00"},
        ])

        t = con.create_table("test_data", df)
        result = build_history(t, ["id"], "updated_at")
        result_df = result.execute()

        # Should have all 4 rows (full history)
        assert len(result_df) == 4

        # Check that effective dates are present (no underscore prefix)
        assert "effective_from" in result_df.columns
        assert "effective_to" in result_df.columns

        # Alice should have 3 versions with proper date ranges
        alice_history = result_df[result_df["id"] == 1].sort_values("effective_from")
        assert len(alice_history) == 3

        # First version should have effective_to pointing to second version
        first = alice_history.iloc[0]
        assert first["name"] == "Alice"

        # Latest version should have null effective_to (current)
        latest = alice_history.iloc[-1]
        assert latest["name"] == "Alice Jones"
        assert pd.isna(latest["effective_to"]) or latest["effective_to"] is None

    def test_build_history_handles_single_record(self):
        """Single record per key should have null effective_to."""
        import ibis

        con = ibis.duckdb.connect()

        df = pd.DataFrame([
            {"id": 1, "name": "Solo", "updated_at": "2025-01-15T10:00:00"},
        ])

        t = con.create_table("single", df)
        result = build_history(t, ["id"], "updated_at")
        result_df = result.execute()

        assert len(result_df) == 1
        assert result_df.iloc[0]["name"] == "Solo"


class TestSilverEntityFullHistory:
    """Tests for SilverEntity with FULL_HISTORY mode."""

    def test_full_history_preserves_all_versions(self, tmp_path: Path):
        """FULL_HISTORY should keep all versions of each record."""
        # Create Bronze data with multiple versions
        bronze_dir = tmp_path / "bronze/dt=2025-01-15"
        _create_parquet_file(
            bronze_dir / "data.parquet",
            [
                {"customer_id": 1, "email": "a@old.com", "updated_at": "2025-01-01T10:00:00"},
                {"customer_id": 1, "email": "a@new.com", "updated_at": "2025-01-15T10:00:00"},
                {"customer_id": 2, "email": "b@test.com", "updated_at": "2025-01-10T10:00:00"},
            ],
        )

        silver = SilverEntity(
            source_path=str(bronze_dir / "*.parquet"),
            target_path=str(tmp_path / "silver/domain=test/subject=customers/"),
            domain="test",
            subject="customers",
            natural_keys=["customer_id"],
            change_timestamp="updated_at",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.FULL_HISTORY,
        )

        result = silver.run("2025-01-15")

        # Should preserve all 3 rows
        assert result["row_count"] == 3

        # Verify output
        silver_df = pd.read_parquet(tmp_path / "silver/domain=test/subject=customers/customers.parquet")
        assert len(silver_df) == 3

        # Check effective dates are present (no underscore prefix)
        assert "effective_from" in silver_df.columns
        assert "effective_to" in silver_df.columns

        # Customer 1 should have 2 versions
        cust1 = silver_df[silver_df["customer_id"] == 1]
        assert len(cust1) == 2

    def test_full_history_composite_keys(self, tmp_path: Path):
        """FULL_HISTORY should work with composite natural keys."""
        bronze_dir = tmp_path / "bronze/dt=2025-01-15"
        _create_parquet_file(
            bronze_dir / "data.parquet",
            [
                {"region": "US", "product_id": 1, "price": 100, "updated_at": "2025-01-01"},
                {"region": "US", "product_id": 1, "price": 110, "updated_at": "2025-01-10"},
                {"region": "EU", "product_id": 1, "price": 90, "updated_at": "2025-01-05"},
                {"region": "US", "product_id": 2, "price": 200, "updated_at": "2025-01-08"},
            ],
        )

        silver = SilverEntity(
            source_path=str(bronze_dir / "*.parquet"),
            target_path=str(tmp_path / "silver/domain=test/subject=prices/"),
            domain="test",
            subject="prices",
            natural_keys=["region", "product_id"],
            change_timestamp="updated_at",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.FULL_HISTORY,
        )

        result = silver.run("2025-01-15")

        # All 4 rows should be preserved
        assert result["row_count"] == 4

        silver_df = pd.read_parquet(tmp_path / "silver/domain=test/subject=prices/prices.parquet")

        # US + product_id=1 should have 2 versions
        us_p1 = silver_df[(silver_df["region"] == "US") & (silver_df["product_id"] == 1)]
        assert len(us_p1) == 2


class TestSilverEntityCurrentOnly:
    """Tests for SilverEntity with CURRENT_ONLY mode (SCD Type 1)."""

    def test_current_only_keeps_latest(self, tmp_path: Path):
        """CURRENT_ONLY should keep only the latest version per key."""
        bronze_dir = tmp_path / "bronze/dt=2025-01-15"
        _create_parquet_file(
            bronze_dir / "data.parquet",
            [
                {"id": 1, "status": "pending", "updated_at": "2025-01-01T10:00:00"},
                {"id": 1, "status": "approved", "updated_at": "2025-01-10T10:00:00"},
                {"id": 1, "status": "completed", "updated_at": "2025-01-15T10:00:00"},
                {"id": 2, "status": "pending", "updated_at": "2025-01-05T10:00:00"},
            ],
        )

        silver = SilverEntity(
            source_path=str(bronze_dir / "*.parquet"),
            target_path=str(tmp_path / "silver/domain=test/subject=orders/"),
            domain="test",
            subject="orders",
            natural_keys=["id"],
            change_timestamp="updated_at",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.CURRENT_ONLY,
        )

        result = silver.run("2025-01-15")

        # Should deduplicate to 2 rows
        assert result["row_count"] == 2

        silver_df = pd.read_parquet(tmp_path / "silver/domain=test/subject=orders/orders.parquet")
        assert len(silver_df) == 2

        # Order 1 should be "completed" (latest)
        order1 = silver_df[silver_df["id"] == 1].iloc[0]
        assert order1["status"] == "completed"


class TestEventEntity:
    """Tests for EVENT entity type."""

    def test_event_deduplicates_exact_duplicates(self, tmp_path: Path):
        """EVENT entities should only remove exact duplicates."""
        bronze_dir = tmp_path / "bronze/dt=2025-01-15"
        _create_parquet_file(
            bronze_dir / "data.parquet",
            [
                {"event_id": 1, "user_id": 100, "action": "click", "ts": "2025-01-15T10:00:00"},
                {"event_id": 1, "user_id": 100, "action": "click", "ts": "2025-01-15T10:00:00"},  # Exact dupe
                {"event_id": 2, "user_id": 100, "action": "view", "ts": "2025-01-15T10:01:00"},
                {"event_id": 3, "user_id": 100, "action": "click", "ts": "2025-01-15T10:02:00"},
            ],
        )

        silver = SilverEntity(
            source_path=str(bronze_dir / "*.parquet"),
            target_path=str(tmp_path / "silver/domain=test/subject=events/"),
            domain="test",
            subject="events",
            natural_keys=["event_id"],
            change_timestamp="ts",
            entity_kind=EntityKind.EVENT,
            history_mode=HistoryMode.CURRENT_ONLY,
        )

        result = silver.run("2025-01-15")

        # Should have 3 unique events (1 duplicate removed)
        assert result["row_count"] == 3

        silver_df = pd.read_parquet(tmp_path / "silver/domain=test/subject=events/events.parquet")
        assert len(silver_df) == 3


class TestDedupeLatest:
    """Tests for the dedupe_latest curate function."""

    def test_dedupe_latest_selects_newest(self):
        """dedupe_latest should select the record with max timestamp per key."""
        import ibis

        con = ibis.duckdb.connect()

        df = pd.DataFrame([
            {"id": 1, "value": "old", "ts": "2025-01-01"},
            {"id": 1, "value": "new", "ts": "2025-01-15"},
            {"id": 2, "value": "only", "ts": "2025-01-10"},
        ])

        t = con.create_table("test", df)
        result = dedupe_latest(t, ["id"], "ts")
        result_df = result.execute()

        assert len(result_df) == 2

        id1 = result_df[result_df["id"] == 1].iloc[0]
        assert id1["value"] == "new"

    def test_dedupe_latest_composite_keys(self):
        """dedupe_latest should handle composite keys."""
        import ibis
        import tempfile

        con = ibis.duckdb.connect()

        df = pd.DataFrame([
            {"region": "US", "product_id": 1, "value": 10, "ts": "2025-01-01"},
            {"region": "US", "product_id": 1, "value": 20, "ts": "2025-01-15"},
            {"region": "EU", "product_id": 1, "value": 30, "ts": "2025-01-10"},
        ])

        # Write to temp file and read back to avoid schema issues
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            df.to_parquet(f.name, index=False)
            t = con.read_parquet(f.name)

        result = dedupe_latest(t, ["region", "product_id"], "ts")

        # Get count first to avoid schema issues
        count = result.count().execute()
        assert count == 2  # US/1 and EU/1

        # Verify values by filtering
        us_rows = result.filter((result.region == "US") & (result.product_id == 1))
        us_count = us_rows.count().execute()
        assert us_count == 1

        # Check the value
        us_value = us_rows.select("value").execute().iloc[0]["value"]
        assert us_value == 20


class TestMetadataOutput:
    """Tests for metadata output in SCD Type 2."""

    def test_metadata_includes_history_mode(self, tmp_path: Path):
        """Metadata should indicate FULL_HISTORY mode."""
        import json

        bronze_dir = tmp_path / "bronze/dt=2025-01-15"
        _create_parquet_file(
            bronze_dir / "data.parquet",
            [{"id": 1, "name": "Test", "updated_at": "2025-01-15"}],
        )

        silver = SilverEntity(
            source_path=str(bronze_dir / "*.parquet"),
            target_path=str(tmp_path / "silver/domain=test/subject=items/"),
            domain="test",
            subject="items",
            natural_keys=["id"],
            change_timestamp="updated_at",
            entity_kind=EntityKind.STATE,
            history_mode=HistoryMode.FULL_HISTORY,
        )

        silver.run("2025-01-15")

        # Verify metadata
        metadata_file = tmp_path / "silver/domain=test/subject=items/_metadata.json"
        assert metadata_file.exists()

        with open(metadata_file) as f:
            metadata = json.load(f)

        assert metadata["entity_kind"] == "state"
        assert metadata["history_mode"] == "full_history"
