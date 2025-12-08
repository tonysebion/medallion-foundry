"""Pattern verification test data module.

This module provides infrastructure for generating multi-batch test data
to verify load pattern business logic (SNAPSHOT, INCREMENTAL_APPEND,
INCREMENTAL_MERGE, CURRENT_HISTORY).

Key components:
- PatternTestDataGenerator: Generates multi-batch time series data
- PatternAssertions: Configuration-driven assertion validation
- AssertionValidator: Validates data against assertions
"""

from tests.pattern_verification.pattern_data.generators import (
    PatternTestDataGenerator,
    PatternScenario,
    generate_all_pattern_scenarios,
)
from tests.pattern_verification.pattern_data.assertions import (
    AssertionResult,
    AssertionReport,
    AssertionValidator,
    PatternAssertions,
    create_snapshot_assertions,
    create_incremental_append_assertions,
    create_incremental_merge_assertions,
    create_scd2_assertions,
)

__all__ = [
    # Generators
    "PatternTestDataGenerator",
    "PatternScenario",
    "generate_all_pattern_scenarios",
    # Assertions
    "AssertionResult",
    "AssertionReport",
    "AssertionValidator",
    "PatternAssertions",
    "create_snapshot_assertions",
    "create_incremental_append_assertions",
    "create_incremental_merge_assertions",
    "create_scd2_assertions",
]
