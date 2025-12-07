"""Schema validation and enforcement per spec Section 6.

This module provides:
- Schema validation for expected columns
- Type checking and coercion
- Schema evolution mode enforcement

Schema Evolution Modes:
- strict: Reject any schema differences
- allow_new_nullable: Allow new nullable columns
- ignore_unknown: Ignore unexpected columns

Example config:
```yaml
schema:
  expected_columns:
    - name: claim_id
      type: string
      nullable: false
    - name: amount
      type: decimal
      nullable: true
      precision: 18
      scale: 2
    - name: processed_at
      type: timestamp
      nullable: true
schema_evolution:
  mode: allow_new_nullable
  allow_type_relaxation: false
```
"""

from .validator import SchemaValidator, validate_schema
from .evolution import SchemaEvolutionMode, apply_evolution_rules
from .types import ColumnSpec, SchemaSpec, parse_schema_config

__all__ = [
    "SchemaValidator",
    "validate_schema",
    "SchemaEvolutionMode",
    "apply_evolution_rules",
    "ColumnSpec",
    "SchemaSpec",
    "parse_schema_config",
]
