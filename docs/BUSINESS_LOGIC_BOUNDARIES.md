# Business Logic Boundaries

This document clarifies where different types of logic belong in the medallion architecture. The key principle: **Bronze and Silver layers should contain NO business logic.**

## The Golden Rule

> If a decision requires understanding the business domain (not just the data format), it belongs in the Gold layer.

## Decision Matrix

| Question | If YES → | If NO → |
|----------|----------|---------|
| Does this filter data based on business criteria? | Gold | Silver/Bronze |
| Does this calculate derived metrics? | Gold | Silver/Bronze |
| Does this apply business rules? | Gold | Silver/Bronze |
| Would a different business have different logic here? | Gold | Silver/Bronze |
| Is this purely technical (dedupe, type conversion, history)? | Silver/Bronze | Gold |

## Bronze Layer: Pure Extraction

### What Bronze DOES
- Extract data from sources exactly as provided
- Add technical metadata (`_extracted_at`, `_source_file`)
- Partition by date for manageability
- Track watermarks for incremental loads
- Generate checksums for data integrity

### What Bronze DOES NOT DO
- Filter records (all records extracted)
- Transform values
- Apply business rules
- Make decisions about data quality
- Join with other sources

### Example: Correct Bronze Configuration
```yaml
bronze:
  system: sales
  entity: orders
  source_type: database_mssql
  query: SELECT * FROM dbo.Orders  # Extract everything
  load_pattern: incremental
  watermark_column: modified_date
```

### Anti-Pattern: Business Logic in Bronze
```yaml
# WRONG: Filtering in extraction
bronze:
  query: SELECT * FROM dbo.Orders WHERE status = 'active'  # Business filter!
```

## Silver Layer: Technical Curation

### What Silver DOES
- Deduplicate by natural keys
- Track history (SCD Type 1 or 2)
- Enforce column selection (schema enforcement)
- Process CDC operations (I/U/D codes)
- Add technical audit columns

### What Silver DOES NOT DO
- Filter based on business rules
- Calculate derived fields
- Apply business transformations
- Make domain-specific decisions

### Example: Correct Silver Configuration
```yaml
silver:
  natural_keys: [order_id]
  change_timestamp: updated_at
  model: scd_type_2
  attributes:
    - customer_id
    - order_total
    - status       # Keep ALL statuses, not just "active"
    - created_at
```

### Anti-Patterns: Business Logic in Silver

#### 1. Business Filtering
```yaml
# WRONG: This is a business decision
silver:
  # Filtering to "active" is business logic
  exclude_where: "status = 'cancelled'"
```

#### 2. Business-Specific Quality Rules
```python
# WRONG: Business threshold in quality check
rules = [
    QualityRule("min_order", "order_total >= 10", Severity.ERROR)  # $10 minimum is business logic
]
```

The correct approach:
```python
# CORRECT: Technical quality only
rules = [
    not_null("order_id", "customer_id"),  # Data integrity
    unique_key("order_id"),               # Technical constraint
]
```

#### 3. Calculated Business Fields
```yaml
# WRONG: Adding calculated business metrics
silver:
  attributes:
    - customer_id
    - profit_margin        # This is a business calculation
    - customer_segment     # This requires business rules to determine
```

## Quality Rules: Data Quality vs Business Quality

### Data Quality (Silver Layer - OK)

| Rule | Purpose | Example |
|------|---------|---------|
| `not_null` | Ensure required fields exist | `not_null("customer_id")` |
| `unique_key` | Ensure natural key uniqueness | `unique_key("order_id")` |
| `valid_timestamp` | Ensure timestamps are parseable | `valid_timestamp("created_at")` |
| `matches_pattern` | Ensure format validity | `matches_pattern("email", r".*@.*")` |

### Business Quality (Gold Layer - NOT in Silver)

| Rule | Why It's Business Logic |
|------|------------------------|
| `order_total > 10` | Minimum order is a business policy |
| `age >= 18` | Age requirements are business rules |
| `status IN ('active', 'pending')` | Valid statuses are business-defined |
| `discount_pct <= 50` | Maximum discount is business policy |

## Delete Mode: Technical Choice with Business Implications

The `delete_mode` setting in Silver is one area where technical and business concerns overlap. Choose based on your data governance requirements:

| Mode | Behavior | When to Use |
|------|----------|-------------|
| `ignore` | Filter out deletes | When deletes are corrections, not business events |
| `tombstone` | Keep with `_deleted=true` | When you need audit trail of deletions |
| `hard_delete` | Remove from Silver | When deletions must propagate immediately |

**Note:** The choice of delete_mode should be driven by data governance requirements (audit, compliance), not business preferences. If in doubt, use `tombstone` to preserve the audit trail and let Gold layer decide how to handle deleted records.

## Late Data Handling

Late data handling is primarily technical, but the definition of "late" can be business-driven:

```python
# Technical: How to handle late data
config = LateDataConfig(mode=LateDataMode.WARN)

# Business decision: What is "late"?
# This threshold should be defined by the business
late_threshold = timedelta(days=7)  # Business-defined SLA
```

## Column Selection: Schema vs Business

### Schema Enforcement (Silver - OK)
```yaml
# Select columns to define the Silver schema
silver:
  attributes:
    - order_id
    - customer_id
    - order_total
    - status
    - created_at
```

### Business Filtering via Columns (Gold - NOT Silver)
If you're excluding columns because "users shouldn't see them" or "they're not needed for reporting", that's a business decision. Silver should include all relevant columns; Gold views can select subsets.

```sql
-- Gold: Select only columns needed for a specific use case
CREATE VIEW gold.order_summary AS
SELECT order_id, customer_id, order_total
FROM silver.orders
-- Excludes status, created_at based on business need
```

## Summary Checklist

Before adding logic to Bronze or Silver, ask:

1. **Is this domain-specific?** → Move to Gold
2. **Would another company do this differently?** → Move to Gold
3. **Does this involve business thresholds?** → Move to Gold
4. **Is this a KPI or metric?** → Move to Gold
5. **Is this purely about data format/structure?** → Keep in Silver
6. **Is this about extraction mechanics?** → Keep in Bronze

## Related Documentation

- [Gold Layer Patterns](./GOLD_LAYER_PATTERNS.md) - Examples of what belongs in Gold
- [Advanced Patterns](./ADVANCED_PATTERNS.md) - CDC, SCD, and technical patterns
- [Architecture](./ARCHITECTURE.md) - Overall system design
