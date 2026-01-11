# Gold Layer Patterns

This framework intentionally stops at the Silver layer. The Gold layer is where **business logic belongs** and should be implemented by your team using tools appropriate to your analytics platform (dbt, Spark, SQL views, etc.).

## Why No Gold Layer in This Framework?

The medallion architecture has clear responsibilities:

| Layer | Responsibility | This Framework |
|-------|----------------|----------------|
| **Bronze** | Raw data extraction, technical metadata | Implemented |
| **Silver** | Curation (dedupe, type enforcement, history tracking) | Implemented |
| **Gold** | Business logic, aggregations, domain models | **Not implemented** |

Gold layer is intentionally excluded because:

1. **Business logic varies wildly** - Every organization has different KPIs, metrics, and domain models
2. **Tool preferences differ** - Some teams use dbt, others use Spark, others use SQL views
3. **Separation of concerns** - Mixing business logic with curation creates maintenance nightmares

## What Belongs in Gold Layer?

### Business Calculations
```sql
-- Gold: Calculate customer lifetime value
SELECT
    customer_id,
    SUM(order_total) as lifetime_value,
    COUNT(*) as total_orders,
    AVG(order_total) as average_order_value
FROM silver.orders
GROUP BY customer_id
```

### Derived Metrics
```sql
-- Gold: Calculate churn risk score
SELECT
    c.customer_id,
    c.name,
    CASE
        WHEN DATEDIFF(day, MAX(o.order_date), GETDATE()) > 90 THEN 'High'
        WHEN DATEDIFF(day, MAX(o.order_date), GETDATE()) > 30 THEN 'Medium'
        ELSE 'Low'
    END as churn_risk
FROM silver.customers c
LEFT JOIN silver.orders o ON c.customer_id = o.customer_id
GROUP BY c.customer_id, c.name
```

### Data Filtering Based on Business Rules
```sql
-- Gold: Active customers only (business definition)
CREATE VIEW gold.active_customers AS
SELECT * FROM silver.customers
WHERE last_order_date >= DATEADD(month, -6, GETDATE())
  AND status != 'suspended'
  AND credit_limit > 0
```

### Aggregations and Summaries
```sql
-- Gold: Daily sales summary
SELECT
    CAST(order_date AS DATE) as sale_date,
    COUNT(*) as order_count,
    SUM(order_total) as total_revenue,
    AVG(order_total) as avg_order_value
FROM silver.orders
WHERE status = 'completed'  -- Business rule: only completed orders
GROUP BY CAST(order_date AS DATE)
```

### Joins Across Business Domains
```sql
-- Gold: Customer order summary with product categories
SELECT
    c.customer_id,
    c.customer_segment,
    p.category,
    SUM(oi.quantity * oi.unit_price) as category_spend
FROM silver.customers c
JOIN silver.orders o ON c.customer_id = o.customer_id
JOIN silver.order_items oi ON o.order_id = oi.order_id
JOIN silver.products p ON oi.product_id = p.product_id
WHERE c.customer_segment IN ('Premium', 'Enterprise')  -- Business filter
GROUP BY c.customer_id, c.customer_segment, p.category
```

## What Belongs in Silver Layer (NOT Gold)

### Technical Deduplication
```yaml
# Silver: Keep latest record per unique columns
silver:
  unique_columns: [customer_id]
  last_updated_column: updated_at
  model: full_merge_dedupe  # Technical deduplication
```

### History Tracking (SCD Type 2)
```yaml
# Silver: Track all historical versions
silver:
  unique_columns: [product_id]
  last_updated_column: modified_date
  model: scd_type_2  # Technical history tracking
```

### Type Enforcement
```yaml
# Silver: Ensure columns exist and have correct types
silver:
  attributes:
    - customer_id
    - name
    - email
    - created_at
```

### CDC Processing
```yaml
# Silver: Process insert/update/delete operations
bronze:
  load_pattern: cdc
  cdc_operation_column: op

silver:
  model: cdc_current_tombstone  # Technical CDC handling
```

## Anti-Patterns: Business Logic in Silver

**DON'T do this in Silver:**

```yaml
# WRONG: Business filtering in Silver
silver:
  unique_columns: [order_id]
  # This filters to "active" orders - that's a business decision!
  filter: "status = 'active'"  # DON'T DO THIS
```

```yaml
# WRONG: Business-specific columns in Silver
silver:
  attributes:
    - customer_lifetime_value  # This is a calculated business metric
    - churn_score              # This is a business calculation
```

**Instead, do this:**

```yaml
# CORRECT: Silver just curates
silver:
  unique_columns: [order_id]
  last_updated_column: updated_at
  attributes:
    - customer_id
    - order_total
    - status  # Keep all statuses, let Gold filter
```

```sql
-- CORRECT: Business logic in Gold
CREATE VIEW gold.active_orders AS
SELECT * FROM silver.orders WHERE status = 'active'
```

## Recommended Gold Layer Tools

Since this framework outputs Parquet files queryable via SQL Server PolyBase, your Gold layer options include:

### SQL Server Views
```sql
-- Create Gold views directly in SQL Server
CREATE VIEW gold.customer_360 AS
SELECT ...
FROM silver_external.customers c
JOIN silver_external.orders o ON c.customer_id = o.customer_id
```

### dbt (Data Build Tool)
```yaml
# models/gold/customer_360.sql
{{ config(materialized='table') }}

SELECT ...
FROM {{ ref('silver_customers') }}
JOIN {{ ref('silver_orders') }}
```

### Spark/Databricks
```python
# Read Silver Parquet, apply business logic
customers = spark.read.parquet("silver/customers/")
orders = spark.read.parquet("silver/orders/")

gold_df = customers.join(orders, "customer_id") \
    .groupBy("customer_id") \
    .agg(sum("order_total").alias("lifetime_value"))
```

## Summary

| Use Silver For | Use Gold For |
|----------------|--------------|
| Deduplication | Business calculations |
| Type enforcement | Domain-specific metrics |
| History tracking (SCD2) | Filtered views |
| CDC processing | Aggregations |
| Schema enforcement | Joins across domains |
| Technical metadata | KPIs and dashboards |

Keep your Silver layer clean and technical. Put all business logic in Gold where it can be versioned, tested, and evolved independently of data extraction and curation.
