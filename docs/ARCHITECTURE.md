# Architecture

This project provides a **single Bronze extraction primitive** that can be reused across many domains and source systems.

At a high level:

```text
          +-------------------+
          |   Orchestration   |
          |  (Any scheduler   |
          |  or orchestrator) |
          +---------+---------+
                    |
                    v
           python bronze_extract.py
                    |
                    v
       +------------+------------+
       |   Config (YAML file)   |
       +------------+------------+
                    |
                    v
       +------------+------------+
       |    Extractor Selector   |
       |  (api / db / custom)   |
       +------------+------------+
                    |
                    v
       +------------+------------+
       |     Extractor (code)    |
       |  -> fetch_records()     |
       +------------+------------+
                    |
          list[dict] records, new_cursor
                    |
                    v
       +------------+------------+
       | Bronze Writer / S3 Uploader |
       +------------+------------+
                    |
                    v
bronze/system=<system>/table=<table>/dt=YYYY-MM-DD/part-0001.parquet
```

## Key components

### 1. CLI (`bronze_extract.py`)

The CLI:

- accepts `--config` (path to YAML)
- accepts `--date` (logical run date, default = today)
- loads and validates config
- builds an extractor based on `source.type`
- invokes `extractor.fetch_records(cfg, run_date)`
- writes CSV/Parquet locally
- optionally uploads to S3
- updates incremental state if configured
- exits with 0 on success, non-zero on error

### 2. Config

Each YAML file has two major sections:

- `platform`: owned by the data platform team
  - Bronze bucket/prefix
  - Partitioning conventions (e.g., `dt=YYYY-MM-DD`)
  - Output defaults (CSV vs Parquet, compression)
  - S3 connection env vars
- `source`: owned by the domain/product/data team
  - `type`: `api`, `db`, or `custom`
  - `system` and `table` identifiers
  - Extractor-specific settings (`api`, `db`, or `custom_extractor`)
  - Run-time hints (`max_rows_per_file`, `local_output_dir`, `s3_enabled`, etc.)

See `docs/CONFIG_REFERENCE.md` for details.

### 3. Extractor interface

The core extensibility point is `BaseExtractor`:

```python
class BaseExtractor(ABC):
    @abstractmethod
    def fetch_records(
        self,
        cfg: Dict[str, Any],
        run_date: date,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        ...
```

Built-in implementations:

- `ApiExtractor`: reads from REST APIs, can use `dlt` state for incremental
- `DbExtractor`: reads from databases using `pyodbc`, supports incremental via a cursor column + local state file

Custom implementations can be added by teams in their own modules and referenced via:

```yaml
source:
  type: "custom"
  custom_extractor:
    module: "my_org.custom_extractors.salesforce"
    class_name: "SalesforceExtractor"
```

### 4. Bronze layout

By default, data is written to the following logical pattern:

```text
system=<system>/table=<table>/dt=<YYYY-MM-DD>/part-0001.parquet
```

The base path is:

- Local: `<local_output_dir>/<relative_path>`
- S3: `s3://<bucket>/<prefix>/<relative_path>`

Where:

- `<system>` is from `source.system`
- `<table>` is from `source.table`
- `dt` is either the supplied `--date` or `today()`

This layout is intentionally friendly for:

- Hive-style catalogs and query engines
- Future table formats that expect partition columns
- Path-based access control policies

### 5. Incremental state

Incremental loading is supported differently for API and DB sources:

- **API**: via dlt-based state (example only; you can swap to another state store)
- **DB**: via a simple local state file storing the last cursor value

The extractor returns `(records, new_cursor)` and the runner is responsible for applying that state in a consistent way.

### 6. Orchestration neutrality

The project avoids coupling to any one orchestrator:

- All logic is in Python
- The CLI is parameterized
- No interactive input or prompts
- Logs are printed to stdout/stderr

This means you can run it from any scheduler, workflow orchestrator, or automation tool that can invoke a Python CLI.

### 7. Future evolution

The scaffold is designed so you can:

- Add concurrency / threading in the extractors
- Add row- or size-based splitting strategies
- Add metrics, OpenLineage, or OpenMetadata hooks
- Introduce more extractor types (e.g., message queues, file watchers)

See `docs/ROADMAP.md` for ideas.
