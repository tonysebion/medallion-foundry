# Extending Extractors

This document explains how to add new extractor types while keeping the overall framework consistent.

The key idea:

> **All extractors implement the same interface and return data in a standard shape.**

```python
class BaseExtractor(ABC):
    @abstractmethod
    def fetch_records(
        self,
        cfg: Dict[str, Any],
        run_date: date,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """
        Returns:
          - records: list of dict-like objects
          - new_cursor: optional string representing new incremental state
        """
```

## Built-in implementations:

- `ApiExtractor`: reads from REST APIs, can use `dlt` state for incremental
- `DbExtractor`: reads from databases using `pyodbc`, supports incremental via a cursor column + local state file

See `extractors/api_extractor.py` for implementation.

## Custom extractors

To add a custom extractor:

1. Create a Python module accessible on `PYTHONPATH`. For example:

   ```text
   my_org/
     custom_extractors/
       __init__.py
       salesforce.py
   ```

2. In `salesforce.py`, implement a class that subclasses `BaseExtractor`:

   ```python
   # my_org/custom_extractors/salesforce.py

   from typing import Dict, Any, List, Optional, Tuple
   from datetime import date

   from core.extractors.base import BaseExtractor

   class SalesforceExtractor(BaseExtractor):
       def fetch_records(
           self,
           cfg: Dict[str, Any],
           run_date: date,
       ) -> Tuple[List[Dict[str, Any]], Optional[str]]:

           src = cfg["source"]
           sf_cfg = src.get("salesforce", {})

           # Example: use your vendor SDK or API here
           # client = SalesforceClient(...)
           # rows = client.query_all(sf_cfg["soql_query"])

           rows: List[Dict[str, Any]] = []  # TODO: implement real call
           cursor_field = sf_cfg.get("incremental", {}).get("cursor_field")

           max_cursor: Optional[str] = None
           for r in rows:
               if cursor_field and cursor_field in r and r[cursor_field] is not None:
                   sval = str(r[cursor_field])
                   if max_cursor is None or sval > max_cursor:
                       max_cursor = sval

           return rows, max_cursor
   ```

3. Reference it in your config:

   ```yaml
   source:
     type: "custom"
     system: "salesforce"
     table: "accounts"

     custom_extractor:
       module: "my_org.custom_extractors.salesforce"
       class_name: "SalesforceExtractor"

     salesforce:
       soql_query: "SELECT Id, Name, LastModifiedDate FROM Account"
       incremental:
         cursor_field: "LastModifiedDate"
   ```

The main runner will:

- Load the YAML
- See `source.type: "custom"`
- Import the module and class
- Instantiate the extractor
- Call `fetch_records(cfg, run_date)`
- Write the returned records into Bronze using the same standardized path/format logic

## Design guidelines for extractors

- Do not write files or talk to S3 directly; leave that to the core runner.
- Keep extractor responsibilities limited to:
  - Talking to the upstream system
  - Translating data into `dict` records
  - Determining incremental cursor state
- Use the config file as your contract with the domain team. Avoid hidden defaults when they can be explicit.
- Log minimal but meaningful info using `print("[INFO] ...")` and `print("[ERROR] ...")` so orchestration logs are clear.

By following these guidelines, new extractors can be added without changing the rest of the system.

---

## Extending Storage Backends

The framework currently supports S3-compatible storage. To add support for other storage systems (Azure Blob Storage, local filesystem, etc.), see:

**[Azure Storage Extension Example](../examples/extensions/azure_storage/README.md)**

This comprehensive example demonstrates:
- Creating a storage backend abstraction layer
- Implementing Azure Blob Storage / ADLS Gen2 support
- Maintaining backward compatibility with S3
- Making storage backends pluggable and optional

The same pattern can be used to add any storage backend while keeping the framework vendor-neutral.
