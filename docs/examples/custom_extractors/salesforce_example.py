"""Example of a custom extractor implementation.

This does NOT actually talk to Salesforce; it just returns fake data
to demonstrate how a custom extractor would be wired.
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import date

from core.infrastructure.io.extractors.base import BaseExtractor


class SalesforceExampleExtractor(BaseExtractor):
    def fetch_records(
        self,
        cfg: Dict[str, Any],
        run_date: date,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        # In a real implementation, use cfg["source"]["salesforce"] to read connection/query info.
        records: List[Dict[str, Any]] = [
            {
                "Id": "001",
                "Name": "Acme Corp",
                "LastModifiedDate": "2025-01-01T00:00:00Z",
            },
            {"Id": "002", "Name": "Globex", "LastModifiedDate": "2025-01-02T00:00:00Z"},
        ]

        # Example of a cursor: max LastModifiedDate
        max_cursor: Optional[str] = max(r["LastModifiedDate"] for r in records)

        return records, max_cursor
