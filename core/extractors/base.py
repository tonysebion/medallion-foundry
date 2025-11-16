from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Tuple
from datetime import date


class BaseExtractor(ABC):
    """Abstract base class for all extractors.

    Implementations must return:

      - a list of dict-like records.
      - an optional new_cursor string representing incremental state.
    """

    @abstractmethod
    def fetch_records(
        self,
        cfg: Dict[str, Any],
        run_date: date,
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        ...
