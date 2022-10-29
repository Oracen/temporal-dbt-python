from dataclasses import dataclass
from typing import Any, Dict


@dataclass
class DbtResults:
    exit_code: int
    log_string: str
    outputs: Dict[str, Dict[str, Any]]
