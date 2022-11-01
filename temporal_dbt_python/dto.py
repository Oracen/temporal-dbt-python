from dataclasses import dataclass
from typing import Any, Dict, Optional


@dataclass
class OperationRequest:
    env: str
    project_location: str
    profile_location: Optional[str] = None


@dataclass
class DbtResults:
    exit_code: int
    log_string: str
    outputs: Dict[str, Dict[str, Any]]
