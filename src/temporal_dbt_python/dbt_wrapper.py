import io
import traceback
import warnings
from contextlib import redirect_stdout
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

from dbt.events.functions import fire_event
from dbt.events.types import MainEncounteredError, MainStackTrace
from dbt.exceptions import Exception as dbtException
from dbt.logger import log_manager
from dbt.main import handle_and_check
from dbt.utils import ExitCodes


class FileCapture:
    def __init__(self):
        """IO Interceptor to prevent DBT from writing to disk"""
        self.buffer: Dict[str, Any] = {}

    def write_file(self, path: str, contents: Dict[str, Any]):
        """Stream interceptior that redirects file writes to an internal buffer"""
        key = Path(path).stem
        self.buffer[key] = contents


@dataclass
class DbtResults:
    exit_code: int
    log_string: str
    outputs: Dict[str, Dict[str, Any]]


def invoke_dbt(args: List[str]) -> int:
    """Isolate DBT call to util function"""
    try:
        _, succeeded = handle_and_check(args)
        exit_code = (ExitCodes.Success if succeeded else ExitCodes.ModelError).value

    except BaseException as e:
        fire_event(MainEncounteredError(str(e)))
        if not isinstance(e, dbtException):
            fire_event(MainStackTrace(stack_trace=traceback.format_exc()))
        exit_code = ExitCodes.UnhandledError.value
    return exit_code


def dbt_handler(
    project_location: str,
    dbt_commands: List[str],
    profile_location: Optional[str] = None,
) -> DbtResults:
    """Wrapper interface to the DBT API"""
    import dbt.clients.system as dbt_system  # Limited context

    # Set up monkey patch to capture file writes
    file_capture = FileCapture()

    dbt_system.write_file = file_capture.write_file

    # STDOUT capture
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="logbook")
    # log_manager._file_handler.disabled = True
    handle = io.StringIO()

    args = dbt_commands + ["--project-dir", project_location]
    if profile_location is not None:
        args.extend(["--profiles-dir", profile_location])

    # Reproduce DBT call interface with printout redirect
    with log_manager.applicationbound(), redirect_stdout(handle):
        exit_code = invoke_dbt(args)
    return DbtResults(exit_code, handle.getvalue(), file_capture.buffer)
