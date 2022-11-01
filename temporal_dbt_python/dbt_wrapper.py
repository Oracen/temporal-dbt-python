import io
import traceback
import warnings
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any, Dict, List, Optional

from temporal_dbt_python.dto import DbtResults


class FileCapture:
    def __init__(self):
        """IO Interceptor to prevent DBT from writing to disk"""
        self.buffer: Dict[str, Any] = {}

    def write_file(self, path: str, contents: Dict[str, Any]):
        """Stream interceptior that redirects file writes to an internal buffer"""
        key = Path(path).stem
        self.buffer[key] = contents


def invoke_dbt(args: List[str]) -> int:
    """Isolate DBT call to util function"""
    from dbt import exceptions
    from dbt import main as dbt_main
    from dbt.events import functions, types
    from dbt.utils import ExitCodes

    dbt_main.log_manager.set_path(None)
    try:
        _, succeeded = dbt_main.handle_and_check(args)
        exit_code = (ExitCodes.Success if succeeded else ExitCodes.ModelError).value
    except BaseException as e:
        functions.fire_event(types.MainEncounteredError(str(e)))
        if not isinstance(e, exceptions.Exception):
            functions.fire_event(
                types.MainStackTrace(stack_trace=traceback.format_exc())
            )
        exit_code = ExitCodes.UnhandledError.value
    return exit_code


def dbt_handler(
    env: str,
    project_location: str,
    dbt_commands: List[str],
    profile_location: Optional[str] = None,
    prevent_writes: bool = False,
) -> DbtResults:
    """Wrapper interface to the DBT API"""
    from logbook import Handler

    Handler.blackhole = True

    # Set up monkey patch to capture file writes
    file_capture = FileCapture()

    if prevent_writes:
        import dbt.clients.system as dbt_system  # Limited context

        file_capture = FileCapture()
        dbt_system.write_file = file_capture.write_file

    # STDOUT capture
    warnings.filterwarnings("ignore", category=DeprecationWarning, module="logbook")
    handle = io.StringIO()
    args = (
        [
            #     "--log-format",
            #     "json",
        ]
        + dbt_commands
        + [
            "--project-dir",
            project_location,
            "--target",
            env,
        ]
    )
    if profile_location is not None:
        args.extend(["--profiles-dir", profile_location])

    # Reproduce DBT call interface with printout redirect
    with redirect_stdout(handle):
        exit_code = invoke_dbt(args)
    return DbtResults(
        exit_code, handle.getvalue(), file_capture.buffer if prevent_writes else {}
    )
