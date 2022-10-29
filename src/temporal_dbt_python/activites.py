import logging
from pathlib import Path
from typing import Callable, Optional

from temporalio import activity

from temporal_dbt_python.dbt_wrapper import DbtResults, dbt_handler
from temporal_dbt_python.exceptions import WorkflowExecutionError


def log_start_activity(env: str, step: str, project_location: str) -> str:
    """create_identifier Convenience wrapper for logging initialisation of activity

    :param env: Target environment
    :type env: str
    :param step: Name of the activity taking the step
    :type step: str
    :param project_location: File location of the DBT project
    :type project_location: str
    :return: Identification string
    :rtype: str
    """
    name = Path(project_location).stem
    identifier = f"{env}--{step}--{name}"
    logging.info(f"Commencing activity {identifier}")
    return identifier


def parse_output(
    identifier: str,
    results: DbtResults,
    store_output_callback: Optional[Callable] = None,
) -> bool:
    """parse_output Convenience wrapper for processing a DBT action's output

    :param identifier: A string summarising the activity identity
    :type identifier: str
    :param results: A `DBTResults` object returned from `dbt_handler`
    :type results: DbtResults
    :param store_output_callback: A callback to store artifacts, defaults to None
    :type store_output_callback: Optional[Callable], optional
    :raises WorkflowExecutionError: If the DBT run failed, pass up an exception
    :return: Boolean denoting success of the parsing
    :rtype: bool
    """
    if results.exit_code != 0:
        logging.error(results.log_string)
        raise WorkflowExecutionError(
            f"Error occured in {identifier} with code {results.exit_code}"
        )
    if store_output_callback is not None:
        store_output_callback(results.outputs)
        results.outputs = {}
    logging.info(f"Activity {identifier} completed successfully")
    return True


@activity
def dbt_run(
    env: str,
    project_location: str,
    profile_location: Optional[str] = None,
    prevent_writes: bool = False,
    store_output_callback: Optional[Callable] = None,
) -> bool:
    """dbt_run Implements `dbt run` in Temporal activity

    :param env: Denotes target environment to execute transform against
    :type env: str
    :param project_location: Relative filepath to the DBT project
    :type project_location: str
    :param profile_location: Filepath for DBT's `profile.yaml`, defaults to None
    :type profile_location: Optional[str], optional
    :param prevent_writes: Boolean to disable writing to file, prevents memory use,
        defaults to False
    :type prevent_writes: bool, optional
    :param store_output_callback: Allows export of DBT artifacts to external sources,
        defaults to None
    :type store_output_callback: Optional[Callable], optional
    :return: Returns a true value denoting the success of the run
    :rtype: bool
    """

    identifier = log_start_activity(env, "dbt_run", project_location)
    results = dbt_handler(
        env,
        profile_location,
        ["run", "--fail-fast"],
        project_location,
        prevent_writes=prevent_writes,
    )
    return parse_output(identifier, results, store_output_callback)


@activity
def dbt_docs_generate(
    env: str,
    project_location: str,
    profile_location: Optional[str] = None,
    prevent_writes: bool = False,
    store_output_callback: Optional[Callable] = None,
) -> bool:
    """dbt_run Implements `dbt docs generate` in Temporal activity

    :param env: Denotes target environment to execute transform against
    :type env: str
    :param project_location: Relative filepath to the DBT project
    :type project_location: str
    :param profile_location: Filepath for DBT's `profile.yaml`, defaults to None
    :type profile_location: Optional[str], optional
    :param prevent_writes: Boolean to disable writing to file, prevents memory use,
        defaults to False
    :type prevent_writes: bool, optional
    :param store_output_callback: Allows export of DBT artifacts to external sources,
        defaults to None
    :type store_output_callback: Optional[Callable], optional
    :return: Returns a true value denoting the success of the run
    :rtype: bool
    """

    identifier = log_start_activity(env, "dbt_docs_generate", project_location)
    results = dbt_handler(
        env,
        profile_location,
        ["docs", "generate"],
        project_location,
        prevent_writes=prevent_writes,
    )
    return parse_output(identifier, results, store_output_callback)


@activity
def dbt_debug(
    env: str, project_location: str, profile_location: Optional[str] = None
) -> bool:
    """dbt_run Implements `dbt debug` in Temporal activity

    :param env: Denotes target environment to execute transform against
    :type env: str
    :param project_location: Relative filepath to the DBT project
    :type project_location: str
    :param profile_location: Filepath for DBT's `profile.yaml`, defaults to None
    :type profile_location: Optional[str], optional
    :return: Returns a true value denoting the success of the run
    :rtype: bool
    """

    identifier = log_start_activity(env, "dbt_debug", project_location)
    results = dbt_handler(
        env,
        profile_location,
        ["debug"],
        project_location,
        prevent_writes=False,
    )
    return parse_output(identifier, results, None)


@activity
def dbt_clean(
    env: str, project_location: str, profile_location: Optional[str] = None
) -> bool:
    """dbt_run Implements `dbt clean` in Temporal activity

    :param env: Denotes target environment to execute transform against
    :type env: str
    :param project_location: Relative filepath to the DBT project
    :type project_location: str
    :param profile_location: Filepath for DBT's `profile.yaml`, defaults to None
    :type profile_location: Optional[str], optional
    :return: Returns a true value denoting the success of the run
    :rtype: bool
    """

    identifier = log_start_activity(env, "dbt_clean", project_location)
    results = dbt_handler(
        env,
        profile_location,
        ["clean"],
        project_location,
        prevent_writes=False,
    )
    return parse_output(identifier, results, None)


@activity
def dbt_deps(
    env: str, project_location: str, profile_location: Optional[str] = None
) -> bool:
    """dbt_run Implements `dbt deps` in Temporal activity

    :param env: Denotes target environment to execute transform against
    :type env: str
    :param project_location: Relative filepath to the DBT project
    :type project_location: str
    :param profile_location: Filepath for DBT's `profile.yaml`, defaults to None
    :type profile_location: Optional[str], optional
    :return: Returns a true value denoting the success of the run
    :rtype: bool
    """

    identifier = log_start_activity(env, "dbt_deps", project_location)
    results = dbt_handler(
        env,
        profile_location,
        ["deps"],
        project_location,
        prevent_writes=False,
    )
    return parse_output(identifier, results, None)
