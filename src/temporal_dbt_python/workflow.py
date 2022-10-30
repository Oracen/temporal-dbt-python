from pathlib import Path
from typing import Awaitable, Callable, Dict, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy

from temporal_dbt_python.activities import DbtActivities, create_notification_callback
from temporal_dbt_python.dto import OperationRequest


@workflow.defn
class DbtRefreshWorkflow:
    activity_mgr: DbtActivities

    def __init__(
        self,
        n_retries: int,
        activity_mgr: DbtActivities,
        alert_error_callback: Optional[Callable[[str], bool]] = None,
        alert_success_callback: Optional[Callable[[str], bool]] = None,
    ) -> None:
        """DbtRefreshWorkflow Executes basic DBT refresh workflow.

        Due to no `temporalio` support for sessions, this workflow should be executed
        on a single worker. Use the Go SDK to compose a session-based workflow based
        on Python activities if multiple machines are required.

        :param n_retries: Number of retries for DBT operations
        :type n_retries: int
        :param env: Denotes target environment to execute transform against
        :type env: str
        :param project_location: Relative filepath to the DBT project
        :type project_location: str
        :param profile_location: Filepath for DBT's `profile.yaml`, defaults to None
        :type profile_location: Optional[str], optional
        :param store_output_callback: Allows export of DBT artifacts to external
            sources, defaults to None
        :type store_output_callback: Optional[Callable], optional
        :param alert_error_callback: Notifies in case of activity failure, defaults to
            None
        :type alert_error_callback: Optional[Callable], optional
        :param alert_success_callback: Notifies on workflow success, defaults to None
        :type alert_success_callback: Optional[Callable], optional
        :return: Returns a true value denoting the success of the run
        :rtype: bool
        """
        self.n_retries = int
        self.activity_mgr = activity_mgr
        self.alert_error_activity = (
            create_notification_callback("error_callback", alert_error_callback)
            if alert_error_callback is not None
            else alert_error_callback
        )
        self.alert_success_activity = (
            create_notification_callback("error_callback", alert_success_callback)
            if alert_error_callback is not None
            else alert_error_callback
        )
        self.retry_policy = RetryPolicy(maximum_attempts=n_retries)

    async def run(self, run_params: OperationRequest):
        pass

    async def _alert(
        self,
        run_params: OperationRequest,
        step_id: str,
        alert_fn: Callable[[str, Dict], Awaitable[None]],
        start_to_close=30,
        max_attempts=3,
    ):
        """Internal wrapper for alert execution"""
        if alert_fn is None:
            return

        project = Path(run_params.project_location).stem  # IDs which project
        env = run_params.env  # IDs which profile target we're hititng
        run_id = workflow.info().run_id  # IDs the specific run ID for follow up
        identifier = f"{project}--{env}--{step_id}--{run_id}"
        return await workflow.execute_activity(
            alert_fn,
            identifier,
            start_to_close_timeout=start_to_close,
            retry_policy=RetryPolicy(maximum_attempts=max_attempts),
        )

    async def alert_success(self, run_params: OperationRequest, step_id: str):
        """alert_success Sends notification to confirm succesful execution of pipeline

        :param step_id: String denoting step of the workflow
        :type step_id: str
        """
        await self._alert(run_params, step_id, self.alert_success_activity)

    async def alert_error(self, run_params: OperationRequest, step_id: str):
        """alert_error Raises more durable notification in case of error

        :param step_id: String denoting step of the workflow
        :type step_id: str
        """
        await self._alert(
            run_params,
            step_id,
            self.alert_error_activity,
            start_to_close=60,
            max_attempts=5,
        )
