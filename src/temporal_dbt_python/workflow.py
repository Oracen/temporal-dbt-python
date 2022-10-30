from datetime import timedelta
from pathlib import Path
from typing import Awaitable, Callable, Dict, Optional

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

from temporal_dbt_python.activities import DbtActivities
from temporal_dbt_python.dto import OperationRequest


@workflow.defn
class DbtRefreshWorkflow:
    activity_mgr: DbtActivities

    @classmethod
    def configure(
        cls,
        n_retries: int,
        start_to_close: int,
        activity_mgr: DbtActivities,
        alert_error_activity: Optional[Callable[[str], bool]] = None,
        alert_success_activity: Optional[Callable[[str], bool]] = None,
    ):
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
        :param alert_error_activity: Notifies in case of activity failure, defaults to
            None
        :type alert_error_activity: Optional[Callable], optional
        :param alert_success_activity: Notifies on workflow success, defaults to None
        :type alert_success_activity: Optional[Callable], optional
        :return: Returns a true value denoting the success of the run
        :rtype: bool
        """
        cls.n_retries = n_retries
        cls.start_to_close = timedelta(seconds=start_to_close)
        cls.activity_mgr = activity_mgr
        cls.alert_error_activity = alert_error_activity
        cls.alert_success_activity = alert_success_activity
        cls.retry_policy = RetryPolicy(maximum_attempts=n_retries)
        return cls

    @workflow.run
    async def run(self, run_params: OperationRequest):
        tasks = [
            ("deps", self.activity_mgr.deps),
            ("debug", self.activity_mgr.debug),
            ("test_source", self.activity_mgr.test_source),
            ("run", self.activity_mgr.run),
            ("test", self.activity_mgr.test),
        ]

        try:
            for name, activity in tasks:
                await workflow.execute_activity(
                    activity,
                    run_params,
                    retry_policy=self.retry_policy,
                    start_to_close_timeout=self.start_to_close,
                )
            await self.alert_success(run_params)
        except ActivityError as ae:
            await self.alert_error(run_params, name)
            raise ApplicationError(f"Workflow failed at step {name}: {str(ae)}")
        finally:
            await workflow.execute_activity(
                self.activity_mgr.clean,
                run_params,
                retry_policy=self.retry_policy,
                start_to_close_timeout=self.start_to_close,
            )

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
        wfid = workflow.info().workflow_id  # IDs the specific workflow for follow up
        identifier = f"{wfid}--{project}--{env}--{step_id}"
        return await workflow.execute_activity(
            alert_fn,
            identifier,
            start_to_close_timeout=timedelta(start_to_close),
            retry_policy=RetryPolicy(maximum_attempts=max_attempts),
        )

    async def alert_success(self, run_params: OperationRequest):
        """alert_success Sends notification to confirm succesful execution of pipeline

        :param step_id: String denoting step of the workflow
        :type step_id: str
        """
        await self._alert(run_params, "complete", self.alert_success_activity)

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
