from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from temporal_dbt_python.activities import DbtActivities
from temporal_dbt_python.dto import OperationRequest

# Operation request bundles params for serialisation


@workflow.defn
class DbtExampleWorkflow:
    activity_mgr: DbtActivities

    @classmethod
    def configure(
        cls,
        activity_mgr: DbtActivities,
    ):
        cls.start_to_close = timedelta(seconds=60)
        cls.activity_mgr = activity_mgr
        cls.retry_policy = RetryPolicy(maximum_attempts=3)
        return cls

    @workflow.run
    async def run(self, run_params: OperationRequest):
        await workflow.execute_activity(
            self.activity_mgr.deps,
            run_params,
            retry_policy=self.retry_policy,
            start_to_close_timeout=self.start_to_close,
        )

        return await workflow.execute_activity(
            self.activity_mgr.run,
            run_params,
            retry_policy=self.retry_policy,
            start_to_close_timeout=self.start_to_close,
        )
