from typing import List, Optional

from temporalio.worker import Worker

from temporal_dbt_python.activities import DbtActivities


def create_worker(
    client,
    activity_mgr: DbtActivities,
    queue_name="dbt-update-operations",
    workflows: Optional[List] = None,
    additional_tasks: Optional[List] = None,
) -> Worker:
    activities = [
        activity_mgr.run,
        activity_mgr.docs_generate,
        activity_mgr.debug,
        activity_mgr.clean,
        activity_mgr.deps,
        activity_mgr.test,
    ]

    activities.extend([] if additional_tasks is None else additional_tasks)
    worker = Worker(
        client=client,
        task_queue=queue_name,
        workflows=[] if workflows is None else workflows,
        activities=activities,
    )
    return worker
