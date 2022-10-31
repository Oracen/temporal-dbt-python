from typing import List, Optional

from temporalio.client import Client
from temporalio.worker import Worker

from temporal_dbt_python.activities import DbtActivities


def create_worker(
    client: Client,
    activity_mgr: DbtActivities,
    queue_name="dbt-update-operations",
    workflows: Optional[List] = None,
    additional_tasks: Optional[List] = None,
) -> Worker:
    """create_worker Convenience function for instantiating worker class

    :param client: Temporal client instance
    :type client: Client
    :param activity_mgr: Instance of the activity manager class
    :type activity_mgr: DbtActivities
    :param queue_name: Queue for worker to listen on, defaults to
        "dbt-update-operations"
    :type queue_name: str, optional
    :param workflows: List of workflows for worker to handle, defaults to None
    :type workflows: Optional[List], optional
    :param additional_tasks: List of additional tasks such as alert callbacks, defaults
        to None
    :type additional_tasks: Optional[List], optional
    :return: Instance of the Worker class
    :rtype: Worker
    """
    activities = [
        activity_mgr.run,
        activity_mgr.docs_generate,
        activity_mgr.debug,
        activity_mgr.clean,
        activity_mgr.deps,
        activity_mgr.test,
        activity_mgr.test_source,
    ]

    activities.extend([] if additional_tasks is None else additional_tasks)
    worker = Worker(
        client=client,
        task_queue=queue_name,
        workflows=[] if workflows is None else workflows,
        activities=activities,
    )
    return worker
