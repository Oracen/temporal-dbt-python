import argparse
import asyncio

from temporal_dbt_python.activities import DbtActivities, create_notifications
from temporal_dbt_python.workers import create_worker
from temporal_dbt_python.workflow import DbtRefreshWorkflow
from temporalio.client import Client


def dummy_callback(input_str):
    print(f"Dummy alert: {input_str}")
    return True  # Signals that the callback fired successfully


async def main(client_address: str, tasks_only: bool = False):
    # Define activities including dummy callbacks
    activity_mgr = DbtActivities()
    additional_args = {}

    if not tasks_only:
        # Notifications needed for workflow
        alert_callbacks = create_notifications(dummy_callback, dummy_callback)

        # Map workflow - skip this if you need to invoke activities from another SDK
        workflow = DbtRefreshWorkflow.configure(1, 600, activity_mgr, **alert_callbacks)
        additional_args["workflows"] = [workflow]
        additional_args["additional_tasks"] = list(alert_callbacks.values())

    # Create
    client = await Client.connect(client_address)
    worker = create_worker(client, activity_mgr, **additional_args)
    print("Starting worker...")
    # Start workflow
    await worker.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-a", "--address", type=str, default="localhost:7233")
    parser.add_argument("-t", "--tasks-only", action="store_true")
    args = parser.parse_args()

    asyncio.run(main(args.address, args.tasks_only))
