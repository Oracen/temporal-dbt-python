# temporal-dbt-python

A Temporal Workflow and worker for automating DBT operations

## Project Description

This is a small learning project to let me experiment with orchestrating self-managed job workers for [DBT](https://www.getdbt.com/) within the [Temporal](https://temporal.io/) framework. It will be modelled off both the [officially supported Airflow DBT operator](https://github.com/gocardless/airflow-dbt) and [Tomas Farias's CLI-free version](https://github.com/tomasfarias/airflow-dbt-python). The intent is to build something that leverages the power of Temporal for running distributed workflows as an alternative to standard data engineering tools such as Airflow or Prefect.

This project hooks into the `dbt-core` API to execute workflows, but DBT is really built to run via the command line. Thus, some things like the `write_file` function can be monkeypatched to re-route artifacts that might otherwise fill the memory of the worker instance. I was unable to kill the logging, and so adding the log directory to your cleaning file would be advisable. It's not a terribly clean solution, but until [Fishtown Analytics finish reworking their APIs](https://github.com/dbt-labs/dbt-core/issues/5527) a quick-and-dirty solution is fine.

This project is experimental and targeted to some POC use cases I have. If you choose to rely on it, understand that it has the following limitations:

- APIs are subject to random arbitrary change
- DBT API support will be limited to what I feel I need
- I don't know if the monkeypatch will behave itself for lots of concurrent workloads in Temporal

While I can see that this limited implementation may not be enough for everyone, I hope it's at least a useful starting point for your own project.

## Example

All activities, workflows and workers work according to the Temporal framework. I'll leave researching that as an exercise to the curious reader.

See `temporal_dbt_python/workflow.py` for an example of how to use the activities within Python.

See `temporal_dbt_polyglot/workflow.go` for an example of how to use the activities within Go.

All DBT activities use an OperationRequest input that specifies a target env and a path to a project directory on the local filesystem. Workflows can be used to chain together multiple DBT operations in the same way as Airflow's DBT operations. To demonstrate this, I've written a quick Python workflow that pulls in the project's deps (assuming a fresh deployment) and runs the data build step. Note that `cls.configure` is used because Temporal will instantiate the class itself.

```python
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

from temporal_dbt_python.activities import DbtActivities
from temporal_dbt_python.dto import OperationRequest

# Operation request bundles params for serialisation


@workflow.defn
class DbtExampleWorkflow:

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

```

# Installing

To install only the Python components, run `poetry install`

For golang, use `go install`. Still figuring out Go myself.

To create a new Python workflow that runs a debug check,

## Requirements

- `python >= 3.7.2`
- `poetry >= 1.2.0`
- `temporal >= 1.18.0`
- `dbt >= 1.0.0`

To run the Go worker, be sure you have golang installed.

You'll also want a locally-running Temporal server to orchestrate the jobs. This could be [the official docker-compose](https://github.com/temporalio/docker-compose) or [Temporalite](https://github.com/temporalio/temporalite). You'll also want your favourite DB to connect to, and a ready-made DBT project to deploy to it.

## Usage

Run tests with `poetry run pytest`

This library essentially executes the DBT command line from a worker. The silly/hacked together solution essentially passes in a path to a directory as the input. The worker then navigates to the specified folder and executes the relevant command. Sophisticated, I know. This means that, for the Temporal Python SDK, you can only run a single worker per queue - too much DBT state is stored on-disk. The Go worker can leverage the `Session` API to ensure all workflow executions are on the same worker, and thus can run many executions at once.

You can start the Python worker with
`poetry run python examples/python/run_worker.py`

Then, trigger the workflow with
`tctl workflow start --workflow_type DbtRefreshWorkflow --taskqueue dbt-update-operations --input '{"env":"dev", "project_location":"./proj-dir/proj_folder"}'`

The Go worker has a slightly different name. For the Go worker, use
`tctl workflow start --workflow_type DbtParallelRefreshWorkflow --taskqueue dbt-update-operations --input '{"env":"dev", "project_location":"./proj-dir/proj_folder"}'`

See the `examples` folder for sample workers.
