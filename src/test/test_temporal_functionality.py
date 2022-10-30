import asyncio
import unittest
from unittest import mock

from temporal_dbt_python.activities import (
    DbtActivities,
    dbt_clean,
    dbt_debug,
    dbt_deps,
    dbt_docs_generate,
    dbt_run,
    dbt_test,
)
from temporal_dbt_python.dto import DbtResults, OperationRequest
from temporal_dbt_python.exceptions import WorkflowExecutionError

results_success = DbtResults(0, "log string", {"test": "results"})
results_fail = DbtResults(1, "log string", {"test": "results"})
dbt_activities = DbtActivities()

op_request = OperationRequest("dev", "./test")


@mock.patch("temporal_dbt_python.activities.dbt_handler", return_value=results_success)
class TestTemporalFunctionality(unittest.TestCase):
    def test_parse_output(self, mock_handler):
        from temporal_dbt_python.activities import parse_output

        class OutputExeption(ValueError):
            """Lazy way to check callback works"""

            pass

        def check_output_callback(*args):
            raise OutputExeption

        # should raise
        with self.assertRaises(WorkflowExecutionError):
            parse_output("id", results_fail, None)

        # should raise signal
        with self.assertRaises(OutputExeption):
            self.assertTrue(parse_output("Id", results_success, check_output_callback))

        # should return true and increment counter
        self.assertTrue(parse_output("Id", results_success))

    def test_activity_dbt_run(self, mock_handler):
        self.assertTrue(dbt_run("dev", "./test"))
        self.assertTrue(asyncio.run(dbt_activities.run(op_request)))

    def test_activity_dbt_docs_generate(self, mock_handler):
        self.assertTrue(dbt_docs_generate("dev", "./test"))
        self.assertTrue(asyncio.run(dbt_activities.docs_generate(op_request)))

    def test_activity_dbt_debug(self, mock_handler):
        self.assertTrue(dbt_debug("dev", "./test"))
        self.assertTrue(asyncio.run(dbt_activities.debug(op_request)))

    def test_activity_dbt_clean(self, mock_handler):
        self.assertTrue(dbt_clean("dev", "./test"))
        self.assertTrue(asyncio.run(dbt_activities.clean(op_request)))

    def test_activity_dbt_deps(self, mock_handler):
        self.assertTrue(dbt_deps("dev", "./test"))
        self.assertTrue(asyncio.run(dbt_activities.deps(op_request)))

    def test_activity_dbt_test(self, mock_handler):
        self.assertTrue(dbt_test("dev", "./test"))
        self.assertTrue(asyncio.run(dbt_activities.test(op_request)))

    def test_activity_dbt_test_source(self, mock_handler):
        self.assertTrue(dbt_test("dev", "./test", sources_only=True))
        self.assertTrue(asyncio.run(dbt_activities.test_source(op_request)))
