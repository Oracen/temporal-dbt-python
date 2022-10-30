import unittest
from unittest import mock


def mock_invoke_success(*args, **kwargs):
    import dbt.clients.system as dbt_system

    print("1")
    print("2")
    dbt_system.write_file("./test.json", {"test": "success"})
    return 0


def mock_invoke_fail(*args, **kwargs):
    import dbt.clients.system as dbt_system

    print("a")
    print("b")
    dbt_system.write_file("./test.json", {"test": "fail"})
    return 1


class TestDbtFunctionality(unittest.TestCase):
    """Some of these are a little silly - waiting on upgraded APIs"""

    def test_write_file(self):
        from temporal_dbt_python import dbt_wrapper

        file_capture = dbt_wrapper.FileCapture()
        file_capture.write_file("./local/file/example.json", {})
        file_capture.write_file("./local/file/example_2.json", {})
        self.assertSetEqual(set(file_capture.buffer.keys()), {"example", "example_2"})

    @mock.patch("dbt.main.handle_and_check", return_value=(None, True))
    def test_invoke_dbt_success(self, mock_dbt_call):
        from temporal_dbt_python.dbt_wrapper import invoke_dbt

        print(mock_dbt_call.call_count)
        self.assertEqual(invoke_dbt([]), 0)

    @mock.patch("dbt.events.functions.fire_event")
    @mock.patch("dbt.main.handle_and_check", side_effect=BaseException)
    def test_invoke_dbt_exception(self, mock_dbt_call, mock_event):
        from temporal_dbt_python.dbt_wrapper import invoke_dbt

        self.assertEqual(invoke_dbt([]), 2)

    @mock.patch(
        "temporal_dbt_python.dbt_wrapper.invoke_dbt", side_effect=mock_invoke_success
    )
    def test_dbt_handler_on_success(self, mock_invoke):
        from temporal_dbt_python.dbt_wrapper import dbt_handler

        results = dbt_handler("dev", "./test", ["run"], prevent_writes=True)
        self.assertEqual(results.exit_code, 0)
        self.assertEqual(results.log_string, "1\n2\n")
        self.assertIn("test", results.outputs)
        self.assertEqual(results.outputs["test"]["test"], "success")

    @mock.patch(
        "temporal_dbt_python.dbt_wrapper.invoke_dbt", side_effect=mock_invoke_fail
    )
    def test_dbt_handler_on_fail(self, mock_invoke):
        from temporal_dbt_python.dbt_wrapper import dbt_handler

        results = dbt_handler("dev", "./test", ["run"], prevent_writes=True)
        self.assertEqual(results.exit_code, 1)
        self.assertEqual(results.log_string, "a\nb\n")
        self.assertIn("test", results.outputs)
        self.assertEqual(results.outputs["test"]["test"], "fail")
