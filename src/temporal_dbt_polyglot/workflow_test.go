package app

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/testsuite"
)

func mockActivity(_ RunParams) (bool, error) {
	return true, nil
}

func Test_Workflow_Success(t *testing.T) {
	// Set up the test suite and testing execution environment
	tasks := [5]string{
		"dbt_debug",
		"dbt_deps",
		"dbt_test_source",
		"dbt_run",
		"dbt_test",
	}
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	successInput := RunParams{"dev", "./test", nil}
	for _, task := range tasks {
		env.RegisterActivityWithOptions(mockActivity, activity.RegisterOptions{Name: task})
	}
	for _, task := range tasks {
		env.OnActivity(task, successInput).Return(true, nil)
	}

	env.ExecuteWorkflow(DbtParallelRefreshWorkflow, successInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result bool
	require.NoError(t, env.GetWorkflowResult(&result))
	require.True(t, result)
}

func Test_Workflow_Fail(t *testing.T) {
	// Set up the test suite and testing execution environment
	tasks := [5]string{
		"dbt_debug",
		"dbt_deps",
		"dbt_test_source",
		"dbt_run",
		"dbt_test",
	}
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	successInput := RunParams{"dev", "./test", nil}
	for _, task := range tasks {
		env.RegisterActivityWithOptions(mockActivity, activity.RegisterOptions{Name: task})
	}
	for _, task := range tasks[:4] {
		env.OnActivity(task, successInput).Return(true, nil)
	}
	// Make run fail
	env.OnActivity("dbt_test", successInput).Return(false, errors.New("An error"))

	env.ExecuteWorkflow(DbtParallelRefreshWorkflow, successInput)
	require.True(t, env.IsWorkflowCompleted())
	require.Error(t, env.GetWorkflowError())

	var result bool
	require.Error(t, env.GetWorkflowResult(&result))
}
