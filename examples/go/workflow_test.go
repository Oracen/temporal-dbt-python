package app

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/internal"
	"go.temporal.io/sdk/testsuite"
)

func Test_Workflow(t *testing.T) {
	// Set up the test suite and testing execution environment
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	successInput := RunParams{"dev", "./test", nil}

	// Mock activity implementation

	env.RegisterActivityWithOptions("dbt_debug", internal.RegisterActivityOptions{Name: "dbt_debug"})

	env.OnActivity("dbt_debug").Return(true, nil)

	env.ExecuteWorkflow(DbtParallelRefreshWorkflow, successInput)
	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())

	var result bool
	require.NoError(t, env.GetWorkflowResult(&result))
	require.True(t, result)

	// successInput := RunParams{"dev", "./test", nil}

	// // Mock activity implementation
	// env.OnActivity(DbtParallelRefreshWorkflow, mock.Anything, successInput).Return(true, "nil")
}
