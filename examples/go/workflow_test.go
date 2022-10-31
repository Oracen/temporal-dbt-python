package app

import (
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/testsuite"
)

func Test_Workflow(t *testing.T) {
	// Set up the test suite and testing execution environment
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	successInput := RunParams{"dev", "./test", nil}

	// Mock activity implementation
	// env.RegisterActivityWithOptions(MockActivity, )
	// TODO: Speak to Temporal lads about mocking externally-defined activities

	env.OnActivity("dbt_debug", mock.Anything, successInput).Return(true, nil)

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
