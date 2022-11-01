package app

import (
	"errors"
	"fmt"
	"time"

	"go.temporal.io/sdk/workflow"
)

// RunParams struct for handling
type RunParams struct {
	Env             string  `json:"env"`
	ProjectLocation string  `json:"project_location"`
	ProfileLocation *string `json:"profile_location"`
}

// DbtParallelRefreshWorkflow Demo polyglot workflow with Sessions
func DbtParallelRefreshWorkflow(
	ctx workflow.Context, runParams RunParams,
) (bool, error) {
	tasks := [5]string{
		"dbt_debug",
		"dbt_deps",
		"dbt_test_source",
		"dbt_run",
		"dbt_test",
	}
	// Set up basic worker
	slowResetConfig := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute * 10,
	}
	sessionOptions := &workflow.SessionOptions{
		CreationTimeout:  time.Minute,
		ExecutionTimeout: time.Minute * 60,
	}
	sessionCtx, err := workflow.CreateSession(ctx, sessionOptions)
	if err != nil {
		return false, err
	}
	defer workflow.CompleteSession(sessionCtx)
	sessionCtx = workflow.WithActivityOptions(sessionCtx, slowResetConfig)

	// Guarantee cleanup is attempted
	defer workflow.ExecuteActivity(sessionCtx, "dbt_clean", runParams)

	// Iterate over tasks
	for _, task := range tasks {
		var result bool
		err := workflow.ExecuteActivity(sessionCtx, task, runParams).Get(sessionCtx, &result)
		fmt.Println("Python worker at step " + task + " returned " + fmt.Sprintf("%v", result))
		if err != nil {
			// On error, alert and exit
			sessionCtx = workflow.WithActivityOptions(
				sessionCtx,
				workflow.ActivityOptions{
					StartToCloseTimeout: time.Second * 30,
				},
			)
			stepIdentifier := runParams.Env + "--" + runParams.ProjectLocation + "--" + task
			workflow.ExecuteActivity(sessionCtx, AlertErrorActivity, stepIdentifier)
			return false, errors.New("Workflow failed at step " + stepIdentifier)
		}
	}
	// Alert success
	sessionCtx = workflow.WithActivityOptions(
		sessionCtx,
		workflow.ActivityOptions{
			StartToCloseTimeout: time.Second * 5,
		},
	)
	stepIdentifier := runParams.Env + "--" + runParams.ProjectLocation + "--completed"
	workflow.ExecuteActivity(sessionCtx, AlertSuccessActivity, stepIdentifier)
	return true, nil
}
