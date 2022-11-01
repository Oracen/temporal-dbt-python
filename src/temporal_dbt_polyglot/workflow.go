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
		StartToCloseTimeout: time.Second * 600,
	}
	ctx = workflow.WithActivityOptions(ctx, slowResetConfig)
	defer workflow.ExecuteActivity(ctx, "dbt_clean", runParams)

	// Iterate over tasks
	for _, task := range tasks {
		var result bool
		err := workflow.ExecuteActivity(ctx, task, runParams).Get(ctx, &result)
		fmt.Println("Python worker at step " + task + " returned " + fmt.Sprintf("%v", result))
		if err != nil {
			// On error, alert and exit
			ctx = workflow.WithActivityOptions(
				ctx,
				workflow.ActivityOptions{
					StartToCloseTimeout: time.Second * 30,
				},
			)
			stepIdentifier := runParams.Env + "--" + runParams.ProjectLocation + "--" + task
			workflow.ExecuteActivity(ctx, AlertErrorActivity, stepIdentifier)
			return false, errors.New("Workflow failed at step " + stepIdentifier)
		}
	}
	// Alert success
	ctx = workflow.WithActivityOptions(
		ctx,
		workflow.ActivityOptions{
			StartToCloseTimeout: time.Second * 5,
		},
	)
	stepIdentifier := runParams.Env + "--" + runParams.ProjectLocation + "--completed"
	workflow.ExecuteActivity(ctx, AlertSuccessActivity, stepIdentifier)
	return true, nil
}
