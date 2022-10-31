package app

import (
	"context"
	"fmt"
)

// AlertErrorActivity Alert on fail
func AlertErrorActivity(ctx context.Context, msg string) (bool, error) {
	_ = fmt.Sprintf("Dummy error alert sent: %s", msg)
	return true, nil
}

// AlertSuccessActivity Alert on success
func AlertSuccessActivity(ctx context.Context, msg string) (bool, error) {
	_ = fmt.Sprintf("Dummy success alert sent: %s", msg)
	return true, nil
}
