package plan

import "context"

// This function can be used to fetch limits dynamically for different IDs
type PlanFunc func(ctx context.Context, ID string) (limit int, err error)
