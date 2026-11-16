package agent

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ErrBudgetExceeded reports that an agent run crossed its token or cost
// budget.
var ErrBudgetExceeded = errors.New("agent: budget exceeded")

// ErrMaxTurnsExceeded reports that an agent loop kept requesting tools past
// its turn limit.
var ErrMaxTurnsExceeded = errors.New("agent: turn limit exceeded")

// BudgetExceededError carries the reason a run failed its budget.
type BudgetExceededError struct{ Reason string }

func (e *BudgetExceededError) Error() string {
	return fmt.Sprintf("agent: budget exceeded: %s", e.Reason)
}

func (e *BudgetExceededError) Is(target error) bool { return target == ErrBudgetExceeded }

func jsonString(s string) string {
	quoted, err := json.Marshal(s)
	if err != nil {
		return `"tool failed"`
	}
	return string(quoted)
}

func toolError(err error) string {
	return `{"error":` + jsonString(err.Error()) + `}`
}
