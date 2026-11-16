// Package pgflow is a minimal stub of the engine, providing just the API
// surface the linter's testdata references. Analysis tests resolve imports
// against GOPATH-style testdata, where this stub stands in for the real
// package.
package pgflow

// Context is the workflow handle.
type Context struct{}

// StepCtx is the context handed to a step function.
type StepCtx interface {
	// IdempotencyKey is unique to the step slot.
	IdempotencyKey() string
}

// Now returns the recorded current time.
func Now(ctx *Context) (int64, error) { return 0, nil }

// UUID returns a recorded id.
func UUID(ctx *Context) (string, error) { return "", nil }

// Step runs an arbitrary side effect once and checkpoints its output.
func Step[T any](ctx *Context, name string, fn func(StepCtx) (T, error)) (T, error) {
	var zero T
	return zero, nil
}
