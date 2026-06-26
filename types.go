package main

import "time"

type Status string

const (
	Pending   Status = "pending"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Retrying  Status = "retrying"
	TimedOut  Status = "timed_out"
)

type Event string

const (
	EventStart   Event = "start"
	EventSucceed Event = "succeed"
	EventFail    Event = "fail"
	EventTimeout Event = "timeout"
	EventRetry   Event = "retry"
)

type Task struct {
	ID         string
	AgentFn    func(ctx context.Context, inputs map[string]any) (map[string]any, error)
	Inputs     map[string]any
	Timeout    time.Duration
	MaxRetries int
}

type TaskState struct {
	Task      Task
	Status    Status
	Attempts  int
	Output    map[string]any
	Err       error
	UpdatedAt time.Time
}
