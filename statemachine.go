package main

import "fmt"

type Status string

const (
	Pending   Status = "pending"
	Running   Status = "running"
	Succeeded Status = "succeeded"
	Failed    Status = "failed"
	Retrying  Status = "retrying"
)

type Event string

const (
	EventStart   Event = "start"
	EventSucceed Event = "succeed"
	EventFail    Event = "fail"
	EventRetry   Event = "retry"
)

var transitions = map[Status]map[Event]Status{
	Pending: {
		EventStart: Running 
	},
	Running: {
		EventSucceed: Succeeded,
		EventFail: Failed,
	},
	Failed: {
		EventRetry: Retrying, 
	},
	Retrying: {
		EventStart: Running,
	}
}

func transition(current Status, event Event) (Status,error) {
	events, ok := transitions[current]
	if !ok {
		return current, fmt.Errorf("no transitions from %q", current)
	}
	next, ok := events[event]
	if !ok {
		return current, fmt.Errorf("event %q invalid from %q", event,current)
	}

	return next,nil
}
