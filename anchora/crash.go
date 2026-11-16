package anchora

import (
	"fmt"
	"os"
	"strings"
	"sync"
)

// anchora_CRASH_AFTER is a fault-injection hook for tests:
//
//	anchora_CRASH_AFTER=<step_name>:<before_checkpoint|after_checkpoint>
//
// When the named step reaches the named phase of its first live execution, the
// process exits immediately, leaving the store exactly as a crash would. A
// restarted executor then recovers the workflow from its checkpoints. It is a
// testing affordance only; production binaries set nothing.
var crashPoint = newCrashHook()

func newCrashHook() func(name, phase string) {
	var once sync.Once
	var stepName, stepPhase string
	return func(name, phase string) {
		once.Do(func() {
			spec := os.Getenv("anchora_CRASH_AFTER")
			if spec == "" {
				return
			}
			gotName, gotPhase, found := strings.Cut(spec, ":")
			if found {
				stepName, stepPhase = gotName, gotPhase
			}
		})
		if stepName == name && stepPhase == phase {
			fmt.Fprintf(os.Stderr, "anchora: crash injection at %s:%s\n", name, phase)
			os.Exit(1)
		}
	}
}
