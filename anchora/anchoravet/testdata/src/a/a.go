package a

import (
	"math/rand"
	"net/http"
	"os"
	"time"

	"github.com/sagnikc395/anchora/anchora"
)

// cleanWorkflow uses only recorded engine calls.
func cleanWorkflow(ctx *anchora.Context, in string) (string, error) {
	now, err := anchora.Now(ctx)
	if err != nil {
		return "", err
	}
	_ = now
	u, err := anchora.UUID(ctx)
	if err != nil {
		return "", err
	}
	return anchora.Step(ctx, "work", func(sc anchora.StepCtx) (string, error) {
		// Inside a step the whole world may be nondeterministic.
		resp, err := http.Get("https://example.com")
		if err != nil {
			return "", err
		}
		defer resp.Body.Close()
		return sc.IdempotencyKey() + u, nil
	})
}

// flakyWorkflow reads the clock directly, which replays differently.
func flakyWorkflow(ctx *anchora.Context, in string) (string, error) {
	started := time.Now() // want `nondeterministic call time.Now in workflow code`
	return anchora.Step(ctx, "work", func(sc anchora.StepCtx) (string, error) {
		return in + started.String(), nil
	})
}

// randomizedWorkflow rolls dice in the workflow body.
func randomizedWorkflow(ctx *anchora.Context, in string) (int, error) {
	n := rand.Intn(100) // want `nondeterministic call math/rand.Intn in workflow code`
	return n, nil
}

// fetchingWorkflow calls the network from the workflow body.
func fetchingWorkflow(ctx *anchora.Context, in string) (string, error) {
	resp, err := http.Get("https://example.com") // want `nondeterministic call net/http.Get in workflow code`
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	return "ok", nil
}

// envWorkflow reads configuration at replay time.
func envWorkflow(ctx *anchora.Context, in string) (string, error) {
	mode := os.Getenv("MODE") // want `nondeterministic call os.Getenv in workflow code`
	return mode + in, nil
}

// goroutineWorkflow spawns work the engine cannot checkpoint.
func goroutineWorkflow(ctx *anchora.Context, in string) (string, error) {
	go func() { // want `bare goroutine in workflow code`
		time.Sleep(time.Second) // want `nondeterministic call time.Sleep in workflow code`
	}()
	return in, nil
}

// timerWorkflow sleeps instead of using a durable timer.
func timerWorkflow(ctx *anchora.Context, in string) (string, error) {
	t := time.NewTimer(time.Second) // want `nondeterministic call time.NewTimer in workflow code`
	defer t.Stop()
	<-t.C
	return in, nil
}

// ordinaryCode is not a workflow function, so it may do anything.
func ordinaryCode() string {
	return time.Now().String() + os.Getenv("HOME")
}

// nestedWorkflow calls the clock inside a step, which is exempt.
func nestedWorkflow(ctx *anchora.Context, in string) (string, error) {
	return anchora.Step(ctx, "outer", func(sc anchora.StepCtx) (string, error) {
		return time.Now().String(), nil // ok: a step body
	})
}
