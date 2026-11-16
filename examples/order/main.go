// Command order is a small end-to-end anchora example.
//
// Start the order workflow:
//
//	DATABASE_URL=postgres://anchora:anchora@localhost:5432/anchora?sslmode=disable go run ./examples/order
//
// It quotes the order, then waits for an approval message. In another
// terminal, approve it:
//
//	DATABASE_URL=postgres://anchora:anchora@localhost:5432/anchora?sslmode=disable \
//	  go run ./examples/order -approve
//
// Kill and restart either process at any point: completed steps replay from
// Postgres instead of running again, and the approval wait survives the
// restart.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/sagnikc395/anchora/anchora"
)

type order struct {
	ID    string
	Total int
}

const (
	orderID  = "order-123"
	approval = "approval"
)

func main() {
	approve := flag.Bool("approve", false, "send the approval message instead of running the order")
	flag.Parse()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		log.Fatal("set DATABASE_URL to a Postgres database")
	}

	ctx := context.Background()
	engine, err := anchora.New(ctx, anchora.Config{
		DatabaseURL: databaseURL,
		Logf:        log.Printf,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := engine.Shutdown(shutdown); err != nil {
			log.Printf("shutdown: %v", err)
		}
	}()

	// Registration must happen before the first Start.
	orders := anchora.RegisterWorkflow(engine, "process_order", func(ctx *anchora.Context, in order) (string, error) {
		quote, err := anchora.Step(ctx, "quote", func(sc anchora.StepCtx) (string, error) {
			return fmt.Sprintf("%s totaling %d", in.ID, in.Total), nil
		})
		if err != nil {
			return "", err
		}
		approved, err := anchora.Recv[bool](ctx, approval, 24*time.Hour)
		if err != nil {
			return "", err
		}
		if !approved {
			return "", fmt.Errorf("order %s rejected", in.ID)
		}
		if err := anchora.Sleep(ctx, time.Second); err != nil {
			return "", err
		}
		return anchora.Step(ctx, "ship", func(sc anchora.StepCtx) (string, error) {
			return "shipped " + quote, nil
		})
	})
	approver := anchora.RegisterWorkflow(engine, "approve_order", func(ctx *anchora.Context, in order) (string, error) {
		if err := anchora.Send(ctx, orderID, approval, true); err != nil {
			return "", err
		}
		return "approved " + in.ID, nil
	})

	if *approve {
		handle, err := approver.Start(ctx, order{ID: orderID})
		if err != nil {
			log.Fatal(err)
		}
		result, err := handle.Result(ctx)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(result)
		return
	}

	handle, err := orders.Start(ctx, order{ID: orderID, Total: 42}, anchora.WithWorkflowID(orderID))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("workflow %s started; approve it with: go run ./examples/order -approve\n", handle.ID())
	result, err := handle.Result(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result)
}
