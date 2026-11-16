package anchora_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/sagnikc395/anchora/anchora"
)

// The Postgres-backed tests run only when a database URL is provided:
//
//	anchora_TEST_DATABASE_URL=postgres://anchora:anchora@localhost:5432/anchora?sslmode=disable go test ./...
//
// They share one database and clean up the workflows they create.
func postgresURL(t *testing.T) string {
	t.Helper()
	url := os.Getenv("anchora_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("set anchora_TEST_DATABASE_URL to run the Postgres integration tests")
	}
	return url
}

func newPostgresEngine(t *testing.T, url, executorID string, lease time.Duration) *anchora.Engine {
	t.Helper()
	eng, err := anchora.New(context.Background(), anchora.Config{
		DatabaseURL:         url,
		ExecutorID:          executorID,
		AppVersion:          "integration",
		Lease:               lease,
		RecoveryInterval:    100 * time.Millisecond,
		MaxRecoveryAttempts: 5,
		Logf:                t.Logf,
	})
	if err != nil {
		t.Fatalf("new engine: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := eng.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	})
	return eng
}

var uniqueSeq atomic.Int64

// uniqueID builds a per-call unique id: UnixNano alone can repeat on macOS,
// whose clock ticks roughly every microsecond.
func uniqueID(prefix string) string {
	return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 10) + "-" + strconv.FormatInt(uniqueSeq.Add(1), 10)
}

func cleanupWorkflows(t *testing.T, url string, ids ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Logf("cleanup: connect: %v", err)
		return
	}
	defer pool.Close()
	for _, id := range ids {
		if _, err := pool.Exec(ctx, `DELETE FROM anchora.workflow_status WHERE workflow_id = $1`, id); err != nil {
			t.Logf("cleanup %s: %v", id, err)
		}
	}
}

func TestPostgresStepsAndTransactionalStep(t *testing.T) {
	url := postgresURL(t)
	ctx := testContext(t)
	id := uniqueID("it-tx")
	t.Cleanup(func() { cleanupWorkflows(t, url, id) })

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS anchora_test_writes (workflow_id TEXT PRIMARY KEY)`); err != nil {
		t.Fatalf("create scratch table: %v", err)
	}

	eng := newPostgresEngine(t, url, "it-exec-a", 2*time.Second)
	var calls atomic.Int32
	wf := anchora.RegisterWorkflow(eng, "it_tx_workflow", func(ctx *anchora.Context, input string) (string, error) {
		value, err := anchora.Tx(ctx, "record", func(sc anchora.StepCtx, tx anchora.TxConn) (string, error) {
			calls.Add(1)
			if _, err := tx.Exec(sc, `INSERT INTO anchora_test_writes (workflow_id) VALUES ($1) ON CONFLICT DO NOTHING`, ctx.WorkflowID()); err != nil {
				return "", err
			}
			return input, nil
		})
		if err != nil {
			return "", err
		}
		now, err := anchora.Now(ctx)
		if err != nil {
			return "", err
		}
		return fmt.Sprintf("%s@%d", value, now.Unix()), nil
	})

	h, err := wf.Start(ctx, "hello", anchora.WithWorkflowID(id))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	result := waitFor(t, h)
	if result == "" {
		t.Fatal("empty output")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("transaction calls = %d, want 1", got)
	}
	var writes int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM anchora_test_writes WHERE workflow_id = $1`, id).Scan(&writes); err != nil {
		t.Fatalf("count writes: %v", err)
	}
	if writes != 1 {
		t.Fatalf("business writes = %d, want exactly 1", writes)
	}

	rec, err := eng.Workflow(ctx, id)
	if err != nil {
		t.Fatalf("load workflow: %v", err)
	}
	if rec.Status != anchora.StatusSuccess || rec.Output == nil {
		t.Fatalf("workflow record = %+v", rec)
	}
	var kinds []string
	rows, err := pool.Query(ctx, `SELECT kind FROM anchora.operation_outputs WHERE workflow_id = $1 ORDER BY function_id`, id)
	if err != nil {
		t.Fatalf("query checkpoints: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var kind string
		if err := rows.Scan(&kind); err != nil {
			t.Fatalf("scan kind: %v", err)
		}
		kinds = append(kinds, kind)
	}
	if len(kinds) != 2 || kinds[0] != "TX" || kinds[1] != "NOW" {
		t.Fatalf("checkpoint kinds = %v, want [TX NOW]", kinds)
	}
}

func TestPostgresRecoveryAfterExecutorLoss(t *testing.T) {
	url := postgresURL(t)
	ctx := testContext(t)
	id := uniqueID("it-recovery")
	t.Cleanup(func() { cleanupWorkflows(t, url, id) })

	release := make(chan string, 1)
	var firstCalls atomic.Int32
	implementation := func(ctx *anchora.Context, input string) (string, error) {
		first, err := anchora.Step(ctx, "first", func(sc anchora.StepCtx) (string, error) {
			firstCalls.Add(1)
			return input + "-first", nil
		})
		if err != nil {
			return "", err
		}
		second, err := anchora.Step(ctx, "second", parked(release))
		if err != nil {
			return "", err
		}
		return first + "+" + second, nil
	}

	engA := newPostgresEngine(t, url, "it-exec-a", 500*time.Millisecond)
	before := anchora.RegisterWorkflow(engA, "it_recovery_workflow", implementation)
	if _, err := before.Start(ctx, "input", anchora.WithWorkflowID(id)); err != nil {
		t.Fatalf("start: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		history, err := engA.Workflow(ctx, id)
		if err != nil {
			t.Fatalf("load workflow: %v", err)
		}
		if history.Status == anchora.StatusPending {
			var count int
			pool, err := pgxpool.New(ctx, url)
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			err = pool.QueryRow(ctx, `SELECT count(*) FROM anchora.operation_outputs WHERE workflow_id = $1`, id).Scan(&count)
			pool.Close()
			if err != nil {
				t.Fatalf("count checkpoints: %v", err)
			}
			if count >= 1 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("workflow never recorded its first step")
		}
		time.Sleep(5 * time.Millisecond)
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := engA.Shutdown(shutdown); err != nil {
		t.Fatalf("shutdown first executor: %v", err)
	}

	release <- "second"
	engB := newPostgresEngine(t, url, "it-exec-b", 500*time.Millisecond)
	after := anchora.RegisterWorkflow(engB, "it_recovery_workflow", implementation)
	recovered, err := after.Start(ctx, "input", anchora.WithWorkflowID(id))
	if err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := waitFor(t, recovered); got != "input-first+second" {
		t.Fatalf("output = %q, want input-first+second", got)
	}
	if got := firstCalls.Load(); got != 1 {
		t.Fatalf("first step calls = %d, want 1 (replayed)", got)
	}
	rec, err := engB.Workflow(ctx, id)
	if err != nil {
		t.Fatalf("load recovered workflow: %v", err)
	}
	if rec.Epoch < 2 {
		t.Fatalf("epoch = %d, want at least 2 after recovery", rec.Epoch)
	}
	if rec.RecoveryAttempts != 1 {
		t.Fatalf("recovery attempts = %d, want 1", rec.RecoveryAttempts)
	}
}

func TestPostgresSendRecv(t *testing.T) {
	url := postgresURL(t)
	ctx := testContext(t)
	receiverID := uniqueID("it-receiver")
	senderID := uniqueID("it-sender")
	t.Cleanup(func() { cleanupWorkflows(t, url, receiverID, senderID) })

	eng := newPostgresEngine(t, url, "it-exec-a", 2*time.Second)
	receiver := anchora.RegisterWorkflow(eng, "it_receiver", func(ctx *anchora.Context, input string) (string, error) {
		return anchora.Recv[string](ctx, "approval", 5*time.Second)
	})
	sender := anchora.RegisterWorkflow(eng, "it_sender", func(ctx *anchora.Context, destination string) (string, error) {
		if err := anchora.Send(ctx, destination, "approval", "approved"); err != nil {
			return "", err
		}
		return "sent", nil
	})

	waiting, err := receiver.Start(ctx, "", anchora.WithWorkflowID(receiverID))
	if err != nil {
		t.Fatalf("start receiver: %v", err)
	}
	sent, err := sender.Start(ctx, receiverID, anchora.WithWorkflowID(senderID))
	if err != nil {
		t.Fatalf("start sender: %v", err)
	}
	if got := waitFor(t, sent); got != "sent" {
		t.Fatalf("sender output = %q", got)
	}
	if got := waitFor(t, waiting); got != "approved" {
		t.Fatalf("receiver output = %q, want approved", got)
	}

	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	var unconsumed int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM anchora.notifications WHERE destination_id = $1 AND consumed_at IS NULL`, receiverID).Scan(&unconsumed); err != nil {
		t.Fatalf("count notifications: %v", err)
	}
	if unconsumed != 0 {
		t.Fatalf("unconsumed messages = %d, want 0", unconsumed)
	}
}

func TestPostgresMigrationsAreIdempotent(t *testing.T) {
	url := postgresURL(t)
	eng := newPostgresEngine(t, url, "it-exec-migrate", time.Second)
	if err := eng.Launch(); err != nil {
		t.Fatalf("launch: %v", err)
	}
	second, err := anchora.New(context.Background(), anchora.Config{
		DatabaseURL: url,
		ExecutorID:  "it-exec-migrate-2",
		AppVersion:  "integration",
	})
	if err != nil {
		t.Fatalf("second engine: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := second.Shutdown(ctx); err != nil {
			t.Errorf("shutdown: %v", err)
		}
	}()
	if err := second.Launch(); err != nil {
		t.Fatalf("second launch: %v", err)
	}
	var version int
	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()
	if err := pool.QueryRow(context.Background(), `SELECT max(version) FROM anchora.schema_migrations`).Scan(&version); err != nil {
		t.Fatalf("read migrations: %v", err)
	}
	if version != 2 {
		t.Fatalf("schema version = %d, want 2", version)
	}
	// The queue columns and tables from migration 2 must exist.
	var queueCols int
	if err := pool.QueryRow(context.Background(), `
SELECT count(*) FROM information_schema.columns
WHERE table_schema = 'anchora' AND table_name = 'workflow_status'
  AND column_name IN ('queue_name', 'priority', 'partition_key', 'deadline_at', 'forked_from', 'continued_to')`).
		Scan(&queueCols); err != nil {
		t.Fatalf("read queue columns: %v", err)
	}
	if queueCols != 6 {
		t.Fatalf("queue columns present = %d, want 6", queueCols)
	}
}

var errIntentional = errors.New("intentional")

// TestPostgresTerminalErrorsAreReplayed verifies that a recorded failure is
// returned again on a later execution instead of being retried.
func TestPostgresTerminalErrorsAreReplayed(t *testing.T) {
	url := postgresURL(t)
	ctx := testContext(t)
	id := uniqueID("it-error")
	t.Cleanup(func() { cleanupWorkflows(t, url, id) })

	eng := newPostgresEngine(t, url, "it-exec-a", 2*time.Second)
	var calls atomic.Int32
	wf := anchora.RegisterWorkflow(eng, "it_failing", func(ctx *anchora.Context, input string) (string, error) {
		return anchora.Step(ctx, "fail", func(sc anchora.StepCtx) (string, error) {
			calls.Add(1)
			return "", errIntentional
		})
	})
	h, err := wf.Start(ctx, "", anchora.WithWorkflowID(id))
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := waitForError(t, h); !strings.Contains(err.Error(), "intentional") {
		// The error must be the rehydrated step error carrying the message.
		t.Fatalf("unexpected error: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("calls = %d, want 1", got)
	}
}
