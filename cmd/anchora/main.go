// Command anchora is the operator CLI for a anchora database. It reads and
// writes the workflow rows directly, the same rows the engine executes from.
//
//	DATABASE_URL=... anchora list
//	DATABASE_URL=... anchora get <id>
//	DATABASE_URL=... anchora steps <id>
//	DATABASE_URL=... anchora cancel <id>
//	DATABASE_URL=... anchora resume <id>
//	DATABASE_URL=... anchora fork <id> --from 7 --id req-123-fork
//	DATABASE_URL=... anchora gc --older-than 30d
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	ctx := context.Background()
	var err error
	switch os.Args[1] {
	case "list":
		err = list(ctx, os.Args[2:])
	case "get":
		err = get(ctx, os.Args[2:])
	case "steps":
		err = steps(ctx, os.Args[2:])
	case "cancel":
		err = cancel(ctx, os.Args[2:])
	case "resume":
		err = resume(ctx, os.Args[2:])
	case "fork":
		err = fork(ctx, os.Args[2:])
	case "gc":
		err = garbageCollect(ctx, os.Args[2:])
	default:
		usage()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "anchora:", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage: anchora <command> [flags]

commands:
  list [-status S] [-queue Q] [-limit N]   workflow rows, newest first
  get <id>                                 one workflow with its payload
  steps <id>                               the recorded operations
  cancel <id>                              mark a pending workflow cancelled
  resume <id>                              expire the lease so an executor claims it
  fork <id> --from N [--id NEWID]          copy history below N and rerun live from N
  gc --older-than 30d [-limit N]           delete terminal workflows older than the cutoff

DATABASE_URL holds the Postgres connection string.`)
	os.Exit(2)
}

func connect(ctx context.Context) (*pgxpool.Pool, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		return nil, fmt.Errorf("DATABASE_URL is not set")
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	return pool, nil
}

func list(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	status := fs.String("status", "", "filter by status")
	queue := fs.String("queue", "", "filter by queue")
	limit := fs.Int("limit", 50, "maximum rows")
	fs.Parse(args)

	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	rows, err := pool.Query(ctx, `
SELECT workflow_id, workflow_name, status, COALESCE(executor_id, ''), epoch, COALESCE(queue_name, ''),
       created_at, COALESCE(continued_to, '')
FROM anchora.workflow_status
WHERE ($1 = '' OR status = $1) AND ($2 = '' OR queue_name = $2)
ORDER BY created_at DESC LIMIT $3`, *status, *queue, *limit)
	if err != nil {
		return err
	}
	defer rows.Close()
	fmt.Printf("%-40s %-24s %-10s %-24s %8s %-16s %s\n", "ID", "NAME", "STATUS", "EXECUTOR", "EPOCH", "QUEUE", "CREATED")
	for rows.Next() {
		var id, name, status, executor, queue, continued string
		var epoch int64
		var created time.Time
		if err := rows.Scan(&id, &name, &status, &executor, &epoch, &queue, &created, &continued); err != nil {
			return err
		}
		if continued != "" {
			id += " > " + continued
		}
		fmt.Printf("%-40s %-24s %-10s %-24s %8d %-16s %s\n", id, name, status, executor, epoch, queue, created.Format(time.RFC3339))
	}
	return rows.Err()
}

func get(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: anchora get <id>")
	}
	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	row := pool.QueryRow(ctx, `
SELECT w.workflow_id, w.workflow_name, w.status, w.app_version, COALESCE(w.executor_id, ''), w.epoch,
       w.recovery_attempts, COALESCE(w.parent_workflow_id, ''), COALESCE(w.forked_from, ''),
       COALESCE(w.continued_to, ''), COALESCE(w.queue_name, ''), w.priority, COALESCE(w.partition_key, ''),
       w.created_at, w.updated_at, COALESCE(i.output, ''), i.error
FROM anchora.workflow_status w
LEFT JOIN anchora.workflow_io i ON i.workflow_id = w.workflow_id
WHERE w.workflow_id = $1`, args[0])
	var id, name, status, version, executor, parent, forkedFrom, continued, queue, partition string
	var epoch int64
	var attempts int
	var priority int
	var created, updated time.Time
	var output, wfErr string
	if err := row.Scan(&id, &name, &status, &version, &executor, &epoch, &attempts, &parent,
		&forkedFrom, &continued, &queue, &priority, &partition, &created, &updated, &output, &wfErr); err != nil {
		return err
	}
	fmt.Printf("id:          %s\nname:        %s\nstatus:      %s\napp_version: %s\nexecutor:    %s\nepoch:       %d\nrecovery:    %d attempts\nqueue:       %s priority %d partition %s\nparent:      %s\nforked from: %s\ncontinued to:%s\ncreated:     %s\nupdated:     %s\noutput:      %s\nerror:       %s\n",
		id, name, status, version, executor, epoch, attempts, queue, priority, partition, parent, forkedFrom, continued, created.Format(time.RFC3339), updated.Format(time.RFC3339), string(output), string(wfErr))
	return nil
}

func steps(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: anchora steps <id>")
	}
	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	rows, err := pool.Query(ctx, `
SELECT function_id, function_name, kind, attempts, COALESCE(child_workflow_id, ''),
       completed_at, COALESCE(output, ''), COALESCE(error::text, '')
FROM anchora.operation_outputs
WHERE workflow_id = $1 ORDER BY function_id`, args[0])
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var functionID, attemptsN int
		var name, kind, child, output, wfErr string
		var completed time.Time
		if err := rows.Scan(&functionID, &name, &kind, &attemptsN, &child, &completed, &output, &wfErr); err != nil {
			return err
		}
		fmt.Printf("%3d  %-8s %-24s attempts=%d %s", functionID, kind, name, attemptsN, completed.Format(time.RFC3339))
		if child != "" {
			fmt.Printf(" child=%s", child)
		}
		if wfErr != "" {
			fmt.Printf(" error=%s", wfErr)
		} else if len(output) > 0 {
			fmt.Printf(" output=%s", firstLine(output))
		}
		fmt.Println()
	}
	return rows.Err()
}

// firstLine keeps step output readable in a terminal.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	const max = 120
	if len(s) > max {
		return s[:max] + "…"
	}
	return s
}

func cancel(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: anchora cancel <id>")
	}
	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	tag, err := pool.Exec(ctx, `
UPDATE anchora.workflow_status SET status = 'CANCELLED', updated_at = now()
WHERE workflow_id = $1 AND status IN ('ENQUEUED', 'PENDING')`, args[0])
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%s is not pending", args[0])
	}
	fmt.Printf("cancelled %s\n", args[0])
	return nil
}

func resume(ctx context.Context, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: anchora resume <id>")
	}
	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	tag, err := pool.Exec(ctx, `
UPDATE anchora.workflow_status SET lease_expires_at = now(), updated_at = now()
WHERE workflow_id = $1 AND status = 'PENDING'`, args[0])
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%s is not pending", args[0])
	}
	fmt.Printf("lease of %s expired; an executor of its app version will claim it\n", args[0])
	return nil
}

func fork(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("fork", flag.ExitOnError)
	from := fs.Int("from", 0, "replay slots below this function id (required)")
	newID := fs.String("id", "", "id of the fork (default: <source>-fork-N)")
	fs.Parse(args)
	if fs.NArg() != 1 || *from < 0 {
		return fmt.Errorf("usage: anchora fork <id> --from N [--id NEWID]")
	}
	src := fs.Arg(0)
	if *newID == "" {
		*newID = fmt.Sprintf("%s-fork-%d", src, *from)
	}
	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	tag, err := tx.Exec(ctx, `
INSERT INTO anchora.workflow_status
    (workflow_id, workflow_name, status, app_version, lease_expires_at, epoch, forked_from)
SELECT $2, workflow_name, 'PENDING', app_version, now(), 0, $1
FROM anchora.workflow_status WHERE workflow_id = $1`, src, *newID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%s not found", src)
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO anchora.workflow_io (workflow_id, inputs) SELECT $2, inputs FROM anchora.workflow_io WHERE workflow_id = $1`,
		src, *newID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO anchora.operation_outputs
    (workflow_id, function_id, function_name, kind, output, error, attempts, child_workflow_id, started_at, completed_at)
SELECT $2, function_id, function_name, kind, output, error, attempts, child_workflow_id, started_at, completed_at
FROM anchora.operation_outputs WHERE workflow_id = $1 AND function_id < $3`, src, *newID, *from); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	fmt.Printf("forked %s below function %d into %s (pending; executors of the source's app version will run it)\n", src, *from, *newID)
	return nil
}

func garbageCollect(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("gc", flag.ExitOnError)
	older := fs.String("older-than", "30d", "delete terminal workflows whose last update is older than this (e.g. 30d, 12h)")
	limit := fs.Int("limit", 1000, "maximum rows per batch")
	fs.Parse(args)
	cutoff, err := parseDuration(*older)
	if err != nil {
		return err
	}
	pool, err := connect(ctx)
	if err != nil {
		return err
	}
	defer pool.Close()
	deadline := time.Now().Add(-cutoff)
	total := 0
	for {
		tag, err := pool.Exec(ctx, `
DELETE FROM anchora.workflow_status WHERE workflow_id IN (
    SELECT workflow_id FROM anchora.workflow_status
    WHERE status IN ('SUCCESS', 'ERROR', 'CANCELLED', 'RETRIES_EXCEEDED') AND updated_at < $1
    LIMIT $2)`, deadline, *limit)
		if err != nil {
			return err
		}
		deleted := int(tag.RowsAffected())
		total += deleted
		if deleted < *limit {
			break
		}
	}
	fmt.Printf("deleted %d terminal workflows updated before %s\n", total, deadline.Format(time.RFC3339))
	return nil
}

// parseDuration accepts plain Go durations and a day suffix the CLI users
// expect: 30d.
func parseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		days, err := strconv.Atoi(strings.TrimSuffix(s, "d"))
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}
	return time.ParseDuration(s)
}
