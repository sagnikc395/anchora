package jobs

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Queue is a durable, at-least-once work queue backed by Redis.
//
// A claimed job is not removed from Redis: it is moved into a lease set scored
// by its deadline and tagged with the claiming worker. A worker that crashes
// stops extending its lease, and any reaper then returns the job to the ready
// list. Every state transition runs as a Lua script so a claim, an ack, or a
// reclaim is atomic even with many workers competing.
//
// Keys, for a queue named "anchora:jobs":
//
//	anchora:jobs           LIST  ready job IDs (LPUSH / RPOP, so FIFO)
//	anchora:jobs:leases    ZSET  in-flight job ID -> lease deadline, unix ms
//	anchora:jobs:owners    HASH  in-flight job ID -> owning worker ID
//	anchora:jobs:notify    LIST  wakeup hints for blocked workers
type Queue struct {
	client                        *redis.Client
	ready, leases, owners, notify string
	waitTimeout                   time.Duration
}

const (
	// notifyBacklog caps the wakeup-hint list. Hints are advisory — the claim
	// script is the source of truth — so an oversized backlog is only waste.
	notifyBacklog = 255
	// defaultQueueName is the key prefix used when none is configured.
	defaultQueueName = "anchora:jobs"
	// claimWait bounds how long an idle worker parks on the notify list before
	// returning, so it can re-check its context between attempts.
	claimWait = time.Second
)

// claimScript moves one ready job into the lease set under a single owner. It
// returns false (redis.Nil) when nothing is ready.
var claimScript = redis.NewScript(`
local id = redis.call('RPOP', KEYS[1])
if not id then return false end
redis.call('ZADD', KEYS[2], ARGV[1], id)
redis.call('HSET', KEYS[3], id, ARGV[2])
return id
`)

// pushScript enqueues a job and wakes one blocked worker.
var pushScript = redis.NewScript(`
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('LPUSH', KEYS[2], '1')
redis.call('LTRIM', KEYS[2], 0, ARGV[2])
return 1
`)

// ackScript releases a lease permanently; the job leaves the queue.
var ackScript = redis.NewScript(`
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
return 1
`)

// requeueScript releases a lease and returns the job to the ready list.
var requeueScript = redis.NewScript(`
redis.call('ZREM', KEYS[2], ARGV[1])
redis.call('HDEL', KEYS[3], ARGV[1])
redis.call('LPUSH', KEYS[1], ARGV[1])
redis.call('LPUSH', KEYS[4], '1')
redis.call('LTRIM', KEYS[4], 0, ARGV[2])
return 1
`)

// extendScript pushes a lease deadline forward, but only for the worker that
// still owns it. Returning 0 tells a worker its lease was reclaimed.
var extendScript = redis.NewScript(`
if redis.call('HGET', KEYS[2], ARGV[1]) ~= ARGV[3] then return 0 end
if redis.call('ZSCORE', KEYS[1], ARGV[1]) == false then return 0 end
redis.call('ZADD', KEYS[1], ARGV[2], ARGV[1])
return 1
`)

// reclaimScript returns every job whose lease expired to the ready list.
var reclaimScript = redis.NewScript(`
local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', ARGV[1], 'LIMIT', 0, ARGV[2])
for _, id in ipairs(expired) do
  redis.call('ZREM', KEYS[2], id)
  redis.call('HDEL', KEYS[3], id)
  redis.call('LPUSH', KEYS[1], id)
end
if #expired > 0 then
  redis.call('LPUSH', KEYS[4], '1')
  redis.call('LTRIM', KEYS[4], 0, ARGV[3])
end
return expired
`)

// NewQueue connects to Redis and verifies the connection. An empty name uses
// the default key prefix. The caller owns the returned queue and must Close it.
func NewQueue(ctx context.Context, redisURL, name string) (*Queue, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, fmt.Errorf("parse Redis URL: %w", err)
	}
	if name == "" {
		name = defaultQueueName
	}
	client := redis.NewClient(options)
	if err := client.Ping(ctx).Err(); err != nil {
		_ = client.Close()
		return nil, fmt.Errorf("connect Redis: %w", err)
	}
	return &Queue{
		client:      client,
		ready:       name,
		leases:      name + ":leases",
		owners:      name + ":owners",
		notify:      name + ":notify",
		waitTimeout: claimWait,
	}, nil
}

// Close releases the Redis client.
func (q *Queue) Close() error { return q.client.Close() }

// Push enqueues a job ID for execution.
func (q *Queue) Push(ctx context.Context, id string) error {
	return pushScript.Run(ctx, q.client, []string{q.ready, q.notify}, id, notifyBacklog).Err()
}

// Claim leases the next ready job for owner until now+lease. It blocks for up
// to one second when the queue is empty and then returns an empty ID, so the
// caller can re-check its context between attempts.
func (q *Queue) Claim(ctx context.Context, owner string, lease time.Duration) (string, error) {
	deadline := time.Now().Add(lease).UnixMilli()
	id, err := claimScript.Run(ctx, q.client, []string{q.ready, q.leases, q.owners}, deadline, owner).Text()
	if err == nil {
		return id, nil
	}
	if !errors.Is(err, redis.Nil) {
		return "", err
	}
	// Nothing ready. Park on the notify list so a Push wakes us immediately;
	// the timeout bounds the cost of a hint that was consumed by another
	// worker, since the claim script above remains the source of truth.
	if err := q.client.BRPop(ctx, q.waitTimeout, q.notify).Err(); err != nil && !errors.Is(err, redis.Nil) {
		return "", err
	}
	return "", nil
}

// Extend pushes owner's lease on id forward. It reports false when the lease
// has already been reclaimed, meaning the worker must stop touching the job.
func (q *Queue) Extend(ctx context.Context, id, owner string, lease time.Duration) (bool, error) {
	deadline := time.Now().Add(lease).UnixMilli()
	held, err := extendScript.Run(ctx, q.client, []string{q.leases, q.owners}, id, deadline, owner).Int()
	if err != nil {
		return false, err
	}
	return held == 1, nil
}

// Ack removes a finished job from the queue.
func (q *Queue) Ack(ctx context.Context, id string) error {
	return ackScript.Run(ctx, q.client, []string{q.leases, q.owners}, id).Err()
}

// Requeue releases a lease and makes the job immediately claimable again.
func (q *Queue) Requeue(ctx context.Context, id string) error {
	return requeueScript.Run(ctx, q.client, []string{q.ready, q.leases, q.owners, q.notify}, id, notifyBacklog).Err()
}

// Reclaim returns jobs whose leases expired to the ready list and reports how
// many were recovered. Every worker may call it; the script is atomic.
func (q *Queue) Reclaim(ctx context.Context, limit int) ([]string, error) {
	if limit <= 0 {
		limit = defaultReclaimBatch
	}
	now := time.Now().UnixMilli()
	return reclaimScript.Run(ctx, q.client, []string{q.ready, q.leases, q.owners, q.notify}, now, limit, notifyBacklog).StringSlice()
}

// Depth reports the number of ready and in-flight jobs.
func (q *Queue) Depth(ctx context.Context) (ready, inFlight int64, err error) {
	if ready, err = q.client.LLen(ctx, q.ready).Result(); err != nil {
		return 0, 0, err
	}
	inFlight, err = q.client.ZCard(ctx, q.leases).Result()
	return ready, inFlight, err
}
