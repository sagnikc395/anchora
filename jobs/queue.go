package jobs

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

type Queue struct {
	client            *redis.Client
	ready, processing string
}

func NewQueue(redisURL, name string) (*Queue, error) {
	options, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	if name == "" {
		name = "anchora:jobs"
	}
	return &Queue{client: redis.NewClient(options), ready: name, processing: name + ":processing"}, nil
}
func (q *Queue) Close() error { return q.client.Close() }
func (q *Queue) Push(ctx context.Context, id string) error {
	return q.client.LPush(ctx, q.ready, id).Err()
}
func (q *Queue) Pop(ctx context.Context) (string, error) {
	return q.client.BRPopLPush(ctx, q.ready, q.processing, time.Second).Result()
}
func (q *Queue) Ack(ctx context.Context, id string) error {
	return q.client.LRem(ctx, q.processing, 1, id).Err()
}
