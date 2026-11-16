// Package memory provides an in-memory pgflow.Store for tests. State lives in
// the process and disappears when it exits; durability is the Postgres store's
// job. The implementation mirrors the Postgres store's fencing and
// first-writer-wins semantics so replay logic can be tested without a database.
package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sagnikc395/anchora/pgflow"
)

// Store is an in-memory pgflow.Store.
type Store struct {
	mu          sync.Mutex
	workflows   map[string]*pgflow.WorkflowRecord
	checkpoints map[string]map[int]*pgflow.Checkpoint
	messages    map[string]*pgflow.Message
	consumed    map[string]time.Time
	attempts    map[string]struct{}
	events      map[string]map[string][]byte
	streams     map[streamKey][]pgflow.StreamChunk
	now         func() time.Time
}

type streamKey struct{ workflowID, key string }

var _ pgflow.Store = (*Store)(nil)

// New returns an empty store.
func New() *Store {
	return &Store{
		workflows:   make(map[string]*pgflow.WorkflowRecord),
		checkpoints: make(map[string]map[int]*pgflow.Checkpoint),
		messages:    make(map[string]*pgflow.Message),
		consumed:    make(map[string]time.Time),
		attempts:    make(map[string]struct{}),
		events:      make(map[string]map[string][]byte),
		streams:     make(map[streamKey][]pgflow.StreamChunk),
		now:         time.Now,
	}
}

// SetNow replaces the store's clock, for tests that need to move time
// forward (lease expiry windows, rate-limit periods).
func (s *Store) SetNow(now func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.now = now
}

// ExpireLeases marks every pending lease as already expired, so a recovery
// sweep picks the workflows up. It exists so tests do not have to wait out a
// real lease.
func (s *Store) ExpireLeases() {
	s.mu.Lock()
	defer s.mu.Unlock()
	expired := s.now().Add(-time.Second)
	for _, rec := range s.workflows {
		if rec.Status == pgflow.StatusPending {
			at := expired
			rec.LeaseExpiresAt = &at
		}
	}
}

func (s *Store) CreateWorkflow(ctx context.Context, rec pgflow.WorkflowRecord, lease time.Duration) (bool, *pgflow.WorkflowRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.workflows[rec.ID]; ok {
		return false, copyRecord(existing), nil
	}
	now := s.now()
	stored := &pgflow.WorkflowRecord{
		ID:               rec.ID,
		Name:             rec.Name,
		AppVersion:       rec.AppVersion,
		RecoveryAttempts: 0,
		ParentWorkflowID: rec.ParentWorkflowID,
		ParentFunctionID: rec.ParentFunctionID,
		ForkedFrom:       rec.ForkedFrom,
		QueueName:        rec.QueueName,
		Priority:         rec.Priority,
		PartitionKey:     rec.PartitionKey,
		Inputs:           append([]byte(nil), rec.Inputs...),
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if rec.Status == pgflow.StatusEnqueued {
		stored.Status = pgflow.StatusEnqueued
		stored.Epoch = 1
	} else {
		stored.Status = pgflow.StatusPending
		expires := now.Add(lease)
		stored.ExecutorID = rec.ExecutorID
		stored.LeaseExpiresAt = &expires
		stored.Epoch = 1
		stored.StartedAt = &now
	}
	s.workflows[rec.ID] = stored
	return true, nil, nil
}

func (s *Store) GetWorkflow(ctx context.Context, workflowID string) (*pgflow.WorkflowRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.workflows[workflowID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", pgflow.ErrWorkflowNotFound, workflowID)
	}
	return copyRecord(rec), nil
}

func (s *Store) LoadCheckpoints(ctx context.Context, workflowID string) (map[int]*pgflow.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	history := make(map[int]*pgflow.Checkpoint)
	for id, cp := range s.checkpoints[workflowID] {
		history[id] = copyCheckpoint(cp)
	}
	return history, nil
}

func (s *Store) SaveCheckpoint(ctx context.Context, workflowID string, epoch int64, cp *pgflow.Checkpoint) (bool, *pgflow.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.checkpoints[workflowID][cp.FunctionID]; ok {
		return false, copyCheckpoint(existing), nil
	}
	rec, ok := s.workflows[workflowID]
	if !ok {
		return false, nil, fmt.Errorf("%w: %s", pgflow.ErrWorkflowNotFound, workflowID)
	}
	if rec.Epoch != epoch {
		return false, nil, pgflow.ErrFenced
	}
	if s.checkpoints[workflowID] == nil {
		s.checkpoints[workflowID] = make(map[int]*pgflow.Checkpoint)
	}
	s.checkpoints[workflowID][cp.FunctionID] = copyCheckpoint(cp)
	return true, nil, nil
}

func (s *Store) CompleteWorkflow(ctx context.Context, workflowID string, epoch int64, status pgflow.Status, output []byte, wfErr *pgflow.ErrorInfo) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.workflows[workflowID]
	if !ok {
		return false, fmt.Errorf("%w: %s", pgflow.ErrWorkflowNotFound, workflowID)
	}
	if rec.Epoch != epoch {
		return false, nil
	}
	if rec.Status != pgflow.StatusPending {
		// A cancelled (or otherwise finished) workflow keeps its state:
		// operator cancellation wins over the executor's completion.
		return false, nil
	}
	rec.Status = status
	rec.Output = append([]byte(nil), output...)
	rec.Error = wfErr
	rec.LeaseExpiresAt = nil
	rec.UpdatedAt = s.now()
	return true, nil
}

func (s *Store) CancelWorkflow(ctx context.Context, workflowID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.workflows[workflowID]
	if !ok {
		return false, fmt.Errorf("%w: %s", pgflow.ErrWorkflowNotFound, workflowID)
	}
	if rec.Status != pgflow.StatusPending && rec.Status != pgflow.StatusEnqueued {
		return false, nil
	}
	rec.Status = pgflow.StatusCancelled
	rec.Error = &pgflow.ErrorInfo{Type: "cancelled", Message: pgflow.ErrCancelled.Error()}
	rec.UpdatedAt = s.now()
	return true, nil
}

func (s *Store) ClaimExpired(ctx context.Context, req pgflow.ClaimExpiredRequest) ([]pgflow.WorkflowRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Limit <= 0 {
		return nil, nil
	}
	now := s.now()
	ids := make([]string, 0, len(s.workflows))
	for id, rec := range s.workflows {
		if rec.Status != pgflow.StatusPending || rec.AppVersion != req.AppVersion {
			continue
		}
		expired := rec.LeaseExpiresAt == nil || rec.LeaseExpiresAt.Before(now)
		if !expired && !(req.IncludeOwned && rec.ExecutorID == req.ExecutorID) {
			continue
		}
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		left, right := s.workflows[ids[i]], s.workflows[ids[j]]
		if !left.CreatedAt.Equal(right.CreatedAt) {
			return left.CreatedAt.Before(right.CreatedAt)
		}
		return left.ID < right.ID
	})
	if len(ids) > req.Limit {
		ids = ids[:req.Limit]
	}
	records := make([]pgflow.WorkflowRecord, 0, len(ids))
	for _, id := range ids {
		rec := s.workflows[id]
		expires := now.Add(req.Lease)
		rec.Epoch++
		rec.RecoveryAttempts++
		rec.ExecutorID = req.ExecutorID
		rec.LeaseExpiresAt = &expires
		rec.UpdatedAt = now
		if rec.StartedAt == nil {
			started := now
			rec.StartedAt = &started
		}
		records = append(records, *copyRecord(rec))
	}
	return records, nil
}

func (s *Store) RenewLeases(ctx context.Context, executorID string, lease time.Duration, workflowIDs []string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	var owned []string
	for _, id := range workflowIDs {
		rec, ok := s.workflows[id]
		if !ok || rec.ExecutorID != executorID || rec.Status != pgflow.StatusPending {
			continue
		}
		expires := now.Add(lease)
		rec.LeaseExpiresAt = &expires
		rec.UpdatedAt = now
		owned = append(owned, id)
	}
	return owned, nil
}

func (s *Store) StartStepAttempt(ctx context.Context, workflowID string, functionID, attempt int, executorID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts[attemptKey(workflowID, functionID, attempt)] = struct{}{}
	return nil
}

func (s *Store) HeartbeatStep(ctx context.Context, workflowID string, functionID, attempt int, details []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attempts[attemptKey(workflowID, functionID, attempt)] = struct{}{}
	return nil
}

func attemptKey(workflowID string, functionID, attempt int) string {
	return fmt.Sprintf("%s:%d:%d", workflowID, functionID, attempt)
}

func (s *Store) TxStep(ctx context.Context, workflowID string, epoch int64, cp *pgflow.Checkpoint, fn func(context.Context, pgflow.TxConn) error) (bool, *pgflow.Checkpoint, error) {
	s.mu.Lock()
	if existing, ok := s.checkpoints[workflowID][cp.FunctionID]; ok {
		s.mu.Unlock()
		return false, copyCheckpoint(existing), nil
	}
	rec, ok := s.workflows[workflowID]
	if !ok {
		s.mu.Unlock()
		return false, nil, fmt.Errorf("%w: %s", pgflow.ErrWorkflowNotFound, workflowID)
	}
	if rec.Epoch != epoch {
		s.mu.Unlock()
		return false, nil, pgflow.ErrFenced
	}
	s.mu.Unlock()

	// The memory store has no transaction to share; fn runs on its own and the
	// checkpoint is only recorded if fn succeeds.
	if err := fn(ctx, nil); err != nil {
		return false, nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.checkpoints[workflowID] == nil {
		s.checkpoints[workflowID] = make(map[int]*pgflow.Checkpoint)
	}
	s.checkpoints[workflowID][cp.FunctionID] = copyCheckpoint(cp)
	return true, nil, nil
}

func (s *Store) SendMessage(ctx context.Context, workflowID string, epoch int64, cp *pgflow.Checkpoint, msg pgflow.Message) (bool, *pgflow.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.checkpoints[workflowID][cp.FunctionID]; ok {
		return false, copyCheckpoint(existing), nil
	}
	rec, ok := s.workflows[workflowID]
	if !ok {
		return false, nil, fmt.Errorf("%w: %s", pgflow.ErrWorkflowNotFound, workflowID)
	}
	if rec.Epoch != epoch {
		return false, nil, pgflow.ErrFenced
	}
	if _, exists := s.messages[msg.ID]; !exists {
		now := s.now()
		msg.CreatedAt = now
		s.messages[msg.ID] = &msg
	}
	if s.checkpoints[workflowID] == nil {
		s.checkpoints[workflowID] = make(map[int]*pgflow.Checkpoint)
	}
	s.checkpoints[workflowID][cp.FunctionID] = copyCheckpoint(cp)
	return true, nil, nil
}

func (s *Store) ReceiveMessage(ctx context.Context, workflowID string, epoch int64, cp *pgflow.Checkpoint, destinationID, topic string) (*pgflow.Message, bool, *pgflow.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.checkpoints[workflowID][cp.FunctionID]; ok {
		return nil, false, copyCheckpoint(existing), nil
	}
	rec, ok := s.workflows[workflowID]
	if !ok {
		return nil, false, nil, fmt.Errorf("%w: %s", pgflow.ErrWorkflowNotFound, workflowID)
	}
	if rec.Epoch != epoch {
		return nil, false, nil, pgflow.ErrFenced
	}

	var pending []*pgflow.Message
	for id, msg := range s.messages {
		if _, used := s.consumed[id]; used {
			continue
		}
		if msg.DestinationID != destinationID || msg.Topic != topic {
			continue
		}
		pending = append(pending, msg)
	}
	if len(pending) == 0 {
		return nil, false, nil, nil
	}
	sort.Slice(pending, func(i, j int) bool {
		if !pending[i].CreatedAt.Equal(pending[j].CreatedAt) {
			return pending[i].CreatedAt.Before(pending[j].CreatedAt)
		}
		return pending[i].ID < pending[j].ID
	})
	msg := pending[0]
	s.consumed[msg.ID] = s.now()
	cp.Output = append([]byte(nil), msg.Payload...)
	if s.checkpoints[workflowID] == nil {
		s.checkpoints[workflowID] = make(map[int]*pgflow.Checkpoint)
	}
	s.checkpoints[workflowID][cp.FunctionID] = copyCheckpoint(cp)
	return copyMessage(msg), true, copyCheckpoint(cp), nil
}

func (s *Store) DequeueWorkflows(ctx context.Context, req pgflow.DequeueRequest) ([]pgflow.WorkflowRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Limit <= 0 {
		return nil, nil
	}
	now := s.now()
	budget := req.Limit
	if req.GlobalConcurrency > 0 {
		pending := 0
		for _, rec := range s.workflows {
			if rec.QueueName == req.Queue && rec.Status == pgflow.StatusPending {
				pending++
			}
		}
		if pending >= req.GlobalConcurrency {
			return nil, nil
		}
		budget = min(budget, req.GlobalConcurrency-pending)
	}
	if req.RateLimitCount > 0 && req.RateLimitPeriod > 0 {
		started := 0
		for _, rec := range s.workflows {
			if rec.QueueName != req.Queue || rec.StartedAt == nil {
				continue
			}
			if now.Sub(*rec.StartedAt) < req.RateLimitPeriod {
				started++
			}
		}
		if started >= req.RateLimitCount {
			return nil, nil
		}
		// A sweep may only spend the rate budget left in this window.
		budget = min(budget, req.RateLimitCount-started)
	}
	if budget <= 0 {
		return nil, nil
	}
	// Partitions in order of their oldest workflow, mirroring the Postgres
	// sweep so one partition cannot starve the others.
	type partition struct {
		key     string
		hasKey  bool
		pending int
		oldest  time.Time
	}
	partitions := map[string]*partition{}
	for _, rec := range s.workflows {
		if rec.QueueName != req.Queue || (rec.Status != pgflow.StatusEnqueued && rec.Status != pgflow.StatusPending) {
			continue
		}
		p, ok := partitions[rec.PartitionKey]
		if !ok {
			p = &partition{key: rec.PartitionKey, hasKey: rec.PartitionKey != "", oldest: rec.CreatedAt}
			partitions[rec.PartitionKey] = p
		}
		if rec.CreatedAt.Before(p.oldest) {
			p.oldest = rec.CreatedAt
		}
		if rec.Status == pgflow.StatusPending {
			p.pending++
		}
	}
	keys := make([]string, 0, len(partitions))
	for key := range partitions {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		return partitions[keys[i]].oldest.Before(partitions[keys[j]].oldest)
	})

	var ids []string
	for _, key := range keys {
		if len(ids) >= budget {
			break
		}
		p := partitions[key]
		take := budget - len(ids)
		if req.PartitionConcurrency > 0 {
			capacity := req.PartitionConcurrency - p.pending
			if capacity <= 0 {
				continue
			}
			if capacity < take {
				take = capacity
			}
		}
		// Priority order, then age order, over this partition's ENQUEUED rows.
		var candidates []string
		for id, rec := range s.workflows {
			if rec.QueueName != req.Queue || rec.Status != pgflow.StatusEnqueued || rec.PartitionKey != key {
				continue
			}
			if rec.AppVersion != req.AppVersion {
				continue
			}
			candidates = append(candidates, id)
		}
		sort.Slice(candidates, func(i, j int) bool {
			left, right := s.workflows[candidates[i]], s.workflows[candidates[j]]
			if left.Priority != right.Priority {
				return left.Priority > right.Priority
			}
			if !left.CreatedAt.Equal(right.CreatedAt) {
				return left.CreatedAt.Before(right.CreatedAt)
			}
			return left.ID < right.ID
		})
		if len(candidates) > take {
			candidates = candidates[:take]
		}
		ids = append(ids, candidates...)
	}

	records := make([]pgflow.WorkflowRecord, 0, len(ids))
	for _, id := range ids {
		rec := s.workflows[id]
		expires := now.Add(req.Lease)
		rec.Status = pgflow.StatusPending
		rec.Epoch++
		rec.ExecutorID = req.ExecutorID
		rec.LeaseExpiresAt = &expires
		rec.UpdatedAt = now
		if rec.StartedAt == nil {
			started := now
			rec.StartedAt = &started
		}
		records = append(records, *copyRecord(rec))
	}
	return records, nil
}

func (s *Store) UpsertQueue(ctx context.Context, q pgflow.Queue) error {
	return nil // The memory store keeps queue config only in the engine.
}

func (s *Store) SetWorkflowEvent(ctx context.Context, workflowID, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.events == nil {
		s.events = map[string]map[string][]byte{}
	}
	if s.events[workflowID] == nil {
		s.events[workflowID] = map[string][]byte{}
	}
	s.events[workflowID][key] = append([]byte(nil), value...)
	return nil
}

func (s *Store) GetWorkflowEvent(ctx context.Context, workflowID, key string) ([]byte, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.events[workflowID][key]
	if !ok {
		return nil, false, nil
	}
	return append([]byte(nil), value...), true, nil
}

func (s *Store) AppendStream(ctx context.Context, workflowID, key string, chunk []byte) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := streamKey{workflowID, key}
	seq := int64(len(s.streams[id]))
	s.streams[id] = append(s.streams[id], pgflow.StreamChunk{Seq: seq, Chunk: append([]byte(nil), chunk...), CreatedAt: s.now()})
	return seq, nil
}

func (s *Store) ReadStreams(ctx context.Context, workflowID, key string, afterSeq int64, limit int) ([]pgflow.StreamChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var chunks []pgflow.StreamChunk
	for _, c := range s.streams[streamKey{workflowID, key}] {
		if c.Seq <= afterSeq {
			continue
		}
		if limit > 0 && len(chunks) >= limit {
			break
		}
		copied := c
		copied.Chunk = append([]byte(nil), c.Chunk...)
		chunks = append(chunks, copied)
	}
	return chunks, nil
}

func (s *Store) ForkWorkflow(ctx context.Context, srcID, newID string, uptoFunctionID int, executorID string, lease time.Duration) (*pgflow.WorkflowRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	src, ok := s.workflows[srcID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", pgflow.ErrWorkflowNotFound, srcID)
	}
	now := s.now()
	expires := now.Add(lease)
	stored := &pgflow.WorkflowRecord{
		ID:             newID,
		Name:           src.Name,
		Status:         pgflow.StatusPending,
		AppVersion:     src.AppVersion,
		ExecutorID:     executorID,
		Epoch:          1,
		ForkedFrom:     srcID,
		Inputs:         append([]byte(nil), src.Inputs...),
		LeaseExpiresAt: &expires,
		CreatedAt:      now,
		UpdatedAt:      now,
		StartedAt:      &now,
	}
	s.workflows[newID] = stored
	for id, cp := range s.checkpoints[srcID] {
		if id < uptoFunctionID {
			if s.checkpoints[newID] == nil {
				s.checkpoints[newID] = make(map[int]*pgflow.Checkpoint)
			}
			s.checkpoints[newID][id] = copyCheckpoint(cp)
		}
	}
	return copyRecord(stored), nil
}

func (s *Store) SetContinuedTo(ctx context.Context, workflowID, continuedTo string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	rec, ok := s.workflows[workflowID]
	if !ok {
		return fmt.Errorf("%w: %s", pgflow.ErrWorkflowNotFound, workflowID)
	}
	rec.ContinuedTo = continuedTo
	rec.UpdatedAt = s.now()
	return nil
}

func (s *Store) ListWorkflows(ctx context.Context, req pgflow.ListWorkflowsRequest) ([]pgflow.WorkflowRecord, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if req.Limit <= 0 {
		req.Limit = 100
	}
	var records []pgflow.WorkflowRecord
	for _, rec := range s.workflows {
		if req.Status != "" && rec.Status != req.Status {
			continue
		}
		if req.Queue != "" && rec.QueueName != req.Queue {
			continue
		}
		records = append(records, *copyRecord(rec))
	}
	sort.Slice(records, func(i, j int) bool {
		if !records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].CreatedAt.After(records[j].CreatedAt)
		}
		return records[i].ID > records[j].ID
	})
	if len(records) > req.Limit {
		records = records[:req.Limit]
	}
	return records, nil
}

func (s *Store) DeleteTerminalWorkflowsBefore(ctx context.Context, cutoff time.Time, limit int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	deleted := 0
	for id, rec := range s.workflows {
		if deleted >= limit {
			break
		}
		if !rec.Status.Terminal() || !rec.UpdatedAt.Before(cutoff) {
			continue
		}
		delete(s.workflows, id)
		delete(s.checkpoints, id)
		delete(s.events, id)
		delete(s.streams, streamKey{workflowID: id})
		deleted++
	}
	return deleted, nil
}

func copyRecord(rec *pgflow.WorkflowRecord) *pgflow.WorkflowRecord {
	copied := *rec
	copied.Inputs = append([]byte(nil), rec.Inputs...)
	copied.Output = append([]byte(nil), rec.Output...)
	if rec.Error != nil {
		info := *rec.Error
		copied.Error = &info
	}
	if rec.LeaseExpiresAt != nil {
		at := *rec.LeaseExpiresAt
		copied.LeaseExpiresAt = &at
	}
	if rec.StartedAt != nil {
		at := *rec.StartedAt
		copied.StartedAt = &at
	}
	if rec.ParentFunctionID != nil {
		id := *rec.ParentFunctionID
		copied.ParentFunctionID = &id
	}
	return &copied
}

func copyCheckpoint(cp *pgflow.Checkpoint) *pgflow.Checkpoint {
	copied := *cp
	copied.Output = append([]byte(nil), cp.Output...)
	if cp.Err != nil {
		info := *cp.Err
		copied.Err = &info
	}
	return &copied
}

func copyMessage(msg *pgflow.Message) *pgflow.Message {
	copied := *msg
	copied.Payload = append([]byte(nil), msg.Payload...)
	return &copied
}
