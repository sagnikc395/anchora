package pgflow

import (
	"fmt"
	"time"
)

// recoveryLoop periodically claims workflows whose executor stopped renewing.
func (e *Engine) recoveryLoop() {
	ticker := time.NewTicker(e.cfg.RecoveryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.recover(false)
		}
	}
}

// recover claims expired workflows and runs them here, or gives up on ones that
// have crashed their executor too many times.
func (e *Engine) recover(includeOwned bool) {
	records, err := e.store.ClaimExpired(e.ctx, ClaimExpiredRequest{
		ExecutorID:   e.executorID,
		AppVersion:   e.AppVersion(),
		Lease:        e.cfg.Lease,
		Limit:        e.cfg.RecoveryBatch,
		IncludeOwned: includeOwned,
	})
	if err != nil {
		if e.ctx.Err() == nil {
			e.logf("pgflow: recovery sweep: %v", err)
		}
		return
	}
	for i := range records {
		rec := records[i]
		if rec.RecoveryAttempts > e.cfg.MaxRecoveryAttempts {
			info := encodeError(fmt.Errorf("%w (%d recoveries)", ErrMaxRecoveryAttempts, rec.RecoveryAttempts))
			if _, err := e.store.CompleteWorkflow(e.ctx, rec.ID, rec.Epoch, StatusRetriesExceeded, nil, info); err != nil && e.ctx.Err() == nil {
				e.logf("pgflow: give up on workflow %s: %v", rec.ID, err)
			}
			continue
		}
		e.dispatch(&rec)
	}
	if len(records) > 0 {
		recoveries.Add(float64(len(records)))
	}
}

// leaseLoop keeps this executor's claims alive. The interval is a third of the
// lease, so two renewals may be lost before another executor may take over.
func (e *Engine) leaseLoop() {
	interval := e.cfg.Lease / 3
	if interval < 25*time.Millisecond {
		interval = 25 * time.Millisecond
	}
	if interval >= e.cfg.Lease {
		interval = e.cfg.Lease / 2
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-e.ctx.Done():
			return
		case <-ticker.C:
			e.renewLeases()
		}
	}
}

// renewLeases extends every lease this process holds and stops runs whose
// workflows were cancelled or claimed by somebody else.
func (e *Engine) renewLeases() {
	e.mu.Lock()
	ids := make([]string, 0, len(e.running))
	for id := range e.running {
		ids = append(ids, id)
	}
	e.mu.Unlock()
	if len(ids) == 0 {
		return
	}
	owned, err := e.store.RenewLeases(e.ctx, e.executorID, e.cfg.Lease, ids)
	if err != nil {
		if e.ctx.Err() == nil {
			e.logf("pgflow: renew leases: %v", err)
		}
		return
	}
	stillOwned := make(map[string]struct{}, len(owned))
	for _, id := range owned {
		stillOwned[id] = struct{}{}
	}
	for _, id := range ids {
		if _, ok := stillOwned[id]; !ok {
			e.cancelRun(id)
		}
	}
}
