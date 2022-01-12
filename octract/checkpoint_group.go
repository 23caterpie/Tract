package octract

import (
	"context"
	"sync"
	"time"

	tract "github.com/23caterpie/Tract"

	"go.opencensus.io/stats"
	"go.opencensus.io/tag"
)

func groupInputCheckpoint(groupContext tract.GroupContext) tract.InputCheckpointClosure {
	return func(req tract.Request, ok bool) {
		if !ok {
			return
		}
		if entry := getRequestCheckpointGroupLedgerEntry(req, groupContext.GroupName); entry != nil {
			entry.startTime = now()
		}
	}
}

func groupOutputCheckpoint(groupContext tract.GroupContext, req tract.Request) tract.OutputCheckpointClosure {
	if entry := getRequestCheckpointGroupLedgerEntry(req, groupContext.GroupName); entry != nil && !entry.startTime.IsZero() {
		ctx, setCtx := getCtx(req)
		// Measure work duration.
		stats.RecordWithTags(ctx,
			[]tag.Mutator{
				tag.Upsert(GroupName, groupContext.GroupName),
			},
			GroupWorkLatency.M(float64(since(entry.startTime))/float64(time.Millisecond)),
		)
		setCtx(ctx)
	}
	return func() {}
}

type (
	requestCheckpointGroupLedgerCtxKey struct{}
	requestCheckpointGroupLedger       struct {
		Ledger map[string]*requestCheckpointGroupLedgerEntry
		Mutex  *sync.Mutex
	}
	requestCheckpointGroupLedgerEntry struct {
		startTime time.Time
	}
)

func getRequestCheckpointGroupLedgerEntry(req tract.Request, key string) *requestCheckpointGroupLedgerEntry {
	creq, ok := req.(tract.ContextRequest)
	if !ok || key == "" {
		return nil
	}

	ctx, setCtx := creq.Context()
	ledger, _ := ctx.Value(requestCheckpointGroupLedgerCtxKey{}).(*requestCheckpointGroupLedger)
	// This part is not protected by a mutex, but ledger creation should be from a central location.. leaving this for now.
	if ledger == nil {
		ledger = &requestCheckpointGroupLedger{
			Ledger: make(map[string]*requestCheckpointGroupLedgerEntry),
			Mutex:  &sync.Mutex{},
		}
		ctx = context.WithValue(ctx, requestCheckpointGroupLedgerCtxKey{}, ledger)
	}
	setCtx(ctx)
	ledger.Mutex.Lock()
	defer ledger.Mutex.Unlock()
	entry := ledger.Ledger[key]
	if entry == nil {
		entry = new(requestCheckpointGroupLedgerEntry)
		ledger.Ledger[key] = entry
	}
	return entry
}
