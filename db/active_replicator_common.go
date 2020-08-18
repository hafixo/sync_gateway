package db

import (
	"context"
	"expvar"
	"sync"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// replicatorCommon defines the struct contents shared by ActivePushReplicator
// and ActivePullReplicator
type activeReplicatorCommon struct {
	config                *ActiveReplicatorConfig
	blipSyncContext       *BlipSyncContext
	blipSender            *blip.Sender
	Stats                 expvar.Map
	Checkpointer          *Checkpointer
	checkpointerCtx       context.Context
	checkpointerCtxCancel context.CancelFunc
	initialStatus         *ReplicationStatus
	state                 string
	lastError             error
	stateErrorLock        sync.RWMutex // state and lastError share their own mutex to support retrieval while holding the main lock
	replicationStats      *BlipSyncStats
	onReplicatorComplete  ReplicatorCompleteFunc
	lock                  sync.RWMutex
}

type ReplicatorCompleteFunc func()

// setErrorState updates state and lastError, and
// returns the error provided.  Expects callers to be holding
// a.lock
func (a *activeReplicatorCommon) setError(err error) (passThrough error) {
	base.Infof(base.KeyReplicate, "ActiveReplicator had error state set with err: %v", err)
	a.stateErrorLock.Lock()
	defer a.stateErrorLock.Unlock()
	a.state = ReplicationStateError
	a.lastError = err
	return err
}

// setState updates replicator state and resets lastError to nil.  Expects callers
// to be holding a.lock
func (a *activeReplicatorCommon) setState(state string) {
	a.stateErrorLock.Lock()
	defer a.stateErrorLock.Unlock()
	a.state = state
	a.lastError = nil
}

func (a *activeReplicatorCommon) getState() string {
	a.stateErrorLock.RLock()
	defer a.stateErrorLock.RUnlock()
	return a.state
}

func (a *activeReplicatorCommon) getLastError() error {
	a.stateErrorLock.RLock()
	defer a.stateErrorLock.RUnlock()
	return a.lastError
}

func (a *activeReplicatorCommon) getStateWithErrorMessage() (state string, lastErrorMessage string) {
	a.stateErrorLock.RLock()
	defer a.stateErrorLock.RUnlock()
	if a.lastError == nil {
		return a.state, ""
	} else {
		return a.state, a.lastError.Error()
	}
}

func (a *activeReplicatorCommon) GetStats() *BlipSyncStats {
	a.lock.RLock()
	defer a.lock.RUnlock()
	return a.replicationStats
}
