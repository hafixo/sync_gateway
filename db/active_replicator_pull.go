package db

import (
	"context"
	"fmt"
	"log"

	"github.com/couchbase/sync_gateway/base"
)

// ActivePullReplicator is a unidirectional pull active replicator.
type ActivePullReplicator struct {
	activeReplicatorCommon
}

func NewPullReplicator(config *ActiveReplicatorConfig) *ActivePullReplicator {
	return &ActivePullReplicator{
		activeReplicatorCommon: activeReplicatorCommon{
			config:           config,
			replicationStats: BlipSyncStatsForSGRPull(config.ReplicationStatsMap),
			state:            ReplicationStateStopped,
		},
	}
}

func (apr *ActivePullReplicator) Start() error {

	apr.lock.Lock()
	defer apr.lock.Unlock()

	if apr == nil {
		return fmt.Errorf("nil ActivePullReplicator, can't start")
	}

	if apr.checkpointerCtx != nil {
		return fmt.Errorf("ActivePullReplicator already running")
	}

	var err error
	apr.blipSender, apr.blipSyncContext, err = connect("-pull", apr.config, apr.replicationStats)
	if err != nil {
		return apr.setError(err)
	}

	if apr.config.ConflictResolverFunc != nil {
		apr.blipSyncContext.conflictResolver = NewConflictResolver(apr.config.ConflictResolverFunc, apr.config.ReplicationStatsMap)
	}
	apr.blipSyncContext.purgeOnRemoval = apr.config.PurgeOnRemoval

	apr.checkpointerCtx, apr.checkpointerCtxCancel = context.WithCancel(context.Background())
	if err := apr._initCheckpointer(); err != nil {
		apr.checkpointerCtx = nil
		return apr.setError(err)
	}

	subChangesRequest := SubChangesRequest{
		Continuous:     apr.config.Continuous,
		Batch:          apr.config.ChangesBatchSize,
		Since:          apr.Checkpointer.lastCheckpointSeq,
		Filter:         apr.config.Filter,
		FilterChannels: apr.config.FilterChannels,
		DocIDs:         apr.config.DocIDs,
		ActiveOnly:     apr.config.ActiveOnly,
		clientType:     clientTypeSGR2,
	}

	if err := subChangesRequest.Send(apr.blipSender); err != nil {
		apr.checkpointerCtxCancel()
		apr.checkpointerCtx = nil
		return apr.setError(err)
	}

	apr.setState(ReplicationStateRunning)
	apr.publishStatus()
	return nil
}

// Complete gracefully shuts down a replication, waiting for all in-flight revisions to be processed
// before stopping the replication
func (apr *ActivePullReplicator) Complete() {

	apr.lock.Lock()
	if apr == nil {
		apr.lock.Unlock()
		return
	}

	err := apr.Checkpointer.waitForExpectedSequences()
	if err != nil {
		base.Infof(base.KeyReplicate, "Timeout draining replication %s - stopping: %v", apr.config.ID, err)
	}

	stopErr := apr._stop()
	if stopErr != nil {
		base.Infof(base.KeyReplicate, "Error attempting to stop replication %s: %v", apr.config.ID, stopErr)
	}

	// unlock the replication before triggering callback, in case callback attempts to access replication information
	// from the replicator
	onCompleteCallback := apr.onReplicatorComplete

	apr.lock.Unlock()

	apr.publishStatus()
	if onCompleteCallback != nil {
		onCompleteCallback()
	}
}

// Stop stops the replication and updates the state in the persisted replication status
func (apr *ActivePullReplicator) Stop() error {

	apr.lock.Lock()
	err := apr._stop()
	apr.lock.Unlock()
	if err != nil {
		return err
	}
	apr.publishStatus()
	return nil
}

func (apr *ActivePullReplicator) _stop() error {
	if apr == nil {
		// noop
		return nil
	}

	if apr.checkpointerCtx != nil {
		apr.checkpointerCtxCancel()
		apr.Checkpointer.CheckpointNow()
	}
	apr.checkpointerCtx = nil

	if apr.blipSender != nil {
		apr.blipSender.Close()
		apr.blipSender = nil
	}

	if apr.blipSyncContext != nil {
		apr.blipSyncContext.Close()
		apr.blipSyncContext = nil
	}
	apr.setState(ReplicationStateStopped)

	return nil
}

func (apr *ActivePullReplicator) _initCheckpointer() error {

	checkpointHash, hashErr := apr.config.CheckpointHash()
	if hashErr != nil {
		return hashErr
	}

	apr.Checkpointer = NewCheckpointer(apr.checkpointerCtx, apr.CheckpointID(), checkpointHash,
		apr.blipSender, apr.config.ActiveDB, apr.config.CheckpointInterval, apr.getPullStatus)

	var err error
	apr.initialStatus, err = apr.Checkpointer.fetchCheckpoints()
	log.Printf("Initializing checkpointer for %s, initialStatus is %v", apr.config.ID, apr.initialStatus)
	if err != nil {
		return err
	}

	apr.registerCheckpointerCallbacks()
	apr.Checkpointer.Start()

	return nil
}

// GetStatus is used externally to retrieve pull replication status.  Combines current running stats with
// initialStatus.
func (apr *ActivePullReplicator) GetStatus() *ReplicationStatus {
	var lastSeqPulled string
	if apr.Checkpointer != nil {
		lastSeqPulled = apr.Checkpointer.calculateSafeProcessedSeq()
	}
	status := apr.getPullStatus(lastSeqPulled)
	return status
}

// getPullStatus is used internally, and passed as statusCallback to checkpointer
func (apr *ActivePullReplicator) getPullStatus(lastSeqPulled string) *ReplicationStatus {
	status := &ReplicationStatus{}
	status.Status, status.ErrorMessage = apr.getStateWithErrorMessage()

	pullStats := apr.replicationStats
	status.DocsRead = pullStats.HandleRevCount.Value()
	status.DocsPurged = pullStats.HandleRevDocsPurgedCount.Value()
	status.RejectedLocal = pullStats.HandleRevErrorCount.Value()
	status.DeltasRecv = pullStats.HandleRevDeltaRecvCount.Value()
	status.DeltasRequested = pullStats.HandleChangesDeltaRequestedCount.Value()
	status.LastSeqPull = lastSeqPulled
	if apr.initialStatus != nil {
		status.PullReplicationStatus.Add(apr.initialStatus.PullReplicationStatus)
	}
	return status
}

// CheckpointID returns a unique ID to be used for the checkpoint client (which is used as part of the checkpoint Doc ID on the recipient)
func (apr *ActivePullReplicator) CheckpointID() string {
	return "sgr2cp:pull:" + apr.config.ID
}

func (apr *ActivePullReplicator) reset() error {
	if apr.state != ReplicationStateStopped {
		return fmt.Errorf("reset invoked for replication %s when the replication was not stopped", apr.config.ID)
	}
	return resetLocalCheckpoint(apr.config.ActiveDB, apr.CheckpointID())
}

// registerCheckpointerCallbacks registers appropriate callback functions for checkpointing.
func (apr *ActivePullReplicator) registerCheckpointerCallbacks() {
	apr.blipSyncContext.sgr2PullAlreadyKnownSeqsCallback = apr.Checkpointer.AddAlreadyKnownSeq

	apr.blipSyncContext.sgr2PullAddExpectedSeqsCallback = apr.Checkpointer.AddExpectedSeqIDAndRevs

	apr.blipSyncContext.sgr2PullProcessedSeqCallback = apr.Checkpointer.AddProcessedSeqIDAndRev

	// Trigger complete for non-continuous replications when caught up
	if !apr.config.Continuous {
		apr.blipSyncContext.emptyChangesMessageCallback = func() {
			// Complete blocks waiting for pending rev messages, so needs
			// it's own goroutine
			go apr.Complete()
		}
	}
}

func (apr *ActivePullReplicator) publishStatus() {
	setLocalCheckpointStatus(apr.config.ActiveDB, apr.CheckpointID(), apr.GetStatus())
}
