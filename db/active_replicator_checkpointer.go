package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/couchbase/go-blip"
	"github.com/couchbase/sync_gateway/base"
)

// Checkpointer implements replicator checkpointing, by keeping two lists of sequences. Those which we expect to be processing revs for (either push or pull), and a map for those which we have done so on.
// Periodically (based on a time interval), these two lists are used to calculate the highest sequence number which we've not had a gap for yet, and send a SetCheckpoint message for this sequence.
type Checkpointer struct {
	clientID           string
	configHash         string
	blipSender         *blip.Sender
	activeDB           *Database
	checkpointInterval time.Duration
	// lock guards the expectedSeqs slice, and processedSeqs map
	lock sync.Mutex
	// expectedSeqs is an ordered list of sequence IDs we expect to process revs for
	expectedSeqs []string
	// processedSeqs is a map of sequence IDs we've processed revs for
	processedSeqs map[string]struct{}
	// idAndRevLookup is a temporary map of DocID/RevID pair to sequence number,
	// to handle cases where we receive a message that doesn't contain a sequence.
	idAndRevLookup map[IDAndRev]string
	// ctx is used to stop the checkpointer goroutine
	ctx context.Context
	// lastRemoteCheckpointRevID is the last known remote checkpoint RevID.
	lastRemoteCheckpointRevID string
	// lastLocalCheckpointRevID is the last known local checkpoint RevID.
	lastLocalCheckpointRevID string
	// lastCheckpointSeq is the last checkpointed sequence
	lastCheckpointSeq string

	// SGR1 Checkpoint Migration:
	// sgr1CheckpointID can be set to be used as a fallback checkpoint if none are found
	sgr1CheckpointID string
	// sgr1CheckpointOnRemote determines which end of the replication to fetch the SGR1 checkpoint from.
	// For SGR1 pull replications, the checkpoints are stored on the source, for push they're stored on the target.
	sgr1CheckpointOnRemote bool
	// InsecureSkipVerify skips TLS verification when fetching a remote SGR1 checkpoint.
	sgr1RemoteInsecureSkipVerify bool
	// remoteDBURL is used to fetch local SGR1 checkpoints.
	remoteDBURL *url.URL

	stats CheckpointerStats
}

type CheckpointerStats struct {
	ExpectedSequenceCount              int64
	ProcessedSequenceCount             int64
	AlreadyKnownSequenceCount          int64
	SetCheckpointCount                 int64
	GetCheckpointHitCount              int64
	GetCheckpointMissCount             int64
	GetCheckpointSGR1FallbackHitCount  int64
	GetCheckpointSGR1FallbackMissCount int64
}

func NewCheckpointer(ctx context.Context, clientID string, configHash string, blipSender *blip.Sender, replicatorConfig *ActiveReplicatorConfig) *Checkpointer {
	return &Checkpointer{
		clientID:                     clientID,
		configHash:                   configHash,
		blipSender:                   blipSender,
		activeDB:                     replicatorConfig.ActiveDB,
		expectedSeqs:                 make([]string, 0),
		processedSeqs:                make(map[string]struct{}),
		idAndRevLookup:               make(map[IDAndRev]string),
		checkpointInterval:           replicatorConfig.CheckpointInterval,
		ctx:                          ctx,
		stats:                        CheckpointerStats{},
		sgr1CheckpointID:             replicatorConfig.SGR1CheckpointID,
		sgr1CheckpointOnRemote:       replicatorConfig.Direction == ActiveReplicatorTypePush,
		remoteDBURL:                  replicatorConfig.RemoteDBURL,
		sgr1RemoteInsecureSkipVerify: replicatorConfig.InsecureSkipVerify,
	}
}

func (c *Checkpointer) AddAlreadyKnownSeq(seq ...string) {
	select {
	case <-c.ctx.Done():
		// replicator already closed, bail out of checkpointing work
		return
	default:
	}

	c.lock.Lock()
	c.expectedSeqs = append(c.expectedSeqs, seq...)
	for _, seq := range seq {
		c.processedSeqs[seq] = struct{}{}
	}
	c.stats.AlreadyKnownSequenceCount += int64(len(seq))
	c.lock.Unlock()
}

func (c *Checkpointer) AddProcessedSeq(seq string) {
	select {
	case <-c.ctx.Done():
		// replicator already closed, bail out of checkpointing work
		return
	default:
	}

	c.lock.Lock()
	c.processedSeqs[seq] = struct{}{}
	c.stats.ProcessedSequenceCount++
	c.lock.Unlock()
}

func (c *Checkpointer) AddProcessedSeqIDAndRev(seq string, idAndRev IDAndRev) {
	select {
	case <-c.ctx.Done():
		// replicator already closed, bail out of checkpointing work
		return
	default:
	}

	c.lock.Lock()

	if seq == "" {
		seq, _ = c.idAndRevLookup[idAndRev]
	}
	// should remove entry in the map even if we have a seq available
	delete(c.idAndRevLookup, idAndRev)

	c.processedSeqs[seq] = struct{}{}
	c.stats.ProcessedSequenceCount++

	c.lock.Unlock()
}

func (c *Checkpointer) AddExpectedSeqs(seqs ...string) {
	if len(seqs) == 0 {
		// nothing to do
		return
	}

	select {
	case <-c.ctx.Done():
		// replicator already closed, bail out of checkpointing work
		return
	default:
	}

	c.lock.Lock()
	c.expectedSeqs = append(c.expectedSeqs, seqs...)
	c.stats.ExpectedSequenceCount += int64(len(seqs))
	c.lock.Unlock()
}

func (c *Checkpointer) AddExpectedSeqIDAndRevs(seqs map[IDAndRev]string) {
	if len(seqs) == 0 {
		// nothing to do
		return
	}

	select {
	case <-c.ctx.Done():
		// replicator already closed, bail out of checkpointing work
		return
	default:
	}

	c.lock.Lock()
	for idAndRev, seq := range seqs {
		c.idAndRevLookup[idAndRev] = seq
		c.expectedSeqs = append(c.expectedSeqs, seq)
	}
	c.stats.ExpectedSequenceCount += int64(len(seqs))
	c.lock.Unlock()
}

func (c *Checkpointer) Start() {
	// Start a time-based checkpointer goroutine
	go func() {
		checkpointInterval := defaultCheckpointInterval
		if c.checkpointInterval > 0 {
			checkpointInterval = c.checkpointInterval
		}
		ticker := time.NewTicker(checkpointInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				c.CheckpointNow()
			case <-c.ctx.Done():
				base.DebugfCtx(c.ctx, base.KeyReplicate, "checkpointer goroutine stopped")
				return
			}
		}
	}()
}

// CheckpointNow forces the checkpointer to send a checkpoint, and blocks until it has finished.
func (c *Checkpointer) CheckpointNow() {
	if c == nil {
		return
	}
	c.lock.Lock()
	defer c.lock.Unlock()

	base.TracefCtx(c.ctx, base.KeyReplicate, "checkpointer: running")

	seq := c._updateCheckpointLists()
	if seq == "" {
		return
	}

	base.InfofCtx(c.ctx, base.KeyReplicate, "checkpointer: calculated seq: %v", seq)
	err := c._setCheckpoints(seq)
	if err != nil {
		base.Warnf("couldn't set checkpoints: %v", err)
	}
}

// Stats returns a copy of the checkpointer stats. Intended for test use - non-test usage may have
// performance implications associated with locking
func (c *Checkpointer) Stats() CheckpointerStats {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.stats
}

// _updateCheckpointLists determines the highest checkpointable sequence, and trims the processedSeqs/expectedSeqs lists up to this point.
func (c *Checkpointer) _updateCheckpointLists() (safeSeq string) {
	base.TracefCtx(c.ctx, base.KeyReplicate, "checkpointer: _updateCheckpointLists(expectedSeqs: %v, procssedSeqs: %v)", c.expectedSeqs, c.processedSeqs)

	maxI := c._calculateSafeExpectedSeqsIdx()
	if maxI == -1 {
		// nothing to do
		return ""
	}

	safeSeq = c.expectedSeqs[maxI]

	// removes to-be checkpointed sequences from processedSeqs list
	for i := 0; i <= maxI; i++ {
		removeSeq := c.expectedSeqs[i]
		delete(c.processedSeqs, removeSeq)
		base.TracefCtx(c.ctx, base.KeyReplicate, "checkpointer: _updateCheckpointLists removed seq %v from processedSeqs map %v", removeSeq, c.processedSeqs)
	}

	// trim expectedSeqs list for all processed seqs
	c.expectedSeqs = c.expectedSeqs[maxI+1:]

	return safeSeq
}

// _calculateSafeExpectedSeqsIdx returns an index into expectedSeqs which is safe to checkpoint.
// Returns -1 if no sequence in the list is able to be checkpointed.
func (c *Checkpointer) _calculateSafeExpectedSeqsIdx() int {
	safeIdx := -1

	// iterates over each (ordered) expected sequence and stops when we find the first sequence we've yet to process a rev message for
	for i, seq := range c.expectedSeqs {
		if _, ok := c.processedSeqs[seq]; !ok {
			break
		}
		safeIdx = i
	}

	return safeIdx
}

// calculateSafeProcessedSeq returns the sequence last processed that is able to be checkpointed, or the last checkpointed sequence.
func (c *Checkpointer) calculateSafeProcessedSeq() string {
	c.lock.Lock()
	defer c.lock.Unlock()

	idx := c._calculateSafeExpectedSeqsIdx()
	if idx == -1 {
		return c.lastCheckpointSeq
	}

	return c.expectedSeqs[idx]
}

const (
	checkpointDocIDPrefix = "checkpoint/"
	checkpointBodyRev     = "_rev"
	checkpointBodyLastSeq = "last_sequence"
	checkpointBodyHash    = "config_hash"
)

type replicationCheckpoint struct {
	Rev        string `json:"_rev"`
	ConfigHash string `json:"config_hash"`
	LastSeq    string `json:"last_sequence"`
}

// AsBody returns a Body representation of replicationCheckpoint for use with putSpecial
func (r *replicationCheckpoint) AsBody() Body {
	return Body{
		checkpointBodyRev:     r.Rev,
		checkpointBodyLastSeq: r.LastSeq,
		checkpointBodyHash:    r.ConfigHash,
	}
}

// NewReplicationCheckpoint converts a revID and checkpoint body into a replicationCheckpoint
func NewReplicationCheckpoint(revID string, body Body) *replicationCheckpoint {
	checkpoint := &replicationCheckpoint{
		Rev: revID,
	}
	lastSeq, ok := body[checkpointBodyLastSeq].(string)
	if ok {
		checkpoint.LastSeq = lastSeq
	}
	configHash, ok := body[checkpointBodyHash].(string)
	if ok {
		checkpoint.ConfigHash = configHash
	}
	return checkpoint
}

func (r *replicationCheckpoint) Copy() *replicationCheckpoint {
	return &replicationCheckpoint{
		Rev:        r.Rev,
		LastSeq:    r.LastSeq,
		ConfigHash: r.ConfigHash,
	}
}

func (c *Checkpointer) fetchSGR1Checkpoint() (sgr1CheckpointSeq string) {
	base.Infof(base.KeyReplicate, "Attempting to fetch SGR1 checkpoint as fallback: %v", c.sgr1CheckpointID)
	var err error

	if c.sgr1CheckpointOnRemote {
		base.DebugfCtx(c.ctx, base.KeyReplicate, "getting SGR1 checkpoint from remote (passive)")
		sgr1CheckpointSeq, _, err = c.getRemoteSGR1Checkpoint()
	} else {
		base.DebugfCtx(c.ctx, base.KeyReplicate, "getting SGR1 checkpoint from local (active)")
		sgr1CheckpointSeq, _, err = c.getLocalSGR1Checkpoint()
	}

	if err != nil || sgr1CheckpointSeq == "" {
		c.stats.GetCheckpointSGR1FallbackMissCount++
		base.Warnf("Unable to get SGR1 checkpoint, continuing without: %v", err)
		return ""
	}

	base.DebugfCtx(c.ctx, base.KeyReplicate, "using upgraded sg-replicate checkpoint seq: %q", sgr1CheckpointSeq)
	c.stats.GetCheckpointSGR1FallbackHitCount++
	return sgr1CheckpointSeq
}

// fetchCheckpoints sets lastCheckpointSeq for the given Checkpointer by requesting various checkpoints on the local and remote.
// Various scenarios this function handles:
// - Matching checkpoints from local and remote. Use that sequence.
// - Both SGR2 checkpoints are missing, and there's no SGR1 checkpoint to fall back on, we'll start the replication from zero.
// - Mismatched config hashes, use a zero value for sequence, so the replication can restart.
// - Mismatched sequences, we'll pick the lower of the two, and attempt to roll back the higher checkpoint to that point.
// - Both SGR2 checkpoints missing, use the sequence from SGR1CheckpointID.
func (c *Checkpointer) fetchCheckpoints() error {
	base.TracefCtx(c.ctx, base.KeyReplicate, "fetchCheckpoints()")

	localCheckpoint, err := c.getLocalCheckpoint()
	if err != nil {
		return err
	}

	base.DebugfCtx(c.ctx, base.KeyReplicate, "got local checkpoint: %v", localCheckpoint)
	c.lastLocalCheckpointRevID = localCheckpoint.Rev

	remoteCheckpoint, err := c.getRemoteCheckpoint()
	if err != nil {
		return err
	}
	base.DebugfCtx(c.ctx, base.KeyReplicate, "got remote checkpoint: %v", remoteCheckpoint)
	c.lastRemoteCheckpointRevID = remoteCheckpoint.Rev

	localSeq := localCheckpoint.LastSeq
	remoteSeq := remoteCheckpoint.LastSeq

	// If both local and remote SGR2 checkpoints are missing, fetch SGR1 checkpoint as fallback as a one-time upgrade.
	// Explicit checkpoint resets are expected to only reset one side of the checkpoints, so shouldn't fall into this handling.
	if localSeq == "" && remoteSeq == "" && c.sgr1CheckpointID != "" {
		if seq := c.fetchSGR1Checkpoint(); seq != "" {
			c.stats.GetCheckpointHitCount++
			c.lastCheckpointSeq = seq
			return nil
		}
	}

	// If localSeq and remoteSeq match, we'll use localSeq as the value.
	checkpointSeq := localSeq

	// Determine the lowest sequence if they don't match
	if localSeq != remoteSeq {
		base.DebugfCtx(c.ctx, base.KeyReplicate, "sequences mismatched, finding lowest of %q %q", localSeq, remoteSeq)
		localSeqVal, err := parseIntegerSequenceID(localSeq)
		if err != nil {
			return err
		}

		remoteSeqVal, err := parseIntegerSequenceID(remoteSeq)
		if err != nil {
			return err
		}

		// roll local/remote checkpoint back to lowest of the two
		if remoteSeqVal.Before(localSeqVal) {
			checkpointSeq = remoteSeq
			newLocalCheckpoint := remoteCheckpoint.Copy()
			newLocalCheckpoint.Rev = c.lastLocalCheckpointRevID
			c.lastLocalCheckpointRevID, err = c.setLocalCheckpoint(newLocalCheckpoint)
			if err != nil {
				base.WarnfCtx(c.ctx, "Unable to roll back local checkpoint: %v", err)
			}
		} else {
			// checkpointSeq already set above for localSeq value
			newRemoteCheckpoint := localCheckpoint.Copy()
			newRemoteCheckpoint.Rev = c.lastRemoteCheckpointRevID
			c.lastRemoteCheckpointRevID, err = c.setRemoteCheckpoint(newRemoteCheckpoint)
			if err != nil {
				base.WarnfCtx(c.ctx, "Unable to roll back remote checkpoint: %v", err)
			}
		}
	}

	// If checkpoint hash has changed, reset checkpoint to zero. Shouldn't need to persist the updated checkpoints in
	// the case both get rolled back to zero, can wait for next persistence.
	if localCheckpoint.ConfigHash != c.configHash || remoteCheckpoint.ConfigHash != c.configHash {
		base.DebugfCtx(c.ctx, base.KeyReplicate, "replicator config changed, unable to use previous checkpoint")
		checkpointSeq = ""
	}

	base.DebugfCtx(c.ctx, base.KeyReplicate, "using checkpointed seq: %q", checkpointSeq)

	if checkpointSeq == "" {
		c.stats.GetCheckpointMissCount++
	} else {
		c.stats.GetCheckpointHitCount++
	}

	c.lastCheckpointSeq = checkpointSeq
	return nil
}

func (c *Checkpointer) _setCheckpoints(seq string) (err error) {
	base.TracefCtx(c.ctx, base.KeyReplicate, "setCheckpoints(%v)", seq)

	c.lastLocalCheckpointRevID, err = c.setLocalCheckpointWithRetry(
		&replicationCheckpoint{
			LastSeq:    seq,
			Rev:        c.lastLocalCheckpointRevID,
			ConfigHash: c.configHash,
		})
	if err != nil {
		return err
	}

	c.lastRemoteCheckpointRevID, err = c.setRemoteCheckpointWithRetry(
		&replicationCheckpoint{
			LastSeq:    seq,
			Rev:        c.lastRemoteCheckpointRevID,
			ConfigHash: c.configHash,
		})
	if err != nil {
		return err
	}

	c.lastCheckpointSeq = seq
	c.stats.SetCheckpointCount++

	return nil
}

// getLocalCheckpoint returns the sequence and rev for the local checkpoint.
// if the checkpoint does not exist, returns empty sequence and rev.
func (c *Checkpointer) getLocalCheckpoint() (checkpoint *replicationCheckpoint, err error) {
	base.TracefCtx(c.ctx, base.KeyReplicate, "getLocalCheckpoint")

	checkpointBytes, err := c.activeDB.GetSpecialBytes(DocTypeLocal, checkpointDocIDPrefix+c.clientID)
	if err != nil {
		if !base.IsKeyNotFoundError(c.activeDB.Bucket, err) {
			return &replicationCheckpoint{}, err
		}
		base.DebugfCtx(c.ctx, base.KeyReplicate, "couldn't find existing local checkpoint for client %q", c.clientID)
		return &replicationCheckpoint{}, nil
	}

	err = base.JSONUnmarshal(checkpointBytes, &checkpoint)
	return checkpoint, err
}

func (c *Checkpointer) setLocalCheckpoint(checkpoint *replicationCheckpoint) (newRev string, err error) {
	base.TracefCtx(c.ctx, base.KeyReplicate, "setLocalCheckpoint(%v)", checkpoint)

	newRev, err = c.activeDB.putSpecial(DocTypeLocal, checkpointDocIDPrefix+c.clientID, checkpoint.Rev, checkpoint.AsBody())
	if err != nil {
		return "", err
	}
	return newRev, nil
}

// setLocalCheckpointWithRetry attempts to rewrite the checkpoint if the rev ID is mismatched, or the checkpoint has since been deleted.
func (c *Checkpointer) setLocalCheckpointWithRetry(checkpoint *replicationCheckpoint) (newRevID string, err error) {
	return c.setRetry(checkpoint,
		c.setLocalCheckpoint,
		c.getLocalCheckpoint,
	)
}

// resetLocalCheckpoint forces the local checkpoint to an empty sequence to roll back the replication.
// avoids deletion to prevent SGR1 checkpoint fallback from being invoked for reset replications.
func resetLocalCheckpoint(activeDB *Database, checkpointID string) error {
	key := RealSpecialDocID(DocTypeLocal, checkpointDocIDPrefix+checkpointID)
	return activeDB.Bucket.Set(key, 0, Body{
		checkpointBodyLastSeq: "",
		checkpointBodyHash:    "",
		checkpointBodyRev:     "0-1",
	})
}

// getRemoteSGR1Checkpoint returns the sequence and rev for the remote SGR1 checkpoint.
// if the checkpoint does not exist, returns empty sequence and rev.
func (c *Checkpointer) getRemoteSGR1Checkpoint() (seq, rev string, err error) {
	// SGR1 checkpoints do not have a "checkpoint/" prefix, which SG adds to keys in BLIP-based getCheckpoint requests,
	// so we'll have to use the REST API to retrieve the checkpoint directly.

	req, err := http.NewRequest(http.MethodGet, c.remoteDBURL.String()+"/_local/"+c.sgr1CheckpointID, nil)
	if err != nil {
		return "", "", err
	}
	client := base.GetHttpClient(c.sgr1RemoteInsecureSkipVerify)
	resp, err := client.Do(req)
	if err != nil {
		return "", "", err
	}

	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
		var body struct {
			LastSequence string `json:"lastSequence,omitempty"`
			RevID        string `json:"_rev"`
		}
		err := json.NewDecoder(resp.Body).Decode(&body)
		if err != nil {
			return "", "", err
		}
		return body.LastSequence, body.RevID, nil
	case http.StatusNotFound:
		base.DebugfCtx(c.ctx, base.KeyReplicate, "couldn't find fallback SGR1 checkpoint for client %q with SGR1 ID: %q", c.clientID, c.sgr1CheckpointID)
		return "", "", nil
	default:
		return "", "", fmt.Errorf("unexpected status code %d from target database", resp.StatusCode)
	}
}

// getLocalSGR1Checkpoint returns the sequence and rev for the local SGR1 checkpoint.
// if the checkpoint does not exist, returns empty sequence and rev.
func (c *Checkpointer) getLocalSGR1Checkpoint() (seq, rev string, err error) {
	body, err := c.activeDB.GetSpecial("local", c.sgr1CheckpointID)
	if err != nil {
		if !base.IsKeyNotFoundError(c.activeDB.Bucket, err) {
			return "", "", err
		}
		base.DebugfCtx(c.ctx, base.KeyReplicate, "couldn't find existing local checkpoint for client %q", c.sgr1CheckpointID)
		return "", "", nil
	}

	seq, _ = body["lastSequence"].(string)
	rev, _ = body[BodyRev].(string)
	return seq, rev, nil
}

// getRemoteCheckpoint returns the sequence and rev for the remote checkpoint.
// if the checkpoint does not exist, returns empty sequence and rev.
func (c *Checkpointer) getRemoteCheckpoint() (checkpoint *replicationCheckpoint, err error) {
	base.TracefCtx(c.ctx, base.KeyReplicate, "getRemoteCheckpoint")

	rq := GetSGR2CheckpointRequest{
		Client: c.clientID,
	}

	if err := rq.Send(c.blipSender); err != nil {
		return &replicationCheckpoint{}, err
	}

	resp, err := rq.Response()
	if err != nil {
		return &replicationCheckpoint{}, err
	}

	if resp == nil {
		base.DebugfCtx(c.ctx, base.KeyReplicate, "couldn't find existing remote checkpoint for client %q", c.clientID)
		return &replicationCheckpoint{}, nil
	}

	return NewReplicationCheckpoint(resp.RevID, resp.Body), nil
}

func (c *Checkpointer) setRemoteCheckpoint(checkpoint *replicationCheckpoint) (newRev string, err error) {
	base.TracefCtx(c.ctx, base.KeyReplicate, "setRemoteCheckpoint(%v)", checkpoint)

	checkpointBody := checkpoint.AsBody()
	rq := SetSGR2CheckpointRequest{
		Client:     c.clientID,
		Checkpoint: checkpoint.AsBody(),
	}

	parentRev, ok := checkpointBody[BodyRev].(string)
	if ok {
		rq.RevID = &parentRev
	}

	if err := rq.Send(c.blipSender); err != nil {
		return "", err
	}

	resp, err := rq.Response()
	if err != nil {
		return "", err
	}

	return resp.RevID, nil
}

// setRemoteCheckpointWithRetry attempts to rewrite the checkpoint if the rev ID is mismatched, or the checkpoint has since been deleted.
func (c *Checkpointer) setRemoteCheckpointWithRetry(checkpoint *replicationCheckpoint) (newRevID string, err error) {
	return c.setRetry(checkpoint,
		c.setRemoteCheckpoint,
		c.getRemoteCheckpoint,
	)
}

func (c *Checkpointer) getCounts() (expectedCount, processedCount int) {
	c.lock.Lock()
	defer c.lock.Unlock()
	return len(c.expectedSeqs), len(c.processedSeqs)
}

// waitForExpectedSequences waits for the expectedSeqs set to drain to zero.
// Intended to be used once the replication has been stopped, to wait for
// in-flight mutations to complete.
// Triggers immediate checkpointing if expectedCount == processedCount.
// Waits up to 10s, polling every 100ms.
func (c *Checkpointer) waitForExpectedSequences() error {
	waitCount := 0
	for waitCount < 100 {
		expectedCount, processedCount := c.getCounts()
		if expectedCount == 0 {
			return nil
		}
		if expectedCount == processedCount {
			c.CheckpointNow()
			// Doing an additional check here, instead of just 'continue',
			// in case of bugs that result in expectedCount==processedCount, but the
			// sets are not identical.  In that scenario, want to sleep before retrying
			updatedExpectedCount, _ := c.getCounts()
			if updatedExpectedCount == 0 {
				return nil
			}
		}
		time.Sleep(100 * time.Millisecond)
		waitCount++
	}
	return errors.New("checkpointer waitForExpectedSequences failed to complete after waiting 10s")
}

type setCheckpointFn func(checkpoint *replicationCheckpoint) (revID string, err error)
type getCheckpointFn func() (checkpoint *replicationCheckpoint, err error)

// TODO: why is setRetry ignoring rev conflicts and just overwriting with an updated rev?
// setRetry is a retry loop for a setCheckpointFn, which will fetch a new RevID from a getCheckpointFn in the event of a write conflict.
func (c *Checkpointer) setRetry(checkpoint *replicationCheckpoint, setFn setCheckpointFn, getFn getCheckpointFn) (newRevID string, err error) {
	for numAttempts := 0; numAttempts < 10; numAttempts++ {
		newRevID, err = setFn(checkpoint)
		if err != nil {
			if strings.HasPrefix(err.Error(), "409") {
				base.WarnfCtx(c.ctx, "rev mismatch from setCheckpoint - updating last known rev ID: %v", err)
				existingCheckpoint, getErr := getFn()
				if getErr == nil {
					base.InfofCtx(c.ctx, base.KeyReplicate, "using new rev from existing checkpoint: %v", existingCheckpoint.Rev)
					checkpoint.Rev = existingCheckpoint.Rev
				} else {
					// fall through to retry
				}
			} else if strings.HasPrefix(err.Error(), "404") {
				base.WarnfCtx(c.ctx, "checkpoint did not exist for attempted update - removing last known rev ID: %v", err)
				checkpoint.Rev = ""
			} else {
				base.WarnfCtx(c.ctx, "got unexpected error from setCheckpoint: %v", err)
			}
			time.Sleep(time.Millisecond * 100)
			continue
		}
		return newRevID, nil
	}
	return "", errors.New("failed to write checkpoint after 10 attempts")
}
