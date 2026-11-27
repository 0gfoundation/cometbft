package blocksync

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	flow "github.com/cometbft/cometbft/internal/flowrate"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/types"
)

func lifecyclePool(t *testing.T, requests chan BlockRequest, errs chan peerError) *BlockPool {
	t.Helper()
	pool := NewBlockPool(1, requests, errs)
	require.NoError(t, pool.Start())
	t.Cleanup(func() {
		if pool.IsRunning() {
			require.NoError(t, pool.Stop())
		}
	})
	return pool
}

// Wait for the real sender to block, rather than racing Stop against a goroutine
// that may not have entered the send yet. No production test hooks are needed.
func waitLifecycleSender(t *testing.T, method string) {
	t.Helper()
	require.Eventually(t, func() bool {
		buf := make([]byte, 1<<20)
		stacks := string(buf[:runtime.Stack(buf, true)])
		for _, stack := range strings.Split(stacks, "\n\n") {
			if strings.Contains(stack, method) &&
				(strings.Contains(stack, "[chan send]") || strings.Contains(stack, "[select]")) {
				return true
			}
		}
		return false
	}, time.Second, time.Millisecond, "sender did not block: %s", method)
}

func waitLifecycleDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("lifecycle operation did not finish")
	}
}

func TestBlockPoolLifecycleStopClearsState(t *testing.T) {
	pool := lifecyclePool(t, nil, nil)
	pool.makeNextRequester(1)
	pool.mtx.Lock()
	r := pool.requesters[1]
	block := &types.Block{Header: types.Header{Height: 1}}
	commit := &types.ExtendedCommit{Height: 1}
	accepted := r.setBlock(block, commit, "")
	peer := newBPPeer(pool, "peer", 1, 10)
	peer.incrPending()
	pool.peers[peer.id] = peer // not available for selection in sortedPeers
	pool.mtx.Unlock()
	require.True(t, accepted)

	require.NoError(t, pool.Stop())
	pool.mtx.Lock()
	assert.Empty(t, pool.requesters)
	assert.Zero(t, peer.numPending)
	assert.False(t, peer.timeout.Stop(), "peer timer must already be stopped")
	pool.mtx.Unlock()
	assert.False(t, r.IsRunning())
	assert.Nil(t, r.getBlock())
	assert.Nil(t, r.getExtendedCommit())
	assert.False(t, r.setBlock(block, commit, ""), "late response must not retain blocks")
	r.redo("peer")
	assert.Empty(t, r.redoCh, "stopped requester must reject redo")
	assert.Error(t, pool.AddBlock("peer", block, commit, 1))
	pool.RedoRequestFrom(1, "peer")
	assert.Nil(t, pool.pickIncrAvailablePeer(1, ""))
}

func TestBlockPoolLifecycleMissingRequester(t *testing.T) {
	pool := lifecyclePool(t, nil, nil)
	assert.Panics(t, pool.PopRequest, "running pool must keep its invariant")
	assert.NotPanics(t, func() {
		assert.Equal(t, p2p.ID(""), pool.RemovePeerAndRedoAllPeerRequests(9))
	})
	require.NoError(t, pool.Stop())
	assert.NotPanics(t, pool.PopRequest)
	assert.EqualValues(t, 1, pool.Height())
	assert.NotPanics(t, func() {
		assert.Equal(t, p2p.ID(""), pool.RemovePeerAndRedoAllPeerRequests(1))
		assert.Equal(t, p2p.ID(""), pool.RemovePeerAndRedoAllPeerRequests(2))
	})
}

func TestBlockPoolLifecycleCreateDuringStop(t *testing.T) {
	pool := lifecyclePool(t, nil, nil)
	pool.mtx.Lock()
	created := make(chan struct{})
	go func() {
		pool.makeNextRequester(1)
		close(created)
	}()
	stopped := make(chan struct{})
	go func() {
		_ = pool.Stop()
		close(stopped)
	}()
	// Stop has set the service flag, even though OnStop cannot acquire mtx yet.
	stopping := assert.Eventually(t, func() bool { return !pool.IsRunning() }, time.Second, time.Millisecond)
	pool.mtx.Unlock()
	require.True(t, stopping)
	waitLifecycleDone(t, created)
	waitLifecycleDone(t, stopped)
	pool.makeNextRequester(2)
	pool.mtx.Lock()
	defer pool.mtx.Unlock()
	assert.Empty(t, pool.requesters, "creation queued behind stop must not publish requesters")
}

func TestBlockPoolLifecycleBlockedRequestStops(t *testing.T) {
	requests := make(chan BlockRequest)
	pool := lifecyclePool(t, requests, nil)
	done := make(chan struct{})
	go func() {
		pool.sendRequest(1, "peer")
		close(done)
	}()
	// Also release the old, broken sender when demonstrating RED.
	t.Cleanup(func() {
		select {
		case <-requests:
		default:
		}
	})
	waitLifecycleSender(t, "(*BlockPool).sendRequest")
	require.NoError(t, pool.Stop())
	waitLifecycleDone(t, done)
}

// Live backpressure must deliver the original peer error once capacity returns,
// not silently discard it. A mismatched extended commit must never be retained.
func TestBlockPoolLifecycleLiveErrorDelivery(t *testing.T) {
	for _, tc := range []struct {
		name   string
		height int64
		commit *types.ExtendedCommit
		want   string
	}{
		{"unexpected-block", 9, nil, "we didn't expect"},
		{"wrong-peer", 1, nil, "not bad"},
		{"extended-commit-height", 1, &types.ExtendedCommit{Height: 2}, "block height 1 != extCommit height 2"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			errs := make(chan peerError, 1)
			errs <- peerError{peerID: "sentinel"}
			pool := lifecyclePool(t, nil, errs)
			pool.makeNextRequester(1)
			result := make(chan error, 1)
			go func() {
				result <- pool.AddBlock("bad", &types.Block{Header: types.Header{Height: tc.height}}, tc.commit, 1)
			}()
			waitLifecycleSender(t, "(*BlockPool).sendError")
			// Verify capacity backpressure leaves the pool usable while still live.
			available := make(chan struct{})
			go func() { pool.Height(); close(available) }()
			waitLifecycleDone(t, available)
			require.Equal(t, p2p.ID("sentinel"), (<-errs).peerID)
			select {
			case delivered := <-errs:
				require.Equal(t, p2p.ID("bad"), delivered.peerID)
				require.ErrorContains(t, delivered.err, tc.want)
				select {
				case returned := <-result:
					require.Equal(t, returned, delivered.err)
				case <-time.After(time.Second):
					t.Fatal("AddBlock did not return after delivery")
				}
			case <-time.After(time.Second):
				t.Fatal("live peer error was lost after queue drain")
			}
			require.True(t, pool.IsRunning())
			pool.mtx.Lock()
			requester := pool.requesters[1]
			pool.mtx.Unlock()
			require.Nil(t, requester.getBlock())
			require.Nil(t, requester.getExtendedCommit())
		})
	}
}

func TestBlockPoolLifecycleSaturatedErrors(t *testing.T) {
	for _, kind := range []string{"unexpected-block", "wrong-peer", "peer-timeout", "slow-peer"} {
		t.Run(kind, func(t *testing.T) {
			errs := make(chan peerError, 1)
			errs <- peerError{}
			pool := lifecyclePool(t, nil, errs)
			pool.makeNextRequester(1)
			if kind == "slow-peer" {
				peer := newBPPeer(pool, "slow", 1, 10)
				peer.numPending = 1
				peer.recvMonitor = flow.New(time.Millisecond, time.Second)
				peer.recvMonitor.SetREMA(1)
				// The monitor reports zero until its first sample has elapsed.
				require.Eventually(t, func() bool { return peer.recvMonitor.Status().CurRate > 0 },
					time.Second, time.Millisecond)
				pool.mtx.Lock()
				pool.peers[peer.id] = peer
				pool.mtx.Unlock()
			}
			done := make(chan struct{})
			go func() {
				switch kind {
				case "unexpected-block":
					_ = pool.AddBlock("bad", &types.Block{Header: types.Header{Height: 9}}, nil, 1)
				case "wrong-peer":
					_ = pool.AddBlock("bad", &types.Block{Header: types.Header{Height: 1}}, nil, 1)
				case "peer-timeout":
					newBPPeer(pool, "bad", 1, 10).onTimeout()
				case "slow-peer":
					pool.removeTimedoutPeers()
				}
				close(done)
			}()
			t.Cleanup(func() { <-errs }) // release RED's blocked sender
			waitLifecycleSender(t, "(*BlockPool).sendError")
			heightDone := make(chan struct{})
			go func() { pool.Height(); close(heightDone) }()
			// Backpressure must not hold the pool lock even before Stop.
			waitLifecycleDone(t, heightDone)
			stopped := make(chan struct{})
			go func() { _ = pool.Stop(); close(stopped) }()
			waitLifecycleDone(t, stopped)
			waitLifecycleDone(t, done)
		})
	}
}
