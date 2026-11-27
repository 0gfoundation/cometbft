package blocksync

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/types"
)

func receiveRedoRequest(t *testing.T, requests <-chan BlockRequest) BlockRequest {
	t.Helper()
	select {
	case request := <-requests:
		return request
	case <-time.After(time.Second):
		t.Fatal("redo did not reschedule request")
		return BlockRequest{}
	}
}

// blockedRedoRequester keeps the real routine in sendRequest while tests drive
// the mutex-protected redo consumer deterministically.
func blockedRedoRequester(t *testing.T) *bpRequester {
	pool := lifecyclePool(t, nil, nil)
	pool.SetPeerRange("a", 1, 10)
	pool.makeNextRequester(1)
	pool.mtx.Lock()
	r := pool.requesters[1]
	pool.mtx.Unlock()
	require.Eventually(t, func() bool { return r.didRequestFrom("a") }, time.Second, time.Millisecond)
	return r
}

func TestRequesterRedoBoundedState(t *testing.T) {
	r := blockedRedoRequester(t)
	r.mtx.Lock()
	r.secondPeerID = "b"
	r.mtx.Unlock()
	for i := 0; i < 1000; i++ {
		r.redo("a")
		r.redo("b")
		r.redo("unknown")
	}
	require.Len(t, r.redoCh, 1)
	r.mtx.Lock()
	pending := r.pendingRedo
	r.mtx.Unlock()
	require.Equal(t, p2p.ID("a"), pending[0].peerID)
	require.Equal(t, p2p.ID("b"), pending[1].peerID)
	require.True(t, r.consumeRedo())
	require.Empty(t, r.requestedFrom())
	require.False(t, r.consumeRedo(), "duplicates must not survive batch consumption")
}

func TestRequesterRedoEpochAndStaleWake(t *testing.T) {
	r := blockedRedoRequester(t)
	r.redo("a")
	require.True(t, r.beginRequestEpoch())
	block := &types.Block{Header: types.Header{Height: 1}}
	require.True(t, r.setBlock(block, nil, "a"))
	<-r.redoCh // old wake may remain, but old pending state must not
	require.False(t, r.consumeRedo())
	require.Equal(t, []p2p.ID{"a"}, r.requestedFrom())
	require.Same(t, block, r.getBlock())
	r.redo("unknown")
	require.Empty(t, r.redoCh)
	require.False(t, r.consumeRedo())
}

// A delayed gotBlock wake after redo must not suppress the live secondary
// request on a height update. No retry-duration override is needed.
func TestRequesterRedoStaleBlockWake(t *testing.T) {
	requests := make(chan BlockRequest)
	pool := lifecyclePool(t, requests, nil)
	pool.SetPeerRange("a", 1, 10)
	pool.SetPeerRange("b", 1, 10)
	pool.makeNextRequester(1)
	receiveRedoRequest(t, requests)
	pool.mtx.Lock()
	r := pool.requesters[1]
	pool.mtx.Unlock()
	require.Eventually(t, func() bool { return len(r.requestedFrom()) == 2 }, time.Second, time.Millisecond)
	r.mtx.Lock()
	second := r.secondPeerID
	r.mtx.Unlock()
	require.True(t, r.setBlock(&types.Block{Header: types.Header{Height: 1}}, nil, second))
	r.redo(second)
	require.False(t, r.consumeRedo())
	receiveRedoRequest(t, requests) // unblock routine, exposing the stale block wake
	require.Eventually(t, func() bool { return len(r.gotBlockCh) == 0 }, time.Second, time.Millisecond)
	r.newHeight(1)
	require.Equal(t, second, receiveRedoRequest(t, requests).PeerID)
}

func TestRequesterRedoPreservesOtherPeerBlock(t *testing.T) {
	r := blockedRedoRequester(t)
	r.mtx.Lock()
	r.secondPeerID = "b"
	r.mtx.Unlock()
	block := &types.Block{Header: types.Header{Height: 1}}
	commit := &types.ExtendedCommit{Height: 1}
	require.True(t, r.setBlock(block, commit, "b"))
	r.redo("a")
	require.False(t, r.consumeRedo())
	require.Same(t, block, r.getBlock())
	require.Same(t, commit, r.getExtendedCommit())
	require.Equal(t, []p2p.ID{"b"}, r.requestedFrom())
	r.redo("b")
	require.True(t, r.consumeRedo())
	require.Nil(t, r.getBlock())
	require.Nil(t, r.getExtendedCommit())
}

func TestRequesterRedoConcurrentAndStopped(t *testing.T) {
	r := blockedRedoRequester(t)
	for iteration := 0; iteration < 100; iteration++ {
		require.True(t, r.beginRequestEpoch())
		r.mtx.Lock()
		r.peerID = "a"
		r.secondPeerID = "b"
		r.mtx.Unlock()
		var producers sync.WaitGroup
		for _, peer := range []p2p.ID{"a", "b"} {
			producers.Add(1)
			go func(peer p2p.ID) {
				defer producers.Done()
				for i := 0; i < 100; i++ {
					r.redo(peer)
				}
			}(peer)
		}
		r.consumeRedo()
		producers.Wait()
		select {
		case <-r.redoCh:
			r.consumeRedo()
		default:
		}
		require.Empty(t, r.requestedFrom(), "concurrent batch consumption must not lose a wake")
	}
	require.NoError(t, r.Stop())
	for i := 0; i < 1000; i++ {
		r.redo("a")
		r.redo("b")
	}
	require.False(t, r.beginRequestEpoch())
	r.mtx.Lock()
	pending := r.pendingRedo
	r.mtx.Unlock()
	require.Equal(t, [2]redoRequest{}, pending, "stopped redo must not retain peer references")
	require.False(t, r.consumeRedo())
	require.Empty(t, r.redoCh)
	require.Empty(t, r.requestedFrom())
}

// With the sender blocked on its second request, both assignments exist but
// requestRoutine cannot consume redo yet. Losing either event stalls retries.
func TestRequesterRedoPreservesBothPeers(t *testing.T) {
	requests := make(chan BlockRequest)
	pool := lifecyclePool(t, requests, nil)
	pool.SetPeerRange("a", 1, 10)
	pool.SetPeerRange("b", 1, 10)
	pool.makeNextRequester(1)
	first := receiveRedoRequest(t, requests)
	pool.mtx.Lock()
	r := pool.requesters[1]
	pool.mtx.Unlock()
	require.Eventually(t, func() bool { return len(r.requestedFrom()) == 2 }, time.Second, time.Millisecond)
	for i := 0; i < 1000; i++ {
		pool.RedoRequestFrom(1, first.PeerID)
	}
	other := p2p.ID("a")
	if first.PeerID == other {
		other = "b"
	}
	pool.RedoRequestFrom(1, other)
	second := receiveRedoRequest(t, requests)
	require.Equal(t, other, second.PeerID)
	receiveRedoRequest(t, requests)
}
