package blocksync

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bcproto "github.com/cometbft/cometbft/api/cometbft/blocksync/v1"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/p2p"
	sm "github.com/cometbft/cometbft/state"
)

type lifecycleConsensusCall struct {
	state   sm.State
	skipWAL bool
}

type lifecycleConsensus struct {
	p2p.BaseReactor
	calls chan lifecycleConsensusCall
}

func (r *lifecycleConsensus) SwitchToConsensus(state sm.State, skipWAL bool) {
	r.calls <- lifecycleConsensusCall{state, skipWAL}
}

// Withhold only block requests; status exchange still uses the production reactor.
type lifecycleWithholdingSource struct {
	*Reactor
	requests chan int64
}

func (r *lifecycleWithholdingSource) Receive(e p2p.Envelope) {
	if request, ok := e.Message.(*bcproto.BlockRequest); ok {
		select {
		case r.requests <- request.Height:
		default:
		}
		return
	}
	r.Reactor.Receive(e)
}

func lifecycleReactors(t *testing.T, withhold bool) (*Reactor, *Reactor, *lifecycleConsensus, <-chan int64) {
	t.Helper()
	genesis, validators := randGenesisDoc()
	sourcePair := newReactor(t, log.NewNopLogger(), genesis, validators, 10)
	targetPair := newReactor(t, log.NewNopLogger(), genesis, validators, 0)
	source, target := sourcePair.reactor.Reactor, targetPair.reactor.Reactor
	source.blockSync = false
	consensus := &lifecycleConsensus{calls: make(chan lifecycleConsensusCall, 1)}
	consensus.BaseReactor = *p2p.NewBaseReactor("lifecycleConsensus", consensus)
	requests := make(chan int64, 100)
	// Register the embedded production Reactor, not ByzantineReactor.Receive,
	// whose copied synchronous implementation bypasses the production handler.
	switches := p2p.MakeConnectedSwitches(cfg.DefaultP2PConfig(), 2, func(i int, s *p2p.Switch) *p2p.Switch {
		if i == 0 {
			if withhold {
				s.AddReactor("BLOCKSYNC", &lifecycleWithholdingSource{source, requests})
			} else {
				s.AddReactor("BLOCKSYNC", source)
			}
		} else {
			s.AddReactor("BLOCKSYNC", target)
			s.AddReactor("CONSENSUS", consensus)
		}
		return s
	}, p2p.Connect2Switches)
	t.Cleanup(func() {
		// Stop network/reactor users before closing the application connections.
		for _, s := range switches {
			assert.NoError(t, s.Stop())
		}
		assert.NoError(t, sourcePair.app.Stop())
		assert.NoError(t, targetPair.app.Stop())
	})
	return source, target, consensus, requests
}

func assertLifecyclePoolStopped(t *testing.T, pool *BlockPool, retained []*bpRequester) {
	t.Helper()
	assert.False(t, pool.IsRunning())
	pool.mtx.Lock()
	assert.Empty(t, pool.requesters)
	for _, peer := range pool.peers {
		assert.Zero(t, peer.numPending)
		if peer.timeout != nil {
			assert.False(t, peer.timeout.Stop(), "peer timer survived shutdown")
		}
	}
	pool.mtx.Unlock()
	for _, requester := range retained {
		assert.False(t, requester.IsRunning())
		assert.Nil(t, requester.getBlock())
		assert.Nil(t, requester.getExtendedCommit())
	}
}

func lifecycleRetainedRequesters(t *testing.T, pool *BlockPool) []*bpRequester {
	t.Helper()
	var retained []*bpRequester
	require.Eventually(t, func() bool {
		pool.mtx.Lock()
		defer pool.mtx.Unlock()
		for _, requester := range pool.requesters {
			retained = append(retained, requester)
		}
		return len(retained) > 0
	}, 10*time.Second, time.Millisecond)
	return retained
}

func lifecycleLateResponses(t *testing.T, source, target *Reactor) {
	t.Helper()
	height := target.store.Height()
	block, _ := source.store.LoadBlock(10)
	require.NotNil(t, block)
	protoBlock, err := block.ToProto()
	require.NoError(t, err)
	response := &bcproto.BlockResponse{Block: protoBlock, ExtCommit: source.store.LoadBlockExtendedCommit(10).ToProto()}
	require.NoError(t, ValidateMsg(response))
	peers := target.Switch.Peers().Copy()
	require.Len(t, peers, 1)
	peer := peers[0]
	// Exercise real Receive (which launches an asynchronous block handler).
	target.Receive(p2p.Envelope{ChannelID: BlocksyncChannel, Src: peer, Message: &bcproto.NoBlockResponse{Height: 10}})
	target.Receive(p2p.Envelope{ChannelID: BlocksyncChannel, Src: peer, Message: response})
	// Receive returning does NOT establish handler completion. Run the same valid
	// response synchronously as an explicit completed post-stop operation as well.
	target.handlePeerResponse(response, peer)
	for i := 0; i < 100; i++ {
		target.pool.RedoRequestFrom(10, peer.ID())
	}
	target.pool.makeNextRequester(1)
	assertLifecyclePoolStopped(t, target.pool, nil)
	assert.Equal(t, height, target.store.Height(), "late responses must not advance the store")
}

// Fails if consensus handoff does not stop/clear the real reactor's pool, or
// if stopped-pool responses can retain blocks or regenerate requesters.
func TestReactorLifecycleConsensusHandoff(t *testing.T) {
	source, target, consensus, _ := lifecycleReactors(t, false)
	retained := lifecycleRetainedRequesters(t, target.pool)
	select {
	case call := <-consensus.calls:
		require.Positive(t, call.state.LastBlockHeight)
		require.True(t, call.skipWAL)
		require.Equal(t, call.state.LastBlockHeight, target.store.Height())
		require.NotNil(t, target.store.LoadBlockExtendedCommit(call.state.LastBlockHeight))
	case <-time.After(10 * time.Second):
		t.Fatal("production reactor did not switch to consensus")
	}
	require.True(t, target.IsRunning(), "handoff stops the pool, not the reactor")
	assertLifecyclePoolStopped(t, target.pool, retained)
	lifecycleLateResponses(t, source, target)
}

// Fails if external Stop cannot interrupt outstanding network requests, leaks
// requester state, or incorrectly invokes consensus without syncing a block.
func TestReactorLifecycleExternalStopPending(t *testing.T) {
	source, target, consensus, requests := lifecycleReactors(t, true)
	var height int64
	select {
	case height = <-requests:
	case <-time.After(5 * time.Second):
		t.Fatal("source did not receive a block request")
	}
	peerID := target.Switch.Peers().Copy()[0].ID()
	require.Eventually(t, func() bool {
		target.pool.mtx.Lock()
		defer target.pool.mtx.Unlock()
		requester := target.pool.requesters[height]
		peer := target.pool.peers[peerID]
		return requester != nil && requester.didRequestFrom(peerID) && peer != nil && peer.numPending > 0
	}, time.Second, time.Millisecond)
	retained := lifecycleRetainedRequesters(t, target.pool)
	stopped := make(chan error, 1)
	go func() { stopped <- target.Stop() }()
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("reactor Stop blocked with outstanding requests")
	}
	assertLifecyclePoolStopped(t, target.pool, retained)
	lifecycleLateResponses(t, source, target)
	assert.Zero(t, target.store.Height())
	select {
	case call := <-consensus.calls:
		t.Fatalf("unexpected consensus callback at height %d", call.state.LastBlockHeight)
	default:
	}
}
