package blocksync

import (
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/cometbft/cometbft/libs/service"
	"github.com/stretchr/testify/require"
)

// Drive the real selection methods without a competing requestRoutine.
type reservationRequester struct{ *bpRequester }

func (*reservationRequester) OnStart() error { return nil }

func TestPeerReservationReleaseOwnership(t *testing.T) {
	for _, mode := range []string{"other-pending", "removed", "replaced", "pool-stopped"} {
		t.Run(mode, func(t *testing.T) {
			pool := lifecyclePool(t, nil, nil)
			pool.SetPeerRange("peer", 1, 10)
			peer := pool.pickIncrAvailablePeer(1, "")
			require.NotNil(t, peer)
			switch mode {
			case "other-pending":
				require.Same(t, peer, pool.pickIncrAvailablePeer(1, ""))
			case "removed", "replaced":
				pool.RemovePeer(peer.id)
				if mode == "replaced" {
					pool.SetPeerRange(peer.id, 1, 10)
				}
			case "pool-stopped":
				require.NoError(t, pool.Stop())
			}
			pool.releasePeerReservation(peer)
			pool.mtx.Lock()
			defer pool.mtx.Unlock()
			if mode == "pool-stopped" {
				require.Zero(t, peer.numPending, "stop already cleared accounting")
			} else {
				require.EqualValues(t, 1, peer.numPending)
			}
			if mode == "other-pending" {
				require.True(t, peer.timeout.Stop(), "other request still needs its timeout")
			}
			if mode == "replaced" {
				require.Zero(t, pool.peers[peer.id].numPending, "must not decrement replacement peer")
			}
		})
	}
}

func TestRequesterStopReleasesUnsentReservation(t *testing.T) {
	for _, secondary := range []bool{false, true} {
		name := "primary"
		if secondary {
			name = "secondary"
		}
		t.Run(name, func(t *testing.T) {
			requests := make(chan BlockRequest, 1)
			pool := lifecyclePool(t, requests, nil)
			pool.SetPeerRange("peer", 1, 10)
			r := newBPRequester(pool, 1)
			r.BaseService = *service.NewBaseService(nil, "reservationRequester", &reservationRequester{r})
			require.NoError(t, r.Start())
			pool.mtx.Lock()
			peer := pool.peers["peer"]
			done := make(chan struct{})
			go func() {
				defer close(done)
				if secondary {
					r.pickSecondPeerAndSendRequest()
				} else {
					r.pickPeerAndSendRequest()
				}
			}()
			// Selection has passed the requester's initial running check, but cannot
			// reserve a peer until the pool lock is released. Stop wins that race.
			blocked := false
			deadline := time.Now().Add(time.Second)
			for time.Now().Before(deadline) {
				buf := make([]byte, 1<<20)
				stacks := string(buf[:runtime.Stack(buf, true)])
				if strings.Contains(stacks, "(*BlockPool).pickIncrAvailablePeer") {
					blocked = true
					break
				}
				runtime.Gosched()
			}
			err := r.Stop()
			pool.mtx.Unlock()
			require.True(t, blocked, "selection never reached pool lock")
			require.NoError(t, err)
			waitLifecycleDone(t, done)
			require.Empty(t, requests, "stopped requester must not send")
			pool.mtx.Lock()
			pending := peer.numPending
			stopped := peer.timeout == nil || !peer.timeout.Stop()
			pool.mtx.Unlock()
			require.Zero(t, pending, "unsent reservation must not leave a healthy peer pending")
			require.True(t, stopped, "last reservation cancellation must stop peer timeout")
		})
	}
}
