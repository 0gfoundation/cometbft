package blocksync

import "time"

// requestRetryTimer is owned by the requester routine, its sole consumer.
// After receiving from C, the owner must call Fired before Stop or Reset.
// None of these operations may run concurrently.
type requestRetryTimer struct {
	*time.Timer
	armed bool
}

func newRequestRetryTimer() *requestRetryTimer {
	timer := &requestRetryTimer{Timer: time.NewTimer(0), armed: true}
	timer.Stop()
	return timer
}

func (timer *requestRetryTimer) Stop() {
	if timer.armed && !timer.Timer.Stop() {
		// In legacy timer mode, expiry can be in flight, not buffered yet.
		// The sole consumer has not received it (Fired clears armed), so
		// wait for that event rather than risk leaving it for the next reset.
		<-timer.C
	}
	timer.armed = false
}

func (timer *requestRetryTimer) Fired() {
	timer.armed = false
}

func (timer *requestRetryTimer) Reset(after time.Duration) {
	timer.Stop()
	timer.Timer.Reset(after)
	timer.armed = true
}
