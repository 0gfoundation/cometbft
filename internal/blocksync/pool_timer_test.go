package blocksync

import (
	"testing"
	"time"
)

func awaitRetryTimer(t *testing.T, timer *requestRetryTimer) time.Time {
	t.Helper()
	select {
	case fired := <-timer.C:
		timer.Fired()
		return fired
	case <-time.After(5 * time.Second):
		t.Fatal("retry timer did not fire")
		return time.Time{}
	}
}

func assertRetryTimerQuiet(t *testing.T, timer *requestRetryTimer) {
	t.Helper()
	select {
	case <-timer.C:
		timer.Fired()
		t.Fatal("stopped retry timer fired")
	case <-time.After(10 * time.Millisecond):
	}
}

// Arm the real timer before checking an expired, unconsumed event. In legacy
// mode, wait for observable buffered delivery rather than assume a sleep fired
// the timer. Synchronous timers have no buffered event; Reset must revoke the
// old deadline's receive instead.
func expireRetryTimer(t *testing.T, timer *requestRetryTimer) {
	t.Helper()
	timer.Reset(0)
	if cap(timer.C) == 0 {
		return
	}
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for len(timer.C) == 0 {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("legacy timer event was not buffered")
		}
	}
}

func TestRequestRetryTimerStartsStopped(t *testing.T) {
	timer := newRequestRetryTimer()
	defer timer.Stop()
	assertRetryTimerQuiet(t, timer)
}

func TestRequestRetryTimerResetAfterConsumedExpiry(t *testing.T) {
	timer := newRequestRetryTimer()
	defer timer.Stop()
	for i := 0; i < 3; i++ {
		timer.Reset(time.Millisecond)
		awaitRetryTimer(t, timer)
	}
}

func TestRequestRetryTimerResetRevokesOldExpiry(t *testing.T) {
	for _, stopFirst := range []bool{false, true} {
		name := "expired"
		if stopFirst {
			name = "stopped_expired"
		}
		t.Run(name, func(t *testing.T) {
			timer := newRequestRetryTimer()
			defer timer.Stop()
			expireRetryTimer(t, timer)
			if stopFirst {
				timer.Stop()
			}
			const delay = 20 * time.Millisecond
			start := time.Now()
			timer.Reset(delay)
			// Check the event's timestamp, not scheduler-sensitive receive time:
			// a stale buffered event must fail even if this goroutine runs late.
			if fired := awaitRetryTimer(t, timer); fired.Before(start.Add(delay)) {
				t.Fatalf("old expiry survived reset: fired %v, reset %v", fired, start)
			}
		})
	}
}

func TestRequestRetryTimerStopCleanup(t *testing.T) {
	for _, state := range []string{"stopped", "active", "expiring", "expired", "consumed"} {
		t.Run(state, func(t *testing.T) {
			timer := newRequestRetryTimer()
			func() {
				defer timer.Stop()
				switch state {
				case "active":
					timer.Reset(time.Hour)
				case "expiring":
					timer.Reset(0) // Stop may race legacy delivery before it is buffered.
				case "expired":
					expireRetryTimer(t, timer)
				case "consumed":
					timer.Reset(0)
					awaitRetryTimer(t, timer)
				}
			}()
			if timer.Timer.Stop() {
				t.Fatal("cleanup left the underlying timer active")
			}
			assertRetryTimerQuiet(t, timer)
			timer.Stop() // cleanup is idempotent
			assertRetryTimerQuiet(t, timer)
		})
	}
}
