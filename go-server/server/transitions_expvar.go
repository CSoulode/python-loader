package main

import "expvar"

func init() {
	expvar.Publish("browsing_state_transitions", expvar.Func(func() any {
		return globalTransitionEventLogSnapshot()
	}))
}

var transitionEventLogForExpvar *TransitionEventLog

func setTransitionEventLogForExpvar(log *TransitionEventLog) {
	if transitionEventLogForExpvar == nil && log != nil {
		transitionEventLogForExpvar = log
	}
}

func globalTransitionEventLogSnapshot() map[string]int64 {
	if transitionEventLogForExpvar == nil {
		return map[string]int64{"events": 0}
	}
	return transitionEventLogForExpvar.Snapshot()
}
