package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTransitionEventLogOverwritesOldest(t *testing.T) {
	log := newTransitionEventLog(2)
	log.Append(TransitionEvent{ParentStateID: "p1", TargetStateID: "t1", DeltaKind: "Root"})
	log.Append(TransitionEvent{ParentStateID: "p2", TargetStateID: "t2", DeltaKind: "Root"})
	log.Append(TransitionEvent{ParentStateID: "p3", TargetStateID: "t3", DeltaKind: "Root"})

	items := log.List()
	if len(items) != 2 {
		t.Fatalf("items = %d, want 2", len(items))
	}
	if items[0].ParentStateID != "p2" || items[1].ParentStateID != "p3" {
		t.Fatalf("items = %#v, want p2,p3", items)
	}
}

func TestTransitionsEndpointAcceptsValidEvent(t *testing.T) {
	server := &DataLoaderServer{transitionLog: newTransitionEventLog(4)}
	body := bytes.NewBufferString(`{"parent_state_id":"p","target_state_id":"t","delta_kind":"Root"}`)
	request := httptest.NewRequest(http.MethodPost, "/v1/transitions", body)
	recorder := httptest.NewRecorder()

	GetTransitionsHandler(server).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want 202", recorder.Code)
	}
	var response transitionAppendResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if !response.Accepted || response.Count != 1 {
		t.Fatalf("response = %#v, want accepted count 1", response)
	}
}

func TestTransitionsEndpointRejectsMalformedEvent(t *testing.T) {
	server := &DataLoaderServer{transitionLog: newTransitionEventLog(4)}
	request := httptest.NewRequest(http.MethodPost, "/v1/transitions", bytes.NewBufferString(`{"target_state_id":"t"}`))
	recorder := httptest.NewRecorder()

	GetTransitionsHandler(server).ServeHTTP(recorder, request)
	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", recorder.Code)
	}
	if got := server.transitionLog.Snapshot()["rejected"]; got != 1 {
		t.Fatalf("rejected = %d, want 1", got)
	}
}
