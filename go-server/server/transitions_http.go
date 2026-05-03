package main

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
)

type transitionAppendResponse struct {
	Accepted bool `json:"accepted"`
	Count    int  `json:"count"`
}

func GetTransitionsHandler(server *DataLoaderServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if server == nil {
			http.Error(w, "server is not configured", http.StatusServiceUnavailable)
			return
		}
		switch r.Method {
		case http.MethodPost:
			handleTransitionPost(w, r, server.ensureTransitionEventLog())
		case http.MethodGet:
			writeJSON(w, http.StatusOK, server.ensureTransitionEventLog().List())
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

func handleTransitionPost(w http.ResponseWriter, r *http.Request, log *TransitionEventLog) {
	event, err := decodeTransitionEvent(r)
	if err != nil {
		log.Reject()
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	log.Append(event)
	writeJSON(w, http.StatusAccepted, transitionAppendResponse{Accepted: true, Count: len(log.List())})
}

func decodeTransitionEvent(r *http.Request) (TransitionEvent, error) {
	defer r.Body.Close()
	var event TransitionEvent
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		return TransitionEvent{}, err
	}
	return event, validateTransitionEvent(event)
}

func validateTransitionEvent(event TransitionEvent) error {
	if strings.TrimSpace(string(event.ParentStateID)) == "" {
		return errors.New("parent_state_id is required")
	}
	if strings.TrimSpace(string(event.TargetStateID)) == "" {
		return errors.New("target_state_id is required")
	}
	if strings.TrimSpace(event.DeltaKind) == "" {
		return errors.New("delta_kind is required")
	}
	return nil
}
