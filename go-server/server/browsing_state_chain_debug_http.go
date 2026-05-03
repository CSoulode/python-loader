package main

import (
	"encoding/json"
	"net/http"
	"strings"
)

type browsingStateInvalidationRequest struct {
	All        bool                     `json:"all"`
	Dependency *browsingStateDependency `json:"dependency"`
	Subtree    StateID                  `json:"subtree"`
}

type browsingStateInvalidationResponse struct {
	Removed int `json:"removed"`
}

func GetBrowsingStateChainInvalidationHandler(server *DataLoaderServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if server == nil {
			http.Error(w, "server is not configured", http.StatusServiceUnavailable)
			return
		}
		request, err := decodeBrowsingStateInvalidationRequest(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		removed := executeBrowsingStateInvalidation(server.ensureBrowsingStateChain(), request)
		writeJSON(w, http.StatusOK, browsingStateInvalidationResponse{Removed: removed})
	}
}

func decodeBrowsingStateInvalidationRequest(r *http.Request) (browsingStateInvalidationRequest, error) {
	defer r.Body.Close()
	if request, ok := invalidationRequestFromQuery(r); ok {
		return request, nil
	}
	var request browsingStateInvalidationRequest
	err := json.NewDecoder(r.Body).Decode(&request)
	return request, err
}

func invalidationRequestFromQuery(r *http.Request) (browsingStateInvalidationRequest, bool) {
	query := r.URL.Query()
	scope := strings.ToLower(strings.TrimSpace(query.Get("scope")))
	if scope == "all" {
		return browsingStateInvalidationRequest{All: true}, true
	}
	if scope == "subtree" {
		return browsingStateInvalidationRequest{Subtree: StateID(query.Get("stateId"))}, true
	}
	if scope != "dependency" {
		return browsingStateInvalidationRequest{}, false
	}
	dep := &browsingStateDependency{Kind: query.Get("kind"), ID: query.Get("id")}
	return browsingStateInvalidationRequest{Dependency: dep}, true
}

func executeBrowsingStateInvalidation(chain *BrowsingStateChain, request browsingStateInvalidationRequest) int {
	if request.All {
		return chain.InvalidateAll()
	}
	if request.Dependency != nil {
		return chain.InvalidateDependency(*request.Dependency)
	}
	return chain.InvalidateSubtree(request.Subtree)
}
