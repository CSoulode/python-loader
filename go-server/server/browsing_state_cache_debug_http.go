package main

import (
	"encoding/json"
	"net/http"
	"strings"
)

type bsInvalidateRequest struct {
	Scope string `json:"scope"`
	Kind  string `json:"kind"`
	Key   string `json:"key"`
}

type bsInvalidateResponse struct {
	Invalidated int `json:"invalidated"`
}

func GetBrowsingStateCacheInvalidateHandler(
	server *DataLoaderServer,
) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if server == nil {
			http.Error(w, "server is not configured", http.StatusServiceUnavailable)
			return
		}

		var req bsInvalidateRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid JSON body", http.StatusBadRequest)
			return
		}

		scope := strings.TrimSpace(req.Scope)
		switch scope {
		case "all":
			writeJSON(w, http.StatusOK, bsInvalidateResponse{
				Invalidated: server.ensureBrowsingStateCache().InvalidateAll(),
			})
		case "dependency":
			kind := strings.TrimSpace(req.Kind)
			key := strings.TrimSpace(req.Key)
			if kind == "" || key == "" {
				http.Error(w, "dependency invalidation requires kind and key", http.StatusBadRequest)
				return
			}
			writeJSON(w, http.StatusOK, bsInvalidateResponse{
				Invalidated: server.ensureBrowsingStateCache().InvalidateDependency(bsCacheDependency{
					Kind: kind,
					Key:  key,
				}),
			})
		default:
			http.Error(w, "scope must be all or dependency", http.StatusBadRequest)
		}
	}
}
