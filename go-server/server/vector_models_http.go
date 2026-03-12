package main

import (
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type vectorModelsHTTPResponse struct {
	DefaultModel string                `json:"defaultModel"`
	Models       []vectorModelHTTPInfo `json:"models"`
}

type vectorModelHTTPInfo struct {
	Name           string `json:"name"`
	TagsetName     string `json:"tagsetName"`
	Dim            int32  `json:"dim"`
	DistanceMetric string `json:"distanceMetric"`
}

func GetVectorModelsHandler(server *DataLoaderServer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		resp, err := server.listVectorModels(r.Context())
		if err != nil {
			http.Error(w, vectorFilterHTTPMessage(err), mapVectorModelsHTTPStatus(err))
			return
		}

		out := vectorModelsHTTPResponse{
			DefaultModel: resp.GetDefaultModel(),
			Models:       make([]vectorModelHTTPInfo, 0, len(resp.GetModels())),
		}
		for _, model := range resp.GetModels() {
			out.Models = append(out.Models, vectorModelHTTPInfo{
				Name:           model.GetName(),
				TagsetName:     model.GetTagsetName(),
				Dim:            model.GetDim(),
				DistanceMetric: model.GetDistanceMetric(),
			})
		}

		w.Header().Set("Cache-Control", "max-age=60")
		writeJSON(w, http.StatusOK, out)
	}
}

func mapVectorModelsHTTPStatus(err error) int {
	switch status.Code(err) {
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.DeadlineExceeded:
		return http.StatusGatewayTimeout
	case codes.Unavailable, codes.FailedPrecondition:
		return http.StatusServiceUnavailable
	default:
		return http.StatusBadGateway
	}
}
