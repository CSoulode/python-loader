package main

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "m3.dataloader/dataloader"
)

// GET /cell
func GetCellHandler(client pb.DataLoaderClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req := &pb.GetCellRequest{
			XAxis:    r.URL.Query().Get("xAxis"),
			YAxis:    r.URL.Query().Get("yAxis"),
			ZAxis:    r.URL.Query().Get("zAxis"),
			Filters:  r.URL.Query().Get("filters"),
			All:      r.URL.Query().Get("all"),
			Timeline: r.URL.Query().Get("timeline"),
		}
		stream, err := client.GetCell(r.Context(), req)
		if err != nil {
			http.Error(w, fmt.Sprintf("rpc error: %v", err), http.StatusBadGateway)
			return
		}
		writeStreamAsJSON(w, func() (proto.Message, error) { return stream.Recv() })
	}
}

// GET /node/:parentId/children?parentId=123
func GetChildNodesHandler(client pb.DataLoaderClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pid, err := strconv.ParseInt(r.PathValue("parentId"), 10, 64)
		if err != nil {
			http.Error(w, "invalid parentId", http.StatusBadRequest)
			return
		}
		stream, err := client.GetChildNodes(r.Context(), &pb.IdRequest{Id: pid})
		if err != nil {
			http.Error(w, fmt.Sprintf("rpc error: %v", err), http.StatusBadGateway)
			return
		}
		writeStreamAsJSON(w, func() (proto.Message, error) { return stream.Recv() })
	}
}

// GET /tagsets?tagTypeId=1
func GetTagsetsHandler(client pb.DataLoaderClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req := &pb.GetTagSetsRequest{}
		if v := r.URL.Query().Get("tagTypeId"); v != "" {
			if id, err := strconv.ParseInt(v, 10, 64); err == nil {
				req.TagTypeId = id
			}
		}
		stream, err := client.GetTagSets(r.Context(), req)
		if err != nil {
			http.Error(w, fmt.Sprintf("rpc error: %v", err), http.StatusBadGateway)
			return
		}
		writeStreamAsJSON(w, func() (proto.Message, error) { return stream.Recv() })
	}
}

func writeStreamAsJSON(w http.ResponseWriter, recv func() (proto.Message, error)) {
	var msgs []proto.Message
	for {
		m, err := recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			http.Error(w, fmt.Sprintf("stream error: %v", err), http.StatusBadGateway)
			return
		}
		msgs = append(msgs, m)
	}

	marshaler := protojson.MarshalOptions{
		UseProtoNames:   true,
		EmitUnpopulated: true,
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write([]byte("["))
	for i, m := range msgs {
		b, err := marshaler.Marshal(m)
		if err != nil {
			http.Error(w, fmt.Sprintf("json marshal error: %v", err), http.StatusInternalServerError)
			return
		}
		if i > 0 {
			w.Write([]byte(","))
		}
		w.Write(b)
	}
	w.Write([]byte("]"))
}
