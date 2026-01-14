package main

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	pb "m3.dataloader/dataloader"
	"m3.dataloader/utilities"
)

// GET /cell
func GetBrowsingStateHandler(client pb.DataLoaderClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		req := &pb.GetCellRequest{
			XAxis:    r.URL.Query().Get("xAxis"),
			YAxis:    r.URL.Query().Get("yAxis"),
			ZAxis:    r.URL.Query().Get("zAxis"),
			Filters:  r.URL.Query().Get("filters"),
			All:      r.URL.Query().Get("all"),
			Timeline: r.URL.Query().Get("timeline"),
		}
		axisX, axisY, axisZ, filters, err := oldParseAxesAndFilters(req)

		if err != nil {
			http.Error(w, fmt.Sprintf("invalid parameters: %v", err), http.StatusBadRequest)
			return
		}

		getValueType := func(valueTypeString string) pb.FilterValueType {
			switch valueTypeString {
			case "node":
				return pb.FilterValueType_NODE
			case "tag":
				return pb.FilterValueType_TAG
			case "daterange":
				return pb.FilterValueType_DATERANGE
			case "dateRange":
				return pb.FilterValueType_DATERANGE
			case "timeRange":
				return pb.FilterValueType_TIMERANGE
			case "timestampRange":
				return pb.FilterValueType_TIMESTAMPRANGE
			default:
				return pb.FilterValueType_UNDEFINED
			}
		}

		newReq := &pb.GetBrowsingStateRequest{
			Filters:  make([]*pb.AxisFilter, 0, 3),
			All:      req.All,
			Timeline: req.Timeline,
		}

		newReq.Filters[0] = &pb.AxisFilter{
			AxisFilterType: pb.AxisType_X_AXIS,
			Value:          int32(utilities.ConditionalAssignInt(axisX.Id != -1, axisX.Id, -1)),
			ValueType:      getValueType(axisX.Type),
		}
		newReq.Filters[1] = &pb.AxisFilter{
			AxisFilterType: pb.AxisType_Y_AXIS,
			Value:          int32(utilities.ConditionalAssignInt(axisY.Id != -1, axisY.Id, -1)),
			ValueType:      getValueType(axisY.Type),
		}
		newReq.Filters[2] = &pb.AxisFilter{
			AxisFilterType: pb.AxisType_Z_AXIS,
			Value:          int32(utilities.ConditionalAssignInt(axisZ.Id != -1, axisZ.Id, -1)),
			ValueType:      getValueType(axisZ.Type),
		}

		for _, f := range filters {
			for _, i := range f.Ids {
				newReq.Filters = append(newReq.Filters, &pb.AxisFilter{
					AxisFilterType: pb.AxisType_FILTER,
					Value:          int32(i),
					ValueType:      getValueType(f.Type),
				})
			}
		}

		stream, err := client.GetBrowsingState(r.Context(), newReq)
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

// GET api/tagsets?tagTypeId=1
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

// GET api/tagsets/:id
/*func GetTagsetsByIdHandler(client pb.DataLoaderClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
		if err != nil {
			http.Error(w, "invalid id", http.StatusBadRequest)
			return
		}
		res, err := client.GetChildNodes(r.Context(), &pb.IdRequest{Id: id})
		if err != nil {
			http.Error(w, fmt.Sprintf("rpc error: %v", err), http.StatusBadGateway)
			return
		}
		writeStreamAsJSON(w, func() (proto.Message, error) { return stream.Recv() })
	}
}*/

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
