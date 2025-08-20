package main

import (
	"encoding/json"
	"fmt"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

func (s *DataLoaderServer) GetCell(req *pb.GetCellRequest, stream pb.DataLoader_GetCellServer) error {
	// Axes come in as JSON blobs
	var axisX, axisY, axisZ qg.ParsedAxis
	if req.XAxis != "" {
		if err := json.Unmarshal([]byte(req.XAxis), &axisX); err != nil {
			return fmt.Errorf("invalid xAxis JSON: %w", err)
		}
	} else {
		axisX = qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}}
	}
	if req.YAxis != "" {
		if err := json.Unmarshal([]byte(req.YAxis), &axisY); err != nil {
			return fmt.Errorf("invalid yAxis JSON: %w", err)
		}
	} else {
		axisY = qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}}
	}
	if req.ZAxis != "" {
		if err := json.Unmarshal([]byte(req.ZAxis), &axisZ); err != nil {
			return fmt.Errorf("invalid zAxis JSON: %w", err)
		}
	} else {
		axisZ = qg.ParsedAxis{Type: "", Id: -1, Ids: map[int]int{1: 1}}
	}

	// Filters as JSON array
	var filters []qg.ParsedFilter
	if req.Filters != "" {
		if err := json.Unmarshal([]byte(req.Filters), &filters); err != nil {
			return fmt.Errorf("invalid filters JSON: %w", err)
		}
	}

	// Flags for “all” and “timeline”
	allDefined := req.All != ""
	timelineDefined := req.Timeline != ""
	sqlstr := ""
	// 2) Shortcut: “all” → PublicCubeObjects
	if allDefined {
		sqlstr = qg.GenerateSQLQueryForCell(
			axisX.Type, axisX.Id,
			axisY.Type, axisY.Id,
			axisZ.Type, axisZ.Id,
			filters,
		)
	}

	if timelineDefined {
		sqlstr = qg.GenerateSQLQueryForTimeline(filters)
	}

	if sqlstr != "" {
		rows, err := s.db.QueryContext(stream.Context(), sqlstr)
		if err != nil {
			return fmt.Errorf("failed to execute query: %w", err)
		}
		defer rows.Close()

		var cubeObjects []*pb.CubeObject
		for rows.Next() {
			c := &pb.CubeObject{}
			if err := rows.Scan(&c.Id, &c.FileUri, &c.ThumbnailUri); err != nil {
				return fmt.Errorf("failed to scan row: %w", err)
			}
			cubeObjects = append(cubeObjects, c)
		}

		if len(cubeObjects) > 0 {
			resp := &pb.CellResponse{
				CubeObjects: cubeObjects,
			}
			if err := stream.Send(resp); err != nil {
				return fmt.Errorf("failed to send CellResponse: %w", err)
			}
		}

		return nil
	}

	if err := axisX.InitializeIds(stream.Context(), s.db); err != nil {
		return fmt.Errorf("init xAxis: %w", err)
	}
	if err := axisY.InitializeIds(stream.Context(), s.db); err != nil {
		return fmt.Errorf("init yAxis: %w", err)
	}
	if err := axisZ.InitializeIds(stream.Context(), s.db); err != nil {
		return fmt.Errorf("init zAxis: %w", err)
	}

	sqlstr = qg.GenerateSQLQueryForState(
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
	)
	rows, err := s.db.QueryContext(stream.Context(), sqlstr)
	if err != nil {
		return fmt.Errorf("query error: %w", err)
	}
	defer rows.Close()

	// local struct to hold each DB row
	type rowCell struct {
		X            int
		Y            int
		Z            int
		Id           int32
		FileUri      string
		ThumbnailUri string
		Count        int32
	}

	for rows.Next() {
		var r rowCell
		if err := rows.Scan(
			&r.X, &r.Y, &r.Z,
			&r.Id, &r.FileUri, &r.ThumbnailUri,
			&r.Count,
		); err != nil {
			return fmt.Errorf("scan state row: %w", err)
		}

		// map the axis‐IDs through the position maps
		posX := axisX.Ids[r.X]
		posY := axisY.Ids[r.Y]
		posZ := axisZ.Ids[r.Z]

		// build and send one CellResponse per row
		resp := &pb.CellResponse{
			X:     int32(posX),
			Y:     int32(posY),
			Z:     int32(posZ),
			Count: r.Count,
			CubeObjects: []*pb.CubeObject{{
				Id:           r.Id,
				FileUri:      r.FileUri,
				ThumbnailUri: r.ThumbnailUri,
			}},
		}
		if err := stream.Send(resp); err != nil {
			return fmt.Errorf("send state response: %w", err)
		}
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration: %w", err)
	}

	return nil
}
