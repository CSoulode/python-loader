package main

import (
	"database/sql"

	pb "m3.dataloader/dataloader"
)

func scanCubeObjectRow(rows *sql.Rows, withTimeline bool) (*pb.CubeObject, error) {
	cubeObject := &pb.CubeObject{}
	var thumb sql.NullString
	if withTimeline {
		var timelineValue any
		if err := rows.Scan(&cubeObject.Id, &cubeObject.FileUri, &thumb, &timelineValue); err != nil {
			return nil, err
		}
	} else if err := rows.Scan(&cubeObject.Id, &cubeObject.FileUri, &thumb); err != nil {
		return nil, err
	}
	if thumb.Valid {
		cubeObject.ThumbnailUri = thumb.String
	}
	return cubeObject, nil
}
