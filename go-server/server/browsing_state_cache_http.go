package main

import "net/http"

func compatResponsesFromBSCacheValue(
	value bsCacheValue,
) []compatBrowsingStateResponse {
	if len(value.Cells) == 0 {
		return nil
	}

	out := make([]compatBrowsingStateResponse, 0, len(value.Cells))
	for _, cell := range value.Cells {
		resp := compatBrowsingStateResponse{
			X:     cell.X,
			Y:     cell.Y,
			Z:     cell.Z,
			Count: cell.Count,
		}
		if len(cell.CubeObjects) > 0 {
			resp.CubeObjects = make([]compatCubeObject, 0, len(cell.CubeObjects))
			for _, cubeObject := range cell.CubeObjects {
				resp.CubeObjects = append(resp.CubeObjects, compatCubeObject{
					Id:           cubeObject.ID,
					FileURI:      cubeObject.FileURI,
					ThumbnailURI: cubeObject.ThumbnailURI,
					FileUri:      cubeObject.FileURI,
					ThumbnailUri: cubeObject.ThumbnailURI,
				})
			}
		}
		out = append(out, resp)
	}
	return out
}

func cachedCellsFromCompatResponses(
	responses []compatBrowsingStateResponse,
) []cachedBrowsingStateCell {
	if len(responses) == 0 {
		return nil
	}

	out := make([]cachedBrowsingStateCell, 0, len(responses))
	for _, resp := range responses {
		cell := cachedBrowsingStateCell{
			X:     resp.X,
			Y:     resp.Y,
			Z:     resp.Z,
			Count: resp.Count,
		}
		if len(resp.CubeObjects) > 0 {
			cell.CubeObjects = make(
				[]cachedCubeObject,
				0,
				len(resp.CubeObjects),
			)
			for _, cubeObject := range resp.CubeObjects {
				cell.CubeObjects = append(cell.CubeObjects, cachedCubeObject{
					ID:           cubeObject.Id,
					FileURI:      cubeObject.FileURI,
					ThumbnailURI: cubeObject.ThumbnailURI,
				})
			}
		}
		out = append(out, cell)
	}
	return out
}

func writeCachedCompatBrowsingStateResponse(
	w http.ResponseWriter,
	entry bsCacheEntrySnapshot,
) {
	writeBSETagHeader(w, entry.ETag)
	cells := compatResponsesFromBSCacheValue(entry.Value)
	if len(entry.Value.AxisBucketInfos) > 0 {
		writeJSON(w, http.StatusOK, compatBrowsingStateEnvelope{
			AxisBucketInfos: convertCompatAxisBucketInfos(entry.Value.AxisBucketInfos),
			Cells:           cells,
		})
		return
	}
	if len(entry.Value.BucketInfos) > 0 {
		writeJSON(w, http.StatusOK, compatBrowsingStateEnvelope{
			BucketInfos: convertCompatBucketInfos(entry.Value.BucketInfos),
			Cells:       cells,
		})
		return
	}
	writeJSON(w, http.StatusOK, cells)
}
