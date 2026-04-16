package main

import (
	"reflect"
	"testing"

	pb "m3.dataloader/dataloader"
)

func TestBrowsingStateCacheRecorderSnapshotKeepsLatestCellState(t *testing.T) {
	recorder := newBrowsingStateCacheRecorder()

	recorder.Record(&pb.BrowsingStateResponse{
		X:     2,
		Y:     1,
		Z:     1,
		Count: 1,
		CubeObjects: []*pb.CubeObject{{
			Id:           7,
			FileUri:      "/media/7",
			ThumbnailUri: "/thumb/7",
		}},
	})
	recorder.Record(&pb.BrowsingStateResponse{
		X:     1,
		Y:     1,
		Z:     1,
		Count: 1,
		CubeObjects: []*pb.CubeObject{{
			Id:           3,
			FileUri:      "/media/3",
			ThumbnailUri: "/thumb/3",
		}},
	})
	recorder.Record(&pb.BrowsingStateResponse{
		X:     2,
		Y:     1,
		Z:     1,
		Count: 4,
		CubeObjects: []*pb.CubeObject{{
			Id:           9,
			FileUri:      "/media/9",
			ThumbnailUri: "/thumb/9",
		}},
	})

	got := recorder.Snapshot()
	want := bsCacheValue{
		Cells: []cachedBrowsingStateCell{
			{
				X:     1,
				Y:     1,
				Z:     1,
				Count: 1,
				CubeObjects: []cachedCubeObject{{
					ID:           3,
					FileURI:      "/media/3",
					ThumbnailURI: "/thumb/3",
				}},
			},
			{
				X:     2,
				Y:     1,
				Z:     1,
				Count: 4,
				CubeObjects: []cachedCubeObject{{
					ID:           9,
					FileURI:      "/media/9",
					ThumbnailURI: "/thumb/9",
				}},
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("snapshot = %+v, want %+v", got, want)
	}
}

func TestBrowsingStateCacheRecorderSnapshotRetainsAxisBucketInfos(t *testing.T) {
	recorder := newBrowsingStateCacheRecorder()

	recorder.Record(&pb.BrowsingStateResponse{
		X:     3,
		Y:     1,
		Z:     1,
		Count: 2,
		AxisBucketInfos: map[string]*pb.BucketInfoList{
			"y": {Items: []*pb.BucketInfo{
				{BucketId: 2, LowerBound: 0.4, UpperBound: 1.0, Label: "far"},
				{BucketId: 1, LowerBound: 0.0, UpperBound: 0.4, Label: "near"},
			}},
		},
		CubeObjects: []*pb.CubeObject{{
			Id: 11,
		}},
	})

	got := recorder.Snapshot()
	if len(got.AxisBucketInfos) != 1 {
		t.Fatalf("axis bucket infos = %+v", got.AxisBucketInfos)
	}
	labels := []string{
		got.AxisBucketInfos["y"][0].GetLabel(),
		got.AxisBucketInfos["y"][1].GetLabel(),
	}
	if !reflect.DeepEqual(labels, []string{"near", "far"}) {
		t.Fatalf("axis bucket labels = %v, want [near far]", labels)
	}
}
