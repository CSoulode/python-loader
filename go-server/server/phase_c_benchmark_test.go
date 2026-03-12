package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"

	pb "m3.dataloader/dataloader"
	qg "m3.dataloader/server/querygen"
)

func BenchmarkPhaseCCacheHitStatePath(b *testing.B) {
	dbURL := strings.TrimSpace(os.Getenv(phaseATestDatabaseEnv))
	if dbURL == "" {
		b.Skipf("%s is not set", phaseATestDatabaseEnv)
	}

	env := setupPhaseATestEnv(b, dbURL)
	defer env.cleanup()

	ctx := context.Background()
	if _, err := env.server.resolveVectorDimensionWithCache(ctx, phaseCSeedVectorDimension(), false); err != nil {
		b.Fatalf("seed cache: %v", err)
	}

	req2D := &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{{
			AxisFilterType: pb.AxisType_X_AXIS,
			Value:          1,
			ValueType:      pb.FilterValueType_TAGSET,
		}},
		RebucketOnly:    true,
		VectorDimension: phaseCRebucketVectorDimension(pb.BucketStrategy_EQUAL_DEPTH),
	}
	req3D := &pb.GetBrowsingStateRequest{
		Filters: []*pb.AxisFilter{
			{AxisFilterType: pb.AxisType_X_AXIS, Value: 1, ValueType: pb.FilterValueType_TAGSET},
			{AxisFilterType: pb.AxisType_Z_AXIS, Value: 2, ValueType: pb.FilterValueType_TAGSET},
		},
		RebucketOnly:    true,
		VectorDimension: phaseCRebucketVectorDimension(pb.BucketStrategy_CUSTOM),
	}

	b.ReportAllocs()
	b.Run("2d", func(b *testing.B) {
		runPhaseCCacheHitBenchmark(b, env.server, req2D, 2)
	})
	b.Run("3d", func(b *testing.B) {
		runPhaseCCacheHitBenchmark(b, env.server, req3D, 2)
	})
}

func runPhaseCCacheHitBenchmark(b *testing.B, server *DataLoaderServer, req *pb.GetBrowsingStateRequest, wantRows int) {
	b.Helper()
	ctx := context.Background()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rows, err := executeBrowsingStateStatePath(ctx, server, req)
		if err != nil {
			b.Fatalf("execute browsing state state path: %v", err)
		}
		if rows != wantRows {
			b.Fatalf("rows = %d, want %d", rows, wantRows)
		}
	}
}

func executeBrowsingStateStatePath(ctx context.Context, server *DataLoaderServer, req *pb.GetBrowsingStateRequest) (int, error) {
	plan, err := server.parseBrowsingStateRequest(ctx, req)
	if err != nil {
		return 0, err
	}
	if strings.TrimSpace(req.GetAll()) != "" || strings.TrimSpace(req.GetTimeline()) != "" {
		return 0, fmt.Errorf("benchmark only supports state-path requests")
	}

	axisOrder, axisX, axisY, axisZ, filters := plan.AxisOrder, plan.AxisX, plan.AxisY, plan.AxisZ, plan.Filters
	if err := initXYZAxes(ctx, server.db, &axisX, &axisY, &axisZ, "BenchmarkPhaseC(initAxes).exec"); err != nil {
		return 0, err
	}

	sqlStr := qg.GenerateSQLQueryForState(
		axisOrder,
		axisX.Type, axisX.Id,
		axisY.Type, axisY.Id,
		axisZ.Type, axisZ.Id,
		filters,
		qg.StateQueryOpts{AxisSubqueries: plan.AxisSubqueries},
	)

	tx, err := server.db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	if disableHashJoins {
		if _, err := tx.ExecContext(ctx, "SET LOCAL enable_hashjoin = off"); err != nil {
			return 0, err
		}
	}

	rows, err := tx.QueryContext(ctx, sqlStr)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var row rowCell
		if err := rows.Scan(
			&row.X, &row.Y, &row.Z,
			&row.Id, &row.FileUri, &row.ThumbnailUri,
			&row.Count,
		); err != nil {
			return 0, err
		}
		count++
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	return count, nil
}
