package runner

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"google.golang.org/grpc"

	pb "m3.dataloader/dataloader"
	kvstorev1 "vectorkv/api/kvstore/v1/gen"
)

type ActiveSession struct {
	Runtime      *ManagedRuntime
	DB           *sql.DB
	DataLoader   pb.DataLoaderClient
	DataConn     *grpc.ClientConn
	VectorClient kvstorev1.VectorKVClient
	VectorConn   *grpc.ClientConn
	Dataset      DatasetConfig
}

func OpenDatasetDB(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseURL)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func StartSession(ctx context.Context, cfg RuntimeConfig) (*ActiveSession, error) {
	db, err := OpenDatasetDB(cfg.DatabaseURL)
	if err != nil {
		return nil, err
	}

	runtime, err := StartManagedRuntime(ctx, cfg)
	if err != nil {
		_ = db.Close()
		return nil, err
	}

	dataAddr := fmt.Sprintf("127.0.0.1:%d", runtime.Ports.ServerGRPC)
	dataLoader, dataConn, err := DialDataLoader(ctx, dataAddr)
	if err != nil {
		runtime.Stop()
		_ = db.Close()
		return nil, err
	}

	vectorAddr := fmt.Sprintf("127.0.0.1:%d", runtime.Ports.VectorGRPC)
	vectorClient, vectorConn, err := DialVectorKV(ctx, vectorAddr)
	if err != nil {
		_ = dataConn.Close()
		runtime.Stop()
		_ = db.Close()
		return nil, err
	}

	return &ActiveSession{
		Runtime:      runtime,
		DB:           db,
		DataLoader:   dataLoader,
		DataConn:     dataConn,
		VectorClient: vectorClient,
		VectorConn:   vectorConn,
		Dataset:      cfg.Dataset,
	}, nil
}

func (s *ActiveSession) Close() {
	if s == nil {
		return
	}
	if s.VectorConn != nil {
		_ = s.VectorConn.Close()
	}
	if s.DataConn != nil {
		_ = s.DataConn.Close()
	}
	if s.Runtime != nil {
		s.Runtime.Stop()
	}
	if s.DB != nil {
		_ = s.DB.Close()
	}
}
