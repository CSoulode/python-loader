package main

import (
	"container/list"
	"context"
	"flag"
	"fmt"
	"log"
	pb "media_downloader/gen/go"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"

	"github.com/cavaliercoder/grab"
)

type ressourceState int

const (
	unavailable ressourceState = 0
	downloading ressourceState = 1
	available   ressourceState = 2

	downloadedPath = "/app/downloaded-medias/"
)

var (
	grpcHost      = mustGetEnv("MEDIA_DOWNLOADER_HOST")
	grpcPort      = mustGetEnvInt("MEDIA_DOWNLOADER_PORT")
	ressources    = make(map[string]*ressourceEntry)
	c             = sync.NewCond(&sync.Mutex{})
	cacheSize     int64
	maxCacheSize  = mustGetEnvInt64("MAX_CACHE_SIZE")
	ressourceTTL  = time.Duration(mustGetEnvInt64("RESSOURCE_TTL")) * time.Second
	cacheElements = list.New()
)

type ressourceEntry struct {
	requestCounter int
	path           string
	state          ressourceState
	timer          *time.Timer
	size           int64
	elem           *list.Element
}

func mustGetEnv(key string) string {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required but not set", key)
	}
	return value
}

func mustGetEnvInt(key string) int {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required but not set", key)
	}
	v, err := strconv.Atoi(value)
	if err != nil {
		log.Fatalf("Environment variable %s must be an integer, got: %s", key, value)
	}
	return v
}

func mustGetEnvInt64(key string) int64 {
	value := os.Getenv(key)
	if value == "" {
		log.Fatalf("Environment variable %s is required but not set", key)
	}
	v, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		log.Fatalf("Environment variable %s must be an integer, got: %s", key, value)
	}
	return v
}

func (res *ressourceEntry) addRequest() {
	res.requestCounter++
}

func (res *ressourceEntry) removeRequest() {
	res.requestCounter--
}

type server struct {
	pb.UnimplementedMediaDownloaderServer
}

func (s *server) RequestMedia(ctx context.Context, req *pb.RequestMediaRequest) (*pb.RequestMediaResponse, error) {
	if uri := req.GetMediaUri(); !(len(uri) >= 7 && (uri[:7] == "http://" || (len(uri) >= 8 && uri[:8] == "https://"))) {
		return &pb.RequestMediaResponse{MediaPath: uri}, nil
	}
	c.L.Lock()
	defer c.L.Unlock()
	log.Printf("Requesting media: %s", req.GetMediaUri())
	entry, ok := ressources[req.GetMediaUri()]
	if !ok {
		ressources[req.GetMediaUri()] = &ressourceEntry{
			requestCounter: 0,
			state:          unavailable,
		}
		entry = ressources[req.GetMediaUri()]
	}
	entry.addRequest()
	if entry.timer != nil {
		log.Printf("Stopping timer for %s", req.GetMediaUri())
		entry.timer.Stop()
		entry.timer = nil
	}

	if entry.state == downloading {
		log.Printf("Media %s is currently being downloaded, waiting for completion", req.GetMediaUri())
		for entry.state == downloading {
			c.Wait()
		}
	}
	if entry.state == available {
		log.Printf("Media %s is already available", req.GetMediaUri())
		return &pb.RequestMediaResponse{MediaPath: entry.path}, nil
	} else if entry.state == unavailable {
		log.Printf("Media %s is not available, starting download", req.GetMediaUri())
		entry.state = downloading
		resp, err := grab.Get(downloadedPath, req.GetMediaUri())
		if err != nil {
			return nil, status.Errorf(codes.NotFound, "remote request failed")
		}
		if resp.Size > maxCacheSize {
			return nil, status.Errorf(codes.ResourceExhausted, "requested media is too large (%d bytes), maximum allowed size is %d bytes", resp.Size, maxCacheSize)
		}
		entry.path = resp.Filename
		entry.size = resp.Size
		cacheSize += entry.size
		for cacheSize > maxCacheSize {
			oldest := cacheElements.Front()
			if oldest == nil {
				return nil, status.Errorf(codes.ResourceExhausted, "cache size exceeded and no elements to remove")
			}
			destroyRessource(oldest.Value.(string))

		}
		entry.elem = cacheElements.PushBack(req.GetMediaUri())

		entry.state = available
		c.Broadcast()
		return &pb.RequestMediaResponse{MediaPath: entry.path}, nil

	}

	return nil, nil
}

func (s *server) ReleaseMedia(ctx context.Context, req *pb.ReleaseMediaRequest) (*pb.ReleaseMediaResponse, error) {
	if uri := req.GetMediaUri(); !(len(uri) >= 7 && (uri[:7] == "http://" || (len(uri) >= 8 && uri[:8] == "https://"))) {
		return &pb.ReleaseMediaResponse{}, nil
	}
	c.L.Lock()
	defer c.L.Unlock()
	log.Printf("Releasing media: %s", req.GetMediaUri())
	ressources[req.GetMediaUri()].removeRequest()
	if ressources[req.GetMediaUri()].requestCounter <= 0 {

		destroyRessource := func() {
			c.L.Lock()
			defer c.L.Unlock()
			destroyRessource(req.GetMediaUri())
		}
		ressources[req.GetMediaUri()].timer = time.AfterFunc(ressourceTTL, destroyRessource)
	}
	return &pb.ReleaseMediaResponse{}, nil
}

func destroyRessource(URI string) {
	res := ressources[URI]
	if res.requestCounter <= 0 {
		cacheElements.Remove(res.elem)
		log.Printf("removing ressource %s", res.path)
		err := os.Remove(res.path)
		if err != nil {
			log.Printf("failed to remove file: %v", err)
		}
		cacheSize -= res.size
		delete(ressources, URI)
		log.Printf("Current ressources: %+v", ressources)
	}
}

func main() {
	flag.Parse()
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", grpcHost, grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	healthcheck := health.NewServer()
	healthgrpc.RegisterHealthServer(s, healthcheck)
	pb.RegisterMediaDownloaderServer(s, &server{})
	log.Printf("server listening at %v", lis.Addr())
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	healthcheck.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
}
