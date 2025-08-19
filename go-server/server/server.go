package main

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"

	"google.golang.org/grpc/health"
	healthgrpc "google.golang.org/grpc/health/grpc_health_v1"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	status "google.golang.org/grpc/status"

	_ "github.com/lib/pq"

	pb "m3.dataloader/dataloader"

	amqp "github.com/rabbitmq/amqp091-go"
	rmq "m3.dataloader/rabbitMQ"
	gutils "m3.dataloader/utilities"
)

var (
	dbname     = gutils.MustGetEnv("DB_NAME")
	user       = gutils.MustGetEnv("DB_USER")
	pwd        = gutils.MustGetEnv("DB_PASSWORD")
	db_host    = gutils.MustGetEnv("DB_HOST")
	db_port    = gutils.MustGetEnvInt("DB_PORT")
	sv_host    = gutils.MustGetEnv("SV_HOST")
	sv_port    = gutils.MustGetEnvInt("SV_PORT")
	BATCH_SIZE = gutils.MustGetEnvInt("BATCH_SIZE")
)

var (
	prod *rmq.Producer
)

type DataLoaderServer struct {
	pb.UnimplementedDataLoaderServer
	db *sql.DB
}

func NewDataLoaderServer(connStr string) (*DataLoaderServer, error) {
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to the database: %w", err)
	}
	fmt.Println("new data loader server created")

	// Ensure the database connection is valid
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping the database: %w", err)
	}

	return &DataLoaderServer{
		db: db,
	}, nil
}

// Close closes the database connection.
func (s *DataLoaderServer) Close() {
	s.db.Close()
}

func makeTagRequest(tagTypeId int64, tagSetId int64, tag string) *pb.CreateTagRequest {
	var request *pb.CreateTagRequest
	switch tagTypeId {
	case 1:
		request = &pb.CreateTagRequest{
			TagTypeId: int64(tagTypeId),
			TagSetId:  tagSetId,
			Value:     &pb.CreateTagRequest_Alphanumerical{Alphanumerical: &pb.AlphanumericalValue{Value: tag}},
		}
	case 2:
		request = &pb.CreateTagRequest{
			TagTypeId: int64(tagTypeId),
			TagSetId:  tagSetId,
			Value:     &pb.CreateTagRequest_Timestamp{Timestamp: &pb.TimeStampValue{Value: tag}},
		}
	case 3:
		request = &pb.CreateTagRequest{
			TagTypeId: int64(tagTypeId),
			TagSetId:  tagSetId,
			Value:     &pb.CreateTagRequest_Time{Time: &pb.TimeValue{Value: tag}},
		}
	case 4:
		request = &pb.CreateTagRequest{
			TagTypeId: int64(tagTypeId),
			TagSetId:  tagSetId,
			Value:     &pb.CreateTagRequest_Date{Date: &pb.DateValue{Value: tag}},
		}
	case 5:
		tagInt, _ := strconv.Atoi(tag)
		request = &pb.CreateTagRequest{
			TagTypeId: int64(tagTypeId),
			TagSetId:  tagSetId,
			Value:     &pb.CreateTagRequest_Numerical{Numerical: &pb.NumericalValue{Value: (int64(tagInt))}},
		}
	}

	return request
}

func listenForTaggingMessage(server *DataLoaderServer) {
	rmq.Listen("tagging.not_added.*.*", func(msg amqp.Delivery) {
		log.Printf("Received message: %s", msg.Body)

		// Get the tag type ID and tagset from the routing key
		routingKeyParts := strings.Split(msg.RoutingKey, ".")
		if len(routingKeyParts) != 4 {
			log.Printf("Invalid routing key format: %s", msg.RoutingKey)
			return
		}

		tagTypeId, err := strconv.Atoi(routingKeyParts[2])
		if err != nil {
			log.Printf("Failed to parse tagTypeId from routing key: %s", err)
			return
		}

		tagset := routingKeyParts[3]

		// Get the information from the message body
		var message map[string]string
		err = json.Unmarshal(msg.Body, &message)
		if err != nil {
			log.Printf("Failed to parse message body: %v", err)
			return
		}

		mediaId, ok := message["mediaID"]
		if !ok {
			log.Printf("Missing mediaID in message body")
			return
		}

		tag, ok := message["taggingValue"]
		if !ok {
			log.Printf("Missing taggingValue in message body")
			return
		}

		log.Printf("Parsed message: mediaID=%s, taggingValue=%s, tagset=%s", mediaId, tag, tagset)

		// Get/Create the tagset ID from the database
		response, err := server.CreateTagSet(context.Background(), &pb.CreateTagSetRequest{
			Name:      tagset,
			TagTypeId: int64(tagTypeId),
		})
		if err != nil {
			log.Printf("Failed to create tagset: %s", err)
			return
		}
		tagsetId := response.GetId()

		// Get the tag ID from the database
		queryString := `SELECT
					t.id
				FROM
					public.tags t
				LEFT JOIN
					public.alphanumerical_tags ant ON t.id = ant.id
				LEFT JOIN
					public.timestamp_tags tst ON t.id = tst.id
				LEFT JOIN
					public.time_tags tt ON t.id = tt.id
				LEFT JOIN
					public.date_tags dt ON t.id = dt.id
				LEFT JOIN
					public.numerical_tags nt ON t.id = nt.id
				WHERE
					COALESCE(ant.name, tst.name::text, tt.name::text, dt.name::text, nt.name::text) = $1
					AND t.tagset_id = $2`

		row := server.db.QueryRow(queryString, tag, tagsetId)
		var tagId int64
		err = row.Scan(&tagId)
		if err != nil && err != sql.ErrNoRows {
			log.Printf("Failed to fetch tag ID from database: %s", err)
			return
		}
		if err == sql.ErrNoRows {
			queryString = "INSERT INTO public.tags (tagtype_id, tagset_id) VALUES ($1, $2) RETURNING id"
			row = server.db.QueryRow(queryString, tagTypeId, tagsetId)
			err = row.Scan(&tagId)
			if err != nil {
				log.Printf("Failed to insert tag into database: %s", err)
				return
			}

			queryString = "INSERT INTO public."
			switch tagTypeId {
			case 1:
				queryString += "alphanumerical_tags"
			case 2:
				queryString += "timestamp_tags"
			case 3:
				queryString += "time_tags"
			case 4:
				queryString += "date_tags"
			case 5:
				queryString += "numerical_tags"
			default:
				log.Print("Error: incorrect tag type was provided.")
				return
			}
			queryString += " (id, name, tagset_id) VALUES ($1, $2, $3);"
			row = server.db.QueryRow(queryString, tagId, tag, tagsetId)
			err = row.Scan()
			if err != nil && err != sql.ErrNoRows {
				log.Printf("Failed to create tag: %s", err)
				return
			}

		}

		queryString = "INSERT INTO public.taggings (object_id, tag_id) VALUES ($1, $2) ON CONFLICT (object_id, tag_id) DO NOTHING;"
		row = server.db.QueryRow(queryString, mediaId, tagId)

		err = row.Scan()
		if err != nil && err != sql.ErrNoRows {
			log.Printf("Failed to insert tagging into database: %s", err)
			return
		}
	})
}

func listenForHierarchyMessage(server *DataLoaderServer) {
	rmq.Listen("hierarchy", func(msg amqp.Delivery) {
		log.Printf("debug : received message: %s", msg.Body)
		// Parse the message body as JSON
		var message map[string]interface{}
		err := json.Unmarshal(msg.Body, &message)
		if err != nil {
			log.Printf("debug : failed to parse message body: %v", err)
			return
		}
		log.Printf("debug : message parsed: %+v", message)

		tagsetName, ok := message["tagset"].(string)
		if !ok {
			log.Printf("debug : missing or invalid tagset in message body")
			return
		}
		log.Printf("debug : tagsetName = %s", tagsetName)

		tagTypeId, ok := message["tagTypeId"].(float64)
		if !ok {
			log.Printf("debug : missing or invalid tagTypeId in message body : %v", message["tagTypeId"])
			return
		}
		log.Printf("debug : tagTypeId = %v", tagTypeId)

		hierarchyName, ok := message["hierarchy"].(string)
		if !ok {
			log.Printf("debug : missing or invalid hierarchy in message body")
			return
		}
		log.Printf("debug : hierarchyName = %s", hierarchyName)

		tag, ok := message["tag"].(string)
		if !ok {
			log.Printf("debug : missing or invalid tag in message body")
			return
		}
		log.Printf("debug : tag = %s", tag)

		log.Printf("debug : creating tagset")
		tagsetResponse, err := server.CreateTagSet(context.Background(), &pb.CreateTagSetRequest{
			Name:      tagsetName,
			TagTypeId: int64(tagTypeId),
		})
		if err != nil {
			log.Printf("debug : failed to create tagset: %s", err)
			return
		}
		log.Printf("debug : tagset created: %+v", tagsetResponse)

		log.Printf("debug : creating hierarchy")
		hierarchyResponse, err := server.CreateHierarchy(context.Background(), &pb.CreateHierarchyRequest{
			Name:     hierarchyName,
			TagSetId: tagsetResponse.GetId(),
		})
		if err != nil {
			log.Printf("debug : failed to create hierarchy: %s", err)
			return
		}
		log.Printf("debug : hierarchy created: %+v", hierarchyResponse)

		log.Printf("debug : creating tag")
		tagResponse, err := server.CreateTag(context.Background(), makeTagRequest(int64(tagTypeId), tagsetResponse.GetId(), tag))
		if err != nil {
			log.Printf("debug : failed to create tag: %s", err)
			return
		}
		log.Printf("debug : tag created: %+v", tagResponse)

		log.Printf("debug : creating root node")
		rootNodeResponse, err := server.CreateNode(context.Background(), &pb.CreateNodeRequest{
			TagId:        tagResponse.Id,
			HierarchyId:  hierarchyResponse.Id,
			ParentNodeId: 0, // Root node has no parent
		})
		if err != nil {
			log.Printf("debug : failed to create root node: %s", err)
			return
		}
		log.Printf("debug : root node created: %+v", rootNodeResponse)

		child, ok := message["child"].(map[string]interface{})
		parentNodeId := rootNodeResponse.Id
		log.Printf("debug : entering child node creation loop")
		for ok && len(child) > 0 {
			tagValue, okTag := child["tag"].(string)
			tagTypeId, okType := child["tagTypeId"].(float64)
			tagsetName, okTagSet := child["tagset"].(string)

			log.Printf("debug : child tagValue = %v, okTag = %v, tagTypeId = %v, okType = %v, tagsetName = %v, okTagSet = %v", tagValue, okTag, tagTypeId, okType, tagsetName, okTagSet)
			if !okTag || tagValue == "" || !okType || !okTagSet || tagsetName == "" {
				log.Printf("debug : missing or invalid child tag or tagTypeId or tagsetId in message body")
				break
			}
			log.Printf("debug : creating tagset")
			tagsetResponse, err := server.CreateTagSet(context.Background(), &pb.CreateTagSetRequest{
				Name:      tagsetName,
				TagTypeId: int64(tagTypeId),
			})
			if err != nil {
				log.Printf("debug : failed to create tagset: %s", err)
				return
			}
			log.Printf("debug : tagset created: %+v", tagsetResponse)
			log.Printf("debug : creating child tag")
			tagResp, err := server.CreateTag(context.Background(), makeTagRequest(int64(tagTypeId), tagsetResponse.GetId(), tagValue))
			if err != nil {
				log.Printf("debug : failed to create child tag: %s", err)
				break
			}
			log.Printf("debug : child tag created: %+v", tagResp)

			log.Printf("debug : creating child node")
			nodeResp, err := server.CreateNode(context.Background(), &pb.CreateNodeRequest{
				TagId:        tagResp.Id,
				HierarchyId:  hierarchyResponse.Id,
				ParentNodeId: parentNodeId,
			})
			if err != nil {
				log.Printf("debug : failed to create child node: %s", err)
				break
			}
			log.Printf("debug : child node created: %+v", nodeResp)
			parentNodeId = nodeResp.Id

			nextChild, okNext := child["child"].(map[string]interface{})
			log.Printf("debug : moving to next child: okNext = %v, nextChild = %+v", okNext, nextChild)
			child = nextChild
			ok = okNext && len(child) > 0
		}
		log.Printf("debug : finished processing hierarchy message")
	})
}

func main() {
	conn_str := fmt.Sprintf("dbname=%s user=%s password=%s host=%s port=%d sslmode=disable",
		dbname,  // dbname
		user,    // user
		pwd,     // password
		db_host, // host
		db_port) // port

	server, err := NewDataLoaderServer(conn_str)
	if err != nil {
		log.Fatalf("Error creating server: %v", err)
	}
	defer server.Close()

	prod = rmq.ProducerConnexionInit()
	defer prod.ConnexionEnd()

	go listenForTaggingMessage(server)

	go listenForHierarchyMessage(server)

	// Create a TCP listener for the gRPC server
	lis, err := net.Listen("tcp", fmt.Sprintf("%s:%d", sv_host, sv_port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// Create and register the implementation of the gRPC server
	grpc_server := grpc.NewServer()
	healthcheck := health.NewServer()
	healthgrpc.RegisterHealthServer(grpc_server, healthcheck)
	pb.RegisterDataLoaderServer(grpc_server, server)
	log.Println("gRPC server listening on port 50051")
	if err := grpc_server.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
	healthcheck.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
}
