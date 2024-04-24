package main

import (
	"net"

	_ "github.com/joho/godotenv/autoload"
	"github.com/micro-tok/discover-service/pkg/cassandra"
	"github.com/micro-tok/discover-service/pkg/config"
	"github.com/micro-tok/discover-service/pkg/pb"
	"github.com/micro-tok/discover-service/pkg/redis"
	"github.com/micro-tok/discover-service/pkg/services"
	"google.golang.org/grpc"
)

func main() {
	cfg := config.LoadConfig()

	cassService := cassandra.NewCassandraService(cfg)

	redisService := redis.NewRedisService(cfg)

	lis, err := net.Listen("tcp", "localhost:"+cfg.ServicePort)
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024 * 1024 * 1024 * 2),
	)

	// Register the service
	pb.RegisterDiscoverServiceServer(grpcServer, services.NewDiscoverService(cassService, redisService))

	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}

}
