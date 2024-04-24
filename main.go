package main

import (
	"net"

	_ "github.com/joho/godotenv/autoload"
	"github.com/micro-tok/discover-service/pkg/config"
	"google.golang.org/grpc"
)

func main() {
	cfg := config.LoadConfig()

	lis, err := net.Listen("tcp", "localhost:"+cfg.ServicePort)
	if err != nil {
		panic(err)
	}

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(1024 * 1024 * 1024 * 2),
	)

	if err := grpcServer.Serve(lis); err != nil {
		panic(err)
	}

}
