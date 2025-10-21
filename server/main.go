package main

import (
	"log"
	"net"

	"github.com/daniil1412412/grpc-file-service/proto"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("ошибка чтения файла %v", err)
	}

	uploadDownloadSem := make(chan struct{}, 10)
	listSem := make(chan struct{}, 100)

	srv := &fileServer{
		storageDir:        "uploads",
		uploadDownloadSem: uploadDownloadSem,
		listSem:           listSem,
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(unaryLimitInterceptor(srv)),
		grpc.StreamInterceptor(streamLimitInterceptor(srv)),
	)

	proto.RegisterFileServiceServer(grpcServer, srv)

	log.Println("сервер запущен")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("ошибка запуска: %v", err)
	}
}
