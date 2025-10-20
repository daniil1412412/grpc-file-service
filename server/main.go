package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	pb "github.com/yourusername/grpc-file-service/proto"
	"google.golang.org/grpc"
)

const (
	storageDir = "./storage"
	addr       = ":50051"
)

func main() {
	// ensure storage dir exists
	if err := os.MkdirAll(storageDir, 0o755); err != nil {
		log.Fatalf("failed to create storage dir: %v", err)
	}

	// semaphores
	uploadDownloadSem := make(chan struct{}, 10) // 10 concurrent uploads/downloads
	listSem := make(chan struct{}, 100)          // 100 concurrent list requests

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	srv := &fileServer{
		storageDir:        storageDir,
		uploadDownloadSem: uploadDownloadSem,
		listSem:           listSem,
	}

	grpcServer := grpc.NewServer(
		grpc.UnaryInterceptor(unaryLimitInterceptor(srv)),
		grpc.StreamInterceptor(streamLimitInterceptor(srv)),
	)

	pb.RegisterFileServiceServer(grpcServer, srv)

	// graceful shutdown
	go func() {
		log.Printf("gRPC server listening on %s", addr)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("grpc serve error: %v", err)
		}
	}()

	// wait for interrupt
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	fmt.Println("shutting down")
	grpcServer.GracefulStop()
}
