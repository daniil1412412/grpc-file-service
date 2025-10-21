package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/daniil1412412/grpc-file-service/proto"
	"google.golang.org/grpc"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: client [upload|download|list] args...")
		return
	}

	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("dial error: %v", err)
	}
	defer conn.Close()
	client := proto.NewFileServiceClient(conn)

	switch os.Args[1] {
	case "upload":
		if len(os.Args) < 3 {
			log.Fatalf("usage: client upload <local-file-path>")
		}
		upload(client, os.Args[2])
	case "download":
		if len(os.Args) < 3 {
			log.Fatalf("usage: client download <filename-on-server> [out-path]")
		}
		out := os.Args[2]
		if len(os.Args) >= 4 {
			out = os.Args[3]
		}
		download(client, os.Args[2], out)
	case "list":
		listFiles(client)
	default:
		fmt.Println("unknown command")
	}
}

func upload(client proto.FileServiceClient, path string) {
	f, err := os.Open(path)
	if err != nil {
		log.Fatalf("open error: %v", err)
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	stream, err := client.Upload(ctx)
	if err != nil {
		log.Fatalf("upload start error: %v", err)
	}

	// send initial message with filename
	base := filepath.Base(path)
	if err := stream.Send(&proto.UploadRequest{Filename: base}); err != nil {
		log.Fatalf("send filename error: %v", err)
	}

	buf := make([]byte, 64*1024)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			if err := stream.Send(&proto.UploadRequest{Data: buf[:n]}); err != nil {
				log.Fatalf("send chunk error: %v", err)
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			log.Fatalf("read error: %v", rerr)
		}
	}

	resp, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("upload finish error: %v", err)
	}
	fmt.Printf("результатt: ok=%v msg=%s\n", resp.Ok, resp.Message)
}

func download(client proto.FileServiceClient, filename, outpath string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	stream, err := client.Download(ctx, &proto.DownloadRequest{Filename: filename})
	if err != nil {
		log.Fatalf("download start error: %v", err)
	}

	out, err := os.Create(outpath)
	if err != nil {
		log.Fatalf("create out file error: %v", err)
	}
	defer out.Close()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("recv error: %v", err)
		}
		_, werr := out.Write(chunk.Data)
		if werr != nil {
			log.Fatalf("write error: %v", werr)
		}
	}
	fmt.Printf("Downloaded %s -> %s\n", filename, outpath)
}

func listFiles(client proto.FileServiceClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.ListFiles(ctx, &proto.ListRequest{})
	if err != nil {
		log.Fatalf("list error: %v", err)
	}
	fmt.Println("файлы на сервере:")
	for _, f := range resp.Files {
		fmt.Printf("- %s | создан: %s | обновлен: %s | %d вес\n", f.Filename, f.CreatedAt, f.ModifiedAt, f.SizeBytes)
	}
}
