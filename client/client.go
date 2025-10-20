package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	pb "github.com/daniil1412412/grpc-file-service/proto"
	"google.golang.org/grpc"
)

const serverAddr = "localhost:50051"

func uploadFile(client pb.FileServiceClient, filepathLocal string) error {
	stream, err := client.Upload(context.Background())
	if err != nil {
		return err
	}

	f, err := os.Open(filepathLocal)
	if err != nil {
		return err
	}
	defer f.Close()

	_, name := filepath.Split(filepathLocal)
	// send first message with filename (no data yet)
	if err := stream.Send(&pb.UploadRequest{Filename: name}); err != nil {
		return err
	}

	buf := make([]byte, 64*1024)
	for {
		n, err := f.Read(buf)
		if n > 0 {
			if err := stream.Send(&pb.UploadRequest{Data: buf[:n]}); err != nil {
				return err
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return err
	}
	fmt.Println("Upload result:", resp.Message)
	return nil
}

func downloadFile(client pb.FileServiceClient, filename, outPath string) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	stream, err := client.Download(ctx, &pb.DownloadRequest{Filename: filename})
	if err != nil {
		return err
	}
	outFile, err := os.Create(outPath)
	if err != nil {
		return err
	}
	defer outFile.Close()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, err := outFile.Write(chunk.GetData()); err != nil {
			return err
		}
	}
	return nil
}

func listFiles(client pb.FileServiceClient) error {
	resp, err := client.ListFiles(context.Background(), &pb.ListRequest{})
	if err != nil {
		return err
	}
	fmt.Println("Files:")
	for _, f := range resp.Files {
		fmt.Printf("%s | created: %s | updated: %s | size: %d\n", f.Filename, f.CreatedAt, f.ModifiedAt, f.SizeBytes)
	}
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("usage: client [upload|download|list] args...")
		return
	}
	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	client := pb.NewFileServiceClient(conn)

	switch os.Args[1] {
	case "upload":
		if len(os.Args) != 3 {
			fmt.Println("upload <local-filepath>")
			return
		}
		if err := uploadFile(client, os.Args[2]); err != nil {
			fmt.Println("upload error:", err)
		}
	case "download":
		if len(os.Args) != 4 {
			fmt.Println("download <filename-on-server> <out-path>")
			return
		}
		if err := downloadFile(client, os.Args[2], os.Args[3]); err != nil {
			fmt.Println("download error:", err)
		}
	case "list":
		if err := listFiles(client); err != nil {
			fmt.Println("list error:", err)
		}
	default:
		fmt.Println("unknown command")
	}
}
