package main

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	pb "github.com/daniil1412412/grpc-file-service/proto"
	"google.golang.org/grpc"
)

// fileServer implements pb.FileServiceServer
type fileServer struct {
	pb.UnimplementedFileServiceServer
	storageDir        string
	uploadDownloadSem chan struct{}
	listSem           chan struct{}
}

// ---- helpers to acquire/release semaphores ----
func (s *fileServer) acquireUploadDownload(ctx context.Context) error {
	select {
	case s.uploadDownloadSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *fileServer) releaseUploadDownload() {
	select {
	case <-s.uploadDownloadSem:
	default:
	}
}
func (s *fileServer) acquireList(ctx context.Context) error {
	select {
	case s.listSem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *fileServer) releaseList() {
	select {
	case <-s.listSem:
	default:
	}
}

// ---- Interceptors (main.go uses them) ----
// but we define them here to access methods of s
func unaryLimitInterceptor(srv *fileServer) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		// Decide which semaphore to use based on method name
		if strings.HasSuffix(info.FullMethod, "/ListFiles") {
			if err := srv.acquireList(ctx); err != nil {
				return nil, err
			}
			defer srv.releaseList()
			return handler(ctx, req)
		}
		// default: treat other unary as upload/download (if any)
		if err := srv.acquireUploadDownload(ctx); err != nil {
			return nil, err
		}
		defer srv.releaseUploadDownload()
		return handler(ctx, req)
	}
}

func streamLimitInterceptor(srv *fileServer) grpc.StreamServerInterceptor {
	return func(
		srvInterface interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if strings.HasSuffix(info.FullMethod, "/Upload") || strings.HasSuffix(info.FullMethod, "/Download") {
			// upload/download use uploadDownloadSem
			if err := srv.acquireUploadDownload(ss.Context()); err != nil {
				return err
			}
			defer srv.releaseUploadDownload()
			return handler(srvInterface, ss)
		}
		if strings.HasSuffix(info.FullMethod, "/ListFiles") {
			if err := srv.acquireList(ss.Context()); err != nil {
				return err
			}
			defer srv.releaseList()
			return handler(srvInterface, ss)
		}
		// default
		return handler(srvInterface, ss)
	}
}

// ---- RPC implementations ----

func sanitizeFilename(name string) string {
	// take base and remove path separators
	name = filepath.Base(name)
	name = strings.ReplaceAll(name, string(os.PathSeparator), "_")
	return name
}

func (s *fileServer) Upload(stream pb.FileService_UploadServer) error {
	// Expect first message to contain filename (or it can be in every message)
	var outFile *os.File
	var filename string
	var receivedBytes int64

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			// finished receiving
			if outFile != nil {
				_ = outFile.Close()
			}
			resp := &pb.UploadResponse{Ok: true, Message: "upload complete"}
			return stream.SendAndClose(resp)
		}
		if err != nil {
			if outFile != nil {
				_ = outFile.Close()
			}
			return err
		}

		// determine filename
		if filename == "" && req.GetFilename() != "" {
			filename = sanitizeFilename(req.GetFilename())
			targetPath := filepath.Join(s.storageDir, filename)

			// create file (overwrite if exists)
			f, err := os.OpenFile(targetPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if err != nil {
				return stream.SendAndClose(&pb.UploadResponse{Ok: false, Message: "failed to create file: " + err.Error()})
			}
			outFile = f
		}

		if filename == "" {
			// no filename yet -> error
			if outFile != nil {
				_ = outFile.Close()
			}
			return stream.SendAndClose(&pb.UploadResponse{Ok: false, Message: "filename not provided in first messages"})
		}

		data := req.GetData()
		if len(data) > 0 {
			n, err := outFile.Write(data)
			if err != nil {
				_ = outFile.Close()
				return stream.SendAndClose(&pb.UploadResponse{Ok: false, Message: "write error: " + err.Error()})
			}
			receivedBytes += int64(n)
		}
	}
}

func (s *fileServer) Download(req *pb.DownloadRequest, stream pb.FileService_DownloadServer) error {
	filename := sanitizeFilename(req.GetFilename())
	if filename == "" {
		return errors.New("empty filename")
	}
	path := filepath.Join(s.storageDir, filename)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, 64*1024) // 64KB chunks
	for {
		n, err := f.Read(buf)
		if n > 0 {
			resp := &pb.DownloadResponse{Data: buf[:n]}
			if err2 := stream.Send(resp); err2 != nil {
				return err2
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *fileServer) ListFiles(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	entries, err := os.ReadDir(s.storageDir)
	if err != nil {
		return nil, err
	}
	var files []*pb.FileInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		full := filepath.Join(s.storageDir, name)
		info, err := os.Stat(full)
		if err != nil {
			continue
		}
		created := info.ModTime() // filesystem may not store creation time portably; use ModTime as both created/updated if needed
		modified := info.ModTime()
		files = append(files, &pb.FileInfo{
			Filename:   name,
			CreatedAt:  created.Format(time.RFC3339),
			ModifiedAt: modified.Format(time.RFC3339),
			SizeBytes:  info.Size(),
		})
	}
	return &pb.ListResponse{Files: files}, nil
}
