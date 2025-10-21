package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/daniil1412412/grpc-file-service/proto"
	"google.golang.org/grpc"
)

// fileServer implements proto.FileServiceServer
type fileServer struct {
	proto.UnimplementedFileServiceServer
	storageDir        string
	uploadDownloadSem chan struct{}
	listSem           chan struct{}
}

// ---- semaphore helpers ----
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

func unaryLimitInterceptor(srv *fileServer) func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if strings.HasSuffix(info.FullMethod, "/ListFiles") {
			if err := srv.acquireList(ctx); err != nil {
				return nil, err
			}
			defer srv.releaseList()
			return handler(ctx, req)
		}
		if err := srv.acquireUploadDownload(ctx); err != nil {
			return nil, err
		}
		defer srv.releaseUploadDownload()
		return handler(ctx, req)
	}
}

func streamLimitInterceptor(srv *fileServer) func(srvInterface interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return func(srvInterface interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if strings.HasSuffix(info.FullMethod, "/Upload") || strings.HasSuffix(info.FullMethod, "/Download") {
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
		return handler(srvInterface, ss)
	}
}

func sanitizeFilename(name string) string {
	name = filepath.Base(name)
	name = strings.ReplaceAll(name, string(os.PathSeparator), "_")
	return name
}

func (s *fileServer) Upload(stream proto.FileService_UploadServer) error {
	if err := os.MkdirAll(s.storageDir, 0o755); err != nil {
		return fmt.Errorf("mkdir error: %w", err)
	}

	var f *os.File
	var filename string

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			if f != nil {
				_ = f.Close()
			}
			return stream.SendAndClose(&proto.UploadResponse{Ok: true, Message: "успешно"})
		}
		if err != nil {
			if f != nil {
				_ = f.Close()
			}
			return err
		}

		if filename == "" {
			filename = sanitizeFilename(req.GetFilename())
			if filename == "" {
				return errors.New("название обязательно")
			}
			path := filepath.Join(s.storageDir, filename)
			file, ferr := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if ferr != nil {
				return fmt.Errorf("файл успешно создан: %w", ferr)
			}
			f = file
		}

		if len(req.GetData()) > 0 {
			if _, werr := f.Write(req.GetData()); werr != nil {
				_ = f.Close()
				return fmt.Errorf("ошибка чтения: %w", werr)
			}
		}
	}
}

func (s *fileServer) Download(req *proto.DownloadRequest, stream proto.FileService_DownloadServer) error {
	filename := sanitizeFilename(req.GetFilename())
	if filename == "" {
		return errors.New("имя файла пустое")
	}

	path := filepath.Join(s.storageDir, filename)
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, 64*1024)
	for {
		n, rerr := f.Read(buf)
		if n > 0 {
			if serr := stream.Send(&proto.DownloadResponse{Data: buf[:n]}); serr != nil {
				return serr
			}
		}
		if rerr == io.EOF {
			break
		}
		if rerr != nil {
			return rerr
		}
	}
	return nil
}

func (s *fileServer) ListFiles(ctx context.Context, req *proto.ListRequest) (*proto.ListResponse, error) {
	if err := os.MkdirAll(s.storageDir, 0o755); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(s.storageDir)
	if err != nil {
		return nil, err
	}
	var files []*proto.FileInfo
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		info, err := e.Info()
		if err != nil {
			continue
		}
		files = append(files, &proto.FileInfo{
			Filename:   e.Name(),
			CreatedAt:  info.ModTime().Format(time.RFC3339), // portable: modtime used
			ModifiedAt: info.ModTime().Format(time.RFC3339),
			SizeBytes:  info.Size(),
		})
	}
	return &proto.ListResponse{Files: files}, nil
}
