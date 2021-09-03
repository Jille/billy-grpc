// Package fsserver implements a gRPC server which passes calls to Billy.
package fsserver

import (
	"context"
	"errors"
	"io"
	"os"
	"sync"

	pb "github.com/Jille/billy-grpc/proto"
	"github.com/go-git/go-billy/v5"
	"github.com/golang/protobuf/ptypes/empty"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service implements pb.BillyServiceServer and can be registered with a grpc.Server.
type Service struct {
	// "Unsafe" so it doesn't compile if we don't implement all the methods.
	pb.UnsafeBillyServiceServer

	callbacks Callbacks
}

// Callbacks are the callbacks this library needs from callers.
type Callbacks interface {
	// FilesystemForPeer is supposed to grab the peer from context.Context and return the filesystem you want to serve that peer; or an error code and an error.
	// codes.Code and error are separate to force you to think about the error code.
	FilesystemForPeer(context.Context) (billy.Filesystem, codes.Code, error)
}

// NewService instantiates a new Service.
func NewService(callbacks Callbacks) *Service {
	return &Service{
		callbacks: callbacks,
	}
}

// RegisterService creates the services and registers it with the given gRPC server.
func RegisterService(s grpc.ServiceRegistrar, callbacks Callbacks) {
	pb.RegisterBillyServiceServer(s, NewService(callbacks))
}

func (s *Service) getFS(ctx context.Context) (billy.Filesystem, error) {
	fs, c, err := s.callbacks.FilesystemForPeer(ctx)
	if err != nil {
		return nil, status.Error(c, err.Error())
	}
	return fs, nil
}

func billyToGRPCError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	if errors.Is(err, os.ErrNotExist) {
		return status.Error(codes.NotFound, "ENOENT")
	}
	if errors.Is(err, os.ErrExist) {
		return status.Error(codes.AlreadyExists, "EEXIST")
	}
	if errors.Is(err, os.ErrPermission) {
		return status.Error(codes.PermissionDenied, "EPERM")
	}
	if errors.Is(err, os.ErrInvalid) {
		return status.Error(codes.InvalidArgument, "EINVAL")
	}
	if errors.Is(err, billy.ErrReadOnly) {
		return status.Error(codes.FailedPrecondition, "billy.ErrReadOnly")
	}
	if errors.Is(err, billy.ErrNotSupported) {
		return status.Error(codes.Unimplemented, "billy.ErrNotSupported")
	}
	if errors.Is(err, billy.ErrCrossedBoundary) {
		return status.Error(codes.InvalidArgument, "billy.ErrCrossedBoundary")
	}
	if pe, ok := err.(*os.PathError); ok {
		err = pe.Err
	}
	return status.Errorf(codes.Unknown, "unknown error %q", err.Error())
}

func (s *Service) FileDescriptor(stream pb.BillyService_FileDescriptorServer) error {
	fs, err := s.getFS(stream.Context())
	if err != nil {
		return err
	}
	msg, err := stream.Recv()
	if err != nil {
		return err
	}
	or := msg.GetOpen()
	flags := 0
	if or.GetForReading() && or.GetForWriting() {
		flags |= os.O_RDWR
	} else if or.GetForReading() {
		flags |= os.O_RDONLY
	} else if or.GetForWriting() {
		flags |= os.O_WRONLY
	}
	if or.GetAppend() {
		flags |= os.O_APPEND
	}
	if or.GetAllowCreation() {
		flags |= os.O_CREATE
	}
	if or.GetExclusive() {
		flags |= os.O_EXCL
	}
	if or.GetTruncate() {
		flags |= os.O_TRUNC
	}
	if or.GetSync() {
		flags |= os.O_SYNC
	}
	var mode os.FileMode = 0777
	if or.GetCreateMode() != 0 {
		mode = os.FileMode(or.GetCreateMode() & 0777)
	}
	fh, err := fs.OpenFile(or.GetPath(), flags, mode)
	if err != nil {
		return billyToGRPCError(err)
	}
	var writeErr error
	respond := func(err error, resp *pb.FileDescriptorResponse) {
		if err != nil {
			err = billyToGRPCError(err)
			s, ok := status.FromError(err)
			if !ok {
				s = status.New(codes.Unknown, err.Error())
			}
			resp.Status = s.Proto()
		} else {
			resp.Status = &spb.Status{}
		}
		writeErr = stream.Send(resp)
	}
	respond(nil, &pb.FileDescriptorResponse{RequestId: msg.GetRequestId()})
	var writeLock sync.Mutex
	for {
		if writeErr != nil {
			return writeErr
		}
		msg, err := stream.Recv()
		if err != nil {
			fh.Close()
			return err
		}
		op := fileDescriptorOp{fh, &writeLock}
		resp := new(pb.FileDescriptorResponse)
		resp.RequestId = msg.GetRequestId()
		if c := msg.GetClose(); c != nil {
			respond(op.close(c, resp), resp)
		} else if c := msg.GetWrite(); c != nil {
			respond(op.write(c, resp), resp)
		} else if c := msg.GetWriteAt(); c != nil {
			respond(op.writeAt(c, resp), resp)
		} else if c := msg.GetRead(); c != nil {
			respond(op.read(c, resp), resp)
		} else if c := msg.GetReadAt(); c != nil {
			respond(op.readAt(c, resp), resp)
		} else if c := msg.GetSeek(); c != nil {
			respond(op.seek(c, resp), resp)
		} else if c := msg.GetTruncate(); c != nil {
			respond(op.truncate(c, resp), resp)
		} else if c := msg.GetLock(); c != nil {
			respond(op.lock(c, resp), resp)
		} else if c := msg.GetUnlock(); c != nil {
			respond(op.unlock(c, resp), resp)
		} else {
			respond(status.Errorf(codes.Unimplemented, "unknown message on FileDescriptor stream: %s", msg), resp)
		}
	}
}

type fileDescriptorOp struct {
	fh        billy.File
	writeLock *sync.Mutex
}

func (f *fileDescriptorOp) close(req *pb.CloseRequest, resp *pb.FileDescriptorResponse) error {
	return f.fh.Close()
}

func (f *fileDescriptorOp) write(req *pb.WriteRequest, resp *pb.FileDescriptorResponse) error {
	f.writeLock.Lock()
	defer f.writeLock.Unlock()
	n, err := f.fh.Write(req.GetData())
	resp.Response = &pb.FileDescriptorResponse_Write{
		Write: &pb.WriteResponse{
			NumWritten: int64(n),
		},
	}
	return err
}

func (f *fileDescriptorOp) writeAt(req *pb.WriteAtRequest, resp *pb.FileDescriptorResponse) error {
	var n int
	var err error
	if wa, ok := f.fh.(io.WriterAt); ok {
		n, err = wa.WriteAt(req.GetData(), req.GetOffset())
	} else {
		f.writeLock.Lock()
		defer f.writeLock.Unlock()
		pos, err := f.fh.Seek(io.SeekCurrent, 0)
		if err != nil {
			return err
		}
		if pos != req.GetOffset() {
			if _, err := f.fh.Seek(io.SeekStart, int(req.GetOffset())); err != nil {
				return err
			}
		}
		n, err = f.fh.Write(req.GetData())
		if err != nil {
			return err
		}
		if _, err := f.fh.Seek(io.SeekStart, int(pos)); err != nil {
			// Avoid accidental damage due to writing in the wrong place.
			// TODO: Handle this cleaner.
			f.fh.Close()
			return err
		}
	}
	resp.Response = &pb.FileDescriptorResponse_Write{
		Write: &pb.WriteResponse{
			NumWritten: int64(n),
		},
	}
	return err
}

func (f *fileDescriptorOp) read(req *pb.ReadRequest, resp *pb.FileDescriptorResponse) error {
	buf := make([]byte, req.GetSize())
	n, err := f.fh.Read(buf)
	resp.Response = &pb.FileDescriptorResponse_Read{
		Read: &pb.ReadResponse{
			Data: buf[:n],
			Eof:  err == io.EOF,
		},
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func (f *fileDescriptorOp) readAt(req *pb.ReadAtRequest, resp *pb.FileDescriptorResponse) error {
	buf := make([]byte, req.GetSize())
	n, err := f.fh.ReadAt(buf, req.GetOffset())
	resp.Response = &pb.FileDescriptorResponse_Read{
		Read: &pb.ReadResponse{
			Data: buf[:n],
			Eof:  err == io.EOF,
		},
	}
	if err == io.EOF {
		return nil
	}
	return err
}

func (f *fileDescriptorOp) seek(req *pb.SeekRequest, resp *pb.FileDescriptorResponse) error {
	whence := io.SeekStart
	switch req.GetWhence() {
	case pb.SeekRequest_SEEK_START:
		whence = io.SeekStart
	case pb.SeekRequest_SEEK_CURRENT:
		whence = io.SeekCurrent
	case pb.SeekRequest_SEEK_END:
		whence = io.SeekEnd
	}
	offset, err := f.fh.Seek(req.GetOffset(), whence)
	if err != nil {
		return err
	}
	resp.Response = &pb.FileDescriptorResponse_Seek{
		Seek: &pb.SeekResponse{
			Offset: offset,
		},
	}
	return nil
}

func (f *fileDescriptorOp) truncate(req *pb.TruncateRequest, resp *pb.FileDescriptorResponse) error {
	return f.fh.Truncate(req.GetSize())
}

func (f *fileDescriptorOp) lock(req *pb.LockRequest, resp *pb.FileDescriptorResponse) error {
	return f.fh.Lock()
}

func (f *fileDescriptorOp) unlock(req *pb.UnlockRequest, resp *pb.FileDescriptorResponse) error {
	return f.fh.Unlock()
}

func (s *Service) Rename(ctx context.Context, req *pb.RenameRequest) (*empty.Empty, error) {
	fs, err := s.getFS(ctx)
	if err != nil {
		return nil, err
	}
	return emptyResponse(fs.Rename(req.GetOldPath(), req.GetNewPath()))
}

func (s *Service) Remove(ctx context.Context, req *pb.RemoveRequest) (*empty.Empty, error) {
	fs, err := s.getFS(ctx)
	if err != nil {
		return nil, err
	}
	return emptyResponse(fs.Remove(req.GetPath()))
}

func (s *Service) MkdirAll(ctx context.Context, req *pb.MkdirAllRequest) (*empty.Empty, error) {
	fs, err := s.getFS(ctx)
	if err != nil {
		return nil, err
	}
	var mode os.FileMode = 0777
	if req.GetMode() != 0 {
		mode = os.FileMode(req.GetMode() & 0777)
	}
	return emptyResponse(fs.MkdirAll(req.GetPath(), mode))
}

func (s *Service) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	fs, err := s.getFS(ctx)
	if err != nil {
		return nil, err
	}
	des, err := fs.ReadDir(req.GetPath())
	if err != nil {
		return nil, err
	}
	ret := &pb.ReadDirResponse{
		Entries: make(map[string]*pb.StatResponse, len(des)),
	}
	for _, de := range des {
		ret.Entries[de.Name()] = statResponse(de)
	}
	return ret, nil
}

func (s *Service) Chmod(ctx context.Context, req *pb.ChmodRequest) (*empty.Empty, error) {
	fs, err := s.getFS(ctx)
	if err != nil {
		return nil, err
	}
	if cfs, ok := fs.(billy.Change); ok {
		return emptyResponse(cfs.Chmod(req.GetPath(), os.FileMode(req.GetMode()&0777)))
	}
	return emptyResponse(billy.ErrNotSupported)
}

func (s *Service) Lchown(ctx context.Context, req *pb.ChownRequest) (*empty.Empty, error) {
	fs, err := s.getFS(ctx)
	if err != nil {
		return nil, err
	}
	if cfs, ok := fs.(billy.Change); ok {
		return emptyResponse(cfs.Lchown(req.GetPath(), int(req.GetUid()), int(req.GetGid())))
	}
	return nil, billy.ErrNotSupported
}

func (s *Service) Chown(ctx context.Context, req *pb.ChownRequest) (*empty.Empty, error) {
	fs, err := s.getFS(ctx)
	if err != nil {
		return nil, err
	}
	if cfs, ok := fs.(billy.Change); ok {
		return emptyResponse(cfs.Chown(req.GetPath(), int(req.GetUid()), int(req.GetGid())))
	}
	return nil, billy.ErrNotSupported
}

func (s *Service) Chtimes(ctx context.Context, req *pb.ChtimesRequest) (*empty.Empty, error) {
	fs, err := s.getFS(ctx)
	if err != nil {
		return nil, err
	}
	if cfs, ok := fs.(billy.Change); ok {
		return emptyResponse(cfs.Chtimes(req.GetPath(), req.GetAtime().AsTime(), req.GetMtime().AsTime()))
	}
	return nil, billy.ErrNotSupported
}

func (s *Service) Stat(ctx context.Context, req *pb.StatRequest) (*pb.StatResponse, error) {
	fs, err := s.getFS(ctx)
	if err != nil {
		return nil, err
	}
	return s.statResponse(fs.Stat(req.GetPath()))
}

func (s *Service) Lstat(ctx context.Context, req *pb.StatRequest) (*pb.StatResponse, error) {
	fs, err := s.getFS(ctx)
	if err != nil {
		return nil, err
	}
	if sfs, ok := fs.(billy.Symlink); ok {
		return s.statResponse(sfs.Lstat(req.GetPath()))
	}
	return nil, billy.ErrNotSupported
}

func (s *Service) statResponse(st os.FileInfo, err error) (*pb.StatResponse, error) {
	if err != nil {
		return nil, billyToGRPCError(err)
	}
	return statResponse(st), nil
}

func statResponse(st os.FileInfo) *pb.StatResponse {
	return &pb.StatResponse{
		Size:  st.Size(),
		Mode:  int32(st.Mode()),
		Mtime: timestamppb.New(st.ModTime()),
		IsDir: st.IsDir(),
	}
}

func (s *Service) Symlink(ctx context.Context, req *pb.SymlinkRequest) (*empty.Empty, error) {
	fs, err := s.getFS(ctx)
	if err != nil {
		return nil, err
	}
	if sfs, ok := fs.(billy.Symlink); ok {
		return emptyResponse(sfs.Symlink(req.GetTarget(), req.GetLink()))
	}
	return nil, billy.ErrNotSupported
}

func (s *Service) Readlink(ctx context.Context, req *pb.ReadlinkRequest) (*pb.ReadlinkResponse, error) {
	fs, err := s.getFS(ctx)
	if err != nil {
		return nil, err
	}
	if sfs, ok := fs.(billy.Symlink); ok {
		target, err := sfs.Readlink(req.GetPath())
		if err != nil {
			return nil, err
		}
		return &pb.ReadlinkResponse{
			Target: target,
		}, nil
	}
	return nil, billy.ErrNotSupported
}

func emptyResponse(err error) (*empty.Empty, error) {
	if err != nil {
		return nil, billyToGRPCError(err)
	}
	return &empty.Empty{}, nil
}
