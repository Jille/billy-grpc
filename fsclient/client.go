// Package fsclient provides a billy.Filesystem that talks to a remote BillyService gRPC server.
package fsclient

import (
	"context"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"strings"
	"sync"
	"time"

	pb "github.com/Jille/billy-grpc/proto"
	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/helper/chroot"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// FileSystem provides an API to the remote filesystem.
type FileSystem struct {
	ctx    context.Context
	client pb.BillyServiceClient
}

var _ billy.Filesystem = &FileSystem{}
var _ billy.Change = &FileSystem{}

// New creates a FileSystem object based on a gRPC connection.
// Because the Billy interface doesn't take a context, the context you pass here is used for all RPCs.
// FileSystem conforms to billy.Filesystem.
func New(ctx context.Context, c grpc.ClientConnInterface) *FileSystem {
	return &FileSystem{
		ctx:    ctx,
		client: pb.NewBillyServiceClient(c),
	}
}

type fileDescriptor struct {
	name                string
	sendMtx             sync.Mutex
	stream              pb.BillyService_FileDescriptorClient
	mtx                 sync.Mutex
	nextId              int64
	outstandingRequests map[int64]chan fileDescriptorResponse
	closing             bool
}

type fileDescriptorResponse struct {
	resp *pb.FileDescriptorResponse
	err  error
}

var _ billy.File = &fileDescriptor{}

func translateRemoteError(err error, op, name string) error {
	pe := &fs.PathError{
		Op:   op,
		Path: name,
		Err:  err,
	}
	switch status.Code(err) {
	case codes.NotFound:
		pe.Err = fs.ErrNotExist
	case codes.InvalidArgument:
		if strings.Contains(err.Error(), "billy.ErrCrossedBoundary") {
			return billy.ErrCrossedBoundary
		}
		pe.Err = fs.ErrInvalid
	case codes.AlreadyExists:
		pe.Err = fs.ErrExist
	case codes.PermissionDenied:
		pe.Err = fs.ErrPermission
	case codes.Unimplemented:
		return billy.ErrNotSupported
	case codes.FailedPrecondition:
		if strings.Contains(err.Error(), "billy.ErrReadOnly") {
			return billy.ErrReadOnly
		}
	}
	return pe
}

func (fd *fileDescriptor) translateRemoteError(err error, op string) error {
	if err != nil {
		return translateRemoteError(err, op, fd.name)
	}
	return nil
}

func handleEmptyResponse(_ *empty.Empty, err error, op, name string) error {
	if err != nil {
		return translateRemoteError(err, op, name)
	}
	return nil
}

func (fd *fileDescriptor) handleEmptyResponse(e *empty.Empty, err error, op string) error {
	return handleEmptyResponse(e, err, op, fd.name)
}

func (f *FileSystem) Open(name string) (billy.File, error) {
	return f.open(&pb.OpenRequest{
		Path:       name,
		ForReading: true,
	})
}

func (f *FileSystem) Create(name string) (billy.File, error) {
	return f.open(&pb.OpenRequest{
		Path:          name,
		ForReading:    true,
		ForWriting:    true,
		AllowCreation: true,
		Truncate:      true,
		CreateMode:    0666,
	})
}

func (f *FileSystem) OpenFile(name string, flags int, perm fs.FileMode) (billy.File, error) {
	req := &pb.OpenRequest{
		Path:       name,
		CreateMode: uint32(perm),
	}
	if flags&os.O_RDWR > 0 {
		req.ForReading = true
		req.ForWriting = true
	} else if flags&os.O_RDONLY > 0 {
		req.ForReading = true
	} else if flags&os.O_WRONLY > 0 {
		req.ForWriting = true
	}
	if flags&os.O_APPEND > 0 {
		req.Append = true
	}
	if flags&os.O_CREATE > 0 {
		req.AllowCreation = true
	}
	if flags&os.O_EXCL > 0 {
		req.Exclusive = true
	}
	if flags&os.O_SYNC > 0 {
		req.Sync = true
	}
	if flags&os.O_TRUNC > 0 {
		req.Truncate = true
	}
	supported := os.O_RDWR | os.O_RDONLY | os.O_WRONLY | os.O_APPEND | os.O_CREATE | os.O_EXCL | os.O_SYNC | os.O_TRUNC
	if flags & ^supported > 0 {
		return nil, errors.New("unsupported flags given to OpenFile")
	}
	return f.open(req)
}

func (f *FileSystem) open(req *pb.OpenRequest) (billy.File, error) {
	stream, err := f.client.FileDescriptor(f.ctx)
	if err != nil {
		return nil, translateRemoteError(err, "open", req.GetPath())
	}
	if err := stream.Send(&pb.FileDescriptorRequest{
		Request: &pb.FileDescriptorRequest_Open{
			Open: req,
		},
	}); err != nil {
		return nil, translateRemoteError(err, "open", req.GetPath())
	}
	msg, err := stream.Recv()
	if err != nil {
		return nil, translateRemoteError(err, "open", req.GetPath())
	}
	if err := status.ErrorProto(msg.GetStatus()); err != nil {
		_ = stream.CloseSend()
		return nil, translateRemoteError(err, "open", req.GetPath())
	}
	return &fileDescriptor{
		name:                req.GetPath(),
		stream:              stream,
		outstandingRequests: map[int64]chan fileDescriptorResponse{},
	}, nil
}

func (h *fileDescriptor) Name() string {
	return h.name
}

func (h *fileDescriptor) receiver() {
	h.mtx.Lock()
	defer h.mtx.Unlock()
	for len(h.outstandingRequests) > 0 || h.closing {
		h.mtx.Unlock()
		msg, err := h.stream.Recv()
		h.mtx.Lock()
		if err != nil {
			for _, ch := range h.outstandingRequests {
				ch <- fileDescriptorResponse{err: err}
			}
			h.outstandingRequests = nil
			break
		}
		ch, ok := h.outstandingRequests[msg.GetRequestId()]
		if !ok {
			// That shouldn't happen.
			continue
		}
		ch <- fileDescriptorResponse{
			err:  status.ErrorProto(msg.GetStatus()),
			resp: msg,
		}
		delete(h.outstandingRequests, msg.GetRequestId())
	}
}

func (h *fileDescriptor) send(req *pb.FileDescriptorRequest) (*pb.FileDescriptorResponse, error) {
	ch := make(chan fileDescriptorResponse, 1)
	h.mtx.Lock()
	if h.outstandingRequests == nil {
		h.mtx.Unlock()
		return nil, errors.New("file descriptor broken due to lost connection")
	}
	h.nextId++
	id := h.nextId
	h.outstandingRequests[id] = ch
	if len(h.outstandingRequests) == 1 {
		go h.receiver()
	}
	h.mtx.Unlock()
	req.RequestId = id

	h.sendMtx.Lock()
	if err := h.stream.Send(req); err != nil {
		h.sendMtx.Unlock()
		return nil, err
	}
	h.sendMtx.Unlock()

	ret := <-ch
	return ret.resp, ret.err
}

func (h *fileDescriptor) Close() error {
	h.mtx.Lock()
	// This causes the receiver thread to keep running until the stream actually closes.
	h.closing = true
	h.mtx.Unlock()
	_, err := h.send(&pb.FileDescriptorRequest{
		Request: &pb.FileDescriptorRequest_Close{
			Close: &pb.CloseRequest{},
		},
	})
	h.sendMtx.Lock()
	_ = h.stream.CloseSend()
	h.sendMtx.Unlock()
	return h.translateRemoteError(err, "close")
}

func (h *fileDescriptor) ReadAt(p []byte, offset int64) (int, error) {
	resp, err := h.send(&pb.FileDescriptorRequest{
		Request: &pb.FileDescriptorRequest_ReadAt{
			ReadAt: &pb.ReadAtRequest{
				Offset: offset,
				Size:   int64(len(p)),
			},
		},
	})
	if err != nil {
		return 0, h.translateRemoteError(err, "read")
	}
	return h.handleReadResponse(p, resp)
}

func (h *fileDescriptor) Read(p []byte) (int, error) {
	resp, err := h.send(&pb.FileDescriptorRequest{
		Request: &pb.FileDescriptorRequest_Read{
			Read: &pb.ReadRequest{
				Size: int64(len(p)),
			},
		},
	})
	if err != nil {
		return 0, h.translateRemoteError(err, "read")
	}
	return h.handleReadResponse(p, resp)
}

func (h *fileDescriptor) handleReadResponse(p []byte, resp *pb.FileDescriptorResponse) (int, error) {
	n := copy(p, resp.GetRead().GetData())
	if resp.GetRead().GetEof() {
		return n, io.EOF
	}
	return n, nil
}

func (h *fileDescriptor) WriteAt(p []byte, offset int64) (int, error) {
	resp, err := h.send(&pb.FileDescriptorRequest{
		Request: &pb.FileDescriptorRequest_WriteAt{
			WriteAt: &pb.WriteAtRequest{
				Offset: offset,
				Data:   p,
			},
		},
	})
	if err != nil {
		return 0, h.translateRemoteError(err, "write")
	}
	return int(resp.GetWrite().GetNumWritten()), nil
}

func (h *fileDescriptor) Write(p []byte) (int, error) {
	resp, err := h.send(&pb.FileDescriptorRequest{
		Request: &pb.FileDescriptorRequest_Write{
			Write: &pb.WriteRequest{
				Data: p,
			},
		},
	})
	if err != nil {
		return 0, h.translateRemoteError(err, "write")
	}
	return int(resp.GetWrite().GetNumWritten()), nil
}

func (h *fileDescriptor) Seek(offset int64, whence int) (int64, error) {
	req := &pb.SeekRequest{
		Offset: offset,
	}
	switch whence {
	case io.SeekStart:
		req.Whence = pb.SeekRequest_SEEK_START
	case io.SeekCurrent:
		req.Whence = pb.SeekRequest_SEEK_CURRENT
	case io.SeekEnd:
		req.Whence = pb.SeekRequest_SEEK_END
	}
	resp, err := h.send(&pb.FileDescriptorRequest{
		Request: &pb.FileDescriptorRequest_Seek{
			Seek: req,
		},
	})
	if err != nil {
		return 0, h.translateRemoteError(err, "seek")
	}
	return resp.GetSeek().GetOffset(), nil
}

func (h *fileDescriptor) Truncate(size int64) error {
	_, err := h.send(&pb.FileDescriptorRequest{
		Request: &pb.FileDescriptorRequest_Truncate{
			Truncate: &pb.TruncateRequest{
				Size: size,
			},
		},
	})
	if err != nil {
		return h.translateRemoteError(err, "truncate")
	}
	return nil
}

func (h *fileDescriptor) Lock() error {
	_, err := h.send(&pb.FileDescriptorRequest{
		Request: &pb.FileDescriptorRequest_Lock{
			Lock: &pb.LockRequest{},
		},
	})
	if err != nil {
		return h.translateRemoteError(err, "lock")
	}
	return nil
}

func (h *fileDescriptor) Unlock() error {
	_, err := h.send(&pb.FileDescriptorRequest{
		Request: &pb.FileDescriptorRequest_Unlock{
			Unlock: &pb.UnlockRequest{},
		},
	})
	if err != nil {
		return h.translateRemoteError(err, "unlock")
	}
	return nil
}

func (f *FileSystem) Stat(name string) (os.FileInfo, error) {
	resp, err := f.client.Stat(f.ctx, &pb.StatRequest{
		Path: name,
	})
	if err != nil {
		return nil, translateRemoteError(err, "stat", name)
	}
	return fileInfo{name, resp}, nil
}

func (f *FileSystem) Lstat(p string) (os.FileInfo, error) {
	resp, err := f.client.Lstat(f.ctx, &pb.StatRequest{
		Path: p,
	})
	if err != nil {
		return nil, translateRemoteError(err, "lstat", p)
	}
	return fileInfo{p, resp}, nil
}

type fileInfo struct {
	path string
	stat *pb.StatResponse
}

func (i fileInfo) Name() string {
	return i.path
}

func (i fileInfo) Size() int64 {
	return i.stat.GetSize()
}

func (i fileInfo) Mode() fs.FileMode {
	return fs.FileMode(i.stat.GetMode())
}

func (i fileInfo) ModTime() time.Time {
	return i.stat.GetMtime().AsTime()
}

func (i fileInfo) IsDir() bool {
	return i.stat.GetIsDir()
}

func (i fileInfo) Sys() interface{} {
	return i.stat
}

func (f *FileSystem) Rename(oldName, newName string) error {
	if _, err := f.client.Rename(f.ctx, &pb.RenameRequest{
		OldPath: oldName,
		NewPath: newName,
	}); err != nil {
		// TODO: Return *os.LinkError, not *os.PathError
		return translateRemoteError(err, "rename", oldName)
	}
	return nil
}

func (f *FileSystem) ReadDir(p string) ([]os.FileInfo, error) {
	resp, err := f.client.ReadDir(f.ctx, &pb.ReadDirRequest{
		Path: p,
	})
	if err != nil {
		return nil, translateRemoteError(err, "readdir", p)
	}
	ret := make([]os.FileInfo, 0, len(resp.GetEntries()))
	for fn, st := range resp.GetEntries() {
		ret = append(ret, fileInfo{
			path: fn,
			stat: st,
		})
	}
	return ret, nil
}

func (f *FileSystem) MkdirAll(p string, mode os.FileMode) error {
	if _, err := f.client.MkdirAll(f.ctx, &pb.MkdirAllRequest{
		Path: p,
		Mode: uint32(mode),
	}); err != nil {
		return translateRemoteError(err, "mkdir", p)
	}
	return nil
}

func (f *FileSystem) Remove(p string) error {
	if _, err := f.client.Remove(f.ctx, &pb.RemoveRequest{
		Path: p,
	}); err != nil {
		return translateRemoteError(err, "remove", p)
	}
	return nil
}

func (f *FileSystem) Chroot(p string) (billy.Filesystem, error) {
	return chroot.New(f, p), nil
}

func (f *FileSystem) Root() string {
	return "/"
}

func (f *FileSystem) Chmod(p string, mode os.FileMode) error {
	if _, err := f.client.Chmod(f.ctx, &pb.ChmodRequest{
		Path: p,
		Mode: int32(mode),
	}); err != nil {
		return translateRemoteError(err, "chmod", p)
	}
	return nil
}

func (f *FileSystem) Chown(p string, uid, gid int) error {
	if _, err := f.client.Chown(f.ctx, &pb.ChownRequest{
		Path: p,
		Uid:  int32(uid),
		Gid:  int32(gid),
	}); err != nil {
		return translateRemoteError(err, "chown", p)
	}
	return nil
}

func (f *FileSystem) Lchown(p string, uid, gid int) error {
	if _, err := f.client.Lchown(f.ctx, &pb.ChownRequest{
		Path: p,
		Uid:  int32(uid),
		Gid:  int32(gid),
	}); err != nil {
		return translateRemoteError(err, "lchown", p)
	}
	return nil
}

func (f *FileSystem) Chtimes(p string, atime, mtime time.Time) error {
	if _, err := f.client.Chtimes(f.ctx, &pb.ChtimesRequest{
		Path:  p,
		Atime: timestamppb.New(atime),
		Mtime: timestamppb.New(mtime),
	}); err != nil {
		return translateRemoteError(err, "chtimes", p)
	}
	return nil
}

func (f *FileSystem) Join(elem ...string) string {
	return path.Join(elem...)
}

func (f *FileSystem) Symlink(target, link string) error {
	if _, err := f.client.Symlink(f.ctx, &pb.SymlinkRequest{
		Target: target,
		Link:   link,
	}); err != nil {
		return translateRemoteError(err, "symlink", link)
	}
	return nil
}

func (f *FileSystem) Readlink(p string) (string, error) {
	resp, err := f.client.Readlink(f.ctx, &pb.ReadlinkRequest{
		Path: p,
	})
	if err != nil {
		return "", translateRemoteError(err, "readlink", p)
	}
	return resp.GetTarget(), nil
}

func (f *FileSystem) TempFile(dir, prefix string) (billy.File, error) {
	return nil, billy.ErrNotSupported
}
