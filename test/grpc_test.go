package grpc_test

import (
	"context"
	"net"
	"testing"

	"github.com/Jille/billy-grpc/fsclient"
	"github.com/Jille/billy-grpc/fsserver"
	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/osfs"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type serverCallbacks struct {
	fs billy.Filesystem
}

func (s serverCallbacks) FilesystemForPeer(context.Context) (billy.Filesystem, codes.Code, error) {
	return s.fs, 0, nil
}

func TestBilly(t *testing.T) {
	// Set up server side.
	s := grpc.NewServer()
	fsserver.RegisterService(s, serverCallbacks{osfs.New(t.TempDir())})
	serverSock, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("Failed to listen on random port: %v", err)
	}
	go s.Serve(serverSock)

	// Connect as a client.
	ctx := context.Background()
	addr := serverSock.Addr().(*net.TCPAddr)
	addr.IP = net.IPv4(127, 0, 0, 1)
	conn, err := grpc.Dial(addr.String(), grpc.WithInsecure())
	if err != nil {
		t.Fatalf("Failed to dial server at %s: %v", addr.String(), err)
	}
	rfs := fsclient.New(ctx, conn)

	// Let's play around.
	fh, err := rfs.Create("file1.txt")
	if err != nil {
		t.Fatalf("Failed to create file1.txt: %v", err)
	}
	n, err := fh.Write([]byte("Hello world"))
	if err != nil {
		t.Errorf("Failed to write to file1.txt: %v", err)
	}
	if n != len("Hello world") {
		t.Errorf("Partial write: %v", err)
	}
	if err := fh.Close(); err != nil {
		t.Errorf("Failed to close file1.txt: %v", err)
	}
	matches, err := rfs.ReadDir("/")
	if err != nil {
		t.Errorf("Failed to ReadDir: %v", err)
	}
	if len(matches) != 1 {
		t.Fatal("No results from ReadDir(/)")
	}
	if matches[0].Name() != "file1.txt" {
		t.Errorf("ReadDir() returned the wrong file %s", matches[0].Name())
	}
	if matches[0].Size() != int64(len("Hello world")) {
		t.Errorf("ReadDir() returned the wrong file size %d", matches[0].Size())
	}
}
