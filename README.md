# billy-grpc

fsserver: [![GoDoc](https://godoc.org/github.com/Jille/billy-grpc/fsserver?status.svg)](https://godoc.org/github.com/Jille/billy-grpc/fsserver)

fsclient: [![GoDoc](https://godoc.org/github.com/Jille/billy-grpc/fsclient?status.svg)](https://godoc.org/github.com/Jille/billy-grpc/fsclient)

The `fsclient` library provides a billy.Filesystem that talks to a gRPC server. The `fsserver` library provides a gRPC server that talks to a billy.Filesystem.

This allows you to access a remote filesystem as if it were local.

In fsserver you can access the gRPC peer from the context and return a different filesystem for different peers. You're expected to perform authentication at or before this layer.

The Billy interface isn't ideal for this, but I didn't want to introduce a 15th standard.
