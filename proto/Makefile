billy.pb.go: billy.proto status/status.pb.go
	protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative billy.proto

status/status.pb.go: status/status.proto
	protoc --go_out=status --go_opt=paths=source_relative status/status.proto
