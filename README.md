# YTDNMgmt

Data node management module.

## Prerequisities
1. Install protobuf compiler
```
 $ mkdir -p /usr/local/protoc
 $ cd /usr/local/protoc
 $ wget https://github.com/protocolbuffers/protobuf/releases/download/v3.9.1/protoc-3.9.1-linux-x86_64.zip
 $ unzip protoc-3.9.1-linux-x86_64.zip
 $ ln -s bin/protoc /usr/local/bin/protoc
```
2. Verify
```
$ protoc --version
```

3. Install plugins of Go protobuf
```
$ go get -u github.com/golang/protobuf/proto
$ go get -u github.com/golang/protobuf/protoc-gen-go
```
4. Install grpc-go
```
$ go get -u google.golang.org/grpc
```

5. Generate code
Entering into project folder and execute:
```
$ protoc -I pb pb/types.proto --go_out=plugins=grpc:pb
```