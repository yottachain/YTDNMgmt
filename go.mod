module github.com/yottachain/YTDNMgmt

go 1.13

require (
	github.com/coreos/go-systemd v0.0.0-20180511133405-39ca1b05acc7 // indirect
	github.com/eoscanada/eos-go v0.8.16
	github.com/golang/protobuf v1.3.2
	github.com/golang/snappy v0.0.0-20180518054509-2e65f85255db // indirect
	github.com/ipipdotnet/ipdb-go v1.2.0
	github.com/ivpusic/grpool v1.0.0
	github.com/libp2p/go-libp2p-core v0.3.0
	github.com/mr-tron/base58 v1.1.3
	github.com/multiformats/go-multiaddr v0.2.0
	github.com/tidwall/gjson v1.3.4 // indirect
	github.com/tidwall/sjson v1.0.4 // indirect
	github.com/xdg/scram v0.0.0-20180814205039-7eeb5667e42c // indirect
	github.com/xdg/stringprep v1.0.0 // indirect
	github.com/yottachain/P2PHost v1.0.0
	github.com/yottachain/YTCrypto v0.0.0-20191111140914-83c018a089b4
	go.etcd.io/etcd v3.3.18+incompatible
	go.mongodb.org/mongo-driver v1.1.3
	google.golang.org/genproto v0.0.0-20191216205247-b31c10ee225f // indirect
	google.golang.org/grpc v1.26.0
)

replace github.com/yottachain/P2PHost => ../../GoWorkspaces/src/github.com/aurawing/P2PHost
