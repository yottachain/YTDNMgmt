package YTDNMgmt

import (
	"context"
	"encoding/gob"
	"io/ioutil"
	"log"
	"time"

	"github.com/golang/protobuf/proto"
	libp2p "github.com/libp2p/go-libp2p"
	host "github.com/libp2p/go-libp2p-core/host"
	peer "github.com/libp2p/go-libp2p-peer"
	ps "github.com/libp2p/go-libp2p-peerstore"
	protocol "github.com/libp2p/go-libp2p-protocol"
	swarm "github.com/libp2p/go-libp2p-swarm"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/yottachain/YTDNMgmt/pb"
)

type Host struct {
	lhost host.Host
}

func NewHost() (*Host, error) {
	host, err := libp2p.New(context.Background())
	if err != nil {
		return nil, err
	}
	return &Host{lhost: host}, nil
}

func (host *Host) CheckVNI(node *Node, vni []byte) (int, error) {
	maddrs, err := stringListToMaddrs(node.Addrs)
	if err != nil {
		return 0, err
	}
	pid, err := peer.IDB58Decode(node.NodeID)
	if err != nil {
		return 0, err
	}
	info := ps.PeerInfo{
		pid,
		maddrs,
	}
	ctx, cancle := context.WithTimeout(context.Background(), time.Second*30)
	defer cancle()
	defer host.lhost.Network().ClosePeer(pid)
	defer host.lhost.Network().(*swarm.Swarm).Backoff().Clear(pid)
	err = host.lhost.Connect(ctx, info)
	if err != nil {
		log.Printf("recheck: connect node failed: %d %s\n", node.ID, err.Error())
		return 1, nil
	}
	downloadRequest := &pb.DownloadShardRequest{VHF: vni}
	checkData, err := proto.Marshal(downloadRequest)
	if err != nil {
		log.Println("error when marshalling protobuf message: downloadrequest: %s\n", err.Error())
		return 0, err
	}
	// 发送下载分片命令
	shardData, err := host.SendMsg(node.NodeID, "/node/0.0.2", append(pb.MsgIDDownloadShardRequest.Bytes(), checkData...))
	if err != nil {
		log.Println("SN send recheck command failed: %d %s %s\n", node.ID, vni, err.Error())
		return 1, nil
	}
	var share pb.DownloadShardResponse
	err = proto.Unmarshal(shardData[2:], &share)
	if err != nil {
		log.Println("SN unmarshal recheck response failed: %d %s %s\n", node.ID, vni, err.Error())
		return 0, err
	}
	if downloadRequest.VerifyVHF(share.Data) {
		return 0, nil
	}
	return 2, nil
}

func (host *Host) SendMsg(id string, msgType string, msg []byte) ([]byte, error) {
	pid := protocol.ID(msgType)
	peerID, err := peer.IDB58Decode(id)
	if err != nil {
		return nil, err
	}
	ctx, cancle := context.WithTimeout(context.Background(), time.Second*time.Duration(30))
	defer cancle()
	stm, err := host.lhost.NewStream(ctx, peerID, pid)
	if err != nil {
		return nil, err
	}
	defer stm.Close()
	stm.SetReadDeadline(time.Now().Add(time.Duration(30) * time.Second))
	ed := gob.NewEncoder(stm)
	err = ed.Encode(msg)
	if err != nil {
		return nil, err
	}
	return ioutil.ReadAll(stm)
}

func (host *Host) TestNetwork(nodeID string, addrs []string) error {
	return host.testNetworkN(nodeID, addrs, 2)
}

func (host *Host) testNetworkN(nodeID string, addrs []string, retries int) error {
	err := host.testNetwork(nodeID, addrs)
	if err == nil {
		return nil
	}
	if retries == 0 {
		return err
	} else {
		return host.testNetworkN(nodeID, addrs, retries-1)
	}
}

func (host *Host) testNetwork(nodeID string, addrs []string) error {
	maddrs, err := stringListToMaddrs(addrs)
	if err != nil {
		return err
	}
	pid, err := peer.IDB58Decode(nodeID)
	if err != nil {
		return err
	}
	info := ps.PeerInfo{
		pid,
		maddrs,
	}
	ctx, cancle := context.WithTimeout(context.Background(), time.Second*5)
	defer cancle()
	defer host.lhost.Network().ClosePeer(pid)
	defer host.lhost.Network().(*swarm.Swarm).Backoff().Clear(pid)
	err = host.lhost.Connect(ctx, info)
	if err != nil {
		return err
	}
	return nil
}

func stringListToMaddrs(addrs []string) ([]ma.Multiaddr, error) {
	maddrs := make([]ma.Multiaddr, len(addrs))
	for k, addr := range addrs {
		maddr, err := ma.NewMultiaddr(addr)
		if err != nil {
			return maddrs, err
		}
		maddrs[k] = maddr
	}
	return maddrs, nil
}
