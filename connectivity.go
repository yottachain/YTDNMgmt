package YTDNMgmt

import (
	"context"
	"time"

	peer "github.com/libp2p/go-libp2p-peer"
	ps "github.com/libp2p/go-libp2p-peerstore"
	swarm "github.com/libp2p/go-libp2p-swarm"
	ma "github.com/multiformats/go-multiaddr"
	host "github.com/yottachain/P2PHost"
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
