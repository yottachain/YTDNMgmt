package YTDNMgmt

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p-host"

	"github.com/libp2p/go-libp2p"
	peer "github.com/libp2p/go-libp2p-peer"
	ps "github.com/libp2p/go-libp2p-peerstore"
	swarm "github.com/libp2p/go-libp2p-swarm"
	ma "github.com/multiformats/go-multiaddr"
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
