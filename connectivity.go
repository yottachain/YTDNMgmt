package YTDNMgmt

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p-core/crypto"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/mr-tron/base58"
	ma "github.com/multiformats/go-multiaddr"
	ytcrypto "github.com/yottachain/YTCrypto"
	host "github.com/yottachain/YTHost"
	hst "github.com/yottachain/YTHost/interface"
	"github.com/yottachain/YTHost/option"
)

//GETTOKEN token ID of
const GETTOKEN = 50311

//Host p2p host
type Host struct {
	//lhost host.Host
	lhost  hst.Host
	config *MiscConfig
}

//NewHost create a new host
func NewHost(config *MiscConfig) (*Host, error) {
	sk, _ := ytcrypto.CreateKey()
	ma, _ := ma.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", 0))
	privbytes, err := base58.Decode(sk)
	if err != nil {
		return nil, err
	}
	pk, err := crypto.UnmarshalSecp256k1PrivateKey(privbytes[1:33])
	if err != nil {
		return nil, err
	}
	lhost, err := host.NewHost(option.ListenAddr(ma), option.Identity(pk))
	if err != nil {
		return nil, err
	}
	go lhost.Accept()
	return &Host{lhost: lhost, config: config}, nil
}

// func NewHost(config *MiscConfig) (*Host, error) {
// 	//host, err := libp2p.New(context.Background())
// 	sk, _ := ytcrypto.CreateKey()
// 	host, err := server.NewServer("0", sk)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return &Host{lhost: host, config: config}, nil
// }

//SendMsg send a message to client
func (host *Host) SendMsg(id string, msg []byte) ([]byte, error) {
	// sendMsgReq := &pb.SendMsgReq{Id: id, Msgid: msg[0:2], Msg: msg[2:]}
	// ctx, cancle := context.WithTimeout(context.Background(), time.Second*time.Duration(host.config.ConnectivityConnectTimeout))
	// defer cancle()
	// sendMsgResp, err := host.lhost.SendMsg(ctx, sendMsgReq)
	// if err != nil {
	// 	return nil, err
	// }
	// return sendMsgResp.Value, nil

	msid := msg[0:2]
	bytebuff := bytes.NewBuffer(msid)
	var tmp uint16
	err := binary.Read(bytebuff, binary.BigEndian, &tmp)
	msgID := int32(tmp)
	ID, err := peer.Decode(id)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*time.Duration(host.config.ConnectivityConnectTimeout))
	if msgID == GETTOKEN {
		ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*1000)
	}
	defer cancel()

	bytes, err := host.lhost.SendMsg(ctx, ID, msgID, msg[2:])
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

//TestNetwork connectivity test
func (host *Host) TestNetwork(nodeID string, addrs []string) error {
	return host.testNetworkN(nodeID, addrs, 2)
}

func (host *Host) testNetworkN(nodeID string, addrs []string, retries int) error {
	err := host.testNetwork(nodeID, addrs)
	if err == nil {
		return nil
	}
	if retries == 0 {
		log.Printf("connectivity: testNetwork: connect %s failed\n", nodeID)
		return err
	}
	return host.testNetworkN(nodeID, addrs, retries-1)
}

func (host *Host) testNetwork(nodeID string, addrs []string) error {
	ctx, cancle := context.WithTimeout(context.Background(), time.Second*time.Duration(host.config.ConnectivityConnectTimeout))
	defer cancle()
	ID, err := peer.Decode(nodeID)
	if err != nil {
		return err
	}
	maddrs, _ := stringListToMaddrs(addrs)
	_, err = host.lhost.ClientStore().Get(ctx, ID, maddrs)
	if err != nil {
		return err
	}
	err = host.lhost.ClientStore().Close(ID)
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
