package YTDNMgmt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (self *NodeDaoImpl) AddrCheck(oldNode, newNode *Node) (relayUrl string, err error) {
	if newNode == nil {
		return "", errors.New("Node can not be nil")
	}
	if RelayUrlCheck(newNode.Addrs) {
		newNode.Relay = 0
	}
	n := rand.Intn(10000)
	if n > 200 && EqualSorted(oldNode.Addrs, newNode.Addrs) {
		return "", nil
	}
	if self.ConnectivityCheck(oldNode.NodeID, newNode.Addrs) {
		if oldNode.Valid == 0 {
			log.Printf("---- Node %d becomes valid.", oldNode.ID)
		}
		newNode.Valid = 1
		return "", nil
	} else {
		if oldNode.Valid == 1 {
			log.Printf("---- Node %d becomes invalid.", oldNode.ID)
		}
		newNode.Valid = 0
		newNode.Relay = 0
		rnode := self.AllocRelayNode()
		if rnode == nil {
			supernode, err := self.GetSuperNodeByID(self.bpID)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%s/p2p/%s/p2p-circuit", CheckPublicAddr(supernode.Addrs), supernode.NodeID), nil
		} else {
			return fmt.Sprintf("%s/p2p/%s/p2p-circuit", CheckPublicAddr(rnode.Addrs), rnode.NodeID), nil
		}

	}
}

func RelayUrlCheck(addrs []string) bool {
	if GetRelayUrl(addrs) != "" {
		return true
	} else {
		return false
	}
}

func GetRelayUrl(addrs []string) string {
	for _, addr := range addrs {
		if strings.Index(addr, "/p2p/") != -1 {
			return addr
		}
	}
	return ""
}

func (self *NodeDaoImpl) ConnectivityCheck(nodeID string, addrs []string) bool {
	err := self.host.TestNetwork(nodeID, addrs)
	if err != nil {
		return false
	}
	return true
}

func CheckPublicAddr(addrs []string) string {
	for _, addr := range addrs {
		if strings.HasPrefix(addr, "/ip4/127.") ||
			strings.HasPrefix(addr, "/ip4/192.168.") ||
			strings.HasPrefix(addr, "/ip4/169.254.") ||
			strings.HasPrefix(addr, "/ip4/10.") ||
			strings.HasPrefix(addr, "/ip4/172.16.") ||
			strings.HasPrefix(addr, "/ip4/172.17.") ||
			strings.HasPrefix(addr, "/ip4/172.18.") ||
			strings.HasPrefix(addr, "/ip4/172.19.") ||
			strings.HasPrefix(addr, "/ip4/172.20.") ||
			strings.HasPrefix(addr, "/ip4/172.21.") ||
			strings.HasPrefix(addr, "/ip4/172.22.") ||
			strings.HasPrefix(addr, "/ip4/172.23.") ||
			strings.HasPrefix(addr, "/ip4/172.24.") ||
			strings.HasPrefix(addr, "/ip4/172.25.") ||
			strings.HasPrefix(addr, "/ip4/172.26.") ||
			strings.HasPrefix(addr, "/ip4/172.27.") ||
			strings.HasPrefix(addr, "/ip4/172.28.") ||
			strings.HasPrefix(addr, "/ip4/172.29.") ||
			strings.HasPrefix(addr, "/ip4/172.30.") ||
			strings.HasPrefix(addr, "/ip4/172.31.") ||
			strings.HasPrefix(addr, "/p2p-circuit/") ||
			strings.Index(addr, "/p2p/") != -1 {
			continue
		} else {
			return addr
		}
	}
	return ""
}

func (self *NodeDaoImpl) AllocRelayNode() *Node {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	node := new(Node)
	options := options.FindOneOptions{}
	options.Sort = bson.D{{"timestamp", -1}}
	err := collection.FindOne(context.Background(), bson.M{"valid": 1, "status": 1, "relay": 1, "bandwidth": bson.M{"$lt": 50}, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}}, &options).Decode(node)
	if err != nil {
		return nil
	}
	return node
}
