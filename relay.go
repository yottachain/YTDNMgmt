package YTDNMgmt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// AddrCheck check if address of node is connectable
func (self *NodeDaoImpl) AddrCheck(oldNode, newNode *Node) (relayUrl string, err error) {
	if newNode == nil {
		log.Println("relay: AddrCheck: node cannot be null")
		return "", errors.New("node cannot be nil")
	}
	if RelayUrlCheck(newNode.Addrs) {
		newNode.Relay = 0
	}
	n := rand.Intn(200 * int(self.Config.Misc.ConnectivityTestInterval))
	if n > 200 && EqualSorted(oldNode.Addrs, newNode.Addrs) {
		return "", nil
	}
	log.Printf("relay: AddrCheck: connectivity check of node %d\n", oldNode.ID)
	if self.ConnectivityCheck(oldNode.NodeID, newNode.Addrs) {
		if oldNode.Valid == 0 {
			log.Printf("relay: AddrCheck: node %d becomes valid\n", oldNode.ID)
		}
		newNode.Valid = 1
		return "", nil
	} else {
		if oldNode.Valid == 1 {
			log.Printf("relay: AddrCheck: node %d becomes invalid\n", oldNode.ID)
		}
		newNode.Valid = 0
		newNode.Relay = 0
		rnode := self.AllocRelayNode()
		if rnode == nil {
			supernode, err := self.GetSuperNodeByID(self.bpID)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("%s/p2p/%s/p2p-circuit", CheckPublicAddr(supernode.Addrs, self.Config.Misc.ExcludeAddrPrefix), supernode.NodeID), nil
		} else {
			return fmt.Sprintf("%s/p2p/%s/p2p-circuit", CheckPublicAddr(rnode.Addrs, self.Config.Misc.ExcludeAddrPrefix), rnode.NodeID), nil
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
	err := self.host1.TestNetwork(nodeID, addrs)
	if err != nil {
		return false
	}
	//log.Printf("### cancel connectivity check")
	return true
}

func CheckPublicAddr(addrs []string, excludeAddrPrefix string) string {
	if excludeAddrPrefix != "" {
		for _, addr := range addrs {
			if strings.HasPrefix(addr, excludeAddrPrefix) {
				return addr
			}
		}
	}
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
			strings.HasPrefix(addr, "/ip6/") ||
			strings.HasPrefix(addr, "/p2p-circuit/") ||
			strings.Index(addr, "/p2p/") != -1 {
			// if excludeAddrPrefix != "" && strings.HasPrefix(addr, excludeAddrPrefix) {
			// 	return addr
			// }
			continue
		} else {
			return addr
		}
	}
	return ""
}

func CheckPublicAddrs(addrs []string, excludeAddrPrefix string) []string {
	filteredAddrs := []string{}
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
			strings.HasPrefix(addr, "/ip6/") ||
			strings.HasPrefix(addr, "/p2p-circuit/") {
			//strings.Index(addr, "/p2p/") != -1 {
			if excludeAddrPrefix != "" && strings.HasPrefix(addr, excludeAddrPrefix) {
				filteredAddrs = append(filteredAddrs, addr)
			}
			continue
		} else {
			filteredAddrs = append(filteredAddrs, addr)
		}
	}
	return dedup(filteredAddrs)
}

func (self *NodeDaoImpl) AllocRelayNode() *Node {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	node := new(Node)
	options := options.FindOneOptions{}
	options.Sort = bson.D{{"timestamp", -1}}
	err := collection.FindOne(context.Background(), bson.M{"valid": 1, "status": 1, "relay": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*self.Config.Misc.AvaliableMinerTimeGap}}, &options).Decode(node)
	if err != nil {
		return nil
	}
	return node
}

func dedup(urls []string) []string {
	if urls == nil || len(urls) == 0 {
		return nil
	}
	sort.Strings(urls)
	j := 0
	for i := 1; i < len(urls); i++ {
		if urls[j] == urls[i] {
			continue
		}
		j++
		// preserve the original data
		// in[i], in[j] = in[j], in[i]
		// only set what is required
		urls[j] = urls[i]
	}
	return urls[:j+1]
}
