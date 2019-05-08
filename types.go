package YTDNMgmt

// Node instance
type Node struct {
	//data node index
	ID int32 `bson:"_id"`
	//data node ID, generated from PubKey
	NodeID string `bson:"nodeid"`
	//public key of data node
	PubKey string `bson:"pubkey"`
	//associated EOS aaccount
	Owner string `bson:"owner"`
	//listening addresses of data node
	Addrs []string `bson:"addrs"`
	//CPU usage of data node
	CPU int32 `bson:"cpu"`
	//memory usage of data node
	Memory int32 `bson:"memory"`
	//bandwidth usage of data node
	Bandwidth int32 `bson:"bandwidth"`
	//max space of data node
	MaxDataSpace int64 `bson:"maxDataSpace"`
	//space assigned to YTFS
	AssignedSpace int64 `bson:"assignedSpace"`
	//pre-allocated space of data node
	ProductiveSpace int64 `bson:"productiveSpace"`
	//used space of data node
	UsedSpace int64 `bson:"usedSpace"`
}

// NewNode create a node struct
func NewNode(id int32, nodeid string, pubkey string, owner string, addrs []string, cpu int32, memory int32, bandwidth int32, maxDataSpace int64, assignedSpace int64, productiveSpace int64, usedSpace int64) *Node {
	return &Node{ID: id, NodeID: nodeid, PubKey: pubkey, Owner: owner, Addrs: addrs, CPU: cpu, Memory: memory, Bandwidth: bandwidth, MaxDataSpace: maxDataSpace, AssignedSpace: assignedSpace, ProductiveSpace: productiveSpace, UsedSpace: usedSpace}
}

// SuperNode instance
type SuperNode struct {
	//super node index
	ID int32 `bson:"_id"`
	//super node ID, generated from PubKey
	NodeID string `bson:"nodeid"`
	//public key of super node
	PubKey string `bson:"pubkey"`
	//private key of super node
	PrivKey string `bson:"privkey"`
	//listening addresses of super node
	Addrs []string `bson:"addrs"`
}

// relative DB and collection name
const (
	MetaDB = "metadata"
	SeqTab = "sequence"

	YottaDB      = "yotta"
	NodeTab      = "Node"
	SuperNodeTab = "SuperNode"
	DNITab       = "DNI"
)

// index type of node and supernode collection
var (
	NodeIdxType      = 100
	SuperNodeIdxType = 101
)
