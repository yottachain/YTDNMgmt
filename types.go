package YTDNMgmt

import "go.mongodb.org/mongo-driver/bson/primitive"

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
	//weight for allocate data node
	Weight float64 `bson:"weight"`
	//Is node valid
	Valid int32 `bson:"valid"`
	//Is relay node
	Relay int32 `bson:"relay"`
	//timestamp of status updating operation
	Timestamp int64 `bson:"timestamp"`
}

//NewNode create a node struct
func NewNode(id int32, nodeid string, pubkey string, owner string, addrs []string, cpu int32, memory int32, bandwidth int32, maxDataSpace int64, assignedSpace int64, productiveSpace int64, usedSpace int64, relay int32) *Node {
	return &Node{ID: id, NodeID: nodeid, PubKey: pubkey, Owner: owner, Addrs: addrs, CPU: cpu, Memory: memory, Bandwidth: bandwidth, MaxDataSpace: maxDataSpace, AssignedSpace: assignedSpace, ProductiveSpace: productiveSpace, UsedSpace: usedSpace, Relay: relay}
}

//SuperNode instance
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

//ContractInfo instance
type ContractInfo struct {
	ID      int32  `bson:"_id"`
	User    string `bson:"user"`
	PrivKey string `bson:"privkey"`
}

//NodeStat statistics of data node
type NodeStat struct {
	ActiveMiners    int64 `bson:"activeMiners"`
	TotalMiners     int64 `bson:"totalMiners"`
	MaxTotal        int64 `bson:"maxTotal"`
	AssignedTotal   int64 `bson:"assignedTotal"`
	ProductiveTotal int64 `bson:"productiveTotal"`
	UsedTotal       int64 `bson:"usedTotal"`
}

//ShardCount shards count of one data node
type ShardCount struct {
	ID  int32 `bson:"_id"`
	Cnt int64 `bson:"cnt"`
}

//SpotCheckList list of spot check
type SpotCheckList struct {
	TaskID    primitive.ObjectID `bson:"_id"`
	TaskList  []*SpotCheckTask   `bson:"taskList"`
	Progress  int32              `bson:"progress"`
	Timestamp int64              `bson:"timestamp"`
	Duration  int64              `bson:"duration"`
}

//SpotCheckTask one spot check task
type SpotCheckTask struct {
	ID     int32  `bson:"id"`
	NodeID string `bson:"nodeid"`
	Addr   string `bson:"addr"`
	VNI    string `bson:"vni"`
}

//DNI
type DNI struct {
	ID     int32              `bson:"_id"`
	Shards []primitive.Binary `bson:"shards"`
}

//VNI
type VNI struct {
	ID  int32  `bson:"_id"`
	VNI []byte `bson:"vni"`
}

//relative DB and collection name
const (
	YottaDB      = "yotta"
	NodeTab      = "Node"
	SuperNodeTab = "SuperNode"
	DNITab       = "DNI"
	SequenceTab  = "Sequence"
	SpotCheckTab = "SpotCheck"
	//ContractInfoTab = "ContractInfo"
)

//index type of node and supernode collection
var (
	NodeIdxType      = 100
	SuperNodeIdxType = 101
)

//interval time of data node reporting status
var IntervalTime int64 = 60
