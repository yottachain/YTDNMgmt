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
	//owner account of this miner
	Owner string `bson:"owner"`
	//profit account of this miner
	ProfitAcc string `bson:"profitAcc"`
	//ID of associated miner pool
	PoolID string `bson:"poolID"`
	//quota allocated by associated miner pool
	Quota int64 `bson:"quota"`
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
	//status code: 0 - after preregister 1 - after register 2 - active
	Status int32 `bson:"status"`
	//timestamp of status updating operation
	Timestamp int64 `bson:"timestamp"`
}

//NewNode create a node struct
func NewNode(id int32, nodeid string, pubkey string, owner string, profitAcc string, poolID string, quota int64, addrs []string, cpu int32, memory int32, bandwidth int32, maxDataSpace int64, assignedSpace int64, productiveSpace int64, usedSpace int64, weight float64, valid int32, relay int32, status int32, timestamp int64) *Node {
	return &Node{ID: id, NodeID: nodeid, PubKey: pubkey, Owner: owner, ProfitAcc: profitAcc, PoolID: poolID, Quota: quota, Addrs: addrs, CPU: cpu, Memory: memory, Bandwidth: bandwidth, MaxDataSpace: maxDataSpace, AssignedSpace: assignedSpace, ProductiveSpace: productiveSpace, UsedSpace: usedSpace, Weight: weight, Valid: valid, Relay: relay, Status: status, Timestamp: timestamp}
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
