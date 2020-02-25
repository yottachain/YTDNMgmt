package YTDNMgmt

import "go.mongodb.org/mongo-driver/bson/primitive"

//All operations of node management
type NodeDao interface {
	SetMaster(master int32)
	ChangeEosURL(eosURL string)
	NewNodeID() (int32, error)
	CallAPI(trx string, apiName string) error
	UpdateNodeStatus(node *Node) (*Node, error)
	IncrUsedSpace(id int32, incr int64) error
	AllocNodes(shardCount int32, errIDs []int32) ([]*Node, error)
	SyncNode(node *Node) error
	GetNodes(nodeIDs []int32) ([]*Node, error)
	GetSuperNodes() ([]*SuperNode, error)
	GetSuperNodePrivateKey(id int32) (string, error)
	GetNodeIDByPubKey(pubkey string) (int32, error)
	GetNodeByPubKey(pubkey string) (*Node, error)
	GetSuperNodeIDByPubKey(pubkey string) (int32, error)
	AddDNI(id int32, shard []byte) error
	ActiveNodesList() ([]*Node, error)
	Statistics() (*NodeStat, error)
	GetSpotCheckList() ([]*SpotCheckList, error)
	GetSTNode() (*Node, error)
	GetSTNodes(count int64) ([]*Node, error)
	UpdateTaskStatus(id string, invalidNodeList []int32) error

	GetInvalidNodes() ([]*ShardCount, error)
	GetRebuildItem(minerID int32, index, total int64) (*Node, []primitive.Binary, error)
	GetRebuildNode(count int64) (*Node, error)
	DeleteDNI(minerID int32, shard []byte) error
	FinishRebuild(id int32) error
}
