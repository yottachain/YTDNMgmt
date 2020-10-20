package YTDNMgmt

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
	ReadableNodesList(timerange int) ([]*Node, error)
	Statistics() (*NodeStat, error)
}
