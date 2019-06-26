package YTDNMgmt

//All operations of node management
type NodeDao interface {
	NewNodeID() (int32, error)
	PreRegisterNode(trx string) error
	ChangeMinerPool(trx string) error
	RegisterNode(node *Node) (*Node, error)
	UpdateNodeStatus(node *Node) (*Node, error)
	IncrUsedSpace(id int32, incr int64) error
	AllocNodes(shardCount int32) ([]Node, error)
	SyncNode(node *Node) error
	GetNodes(nodeIDs []int32) ([]Node, error)
	GetSuperNodes() ([]SuperNode, error)
	GetSuperNodePrivateKey(id int32) (string, error)
	GetNodeIDByPubKey(pubkey string) (int32, error)
	GetNodeByPubKey(pubkey string) (*Node, error)
	GetSuperNodeIDByPubKey(pubkey string) (int32, error)
	AddDNI(id int32, shard []byte) error
	ActiveNodesList() ([]Node, error)
	Statistics() (*NodeStat, error)
	GetSpotCheckList() ([]*SpotCheckList, error)
	GetSTNode() (*Node, error)
	UpdateTaskStatus(id string, progress int32, invalidNodeList []int32) error
}
