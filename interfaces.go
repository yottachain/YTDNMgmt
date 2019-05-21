package YTDNMgmt

//All operations of node management
type NodeDao interface {
	RegisterNode(node *Node) (*Node, error)
	UpdateNodeStatus(node *Node) (*Node, error)
	IncrUsedSpace(id int32, incr int64) error
	AllocNodes(shardCount int32) ([]Node, error)
	GetNodes(nodeIDs []int32) ([]Node, error)
	GetSuperNodes() ([]SuperNode, error)
	GetSuperNodePrivateKey(id int32) (string, error)
	GetNodeIDByPubKey(pubkey string) (int32, error)
	GetSuperNodeIDByPubKey(pubkey string) (int32, error)
	AddDNI(id int32, shard []byte) error
	ActiveNodesList() ([]Node, error)
	Statistics() (*NodeStat, error)
}
