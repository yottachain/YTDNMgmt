package YTDNMgmt

//Node instance
type Node struct {
	ID     int32
	NodeID string
	PubKey string
	Addrs  []string
}

//SuperNode instance
type SuperNode struct {
	ID      int32
	NodeID  string
	PubKey  string
	PrivKey string
	Addrs   []string
}

const YOTTA_DB string = "yotta"
const NODE_TAB string = "Node"
const SUPERNODE_TAB string = "SuperNode"
