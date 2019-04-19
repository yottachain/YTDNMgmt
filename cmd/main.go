package main

import (
	"fmt"

	nodemgmt "github.com/yottachain/YTDNMgmt"
)

func main() {
	nodeDao, _ := nodemgmt.NewInstance("mongodb://localhost:27017")
	// nodes, _ := nodeDao.AllocNodes(3)
	// for _, node := range nodes {
	// 	fmt.Println(node.NodeID)
	// }

	supernodes, _ := nodeDao.GetSuperNodes()
	for _, supernode := range supernodes {
		fmt.Println(supernode.PubKey)
	}
}
