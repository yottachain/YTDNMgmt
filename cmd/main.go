package main

import (
	nodemgmt "github.com/yottachain/YTDNMgmt"
)

func main() {
	nodeDao, _ := nodemgmt.NewInstance("mongodb://localhost:27017")
	nodeDao.IncrUsedSpace(4, 1000)
	// nodes, _ := nodeDao.AllocNodes(3)
	// for _, node := range nodes {
	// 	fmt.Println(node.NodeID)
	// }

	// supernodes, _ := nodeDao.GetSuperNodes()
	// for _, supernode := range supernodes {
	// 	fmt.Println(supernode.PubKey)
	// }

	// node := nodemgmt.NewNode(0, "16Uiu2HAmDJwmxHVbN33mjJHxQdXFzF1WMiu1kMd7JshDgmHeVciA", "5JvCxXLSLzihWdXT7C9mtQkfLFHJZPdX1hxQo6su7dNt28mZ5W2", "testuser",
	// 	[]string{"/ip4/10.0.1.4/tcp/9999", "/ip4/127.0.0.1/tcp/9999"}, 0, 0, 0, 0, 0, 0, 0)
	// node, err := nodeDao.RegisterNode(node)
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }

	// node = nodemgmt.NewNode(4, "", "", "",
	// 	[]string{"/ip4/10.0.1.5/tcp/9990", "/ip4/127.0.0.1/tcp/9990"}, 10, 20, 50, 50, 50, 10, 10)
	// n, err := nodeDao.UpdateNodeStatus(node)
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }
	// log.Println(n)
}
