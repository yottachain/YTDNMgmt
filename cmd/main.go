package main

import (
	"fmt"
	"log"

	eos "github.com/eoscanada/eos-go"
	nodemgmt "github.com/yottachain/YTDNMgmt"
)

type Miner struct {
	Owner   eos.AccountName `json:"owner"`
	MinerID uint64          `json:"minerid"`
}

func main() {

	nodeDao, _ := nodemgmt.NewInstance("mongodb://127.0.0.1:27017", "http://152.136.16.118:8888", "username1234", "5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx", "hddpool12345", 2)
	list, err := nodeDao.GetSpotCheckList()
	if err != nil {
		log.Fatalln(err.Error())
	}
	fmt.Printf("%d\n", len(list[0].TaskList))
	node, err := nodeDao.GetSTNode()
	if err != nil {
		log.Fatalln(err.Error())
	}
	fmt.Printf("%d\n", node.ID)
	err = nodeDao.UpdateTaskStatus("5d0854da30a31ead856c870c", 60, nil)
	if err != nil {
		log.Fatalln(err.Error())
	}

	// host, err := YTDNMgmt.NewHost()
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }
	// err = host.TestNetwork("16Uiu2HAmTFtXjT9GxzABmkATsmC2SZqtpxyTPqZ9cszCCZn4rM8M", []string{"/ip4/127.0.0.1/tcp/9001", "/ip4/192.168.123.50/tcp/9001", "/ip4/10.211.55.2/tcp/9001", "/ip4/10.37.129.2/tcp/9001", "/ip4/36.110.28.94/tcp/9001", "/ip4/152.136.18.185/tcp/9999/p2p/16Uiu2HAkwNCD9HSH5hh36LmzgLjRcQiQFpT9spwspaAM5AH3rqA9/p2p-circuit"})
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }
	// nodeDao, _ := nodemgmt.NewInstance("mongodb://152.136.18.185:27017", "http://152.136.16.118:8888", "username1234", "5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx", "hddpool12345", 2)
	// node := new(nodemgmt.Node)
	// node.ID = 767
	// node.Bandwidth = 0
	// node.CPU = 10
	// node.MaxDataSpace = 2621440
	// node.Memory = 30
	// node.Addrs = []string{"/ip4/127.0.0.1/tcp/9001", "/ip4/192.168.123.50/tcp/9001", "/ip4/10.211.55.2/tcp/9001", "/ip4/10.37.129.2/tcp/9001", "/ip4/36.110.28.94/tcp/9001", "/ip4/152.136.18.185/tcp/9999/p2p/16Uiu2HAkwNCD9HSH5hh36LmzgLjRcQiQFpT9spwspaAM5AH3rqA9/p2p-circuit"}
	// node.Relay = 0
	// _, err := nodeDao.UpdateNodeStatus(node)
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }

	// node := nodeDao.AllocRelayNode()
	// if node == nil {
	// 	fmt.Println("nil")
	// }
	// fmt.Printf("%d", node.ID)
	// // nodeDao.IncrUsedSpace(4, 1000)
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

	//api := eos.New("http://152.136.11.202:8888")
	// api := eos.New("http://192.168.111.140:8888")

	// account := eos.AccountName("alice")
	// info, err := api.GetAccount(account)
	// if err != nil {
	// 	if err == eos.ErrNotFound {
	// 		fmt.Printf("unknown account: %s", account)
	// 		return
	// 	}

	// 	panic(fmt.Errorf("get account: %s", err))
	// }

	// bytes, err := json.Marshal(info)
	// if err != nil {
	// 	panic(fmt.Errorf("json marshal response: %s", err))
	// }

	// fmt.Println(string(bytes))

	// api := eos.New("http://192.168.111.140:8888")

	// infoResp, _ := api.GetInfo()
	// fmt.Println(infoResp.ChainID)
	// accountResp, _ := api.GetAccount("alice")
	// fmt.Println("Permission for initn:", accountResp.Permissions[0].RequiredAuth.Keys)

	/////////////////////////////////////////////////////////////////////////////////////

	// privatekey := "5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx"
	// //api := eos.New("http://152.136.11.202:8888")
	// api := eos.New("http://152.136.18.185:8888")
	// keyBag := &eos.KeyBag{}
	// err := keyBag.ImportPrivateKey(privatekey)
	// if err != nil {
	// 	panic(fmt.Errorf("import private key: %s", err))
	// }
	// api.SetSigner(keyBag)
	// api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
	// 	publickey, _ := ytcrypto.GetPublicKeyByPrivateKey(privatekey)
	// 	pubkey, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "EOS", publickey))
	// 	return []ecc.PublicKey{pubkey}, nil
	// })

	// // from := eos.AccountName("username1234")
	// // to := eos.AccountName("hddpool12345")

	// action := &eos.Action{
	// 	Account: eos.AN("hddpool12345"),
	// 	Name:    eos.ActN("newmaccount"),
	// 	Authorization: []eos.PermissionLevel{
	// 		{Actor: eos.AN("username1234"), Permission: eos.PN("active")},
	// 	},
	// 	ActionData: eos.NewActionData(Miner{Owner: "username1234", MinerID: 72}),
	// }

	// // quantity, err := eos.NewEOSAssetFromString("1.0000 EOS")
	// // memo := ""

	// // if err != nil {
	// // 	panic(fmt.Errorf("invalid quantity: %s", err))
	// // }

	// txOpts := &eos.TxOptions{}
	// if err := txOpts.FillFromChain(api); err != nil {
	// 	panic(fmt.Errorf("filling tx opts: %s", err))
	// }

	// tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	// signedTx, packedTx, err := api.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	// if err != nil {
	// 	panic(fmt.Errorf("sign transaction: %s", err))
	// }

	// content, err := json.MarshalIndent(signedTx, "", "  ")
	// if err != nil {
	// 	panic(fmt.Errorf("json marshalling transaction: %s", err))
	// }

	// fmt.Println(string(content))
	// fmt.Println()

	// response, err := api.PushTransaction(packedTx)
	// if err != nil {
	// 	panic(fmt.Errorf("push transaction: %s", err))
	// }

	// fmt.Printf("Transaction [%s] submitted to the network succesfully.\n", hex.EncodeToString(response.Processed.ID))

	/////////////////////////////////////////////////////////////////////////////////////

	// api := eos.New("http://152.136.11.202:8888")
	// keyBag := &eos.KeyBag{}
	// err := keyBag.ImportPrivateKey("5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx")
	// if err != nil {
	// 	panic(fmt.Errorf("import private key: %s", err))
	// }
	// api.SetSigner(keyBag)
	// api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
	// 	pubkey, _ := ecc.NewPublicKey("EOS4uvDGmiLVpodsnXG4D2J8o66gP3HZxT9TC4wDmtdoBHNmZsjUg")
	// 	return []ecc.PublicKey{pubkey}, nil
	// })

	// from := eos.AccountName("username1234")
	// to := eos.AccountName("username1111")

	// quantity, err := eos.NewEOSAssetFromString("1.0000")
	// memo := ""

	// if err != nil {
	// 	panic(fmt.Errorf("invalid quantity: %s", err))
	// }

	// txOpts := &eos.TxOptions{}
	// if err := txOpts.FillFromChain(api); err != nil {
	// 	panic(fmt.Errorf("filling tx opts: %s", err))
	// }

	// tx := eos.NewTransaction([]*eos.Action{token.NewTransfer(from, to, quantity, memo)}, txOpts)
	// signedTx, packedTx, err := api.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	// if err != nil {
	// 	panic(fmt.Errorf("sign transaction: %s", err))
	// }

	// content, err := json.MarshalIndent(signedTx, "", "  ")
	// if err != nil {
	// 	panic(fmt.Errorf("json marshalling transaction: %s", err))
	// }

	// fmt.Println(string(content))
	// fmt.Println()

	// response, err := api.PushTransaction(packedTx)
	// if err != nil {
	// 	panic(fmt.Errorf("push transaction: %s", err))
	// }

	// fmt.Printf("Transaction [%s] submitted to the network succesfully.\n", hex.EncodeToString(response.Processed.ID))

	// etx, _ := eostx.NewInstance("http://152.136.16.118:8888", "producer1", "5HtM6e3mQNLEu2TkQ1ZrbMNpRQiHGsKxEsLdxd9VsdCmp1um8QH", "hddpool12345")
	// err := etx.AddMiner("username1234", 99999)
	// if err != nil {
	// 	fmt.Println(err.Error())
	// }
	// err = etx.AddSpace("username3333", 10000010, 100)
	// if err != nil {
	// 	fmt.Println(err.Error())
	// }
	// host, _ := nodemgmt.NewHost()
	// host.TestNetwork("16Uiu2HAkxLW724sE9WY1koVhHruVS9HqG6ZEjrFzqUpct4QSjMnY", []string{"/ip4/39.165.1.156/tcp/19003"})

	// nmgmt, err := nodemgmt.NewInstance("mongodb://127.0.0.1:27017", "http://152.136.17.115:8888")
	// if err != nil {
	// 	panic(err.Error())
	// }
	// n, err := nodemgmt.AllocRelayNode(nmgmt)
	// //nodes, err := nmgmt.AllocNodes(1)
	// if err != nil {
	// 	panic(err.Error())
	// }
	// fmt.Printf("$d\n", n.ID)
	// n := nodemgmt.Node{NodeID: "123", PubKey: "abc", Owner: "storename123", Addrs: []string{"/ip4/127.0.0.1/tcp/8888"}}
	// _, err = nmgmt.RegisterNode(&n)
	// if err != nil {
	// 	panic(err.Error())
	// }
	// fmt.Printf("%v\n", nn)
	// node.Reg
	// stat, err := nmgmt.Statistics()
	// if err != nil {
	// 	panic(err.Error())
	// }
	// fmt.Printf("%+v\n", stat)

	// nodes, err := node.AllocNodes(80)
	// if err != nil {
	// 	panic(err.Error())
	// }
	//fmt.Println(len(nodes))
}
