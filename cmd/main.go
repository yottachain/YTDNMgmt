package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"time"

	nodemgmt "github.com/yottachain/YTDNMgmt"
	pb "github.com/yottachain/YTDNMgmt/pb"
	"go.etcd.io/etcd/clientv3"
	"google.golang.org/grpc"
)

const NODEMGMT_ETCD_PREFIX = "/nodemgmt/"
const NODEMGMT_MONGOURL = NODEMGMT_ETCD_PREFIX + "mongoURL"
const NODEMGMT_EOSURL = NODEMGMT_ETCD_PREFIX + "eosURL"
const NODEMGMT_BPACCOUNT = NODEMGMT_ETCD_PREFIX + "bpAccount"
const NODEMGMT_BPPRIVKEY = NODEMGMT_ETCD_PREFIX + "bpPrivkey"
const NODEMGMT_CONTRACTOWNERM = NODEMGMT_ETCD_PREFIX + "contractOwnerM"
const NODEMGMT_CONTRACTOWNERD = NODEMGMT_ETCD_PREFIX + "contractOwnerD"
const NODEMGMT_SHADOWACCOUNT = NODEMGMT_ETCD_PREFIX + "shadowAccount"
const NODEMGMT_BPID = NODEMGMT_ETCD_PREFIX + "bpid"

func main() {
	var enablePprof bool = true
	var pprofPort int
	enablePprofStr := os.Getenv("P2PHOST_ENABLEPPROF")
	ep, err := strconv.ParseBool(enablePprofStr)
	if err != nil {
		enablePprof = false
	} else {
		enablePprof = ep
	}
	pprofPortStr := os.Getenv("P2PHOST_PPROFPORT")
	pp, err := strconv.Atoi(pprofPortStr)
	if err != nil {
		pprofPort = 6161
	} else {
		pprofPort = pp
	}
	if enablePprof {
		go func() {
			http.ListenAndServe(fmt.Sprintf("0.0.0.0:%d", pprofPort), nil)
		}()
		log.Printf("enable pprof server: 0.0.0.0:%d\n", pprofPort)
	}

	etcdHostname := os.Getenv("ETCDHOSTNAME")
	if etcdHostname == "" {
		etcdHostname = "etcd-svc"
	}
	etcdPortStr := os.Getenv("ETCDPORT")
	etcdPort, err := strconv.Atoi(etcdPortStr)
	if err != nil {
		etcdPort = 2379
	}
	log.Printf("ETCD URL: %s:%d\n", etcdHostname, etcdPort)
	clnt, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{fmt.Sprintf("%s:%d", etcdHostname, etcdPort)},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalln("connect etcd failed, err: ", err)
	}
	log.Println("connect etcd success")
	defer clnt.Close()

	for {
		time.Sleep(time.Second * 1)
		resp, err := clnt.Get(context.Background(), NODEMGMT_MONGOURL)
		if err != nil {
			log.Printf("get %s failed, err: %s\n", NODEMGMT_MONGOURL, err)
			continue
		}
		if len(resp.Kvs) == 0 {
			log.Printf("get %s failed, no content\n", NODEMGMT_MONGOURL)
			continue
		}
		mongoURL := resp.Kvs[0].Value
		log.Printf("Read mongodb URL from ETCD: %s\n", mongoURL)

		resp, err = clnt.Get(context.Background(), NODEMGMT_EOSURL)
		if err != nil {
			log.Printf("get %s failed, err: %s\n", NODEMGMT_EOSURL, err)
			continue
		}
		if len(resp.Kvs) == 0 {
			log.Printf("get %s failed, no content\n", NODEMGMT_EOSURL)
			continue
		}
		eosURL := resp.Kvs[0].Value
		log.Printf("Read EOS URL from ETCD: %s\n", eosURL)

		resp, err = clnt.Get(context.Background(), NODEMGMT_BPACCOUNT)
		if err != nil {
			log.Printf("get %s failed, err: %s\n", NODEMGMT_BPACCOUNT, err)
			continue
		}
		if len(resp.Kvs) == 0 {
			log.Printf("get %s failed, no content\n", NODEMGMT_BPACCOUNT)
			continue
		}
		bpAccount := resp.Kvs[0].Value
		log.Printf("Read BP account from ETCD: %s\n", bpAccount)

		resp, err = clnt.Get(context.Background(), NODEMGMT_BPPRIVKEY)
		if err != nil {
			log.Printf("get %s failed, err: %s\n", NODEMGMT_BPPRIVKEY, err)
			continue
		}
		if len(resp.Kvs) == 0 {
			log.Printf("get %s failed, no content\n", NODEMGMT_BPPRIVKEY)
			continue
		}
		bpPrivkey := resp.Kvs[0].Value
		log.Printf("Read BP private key from ETCD: %s\n", bpPrivkey)

		resp, err = clnt.Get(context.Background(), NODEMGMT_CONTRACTOWNERM)
		if err != nil {
			log.Printf("get %s failed, err: %s\n", NODEMGMT_CONTRACTOWNERM, err)
			continue
		}
		if len(resp.Kvs) == 0 {
			log.Printf("get %s failed, no content\n", NODEMGMT_CONTRACTOWNERM)
			continue
		}
		contractOwnerM := resp.Kvs[0].Value
		log.Printf("Read contract owner M from ETCD: %s\n", contractOwnerM)

		resp, err = clnt.Get(context.Background(), NODEMGMT_CONTRACTOWNERD)
		if err != nil {
			log.Printf("get %s failed, err: %s\n", NODEMGMT_CONTRACTOWNERD, err)
			continue
		}
		if len(resp.Kvs) == 0 {
			log.Printf("get %s failed, no content\n", NODEMGMT_CONTRACTOWNERD)
			continue
		}
		contractOwnerD := resp.Kvs[0].Value
		log.Printf("Read contract owner D from ETCD: %s\n", contractOwnerD)

		resp, err = clnt.Get(context.Background(), NODEMGMT_SHADOWACCOUNT)
		if err != nil {
			log.Printf("get %s failed, err: %s\n", NODEMGMT_SHADOWACCOUNT, err)
			continue
		}
		if len(resp.Kvs) == 0 {
			log.Printf("get %s failed, no content\n", NODEMGMT_SHADOWACCOUNT)
			continue
		}
		shadowAccount := resp.Kvs[0].Value
		log.Printf("Read shadow account from ETCD: %s\n", shadowAccount)

		resp, err = clnt.Get(context.Background(), NODEMGMT_BPID)
		if err != nil {
			log.Printf("get %s failed, err: %s\n", NODEMGMT_BPID, err)
			continue
		}
		if len(resp.Kvs) == 0 {
			log.Printf("get %s failed, no content\n", NODEMGMT_BPID)
			continue
		}
		bpidstr := resp.Kvs[0].Value
		bpid, err := strconv.Atoi(string(bpidstr))
		if err != nil {
			log.Printf("parse %s failed, err: %s\n", NODEMGMT_BPID, err)
			continue
		}
		log.Printf("Read BP ID from ETCD: %d\n", bpid)

		nodeDao, err := nodemgmt.NewInstance(string(mongoURL), string(eosURL), string(bpAccount), string(bpPrivkey), string(contractOwnerM), string(contractOwnerD), string(shadowAccount), int32(bpid))
		if err != nil {
			log.Fatalf("create nodemgmt instance failed, err: %s\n", err)
		}
		log.Printf("create nodemgmt instance successful.\n")
		server := &nodemgmt.Server{NodeService: nodeDao}

		nodemgmtPortStr := os.Getenv("NODEMGMT_GRPCPORT")
		nodemgmtPort, err := strconv.Atoi(nodemgmtPortStr)
		if err != nil {
			nodemgmtPort = 11001
		}
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", nodemgmtPort))
		if err != nil {
			log.Fatalf("failed to listen port %d: %v", nodemgmtPort, err)
		}
		log.Printf("GRPC address: 0.0.0.0:%d\n", nodemgmtPort)
		grpcServer := grpc.NewServer()
		pb.RegisterYTDNMgmtServer(grpcServer, server)
		grpcServer.Serve(lis)
		log.Printf("GRPC server started.")
		break
	}
}

func main1() {
	conn, err := grpc.Dial("127.0.0.1:1234", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewYTDNMgmtClient(conn)
	id, err := client.NewNodeID(context.Background(), &pb.Empty{})
	if err != nil {
		log.Fatalf("%s", err.Error())
	}
	fmt.Println("Got ID: ", id.Value)
}

func main2() {
	// port := 9002
	// privkey := "5HtM6e3mQNLEu2TkQ1ZrbMNpRQiHGsKxEsLdxd9VsdCmp1um8QH"
	// opts := []libp2p.Option{
	// 	libp2p.ListenAddrStrings(fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic", port)),
	// 	libp2p.NATPortMap(),
	// 	libp2p.Transport(quic.NewTransport),
	// }
	// if privkey != "" {
	// 	privbytes, err := base58.Decode(privkey)
	// 	if err != nil {
	// 		return errors.New("bad format of private key,Base58 format needed")
	// 	}
	// 	priv, err := crypto.UnmarshalSecp256k1PrivateKey(privbytes[1:33])
	// 	if err != nil {
	// 		return errors.New("bad format of private key")
	// 	}
	// 	opts = append(opts, libp2p.Identity(priv))
	// }
	// var err error
	// p2phst, err = libp2p.New(context.Background(), opts...)
	// if err != nil {
	// 	panic(err.Error())
	// }
	// time.Sleep(time.Duration(500) * time.Second)
	// nodeid := flag.String("nodeid", "16Uiu2HAm8jAW9tfocDdsqEV83Vni1L4bfc7X4oDm1N5A9HHhjwj5", "node id")
	// ip := flag.String("ip", "152.136.176.218", "ip address")
	// port := flag.String("port", "9999", "port number")

	// flag.Parse()

	// host, err := nodemgmt.NewHost()
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }
	// err = host.TestNetwork("16Uiu2HAmRpc3y8FBuDeEjcg8bDupP7BVWrJRfi9a56nfU7yCPk2c", []string{"/ip4/192.168.8.2/tcp/9001", "/ip4/125.125.94.125/tcp/9001"})
	// //err = host.TestNetwork(fmt.Sprintf("%s", *nodeid), []string{fmt.Sprintf("/ip4/%s/tcp/%s", *ip, *port)})
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }
	// fmt.Println("ok!")

	// skb, err := base58.Decode("4XZF1PwNDDCbbVQbmDnGy3Un4XCiGQtc2hHwJgt7RgD87cXxL")
	// fmt.Println(len(skb))
	// id, _ := nodemgmt.IdFromPublicKey("71e3SzWnnthKC4cVahNBUr5gdQqs2JFbiYMUyTvjqw6S5qBfgq")
	// fmt.Println(id)
	// rawjson := `{"signatures":["SIG_K1_JwPcE1LhEzNuXEZ6AYeZeKs4dYfqDijgeS8bUZjWZDCdMetkkFFem2tJzsmBDjZTKCSjzRPdbAPbJHtdv5KTBaXSSuJm2u"],"compression":"zlib","packed_context_free_data":"78da010000ffff00000001","packed_trx":"78dad2eb148995aa08396feacdc0c0c0c01870424031282a388b818121dc577de72c4606068970c5b92fd6826457bc35328aae61668002980484ee818ab24486388268237323cbacfc888ac48cf2bc6433a73c1fc78a22af00476713b7d4b0148fb05017f3749fcc8ce2e45c97e28c44439790b4b47c0640000000fffff0c32622"}`
	// nodeDao, _ := nodemgmt.NewInstance("mongodb://152.136.16.118:27017", "http://152.136.16.118:8888", "producer1", "5HtM6e3mQNLEu2TkQ1ZrbMNpRQiHGsKxEsLdxd9VsdCmp1um8QH", "hddpool12345", "hdddeposit12", 2)
	// err = nodeDao.PreRegisterNode(rawjson)
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }

	// etx, _ := eostx.NewInstance("http://152.136.17.115:8888", "producer1", "5HtM6e3mQNLEu2TkQ1ZrbMNpRQiHGsKxEsLdxd9VsdCmp1um8QH", "hddpool12345")
	// rate, err := etx.GetExchangeRate()
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }
	// fmt.Println(rate)

	// o := new(nodemgmt.Node)
	// o.NodeID = "16Uiu2HAmT2HyPoPBGSmc53G7uKsPtW9uvT4abQaafXFPstPTi6zv"
	// n := new(nodemgmt.Node)
	// n.ID = 730
	// n.NodeID = "16Uiu2HAmT2HyPoPBGSmc53G7uKsPtW9uvT4abQaafXFPstPTi6zv"
	// n.PubKey = "8TFNo4gqjKKd5yXoxaP6KgGnaGRr1ZoywX5Anw2KRNHF1mNcj5"
	// n.Owner = "usernamefang"
	// n.Addrs = []string{"/ip4/10.0.21.180/tcp/9001", "/ip4/36.110.28.94/tcp/9001", "/ip4/152.136.17.115/tcp/9999/p2p/16Uiu2HAm9fBJNUzSD5V9aFJQQHbxE3rPsTiyrYk7vju18JCf3xm8/p2p-circuit"}
	// _, err := nodeDao.AddrCheck(o, n)
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }

	// nodeDao, _ := nodemgmt.NewInstance("mongodb://152.136.18.185:27017", "http://152.136.16.118:8888", "username1234", "5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx", "hddpool12345", "hddpool12345", 2)
	// _ = nodeDao.Punish(10, 10000, 1, 100, false)
	// _ = nodeDao.CheckErrorNode(1880)
	// list, err := nodeDao.GetSpotCheckList()
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }
	// fmt.Printf("%d\n", len(list[0].TaskList))
	// nodes, err := nodeDao.GetSTNodes(3)
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }
	// fmt.Printf("%d\n", nodes[0].ID)
	// err = nodeDao.UpdateTaskStatus("5d2ef5335de23f9598466b9e", []int32{188, 215})
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }

	nodeDao, _ := nodemgmt.NewInstance("mongodb://122.152.203.189:27017", "http://152.136.18.185:8888", "username1234", "5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx", "hddpool12345", "hddpool12345", "producer1", 2)
	nodes, err := nodeDao.AllocNodes(320, nil)
	if err != nil {
		panic(err.Error())
	}
	fmt.Printf("%d nodes", len(nodes))
	// err := nodeDao.AddDNI(4, []byte{49, 50, 51, 52, 53, 54, 55})
	// err = nodeDao.AddDNI(4, []byte{52, 53, 54, 55})
	// err = nodeDao.AddDNI(4, []byte{56, 57, 58, 59})
	// err = nodeDao.AddDNI(4, []byte{41, 42, 43, 44, 45, 46, 47})
	//err = nodeDao.DeleteDNI(4, []byte{52, 53, 54, 55})
	// str, err := nodeDao.GetRandomVNI(4, 0)
	// fmt.Println(str)
	// list, err := nodeDao.GetInvalidNodes()
	// if err != nil {
	// 	panic(err.Error())
	// }
	// fmt.Printf("has %d", len(list))
	// n, s, err := nodeDao.GetRebuildItem(4, 0, 3)
	// if err != nil {
	// 	panic(err.Error())
	// }
	// fmt.Println(n.NodeID)
	// fmt.Printf("%d", len(s))
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

	//etx, _ := eostx.NewInstance("http://152.136.16.118:8888", "producer1", "5HtM6e3mQNLEu2TkQ1ZrbMNpRQiHGsKxEsLdxd9VsdCmp1um8QH", "hddpool12345")
	//etx.ExampleAPI_GetAccount("username1234")
	// data, err := etx.GetPledgeData(842)
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }
	// fmt.Println(data.AccountName, data.Deposit)
	// asset, err := eostx.GetPledgeAmount(data)
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }
	// asset.Amount = asset.Amount / 10
	// err = etx.DeducePledge(842, asset)
	// if err != nil {
	// 	log.Fatalln(err.Error())
	// }
	// err = etx.AddMiner("username1234", 99999)
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
