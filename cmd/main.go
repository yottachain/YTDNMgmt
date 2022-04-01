package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aurawing/eos-go"
	uuid "github.com/gofrs/uuid"
	"github.com/mr-tron/base58"
	ytcrypto "github.com/yottachain/YTCrypto"
	nodemgmt "github.com/yottachain/YTDNMgmt"
	"github.com/yottachain/YTDNMgmt/eostx"
	pb "github.com/yottachain/YTDNMgmt/pb"
	"go.etcd.io/etcd/clientv3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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
const NODEMGMT_MASTER = NODEMGMT_ETCD_PREFIX + "master"

func main1() {
	api := eos.New("http://47.93.13.197:8888")
	req := eos.GetTableRowsRequest{
		Code:       "hddpool12345",
		Scope:      "hddpool12345",
		Table:      "miner",
		LowerBound: fmt.Sprintf("%d", 22088),
		UpperBound: fmt.Sprintf("%d", 22088),
		Limit:      1,
		KeyType:    "i64",
		Index:      "1",
		JSON:       true,
	}
	resp, err := api.GetTableRows(req)
	if err != nil {
		panic(err)
	}
	if resp.More {
		panic(fmt.Errorf("more than one rows returned：get miner info 2"))
	}
	rows := make([]eostx.MinerInfo, 0)
	err = json.Unmarshal(resp.Rows, &rows)
	if err != nil {
		panic(err)
	}
	if len(rows) == 0 {
		panic(fmt.Errorf("no matched row found, minerid: %s", req.Scope))
	}
	fmt.Printf("%+v\n", &rows[0])
}

func main0() {
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

		resp, err = clnt.Get(context.Background(), NODEMGMT_MASTER)
		if err != nil {
			log.Printf("get %s failed, err: %s\n", NODEMGMT_MASTER, err)
			continue
		}
		if len(resp.Kvs) == 0 {
			log.Printf("get %s failed, no content\n", NODEMGMT_MASTER)
			continue
		}
		masterstr := resp.Kvs[0].Value
		master, err := strconv.Atoi(string(masterstr))
		if err != nil {
			log.Printf("parse %s failed, err: %s\n", NODEMGMT_MASTER, err)
			continue
		}
		log.Printf("Read master status from ETCD: %d\n", master)

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

		nodeDao, err := nodemgmt.NewInstance(string(mongoURL), string(eosURL), string(bpAccount), string(bpPrivkey), string(contractOwnerM), string(contractOwnerD), string(shadowAccount), int32(bpid), int32(master), nil)
		if err != nil {
			log.Fatalf("create nodemgmt instance failed, err: %s\n", err)
		}
		log.Printf("create nodemgmt instance successful\n")
		server := &nodemgmt.Server{NodeService: nodeDao}

		nodemgmtPortStr := os.Getenv("NODEMGMT_GRPCPORT")
		nodemgmtPort, err := strconv.Atoi(nodemgmtPortStr)
		if err != nil {
			nodemgmtPort = 11001
		}
		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", nodemgmtPort))
		if err != nil {
			log.Fatalf("failed to listen port %d: %v\n", nodemgmtPort, err)
		}
		log.Printf("GRPC address: 0.0.0.0:%d\n", nodemgmtPort)
		grpcServer := grpc.NewServer()
		pb.RegisterYTDNMgmtServer(grpcServer, server)
		grpcServer.Serve(lis)
		log.Printf("GRPC server started\n")
		break
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	var id int
	var sk string
	var apiUrl string
	var err error
	fmt.Print("请输入矿机ID：")
	idstr, _ := reader.ReadString('\n')
	idstr = RemoveBlank(idstr)
	if idstr == "" {
		fmt.Println("矿机ID不可为空！")
		return
	}
	id, err = strconv.Atoi(idstr)
	if err != nil {
		fmt.Printf("发生错误：%s\n", err.Error())
		return
	}
	fmt.Print("请输入矿机所有者私钥：")
	sk, _ = reader.ReadString('\n')
	sk = RemoveBlank(sk)
	fmt.Print("是否需要修改API入口地址（默认值为http://sn.yottachain.net:8082/NodeQuit）,不需修改请直接回车：")
	apiUrl, _ = reader.ReadString('\n')
	apiUrl = RemoveBlank(apiUrl)
	if apiUrl == "" {
		apiUrl = "http://sn.yottachain.net:8082/NodeQuit"
	}
	u := uuid.Must(uuid.NewV1())
	nonce := u.String()
	sig, err := ytcrypto.Sign(sk, []byte(fmt.Sprintf("%d&%s", id, nonce)))
	if err != nil {
		fmt.Printf("发生错误：%s\n", err.Error())
		return
	}
	resp, err := http.PostForm(fmt.Sprintf("%s?nodeID=%d&nonce=%s&signature=%s", apiUrl, id, nonce, sig), url.Values{})
	if err != nil {
		fmt.Printf("发生错误：%s\n", err.Error())
		return
	}
	defer resp.Body.Close()
	//返回200表示成功
	if resp.StatusCode != 200 {
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			fmt.Printf("发生错误：%s\n", err.Error())
			return
		}
		fmt.Printf("发生错误: %s\n", body)
		return
	}
	fmt.Println("已将当前矿机加入退出队列，待矿机的Status变为99时表示退出操作完成")

	// fmt.Println(uuid.Must(uuid.NewV1()).String())
	// //获取矿机ID
	// idstr := os.Args[2]
	// id, err := strconv.Atoi(idstr)
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("nodeID: %d\n", id)
	// //生成UUIDv1
	// u := uuid.Must(uuid.NewV1())
	// nonce := u.String()
	// fmt.Printf("nonce: %s\n", nonce)
	// //使用矿机所有者私钥对“矿机ID&UUIDv1”字符串进行签名
	// sig, err := ytcrypto.Sign(os.Args[3], []byte(fmt.Sprintf("%d&%s", id, nonce)))
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("signature: %s\n", sig)
	// //通过http接口将各参数发给SN
	// resp, err := http.PostForm(fmt.Sprintf("%s?nodeID=%d&nonce=%s&signature=%s", os.Args[1], id, nonce, sig), url.Values{})
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Printf("Status: %d\n", resp.StatusCode)
	// defer resp.Body.Close()
	// //返回200表示成功
	// if resp.StatusCode != 200 {
	// 	body, err := ioutil.ReadAll(resp.Body)
	// 	if err != nil {
	// 		panic(err)
	// 	}
	// 	fmt.Printf("Error: %s\n", body)
	// }
}

func RemoveBlank(str string) string {
	str = strings.Replace(str, " ", "", -1)
	str = strings.Replace(str, "\n", "", -1)
	str = strings.Replace(str, "\r", "", -1)
	return str
}

var lock sync.RWMutex

func main3() {
	mongoCli, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://admin:SL*GAy6xL87@192.168.8.9:26078"))
	if err != nil {
		panic(err)
	}
	shardMap, err := refreshShardMap(mongoCli)
	if err != nil {
		panic(err)
	}
	go func() {
		for {
			time.Sleep(time.Duration(10) * time.Minute)
			sm, err := refreshShardMap(mongoCli)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			lock.Lock()
			shardMap = sm
			lock.Unlock()
		}
	}()
	mux := http.NewServeMux()
	mux.HandleFunc("/node_shards", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		mineridstr := r.Form.Get("minerid")
		if mineridstr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, "矿机ID不存在！\n")
			return
		}
		minerid, err := strconv.Atoi(mineridstr)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, fmt.Sprintf("解析矿机ID失败：%s\n", err.Error()))
			return
		}
		lock.RLock()
		defer lock.RUnlock()
		m := shardMap[int32(minerid)]
		data, err := json.Marshal(m)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, fmt.Sprintf("序列化分片哈希失败：%s\n", err.Error()))
			return
		}
		io.WriteString(w, string(data))
	})
	server := &http.Server{
		Addr:    ":22222",
		Handler: mux,
	}
	server.ListenAndServe()
}

func refreshShardMap(mongoCli *mongo.Client) (map[int32][]string, error) {
	fmt.Printf("%s: refreshing shard map...\n", time.Now().String())

	shardMap := make(map[int32][]string)
	collection := mongoCli.Database("metabase").Collection("shards")
	cur, err := collection.Find(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		shard := new(Shard)
		err := cur.Decode(shard)
		if err != nil {
			panic(err)
		}
		if shard.NodeID != 0 {
			shardMap[shard.NodeID] = append(shardMap[shard.NodeID], base58.Encode(shard.VHF))
		}
		if shard.NodeID2 != 0 {
			shardMap[shard.NodeID2] = append(shardMap[shard.NodeID2], base58.Encode(shard.VHF))
		}
	}
	fmt.Printf("%s: refreshing shard map finished, total %d miners\n", time.Now().String(), len(shardMap))
	return shardMap, err
}

type Shard struct {
	ID      int64  `bson:"_id"`
	NodeID  int32  `bson:"nodeId"`
	VHF     []byte `bson:"VHF"`
	NodeID2 int32  `bson:"nodeId2"`
}
