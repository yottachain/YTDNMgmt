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

type TestStruct struct {
	ID        int32            `bson:"_id"`
	Uspace    map[string]int64 `bson:"uspace"`
	Timestamp int64            `bson:"timestamp"`
}

func main() {
	cli, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://127.0.0.1:27017/?connect=direct"))
	if err != nil {
		panic(err)
	}
	collection := cli.Database("test").Collection("Test")
	_, err = collection.InsertOne(context.Background(), &TestStruct{ID: 1, Uspace: map[string]int64{"sn0": 18818, "sn1": 8173}, Timestamp: time.Now().Unix()})
	if err != nil {
		panic(err)
	}
}
