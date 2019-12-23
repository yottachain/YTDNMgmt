package YTDNMgmt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/mr-tron/base58"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/yottachain/YTDNMgmt/eostx"
)

//NodeDaoImpl node manipulator implemention
type NodeDaoImpl struct {
	client *mongo.Client
	eostx  *eostx.EosTX
	host   *Host
	ns     *NodesSelector
	bpID   int32
}

var incr int64 = 0
var index int32 = -1

//NewInstance create a new instance of NodeDaoImpl
func NewInstance(mongoURL, eosURL, bpAccount, bpPrivkey, contractOwnerM, contractOwnerD, shadowAccount string, bpID int32) (*NodeDaoImpl, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		log.Printf("nodemgmt: NewInstance: error when creating mongodb client: %s %s\n", mongoURL, err.Error())
		return nil, err
	}
	log.Printf("nodemgmt: NewInstance: create mongodb client: %s\n", mongoURL)
	etx, err := eostx.NewInstance(eosURL, bpAccount, bpPrivkey, contractOwnerM, contractOwnerD, shadowAccount)
	if err != nil {
		log.Printf("nodemgmt: NewInstance: error when creating eos client failed: %s %s\n", eosURL, err.Error())
		return nil, err
	}
	log.Printf("nodemgmt: NewInstance: create eos client: %s\n", eosURL)
	host, err := NewHost()
	if err != nil {
		log.Printf("nodemgmt: NewInstance: error when creating host failed: %s\n", err.Error())
		return nil, err
	}
	log.Println("nodemgmt: NewInstance: create host")
	dao := &NodeDaoImpl{client: client, eostx: etx, host: host, ns: &NodesSelector{}, bpID: bpID}
	dao.StartRecheck()
	dao.ns.Start(context.Background(), dao)
	if incr == 0 {
		collection := client.Database(YottaDB).Collection(SuperNodeTab)
		c, err := collection.CountDocuments(context.Background(), bson.D{})
		if err != nil {
			log.Printf("nodemgmt: NewInstance: error when calculating count of supernode failed: %s\n", err.Error())
			return nil, err
		}
		log.Printf("nodemgmt: NewInstance: count of supernode: %d\n", c)
		incr = c
	}
	if index == -1 {
		index = bpID
		log.Printf("nodemgmt: NewInstance: index of SN: %d\n", index)
	}
	return dao, nil
}

func getCurrentNodeIndex(client *mongo.Client) (int32, error) {
	collection := client.Database(YottaDB).Collection(SequenceTab)
	m := make(map[string]int32)
	err := collection.FindOne(context.Background(), bson.M{"_id": NodeIdxType}).Decode(&m)
	if err != nil {
		log.Printf("nodemgmt: getCurrentNodeIndex: index of SN: %d\n", index)
		return 0, err
	}
	return m["seq"], nil
}

//NewNodeID get newest id of node
func (self *NodeDaoImpl) NewNodeID() (int32, error) {
	collection := self.client.Database(YottaDB).Collection(SequenceTab)
	_, err := collection.InsertOne(context.Background(), bson.M{"_id": NodeIdxType, "seq": index})
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			log.Printf("nodemgmt: NewNodeID: error when calculating new node ID: %s\n", err.Error())
			return 0, err
		}
	}
	m := make(map[string]int32)
	err = collection.FindOne(context.Background(), bson.M{"_id": NodeIdxType}).Decode(&m)
	if err != nil {
		log.Printf("nodemgmt: NewNodeID: error when decoding sequence: %d %s\n", NodeIdxType, err.Error())
		return 0, err
	}
	return m["seq"] + int32(incr), nil
}

//PreRegisterNode register node on chain and pledge YTA for assignable space
func (self *NodeDaoImpl) PreRegisterNode(trx string) error {
	rate, err := self.eostx.GetExchangeRate()
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when fetching exchange rate from BP: %s\n", err.Error())
		return err
	}
	signedTrx, regData, err := self.eostx.PreRegisterTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when sending sign transaction: %s\n", err.Error())
		return err
	}
	pledgeAmount := int64(regData.DepAmount.Amount)
	adminAccount := string(regData.Owner)
	profitAccount := string(regData.Owner)
	minerID := int32(regData.MinerID)
	pubkey := regData.Extra
	if adminAccount == "" || profitAccount == "" || minerID == 0 || minerID%int32(incr) != index || pubkey == "" {
		log.Printf("nodemgmt: PreRegisterNode: error when parsing parameters from raw transaction: %s\n", "please check adminAccount, profitAccount, minerID, public key and owner of currect node")
		return errors.New("bad parameters in regminer transaction")
	}
	nodeid, err := IDFromPublicKey(pubkey)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when generating ID from public key: %s\n", err.Error())
		return err
	}
	currID, err := self.NewNodeID()
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when calculating new node ID: %s\n", err.Error())
		return err
	}
	if currID != minerID {
		log.Printf("nodemgmt: PreRegisterNode: error: %s\n", "current ID is not equal to minerID")
		return errors.New("node ID is invalid, please retry")
	}
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when sending transaction: %s\n", err.Error())
		return err
	}
	collection := self.client.Database(YottaDB).Collection(SequenceTab)
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": NodeIdxType}, bson.M{"$set": bson.M{"seq": minerID}})
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when updating sequence of Node: %d %s\n", minerID, err.Error())
		return err
	}
	node := new(Node)
	node.ID = minerID
	node.NodeID = nodeid
	node.PubKey = pubkey
	node.Owner = adminAccount
	node.ProfitAcc = profitAccount
	node.PoolID = ""
	node.Quota = 0
	node.AssignedSpace = pledgeAmount * 65536 * int64(rate) / 1000000 //TODO: 1YTA=1G 后需调整
	node.Valid = 0
	node.Relay = 0
	node.Status = 0
	node.Timestamp = time.Now().Unix()
	node.Version = 0
	collection = self.client.Database(YottaDB).Collection(NodeTab)
	_, err = collection.InsertOne(context.Background(), node)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when inserting node to database: %d %s\n", minerID, err.Error())
		return err
	}
	log.Printf("nodemgmt: PreRegisterNode: new node registered: ID->%d, noeID->%s, pubkey->%s, owner->%s, proficAcc->%s, assignedSpace->%d\n", node.ID, node.NodeID, node.PubKey, node.Owner, node.ProfitAcc, node.AssignedSpace)
	return nil
}

//ChangeMinerPool add miner to a pool
func (self *NodeDaoImpl) ChangeMinerPool(trx string) error {
	signedTrx, poolData, err := self.eostx.ChangMinerPoolTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when signing raw transaction: %s\n", err.Error())
		return err
	}
	minerID := int32(poolData.MinerID)
	poolID := string(poolData.PoolID)
	minerProfit := string(poolData.MinerProfit)
	quota := poolData.MaxSpace
	if minerID%int32(incr) != index {
		log.Printf("nodemgmt: ChangeMinerPool: minerID %d is not belong to SN%d\n", minerID, index)
		return errors.New("transaction sends to incorrect super node")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	node := new(Node)
	err = collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(node)
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when decoding node: %d %s\n", minerID, err.Error())
		return err
	}
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when sending transaction: %s\n", err.Error())
		return err
	}
	var afterReg bool = false
	if node.Status == 0 && node.ProductiveSpace == 0 {
		afterReg = true
	}
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": minerID}, bson.M{"$set": bson.M{"poolID": poolID, "profitAcc": minerProfit, "quota": quota}})
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when updating poolID->%s, profitAcc->%s, quota->%d: %s\n", poolID, minerProfit, quota, err.Error())
		return err
	}
	if afterReg {
		log.Printf("node %d is not added to pool %s for first time\n", minerID, poolID)
		err = collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(node)
		if err != nil {
			log.Printf("nodemgmt: ChangeMinerPool: error when decoding node: %d %s\n", minerID, err.Error())
			return err
		}
		assignable := Min(node.AssignedSpace, node.Quota)
		if assignable <= 0 {
			log.Printf("nodemgmt: ChangeMinerPool: warning: assignable space is %d\n", assignable)
			return fmt.Errorf("assignable space is %d", assignable)
		}
		if assignable >= 65536 {
			assignable = 65536
		}
		err := self.IncrProductiveSpace(node.ID, assignable)
		if err != nil {
			log.Printf("nodemgmt: ChangeMinerPool: error when increasing assignable space: %s\n", err.Error())
			return err
		}
		log.Printf("nodemgmt: ChangeMinerPool: increased assignable space %d\n", assignable)
		err = self.eostx.AddSpace(node.ProfitAcc, uint64(node.ID), uint64(assignable))
		if err != nil {
			log.Printf("nodemgmt: ChangeMinerPool: error when adding assignable space on BP: %s\n", err.Error())
			self.IncrProductiveSpace(node.ID, -1*assignable)
			return err
		}
		log.Printf("nodemgmt: ChangeMinerPool: added assignable space on BP %d\n", assignable)
	}
	log.Printf("nodemgmt: ChangeMinerPool: node %d is added to pool %s\n", minerID, poolID)
	return nil
}

//RegisterNode create a new data node
func (self *NodeDaoImpl) RegisterNode(node *Node) (*Node, error) {
	return nil, errors.New("no use")
}

//UpdateNode update data info by data node status
func (self *NodeDaoImpl) UpdateNodeStatus(node *Node) (*Node, error) {
	if node == nil {
		log.Println("nodemgmt: UpdateNodeStatus: warning: report info is null")
		return nil, errors.New("node is null")
	}
	if node.ID == 0 {
		log.Println("nodemgmt: UpdateNodeStatus: warning: node ID cannot be 0")
		return nil, errors.New("node ID cannot be 0")
	}
	if node.ID%int32(incr) != index {
		log.Printf("nodemgmt: UpdateNodeStatus: warning: node %d do not belong to current SN\n", node.ID)
		return nil, errors.New("node do not belong to this SN")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	n := new(Node)
	err := collection.FindOne(context.Background(), bson.M{"_id": node.ID}).Decode(n)
	if err != nil {
		log.Printf("nodemgmt: UpdateNodeStatus: error when decoding node %d: %s\n", node.ID, err.Error())
		return nil, err
	}
	if time.Now().Unix()-n.Timestamp < 50 {
		log.Printf("nodemgmt: UpdateNodeStatus: warning: reporting of node %d is too frequency\n", n.ID)
		return nil, errors.New("reporting is too frequency")
	}
	if n.Quota == 0 || n.AssignedSpace == 0 {
		log.Printf("nodemgmt: UpdateNodeStatus: warning: node %d has not been pledged or added to a pool\n", n.ID)
		//return nil, fmt.Errorf("node %d has not been pledged or added to a pool", n.ID)
	}
	node.Valid = n.Valid
	node.Addrs = CheckPublicAddrs(node.Addrs)
	relayURL, err := self.AddrCheck(n, node)
	if err != nil {
		log.Printf("nodemgmt: UpdateNodeStatus: error when checking public address of node %d: %s\n", n.ID, err.Error())
		return nil, err
	}
	var status int32 = 1
	if n.Status > 1 {
		status = n.Status
	}
	// weight := float64(Min(n.AssignedSpace, n.Quota, node.MaxDataSpace) - n.UsedSpace)
	weight := n.AssignedSpace
	if weight < 0 {
		weight = 0
	}
	opts := new(options.FindOneAndUpdateOptions)
	opts = opts.SetReturnDocument(options.After)
	timestamp := time.Now().Unix()
	update := bson.M{"$set": bson.M{"cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "addrs": node.Addrs, "weight": weight, "valid": node.Valid, "relay": node.Relay, "status": status, "timestamp": timestamp, "version": node.Version}}
	if node.UsedSpace != 0 {
		update = bson.M{"$set": bson.M{"cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "realSpace": node.UsedSpace, "addrs": node.Addrs, "weight": weight, "valid": node.Valid, "relay": node.Relay, "status": status, "timestamp": timestamp, "version": node.Version}}
	}
	result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": node.ID}, update, opts)
	err = result.Decode(n)
	if err != nil {
		log.Printf("nodemgmt: UpdateNodeStatus: error when decoding node %d: %s\n", n.ID, err.Error())
		return nil, err
	}
	if relayURL != "" {
		log.Printf("nodemgmt: UpdateNodeStatus: allocated relay URL for node %d: %s\n", n.ID, relayURL)
		n.Addrs = []string{relayURL}
	} else {
		n.Addrs = nil
	}

	if n.UsedSpace+prePurphaseThreshold > n.ProductiveSpace {
		assignable := Min(n.AssignedSpace, n.Quota, n.MaxDataSpace) - n.ProductiveSpace
		if assignable <= 0 {
			log.Printf("nodemgmt: UpdateNodeStatus: warning: node %d has no left space for allocating\n", n.ID)
		} else {
			if assignable >= prePurphaseAmount {
				assignable = prePurphaseAmount
			}
			err = self.IncrProductiveSpace(node.ID, assignable)
			if err != nil {
				log.Printf("nodemgmt: UpdateNodeStatus: error when increasing productive space for node %d: %s\n", n.ID, err.Error())
				return nil, err
			}
			err = self.eostx.AddSpace(node.ProfitAcc, uint64(node.ID), uint64(assignable))
			if err != nil {
				log.Printf("nodemgmt: UpdateNodeStatus: error when adding space for node %d: %s\n", n.ID, err.Error())
				self.IncrProductiveSpace(node.ID, -1*assignable)
				return nil, err
			}
			n.ProductiveSpace += assignable
			log.Printf("nodemgmt: UpdateNodeStatus: pre-purchase productive space of node %d: %d\n", n.ID, assignable)
		}
	}
	return n, nil
}

//IncrUsedSpace increase user space of one node
func (self *NodeDaoImpl) IncrUsedSpace(id int32, incr int64) error {
	if incr < 0 {
		return errors.New("incremental space cannot be minus")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	n := new(Node)
	err := collection.FindOne(context.Background(), bson.M{"_id": id}).Decode(n)
	if err != nil {
		return err
	}
	weight := float64(Min(n.AssignedSpace, n.Quota, n.MaxDataSpace) - n.UsedSpace)
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": bson.M{"weight": weight}, "$inc": bson.M{"usedSpace": incr}})
	return err
}

//IncrProductiveSpace increase productive space of one node
func (self *NodeDaoImpl) IncrProductiveSpace(id int32, incr int64) error {
	if incr < 0 {
		return errors.New("incremental space cannot be minus")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err := collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$inc": bson.M{"productiveSpace": incr}})
	return err
}

//AllocNodes by shard count
func (self *NodeDaoImpl) AllocNodes(shardCount int32, errids []int32) ([]*Node, error) {
	return self.ns.Alloc(shardCount, errids)
}

//SyncNode sync node info to other SN
func (self *NodeDaoImpl) SyncNode(node *Node) error {
	if node.ID == 0 {
		log.Println("nodemgmt: SyncNode: warning: node ID cannot be 0")
		return errors.New("node ID cannot be 0")
	}
	if node.ID%int32(incr) == index {
		return nil
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err := collection.InsertOne(context.Background(), node)
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			log.Printf("nodemgmt: SyncNode: error when inserting node %d to database: %s\n", node.ID, err.Error())
			return err
		} else {
			_, err := collection.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version}})
			if err != nil {
				log.Printf("nodemgmt: SyncNode: error when updating record of node %d in database: %s\n", node.ID, err.Error())
				return err
			}
		}
	}
	return nil
}

//GetNodes by node IDs
func (self *NodeDaoImpl) GetNodes(nodeIDs []int32) ([]*Node, error) {
	cond := bson.A{}
	for _, id := range nodeIDs {
		cond = append(cond, bson.D{{"_id", id}})
	}
	nodes := make([]*Node, 0)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.D{{"$or", cond}})
	if err != nil {
		log.Printf("nodemgmt: GetNodes: error when finding nodes %v in database: %s\n", nodeIDs, err.Error())
		return nil, err
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: GetNodes: error when decoding nodes: %s\n", err.Error())
			return nil, err
		}
		nodes = append(nodes, result)
	}
	return nodes, nil
}

//GetSuperNodes get all super nodes
func (self *NodeDaoImpl) GetSuperNodes() ([]*SuperNode, error) {
	supernodes := make([]*SuperNode, 0)
	collection := self.client.Database(YottaDB).Collection(SuperNodeTab)
	cur, err := collection.Find(context.Background(), bson.D{})
	if err != nil {
		log.Printf("nodemgmt: GetSuperNodes: error when finding all supernodes in database: %s\n", err.Error())
		return nil, err
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		result := new(SuperNode)
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: GetSuperNodes: error when decoding supernodes: %s\n", err.Error())
			return nil, err
		}
		supernodes = append(supernodes, result)
	}
	return supernodes, nil
}

//GetSuperNodePrivateKey get private key of super node with certain ID
func (self *NodeDaoImpl) GetSuperNodeByID(id int32) (*SuperNode, error) {
	collection := self.client.Database(YottaDB).Collection(SuperNodeTab)
	supernode := new(SuperNode)
	err := collection.FindOne(context.Background(), bson.D{{"_id", id}}).Decode(supernode)
	if err != nil {
		log.Printf("nodemgmt: GetSuperNodes: error when finding supernode by ID: %d %s\n", id, err.Error())
		return nil, err
	}
	return supernode, nil
}

//GetSuperNodePrivateKey get private key of super node with certain ID
func (self *NodeDaoImpl) GetSuperNodePrivateKey(id int32) (string, error) {
	collection := self.client.Database(YottaDB).Collection(SuperNodeTab)
	cur, err := collection.Find(context.Background(), bson.D{{"_id", id}})
	if err != nil {
		log.Printf("nodemgmt: GetSuperNodePrivateKey: error when finding supernode by ID: %d %s\n", id, err.Error())
		return "", err
	}
	defer cur.Close(context.Background())
	result := new(SuperNode)
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: GetSuperNodePrivateKey: error when decoding supernode by ID: %d %s\n", id, err.Error())
			return "", err
		}
		return result.PrivKey, nil
	}
	return "", errors.New("no result")
}

//GetNodeIDByPubKey get node ID by public key
func (self *NodeDaoImpl) GetNodeIDByPubKey(pubkey string) (int32, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.D{{"pubkey", pubkey}})
	if err != nil {
		log.Printf("nodemgmt: GetNodeIDByPubKey: error when finding node by pubkey %s: %s\n", pubkey, err.Error())
		return 0, err
	}
	defer cur.Close(context.Background())
	result := new(Node)
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: GetNodeIDByPubKey: error when decoding node by pubkey %s: %s\n", pubkey, err.Error())
			return 0, err
		}
		return result.ID, nil
	}
	return 0, errors.New("no result")
}

//GetNodeByPubKey get node by public key
func (self *NodeDaoImpl) GetNodeByPubKey(pubkey string) (*Node, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.D{{"pubkey", pubkey}})
	if err != nil {
		log.Printf("nodemgmt: GetNodeByPubKey: error when finding node by pubkey %s: %s\n", pubkey, err.Error())
		return nil, err
	}
	defer cur.Close(context.Background())
	result := new(Node)
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: GetNodeByPubKey: error when decoding node by pubkey %s: %s\n", pubkey, err.Error())
			return nil, err
		}
		return result, nil
	}
	return nil, errors.New("No result")
}

//GetSuperNodeIDByPubKey get super node ID by public key
func (self *NodeDaoImpl) GetSuperNodeIDByPubKey(pubkey string) (int32, error) {
	collection := self.client.Database(YottaDB).Collection(SuperNodeTab)
	cur, err := collection.Find(context.Background(), bson.D{{"pubkey", pubkey}})
	if err != nil {
		log.Printf("nodemgmt: GetSuperNodeIDByPubKey: error when finding supernode by pubkey %s: %s\n", pubkey, err.Error())
		return 0, err
	}
	defer cur.Close(context.Background())
	result := new(SuperNode)
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: GetSuperNodeIDByPubKey: error when decoding supernode by pubkey %s: %s\n", pubkey, err.Error())
			return 0, err
		}
		return result.ID, nil
	}
	return 0, errors.New("no result")
}

//AddDNI add digest of one shard
func (self *NodeDaoImpl) AddDNI(id int32, shard []byte) error {
	collection := self.client.Database(YottaDB).Collection(DNITab)
	oid := primitive.NewObjectID()
	_, err := collection.InsertOne(context.Background(), bson.M{"_id": oid, "shard": shard, "minerID": id, "delete": 0})
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			log.Printf("nodemgmt: AddDNI: error when inserting DNI of node %d to database: %s %s\n", id, base58.Encode(shard), err.Error())
			return err
		} else {
			return nil
		}
	} else {
		collectionNode := self.client.Database(YottaDB).Collection(NodeTab)
		_, err = collectionNode.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$inc": bson.M{"usedSpace": 1}})
		if err != nil {
			log.Printf("nodemgmt: AddDNI: error when updating used space of node %d: %s\n", id, err.Error())
			collection.DeleteOne(context.Background(), bson.M{"_id": oid})
			return err
		}
		return nil
	}
}

//ActiveNodesList show id and public IP of all active data nodes
func (self *NodeDaoImpl) ActiveNodesList() ([]*Node, error) {
	nodes := make([]*Node, 0)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.M{"valid": 1, "status": 1, "assignedSpace": bson.M{"$gt": 0}, "quota": bson.M{"$gt": 0}, "cpu": bson.M{"$lt": 98}, "memory": bson.M{"$lt": 95}, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*avaliableNodeTimeGap}, "weight": bson.M{"$gt": 65536}, "version": bson.M{"$gte": minerVersionThreadshold}})
	if err != nil {
		log.Printf("nodemgmt: ActiveNodesList: error when finding active nodes list: %s\n", err.Error())
		return nil, err
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: ActiveNodesList: error when decoding nodes: %s\n", err.Error())
			return nil, err
		}
		result.Addrs = []string{CheckPublicAddr(result.Addrs)}
		nodes = append(nodes, result)
	}
	return nodes, nil
}

//Statistics of data nodes
func (self *NodeDaoImpl) Statistics() (*NodeStat, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	active, err := collection.CountDocuments(context.Background(), bson.M{"valid": 1, "status": 1, "assignedSpace": bson.M{"$gt": 0}, "quota": bson.M{"$gt": 0}, "cpu": bson.M{"$lt": 98}, "memory": bson.M{"$lt": 95}, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*avaliableNodeTimeGap}, "weight": bson.M{"$gt": 65536}, "version": bson.M{"$gte": minerVersionThreadshold}})
	if err != nil {
		log.Printf("nodemgmt: Statistics: error when counting active nodes: %s\n", err.Error())
		return nil, err
	}
	total, err := collection.CountDocuments(context.Background(), bson.M{})
	if err != nil {
		log.Printf("nodemgmt: Statistics: error when counting all nodes: %s\n", err.Error())
		return nil, err
	}
	pipeline := mongo.Pipeline{
		{{"$project", bson.D{{"maxDataSpace", 1}, {"assignedSpace", 1}, {"productiveSpace", 1}, {"usedSpace", 1}, {"_id", 0}}}},
		{{"$group", bson.D{{"_id", ""}, {"maxTotal", bson.D{{"$sum", "$maxDataSpace"}}}, {"assignedTotal", bson.D{{"$sum", "$assignedSpace"}}}, {"productiveTotal", bson.D{{"$sum", "$productiveSpace"}}}, {"usedTotal", bson.D{{"$sum", "$usedSpace"}}}}}},
	}
	cur, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Printf("nodemgmt: Statistics: error when calculating space statistics: %s\n", err.Error())
		return nil, err
	}
	result := new(NodeStat)
	defer cur.Close(context.Background())
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: Statistics: error when decoding space statistics: %s\n", err.Error())
			return nil, err
		}
	}
	result.ActiveMiners = active
	result.TotalMiners = total
	return result, nil
}
