package YTDNMgmt

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/yottachain/YTDNMgmt/eostx"
)

type NodeDaoImpl struct {
	client *mongo.Client
	eostx  *eostx.EosTX
	host   *Host
	bpID   int32
}

var incr int64 = 0
var index int32 = -1

//NewInstance create a new instance of NodeDaoImpl
func NewInstance(mongoURL, eosURL, bpAccount, bpPrivkey, contractOwnerM, contractOwnerD string, bpID int32) (*NodeDaoImpl, error) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		return nil, err
	}
	etx, err := eostx.NewInstance(eosURL, bpAccount, bpPrivkey, contractOwnerM, contractOwnerD)
	if err != nil {
		return nil, err
	}
	host, err := NewHost()
	if err != nil {
		return nil, err
	}
	dao := &NodeDaoImpl{client: client, eostx: etx, host: host, bpID: bpID}
	if incr == 0 {
		collection := client.Database(YottaDB).Collection(SuperNodeTab)
		c, err := collection.CountDocuments(context.Background(), bson.D{})
		if err != nil {
			return nil, err
		}
		incr = c
	}
	if index == -1 {
		idx, err := getCurrentSuperNodeIndex(client)
		if err != nil {
			return nil, err
		}
		index = idx
	}
	return dao, nil
}

func getCurrentSuperNodeIndex(client *mongo.Client) (int32, error) {
	collection := client.Database(YottaDB).Collection(SequenceTab)
	m := make(map[string]int32)
	err := collection.FindOne(context.Background(), bson.M{"_id": SuperNodeIdxType}).Decode(&m)
	if err != nil {
		return 0, err
	}
	return m["seq"], nil
}

// // generate a new id for Node collection
// func newID(client *mongo.Client) (int32, error) {
// 	if incr == 0 {
// 		collection := client.Database(YottaDB).Collection(SuperNodeTab)
// 		c, err := collection.CountDocuments(context.Background(), bson.D{})
// 		if err != nil {
// 			return 0, err
// 		}
// 		incr = c
// 	}
// 	collection := client.Database(YottaDB).Collection(SequenceTab)
// 	initseq, err := getCurrentSuperNodeIndex(client)
// 	if err != nil {
// 		return 0, err
// 	}
// 	_, err = collection.InsertOne(context.Background(), bson.M{"_id": NodeIdxType, "seq": initseq})
// 	if err != nil {
// 		errstr := err.Error()
// 		if !strings.ContainsAny(errstr, "duplicate key error") {
// 			return 0, err
// 		}
// 	}
// 	opts := new(options.FindOneAndUpdateOptions)
// 	opts = opts.SetReturnDocument(options.After)
// 	result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": NodeIdxType}, bson.M{"$inc": bson.M{"seq": incr}}, opts)
// 	m := make(map[string]int32)
// 	err = result.Decode(&m)
// 	if err != nil {
// 		return 0, err
// 	}
// 	return m["seq"], nil
// }

//NewNodeID get newest id of node
func (self *NodeDaoImpl) NewNodeID() (int32, error) {
	collection := self.client.Database(YottaDB).Collection(SequenceTab)
	_, err := collection.InsertOne(context.Background(), bson.M{"_id": NodeIdxType, "seq": index})
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			return 0, err
		}
	}
	m := make(map[string]int32)
	err = collection.FindOne(context.Background(), bson.M{"_id": NodeIdxType}).Decode(&m)
	if err != nil {
		return 0, err
	}
	return m["seq"] + int32(incr), nil
}

//PreRegisterNode register node on chain and pledge YTA for assignable space
func (self *NodeDaoImpl) PreRegisterNode(trx string) error {
	rate, err := self.eostx.GetExchangeRate()
	if err != nil {
		return err
	}
	signedTrx, regData, err := self.eostx.PreRegisterTrx(trx)
	if err != nil {
		return err
	}
	pledgeAmount := int64(regData.DepAmount.Amount)
	adminAccount := string(regData.Owner)
	profitAccount := string(regData.Owner)
	minerID := int32(regData.MinerID)
	pubkey := regData.Extra
	if adminAccount == "" || profitAccount == "" || minerID == 0 || minerID%int32(incr) != index || pubkey == "" {
		return errors.New("bad parameters in regminer transaction")
	}
	nodeid, err := IdFromPublicKey(pubkey)
	if err != nil {
		return err
	}
	currID, err := self.NewNodeID()
	if err != nil {
		return err
	}
	if currID != minerID {
		return errors.New("node ID is invalid, please retry")
	}
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		return err
	}
	collection := self.client.Database(YottaDB).Collection(SequenceTab)
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": NodeIdxType}, bson.M{"$set": bson.M{"seq": minerID}})
	if err != nil {
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
		return err
	}
	return nil
}

//ChangeMinerPool add miner to a pool
func (self *NodeDaoImpl) ChangeMinerPool(trx string) error {
	signedTrx, poolData, err := self.eostx.ChangMinerPoolTrx(trx)
	if err != nil {
		return err
	}
	minerID := int32(poolData.MinerID)
	poolID := string(poolData.PoolID)
	minerProfit := string(poolData.MinerProfit)
	quota := poolData.MaxSpace
	if minerID%int32(incr) != index {
		return errors.New("transaction sends to incorrect super node.")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	node := new(Node)
	err = collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(&node)
	if err != nil {
		return err
	}
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		return err
	}
	var afterReg bool = false
	if node.Status == 0 && node.ProductiveSpace == 0 {
		afterReg = true
	}
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": minerID}, bson.M{"$set": bson.M{"poolID": poolID, "profitAcc": minerProfit, "quota": quota}})
	if err != nil {
		return err
	}
	if afterReg {
		err = collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(&node)
		if err != nil {
			return err
		}
		assignable := Min(node.AssignedSpace, node.Quota)
		if assignable <= 0 {
			return errors.New("assignable space is 0")
		}
		if assignable >= 65536 {
			assignable = 65536
		}
		err := self.IncrProductiveSpace(node.ID, assignable)
		if err != nil {
			return err
		}
		err = self.eostx.AddSpace(node.ProfitAcc, uint64(node.ID), uint64(assignable))
		if err != nil {
			self.IncrProductiveSpace(node.ID, -1*assignable)
			return err
		}
	}
	return nil
}

//RegisterNode create a new data node
func (self *NodeDaoImpl) RegisterNode(node *Node) (*Node, error) {
	// if node == nil {
	// 	return nil, errors.New("node is null")
	// }
	// if node.ID == 0 {
	// 	return nil, errors.New("node ID must not be null")
	// }
	// if node.NodeID == "" {
	// 	return nil, errors.New("node ID must not be null")
	// }
	// if node.PubKey == "" {
	// 	return nil, errors.New("public key must not be null")
	// }
	// if node.Status != 0 {
	// 	return nil, errors.New("node status is not after-preregister")
	// }
	// collection := self.client.Database(YottaDB).Collection(NodeTab)
	// n := new(Node)
	// err := collection.FindOne(context.Background(), bson.M{"_id": node.ID}).Decode(&n)
	// if err != nil {
	// 	return nil, err
	// }
	// if node.Owner != n.Owner {
	// 	return nil, errors.New("owner not match")
	// }
	// n.NodeID = node.NodeID
	// n.PubKey = node.PubKey
	// n.Status = 1
	// n.Timestamp = time.Now().Unix()

	// opts := new(options.FindOneAndUpdateOptions)
	// opts = opts.SetReturnDocument(options.After)
	// result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"nodeid": n.NodeID, "pubkey": n.PubKey, "status": n.Status, "timestamp": n.Timestamp}}, opts)
	// err = result.Decode(&node)
	// if err != nil {
	// 	return nil, err
	// }
	// return node, nil
	return nil, errors.New("no use")
}

//UpdateNode update data info by data node status
func (self *NodeDaoImpl) UpdateNodeStatus(node *Node) (*Node, error) {
	if node == nil {
		return nil, errors.New("node is null")
	}
	if node.ID == 0 {
		return nil, errors.New("node ID cannot be null")
	}
	if node.ID%int32(incr) != index {
		return nil, errors.New("node do not belong to this SN")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	n := new(Node)
	err := collection.FindOne(context.Background(), bson.M{"_id": node.ID}).Decode(&n)
	if err != nil {
		return nil, err
	}
	if n.Quota == 0 || n.AssignedSpace == 0 {
		return nil, fmt.Errorf("node %d has not been pledged or added to a pool", n.ID)
	}
	node.Valid = n.Valid
	relayURL, err := self.AddrCheck(n, node)
	if err != nil {
		return nil, err
	}
	var status int32 = 1
	if n.Status > 1 {
		status = n.Status
	}
	weight := math.Sqrt(2*math.Pow(float64(node.CPU)/100, 2) + 2*math.Pow(float64(node.Memory)/100, 2) + 2*math.Pow(float64(node.Bandwidth)/100, 2) + math.Pow(float64(n.UsedSpace)/float64(Min(n.AssignedSpace, n.Quota, node.MaxDataSpace)), 2))
	opts := new(options.FindOneAndUpdateOptions)
	opts = opts.SetReturnDocument(options.After)
	var timestamp int64
	if self.CheckErrorNode(node.ID) {
		timestamp = time.Now().Unix()
	} else {
		timestamp = n.Timestamp
	}

	result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "addrs": node.Addrs, "weight": weight, "valid": node.Valid, "relay": node.Relay, "status": status, "timestamp": timestamp, "version": node.Version}}, opts)
	err = result.Decode(&n)
	if err != nil {
		return nil, err
	}
	if relayURL != "" {
		n.Addrs = []string{relayURL}
	} else {
		n.Addrs = nil
	}
	return n, nil
}

func (self *NodeDaoImpl) CheckErrorNode(id int32) bool {
	collection := self.client.Database(YottaDB).Collection(ErrorNodeTab)
	m := make(map[string]interface{})
	err := collection.FindOne(context.Background(), bson.M{"_id": 1, "nodes": bson.M{"$in": bson.A{id}}}, options.FindOne().SetProjection(bson.M{"nodes": 1})).Decode(&m)
	if err != nil {
		return true
	} else {
		return false
	}
}

//IncrUsedSpace increase user space of one node
func (self *NodeDaoImpl) IncrUsedSpace(id int32, incr int64) error {
	if incr < 0 {
		return errors.New("incremental space cannot be minus")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	n := new(Node)
	err := collection.FindOne(context.Background(), bson.M{"_id": id}).Decode(&n)
	if err != nil {
		return err
	}
	weight := math.Sqrt(2*math.Pow(float64(n.CPU)/100, 2) + 2*math.Pow(float64(n.Memory)/100, 2) + 2*math.Pow(float64(n.Bandwidth)/100, 2) + math.Pow(float64(n.UsedSpace+incr)/float64(Min(n.AssignedSpace, n.Quota, n.MaxDataSpace)), 2))
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
func (self *NodeDaoImpl) AllocNodes(shardCount int32, errids []int32) ([]Node, error) {
	sc := shardCount
	if shardCount == 1 {
		shardCount = 2
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	nodes := make([]Node, 0)
	m := make(map[int32]int64)
	var i int64
	options := options.FindOptions{}
	options.Sort = bson.D{{"weight", 1}}
	limit := int64(shardCount)
	options.Limit = &limit
	for {
		cond := bson.M{"valid": 1, "status": 1, "assignedSpace": bson.M{"$gt": 0}, "quota": bson.M{"$gt": 0}, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}, "version": bson.M{"$gte": 4}}
		if errids != nil && len(errids) > 0 {
			cond["_id"] = bson.M{"$nin": errids}
		}
		cur, err := collection.Find(context.Background(), cond, &options)
		if err != nil {
			return nil, err
		}
		canLoop := false
		for cur.Next(context.Background()) {
			result := new(Node)
			err := cur.Decode(result)
			if err != nil {
				cur.Close(context.Background())
				return nil, err
			}

			m[result.ID] += 1
			// increase 1GB when remain space less than 1/8 GB
			if result.UsedSpace+m[result.ID]+8192 > result.ProductiveSpace {
				if result.ID%int32(incr) != index {
					m[result.ID] -= 1
					continue
				}
				assignable := Min(result.AssignedSpace, result.Quota, result.MaxDataSpace) - result.ProductiveSpace
				if assignable <= 0 {
					m[result.ID] -= 1
					continue
				}
				if assignable >= 65536 {
					assignable = 65536
				}
				err := self.IncrProductiveSpace(result.ID, assignable)
				if err != nil {
					m[result.ID] -= 1
					continue
				}
				err = self.eostx.AddSpace(result.ProfitAcc, uint64(result.ID), uint64(assignable))
				if err != nil {
					self.IncrProductiveSpace(result.ID, -1*assignable)
					m[result.ID] -= 1
					continue
				}
				result.ProductiveSpace += assignable
			}

			nodes = append(nodes, *result)
			i += 1
			canLoop = true
			if i == int64(shardCount) {
				cur.Close(context.Background())
				if sc == 1 && len(nodes) > 0 {
					return nodes[0:1], nil
				}
				return nodes, nil
			}
		}
		cur.Close(context.Background())
		if !canLoop {
			break
		}
	}
	return nil, errors.New("no enough data nodes can be allocted")
}

//SyncNode sync node info to other SN
func (self *NodeDaoImpl) SyncNode(node *Node) error {
	if node.ID == 0 {
		return errors.New("node ID must not be null")
	}
	if node.ID%int32(incr) == index {
		return nil
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err := collection.InsertOne(context.Background(), node)
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			return err
		} else {
			_, err := collection.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version}})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

//GetNodes by node IDs
func (self *NodeDaoImpl) GetNodes(nodeIDs []int32) ([]Node, error) {
	cond := bson.A{}
	for _, id := range nodeIDs {
		cond = append(cond, bson.D{{"_id", id}})
	}
	nodes := make([]Node, 0)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.D{{"$or", cond}})
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(result)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, *result)
	}
	return nodes, nil
}

//GetSuperNodes get all super nodes
func (self *NodeDaoImpl) GetSuperNodes() ([]SuperNode, error) {
	supernodes := make([]SuperNode, 0)
	collection := self.client.Database(YottaDB).Collection(SuperNodeTab)
	cur, err := collection.Find(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		result := new(SuperNode)
		err := cur.Decode(result)
		if err != nil {
			return nil, err
		}
		supernodes = append(supernodes, *result)
	}
	return supernodes, nil
}

//GetSuperNodePrivateKey get private key of super node with certain ID
func (self *NodeDaoImpl) GetSuperNodeByID(id int32) (*SuperNode, error) {
	collection := self.client.Database(YottaDB).Collection(SuperNodeTab)
	supernode := new(SuperNode)
	err := collection.FindOne(context.Background(), bson.D{{"_id", id}}).Decode(supernode)
	if err != nil {
		return nil, err
	}
	return supernode, nil
}

//GetSuperNodePrivateKey get private key of super node with certain ID
func (self *NodeDaoImpl) GetSuperNodePrivateKey(id int32) (string, error) {
	collection := self.client.Database(YottaDB).Collection(SuperNodeTab)
	cur, err := collection.Find(context.Background(), bson.D{{"_id", id}})
	if err != nil {
		return "", err
	}
	defer cur.Close(context.Background())
	result := new(SuperNode)
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
			return "", err
		}
		return result.PrivKey, nil
	}
	return "", errors.New("No result")
}

//GetNodeIDByPubKey get node ID by public key
func (self *NodeDaoImpl) GetNodeIDByPubKey(pubkey string) (int32, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.D{{"pubkey", pubkey}})
	if err != nil {
		return 0, err
	}
	defer cur.Close(context.Background())
	result := new(Node)
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
			return 0, err
		}
		return result.ID, nil
	}
	return 0, errors.New("No result")
}

//GetNodeByPubKey get node by public key
func (self *NodeDaoImpl) GetNodeByPubKey(pubkey string) (*Node, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.D{{"pubkey", pubkey}})
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())
	result := new(Node)
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
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
		return 0, err
	}
	defer cur.Close(context.Background())
	result := new(SuperNode)
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
			return 0, err
		}
		return result.ID, nil
	}
	return 0, errors.New("No result")
}

//AddDNI add digest of one shard
func (self *NodeDaoImpl) AddDNI(id int32, shard []byte) error {
	collection := self.client.Database(YottaDB).Collection(DNITab)
	_, err := collection.InsertOne(context.Background(), bson.M{"_id": primitive.NewObjectID(), "shard": shard, "minerID": id})
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			return err
		} else {
			return nil
		}
	} else {
		collection = self.client.Database(YottaDB).Collection(NodeTab)
		_, err = collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$inc": bson.M{"usedSpace": 1}})
		return err
	}
	// _, err = collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$push": bson.M{"shards": shard}})
	// if err != nil {
	// 	return err
	// }

	// pipeline := mongo.Pipeline{
	// 	{{"$match", bson.D{{"_id", id}}}},
	// 	{{"$project", bson.D{{"cnt", bson.D{{"$size", "$shards"}}}}}},
	// }
	// cur, err := collection.Aggregate(context.Background(), pipeline)
	// if err != nil {
	// 	return err
	// }
	// result := new(ShardCount)
	// defer cur.Close(context.Background())
	// if cur.Next(context.Background()) {
	// 	err := cur.Decode(&result)
	// 	if err != nil {
	// 		return err
	// 	}
	// }
	// collection = self.client.Database(YottaDB).Collection(NodeTab)
	// _, err = collection.UpdateOne(context.Background(), bson.M{"_id": result.ID}, bson.M{"$set": bson.M{"usedSpace": result.Cnt}})
	// return err
}

//ActiveNodesList show id and public IP of all active data nodes
func (self *NodeDaoImpl) ActiveNodesList() ([]Node, error) {
	nodes := make([]Node, 0)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.M{"valid": 1, "status": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}})
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(result)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, *result)
	}
	return nodes, nil
}

//Statistics of data nodes
func (self *NodeDaoImpl) Statistics() (*NodeStat, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	active, err := collection.CountDocuments(context.Background(), bson.M{"valid": 1, "status": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}})
	if err != nil {
		return nil, err
	}
	total, err := collection.CountDocuments(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	pipeline := mongo.Pipeline{
		{{"$project", bson.D{{"maxDataSpace", 1}, {"assignedSpace", 1}, {"productiveSpace", 1}, {"usedSpace", 1}, {"_id", 0}}}},
		{{"$group", bson.D{{"_id", ""}, {"maxTotal", bson.D{{"$sum", "$maxDataSpace"}}}, {"assignedTotal", bson.D{{"$sum", "$assignedSpace"}}}, {"productiveTotal", bson.D{{"$sum", "$productiveSpace"}}}, {"usedTotal", bson.D{{"$sum", "$usedSpace"}}}}}},
	}
	cur, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		return nil, err
	}
	result := new(NodeStat)
	defer cur.Close(context.Background())
	if cur.Next(context.Background()) {
		err := cur.Decode(&result)
		if err != nil {
			return nil, err
		}
	}
	result.ActiveMiners = active
	result.TotalMiners = total
	return result, nil
}
