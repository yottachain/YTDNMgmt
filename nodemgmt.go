package YTDNMgmt

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
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

// create a new instance of NodeDaoImpl
func NewInstance(mongoURL, eosURL, bpAccount, bpPrivkey, contractOwner string, bpID int32) (*NodeDaoImpl, error) {
	//ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		return nil, err
	}
	// ci, err := getContractInfo(client)
	// if err != nil {
	// 	return nil, err
	// }
	etx, err := eostx.NewInstance(eosURL, bpAccount, bpPrivkey, contractOwner)
	if err != nil {
		return nil, err
	}
	host, err := NewHost()
	if err != nil {
		return nil, err
	}
	return &NodeDaoImpl{client: client, eostx: etx, host: host, bpID: bpID}, nil
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

// func getContractInfo(client *mongo.Client) (*ContractInfo, error) {
// 	collection := client.Database(YottaDB).Collection(ContractInfoTab)
// 	ci := new(ContractInfo)
// 	err := collection.FindOne(context.Background(), bson.M{"_id": 0}).Decode(&ci)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return ci, nil
// }

// generate a new id for Node collection
func newID(client *mongo.Client) (int32, error) {
	if incr == 0 {
		collection := client.Database(YottaDB).Collection(SuperNodeTab)
		c, err := collection.CountDocuments(context.Background(), bson.D{})
		if err != nil {
			return 0, err
		}
		incr = c
	}
	collection := client.Database(YottaDB).Collection(SequenceTab)
	initseq, err := getCurrentSuperNodeIndex(client)
	if err != nil {
		return 0, err
	}
	_, err = collection.InsertOne(context.Background(), bson.M{"_id": NodeIdxType, "seq": initseq})
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			return 0, err
		}
	}
	opts := new(options.FindOneAndUpdateOptions)
	opts = opts.SetReturnDocument(options.After)
	result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": NodeIdxType}, bson.M{"$inc": bson.M{"seq": incr}}, opts)
	m := make(map[string]int32)
	err = result.Decode(&m)
	if err != nil {
		return 0, err
	}
	return m["seq"], nil
}

// RegisterNode create a new data node
func (self *NodeDaoImpl) RegisterNode(node *Node) (*Node, error) {
	// TODO: pre-allocate space
	if node == nil {
		return nil, errors.New("node is null")
	}
	if node.Owner == "" {
		return nil, errors.New("No owner found")
	}
	if node.ID > 0 {
		return nil, errors.New("node ID must be null")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	id, err := newID(self.client)
	if err != nil {
		return nil, err
	}
	node.ID = id
	node.MaxDataSpace = 67108864  //1T
	node.AssignedSpace = 67108864 //1T
	node.Weight = 0
	node.Timestamp = time.Now().Unix()
	node.Valid = 0
	node.Relay = 0
	relayUrl, err := self.AddrCheck(new(Node), node)
	if err != nil {
		return nil, err
	}
	//ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = collection.InsertOne(context.Background(), node)
	if err != nil {
		return nil, err
	}
	err = self.eostx.AddMiner(node.Owner, uint64(id))
	if err != nil {
		_, _ = collection.DeleteOne(context.Background(), bson.M{"_id": id})
		return nil, fmt.Errorf("Error when writing owner info into contract: %s %s", node.Owner, id)
	}
	//TODO: connectivity judgement

	err = collection.FindOne(context.Background(), bson.M{"_id": id}).Decode(&node)
	if err != nil {
		return nil, err
	}
	if relayUrl != "" {
		node.Addrs = []string{relayUrl}
	} else {
		node.Addrs = nil
	}
	return node, nil
}

// UpdateNode update data info by data node status
func (self *NodeDaoImpl) UpdateNodeStatus(node *Node) (*Node, error) {
	if node == nil {
		return nil, errors.New("node is null")
	}
	if node.ID == 0 {
		return nil, errors.New("node ID cannot be null")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	n := new(Node)
	err := collection.FindOne(context.Background(), bson.M{"_id": node.ID}).Decode(&n)
	if err != nil {
		return nil, err
	}
	node.Valid = n.Valid
	relayUrl, err := self.AddrCheck(n, node)
	if err != nil {
		return nil, err
	}
	weight := math.Sqrt(2*math.Pow(float64(node.CPU)/100, 2) + 2*math.Pow(float64(node.Memory)/100, 2) + 2*math.Pow(float64(node.Bandwidth)/100, 2) + math.Pow(float64(n.UsedSpace)/float64(n.AssignedSpace), 2))
	opts := new(options.FindOneAndUpdateOptions)
	opts = opts.SetReturnDocument(options.After)
	result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "addrs": node.Addrs, "weight": weight, "valid": node.Valid, "relay": node.Relay, "timestamp": time.Now().Unix()}}, opts)
	err = result.Decode(&n)
	if err != nil {
		return nil, err
	}
	if relayUrl != "" {
		n.Addrs = []string{relayUrl}
	} else {
		n.Addrs = nil
	}
	return n, nil
}

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
	weight := math.Sqrt(2*math.Pow(float64(n.CPU)/100, 2) + 2*math.Pow(float64(n.Memory)/100, 2) + 2*math.Pow(float64(n.Bandwidth)/100, 2) + math.Pow(float64(n.UsedSpace+incr)/float64(n.AssignedSpace), 2))
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": bson.M{"weight": weight}, "$inc": bson.M{"usedSpace": incr}})
	return err
}

func (self *NodeDaoImpl) IncrProductiveSpace(id int32, incr int64) error {
	if incr < 0 {
		return errors.New("incremental space cannot be minus")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err := collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$inc": bson.M{"productiveSpace": incr}})
	return err
}

// AllocNodes by shard cound
func (self *NodeDaoImpl) AllocNodes(shardCount int32) ([]Node, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	nodes := make([]Node, 0)
	m := make(map[int32]int64)
	var i int64
	options := options.FindOptions{}
	options.Sort = bson.D{{"weight", 1}}
	limit := int64(shardCount)
	options.Limit = &limit
	for {
		cur, err := collection.Find(context.Background(), bson.M{"valid": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}}, &options)
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
				assignable := result.AssignedSpace - result.ProductiveSpace
				if assignable == 0 {
					m[result.ID] -= 1
					continue
				}
				if assignable >= 65536 {
					assignable = 65536
				}
				err := self.IncrProductiveSpace(result.ID, assignable)
				if err != nil {
					continue
				}
				err = self.eostx.AddSpace(result.Owner, uint64(result.ID), uint64(assignable))
				if err != nil {
					self.IncrProductiveSpace(result.ID, -1*assignable)
					continue
				}
				result.ProductiveSpace += assignable
			}

			nodes = append(nodes, *result)
			i += 1
			canLoop = true
			if i == int64(shardCount) {
				cur.Close(context.Background())
				return nodes, nil
			}
		}
		cur.Close(context.Background())
		if !canLoop {
			break
		}
	}

	return nil, errors.New("No enough data nodes can be allocted")
}

// GetNodes by node IDs
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

// GetSuperNodes get all super nodes
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

// GetSuperNodePrivateKey get private key of super node with certain ID
func (self *NodeDaoImpl) GetSuperNodeByID(id int32) (*SuperNode, error) {
	collection := self.client.Database(YottaDB).Collection(SuperNodeTab)
	supernode := new(SuperNode)
	err := collection.FindOne(context.Background(), bson.D{{"_id", id}}).Decode(supernode)
	if err != nil {
		return nil, err
	}
	return supernode, nil
}

// GetSuperNodePrivateKey get private key of super node with certain ID
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

// GetNodeIDByPubKey get node ID by public key
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

// GetSuperNodeIDByPubKey get super node ID by public key
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

// AddDNI add digest of one shard
func (self *NodeDaoImpl) AddDNI(id int32, shard []byte) error {
	collection := self.client.Database(YottaDB).Collection(DNITab)
	_, err := collection.InsertOne(context.Background(), bson.M{"_id": id, "shards": bson.A{shard}})
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			return err
		}
	} else {
		return nil
	}
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$addToSet": bson.M{"shards": shard}})
	if err != nil {
		return err
	}

	pipeline := mongo.Pipeline{
		{{"$match", bson.D{{"_id", id}}}},
		{{"$project", bson.D{{"cnt", bson.D{{"$size", "$shards"}}}}}},
	}
	cur, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		return err
	}
	result := new(ShardCount)
	defer cur.Close(context.Background())
	if cur.Next(context.Background()) {
		err := cur.Decode(&result)
		if err != nil {
			return err
		}
	}
	collection = self.client.Database(YottaDB).Collection(NodeTab)
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": result.ID}, bson.M{"$set": bson.M{"usedSpace": result.Cnt}})
	return err
}

// ActiveNodesList show id and public IP of all active data nodes
func (self *NodeDaoImpl) ActiveNodesList() ([]Node, error) {
	nodes := make([]Node, 0)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.M{"valid": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}})
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

// Statistics of data nodes
func (self *NodeDaoImpl) Statistics() (*NodeStat, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	active, err := collection.CountDocuments(context.Background(), bson.M{"valid": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}})
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
