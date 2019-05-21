package YTDNMgmt

import (
	"context"
	"errors"
	"fmt"
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
}

var incr int64 = 0

// create a new instance of NodeDaoImpl
func NewInstance(mongoURL, eosURL string) (*NodeDaoImpl, error) {
	//ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		return nil, err
	}
	ci, err := getContractInfo(client)
	if err != nil {
		return nil, err
	}
	etx, err := eostx.NewInstance(eosURL, ci.User, ci.PrivKey)
	if err != nil {
		return nil, err
	}
	return &NodeDaoImpl{client: client, eostx: etx}, nil
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

func getContractInfo(client *mongo.Client) (*ContractInfo, error) {
	collection := client.Database(YottaDB).Collection(ContractInfoTab)
	ci := new(ContractInfo)
	err := collection.FindOne(context.Background(), bson.M{"_id": 0}).Decode(&ci)
	if err != nil {
		return nil, err
	}
	return ci, nil
}

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
	node.Timestamp = time.Now().Unix()
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
	err = collection.FindOne(context.Background(), bson.M{"_id": id}).Decode(&node)
	if err != nil {
		return nil, err
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
	//ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	opts := new(options.FindOneAndUpdateOptions)
	opts = opts.SetReturnDocument(options.After)
	result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "usedSpace": node.UsedSpace, "addrs": node.Addrs, "timestamp": time.Now().Unix()}}, opts)
	n := new(Node)
	err := result.Decode(&n)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (self *NodeDaoImpl) IncrUsedSpace(id int32, incr int64) error {
	if incr < 0 {
		return errors.New("incremental space cannot be minus")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err := collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$inc": bson.M{"usedSpace": incr}})
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
	//ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	// TODO: allocated nodes must have enough space
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	c, err := collection.CountDocuments(context.Background(), bson.M{"timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}})
	if err != nil {
		return nil, err
	}
	if c == 0 {
		return nil, errors.New("No enough data nodes can be allocted")
	}
	nodes := make([]Node, 0)
	tnodes := make([]Node, 0)
	cur, err := collection.Find(context.Background(), bson.M{"timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}})
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())
	m := make(map[int32]int64)
	var i int64
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(result)
		if err != nil {
			return nil, err
		}
		// if result.AssignedSpace <= result.ProductiveSpace {
		// 	continue
		// }

		m[result.ID] += 1
		// increase 1GB when remain space less than 1/8 GB
		if result.UsedSpace+m[result.ID]+8192 >= result.ProductiveSpace {
			assignable := result.AssignedSpace - result.ProductiveSpace
			if assignable == 0 {
				continue
			}
			if assignable >= 65536 {
				assignable = 65536
			}
			err := self.IncrProductiveSpace(result.ID, assignable)
			if err != nil {
				continue
			}
			// err = self.eostx.AddSpace(result.Owner, uint64(result.ID), uint64(assignable))
			// if err != nil {
			// 	//self.IncrProductiveSpace(result.ID, -assignable)
			// 	continue
			// }
			result.ProductiveSpace += assignable
		}

		nodes = append(nodes, *result)
		tnodes = append(tnodes, *result)
		i += 1
		if i == int64(shardCount) {
			return nodes, nil
		}
	}

	if len(nodes) == 0 {
		return nil, errors.New("No enough data nodes can be allocted")
	}

	for _, tnode := range tnodes {
		left := int64(shardCount) - i
		if left == 0 {
			break
		}
		var assignable int64
		var alloc int64
		if tnode.AssignedSpace-tnode.UsedSpace-m[tnode.ID]-8192 >= int64(left) {
			assignable = tnode.UsedSpace + m[tnode.ID] + 8192 + int64(left) - tnode.ProductiveSpace
			if assignable < 0 {
				assignable = 0
			}
			alloc = left
		} else if tnode.AssignedSpace-tnode.UsedSpace-m[tnode.ID]-8192 <= 0 {
			continue
		} else {
			assignable = tnode.AssignedSpace - tnode.ProductiveSpace
			if assignable < 0 {
				assignable = 0
			}
			alloc = tnode.AssignedSpace - 8192 - tnode.UsedSpace - m[tnode.ID]
		}
		if assignable > 0 {
			err := self.IncrProductiveSpace(tnode.ID, assignable)
			if err != nil {
				continue
			}
			// err = self.eostx.AddSpace(tnode.Owner, uint64(tnode.ID), uint64(assignable))
			// if err != nil {
			// 	self.IncrProductiveSpace(tnode.ID, -assignable)
			// 	continue
			// }
			tnode.ProductiveSpace += assignable
		}
		var j int64 = 0
		for ; j < alloc; j++ {
			nodes = append(nodes, tnode)
		}
		i += alloc
	}

	if int64(shardCount) != i {
		return nil, errors.New("No enough data nodes can be allocted")
	} else {
		return nodes, nil
	}
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
	return nil
}
