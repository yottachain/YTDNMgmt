package YTDNMgmt

import (
	"context"
	"errors"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type NodeDaoImpl struct {
	client *mongo.Client
}

// create a new instance of NodeDaoImpl
func NewInstance(urls string) (*NodeDaoImpl, error) {
	//ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(urls))
	if err != nil {
		return nil, err
	}
	return &NodeDaoImpl{client}, nil
}

// generate a new id for Node collection
func newID(client *mongo.Client) (int32, error) {
	collection := client.Database(MetaDB).Collection(SeqTab)
	//ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err := collection.InsertOne(context.Background(), bson.M{"_id": NodeIdxType, "seq": 0})
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			return 0, err
		}
	}
	opts := new(options.FindOneAndUpdateOptions)
	opts = opts.SetReturnDocument(options.After)
	result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": NodeIdxType}, bson.M{"$inc": bson.M{"seq": 1}}, opts)
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
	if node.ID > 0 {
		return nil, errors.New("node ID must be null")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	id, err := newID(self.client)
	if err != nil {
		return nil, err
	}
	node.ID = id
	//ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = collection.InsertOne(context.Background(), node)
	if err != nil {
		return nil, err
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
	result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "usedSpace": node.UsedSpace, "addrs": node.Addrs}}, opts)
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

// AllocNodes by shard cound
func (self *NodeDaoImpl) AllocNodes(shardCount int32) ([]Node, error) {
	//ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	// TODO: allocated nodes must have enough space
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	c, err := collection.CountDocuments(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}
	if int64(shardCount) <= c {
		nodes := make([]Node, 0)
		cur, err := collection.Find(context.Background(), bson.D{})
		if err != nil {
			return nil, err
		}
		defer cur.Close(context.Background())
		var i int32
		for i = 0; i < shardCount; i++ {
			cur.Next(context.Background())
			result := new(Node)
			err := cur.Decode(result)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, *result)
		}
		return nodes, nil
	}
	nodes := make([]Node, 0)
	cur, err := collection.Find(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())
	var i int64
	for i = 0; i < c; i++ {
		cur.Next(context.Background())
		result := new(Node)
		err := cur.Decode(result)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, *result)
	}
	for i = 0; i < int64(shardCount)-c; i++ {
		nodes = append(nodes, nodes[i%c])
	}
	return nodes, nil
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
