package YTDNMgmt

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type NodeDaoImpl struct {
	client *mongo.Client
}

//Create a new instance of NodeDaoImpl
func NewInstance(urls string) (*NodeDaoImpl, error) {
	ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(urls))
	if err != nil {
		return nil, err
	}
	return &NodeDaoImpl{client}, nil
}

//Allocate node by shard cound
func (self *NodeDaoImpl) AllocNodes(shardCount int32) ([]Node, error) {
	//ctx, _ := context.WithTimeout(context.Background(), 30*time.Second)
	collection := self.client.Database(YOTTA_DB).Collection(NODE_TAB)
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
	} else {
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
}

func (self *NodeDaoImpl) GetNodes(nodeIDs []int32) ([]Node, error) {
	cond := bson.A{}
	for _, id := range nodeIDs {
		cond = append(cond, bson.D{{"id", id}})
	}
	nodes := make([]Node, 0)
	collection := self.client.Database(YOTTA_DB).Collection(NODE_TAB)
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

func (self *NodeDaoImpl) GetSuperNodes() ([]SuperNode, error) {
	supernodes := make([]SuperNode, 0)
	collection := self.client.Database(YOTTA_DB).Collection(SUPERNODE_TAB)
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

func (self *NodeDaoImpl) GetSuperNodePrivateKey(id int32) (string, error) {
	collection := self.client.Database(YOTTA_DB).Collection(SUPERNODE_TAB)
	cur, err := collection.Find(context.Background(), bson.D{{"id", id}})
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

func (self *NodeDaoImpl) GetNodeIDByPubKey(pubkey string) (int32, error) {
	collection := self.client.Database(YOTTA_DB).Collection(NODE_TAB)
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

func (self *NodeDaoImpl) GetSuperNodeIDByPubKey(pubkey string) (int32, error) {
	collection := self.client.Database(YOTTA_DB).Collection(SUPERNODE_TAB)
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
