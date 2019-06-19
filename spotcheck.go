package YTDNMgmt

import (
	"context"
	"encoding/base64"
	"errors"
	"math/rand"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (self *NodeDaoImpl) GetSpotCheckList() ([]*SpotCheckList, error) {
	spotCheckLists := make([]*SpotCheckList, 0)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.M{"usedSpace": bson.M{"$gt": 0}, "valid": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}})
	if err != nil {
		return nil, err
	}
	defer cur.Close(context.Background())
	i := 0
	spotCheckList := new(SpotCheckList)
	for cur.Next(context.Background()) {
		node := new(Node)
		err := cur.Decode(node)
		if err != nil {
			return nil, err
		}
		spotCheckTask := new(SpotCheckTask)
		spotCheckTask.ID = node.ID
		spotCheckTask.NodeID = node.NodeID
		addr := GetRelayUrl(node.Addrs)
		if addr != "" {
			spotCheckTask.Addr = addr
		} else {
			spotCheckTask.Addr = CheckPublicAddr(node.Addrs)
		}
		spotCheckTask.VNI, err = self.GetRandomVNI(node.ID, rand.Int63n(node.UsedSpace))
		if err != nil {
			return nil, err
		}
		spotCheckList.TaskList = append(spotCheckList.TaskList, spotCheckTask)
		i++
		if i == 5000 {
			//TODO: save to mongodb and get id
			spotCheckList, err := self.SaveSpotCheckList(spotCheckList)
			if err != nil {
				return nil, err
			}
			spotCheckLists = append(spotCheckLists, spotCheckList)
			spotCheckList = new(SpotCheckList)
			i = 0
		}
	}
	if i != 0 {
		spotCheckList, err := self.SaveSpotCheckList(spotCheckList)
		if err != nil {
			return nil, err
		}
		spotCheckLists = append(spotCheckLists, spotCheckList)
	}
	return spotCheckLists, nil

}

func (self *NodeDaoImpl) GetSTNode() (*Node, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	node := new(Node)
	options := options.FindOneOptions{}
	options.Sort = bson.D{{"timestamp", -1}}
	err := collection.FindOne(context.Background(), bson.M{"valid": 1, "bandwidth": bson.M{"$lt": 50}, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}}, &options).Decode(node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

func (self *NodeDaoImpl) UpdateTaskStatus(id string, progress int32, invalidNodeList []int32) error {
	collection := self.client.Database(YottaDB).Collection(SpotCheckTab)
	taskID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return err
	}
	now := time.Now().Unix()
	oldval := new(SpotCheckList)
	err = collection.FindOne(context.Background(), bson.M{"_id": taskID}, options.FindOne().SetProjection(bson.M{"_id": 1, "progress": 1, "timestamp": 1, "duration": 1})).Decode(&oldval)
	if err != nil {
		return err
	}
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": taskID}, bson.M{"$set": bson.M{"progress": progress, "duration": now - oldval.Timestamp}})
	if err != nil {
		return err
	}
	if progress == 100 && invalidNodeList != nil {
		//TODO: processing invalid node
	}
	return nil
}

func (self *NodeDaoImpl) GetRandomVNI(id int32, index int64) (string, error) {
	collection := self.client.Database(YottaDB).Collection(DNITab)
	pipeline := mongo.Pipeline{
		{{"$match", bson.D{{"_id", id}}}},
		{{"$project", bson.D{{"vni", bson.D{{"$arrayElemAt", bson.A{"$shards", index}}}}}}},
	}
	cur, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		return "", err
	}
	result := new(VNI)
	defer cur.Close(context.Background())
	if cur.Next(context.Background()) {
		err := cur.Decode(&result)
		if err != nil {
			return "", err
		}
		return base64.StdEncoding.EncodeToString(result.VNI), nil
	} else {
		return "", errors.New("no matched DNI.")
	}
}

func (self *NodeDaoImpl) SaveSpotCheckList(list *SpotCheckList) (*SpotCheckList, error) {
	list.TaskID = primitive.NewObjectID()
	list.Timestamp = time.Now().Unix()
	collection := self.client.Database(YottaDB).Collection(SpotCheckTab)
	_, err := collection.InsertOne(context.Background(), list)
	if err != nil {
		return nil, err
	}
	return list, nil
}
