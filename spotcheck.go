package YTDNMgmt

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (self *NodeDaoImpl) GetSpotCheckList() ([]*SpotCheckList, error) {
	err := self.SaveErrorNodeIDs()
	if err != nil {
		return nil, err
	}
	spotCheckLists := make([]*SpotCheckList, 0)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.M{"usedSpace": bson.M{"$gt": 0}, "assignedSpace": bson.M{"$gt": 0}, "status": 1, "_id": bson.M{"$mod": bson.A{incr, index}}})
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
		// if i == 5000 {
		// 	//TODO: save to mongodb and get id
		// 	spotCheckList, err := self.SaveSpotCheckList(spotCheckList)
		// 	if err != nil {
		// 		return nil, err
		// 	}
		// 	spotCheckLists = append(spotCheckLists, spotCheckList)
		// 	spotCheckList = new(SpotCheckList)
		// 	i = 0
		// }
	}
	if i != 0 {
		// spotCheckList, err := self.SaveSpotCheckList(spotCheckList)
		// if err != nil {
		// 	return nil, err
		// }
		spotCheckList.TaskID = primitive.NewObjectID()
		spotCheckList.Timestamp = time.Now().Unix()
		for range []int32{1, 2, 3} {
			spotCheckLists = append(spotCheckLists, spotCheckList)
		}
		self.SaveSpotCheckList(spotCheckList)
	}
	go self.PunishNodes()
	return spotCheckLists, nil

}

func (self *NodeDaoImpl) GetSTNode() (*Node, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	node := new(Node)
	options := options.FindOneOptions{}
	options.Sort = bson.D{{"timestamp", -1}}
	err := collection.FindOne(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}, "valid": 1, "status": 1, "bandwidth": bson.M{"$lt": 50}, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}, "version": bson.M{"$gte": 6}}, &options).Decode(node)
	if err != nil {
		return nil, err
	}
	return node, nil
}

//GetSTNodes get spotcheck nodes by count
func (self *NodeDaoImpl) GetSTNodes(count int64) ([]*Node, error) {
	options := options.FindOptions{}
	options.Sort = bson.D{{"timestamp", -1}}
	options.Limit = &count
	nodes := make([]*Node, 0)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}, "valid": 1, "status": 1, "bandwidth": bson.M{"$lt": 50}, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}, "version": bson.M{"$gte": 6}}, &options)
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
		nodes = append(nodes, result)
	}
	if len(nodes) > 0 {
		for len(nodes) < 3 {
			nodes = append(nodes, nodes[0])
		}
	} else {
		return nil, errors.New("No enougth spot check nodes")
	}
	return nodes, nil
}

func (self *NodeDaoImpl) UpdateTaskStatus(id string, invalidNodeList []int32) error {
	collection := self.client.Database(YottaDB).Collection(SpotCheckTab)
	taskID, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		return err
	}
	// now := time.Now().Unix()
	// oldval := new(SpotCheckList)
	task := bson.M{}
	err = collection.FindOne(context.Background(), bson.M{"_id": taskID}).Decode(&task)
	if err != nil {
		return err
	}
	for _, invId := range invalidNodeList {
		_, err = collection.UpdateOne(context.Background(), bson.M{"_id": taskID}, bson.M{"$inc": bson.M{fmt.Sprintf("nodes.%d", invId): 1}})
		if err != nil {
			return err
		}
	}
	return nil
}

func (self *NodeDaoImpl) GetRandomVNI(id int32, index int64) (string, error) {
	collection := self.client.Database(YottaDB).Collection(DNITab)
	options := options.FindOneOptions{}
	options.Skip = &index
	dni := new(DNI)
	err := collection.FindOne(context.Background(), bson.M{"minerID": id}, &options).Decode(dni)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(dni.Shard.Data), nil

	// pipeline := mongo.Pipeline{
	// 	{{"$match", bson.D{{"_id", id}}}},
	// 	{{"$project", bson.D{{"vni", bson.D{{"$arrayElemAt", bson.A{"$shards", index}}}}}}},
	// }
	// cur, err := collection.Aggregate(context.Background(), pipeline)
	// if err != nil {
	// 	return "", err
	// }
	// result := new(VNI)
	// defer cur.Close(context.Background())
	// if cur.Next(context.Background()) {
	// 	err := cur.Decode(&result)
	// 	if err != nil {
	// 		return "", err
	// 	}
	// 	return base64.StdEncoding.EncodeToString(result.VNI), nil
	// } else {
	// 	return "", errors.New("no matched DNI.")
	// }
}

func (self *NodeDaoImpl) SaveSpotCheckList(list *SpotCheckList) (*SpotCheckList, error) {
	collection := self.client.Database(YottaDB).Collection(SpotCheckTab)
	_, err := collection.DeleteMany(context.Background(), bson.M{})
	if err != nil {
		return nil, err
	}
	spr := new(SpotCheckRecord)
	spr.TaskID = list.TaskID
	spr.Nodes = make(map[string]int)
	spr.Timestamp = list.Timestamp
	_, err = collection.InsertOne(context.Background(), spr)
	if err != nil {
		return nil, err
	}
	return list, nil
}

func (self *NodeDaoImpl) SaveErrorNodeIDs() error {
	collection := self.client.Database(YottaDB).Collection(SpotCheckTab)
	task := new(SpotCheckRecord)
	err := collection.FindOne(context.Background(), bson.M{}).Decode(&task)
	if err != nil {
		return nil
	}
	invIDs := make([]int, 0)
	for key, value := range task.Nodes {
		if int32(value) > 1 {
			k, err := strconv.Atoi(key)
			if err != nil {
				return err
			}
			invIDs = append(invIDs, k)
		}
	}
	collection = self.client.Database(YottaDB).Collection(ErrorNodeTab)
	_, err = collection.DeleteMany(context.Background(), bson.M{})
	if err != nil {
		return err
	}
	_, err = collection.InsertOne(context.Background(), bson.M{"_id": 1, "nodes": invIDs})
	if err != nil {
		return err
	}
	return nil
}

// func (self *NodeDaoImpl) SaveSpotCheckList(list *SpotCheckList) (*SpotCheckList, error) {
// 	list.TaskID = primitive.NewObjectID()
// 	list.Timestamp = time.Now().Unix()
// 	collection := self.client.Database(YottaDB).Collection(SpotCheckTab)
// 	_, err := collection.InsertOne(context.Background(), list)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return list, nil
// }

func (self *NodeDaoImpl) PunishNodes() error {
	rate, err := self.eostx.GetExchangeRate()
	if err != nil {
		return err
	}
	err = self.Punish(10, 3, 2, rate, true) //失效两小时的矿机惩罚10%抵押
	if err != nil {
		return err
	}
	err = self.Punish(40, 25, 24, rate, false) //失效一天的矿机惩罚40%抵押
	if err != nil {
		return err
	}
	err = self.Punish(100, 169, 168, rate, false) //失效一周的矿机惩罚全部抵押
	if err != nil {
		return err
	}
	return nil
}

func (self *NodeDaoImpl) Punish(percent int64, from int64, to int64, rate int32, mark bool) error {
	nodes := make([]Node, 0)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.M{"status": bson.M{"$gte": 1}, "assignedSpace": bson.M{"$gt": 0}, "_id": bson.M{"$mod": bson.A{incr, index}}, "timestamp": bson.M{"$gte": time.Now().Unix() - 3600*from - 1800, "$lt": time.Now().Unix() - 3600*to - 1800}})
	if err != nil {
		return err
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(result)
		if err != nil {
			return err
		}
		nodes = append(nodes, *result)
	}
	for _, n := range nodes {
		// pledgeData, err := self.eostx.GetPledgeData(uint64(n.ID))
		// if err != nil {
		// 	return err
		// }
		// totalAsset := pledgeData.Total
		// punishAsset := pledgeData.Deposit

		// punishFee := int64(totalAsset.Amount) * percent / 100
		// if punishFee < int64(punishAsset.Amount) {
		// 	punishAsset.Amount = eos.Int64(punishFee)
		// }
		// err = self.eostx.DeducePledge(uint64(n.ID), &punishAsset)
		// if err != nil {
		// 	return err
		// }
		// newAssignedSpace := n.AssignedSpace - int64(punishAsset.Amount)*65536*int64(rate)/1000000
		// if newAssignedSpace < 0 {
		// 	newAssignedSpace = 0
		// }
		collection = self.client.Database(YottaDB).Collection(NodeTab)
		status := n.Status
		// invalid over 24 hours begin data rebuilding
		if mark {
			status = 2
			_, err = collection.UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"tasktimestamp": int64(0)}})
			if err != nil {
				return err
			}
		}
		_, err = collection.UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{ /* "assignedSpace": newAssignedSpace,*/ "status": status}})
		if err != nil {
			return err
		}
	}
	return nil
}
