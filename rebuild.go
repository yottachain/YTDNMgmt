package YTDNMgmt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var selectedNode int64

func init() {
	sn := os.Getenv("SELECTEDNODE")
	ep, err := strconv.ParseInt(sn, 10, 64)
	if err != nil {
		selectedNode = 30
	} else {
		selectedNode = ep
	}
}

func (self *NodeDaoImpl) GetInvalidNodes() ([]*ShardCount, error) {
	lists := make([]*ShardCount, 0)
	collection := self.client.Database(YottaDB).Collection(DNITab)
	collectionNode := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collectionNode.Find(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}, "status": 2, "tasktimestamp": bson.M{"$exists": true, "$ne": nil, "$lt": time.Now().Unix() - 1800}})
	//cur, err := collectionNode.Find(context.Background(), bson.M{"$and": bson.A{bson.M{"_id": selectedNode}, bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}}}})
	if err != nil {
		log.Printf("GetInvalidNodes error: %s\n", err.Error())
		return nil, err
	}
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(&result)
		if err != nil {
			return nil, err
		}
		_, err = collection.DeleteMany(context.Background(), bson.M{"minerID": result.ID, "delete": 1})
		if err != nil {
			return nil, err
		}
		count, err := collection.CountDocuments(context.Background(), bson.M{"minerID": result.ID})
		if err != nil {
			return nil, err
		}
		if count == 0 {
			_, err := collectionNode.UpdateOne(context.Background(), bson.M{"_id": result.ID}, bson.M{"$unset": bson.M{"tasktimestamp": ""}, "$set": bson.M{"usedSpace": 0, "status": 3}})
			if err != nil {
				return nil, err
			}
			continue
		}
		_, err = collectionNode.UpdateOne(context.Background(), bson.M{"_id": result.ID}, bson.M{"$set": bson.M{"usedSpace": count}})
		if err != nil {
			return nil, err
		}
		shardCount := new(ShardCount)
		shardCount.ID = result.ID
		shardCount.Cnt = count
		lists = append(lists, shardCount)
	}
	return lists, nil

	// _, err := collection.UpdateMany(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}, "tasktimestamp": bson.M{"$exists": true, "$ne": nil, "$lt": time.Now().Unix() - 1800}}, bson.M{"$pull": bson.M{"shards": []byte{0}}})
	// if err != nil {
	// 	return nil, err
	// }
	// pipeline := mongo.Pipeline{
	// 	{{"$match", bson.D{{"_id", bson.M{"$mod": bson.A{incr, index}}}, {"timestamp", bson.M{"$exists": true, "$ne": nil}}, {"timestamp", bson.M{"$lt": time.Now().Unix() - 1800}}}}},
	// 	{{"$project", bson.D{{"cnt", bson.D{{"$size", "$shards"}}}}}},
	// }
	// cur, err := collection.Aggregate(context.Background(), pipeline)
	// if err != nil {
	// 	return nil, err
	// }
	// defer cur.Close(context.Background())
	// for cur.Next(context.Background()) {
	// 	result := new(ShardCount)
	// 	err := cur.Decode(&result)
	// 	if err != nil {
	// 		return nil, err
	// 	}
	// 	if result.Cnt == 0 {
	// 		_, err := collection.UpdateOne(context.Background(), bson.M{"_id": result.ID}, bson.M{"$unset": bson.M{"timestamp": ""}})
	// 		if err != nil {
	// 			return nil, err
	// 		}
	// 		_, err = collectionNode.UpdateOne(context.Background(), bson.M{"_id": result.ID}, bson.M{"$set": bson.M{"usedSpace": 0, "status": 3}})
	// 		if err != nil {
	// 			return nil, err
	// 		}

	// 		continue
	// 	}
	// 	lists = append(lists, *result)
	// }
	// return lists, nil
}

func (self *NodeDaoImpl) GetRebuildItem(minerID int32, index, total int64) (*Node, []primitive.Binary, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	n := new(Node)
	err := collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(&n)
	if err != nil {
		return nil, nil, err
	}
	shardCount := n.UsedSpace
	if index*total > shardCount {
		return nil, nil, errors.New("index out of range")
	}
	from := index * total
	end := (index + 1) * total
	if end >= shardCount {
		end = shardCount
	}
	count := end - from
	collectionDNI := self.client.Database(YottaDB).Collection(DNITab)
	options := options.FindOptions{}
	options.Skip = &from
	options.Limit = &count
	cur, err := collectionDNI.Find(context.Background(), bson.M{"minerID": minerID}, &options)
	if err != nil {
		return nil, nil, err
	}
	defer cur.Close(context.Background())
	shards := make([]primitive.Binary, 0)
	for cur.Next(context.Background()) {
		result := new(DNI)
		err := cur.Decode(result)
		if err != nil {
			return nil, nil, err
		}
		if result.Delete == 1 {
			b := new(primitive.Binary)
			b.Subtype = 0
			b.Data = []byte{0}
			shards = append(shards, *b)
		} else {
			shards = append(shards, result.Shard)
		}
	}

	rebuildNode, err := self.GetRebuildNode(count)
	if err != nil {
		return nil, nil, err
	}
	return rebuildNode, shards, nil

	// pipeline := mongo.Pipeline{
	// 	{{"$match", bson.D{{"_id", minerID}}}},
	// 	{{"$project", bson.D{{"shards", bson.D{{"$slice", bson.A{"$shards", from, count}}}}}}},
	// }
	// cur, err := collectionDNI.Aggregate(context.Background(), pipeline)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// result := new(DNI)
	// defer cur.Close(context.Background())
	// if cur.Next(context.Background()) {
	// 	err := cur.Decode(&result)
	// 	if err != nil {
	// 		return nil, nil, err
	// 	}
	// } else {
	// 	return nil, nil, fmt.Errorf("no miner with ID: %d", minerID)
	// }
	// rebuildNode, err := self.GetRebuildNode(count)
	// if err != nil {
	// 	return nil, nil, err
	// }
	// return rebuildNode, result.Shards, nil
}

func (self *NodeDaoImpl) GetRebuildNode(count int64) (*Node, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	options := options.FindOptions{}
	options.Sort = bson.D{{"timestamp", -1}}
	cur, err := collection.Find(context.Background(), bson.M{"valid": 1, "status": 1, "bandwidth": bson.M{"$lt": 50}, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*2}, "version": bson.M{"$gte": 6}}, &options)
	if err != nil {
		return nil, err
	}
	result := new(Node)
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		err := cur.Decode(&result)
		if err != nil {
			return nil, err
		}
		availible := Min(result.AssignedSpace, result.Quota, result.MaxDataSpace) - result.UsedSpace
		assignable := Min(result.AssignedSpace, result.Quota, result.MaxDataSpace) - result.ProductiveSpace
		if availible < count {
			continue
		}
		allocated := count - (availible - assignable) + 65536 - (count-(availible-assignable))%65536
		if allocated > assignable {
			allocated = assignable
		}
		if allocated > 0 {
			err = self.IncrProductiveSpace(result.ID, allocated)
			if err != nil {
				return nil, err
			}
			err = self.eostx.AddSpace(result.ProfitAcc, uint64(result.ID), uint64(allocated))
			if err != nil {
				self.IncrProductiveSpace(result.ID, -1*allocated)
				return nil, err
			}
			result.ProductiveSpace += allocated
		}
		return result, nil
	}
	return nil, fmt.Errorf("no node could be allocated")
}

func (self *NodeDaoImpl) DeleteDNI(minerID int32, shard []byte) error {
	collectionDNI := self.client.Database(YottaDB).Collection(DNITab)
	collectionNode := self.client.Database(YottaDB).Collection(NodeTab)
	//_, err := collectionDNI.UpdateOne(context.Background(), bson.M{"_id": minerID, "shards": shard}, bson.M{"$set": bson.M{"shards.$": []byte{0}, "timestamp": time.Now().Unix()}})
	_, err := collectionDNI.UpdateOne(context.Background(), bson.M{"minerID": minerID, "shard": shard}, bson.M{"$set": bson.M{"delete": 1}})
	if err != nil {
		return err
	}
	_, err = collectionNode.UpdateOne(context.Background(), bson.M{"_id": minerID}, bson.M{"$set": bson.M{"tasktimestamp": time.Now().Unix()}})
	if err != nil {
		return err
	}
	return nil
}
