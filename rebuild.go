package YTDNMgmt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (self *NodeDaoImpl) GetInvalidNodes() ([]*ShardCount, error) {
	lists := make([]*ShardCount, 0)
	collection := self.client.Database(YottaDB).Collection(DNITab)
	collectionNode := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collectionNode.Find(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}, "status": 2, "tasktimestamp": bson.M{"$exists": true, "$ne": nil, "$lt": time.Now().Unix() - invalidNodeTimeGap}})
	if err != nil {
		log.Printf("GetInvalidNodes error: %s\n", err.Error())
		return nil, err
	}
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(result)
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
}

func (self *NodeDaoImpl) GetRebuildItem(minerID int32, index, total int64) (*Node, []primitive.Binary, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	n := new(Node)
	err := collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(n)
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
}

func (self *NodeDaoImpl) GetRebuildNode(count int64) (*Node, error) {
	// collection := self.client.Database(YottaDB).Collection(NodeTab)
	// for range [10]byte{} {
	// 	total, err := collection.CountDocuments(context.Background(), bson.M{"valid": 1, "status": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*avaliableNodeTimeGap}, "version": bson.M{"$gte": minerVersionThreshold}})
	// 	if err != nil {
	// 		log.Printf("rebuild: GetRebuildNode: error when calculating total count of rebuildable nodes: %s\n", err.Error())
	// 		continue
	// 	}
	// 	if total == 0 {
	// 		log.Printf("rebuild: GetRebuildNode: count of rebuildable nodes is 0\n")
	// 		continue
	// 	}
	// 	index := rand.Intn(int(total))
	// 	options := options.FindOptions{}
	// 	skip := int64(index)
	// 	limit := int64(1)
	// 	options.Limit = &limit
	// 	options.Skip = &skip
	// 	//options.Sort = bson.D{{"timestamp", -1}}
	// 	cur, err := collection.Find(context.Background(), bson.M{"valid": 1, "status": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*avaliableNodeTimeGap}, "version": bson.M{"$gte": minerVersionThreshold}}, &options)
	// 	if err != nil {
	// 		log.Printf("rebuild: GetRebuildNode: error when creating cursor: %s\n", err.Error())
	// 		continue
	// 	}
	// 	result := new(Node)
	// 	if cur.Next(context.Background()) {
	// 		err := cur.Decode(result)
	// 		if err != nil {
	// 			cur.Close(context.Background())
	// 			log.Printf("rebuild: GetRebuildNode: error when decoding rebuildable nodes: %s\n", err.Error())
	// 			continue
	// 		}
	// 		// availible := Min(result.AssignedSpace, result.Quota, result.MaxDataSpace) - result.UsedSpace
	// 		// assignable := Min(result.AssignedSpace, result.Quota, result.MaxDataSpace) - result.ProductiveSpace
	// 		// if availible < count {
	// 		// 	continue
	// 		// }
	// 		// allocated := count - (availible - assignable) + prePurphaseAmount - (count-(availible-assignable))%prePurphaseAmount
	// 		// if allocated > assignable {
	// 		// 	allocated = assignable
	// 		// }
	// 		// if allocated > 0 {
	// 		// 	err = self.IncrProductiveSpace(result.ID, allocated)
	// 		// 	if err != nil {
	// 		// 		return nil, err
	// 		// 	}
	// 		// 	err = self.eostx.AddSpace(result.ProfitAcc, uint64(result.ID), uint64(allocated))
	// 		// 	if err != nil {
	// 		// 		self.IncrProductiveSpace(result.ID, -1*allocated)
	// 		// 		return nil, err
	// 		// 	}
	// 		// 	result.ProductiveSpace += allocated
	// 		// }
	// 		return result, nil
	// 	}
	// }
	// return nil, fmt.Errorf("no nodes can be allocated")
	nodes, err := self.ns.Alloc(1, []int32{})
	if err != nil {
		log.Printf("rebuild: GetRebuildNode: error when allocating rebuildable node: %s\n", err.Error())
		return nil, err
	}
	if nodes == nil || len(nodes) == 0 {
		log.Printf("rebuild: GetRebuildNode: no rebuildable nodes can be allocated\n")
		return nil, fmt.Errorf("no rebuildable nodes can be allocated")
	}
	for _, n := range nodes {
		self.setRebuildFlag(n.ID)
		n.Rebuilding = 1
	}
	return nodes[0], nil
}

func (self *NodeDaoImpl) setRebuildFlag(id int32) error {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err := collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": bson.M{"rebuilding": 1}})
	if err != nil {
		log.Printf("rebuild: setRebuildFlag: error when change rebuild flag to 1: %s\n", err.Error())
		return err
	}
	return nil
}

func (self *NodeDaoImpl) FinishRebuild(id int32) error {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err := collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": bson.M{"rebuilding": 0}})
	if err != nil {
		log.Printf("rebuild: FinishRebuild: error when change rebuild flag to 0: %s\n", err.Error())
		return err
	}
	return nil
}

func (self *NodeDaoImpl) DeleteDNI(minerID int32, shard []byte) error {
	collectionDNI := self.client.Database(YottaDB).Collection(DNITab)
	collectionNode := self.client.Database(YottaDB).Collection(NodeTab)
	node := new(Node)
	err := collectionNode.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(node)
	if err != nil {
		return err
	}
	if node.Status != 2 {
		return fmt.Errorf("can not delete shards of node %d with status %d", node.ID, node.Status)
	}
	_, err = collectionDNI.UpdateOne(context.Background(), bson.M{"minerID": minerID, "delete": 0, "shard": shard}, bson.M{"$set": bson.M{"delete": 1}})
	if err != nil {
		return err
	}
	_, err = collectionNode.UpdateOne(context.Background(), bson.M{"_id": minerID}, bson.M{"$set": bson.M{"tasktimestamp": time.Now().Unix()}})
	if err != nil {
		return err
	}
	return nil
}
