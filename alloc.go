package YTDNMgmt

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sort"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// NodesSelector allocate nodes for client
type NodesSelector struct {
	Range   []int64
	Nodes   map[int64]*Node
	Sum     int64
	nodeIDs []int32
}

type Int32Slice []int32

func (s Int32Slice) Len() int           { return len(s) }
func (s Int32Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Int32Slice) Less(i, j int) bool { return s[i] < s[j] }

type Int64Slice []int64

func (s Int64Slice) Len() int           { return len(s) }
func (s Int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Int64Slice) Less(i, j int) bool { return s[i] < s[j] }

func init() {
	rand.Seed(time.Now().UnixNano())
}

// Start NodeSelector
func (s *NodesSelector) Start(ctx context.Context, nodeMgr *NodeDaoImpl) {
	s.refresh(nodeMgr)
	ticker := time.NewTicker(10 * time.Minute)
	go func(t *time.Ticker) {
		for {
			select {
			case <-ctx.Done():
				log.Printf("Stop refresh operation of NodeSelector")
				return
			case <-t.C:
				s.refresh(nodeMgr)
			}
		}
	}(ticker)
}

func (s *NodesSelector) refresh(nodeMgr *NodeDaoImpl) {
	rg := make([]int64, 0)
	nodeMap := make(map[int64]*Node)
	nodeIDs := make([]int32, 0)
	var sum int64
	collection := nodeMgr.client.Database(YottaDB).Collection(NodeTab)
	cond := bson.M{"valid": 1, "status": 1, "assignedSpace": bson.M{"$gt": 0}, "quota": bson.M{"$gt": 0}, "cpu": bson.M{"$lt": 98}, "memory": bson.M{"$lt": 95}, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*3}, "$or": bson.A{bson.M{"weight": bson.M{"$gt": 65536}}, bson.M{"weight": bson.M{"$lt": 0.9}}}, "version": bson.M{"$gte": 6}}
	cur, err := collection.Find(context.Background(), cond)
	if err != nil {
		log.Printf("Error when refresh NodeSelector: %s", err)
		return
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(result)
		if err != nil {
			log.Printf("Error when refresh NodeSelector: %s", err)
			return
		}
		if result.Weight <= 0 {
			continue
		}
		if result.Weight < 1 {
			result.Weight = float64(Min(result.AssignedSpace, result.Quota, result.MaxDataSpace)-result.UsedSpace) * (1 - result.Weight)
		}
		sum += int64(result.Weight)
		nodeMap[sum] = result
		rg = append(rg, sum)
		nodeIDs = append(nodeIDs, result.ID)
	}
	sort.Sort(Int32Slice(nodeIDs))
	s.Range = rg
	s.Nodes = nodeMap
	s.nodeIDs = nodeIDs
	if len(rg)-1 > 0 {
		s.Sum = rg[len(rg)-1]
	} else {
		s.Sum = 0
	}
	log.Printf("Refresh finished, %d nodes can be allocated.", len(rg))
}

// Alloc nodes for client
func (s *NodesSelector) Alloc(shardCount int32, errids []int32) ([]*Node, error) {
	sort.Sort(Int32Slice(errids))
	rg := s.Range
	nodeMap := s.Nodes
	totalWeight := s.Sum
	nodeIDs := s.nodeIDs
	errids = exclude(errids, nodeIDs)
	nodes := make([]*Node, 0)
	t := len(rg)
	if t-len(errids) <= 0 {
		return nil, errors.New("no enough data nodes can be allocted")
	}
	if int(shardCount)+len(errids) >= t {
		s := shardCount / int32(t-len(errids))
		m := shardCount % int32(t-len(errids))
		if s > 0 {
			ns := make([]*Node, 0)
			for _, v := range nodeMap {
				if binarySearch32(errids, v.ID) == -1 {
					ns = append(ns, v)
				}
			}
			nodes = append(nodes, ns...)
			if s-1 > 0 {
				for i := 0; i < int(s)-1; i++ {
					nodes = append(nodes, ns...)
				}
			}
		}
		shardCount = m
	}
	allcNodeIds := make(map[int32]bool)
	for i := 0; i < int(shardCount); i++ {
		r := rand.Int63n(totalWeight)
		index := rangeSearch(rg, r)
		n := rg[index]
		id := nodeMap[n].ID
		if binarySearch32(errids, id) != -1 {
			i--
			continue
		}
		if allcNodeIds[id] {
			i--
			continue
		}
		allcNodeIds[id] = true
		nodes = append(nodes, nodeMap[n])
	}
	log.Printf("Allocated %d nodes.", len(nodes))
	return nodes, nil
}

func exclude(errids []int32, nodeids []int32) []int32 {
	ret := make([]int32, 0)
	for _, v := range errids {
		if binarySearch32(nodeids, v) != -1 {
			ret = append(ret, v)
		}
	}
	return ret
}

func rangeSearch(arr []int64, k int64) int {
	low := 0
	high := len(arr) - 1
	for low <= high {
		mid := low + (high-low)>>1
		if (mid == 0 && k <= arr[mid]) || (mid > 0 && k > arr[mid-1] && k <= arr[mid]) {
			return mid
		} else if k > arr[mid] {
			low = mid + 1
		} else {
			high = mid
		}
	}
	return -1
}

func binarySearch(arr []int64, k int64) int {
	low := 0
	high := len(arr) - 1
	for low <= high {
		mid := low + (high-low)>>1
		if k < arr[mid] {
			high = mid - 1
		} else if k > arr[mid] {
			low = mid + 1
		} else {
			return mid
		}
	}
	return -1
}

func binarySearch32(arr []int32, k int32) int {
	low := 0
	high := len(arr) - 1
	for low <= high {
		mid := low + (high-low)>>1
		if k < arr[mid] {
			high = mid - 1
		} else if k > arr[mid] {
			low = mid + 1
		} else {
			return mid
		}
	}
	return -1
}
