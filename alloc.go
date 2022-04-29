package YTDNMgmt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ipipdotnet/ipdb-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// NodesSelector allocate nodes for client
type NodesSelector struct {
	db     *ipdb.City
	rwlock *sync.RWMutex
	// Range   []int64
	// Nodes   map[int64]*Node
	// Sum     int64
	Root    *WRoot
	nodeIDs []int32
	Config  *MiscConfig
}

// WRoot root weight of regions
type WRoot struct {
	Ranges  []int64
	Regions map[int64]*WRegion
	Index   map[string]int64
	Sum     int64
}

// WRegion weight of nodes in one region
type WRegion struct {
	Name   string
	Ranges []int64
	Nodes  map[int64]*Node
	Sum    int64
}

//Int32Slice int slice
type Int32Slice []int32

func (s Int32Slice) Len() int           { return len(s) }
func (s Int32Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Int32Slice) Less(i, j int) bool { return s[i] < s[j] }

//Int64Slice int64 slice
type Int64Slice []int64

func (s Int64Slice) Len() int           { return len(s) }
func (s Int64Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s Int64Slice) Less(i, j int) bool { return s[i] < s[j] }

func init() {
	rand.Seed(time.Now().UnixNano())
}

// GetRegionInfo get region infomation from a IP address
func (s *NodesSelector) GetRegionInfo(node *Node) string {
	maddrPub := CheckPublicAddr(node.Addrs, s.Config.ExcludeAddrPrefix)
	if maddrPub == "" {
		return ""
	}
	str := strings.TrimPrefix(maddrPub, "/ip4/")
	str = str[0:strings.Index(str, "/")]
	m, err := s.db.FindMap(str, "EN")
	if err != nil {
		log.Printf("alloc: GetRegionInfo: error when fetching region infomation from IPDB: %s\n", err.Error())
		return ""
	}
	if m["city_name"] == "" {
		return m["country_name"]
	}
	return m["city_name"]
}

// Start NodeSelector
func (s *NodesSelector) Start(ctx context.Context, nodeMgr *NodeDaoImpl) {
	ipDBPath := s.Config.IPDBPath
	nodeAllocRefreshInterval := s.Config.MinerAllocRefreshInterval
	poolWeightRefreshInterval := s.Config.PoolWeightRefreshInterval
	log.Printf("alloc: Start: NodeSelector is starting...\n")
	s.nodeIDs = make([]int32, 0)
	db, err := ipdb.NewCity(ipDBPath)
	if err != nil {
		log.Fatalf("alloc: Start: fatal when reading IPDB: %s %s\n", ipDBPath, err.Error())
	}
	s.rwlock = new(sync.RWMutex)
	s.db = db
	// reload IPDB regularly
	go func() {
		for {
			time.Sleep(time.Hour * time.Duration(24))
			s.rwlock.Lock()
			log.Printf("alloc: Start: refreshing IPDB: %s\n", ipDBPath)
			err := s.db.Reload(ipDBPath)
			if err != nil {
				log.Fatalf("alloc: Start: fatal when refreshing IPDB: %s\n", err.Error())
			}
			s.rwlock.Unlock()
		}
	}()
	err = s.refresh(nodeMgr)
	if err != nil {
		log.Fatalf("alloc: Start: error when refreshing node list: %s\n", err.Error())
	}
	// refresh allocatable nodes list regularly
	ticker := time.NewTicker(time.Duration(nodeAllocRefreshInterval) * time.Minute)
	go func(t *time.Ticker) {
		for {
			select {
			case <-ctx.Done():
				log.Printf("alloc: Start: stop refresh operation of node\n")
				return
			case <-t.C:
				_ = s.refresh(nodeMgr)
			}
		}
	}(ticker)

	err = s.refreshPoolWeight(nodeMgr)
	if err != nil {
		log.Fatalf("alloc: Start: error when refreshing pool weight: %s\n", err.Error())
	}
	// refresh pool weight regularly
	ticker2 := time.NewTicker(time.Duration(poolWeightRefreshInterval) * time.Minute)
	go func(t *time.Ticker) {
		for {
			select {
			case <-ctx.Done():
				log.Printf("alloc: Start: stop refresh operation of pool weight\n")
				return
			case <-t.C:
				_ = s.refreshPoolWeight(nodeMgr)
			}
		}
	}(ticker2)
}

func (s *NodesSelector) refreshPoolWeight(nodeMgr *NodeDaoImpl) error {
	if atomic.LoadInt32(&(nodeMgr.master)) == 0 {
		return nil
	}
	now := time.Now().Unix()
	collection := nodeMgr.client.Database(YottaDB).Collection(NodeTab)
	collectionPW := nodeMgr.client.Database(YottaDB).Collection(PoolWeightTab)
	collectionSS := nodeMgr.client.Database(YottaDB).Collection(SpaceSumTab)
	pipeline1 := mongo.Pipeline{
		{{"$match", bson.D{{"status", bson.D{{"$lt", 99}}}}}},
		{{"$project", bson.D{{"usedSpace", 1}}}},
		{{"$group", bson.D{{"_id", ""}, {"totalSpace", bson.D{{"$sum", "$usedSpace"}}}}}},
	}
	cur, err := collection.Aggregate(context.Background(), pipeline1)
	if err != nil {
		log.Printf("alloc: refreshPoolWeight: error when aggregating total space: %s\n", err.Error())
		return err
	}
	ts := new(PoolWeight)
	if cur.Next(context.Background()) {
		err := cur.Decode(ts)
		if err != nil {
			log.Printf("alloc: refreshPoolWeight: error when decoding total space: %s\n", err.Error())
			cur.Close(context.Background())
			return err
		}
	}
	cur.Close(context.Background())
	totalSpace := ts.TotalSpace

	var referralSpace int64 = 0
	pipeline2 := mongo.Pipeline{
		{{"$group", bson.D{{"_id", ""}, {"referralSpace", bson.D{{"$sum", "$usedspace"}}}}}},
	}
	cur, err = collectionSS.Aggregate(context.Background(), pipeline2)
	if err != nil {
		log.Printf("alloc: refreshPoolWeight: error when aggregating referral space: %s\n", err.Error())
	} else {
		if cur.Next(context.Background()) {
			ts := new(PoolWeight)
			err := cur.Decode(ts)
			if err != nil {
				log.Printf("alloc: refreshPoolWeight: error when decoding referral space: %s\n", err.Error())
				cur.Close(context.Background())
			} else {
				referralSpace = ts.ReferralSpace
			}
		}
		cur.Close(context.Background())
	}

	errTime := now - s.Config.PoolErrorMinerTimeThreshold //int64(spotcheckInterval)*60 - int64(punishPhase1)*punishGapUnit
	pipeline3 := mongo.Pipeline{
		{{"$match", bson.D{{"poolOwner", bson.D{{"$ne", ""}}}, {"status", bson.D{{"$lt", 99}}}}}},
		{{"$project", bson.D{{"poolOwner", 1}, {"usedSpace", 1}, {"err", bson.D{{"$cond", bson.D{{"if", bson.D{{"$or", bson.A{bson.D{{"$eq", bson.A{"$status", 99}}}, bson.D{{"$lt", bson.A{"$timestamp", errTime}}}}}}}, {"then", 1}, {"else", 0}}}}}}}},
		{{"$group", bson.D{{"_id", "$poolOwner"}, {"poolTotalSpace", bson.D{{"$sum", "$usedSpace"}}}, {"poolTotalCount", bson.D{{"$sum", 1}}}, {"poolErrorCount", bson.D{{"$sum", "$err"}}}}}},
	}
	cur, err = collection.Aggregate(context.Background(), pipeline3)
	if err != nil {
		log.Printf("alloc: refreshPoolWeight: error when aggregating pool total space: %s\n", err.Error())
		return err
	}
	pws := make(map[string]*PoolWeight)
	for cur.Next(context.Background()) {
		result := new(PoolWeight)
		err := cur.Decode(result)
		if err != nil {
			log.Printf("alloc: refreshPoolWeight: error when decoding pool total space: %s\n", err.Error())
			continue
		}
		result.Timestamp = now
		result.TotalSpace = totalSpace
		result.ReferralSpace = referralSpace
		pws[result.ID] = result
	}
	cur.Close(context.Background())

	//TODO: Get PoolReferralSpace and ReferralSpace from SN
	pipeline4 := mongo.Pipeline{
		{{"$group", bson.D{{"_id", "$mowner"}, {"poolReferralSpace", bson.D{{"$sum", "$usedspace"}}}}}},
	}
	cur, err = collection.Aggregate(context.Background(), pipeline4)
	if err != nil {
		log.Printf("alloc: refreshPoolWeight: error when aggregating pool referral space: %s\n", err.Error())
	} else {
		for cur.Next(context.Background()) {
			result := new(PoolWeight)
			err := cur.Decode(result)
			if err != nil {
				log.Printf("alloc: refreshPoolWeight: error when decoding pool referral space: %s\n", err.Error())
				continue
			}
			if pws[result.ID] != nil {
				pws[result.ID].PoolReferralSpace = result.PoolReferralSpace
			}
		}
		cur.Close(context.Background())
	}

	for _, p := range pws {
		p.ManualWeight = 100
		_, err = collectionPW.InsertOne(context.Background(), p)
		if err != nil {
			errstr := err.Error()
			if !strings.ContainsAny(errstr, "duplicate key error") {
				log.Printf("alloc: refreshPoolWeight: error when inserting pool weight %s to database: %s\n", p.ID, err.Error())
				continue
			} else {
				_, err := collectionPW.UpdateOne(context.Background(), bson.M{"_id": p.ID}, bson.M{"$set": bson.M{"poolTotalSpace": p.PoolTotalSpace, "totalSpace": totalSpace, "poolTotalCount": p.PoolTotalCount, "poolErrorCount": p.PoolErrorCount, "timestamp": p.Timestamp}})
				if err != nil {
					log.Printf("alloc: refreshPoolWeight: error when updating pool weight %s in database: %s\n", p.ID, err.Error())
					continue
				}
			}
		}
		threshold := float64(s.Config.PoolErrorMinerPercentageThreshold) / 100
		value := float64(p.PoolTotalCount-p.PoolErrorCount) / float64(p.PoolTotalCount)
		if value < threshold {
			log.Printf("alloc: refreshPoolWeight: warning: error miners in pool %s have reached %f%%\n", p.ID, (1-value)*100)
		}
	}

	_, err = collectionPW.DeleteMany(context.Background(), bson.M{"timestamp": bson.M{"$ne": now}})
	if err != nil {
		log.Printf("alloc: refreshPoolWeight: error when deleting expired pool weight in database: %s\n", err.Error())
		return err
	}
	return nil
}

func (s *NodesSelector) refresh(nodeMgr *NodeDaoImpl) error {
	log.Printf("alloc: refresh: refreshing node map\n")
	nodeIDs := make([]int32, 0)
	regionMap := make(map[string]*WRegion)
	collection := nodeMgr.client.Database(YottaDB).Collection(NodeTab)
	cond := bson.M{"valid": 1, "status": 1, "assignedSpace": bson.M{"$gt": 0}, "quota": bson.M{"$gt": 0}, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*s.Config.AvaliableMinerTimeGap}, "weight": bson.M{"$gt": 0}, "manualWeight": bson.M{"$gt": 0}, "version": bson.M{"$gte": s.Config.MinerVersionThreshold}}
	cur, err := collection.Find(context.Background(), cond)
	if err != nil {
		log.Printf("alloc: refresh: error when refreshing NodeSelector: %s\n", err)
		return err
	}
	defer cur.Close(context.Background())
	nodeCount := 0
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(result)
		if err != nil {
			log.Printf("alloc: refresh: error when decoding allocatable nodes: %s\n", err.Error())
			continue
		}
		// if result.Weight <= 0 {
		// 	log.Printf("alloc: refresh: skip node %d during refreshing, cause its weight is 0\n", result.ID)
		// 	continue
		// }
		region := s.GetRegionInfo(result)
		if regionMap[region] == nil {
			wr := new(WRegion)
			wr.Name = region
			wr.Ranges = make([]int64, 0)
			wr.Nodes = make(map[int64]*Node)
			regionMap[region] = wr
		}
		regionMap[region].Sum += int64(result.Weight)
		regionMap[region].Nodes[regionMap[region].Sum] = result
		regionMap[region].Ranges = append(regionMap[region].Ranges, regionMap[region].Sum)
		nodeIDs = append(nodeIDs, result.ID)
		nodeCount++
	}
	root := new(WRoot)
	root.Ranges = make([]int64, 0)
	root.Regions = make(map[int64]*WRegion)
	root.Index = make(map[string]int64)
	for _, v := range regionMap {
		root.Sum += v.Sum
		root.Regions[root.Sum] = v
		root.Ranges = append(root.Ranges, root.Sum)
		root.Index[v.Name] = v.Sum
		log.Printf("alloc: refresh: total weight of %s: %d\n", v.Name, v.Sum)
	}
	sort.Sort(Int32Slice(nodeIDs))
	s.Root = root
	s.nodeIDs = nodeIDs
	log.Printf("alloc: refresh: refreshing finished, %d nodes can be allocated\n", nodeCount)
	return nil
}

//RandomNodeID select a random node ID
func (s *NodesSelector) RandomNodeID() (int32, error) {
	nodeIDs := s.nodeIDs
	if len(nodeIDs) == 0 {
		return 0, fmt.Errorf("no node can be allocated")
	}
	index := rand.Int31n(int32(len(nodeIDs)))
	return index, nil
}

// Alloc nodes for client
func (s *NodesSelector) Alloc(shardCount int32, errids []int32) ([]*Node, error) {
	log.Printf("alloc: Alloc: allocating %d nodes, errids: %v\n", shardCount, errids)
	sort.Sort(Int32Slice(errids))
	rg := s.Root.Ranges
	regionMap := s.Root.Regions
	totalWeight := s.Root.Sum
	nodeIDs := s.nodeIDs
	errids = exclude(errids, nodeIDs)
	nodes := make([]*Node, 0)
	t := len(s.nodeIDs)
	log.Printf("alloc: Alloc: count of allocatable nodes: %d\n", t)
	if t-len(errids) <= 0 {
		log.Printf("alloc: Alloc: warning: no enough data nodes can be allocted, count of allocatable nodes->%d, count of error node list->%d\n", t, len(errids))
		return nil, errors.New("no enough data nodes can be allocted")
	}
	if int(shardCount)+len(errids) >= t {
		s := shardCount / int32(t-len(errids))
		m := shardCount % int32(t-len(errids))
		if s > 0 {
			log.Printf("alloc: Alloc: duplicate allocate: s=%d, m=%d\n", s, m)
			ns := make([]*Node, 0)
			for _, rm := range regionMap {
				for _, v := range rm.Nodes {
					if binarySearch32(errids, v.ID) == -1 {
						ns = append(ns, v)
					}
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
	// allcNodeIds := make(map[int32]bool)
	for i := 0; i < int(shardCount); i++ {
		r := rand.Int63n(totalWeight)
		index := rangeSearch(rg, r)
		n := rg[index]
		nodeRegion := regionMap[n]
		if index > 0 {
			r -= rg[index-1]
		}
		index = rangeSearch(nodeRegion.Ranges, r)
		n = nodeRegion.Ranges[index]
		id := nodeRegion.Nodes[n].ID
		//log.Printf("alloc: allocate node: %d\n", id)
		if binarySearch32(errids, id) != -1 {
			//log.Printf("alloc: node in error list, skip: %d\n", id)
			i--
			continue
		}
		// if allcNodeIds[id] {
		// 	log.Printf("alloc: node have been allocated, skip: %d\n", id)
		// 	i--
		// 	continue
		// }
		// allcNodeIds[id] = true
		nodes = append(nodes, nodeRegion.Nodes[n])
	}
	log.Printf("alloc: Alloc: allocated %d nodes\n", len(nodes))
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
