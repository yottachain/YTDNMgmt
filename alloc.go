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
	"time"

	"github.com/ipipdotnet/ipdb-go"
	"go.mongodb.org/mongo-driver/bson"
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

// GetRegionInfo get region infomation from a IP address
func (s *NodesSelector) GetRegionInfo(node *Node) string {
	maddrPub := CheckPublicAddr(node.Addrs)
	if maddrPub == "" {
		return ""
	}
	str := strings.TrimPrefix(maddrPub, "/ip4/")
	str = str[0:strings.Index(str, "/")]
	m, err := s.db.FindMap(str, "EN")
	if err != nil {
		log.Printf("alloc: error happens when get region infomation from IPDB: %s\n", err.Error())
		return ""
	}
	if m["city_name"] == "" {
		return m["country_name"]
	}
	return m["city_name"]
}

// Start NodeSelector
func (s *NodesSelector) Start(ctx context.Context, nodeMgr *NodeDaoImpl) {
	log.Printf("alloc: NodeSelector is starting...\n")
	s.nodeIDs = make([]int32, 0)
	db, err := ipdb.NewCity(ipDBPath)
	if err != nil {
		log.Fatalf("alloc: fatal when read IP DB: %s %s\n", ipDBPath, err.Error())
	}
	s.rwlock = new(sync.RWMutex)
	s.db = db
	// update IP DB regularly
	go func() {
		for {
			time.Sleep(time.Hour * time.Duration(24))
			s.rwlock.Lock()
			log.Printf("alloc: refresh IP DB: %s\n", ipDBPath)
			err := s.db.Reload(ipDBPath)
			if err != nil {
				panic(fmt.Sprintf("alloc: %s\n", err.Error()))
			}
			s.rwlock.Unlock()
		}
	}()
	err = s.refresh(nodeMgr)
	if err != nil {
		panic(err)
	}
	ticker := time.NewTicker(time.Duration(nodeAllocRefreshInterval) * time.Minute)
	go func(t *time.Ticker) {
		for {
			select {
			case <-ctx.Done():
				log.Printf("alloc: stop refresh operation of NodeSelector\n")
				return
			case <-t.C:
				_ = s.refresh(nodeMgr)
			}
		}
	}(ticker)
}

func (s *NodesSelector) refresh(nodeMgr *NodeDaoImpl) error {
	log.Printf("alloc: refreshing node map\n")
	regionMap := make(map[string]*WRegion)
	collection := nodeMgr.client.Database(YottaDB).Collection(NodeTab)
	cond := bson.M{"valid": 1, "status": 1, "assignedSpace": bson.M{"$gt": 0}, "quota": bson.M{"$gt": 0}, "cpu": bson.M{"$lt": 98}, "memory": bson.M{"$lt": 95}, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*avaliableNodeTimeGap}, "weight": bson.M{"$gt": 65536}, "version": bson.M{"$gte": minerVersionThreadshold}}
	cur, err := collection.Find(context.Background(), cond)
	if err != nil {
		log.Printf("alloc: error when refresh NodeSelector: %s\n", err)
		return err
	}
	defer cur.Close(context.Background())
	nodeCount := 0
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(result)
		if err != nil {
			log.Printf("alloc: error when refresh NodeSelector root: %s\n", err.Error())
			continue
		}
		if result.Weight <= 0 {
			log.Printf("alloc: skip node %d during refreshing, its weight is 0\n", result.ID)
			continue
		}
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
		s.nodeIDs = append(s.nodeIDs, result.ID)
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
		log.Printf("alloc: total weight of %s: %d\n", v.Name, v.Sum)
	}
	sort.Sort(Int32Slice(s.nodeIDs))
	s.Root = root
	log.Printf("refresh finished, %d nodes can be allocated\n", nodeCount)
	return nil
}

// Alloc nodes for client
func (s *NodesSelector) Alloc(shardCount int32, errids []int32) ([]*Node, error) {
	log.Printf("alloc: allocating %d nodes, errids: %v\n", shardCount, errids)
	sort.Sort(Int32Slice(errids))
	rg := s.Root.Ranges
	regionMap := s.Root.Regions
	totalWeight := s.Root.Sum
	nodeIDs := s.nodeIDs
	errids = exclude(errids, nodeIDs)
	nodes := make([]*Node, 0)
	t := len(s.nodeIDs)
	log.Printf("alloc: count of allocatable nodes: %d\n", t)
	if t-len(errids) <= 0 {
		return nil, errors.New("no enough data nodes can be allocted")
	}
	if int(shardCount)+len(errids) >= t {
		s := shardCount / int32(t-len(errids))
		m := shardCount % int32(t-len(errids))
		if s > 0 {
			log.Printf("alloc: duplicate allocate: s=%d, m=%d\n", s, m)
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
	log.Printf("alloc: allocated %d nodes", len(nodes))
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
