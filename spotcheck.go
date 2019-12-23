package YTDNMgmt

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/eoscanada/eos-go"
	proto "github.com/golang/protobuf/proto"
	"github.com/ivpusic/grpool"
	pbh "github.com/yottachain/P2PHost/pb"
	pb "github.com/yottachain/YTDNMgmt/pb"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func (self *NodeDaoImpl) StartRecheck() {
	log.Printf("spotcheck: StartRecheck: starting recheck executor...\n")
	log.Printf("spotcheck: StartRecheck: clear spotcheck tasks...\n")
	collection := self.client.Database(YottaDB).Collection(SpotCheckTab)
	_, err := collection.DeleteMany(context.Background(), bson.M{"status": 0})
	if err != nil {
		log.Printf("spotcheck: StartRecheck: error happens when deleting spotcheck tasks with status 0: %s\n", err.Error())
	}
	_, err = collection.UpdateMany(context.Background(), bson.M{"status": 2}, bson.M{"$set": bson.M{"status": 1}})
	if err != nil {
		log.Printf("spotcheck: StartRecheck: error happens when update spotcheck tasks with status 2 to 1: %s\n", err.Error())
	}
	go self.doRecheck()
}

func (self *NodeDaoImpl) doRecheck() {
	pool := grpool.NewPool(500, 1000)
	defer pool.Release()
	collection := self.client.Database(YottaDB).Collection(SpotCheckTab)
	collectionNode := self.client.Database(YottaDB).Collection(NodeTab)
	collectionErr := self.client.Database(YottaDB).Collection(ErrorNodeTab)
	for {
		cur, err := collection.Find(context.Background(), bson.M{"status": 1})
		if err != nil {
			log.Printf("spotcheck: doRecheck: error when selecting recheckable task: %s\n", err.Error())
			time.Sleep(time.Second * time.Duration(10))
			continue
		}
		for cur.Next(context.Background()) {
			var flag = true
			spr := new(SpotCheckRecord)
			err := cur.Decode(spr)
			if err != nil {
				log.Printf("spotcheck: doRecheck: error when decoding recheckable task: %s\n", err.Error())
				continue
			}
			n := new(Node)
			err = collectionNode.FindOne(context.Background(), bson.M{"_id": spr.NID}).Decode(n)
			if err != nil {
				log.Printf("spotcheck: doRecheck: error when decoding node: %s\n", err.Error())
				continue
			}

			t := time.Now().Unix() - spr.Timestamp
			for i := range [10080]byte{} {
				if t > punishGapUnit*int64(i+1) && spr.ErrCount == int64(i) {
					_, err = collection.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"errCount": i + 1}})
					if err != nil {
						log.Printf("spotcheck: doRecheck: error when updating error count: %s\n", err.Error())
						break
					}
					spr.ErrCount = int64(i + 1)
					if i+1 == rebuildPhase {
						_, err = collectionNode.UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
						if err != nil {
							log.Printf("spotcheck: doRecheck: error when updating rebuild status: %s\n", err.Error())
						}
					}

					if i+1 == punishPhase1 {
						log.Printf("spotcheck: doRecheck: miner %d has been offline for %d seconds, do phase1 punishment\n", spr.NID, punishGapUnit*int64(i+1))
						err := self.Punish(n, 10, false)
						if err != nil {
							log.Printf("spotcheck: doRecheck: error when punishing: %s\n", err.Error())
						}
						break
					}
					if i+1 == punishPhase2 {
						log.Printf("spotcheck: doRecheck: miner %d has been offline for %d seconds, do phase2 punishment\n", spr.NID, punishGapUnit*int64(i+1))
						err := self.Punish(n, 40, false)
						if err != nil {
							log.Printf("spotcheck: doRecheck: error when punishing: %s\n", err.Error())
						}
						break
					}
					if i+1 == punishPhase3 {
						log.Printf("spotcheck: doRecheck: miner %d has been offline for %d seconds, do phase3 punishment\n", spr.NID, punishGapUnit*int64(i+1))
						_, err := collectionErr.InsertOne(context.Background(), spr)
						if err != nil {
							log.Printf("spotcheck: doRecheck: error when inserting timeout task to error node collection: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
							break
						}
						_, err = collection.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
						if err != nil {
							log.Printf("spotcheck: doRecheck: error when deleting timeout task: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
							collectionErr.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
							break
						}
						err = self.Punish(n, 100, false)
						if err != nil {
							log.Printf("spotcheck: doRecheck: error when doing phase3 punishment: %s\n", err.Error())
						}
						err = self.eostx.CalculateProfit(n.Owner, uint64(n.ID), false)
						if err != nil {
							log.Printf("spotcheck: doRecheck: error when stopping profit calculation: %s\n", err.Error())
						}
						flag = false
					}
				}
			}

			// if t > punishGapUnit*4 && spr.ErrCount == 0 {
			// 	log.Printf("spotcheck: miner %d has been offline for 4 hours, punish 10%% deposit\n", spr.NID)
			// 	err := self.Punish(n, 10, false)
			// 	if err != nil {
			// 		log.Printf("warning: spotcheck: error happens when punishing 10%%: %s\n", err.Error())
			// 	}
			// 	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"errCount": 1}})
			// 	if err != nil {
			// 		log.Printf("warning: spotcheck: error happens when update error count: %s\n", err.Error())
			// 	}
			// }
			// if t > punishGapUnit*8 && spr.ErrCount == 1 {
			// 	log.Printf("spotcheck: miner %d has been offline for 8 hours, punish 40%% deposit\n", spr.NID)
			// 	err := self.Punish(n, 40, false)
			// 	if err != nil {
			// 		log.Printf("warning: spotcheck: error happens when punishing 40%%: %s\n", err.Error())
			// 	}
			// 	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"errCount": 2}})
			// 	if err != nil {
			// 		log.Printf("warning: spotcheck: error happens when update error count: %s\n", err.Error())
			// 	}
			// }
			// if t > punishGapUnit*24 && spr.ErrCount == 2 {
			// 	log.Printf("spotcheck: miner %d has been offline for 24 hours, punish 100%% deposit\n", spr.NID)
			// 	_, err := collectionErr.InsertOne(context.Background(), spr)
			// 	if err != nil {
			// 		log.Printf("spotcheck: error when insert timeout task to error node collection: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
			// 		cur.Close(context.Background())
			// 		continue
			// 	}
			// 	_, err = collection.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
			// 	if err != nil {
			// 		log.Printf("spotcheck: error when delete timeout task: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
			// 		collectionErr.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
			// 		cur.Close(context.Background())
			// 		continue
			// 	}
			// 	err = self.Punish(n, 100, true)
			// 	if err != nil {
			// 		log.Printf("warning: spotcheck: error happens when punishing 100%%: %s\n", err.Error())
			// 	}
			// 	err = self.eostx.CalculateProfit(n.Owner, uint64(n.ID), false)
			// 	if err != nil {
			// 		log.Printf("warning: spotcheck: error happens when stop profit calculation: %s\n", err.Error())
			// 	}
			// 	continue
			// }

			if flag {
				//calculate probability for rechecking
				r := rand.Int63n(10 * int64(spotcheckInterval))
				if r < 10 {
					// rechecking
					_, err = collection.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 2}})
					if err != nil {
						log.Printf("spotcheck: error when update recheckable task to status 2: %s\n", err.Error())
						cur.Close(context.Background())
						continue
					}
					spr.Status = 2
					pool.JobQueue <- func() {
						self.checkDataNode(spr)
					}
				}
			}
		}
		cur.Close(context.Background())
		time.Sleep(time.Second * time.Duration(10))
	}
}

func (self *NodeDaoImpl) checkDataNode(spr *SpotCheckRecord) {
	log.Printf("spotcheck: SN rechecking task: %d -> %s -> %s\n", spr.NID, spr.TaskID, spr.VNI)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	collectionSpotCheck := self.client.Database(YottaDB).Collection(SpotCheckTab)
	collectionErr := self.client.Database(YottaDB).Collection(ErrorNodeTab)
	n := new(Node)
	err := collection.FindOne(context.Background(), bson.M{"_id": spr.NID}).Decode(n)
	if err != nil {
		log.Printf("spotcheck: decode node which performing rechecking error: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
		_, err = collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
		if err != nil {
			log.Printf("warning: spotcheck: error happens when update task %s status to 1: %s\n", spr.TaskID, err.Error())
		}
		return
	}
	b, err := self.CheckVNI(n, spr)
	if err != nil {
		_, err := collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
		if err != nil {
			log.Printf("warning: spotcheck: error happens when update task %s status to 1: %s\n", spr.TaskID, err.Error())
		} else {
			log.Printf("spotcheck: vni check error: %d -> %s -> %s\n", spr.NID, spr.TaskID, spr.VNI)
		}
	} else {
		if b {
			_, err := collectionSpotCheck.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
			if err != nil {
				log.Printf("warning: spotcheck: error happens when delete task %s: %s\n", spr.TaskID, err.Error())
			} else {
				log.Printf("spotcheck: vni check success: %d -> %s -> %s\n", spr.NID, spr.TaskID, spr.VNI)
			}
		} else {
			log.Printf("spotcheck: vni check failed, checking 100 more VNIs: %d -> %s -> %s\n", spr.NID, spr.TaskID, spr.VNI)
			i := 0
			var errCount int64 = 0
			for range [100]byte{} {
				i++
				spr.VNI, err = self.GetRandomVNI(n.ID, rand.Int63n(n.UsedSpace))
				if err != nil {
					_, err := collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
					if err != nil {
						log.Printf("warning: spotcheck: error happens when update task %s status to 1: %s\n", spr.TaskID, err.Error())
					} else {
						log.Printf("spotcheck: get random vni%d error: %d -> %s -> %s\n", i, spr.NID, spr.TaskID, spr.VNI)
					}
					return
				}
				b, err := self.CheckVNI(n, spr)
				if err != nil {
					_, err := collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
					if err != nil {
						log.Printf("warning: spotcheck: error happens when update task %s status to 1: %s\n", spr.TaskID, err.Error())
					} else {
						log.Printf("spotcheck: vni%d check error: %d -> %s -> %s\n", i, spr.NID, spr.TaskID, spr.VNI)
					}
					return
				}
				if !b {
					errCount++
				}
			}
			spr.ErrCount = errCount
			log.Printf("spotcheck: finished 100 VNIs check, %d verify errors in %d checks\n", errCount, i)
			_, err := collectionErr.InsertOne(context.Background(), spr)
			if err != nil {
				log.Printf("spotcheck: error when insert verify error task to error node collection: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
				return
			}
			_, err = collectionSpotCheck.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
			if err != nil {
				log.Printf("spotcheck: error when delete verify error task: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
				_, err := collectionErr.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
				if err != nil {
					log.Printf("warning: spotcheck: error happens when delete task %s: %s\n", spr.TaskID, err.Error())
				}
				_, err = collection.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
				if err != nil {
					log.Printf("warning: spotcheck: error happens when update task %s status to 1: %s\n", spr.TaskID, err.Error())
				}
			}
			self.Punish(n, 100, true)
		}
	}
}

//CheckVNI check whether vni is correct
func (self *NodeDaoImpl) CheckVNI(node *Node, spr *SpotCheckRecord) (bool, error) {
	// return false, errors.New("fake result of CheckVNI")
	// log.Printf("spotcheck: check VNI of node: %d -> %s\n", node.ID, spr.VNI)
	// maddrs, err := stringListToMaddrs(node.Addrs)
	// if err != nil {
	// 	log.Printf("spotcheck: error when check VNI of node: %d -> %s -> %s\n", node.ID, spr.VNI, err.Error())
	// 	return false, err
	// }
	// pid, err := peer.IDB58Decode(node.NodeID)
	// if err != nil {
	// 	log.Printf("spotcheck: error when check VNI of node: %d -> %s -> %s\n", node.ID, spr.VNI, err.Error())
	// 	return false, err
	// }
	// info := ps.PeerInfo{
	// 	pid,
	// 	maddrs,
	// }
	ctx, cancle := context.WithTimeout(context.Background(), time.Second*time.Duration(connectTimeout))
	defer cancle()
	// defer self.host.lhost.Network().ClosePeer(pid)
	// defer self.host.lhost.Network().(*swarm.Swarm).Backoff().Clear(pid)
	// err = self.host.lhost.Connect(ctx, info)
	// if err != nil {
	// 	log.Printf("spotcheck: connect node failed: %d -> %s -> %s\n", node.ID, spr.VNI, err.Error())
	// 	return false, err
	// }
	req := &pbh.ConnectReq{Id: node.NodeID, Addrs: node.Addrs}
	_, err := self.host.lhost.Connect(ctx, req)
	if err != nil {
		return false, err
	}
	defer self.host.lhost.DisConnect(context.Background(), &pbh.StringMsg{Value: node.NodeID})

	rawvni, err := base64.StdEncoding.DecodeString(spr.VNI)
	if err != nil {
		log.Printf("spotcheck: error when unmarshaling VNI: %d %s %s\n", node.ID, spr.VNI, err.Error())
		return false, err
	}
	rawvni = rawvni[len(rawvni)-16:]
	downloadRequest := &pb.DownloadShardRequest{VHF: rawvni}
	checkData, err := proto.Marshal(downloadRequest)
	if err != nil {
		log.Printf("spotcheck: error when marshalling protobuf message: downloadrequest: %d -> %s -> %s\n", node.ID, spr.VNI, err.Error())
		return false, err
	}
	shardData, err := self.host.SendMsg(node.NodeID, append(pb.MsgIDDownloadShardRequest.Bytes(), checkData...))
	if err != nil {
		log.Printf("spotcheck: SN send recheck command failed: %d -> %s -> %s\n", node.ID, spr.VNI, err.Error())
		return false, err
	}
	var share pb.DownloadShardResponse
	err = proto.Unmarshal(shardData[2:], &share)
	if err != nil {
		log.Printf("spotcheck: SN unmarshal recheck response failed: %d -> %s -> %s\n", node.ID, spr.VNI, err.Error())
		return false, err
	}
	if downloadRequest.VerifyVHF(share.Data) {
		return true, nil
	} else {
		return false, nil
	}
}

// UpdateTaskStatus process error task of spotchecking
func (self *NodeDaoImpl) UpdateTaskStatus(taskid string, invalidNodeList []int32) error {
	collection := self.client.Database(YottaDB).Collection(SpotCheckTab)
	for _, id := range invalidNodeList {
		spr := new(SpotCheckRecord)
		err := collection.FindOne(context.Background(), bson.M{"_id": taskid}).Decode(spr)
		if err != nil {
			log.Printf("spotcheck: UpdateTaskStatus: error when fetching spotcheck task in mongodb: %d -> %s -> %s\n", id, taskid, err.Error())
			continue
		}
		if spr.Status > 0 {
			log.Printf("spotcheck: UpdateTaskStatus: task is already under rechecking: %d -> %s -> %s\n", id, taskid, err.Error())
			continue
		}
		sping := new(SpotCheckRecord)
		err = collection.FindOne(context.Background(), bson.M{"nid": id, "status": bson.M{"$gt": 0}}).Decode(sping)
		if err != nil {
			log.Printf("spotcheck: UpdateTaskStatus: update task status to 1: %s\n", taskid)
			_, err := collection.UpdateOne(context.Background(), bson.M{"_id": taskid}, bson.M{"$set": bson.M{"status": 1, "timestamp": time.Now().Unix()}})
			if err != nil {
				log.Printf("spotcheck: UpdateTaskStatus: error happens when updating task %s status to 1: %s\n", spr.TaskID, err.Error())
			}
		}
		_, err = collection.DeleteMany(context.Background(), bson.M{"nid": id, "status": 0})
		if err != nil {
			log.Printf("spotcheck: UpdateTaskStatus: error when deleting tasks with node ID %d: %s\n", id, err.Error())
		}
	}
	return nil
}

// GetRandomVNI find one VNI by miner ID and index of DNI table
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
}

// GetSpotCheckList creates a spotcheck task
func (self *NodeDaoImpl) GetSpotCheckList() ([]*SpotCheckList, error) {
	spotCheckLists := make([]*SpotCheckList, 0)
	spotCheckList := new(SpotCheckList)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	collectionSpotCheck := self.client.Database(YottaDB).Collection(SpotCheckTab)
	for range [10]byte{} {
		total, err := collection.CountDocuments(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}, "usedSpace": bson.M{"$gt": 0}, "assignedSpace": bson.M{"$gt": 0}, "status": 1})
		if err != nil {
			log.Printf("spotcheck: GetSpotCheckList: error when calculating total count of spotcheckable nodes: %s\n", err.Error())
			continue
		}
		if total == 0 {
			break
		}
		n := rand.Intn(int(total))
		optionf := new(options.FindOptions)
		skip := int64(n)
		limit := int64(1)
		optionf.Limit = &limit
		optionf.Skip = &skip
		cur, err := collection.Find(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}, "usedSpace": bson.M{"$gt": 0}, "assignedSpace": bson.M{"$gt": 0}, "status": 1}, optionf)
		if err != nil {
			log.Printf("spotcheck: GetSpotCheckList: error when fetching spotcheckable node: %s\n", err.Error())
			continue
		}
		if cur.Next(context.Background()) {
			node := new(Node)
			err := cur.Decode(node)
			if err != nil {
				log.Printf("spotcheck: GetSpotCheckList: error when decoding spot task: %s\n", err.Error())
				cur.Close(context.Background())
				continue
			}
			log.Printf("spotcheck: GetSpotCheckList: node %d wil be spotchecked\n", node.ID)
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
				log.Printf("spotcheck: GetSpotCheckList: error when selecting random vni: %d %s\n", node.ID, err.Error())
				cur.Close(context.Background())
				continue
			}
			log.Printf("spotcheck: GetSpotCheckList: select random VNI for node %d -> %s\n", node.ID, spotCheckTask.VNI)
			spotCheckList.TaskID = primitive.NewObjectID()
			spotCheckList.Timestamp = time.Now().Unix()
			spotCheckList.TaskList = append(spotCheckList.TaskList, spotCheckTask)
			spotCheckLists = append(spotCheckLists, spotCheckList)
			cur.Close(context.Background())

			sping := new(SpotCheckRecord)
			err = collectionSpotCheck.FindOne(context.Background(), bson.M{"nid": spotCheckTask.ID, "status": bson.M{"$gt": 0}}).Decode(sping)
			if err == nil {
				log.Printf("spotcheck: GetSpotCheckList: warning: node %d is already under rechecking: %s\n", spotCheckTask.ID, sping.TaskID)
				_, err = collectionSpotCheck.DeleteMany(context.Background(), bson.M{"nid": spotCheckTask.ID, "status": 0})
				if err != nil {
					log.Printf("spotcheck: GetSpotCheckList: error when deleting spotcheck task when another task with same node is under rechecking: %d %s\n", node.ID, err.Error())
				}
				return nil, fmt.Errorf("a spotcheck task with node id %d is under rechecking: %s", spotCheckTask.ID, spotCheckList.TaskID)
			}

			spr := &SpotCheckRecord{TaskID: spotCheckList.TaskID.Hex(), NID: spotCheckTask.ID, VNI: spotCheckTask.VNI, Status: 0, Timestamp: spotCheckList.Timestamp}
			_, err = collectionSpotCheck.InsertOne(context.Background(), spr)
			if err != nil {
				log.Printf("spotcheck: error when get spotcheck vni: %d %s\n", node.ID, err.Error())
				continue
			}

			//delete expired task
			_, err = collectionSpotCheck.DeleteMany(context.Background(), bson.M{"nid": spotCheckTask.ID, "status": 0, "timestamp": bson.M{"$lt": time.Now().Unix() - 3600*24}})
			if err != nil {
				log.Printf("warning: spotcheck: error when delete timeout spotcheck task of node: %d %s\n", node.ID, err.Error())
			}

			return spotCheckLists, nil
		}
		cur.Close(context.Background())
		continue
	}
	log.Printf("spotcheck: get spotcheck list: no nodes can be spotchecked\n")
	return nil, errors.New("no nodes can be spotchecked")
}

func (self *NodeDaoImpl) Punish(node *Node, percent int64, mark bool) error {
	rate, err := self.eostx.GetExchangeRate()
	if err != nil {
		return err
	}
	pledgeData, err := self.eostx.GetPledgeData(uint64(node.ID))
	if err != nil {
		return err
	}
	totalAsset := pledgeData.Total
	leftAsset := pledgeData.Deposit
	punishAsset := pledgeData.Deposit

	punishFee := int64(totalAsset.Amount) * percent / 100
	if punishFee < int64(punishAsset.Amount) {
		punishAsset.Amount = eos.Int64(punishFee)
	}
	err = self.eostx.DeducePledge(uint64(node.ID), &punishAsset)
	if err != nil {
		return err
	}
	//newAssignedSpace := node.AssignedSpace - int64(punishAsset.Amount)*65536*int64(rate)/1000000
	newAssignedSpace := int64(leftAsset.Amount-punishAsset.Amount) * 65536 * int64(rate) / 1000000
	if newAssignedSpace < 0 {
		newAssignedSpace = 0
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	// invalid over 24 hours begin data rebuilding
	if mark {
		_, err = collection.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
		if err != nil {
			return err
		}
	}
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"assignedSpace": newAssignedSpace}})
	if err != nil {
		return err
	}
	return nil
}

// GetSTNode find a random miner to perform spotcheck
func (self *NodeDaoImpl) GetSTNode() (*Node, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	c, err := collection.CountDocuments(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}, "usedSpace": bson.M{"$gt": 0}, "status": 1, "version": bson.M{"$gte": minerVersionThreadshold}})
	if err != nil {
		return nil, errors.New("spotcheck: error when get count of spotcheckable nodes")
	}
	d, err := collection.CountDocuments(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}, "status": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*avaliableNodeTimeGap}, "version": bson.M{"$gte": minerVersionThreadshold}})
	if err != nil {
		return nil, errors.New("spotcheck: error when get count of spotcheck-executing nodes")
	}
	if c == 0 || d == 0 {
		return NewNode(0, "", "", "", "", "", 0, nil, 0, 0, 0, 0, int64(spotcheckInterval), c, d, 0, 0, 0, 0, 0, 0), nil
	}
	if float32(c)/float32(d) > 2 {
		c = 2000
		d = 1000
	}
	if d < 100 {
		c = c * 100
		d = d * 100
	}
	n := rand.Int63n(d * int64(spotcheckInterval))
	if n < c {
		return NewNode(1, "", "", "", "", "", 0, nil, 0, 0, 0, 0, int64(spotcheckInterval), c, d, 0, 0, 0, 0, 0, 0), nil
	}
	return NewNode(0, "", "", "", "", "", 0, nil, 0, 0, 0, 0, int64(spotcheckInterval), c, d, 0, 0, 0, 0, 0, 0), nil
}

//GetSTNodes get spotcheck nodes by count
func (self *NodeDaoImpl) GetSTNodes(count int64) ([]*Node, error) {
	options := options.FindOptions{}
	options.Sort = bson.D{{"timestamp", -1}}
	options.Limit = &count
	nodes := make([]*Node, 0)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}, "valid": 1, "status": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*avaliableNodeTimeGap}, "version": bson.M{"$gte": minerVersionThreadshold}}, &options)
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
		return nil, errors.New("no enough spot check nodes")
	}
	return nodes, nil
}

func (self *NodeDaoImpl) SaveSpotCheckList(list *SpotCheckList) (*SpotCheckList, error) {
	// collection := self.client.Database(YottaDB).Collection(SpotCheckTab)
	// _, err := collection.DeleteMany(context.Background(), bson.M{})
	// if err != nil {
	// 	return nil, err
	// }
	// spr := new(SpotCheckRecord)
	// spr.TaskID = list.TaskID
	// spr.Nodes = make(map[string]int)
	// spr.Timestamp = list.Timestamp
	// _, err = collection.InsertOne(context.Background(), spr)
	// if err != nil {
	// 	return nil, err
	// }
	// return list, nil
	return nil, errors.New("deprecated")
}

func (self *NodeDaoImpl) SaveErrorNodeIDs() error {
	// collection := self.client.Database(YottaDB).Collection(SpotCheckTab)
	// task := new(SpotCheckRecord)
	// err := collection.FindOne(context.Background(), bson.M{}).Decode(task)
	// if err != nil {
	// 	return nil
	// }
	// invIDs := make([]int, 0)
	// for key, value := range task.Nodes {
	// 	if int32(value) > 1 {
	// 		k, err := strconv.Atoi(key)
	// 		if err != nil {
	// 			return err
	// 		}
	// 		invIDs = append(invIDs, k)
	// 	}
	// }
	// collection = self.client.Database(YottaDB).Collection(ErrorNodeTab)
	// _, err = collection.DeleteMany(context.Background(), bson.M{})
	// if err != nil {
	// 	return err
	// }
	// _, err = collection.InsertOne(context.Background(), bson.M{"_id": 1, "nodes": invIDs})
	// if err != nil {
	// 	return err
	// }
	// return nil
	return errors.New("deprecated")
}
