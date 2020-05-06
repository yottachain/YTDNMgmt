package YTDNMgmt

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"sync/atomic"
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
	if enableSpotCheck {
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
}

func (self *NodeDaoImpl) doRecheck() {
	pool := grpool.NewPool(500, 1000)
	defer pool.Release()
	collection := self.client.Database(YottaDB).Collection(SpotCheckTab)
	collectionNode := self.client.Database(YottaDB).Collection(NodeTab)
	collectionErr := self.client.Database(YottaDB).Collection(ErrorNodeTab)
	for {
		if atomic.LoadInt32(&(self.master)) == 0 {
			time.Sleep(time.Second * time.Duration(10))
			continue
		}
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
				_, err = collection.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
				if err != nil {
					log.Printf("spotcheck: doRecheck: error when deleting task related a non-exist node: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
				}
				continue
			}
			if n.Status > 1 {
				_, err = collection.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
				if err != nil {
					log.Printf("spotcheck: doRecheck: error when deleting task with node status bigger than 1: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
				}
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
					// if i+1 == int(rebuildPhase) {
					// 	_, err = collectionNode.UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
					// 	if err != nil {
					// 		log.Printf("spotcheck: doRecheck: error when updating rebuild status: %s\n", err.Error())
					// 	}
					// 	err = self.eostx.CalculateProfit(n.Owner, uint64(n.ID), false)
					// 	if err != nil {
					// 		log.Printf("spotcheck: doRecheck: error when stopping profit calculation: %s\n", err.Error())
					// 	}
					// 	flag = false
					// }

					if i+1 == int(punishPhase1) {
						log.Printf("spotcheck: doRecheck: miner %d has been offline for %d seconds, do phase1 punishment\n", spr.NID, punishGapUnit*int64(i+1))
						if punishPhase1Percent > 0 {
							if self.ifNeedPunish(n.PoolOwner) {
								left, err := self.Punish(n, int64(punishPhase1Percent))
								if err != nil {
									log.Printf("spotcheck: doRecheck: error when punishing %d%%: %s\n", punishPhase1Percent, err.Error())
								} else if flag && left == 0 {
									log.Printf("spotcheck: doRecheck: no deposit can be punished: %d\n", n.ID)
									spr.Status = 3
									_, err := collectionErr.InsertOne(context.Background(), spr)
									if err != nil {
										log.Printf("spotcheck: doRecheck: error when inserting deposit exhausted task to error node collection: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
										break
									}
									_, err = collection.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
									if err != nil {
										log.Printf("spotcheck: doRecheck: error when deleting deposit exhausted task: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
										collectionErr.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
										break
									}
									_, err = collectionNode.UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
									if err != nil {
										log.Printf("spotcheck: doRecheck: error when updating rebuild status: %s\n", err.Error())
									}
									// err = self.eostx.CalculateProfit(n.Owner, uint64(n.ID), false)
									// if err != nil {
									// 	log.Printf("spotcheck: doRecheck: error when stopping profit calculation: %s\n", err.Error())
									// }
									flag = false
								}
							}
						}
						break
					}
					if i+1 == int(punishPhase2) {
						log.Printf("spotcheck: doRecheck: miner %d has been offline for %d seconds, do phase2 punishment\n", spr.NID, punishGapUnit*int64(i+1))
						if punishPhase2Percent > 0 {
							if self.ifNeedPunish(n.PoolOwner) {
								left, err := self.Punish(n, int64(punishPhase2Percent))
								if err != nil {
									log.Printf("spotcheck: doRecheck: error when punishing %d%%: %s\n", punishPhase2Percent, err.Error())
								} else if flag && left == 0 {
									log.Printf("spotcheck: doRecheck: no deposit can be punished: %d\n", n.ID)
									spr.Status = 3
									_, err := collectionErr.InsertOne(context.Background(), spr)
									if err != nil {
										log.Printf("spotcheck: doRecheck: error when inserting deposit exhausted task to error node collection: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
										break
									}
									_, err = collection.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
									if err != nil {
										log.Printf("spotcheck: doRecheck: error when deleting deposit exhausted task: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
										collectionErr.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
										break
									}
									_, err = collectionNode.UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
									if err != nil {
										log.Printf("spotcheck: doRecheck: error when updating exhausted Node %d: %s\n", n.ID, err.Error())
									}
									// err = self.eostx.CalculateProfit(n.Owner, uint64(n.ID), false)
									// if err != nil {
									// 	log.Printf("spotcheck: doRecheck: error when stopping profit calculation: %s\n", err.Error())
									// }
									flag = false
								}
							}
						}
						break
					}
					if i+1 == int(punishPhase3) {
						log.Printf("spotcheck: doRecheck: miner %d has been offline for %d seconds, do phase3 punishment\n", spr.NID, punishGapUnit*int64(i+1))
						spr.Status = 1
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
						_, err = collectionNode.UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
						if err != nil {
							log.Printf("spotcheck: doRecheck: error when updating rebuild status: %s\n", err.Error())
						}
						// err = self.eostx.CalculateProfit(n.Owner, uint64(n.ID), false)
						// if err != nil {
						// 	log.Printf("spotcheck: doRecheck: error when stopping profit calculation: %s\n", err.Error())
						// }
						if punishPhase3Percent > 0 {
							if self.ifNeedPunish(n.PoolOwner) {
								_, err = self.Punish(n, int64(punishPhase3Percent))
								if err != nil {
									log.Printf("spotcheck: doRecheck: error when doing phase3 punishment: %s\n", err.Error())
								}
							}
						}
						flag = false
					}
				}
			}

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

func (self *NodeDaoImpl) ifNeedPunish(poolOwner string) bool {
	collection := self.client.Database(YottaDB).Collection(PoolWeightTab)
	result := new(PoolWeight)
	err := collection.FindOne(context.Background(), bson.M{"_id": poolOwner}).Decode(result)
	if err != nil {
		log.Printf("spotcheck: ifNeedPunish: error happens when get pool weight of %s: %s\n", poolOwner, err.Error())
		return false
	}
	if result.PoolTotalCount == 0 {
		return false
	}
	threshold := float64(errorNodePercentThreshold) / 100
	value := float64(result.PoolTotalCount-result.PoolErrorCount) / float64(result.PoolTotalCount)
	if value < threshold {
		return true
	}
	return false
}

func (self *NodeDaoImpl) checkDataNode(spr *SpotCheckRecord) {
	log.Printf("spotcheck: checkDataNode: SN rechecking task: %d -> %s -> %s\n", spr.NID, spr.TaskID, spr.VNI)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	collectionSpotCheck := self.client.Database(YottaDB).Collection(SpotCheckTab)
	collectionErr := self.client.Database(YottaDB).Collection(ErrorNodeTab)
	n := new(Node)
	err := collection.FindOne(context.Background(), bson.M{"_id": spr.NID}).Decode(n)
	if err != nil {
		log.Printf("spotcheck: checkDataNode: decode node which performing rechecking error: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
		_, err = collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
		if err != nil {
			log.Printf("spotcheck: checkDataNode: error happens when update task %s status to 1: %s\n", spr.TaskID, err.Error())
		}
		return
	}
	if n.Status > 1 {
		_, err := collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
		if err != nil {
			log.Printf("spotcheck: checkDataNode: error happens when update task %s status to 1 whose node status is bigger than 1: %s\n", spr.TaskID, err.Error())
		}
		return
	}
	b, err := self.CheckVNI(n, spr)
	if err != nil {
		log.Printf("spotcheck: checkDataNode: vni check error: %d -> %s -> %s: %s\n", spr.NID, spr.TaskID, spr.VNI, err.Error())
		_, err := collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
		if err != nil {
			log.Printf("spotcheck: checkDataNode: error happens when update task %s status to 1: %s\n", spr.TaskID, err.Error())
		}
	} else {
		if b {
			_, err := collectionSpotCheck.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
			if err != nil {
				log.Printf("spotcheck: checkDataNode: error happens when delete task %s: %s\n", spr.TaskID, err.Error())
			} else {
				log.Printf("spotcheck: checkDataNode: vni check success: %d -> %s -> %s\n", spr.NID, spr.TaskID, spr.VNI)
			}
		} else {
			errstr := ""
			if err != nil {
				errstr = err.Error()
			}
			log.Printf("spotcheck: checkDataNode: vni check failed, checking 100 more VNIs: %d -> %s -> %s: %s\n", spr.NID, spr.TaskID, spr.VNI, errstr)
			i := 0
			var errCount int64 = 0
			for range [100]byte{} {
				i++
				spr.VNI, err = self.GetRandomVNI(n.ID)
				if err != nil {
					log.Printf("spotcheck: checkDataNode: get random vni%d error: %d -> %s -> %s: %s\n", i, spr.NID, spr.TaskID, spr.VNI, err.Error())
					_, err := collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
					if err != nil {
						log.Printf("spotcheck: checkDataNode: error happens when update task %s status to 1: %s\n", spr.TaskID, err.Error())
					}
					return
				}
				b, err := self.CheckVNI(n, spr)
				if err != nil {
					log.Printf("spotcheck: checkDataNode: vni%d check error: %d -> %s -> %s: %s\n", i, spr.NID, spr.TaskID, spr.VNI, err.Error())
					_, err := collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
					if err != nil {
						log.Printf("spotcheck: checkDataNode: error happens when update task %s status to 1: %s\n", spr.TaskID, err.Error())
					}
					return
				}
				if !b {
					errCount++
				}
			}
			spr.ErrCount = errCount
			spr.Status = 2
			log.Printf("spotcheck: checkDataNode: finished 100 VNIs check, %d verify errors in %d checks\n", errCount, i)
			if errCount == 100 {
				_, err := collectionErr.InsertOne(context.Background(), spr)
				if err != nil {
					log.Printf("spotcheck: checkDataNode: error when insert verify error task to error node collection: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
					return
				}
			}
			_, err = collectionSpotCheck.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
			if err != nil {
				log.Printf("spotcheck: checkDataNode: error when delete verify error task: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
				_, err := collectionErr.DeleteOne(context.Background(), bson.M{"_id": spr.TaskID})
				if err != nil {
					log.Printf("spotcheck: checkDataNode: error happens when delete task %s: %s\n", spr.TaskID, err.Error())
				}
				_, err = collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 1}})
				if err != nil {
					log.Printf("spotcheck: checkDataNode: error happens when update task %s status to 1: %s\n", spr.TaskID, err.Error())
				}
				return
			}
			if errCount == 100 {
				log.Printf("spotcheck: checkDataNode: 100/100 random VNI checking of miner %d has failed, punish %d%% deposit\n", spr.NID, punishPhase3Percent)
				if punishPhase3Percent > 0 {
					if self.ifNeedPunish(n.PoolOwner) {
						_, err := self.Punish(n, int64(punishPhase3Percent))
						if err != nil {
							log.Printf("spotcheck: checkDataNode: error happens when punishing 50%% deposit: %s\n", err.Error())
						}
					}
				}
				// err = self.eostx.CalculateProfit(n.ProfitAcc, uint64(n.ID), false)
				// if err != nil {
				// 	log.Printf("spotcheck: checkDataNode: error when stopping profit calculation: %s\n", err.Error())
				// }
				_, err = collection.UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
				if err != nil {
					log.Printf("spotcheck: checkDataNode: error when updating rebuild status: %s\n", err.Error())
				}
			} else {
				log.Printf("spotcheck: checkDataNode: %d/100 random VNI checking of miner %d has failed, punish %d%% deposit\n", errCount, spr.NID, punishPhase1Percent)
				if punishPhase1Percent > 0 {
					if self.ifNeedPunish(n.PoolOwner) {
						left, err := self.Punish(n, int64(punishPhase1Percent))
						if err != nil {
							log.Printf("spotcheck: checkDataNode: error happens when punishing 1%% deposit: %s\n", err.Error())
						} else if left == 0 {
							log.Printf("spotcheck: checkDataNode: no deposit can be punished: %d\n", spr.NID)
							spr.Status = 3
							_, err := collectionErr.InsertOne(context.Background(), spr)
							if err != nil {
								log.Printf("spotcheck: checkDataNode: error when inserting deposit exhausted task to error node collection: %d -> %s -> %s\n", spr.NID, spr.TaskID, err.Error())
								return
							}
							// err = self.eostx.CalculateProfit(n.ProfitAcc, uint64(n.ID), false)
							// if err != nil {
							// 	log.Printf("spotcheck: checkDataNode: error when stopping profit calculation: %s\n", err.Error())
							// }
							_, err = collection.UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
							if err != nil {
								log.Printf("spotcheck: checkDataNode: error when updating rebuild status: %s\n", err.Error())
							}
							_, err = collectionSpotCheck.UpdateOne(context.Background(), bson.M{"_id": spr.TaskID}, bson.M{"$set": bson.M{"status": 3}})
							if err != nil {
								log.Printf("spotcheck: checkDataNode: error when updating error node status to %d: %s\n", 3, err.Error())
							}
						}
					}
				}
			}
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
	_, err := self.host2.lhost.Connect(ctx, req)
	if err != nil {
		return false, err
	}
	defer self.host2.lhost.DisConnect(context.Background(), &pbh.StringMsg{Value: node.NodeID})

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
	shardData, err := self.host2.SendMsg(node.NodeID, append(pb.MsgIDDownloadShardRequest.Bytes(), checkData...))
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
func (self *NodeDaoImpl) GetRandomVNI(id int32) (string, error) {
	collection := self.client.Database(YottaDB).Collection(DNITab)
	startTime := spotCheckSkipTime
	var limit int64 = 1
	opt := options.FindOptions{}
	opt.Limit = &limit
	if startTime == 0 {
		opt.Sort = bson.M{"_id": 1}
		firstDNI := new(DNI)
		cur0, err := collection.Find(context.Background(), bson.M{"minerID": id, "delete": 0}, &opt)
		if err != nil {
			return "", fmt.Errorf("find first VNI failed: %s", err.Error())
		}
		defer cur0.Close(context.Background())
		if cur0.Next(context.Background()) {
			err := cur0.Decode(firstDNI)
			if err != nil {
				return "", fmt.Errorf("error when decoding first DNI: %s", err.Error())
			}
		} else {
			return "", fmt.Errorf("cannot find first DNI")
		}
		startTime = firstDNI.ID.Timestamp().Unix()
	}
	opt.Sort = bson.M{"_id": -1}
	lastDNI := new(DNI)
	cur1, err := collection.Find(context.Background(), bson.M{"minerID": id, "delete": 0}, &opt)
	if err != nil {
		return "", fmt.Errorf("find last VNI failed: %s", err.Error())
	}
	defer cur1.Close(context.Background())
	if cur1.Next(context.Background()) {
		err := cur1.Decode(lastDNI)
		if err != nil {
			return "", fmt.Errorf("error when decoding last DNI: %s", err.Error())
		}
	} else {
		return "", fmt.Errorf("cannot find last DNI")
	}
	endTime := lastDNI.ID.Timestamp().Unix()
	if startTime > endTime {
		return "", fmt.Errorf("no valid VNIs can be spotchecked")
	}
	delta := rand.Int63n(endTime - startTime)
	selectedTime := startTime + delta
	selectedID := primitive.NewObjectIDFromTimestamp(time.Unix(selectedTime, 0))
	selectedDNI := new(DNI)
	cur2, err := collection.Find(context.Background(), bson.M{"minerID": id, "delete": 0, "_id": bson.M{"$gte": selectedID}}, &opt)
	if err != nil {
		return "", fmt.Errorf("find random VNI failed: %s", err.Error())
	}
	defer cur2.Close(context.Background())
	if cur2.Next(context.Background()) {
		err := cur2.Decode(selectedDNI)
		if err != nil {
			return "", fmt.Errorf("error when decoding random DNI: %s", err.Error())
		}
	} else {
		return "", fmt.Errorf("cannot find random DNI")
	}

	// startTime := primitive.NewObjectIDFromTimestamp(time.Unix(spotCheckSkipTime, 0))
	// t := strings.Join([]string{startTime.Hex()[:8], "0000000000000000"}, "")
	// startTime, err := primitive.ObjectIDFromHex(string(t))
	// if err != nil {
	// 	return "", fmt.Errorf("parse skipt time object ID failed: %s", err.Error())
	// }
	// collection := self.client.Database(YottaDB).Collection(DNITab)
	// pipeline := mongo.Pipeline{
	// 	{{"$match", bson.D{{"minerID", id}, {"delete", 0}, {"_id", bson.D{{"$gte", startTime}}}}}},
	// 	{{"$sample", bson.D{{"size", 1}}}},
	// }
	// cur, err := collection.Aggregate(context.Background(), pipeline)
	// if err != nil {
	// 	return "", fmt.Errorf("error when finding random VNI: %s", err.Error())
	// }
	// defer cur.Close(context.Background())
	// selectedDNI := new(DNI)
	// if cur.Next(context.Background()) {
	// 	err := cur.Decode(selectedDNI)
	// 	if err != nil {
	// 		return "", fmt.Errorf("error when decoding random DNI: %s", err.Error())
	// 	}
	// } else {
	// 	return "", fmt.Errorf("cannot find random DNI")
	// }

	// var limit int64 = 1
	// opt := options.FindOptions{}
	// opt.Sort = bson.M{"shard": 1}
	// opt.Limit = &limit
	// beginDNI := new(DNI)
	// cur1, err := collection.Find(context.Background(), bson.M{"minerID": id, "delete": 0, "_id": bson.M{"$gt": startTime}}, &opt)
	// if err != nil {
	// 	return "", fmt.Errorf("find begin VNI failed: %s", err.Error())
	// }
	// defer cur1.Close(context.Background())
	// if cur1.Next(context.Background()) {
	// 	err := cur1.Decode(beginDNI)
	// 	if err != nil {
	// 		return "", fmt.Errorf("error when decoding begin DNI: %s", err.Error())
	// 	}
	// 	fmt.Printf("begin DNI: %s\n", base64.StdEncoding.EncodeToString(beginDNI.Shard.Data))
	// } else {
	// 	return "", fmt.Errorf("cannot find begin DNI")
	// }
	// opt.Sort = bson.M{"shard": -1}
	// endDNI := new(DNI)
	// cur2, err := collection.Find(context.Background(), bson.M{"minerID": id, "delete": 0, "_id": bson.M{"$gt": startTime}}, &opt)
	// if err != nil {
	// 	return "", fmt.Errorf("find end VNI failed: %s", err.Error())
	// }
	// defer cur2.Close(context.Background())
	// if cur2.Next(context.Background()) {
	// 	err := cur2.Decode(endDNI)
	// 	if err != nil {
	// 		return "", fmt.Errorf("error when decoding end DNI: %s", err.Error())
	// 	}
	// 	fmt.Printf("end DNI: %s\n", base64.StdEncoding.EncodeToString(endDNI.Shard.Data))
	// } else {
	// 	return "", fmt.Errorf("cannot find end DNI")
	// }
	// begin := new(big.Int)
	// begin.SetBytes(beginDNI.Shard.Data)
	// fmt.Printf("begin index: %d\n", begin)
	// end := new(big.Int)
	// end.SetBytes(endDNI.Shard.Data)
	// fmt.Printf("end index: %d\n", end)
	// delta := end.Sub(end, begin)
	// fmt.Printf("delta: %d\n", delta)
	// deltaRand, err := brand.Int(brand.Reader, delta)
	// if err != nil {
	// 	return "", fmt.Errorf("generate random VNI failed: %s", err.Error())
	// }
	// fmt.Printf("delta rand: %d\n", deltaRand)
	// randDNIBig := deltaRand.Add(deltaRand, begin)
	// fmt.Printf("rand: %d\n", randDNIBig)
	// selectedDNI := new(DNI)
	// err = collection.FindOne(context.Background(), bson.M{"minerID": id, "_id": bson.M{"$gt": startTime}, "delete": 0, "shard": bson.M{"$gte": randDNIBig.Bytes()}}).Decode(selectedDNI)
	// if err != nil {
	// 	return "", fmt.Errorf("find random VNI failed: %s", err.Error())
	// }
	return base64.StdEncoding.EncodeToString(selectedDNI.Shard.Data), nil
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
			spotCheckTask.VNI, err = self.GetRandomVNI(node.ID)
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
				log.Printf("spotcheck: GetSpotCheckList: error when getting spotcheck vni: %d %s\n", node.ID, err.Error())
				continue
			}

			//delete expired task
			_, err = collectionSpotCheck.DeleteMany(context.Background(), bson.M{"nid": spotCheckTask.ID, "status": 0, "timestamp": bson.M{"$lt": time.Now().Unix() - 3600*24}})
			if err != nil {
				log.Printf("spotcheck: GetSpotCheckList: error when deleting timeout spotcheck task of node: %d %s\n", node.ID, err.Error())
			}

			return spotCheckLists, nil
		}
		cur.Close(context.Background())
		continue
	}
	log.Printf("spotcheck: GetSpotCheckList: no nodes can be spotchecked\n")
	return nil, errors.New("no nodes can be spotchecked")
}

func (self *NodeDaoImpl) Punish(node *Node, percent int64) (int64, error) {
	// rate, err := self.eostx.GetExchangeRate()
	// if err != nil {
	// 	return 0, err
	// }
	pledgeData, err := self.eostx.GetPledgeData(uint64(node.ID))
	if err != nil {
		return 0, err
	}
	totalAsset := pledgeData.Total
	leftAsset := pledgeData.Deposit
	punishAsset := pledgeData.Deposit

	var retLeft int64 = 0
	punishFee := int64(totalAsset.Amount) * percent / 100
	if punishFee < int64(punishAsset.Amount) {
		punishAsset.Amount = eos.Int64(punishFee)
		retLeft = int64(leftAsset.Amount - punishAsset.Amount)
	}
	err = self.eostx.DeducePledge(uint64(node.ID), &punishAsset)
	if err != nil {
		return 0, err
	}
	log.Printf("spotcheck: Punish: punish %d%% deposit of node %d: %d\n", percent, node.ID, punishFee)
	// //newAssignedSpace := node.AssignedSpace - int64(punishAsset.Amount)*65536*int64(rate)/1000000
	// newAssignedSpace := int64(leftAsset.Amount-punishAsset.Amount) * 65536 * int64(rate) / 1000000
	// if newAssignedSpace < 0 {
	// 	newAssignedSpace = 0
	// }
	// collection := self.client.Database(YottaDB).Collection(NodeTab)
	// // invalid over 24 hours begin data rebuilding
	// // if mark {
	// // 	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"status": 2, "tasktimestamp": int64(0)}})
	// // 	if err != nil {
	// // 		return err
	// // 	}
	// // }
	// _, err = collection.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"assignedSpace": newAssignedSpace}})
	// if err != nil {
	// 	return 0, err
	// }
	return retLeft, nil
}

// GetSTNode find a random miner to perform spotcheck
func (self *NodeDaoImpl) GetSTNode() (*Node, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	c, err := collection.CountDocuments(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}, "usedSpace": bson.M{"$gt": 0}, "status": 1, "version": bson.M{"$gte": minerVersionThreshold}})
	if err != nil {
		return nil, errors.New("spotcheck: error when get count of spotcheckable nodes")
	}
	d, err := collection.CountDocuments(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}, "status": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*avaliableNodeTimeGap}, "version": bson.M{"$gte": minerVersionThreshold}})
	if err != nil {
		return nil, errors.New("spotcheck: error when get count of spotcheck-executing nodes")
	}
	if c == 0 || d == 0 {
		return NewNode(0, "", "", "", "", "", "", 0, nil, 0, 0, 0, 0, int64(spotcheckInterval), c, d, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ""), nil
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
	if n < c && enableSpotCheck {
		return NewNode(1, "", "", "", "", "", "", 0, nil, 0, 0, 0, 0, int64(spotcheckInterval), c, d, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ""), nil
	}
	return NewNode(0, "", "", "", "", "", "", 0, nil, 0, 0, 0, 0, int64(spotcheckInterval), c, d, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ""), nil
}

//GetSTNodes get spotcheck nodes by count
func (self *NodeDaoImpl) GetSTNodes(count int64) ([]*Node, error) {
	options := options.FindOptions{}
	options.Sort = bson.D{{"timestamp", -1}}
	options.Limit = &count
	nodes := make([]*Node, 0)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.M{"_id": bson.M{"$mod": bson.A{incr, index}}, "valid": 1, "status": 1, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*avaliableNodeTimeGap}, "version": bson.M{"$gte": minerVersionThreshold}}, &options)
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
