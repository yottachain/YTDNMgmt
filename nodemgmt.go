package YTDNMgmt

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/aurawing/auramq"
	"github.com/aurawing/auramq/msg"
	"github.com/aurawing/eos-go"
	proto "github.com/golang/protobuf/proto"
	"github.com/mr-tron/base58"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/yottachain/YTDNMgmt/eostx"
	pb "github.com/yottachain/YTDNMgmt/pb"
	nodesync "github.com/yottachain/YTDNMgmt/sync"
)

//NodeDaoImpl node manipulator implemention
type NodeDaoImpl struct {
	client      *mongo.Client
	eostx       *eostx.EosTX
	host        *Host
	ns          *NodesSelector
	bpID        int32
	master      int32
	syncService *nodesync.Service
	Config      *Config
}

var incr int64 = 0
var index int32 = -1

//NewInstance create a new instance of NodeDaoImpl
func NewInstance(mongoURL, eosURL, bpAccount, bpPrivkey, contractOwnerM, contractOwnerD, shadowAccount string, bpID int32, isMaster int32, config *Config) (*NodeDaoImpl, error) {
	if config.Misc.EnableTest {
		YottaDB = fmt.Sprintf("%s%d", YottaDB, bpID)
	}
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURL))
	if err != nil {
		log.Printf("nodemgmt: NewInstance: error when creating mongodb client: %s %s\n", mongoURL, err.Error())
		return nil, err
	}
	log.Printf("nodemgmt: NewInstance: create mongodb client: %s\n", mongoURL)
	etx, err := eostx.NewInstance(eosURL, bpAccount, bpPrivkey, contractOwnerM, contractOwnerD, shadowAccount)
	if err != nil {
		log.Printf("nodemgmt: NewInstance: error when creating eos client failed: %s %s\n", eosURL, err.Error())
		return nil, err
	}
	log.Printf("nodemgmt: NewInstance: create eos client: %s\n", eosURL)
	host, err := NewHost(config.Misc)
	if err != nil {
		log.Printf("nodemgmt: NewInstance: error when creating host failed: %s\n", err.Error())
		return nil, err
	}
	log.Println("nodemgmt: NewInstance: create host")
	dao := &NodeDaoImpl{client: client, eostx: etx, host: host, ns: &NodesSelector{Config: config.Misc}, bpID: bpID, master: isMaster}
	log.Printf("nodemgmt: NewInstance: master status is %d\n", isMaster)
	//dao.StartRecheck()
	dao.ns.Start(context.Background(), dao)
	if incr == 0 {
		collection := client.Database(YottaDB).Collection(SuperNodeTab)
		c, err := collection.CountDocuments(context.Background(), bson.D{})
		if err != nil {
			log.Printf("nodemgmt: NewInstance: error when calculating count of supernode failed: %s\n", err.Error())
			return nil, err
		}
		log.Printf("nodemgmt: NewInstance: count of supernode: %d\n", c)
		incr = c
		config.SNCount = c
	}
	if index == -1 {
		index = bpID
		log.Printf("nodemgmt: NewInstance: index of SN: %d\n", index)
	}
	go func() {
		http.HandleFunc("/quit", func(w http.ResponseWriter, r *http.Request) {
			r.ParseForm()
			mineridstr := r.Form.Get("minerid")
			if mineridstr == "" {
				io.WriteString(w, "矿机ID不存在！\n")
				return
			}
			minerid, err := strconv.Atoi(mineridstr)
			if err != nil {
				io.WriteString(w, fmt.Sprintf("解析矿机ID失败：%s\n", err.Error()))
				return
			}
			percentStr := r.Form.Get("percent")
			if percentStr == "" {
				percentStr = "100"
			}
			percent, err := strconv.Atoi(percentStr)
			if err != nil {
				io.WriteString(w, fmt.Sprintf("解析扣抵押百分比失败：%s\n", err.Error()))
				return
			}
			s, err := dao.MinerQuit(int32(minerid), int32(percent))
			if err != nil {
				io.WriteString(w, fmt.Sprintf("扣抵押失败：%s\n", err.Error()))
				return
			}
			io.WriteString(w, fmt.Sprintf("扣抵押成功：%s\n", s))
		})
		http.HandleFunc("/batchquit", func(w http.ResponseWriter, r *http.Request) {
			r.ParseForm()
			mineridstr := r.Form.Get("minerid")
			if mineridstr == "" {
				io.WriteString(w, "矿机ID不存在！\n")
				return
			}
			minerid, err := strconv.Atoi(mineridstr)
			if err != nil {
				io.WriteString(w, fmt.Sprintf("解析矿机ID失败：%s\n", err.Error()))
				return
			}
			percentStr := r.Form.Get("percent")
			if percentStr == "" {
				io.WriteString(w, "扣抵押百分比不存在！\n")
				return
			}
			percent, err := strconv.Atoi(percentStr)
			if err != nil {
				io.WriteString(w, fmt.Sprintf("解析扣抵押百分比失败：%s\n", err.Error()))
				return
			}

			s, err := dao.BatchMinerQuit(int32(minerid), int32(percent))
			if err != nil {
				io.WriteString(w, fmt.Sprintf("扣抵押失败：%s\n", err.Error()))
				return
			}
			io.WriteString(w, fmt.Sprintf("扣抵押成功：%s\n", s))
		})
		http.HandleFunc("/change_weight", func(w http.ResponseWriter, r *http.Request) {
			r.ParseForm()
			mineridstr := r.Form.Get("minerid")
			if mineridstr == "" {
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, "矿机ID不存在！\n")
				return
			}
			minerid, err := strconv.Atoi(mineridstr)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, fmt.Sprintf("解析矿机ID失败：%s\n", err.Error()))
				return
			}
			weightStr := r.Form.Get("weight")
			if weightStr == "" {
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, "权重参数不存在！\n")
				return
			}
			weight, err := strconv.Atoi(weightStr)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, fmt.Sprintf("解析权重参数失败：%s\n", err.Error()))
				return
			}
			if weight < 0 {
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, fmt.Sprintf("权重参数不正确：%d\n", weight))
				return
			}
			err = dao.ChangeManualWeight(int32(minerid), int32(weight))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				io.WriteString(w, fmt.Sprintf("修改权重失败：%s\n", err.Error()))
				return
			}
			io.WriteString(w, fmt.Sprintln("修改权重成功"))
		})
		http.ListenAndServe("0.0.0.0:12345", nil)
	}()

	callback := func(msg *msg.Message) {
		log.Printf("nodemgmt: synccallback: received information of type %d from %s to %s\n", msg.Type, msg.Sender, msg.Destination)
		if msg.GetType() == auramq.BROADCAST {
			if msg.GetDestination() == config.AuraMQ.MinerSyncTopic {
				nodemsg := new(pb.NodeMsg)
				err := proto.Unmarshal(msg.Content, nodemsg)
				if err != nil {
					log.Println("nodemgmt: synccallback: decoding nodeMsg failed:", err)
					return
				}
				node := new(Node)
				node.Fillby(nodemsg)
				dao.SyncNode(node)
			}
		} else if msg.GetType() == auramq.P2P {
			if msg.GetDestination() == fmt.Sprintf("sn%d", config.SNID) {
				b := msg.GetContent()
				msgType := b[0]
				content := b[1:]
				if msgType == byte(UpdateUspaceMessage) {
					umsg := new(pb.UpdateUspaceMessage)
					err := proto.Unmarshal(content, umsg)
					if err != nil {
						log.Println("nodemgmt: synccallback: error when unmarshaling UpdateUspaceMessage:", err)
						return
					}
					log.Printf("nodemgmt: synccallback: received update uspace message of node %d from %s to %s\n", umsg.NodeID, msg.Sender, msg.Destination)
					dao.updateUspace(umsg)
				} else if msgType == byte(PunishMessage) {
					pmsg := new(pb.PunishMessage)
					err := proto.Unmarshal(content, pmsg)
					if err != nil {
						log.Println("nodemgmt: synccallback: error when unmarshaling PunishMessage:", err)
						return
					}
					log.Printf("nodemgmt: synccallback: received punish message of node %d from %s to %s\n", pmsg.NodeID, msg.Sender, msg.Destination)
					collection := dao.client.Database(YottaDB).Collection(NodeTab)
					node := new(Node)
					err = collection.FindOne(context.Background(), bson.M{"_id": pmsg.NodeID}).Decode(node)
					if err != nil {
						log.Printf("nodemgmt: synccallback: error when decoding struct of miner %d: %s\n", pmsg.NodeID, err.Error())
						return
					}
					if node.Status > 1 {
						if b, err := proto.Marshal(node.Convert()); err != nil {
							log.Printf("nodemgmt: synccallback: marshal node %d failed: %s\n", node.ID, err)
						} else {
							if dao.syncService != nil {
								log.Println("nodemgmt: synccallback: publish information of node", node.ID)
								dao.syncService.Publish("sync", b)
							}
						}
						return
					}
					if pmsg.Type == 0 {
						errorCount := pmsg.Count
						ruleMap := pmsg.Rule
						keys := make([]int32, 0, len(ruleMap))
						for k := range ruleMap {
							keys = append(keys, k)
						}
						sort.Sort(Int32Slice(keys))
						_, err = collection.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"errorCount": errorCount}})
						if err != nil {
							log.Printf("nodemgmt: synccallback: error when updating error count of miner %d to %d: %s\n", node.ID, errorCount, err.Error())
							return
						}
						if pmsg.NeedPunish {
							var i int = 0
							var p int32 = 0
							if node.ErrorCount < int64(errorCount-1) {
								for i, p = range keys {
									if int32(node.ErrorCount) < p && errorCount > p {
										left, err := dao.punish(node.ID, int64(ruleMap[p]))
										if err != nil {
											return
										}
										if left == 0 {
											_, err = collection.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"status": 2}})
											if err != nil {
												log.Printf("nodemgmt: synccallback: error when updating status of miner %d to 2 of type 0: %s\n", node.ID, err.Error())
											}
											if b, err := proto.Marshal(node.Convert()); err != nil {
												log.Printf("nodemgmt: synccallback: marshal node %d failed: %s\n", node.ID, err)
											} else {
												if dao.syncService != nil {
													log.Println("nodemgmt: synccallback: publish information of node", node.ID)
													dao.syncService.Publish("sync", b)
												}
											}
											return
										}
									}
								}
							}
							for i, p = range keys {
								if errorCount == p {
									left, err := dao.punish(node.ID, int64(ruleMap[p]))
									if err != nil {
										return
									}
									if left == 0 || (i == len(keys)-1) {
										_, err = collection.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"status": 2}})
										if err != nil {
											log.Printf("nodemgmt: synccallback: error when updating status of miner %d to 2 of type 0: %s\n", node.ID, err.Error())
										}
										if b, err := proto.Marshal(node.Convert()); err != nil {
											log.Printf("nodemgmt: synccallback: marshal node %d failed: %s\n", node.ID, err)
										} else {
											if dao.syncService != nil {
												log.Println("nodemgmt: synccallback: publish information of node", node.ID)
												dao.syncService.Publish("sync", b)
											}
										}
										return
									}
									break
								}
							}
						}
					} else if pmsg.Type == 1 && pmsg.NeedPunish {
						left, err := dao.punish(node.ID, int64(pmsg.Count))
						if err != nil {
							return
						}
						if left == 0 {
							_, err = collection.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"status": 2}})
							if err != nil {
								log.Printf("nodemgmt: synccallback: error when updating status of miner %d to 2 of type 1: %s\n", node.ID, err.Error())
							}
							if b, err := proto.Marshal(node.Convert()); err != nil {
								log.Printf("nodemgmt: synccallback: marshal node %d failed: %s\n", node.ID, err)
							} else {
								if dao.syncService != nil {
									log.Println("nodemgmt: synccallback: publish information of node", node.ID)
									dao.syncService.Publish("sync", b)
								}
							}
							return
						}
					} else if pmsg.Type == 2 && pmsg.NeedPunish {
						_, err := dao.punish(node.ID, int64(pmsg.Count))
						if err != nil {
							return
						}
						_, err = collection.UpdateOne(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": bson.M{"status": 2}})
						if err != nil {
							log.Printf("nodemgmt: synccallback: error when updating status of miner %d to 2 of type 2: %s\n", node.ID, err.Error())
						}
						if b, err := proto.Marshal(node.Convert()); err != nil {
							log.Printf("nodemgmt: synccallback: marshal node %d failed: %s\n", node.ID, err)
						} else {
							if dao.syncService != nil {
								log.Println("nodemgmt: synccallback: publish information of node", node.ID)
								dao.syncService.Publish("sync", b)
							}
						}
						return
					}
				}
			}
		}
	}
	syncService, err := nodesync.StartSync(etx, config.AuraMQ.BindAddr, config.AuraMQ.RouterBufferSize, config.AuraMQ.SubscriberBufferSize, config.AuraMQ.ReadBufferSize, config.AuraMQ.WriteBufferSize, config.AuraMQ.PingWait, config.AuraMQ.ReadWait, config.AuraMQ.WriteWait, config.AuraMQ.MinerSyncTopic, int(config.SNID), config.AuraMQ.AllSNURLs, config.AuraMQ.AllowedAccounts, callback, shadowAccount, bpPrivkey, &dao.master)
	if err != nil {
		log.Fatalln("nodemgmt: NewInstance: fatal error when creating sync service:", err)
	}
	dao.syncService = syncService
	dao.Config = config
	log.Printf("nodemgmt: NewInstance: config is: %v\n", config)
	if config.PProf.Enable {
		go func() {
			err := http.ListenAndServe(config.PProf.BindAddr, nil)
			if err != nil {
				log.Printf("error when starting pprof server on address %s: %s\n", config.PProf.BindAddr, err)
			} else {
				log.Println("enable pprof server:", config.PProf.BindAddr)
			}
		}()
	}
	return dao, nil
}

func (self *NodeDaoImpl) punish(nodeID int32, percent int64) (int64, error) {
	log.Printf("nodemgmt: punish: punishing %d deposit of miner %d\n", percent, nodeID)
	rate, err := self.eostx.GetExchangeRate()
	if err != nil {
		return 0, err
	}
	pledgeData, err := self.eostx.GetPledgeData(uint64(nodeID))
	if err != nil {
		log.Printf("nodemgmt: punish: error when get pledge data of miner %d: %s\n", nodeID, err.Error())
		return 0, err
	}
	log.Printf("nodemgmt: punish: get pledge data of miner %d: %d/%d\n", nodeID, int64(pledgeData.Deposit.Amount), int64(pledgeData.Total.Amount))
	totalAsset := pledgeData.Total
	leftAsset := pledgeData.Deposit
	punishAsset := pledgeData.Deposit
	if leftAsset.Amount == 0 {
		log.Printf("nodemgmt: punish: no left deposit of miner %d\n", nodeID)
		return 0, nil
	}
	var retLeft int64 = 0
	punishFee := int64(totalAsset.Amount) * percent / 100
	if punishFee < int64(punishAsset.Amount) {
		punishAsset.Amount = eos.Int64(punishFee)
		retLeft = int64(leftAsset.Amount - punishAsset.Amount)
	}
	err = self.eostx.DeducePledge(uint64(nodeID), &punishAsset)
	if err != nil {
		log.Printf("nodemgmt: punish: error when punishing %f YTA of miner %d: %s\n", float64(punishFee)/10000, nodeID, err.Error())
		return 0, err
	}
	log.Printf("nodemgmt: punish: punishing %f YTA of miner %d\n", float64(punishFee)/10000, nodeID)
	newAssignedSpace := retLeft * 65536 * int64(rate) / 1000000
	if newAssignedSpace < 0 {
		newAssignedSpace = 0
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": nodeID}, bson.M{"$set": bson.M{"assignedSpace": newAssignedSpace}})
	if err != nil {
		return 0, err
	}
	return retLeft, nil
}

func (self *NodeDaoImpl) updateUspace(msg *pb.UpdateUspaceMessage) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err := collection.UpdateOne(context.Background(), bson.M{"_id": msg.NodeID}, bson.M{"$set": bson.M{fmt.Sprintf("uspaces.sn%d", msg.FromNodeID): msg.Uspace}})
	if err != nil {
		log.Printf("nodemgmt: updateUspace: error when update uspace of miner %d from miner %d: %s\n", msg.NodeID, msg.FromNodeID, err)
	}
}

//SetMaster change master status
func (self *NodeDaoImpl) SetMaster(master int32) {
	log.Printf("nodemgmt: SetMaster: master status is %d\n", master)
	atomic.StoreInt32(&(self.master), master)
}

//ChangeEosURL change EOS URL
func (self *NodeDaoImpl) ChangeEosURL(eosURL string) {
	self.eostx.ChangeEosURL(eosURL)
}

func getCurrentNodeIndex(client *mongo.Client) (int32, error) {
	collection := client.Database(YottaDB).Collection(SequenceTab)
	m := make(map[string]int32)
	err := collection.FindOne(context.Background(), bson.M{"_id": NodeIdxType}).Decode(&m)
	if err != nil {
		log.Printf("nodemgmt: getCurrentNodeIndex: index of SN: %d\n", index)
		return 0, err
	}
	return m["seq"], nil
}

//UpdateNodeStatus update data info by data node status
func (self *NodeDaoImpl) UpdateNodeStatus(node *Node) (*Node, error) {
	if node == nil {
		log.Println("nodemgmt: UpdateNodeStatus: warning: report info is null")
		return nil, errors.New("node is null")
	}
	if node.ID == 0 {
		log.Println("nodemgmt: UpdateNodeStatus: warning: node ID cannot be 0")
		return nil, errors.New("node ID cannot be 0")
	}
	if node.ID%int32(incr) != index {
		log.Printf("nodemgmt: UpdateNodeStatus: warning: node %d do not belong to current SN\n", node.ID)
		return nil, errors.New("node do not belong to this SN")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	n := new(Node)
	err := collection.FindOne(context.Background(), bson.M{"_id": node.ID}).Decode(n)
	if err != nil {
		log.Printf("nodemgmt: UpdateNodeStatus: error when decoding node %d: %s\n", node.ID, err.Error())
		return nil, err
	}
	if time.Now().Unix()-n.Timestamp < 20 {
		log.Printf("nodemgmt: UpdateNodeStatus: warning: reporting of node %d is too frequency\n", n.ID)
		return nil, errors.New("reporting is too frequency")
	}
	if n.Quota == 0 {
		log.Printf("nodemgmt: UpdateNodeStatus: warning: node %d has not been added to a pool\n", n.ID)
		return nil, fmt.Errorf("node %d has not been added to a pool", n.ID)
	}
	if n.AssignedSpace == 0 {
		log.Printf("nodemgmt: UpdateNodeStatus: warning: node %d has not been pledged or punished all deposit\n", n.ID)
	}
	// else if n.Status == 2 && n.Valid == 1 {
	// 	errNode := new(SpotCheckRecord)
	// 	collectionErr := self.client.Database(YottaDB).Collection(ErrorNodeTab)
	// 	err := collectionErr.FindOne(context.Background(), bson.M{"nid": n.ID}).Decode(errNode)
	// 	if err != nil {
	// 		log.Printf("nodemgmt: UpdateNodeStatus: warning: cannot find node %d in error node table\n", n.ID)
	// 	} else if errNode.Status == 1 { //timeout miner and deposit exhausted miner can be recovered
	// 		_, err := collectionErr.DeleteOne(context.Background(), bson.M{"nid": n.ID})
	// 		if err != nil {
	// 			log.Printf("nodemgmt: UpdateNodeStatus: error when deleting error node %d in error node table\n", n.ID)
	// 		} else {
	// 			_, err := collection.UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"status": 1}, "$unset": bson.M{"tasktimestamp": 1}})
	// 			if err != nil {
	// 				log.Printf("nodemgmt: UpdateNodeStatus: error when updating status of node %d from 2 to 1: %s\n", n.ID, err.Error())
	// 				collectionErr.InsertOne(context.Background(), errNode)
	// 			} else {
	// 				collectionDNI := self.client.Database(YottaDB).Collection(DNITab)
	// 				_, err := collectionDNI.DeleteMany(context.Background(), bson.M{"minerID": n.ID, "delete": 1})
	// 				if err != nil {
	// 					log.Printf("nodemgmt: UpdateNodeStatus: error when remove deleted shards of node %d: %s\n", n.ID, err.Error())
	// 				}
	// 				newUsedSpace, err := collectionDNI.CountDocuments(context.Background(), bson.M{"minerID": n.ID, "delete": 0})
	// 				if err != nil {
	// 					log.Printf("nodemgmt: UpdateNodeStatus: error when counting shards of node %d: %s\n", n.ID, err.Error())
	// 				} else {
	// 					_, err := collection.UpdateOne(context.Background(), bson.M{"_id": n.ID}, bson.M{"$set": bson.M{"usedSpace": newUsedSpace}})
	// 					if err != nil {
	// 						log.Printf("nodemgmt: UpdateNodeStatus: error when updating used space of node %d: %s\n", n.ID, err.Error())
	// 					}
	// 				}
	// 			}
	// 		}
	// 	}
	// }

	var assignedSpaceBP int64 = -1
	var productiveSpaceBP int64 = -1
	if rand.Int63n(int64(self.Config.Misc.BPSyncInterval)*100) < 100 {
		rate, err := self.eostx.GetExchangeRate()
		if err != nil {
			log.Printf("nodemgmt: UpdateNodeStatus: error when fetching exchange rate of miner %d from BP: %s\n", node.ID, err.Error())
		} else {
			pledgeData, err := self.eostx.GetPledgeData(uint64(node.ID))
			if err != nil {
				log.Printf("nodemgmt: UpdateNodeStatus: error when fetching pledge data of miner %d from BP: %s\n", node.ID, err.Error())
			} else {
				assignedSpaceBP = int64(pledgeData.Deposit.Amount) * 65536 * int64(rate) / 1000000
				log.Printf("nodemgmt: UpdateNodeStatus: sync assigned space of miner %d from BP: %d -> %d\n", node.ID, n.AssignedSpace, assignedSpaceBP)
				n.AssignedSpace = assignedSpaceBP
			}
		}
		minerInfo, err := self.eostx.GetMinerInfo(uint64(node.ID))
		if err != nil {
			log.Printf("nodemgmt: UpdateNodeStatus: error when fetching miner info of miner %d from BP: %s\n", node.ID, err.Error())
		}
		maxspace, err := strconv.ParseInt(string(minerInfo.MaxSpace), 10, 64)
		if err != nil {
			log.Printf("nodemgmt: UpdateNodeStatus: error when parsing max space(%s) of miner %d from BP: %s\n", string(minerInfo.MaxSpace), node.ID, err.Error())
		} else {
			spaceleft, err := strconv.ParseInt(string(minerInfo.SpaceLeft), 10, 64)
			if err != nil {
				log.Printf("nodemgmt: UpdateNodeStatus: error when parsing space left(%s) of miner %d from BP: %s\n", string(minerInfo.SpaceLeft), node.ID, err.Error())
			} else {
				productiveSpaceBP = maxspace - spaceleft
				log.Printf("nodemgmt: UpdateNodeStatus: sync productive space of miner %d from BP: %d -> %d\n", node.ID, n.ProductiveSpace, productiveSpaceBP)
				n.ProductiveSpace = productiveSpaceBP
			}
		}
	}

	node.Valid = n.Valid
	node.Addrs = CheckPublicAddrs(node.Addrs, self.Config.Misc.ExcludeAddrPrefix)
	relayURL, err := self.AddrCheck(n, node)
	if err != nil {
		log.Printf("nodemgmt: UpdateNodeStatus: error when checking public address of node %d: %s\n", n.ID, err.Error())
		return nil, err
	}
	if n.PoolID != "" && n.PoolOwner == "" {
		poolInfo, err := self.eostx.GetPoolInfoByPoolID(n.PoolID)
		if err != nil {
			log.Printf("nodemgmt: UpdateNodeStatus: error when get pool owner %d: %s\n", n.ID, err.Error())
			return nil, err
		}
		node.PoolOwner = string(poolInfo.Owner)
	} else {
		node.PoolOwner = n.PoolOwner
	}
	var status int32 = 1
	if n.Status > 1 {
		status = n.Status
	}
	var sum int64 = 0
	for _, v := range n.Uspaces {
		sum += v
	}
	usedSpace := Max(sum, n.UsedSpace)
	// calculate w1
	leftSpace := float64(Min(n.AssignedSpace, n.Quota, node.MaxDataSpace) - n.RealSpace)
	w11 := math.Atan(leftSpace/10000) * 1.6 / math.Pi
	w12 := 0.8
	if n.CPU >= 90 {
		w12 = math.Atan(math.Pow(1.02, math.Pow(1.02, 30*float64(100-n.CPU))-1)-1) * 1.6 / math.Pi
	}
	w13 := 0.8
	if n.Memory >= 80 {
		w13 = math.Atan(math.Pow(1.01, math.Pow(1.01, 30*float64(100-n.Memory))-1)-1) * 1.6 / math.Pi
	}
	w14 := 0.8
	if n.Bandwidth >= 80 {
		w14 = math.Atan(math.Pow(1.01, math.Pow(1.01, 30*float64(100-n.Bandwidth))-1)-1) * 1.6 / math.Pi
	}
	w1 := math.Sqrt(math.Sqrt(w11*w12*w13*w14)) + 0.6
	if leftSpace <= 655360 {
		w1 = 0
	}
	// calculate w2
	w2 := self.calculateW2(n.PoolOwner)
	// calculate w3
	w3 := float64(n.AssignedSpace) / 67108864
	// calculate w4
	w4 := 1.0
	if node.Rebuilding > 0 {
		w4 = 0.1
	}
	w5 := 1.0
	w6 := 1.0
	collectionPW := self.client.Database(YottaDB).Collection(PoolWeightTab)
	pw := new(PoolWeight)
	err = collectionPW.FindOne(context.Background(), bson.M{"_id": n.PoolOwner}).Decode(pw)
	if err != nil {
		log.Printf("nodemgmt: UpdateNodeStatus: warning when decoding pool weight of node %d: %s\n", n.ID, err.Error())
	} else {
		w5 = float64(pw.ManualWeight) / 100.0
		w6 = float64(pw.PoolTotalCount-pw.PoolErrorCount) / float64(pw.PoolTotalCount)
	}
	w7 := float64(n.ManualWeight) / 100.0
	weight := int64(float64(n.AssignedSpace) * w1 * w2 * w3 * w4 * w5 * w6 * w7)
	log.Printf("nodemgmt: UpdateNodeStatus: weight of miner %d is %d\n", n.ID, weight)
	if weight < 0 {
		weight = 0
	}
	opts := new(options.FindOneAndUpdateOptions)
	opts = opts.SetReturnDocument(options.After)
	timestamp := time.Now().Unix()

	otherDoc := bson.A{}
	if node.Ext != "" && node.Ext[0] == '[' && node.Ext[len(node.Ext)-1] == ']' {
		var bdoc interface{}
		err = bson.UnmarshalExtJSON([]byte(node.Ext), true, &bdoc)
		if err != nil {
			log.Printf("nodemgmt: UpdateNodeStatus: warning when parse ext document %s\n", err.Error())
		} else {
			otherDoc, _ = bdoc.(bson.A)
			enableExperiment := true
			if enableExperiment {
				if len(otherDoc) > 0 {
					params, ok := otherDoc[0].(bson.D)
					if ok {
						if _, ok := params.Map()["ytfs_error_count"]; ok {
							ytfsErrorCount, ok := params.Map()["ytfs_error_count"].(int32)
							if ok {
								log.Printf("nodemgmt: UpdateNodeStatus: miner%d's ytfs_error_count=%d\n", n.ID, ytfsErrorCount)
								if ytfsErrorCount >= 100 {
									log.Printf("nodemgmt: UpdateNodeStatus: miner%d's ytfs_error_count>100, set weight to 0\n", n.ID)
									weight = 0
								}
							} else {
								log.Printf("nodemgmt: UpdateNodeStatus: warning when converting ytfs_error_count to int32 of miner %d\n", n.ID)
							}
						} else {
							log.Printf("nodemgmt: UpdateNodeStatus: warning no ytfs_error_count property of miner %d\n", n.ID)
						}
					} else {
						log.Printf("nodemgmt: UpdateNodeStatus: warning when converting otherdoc to bson.M of miner %d\n", n.ID)
					}
				}
			}
		}
	}

	update := bson.M{"$set": bson.M{"poolOwner": node.PoolOwner, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "addrs": node.Addrs, "weight": weight, "valid": node.Valid, "relay": node.Relay, "status": status, "timestamp": timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc}}
	if assignedSpaceBP != -1 {
		s, ok := update["$set"].(bson.M)
		if ok {
			s["assignedSpace"] = assignedSpaceBP
		} else {
			log.Printf("nodemgmt: UpdateNodeStatus: warning when set assigned space update condition of node %d\n", n.ID)
		}
	}
	if productiveSpaceBP != -1 {
		s, ok := update["$set"].(bson.M)
		if ok {
			s["productiveSpace"] = productiveSpaceBP
		} else {
			log.Printf("nodemgmt: UpdateNodeStatus: warning when set productive space update condition of node %d\n", n.ID)
		}
	}
	log.Printf("nodemgmt: UpdateNodeStatus: update condition of node %d: %v\n", n.ID, update)
	result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": node.ID}, update, opts)
	err = result.Decode(n)
	if err != nil {
		log.Printf("nodemgmt: UpdateNodeStatus: error when decoding node %d: %s\n", n.ID, err.Error())
		return nil, err
	}
	n.Ext = node.Ext
	filteredAddrs := n.Addrs
	if relayURL != "" {
		log.Printf("nodemgmt: UpdateNodeStatus: allocated relay URL for node %d: %s\n", n.ID, relayURL)
		n.Addrs = []string{relayURL}
	} else {
		n.Addrs = nil
	}

	if usedSpace+self.Config.Misc.PrePurchaseThreshold > n.ProductiveSpace {
		assignable := Min(n.AssignedSpace, n.Quota, n.MaxDataSpace) - n.ProductiveSpace
		if assignable <= 0 {
			log.Printf("nodemgmt: UpdateNodeStatus: warning: node %d has no left space for allocating\n", n.ID)
		} else {
			if assignable >= self.Config.Misc.PrePurchaseAmount {
				assignable = self.Config.Misc.PrePurchaseAmount
			}
			err = self.IncrProductiveSpace(n.ID, assignable)
			if err != nil {
				log.Printf("nodemgmt: UpdateNodeStatus: error when increasing productive space for node %d: %s\n", n.ID, err.Error())
				return nil, err
			}
			err = self.eostx.AddSpace(n.ProfitAcc, uint64(n.ID), uint64(assignable))
			if err != nil {
				log.Printf("nodemgmt: UpdateNodeStatus: error when adding space for node %d: %s\n", n.ID, err.Error())
				self.IncrProductiveSpace(n.ID, -1*assignable)
				return nil, err
			}
			n.ProductiveSpace += assignable
			log.Printf("nodemgmt: UpdateNodeStatus: pre-purchase productive space of node %d: %d\n", n.ID, assignable)
		}
	}
	n.Addrs = filteredAddrs
	if n.Uspaces == nil {
		n.Uspaces = make(map[string]int64)
	}
	if b, err := proto.Marshal(n.Convert()); err != nil {
		log.Printf("nodemgmt: UpdateNodeStatus: marshal node %d failed: %s\n", n.ID, err)
	} else {
		log.Println("nodemgmt: UpdateNodeStatus: publish information of node", n.ID)
		self.syncService.Publish("sync", b)
	}
	return n, nil
}

func (self *NodeDaoImpl) calculateW2(poolOwner string) float64 {
	pw := new(PoolWeight)
	collection := self.client.Database(YottaDB).Collection(PoolWeightTab)
	err := collection.FindOne(context.Background(), bson.M{"_id": poolOwner}).Decode(pw)
	if err != nil {
		log.Printf("nodemgmt: calculateW2: error when decoding weight of pool %s: %s\n", poolOwner, err.Error())
		return 1
	}
	if pw.PoolTotalSpace == 0 || pw.ReferralSpace == 0 || pw.TotalSpace == 0 {
		return 1
	}
	return 1 + (float64(pw.PoolReferralSpace)/float64(pw.PoolTotalSpace))/(float64(pw.ReferralSpace)/float64(pw.TotalSpace))
}

//IncrUsedSpace increase user space of one node
func (self *NodeDaoImpl) IncrUsedSpace(id int32, incr int64) error {
	if incr < 0 {
		return errors.New("incremental space cannot be minus")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	n := new(Node)
	err := collection.FindOne(context.Background(), bson.M{"_id": id}).Decode(n)
	if err != nil {
		return err
	}
	//weight := float64(Min(n.AssignedSpace, n.Quota, n.MaxDataSpace) - n.UsedSpace)
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$inc": bson.M{"usedSpace": incr}})
	return err
}

//IncrProductiveSpace increase productive space of one node
func (self *NodeDaoImpl) IncrProductiveSpace(id int32, incr int64) error {
	// if incr < 0 {
	// 	return errors.New("incremental space cannot be minus")
	// }
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err := collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$inc": bson.M{"productiveSpace": incr}})
	return err
}

//AllocNodes by shard count
func (self *NodeDaoImpl) AllocNodes(shardCount int32, errids []int32) ([]*Node, error) {
	return self.ns.Alloc(shardCount, errids)
}

//SyncNode sync node info to other SN
func (self *NodeDaoImpl) SyncNode(node *Node) error {
	if node.ID == 0 {
		log.Println("nodemgmt: SyncNode: warning: node ID cannot be 0")
		return errors.New("node ID cannot be 0")
	}
	if node.ID%int32(incr) == index {
		return nil
	}
	node.Addrs = CheckPublicAddrs(node.Addrs, self.Config.Misc.ExcludeAddrPrefix)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	otherDoc := bson.A{}
	if node.Ext != "" && node.Ext[0] == '[' && node.Ext[len(node.Ext)-1] == ']' {
		var bdoc interface{}
		err := bson.UnmarshalExtJSON([]byte(node.Ext), true, &bdoc)
		if err != nil {
			log.Printf("nodemgmt: SyncNode: warning when parse ext document %s\n", err.Error())
		} else {
			otherDoc, _ = bdoc.(bson.A)
		}
	}
	if node.Uspaces == nil {
		node.Uspaces = make(map[string]int64)
	}
	_, err := collection.InsertOne(context.Background(), bson.M{"_id": node.ID, "nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "uspaces": node.Uspaces, "manualWeight": node.ManualWeight, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc})
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			log.Printf("nodemgmt: SyncNode: error when inserting node %d to database: %s\n", node.ID, err.Error())
			return err
		} else {
			cond := bson.M{"nodeid": node.NodeID, "pubkey": node.PubKey, "owner": node.Owner, "profitAcc": node.ProfitAcc, "poolID": node.PoolID, "poolOwner": node.PoolOwner, "quota": node.Quota, "addrs": node.Addrs, "cpu": node.CPU, "memory": node.Memory, "bandwidth": node.Bandwidth, "maxDataSpace": node.MaxDataSpace, "assignedSpace": node.AssignedSpace, "productiveSpace": node.ProductiveSpace, "usedSpace": node.UsedSpace, "manualWeight": node.ManualWeight, "weight": node.Weight, "valid": node.Valid, "relay": node.Relay, "status": node.Status, "timestamp": node.Timestamp, "version": node.Version, "rebuilding": node.Rebuilding, "realSpace": node.RealSpace, "tx": node.Tx, "rx": node.Rx, "other": otherDoc}
			currentSN := fmt.Sprintf("sn%d", self.Config.SNID)
			log.Printf("nodemgmt: SyncNode: currentSN is %s, uspace is %v\n", currentSN, node.Uspaces)
			for k, v := range node.Uspaces {
				if k != currentSN {
					cond[fmt.Sprintf("uspaces.%s", k)] = v
				}
			}
			log.Printf("nodemgmt: SyncNode: cond %v\n", cond)
			opts := new(options.FindOneAndUpdateOptions)
			opts = opts.SetReturnDocument(options.After)
			result := collection.FindOneAndUpdate(context.Background(), bson.M{"_id": node.ID}, bson.M{"$set": cond}, opts)
			updatedNode := new(Node)
			err := result.Decode(updatedNode)
			if err != nil {
				log.Printf("nodemgmt: SyncNode: error when updating record of node %d in database: %s\n", node.ID, err.Error())
				return err
			}
			if s, ok := updatedNode.Uspaces[currentSN]; ok {
				self.sendUspace(node.ID, self.Config.SNID, s)
			}
		}
	}
	return nil
}

func (self *NodeDaoImpl) sendUspace(to, from int32, uspace int64) {
	log.Printf("nodemgmt: sendUspace: send UpdateUspaceMessage of miner %d, from miner %d: %d\n", to, from, uspace)
	msg := &pb.UpdateUspaceMessage{NodeID: to, FromNodeID: from, Uspace: uspace}
	b, err := proto.Marshal(msg)
	if err != nil {
		log.Printf("nodemgmt: sendUspace: error when marshaling UpdateUspaceMessage of miner %d, from miner %d: %s\n", to, from, err.Error())
		return
	}
	self.syncService.Send(fmt.Sprintf("sn%d", int64(to)%self.Config.SNCount), append([]byte{byte(UpdateUspaceMessage)}, b...))
}

//GetNodes by node IDs
func (self *NodeDaoImpl) GetNodes(nodeIDs []int32) ([]*Node, error) {
	if len(nodeIDs) == 1 && nodeIDs[0] == -1 {
		return self.GetForbiddenNodes()
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.M{"_id": bson.M{"$in": nodeIDs}})
	if err != nil {
		log.Printf("nodemgmt: GetNodes: error when finding nodes %v in database: %s\n", nodeIDs, err.Error())
		return nil, err
	}
	nodes := make([]*Node, 0)
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: GetNodes: error when decoding nodes: %s\n", err.Error())
			return nil, err
		}
		nodes = append(nodes, result)
	}
	return nodes, nil
}

//GetForbiddenNodes get all forbidden nodes(manualWeight is 0)
func (self *NodeDaoImpl) GetForbiddenNodes() ([]*Node, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.M{"manualWeight": 0})
	if err != nil {
		log.Printf("nodemgmt: GetForbiddenNodes: error when finding forbidden nodes in database: %s\n", err.Error())
		return nil, err
	}
	nodes := make([]*Node, 0)
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: GetForbiddenNodes: error when decoding forbidden nodes: %s\n", err.Error())
			return nil, err
		}
		nodes = append(nodes, result)
	}
	return nodes, nil
}

//GetSuperNodes get all super nodes
func (self *NodeDaoImpl) GetSuperNodes() ([]*SuperNode, error) {
	supernodes := make([]*SuperNode, 0)
	collection := self.client.Database(YottaDB).Collection(SuperNodeTab)
	cur, err := collection.Find(context.Background(), bson.D{})
	if err != nil {
		log.Printf("nodemgmt: GetSuperNodes: error when finding all supernodes in database: %s\n", err.Error())
		return nil, err
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		result := new(SuperNode)
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: GetSuperNodes: error when decoding supernodes: %s\n", err.Error())
			return nil, err
		}
		supernodes = append(supernodes, result)
	}
	return supernodes, nil
}

//GetSuperNodePrivateKey get private key of super node with certain ID
func (self *NodeDaoImpl) GetSuperNodeByID(id int32) (*SuperNode, error) {
	collection := self.client.Database(YottaDB).Collection(SuperNodeTab)
	supernode := new(SuperNode)
	err := collection.FindOne(context.Background(), bson.D{{"_id", id}}).Decode(supernode)
	if err != nil {
		log.Printf("nodemgmt: GetSuperNodes: error when finding supernode by ID: %d %s\n", id, err.Error())
		return nil, err
	}
	return supernode, nil
}

//GetSuperNodePrivateKey get private key of super node with certain ID
func (self *NodeDaoImpl) GetSuperNodePrivateKey(id int32) (string, error) {
	collection := self.client.Database(YottaDB).Collection(SuperNodeTab)
	cur, err := collection.Find(context.Background(), bson.D{{"_id", id}})
	if err != nil {
		log.Printf("nodemgmt: GetSuperNodePrivateKey: error when finding supernode by ID: %d %s\n", id, err.Error())
		return "", err
	}
	defer cur.Close(context.Background())
	result := new(SuperNode)
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: GetSuperNodePrivateKey: error when decoding supernode by ID: %d %s\n", id, err.Error())
			return "", err
		}
		return result.PrivKey, nil
	}
	return "", errors.New("no result")
}

//GetNodeIDByPubKey get node ID by public key
func (self *NodeDaoImpl) GetNodeIDByPubKey(pubkey string) (int32, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.D{{"pubkey", pubkey}})
	if err != nil {
		log.Printf("nodemgmt: GetNodeIDByPubKey: error when finding node by pubkey %s: %s\n", pubkey, err.Error())
		return 0, err
	}
	defer cur.Close(context.Background())
	result := new(Node)
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: GetNodeIDByPubKey: error when decoding node by pubkey %s: %s\n", pubkey, err.Error())
			return 0, err
		}
		return result.ID, nil
	}
	return 0, errors.New("no result")
}

//GetNodeByPubKey get node by public key
func (self *NodeDaoImpl) GetNodeByPubKey(pubkey string) (*Node, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.D{{"pubkey", pubkey}})
	if err != nil {
		log.Printf("nodemgmt: GetNodeByPubKey: error when finding node by pubkey %s: %s\n", pubkey, err.Error())
		return nil, err
	}
	defer cur.Close(context.Background())
	result := new(Node)
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: GetNodeByPubKey: error when decoding node by pubkey %s: %s\n", pubkey, err.Error())
			return nil, err
		}
		return result, nil
	}
	return nil, errors.New("No result")
}

//GetSuperNodeIDByPubKey get super node ID by public key
func (self *NodeDaoImpl) GetSuperNodeIDByPubKey(pubkey string) (int32, error) {
	collection := self.client.Database(YottaDB).Collection(SuperNodeTab)
	cur, err := collection.Find(context.Background(), bson.D{{"pubkey", pubkey}})
	if err != nil {
		log.Printf("nodemgmt: GetSuperNodeIDByPubKey: error when finding supernode by pubkey %s: %s\n", pubkey, err.Error())
		return 0, err
	}
	defer cur.Close(context.Background())
	result := new(SuperNode)
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: GetSuperNodeIDByPubKey: error when decoding supernode by pubkey %s: %s\n", pubkey, err.Error())
			return 0, err
		}
		return result.ID, nil
	}
	return 0, errors.New("no result")
}

//AddDNI add digest of one shard
func (self *NodeDaoImpl) AddDNI(id int32, shard []byte) error {
	collection := self.client.Database(YottaDB).Collection(DNITab)
	oid := primitive.NewObjectID()
	_, err := collection.InsertOne(context.Background(), bson.M{"_id": oid, "shard": shard, "minerID": id, "delete": 0})
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			log.Printf("nodemgmt: AddDNI: error when inserting DNI of node %d to database: %s %s\n", id, base58.Encode(shard), err.Error())
			return err
		} else {
			return nil
		}
	} else {
		collectionNode := self.client.Database(YottaDB).Collection(NodeTab)
		_, err = collectionNode.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$inc": bson.M{"usedSpace": 1}})
		if err != nil {
			log.Printf("nodemgmt: AddDNI: error when updating used space of node %d: %s\n", id, err.Error())
			collection.DeleteOne(context.Background(), bson.M{"_id": oid})
			return err
		}
		return nil
	}
}

//ActiveNodesList show id and public IP of all active data nodes
func (self *NodeDaoImpl) ActiveNodesList() ([]*Node, error) {
	nodes := make([]*Node, 0)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	cur, err := collection.Find(context.Background(), bson.M{"valid": 1, "status": 1, "assignedSpace": bson.M{"$gt": 0}, "quota": bson.M{"$gt": 0}, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*self.Config.Misc.AvaliableMinerTimeGap}, "weight": bson.M{"$gt": 0}, "version": bson.M{"$gte": self.Config.Misc.MinerVersionThreshold}})
	if err != nil {
		log.Printf("nodemgmt: ActiveNodesList: error when finding active nodes list: %s\n", err.Error())
		return nil, err
	}
	defer cur.Close(context.Background())
	for cur.Next(context.Background()) {
		result := new(Node)
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: ActiveNodesList: error when decoding nodes: %s\n", err.Error())
			return nil, err
		}
		result.Addrs = []string{CheckPublicAddr(result.Addrs, self.Config.Misc.ExcludeAddrPrefix)}
		nodes = append(nodes, result)
	}
	return nodes, nil
}

//Statistics of data nodes
func (self *NodeDaoImpl) Statistics() (*NodeStat, error) {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	active, err := collection.CountDocuments(context.Background(), bson.M{"valid": 1, "status": 1, "assignedSpace": bson.M{"$gt": 0}, "quota": bson.M{"$gt": 0}, "timestamp": bson.M{"$gt": time.Now().Unix() - IntervalTime*self.Config.Misc.AvaliableMinerTimeGap}, "weight": bson.M{"$gt": 0}, "version": bson.M{"$gte": self.Config.Misc.MinerVersionThreshold}})
	if err != nil {
		log.Printf("nodemgmt: Statistics: error when counting active nodes: %s\n", err.Error())
		return nil, err
	}
	total, err := collection.CountDocuments(context.Background(), bson.M{})
	if err != nil {
		log.Printf("nodemgmt: Statistics: error when counting all nodes: %s\n", err.Error())
		return nil, err
	}
	pipeline := mongo.Pipeline{
		{{"$project", bson.D{{"maxDataSpace", 1}, {"assignedSpace", 1}, {"productiveSpace", 1}, {"usedSpace", 1}, {"_id", 0}}}},
		{{"$group", bson.D{{"_id", ""}, {"maxTotal", bson.D{{"$sum", "$maxDataSpace"}}}, {"assignedTotal", bson.D{{"$sum", "$assignedSpace"}}}, {"productiveTotal", bson.D{{"$sum", "$productiveSpace"}}}, {"usedTotal", bson.D{{"$sum", "$usedSpace"}}}}}},
	}
	cur, err := collection.Aggregate(context.Background(), pipeline)
	if err != nil {
		log.Printf("nodemgmt: Statistics: error when calculating space statistics: %s\n", err.Error())
		return nil, err
	}
	result := new(NodeStat)
	defer cur.Close(context.Background())
	if cur.Next(context.Background()) {
		err := cur.Decode(result)
		if err != nil {
			log.Printf("nodemgmt: Statistics: error when decoding space statistics: %s\n", err.Error())
			return nil, err
		}
	}
	result.ActiveMiners = active
	result.TotalMiners = total
	return result, nil
}

//MinerQuit quit miner
func (self *NodeDaoImpl) MinerQuit(id int32, percent int32) (string, error) {
	if id%int32(incr) != index {
		log.Printf("nodemgmt: MinerQuit: warning: node %d do not belong to current SN\n", id)
		return "", errors.New("miner do not belong to this SN")
	}
	rate, err := self.eostx.GetExchangeRate()
	if err != nil {
		log.Printf("nodemgmt: MinerQuit: error when fetching exchange rate from BP: %s\n", err.Error())
		return "", err
	}
	pledgeData, err := self.eostx.GetPledgeData(uint64(id))
	if err != nil {
		return "", err
	}
	totalAsset := pledgeData.Total
	punishAsset := pledgeData.Deposit
	log.Printf("nodemgmt: MinerQuit: deposit of miner %d is %dYTA, total %dYTA\n", id, punishAsset.Amount/10000, totalAsset.Amount/10000)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	node := new(Node)
	err = collection.FindOne(context.Background(), bson.M{"_id": id}).Decode(node)
	if err != nil {
		log.Printf("nodemgmt: MinerQuit: error when decoding information of miner %d: %s\n", id, err.Error())
		return "", err
	}
	if node.UsedSpace == 0 {
		return fmt.Sprintf("UsedSpace of miner %d is 0\n", node.ID), nil
	}
	punishAmount := node.UsedSpace * 1000000 * int64(percent) / int64(rate) / 6553600
	if punishAmount < int64(punishAsset.Amount) {
		punishAsset.Amount = eos.Int64(punishAmount)
	}
	err = self.eostx.DeducePledge(uint64(node.ID), &punishAsset)
	if err != nil {
		return "", err
	}
	assignedSpace := node.AssignedSpace - node.UsedSpace*int64(percent)/100
	if assignedSpace < 0 {
		assignedSpace = node.AssignedSpace
	}
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": bson.M{"assignedSpace": assignedSpace}})
	if err != nil {
		log.Printf("spotcheck: MinerQuit: warning when punishing %dYTA deposit of node %d: %s\n", punishAsset.Amount/10000, node.ID, err.Error())
	}
	resp := fmt.Sprintf("punish %dYTA deposit of node %d\n", punishAsset.Amount/10000, node.ID)
	log.Printf("spotcheck: MinerQuit: %s", resp)
	return resp, nil
}

//BatchMinerQuit quit miner
func (self *NodeDaoImpl) BatchMinerQuit(id, percent int32) (string, error) {
	if id%int32(incr) != index {
		log.Printf("nodemgmt: MinerQuit: warning: node %d do not belong to current SN\n", id)
		return "", errors.New("miner do not belong to this SN")
	}
	rate, err := self.eostx.GetExchangeRate()
	if err != nil {
		log.Printf("nodemgmt: MinerQuit: error when fetching exchange rate from BP: %s\n", err.Error())
		return "", err
	}
	pledgeData, err := self.eostx.GetPledgeData(uint64(id))
	if err != nil {
		return "", err
	}
	totalAsset := pledgeData.Total
	punishAsset := pledgeData.Deposit
	log.Printf("nodemgmt: BatchMinerQuit: deposit of miner %d is %dYTA, total %dYTA\n", id, punishAsset.Amount/10000, totalAsset.Amount/10000)
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	node := new(Node)
	err = collection.FindOne(context.Background(), bson.M{"_id": id}).Decode(node)
	if err != nil {
		log.Printf("nodemgmt: BatchMinerQuit: error when decoding information of miner %d: %s\n", id, err.Error())
		return "", err
	}
	if node.AssignedSpace == 0 {
		return fmt.Sprintf("AssignedSpace of miner %d is 0\n", node.ID), nil
	}
	punishAmount := int64(pledgeData.Deposit.Amount) * int64(percent) / 100 //node.AssignedSpace * int64(percent) * 10000 / int64(rate) / 65536
	if punishAmount < int64(punishAsset.Amount) {
		punishAsset.Amount = eos.Int64(punishAmount)
	}
	err = self.eostx.DeducePledge(uint64(node.ID), &punishAsset)
	if err != nil {
		return "", err
	}
	assignedSpace := node.AssignedSpace - int64(punishAsset.Amount)*65536*int64(rate)/1000000 //node.AssignedSpace*int64(percent)/100
	if assignedSpace < 0 {
		assignedSpace = node.AssignedSpace
	}
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": bson.M{"assignedSpace": assignedSpace}})
	if err != nil {
		log.Printf("spotcheck: BatchMinerQuit: warning when punishing %dYTA deposit of node %d: %s\n", punishAsset.Amount/10000, node.ID, err.Error())
	}
	resp := fmt.Sprintf("punish %dYTA deposit of node %d\n", punishAsset.Amount/10000, node.ID)
	log.Printf("spotcheck: BatchMinerQuit: %s", resp)
	return resp, nil
}

//ChangeManualWeight modify manual weight
func (self *NodeDaoImpl) ChangeManualWeight(id, weight int32) error {
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err := collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": bson.M{"manualWeight": weight}})
	return err
}
