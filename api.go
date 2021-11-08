package YTDNMgmt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	proto "github.com/golang/protobuf/proto"
	"go.mongodb.org/mongo-driver/bson"
)

var EnableReg int32 = 1

//NewNodeID get newest id of node
func (self *NodeDaoImpl) NewNodeID() (int32, error) {
	if atomic.LoadInt32(&EnableReg) == 0 {
		return 0, errors.New("register disabled")
	}
	collection := self.client.Database(YottaDB).Collection(SequenceTab)
	_, err := collection.InsertOne(context.Background(), bson.M{"_id": NodeIdxType, "seq": index})
	if err != nil {
		errstr := err.Error()
		if !strings.ContainsAny(errstr, "duplicate key error") {
			log.Printf("nodemgmt: NewNodeID: error when calculating new node ID: %s\n", err.Error())
			return 0, err
		}
	}
	m := make(map[string]int32)
	err = collection.FindOne(context.Background(), bson.M{"_id": NodeIdxType}).Decode(&m)
	if err != nil {
		log.Printf("nodemgmt: NewNodeID: error when decoding sequence: %d %s\n", NodeIdxType, err.Error())
		return 0, err
	}
	return m["seq"] + int32(incr), nil
}

//CallAPI api of all eos related transaction
func (self *NodeDaoImpl) CallAPI(trx string, apiName string) error {
	var err error
	if apiName == "" {
		return errors.New("API name cannot be empty")
	}
	switch apiName {
	case "PreRegisterNode":
		if !self.ns.Config.ContractUpgraded {
			err = self.PreRegisterNode(trx)
		} else {
			err = self.PreRegisterNode2(trx)
		}
	case "ChangeMinerPool":
		err = self.ChangeMinerPool(trx)
	case "ChangeAdminAcc":
		err = self.ChangeAdminAcc(trx)
	case "ChangeProfitAcc":
		err = self.ChangeProfitAcc(trx)
	case "ChangePoolID":
		err = self.ChangePoolID(trx)
	case "ChangeAssignedSpace":
		if !self.ns.Config.ContractUpgraded {
			err = self.ChangeAssignedSpace(trx)
		} else {
			err = errors.New("function deprecated")
		}
	case "ChangeDepAcc":
		err = self.ChangeDepAcc(trx)
	case "ChangeDeposit":
		if !self.ns.Config.ContractUpgraded {
			err = self.ChangeDeposit(trx)
		} else {
			err = errors.New("function deprecated")
		}
	case "IncreaseDeposit":
		err = self.IncreaseDeposit(trx)
	}
	if err != nil {
		return err
	}
	return nil
}

//PreRegisterNode register node on chain and pledge YTA for assignable space
func (self *NodeDaoImpl) PreRegisterNode(trx string) error {
	if atomic.LoadInt32(&EnableReg) == 0 {
		return errors.New("register disabled")
	}
	rate, err := self.eostx.GetExchangeRate()
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when fetching exchange rate from BP: %s\n", err.Error())
		return err
	}
	signedTrx, regData, err := self.eostx.PreRegisterTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when extracting transaction: %s\n", err.Error())
		return err
	}
	pledgeAmount := int64(regData.DepAmount.Amount)
	adminAccount := string(regData.Owner)
	profitAccount := string(regData.Owner)
	minerID := int32(regData.MinerID)
	pubkey := regData.Extra
	if adminAccount == "" || profitAccount == "" || minerID == 0 || minerID%int32(incr) != index || pubkey == "" {
		log.Printf("nodemgmt: PreRegisterNode: error when parsing parameters from raw transaction: %s\n", "please check adminAccount, profitAccount, minerID, public key and owner of currect node")
		return errors.New("bad parameters in PreRegisterNode transaction")
	}
	nodeid, err := IDFromPublicKey(pubkey)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when generating ID from public key: %s\n", err.Error())
		return err
	}
	currID, err := self.NewNodeID()
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when calculating new node ID: %s\n", err.Error())
		return err
	}
	if currID != minerID {
		log.Printf("nodemgmt: PreRegisterNode: error: %s\n", "current ID is not equal to minerID")
		return errors.New("node ID is invalid, please retry")
	}
	collection := self.client.Database(YottaDB).Collection(SequenceTab)
	result, err := collection.UpdateOne(context.Background(), bson.M{"_id": NodeIdxType, "seq": minerID - int32(incr)}, bson.M{"$set": bson.M{"seq": minerID}})
	if err != nil || (result.MatchedCount != 1 && result.ModifiedCount != 1) {
		log.Printf("nodemgmt: PreRegisterNode: error when updating sequence of Node: %d %s\n", minerID, err.Error())
		return err
	}
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when sending transaction: %s\n", err.Error())
		return err
	}
	node := new(Node)
	node.ID = minerID
	node.NodeID = nodeid
	node.PubKey = pubkey
	node.Owner = adminAccount
	node.ProfitAcc = profitAccount
	node.PoolID = ""
	node.PoolOwner = ""
	node.Quota = 0
	node.AssignedSpace = pledgeAmount * 65536 * int64(rate) / 1000000 //TODO: 1YTA=1G 后需调整
	node.Uspaces = make(map[string]int64)
	node.Valid = 0
	node.Relay = 0
	node.Status = 0
	node.Timestamp = time.Now().Unix()
	node.Version = 0
	node.ManualWeight = 100
	if self.disableBP {
		node.PoolID = "testpool"
		node.PoolOwner = "poolowner123"
		node.Quota = node.AssignedSpace
	}
	collection = self.client.Database(YottaDB).Collection(NodeTab)
	_, err = collection.InsertOne(context.Background(), node)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when inserting node to database: %d %s\n", minerID, err.Error())
		return err
	}
	collectionLog := self.client.Database(YottaDB).Collection(NodeLogTab)
	nodelog := NewNodeLog(self.bpID, node.ID, -1, node.Status, "new")
	_, err = collectionLog.InsertOne(context.Background(), nodelog)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: warning when add node log of miner %d: %s\n", node.ID, err.Error())
	} else {
		log.Printf("nodemgmt: PreRegisterNode: adding node log of miner %d\n", node.ID)
	}
	log.Printf("nodemgmt: PreRegisterNode: new node registered: ID->%d, nodeID->%s, pubkey->%s, owner->%s, proficAcc->%s, assignedSpace->%d\n", node.ID, node.NodeID, node.PubKey, node.Owner, node.ProfitAcc, node.AssignedSpace)
	return nil
}

//PreRegisterNode2 register node on chain and pledge YTA for assignable space
func (self *NodeDaoImpl) PreRegisterNode2(trx string) error {
	if atomic.LoadInt32(&EnableReg) == 0 {
		return errors.New("register disabled")
	}
	rate, err := self.eostx.GetExchangeRate()
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode2: error when fetching exchange rate from BP: %s\n", err.Error())
		return err
	}
	signedTrx, regData, err := self.eostx.PreRegisterTrx2(trx)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode2: error when extracting transaction: %s\n", err.Error())
		return err
	}
	pledgeAmount := int64(regData.DepAmount.Amount)
	if regData.IsCalc {
		pledgeAmount = 0
	}
	adminAccount := string(regData.Owner)
	profitAccount := string(regData.Owner)
	minerID := int32(regData.MinerID)
	pubkey := regData.Extra
	poolID := string(regData.PoolID)
	minerProfit := string(regData.MinerProfit)
	quota := regData.MaxSpace
	if adminAccount == "" || profitAccount == "" || minerID == 0 || minerID%int32(incr) != index || pubkey == "" || poolID == "" || minerProfit == "" {
		log.Printf("nodemgmt: PreRegisterNode2: error when parsing parameters from raw transaction: %s\n", "please check adminAccount, profitAccount, minerID, public key, pool ID and miner profit account")
		return errors.New("bad parameters in PreRegisterNode transaction")
	}
	log.Printf("nodemgmt: PreRegisterNode2: parsing transaction: %v\n", regData)
	nodeid, err := IDFromPublicKey(pubkey)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode2: error when generating ID from public key: %s\n", err.Error())
		return err
	}
	poolInfo, err := self.eostx.GetPoolInfoByPoolID(poolID)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode2: error when get pool owner: %d %s\n", minerID, err.Error())
		return fmt.Errorf("error when get pool owner of miner %d: %w", minerID, err)
	}
	currID, err := self.NewNodeID()
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode2: error when calculating new node ID: %s\n", err.Error())
		return err
	}
	if currID != minerID {
		log.Printf("nodemgmt: PreRegisterNode2: error: %s\n", "current ID is not equal to minerID")
		return errors.New("node ID is invalid, please retry")
	}
	collection := self.client.Database(YottaDB).Collection(SequenceTab)
	result, err := collection.UpdateOne(context.Background(), bson.M{"_id": NodeIdxType, "seq": minerID - int32(incr)}, bson.M{"$set": bson.M{"seq": minerID}})
	if err != nil || (result.MatchedCount != 1 && result.ModifiedCount != 1) {
		log.Printf("nodemgmt: PreRegisterNode2: error when updating sequence of Node: %d %s\n", minerID, err.Error())
		return err
	}
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode2: error when sending transaction: %s\n", err.Error())
		return err
	}
	node := new(Node)
	node.ID = minerID
	node.NodeID = nodeid
	node.PubKey = pubkey
	node.Owner = adminAccount
	node.ProfitAcc = minerProfit
	node.PoolID = poolID
	node.PoolOwner = string(poolInfo.Owner)
	node.Quota = int64(quota)
	node.AssignedSpace = pledgeAmount * 65536 * int64(rate) / 1000000 //TODO: 1YTA=1G 后需调整
	node.Uspaces = make(map[string]int64)
	node.Valid = 0
	node.Relay = 0
	node.Status = 0
	node.Timestamp = time.Now().Unix()
	node.Version = 0
	node.ManualWeight = 100
	node.ForceSync = true
	if self.disableBP {
		node.PoolID = "testpool"
		node.PoolOwner = "poolowner123"
		node.Quota = node.AssignedSpace
	}
	collection = self.client.Database(YottaDB).Collection(NodeTab)
	_, err = collection.InsertOne(context.Background(), node)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode2: error when inserting node to database: %d %s\n", minerID, err.Error())
		return err
	}
	collectionLog := self.client.Database(YottaDB).Collection(NodeLogTab)
	nodelog := NewNodeLog(self.bpID, node.ID, -1, node.Status, "new")
	_, err = collectionLog.InsertOne(context.Background(), nodelog)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode2: warning when add node log of miner %d: %s\n", node.ID, err.Error())
	} else {
		log.Printf("nodemgmt: PreRegisterNode2: adding node log of miner %d\n", node.ID)
	}
	log.Printf("nodemgmt: PreRegisterNode2: new node registered: ID->%d, nodeID->%s, pubkey->%s, owner->%s, proficAcc->%s, assignedSpace->%d\n", node.ID, node.NodeID, node.PubKey, node.Owner, node.ProfitAcc, node.AssignedSpace)
	err = self.IncrProductiveSpace(node.ID, self.Config.Misc.PrePurchaseAmount)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode2: error when increasing productive space: %s\n", err.Error())
		return fmt.Errorf("error when increasing productive space of miner %d: %w", minerID, err)
	}
	log.Printf("nodemgmt: PreRegisterNode2: increased productive space: %d\n", self.Config.Misc.PrePurchaseAmount)
	err = self.eostx.AddSpace(node.ProfitAcc, uint64(node.ID), uint64(self.Config.Misc.PrePurchaseAmount))
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode2: error when adding productive space on BP: %s\n", err.Error())
		self.IncrProductiveSpace(node.ID, -1*self.Config.Misc.PrePurchaseAmount)
		return fmt.Errorf("error when adding productive space of miner %d: %w", minerID, err)
	}
	node.ProductiveSpace += self.Config.Misc.PrePurchaseAmount
	log.Printf("nodemgmt: PreRegisterNode2: added assignable space on BP %d\n", self.Config.Misc.PrePurchaseAmount)
	if b, err := proto.Marshal(node.Convert()); err != nil {
		log.Printf("nodemgmt: PreRegisterNode2: marshal node %d failed: %s\n", node.ID, err)
	} else {
		log.Println("nodemgmt: PreRegisterNode2: publish information of node", node.ID)
		self.syncService.Publish("sync", b)
	}
	return nil
}

//ChangeMinerPool add miner to a pool
func (self *NodeDaoImpl) ChangeMinerPool(trx string) error {
	if self.disableBP {
		return errors.New("System is under non-BP mode")
	}
	signedTrx, poolData, err := self.eostx.ChangeMinerPoolTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when extracting transaction: %s\n", err.Error())
		return err
	}
	minerID := int32(poolData.MinerID)
	poolID := string(poolData.PoolID)
	minerProfit := string(poolData.MinerProfit)
	quota := poolData.MaxSpace
	poolInfo, err := self.eostx.GetPoolInfoByPoolID(poolID)
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when get pool owner: %d %s\n", minerID, err.Error())
		return fmt.Errorf("error when get pool owner of miner %d: %w", minerID, err)
	}
	if minerID%int32(incr) != index {
		log.Printf("nodemgmt: ChangeMinerPool: minerID %d is not belong to SN%d\n", minerID, index)
		return errors.New("transaction sends to incorrect super node")
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	node := new(Node)
	err = collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(node)
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when decoding node: %d %s\n", minerID, err.Error())
		return fmt.Errorf("error when decoding miner %d: %w", minerID, err)
	}
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when sending transaction: %s\n", err.Error())
		return fmt.Errorf("error when sending transaction of miner %d: %w", minerID, err)
	}
	var afterReg bool = false
	if node.Status == 0 && node.ProductiveSpace == 0 {
		afterReg = true
	}
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": minerID}, bson.M{"$set": bson.M{"poolID": poolID, "poolOwner": poolInfo.Owner, "profitAcc": minerProfit, "quota": quota, "forceSync": true}})
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when updating poolID->%s, profitAcc->%s, quota->%d: %s\n", poolID, minerProfit, quota, err.Error())
		return fmt.Errorf("error when updating poolID->%s, profitAcc->%s, quota->%d of miner %d: %w", poolID, minerProfit, quota, minerID, err)
	}
	if afterReg {
		log.Printf("node %d is not added to pool %s for first time\n", minerID, poolID)
		err = collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(node)
		if err != nil {
			log.Printf("nodemgmt: ChangeMinerPool: error when decoding node: %d %s\n", minerID, err.Error())
			return fmt.Errorf("error when decoding miner %d: %w", minerID, err)
		}
		assignable := Min(node.AssignedSpace, node.Quota)
		if assignable <= 0 {
			log.Printf("nodemgmt: ChangeMinerPool: warning: assignable space is %d\n", assignable)
			return fmt.Errorf("assignable space is %d", assignable)
		}
		if assignable >= self.Config.Misc.PrePurchaseAmount {
			assignable = self.Config.Misc.PrePurchaseAmount
		}
		err := self.IncrProductiveSpace(node.ID, assignable)
		if err != nil {
			log.Printf("nodemgmt: ChangeMinerPool: error when increasing productive space: %s\n", err.Error())
			return fmt.Errorf("error when increasing productive space of miner %d: %w", minerID, err)
		}
		log.Printf("nodemgmt: ChangeMinerPool: increased productive space: %d\n", assignable)
		err = self.eostx.AddSpace(node.ProfitAcc, uint64(node.ID), uint64(assignable))
		if err != nil {
			log.Printf("nodemgmt: ChangeMinerPool: error when adding productive space on BP: %s\n", err.Error())
			self.IncrProductiveSpace(node.ID, -1*assignable)
			return fmt.Errorf("error when adding productive space of miner %d: %w", minerID, err)
		}
		log.Printf("nodemgmt: ChangeMinerPool: added assignable space on BP %d\n", assignable)
	}
	log.Printf("nodemgmt: ChangeMinerPool: node %d is added to pool %s\n", minerID, poolID)
	node = new(Node)
	err = collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(node)
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when decoding node: %d %s\n", minerID, err.Error())
		return fmt.Errorf("error when decoding miner %d: %w", minerID, err)
	}
	if b, err := proto.Marshal(node.Convert()); err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: marshal node %d failed: %s\n", node.ID, err)
	} else {
		log.Println("nodemgmt: ChangeMinerPool: publish information of node", node.ID)
		self.syncService.Publish("sync", b)
	}
	return nil
}

//ChangeAdminAcc change admin account of miner
func (self *NodeDaoImpl) ChangeAdminAcc(trx string) error {
	if self.disableBP {
		return errors.New("System is under non-BP mode")
	}
	signedTrx, adminData, err := self.eostx.ChangeAdminAccTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangeAdmin: error when extracting transaction: %s\n", err.Error())
		return err
	}
	minerID := int64(adminData.MinerID)
	newAdmin := string(adminData.NewAdminAcc)
	if minerID == 0 || minerID%incr != int64(index) || newAdmin == "" {
		log.Printf("nodemgmt: ChangeAdmin: error when parsing parameters from raw transaction: %s\n", "please check minerID and new admin account name of currect node")
		return errors.New("bad parameters in ChangeAdminAcc transaction")
	}
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: ChangeAdmin: error when sending transaction: %s\n", err.Error())
		return err
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": minerID}, bson.M{"$set": bson.M{"owner": newAdmin}})
	if err != nil {
		log.Printf("nodemgmt: ChangeAdmin: error when updating new admin account of Node: %d %s\n", minerID, err.Error())
		return err
	}
	log.Printf("nodemgmt: ChangeAdmin: node %d has changed admin: %s\n", minerID, newAdmin)
	node := new(Node)
	err = collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(node)
	if err != nil {
		log.Printf("nodemgmt: ChangeAdmin: error when decoding node: %d %s\n", minerID, err.Error())
		return fmt.Errorf("error when decoding miner %d: %w", minerID, err)
	}
	if b, err := proto.Marshal(node.Convert()); err != nil {
		log.Printf("nodemgmt: ChangeAdmin: marshal node %d failed: %s\n", node.ID, err)
	} else {
		log.Println("nodemgmt: ChangeAdmin: publish information of node", node.ID)
		self.syncService.Publish("sync", b)
	}
	return nil
}

//ChangeProfitAcc change profit account of miner
func (self *NodeDaoImpl) ChangeProfitAcc(trx string) error {
	if self.disableBP {
		return errors.New("System is under non-BP mode")
	}
	signedTrx, profitData, err := self.eostx.ChangeProfitAccTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangeProfitAcc: error when extracting transaction: %s\n", err.Error())
		return err
	}
	minerID := int64(profitData.MinerID)
	newProfit := string(profitData.NewProfitAcc)
	if minerID == 0 || minerID%incr != int64(index) || newProfit == "" {
		log.Printf("nodemgmt: ChangeProfitAcc: error when parsing parameters from raw transaction: %s\n", "please check minerID and new profit account name of currect node")
		return errors.New("bad parameters in ChangeProfitAcc transaction")
	}
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: ChangeProfitAcc: error when sending transaction: %s\n", err.Error())
		return err
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": minerID}, bson.M{"$set": bson.M{"profitAcc": newProfit}})
	if err != nil {
		log.Printf("nodemgmt: ChangeProfitAcc: error when updating new profit account of Node: %d %s\n", minerID, err.Error())
		return err
	}
	log.Printf("nodemgmt: ChangeProfitAcc: node %d has changed profit account: %s\n", minerID, newProfit)
	node := new(Node)
	err = collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(node)
	if err != nil {
		log.Printf("nodemgmt: ChangeProfitAcc: error when decoding node: %d %s\n", minerID, err.Error())
		return fmt.Errorf("error when decoding miner %d: %w", minerID, err)
	}
	if b, err := proto.Marshal(node.Convert()); err != nil {
		log.Printf("nodemgmt: ChangeProfitAcc: marshal node %d failed: %s\n", node.ID, err)
	} else {
		log.Println("nodemgmt: ChangeProfitAcc: publish information of node", node.ID)
		self.syncService.Publish("sync", b)
	}
	return nil
}

//ChangePoolID change pool ID of miner
func (self *NodeDaoImpl) ChangePoolID(trx string) error {
	if self.disableBP {
		return errors.New("System is under non-BP mode")
	}
	signedTrx, poolData, err := self.eostx.ChangePoolIDTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangePoolID: error when extracting transaction: %s\n", err.Error())
		return err
	}
	minerID := int64(poolData.MinerID)
	newPoolID := string(poolData.NewPoolID)
	if minerID == 0 || minerID%incr != int64(index) || newPoolID == "" {
		log.Printf("nodemgmt: ChangePoolID: error when parsing parameters from raw transaction: %s\n", "please check minerID and new pool ID of currect node")
		return errors.New("bad parameters in ChangePoolID transaction")
	}
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: ChangePoolID: error when sending transaction: %s\n", err.Error())
		return err
	}
	poolInfo, err := self.eostx.GetPoolInfoByPoolID(newPoolID)
	if err != nil {
		log.Printf("nodemgmt: ChangePoolID: error when get pool owner: %d %s\n", minerID, err.Error())
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": minerID}, bson.M{"$set": bson.M{"poolID": newPoolID, "poolOwner": poolInfo.Owner}})
	if err != nil {
		log.Printf("nodemgmt: ChangePoolID: error when updating pool ID of Node: %d %s\n", minerID, err.Error())
		return err
	}
	log.Printf("nodemgmt: ChangePoolID: node %d has changed pool ID: %s\n", minerID, newPoolID)
	node := new(Node)
	err = collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(node)
	if err != nil {
		log.Printf("nodemgmt: ChangePoolID: error when decoding node: %d %s\n", minerID, err.Error())
		return fmt.Errorf("error when decoding miner %d: %w", minerID, err)
	}
	if b, err := proto.Marshal(node.Convert()); err != nil {
		log.Printf("nodemgmt: ChangePoolID: marshal node %d failed: %s\n", node.ID, err)
	} else {
		log.Println("nodemgmt: ChangePoolID: publish information of node", node.ID)
		self.syncService.Publish("sync", b)
	}
	return nil
}

//ChangeDepAcc change dep account of miner
func (self *NodeDaoImpl) ChangeDepAcc(trx string) error {
	if self.disableBP {
		return errors.New("System is under non-BP mode")
	}
	signedTrx, newDepAcc, err := self.eostx.ChangeDepAccTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangeDepAcc: error when extracting transaction: %s\n", err.Error())
		return err
	}
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: ChangeDepAcc: error when sending transaction: %s\n", err.Error())
		return err
	}
	log.Printf("nodemgmt: ChangeDepAcc: node %d has changed dep account: %s\n", newDepAcc.MinerID, newDepAcc.NewDepAcc)
	return nil
}

//ChangeDeposit change deposit of miner
func (self *NodeDaoImpl) ChangeDeposit(trx string) error {
	if self.disableBP {
		return errors.New("System is under non-BP mode")
	}
	signedTrx, newDeposit, err := self.eostx.ChangeDepositTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangeDeposit: error when extracting transaction: %s\n", err.Error())
		return err
	}
	rate, err := self.eostx.GetExchangeRate()
	if err != nil {
		log.Printf("nodemgmt: ChangeDeposit: error when fetching exchange rate from BP: %s\n", err.Error())
		return err
	}
	delta := int64(newDeposit.Quant.Amount) * 65536 * int64(rate) / 1000000
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: ChangeDeposit: error when sending transaction: %s\n", err.Error())
		return err
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	if !newDeposit.IsIncrease {
		delta = delta * (-1)
	}
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": newDeposit.MinerID}, bson.M{"$inc": bson.M{"assignedSpace": delta}})
	if err != nil {
		log.Printf("nodemgmt: ChangeDeposit: error when updating assignedSpace of Node: %d %s\n", newDeposit.MinerID, err.Error())
		return err
	}
	log.Printf("nodemgmt: ChangeDeposit: node %d has changed deposit: %d\n", newDeposit.MinerID, int64(newDeposit.Quant.Amount))
	return nil
}

//ChangeAssignedSpace change assigned space of miner
func (self *NodeDaoImpl) ChangeAssignedSpace(trx string) error {
	if self.disableBP {
		return errors.New("System is under non-BP mode")
	}
	signedTrx, newSpaceData, err := self.eostx.ChangeAssignedSpaceTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangeAssignedSpace: error when extracting transaction: %s\n", err.Error())
		return err
	}
	minerID := int64(newSpaceData.MinerID)
	newAssignedSpace := int64(newSpaceData.MaxSpace)
	if minerID == 0 || minerID%incr != int64(index) || newAssignedSpace == 0 {
		log.Printf("nodemgmt: ChangeAssignedSpace: error when parsing parameters from raw transaction: %s\n", "please check minerID and new assigned space of currect node")
		return errors.New("bad parameters in ChangeAssignedSpace transaction")
	}
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: ChangeAssignedSpace: error when sending transaction: %s\n", err.Error())
		return err
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": minerID}, bson.M{"$set": bson.M{"quota": newAssignedSpace}})
	if err != nil {
		log.Printf("nodemgmt: ChangeAssignedSpace: error when updating assigned space of Node: %d %s\n", minerID, err.Error())
		return err
	}
	log.Printf("nodemgmt: ChangeAssignedSpace: node %d has changed assigned space: %d\n", minerID, newAssignedSpace)
	node := new(Node)
	err = collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(node)
	if err != nil {
		log.Printf("nodemgmt: ChangeAssignedSpace: error when decoding node: %d %s\n", minerID, err.Error())
		return fmt.Errorf("error when decoding miner %d: %w", minerID, err)
	}
	if b, err := proto.Marshal(node.Convert()); err != nil {
		log.Printf("nodemgmt: ChangeAssignedSpace: marshal node %d failed: %s\n", node.ID, err)
	} else {
		log.Println("nodemgmt: ChangeAssignedSpace: publish information of node", node.ID)
		self.syncService.Publish("sync", b)
	}
	return nil
}

//IncreaseDeposit increase deposit of miner
func (self *NodeDaoImpl) IncreaseDeposit(trx string) error {
	if self.disableBP {
		return errors.New("system is under non-BP mode")
	}
	signedTrx, newDeposit, err := self.eostx.IncreaseDepositTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: IncreaseDeposit: error when extracting transaction: %s\n", err.Error())
		return err
	}
	// rate, err := self.eostx.GetExchangeRate()
	// if err != nil {
	// 	log.Printf("nodemgmt: IncreaseDeposit: error when fetching exchange rate from BP: %s\n", err.Error())
	// 	return err
	// }
	//delta := int64(newDeposit.DepAmount.Amount) * 65536 * int64(rate) / 1000000
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: IncreaseDeposit: error when sending transaction: %s\n", err.Error())
		return err
	}
	collection := self.client.Database(YottaDB).Collection(NodeTab)
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": newDeposit.MinerID}, bson.M{"$set": bson.M{"forceSync": true}})
	if err != nil {
		log.Printf("nodemgmt: IncreaseDeposit: error when updating foreSync status of Node: %d %s\n", newDeposit.MinerID, err.Error())
		return err
	}
	log.Printf("nodemgmt: IncreaseDeposit: node %d has changed status of forceSync to true\n", newDeposit.MinerID)
	return nil
}
