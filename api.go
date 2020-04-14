package YTDNMgmt

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

//NewNodeID get newest id of node
func (self *NodeDaoImpl) NewNodeID() (int32, error) {
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
		err = self.PreRegisterNode(trx)
	case "ChangeMinerPool":
		err = self.ChangeMinerPool(trx)
	case "ChangeAdminAcc":
		err = self.ChangeAdminAcc(trx)
	case "ChangeProfitAcc":
		err = self.ChangeProfitAcc(trx)
	case "ChangePoolID":
		err = self.ChangePoolID(trx)
	case "ChangeAssignedSpace":
		err = self.ChangeAssignedSpace(trx)
	case "ChangeDepAcc":
		err = self.ChangeDepAcc(trx)
	case "ChangeDeposit":
		err = self.ChangeDeposit(trx)
	}
	if err != nil {
		return err
	}
	return nil
}

//PreRegisterNode register node on chain and pledge YTA for assignable space
func (self *NodeDaoImpl) PreRegisterNode(trx string) error {
	rate, err := self.eostx.GetExchangeRate()
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when fetching exchange rate from BP: %s\n", err.Error())
		return err
	}
	signedTrx, regData, err := self.eostx.PreRegisterTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when sending sign transaction: %s\n", err.Error())
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
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when sending transaction: %s\n", err.Error())
		return err
	}
	collection := self.client.Database(YottaDB).Collection(SequenceTab)
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": NodeIdxType}, bson.M{"$set": bson.M{"seq": minerID}})
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when updating sequence of Node: %d %s\n", minerID, err.Error())
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
	node.Valid = 0
	node.Relay = 0
	node.Status = 0
	node.Timestamp = time.Now().Unix()
	node.Version = 0
	collection = self.client.Database(YottaDB).Collection(NodeTab)
	_, err = collection.InsertOne(context.Background(), node)
	if err != nil {
		log.Printf("nodemgmt: PreRegisterNode: error when inserting node to database: %d %s\n", minerID, err.Error())
		return err
	}
	log.Printf("nodemgmt: PreRegisterNode: new node registered: ID->%d, nodeID->%s, pubkey->%s, owner->%s, proficAcc->%s, assignedSpace->%d\n", node.ID, node.NodeID, node.PubKey, node.Owner, node.ProfitAcc, node.AssignedSpace)
	return nil
}

//ChangeMinerPool add miner to a pool
func (self *NodeDaoImpl) ChangeMinerPool(trx string) error {
	signedTrx, poolData, err := self.eostx.ChangeMinerPoolTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when signing raw transaction: %s\n", err.Error())
		return err
	}
	minerID := int32(poolData.MinerID)
	poolID := string(poolData.PoolID)
	minerProfit := string(poolData.MinerProfit)
	quota := poolData.MaxSpace
	poolInfo, err := self.eostx.GetPoolInfoByPoolID(poolID)
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when get pool owner: %d %s\n", minerID, err.Error())
		return err
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
		return err
	}
	err = self.eostx.SendTrx(signedTrx)
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when sending transaction: %s\n", err.Error())
		return err
	}
	var afterReg bool = false
	if node.Status == 0 && node.ProductiveSpace == 0 {
		afterReg = true
	}
	_, err = collection.UpdateOne(context.Background(), bson.M{"_id": minerID}, bson.M{"$set": bson.M{"poolID": poolID, "poolOwner": poolInfo.Owner, "profitAcc": minerProfit, "quota": quota}})
	if err != nil {
		log.Printf("nodemgmt: ChangeMinerPool: error when updating poolID->%s, profitAcc->%s, quota->%d: %s\n", poolID, minerProfit, quota, err.Error())
		return err
	}
	if afterReg {
		log.Printf("node %d is not added to pool %s for first time\n", minerID, poolID)
		err = collection.FindOne(context.Background(), bson.M{"_id": minerID}).Decode(node)
		if err != nil {
			log.Printf("nodemgmt: ChangeMinerPool: error when decoding node: %d %s\n", minerID, err.Error())
			return err
		}
		assignable := Min(node.AssignedSpace, node.Quota)
		if assignable <= 0 {
			log.Printf("nodemgmt: ChangeMinerPool: warning: assignable space is %d\n", assignable)
			return fmt.Errorf("assignable space is %d", assignable)
		}
		if assignable >= prePurphaseAmount {
			assignable = prePurphaseAmount
		}
		err := self.IncrProductiveSpace(node.ID, assignable)
		if err != nil {
			log.Printf("nodemgmt: ChangeMinerPool: error when increasing assignable space: %s\n", err.Error())
			return err
		}
		log.Printf("nodemgmt: ChangeMinerPool: increased assignable space %d\n", assignable)
		err = self.eostx.AddSpace(node.ProfitAcc, uint64(node.ID), uint64(assignable))
		if err != nil {
			log.Printf("nodemgmt: ChangeMinerPool: error when adding assignable space on BP: %s\n", err.Error())
			self.IncrProductiveSpace(node.ID, -1*assignable)
			return err
		}
		log.Printf("nodemgmt: ChangeMinerPool: added assignable space on BP %d\n", assignable)
	}
	log.Printf("nodemgmt: ChangeMinerPool: node %d is added to pool %s\n", minerID, poolID)
	return nil
}

//ChangeAdminAcc change admin account of miner
func (self *NodeDaoImpl) ChangeAdminAcc(trx string) error {
	signedTrx, adminData, err := self.eostx.ChangeAdminAccTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangeAdmin: error when sending sign transaction: %s\n", err.Error())
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
	return nil
}

//ChangeProfitAcc change profit account of miner
func (self *NodeDaoImpl) ChangeProfitAcc(trx string) error {
	signedTrx, profitData, err := self.eostx.ChangeProfitAccTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangeProfitAcc: error when sending sign transaction: %s\n", err.Error())
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
	return nil
}

//ChangePoolID change pool ID of miner
func (self *NodeDaoImpl) ChangePoolID(trx string) error {
	signedTrx, poolData, err := self.eostx.ChangePoolIDTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangePoolID: error when sending sign transaction: %s\n", err.Error())
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
	return nil
}

//ChangeDepAcc change dep account of miner
func (self *NodeDaoImpl) ChangeDepAcc(trx string) error {
	signedTrx, newDepAcc, err := self.eostx.ChangeDepAccTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangeDepAcc: error when sending sign transaction: %s\n", err.Error())
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

//ChangeDeposit change dep account of miner
func (self *NodeDaoImpl) ChangeDeposit(trx string) error {
	signedTrx, newDeposit, err := self.eostx.ChangeDepositTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangeDeposit: error when sending sign transaction: %s\n", err.Error())
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

//ChangeAssignedSpace change pool ID of miner
func (self *NodeDaoImpl) ChangeAssignedSpace(trx string) error {
	signedTrx, newSpaceData, err := self.eostx.ChangeAssignedSpaceTrx(trx)
	if err != nil {
		log.Printf("nodemgmt: ChangeAssignedSpace: error when sending sign transaction: %s\n", err.Error())
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
	return nil
}
