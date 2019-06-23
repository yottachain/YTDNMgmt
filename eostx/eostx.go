package eostx

import (
	"encoding/json"
	"fmt"

	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	ytcrypto "github.com/yottachain/YTCrypto"
)

// NewInstance create a new eostx instance contans connect url, contract owner and it's private key
func NewInstance(url, bpAccount, privateKey, contractOwner string) (*EosTX, error) {
	api := eos.New(url)
	keyBag := &eos.KeyBag{}
	err := keyBag.ImportPrivateKey(privateKey)
	if err != nil {
		return nil, fmt.Errorf("import private key: %s", err)
	}
	api.SetSigner(keyBag)
	api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
		publickey, _ := ytcrypto.GetPublicKeyByPrivateKey(privateKey)
		pubkey, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "EOS", publickey))
		return []ecc.PublicKey{pubkey}, nil
	})
	return &EosTX{API: api, BpAccount: bpAccount, ContractOwner: contractOwner}, nil
}

// AddMiner call contract to add a record of datanode owner and miner ID
func (eostx *EosTX) AddMiner(owner string, minerID uint64) error {
	action := &eos.Action{
		Account: eos.AN(eostx.ContractOwner),
		Name:    eos.ActN("newmaccount"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(eostx.BpAccount), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(Miner{Owner: eos.AN(owner), MinerID: minerID, Caller: eos.AN(eostx.BpAccount)}),
	}
	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(eostx.API); err != nil {
		return fmt.Errorf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := eostx.API.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		return fmt.Errorf("sign transaction: %s", err)
	}

	_, err = eostx.API.PushTransaction(packedTx)
	if err != nil {
		return fmt.Errorf("push transaction: %s", err)
	}
	return nil
}

// AddProfit call contract to add profit to a miner assigned by minerID
func (eostx *EosTX) AddSpace(owner string, minerID, space uint64) error {
	action := &eos.Action{
		Account: eos.AN(eostx.ContractOwner),
		Name:    eos.ActN("addmprofit"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(eostx.BpAccount), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(Profit{Owner: eos.AN(owner), MinerID: minerID, Space: space, Caller: eos.AN(eostx.BpAccount)}),
	}
	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(eostx.API); err != nil {
		return fmt.Errorf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := eostx.API.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		return fmt.Errorf("sign transaction: %s", err)
	}

	_, err = eostx.API.PushTransaction(packedTx)
	if err != nil {
		return fmt.Errorf("push transaction: %s", err)
	}
	return nil
}

//GetPledgeData get pledge data of one miner
func (eostx *EosTX) GetPledgeData(minerid uint64) (*PledgeData, error) {
	req := eos.GetTableRowsRequest{
		Code:  "hdddeposit12",
		Scope: fmt.Sprintf("%d", minerid),
		Table: "minerdeposit",
		JSON:  true,
	}
	resp, err := eostx.API.GetTableRows(req)
	if err != nil {
		return nil, fmt.Errorf("get table row failed, minerid: %s", req.Scope)
	}
	if resp.More == true {
		return nil, fmt.Errorf("more than one rows returned, minerid: %s", req.Scope)
	}
	fmt.Println(string(resp.Rows))
	rows := make([]PledgeData, 0)
	err = json.Unmarshal(resp.Rows, &rows)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, fmt.Errorf("no matched row found, minerid: %s", req.Scope)
	}
	return &rows[0], nil
}

//GetPledgeCount get pledge amount of one miner
func GetPledgeAmount(data *PledgeData) (*eos.Asset, error) {
	dataAsset, err := NewYTAAssetFromString(data.Deposit)
	if err != nil {
		return nil, err
	}
	return &dataAsset, nil
}

// PayForfeit invalid miner need to pay forfeit
func (eostx *EosTX) DeducePledge(minerID uint64, count *eos.Asset) error {
	data, err := eostx.GetPledgeData(minerID)
	if err != nil {
		return err
	}
	err = eostx.payForfeit(data.AccountName, minerID, count)
	if err != nil {
		return err
	}
	err = eostx.drawForfeit(data.AccountName)
	if err != nil {
		err = eostx.cutVote(data.AccountName)
		return err
	}
	return nil
}

func (eostx *EosTX) payForfeit(user string, minerID uint64, count *eos.Asset) error {
	action := &eos.Action{
		Account: eos.AN("hdddeposit12"),
		Name:    eos.ActN("payforfeit"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(eostx.BpAccount), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(PayForfeit{User: eos.AN(user), MinerID: minerID, Quant: *count, AccType: 2, Caller: eos.AN(eostx.BpAccount)}),
	}
	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(eostx.API); err != nil {
		return fmt.Errorf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := eostx.API.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		return fmt.Errorf("sign transaction: %s", err)
	}

	_, err = eostx.API.PushTransaction(packedTx)
	if err != nil {
		return fmt.Errorf("push transaction: %s", err)
	}
	return nil
}

func (eostx *EosTX) drawForfeit(user string) error {
	action := &eos.Action{
		Account: eos.AN("hdddeposit12"),
		Name:    eos.ActN("drawforfeit"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(eostx.BpAccount), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(DrawForfeit{User: eos.AN(user), AccType: 2, Caller: eos.AN(eostx.BpAccount)}),
	}
	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(eostx.API); err != nil {
		return fmt.Errorf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := eostx.API.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		return fmt.Errorf("sign transaction: %s", err)
	}

	_, err = eostx.API.PushTransaction(packedTx)
	if err != nil {
		return fmt.Errorf("push transaction: %s", err)
	}
	return nil
}

func (eostx *EosTX) cutVote(user string) error {
	action := &eos.Action{
		Account: eos.AN("hdddeposit12"),
		Name:    eos.ActN("cutvote"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(eostx.BpAccount), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(DrawForfeit{User: eos.AN(user), AccType: 2, Caller: eos.AN(eostx.BpAccount)}),
	}
	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(eostx.API); err != nil {
		return fmt.Errorf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := eostx.API.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		return fmt.Errorf("sign transaction: %s", err)
	}

	_, err = eostx.API.PushTransaction(packedTx)
	if err != nil {
		return fmt.Errorf("push transaction: %s", err)
	}
	return nil
}
