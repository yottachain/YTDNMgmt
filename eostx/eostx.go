package eostx

import (
	"fmt"

	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	ytcrypto "github.com/yottachain/YTCrypto"
)

type EosTX struct {
	API           *eos.API
	BpAccount     string
	ContractOwner string
}

type Miner struct {
	Owner   eos.AccountName `json:"owner"`
	MinerID uint64          `json:"minerid"`
	Caller  eos.AccountName `json:"caller"`
}

type Profit struct {
	Owner   eos.AccountName `json:"owner"`
	MinerID uint64          `json:"minerid"`
	Space   uint64          `json:"space"`
	Caller  eos.AccountName `json:"caller"`
}

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

	// content, err := json.MarshalIndent(signedTx, "", "  ")
	// if err != nil {
	// 	panic(fmt.Errorf("json marshalling transaction: %s", err))
	// }

	// fmt.Println(string(content))
	// fmt.Println()

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

	// content, err := json.MarshalIndent(signedTx, "", "  ")
	// if err != nil {
	// 	panic(fmt.Errorf("json marshalling transaction: %s", err))
	// }

	// fmt.Println(string(content))
	// fmt.Println()

	_, err = eostx.API.PushTransaction(packedTx)
	if err != nil {
		return fmt.Errorf("push transaction: %s", err)
	}
	return nil
}
