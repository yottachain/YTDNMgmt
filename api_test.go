package YTDNMgmt

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/aurawing/eos-go"
	"github.com/aurawing/eos-go/ecc"
	_ "github.com/aurawing/eos-go/system"
	_ "github.com/aurawing/eos-go/token"

	ytcrypto "github.com/yottachain/YTCrypto"
	"github.com/yottachain/YTDNMgmt/eostx"
)

var mongoURL = "mongodb://139.9.141.118:27017/?connect=direct"
var eosURL = "http://127.0.0.1:8888"
var nodeDao *NodeDaoImpl

func TestMain(m *testing.M) {
	nodeDao, _ = NewInstance(mongoURL, eosURL, "producer1", "5HtM6e3mQNLEu2TkQ1ZrbMNpRQiHGsKxEsLdxd9VsdCmp1um8QH", "hddpool12345", "hdddeposit12", "shadow1", 0, 1, nil)
	os.Exit(m.Run())
}

func TestChangeAdminAcc(t *testing.T) {
	minerID := 220
	newAdminAcc := "storename123"
	oldAdmin := "username1234"
	privatekey := "5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx"
	api := eos.New(eosURL)
	keyBag := &eos.KeyBag{}
	err := keyBag.ImportPrivateKey(privatekey)
	if err != nil {
		t.Fatalf("import private key: %s", err)
	}
	api.SetSigner(keyBag)
	api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
		publickey, _ := ytcrypto.GetPublicKeyByPrivateKey(privatekey)
		pubkey, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "EOS", publickey))
		return []ecc.PublicKey{pubkey}, nil
	})

	action := &eos.Action{
		Account: eos.AN("hddpool12345"),
		Name:    eos.ActN("mchgadminacc"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(oldAdmin), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(eostx.ChangeAdminAcc{MinerID: uint64(minerID), NewAdminAcc: eos.AN(newAdminAcc)}),
	}

	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(api); err != nil {
		t.Fatalf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := api.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		t.Fatalf("sign transaction: %s", err)
	}

	content, err := json.MarshalIndent(packedTx, "", "  ")
	if err != nil {
		t.Fatalf("json marshalling transaction: %s", err)
	}

	err = nodeDao.ChangeAdminAcc(string(content))
	if err != nil {
		t.Fatalf("change admin account failed: %s", err)
	}
	t.Log("change admin account success!")
}

func TestChangeProfitAcc(t *testing.T) {
	minerID := 220
	adminAcc := "storename123"
	adminPrivatekey := "5KfbRow4L71fZnnu9XEnkmVqByi6CSmRiADJCx6asRS4TUEkU79"
	newProfitAcc := "storename123"
	poolOwner := "storepoolown"
	poolOwnerPrivatekey := "5JkjKo4UGaTQFVuVpDZDV3LNvLrd2DgGRpTNB4E1o9gVuUf7aYZ"
	api := eos.New(eosURL)
	keyBag := &eos.KeyBag{}
	err := keyBag.ImportPrivateKey(adminPrivatekey)
	if err != nil {
		t.Fatalf("import private key: %s", err)
	}
	err = keyBag.ImportPrivateKey(poolOwnerPrivatekey)
	if err != nil {
		t.Fatalf("import private key: %s", err)
	}
	api.SetSigner(keyBag)
	api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
		publickey1, _ := ytcrypto.GetPublicKeyByPrivateKey(adminPrivatekey)
		pubkey1, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "EOS", publickey1))
		publickey2, _ := ytcrypto.GetPublicKeyByPrivateKey(poolOwnerPrivatekey)
		pubkey2, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "EOS", publickey2))
		return []ecc.PublicKey{pubkey1, pubkey2}, nil
	})

	action := &eos.Action{
		Account: eos.AN("hddpool12345"),
		Name:    eos.ActN("mchgowneracc"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(adminAcc), Permission: eos.PN("active")},
			{Actor: eos.AN(poolOwner), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(eostx.ChangeProfitAcc{MinerID: uint64(minerID), NewProfitAcc: eos.AN(newProfitAcc)}),
	}

	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(api); err != nil {
		t.Fatalf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := api.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		t.Fatalf("sign transaction: %s", err)
	}

	content, err := json.MarshalIndent(packedTx, "", "  ")
	if err != nil {
		t.Fatalf("json marshalling transaction: %s", err)
	}

	err = nodeDao.ChangeProfitAcc(string(content))
	if err != nil {
		t.Fatalf("change profit account failed: %s", err)
	}
	t.Log("change profit account success!")
}

func TestChangePoolIDAcc(t *testing.T) {
	minerID := 220
	adminAcc := "storename123"
	adminPrivatekey := "5KfbRow4L71fZnnu9XEnkmVqByi6CSmRiADJCx6asRS4TUEkU79"
	newPoolID := "username1234"
	poolOwner := "username1234"
	poolOwnerPrivatekey := "5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx"
	api := eos.New(eosURL)
	keyBag := &eos.KeyBag{}
	err := keyBag.ImportPrivateKey(adminPrivatekey)
	if err != nil {
		t.Fatalf("import private key: %s", err)
	}
	err = keyBag.ImportPrivateKey(poolOwnerPrivatekey)
	if err != nil {
		t.Fatalf("import private key: %s", err)
	}
	api.SetSigner(keyBag)
	api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
		publickey1, _ := ytcrypto.GetPublicKeyByPrivateKey(adminPrivatekey)
		pubkey1, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "EOS", publickey1))
		publickey2, _ := ytcrypto.GetPublicKeyByPrivateKey(poolOwnerPrivatekey)
		pubkey2, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "EOS", publickey2))
		return []ecc.PublicKey{pubkey1, pubkey2}, nil
	})

	action := &eos.Action{
		Account: eos.AN("hddpool12345"),
		Name:    eos.ActN("mchgstrpool"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(adminAcc), Permission: eos.PN("active")},
			{Actor: eos.AN(poolOwner), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(eostx.ChangePoolID{MinerID: uint64(minerID), NewPoolID: eos.AN(newPoolID)}),
	}

	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(api); err != nil {
		t.Fatalf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := api.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		t.Fatalf("sign transaction: %s", err)
	}

	content, err := json.MarshalIndent(packedTx, "", "  ")
	if err != nil {
		t.Fatalf("json marshalling transaction: %s", err)
	}

	err = nodeDao.ChangePoolID(string(content))
	if err != nil {
		t.Fatalf("change pool ID failed: %s", err)
	}
	t.Log("change pool ID success!")
}

func TestChangeDepAcc(t *testing.T) {
	minerID := 220
	newDepAcc := "storename123"
	privatekey := "5KfbRow4L71fZnnu9XEnkmVqByi6CSmRiADJCx6asRS4TUEkU79"
	api := eos.New(eosURL)
	keyBag := &eos.KeyBag{}
	err := keyBag.ImportPrivateKey(privatekey)
	if err != nil {
		t.Fatalf("import private key: %s", err)
	}
	api.SetSigner(keyBag)
	api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
		publickey, _ := ytcrypto.GetPublicKeyByPrivateKey(privatekey)
		pubkey, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "EOS", publickey))
		return []ecc.PublicKey{pubkey}, nil
	})

	action := &eos.Action{
		Account: eos.AN("hdddeposit12"),
		Name:    eos.ActN("mchgdepacc"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(newDepAcc), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(eostx.ChangeDepAcc{MinerID: uint64(minerID), NewDepAcc: eos.AN(newDepAcc)}),
	}

	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(api); err != nil {
		t.Fatalf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := api.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		t.Fatalf("sign transaction: %s", err)
	}

	content, err := json.MarshalIndent(packedTx, "", "  ")
	if err != nil {
		t.Fatalf("json marshalling transaction: %s", err)
	}

	err = nodeDao.ChangeDepAcc(string(content))
	if err != nil {
		t.Fatalf("change dep account failed: %s", err)
	}
	t.Log("change dep account success!")
}

func TestChangeDeposit(t *testing.T) {
	minerID := 220
	depAcc := "storename123"
	privatekey := "5KfbRow4L71fZnnu9XEnkmVqByi6CSmRiADJCx6asRS4TUEkU79"
	quant := eostx.NewYTAAsset(2000000)
	api := eos.New(eosURL)
	keyBag := &eos.KeyBag{}
	err := keyBag.ImportPrivateKey(privatekey)
	if err != nil {
		t.Fatalf("import private key: %s", err)
	}
	api.SetSigner(keyBag)
	api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
		publickey, _ := ytcrypto.GetPublicKeyByPrivateKey(privatekey)
		pubkey, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "EOS", publickey))
		return []ecc.PublicKey{pubkey}, nil
	})

	action := &eos.Action{
		Account: eos.AN("hdddeposit12"),
		Name:    eos.ActN("chgdeposit"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(depAcc), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(eostx.ChangeDeposit{User: eos.AN(depAcc), MinerID: uint64(minerID), IsIncrease: false, Quant: quant}),
	}

	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(api); err != nil {
		t.Fatalf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := api.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		t.Fatalf("sign transaction: %s", err)
	}

	content, err := json.MarshalIndent(packedTx, "", "  ")
	if err != nil {
		t.Fatalf("json marshalling transaction: %s", err)
	}

	err = nodeDao.ChangeDepAcc(string(content))
	if err != nil {
		t.Fatalf("change deposit failed: %s", err)
	}
	t.Log("change deposit success!")
}

func TestChangeAssignedSpace(t *testing.T) {
	minerID := 220
	adminAcc := "storename123"
	adminPrivatekey := "5KfbRow4L71fZnnu9XEnkmVqByi6CSmRiADJCx6asRS4TUEkU79"
	poolOwner := "username1234"
	poolOwnerPrivatekey := "5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx"
	var newAssignedSpace uint64 = 124519401

	api := eos.New(eosURL)
	keyBag := &eos.KeyBag{}
	err := keyBag.ImportPrivateKey(adminPrivatekey)
	if err != nil {
		t.Fatalf("import private key: %s", err)
	}
	err = keyBag.ImportPrivateKey(poolOwnerPrivatekey)
	if err != nil {
		t.Fatalf("import private key: %s", err)
	}
	api.SetSigner(keyBag)
	api.SetCustomGetRequiredKeys(func(tx *eos.Transaction) ([]ecc.PublicKey, error) {
		publickey1, _ := ytcrypto.GetPublicKeyByPrivateKey(adminPrivatekey)
		pubkey1, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "EOS", publickey1))
		publickey2, _ := ytcrypto.GetPublicKeyByPrivateKey(poolOwnerPrivatekey)
		pubkey2, _ := ecc.NewPublicKey(fmt.Sprintf("%s%s", "EOS", publickey2))
		return []ecc.PublicKey{pubkey1, pubkey2}, nil
	})

	action := &eos.Action{
		Account: eos.AN("hddpool12345"),
		Name:    eos.ActN("mchgspace"),
		Authorization: []eos.PermissionLevel{
			{Actor: eos.AN(adminAcc), Permission: eos.PN("active")},
			{Actor: eos.AN(poolOwner), Permission: eos.PN("active")},
		},
		ActionData: eos.NewActionData(eostx.ChangeAssignedSpace{MinerID: uint64(minerID), MaxSpace: newAssignedSpace}),
	}

	txOpts := &eos.TxOptions{}
	if err := txOpts.FillFromChain(api); err != nil {
		t.Fatalf("filling tx opts: %s", err)
	}

	tx := eos.NewTransaction([]*eos.Action{action}, txOpts)
	_, packedTx, err := api.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		t.Fatalf("sign transaction: %s", err)
	}

	content, err := json.MarshalIndent(packedTx, "", "  ")
	if err != nil {
		t.Fatalf("json marshalling transaction: %s", err)
	}

	err = nodeDao.ChangeAssignedSpace(string(content))
	if err != nil {
		t.Fatalf("change assigned space failed: %s", err)
	}
	t.Log("change assigned space success!")
}
