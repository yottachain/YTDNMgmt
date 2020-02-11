package YTDNMgmt

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/eoscanada/eos-go"
	"github.com/eoscanada/eos-go/ecc"
	_ "github.com/eoscanada/eos-go/system"
	_ "github.com/eoscanada/eos-go/token"

	ytcrypto "github.com/yottachain/YTCrypto"
	"github.com/yottachain/YTDNMgmt/eostx"
)

var eosURL = "http://152.136.16.118:8888"
var nodeDao *NodeDaoImpl

func TestMain(m *testing.M) {
	nodeDao, _ = NewInstance("mongodb://152.136.18.185:27017", eosURL, "username1234", "5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx", "hddpool12345", "hddpool12345", "hddpool12345", 0, 1)
}

func TestChangeAdminAcc(t *testing.T) {
	minerID := 1
	newAdminAcc := "new_admin"
	oldAdmin := "old_admin"
	privatekey := "old_admin_privatekey"
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
	signedTx, _, err := api.SignTransaction(tx, txOpts.ChainID, eos.CompressionNone)
	if err != nil {
		t.Fatalf("sign transaction: %s", err)
	}

	content, err := json.MarshalIndent(signedTx, "", "  ")
	if err != nil {
		t.Fatalf("json marshalling transaction: %s", err)
	}

	err = nodeDao.ChangeAdminAcc(string(content))
	if err != nil {
		t.Fatalf("change admin account failed: %s", err)
	}
	t.Log("change admin account success!")
}
