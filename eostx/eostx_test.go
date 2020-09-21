package eostx

import (
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/aurawing/eos-go"
	ytcrypto "github.com/yottachain/YTCrypto"
)

var etx *EosTX

func TestMain(m *testing.M) {
	etx, _ = NewInstance("http://47.94.166.95:8888", "username1234", "5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx", "hddpool12345", "hdddeposit12", "username1234", "YTA", 4)
	os.Exit(m.Run())
}

func TestGetFreezedLogs(t *testing.T) {
	info, err := etx.GetPoolInfoByPoolID("storepoolown")
	if err != nil {
		t.Log(err)
	}
	// event := m["0x549f6b834b7e9bf2c0d7fc414406eb8e51b900de568fc909a277d8a0d58e2c6f"]
	t.Logf("pool owner: %s\n", info.Owner)
}

func TestGetAccount(t *testing.T) {
	api := eos.New("http://49.235.52.30:8888")
	// assets, err := api.GetBalance(eos.AN("hddpool12345"), "EOS", eos.AN("eosio"))
	// if err != nil {
	// 	log.Println("error:", err)
	// }
	// fmt.Println("assets:", len(assets))
	accountResp, err := api.GetAccount("shadow5")
	if err != nil {
		log.Println("error:", err)
	}
	fmt.Println("Permission for initn:", accountResp.Permissions[0].RequiredAuth.Keys[0].PublicKey)
	fmt.Println("Permission for initn:", accountResp.Permissions[1].RequiredAuth.Keys[0].PublicKey)
}

func TestGetPublicKeyByPrivateKey(t *testing.T) {
	sk := "5JdLhJCky1e4R9NHYHPLEfHuVwMvYCuZ9tjMAazMDPGEE7bUdav"
	pk, _ := ytcrypto.GetPublicKeyByPrivateKey(sk)
	fmt.Println(pk)
	s, _ := ytcrypto.Sign(sk, []byte("hahaha"))
	fmt.Println("sign string:", s)
	b := ytcrypto.Verify("6Z4NLVUnfsCNjowCZ9pqPWjkqrUAdYTjKZg7sTTZ8fBz7ram5s", []byte("hahaha"), s)
	fmt.Println("verified:", b)
}

func TestGetExchangeRate(t *testing.T) {
	etx, err := NewInstance("http://123.57.65.239:8888", "hdddeposit12", "5Ht53hTmD516enMfi9dCaWmjuTSZ9qjDHjfDABtWmKdr4wzVsxY", "hddpool12345", "store.mta", "eosio", "MTA", 4)
	if err != nil {
		t.Fatal(err)
	}
	r, err := etx.GetExchangeRate()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("rate: %d", r)
}

func TestGetPledgeData(t *testing.T) {
	etx, err := NewInstance("http://123.57.65.239:8888", "hdddeposit12", "5Ht53hTmD516enMfi9dCaWmjuTSZ9qjDHjfDABtWmKdr4wzVsxY", "hddpool12345", "store.mta", "eosio", "MTA", 4)
	if err != nil {
		t.Fatal(err)
	}
	p, err := etx.GetPledgeData(1001)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pledge: %+v", p)
}

func TestGetMinerInfo(t *testing.T) {
	etx, err := NewInstance("http://123.57.65.239:8888", "hdddeposit12", "5Ht53hTmD516enMfi9dCaWmjuTSZ9qjDHjfDABtWmKdr4wzVsxY", "store.mta", "store.mta", "eosio", "MTA", 4)
	if err != nil {
		t.Fatal(err)
	}
	p, err := etx.GetMinerInfo(1001)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("miner: %+v", p)
}

func TestGetPoolInfo(t *testing.T) {
	etx, err := NewInstance("http://192.168.36.132:8888", "hdddeposit12", "5Ht53hTmD516enMfi9dCaWmjuTSZ9qjDHjfDABtWmKdr4wzVsxY", "hddpool12345", "hdddeposit12", "eosio", "YTA", 4)
	if err != nil {
		t.Fatal(err)
	}
	p, err := etx.GetPoolInfoByPoolID("storepoolown")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pool: %+v", p)
}
