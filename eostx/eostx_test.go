package eostx

import (
	"os"
	"testing"
)

var etx *EosTX

func TestMain(m *testing.M) {
	etx, _ = NewInstance("http://47.94.166.95:8888", "username1234", "5JcDH48njDbUQLu1R8SWwKsfWLnqBpWXDDiCgxFC3hioDuwLhVx", "hddpool12345", "hdddeposit12", "username1234")
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
