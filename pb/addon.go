package pb

import (
	"bytes"
	"crypto"
)

// VerifyVHF 验证 DAT sha3 256 和vhf 是否相等
func (req *DownloadShardRequest) VerifyVHF(data []byte) bool {
	sha := crypto.SHA256.New()
	sha.Write(data)
	return bytes.Equal(sha.Sum(nil), req.VHF[:])
}
