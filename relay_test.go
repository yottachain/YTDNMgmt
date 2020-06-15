package YTDNMgmt

import (
	"fmt"
	"testing"
)

func TestCheckPublicAddrs(t *testing.T) {
	addrs := []string{"/ip4/172.16.31.132/tcp/9001", "/ip6/fe80::5054:ff:fe46:88ca/tcp/9001", "/ip4/49.233.90.137/tcp/9001"}
	addr := CheckPublicAddr(addrs, "")
	fmt.Println(addr)
	addrs = CheckPublicAddrs(addrs, "")
	for _, a := range addrs {
		fmt.Println(a)
	}
}
