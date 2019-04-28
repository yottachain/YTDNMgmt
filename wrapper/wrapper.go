package main

/*
#cgo CFLAGS: -std=c99

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

typedef struct node {
	int id;
	char *nodeid;
	char *pubkey;
	char *owner;
	char **addrs;
	int addrsize;
	int32_t cpu;
	int32_t memory;
	int32_t bandwidth;
	int64_t maxDataSpace;
	int64_t assignedSpace;
	int64_t productiveSpace;
	int64_t usedSpace;
	char *error;
} node;

typedef struct supernode {
	int id;
	char *nodeid;
	char *pubkey;
	char *privkey;
	char **addrs;
	int addrsize;
	char *error;
} supernode;

typedef struct allocnoderet {
	node **nodes;
	int size;
	char *error;
} allocnoderet;

typedef struct allocsupernoderet {
	supernode **supernodes;
	int size;
	char *error;
} allocsupernoderet;

typedef struct stringwitherror {
	char *str;
	char *error;
} stringwitherror;

typedef struct intwitherror {
	int id;
	char *error;
} intwitherror;

extern void FreeNode(node *ptr);
extern void FreeSuperNode(supernode *ptr);

static char** makeCharArray(int size) {
	char **ret = (char**)malloc(sizeof(char*) * size);
	memset(ret, 0 , sizeof(char*) * size);
	return ret;
}

static void setArrayString(char **a, char *s, int n) {
    a[n] = s;
}

static void freeCharArray(char **a, int size) {
    int i;
    for (i = 0; i < size; i++) {
		free(a[i]);
		a[i] = NULL;
	}
    free(a);
}

static node** makeNodeArray(int size) {
	node **ret = (node**)malloc(sizeof(node*) * size);
	memset(ret, 0 , sizeof(node*) * size);
	return ret;
}

static void setArrayNode(node **nodes, node *node, int n) {
	nodes[n] = node;
}

static void freeNodeArray(node **nodes, int size) {
	int i;
	for (i = 0; i < size; i++) {
		FreeNode(nodes[i]);
		nodes[i] = NULL;
	}
	free(nodes);
}

static supernode** makeSuperNodeArray(int size) {
	supernode **ret = (supernode**)malloc(sizeof(supernode*) * size);
	memset(ret, 0 , sizeof(supernode*) * size);
	return ret;
}

static void setArraySuperNode(supernode **supernodes, supernode *supernode, int n) {
	supernodes[n] = supernode;
}

static void freeSuperNodeArray(supernode **supernodes, int size) {
	int i;
	for (i = 0; i < size; i++) {
		FreeSuperNode(supernodes[i]);
		supernodes[i] = NULL;
	}
	free(supernodes);
}
*/
import "C"
import (
	"errors"
	"sync"
	"unsafe"

	nodemgmt "github.com/yottachain/YTDNMgmt"
)

var nodeDao nodemgmt.NodeDao
var mu sync.Mutex

//export NewInstance
func NewInstance(urls *C.char) *C.char {
	mu.Lock()
	defer mu.Unlock()
	if nodeDao != nil {
		return C.CString("Node management module has started")
	}
	gurls := C.GoString(urls)
	var err error
	nodeDao, err = nodemgmt.NewInstance(gurls)
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export RegisterNode
func RegisterNode(node *C.node) *C.node {
	length := int(node.addrsize)
	tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(node.addrs))[:length:length]
	addrs := make([]string, length)
	for i, s := range tmpslice {
		addrs[i] = C.GoString(s)
	}
	gnode := nodemgmt.NewNode(int32(node.id), C.GoString(node.nodeid), C.GoString(node.pubkey), C.GoString(node.owner), addrs, int32(node.cpu), int32(node.memory), int32(node.bandwidth), int64(node.maxDataSpace), int64(node.assignedSpace), int64(node.productiveSpace), int64(node.usedSpace))
	gnode, err := nodeDao.RegisterNode(gnode)
	return createNodeStruct(*gnode, err)
}

//export UpdateNodeStatus
func UpdateNodeStatus(node *C.node) *C.node {
	length := int(node.addrsize)
	tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(node.addrs))[:length:length]
	addrs := make([]string, length)
	for i, s := range tmpslice {
		addrs[i] = C.GoString(s)
	}
	gnode := nodemgmt.NewNode(int32(node.id), C.GoString(node.nodeid), C.GoString(node.pubkey), C.GoString(node.owner), addrs, int32(node.cpu), int32(node.memory), int32(node.bandwidth), int64(node.maxDataSpace), int64(node.assignedSpace), int64(node.productiveSpace), int64(node.usedSpace))
	gnode, err := nodeDao.UpdateNodeStatus(gnode)
	return createNodeStruct(*gnode, err)
}

//export IncrUsedSpace
func IncrUsedSpace(id C.int32_t, incr C.int64_t) *C.char {
	err := nodeDao.IncrUsedSpace(int32(id), int64(incr))
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export AllocNodes
func AllocNodes(shardCount C.int) *C.allocnoderet {
	if nodeDao == nil {
		return createAllocnoderet(nil, errors.New("Node management module has not started"))
	}
	nodes, err := nodeDao.AllocNodes(int32(shardCount))
	return createAllocnoderet(nodes, err)
}

//export GetNodes
func GetNodes(nodeIDs *C.int, size C.int) *C.allocnoderet {
	if nodeDao == nil {
		return createAllocnoderet(nil, errors.New("Node management module has not started"))
	}
	length := int(size)
	tmpslice := (*[1 << 30]C.int)(unsafe.Pointer(nodeIDs))[:length:length]
	gnodeIDs := make([]int32, length)
	for i, s := range tmpslice {
		gnodeIDs[i] = int32(s)
	}
	nodes, err := nodeDao.GetNodes(gnodeIDs)
	return createAllocnoderet(nodes, err)
}

//export GetSuperNodes
func GetSuperNodes() *C.allocsupernoderet {
	if nodeDao == nil {
		return createAllocsupernoderet(nil, errors.New("Node management module has not started"))
	}
	supernodes, err := nodeDao.GetSuperNodes()
	return createAllocsupernoderet(supernodes, err)
}

//export GetSuperNodePrivateKey
func GetSuperNodePrivateKey(id C.int) *C.stringwitherror {
	if nodeDao == nil {
		return createStringwitherror("", errors.New("Node management module has not started"))
	}
	privkey, err := nodeDao.GetSuperNodePrivateKey(int32(id))
	return createStringwitherror(privkey, err)
}

//export GetNodeIDByPubKey
func GetNodeIDByPubKey(pubkey *C.char) *C.intwitherror {
	if nodeDao == nil {
		return createIntwitherror(0, errors.New("Node management module has not started"))
	}
	id, err := nodeDao.GetNodeIDByPubKey(C.GoString(pubkey))
	return createIntwitherror(id, err)
}

//export GetSuperNodeIDByPubKey
func GetSuperNodeIDByPubKey(pubkey *C.char) *C.intwitherror {
	if nodeDao == nil {
		return createIntwitherror(0, errors.New("Node management module has not started"))
	}
	id, err := nodeDao.GetSuperNodeIDByPubKey(C.GoString(pubkey))
	return createIntwitherror(id, err)
}

func createAllocnoderet(nodes []nodemgmt.Node, err error) *C.allocnoderet {
	ptr := (*C.allocnoderet)(C.malloc(C.size_t(unsafe.Sizeof(C.allocnoderet{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.allocnoderet{})))
	if err != nil {
		(*ptr).error = C.CString(err.Error())
		return ptr
	}
	size := len(nodes)
	cnodes := C.makeNodeArray(C.int(size))
	for i, s := range nodes {
		C.setArrayNode(cnodes, createNodeStruct(s, nil), C.int(i))
	}
	(*ptr).nodes = cnodes
	(*ptr).size = C.int(size)
	return ptr
}

//export FreeAllocnoderet
func FreeAllocnoderet(ptr *C.allocnoderet) {
	if ptr != nil {
		if (*ptr).nodes != nil {
			C.freeNodeArray((*ptr).nodes, (*ptr).size)
			(*ptr).nodes = nil
		}
		if (*ptr).error != nil {
			C.free(unsafe.Pointer((*ptr).error))
			(*ptr).error = nil
		}
		C.free(unsafe.Pointer(ptr))
	}
}

func createNodeStruct(node nodemgmt.Node, err error) *C.node {
	ptr := (*C.node)(C.malloc(C.size_t(unsafe.Sizeof(C.node{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.node{})))
	if err != nil {
		(*ptr).error = C.CString(err.Error())
		return ptr
	}
	(*ptr).id = C.int(node.ID)
	if node.NodeID != "" {
		(*ptr).nodeid = C.CString(node.NodeID)
	}
	if node.PubKey != "" {
		(*ptr).pubkey = C.CString(node.PubKey)
	}
	if node.Owner != "" {
		(*ptr).owner = C.CString(node.Owner)
	}
	if node.Addrs != nil && len(node.Addrs) != 0 {
		caddrs := C.makeCharArray(C.int(len(node.Addrs)))
		for i, s := range node.Addrs {
			C.setArrayString(caddrs, C.CString(s), C.int(i))
		}
		(*ptr).addrs = caddrs
		(*ptr).addrsize = C.int(len(node.Addrs))
	}
	if node.CPU > 0 {
		(*ptr).cpu = C.int32_t(node.CPU)
	}
	if node.Memory > 0 {
		(*ptr).memory = C.int32_t(node.Memory)
	}
	if node.Bandwidth > 0 {
		(*ptr).bandwidth = C.int32_t(node.Bandwidth)
	}
	if node.MaxDataSpace > 0 {
		(*ptr).maxDataSpace = C.int64_t(node.MaxDataSpace)
	}
	if node.AssignedSpace > 0 {
		(*ptr).assignedSpace = C.int64_t(node.AssignedSpace)
	}
	if node.ProductiveSpace > 0 {
		(*ptr).productiveSpace = C.int64_t(node.ProductiveSpace)
	}
	if node.UsedSpace > 0 {
		(*ptr).usedSpace = C.int64_t(node.UsedSpace)
	}
	return ptr
}

//export FreeNode
func FreeNode(ptr *C.node) {
	if ptr != nil {
		if (*ptr).nodeid != nil {
			C.free(unsafe.Pointer((*ptr).nodeid))
		}
		if (*ptr).pubkey != nil {
			C.free(unsafe.Pointer((*ptr).pubkey))
		}
		if (*ptr).owner != nil {
			C.free(unsafe.Pointer((*ptr).owner))
		}
		if (*ptr).addrs != nil {
			C.freeCharArray((*ptr).addrs, (*ptr).addrsize)
			(*ptr).addrs = nil
		}
		(*ptr).cpu = 0
		(*ptr).memory = 0
		(*ptr).bandwidth = 0
		(*ptr).maxDataSpace = 0
		(*ptr).assignedSpace = 0
		(*ptr).productiveSpace = 0
		(*ptr).usedSpace = 0
		if (*ptr).error != nil {
			C.free(unsafe.Pointer((*ptr).error))
			(*ptr).error = nil
		}
		C.free(unsafe.Pointer(ptr))
	}
}

func createAllocsupernoderet(supernodes []nodemgmt.SuperNode, err error) *C.allocsupernoderet {
	ptr := (*C.allocsupernoderet)(C.malloc(C.size_t(unsafe.Sizeof(C.allocsupernoderet{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.allocsupernoderet{})))
	if err != nil {
		(*ptr).error = C.CString(err.Error())
		return ptr
	}
	size := len(supernodes)
	csupernodes := C.makeSuperNodeArray(C.int(size))
	for i, s := range supernodes {
		C.setArraySuperNode(csupernodes, createSuperNodeStruct(s, nil), C.int(i))
	}
	(*ptr).supernodes = csupernodes
	(*ptr).size = C.int(size)
	return ptr
}

//export FreeAllocsupernoderet
func FreeAllocsupernoderet(ptr *C.allocsupernoderet) {
	if ptr != nil {
		if (*ptr).supernodes != nil {
			C.freeSuperNodeArray((*ptr).supernodes, (*ptr).size)
			(*ptr).supernodes = nil
		}
		if (*ptr).error != nil {
			C.free(unsafe.Pointer((*ptr).error))
			(*ptr).error = nil
		}
		C.free(unsafe.Pointer(ptr))
	}
}

func createSuperNodeStruct(supernode nodemgmt.SuperNode, err error) *C.supernode {
	ptr := (*C.supernode)(C.malloc(C.size_t(unsafe.Sizeof(C.supernode{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.supernode{})))
	if err != nil {
		(*ptr).error = C.CString(err.Error())
		return ptr
	}
	(*ptr).id = C.int(supernode.ID)
	if supernode.NodeID != "" {
		(*ptr).nodeid = C.CString(supernode.NodeID)
	}
	if supernode.PubKey != "" {
		(*ptr).pubkey = C.CString(supernode.PubKey)
	}
	if supernode.PrivKey != "" {
		(*ptr).privkey = C.CString(supernode.PrivKey)
	}
	if supernode.Addrs != nil && len(supernode.Addrs) != 0 {
		caddrs := C.makeCharArray(C.int(len(supernode.Addrs)))
		for i, s := range supernode.Addrs {
			C.setArrayString(caddrs, C.CString(s), C.int(i))
		}
		(*ptr).addrs = caddrs
		(*ptr).addrsize = C.int(len(supernode.Addrs))
	}
	return ptr
}

//export FreeSuperNode
func FreeSuperNode(ptr *C.supernode) {
	if ptr != nil {
		if (*ptr).nodeid != nil {
			C.free(unsafe.Pointer((*ptr).nodeid))
		}
		if (*ptr).pubkey != nil {
			C.free(unsafe.Pointer((*ptr).pubkey))
		}
		if (*ptr).privkey != nil {
			C.free(unsafe.Pointer((*ptr).privkey))
		}
		if (*ptr).addrs != nil {
			C.freeCharArray((*ptr).addrs, (*ptr).addrsize)
			(*ptr).addrs = nil
		}
		if (*ptr).error != nil {
			C.free(unsafe.Pointer((*ptr).error))
			(*ptr).error = nil
		}
		C.free(unsafe.Pointer(ptr))
	}
}

func createStringwitherror(str string, err error) *C.stringwitherror {
	ptr := (*C.stringwitherror)(C.malloc(C.size_t(unsafe.Sizeof(C.stringwitherror{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.stringwitherror{})))
	if err != nil {
		(*ptr).error = C.CString(err.Error())
		return ptr
	}
	(*ptr).str = C.CString(str)
	return ptr
}

//export FreeStringwitherror
func FreeStringwitherror(ptr *C.stringwitherror) {
	if ptr != nil {
		if (*ptr).str != nil {
			C.free(unsafe.Pointer((*ptr).str))
			(*ptr).str = nil
		}
		if (*ptr).error != nil {
			C.free(unsafe.Pointer((*ptr).error))
			(*ptr).error = nil
		}
		C.free(unsafe.Pointer(ptr))
	}
}

func createIntwitherror(id int32, err error) *C.intwitherror {
	ptr := (*C.intwitherror)(C.malloc(C.size_t(unsafe.Sizeof(C.intwitherror{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.intwitherror{})))
	if err != nil {
		(*ptr).error = C.CString(err.Error())
		return ptr
	}
	(*ptr).id = C.int(id)
	return ptr
}

//export FreeIntwitherror
func FreeIntwitherror(ptr *C.intwitherror) {
	if ptr != nil {
		if (*ptr).error != nil {
			C.free(unsafe.Pointer((*ptr).error))
			(*ptr).error = nil
		}
		C.free(unsafe.Pointer(ptr))
	}
}

//export FreeString
func FreeString(ptr unsafe.Pointer) {
	C.free(ptr)
}

func main() {

}
