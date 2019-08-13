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
	char* profitAcc;
	char* poolID;
	int64_t quota;
	char **addrs;
	int addrsize;
	int32_t cpu;
	int32_t memory;
	int32_t bandwidth;
	int64_t maxDataSpace;
	int64_t assignedSpace;
	int64_t productiveSpace;
	int64_t usedSpace;
	double weight;
	int32_t valid;
	int32_t relay;
	int32_t status;
	int64_t timestamp;
	int32_t version;
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

typedef struct nodestatret {
	int64_t activeMiners;
	int64_t totalMiners;
	int64_t maxTotal;
	int64_t assignedTotal;
	int64_t productiveTotal;
	int64_t usedTotal;
	char *error;
} nodestatret;

typedef struct spotchecktask {
	int32_t id;
	char *nodeid;
	char *addr;
	char *vni;
} spotchecktask;

typedef struct spotchecklist {
	char *taskid;
	spotchecktask **tasklist;
	int size;
	int64_t timestamp;
} spotchecklist;

typedef struct spotchecklists {
	spotchecklist **list;
	int size;
	char *error;
} spotchecklists;

typedef struct vni {
	char *shard;
	int size;
} vni;

typedef struct rebuilditem {
	node *node;
	vni **shards;
	int size;
	char *error;
} rebuilditem;

typedef struct shardcount {
	int32_t id;
	int64_t cnt;
} shardcount;

typedef struct shardcountlist {
	shardcount **shardcounts;
	int size;
	char *error;
} shardcountlist;

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
extern void FreeSpotchecktask(spotchecktask *ptr);
extern void FreeSpotchecklist(spotchecklist *ptr);
extern void FreeShardcount(shardcount *ptr);
extern void FreeShardcountlist(shardcountlist *ptr);
extern void FreeVNI(vni *ptr);

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

static spotchecktask** makeSpotchecktaskArray(int size) {
	spotchecktask **ret = (spotchecktask**)malloc(sizeof(spotchecktask*) * size);
	memset(ret, 0 , sizeof(spotchecktask*) * size);
	return ret;
}

static void setSpotchecktaskArray(spotchecktask **spotchecklist, spotchecktask *task, int n) {
	spotchecklist[n] = task;
}

static void freeSpotchecktaskArray(spotchecktask **spotchecklist, int size) {
	int i;
	for (i = 0; i < size; i++) {
		FreeSpotchecktask(spotchecklist[i]);
		spotchecklist[i] = NULL;
	}
	free(spotchecklist);
}

static spotchecklist** makeSpotchecklistArray(int size) {
	spotchecklist **ret = (spotchecklist**)malloc(sizeof(spotchecklist*) * size);
	memset(ret, 0 , sizeof(spotchecklist*) * size);
	return ret;
}

static void setSpotchecklistArray(spotchecklist **spotchecklists, spotchecklist *list, int n) {
	spotchecklists[n] = list;
}

static void freeSpotchecklistArray(spotchecklist **spotchecklists, int size) {
	int i;
	for (i = 0; i < size; i++) {
		FreeSpotchecklist(spotchecklists[i]);
		spotchecklists[i] = NULL;
	}
	free(spotchecklists);
}

static shardcount** makeShardcountArray(int size) {
	shardcount **ret = (shardcount**)malloc(sizeof(shardcount*) * size);
	memset(ret, 0 , sizeof(shardcount*) * size);
	return ret;
}

static void setShardcountArray(shardcount **shardcounts, shardcount *shardcount, int n) {
	shardcounts[n] = shardcount;
}

static void freeShardcountArray(shardcount **shardcounts, int size) {
	int i;
	for (i = 0; i < size; i++) {
		FreeShardcount(shardcounts[i]);
		shardcounts[i] = NULL;
	}
	free(shardcounts);
}

static vni** makeVNIArray(int size) {
	vni **ret = (vni**)malloc(sizeof(vni*) * size);
	memset(ret, 0 , sizeof(vni*) * size);
	return ret;
}

static void setVNIArray(vni **vnis, vni *vni, int n) {
	vnis[n] = vni;
}

static void freeVNIArray(vni **vnis, int size) {
	int i;
	for (i = 0; i < size; i++) {
		FreeVNI(vnis[i]);
		vnis[i] = NULL;
	}
	free(vnis);
}
*/
import "C"
import (
	"errors"
	"sync"
	"unsafe"

	nodemgmt "github.com/yottachain/YTDNMgmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var nodeDao nodemgmt.NodeDao
var mu sync.Mutex

//export NewInstance
func NewInstance(mongodbURL, eosURL, bpAccount, bpPrivkey, contractOwnerM, contractOwnerD *C.char, bpid C.int32_t) *C.char {
	mu.Lock()
	defer mu.Unlock()
	if nodeDao != nil {
		return C.CString("Node management module has started")
	}
	murl := C.GoString(mongodbURL)
	eurl := C.GoString(eosURL)
	bpAcc := C.GoString(bpAccount)
	bpPriv := C.GoString(bpPrivkey)
	ctrcOwnerM := C.GoString(contractOwnerM)
	ctrcOwnerD := C.GoString(contractOwnerD)
	var err error
	nodeDao, err = nodemgmt.NewInstance(murl, eurl, bpAcc, bpPriv, ctrcOwnerM, ctrcOwnerD, int32(bpid))
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export NewNodeID
func NewNodeID() *C.intwitherror {
	if nodeDao == nil {
		return createIntwitherror(0, errors.New("Node management module has not started"))
	}
	id, err := nodeDao.NewNodeID()
	return createIntwitherror(id, err)
}

//export PreRegisterNode
func PreRegisterNode(trx *C.char) *C.char {
	if nodeDao == nil {
		return C.CString("Node management module has not started")
	}
	err := nodeDao.PreRegisterNode(C.GoString(trx))
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export ChangeMinerPool
func ChangeMinerPool(trx *C.char) *C.char {
	if nodeDao == nil {
		return C.CString("Node management module has not started")
	}
	err := nodeDao.ChangeMinerPool(C.GoString(trx))
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export RegisterNode
func RegisterNode(node *C.node) *C.node {
	if nodeDao == nil {
		return createNodeStruct(nil, errors.New("Node management module has not started"))
	}
	length := int(node.addrsize)
	tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(node.addrs))[:length:length]
	addrs := make([]string, length)
	for i, s := range tmpslice {
		addrs[i] = C.GoString(s)
	}
	gnode := nodemgmt.NewNode(int32(node.id), C.GoString(node.nodeid), C.GoString(node.pubkey), C.GoString(node.owner), C.GoString(node.profitAcc), C.GoString(node.poolID), int64(node.quota), addrs, int32(node.cpu), int32(node.memory), int32(node.bandwidth), int64(node.maxDataSpace), int64(node.assignedSpace), int64(node.productiveSpace), int64(node.usedSpace), float64(node.weight), int32(node.valid), int32(node.relay), int32(node.status), int64(node.timestamp), int32(node.version))
	gnode, err := nodeDao.RegisterNode(gnode)
	return createNodeStruct(gnode, err)
}

//export UpdateNodeStatus
func UpdateNodeStatus(node *C.node) *C.node {
	if nodeDao == nil {
		return createNodeStruct(nil, errors.New("Node management module has not started"))
	}
	length := int(node.addrsize)
	addrs := make([]string, length)
	if length > 0 {
		tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(node.addrs))[:length:length]
		for i, s := range tmpslice {
			addrs[i] = C.GoString(s)
		}
	}
	gnode := nodemgmt.NewNode(int32(node.id), C.GoString(node.nodeid), C.GoString(node.pubkey), C.GoString(node.owner), C.GoString(node.profitAcc), C.GoString(node.poolID), int64(node.quota), addrs, int32(node.cpu), int32(node.memory), int32(node.bandwidth), int64(node.maxDataSpace), int64(node.assignedSpace), int64(node.productiveSpace), int64(node.usedSpace), float64(node.weight), int32(node.valid), int32(node.relay), int32(node.status), int64(node.timestamp), int32(node.version))
	gnode, err := nodeDao.UpdateNodeStatus(gnode)
	return createNodeStruct(gnode, err)
}

//export IncrUsedSpace
func IncrUsedSpace(id C.int32_t, incr C.int64_t) *C.char {
	if nodeDao == nil {
		return C.CString("Node management module has not started")
	}
	err := nodeDao.IncrUsedSpace(int32(id), int64(incr))
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export AllocNodes
func AllocNodes(shardCount C.int, errIDs *C.int, size C.int) *C.allocnoderet {
	if nodeDao == nil {
		return createAllocnoderet(nil, errors.New("Node management module has not started"))
	}
	var gErrIDs []int32
	if size > 0 {
		length := int(size)
		tmpslice := (*[1 << 30]C.int)(unsafe.Pointer(errIDs))[:length:length]
		gErrIDs = make([]int32, length)
		for i, s := range tmpslice {
			gErrIDs[i] = int32(s)
		}
	}
	nodes, err := nodeDao.AllocNodes(int32(shardCount), gErrIDs)
	return createAllocnoderet(nodes, err)
}

//export SyncNode
func SyncNode(node *C.node) *C.char {
	if nodeDao == nil {
		return C.CString("Node management module has not started")
	}
	length := int(node.addrsize)
	addrs := make([]string, length)
	if length > 0 {
		tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(node.addrs))[:length:length]
		for i, s := range tmpslice {
			addrs[i] = C.GoString(s)
		}
	}
	gnode := nodemgmt.NewNode(int32(node.id), C.GoString(node.nodeid), C.GoString(node.pubkey), C.GoString(node.owner), C.GoString(node.profitAcc), C.GoString(node.poolID), int64(node.quota), addrs, int32(node.cpu), int32(node.memory), int32(node.bandwidth), int64(node.maxDataSpace), int64(node.assignedSpace), int64(node.productiveSpace), int64(node.usedSpace), float64(node.weight), int32(node.valid), int32(node.relay), int32(node.status), int64(node.timestamp), int32(node.version))
	err := nodeDao.SyncNode(gnode)
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
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
func GetSuperNodePrivateKey(id C.int32_t) *C.stringwitherror {
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

//export GetNodeByPubKey
func GetNodeByPubKey(pubkey *C.char) *C.node {
	if nodeDao == nil {
		return createNodeStruct(nil, errors.New("Node management module has not started"))
	}
	gnode, err := nodeDao.GetNodeByPubKey(C.GoString(pubkey))
	return createNodeStruct(gnode, err)
}

//export GetSuperNodeIDByPubKey
func GetSuperNodeIDByPubKey(pubkey *C.char) *C.intwitherror {
	if nodeDao == nil {
		return createIntwitherror(0, errors.New("Node management module has not started"))
	}
	id, err := nodeDao.GetSuperNodeIDByPubKey(C.GoString(pubkey))
	return createIntwitherror(id, err)
}

//export AddDNI
func AddDNI(id C.int32_t, shard *C.char, size C.longlong) *C.char {
	if nodeDao == nil {
		return C.CString("Node management module has not started")
	}
	shardSlice := (*[1 << 30]byte)(unsafe.Pointer(shard))[:int64(size):int64(size)]
	err := nodeDao.AddDNI(int32(id), shardSlice)
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//--------------------- Statistics ------------------------------

//export ActiveNodesList
func ActiveNodesList() *C.allocnoderet {
	if nodeDao == nil {
		return createAllocnoderet(nil, errors.New("Node management module has not started"))
	}
	nodes, err := nodeDao.ActiveNodesList()
	return createAllocnoderet(nodes, err)
}

//export Statistics
func Statistics() *C.nodestatret {
	if nodeDao == nil {
		return createNodestatret(nil, errors.New("Node management module has not started"))
	}
	stat, err := nodeDao.Statistics()
	return createNodestatret(stat, err)
}

//--------------------- SpotCheck ------------------------------

//export GetSpotCheckList
func GetSpotCheckList() *C.spotchecklists {
	if nodeDao == nil {
		return createSpotchecklists(nil, errors.New("Node management module has not started"))
	}
	list, err := nodeDao.GetSpotCheckList()
	return createSpotchecklists(list, err)
}

//export GetSTNode
func GetSTNode() *C.node {
	if nodeDao == nil {
		return createNodeStruct(nil, errors.New("Node management module has not started"))
	}
	gnode, err := nodeDao.GetSTNode()
	return createNodeStruct(gnode, err)
}

//export GetSTNodes
func GetSTNodes(count C.int64_t) *C.allocnoderet {
	if nodeDao == nil {
		return createAllocnoderet(nil, errors.New("Node management module has not started"))
	}
	nodes, err := nodeDao.GetSTNodes(int64(count))
	return createAllocnoderet(nodes, err)
}

//export UpdateTaskStatus
func UpdateTaskStatus(id *C.char, invalidNodeList *C.int32_t, size C.int32_t) *C.char {
	if nodeDao == nil {
		return C.CString("Node management module has not started")
	}
	length := int(size)
	var gnodeIDs []int32
	if invalidNodeList != nil {
		tmpslice := (*[1 << 30]C.int32_t)(unsafe.Pointer(invalidNodeList))[:length:length]
		gnodeIDs = make([]int32, length)
		for i, s := range tmpslice {
			gnodeIDs[i] = int32(s)
		}
	}
	err := nodeDao.UpdateTaskStatus(C.GoString(id), gnodeIDs)
	if err != nil {
		return C.CString(err.Error())
	} else {
		return nil
	}
}

//--------------------- Rebuild ------------------------------

//export GetInvalidNodes
func GetInvalidNodes() *C.shardcountlist {
	if nodeDao == nil {
		return createShardcountlist(nil, errors.New("Node management module has not started"))
	}
	list, err := nodeDao.GetInvalidNodes()
	return createShardcountlist(list, err)
}

//export GetRebuildItem
func GetRebuildItem(minerID C.int32_t, index, total C.int64_t) *C.rebuilditem {
	if nodeDao == nil {
		return createRebuilditem(nil, nil, errors.New("Node management module has not started"))
	}
	node, list, err := nodeDao.GetRebuildItem(int32(minerID), int64(index), int64(total))
	return createRebuilditem(node, list, err)
}

//export DeleteDNI
func DeleteDNI(id C.int32_t, shard *C.char, size C.longlong) *C.char {
	if nodeDao == nil {
		return C.CString("Node management module has not started")
	}
	shardSlice := (*[1 << 30]byte)(unsafe.Pointer(shard))[:int64(size):int64(size)]
	err := nodeDao.DeleteDNI(int32(id), shardSlice)
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//--------------------- Free functions ------------------------------

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
		C.setArrayNode(cnodes, createNodeStruct(&s, nil), C.int(i))
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

func createNodestatret(nodeStat *nodemgmt.NodeStat, err error) *C.nodestatret {
	ptr := (*C.nodestatret)(C.malloc(C.size_t(unsafe.Sizeof(C.nodestatret{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.nodestatret{})))
	if err != nil {
		(*ptr).error = C.CString(err.Error())
		return ptr
	}
	(*ptr).activeMiners = C.int64_t(nodeStat.ActiveMiners)
	(*ptr).totalMiners = C.int64_t(nodeStat.TotalMiners)
	(*ptr).maxTotal = C.int64_t(nodeStat.MaxTotal)
	(*ptr).assignedTotal = C.int64_t(nodeStat.AssignedTotal)
	(*ptr).productiveTotal = C.int64_t(nodeStat.ProductiveTotal)
	(*ptr).usedTotal = C.int64_t(nodeStat.UsedTotal)
	return ptr
}

//export FreeNodestatret
func FreeNodestatret(ptr *C.nodestatret) {
	if ptr != nil {
		if (*ptr).error != nil {
			C.free(unsafe.Pointer((*ptr).error))
			(*ptr).error = nil
		}
		(*ptr).activeMiners = 0
		(*ptr).totalMiners = 0
		(*ptr).maxTotal = 0
		(*ptr).assignedTotal = 0
		(*ptr).productiveTotal = 0
		(*ptr).usedTotal = 0
		C.free(unsafe.Pointer(ptr))
	}
}

func createNodeStruct(node *nodemgmt.Node, err error) *C.node {
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
	if node.ProfitAcc != "" {
		(*ptr).profitAcc = C.CString(node.ProfitAcc)
	}
	if node.PoolID != "" {
		(*ptr).poolID = C.CString(node.PoolID)
	}
	(*ptr).quota = C.int64_t(node.Quota)
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
	(*ptr).weight = C.double(node.Weight)
	(*ptr).valid = C.int32_t(node.Valid)
	(*ptr).relay = C.int32_t(node.Relay)
	(*ptr).status = C.int32_t(node.Status)
	(*ptr).timestamp = C.int64_t(node.Timestamp)
	(*ptr).version = C.int32_t(node.Version)
	return ptr
}

//export FreeNode
func FreeNode(ptr *C.node) {
	if ptr != nil {
		(*ptr).id = 0
		if (*ptr).nodeid != nil {
			C.free(unsafe.Pointer((*ptr).nodeid))
		}
		if (*ptr).pubkey != nil {
			C.free(unsafe.Pointer((*ptr).pubkey))
		}
		if (*ptr).owner != nil {
			C.free(unsafe.Pointer((*ptr).owner))
		}
		if (*ptr).profitAcc != nil {
			C.free(unsafe.Pointer((*ptr).profitAcc))
		}
		if (*ptr).poolID != nil {
			C.free(unsafe.Pointer((*ptr).poolID))
		}
		(*ptr).quota = 0
		if (*ptr).addrs != nil {
			C.freeCharArray((*ptr).addrs, (*ptr).addrsize)
			(*ptr).addrs = nil
			(*ptr).addrsize = 0
		}
		(*ptr).cpu = 0
		(*ptr).memory = 0
		(*ptr).bandwidth = 0
		(*ptr).maxDataSpace = 0
		(*ptr).assignedSpace = 0
		(*ptr).productiveSpace = 0
		(*ptr).usedSpace = 0
		(*ptr).weight = 0
		(*ptr).valid = 0
		(*ptr).relay = 0
		(*ptr).status = 0
		(*ptr).timestamp = 0
		(*ptr).version = 0
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

func createSpotchecklists(lists []*nodemgmt.SpotCheckList, err error) *C.spotchecklists {
	ptr := (*C.spotchecklists)(C.malloc(C.size_t(unsafe.Sizeof(C.spotchecklists{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.spotchecklists{})))
	if err != nil {
		(*ptr).error = C.CString(err.Error())
		return ptr
	}
	if lists != nil && len(lists) != 0 {
		clists := C.makeSpotchecklistArray(C.int(len(lists)))
		for i, s := range lists {
			C.setSpotchecklistArray(clists, createSpotchecklist(s), C.int(i))
		}
		(*ptr).list = clists
		(*ptr).size = C.int(len(lists))
	}
	return ptr
}

//export FreeSpotchecklists
func FreeSpotchecklists(ptr *C.spotchecklists) {
	if ptr != nil {
		if (*ptr).list != nil {
			C.freeSpotchecklistArray((*ptr).list, (*ptr).size)
			(*ptr).list = nil
		}
		C.free(unsafe.Pointer(ptr))
	}
}

func createSpotchecklist(list *nodemgmt.SpotCheckList) *C.spotchecklist {
	ptr := (*C.spotchecklist)(C.malloc(C.size_t(unsafe.Sizeof(C.spotchecklist{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.spotchecklist{})))
	if !list.TaskID.IsZero() {
		(*ptr).taskid = C.CString(list.TaskID.Hex())
	}
	if list.TaskList != nil && len(list.TaskList) != 0 {
		tasks := C.makeSpotchecktaskArray(C.int(len(list.TaskList)))
		for i, s := range list.TaskList {
			C.setSpotchecktaskArray(tasks, createSpotchecktask(s), C.int(i))
		}
		(*ptr).tasklist = tasks
		(*ptr).size = C.int(len(list.TaskList))
	}
	//(*ptr).duration = C.int64_t(list.Duration)
	(*ptr).timestamp = C.int64_t(list.Timestamp)
	//(*ptr).progress = C.int32_t(list.Progress)
	return ptr
}

//export FreeSpotchecklist
func FreeSpotchecklist(ptr *C.spotchecklist) {
	if ptr != nil {
		if (*ptr).taskid != nil {
			C.free(unsafe.Pointer((*ptr).taskid))
		}
		if (*ptr).tasklist != nil {
			C.freeSpotchecktaskArray((*ptr).tasklist, (*ptr).size)
			(*ptr).tasklist = nil
		}
		//(*ptr).duration = 0
		(*ptr).timestamp = 0
		//(*ptr).progress = 0
		C.free(unsafe.Pointer(ptr))
	}
}

func createSpotchecktask(task *nodemgmt.SpotCheckTask) *C.spotchecktask {
	ptr := (*C.spotchecktask)(C.malloc(C.size_t(unsafe.Sizeof(C.spotchecktask{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.spotchecktask{})))
	(*ptr).id = C.int(task.ID)
	if task.NodeID != "" {
		(*ptr).nodeid = C.CString(task.NodeID)
	}
	if task.Addr != "" {
		(*ptr).addr = C.CString(task.Addr)
	}
	if task.VNI != "" {
		(*ptr).vni = C.CString(task.VNI)
	}
	return ptr
}

//export FreeSpotchecktask
func FreeSpotchecktask(ptr *C.spotchecktask) {
	if ptr != nil {
		(*ptr).id = 0
		if (*ptr).nodeid != nil {
			C.free(unsafe.Pointer((*ptr).nodeid))
		}
		if (*ptr).addr != nil {
			C.free(unsafe.Pointer((*ptr).addr))
		}
		if (*ptr).vni != nil {
			C.free(unsafe.Pointer((*ptr).vni))
		}
		C.free(unsafe.Pointer(ptr))
	}
}

func createVNI(vni primitive.Binary) *C.vni {
	ptr := (*C.vni)(C.malloc(C.size_t(unsafe.Sizeof(C.vni{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.vni{})))
	(*ptr).shard = (*C.char)(unsafe.Pointer(&(vni.Data)[0]))
	(*ptr).size = C.int(len(vni.Data))
	return ptr
}

//export FreeVNI
func FreeVNI(ptr *C.vni) {
	if ptr != nil {
		if (*ptr).shard != nil {
			C.memset(unsafe.Pointer((*ptr).shard), 0, C.size_t((*ptr).size))
		}
		C.free(unsafe.Pointer(ptr))
	}
}

func createShards(shards []primitive.Binary) **C.vni {
	vnis := C.makeVNIArray(C.int(len(shards)))
	for i, s := range shards {
		C.setVNIArray(vnis, createVNI(s), C.int(i))
	}
	return vnis
}

func createRebuilditem(node *nodemgmt.Node, shards []primitive.Binary, err error) *C.rebuilditem {
	ptr := (*C.rebuilditem)(C.malloc(C.size_t(unsafe.Sizeof(C.rebuilditem{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.rebuilditem{})))
	if err != nil {
		(*ptr).error = C.CString(err.Error())
		return ptr
	}
	if node != nil {
		cnode := createNodeStruct(node, nil)
		(*ptr).node = cnode
	} else {
		(*ptr).error = C.CString("no rebuilding node allocated")
		return ptr
	}
	if shards != nil {
		(*ptr).shards = createShards(shards)
		(*ptr).size = C.int(len(shards))
	}
	return ptr
}

//export FreeRebuilditem
func FreeRebuilditem(ptr *C.rebuilditem) {
	if ptr != nil {
		if (*ptr).node != nil {
			FreeNode((*ptr).node)
			(*ptr).node = nil
		}
		if (*ptr).shards != nil {
			C.freeVNIArray((*ptr).shards, (*ptr).size)
			(*ptr).shards = nil
		}
		if (*ptr).error != nil {
			C.free(unsafe.Pointer((*ptr).error))
			(*ptr).error = nil
		}
		C.free(unsafe.Pointer(ptr))
	}
}

func createShardcountlist(list []nodemgmt.ShardCount, err error) *C.shardcountlist {
	ptr := (*C.shardcountlist)(C.malloc(C.size_t(unsafe.Sizeof(C.shardcountlist{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.shardcountlist{})))
	if err != nil {
		(*ptr).error = C.CString(err.Error())
		return ptr
	}
	if list != nil && len(list) != 0 {
		tasks := C.makeShardcountArray(C.int(len(list)))
		for i, s := range list {
			C.setShardcountArray(tasks, createShardcount(s), C.int(i))
		}
		(*ptr).shardcounts = tasks
		(*ptr).size = C.int(len(list))
	}
	return ptr
}

//export FreeShardcountlist
func FreeShardcountlist(ptr *C.shardcountlist) {
	if ptr != nil {
		if (*ptr).shardcounts != nil {
			C.freeShardcountArray((*ptr).shardcounts, (*ptr).size)
			(*ptr).shardcounts = nil
		}
		C.free(unsafe.Pointer(ptr))
	}
}

func createShardcount(item nodemgmt.ShardCount) *C.shardcount {
	ptr := (*C.shardcount)(C.malloc(C.size_t(unsafe.Sizeof(C.shardcount{}))))
	C.memset(unsafe.Pointer(ptr), 0, C.size_t(unsafe.Sizeof(C.shardcount{})))
	(*ptr).id = C.int32_t(item.ID)
	(*ptr).cnt = C.int64_t(item.Cnt)
	return ptr
}

//export FreeShardcount
func FreeShardcount(ptr *C.shardcount) {
	if ptr != nil {
		(*ptr).id = 0
		(*ptr).cnt = 0
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
