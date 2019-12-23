package YTDNMgmt

import (
	"os"
	"strconv"
)

var excludeAddrPrefix string // excluded IP range, used in intranet

var prePurphaseThreshold int64 // how many left space remains to , default 32768(512MB)
var prePurphaseAmount int64    // storage amount of pre-purchase, default 32768(512MB)
var avaliableNodeTimeGap int64 // last report time of one node within (NOW-avaliableNodeTimeGap) wil be considered active

var nodeAllocRefreshInterval int64 // interval time of node allocation list refreshing

var minerVersionThreadshold int32 // minimum version which active miner must satisfy

var connectTimeout int32 // connect timeout of all connections
var readTimeout int32    // read timeout of all connections

var invalidNodeTimeGap int64 // time gap for judging which node need to be rebuilt

var recheckRetries int32           // retry count during rechecking when network connecting failed
var recheckRetryInterval int32     // retry interval of recheck task
var spotcheckInterval int32        // interval time of spotcheck
var connectivityTestInterval int32 // interval time of connectivity test
var punishGapUnit int64            //unit of time gap for punishing
var punishPhase1 int               //first phase period of punishment
var punishPhase2 int               //second phase period of punishment
var punishPhase3 int               //third phase period of punishment
var rebuildPhase int               //phase period of node rebuild

var ipDBPath string //path of IPDB

func init() {
	excludeAddrPrefix = os.Getenv("NODEMGMT_EXCLUDEADDR")
	prePurphaseThresholdStr := os.Getenv("NODEMGMT_PREPURCHASETHREADSHOLD")
	ppts, err := strconv.Atoi(prePurphaseThresholdStr)
	if err != nil {
		prePurphaseThreshold = 32768
	} else {
		prePurphaseThreshold = int64(ppts)
	}

	prePurphaseAmountStr := os.Getenv("NODEMGMT_PREPURCHASEAMOUNT")
	ppa, err := strconv.Atoi(prePurphaseAmountStr)
	if err != nil {
		prePurphaseAmount = 65536
	} else {
		prePurphaseAmount = int64(ppa)
	}

	avaliableNodeTimeGapStr := os.Getenv("NODEMGMT_AVALIABLENODETIMEGAP")
	antg, err := strconv.Atoi(avaliableNodeTimeGapStr)
	if err != nil {
		avaliableNodeTimeGap = 3
	} else {
		avaliableNodeTimeGap = int64(antg)
	}

	nodeAllocRefreshIntervalStr := os.Getenv("NODEMGMT_NODEALLOCREFRESHINTERVAL")
	nari, err := strconv.Atoi(nodeAllocRefreshIntervalStr)
	if err != nil {
		nodeAllocRefreshInterval = 10
	} else {
		nodeAllocRefreshInterval = int64(nari)
	}

	minerVersionThreadsholdStr := os.Getenv("NODEMGMT_MINERVERSIONTHREADSHOLD")
	mvt, err := strconv.Atoi(minerVersionThreadsholdStr)
	if err != nil {
		minerVersionThreadshold = 10
	} else {
		minerVersionThreadshold = int32(mvt)
	}

	connectTimeoutStr := os.Getenv("NODEMGMT_CONNECTTIMEOUT")
	ct, err := strconv.Atoi(connectTimeoutStr)
	if err != nil {
		connectTimeout = 10
	} else {
		connectTimeout = int32(ct)
	}

	readTimeoutStr := os.Getenv("NODEMGMT_READTIMEOUT")
	rt, err := strconv.Atoi(readTimeoutStr)
	if err != nil {
		readTimeout = 10
	} else {
		readTimeout = int32(rt)
	}

	invalidNodeTimeGapStr := os.Getenv("NODEMGMT_INVALIDNODETIMEGAP")
	intg, err := strconv.Atoi(invalidNodeTimeGapStr)
	if err != nil {
		invalidNodeTimeGap = 1800
	} else {
		invalidNodeTimeGap = int64(intg)
	}

	recheckRetriesStr := os.Getenv("NODEMGMT_RECHECKRETRIES")
	rr, err := strconv.Atoi(recheckRetriesStr)
	if err != nil {
		recheckRetries = 5
	} else {
		recheckRetries = int32(rr)
	}

	recheckRetryIntervalStr := os.Getenv("NODEMGMT_RECHECKRETRYINTERVAL")
	rri, err := strconv.Atoi(recheckRetryIntervalStr)
	if err != nil {
		recheckRetryInterval = 60
	} else {
		recheckRetryInterval = int32(rri)
	}

	spotcheckIntervalStr := os.Getenv("NODEMGMT_SPOTCHECKINTERVAL")
	si, err := strconv.Atoi(spotcheckIntervalStr)
	if err != nil {
		spotcheckInterval = 60 //60min
	} else {
		spotcheckInterval = int32(si)
	}

	connectivityTestIntervalStr := os.Getenv("NODEMGMT_CONNECTIVITYTESTINTERVAL")
	cti, err := strconv.Atoi(connectivityTestIntervalStr)
	if err != nil {
		connectivityTestInterval = 60
	} else {
		connectivityTestInterval = int32(cti)
	}

	punishGapUnitStr := os.Getenv("NODEMGMT_PUNISHGAPUNIT")
	pgu, err := strconv.Atoi(punishGapUnitStr)
	if err != nil {
		punishGapUnit = 60 //60s
	} else {
		punishGapUnit = int64(pgu)
	}

	punishPhase1Str := os.Getenv("NODEMGMT_PUNISHPHASE1")
	pp1, err := strconv.Atoi(punishPhase1Str)
	if err != nil || pp1 > 10080 {
		punishPhase1 = 240
	} else {
		punishPhase1 = pp1
	}

	punishPhase2Str := os.Getenv("NODEMGMT_PUNISHPHASE2")
	pp2, err := strconv.Atoi(punishPhase2Str)
	if err != nil || pp2 > 10080 {
		punishPhase2 = 1440
	} else {
		punishPhase2 = pp2
	}

	punishPhase3Str := os.Getenv("NODEMGMT_PUNISHPHASE3")
	pp3, err := strconv.Atoi(punishPhase3Str)
	if err != nil || pp3 > 10080 {
		punishPhase3 = 10080
	} else {
		punishPhase3 = pp3
	}

	rebuildPhaseStr := os.Getenv("NODEMGMT_REBUILDPHASE")
	rp, err := strconv.Atoi(rebuildPhaseStr)
	if err != nil || rp > 10080 {
		rebuildPhase = 1440
	} else {
		rebuildPhase = rp
	}

	ipDBPath = os.Getenv("NODEMGMT_IPDBPATH")
	if ipDBPath == "" {
		ipDBPath = "/app/ytsn/yotta.ipdb"
	}
}
