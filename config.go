package YTDNMgmt

import (
	"log"
	"os"
	"path"
	"strings"

	"github.com/spf13/viper"
)

//Field names of config file
const (
	//bind address of grpc
	GrpcBindAddrField = "grpc-bind-addr"
	//ID of current SN
	SNIDField = "sn-id"
	//count of all SN
	SNCountField = "sn-count"

	////config of eos
	EOSURLField            = "eos.url"
	EOSBPAccountField      = "eos.bp-account"
	EOSBPPrivateKeyField   = "eos.bp-privatekey"
	EOSContractOwnerMField = "eos.contract-ownerm"
	EOSContractOwnerDField = "eos.contract-ownerd"
	EOSShadowAccountField  = "eos.shadow-account"

	//config of PProf
	PprofEnableField   = "pprof.enable"
	PprofBindAddrField = "pprof.bind-addr"

	//config of MQ
	AuramqBindAddrField             = "auramq.bind-addr"
	AuramqRouterBufferSizeField     = "auramq.router-buffer-size"
	AuramqSubscriberBufferSizeField = "auramq.subscriber-buffer-size"
	AuramqReadBufferSizeField       = "auramq.read-buffer-size"
	AuramqWriteBufferSizeField      = "auramq.write-buffer-size"
	AuramqPingWaitField             = "auramq.ping-wait"
	AuramqReadWaitField             = "auramq.read-wait"
	AuramqWriteWaitField            = "auramq.write-wait"
	AuramqMinerSyncTopicField       = "auramq.miner-sync-topic"
	AuramqAllSNURLsField            = "auramq.all-sn-urls"
	AuramqAllowedAccountsField      = "auramq.allowed-accounts"

	//Misc config
	MiscExcludeAddrPrefixField                 = "misc.exclude-addr-prefix"
	MiscPrePurchaseThresholdField              = "misc.pre-purchase-threshold"
	MiscPrePurchaseAmountField                 = "misc.pre-purchase-amount"
	MiscAvaliableMinerTimeGapField             = "misc.avaliable-miner-time-gap"
	MiscMinerAllocRefreshIntervalField         = "misc.miner-alloc-refresh-interval"
	MiscPoolWeightRefreshIntervalField         = "misc.pool-weight-refresh-interval"
	MiscMinerVersionThresholdField             = "misc.miner-version-threshold"
	MiscConnectivityConnectTimeoutField        = "misc.connectivity-connect-timeout"
	MiscConnectivityReadTimeoutField           = "misc.connectivity-read-timeout"
	MiscPoolErrorMinerTimeThresholdField       = "misc.pool-error-miner-time-threshold"
	MiscPoolErrorMinerPercentageThresholdField = "misc.pool-error-miner-percentage-threshold"
	MiscConnectivityTestIntervalField          = "misc.connectivity-test-interval"
	MiscEnableTestField                        = "misc.enable-test"
	MiscEnableExtraWeightParams                = "misc.enable-extra-weight-params"
	MiscIPDBPathField                          = "misc.ipdb-path"
)

//Config system configuration
type Config struct {
	GrpcBindAddr string        `mapstructure:"grpc-bind-addr"`
	SNID         int32         `mapstructure:"sn-id"`
	SNCount      int64         `mapstructure:"sn-count"`
	EOS          *EOSConfig    `mapstructure:"eos"`
	PProf        *PProfConfig  `mapstructure:"pprof"`
	AuraMQ       *AuraMQConfig `mapstructure:"auramq"`
	Misc         *MiscConfig   `mapstructure:"misc"`
}

//EOSConfig EOS configuration
type EOSConfig struct {
	URL            string `mapstructure:"url"`
	BPAccount      string `mapstructure:"bp-account"`
	BPPrivateKey   string `mapstructure:"bp-privatekey"`
	ContractOwnerM string `mapstructure:"contract-ownerm"`
	ContractOwnerD string `mapstructure:"contract-ownerd"`
	ShadowAccount  string `mapstructure:"shadow-account"`
}

//PProfConfig PProf configuration
type PProfConfig struct {
	Enable   bool   `mapstructure:"enable"`
	BindAddr string `mapstructure:"bind-addr"`
}

//AuraMQConfig auramq configuration
type AuraMQConfig struct {
	BindAddr             string   `mapstructure:"bind-addr"`
	RouterBufferSize     int      `mapstructure:"router-buffer-size"`
	SubscriberBufferSize int      `mapstructure:"subscriber-buffer-size"`
	ReadBufferSize       int      `mapstructure:"read-buffer-size"`
	WriteBufferSize      int      `mapstructure:"write-buffer-size"`
	PingWait             int      `mapstructure:"ping-wait"`
	ReadWait             int      `mapstructure:"read-wait"`
	WriteWait            int      `mapstructure:"write-wait"`
	MinerSyncTopic       string   `mapstructure:"miner-sync-topic"`
	AllSNURLs            []string `mapstructure:"all-sn-urls"`
	AllowedAccounts      []string `mapstructure:"allowed-accounts"`
}

//MiscConfig miscellaneous configuration
type MiscConfig struct {
	ExcludeAddrPrefix                 string `mapstructure:"exclude-addr-prefix"`
	PrePurchaseThreshold              int64  `mapstructure:"pre-purchase-threshold"`
	PrePurchaseAmount                 int64  `mapstructure:"pre-purchase-amount"`
	AvaliableMinerTimeGap             int64  `mapstructure:"avaliable-miner-time-gap"`
	MinerAllocRefreshInterval         int64  `mapstructure:"miner-alloc-refresh-interval"`
	PoolWeightRefreshInterval         int64  `mapstructure:"pool-weight-refresh-interval"`
	MinerVersionThreshold             int32  `mapstructure:"miner-version-threshold"`
	ConnectivityConnectTimeout        int32  `mapstructure:"connectivity-connect-timeout"`
	ConnectivityReadTimeout           int32  `mapstructure:"connectivity-read-timeout"`
	PoolErrorMinerTimeThreshold       int64  `mapstructure:"pool-error-miner-time-threshold"`
	PoolErrorMinerPercentageThreshold int32  `mapstructure:"pool-error-miner-percentage-threshold"`
	ConnectivityTestInterval          int32  `mapstructure:"connectivity-test-interval"`
	EnableTest                        bool   `mapstructure:"enable-test"`
	EnableExtraWeightParams           bool   `mapstructure:"enable-extra-weight-params"`
	IPDBPath                          string `mapstructure:"ipdb-path"`
}

//InitConfig decode config file
func InitConfig(eosURL, bpAccount, bpPrivkey, contractOwnerM, contractOwnerD, shadowAccount string, bpID int32) *Config {
	// configDir := os.Getenv("NODEMGMT_CONFIGDIR")
	configDir := os.Getenv("YTSN_HOME")
	if configDir == "" {
		configDir = "../conf"
	} else {
		configDir = path.Join(configDir, "conf")
	}
	viper.SetDefault(GrpcBindAddrField, ":11001")
	viper.SetDefault(PprofEnableField, false)
	viper.SetDefault(PprofBindAddrField, ":6161")
	viper.SetDefault(AuramqBindAddrField, ":8787")
	viper.SetDefault(AuramqRouterBufferSizeField, 1024)
	viper.SetDefault(AuramqSubscriberBufferSizeField, 1024)
	viper.SetDefault(AuramqReadBufferSizeField, 4096)
	viper.SetDefault(AuramqWriteBufferSizeField, 4096)
	viper.SetDefault(AuramqPingWaitField, 30)
	viper.SetDefault(AuramqReadWaitField, 60)
	viper.SetDefault(AuramqWriteWaitField, 10)
	viper.SetDefault(AuramqMinerSyncTopicField, "sync")
	viper.SetDefault(AuramqAllSNURLsField, []string{})
	viper.SetDefault(AuramqAllowedAccountsField, []string{})
	viper.SetDefault(MiscExcludeAddrPrefixField, "")
	viper.SetDefault(MiscPrePurchaseThresholdField, 32768)
	viper.SetDefault(MiscPrePurchaseAmountField, 65536)
	viper.SetDefault(MiscAvaliableMinerTimeGapField, 3)
	viper.SetDefault(MiscMinerAllocRefreshIntervalField, 10)
	viper.SetDefault(MiscPoolWeightRefreshIntervalField, 10)
	viper.SetDefault(MiscMinerVersionThresholdField, 1)
	viper.SetDefault(MiscConnectivityConnectTimeoutField, 10)
	viper.SetDefault(MiscConnectivityReadTimeoutField, 10)
	viper.SetDefault(MiscPoolErrorMinerTimeThresholdField, 14400)
	viper.SetDefault(MiscPoolErrorMinerPercentageThresholdField, 95)
	viper.SetDefault(MiscConnectivityTestIntervalField, 60)
	viper.SetDefault(MiscEnableTestField, false)
	viper.SetDefault(MiscEnableExtraWeightParams, true)
	viper.SetDefault(MiscIPDBPathField, "/app/ytsn/yotta.ipdb")

	viper.AddConfigPath(configDir)
	viper.AddConfigPath(".")
	viper.SetConfigName("nodemgmt")
	viper.SetConfigType("yaml")

	viper.AutomaticEnv()
	viper.SetEnvPrefix("NODEMGMT")
	viper.SetEnvKeyReplacer(strings.NewReplacer("_", "."))

	// If a config file is found, read it in.
	if err := viper.ReadInConfig(); err == nil {
		log.Println("nodemgmt: InitConfig: using config file:", viper.ConfigFileUsed())
	} else {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {
			// Config file not found; ignore error if desired
			log.Fatalln("nodemgmt: InitConfig: config file not found.")
		} else {
			// Config file was found but another error was produced
			log.Fatalln("nodemgmt: InitConfig: Error:", err.Error())
		}
	}
	config := new(Config)
	if err := viper.Unmarshal(config); err != nil {
		log.Fatalf("nodemgmt: InitConfig: unable to decode into config struct, %s\n", err)
	}
	config.EOS = &EOSConfig{URL: eosURL, BPAccount: bpAccount, BPPrivateKey: bpPrivkey, ContractOwnerM: contractOwnerM, ContractOwnerD: contractOwnerD, ShadowAccount: shadowAccount}
	config.SNID = bpID
	return config
}
