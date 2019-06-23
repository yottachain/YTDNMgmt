package eostx

import eos "github.com/eoscanada/eos-go"

type EosTX struct {
	API           *eos.API
	BpAccount     string
	ContractOwner string
}

type Miner struct {
	Owner   eos.AccountName `json:"owner"`
	MinerID uint64          `json:"minerid"`
	Caller  eos.AccountName `json:"caller"`
}

type Profit struct {
	Owner   eos.AccountName `json:"owner"`
	MinerID uint64          `json:"minerid"`
	Space   uint64          `json:"space"`
	Caller  eos.AccountName `json:"caller"`
}

type PledgeData struct {
	MinerID     uint32 `json:"minerid"`
	AccountName string `json:"account_name"`
	Deposit     string `json:"deposit"`
}

type PayForfeit struct {
	User    eos.AccountName `json:"user"`
	MinerID uint64          `json:"minerid"`
	Quant   eos.Asset       `json:"quant"`
	AccType uint8           `json:"acc_type"`
	Caller  eos.AccountName `json:"caller"`
}

type DrawForfeit struct {
	User    eos.AccountName `json:"user"`
	AccType uint8           `json:"acc_type"`
	Caller  eos.AccountName `json:"caller"`
}

// YTASymbol represents the standard YTA symbol on the chain.
var YTASymbol = eos.Symbol{Precision: 4, Symbol: "YTA"}
