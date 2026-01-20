package tron

import (
	"github.com/aminvici/bds/common/metric"
	model "github.com/aminvici/bds/service/model/tron"
)

const (
	MetricReceiveMessages             = "receive_messages"
	MetricParseDataError              = "parse_data_error"
	MetricVaildationSuccess           = "validation_success"
	MetricVaildationError             = "validation_error"
	MetricDatabaseRollback            = "database_rollback"
	MetricDatabaseCommit              = "database_commit"
	MetricCronWorkerJob               = "cron_worker_job"
	MetricCronWorkerJobUpdateMetaData = "cron_worker_job_update_meta_data"
	MetricRPCCall                     = "rpc_call"
	MetricRevertBlock                 = "revert_block"
)

const (
	AccountCreateContract         = "AccountCreateContract"
	TransferContract              = "TransferContract"
	TransferAssetContract         = "TransferAssetContract"
	VoteAssetContract             = "VoteAssetContract"
	VoteWitnessContract           = "VoteWitnessContract"
	WitnessCreateContract         = "WitnessCreateContract"
	AssetIssueContract            = "AssetIssueContract"
	WitnessUpdateContract         = "WitnessUpdateContract"
	ParticipateAssetIssueContract = "ParticipateAssetIssueContract"
	AccountUpdateContract         = "AccountUpdateContract"
	FreezeBalanceContract         = "FreezeBalanceContract"
	UnfreezeBalanceContract       = "UnfreezeBalanceContract"
	WithdrawBalanceContract       = "WithdrawBalanceContract"
	UnfreezeAssetContract         = "UnfreezeAssetContract"
	UpdateAssetContract           = "UpdateAssetContract"
	ProposalCreateContract        = "ProposalCreateContract"
	ProposalApproveContract       = "ProposalApproveContract"
	ProposalDeleteContract        = "ProposalDeleteContract"
	SetAccountIdContract          = "SetAccountIdContract"
	CustomContract                = "CustomContract"
	CreateSmartContract           = "CreateSmartContract"
	TriggerSmartContract          = "TriggerSmartContract"
	GetContract                   = "GetContract"
	UpdateSettingContract         = "UpdateSettingContract"
	ExchangeCreateContract        = "ExchangeCreateContract"
	ExchangeInjectContract        = "ExchangeInjectContract"
	ExchangeWithdrawContract      = "ExchangeWithdrawContract"
	ExchangeTransactionContract   = "ExchangeTransactionContract"
	UpdateEnergyLimitContract     = "UpdateEnergyLimitContract"
)

var (
	stats = metric.NewMap("tron")
)

type TRONBlockData struct {
	Block        *model.Block
	Transactions []*model.Transaction
}
