package etc

import (
	"github.com/aminvici/bds/common/cuckoofilter"
	"github.com/aminvici/bds/common/metric"
	model "github.com/aminvici/bds/service/model/etc"
	"math/big"
	"sync"
)

const (
	MetricReceiveMessages                       = "receive_messages"
	MetricParseDataError                        = "parse_data_error"
	MetricVaildationSuccess                     = "validation_success"
	MetricVaildationError                       = "validation_error"
	MetricDatabaseRollback                      = "database_rollback"
	MetricDatabaseCommit                        = "database_commit"
	MetricCronWorkerJob                         = "cron_worker_job"
	MetricCronWorkerJobUpdateMetaData           = "cron_worker_job_update_meta_data"
	MetricCronWorkerJobGetBatchBlock            = "cron_worker_job_get_batch_block"
	MetricCronWorkerJobRefreshContractAddresses = "cron_worker_job_refresh_contract_addresses"
	MetricCronWorkerJobRefreshPoolName          = "cron_worker_job_refresh_pool_name"
	MetricRPCCall                               = "rpc_call"
	MetricRevertBlock                           = "revert_block"

	AccountTypeNormal   = 0
	AccountTypeContract = 1
	AccountTypeMiner    = 2
)

var (
	stats                 = metric.NewMap("etc")
	contractAddressFilter = cuckoofilter.New()
	poolNameMap           = new(sync.Map)
	maxBigNumber, _       = new(big.Int).SetString("100000000000000000000000000000000000000", 10)
	defaultBigNumber, _   = new(big.Int).SetString("-1", 10)
)

type ETCBlockData struct {
	Block             *model.Block
	Uncles            []*model.Uncle
	Transactions      []*model.Transaction
	TokenTransactions []*model.TokenTransaction
	Accounts          []*model.Account
	Tokens            []*model.Token
	TokenAccounts     []*model.TokenAccount
}
