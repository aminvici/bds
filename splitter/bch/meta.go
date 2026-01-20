package bch

import (
	"github.com/aminvici/bds/common/metric"
	model "github.com/aminvici/bds/service/model/bch"
)

const (
	MetricReceiveMessages             = "receive_messages"
	MetricParseDataError              = "parse_data_error"
	MetricVaildationSuccess           = "validation_success"
	MetricVaildationError             = "validation_error"
	MetricDatabaseRollback            = "database_rollback"
	MetricDatabaseCommit              = "database_commit"
	MetricCronWorkerJob               = "cron_worker_job"
	MetricCronWorkerJobGetBatchBlock  = "cron_worker_job_get_batch_block"
	MetricCronWorkerJobUpdateMetaData = "cron_worker_job_update_meta_data"
	MetricRPCCall                     = "rpc_call"
	MetricRevertBlock                 = "revert_block"
)

var (
	stats = metric.NewMap("bch")
)

type BCHBlockData struct {
	Block        *model.Block
	Transactions []*model.Transaction
	VIns         []*model.VIn
	VOuts        []*model.VOut
}
