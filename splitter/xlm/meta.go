package xlm

import (
	"github.com/aminvici/bds/common/metric"
	model "github.com/aminvici/bds/service/model/xlm"
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
	MetricReceiveLedger               = "receive_ledger"
	MetricSendLedger                  = "send_ledger"
	MetricSaveLedger                  = "save_ledger"
)

var (
	stats = metric.NewMap("xlm")
)

type XLMLedgerData struct {
	Ledger       *model.Ledger
	Transactions []*model.Transaction
	Operations   []*model.Operation
}
