package ltc

import (
	"github.com/aminvici/bds/common/cron"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/config"
)

type CronWorker struct {
	splitter *LTCSplitter
	crontab  *cron.Cron
}

func NewCronWorker(splitter *LTCSplitter) *CronWorker {
	worker := new(CronWorker)
	worker.splitter = splitter
	worker.crontab = cron.New()
	return worker
}

func (w *CronWorker) Prepare() error {
	jobList := []WorkerJob{
		newUpdateMetaDataJob(w.splitter),
	}

	for _, job := range jobList {
		log.Debug("worker ltc: prepare %s", job.Name())
		err := job.run()
		if err != nil {
			return err
		}
	}

	return nil
}

func (w *CronWorker) Start() error {
	var err error
	var expr string
	var job WorkerJob

	expr = config.SplitterConfig.CronLTCSetting.UpdateMetaExpr
	if len(expr) != 0 {
		job = newUpdateMetaDataJob(w.splitter)
		err = w.crontab.AddJob(job.Name(), expr, job)
		if err != nil {
			return err
		}
		stats.Add(MetricCronWorkerJob, 1)
		log.Debug("worker ltc: add job %s", job.Name())
	}

	expr = config.SplitterConfig.CronLTCSetting.GetBatchBlockExpr
	if len(expr) != 0 {
		job = newGetBatchBlockJob(w.splitter)
		err = w.crontab.AddJob(job.Name(), expr, job)
		if err != nil {
			return err
		}
		stats.Add(MetricCronWorkerJob, 1)
		log.Info("worker ltc: add job %s", job.Name())
	}

	w.crontab.Start()
	return nil
}

func (w *CronWorker) Stop() {
	w.crontab.Stop()
}
