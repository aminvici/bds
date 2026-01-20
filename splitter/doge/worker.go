package doge

import (
	"github.com/aminvici/bds/common/cron"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/config"
)

type CronWorker struct {
	splitter *DOGESplitter
	crontab  *cron.Cron
}

func NewCronWorker(splitter *DOGESplitter) *CronWorker {
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
		log.Debug("worker doge: prepare %s", job.Name())
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

	expr = config.SplitterConfig.CronDOGESetting.UpdateMetaExpr
	if len(expr) != 0 {
		job = newUpdateMetaDataJob(w.splitter)
		err = w.crontab.AddJob(job.Name(), expr, job)
		if err != nil {
			return err
		}
		stats.Add(MetricCronWorkerJob, 1)
		log.Debug("worker doge: add job %s", job.Name())
	}

	expr = config.SplitterConfig.CronDOGESetting.GetBatchBlockExpr
	if len(expr) != 0 {
		job = newGetBatchBlockJob(w.splitter)
		err = w.crontab.AddJob(job.Name(), expr, job)
		if err != nil {
			return err
		}
		stats.Add(MetricCronWorkerJob, 1)
		log.Info("worker doge: add job %s", job.Name())
	}

	w.crontab.Start()
	return nil
}

func (w *CronWorker) Stop() {
	w.crontab.Stop()
}
