package xrp

import (
	"fmt"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/xrp"
	"strconv"
	"time"
)

type WorkerJob interface {
	Run()
	Name() string
	run() error
}

type updateMetaDataJob struct {
	splitter *XRPSplitter
	name     string
}

func newUpdateMetaDataJob(splitter *XRPSplitter) *updateMetaDataJob {
	j := new(updateMetaDataJob)
	j.splitter = splitter
	j.name = "update meta data"
	return j
}

func (j *updateMetaDataJob) Run() {
	_ = j.run()
}

func (j *updateMetaDataJob) Name() string {
	return j.name
}

func (j *updateMetaDataJob) run() error {
	startTime := time.Now()
	db := service.NewDatabase(j.splitter.cfg.Engine)
	metas := make([]*model.Meta, 0)
	err := db.Find(&metas)
	if err != nil {
		log.Error("worker xrp: job '%s' get table list from meta error", j.name)
		return err
	}

	for _, meta := range metas {
		cond := new(model.Meta)
		cond.Name = meta.Name
		data := new(model.Meta)

		var countSql string
		if j.splitter.cfg.Engine.DriverName() == "mssql" {
			countSql = fmt.Sprintf("SELECT b.rows AS count FROM sysobjects a INNER JOIN sysindexes b ON a.id = b.id WHERE a.type = 'u' AND b.indid in (0,1) AND a.name='%s'", meta.Name)
		} else {
			countSql = fmt.Sprintf("SELECT COUNT(1) FROM `%s`", meta.Name)
		}
		result, err := db.QueryString(countSql)
		if err != nil {
			log.Error("worker xrp: job %s get table %s count from meta error", j.name, meta.Name)
			log.DetailError(err)
			continue
		}
		if len(result) == 0 {
			continue
		}
		count, _ := strconv.ParseInt(result[0]["count"], 10, 64)

		sql := db.Table(meta.Name).Cols("id").Desc("id").Limit(1, 0)
		result, err = sql.QueryString()
		if err != nil {
			log.Error("worker xrp: job '%s' get table %s id from meta error", j.name, meta.Name)
			log.DetailError(err)
			continue
		}
		for _, v := range result {
			id, _ := strconv.ParseInt(v["id"], 10, 64)
			data.LastID = id
			data.Count = count
			_, err = db.Update(data, cond)
			if err != nil {
				log.Error("worker xrp: job '%s' update table %s meta error", j.name, meta.Name)
				log.DetailError(err)
				continue
			}

		}
	}
	stats.Add(MetricCronWorkerJobUpdateMetaData, 1)
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("worker xrp: job '%s' elasped time %s", j.name, elaspedTime.String())
	return nil
}

type getBatchBlockJob struct {
	splitter *XRPSplitter
	name     string
}

func newGetBatchBlockJob(splitter *XRPSplitter) *getBatchBlockJob {
	j := new(getBatchBlockJob)
	j.splitter = splitter
	j.name = "'get batch block'"
	return j
}

func (j *getBatchBlockJob) Run() {
	_ = j.run()
}

func (j *getBatchBlockJob) Name() string {
	return j.name
}

func (j *getBatchBlockJob) run0() error {
	startTime := time.Now()
	db := service.NewDatabase(j.splitter.cfg.Engine)
	totalCompleteLedgers, err := j.splitter.remoteHandler.GetCompleteLedgers()
	if err != nil {
		log.Error("worker xrp: job %s get closed ledgers error.", j.name)
		log.Error("worker xrp: %s", err.Error())
	}
	batchSize := int64(1000)

	ledgerList := make(map[int64]bool, 0)
	sql := "select ledger_index from xrp_block order by ledger_index asc"
	result, err := db.QueryString(sql)
	if err != nil {
		log.Error("worker xrp: get ledger_index from database error")
		log.DetailError(err)
		return err
	}
	for _, v := range result {
		ledger_index, _ := strconv.ParseInt(v["ledger_index"], 10, 64)
		ledgerList[ledger_index] = true
	}
	for _, cl := range totalCompleteLedgers {
		missedLedger := make(map[int64]bool, 0)
		i := int64(0)
		sendStart := cl.endLedger
		sendEnd := cl.startLedger
		for k := cl.startLedger; k <= cl.endLedger; k++ {
			if _, ok := ledgerList[k]; !ok {
				missedLedger[k] = true
				if k < sendStart {
					sendStart = k
				}
				if k > sendEnd {
					sendEnd = k
				}
			}
			i++
			if i >= batchSize {
				if sendEnd >= sendStart && len(missedLedger) > 0 {
					log.Info("splitter xrp: send batch block from %d to %d.", sendStart, sendEnd)
					err = j.splitter.remoteHandler.SendBatchBlock(sendStart, sendEnd)
					if err != nil {
						log.Error("splitter xrp: send batch block error: %s", err.Error())
						return err
					}
				}
				missedLedger = make(map[int64]bool, 0)
				i = 0
				sendStart = cl.endLedger
				sendEnd = k + 1

			}
		}
		if len(missedLedger) > 0 {
			if sendEnd >= sendStart {
				log.Info("splitter xrp: send batch block from %d to %d.", sendStart, sendEnd)
				err = j.splitter.remoteHandler.SendBatchBlock(sendStart, sendEnd)
				if err != nil {
					log.Error("splitter xrp: send batch block error: %s", err.Error())
					return err
				}
			}
		}
	}

	elaspedTime := time.Now().Sub(startTime)
	log.Debug("worker xrp: job '%s' elasped time %s", j.name, elaspedTime.String())
	return nil
}

func (j *getBatchBlockJob) run() error {
	startTime := time.Now()
	db := service.NewDatabase(j.splitter.cfg.Engine)
	totalCompleteLedgers, err := j.splitter.remoteHandler.GetCompleteLedgers()
	if err != nil {
		log.Error("worker xrp: job %s get closed ledgers error\n\n", j.name)
		log.Error("worker xrp: %s", err.Error())
	}
	batchSize := int64(1000)

	ledgerList := make(map[int64]bool, 0)
	sql := "select ledger_index from xrp_block order by ledger_index asc"
	result, err := db.QueryString(sql)
	if err != nil {
		log.Error("worker xrp: get ledger_index from database error")
		log.DetailError(err)
		return err
	}
	for _, v := range result {
		ledger_index, _ := strconv.ParseInt(v["ledger_index"], 10, 64)
		ledgerList[ledger_index] = true
	}
	for _, cl := range totalCompleteLedgers {
		missedLedger := make(map[int64]bool, 0)
		i := int64(0)
		sendStart := int64(0)
		sendEnd := int64(0)
		countStart := false
		for k := cl.startLedger; k <= cl.endLedger; k++ {
			if _, ok := ledgerList[k]; !ok {
				if !countStart {
					sendStart = k
					sendEnd = k
					countStart = true
				} else {
					sendEnd = k
				}
				missedLedger[k] = true
				i++
			} else if countStart {
				break
			}

			if i >= batchSize {
				break
			}
		}
		if len(missedLedger) > 0 {
			if sendEnd >= sendStart {
				log.Info("splitter xrp: send batch block from %d to %d.", sendStart, sendEnd)
				err = j.splitter.remoteHandler.SendBatchBlock(sendStart, sendEnd)
				if err != nil {
					log.Error("splitter xrp: send batch block error: %s", err.Error())
					return err
				}
			}
		}

	}

	elaspedTime := time.Now().Sub(startTime)
	log.Debug("worker xrp: job '%s' elasped time %s", j.name, elaspedTime.String())
	return nil
}
