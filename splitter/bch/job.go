package bch

import (
	"fmt"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/bch"
	"strconv"
	"time"
)

type WorkerJob interface {
	Run()
	Name() string
	run() error
}

type updateMetaDataJob struct {
	splitter *BCHSplitter
	name     string
}

func newUpdateMetaDataJob(splitter *BCHSplitter) *updateMetaDataJob {
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
		log.Error("worker bch: job %s get table list from meta error", j.name)
		return err
	}
	//update meta table
	for _, meta := range metas {
		cond := new(model.Meta)
		cond.Name = meta.Name
		data := new(model.Meta)

		//count table size
		var countSql string
		if j.splitter.cfg.Engine.DriverName() == "mssql" {
			countSql = fmt.Sprintf("SELECT b.rows AS count FROM sysobjects a INNER JOIN sysindexes b ON a.id = b.id WHERE a.type = 'u' AND b.indid in (0,1) AND a.name='%s'", meta.Name)
		} else {
			countSql = fmt.Sprintf("SELECT COUNT(1) FROM `%s`", meta.Name)
		}
		result, err := db.QueryString(countSql)
		if err != nil {
			log.Error("worker bch: job %s get table %s count from meta error", j.name, meta.Name)
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
			log.Error("worker bch: job %s get table %s id from meta error", j.name, meta.Name)
			log.DetailError(err)
			continue
		}
		//update meta
		for _, v := range result {
			id, _ := strconv.ParseInt(v["id"], 10, 64)
			data.LastID = id
			data.Count = count
			_, err = db.Update(data, cond)
			if err != nil {
				log.Error("worker bch: job %s update table %s meta error", j.name, meta.Name)
				log.DetailError(err)
				continue
			}

		}
	}
	stats.Add(MetricCronWorkerJobUpdateMetaData, 1)
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("worker bch: job %s elasped time %s", j.name, elaspedTime.String())
	return nil
}

type getBatchBlockJob struct {
	splitter *BCHSplitter
	name     string
}

func newGetBatchBlockJob(splitter *BCHSplitter) *getBatchBlockJob {
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

func (j *getBatchBlockJob) run() error {
	startTime := time.Now()
	db := service.NewDatabase(j.splitter.cfg.Engine)
	blocks := make([]*model.Block, 0)
	err := db.Desc("height").Limit(1).Find(&blocks)
	if err != nil {
		log.Error("worker bch: job '%s' get latest block error", j.name)
	}
	var start, end int64
	var old, free bool

	now := time.Now()
	if len(blocks) > 0 {
		start = blocks[0].Height + 1
		end = blocks[0].Height + int64(j.splitter.cfg.MaxBatchBlock)
		if (now.Unix() - blocks[0].Timestamp) > 1000 {
			old = true
		} else {
			old = false
		}
	} else {
		start = 0
		end = int64(j.splitter.cfg.MaxBatchBlock)
		old = true
	}

	if now.Sub(j.splitter.latestSaveDataTimestamp).Seconds() > 60 && now.Sub(j.splitter.latestReceiveMessageTimestamp) > 15 {
		log.Warn("worker bch: job '%s' splitter bch no message received or no data processing", j.name)
		free = true
	}

	if free && old {
		log.Info("worker bch: job '%s' get block range from %d to %d", j.name, start, end)
		err = j.splitter.remoteHandler.SendBatchBlock(start, end)
		if err != nil {
			log.Error("worker bch: job '%s' rpc call error", j.name)
			log.DetailError(err)
		}

		stats.Add(MetricCronWorkerJobGetBatchBlock, 1)
	}

	elaspedTime := time.Now().Sub(startTime)
	log.Debug("worker bch: job '%s' elasped time %s", j.name, elaspedTime.String())
	return nil
}
