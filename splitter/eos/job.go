package eos

import (
	"fmt"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/eos"
	"strconv"
	"time"
)

type WorkerJob interface {
	Run()
	Name() string
	run() error
}

type updateMetaDataJob struct {
	splitter *EOSSplitter
	name     string
}

func newUpdateMetaDataJob(splitter *EOSSplitter) *updateMetaDataJob {
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
		log.Error("worker eos: job %s get table list from meta error", j.name)
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
			log.Error("worker eos: job %s get table %s count from meta error", j.name, meta.Name)
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
			log.Error("worker eos: job %s get table %s id from meta error", j.name, meta.Name)
			log.DetailError(err)
			continue
		}
		for _, v := range result {
			id, _ := strconv.ParseInt(v["id"], 10, 64)
			data.LastID = id
			data.Count = count
			_, err = db.Update(data, cond)
			if err != nil {
				log.Error("worker eos: job %s update table %s meta error", j.name, meta.Name)
				log.DetailError(err)
				continue
			}
		}
	}
	stats.Add(MetricCronWorkerJobUpdateMetaData, 1)
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("worker eos: job %s elasped time %s", j.name, elaspedTime.String())
	return nil
}
