package eos

import (
	"fmt"
	"github.com/go-xorm/xorm"
	"github.com/aminvici/bds/common/cuckoofilter"
	"github.com/aminvici/bds/common/httputils"
	"github.com/aminvici/bds/common/kafka"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/eos"
	"github.com/xeipuuv/gojsonschema"
	"strings"
	"time"
)

type SplitterConfig struct {
	Engine                     *xorm.Engine
	Consumer                   *kafka.ConsumerGroup
	Topic                      string
	DatabaseEnable             bool
	DatabaseWorkerBuffer       int
	DatabaseWorkerNumber       int
	SkipHeight                 int
	SkipMissBlock              bool
	MaxBatchBlock              int
	Endpoint                   string
	KafkaProxyHost             string
	KafkaProxyPort             string
	User                       string
	Password                   string
	JSONSchemaFile             string
	JSONSchemaValidationEnable bool
}

type EOSSplitter struct {
	cfg                    *SplitterConfig
	databaseWorkerChan     chan *EOSBlockData
	databaseWorkerStopChan chan bool
	remoteHandler          *httpHandler
	contractAddressFilter  *cuckoofilter.CuckooFilter
	cronWorker             *CronWorker
	jsonSchemaLoader       gojsonschema.JSONLoader
}

func NewSplitter(cfg *SplitterConfig) (*EOSSplitter, error) {
	var err error
	s := new(EOSSplitter)
	s.cfg = cfg
	s.databaseWorkerChan = make(chan *EOSBlockData, s.cfg.DatabaseWorkerBuffer)
	s.databaseWorkerStopChan = make(chan bool, s.cfg.DatabaseWorkerNumber)
	httpClient := httputils.NewRestClientWithBasicAuth(s.cfg.User, s.cfg.Password)
	s.remoteHandler, err = newHTTPHandler(httpClient, s.cfg.Endpoint, s.cfg.KafkaProxyHost, s.cfg.KafkaProxyPort, s.cfg.Topic)
	if err != nil {
		log.DetailError(err)
		return nil, err
	}

	if s.cfg.JSONSchemaValidationEnable {
		f := fmt.Sprintf("file://%s", s.cfg.JSONSchemaFile)
		s.jsonSchemaLoader = gojsonschema.NewReferenceLoader(f)
	}

	s.cronWorker = NewCronWorker(s)
	err = s.cronWorker.Prepare()
	if err != nil {
		log.DetailError(err)
		return nil, err
	}

	return s, nil
}

func (s *EOSSplitter) Start() {
	for i := 0; i < 1; i++ {
		go s.databaseWorker(i)
	}

	err := s.cfg.Consumer.Start(s.cfg.Topic)
	if err != nil {
		log.Error("splitter eos: consumer start error")
		log.DetailError(err)
		return
	}

	log.Debug("splitter eos: consumer start topic %s", s.cfg.Topic)
	for {
		select {
		case message := <-s.cfg.Consumer.MessageChannel():
			stats.Add(MetricReceiveMessages, 1)
			if s.cfg.JSONSchemaValidationEnable {
				startTime := time.Now()
				dataLoader := gojsonschema.NewStringLoader(string(message.Data))
				result, err := gojsonschema.Validate(s.jsonSchemaLoader, dataLoader)
				if err != nil {
					log.Error("splitter eos: json schema validation error")
					log.DetailError(err)
				}
				if !result.Valid() {
					for _, err := range result.Errors() {
						log.Error("splitter eos: data invalid %s", strings.ToLower(err.String()))
					}
					stats.Add(MetricVaildationError, 1)
				} else {
					stats.Add(MetricVaildationSuccess, 1)
				}
				elaspedTime := time.Now().Sub(startTime)
				log.Debug("splitter eos: json schema validation elasped %s", elaspedTime)
			}

			data, err := ParseBlock(string(message.Data))
			if err != nil {
				stats.Add(MetricParseDataError, 1)
				log.Error("splitter eos: parse block error")
				log.DetailError(err)
				continue
			}

			if s.cfg.DatabaseEnable {
				s.databaseWorkerChan <- data
			}
		}
	}
}

func (s *EOSSplitter) Stop() {
	s.databaseWorkerStopChan <- true
}

func (s *EOSSplitter) CheckBlock(curBlock *EOSBlockData) bool {
	if curBlock.Block.BlockNum == 1 {
		return true
	}
	db := service.NewDatabase(s.cfg.Engine)
	preBlock := make([]*model.Block, 0)
	err := db.Where("block_num = ?", curBlock.Block.BlockNum-1).Find(&preBlock)
	if err != nil {
		log.DetailError(err)
		return false
	}
	if len(preBlock) != 1 {
		log.Warn("splitter eos: can not find previous block %d", curBlock.Block.BlockNum-1)
		blocks := make([]*model.Block, 0)
		err = db.Desc("block_num").Limit(1).Find(&blocks)
		if err != nil {
			log.DetailError(err)
		} else {
			end := curBlock.Block.BlockNum
			log.Debug("splitter eos: get latest block %d from database", blocks[0].BlockNum)
			if curBlock.Block.BlockNum > blocks[0].BlockNum+int64(s.cfg.MaxBatchBlock) {
				end = blocks[0].BlockNum + int64(s.cfg.MaxBatchBlock)
			}
			log.Debug("splitter eos: get block range from %d to %d", blocks[0].BlockNum+1, end)
			err = s.remoteHandler.SendBatchBlock(blocks[0].BlockNum+1, end)
			if err != nil {
				log.DetailError(err)
			}
		}
		return false
	}
	if preBlock[0].Hash != curBlock.Block.Previous {
		log.Warn("splitter eos: block %d is revert", curBlock.Block.BlockNum-1)
		err = s.remoteHandler.SendBatchBlock(preBlock[0].BlockNum, curBlock.Block.BlockNum)
		if err != nil {
			log.DetailError(err)
		}
		return false
	}
	log.Debug("splitter eos: check block %d pass", curBlock.Block.BlockNum)
	return true
}

func (s *EOSSplitter) RevertBlock(num int64, tx *service.Transaction) error {
	startTime := time.Now()
	var err error

	err = revertBlock(num, tx)
	if err != nil {
		return err
	}

	err = revertTransaction(num, tx)
	if err != nil {
		return err
	}

	err = revertAction(num, tx)
	if err != nil {
		return err
	}

	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eos: revert block %d elasped %s", num, elaspedTime.String())
	return nil
}

func (s *EOSSplitter) databaseWorker(i int) {
	log.Info("splitter eos: starting database worker %d", i)
	for {
		select {
		case data := <-s.databaseWorkerChan:
			startTime := time.Now()
			if data.Block.BlockNum != 0 && !s.CheckBlock(data) {
				continue
			}
		START:
			data.Block.ID = 0
			for _, v := range data.Transactions {
				v.ID = 0
			}
			for _, v := range data.Actions {
				v.ID = 0
			}
			tx := service.NewTransaction(s.cfg.Engine)

			rollbackFunc := func(err error) {
				_ = tx.Rollback()
				tx.Close()
				log.DetailError(err)
				stats.Add(MetricDatabaseRollback, 1)
			}

			err := tx.Begin()
			if err != nil {
				rollbackFunc(err)
				goto START
			}
			blockTemp := new(model.Block)
			blockTemp.BlockNum = data.Block.BlockNum
			has, err := tx.Get(blockTemp)
			if err != nil {
				rollbackFunc(err)
				goto START
			}
			if has {
				if blockTemp.Hash == data.Block.Hash {
					log.Warn("splitter eos: block %d has been stored", data.Block.BlockNum)
					_ = tx.Rollback()
					tx.Close()
					continue
				} else {
					blocks := make([]*model.Block, 0)
					err = tx.Desc("block_num").Limit(1).Find(&blocks)
					if err != nil {
						rollbackFunc(err)
						goto START
					}
					if blocks[0].BlockNum-data.Block.BlockNum > 15 {
						log.Warn("splitter eos: block %d reverted is too old", data.Block.BlockNum)
						_ = tx.Rollback()
						tx.Close()
						continue
					}
					for i := blocks[0].BlockNum; i >= data.Block.BlockNum; i-- {
						err = s.RevertBlock(i, tx)
						if err != nil {
							rollbackFunc(err)
							goto START
						}
						stats.Add(MetricRevertBlock, 1)
					}
				}
			}

			//log.Debug("splitter eos: block----%v.", data.Block)
			var affected int64
			affected, err = tx.BatchInsert(data.Block)
			if err != nil {
				rollbackFunc(err)
				goto START
			}
			log.Debug("splitter eos: block write %d rows", affected)

			affected, err = tx.BatchInsert(data.Transactions)
			if err != nil {
				rollbackFunc(err)
				goto START
			}
			log.Debug("splitter eos: transaction write %d rows", affected)

			affected, err = tx.BatchInsert(data.Actions)
			if err != nil {
				rollbackFunc(err)
				goto START
			}
			log.Debug("splitter eos: action write %d rows", affected)

			// no update?
			err = tx.Commit()
			if err != nil {
				rollbackFunc(err)
				goto START
			}
			tx.Close()
			stats.Add(MetricDatabaseCommit, 1)
			elaspedTime := time.Now().Sub(startTime)
			log.Debug("splitter eos: block %d write done elasped: %s", data.Block.BlockNum, elaspedTime.String())
		case stop := <-s.databaseWorkerStopChan:
			if stop {
				msg := fmt.Sprintf("splitter eos: database worker %d stopped", i)
				log.Info(msg)
				return
			}
		}
	}
}
