package xrp

import (
	"fmt"
	"github.com/go-xorm/xorm"
	"github.com/aminvici/bds/common/httputils"
	"github.com/aminvici/bds/common/jsonrpc"
	"github.com/aminvici/bds/common/kafka"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/xrp"
	"github.com/xeipuuv/gojsonschema"
	"strconv"
	"strings"
	"time"
)

type SplitterConfig struct {
	Engine                     *xorm.Engine
	Consumer                   *kafka.ConsumerGroup
	Topic                      string
	DatabaseEnable             bool
	MaxBatchBlock              int
	Endpoint                   string
	User                       string
	Password                   string
	JSONSchemaFile             string
	JSONSchemaValidationEnable bool
	DatabaseWorkerNumber       int
	DatabaseWorkerBuffer       int
}

type XRPSplitter struct {
	cfg                           *SplitterConfig
	remoteHandler                 *rpcHandler
	cronWorker                    *CronWorker
	jsonSchemaLoader              gojsonschema.JSONLoader
	missedBlockList               map[int64]bool
	latestSaveDataTimestamp       time.Time
	latestReceiveMessageTimestamp time.Time
	databaseWorkerChan            chan *XRPBlockData
	databaseWorkerStopChan        chan bool
}

func NewSplitter(cfg *SplitterConfig) (*XRPSplitter, error) {
	var err error
	s := new(XRPSplitter)
	s.cfg = cfg
	s.databaseWorkerChan = make(chan *XRPBlockData, cfg.DatabaseWorkerBuffer)
	s.databaseWorkerStopChan = make(chan bool, 0)
	s.missedBlockList = make(map[int64]bool, 0)
	httpClient := httputils.NewRestClientWithBasicAuth(s.cfg.User, s.cfg.Password)
	s.remoteHandler, err = newRPCHandler(jsonrpc.New(httpClient, s.cfg.Endpoint))
	if err != nil {
		log.DetailError(err)
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

func (s *XRPSplitter) Start() {
	err := s.cronWorker.Start()
	if err != nil {
		log.Error("splitter xrp: cron worker start error")
		log.DetailError(err)
		return
	}

	err = s.cfg.Consumer.Start(s.cfg.Topic)
	if err != nil {
		log.Error("splitter xrp: consumer start error")
		log.DetailError(err)
		return
	}

	for i := 0; i < s.cfg.DatabaseWorkerNumber; i++ {
		go s.databaseWorker(i)
	}

	log.Debug("splitter xrp: consumer start topic %s", s.cfg.Topic)
	log.Debug("splitter xrp: database enable is %v", s.cfg.DatabaseEnable)

	for {
		select {
		case message := <-s.cfg.Consumer.MessageChannel():
			stats.Add(MetricReceiveMessages, 1)
			s.latestReceiveMessageTimestamp = time.Now()

		START:
			if s.cfg.JSONSchemaValidationEnable {
				ok, err := s.jsonSchemaValid(string(message.Data))
				if err != nil {
					log.Error("splitter xrp: json schema valid error")
				}
				if !ok {
					log.Warn("splitter xrp: json schema valid failed")
				}
			}

			data, err := ParseBlock(string(message.Data))
			if err != nil {
				stats.Add(MetricParseDataError, 1)
				log.Error("splitter xrp: block parse error, retry after 5s")
				log.DetailError(err)
				time.Sleep(time.Second * 5)
				goto START
			}

			if s.cfg.DatabaseEnable {
				s.databaseWorkerChan <- data
				s.cfg.Consumer.MarkOffset(message)
			}
		}
	}
}

func (s *XRPSplitter) Stop() {
	s.cronWorker.Stop()
}

func (s *XRPSplitter) CheckBlock(curBlock *XRPBlockData) (bool, int64) {
	db := service.NewDatabase(s.cfg.Engine)
	height := int64(-1)
	prevBlock := make([]*model.Block, 0)
	err := db.Where("height = ?", curBlock.Block.LedgerIndex-1).Find(&prevBlock)
	if err != nil {
		log.DetailError(err)
		return false, height
	}

	if len(prevBlock) != 1 {
		log.Warn("splitter xrp: can not find previous block %d", curBlock.Block.LedgerIndex-1)
		blocks := make([]*model.Block, 0)
		err = db.Desc("height").Limit(1).Find(&blocks)
		if err != nil {
			log.DetailError(err)
		} else {
			if len(blocks) > 0 {
				height = blocks[0].LedgerIndex + 1
			} else {
				log.Warn("splitter xrp: database empty")
				height = 0
			}
		}
		return false, height
	}

	if prevBlock[0].Hash != curBlock.Block.ParentHash {
		log.Warn("splitter xrp: block %d is revert", curBlock.Block.LedgerIndex-1)
		err = s.remoteHandler.SendBatchBlock(prevBlock[0].LedgerIndex, curBlock.Block.LedgerIndex)
		if err != nil {
			log.DetailError(err)
		}
		height = prevBlock[0].LedgerIndex
		return false, height
	}
	log.Debug("splitter xrp: check block %d pass", curBlock.Block.LedgerIndex)
	return true, height
}

func (s *XRPSplitter) SaveBlock(data *XRPBlockData) error {
	startTime := time.Now()
	tx := service.NewTransaction(s.cfg.Engine)
	defer tx.Close()

	err := tx.Begin()
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	blockTemp := new(model.Block)
	blockTemp.LedgerIndex = data.Block.LedgerIndex
	has, err := tx.Get(blockTemp)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	if has {
		if blockTemp.Hash == data.Block.Hash || blockTemp.CloseTime < data.Block.CloseTime {
			log.Warn("splitter xrp: block %d has been stored", data.Block.LedgerIndex)
			_ = tx.Rollback()
			return nil
		} else {
			log.Warn("splitter xrp: block %d need to be replaced by the new one", data.Block.LedgerIndex)
			s.revertLedger(data.Block.LedgerIndex)
		}
	}

	var affected int64
	blocks := make([]*model.Block, 0)
	blocks = append(blocks, data.Block)
	affected, err = tx.BatchInsert(blocks)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter xrp: block write %d rows", affected)

	affected, err = tx.BatchInsert(data.Transactions)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter xrp: transaction write %d rows", affected)

	affected, err = tx.BatchInsert(data.AffectedNodes)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter xrp: affected nodes write %d rows", affected)

	affected, err = tx.BatchInsert(data.Paths)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter xrp: paths write %d rows", affected)

	affected, err = tx.BatchInsert(data.Amounts)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter xrp: amounts write %d rows", affected)
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		return err
	}
	tx.Close()
	stats.Add(MetricDatabaseCommit, 1)
	elaspedTime := time.Now().Sub(startTime)
	s.latestSaveDataTimestamp = time.Now()
	log.Debug("splitter xrp: block %d write done elasped: %s", data.Block.LedgerIndex, elaspedTime.String())
	return nil
}
func (s *XRPSplitter) revertLedger(ledgerIndex int64) error {
	tx := service.NewTransaction(s.cfg.Engine)
	defer tx.Close()

	err := tx.Begin()
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	sql := fmt.Sprintf("delete from xrp_block where ledger_index = %d", ledgerIndex)
	_, err = tx.Exec(sql)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	sql = fmt.Sprintf("delete from xrp_transaction where ledger_index = %d", ledgerIndex)
	_, err = tx.Exec(sql)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	sql = fmt.Sprintf("delete from xrp_amount where ledger_index = %d", ledgerIndex)
	_, err = tx.Exec(sql)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	sql = fmt.Sprintf("delete from xrp_transaction_affected_nodes where ledger_index = %d", ledgerIndex)
	_, err = tx.Exec(sql)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	sql = fmt.Sprintf("delete from xrp_path where ledger_index = %d", ledgerIndex)
	_, err = tx.Exec(sql)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		return err
	}
	tx.Close()
	return nil
}
func (s *XRPSplitter) CheckMissedBlock() ([]int64, error) {
	missedList := make([]int64, 0)

	db := service.NewDatabase(s.cfg.Engine)
	sql := fmt.Sprintf("SELECT height FROM xrp_block ORDER BY height ASC")
	data, err := db.QueryString(sql)
	if err != nil {
		return nil, err
	}

	blockList := make([]*model.Block, 0)
	for _, v := range data {
		block := new(model.Block)
		tmp := v["height"]
		height, err := strconv.ParseInt(tmp, 10, 64)
		if err != nil {
			return nil, err
		}
		block.LedgerIndex = height
		blockList = append(blockList, block)
	}

	if len(blockList) > 0 {
		checkList := make(map[int64]bool, 0)
		for _, b := range blockList {
			checkList[b.LedgerIndex] = true
		}

		for i := int64(0); i <= blockList[len(blockList)-1].LedgerIndex; i++ {
			if _, ok := checkList[i]; !ok {
				missedList = append(missedList, i)
			}
		}
	}

	return missedList, nil
}

func (s *XRPSplitter) jsonSchemaValid(data string) (bool, error) {
	startTime := time.Now()
	dataLoader := gojsonschema.NewStringLoader(data)
	result, err := gojsonschema.Validate(s.jsonSchemaLoader, dataLoader)
	if err != nil {
		log.Error("splitter xrp: json schema validation error")
		log.DetailError(err)
		return false, err
	}
	if !result.Valid() {
		for _, err := range result.Errors() {
			log.Error("splitter xrp: data invalid %s", strings.ToLower(err.String()))
			return false, nil
		}
		stats.Add(MetricVaildationError, 1)
	} else {
		stats.Add(MetricVaildationSuccess, 1)
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter xrp: json schema validation elasped %s", elaspedTime)
	return true, nil
}

func (s *XRPSplitter) databaseWorker(i int) {
	log.Info("splitter xrp: starting database worker %d", i)
	for {
		select {
		case data := <-s.databaseWorkerChan:
			err := s.SaveBlock(data)
			if err != nil {
				log.Error("splitter xrp: block %d save error, retry after 5s", data.Block.LedgerIndex)
				log.DetailError(err)
			}
		case stop := <-s.databaseWorkerStopChan:
			if stop {
				msg := fmt.Sprintf("splitter xrp: database worker %d stopped", i)
				log.Info("splitter xrp: ", msg)
				return
			}
		}
	}
}
