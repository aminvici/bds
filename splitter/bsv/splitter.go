package bsv

import (
	"fmt"
	"github.com/go-xorm/xorm"
	"github.com/aminvici/bds/common/httputils"
	"github.com/aminvici/bds/common/jsonrpc"
	"github.com/aminvici/bds/common/kafka"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/config"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/bsv"
	"github.com/xeipuuv/gojsonschema"
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
}

type BSVSplitter struct {
	cfg                           *SplitterConfig
	remoteHandler                 *rpcHandler
	cronWorker                    *CronWorker
	jsonSchemaLoader              gojsonschema.JSONLoader
	missedBlockList               map[int64]bool
	latestSaveDataTimestamp       time.Time
	latestReceiveMessageTimestamp time.Time
}

func NewSplitter(cfg *SplitterConfig) (*BSVSplitter, error) {
	var err error
	s := new(BSVSplitter)
	s.cfg = cfg
	s.missedBlockList = make(map[int64]bool, 0)
	httpClient := httputils.NewRestClientWithBasicAuth(s.cfg.User, s.cfg.Password)
	s.remoteHandler, err = newRPCHandler(jsonrpc.New(httpClient, s.cfg.Endpoint))
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

func (s *BSVSplitter) Stop() {
	s.cronWorker.Stop()
}

func (s *BSVSplitter) CheckBlock(curBlock *BSVBlockData) (bool, int64) {
	db := service.NewDatabase(s.cfg.Engine)
	height := int64(-1)
	preBlock := make([]*model.Block, 0)
	err := db.Where("height = ?", curBlock.Block.Height-1).Find(&preBlock)
	if err != nil {
		log.DetailError(err)
		return false, height
	}
	if len(preBlock) != 1 {
		var start, end int64
		log.Warn("splitter bsv: can not find previous block %d", curBlock.Block.Height-1)
		blocks := make([]*model.Block, 0)
		err = db.Desc("height").Limit(1).Find(&blocks)
		if err != nil {
			log.DetailError(err)
			return false, height
		} else {
			if len(blocks) == 0 {
				start = -1
			} else {
				start = blocks[0].Height
			}
			end = curBlock.Block.Height
			log.Debug("splitter bsv: get latest block %d from database", start)
			if curBlock.Block.Height > start+int64(s.cfg.MaxBatchBlock) {
				end = start + int64(s.cfg.MaxBatchBlock)
			}
			log.Debug("splitter bsv: get block range from %d to %d", start+1, end)
			err = s.remoteHandler.SendBatchBlock(start+1, end)
			if err != nil {
				log.DetailError(err)
			}
			return false, start + 1
		}

	}
	if preBlock[0].Hash != curBlock.Block.PreviousHash {
		log.Warn("splitter bsv: block %d is revert", curBlock.Block.Height-1)
		err = s.remoteHandler.SendBatchBlock(preBlock[0].Height, curBlock.Block.Height)
		if err != nil {
			log.DetailError(err)
		}
		return false, preBlock[0].Height
	}
	log.Debug("splitter bsv: check block %d pass", curBlock.Block.Height)
	return true, height
}

//revert block by height
func (s *BSVSplitter) RevertBlock(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	//revert vout is_used, address value, miner coinbase_times
	err := revertBlock(height, tx)
	if err != nil {
		return err
	}
	//revert block table
	sql := fmt.Sprintf("DELETE from bsv_block WHERE height = %d", height)
	affected, err := tx.Execute(sql)
	if err != nil {
		return err
	}
	log.Debug("splitter bsv: revert block %d from bsv_block table, affected", height, affected)
	//revert transaction table
	sql = fmt.Sprintf("delete from bsv_transaction where block_height = %d", height)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	log.Debug("splitter bsv: revert block %d from bsv_transaction table, affected", height, affected)
	//revert vin table
	sql = fmt.Sprintf("DELETE FROM bsv_vin WHERE block_height = %d", height)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	log.Debug("splitter bsv: revert block %d from bsv_vin table, affected", height, affected)
	//revert vout table
	sql = fmt.Sprintf("DELETE FROM bsv_vout WHERE block_height = %d", height)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	log.Debug("splitter bsv: revert block %d from bsv_vout table, affected", height, affected)

	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter bsv: revert block %d elasped %s", height, elaspedTime.String())
	return nil
}

func (s *BSVSplitter) Start() {
	//start kafka consumer
	err := s.cfg.Consumer.Start(s.cfg.Topic)
	if err != nil {
		log.Error("splitter bsv: consumer start error")
		log.DetailError(err)
		return
	}

	log.Debug("splitter bsv: consumer start topic %s", s.cfg.Topic)
	log.Debug("splitter bsv: database enable is %v", s.cfg.DatabaseEnable)

	//start cron worker
	err = s.cronWorker.Start()
	if err != nil {
		log.Error("splitter bsv: cron worker start error")
		log.DetailError(err)
		return
	}

	for {
		select {
		case message := <-s.cfg.Consumer.MessageChannel():
			log.Debug("splitter bsv: topic %s receive data on partition %d offset %d length %d",
				message.Topic, message.Partition, message.Offset, len(message.Data))
			stats.Add(MetricReceiveMessages, 1)
			s.latestReceiveMessageTimestamp = time.Now()

		START:
			//JSON schema check
			if s.cfg.JSONSchemaValidationEnable {
				ok, err := s.jsonSchemaValid(string(message.Data))
				if err != nil {
					log.Error("splitter bsv: json schema valid error")
				}
				if !ok {
					log.Warn("splitter bsv: json schema valid failed")
				}
			}

			//parse block
			data, err := ParseBlock(string(message.Data))
			if err != nil {
				stats.Add(MetricParseDataError, 1)
				log.Error("splitter bsv: block parse error, retry after 5s")
				log.DetailError(err)
				time.Sleep(time.Second * 5)
				goto START
			}

			//check block
			if _, ok := s.missedBlockList[data.Block.Height]; !ok {
				log.Debug("splitter bsv: checking block %d", data.Block.Height)
				ok, height := s.CheckBlock(data)
				if data.Block.Height != 0 && !ok {
					log.Debug("splitter bsv: block check failed, expected height %d, this block height %d", height, data.Block.Height)
					continue
				}
			} else {
				log.Debug("splitter bsv: block %d is missed", data.Block.Height)
				delete(s.missedBlockList, data.Block.Height)
			}

			//save block
			if s.cfg.DatabaseEnable {
				err = s.SaveBlock(data)
				if err != nil {
					log.Error("splitter bsv: block %d save error, retry after 5s", data.Block.Height)
					log.DetailError(err)
					time.Sleep(time.Second * 5)
					goto START
				} else {
					log.Info("splitter bsv: block %d save success", data.Block.Height)
					s.cfg.Consumer.MarkOffset(message)
				}
			}
		}
	}
}

//check json schema
func (s *BSVSplitter) jsonSchemaValid(data string) (bool, error) {
	startTime := time.Now()
	dataLoader := gojsonschema.NewStringLoader(data)
	result, err := gojsonschema.Validate(s.jsonSchemaLoader, dataLoader)
	if err != nil {
		log.Error("splitter bsv: json schema validation error")
		log.DetailError(err)
		return false, err
	}
	if !result.Valid() {
		for _, err := range result.Errors() {
			log.Error("splitter bsv: data invalid %s", strings.ToLower(err.String()))
			return false, nil
		}
		stats.Add(MetricVaildationError, 1)
	} else {
		stats.Add(MetricVaildationSuccess, 1)
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter bsv: json schema validation elasped %s", elaspedTime)
	return true, nil
}

func (s *BSVSplitter) SaveBlock(data *BSVBlockData) error {
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
	blockTemp.Height = data.Block.Height
	has, err := tx.Get(blockTemp)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	//judge if block has been stored and if the block needs to be reverted
	if data.Block.Height == 0 {
		blocks := make([]*model.Block, 0)
		err = tx.Desc("height").Limit(1).Find(&blocks)
		if err != nil {
			log.DetailError(err)
			return err
		}
		if len(blocks) != 0 {
			log.Warn("splitter bsv: block %d has been stored", data.Block.Height)
			_ = tx.Rollback()
			return nil
		}
	}
	if data.Block.Height != 0 && has {
		if blockTemp.Hash == data.Block.Hash {
			log.Warn("splitter bsv: block %d has been stored", data.Block.Height)
			_ = tx.Rollback()
			return nil
		} else {
			blocks := make([]*model.Block, 0)
			err = tx.Desc("height").Limit(1).Find(&blocks)
			if err != nil {
				_ = tx.Rollback()
				log.DetailError(err)
				stats.Add(MetricDatabaseRollback, 1)
				return err
			}
			if blocks[0].Height-data.Block.Height > 6 {
				log.Warn("splitter bsv: block %d reverted is too old", data.Block.Height)
				_ = tx.Rollback()
				return nil
			}
			for i := blocks[0].Height; i >= data.Block.Height; i-- {
				err = s.RevertBlock(i, tx)
				if err != nil {
					_ = tx.Rollback()
					log.DetailError(err)
					stats.Add(MetricDatabaseRollback, 1)
					return err
				}
				stats.Add(MetricRevertBlock, 1)
			}
		}
	}
	var affected int64
	version := data.Block.Version

	//Fill in the name of the miner
	err = GetBlockMiner(data, tx)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}

	//insert block
	blockList := make([]*model.Block, 0)
	blockList = append(blockList, data.Block)
	affected, err = tx.BatchInsert(blockList)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	if config.SplitterConfig.DatabaseBSVSetting.Type != "postgres" {
		sql := fmt.Sprintf("UPDATE bsv_block SET version='%d' WHERE height='%d'", version, data.Block.Height)
		_, err = tx.Execute(sql)
		if err != nil {
			_ = tx.Rollback()
			log.DetailError(err)
			stats.Add(MetricDatabaseRollback, 1)
			return err
		}
	}
	log.Debug("splitter bsv: block write %d rows", affected)

	//insert vouts
	affected, err = tx.BatchInsert(data.VOuts)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter bsv: vout write %d rows", affected)

	//get vin address and value
	err = updateVInAddressAndValue(tx, data)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}

	var txVersion []int64
	for _, v := range data.Transactions {
		txVersion = append(txVersion, v.Version)
	}

	//insert transactions
	affected, err = tx.BatchInsert(data.Transactions)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	if config.SplitterConfig.DatabaseBSVSetting.Type != "postgres" {
		err = updateTransactionVersion(tx, txVersion, data)
		if err != nil {
			_ = tx.Rollback()
			log.DetailError(err)
			stats.Add(MetricDatabaseRollback, 1)
			return err
		}
	}
	log.Debug("splitter bsv: transaction write %d rows", affected)

	//insert vins
	affected, err = tx.BatchInsert(data.VIns)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter bsv: vin write %d rows", affected)

	//update address value, vout is_used, miner coinbase_times after each block
	err = UpdateBlock(data, tx)
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
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}

	tx.Close()
	stats.Add(MetricDatabaseCommit, 1)
	elaspedTime := time.Now().Sub(startTime)
	s.latestSaveDataTimestamp = time.Now()
	log.Debug("splitter bsv: block %d write done elasped: %s", data.Block.Height, elaspedTime.String())
	return nil
}
