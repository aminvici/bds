package eth

import (
	"fmt"
	"github.com/go-xorm/xorm"
	"github.com/aminvici/bds/common/httputils"
	"github.com/aminvici/bds/common/jsonrpc"
	"github.com/aminvici/bds/common/kafka"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/eth"
	"github.com/xeipuuv/gojsonschema"
	"math/big"
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

type ETHSplitter struct {
	cfg                           *SplitterConfig
	remoteHandler                 *rpcHandler
	cronWorker                    *CronWorker
	jsonSchemaLoader              gojsonschema.JSONLoader
	missedBlockList               map[int64]bool
	latestSaveDataTimestamp       time.Time
	latestReceiveMessageTimestamp time.Time
}

func NewSplitter(cfg *SplitterConfig) (*ETHSplitter, error) {
	var err error
	s := new(ETHSplitter)
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

func (s *ETHSplitter) Start() {
	//start kafka consumer
	err := s.cfg.Consumer.Start(s.cfg.Topic)
	if err != nil {
		log.Error("splitter eth: consumer start error")
		log.DetailError(err)
		return
	}

	log.Debug("splitter eth: consumer start topic %s", s.cfg.Topic)
	log.Debug("splitter eth: database enable is %v", s.cfg.DatabaseEnable)

	//start worker
	err = s.cronWorker.Start()
	if err != nil {
		log.Error("splitter eth: cron worker start error")
		log.DetailError(err)
		return
	}

	log.Debug("splitter eth: consumer start topic %s", s.cfg.Topic)
	log.Debug("splitter eth: database enable is %v", s.cfg.DatabaseEnable)

	//initial excepted block
	expectedBlock := int64(-1)
	for {
		select {
		case message := <-s.cfg.Consumer.MessageChannel():
			log.Debug("splitter eth: topic %s receive data on partition %d offset %d length %d",
				message.Topic, message.Partition, message.Offset, len(message.Data))
			stats.Add(MetricReceiveMessages, 1)
			s.latestReceiveMessageTimestamp = time.Now()

		START:
			//check json schema
			if s.cfg.JSONSchemaValidationEnable {
				ok, err := s.jsonSchemaValid(string(message.Data))
				if err != nil {
					log.Error("splitter eth: json schema valid error")
				}
				if !ok {
					log.Warn("splitter eth: json schema valid failed")
				}
			}
			//parser block
			data, err := ParseBlock(string(message.Data))
			if err != nil {
				stats.Add(MetricParseDataError, 1)
				log.Error("splitter eth: block parse error, retry after 5s")
				log.DetailError(err)
				time.Sleep(time.Second * 5)
				goto START
			}

			if _, ok := s.missedBlockList[data.Block.Height]; !ok {
				if expectedBlock > 0 && data.Block.Height > expectedBlock {
					log.Debug("splitter eth: block %d is not expected, is %d, skip", data.Block.Height, expectedBlock)
					continue
				} else if data.Block.Height == expectedBlock {
					expectedBlock = -1
				}

				log.Debug("splitter eth: checking block %d", data.Block.Height)
				//check if block is expected
				ok, height := s.CheckBlock(data)
				if data.Block.Height != 0 && !ok {
					log.Debug("splitter eth: block %d check failed, update expected height %d", data.Block.Height, height)
					end := data.Block.Height
					log.Debug("splitter eth: get latest block %d from database", height-1)
					if data.Block.Height > height+int64(s.cfg.MaxBatchBlock) {
						end = height + int64(s.cfg.MaxBatchBlock) - 1
					}
					log.Debug("splitter eth: get block range from %d to %d", height, end)
					//get batch block
					if height == 0 {
						err = s.remoteHandler.SendBatchBlock(height+1, end)
					} else {
						err = s.remoteHandler.SendBatchBlock(height, end)
					}

					expectedBlock = end
					if err != nil {
						log.DetailError(err)
					}
					continue
				}
			} else {
				log.Debug("splitter eth: block %d is missed", data.Block.Height)
				delete(s.missedBlockList, data.Block.Height)
			}
			//save block
			if s.cfg.DatabaseEnable {
				err = s.SaveBlock(data)
				if err != nil {
					log.Error("splitter eth: block %d save error, retry after 5s", data.Block.Height)
					log.DetailError(err)
					time.Sleep(time.Second * 5)
					goto START
				} else {
					log.Info("splitter eth: block %d save success", data.Block.Height)
					s.cfg.Consumer.MarkOffset(message)
				}
			}
		}
	}
}

func (s *ETHSplitter) Stop() {
	s.cronWorker.Stop()
}

func (s *ETHSplitter) CheckBlock(curBlock *ETHBlockData) (bool, int64) {
	db := service.NewDatabase(s.cfg.Engine)
	height := int64(-1)
	prevBlock := make([]*model.Block, 0)
	//get block that height = data.Block.Height - 1
	err := db.Where("height = ?", curBlock.Block.Height-1).Find(&prevBlock)
	if err != nil {
		log.DetailError(err)
		return false, height
	}

	if len(prevBlock) != 1 {
		log.Warn("splitter eth: can not find previous block %d", curBlock.Block.Height-1)
		blocks := make([]*model.Block, 0)
		//get max height of block
		err = db.Desc("height").Limit(1).Find(&blocks)
		if err != nil {
			log.DetailError(err)
		} else {
			if len(blocks) > 0 {
				height = blocks[0].Height + 1
			} else {
				log.Warn("splitter eth: database empty")
				height = 0
			}
		}
		return false, height
	}

	//judge if need to be reverted
	if prevBlock[0].Hash != curBlock.Block.ParentHash {
		log.Warn("splitter eth: block %d is revert", curBlock.Block.Height-1)
		//get block by rpc
		err = s.remoteHandler.SendBatchBlock(prevBlock[0].Height, curBlock.Block.Height)
		if err != nil {
			log.DetailError(err)
		}
		height = prevBlock[0].Height
		return false, height
	}
	log.Debug("splitter eth: check block %d pass", curBlock.Block.Height)
	// update real difficuty
	difficulty := (big.Int)(curBlock.Block.Difficulty)
	difficultyFloat := new(big.Float).SetInt(&difficulty)
	interval := new(big.Float).SetInt64(curBlock.Block.Timestamp - prevBlock[0].Timestamp)
	second := new(big.Float).SetInt64(15)
	realDifficulty, _ := new(big.Float).Quo(new(big.Float).Mul(difficultyFloat, interval), second).Float64()
	log.Debug("splitter eth: block %d real difficulty %f", curBlock.Block.Height, realDifficulty)
	curBlock.Block.RealDifficulty = realDifficulty
	return true, height
}

func (s *ETHSplitter) SaveBlock(data *ETHBlockData) error {
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
	if has {
		if blockTemp.Hash == data.Block.Hash {
			log.Warn("splitter eth: block %d has been stored", data.Block.Height)
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
			if blocks[0].Height-data.Block.Height > 30 {
				log.Warn("splitter eth: block %d reverted is too old", data.Block.Height)
				_ = tx.Rollback()
				return nil
			}
			for i := blocks[0].Height; i >= data.Block.Height; i-- {
				log.Info("splitter eth: revert block ", i)
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

	//insert block
	var affected int64
	blockList := make([]*model.Block, 0)
	blockList = append(blockList, data.Block)
	affected, err = tx.BatchInsert(blockList)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter eth: block write %d rows", affected)

	//insert transactions
	affected, err = tx.BatchInsert(data.Transactions)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter eth: transaction write %d rows", affected)

	//insert uncles
	affected, err = tx.BatchInsert(data.Uncles)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter eth: uncle write %d rows", affected)

	//insert token transaction
	affected, err = tx.BatchInsert(data.TokenTransactions)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter eth: token transfer write %d rows", affected)

	//insert ENSes
	affected, err = tx.BatchInsert(data.ENSes)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter eth: ens tx write %d rows", affected)

	//insert internal transaction
	affected, err = tx.BatchInsert(data.InternalTransactions)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter eth: internal transactions write %d rows", affected)

	//update token and toke account
	err = updateToken(data, tx)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}

	//update account
	err = updateAccount(data, tx)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}

	//update realDifficulty
	err = updateRealDifficulty(data, tx)
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
	stats.Add(MetricDatabaseCommit, 1)
	elaspedTime := time.Now().Sub(startTime)
	s.latestSaveDataTimestamp = time.Now()
	log.Debug("splitter eth: block %d write done elasped: %s", data.Block.Height, elaspedTime.String())
	return nil
}

//revert block
func (s *ETHSplitter) RevertBlock(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	var err error
	//revert account balance by rpc
	err = revertAccountBalance(height, tx, s.remoteHandler)
	if err != nil {
		return err
	}
	//revert token account by rpc
	err = revertTokenAccount(height, tx, s.remoteHandler)
	if err != nil {
		return err
	}

	//revert miner by height
	err = revertMiner(height, tx)
	if err != nil {
		return err
	}

	//revert block by height
	err = revertBlock(height, tx)
	if err != nil {
		return err
	}

	//revert uncle by height
	err = revertUncle(height, tx)
	if err != nil {
		return err
	}

	//revert transaction by height
	err = revertTransaction(height, tx)
	if err != nil {
		return err
	}

	//revert token transaction by height
	err = revertTokenTransaction(height, tx)
	if err != nil {
		return err
	}

	//revert ens by height
	err = revertENS(height, tx)
	if err != nil {
		return err
	}

	//revert internal transaction by height
	err = revertInternalTransaction(height, tx)
	if err != nil {
		return err
	}

	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eth: revert block %d elasped %s", height, elaspedTime.String())
	return nil
}

type TokenAccountBalance struct {
	Address      string
	TokenAddress string
	Balance      *big.Int
}

//check json schema
func (s *ETHSplitter) jsonSchemaValid(data string) (bool, error) {
	startTime := time.Now()
	dataLoader := gojsonschema.NewStringLoader(data)
	result, err := gojsonschema.Validate(s.jsonSchemaLoader, dataLoader)
	if err != nil {
		log.Error("splitter eth: json schema validation error")
		log.DetailError(err)
		return false, err
	}
	if !result.Valid() {
		for _, err := range result.Errors() {
			log.Error("splitter eth: data invalid %s", strings.ToLower(err.String()))
			return false, nil
		}
		stats.Add(MetricVaildationError, 1)
	} else {
		stats.Add(MetricVaildationSuccess, 1)
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eth: json schema validation elasped %s", elaspedTime)
	return true, nil
}
