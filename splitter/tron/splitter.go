package tron

import (
	"fmt"
        "errors"
	"github.com/go-xorm/xorm"
	"github.com/jdcloud-bds/bds/common/httputils"
	"github.com/jdcloud-bds/bds/common/json"
	"github.com/jdcloud-bds/bds/common/kafka"
	"github.com/jdcloud-bds/bds/common/log"
	"github.com/jdcloud-bds/bds/service"
	model "github.com/jdcloud-bds/bds/service/model/tron"
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
	ConcurrentHeight           int
	DatabaseWorkerBuffer       int
	DatabaseWorkerNumber       int
	SkipHeight                 int
	SkipMissBlock              bool
	MaxBatchBlock              int
	Endpoint                   string
	User                       string
	Password                   string
	JSONSchemaFile             string
	JSONSchemaValidationEnable bool
}

type TRONSplitter struct {
	cfg                           *SplitterConfig
	remoteHandler                 *httpHandler
	databaseWorkerChan            chan *TRONBlockData
	databaseWorkerStopChan        chan bool
	missedBlockList               map[int64]bool
	latestSaveDataTimestamp       time.Time
	latestReceiveMessageTimestamp time.Time
	cronWorker                    *CronWorker
	jsonSchemaLoader              gojsonschema.JSONLoader
	singleBlockMode               bool
}

func NewSplitter(cfg *SplitterConfig) (*TRONSplitter, error) {
	var err error
	s := new(TRONSplitter)
	s.cfg = cfg
	s.databaseWorkerChan = make(chan *TRONBlockData, s.cfg.DatabaseWorkerBuffer)
	s.databaseWorkerStopChan = make(chan bool, s.cfg.DatabaseWorkerNumber)
	s.missedBlockList = make(map[int64]bool, 0)
	httpClient := httputils.NewRestClientWithBasicAuth(s.cfg.User, s.cfg.Password)
	s.remoteHandler, err = newHTTPHandler(httpClient, s.cfg.Endpoint)
	if err != nil {
		log.DetailError(err)
		return s, err
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

func (s *TRONSplitter) Start() {
	err := s.cfg.Consumer.Start(s.cfg.Topic)
	if err != nil {
		log.Error("splitter tron: consumer start error")
		log.DetailError(err)
		return
	}
	log.Debug("splitter tron: consumer start topic %s", s.cfg.Topic)

	maxNumber, err := s.getMaxBlockNumberInDB()
	if err != nil {
		return
	}
	log.Debug("splitter tron: max block in db is %d", maxNumber)

	expectedBlock := maxNumber + 1

	err = s.CheckWorkMode()
	if err != nil {
		log.Error("splitter tron: check work mode error")
		log.DetailError(err)
		return
	}
	if s.singleBlockMode {
		err = s.cronWorker.Start()
		if err != nil {
			log.Error("splitter tron: cron worker start error")
			log.DetailError(err)
			return
		}
	} else {
		expectedBlock = int64(s.cfg.ConcurrentHeight) + 1
		for i := 0; i < s.cfg.DatabaseWorkerNumber; i++ {
			go s.databaseWorker(i)
		}
		go s.ConcurrentRPCWorker()
	}

	sending := false
	end := int64(0)
	for {
		select {
		case message := <-s.cfg.Consumer.MessageChannel():
			stats.Add(MetricReceiveMessages, 1)
			s.latestReceiveMessageTimestamp = time.Now()
		START:
			if s.cfg.JSONSchemaValidationEnable {
				startTime := time.Now()
				dataLoader := gojsonschema.NewStringLoader(string(message.Data))
				result, err := gojsonschema.Validate(s.jsonSchemaLoader, dataLoader)
				if err != nil {
					log.Error("splitter tron: json schema validation error")
					log.DetailError(err)
				}
				if !result.Valid() {
					for _, err := range result.Errors() {
						log.Error("splitter tron: data invalid %s", strings.ToLower(err.String()))
					}
					stats.Add(MetricVaildationError, 1)
				} else {
					stats.Add(MetricVaildationSuccess, 1)
				}
				elaspedTime := time.Now().Sub(startTime)
				log.Debug("splitter tron: json schema validation elasped %s", elaspedTime)
			}

			//log.Debug("splitter tron: receive message data :%s", message.Data)
			data, err := ParseBlock(string(message.Data))
			if err != nil {
				stats.Add(MetricParseDataError, 1)
				log.Error("splitter tron: parse block error, retry after 1s")
				log.DetailError(err)
				time.Sleep(time.Second * 1)
				goto START
			}
			if !s.singleBlockMode {
				ConcurrentHeight := int64(s.cfg.ConcurrentHeight)
				if data.Block.BlockNumber > ConcurrentHeight {
					continue
				}
				s.databaseWorkerChan <- data
				continue
			}

			if data.Block.BlockNumber > expectedBlock {
				//log.Debug("splitter tron: block %d is not expected, is %d, skip", data.Block.BlockNumber, expectedBlock)
				if sending == false {
				SEND:
					end = data.Block.BlockNumber
					if data.Block.BlockNumber > expectedBlock+int64(s.cfg.MaxBatchBlock) {
						end = expectedBlock + int64(s.cfg.MaxBatchBlock)
					}
					log.Debug("splitter tron: get block range from %d to %d", expectedBlock, end)
					err = s.remoteHandler.SendBatchBlock(expectedBlock, end)
					if err != nil {
						log.DetailError(err)
						time.Sleep(1 * time.Second)
						goto SEND
					}
				}
				sending = true
				continue
			}

		SAVE:
			if s.cfg.DatabaseEnable {
				//s.databaseWorkerChan <- data
				err, ok := s.CheckReversal(data)
				if err != nil {
					log.Error("splitter tron: block %d check reversal error", data.Block.BlockNumber)
					time.Sleep(1 * time.Second)
					goto SAVE
				}
				if ok {
					continue
				}
				err = s.SaveBlock(data)
				if err != nil {
					log.Error("splitter tron: block %d save error, retry after 100ms.", data.Block.BlockNumber)
					log.DetailError(err)
					time.Sleep(100 * time.Millisecond)
					goto SAVE
				}
				expectedBlock = data.Block.BlockNumber + 1
				if sending && expectedBlock == end {
					log.Debug("splitter tron: receive expected end block is %d and update 'sending' flag from %v to false.", data.Block.BlockNumber, sending)
					sending = false
				}
			}
		}
	}
}

func (s *TRONSplitter) Stop() {
	s.databaseWorkerStopChan <- true
	s.cronWorker.Stop()
}

func (s *TRONSplitter) getMaxBlockNumberInDB() (int64, error) {
	var err error
	var maxNumber int64
	db := service.NewDatabase(s.cfg.Engine)
	blocks := make([]*model.Block, 0)
	err = db.Desc("block_number").Limit(1).Find(&blocks)
	if err != nil {
		log.Error("get max block in db error", err)
		log.DetailError(err)
		return maxNumber, err
	} else {
		if len(blocks) > 0 {
			maxNumber = blocks[0].BlockNumber
		} else {
			log.Warn("splitter tron: database empty")
			maxNumber = 0
		}
	}
	return maxNumber, nil
}

func (s *TRONSplitter) CheckReversal(curBlock *TRONBlockData) (error, bool) {
	if curBlock.Block.BlockNumber == 1 {
		return nil, false
	}
	db := service.NewDatabase(s.cfg.Engine)
	preBlock := make([]*model.Block, 0)
	err := db.Where("block_number = ?", curBlock.Block.BlockNumber-1).Find(&preBlock)
	if err != nil {
		log.DetailError(err)
		return err, false
	}
	if len(preBlock) != 1 {
		log.Warn("splitter tron: can not find previous block %d", curBlock.Block.BlockNumber-1)
		return errors.New("splitter tron: can not find previous block."), false
	}
	if preBlock[0].BlockHash != curBlock.Block.ParentHash {
		log.Warn("splitter tron: block %d is revert", curBlock.Block.BlockNumber-1)
		err = s.remoteHandler.SendBatchBlock(preBlock[0].BlockNumber, curBlock.Block.BlockNumber+1)
		if err != nil {
			log.DetailError(err)
			return err, true
		}
		return nil, true
	}
	log.Debug("splitter tron: check block %d pass", curBlock.Block.BlockNumber)
	return nil, false
}

func (s *TRONSplitter) CheckBlock(curBlock *TRONBlockData) bool {
	if curBlock.Block.BlockNumber == 1 {
		return true
	}
	db := service.NewDatabase(s.cfg.Engine)
	preBlock := make([]*model.Block, 0)
	err := db.Where("block_number = ?", curBlock.Block.BlockNumber-1).Find(&preBlock)
	if err != nil {
		log.DetailError(err)
		return false
	}
	if len(preBlock) != 1 {
		log.Warn("splitter tron: can not find previous block %d", curBlock.Block.BlockNumber-1)
		blocks := make([]*model.Block, 0)
		err = db.Desc("block_number").Limit(1).Find(&blocks)
		if err != nil {
			log.DetailError(err)
		} else {
			end := curBlock.Block.BlockNumber
			log.Debug("splitter tron: get latest block %d from database", blocks[0].BlockNumber)
			if curBlock.Block.BlockNumber > blocks[0].BlockNumber+int64(s.cfg.MaxBatchBlock) {
				end = blocks[0].BlockNumber + int64(s.cfg.MaxBatchBlock)
			}
			log.Debug("splitter tron: get block range from %d to %d", blocks[0].BlockNumber+1, end)
			err = s.remoteHandler.SendBatchBlock(blocks[0].BlockNumber+1, end)
			if err != nil {
				log.DetailError(err)
			}
		}
		return false
	}
	if preBlock[0].BlockHash != curBlock.Block.ParentHash {
		log.Warn("splitter tron: block %d is revert", curBlock.Block.BlockNumber-1)
		err = s.remoteHandler.SendBatchBlock(preBlock[0].BlockNumber, curBlock.Block.BlockNumber)
		if err != nil {
			log.DetailError(err)
		}
	}
	log.Debug("splitter tron: check block %d pass", curBlock.Block.BlockNumber)
	return true
}

func (s *TRONSplitter) SaveBlock(data *TRONBlockData) error {
	startTime := time.Now()
	data.Block.ID = 0
	for _, v := range data.Transactions {
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
		return err
	}

	if s.singleBlockMode {
		blockTemp := new(model.Block)
		blockTemp.BlockNumber = data.Block.BlockNumber
		has, err := tx.Get(blockTemp)
		if err != nil {
			rollbackFunc(err)
			return err
		}
		if has {
			if blockTemp.BlockHash == data.Block.BlockHash {
				log.Warn("splitter tron: block %d has been stored", data.Block.BlockNumber)
				_ = tx.Rollback()
				tx.Close()
				return nil
			} else {
				blocks := make([]*model.Block, 0)
				err = tx.Desc("block_number").Limit(1).Find(&blocks)
				if err != nil {
					rollbackFunc(err)
					return err
				}
				if blocks[0].BlockNumber-data.Block.BlockNumber > 19 {
					log.Warn("splitter tron: block %d reverted is too old", data.Block.BlockNumber)
					_ = tx.Rollback()
					tx.Close()
					return nil
				}
				for i := blocks[0].BlockNumber; i >= data.Block.BlockNumber; i-- {
					err = s.RevertBlock(i, tx)
					if err != nil {
						rollbackFunc(err)
						return err
					}
					stats.Add(MetricRevertBlock, 1)
				}
			}
		}
	}

	var affected int64
	affected, err = tx.BatchInsert(data.Block)
	if err != nil {
		rollbackFunc(err)
		return err
	}
	log.Debug("splitter tron: block write %d rows", affected)

	affected, err = tx.BatchInsert(data.Transactions)
	if err != nil {
		rollbackFunc(err)
		return err
	}
	log.Debug("splitter tron: transaction write %d rows", affected)

	//Insert contract
	for _, transaction := range data.Transactions {
		for _, contract := range transaction.Contracts {
			switch contract.Type {
			case AccountCreateContract:
				accountCreateContract := new(model.AccountCreateContract)
				accountCreateContract.ID = 0
				accountCreateContract.BlockNumber = data.Block.BlockNumber
				accountCreateContract.TransactionHash = transaction.Hash
				accountCreateContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				accountCreateContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				accountCreateContract.AccountAddress = json.Get(contractValueJson, "account_address").String()
				accountCreateContract.Type = json.Get(contractValueJson, "type").Int()

				affected, err = tx.BatchInsert(accountCreateContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case TransferContract:
				transferContract := new(model.TransferContract)
				transferContract.ID = 0
				transferContract.BlockNumber = data.Block.BlockNumber
				transferContract.TransactionHash = transaction.Hash
				transferContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				transferContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				transferContract.ToAddress = json.Get(contractValueJson, "to_address").String()
				transferContract.Amount = json.Get(contractValueJson, "amount").Int()

				affected, err = tx.BatchInsert(transferContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case TransferAssetContract:
				transferAssetContract := new(model.TransferAssetContract)
				transferAssetContract.ID = 0
				transferAssetContract.BlockNumber = data.Block.BlockNumber
				transferAssetContract.TransactionHash = transaction.Hash
				transferAssetContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				transferAssetContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				transferAssetContract.ToAddress = json.Get(contractValueJson, "to_address").String()
				transferAssetContract.Amount = json.Get(contractValueJson, "amount").Int()
				transferAssetContract.AssetName = json.Get(contractValueJson, "asset_name").String()

				affected, err = tx.BatchInsert(transferAssetContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case VoteAssetContract:
				voteAssetContractList := make([]*model.VoteAssetContract, 0)
				contractValueJson := json.Parse(contract.Value).String()
				voteAddressList := json.Get(contractValueJson, "vote_address").Array()
				for i, voteAddress := range voteAddressList {
					voteAssetContract := new(model.VoteAssetContract)
					voteAssetContract.ID = 0
					voteAssetContract.VoteAddress = voteAddress.String()
					voteAssetContract.BlockNumber = data.Block.BlockNumber
					voteAssetContract.TransactionHash = transaction.Hash
					voteAssetContract.Timestamp = transaction.Timestamp

					voteAssetContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
					voteAssetContract.Support = json.Get(contractValueJson, "support").Bool()
					voteAssetContract.Count = json.Get(contractValueJson, "count").Int()
					voteAssetContract.VoteAddressNumber = i + 1

					voteAssetContractList = append(voteAssetContractList, voteAssetContract)
				}

				affected, err = tx.BatchInsert(voteAssetContractList)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case VoteWitnessContract:
				voteWitnessContractList := make([]*model.VoteWitnessContract, 0)
				contractValueJson := json.Parse(contract.Value).String()
				voteList := json.Get(contractValueJson, "votes").Array()
				for i, vote := range voteList {
					voteWitnessContract := new(model.VoteWitnessContract)
					voteWitnessContract.ID = 0
					voteWitnessContract.VoteAddress = json.Get(vote.String(), "vote_address").String()
					voteWitnessContract.BlockNumber = data.Block.BlockNumber
					voteWitnessContract.TransactionHash = transaction.Hash
					voteWitnessContract.Timestamp = transaction.Timestamp

					voteWitnessContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
					voteWitnessContract.Support = json.Get(contractValueJson, "support").Bool()
					voteWitnessContract.VoteCount = json.Get(vote.String(), "vote_count").Int()
					voteWitnessContract.VoteAddressNumber = i + 1

					voteWitnessContractList = append(voteWitnessContractList, voteWitnessContract)
				}

				affected, err = tx.BatchInsert(voteWitnessContractList)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case WitnessCreateContract:
				witnessCreateContract := new(model.WitnessCreateContract)
				witnessCreateContract.ID = 0
				witnessCreateContract.BlockNumber = data.Block.BlockNumber
				witnessCreateContract.TransactionHash = transaction.Hash
				witnessCreateContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				witnessCreateContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				witnessCreateContract.Url = json.Get(contractValueJson, "url").String()

				affected, err = tx.BatchInsert(witnessCreateContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case AssetIssueContract:
				assetIssueContractList := make([]*model.AssetIssueContract, 0)
				contractValueJson := json.Parse(contract.Value).String()
				frozenList := json.Get(contractValueJson, "frozen_supply").Array()
				for i, frozen := range frozenList {
					assetIssueContract := new(model.AssetIssueContract)
					assetIssueContract.ID = 0
					assetIssueContract.BlockNumber = data.Block.BlockNumber
					assetIssueContract.TransactionHash = transaction.Hash
					assetIssueContract.Timestamp = transaction.Timestamp
					assetIssueContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
					assetIssueContract.Name = json.Get(contractValueJson, "name").String()
					assetIssueContract.Abbr = json.Get(contractValueJson, "abbr").String()

					assetIssueContract.FrozenAmount = json.Get(frozen.String(), "frozen_amount").Int()
					assetIssueContract.FrozenDays = json.Get(frozen.String(), "frozen_days").Int()
					assetIssueContract.TotalSupply = json.Get(contractValueJson, "total_supply").Int()
					assetIssueContract.TrxNum = json.Get(contractValueJson, "trx_num").Int()
					assetIssueContract.Precision = json.Get(contractValueJson, "precision").Int()
					assetIssueContract.Num = json.Get(contractValueJson, "num").Int()
					assetIssueContract.StartTime = json.Get(contractValueJson, "start_time").Int() / 1000
					assetIssueContract.EndTime = json.Get(contractValueJson, "end_time").Int() / 1000
					assetIssueContract.Order = json.Get(contractValueJson, "order").Int()
					assetIssueContract.VoteScore = json.Get(contractValueJson, "vote_score").Int()
					assetIssueContract.Description = json.Get(contractValueJson, "description").String()
					assetIssueContract.Url = json.Get(contractValueJson, "url").String()
					assetIssueContract.FreeAssetNetLimit = json.Get(contractValueJson, "free_asset_net_limit").Int()
					assetIssueContract.PublicFreeAssetNetLimit = json.Get(contractValueJson, "public_free_asset_net_limit").Int()
					assetIssueContract.PublicFreeAssetNetUsage = json.Get(contractValueJson, "public_free_asset_net_usage").Int()
					assetIssueContract.PublicLatestFreeNetTime = json.Get(contractValueJson, "public_latest_free_net_time").Int()
					assetIssueContract.FrozenSupplyNum = i + 1
					assetIssueContract.AssetID = json.Get(contractValueJson, "id").String()

					assetIssueContractList = append(assetIssueContractList, assetIssueContract)
				}

				affected, err = tx.BatchInsert(assetIssueContractList)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case WitnessUpdateContract:
				witnessUpdateContract := new(model.WitnessCreateContract)
				witnessUpdateContract.ID = 0
				witnessUpdateContract.BlockNumber = data.Block.BlockNumber
				witnessUpdateContract.TransactionHash = transaction.Hash
				witnessUpdateContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				witnessUpdateContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				witnessUpdateContract.Url = json.Get(contractValueJson, "update_url").String()

				affected, err = tx.BatchInsert(witnessUpdateContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case ParticipateAssetIssueContract:
				participateAssetIssueContract := new(model.ParticipateAssetIssueContract)
				participateAssetIssueContract.ID = 0
				participateAssetIssueContract.BlockNumber = data.Block.BlockNumber
				participateAssetIssueContract.TransactionHash = transaction.Hash
				participateAssetIssueContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				participateAssetIssueContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				participateAssetIssueContract.ToAddress = json.Get(contractValueJson, "to_address").String()
				participateAssetIssueContract.Amount = json.Get(contractValueJson, "amount").Int()
				participateAssetIssueContract.AssetName = json.Get(contractValueJson, "asset_name").String()

				affected, err = tx.BatchInsert(participateAssetIssueContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case AccountUpdateContract:
				accountUpdateContract := new(model.AccountUpdateContract)
				accountUpdateContract.ID = 0
				accountUpdateContract.BlockNumber = data.Block.BlockNumber
				accountUpdateContract.TransactionHash = transaction.Hash
				accountUpdateContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				accountUpdateContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				accountUpdateContract.AccountName = json.Get(contractValueJson, "account_name").String() // can't get account_name instead of code

				affected, err = tx.BatchInsert(accountUpdateContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case FreezeBalanceContract:
				freezeBalanceContract := new(model.FreezeBalanceContract)
				freezeBalanceContract.ID = 0
				freezeBalanceContract.BlockNumber = data.Block.BlockNumber
				freezeBalanceContract.TransactionHash = transaction.Hash
				freezeBalanceContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				freezeBalanceContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				freezeBalanceContract.FrozenBalance = json.Get(contractValueJson, "frozen_balance").Int()
				freezeBalanceContract.FrozenDuration = json.Get(contractValueJson, "frozen_duration").Int()

				affected, err = tx.BatchInsert(freezeBalanceContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case UnfreezeBalanceContract:
				unfreezeBalanceContract := new(model.UnfreezeBalanceContract)
				unfreezeBalanceContract.ID = 0
				unfreezeBalanceContract.BlockNumber = data.Block.BlockNumber
				unfreezeBalanceContract.TransactionHash = transaction.Hash
				unfreezeBalanceContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				unfreezeBalanceContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()

				affected, err = tx.BatchInsert(unfreezeBalanceContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case UnfreezeAssetContract:
				unfreezeAssetContract := new(model.UnfreezeAssetContract)
				unfreezeAssetContract.ID = 0
				unfreezeAssetContract.BlockNumber = data.Block.BlockNumber
				unfreezeAssetContract.TransactionHash = transaction.Hash
				unfreezeAssetContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				unfreezeAssetContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()

				affected, err = tx.BatchInsert(unfreezeAssetContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case WithdrawBalanceContract:
				withdrawBalanceContract := new(model.UnfreezeAssetContract)
				withdrawBalanceContract.ID = 0
				withdrawBalanceContract.BlockNumber = data.Block.BlockNumber
				withdrawBalanceContract.TransactionHash = transaction.Hash
				withdrawBalanceContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				withdrawBalanceContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()

				affected, err = tx.BatchInsert(withdrawBalanceContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case UpdateAssetContract:
				updateAssetContract := new(model.UpdateAssetContract)
				updateAssetContract.ID = 0
				updateAssetContract.BlockNumber = data.Block.BlockNumber
				updateAssetContract.TransactionHash = transaction.Hash
				updateAssetContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				updateAssetContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				updateAssetContract.Description = json.Get(contractValueJson, "description").String()
				updateAssetContract.Url = json.Get(contractValueJson, "url").String()
				updateAssetContract.NewLimit = json.Get(contractValueJson, "new_limit").Int()
				updateAssetContract.NewPublicLimit = json.Get(contractValueJson, "new_public_limit").Int()

				affected, err = tx.BatchInsert(updateAssetContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case CreateSmartContract:
				deployContract := new(model.CreateSmartContract)
				deployContract.ID = 0
				deployContract.BlockNumber = data.Block.BlockNumber
				deployContract.TransactionHash = transaction.Hash
				deployContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				deployContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				deployContract.ContractAddress = json.Get(contractValueJson, "contract_address").String()
				deployContract.Name = json.Get(contractValueJson, "new_contract.name").String()
				deployContract.TokenID = json.Get(contractValueJson, "token_id").Int()
				deployContract.CallTokenValue = json.Get(contractValueJson, "call_token_value").Int()

				affected, err = tx.BatchInsert(deployContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case TriggerSmartContract:
				triggerSmartContract := new(model.TriggerSmartContract)
				triggerSmartContract.ID = 0
				triggerSmartContract.BlockNumber = data.Block.BlockNumber
				triggerSmartContract.TransactionHash = transaction.Hash
				triggerSmartContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				triggerSmartContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				triggerSmartContract.ContractAddress = json.Get(contractValueJson, "contract_address").String()
				//triggerSmartContract.Data = json.Get(contractValueJson, "data").String()
				triggerSmartContract.TokenID = json.Get(contractValueJson, "token_id").Int()
				triggerSmartContract.CallTokenValue = json.Get(contractValueJson, "call_token_value").Int()
				triggerSmartContract.CallValue = json.Get(contractValueJson, "call_value").Int()

				affected, err = tx.BatchInsert(triggerSmartContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case ProposalCreateContract:
				proposalCreateContract := new(model.ProposalCreateContract)
				proposalCreateContract.ID = 0
				proposalCreateContract.BlockNumber = data.Block.BlockNumber
				proposalCreateContract.TransactionHash = transaction.Hash
				proposalCreateContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				proposalCreateContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				proposalCreateContract.Parameters = json.Get(contractValueJson, "parameters").String()

				affected, err = tx.BatchInsert(proposalCreateContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case ProposalApproveContract:
				proposalApproveContract := new(model.ProposalApproveContract)
				proposalApproveContract.ID = 0
				proposalApproveContract.BlockNumber = data.Block.BlockNumber
				proposalApproveContract.TransactionHash = transaction.Hash
				proposalApproveContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				proposalApproveContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				proposalApproveContract.ProposalID = json.Get(contractValueJson, "proposal_id").Int()
				proposalApproveContract.IsAddApproval = json.Get(contractValueJson, "is_add_approval").Bool()

				affected, err = tx.BatchInsert(proposalApproveContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case ProposalDeleteContract:
				proposalDeleteContract := new(model.ProposalApproveContract)
				proposalDeleteContract.ID = 0
				proposalDeleteContract.BlockNumber = data.Block.BlockNumber
				proposalDeleteContract.TransactionHash = transaction.Hash
				proposalDeleteContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				proposalDeleteContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				proposalDeleteContract.ProposalID = json.Get(contractValueJson, "proposal_id").Int()

				affected, err = tx.BatchInsert(proposalDeleteContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case ExchangeCreateContract:
				exchangeCreateContract := new(model.ExchangeCreateContract)
				exchangeCreateContract.ID = 0
				exchangeCreateContract.BlockNumber = data.Block.BlockNumber
				exchangeCreateContract.TransactionHash = transaction.Hash
				exchangeCreateContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				exchangeCreateContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				exchangeCreateContract.FirstTokenID = json.Get(contractValueJson, "first_token_id").String()
				exchangeCreateContract.FirstTokenBalance = json.Get(contractValueJson, "first_token_balance").Int()
				exchangeCreateContract.SecondTokenID = json.Get(contractValueJson, "second_token_id").String()
				exchangeCreateContract.SecondTokenBalance = json.Get(contractValueJson, "second_token_balance").Int()

				affected, err = tx.BatchInsert(exchangeCreateContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case ExchangeInjectContract:
				exchangeInjectContract := new(model.ExchangeInjectContract)
				exchangeInjectContract.ID = 0
				exchangeInjectContract.BlockNumber = data.Block.BlockNumber
				exchangeInjectContract.TransactionHash = transaction.Hash
				exchangeInjectContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				exchangeInjectContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				exchangeInjectContract.TokenID = json.Get(contractValueJson, "token_id").String()
				exchangeInjectContract.ExchangeID = json.Get(contractValueJson, "exchagne_id").Int()
				exchangeInjectContract.Quant = json.Get(contractValueJson, "quant").Int()

				affected, err = tx.BatchInsert(exchangeInjectContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case ExchangeWithdrawContract:
				exchangeWithdrawContract := new(model.ExchangeWithdrawContract)
				exchangeWithdrawContract.ID = 0
				exchangeWithdrawContract.BlockNumber = data.Block.BlockNumber
				exchangeWithdrawContract.TransactionHash = transaction.Hash
				exchangeWithdrawContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				exchangeWithdrawContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				exchangeWithdrawContract.TokenID = json.Get(contractValueJson, "token_id").String()
				exchangeWithdrawContract.ExchangeID = json.Get(contractValueJson, "exchagne_id").Int()
				exchangeWithdrawContract.Quant = json.Get(contractValueJson, "quant").Int()

				affected, err = tx.BatchInsert(exchangeWithdrawContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case ExchangeTransactionContract:
				exchangeTransactionContract := new(model.ExchangeTransactionContract)
				exchangeTransactionContract.ID = 0
				exchangeTransactionContract.BlockNumber = data.Block.BlockNumber
				exchangeTransactionContract.TransactionHash = transaction.Hash
				exchangeTransactionContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				exchangeTransactionContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				exchangeTransactionContract.TokenID = json.Get(contractValueJson, "token_id").String()
				exchangeTransactionContract.ExchangeID = json.Get(contractValueJson, "exchagne_id").Int()
				exchangeTransactionContract.Quant = json.Get(contractValueJson, "quant").Int()
				exchangeTransactionContract.Expected = json.Get(contractValueJson, "expected").Int()

				affected, err = tx.BatchInsert(exchangeTransactionContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case UpdateSettingContract:
				updateSettingContract := new(model.UpdateSettingContract)
				updateSettingContract.ID = 0
				updateSettingContract.BlockNumber = data.Block.BlockNumber
				updateSettingContract.TransactionHash = transaction.Hash
				updateSettingContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				updateSettingContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				updateSettingContract.ContractAddress = json.Get(contractValueJson, "contract_address").String()
				updateSettingContract.ConsumeUserResourcePercent = json.Get(contractValueJson, "consume_user_resource_percent").Int()

				affected, err = tx.BatchInsert(updateSettingContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
			case UpdateEnergyLimitContract:
				updateEnergyLimitContract := new(model.UpdateEnergyLimitContract)
				updateEnergyLimitContract.ID = 0
				updateEnergyLimitContract.BlockNumber = data.Block.BlockNumber
				updateEnergyLimitContract.TransactionHash = transaction.Hash
				updateEnergyLimitContract.Timestamp = transaction.Timestamp

				contractValueJson := json.Parse(contract.Value).String()
				updateEnergyLimitContract.OwnerAddress = json.Get(contractValueJson, "owner_address").String()
				updateEnergyLimitContract.ContractAddress = json.Get(contractValueJson, "contract_address").String()
				updateEnergyLimitContract.OriginEnergyLimit = json.Get(contractValueJson, "origin_energy_limit").Int()

				affected, err = tx.BatchInsert(updateEnergyLimitContract)
				if err != nil {
					log.Error("splitter tron: %s transaction %s write error.", contract.Type, transaction.Hash)
					rollbackFunc(err)
					return err
				}
				log.Debug("splitter tron: %s transaction %s write %d.", contract.Type, transaction.Hash, affected)
				// todo add other contract
			default:
				log.Warn("splitter tron: unknown contract type %s, transaction %s.", contract.Type, transaction.Hash)
			}
		}
	}

	err = tx.Commit()
	if err != nil {
		rollbackFunc(err)
		return err
	}
	tx.Close()
	stats.Add(MetricDatabaseCommit, 1)
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter tron: block %d write done elasped: %s", data.Block.BlockNumber, elaspedTime.String())

	return nil
}

func (s *TRONSplitter) RevertBlock(num int64, tx *service.Transaction) error {
	startTime := time.Now()
	err := revertBlock(num, tx)
	if err != nil {
		return err
	}
	err = revertTransaction(num, tx)
	if err != nil {
		return err
	}
	err = revertContract(num, tx)
	if err != nil {
		return err
	}
	//sql := fmt.Sprintf("DELETE FROM tron_block WHERE block_number = %d", num)
	//affected, err := tx.Execute(sql)
	//if err != nil {
	//	return err
	//}
	//log.Debug("splitter tron: revert block %d from tron_block table, affected", num, affected)
	//
	//sql = fmt.Sprintf("delete from tron_transaction where block_number = %d", num)
	//affected, err = tx.Execute(sql)
	//if err != nil {
	//	return err
	//}
	//log.Debug("splitter tron: revert block %d from tron_transaction table, affected", num, affected)

	err = tx.Commit()
	if err != nil {
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d elasped %s", num, elaspedTime.String())
	return nil
}

func (s *TRONSplitter) databaseWorker(i int) {
	log.Info("splitter tron: starting database worker %d", i)
	for {
		select {
		case data := <-s.databaseWorkerChan:
		RESAVE:
			err := s.SaveBlock(data)
			if err != nil {
				log.Error("splitter tron: block %d save error, retry after 100ms", data.Block.BlockNumber)
				log.DetailError(err)
				time.Sleep(100 * time.Millisecond)
				goto RESAVE
			}
		case stop := <-s.databaseWorkerStopChan:
			if stop {
				msg := fmt.Sprintf("splitter tron: database worker %d stopped", i)
				log.Info(msg)
				return
			}
		}
	}
}

func (s *TRONSplitter) CheckMissedBlock() ([]int64, error) {
	missedList := make([]int64, 0)

	db := service.NewDatabase(s.cfg.Engine)
	sql := fmt.Sprintf("SELECT block_number FROM tron_block ORDER BY block_number ASC")
	data, err := db.QueryString(sql)
	if err != nil {
		return nil, err
	}

	blockList := make([]*model.Block, 0)
	for _, v := range data {
		block := new(model.Block)
		tmp := v["block_number"]
		num, err := strconv.ParseInt(tmp, 10, 64)
		if err != nil {
			return nil, err
		}
		block.BlockNumber = num
		blockList = append(blockList, block)
	}

	if len(blockList) > 0 {
		checkList := make(map[int64]bool, 0)
		for _, b := range blockList {
			checkList[b.BlockNumber] = true
		}

		for i := int64(1); i <= blockList[len(blockList)-1].BlockNumber; i++ {
			if _, ok := checkList[i]; !ok {
				missedList = append(missedList, i)
			}
		}
	}

	return missedList, nil
}

func (s *TRONSplitter) CheckWorkMode() error {
	db := service.NewDatabase(s.cfg.Engine)
	blocks := make([]*model.Block, 0)
	err := db.Desc("block_number").Limit(1).Find(&blocks)
	if err != nil {
		return err
	}
	ConcurrentHeight := int64(s.cfg.ConcurrentHeight)
	if len(blocks) > 0 && blocks[0].BlockNumber >= ConcurrentHeight {
		s.singleBlockMode = true
	} else {
		s.singleBlockMode = false
	}
	return nil
}

func (s *TRONSplitter) ConcurrentRPCWorker() {
	err := s.ConcurrentGetBlock()
	if err != nil {
		log.DetailError(err)
		return
	}
	log.Debug("concurrent rpc worker: get block done")

	err = s.GetMissBlock()
	if err != nil {
		log.Error("concurrent rpc worker: check missed block error")
		log.DetailError(err)
		return
	}
	log.Debug("concurrent rpc worker: get miss block done")

	s.singleBlockMode = true
	for i := 0; i < s.cfg.DatabaseWorkerNumber; i++ {
		s.ConcurrentStop()
	}
	err = s.cronWorker.Start()
	if err != nil {
		log.Error("concurrent rpc worker: cron worker start error")
		log.DetailError(err)
		return
	}
	log.Info("concurrent rpc worker: change work mode")
}

func (s *TRONSplitter) ConcurrentStop() {
	s.databaseWorkerStopChan <- true
}

func (s *TRONSplitter) ConcurrentGetBlock() error {
	db := service.NewDatabase(s.cfg.Engine)
	blocks := make([]*model.Block, 0)
	err := db.Desc("block_number").Limit(1).Find(&blocks)
	if err != nil {
		return err
	}
	var startHeight, ConcurrentHeight int64
	if len(blocks) == 0 {
		startHeight = 0
	} else {
		startHeight = blocks[0].BlockNumber + 1
	}
	ConcurrentHeight = int64(s.cfg.ConcurrentHeight)
	routineCount := s.cfg.DatabaseWorkerNumber
	routineHeight := make(map[int]int64, 0)
	for i := 0; i < routineCount; i++ {
		routineHeight[i] = -1
	}
	for {
		if startHeight > ConcurrentHeight {
			break
		}

		var endHeight int64
		for i := 0; i < routineCount; i++ {
			if routineHeight[i] < 0 {
				if startHeight+int64(s.cfg.MaxBatchBlock) <= ConcurrentHeight {
					endHeight = startHeight + int64(s.cfg.MaxBatchBlock)
				} else {
					endHeight = ConcurrentHeight + 1
				}
				log.Info("SendBatchBlock from ", startHeight, "to ", endHeight, " when i is: ", i)
				routineHeight[i] = endHeight - 1
				go s.remoteHandler.SendBatchBlock(startHeight, endHeight)
				startHeight = endHeight
				if startHeight > ConcurrentHeight {
					break
				}
			} else {
				blockTemp := new(model.Block)
				blockTemp.BlockNumber = routineHeight[i]
				has, err := db.Get(blockTemp)
				if err != nil {
					log.DetailError(err)
				}
				if has {
					routineHeight[i] = -1
				}
			}
			time.Sleep(time.Second * 10)
		}
	}
	return nil
}

func (s *TRONSplitter) GetMissBlock() error {
	for {
		missedBlockList, err := s.CheckMissedBlock()
		if err != nil {
			return err
		}
		if len(missedBlockList) == 0 {
			break
		} else {
			for _, v := range missedBlockList {
				err = s.remoteHandler.SendBlock(v)
				if err != nil {
					log.DetailError(err)
				}
			}
		}
		time.Sleep(time.Second * 60)
	}
	return nil
}
