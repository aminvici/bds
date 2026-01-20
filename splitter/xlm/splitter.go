package xlm

import (
	"fmt"
	"github.com/go-xorm/xorm"
	"github.com/aminvici/bds/common/kafka"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/xlm"
	"github.com/xeipuuv/gojsonschema"
	"net/http"
	"strings"
	"time"
)

type SplitterConfig struct {
	Engine                     *xorm.Engine
	Consumer                   *kafka.ConsumerGroup
	Topic                      string
	DatabaseEnable             bool
	MaxBatchLedger             int
	Endpoint                   string
	JSONSchemaFile             string
	JSONSchemaValidationEnable bool
	ConcurrentHeight           int64
	ConcurrentHTTP             int64
	DatabaseWorkerBuffer       int
	DatabaseWorkerNumber       int
}

type XLMSplitter struct {
	cfg                           *SplitterConfig
	cronWorker                    *CronWorker
	jsonSchemaLoader              gojsonschema.JSONLoader
	latestSaveDataTimestamp       time.Time
	latestReceiveMessageTimestamp time.Time
	databaseWorkerChan            chan *XLMLedgerData
	databaseWorkerStopChan        chan bool
}

func NewSplitter(cfg *SplitterConfig) (*XLMSplitter, error) {
	var err error
	s := new(XLMSplitter)
	s.cfg = cfg
	if s.cfg.JSONSchemaValidationEnable {
		f := fmt.Sprintf("file://%s", s.cfg.JSONSchemaFile)
		s.jsonSchemaLoader = gojsonschema.NewReferenceLoader(f)
	}
	s.databaseWorkerChan = make(chan *XLMLedgerData, cfg.DatabaseWorkerBuffer)
	s.databaseWorkerStopChan = make(chan bool, 0)

	s.cronWorker = NewCronWorker(s)
	err = s.cronWorker.Prepare()
	if err != nil {
		log.DetailError(err)
		return nil, err
	}

	return s, nil
}

func (s *XLMSplitter) Start() {
	err := s.cronWorker.Start()
	if err != nil {
		log.Error("splitter xlm: cron worker start error")
		log.DetailError(err)
		return
	}
	for i := 0; i < s.cfg.DatabaseWorkerNumber; i++ {
		go s.databaseWorker(i)
	}
GETMAXSEQ:
	maxLedger, err := s.getMaxSequenceInDB()
	if err != nil {
		log.Error("splitter xlm:get max seq error:", err)
		log.DetailError(err)
		time.Sleep(1 * time.Second)
		goto GETMAXSEQ
	}
	log.Debug("splitter xlm: max sequence in db is %d", maxLedger)

	if maxLedger < s.cfg.ConcurrentHeight {
		s.getLedgerFromHorizon(maxLedger)
	}
UPDATEMAXSEQ:
	maxLedger, err = s.getMaxSequenceInDB()
	if err != nil {
		log.Error("splitter xlm:update  max seq error:", err)
		log.DetailError(err)
		time.Sleep(1 * time.Second)
		goto UPDATEMAXSEQ
	}
	log.Debug("splitter xlm: max sequence in db is %d", maxLedger)

	err = s.cfg.Consumer.Start(s.cfg.Topic)
	if err != nil {
		log.Error("splitter xlm: consumer start error")
		log.DetailError(err)
		return
	}

	log.Debug("splitter xlm: consumer start topic %s", s.cfg.Topic)
	log.Debug("splitter xlm: database enable is %v", s.cfg.DatabaseEnable)

	expectedLedger := maxLedger + 1
	sendFlag := true
	for {
		select {
		case message := <-s.cfg.Consumer.MessageChannel():
			log.Debug("splitter xlm: topic %s receive data on partition %d offset %d length %d",
				message.Topic, message.Partition, message.Offset, len(message.Data))
			stats.Add(MetricReceiveMessages, 1)
			s.latestReceiveMessageTimestamp = time.Now()

		START:
			if s.cfg.JSONSchemaValidationEnable {
				ok, err := s.jsonSchemaValid(string(message.Data))
				if err != nil {
					log.Error("splitter xlm: json schema valid error")
				}
				if !ok {
					log.Warn("splitter xlm: json schema valid failed")
				}
			}

			data, err := ParseLedger(string(message.Data))
			if err != nil {
				stats.Add(MetricParseDataError, 1)
				log.Error("splitter xlm: ledger parse error, retry after 5s")
				log.DetailError(err)
				time.Sleep(time.Second * 5)
				goto START
			}

			for _, d := range data {
				if d.Ledger.Sequence != expectedLedger {
					log.Debug("splitter xlm: ledger %d is not expected, is %d, skip", d.Ledger.Sequence, expectedLedger)
					if sendFlag == true {
					SEND:
						err := s.sendToKafka(expectedLedger, expectedLedger+10)
						if err != nil {
							log.Error("splitter xlm: send to kafka from %d to %d error", expectedLedger, expectedLedger+10)
							log.DetailError(err)
							time.Sleep(1 * time.Second)
							goto SEND
						}
						log.Debug("splitter xlm: send flag is %s ,send to kafka ledger from %d to %d", sendFlag, expectedLedger, expectedLedger+10)
					}
					sendFlag = false
					continue
				}
				if s.cfg.DatabaseEnable {
					ds := make([]*XLMLedgerData, 0)
					ds = append(ds, d)
					err = s.SaveLedger(ds)
					if err != nil {
						log.Error("splitter xlm: ledger %d save error, retry after 5s", d.Ledger.Sequence)
						log.DetailError(err)
						time.Sleep(time.Second * 5)
						goto START
					} else {
						log.Info("splitter xlm: ledger %d save success", d.Ledger.Sequence)
						s.cfg.Consumer.MarkOffset(message)
					}
					expectedLedger = d.Ledger.Sequence + 1
					sendFlag = true
					log.Debug("splitter xlm: update expected ledger is %d and send flag %v", expectedLedger, sendFlag)

				}
			}
		}
	}
}

func (s *XLMSplitter) sendToKafka(start, end int64) error {
	url := fmt.Sprintf("http://%s/send?ledger_start=%d&ledger_end=%d&send_kafka=true", s.cfg.Endpoint, start, end)
	_, err := http.Get(url)
	if err != nil {
		return err
	}
	return nil
}

func (s *XLMSplitter) Stop() {
	s.cronWorker.Stop()
}

func (s *XLMSplitter) CheckLedger(curLedger *XLMLedgerData) (bool, int64) {
	db := service.NewDatabase(s.cfg.Engine)
	height := int64(-1)
	prevLedger := make([]*model.Ledger, 0)
	err := db.Where("sequence = ?", curLedger.Ledger.Sequence-1).Find(&prevLedger)
	if err != nil {
		log.DetailError(err)
		return false, height
	}

	if len(prevLedger) != 1 {
		log.Warn("splitter xlm: can not find previous ledger %d", curLedger.Ledger.Sequence-1)
		ledgers := make([]*model.Ledger, 0)
		err = db.Desc("sequence").Limit(1).Find(&ledgers)
		if err != nil {
			log.DetailError(err)
		} else {
			if len(ledgers) > 0 {
				height = ledgers[0].Sequence + 1
			} else {
				log.Warn("splitter xlm: database empty")
				height = 1
			}
		}
		return false, height
	}
	log.Debug("splitter xlm: check ledger %d pass", curLedger.Ledger.Sequence)
	return true, height
}

func (s *XLMSplitter) SaveLedger(data []*XLMLedgerData) error {
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
	if len(data) == 1 {
		ledgerTemp := new(model.Ledger)
		ledgerTemp.Sequence = data[0].Ledger.Sequence
		has, err := tx.Get(ledgerTemp)
		if err != nil {
			_ = tx.Rollback()
			log.DetailError(err)
			stats.Add(MetricDatabaseRollback, 1)
			return err
		}
		if has {
			log.Warn("splitter xlm: ledger %d has been stored", data[0].Ledger.Sequence)
			_ = tx.Rollback()
			return nil
		}
	}
	ledgers := make([]*model.Ledger, 0)
	transactions := make([]*model.Transaction, 0)
	operations := make([]*model.Operation, 0)
	for _, d := range data {
		ledgers = append(ledgers, d.Ledger)
		transactions = append(transactions, d.Transactions...)
		operations = append(operations, d.Operations...)
	}
	var affected int64
	affected, err = tx.BatchInsert(ledgers)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter xlm: ledger write %d rows", affected)

	affected, err = tx.BatchInsert(transactions)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter xlm: transaction write %d rows", affected)

	affected, err = tx.BatchInsert(operations)
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		stats.Add(MetricDatabaseRollback, 1)
		return err
	}
	log.Debug("splitter xlm: operations write %d rows", affected)

	err = tx.Commit()
	if err != nil {
		_ = tx.Rollback()
		log.DetailError(err)
		return err
	}
	tx.Close()
	stats.Add(MetricDatabaseCommit, int64(len(ledgers)))
	stats.Add(MetricSaveLedger, int64(len(ledgers)))
	elaspedTime := time.Now().Sub(startTime)
	s.latestSaveDataTimestamp = time.Now()
	log.Debug("splitter xlm: %d ledgers write done elasped: %s", len(ledgers), elaspedTime.String())
	return nil
}

func (s *XLMSplitter) databaseWorker(i int) {
	log.Info("splitter xlm: starting database worker %d", i)
	ledgerBuffer := make([]*XLMLedgerData, 0)
	for {
		select {
		case data := <-s.databaseWorkerChan:
			ledgerBuffer = append(ledgerBuffer, data)
			if len(ledgerBuffer) >= 1000 {
			INSERT1:
				err := s.SaveLedger(ledgerBuffer)
				if err != nil {
					log.DetailError(err)
					time.Sleep(100 * time.Millisecond)
					goto INSERT1
				}
				ledgerBuffer = make([]*XLMLedgerData, 0)
			}
		case <-time.After(1 * time.Second):
			if len(ledgerBuffer) != 0 {
			INSERT2:
				err := s.SaveLedger(ledgerBuffer)
				if err != nil {
					log.DetailError(err)
					time.Sleep(100 * time.Millisecond)
					goto INSERT2
				}
				ledgerBuffer = make([]*XLMLedgerData, 0)
			}
		case stop := <-s.databaseWorkerStopChan:
			if stop {
				msg := fmt.Sprintf("splitter xlm: database worker %d stopped", i)
				log.Info(msg)
				return
			}
		}
	}
}

func (s *XLMSplitter) jsonSchemaValid(data string) (bool, error) {
	startTime := time.Now()
	dataLoader := gojsonschema.NewStringLoader(data)
	result, err := gojsonschema.Validate(s.jsonSchemaLoader, dataLoader)
	if err != nil {
		log.Error("splitter xlm: json schema validation error")
		log.DetailError(err)
		return false, err
	}
	if !result.Valid() {
		for _, err := range result.Errors() {
			log.Error("splitter xlm: data invalid %s", strings.ToLower(err.String()))
			return false, nil
		}
		stats.Add(MetricVaildationError, 1)
	} else {
		stats.Add(MetricVaildationSuccess, 1)
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter xlm: json schema validation elasped %s", elaspedTime)
	return true, nil
}
