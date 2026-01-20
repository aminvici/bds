package splitter

import (
	"time"

	"github.com/go-xorm/xorm"
	"github.com/aminvici/bds/common/kafka"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/config"
	"github.com/aminvici/bds/service"
	"github.com/aminvici/bds/splitter/bch"
	"github.com/aminvici/bds/splitter/bsv"
	"github.com/aminvici/bds/splitter/btc"
	"github.com/aminvici/bds/splitter/doge"
	"github.com/aminvici/bds/splitter/eos"
	"github.com/aminvici/bds/splitter/etc"
	"github.com/aminvici/bds/splitter/eth"
	"github.com/aminvici/bds/splitter/ltc"
	"github.com/aminvici/bds/splitter/tron"
	"github.com/aminvici/bds/splitter/xlm"
	"github.com/aminvici/bds/splitter/xrp"
)

type Splitter struct {
	btcEngine  *xorm.Engine
	bchEngine  *xorm.Engine
	ltcEngine  *xorm.Engine
	ethEngine  *xorm.Engine
	etcEngine  *xorm.Engine
	eosEngine  *xorm.Engine
	dogeEngine *xorm.Engine
	bsvEngine  *xorm.Engine
	tronEngine *xorm.Engine
	xrpEngine  *xorm.Engine
	xlmEngine  *xorm.Engine

	btcConsumer  *kafka.ConsumerGroup
	bchConsumer  *kafka.ConsumerGroup
	ltcConsumer  *kafka.ConsumerGroup
	ethConsumer  *kafka.ConsumerGroup
	etcConsumer  *kafka.ConsumerGroup
	eosConsumer  *kafka.ConsumerGroup
	xrpConsumer  *kafka.ConsumerGroup
	dogeConsumer *kafka.ConsumerGroup
	bsvConsumer  *kafka.ConsumerGroup
	tronConsumer *kafka.ConsumerGroup
	xlmConsumer  *kafka.ConsumerGroup
}

func New() (*Splitter, error) {
	var err error
	p := new(Splitter)
	err = p.NewEngine()
	if err != nil {
		log.Error("splitter: create engine error")
		log.DetailError(err)
		return nil, err
	}
	err = p.NewConsumer()
	if err != nil {
		log.Error("splitter: create consumer error")
		log.DetailError(err)
		return nil, err
	}
	return p, nil
}

func (p *Splitter) NewEngine() error {
	var err error
	//Loading configuration file information of BTC database
	if config.SplitterConfig.BTCSetting.Enable {
		log.Info("splitter: btc engine enabled")
		p.btcEngine, err = service.NewEngine(&service.DatabaseConfig{
			Type:         config.SplitterConfig.DatabaseBTCSetting.Type,
			Host:         config.SplitterConfig.DatabaseBTCSetting.Host,
			Port:         config.SplitterConfig.DatabaseBTCSetting.Port,
			Database:     config.SplitterConfig.DatabaseBTCSetting.Database,
			User:         config.SplitterConfig.DatabaseBTCSetting.User,
			Password:     config.SplitterConfig.DatabaseBTCSetting.Password,
			MaxOpenConns: config.SplitterConfig.DatabaseBTCSetting.MaxOpenConns,
			MaxIdleConns: config.SplitterConfig.DatabaseBTCSetting.MaxIdleConns,
			SQLLogFile:   config.SplitterConfig.DatabaseBTCSetting.SQLLogFile,
			Debug:        config.SplitterConfig.DatabaseBTCSetting.Debug,
		})
		if err != nil {
			return err
		}

		//Check the existence of tables in the database
		err = service.CheckBTCTable(p.btcEngine)
		if err != nil {
			return err
		}
	} else {
		log.Info("splitter: btc engine disabled")
	}

	//Loading configuration file information of BCH database
	if config.SplitterConfig.BCHSetting.Enable {
		log.Info("splitter: bch engine enabled")
		p.bchEngine, err = service.NewEngine(&service.DatabaseConfig{
			Type:         config.SplitterConfig.DatabaseBCHSetting.Type,
			Host:         config.SplitterConfig.DatabaseBCHSetting.Host,
			Port:         config.SplitterConfig.DatabaseBCHSetting.Port,
			Database:     config.SplitterConfig.DatabaseBCHSetting.Database,
			User:         config.SplitterConfig.DatabaseBCHSetting.User,
			Password:     config.SplitterConfig.DatabaseBCHSetting.Password,
			MaxOpenConns: config.SplitterConfig.DatabaseBCHSetting.MaxOpenConns,
			MaxIdleConns: config.SplitterConfig.DatabaseBCHSetting.MaxIdleConns,
			SQLLogFile:   config.SplitterConfig.DatabaseBCHSetting.SQLLogFile,
			Debug:        config.SplitterConfig.DatabaseBCHSetting.Debug,
		})
		if err != nil {
			return err
		}

		//Check the existence of tables in the database
		err = service.CheckBCHTable(p.bchEngine)
		if err != nil {
			return err
		}
	} else {
		log.Info("splitter: bch engine disabled")
	}

	//Loading configuration file information of ETC database
	if config.SplitterConfig.ETCSetting.Enable {
		log.Info("splitter: etc engine enabled")
		p.etcEngine, err = service.NewEngine(&service.DatabaseConfig{
			Type:         config.SplitterConfig.DatabaseETCSetting.Type,
			Host:         config.SplitterConfig.DatabaseETCSetting.Host,
			Port:         config.SplitterConfig.DatabaseETCSetting.Port,
			Database:     config.SplitterConfig.DatabaseETCSetting.Database,
			User:         config.SplitterConfig.DatabaseETCSetting.User,
			Password:     config.SplitterConfig.DatabaseETCSetting.Password,
			MaxOpenConns: config.SplitterConfig.DatabaseETCSetting.MaxOpenConns,
			MaxIdleConns: config.SplitterConfig.DatabaseETCSetting.MaxIdleConns,
			SQLLogFile:   config.SplitterConfig.DatabaseETCSetting.SQLLogFile,
			Debug:        config.SplitterConfig.DatabaseETCSetting.Debug,
		})
		if err != nil {
			return err
		}

		//Check the existence of tables in the database
		err = service.CheckETCTable(p.etcEngine)
		if err != nil {
			return err
		}
	} else {
		log.Info("splitter: etc engine disabled")
	}

	//Loading configuration file information of ETH database
	if config.SplitterConfig.ETHSetting.Enable {
		log.Info("splitter: eth engine enabled")
		p.ethEngine, err = service.NewEngine(&service.DatabaseConfig{
			Type:         config.SplitterConfig.DatabaseETHSetting.Type,
			Host:         config.SplitterConfig.DatabaseETHSetting.Host,
			Port:         config.SplitterConfig.DatabaseETHSetting.Port,
			Database:     config.SplitterConfig.DatabaseETHSetting.Database,
			User:         config.SplitterConfig.DatabaseETHSetting.User,
			Password:     config.SplitterConfig.DatabaseETHSetting.Password,
			MaxOpenConns: config.SplitterConfig.DatabaseETHSetting.MaxOpenConns,
			MaxIdleConns: config.SplitterConfig.DatabaseETHSetting.MaxIdleConns,
			SQLLogFile:   config.SplitterConfig.DatabaseETHSetting.SQLLogFile,
			Debug:        config.SplitterConfig.DatabaseETHSetting.Debug,
		})
		if err != nil {
			return err
		}

		//Check the existence of tables in the database
		err = service.CheckETHTable(p.ethEngine)
		if err != nil {
			return err
		}
	} else {
		log.Info("splitter: eth engine disabled")
	}

	//Loading configuration file information of LTC database
	if config.SplitterConfig.LTCSetting.Enable {
		log.Info("splitter: ltc engine enabled")
		p.ltcEngine, err = service.NewEngine(&service.DatabaseConfig{
			Type:         config.SplitterConfig.DatabaseLTCSetting.Type,
			Host:         config.SplitterConfig.DatabaseLTCSetting.Host,
			Port:         config.SplitterConfig.DatabaseLTCSetting.Port,
			Database:     config.SplitterConfig.DatabaseLTCSetting.Database,
			User:         config.SplitterConfig.DatabaseLTCSetting.User,
			Password:     config.SplitterConfig.DatabaseLTCSetting.Password,
			MaxOpenConns: config.SplitterConfig.DatabaseLTCSetting.MaxOpenConns,
			MaxIdleConns: config.SplitterConfig.DatabaseLTCSetting.MaxIdleConns,
			SQLLogFile:   config.SplitterConfig.DatabaseLTCSetting.SQLLogFile,
			Debug:        config.SplitterConfig.DatabaseLTCSetting.Debug,
		})
		if err != nil {
			return err
		}

		//Check the existence of tables in the database
		err = service.CheckLTCTable(p.ltcEngine)
		if err != nil {
			return err
		}
	} else {
		log.Info("splitter: ltc engine disabled")
	}

	//Loading configuration file information of DOGE database
	if config.SplitterConfig.DOGESetting.Enable {
		log.Info("splitter: doge engine enabled")
		p.dogeEngine, err = service.NewEngine(&service.DatabaseConfig{
			Type:         config.SplitterConfig.DatabaseDOGESetting.Type,
			Host:         config.SplitterConfig.DatabaseDOGESetting.Host,
			Port:         config.SplitterConfig.DatabaseDOGESetting.Port,
			Database:     config.SplitterConfig.DatabaseDOGESetting.Database,
			User:         config.SplitterConfig.DatabaseDOGESetting.User,
			Password:     config.SplitterConfig.DatabaseDOGESetting.Password,
			MaxOpenConns: config.SplitterConfig.DatabaseDOGESetting.MaxOpenConns,
			MaxIdleConns: config.SplitterConfig.DatabaseDOGESetting.MaxIdleConns,
			SQLLogFile:   config.SplitterConfig.DatabaseDOGESetting.SQLLogFile,
			Debug:        config.SplitterConfig.DatabaseDOGESetting.Debug,
		})
		if err != nil {
			return err
		}

		//Check the existence of tables in the database
		err = service.CheckDOGETable(p.dogeEngine)
		if err != nil {
			return err
		}
	} else {
		log.Info("splitter: doge engine disabled")
	}

	//Loading configuration file information of EOS database
	if config.SplitterConfig.EOSSetting.Enable {
		log.Info("splitter: eos engine enabled")
		p.eosEngine, err = service.NewEngine(&service.DatabaseConfig{
			Type:         config.SplitterConfig.DatabaseEOSSetting.Type,
			Host:         config.SplitterConfig.DatabaseEOSSetting.Host,
			Port:         config.SplitterConfig.DatabaseEOSSetting.Port,
			Database:     config.SplitterConfig.DatabaseEOSSetting.Database,
			User:         config.SplitterConfig.DatabaseEOSSetting.User,
			Password:     config.SplitterConfig.DatabaseEOSSetting.Password,
			MaxOpenConns: config.SplitterConfig.DatabaseEOSSetting.MaxOpenConns,
			MaxIdleConns: config.SplitterConfig.DatabaseEOSSetting.MaxIdleConns,
			SQLLogFile:   config.SplitterConfig.DatabaseEOSSetting.SQLLogFile,
			Debug:        config.SplitterConfig.DatabaseEOSSetting.Debug,
		})
		if err != nil {
			return err
		}

		//Check the existence of tables in the database
		err = service.CheckEOSTable(p.eosEngine)
		if err != nil {
			return err
		}
	} else {
		log.Info("splitter: eos engine disabled")
	}

	//Loading configuration file information of XLM database
	if config.SplitterConfig.XLMSetting.Enable {
		log.Info("splitter: xlm engine enabled")
		p.xlmEngine, err = service.NewEngine(&service.DatabaseConfig{
			Type:         config.SplitterConfig.DatabaseXLMSetting.Type,
			Host:         config.SplitterConfig.DatabaseXLMSetting.Host,
			Port:         config.SplitterConfig.DatabaseXLMSetting.Port,
			Database:     config.SplitterConfig.DatabaseXLMSetting.Database,
			User:         config.SplitterConfig.DatabaseXLMSetting.User,
			Password:     config.SplitterConfig.DatabaseXLMSetting.Password,
			MaxOpenConns: config.SplitterConfig.DatabaseXLMSetting.MaxOpenConns,
			MaxIdleConns: config.SplitterConfig.DatabaseXLMSetting.MaxIdleConns,
			SQLLogFile:   config.SplitterConfig.DatabaseXLMSetting.SQLLogFile,
			Debug:        config.SplitterConfig.DatabaseXLMSetting.Debug,
		})
		if err != nil {
			return err
		}

		//Check the existence of tables in the database
		err = service.CheckXLMTable(p.xlmEngine)
		if err != nil {
			return err
		}
	} else {
		log.Info("splitter: xlm engine disabled")
	}

	//Loading configuration file information of TRON database
	if config.SplitterConfig.TRONSetting.Enable {
		log.Info("splitter: tron engine enabled")
		p.tronEngine, err = service.NewEngine(&service.DatabaseConfig{
			Type:         config.SplitterConfig.DatabaseTRONSetting.Type,
			Host:         config.SplitterConfig.DatabaseTRONSetting.Host,
			Port:         config.SplitterConfig.DatabaseTRONSetting.Port,
			Database:     config.SplitterConfig.DatabaseTRONSetting.Database,
			User:         config.SplitterConfig.DatabaseTRONSetting.User,
			Password:     config.SplitterConfig.DatabaseTRONSetting.Password,
			MaxOpenConns: config.SplitterConfig.DatabaseTRONSetting.MaxOpenConns,
			MaxIdleConns: config.SplitterConfig.DatabaseTRONSetting.MaxIdleConns,
			SQLLogFile:   config.SplitterConfig.DatabaseTRONSetting.SQLLogFile,
			Debug:        config.SplitterConfig.DatabaseTRONSetting.Debug,
		})
		if err != nil {
			return err
		}

		//Check the existence of tables in the database
		err = service.CheckTRONTable(p.tronEngine)
		if err != nil {
			return err
		}
	} else {
		log.Info("splitter: tron engine disabled")
	}

	//Loading configuration file information of BSV database
	if config.SplitterConfig.BSVSetting.Enable {
		log.Info("splitter: bsv engine enabled")
		p.bsvEngine, err = service.NewEngine(&service.DatabaseConfig{
			Type:         config.SplitterConfig.DatabaseBSVSetting.Type,
			Host:         config.SplitterConfig.DatabaseBSVSetting.Host,
			Port:         config.SplitterConfig.DatabaseBSVSetting.Port,
			Database:     config.SplitterConfig.DatabaseBSVSetting.Database,
			User:         config.SplitterConfig.DatabaseBSVSetting.User,
			Password:     config.SplitterConfig.DatabaseBSVSetting.Password,
			MaxOpenConns: config.SplitterConfig.DatabaseBSVSetting.MaxOpenConns,
			MaxIdleConns: config.SplitterConfig.DatabaseBSVSetting.MaxIdleConns,
			SQLLogFile:   config.SplitterConfig.DatabaseBSVSetting.SQLLogFile,
			Debug:        config.SplitterConfig.DatabaseBSVSetting.Debug,
		})
		if err != nil {
			log.DetailError(err)
			return err
		}

		//Check the existence of tables in the database
		err = service.CheckBSVTable(p.bsvEngine)
		if err != nil {
			log.DetailError(err)
			return err
		}
	} else {
		log.Info("splitter: bsv engine disabled")
	}

	//Loading configuration file information of XRP database
	if config.SplitterConfig.XRPSetting.Enable {
		log.Info("splitter: xrp engine enabled")
		p.xrpEngine, err = service.NewEngine(&service.DatabaseConfig{
			Type:         config.SplitterConfig.DatabaseXRPSetting.Type,
			Host:         config.SplitterConfig.DatabaseXRPSetting.Host,
			Port:         config.SplitterConfig.DatabaseXRPSetting.Port,
			Database:     config.SplitterConfig.DatabaseXRPSetting.Database,
			User:         config.SplitterConfig.DatabaseXRPSetting.User,
			Password:     config.SplitterConfig.DatabaseXRPSetting.Password,
			MaxOpenConns: config.SplitterConfig.DatabaseXRPSetting.MaxOpenConns,
			MaxIdleConns: config.SplitterConfig.DatabaseXRPSetting.MaxIdleConns,
			SQLLogFile:   config.SplitterConfig.DatabaseXRPSetting.SQLLogFile,
			Debug:        config.SplitterConfig.DatabaseXRPSetting.Debug,
		})
		if err != nil {
			return err
		}

		//Check the existence of tables in the database
		err = service.CheckXRPTable(p.xrpEngine)
		if err != nil {
			return err
		}
	} else {
		log.Info("splitter: xrp engine disabled")
	}
	return nil
}

func (p *Splitter) NewConsumer() error {
	var err error
	//Loading configuration file information of BTC kafka
	if config.SplitterConfig.BTCSetting.Enable {
		p.btcConsumer, err = kafka.NewConsumerGroup(&kafka.ConsumerGroupConfig{
			BrokerList:   config.SplitterConfig.KafkaBTCSetting.BrokerList,
			BufferSize:   config.SplitterConfig.KafkaBTCSetting.BufferSize,
			ClientID:     config.SplitterConfig.KafkaBTCSetting.ClientID,
			GroupID:      config.SplitterConfig.KafkaBTCSetting.GroupID,
			ReturnErrors: config.SplitterConfig.KafkaBTCSetting.ReturnErrors,
		})
		if err != nil {
			return err
		}
	}

	//Loading configuration file information of ETH kafka
	if config.SplitterConfig.ETHSetting.Enable {
		p.ethConsumer, err = kafka.NewConsumerGroup(&kafka.ConsumerGroupConfig{
			BrokerList:   config.SplitterConfig.KafkaETHSetting.BrokerList,
			BufferSize:   config.SplitterConfig.KafkaETHSetting.BufferSize,
			ClientID:     config.SplitterConfig.KafkaETHSetting.ClientID,
			GroupID:      config.SplitterConfig.KafkaETHSetting.GroupID,
			ReturnErrors: config.SplitterConfig.KafkaETHSetting.ReturnErrors,
		})
		if err != nil {
			return err
		}
	}

	//Loading configuration file information of BCH kafka
	if config.SplitterConfig.BCHSetting.Enable {
		p.bchConsumer, err = kafka.NewConsumerGroup(&kafka.ConsumerGroupConfig{
			BrokerList:   config.SplitterConfig.KafkaBCHSetting.BrokerList,
			BufferSize:   config.SplitterConfig.KafkaBCHSetting.BufferSize,
			ClientID:     config.SplitterConfig.KafkaBCHSetting.ClientID,
			GroupID:      config.SplitterConfig.KafkaBCHSetting.GroupID,
			ReturnErrors: config.SplitterConfig.KafkaBCHSetting.ReturnErrors,
		})
		if err != nil {
			return err
		}
	}

	//Loading configuration file information of ETC kafka
	if config.SplitterConfig.ETCSetting.Enable {
		p.etcConsumer, err = kafka.NewConsumerGroup(&kafka.ConsumerGroupConfig{
			BrokerList:   config.SplitterConfig.KafkaETCSetting.BrokerList,
			BufferSize:   config.SplitterConfig.KafkaETCSetting.BufferSize,
			ClientID:     config.SplitterConfig.KafkaETCSetting.ClientID,
			GroupID:      config.SplitterConfig.KafkaETCSetting.GroupID,
			ReturnErrors: config.SplitterConfig.KafkaETCSetting.ReturnErrors,
		})
		if err != nil {
			return err
		}
	}

	//Loading configuration file information of LTC kafka
	if config.SplitterConfig.LTCSetting.Enable {
		p.ltcConsumer, err = kafka.NewConsumerGroup(&kafka.ConsumerGroupConfig{
			BrokerList:   config.SplitterConfig.KafkaLTCSetting.BrokerList,
			BufferSize:   config.SplitterConfig.KafkaLTCSetting.BufferSize,
			ClientID:     config.SplitterConfig.KafkaLTCSetting.ClientID,
			GroupID:      config.SplitterConfig.KafkaLTCSetting.GroupID,
			ReturnErrors: config.SplitterConfig.KafkaLTCSetting.ReturnErrors,
		})
		if err != nil {
			return err
		}
	}

	//Loading configuration file information of DOGE kafka
	if config.SplitterConfig.DOGESetting.Enable {
		p.dogeConsumer, err = kafka.NewConsumerGroup(&kafka.ConsumerGroupConfig{
			BrokerList:   config.SplitterConfig.KafkaDOGESetting.BrokerList,
			BufferSize:   config.SplitterConfig.KafkaDOGESetting.BufferSize,
			ClientID:     config.SplitterConfig.KafkaDOGESetting.ClientID,
			GroupID:      config.SplitterConfig.KafkaDOGESetting.GroupID,
			ReturnErrors: config.SplitterConfig.KafkaDOGESetting.ReturnErrors,
		})
		if err != nil {
			return err
		}
	}

	//Loading configuration file information of EOS kafka
	if config.SplitterConfig.EOSSetting.Enable {
		p.eosConsumer, err = kafka.NewConsumerGroup(&kafka.ConsumerGroupConfig{
			BrokerList:   config.SplitterConfig.KafkaEOSSetting.BrokerList,
			BufferSize:   config.SplitterConfig.KafkaEOSSetting.BufferSize,
			ClientID:     config.SplitterConfig.KafkaEOSSetting.ClientID,
			GroupID:      config.SplitterConfig.KafkaEOSSetting.GroupID,
			ReturnErrors: config.SplitterConfig.KafkaEOSSetting.ReturnErrors,
		})
		if err != nil {
			return err
		}
	}

	//Loading configuration file information of XRP kafka
	if config.SplitterConfig.XRPSetting.Enable {
		p.xrpConsumer, err = kafka.NewConsumerGroup(&kafka.ConsumerGroupConfig{
			BrokerList:   config.SplitterConfig.KafkaXRPSetting.BrokerList,
			BufferSize:   config.SplitterConfig.KafkaXRPSetting.BufferSize,
			ClientID:     config.SplitterConfig.KafkaXRPSetting.ClientID,
			GroupID:      config.SplitterConfig.KafkaXRPSetting.GroupID,
			ReturnErrors: config.SplitterConfig.KafkaXRPSetting.ReturnErrors,
		})
		if err != nil {
			return err
		}
	}

	//Loading configuration file information of XLM kafka
	if config.SplitterConfig.XLMSetting.Enable {
		p.xlmConsumer, err = kafka.NewConsumerGroup(&kafka.ConsumerGroupConfig{
			BrokerList:   config.SplitterConfig.KafkaXLMSetting.BrokerList,
			BufferSize:   config.SplitterConfig.KafkaXLMSetting.BufferSize,
			ClientID:     config.SplitterConfig.KafkaXLMSetting.ClientID,
			GroupID:      config.SplitterConfig.KafkaXLMSetting.GroupID,
			ReturnErrors: config.SplitterConfig.KafkaXLMSetting.ReturnErrors,
		})
		if err != nil {
			return err
		}
	}

	//Loading configuration file information of BSV kafka
	if config.SplitterConfig.BSVSetting.Enable {
		p.bsvConsumer, err = kafka.NewConsumerGroup(&kafka.ConsumerGroupConfig{
			BrokerList:   config.SplitterConfig.KafkaBSVSetting.BrokerList,
			BufferSize:   config.SplitterConfig.KafkaBSVSetting.BufferSize,
			ClientID:     config.SplitterConfig.KafkaBSVSetting.ClientID,
			GroupID:      config.SplitterConfig.KafkaBSVSetting.GroupID,
			ReturnErrors: config.SplitterConfig.KafkaBSVSetting.ReturnErrors,
		})
		if err != nil {
			return err
		}
	}

	//Loading configuration file information of TRON kafka
	if config.SplitterConfig.TRONSetting.Enable {
		p.tronConsumer, err = kafka.NewConsumerGroup(&kafka.ConsumerGroupConfig{
			BrokerList:   config.SplitterConfig.KafkaTRONSetting.BrokerList,
			BufferSize:   config.SplitterConfig.KafkaTRONSetting.BufferSize,
			ClientID:     config.SplitterConfig.KafkaTRONSetting.ClientID,
			GroupID:      config.SplitterConfig.KafkaTRONSetting.GroupID,
			ReturnErrors: config.SplitterConfig.KafkaTRONSetting.ReturnErrors,
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Splitter) Run() {
	//Loading configuration file information of BTC node
	if config.SplitterConfig.BTCSetting.Enable {
		btcConfig := &btc.SplitterConfig{
			p.btcEngine,
			p.btcConsumer,
			config.SplitterConfig.KafkaBTCSetting.Topic,
			config.SplitterConfig.BTCSetting.DatabaseEnable,
			config.SplitterConfig.BTCSetting.MaxBatchBlock,
			config.SplitterConfig.BTCSetting.Endpoint,
			config.SplitterConfig.BTCSetting.User,
			config.SplitterConfig.BTCSetting.Password,
			config.SplitterConfig.BTCSetting.OmniEndpoint,
			config.SplitterConfig.BTCSetting.OmniUser,
			config.SplitterConfig.BTCSetting.OmniPassword,
			config.SplitterConfig.BTCSetting.JSONSchemaFile,
			config.SplitterConfig.BTCSetting.JSONSchemaValidationEnable,
			config.SplitterConfig.BTCSetting.OmniEnable,
		}
		btcSplitter, err := btc.NewSplitter(btcConfig)
		if err != nil {
			panic(err)
		}
		//start BTC splitter
		go btcSplitter.Start()
	}

	//Loading configuration file information of BCH node
	if config.SplitterConfig.BCHSetting.Enable {
		bchConfig := &bch.SplitterConfig{
			p.bchEngine,
			p.bchConsumer,
			config.SplitterConfig.KafkaBCHSetting.Topic,
			config.SplitterConfig.BCHSetting.DatabaseEnable,
			config.SplitterConfig.BCHSetting.MaxBatchBlock,
			config.SplitterConfig.BCHSetting.Endpoint,
			config.SplitterConfig.BCHSetting.User,
			config.SplitterConfig.BCHSetting.Password,
			config.SplitterConfig.BCHSetting.JSONSchemaFile,
			config.SplitterConfig.BCHSetting.JSONSchemaValidationEnable,
		}
		bchSplitter, err := bch.NewSplitter(bchConfig)
		if err != nil {
			panic(err)
		}
		//start BCH splitter
		go bchSplitter.Start()
	}

	//Loading configuration file information of LTC node
	if config.SplitterConfig.LTCSetting.Enable {
		ltcConfig := &ltc.SplitterConfig{
			p.ltcEngine,
			p.ltcConsumer,
			config.SplitterConfig.KafkaLTCSetting.Topic,
			config.SplitterConfig.LTCSetting.DatabaseEnable,
			config.SplitterConfig.LTCSetting.MaxBatchBlock,
			config.SplitterConfig.LTCSetting.Endpoint,
			config.SplitterConfig.LTCSetting.User,
			config.SplitterConfig.LTCSetting.Password,
			config.SplitterConfig.LTCSetting.JSONSchemaFile,
			config.SplitterConfig.LTCSetting.JSONSchemaValidationEnable,
		}
		ltcSplitter, err := ltc.NewSplitter(ltcConfig)
		if err != nil {
			panic(err)
		}
		//start LTC splitter
		go ltcSplitter.Start()
	}

	//Loading configuration file information of DOGE node
	if config.SplitterConfig.DOGESetting.Enable {
		dogeConfig := &doge.SplitterConfig{
			p.dogeEngine,
			p.dogeConsumer,
			config.SplitterConfig.KafkaDOGESetting.Topic,
			config.SplitterConfig.DOGESetting.DatabaseEnable,
			config.SplitterConfig.DOGESetting.MaxBatchBlock,
			config.SplitterConfig.DOGESetting.Endpoint,
			config.SplitterConfig.DOGESetting.User,
			config.SplitterConfig.DOGESetting.Password,
			config.SplitterConfig.DOGESetting.JSONSchemaFile,
			config.SplitterConfig.DOGESetting.JSONSchemaValidationEnable,
		}
		dogeSplitter, err := doge.NewSplitter(dogeConfig)
		if err != nil {
			panic(err)
		}

		//start DOGE splitter
		go dogeSplitter.Start()
	}

	//Loading configuration file information of ETH node
	if config.SplitterConfig.ETHSetting.Enable {
		ethConfig := &eth.SplitterConfig{
			p.ethEngine,
			p.ethConsumer,
			config.SplitterConfig.KafkaETHSetting.Topic,
			config.SplitterConfig.ETHSetting.DatabaseEnable,
			config.SplitterConfig.ETHSetting.MaxBatchBlock,
			config.SplitterConfig.ETHSetting.Endpoint,
			config.SplitterConfig.ETHSetting.User,
			config.SplitterConfig.ETHSetting.Password,
			config.SplitterConfig.ETHSetting.JSONSchemaFile,
			config.SplitterConfig.ETHSetting.JSONSchemaValidationEnable,
		}
		ethSplitter, err := eth.NewSplitter(ethConfig)
		if err != nil {
			panic(err)
		}
		//start ETH splitter
		go ethSplitter.Start()
	}

	//Loading configuration file information of ETC node
	if config.SplitterConfig.ETCSetting.Enable {
		etcConfig := &etc.SplitterConfig{
			p.etcEngine,
			p.etcConsumer,
			config.SplitterConfig.KafkaETCSetting.Topic,
			config.SplitterConfig.ETCSetting.DatabaseEnable,
			config.SplitterConfig.ETCSetting.SkipHeight,
			config.SplitterConfig.ETCSetting.SkipMissBlock,
			config.SplitterConfig.ETCSetting.MaxBatchBlock,
			config.SplitterConfig.ETCSetting.Endpoint,
			config.SplitterConfig.ETCSetting.User,
			config.SplitterConfig.ETCSetting.Password,
			config.SplitterConfig.ETCSetting.JSONSchemaFile,
			config.SplitterConfig.ETCSetting.JSONSchemaValidationEnable,
		}
		etcSplitter, err := etc.NewSplitter(etcConfig)
		if err != nil {
			panic(err)
		}
		//start ETC splitter
		go etcSplitter.Start()
	}

	//Loading configuration file information of EOS node
	if config.SplitterConfig.EOSSetting.Enable {
		eosConfig := &eos.SplitterConfig{
			p.eosEngine,
			p.eosConsumer,
			config.SplitterConfig.KafkaEOSSetting.Topic,
			config.SplitterConfig.EOSSetting.DatabaseEnable,
			config.SplitterConfig.EOSSetting.DatabaseWorkerBuffer,
			config.SplitterConfig.EOSSetting.DatabaseWorkerNumber,
			config.SplitterConfig.EOSSetting.SkipHeight,
			config.SplitterConfig.EOSSetting.SkipMissBlock,
			config.SplitterConfig.EOSSetting.MaxBatchBlock,
			config.SplitterConfig.EOSSetting.Endpoint,
			config.SplitterConfig.EOSSetting.KafkaProxyHost,
			config.SplitterConfig.EOSSetting.KafkaProxyPort,
			config.SplitterConfig.EOSSetting.User,
			config.SplitterConfig.EOSSetting.Password,
			config.SplitterConfig.EOSSetting.JSONSchemaFile,
			config.SplitterConfig.EOSSetting.JSONSchemaValidationEnable,
		}
		eosSplitter, err := eos.NewSplitter(eosConfig)
		if err != nil {
			panic(err)
		}
		//start EOS splitter
		go eosSplitter.Start()
	}

	//Loading configuration file information of BSV node
	if config.SplitterConfig.BSVSetting.Enable {
		bsvConfig := &bsv.SplitterConfig{
			p.bsvEngine,
			p.bsvConsumer,
			config.SplitterConfig.KafkaBSVSetting.Topic,
			config.SplitterConfig.BSVSetting.DatabaseEnable,
			config.SplitterConfig.BSVSetting.MaxBatchBlock,
			config.SplitterConfig.BSVSetting.Endpoint,
			config.SplitterConfig.BSVSetting.User,
			config.SplitterConfig.BSVSetting.Password,
			config.SplitterConfig.BSVSetting.JSONSchemaFile,
			config.SplitterConfig.BSVSetting.JSONSchemaValidationEnable,
		}
		bsvSplitter, err := bsv.NewSplitter(bsvConfig)
		if err != nil {
			panic(err)
		}
		//start BSV splitter
		go bsvSplitter.Start()
	}

	//Loading configuration file information of TRON node
	if config.SplitterConfig.TRONSetting.Enable {
		tronConfig := &tron.SplitterConfig{
			p.tronEngine,
			p.tronConsumer,
			config.SplitterConfig.KafkaTRONSetting.Topic,
			config.SplitterConfig.TRONSetting.DatabaseEnable,
			config.SplitterConfig.TRONSetting.ConcurrentHeight,
			config.SplitterConfig.TRONSetting.DatabaseWorkerBuffer,
			config.SplitterConfig.TRONSetting.DatabaseWorkerNumber,
			config.SplitterConfig.TRONSetting.SkipHeight,
			config.SplitterConfig.TRONSetting.SkipMissBlock,
			config.SplitterConfig.TRONSetting.MaxBatchBlock,
			config.SplitterConfig.TRONSetting.Endpoint,
			config.SplitterConfig.TRONSetting.User,
			config.SplitterConfig.TRONSetting.Password,
			config.SplitterConfig.TRONSetting.JSONSchemaFile,
			config.SplitterConfig.TRONSetting.JSONSchemaValidationEnable,
		}
		tronSplitter, err := tron.NewSplitter(tronConfig)
		if err != nil {
			panic(err)
		}
		//start TRON splitter
		go tronSplitter.Start()
	}

	//Loading configuration file information of XRP node
	if config.SplitterConfig.XRPSetting.Enable {
		xrpConfig := &xrp.SplitterConfig{
			p.xrpEngine,
			p.xrpConsumer,
			config.SplitterConfig.KafkaXRPSetting.Topic,
			config.SplitterConfig.XRPSetting.DatabaseEnable,
			config.SplitterConfig.XRPSetting.MaxBatchBlock,
			config.SplitterConfig.XRPSetting.Endpoint,
			config.SplitterConfig.XRPSetting.User,
			config.SplitterConfig.XRPSetting.Password,
			config.SplitterConfig.XRPSetting.JSONSchemaFile,
			config.SplitterConfig.XRPSetting.JSONSchemaValidationEnable,
			config.SplitterConfig.XRPSetting.DatabaseWorkerNumber,
			config.SplitterConfig.XRPSetting.DatabaseWorkerBuffer,
		}
		xrpSplitter, err := xrp.NewSplitter(xrpConfig)
		if err != nil {
			panic(err)
		}
		//start XRP splitter
		go xrpSplitter.Start()
	}

	//Loading configuration file information of XLM node
	if config.SplitterConfig.XLMSetting.Enable {
		xlmConfig := &xlm.SplitterConfig{
			p.xlmEngine,
			p.xlmConsumer,
			config.SplitterConfig.KafkaXLMSetting.Topic,
			config.SplitterConfig.XLMSetting.DatabaseEnable,
			config.SplitterConfig.XLMSetting.MaxBatchBlock,
			config.SplitterConfig.XLMSetting.Endpoint,
			config.SplitterConfig.XLMSetting.JSONSchemaFile,
			config.SplitterConfig.XLMSetting.JSONSchemaValidationEnable,
			config.SplitterConfig.XLMSetting.ConcurrentHeight,
			config.SplitterConfig.XLMSetting.ConcurrentHTTP,
			config.SplitterConfig.XLMSetting.DatabaseWorkerBuffer,
			config.SplitterConfig.XLMSetting.DatabaseWorkerNumber,
		}
		xlmSplitter, err := xlm.NewSplitter(xlmConfig)
		if err != nil {
			panic(err)
		}
		//start XLM splitter
		go xlmSplitter.Start()
	}
	timer := time.NewTicker(time.Duration(60) * time.Second)
	for {
		select {
		case <-timer.C:
			log.Debug("splitter: %s", time.Now())
		}
	}
}
