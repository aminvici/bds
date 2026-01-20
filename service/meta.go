package service

import (
	"fmt"
	"strings"

	"github.com/go-xorm/xorm"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/service/model/bch"
	"github.com/aminvici/bds/service/model/bsv"
	"github.com/aminvici/bds/service/model/btc"
	"github.com/aminvici/bds/service/model/doge"
	"github.com/aminvici/bds/service/model/eos"
	"github.com/aminvici/bds/service/model/etc"
	"github.com/aminvici/bds/service/model/eth"
	"github.com/aminvici/bds/service/model/ltc"
	"github.com/aminvici/bds/service/model/tron"
	"github.com/aminvici/bds/service/model/xlm"
	"github.com/aminvici/bds/service/model/xrp"
)

var (
	tableMap = map[string]interface{}{
		// bch
		new(bch.Meta).TableName():                     new(bch.Meta),
		new(bch.Block).TableName():                    new(bch.Block),
		new(bch.Transaction).TableName():              new(bch.Transaction),
		new(bch.VIn).TableName():                      new(bch.VIn),
		new(bch.VOut).TableName():                     new(bch.VOut),
		new(bch.Address).TableName():                  new(bch.Address),
		new(bch.Mining).TableName():                   new(bch.Mining),
		new(bch.StatisticsMonth).TableName():          new(bch.StatisticsMonth),
		new(bch.StatisticsDayBlock).TableName():       new(bch.StatisticsDayBlock),
		new(bch.StatisticsDayTransaction).TableName(): new(bch.StatisticsDayTransaction),
		new(bch.StatisticsDayMinerPool).TableName():   new(bch.StatisticsDayMinerPool),
		new(bch.StatisticsDayOHLC).TableName():        new(bch.StatisticsDayOHLC),

		// btc
		new(btc.Meta).TableName():        new(btc.Meta),
		new(btc.Block).TableName():       new(btc.Block),
		new(btc.Transaction).TableName(): new(btc.Transaction),
		new(btc.VIn).TableName():         new(btc.VIn),
		new(btc.VOut).TableName():        new(btc.VOut),
		new(btc.Address).TableName():     new(btc.Address),
		//new(btc.AddressFeature).TableName():           new(btc.AddressFeature),
		new(btc.User).TableName():                     new(btc.User),
		new(btc.Mining).TableName():                   new(btc.Mining),
		new(btc.Mempool).TableName():                  new(btc.Mempool),
		new(btc.Retention).TableName():                new(btc.Retention),
		new(btc.OmniTansaction).TableName():           new(btc.OmniTansaction),
		new(btc.TetherAddress).TableName():            new(btc.TetherAddress),
		new(btc.Node).TableName():                     new(btc.Node),
		new(btc.StatisticsDayBlock).TableName():       new(btc.StatisticsDayBlock),
		new(btc.StatisticsDayTransaction).TableName(): new(btc.StatisticsDayTransaction),
		new(btc.StatisticsDayMinerPool).TableName():   new(btc.StatisticsDayMinerPool),
		new(btc.StatisticsDayMinerCost).TableName():   new(btc.StatisticsDayMinerCost),
		new(btc.StatisticsDayOHLC).TableName():        new(btc.StatisticsDayOHLC),
		new(btc.StatisticsDayUser).TableName():        new(btc.StatisticsDayUser),
		new(btc.StatisticsDayGrafanaData).TableName(): new(btc.StatisticsDayGrafanaData),
		new(btc.StatisticsWeek).TableName():           new(btc.StatisticsWeek),
		new(btc.StatisticsMonth).TableName():          new(btc.StatisticsMonth),

		// eth
		new(eth.Meta).TableName():                             new(eth.Meta),
		new(eth.Block).TableName():                            new(eth.Block),
		new(eth.Uncle).TableName():                            new(eth.Uncle),
		new(eth.Transaction).TableName():                      new(eth.Transaction),
		new(eth.InternalTransaction).TableName():              new(eth.InternalTransaction),
		new(eth.TokenTransaction).TableName():                 new(eth.TokenTransaction),
		new(eth.ENS).TableName():                              new(eth.ENS),
		new(eth.Token).TableName():                            new(eth.Token),
		new(eth.Account).TableName():                          new(eth.Account),
		new(eth.TokenAccount).TableName():                     new(eth.TokenAccount),
		new(eth.Mempool).TableName():                          new(eth.Mempool),
		new(eth.Node).TableName():                             new(eth.Node),
		new(eth.AccountExtra).TableName():                     new(eth.AccountExtra),
		new(eth.TokenAccountExtra).TableName():                new(eth.TokenAccountExtra),
		new(eth.MinerPoolAddress).TableName():                 new(eth.MinerPoolAddress),
		new(eth.StatisticsDayBlock).TableName():               new(eth.StatisticsDayBlock),
		new(eth.StatisticsDayTransaction).TableName():         new(eth.StatisticsDayTransaction),
		new(eth.StatisticsDayInternalTransaction).TableName(): new(eth.StatisticsDayInternalTransaction),
		new(eth.StatisticsDayTokenTransaction).TableName():    new(eth.StatisticsDayTokenTransaction),
		new(eth.StatisticsDayMinerPool).TableName():           new(eth.StatisticsDayMinerPool),
		new(eth.StatisticsDayMinerCost).TableName():           new(eth.StatisticsDayMinerCost),
		new(eth.StatisticsDayOHLC).TableName():                new(eth.StatisticsDayOHLC),
		//new(eth.StatisticsDayTokens).TableName():              new(eth.StatisticsDayTokens),
		//new(eth.StatisticsDayTokensTotal).TableName():         new(eth.StatisticsDayTokensTotal),
		new(eth.StatisticsDayTokenTransaction).TableName():       new(eth.StatisticsDayTokenTransaction),
		new(eth.StatisticsDayMinerPool).TableName():              new(eth.StatisticsDayMinerPool),
		new(eth.StatisticsDayMinerCost).TableName():              new(eth.StatisticsDayMinerCost),
		new(eth.StatisticsDayAdditionalIndexes).TableName():      new(eth.StatisticsDayAdditionalIndexes),
		new(eth.StatisticsDayAdditionalTokenIndexes).TableName(): new(eth.StatisticsDayAdditionalTokenIndexes),
		new(eth.StatisticsWeek).TableName():                      new(eth.StatisticsWeek),
		new(eth.StatisticsMonth).TableName():                     new(eth.StatisticsMonth),
		new(eth.StatisticsMonthTokens).TableName():               new(eth.StatisticsMonthTokens),

		// etc
		new(etc.Meta).TableName():                          new(etc.Meta),
		new(etc.Block).TableName():                         new(etc.Block),
		new(etc.Uncle).TableName():                         new(etc.Uncle),
		new(etc.Transaction).TableName():                   new(etc.Transaction),
		new(etc.TokenTransaction).TableName():              new(etc.TokenTransaction),
		new(etc.Token).TableName():                         new(etc.Token), //dev
		new(etc.Balance).TableName():                       new(etc.Balance),
		new(etc.Account).TableName():                       new(etc.Account),
		new(etc.TokenAccount).TableName():                  new(etc.TokenAccount),
		new(etc.StatisticsDay).TableName():                 new(etc.StatisticsDay),
		new(etc.StatisticsDayBlock).TableName():            new(etc.StatisticsDayBlock),
		new(etc.StatisticsDayTransaction).TableName():      new(etc.StatisticsDayTransaction),
		new(etc.StatisticsDayTokenTransaction).TableName(): new(etc.StatisticsDayTokenTransaction),

		// ltc
		new(ltc.Meta).TableName():                     new(ltc.Meta),
		new(ltc.Block).TableName():                    new(ltc.Block),
		new(ltc.Transaction).TableName():              new(ltc.Transaction),
		new(ltc.VIn).TableName():                      new(ltc.VIn),
		new(ltc.VOut).TableName():                     new(ltc.VOut),
		new(ltc.Address).TableName():                  new(ltc.Address),
		new(ltc.Mining).TableName():                   new(ltc.Mining),
		new(ltc.Node).TableName():                     new(ltc.Node),
		new(ltc.StatisticsMonth).TableName():          new(ltc.StatisticsMonth),
		new(ltc.StatisticsDayBlock).TableName():       new(ltc.StatisticsDayBlock),
		new(ltc.StatisticsDayTransaction).TableName(): new(ltc.StatisticsDayTransaction),
		new(ltc.StatisticsDayMinerPool).TableName():   new(ltc.StatisticsDayMinerPool),
		new(ltc.StatisticsDayOHLC).TableName():        new(ltc.StatisticsDayOHLC),

		// eos
		new(eos.Meta).TableName():        new(eos.Meta),
		new(eos.Block).TableName():       new(eos.Block),
		new(eos.Transaction).TableName(): new(eos.Transaction),
		new(eos.Action).TableName():      new(eos.Action),

		// xrp
		new(xrp.Meta).TableName():          new(xrp.Meta),
		new(xrp.Block).TableName():         new(xrp.Block),
		new(xrp.Transaction).TableName():   new(xrp.Transaction),
		new(xrp.Path).TableName():          new(xrp.Path),
		new(xrp.Account).TableName():       new(xrp.Account),
		new(xrp.AffectedNodes).TableName(): new(xrp.AffectedNodes),
		new(xrp.Amount).TableName():        new(xrp.Amount),

		// doge
		new(doge.Meta).TableName():                     new(doge.Meta),
		new(doge.Block).TableName():                    new(doge.Block),
		new(doge.Transaction).TableName():              new(doge.Transaction),
		new(doge.VIn).TableName():                      new(doge.VIn),
		new(doge.VOut).TableName():                     new(doge.VOut),
		new(doge.Address).TableName():                  new(doge.Address),
		new(doge.Mining).TableName():                   new(doge.Mining),
		new(doge.Node).TableName():                     new(doge.Node),
		new(doge.StatisticsDayBlock).TableName():       new(doge.StatisticsDayBlock),
		new(doge.StatisticsDayTransaction).TableName(): new(doge.StatisticsDayTransaction),
		new(doge.StatisticsDayOHLC).TableName():        new(doge.StatisticsDayOHLC),
		new(doge.StatisticsMonth).TableName():          new(doge.StatisticsMonth),

		//bsv
		new(bsv.Meta).TableName():                     new(bsv.Meta),
		new(bsv.Block).TableName():                    new(bsv.Block),
		new(bsv.Transaction).TableName():              new(bsv.Transaction),
		new(bsv.VIn).TableName():                      new(bsv.VIn),
		new(bsv.VOut).TableName():                     new(bsv.VOut),
		new(bsv.Address).TableName():                  new(bsv.Address),
		new(bsv.Mining).TableName():                   new(bsv.Mining),
		new(bsv.StatisticsDayBlock).TableName():       new(bsv.StatisticsDayBlock),
		new(bsv.StatisticsDayTransaction).TableName(): new(bsv.StatisticsDayTransaction),
		new(bsv.StatisticsDayMinerPool).TableName():   new(bsv.StatisticsDayMinerPool),
		new(bsv.StatisticsDayOHLC).TableName():        new(bsv.StatisticsDayOHLC),
		new(bsv.StatisticsMonth).TableName():          new(bsv.StatisticsMonth),

		// tron
		new(tron.Meta).TableName():                          new(tron.Meta),
		new(tron.Block).TableName():                         new(tron.Block),
		new(tron.Transaction).TableName():                   new(tron.Transaction),
		new(tron.AccountCreateContract).TableName():         new(tron.AccountCreateContract),
		new(tron.AccountUpdateContract).TableName():         new(tron.AccountUpdateContract),
		new(tron.AssetIssueContract).TableName():            new(tron.AssetIssueContract),
		new(tron.CreateSmartContract).TableName():           new(tron.CreateSmartContract),
		new(tron.ExchangeCreateContract).TableName():        new(tron.ExchangeCreateContract),
		new(tron.ExchangeInjectContract).TableName():        new(tron.ExchangeInjectContract),
		new(tron.ExchangeTransactionContract).TableName():   new(tron.ExchangeTransactionContract),
		new(tron.ExchangeWithdrawContract).TableName():      new(tron.ExchangeWithdrawContract),
		new(tron.FreezeBalanceContract).TableName():         new(tron.FreezeBalanceContract),
		new(tron.ParticipateAssetIssueContract).TableName(): new(tron.ParticipateAssetIssueContract),
		new(tron.ProposalApproveContract).TableName():       new(tron.ProposalApproveContract),
		new(tron.ProposalCreateContract).TableName():        new(tron.ProposalCreateContract),
		new(tron.ProposalDeleteContract).TableName():        new(tron.ProposalDeleteContract),
		new(tron.TransferContract).TableName():              new(tron.TransferContract),
		new(tron.TransferAssetContract).TableName():         new(tron.TransferAssetContract),
		new(tron.TriggerSmartContract).TableName():          new(tron.TriggerSmartContract),
		new(tron.UnfreezeAssetContract).TableName():         new(tron.UnfreezeAssetContract),
		new(tron.UnfreezeBalanceContract).TableName():       new(tron.UnfreezeBalanceContract),
		new(tron.UpdateAssetContract).TableName():           new(tron.UpdateAssetContract),
		new(tron.UpdateEnergyLimitContract).TableName():     new(tron.UpdateEnergyLimitContract),
		new(tron.UpdateSettingContract).TableName():         new(tron.UpdateSettingContract),
		new(tron.VoteAssetContract).TableName():             new(tron.VoteAssetContract),
		new(tron.VoteWitnessContract).TableName():           new(tron.VoteWitnessContract),
		new(tron.WithdrawBalanceContract).TableName():       new(tron.WithdrawBalanceContract),
		new(tron.WitnessCreateContract).TableName():         new(tron.WitnessCreateContract),
		new(tron.WitnessUpdateContract).TableName():         new(tron.WitnessUpdateContract),

		// xlm
		new(xlm.Meta).TableName():        new(xlm.Meta),
		new(xlm.Ledger).TableName():      new(xlm.Ledger),
		new(xlm.Transaction).TableName(): new(xlm.Transaction),
		new(xlm.Operation).TableName():   new(xlm.Operation),
	}
)

func GetTable(tableName string) interface{} {
	if _, ok := tableMap[tableName]; ok {
		return tableMap[tableName]
	}
	return nil
}

func GetTableInfo(engine *xorm.Engine, tableName string) *xorm.Table {
	table := GetTable(tableName)
	if table != nil {
		return engine.TableInfo(table)
	}

	return nil
}

func syncTable(engine *xorm.Engine, prefix string, fn func(string) error) error {
	for tableName, table := range tableMap {
		if len(prefix) > 0 && !strings.HasPrefix(tableName, prefix) {
			continue
		}

		ok, err := engine.IsTableExist(table)
		if err != nil {
			return err
		}
		if !ok {
			log.Debug("database: table '%s' not exists, create it", engine.TableName(table))
			err = engine.Sync2(table)
			if err != nil {
				return err
			}
		} else {
			log.Debug("database: table '%s' exists, skip", engine.TableName(table))
		}
		if fn != nil {
			err = fn(tableName)
			if err != nil {
				return nil
			}
		}
	}
	return nil
}

func CheckBCHTable(engine *xorm.Engine) error {
	err := syncTable(engine, fmt.Sprintf("%s_meta", bch.TablePrefix), nil)
	if err != nil {
		return err
	}

	fn := func(tableName string) error {
		meta := new(bch.Meta)
		meta.Name = engine.TableName(tableName)
		if meta.Name != meta.TableName() {
			has, err := engine.Get(meta)
			if err != nil {
				return err
			}
			if !has {
				meta.LastID = 0
				meta.Count = 0
				_, err := engine.Insert(meta)
				if err != nil {
					return err
				}
				log.Debug("database: table '%s' generate meta", engine.TableName(tableName))
			}
		}
		return nil
	}

	err = syncTable(engine, bch.TablePrefix, fn)
	if err != nil {
		return err
	}
	return nil
}

func CheckBTCTable(engine *xorm.Engine) error {
	err := syncTable(engine, fmt.Sprintf("%s_meta", btc.TablePrefix), nil)
	if err != nil {
		return err
	}

	fn := func(tableName string) error {
		meta := new(btc.Meta)
		meta.Name = engine.TableName(tableName)
		if meta.Name != meta.TableName() {
			has, err := engine.Get(meta)
			if err != nil {
				return err
			}
			if !has {
				meta.LastID = 0
				meta.Count = 0
				_, err := engine.Insert(meta)
				if err != nil {
					return err
				}
				log.Debug("database: table '%s' generate meta", engine.TableName(tableName))
			}
		}
		return nil
	}

	err = syncTable(engine, btc.TablePrefix, fn)
	if err != nil {
		return err
	}
	return nil
}

func CheckETCTable(engine *xorm.Engine) error {
	err := syncTable(engine, fmt.Sprintf("%s_meta", etc.TablePrefix), nil)
	if err != nil {
		return err
	}

	fn := func(tableName string) error {
		meta := new(etc.Meta)
		meta.Name = engine.TableName(tableName)
		if meta.Name != meta.TableName() {
			has, err := engine.Get(meta)
			if err != nil {
				return err
			}
			if !has {
				meta.LastID = 0
				meta.Count = 0
				_, err := engine.Insert(meta)
				if err != nil {
					return err
				}
				log.Debug("database: table '%s' generate meta", engine.TableName(tableName))
			}
		}
		return nil
	}

	err = syncTable(engine, etc.TablePrefix, fn)
	if err != nil {
		return err
	}
	return nil
}

func CheckETHTable(engine *xorm.Engine) error {
	err := syncTable(engine, fmt.Sprintf("%s_meta", eth.TablePrefix), nil)
	if err != nil {
		return err
	}

	fn := func(tableName string) error {
		meta := new(eth.Meta)
		meta.Name = engine.TableName(tableName)
		if meta.Name != meta.TableName() {
			has, err := engine.Get(meta)
			if err != nil {
				return err
			}
			if !has {
				meta.LastID = 0
				meta.Count = 0
				_, err := engine.Insert(meta)
				if err != nil {
					return err
				}
				log.Debug("database: table '%s' generate meta", engine.TableName(tableName))
			}
		}
		return nil
	}

	err = syncTable(engine, eth.TablePrefix, fn)
	if err != nil {
		return err
	}
	return nil
}

func CheckLTCTable(engine *xorm.Engine) error {
	err := syncTable(engine, fmt.Sprintf("%s_meta", ltc.TablePrefix), nil)
	if err != nil {
		return err
	}

	fn := func(tableName string) error {
		meta := new(ltc.Meta)
		meta.Name = engine.TableName(tableName)
		if meta.Name != meta.TableName() {
			has, err := engine.Get(meta)
			if err != nil {
				return err
			}
			if !has {
				meta.LastID = 0
				meta.Count = 0
				_, err := engine.Insert(meta)
				if err != nil {
					return err
				}
				log.Debug("database: table '%s' generate meta", engine.TableName(tableName))
			}
		}
		return nil
	}

	err = syncTable(engine, ltc.TablePrefix, fn)
	if err != nil {
		return err
	}
	return nil
}

func CheckEOSTable(engine *xorm.Engine) error {
	err := syncTable(engine, fmt.Sprintf("%s_meta", eos.TablePrefix), nil)
	if err != nil {
		return err
	}

	fn := func(tableName string) error {
		meta := new(eos.Meta)
		meta.Name = engine.TableName(tableName)
		if meta.Name != meta.TableName() {
			has, err := engine.Get(meta)
			if err != nil {
				return err
			}
			if !has {
				meta.LastID = 0
				meta.Count = 0
				_, err := engine.Insert(meta)
				if err != nil {
					return err
				}
				log.Debug("database: table '%s' generate meta", engine.TableName(tableName))
			}
		}
		return nil
	}

	err = syncTable(engine, eos.TablePrefix, fn)
	if err != nil {
		return err
	}
	return nil
}

func CheckXRPTable(engine *xorm.Engine) error {
	err := syncTable(engine, fmt.Sprintf("%s_meta", xrp.TablePrefix), nil)
	if err != nil {
		return err
	}

	fn := func(tableName string) error {
		meta := new(xrp.Meta)
		meta.Name = engine.TableName(tableName)
		if meta.Name != meta.TableName() {
			has, err := engine.Get(meta)
			if err != nil {
				return err
			}
			if !has {
				meta.LastID = 0
				meta.Count = 0
				_, err := engine.Insert(meta)
				if err != nil {
					return err
				}
				log.Debug("database: table '%s' generate meta", engine.TableName(tableName))
			}
		}
		return nil
	}

	err = syncTable(engine, xrp.TablePrefix, fn)
	if err != nil {
		return err
	}
	return nil
}

func CheckXLMTable(engine *xorm.Engine) error {
	err := syncTable(engine, fmt.Sprintf("%s_meta", xlm.TablePrefix), nil)
	if err != nil {
		return err
	}

	fn := func(tableName string) error {
		meta := new(xlm.Meta)
		meta.Name = engine.TableName(tableName)
		if meta.Name != meta.TableName() {
			has, err := engine.Get(meta)
			if err != nil {
				return err
			}
			if !has {
				meta.LastID = 0
				meta.Count = 0
				_, err := engine.Insert(meta)
				if err != nil {
					return err
				}
				log.Debug("database: table '%s' generate meta", engine.TableName(tableName))
			}
		}
		return nil
	}

	err = syncTable(engine, xlm.TablePrefix, fn)
	if err != nil {
		return err
	}
	return nil
}

func CheckDOGETable(engine *xorm.Engine) error {
	fn := func(tableName string) error {
		meta := new(doge.Meta)
		meta.Name = engine.TableName(tableName)
		if meta.Name != meta.TableName() {
			has, err := engine.Get(meta)
			if err != nil {
				return err
			}
			if !has {
				meta.LastID = 0
				meta.Count = 0
				_, err := engine.Insert(meta)
				if err != nil {
					return err
				}
				log.Debug("database: table '%s' generate meta", engine.TableName(tableName))
			}
		}
		return nil
	}

	err := syncTable(engine, doge.TablePrefix, fn)
	if err != nil {
		return err
	}
	return nil
}

func CheckBSVTable(engine *xorm.Engine) error {
	err := syncTable(engine, fmt.Sprintf("%s_meta", bsv.TablePrefix), nil)
	if err != nil {
		return err
	}

	fn := func(tableName string) error {
		meta := new(bsv.Meta)
		meta.Name = engine.TableName(tableName)
		if meta.Name != meta.TableName() {
			has, err := engine.Get(meta)
			if err != nil {
				return err
			}
			if !has {
				meta.LastID = 0
				meta.Count = 0
				_, err := engine.Insert(meta)
				if err != nil {
					return err
				}
				log.Debug("database: table '%s' generate meta", engine.TableName(tableName))
			}
		}
		return nil
	}

	err = syncTable(engine, bsv.TablePrefix, fn)
	if err != nil {
		return err
	}
	return nil
}

func CheckTRONTable(engine *xorm.Engine) error {
	err := syncTable(engine, fmt.Sprintf("%s_meta", tron.TablePrefix), nil)
	if err != nil {
		return err
	}

	fn := func(tableName string) error {
		meta := new(tron.Meta)
		meta.Name = engine.TableName(tableName)
		if meta.Name != meta.TableName() {
			has, err := engine.Get(meta)
			if err != nil {
				return err
			}
			if !has {
				meta.LastID = 0
				meta.Count = 0
				_, err := engine.Insert(meta)
				if err != nil {
					return err
				}
				log.Debug("database: table '%s' generate meta", engine.TableName(tableName))
			}
		}
		return nil
	}

	err = syncTable(engine, tron.TablePrefix, fn)
	if err != nil {
		return err
	}
	return nil
}
