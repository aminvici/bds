package eth

import (
	"github.com/aminvici/bds/common/math"
)

type StatisticsDayTransaction struct {
	ID                         int64                `xorm:"id bigint autoincr pk"`
	Timestamp                  int64                `xorm:"timestamp int notnull unique index"`
	Count                      int                  `xorm:"count int notnull default '0'"`
	Rate                       float64              `xorm:"rate decimal(38,4) notnull default '0'"`
	ValueSum                   math.HexOrDecimal256 `xorm:"value_sum decimal(38,0) notnull default '0'"`
	ValueAvg                   float64              `xorm:"value_avg decimal(38,4) notnull default '0'"`
	GasLimitAvg                float64              `xorm:"gas_limit_avg decimal(38,4) notnull default '0'"`
	GasUsedAvg                 float64              `xorm:"gas_used_avg decimal(38,4) notnull default '0'"`
	GasPriceAvg                float64              `xorm:"gas_price_avg decimal(38,4) notnull default '0'"`
	FeeAvg                     float64              `xorm:"fee_avg decimal(38,4) notnull default '0'"`
	AddressCount               int64                `xorm:"address_count int notnull default '0'"`
	TotalAddressCount          int64                `xorm:"total_address_count int notnull default '0'"`
	ActiveAddressCount         int64                `xorm:"active_address_count int  notnull default '0'"`
	SleepAddressCount          int64                `xorm:"sleep_address int notnull default '0'"`
	AddressTransactionCountAvg float64              `xorm:"address_tx_count_avg decimal(38,4) notnull default '0'"`
	AddressTransactionValueAvg float64              `xorm:"address_tx_value_avg decimal(38,4) notnull default '0'"`
	ContractTransactionCount   int64                `xorm:"contract_tx_count int notnull default '0'"`
	StoreRate                  float64              `xorm:"store_rate decimal(38,4) notnull default '0'"`
	MarketValueRatio           float64              `xorm:"market_value_ratio decimal(38,4) notnull default '0'"`
	SizeAvg                    float64              `xorm:"size_avg decimal(38,4) notnull default '0'"`
	SizeFeeAvg                 float64              `xorm:"size_fee_avg decimal(38,4) notnull default '0'"`
	FreshRate                  float64              `xorm:"fresh_rate decimal(38,4) notnull default '0'"`
	ValueWithoutHotAddress     math.HexOrDecimal256 `xorm:"value_without_hot_address decimal(38,0) notnull default '0'"`
	ValueWithoutLongChain      math.HexOrDecimal256 `xorm:"value_without_long_chain decimal(38,0) notnull default '0'"`
}

func (t StatisticsDayTransaction) TableName() string {
	return tableName("statistics_day_transaction")
}
