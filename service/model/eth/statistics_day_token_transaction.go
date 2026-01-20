package eth

import (
	"github.com/aminvici/bds/common/math"
)

type StatisticsDayTokenTransaction struct {
	ID                         int64                `xorm:"id bigint autoincr pk"`
	Timestamp                  int64                `xorm:"timestamp int notnull unique(IDX_eth_token_address)"`
	TokenAddress               string               `xorm:"token_address char(40) notnull unique(IDX_eth_token_address)"`
	Count                      int64                `xorm:"count int notnull default '0'"`
	ValueSum                   math.HexOrDecimal256 `xorm:"value_sum decimal(38,0) notnull default '0'"`
	ValueAvg                   float64              `xorm:"value_avg decimal(38,4) notnull default '0'"`
	StoreRate                  float64              `xorm:"store_rate decimal(38,4) notnull default '0'"`
	MarketValueRatio           float64              `xorm:"market_value_ratio decimal(38,4) notnull default '0'"`
	AddressCount               int64                `xorm:"address_count int notnull default '0'"`
	TotalAddressCount          int64                `xorm:"total_address_count int notnull default '0'"`
	ActiveAddressCount         int64                `xorm:"active_address_count int notnull default '0'"`
	AddressTransactionCountAvg float64              `xorm:"address_tx_count_avg decimal(38,4) notnull default '0'"`
	AddressTransactionValueAvg float64              `xorm:"address_tx_value_avg decimal(38,4) notnull default '0'"`
}

func (t StatisticsDayTokenTransaction) TableName() string {
	return tableName("statistics_day_token_transaction")
}
