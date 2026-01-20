package eth

import (
	"github.com/aminvici/bds/common/math"
)

type StatisticsDayInternalTransaction struct {
	ID                         int64                `xorm:"id bigint autoincr pk"`
	Timestamp                  int64                `xorm:"timestamp int notnull unique"`
	Count                      int64                `xorm:"count int notnull default '0'"`
	ValueSum                   math.HexOrDecimal256 `xorm:"value_sum decimal(38,0) notnull default '0'"`
	ValueAvg                   float64              `xorm:"value_avg decimal(38,4) notnull default '0'"`
	Rate                       float64              `xorm:"rate decimal(38,4) notnull default '0'"`
	GasLimitAvg                float64              `xorm:"gas_limit_avg decimal(38,4) notnull default '0'"`
	GasUsedAvg                 float64              `xorm:"gas_used_avg decimal(38,4) notnull default '0'"`
	AddressCount               int64                `xorm:"address_count int  notnull default '0'"`
	TotalAddressCount          int64                `xorm:"total_address_count int  notnull default '0'"`
	ActiveAddressCount         int64                `xorm:"active_address_count int  notnull default '0'"`
	AddressTransactionCountAvg float64              `xorm:"address_tx_count_avg decimal(38,4) notnull default '0'"`
	AddressTransactionValueAvg float64              `xorm:"address_tx_value_avg decimal(38,3) notnull default '0'"`
}

func (t StatisticsDayInternalTransaction) TableName() string {
	return tableName("statistics_day_internal_transaction")
}
