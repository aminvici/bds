package eth

import (
	"github.com/aminvici/bds/common/math"
)

type InternalTransaction struct {
	ID                       int64                `xorm:"id bigint autoincr pk"`
	BlockHeight              int64                `xorm:"block_height int index"`
	Hash                     string               `xorm:"hash varchar(64) notnull index"`
	Timestamp                int64                `xorm:"timestamp int notnull index"`
	Type                     string               `xorm:"type varchar(20) notnull"`
	From                     string               `xorm:"from varchar(40) notnull index"`
	To                       string               `xorm:"to varchar(40) notnull index"`
	Value                    math.HexOrDecimal256 `xorm:"value decimal(38,0) notnull"`
	GasLimit                 int64                `xorm:"gas_limit bigint notnull"`
	GasUsed                  int64                `xorm:"gas_used bigint notnull"`
	TransactionIndex         int64                `xorm:"tx_index int notnull"`
	InternalTransactionIndex int64                `xorm:"internal_tx_index int notnull"`
}

func (t InternalTransaction) TableName() string {
	return tableName("internal_transaction")
}
