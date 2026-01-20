package eth

import (
	"github.com/aminvici/bds/common/math"
)

type Transaction struct {
	ID                    int64                `xorm:"id bigint autoincr pk"`
	Hash                  string               `xorm:"hash char(64) notnull unique index"`
	BlockHeight           int64                `xorm:"block_height int index"`
	From                  string               `xorm:"from char(40) notnull index"`
	To                    string               `xorm:"to char(40) notnull index"`
	ContractAddress       string               `xorm:"contract_address char(40) notnull index"`
	Value                 math.HexOrDecimal256 `xorm:"value decimal(38,0) notnull"`
	Timestamp             int64                `xorm:"timestamp int notnull index"`
	GasNumber             int64                `xorm:"gas_number bigint notnull"`
	GasPrice              int64                `xorm:"gas_price bigint notnull"`
	GasUsed               int64                `xorm:"gas_used bigint notnull"`
	CumulativeGasUsed     int64                `xorm:"cumulative_gas_used bigint notnull"`
	Nonce                 int                  `xorm:"nonce int notnull"`
	V                     string               `xorm:"v char(2) notnull"`
	R                     string               `xorm:"r char(64) notnull"`
	S                     string               `xorm:"s char(64) notnull"`
	TransactionBlockIndex int                  `xorm:"tx_block_index smallint notnull"`
	Status                uint                 `xorm:"status tinyint notnull"`
	Type                  int                  `xorm:"type tinyint notnull"`
	Root                  string               `xorm:"root char(64) notnull"`
	LogsBloom             string               `xorm:"logs_bloom varchar(512) notnull"`
	LogLen                int                  `xorm:"log_len int notnull"`
	TransactionSize       int                  `xorm:"tx_size int notnull"`
}

func (t Transaction) TableName() string {
	return tableName("transaction")
}
