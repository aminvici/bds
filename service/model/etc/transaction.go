package etc

import (
	"github.com/aminvici/bds/common/math"
)

type Transaction struct {
	ID                    int64                `xorm:"id bigint autoincr pk"`
	Hash                  string               `xorm:"hash char(64) notnull index"`
	BlockHeight           int64                `xorm:"block_height int index"`
	From                  string               `xorm:"from char(40) notnull index"`
	To                    string               `xorm:"to char(40) notnull index"`
	ContractAddress       string               `xorm:"contract_address char(40) notnull index"`
	Value                 math.HexOrDecimal256 `xorm:"value decimal(30,0) notnull"`
	Timestamp             int64                `xorm:"timestamp int notnull index"`
	Gas                   int64                `xorm:"gas bigint notnull"`
	GasPrice              int64                `xorm:"gas_price bigint notnull"`
	GasUsed               int64                `xorm:"gas_used bigint notnull"`
	CumulativeGasUsed     int64                `xorm:"cumulative_gas_used bigint notnull"`
	Nonce                 int                  `xorm:"nonce int notnull"`
	TransactionBlockIndex int                  `xorm:"tx_block_index smallint notnull"`
	Status                uint                 `xorm:"status tinyint notnull"`
	Type                  int                  `xorm:"type tinyint notnull"`
	Root                  string               `xorm:"root char(64) notnull"`
	ChainID               int                  `xorm:"chain_id int notnull"`
	LogLen                int                  `xorm:"log_len int notnull"`
	ReplayProtected       bool                 `xorm:"replay_protected tinyint notnull"`
}

func (t Transaction) TableName() string {
	return tableName("transaction")
}
