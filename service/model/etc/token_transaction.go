package etc

import (
	"github.com/aminvici/bds/common/math"
)

type TokenTransaction struct {
	ID                     int64                `xorm:"id bigint autoincr pk"`
	BlockHeight            int64                `xorm:"block_height int index"`
	ParentTransactionHash  string               `xorm:"parent_hash char(64) notnull index"`
	ParentTransactionIndex int64                `xorm:"parent_tx_index smallint notnull"`
	From                   string               `xorm:"from char(40) notnull index"`
	To                     string               `xorm:"to char(40) notnull index"`
	Value                  math.HexOrDecimal256 `xorm:"value decimal(38,0) notnull"`
	Timestamp              int64                `xorm:"timestamp int notnull index"`
	TokenAddress           string               `xorm:"token_address char(40) notnull"`
	LogIndex               int64                `xorm:"log_index int notnull"`
}

func (t TokenTransaction) TableName() string {
	return tableName("token_transaction")
}
