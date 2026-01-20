package eth

import (
	"github.com/aminvici/bds/common/math"
)

type ENS struct {
	ID                    int64                `xorm:"id bigint autoincr pk"`
	Timestamp             int64                `xorm:"timestamp int notnull index"`
	Hash                  string               `xorm:"hash char(64) notnull index"`
	BlockHeight           int64                `xorm:"block_height int index"`
	TransactionBlockIndex int                  `xorm:"tx_block_index smallint notnull"`
	LabelHash             string               `xorm:"label_hash char(64) notnull index"`
	From                  string               `xorm:"from char(40) notnull index"`
	To                    string               `xorm:"to char(40) notnull index"`
	FunctionType          string               `xorm:"function_type char(40) notnull index"`
	RegistrationDate      int64                `xorm:"registration_date int notnull index"`
	Bidder                string               `xorm:"bidder char(40) notnull index"`
	Deposit               math.HexOrDecimal256 `xorm:"deposit decimal(38,0) notnull"`
	Owner                 string               `xorm:"owner char(40) notnull index"`
	Value                 math.HexOrDecimal256 `xorm:"value decimal(38,0) notnull"`
	Status                int                  `xorm:"status tinyint notnull"`
}

func (t ENS) TableName() string {
	return tableName("ens")
}
