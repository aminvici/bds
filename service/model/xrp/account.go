package xrp

import (
	"github.com/aminvici/bds/common/math"
)

type Account struct {
	ID           int64  `xorm:"id bigint autoincr pk"`
	Address      string `xorm:"address char(34) notnull index"` //账户地址
	SignerWeight int    `xorm:"signer_weight int null default '0'"`
	SignerEntrie string `xorm:"signer_entrie char(34) notnull index"`

	Type                int                  `xorm:"type tinyint notnull default '0'"`          //账户类型
	Balance             math.HexOrDecimal256 `xorm:"balance decimal(38,0) notnull default '0'"` //账户余额
	Creator             string               `xorm:"creator char(40) notnull default '' "`
	BirthTimestamp      int64                `xorm:"birth_timestamp int notnull default '0' "`      //账户第一次出现的时间
	LastActiveTimestamp int64                `xorm:"last_active_timestamp int notnull default '0'"` //账户上次活跃时间
}

func (t Account) TableName() string {
	return tableName("account")
}
