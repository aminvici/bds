package etc

import (
	"github.com/aminvici/bds/common/math"
)

type TokenAccount struct {
	ID                  int64                `xorm:"id bigint autoincr pk"`
	Address             string               `xorm:"address char(40) notnull unique(IDX_eth_token_address)"` //账户地址
	TokenAddress        string               `xorm:"token_address char(40) notnull unique(IDX_eth_token_address)"`
	Balance             math.HexOrDecimal256 `xorm:"balance decimal(38,0) notnull default '0'"`     //账户余额
	BirthTimestamp      int64                `xorm:"birth_timestamp int notnull default '0' "`      //账户第一次出现的时间
	LastActiveTimestamp int64                `xorm:"last_active_timestamp int notnull default '0'"` //账户上次活跃时间
}

func (t TokenAccount) TableName() string {
	return tableName("token_account")
}
