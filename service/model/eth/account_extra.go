package eth

import (
	"github.com/aminvici/bds/common/math"
)

type AccountExtra struct {
	ID                      int64                `xorm:"id bigint autoincr pk"`
	Address                 string               `xorm:"address char(40) notnull unique index"`                  //账户地址
	Balance                 math.HexOrDecimal256 `xorm:"balance decimal(38,0) notnull default '0'"`              //账户余额
	BirthTimestamp          int64                `xorm:"birth_timestamp int(10) notnull default '0' "`           //账户第一次出现的时间
	LastActiveTimestamp     int64                `xorm:"last_active_timestamp int(10) notnull default '0'"`      //账户上次活跃时间
	LastLastActiveTimestamp int64                `xorm:"last_last_active_timestamp int(10) notnull default '0'"` //账户上上次活跃时间
}

func (t AccountExtra) TableName() string {
	return tableName("account_extra")
}
