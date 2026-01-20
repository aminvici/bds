package etc

import (
	"github.com/aminvici/bds/common/math"
)

type Balance struct {
	ID        int64                `xorm:"id  bigint  autoincr pk"`
	Address   string               `xorm:"address char(40) notnull"`                  //账户地址
	Balance   math.HexOrDecimal256 `xorm:"balance decimal(30,0) notnull default '0'"` //账户余额
	Timestamp int64                `xorm:"timestamp int "`
}

func (t Balance) TableName() string {
	return tableName("balance")
}
