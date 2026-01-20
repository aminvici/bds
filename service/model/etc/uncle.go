package etc

import (
	"github.com/aminvici/bds/common/math"
)

type Uncle struct {
	ID              int64                `xorm:"id bigint autoincr pk"`
	Height          int64                `xorm:"height int notnull index"`
	Hash            string               `xorm:"hash char(64) notnull index"`
	BlockHeight     int64                `xorm:"block_height int notnull index"`
	ParentHash      string               `xorm:"parent_hash char(64) notnull"`
	Sha3uncles      string               `xorm:"sha3_uncles char(64) notnull"`
	Nonce           string               `xorm:"nonce char(16) notnull"`
	MixHash         string               `xorm:"mix_hash char(64) notnull"`
	Miner           string               `xorm:"miner char(40) not null"`
	PoolName        string               `xorm:"pool_name varchar(50) notnull default ''"`
	Timestamp       int64                `xorm:"timestamp int notnull index"`
	ExtraData       string               `xorm:"extra_data varchar(64) notnull"`
	LogsBloom       string               `xorm:"logs_bloom varchar(512) notnull"`
	TransactionRoot string               `xorm:"transactions_root char(64) notnull"`
	StateRoot       string               `xorm:"state_root char(64) notnull"`
	ReceiptsRoot    string               `xorm:"receipt_root char(64) notnull"`
	GasUsed         int64                `xorm:"gas_used bigint notnull"`
	GasLimit        int64                `xorm:"gas_limit bigint notnull"`
	UncleIndex      int                  `xorm:"uncle_index tinyint notnull"`
	Difficulty      int64                `xorm:"difficulty bigint notnull"`
	TotalDifficulty math.HexOrDecimal256 `xorm:"total_difficulty decimal(30,0) notnull"`
	Size            int64                `xorm:"size int notnull"`
	UncleLen        int                  `xorm:"uncle_len tinyint notnull"`
	Reward          uint64               `xorm:"reward bigint notnull"`
}

func (t Uncle) TableName() string {
	return tableName("uncle")
}
