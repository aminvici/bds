package etc

import (
	"github.com/aminvici/bds/common/math"
)

type Block struct {
	ID                     int64                `xorm:"id bigint autoincr pk"`
	Height                 int64                `xorm:"height int notnull unique index"`
	Hash                   string               `xorm:"hash char(64) notnull index"`
	ParentHash             string               `xorm:"parent_hash char(64) notnull"`
	SHA3Uncles             string               `xorm:"sha3_uncles char(64) notnull"`
	Nonce                  string               `xorm:"nonce char(16) notnull"`
	MixHash                string               `xorm:"mix_hash char(64) notnull"`
	Miner                  string               `xorm:"miner char(40) not null"`
	PoolName               string               `xorm:"pool_name varchar(50) notnull index default ''"`
	Timestamp              int64                `xorm:"timestamp int notnull index"`
	ExtraData              string               `xorm:"extra_data varchar(64) notnull"`
	LogsBloom              string               `xorm:"logs_bloom varchar(512) notnull"`
	TransactionRoot        string               `xorm:"transactions_root char(64) notnull"`
	StateRoot              string               `xorm:"state_root char(64) notnull"`
	ReceiptsRoot           string               `xorm:"receipts_root char(64) notnull"`
	GasUsed                int64                `xorm:"gas_used bigint notnull"`
	GasLimit               int64                `xorm:"gas_limit bigint notnull"`
	Difficulty             math.HexOrDecimal256 `xorm:"difficulty decimal(38,0) notnull"`
	TotalDifficulty        math.HexOrDecimal256 `xorm:"total_difficulty decimal(38,0) notnull"`
	RealDifficulty         float64              `xorm:"real_difficulty double notnull"`
	Size                   int64                `xorm:"size int notnull"`
	UncleLen               int                  `xorm:"uncle_len tinyint notnull"`
	TransactionLen         int                  `xorm:"tx_len int notnull"`
	ContractTransactionLen int                  `xorm:"contract_tx_len int notnull"`
	BlockReward            math.HexOrDecimal256 `xorm:"block_reward decimal(30,0) notnull"`
	BlockUncleReward       uint64               `xorm:"block_uncle_reward bigint notnull"`
}

func (t Block) TableName() string {
	return tableName("block")
}
