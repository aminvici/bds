package doge

import "github.com/aminvici/bds/common/math"

type StatisticsDayBlock struct {
	ID               int64                `xorm:"id bigint autoincr pk"`
	Timestamp        int64                `xorm:"timestamp int notnull unique"`
	BlockSizeSum     int64                `xorm:"block_size_sum bigint notnull default '0'"`
	BlockSizeNew     int64                `xorm:"block_size_new bigint notnull default '0'"`
	BlockSizeAvg     float64              `xorm:"block_size_avg double notnull default '0'"`
	BlockFeeAvg      float64              `xorm:"block_fee_avg double notnull default '0'"`
	BlockCoinbaseAvg float64              `xorm:"block_coinbase_avg double notnull default '0'"`
	BlockTimeSpent   float64              `xorm:"block_time_spent double notnull default '0'"`
	DifficultySum    float64              `xorm:"difficulty_sum double notnull default '0'"`
	Supply           math.HexOrDecimal256 `xorm:"supply decimal(38,0) notnull default '0'"`
	MinerCount       int64                `xorm:"miner_count bigint notnull default '0'"`
	TotalMinerCount  int64                `xorm:"total_miner_count bigint notnull default '0'"`
}

func (t StatisticsDayBlock) TableName() string {
	return tableName("statistics_day_block")
}
