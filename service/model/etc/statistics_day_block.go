package etc

import (
	"github.com/aminvici/bds/common/math"
)

/*CREATE TABLE `eth_calc_daily` (
  `id`                   INT(10) UNSIGNED NOT NULL AUTO_INCREMENT,
  `date`                 datetime         NOT NULL COMMENT '日期',
  `block_size_sum`       INT(10) UNSIGNED NOT NULL DEFAULT '0' COMMENT '区块链总体积',
  `block_size_avg`       DOUBLE           NOT NULL DEFAULT '0' COMMENT '区块体积平均大小',
  `difficulty_sum`       DOUBLE           NOT NULL DEFAULT '0' COMMENT '全网算力',
  `block_time_spent`     DOUBLE           NOT NULL DEFAULT '0' COMMENT '区块产生时间',
  `tx_rate`              DOUBLE           NOT NULL DEFAULT '0' COMMENT '交易速率',
  `tx_count`             INT(10) UNSIGNED NOT NULL DEFAULT '0' COMMENT '交易次数',
  `tx_value_sum`         DECIMAL(30,0)    NOT NULL DEFAULT '0' COMMENT '交易金额',
  `tx_value_avg`         DECIMAL(30,4)    NOT NULL DEFAULT '0' COMMENT '交易金额平均值',
  `tx_gaslimit_avg`      DOUBLE           NOT NULL DEFAULT '0' COMMENT '交易gaspro',
  `tx_gasused_avg`       DOUBLE           NOT NULL DEFAULT '0' COMMENT '交易gasused',
  `tx_gasprice_avg`      DOUBLE           NOT NULL DEFAULT '0' COMMENT '交易gasprice',
  `miner`                INT(10) UNSIGNED NOT NULL DEFAULT '0' COMMENT '已获奖励矿工数量',
  `tx_fee_avg`           DECIMAL(30,4)    NOT NULL DEFAULT '0' COMMENT '交易矿工费平均值',
  `block_gaslimit_avg`   DOUBLE           NOT NULL DEFAULT '0' COMMENT '区块gaslimit平均值',
  `block_gasused_avg`    DOUBLE           NOT NULL DEFAULT '0' COMMENT '区块gasused平均值',
  `block_profit_avg`     DECIMAL(30,4)    NOT NULL DEFAULT '0' COMMENT '区块矿工总收益平均值',
  `block_fee_avg`        DECIMAL(30,4)    NOT NULL DEFAULT '0' COMMENT '区块交易费平均值',
  `block_reward_avg`     DECIMAL(30,4)    NOT NULL DEFAULT '0' COMMENT '区块奖励平均值',
  `uncle_gaslimit_avg`   DOUBLE           NOT NULL DEFAULT '0' COMMENT '叔块gaslimit平均值',
  `uncle_gasused_avg`    DOUBLE           NOT NULL DEFAULT '0' COMMENT '叔块gasused平均值',
  `uncle_reward_avg`     DECIMAL(30,4)    NOT NULL DEFAULT '0' COMMENT '叔块矿工总收益平均值',
  `new_address_count`    INT(10) UNSIGNED NOT NULL DEFAULT '0' COMMENT '每日新增账户数',
  `total_address_uptonow`INT(10) UNSIGNED NOT NULL DEFAULT '0' COMMENT '每日总的账户'
  `active_address_count` INT(10) UNSIGNED NOT NULL DEFAULT '0' COMMENT '日活',
  `tx_address_cnt_avg`   DOUBLE           NOT NULL DEFAULT '0' COMMENT '每日参与交易的交易地址的交易次数的平均值',
  `tx_address_value_avg` DECIMAL(30,4)    NOT NULL DEFAULT '0' COMMENT '每日参与交易的交易地址的交易金额的平均值',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_date` (`date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


*/
type StatisticsDayBlock struct {
	ID                       int64                `xorm:"id bigint autoincr pk"`
	Timestamp                int64                `xorm:"timestamp int notnull unique index"`
	BlockCount               int64                `xorm:"block_count bigint notnull default '0'"`
	BlockSizeSum             int64                `xorm:"block_size_sum bigint notnull default '0'"`
	BlockSizeAvg             float64              `xorm:"block_size_avg decimal(38,4) notnull default '0'"`
	BlockTimeSpent           float64              `xorm:"block_time_spent decimal(38,4) notnull default '0'"`
	DifficultySum            float64              `xorm:"difficulty_sum decimal(38,4) notnull default '0'"`
	ForkNumber               int                  `xorm:"fork_number int notnull default '0'"`
	MinerCount               int                  `xorm:"miner_count int notnull default '0'"`
	TotalMinerCount          int64                `xorm:"total_miner_count bigint notnull default '0'"`
	BlockGasLimitAvg         float64              `xorm:"block_gas_limit_avg decimal(38,4) notnull default '0'"`
	BlockGasUsedAvg          float64              `xorm:"block_gas_used_avg decimal(38,4) notnull default '0'"`
	BlockFeeAvg              float64              `xorm:"block_fee_avg decimal(38,4) notnull default '0'"`
	BlockRewardAvg           float64              `xorm:"block_reward_avg decimal(38,4) notnull default '0'"`
	BlockRefereneceRewardAvg float64              `xorm:"block_reference_reward_avg decimal(38,4) notnull default '0'"`
	UncleGasLimitAvg         float64              `xorm:"uncle_gas_limit_avg decimal(38,4) notnull default '0'"`
	UncleGasUsedAvg          float64              `xorm:"uncle_gas_used_avg decimal(38,4) notnull default '0'"`
	UncleRewardAvg           float64              `xorm:"uncle_reward_avg decimal(38,4) notnull default '0'"`
	Supply                   math.HexOrDecimal256 `xorm:"supply decimal(38,0) notnull default '0'"`
	MinerFeeMid              int64                `xorm:"miner_fee_mid bigint notnull default '0'"`
}

func (t StatisticsDayBlock) TableName() string {
	return tableName("statistics_day_block")
}
