package eth

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
type StatisticsDayAdditionalIndexes struct {
	ID                 int64 `xorm:"id bigint autoincr pk"`
	Timestamp          int64 `xorm:"timestamp int notnull unique index"`
	ActiveAddress      int64 `xorm:"active_address decimal(38,0) null default '0'"`
	NewAddress0        int64 `xorm:"new_address_0 decimal(38,0) null default '0'"`
	NewAddress1To7     int64 `xorm:"new_address_1_7 decimal(38,0) null default '0'"`
	NewAddress8To14    int64 `xorm:"new_address_8_14 decimal(38,0) null default '0'"`
	NewAddress15To21   int64 `xorm:"new_address_15_21 decimal(38,0) null default '0'"`
	NewAddress22To28   int64 `xorm:"new_address_22_28 decimal(38,0) null default '0'"`
	NewAddress29ToPlus int64 `xorm:"new_address_29_plus decimal(38,0) null default '0'"`

	HourActiveCount0  int64 `xorm:"hour_active_count_0 int null default '0'"`
	HourActiveCount1  int64 `xorm:"hour_active_count_1 int null default '0'"`
	HourActiveCount2  int64 `xorm:"hour_active_count_2 int null default '0'"`
	HourActiveCount3  int64 `xorm:"hour_active_count_3 int null default '0'"`
	HourActiveCount4  int64 `xorm:"hour_active_count_4 int null default '0'"`
	HourActiveCount5  int64 `xorm:"hour_active_count_5 int null default '0'"`
	HourActiveCount6  int64 `xorm:"hour_active_count_6 int null default '0'"`
	HourActiveCount7  int64 `xorm:"hour_active_count_7 int null default '0'"`
	HourActiveCount8  int64 `xorm:"hour_active_count_8 int null default '0'"`
	HourActiveCount9  int64 `xorm:"hour_active_count_9 int null default '0'"`
	HourActiveCount10 int64 `xorm:"hour_active_count_10 int null default '0'"`
	HourActiveCount11 int64 `xorm:"hour_active_count_11 int null default '0'"`
	HourActiveCount12 int64 `xorm:"hour_active_count_12 int null default '0'"`
	HourActiveCount13 int64 `xorm:"hour_active_count_13 int null default '0'"`
	HourActiveCount14 int64 `xorm:"hour_active_count_14 int null default '0'"`
	HourActiveCount15 int64 `xorm:"hour_active_count_15 int null default '0'"`
	HourActiveCount16 int64 `xorm:"hour_active_count_16 int null default '0'"`
	HourActiveCount17 int64 `xorm:"hour_active_count_17 int null default '0'"`
	HourActiveCount18 int64 `xorm:"hour_active_count_18 int null default '0'"`
	HourActiveCount19 int64 `xorm:"hour_active_count_19 int null default '0'"`
	HourActiveCount20 int64 `xorm:"hour_active_count_20 int null default '0'"`
	HourActiveCount21 int64 `xorm:"hour_active_count_21 int null default '0'"`
	HourActiveCount22 int64 `xorm:"hour_active_count_22 int null default '0'"`
	HourActiveCount23 int64 `xorm:"hour_active_count_23 int null default '0'"`

	TransactionValueInterval1 float64 `xorm:"transaction_value_interval_1 decimal(38,4) null default '0'"`
	TransactionValueInterval2 float64 `xorm:"transaction_value_interval_2 decimal(38,4) null default '0'"`
	TransactionValueInterval3 float64 `xorm:"transaction_value_interval_3 decimal(38,4) null default '0'"`
	TransactionValueInterval4 float64 `xorm:"transaction_value_interval_4 decimal(38,4) null default '0'"`
	TransactionValueInterval5 float64 `xorm:"transaction_value_interval_5 decimal(38,4) null default '0'"`
	TransactionValueInterval6 float64 `xorm:"transaction_value_interval_6 decimal(38,4) null default '0'"`

	TransactionCountInterval1 int64 `xorm:"transaction_count_interval_1 int null default '0'"`
	TransactionCountInterval2 int64 `xorm:"transaction_count_interval_2 int null default '0'"`
	TransactionCountInterval3 int64 `xorm:"transaction_count_interval_3 int null default '0'"`
	TransactionCountInterval4 int64 `xorm:"transaction_count_interval_4 int null default '0'"`
	TransactionCountInterval5 int64 `xorm:"transaction_count_interval_5 int null default '0'"`
	TransactionCountInterval6 int64 `xorm:"transaction_count_interval_6 int null default '0'"`

	ActiveCountIn1Days  int64 `xorm:"active_count_in_1_days int null default '0'"`
	ActiveCountIn2Days  int64 `xorm:"active_count_in_2_days int null default '0'"`
	ActiveCountIn3Days  int64 `xorm:"active_count_in_3_days int null default '0'"`
	ActiveCountIn4Days  int64 `xorm:"active_count_in_4_days int null default '0'"`
	ActiveCountIn5Days  int64 `xorm:"active_count_in_5_days int null default '0'"`
	ActiveCountIn6Days  int64 `xorm:"active_count_in_6_days int null default '0'"`
	ActiveCountIn7Days  int64 `xorm:"active_count_in_7_days int null default '0'"`
	ActiveCountIn8Days  int64 `xorm:"active_count_in_8_days int null default '0'"`
	ActiveCountIn9Days  int64 `xorm:"active_count_in_9_days int null default '0'"`
	ActiveCountIn10Days int64 `xorm:"active_count_in_10_days int null default '0'"`
	ActiveCountIn11Days int64 `xorm:"active_count_in_11_days int null default '0'"`
	ActiveCountIn12Days int64 `xorm:"active_count_in_12_days int null default '0'"`
	ActiveCountIn13Days int64 `xorm:"active_count_in_13_days int null default '0'"`
	ActiveCountIn14Days int64 `xorm:"active_count_in_14_days int null default '0'"`
	ActiveCountIn15Days int64 `xorm:"active_count_in_15_days int null default '0'"`

	LargeTransactionCountPercentage float64 `xorm:"large_transaction_count_percentage decimal(38,4) null default '0'"`
	LargeTransactionValuePercentage float64 `xorm:"large_transaction_value_percentage decimal(38,4) null default '0'"`

	ReviveAccountNumber     int64 `xorm:"revive_account_number int null default '0'"`
	ReliveAccountNumber     int64 `xorm:"relive_account_number int null default '0'"`
	DeadAccountNumberByThen int64 `xorm:"dead_account_number_by_then int null default '0'"`

	ContractTransactionCount int64 `xorm:"contract_transaction_count int null default '0'"`

	AccountBalanceInterval1  int64 `xorm:"account_balance_interval_1 int null default '0'"`
	AccountBalanceInterval2  int64 `xorm:"account_balance_interval_2 int null default '0'"`
	AccountBalanceInterval3  int64 `xorm:"account_balance_interval_3 int null default '0'"`
	AccountBalanceInterval4  int64 `xorm:"account_balance_interval_4 int null default '0'"`
	AccountBalanceInterval5  int64 `xorm:"account_balance_interval_5 int null default '0'"`
	AccountBalanceInterval6  int64 `xorm:"account_balance_interval_6 int null default '0'"`
	AccountBalanceInterval7  int64 `xorm:"account_balance_interval_7 int null default '0'"`
	AccountBalanceInterval8  int64 `xorm:"account_balance_interval_8 int null default '0'"`
	AccountBalanceInterval9  int64 `xorm:"account_balance_interval_9 int null default '0'"`
	AccountBalanceInterval10 int64 `xorm:"account_balance_interval_10 int null default '0'"`

	TotalTokenTransferValue math.HexOrDecimal256 `xorm:"total_token_transfer_value decimal(38,0) null default '0'"`

	BlockGasPriceAvg   float64 `xorm:"block_gasprice_avg decimal(38,4) null default '0'"`
	UncleNumber        int64   `xorm:"uncle_number int null default '0'"`
	UncleRewardSum     int64   `xorm:"uncle_reward_sum decimal(38,0) null default '0'"`
	BlockRewardSum     int64   `xorm:"block_reward_sum decimal(38,0) null default '0'"`
	BlockDifficultyAvg int64   `xorm:"block_difficulty_avg int null default '0'"`
}

func (t StatisticsDayAdditionalIndexes) TableName() string {
	return tableName("statistics_day_additional_indexes")
}
