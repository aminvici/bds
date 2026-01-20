package etc

import (
	"github.com/aminvici/bds/common/math"
)

type StatisticsDay struct {
	ID                   int64                `xorm:"id bigint autoincr pk"`
	Timestamp            int64                `xorm:"timestamp int notnull unique index"`
	BlockNum             int64                `xorm:"block_num bigint notnull default '0'"`
	BlockChainNum        int64                `xorm:"block_chain_num bigint notnull default '0'"`
	BlockUncleNum        int64                `xorm:"block_uncle_num bigint notnull default '0'"`
	BlockSizeSum         int64                `xorm:"block_size_sum bigint notnull default '0'"`
	BlockSizeAvg         float64              `xorm:"block_size_avg double notnull default '0'"`
	BlockTimeSpent       float64              `xorm:"block_time_spent double notnull default '0'"`
	DifficultySum        float64              `xorm:"difficulty_sum double notnull default '0'"`
	ForkNum              int                  `xorm:"fork_number int notnull default '0'"`
	TxRate               float64              `xorm:"tx_rate double notnull default '0'"`
	TxCount              int                  `xorm:"tx_count int notnull default '0'"`
	TxValueSum           float64              `xorm:"tx_value_sum Decimal(30,0) notnull default '0'"`
	TxValueAvg           float64              `xorm:"tx_value_avg Decimal(30,4) notnull default '0'"`
	TxGasLimitAvg        float64              `xorm:"tx_gaslimit_avg double notnull default '0'"`
	TxGasUsedAvg         float64              `xorm:"tx_gasused_avg double notnull default '0'"`
	TxGasPriceAvg        float64              `xorm:"tx_gasprice_avg double notnull default '0'"`
	Miner                int                  `xorm:"miner int  notnull default '0'"`
	NewMiner             int                  `xorm:"new_miner int  notnull default '0'"`
	TotalMinerUpToNow    int64                `xorm:"total_miner_uptonow bigint notnull default '0'"`
	TxFeeAvg             float64              `xorm:"tx_fee_avg decimal(30,4) notnull default '0'"`
	BlockGaslimitAvg     float64              `xorm:"block_gaslimit_avg double notnull default '0'"`
	BlockGasusedAvg      float64              `xorm:"block_gasused_avg double notnull default '0'"`
	BlockFeeAvg          float64              `xorm:"block_fee_avg double notnull default '0'"`
	BlockRewardAvg       float64              `xorm:"block_reward_avg double notnull default '0'"`
	ReferenceUncleReward float64              `xorm:"block_reference_rwd double notnull default '0'"`
	UncleGasLimitAvg     float64              `xorm:"uncle_gaslimit_avg double notnull default '0'"`
	UncleGasusedAvg      float64              `xorm:"uncle_gasused_avg double notnull default '0'"`
	UncleRewardAvg       float64              `xorm:"uncle_reward_avg double notnull default '0'"`
	NewAddressCount      int64                `xorm:"new_address_count int  notnull default '0'"`
	TotalAddressUpToNow  int64                `xorm:"total_address_uptonow int  notnull default '0'"`
	ActiveAddressCount   int64                `xorm:"active_address_count int  notnull default '0'"`
	TxAddressCntAvg      float64              `xorm:"tx_address_cnt_avg double notnull default '0'"`
	TxAddressValueAvg    float64              `xorm:"tx_address_value_avg decimal(30,4) notnull default '0'"`
	TotalCoin            math.HexOrDecimal256 `xorm:"total_coin decimal(38,0) notnull default '0'"`
	Price                float64              `xorm:"price double notnull default '0'"`
	MarketValue          float64              `xorm:"market_value double notnull default '0'"`
	StoreRate            float64              `xorm:"store_rate double notnull default '0'"`
	RatioOfMarketValue   float64              `xorm:"ratio_of_market_value double notnull default '0'"`
	ContractTxNum        int64                `xorm:"contract_tx_num int  notnull default '0'"`
	//TxsizeAvg            float64              `xorm:"tx_size_avg double notnull default '0'"`
	//TxSizeFeeAveg        float64              `xorm:"tx_size_fee_avg double notnull default '0'"`
	//EXRate               float64 `xorm:"ex_rate double notnull default '0'"`
}

func (t StatisticsDay) TableName() string {
	return tableName("statistics_day")
}
