package eth

import (
	"fmt"
	"github.com/aminvici/bds/common/json"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/common/math"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/eth"
	"math/big"
	"strings"
	"time"
)

//parser block
func ParseBlock(data string) (*ETHBlockData, error) {
	startTime := time.Now()
	var err error

	b := new(ETHBlockData)
	b.Block = new(model.Block)
	b.Uncles = make([]*model.Uncle, 0)
	b.Transactions = make([]*model.Transaction, 0)
	b.InternalTransactions = make([]*model.InternalTransaction, 0)
	b.TokenTransactions = make([]*model.TokenTransaction, 0)
	b.Tokens = make([]*model.Token, 0)
	b.ENSes = make([]*model.ENS, 0)

	b.Accounts = make([]*model.Account, 0)
	b.TokenAccounts = make([]*model.TokenAccount, 0)

	b.Block.Height = json.Get(data, "block.height").Int()
	b.Block.Timestamp = json.Get(data, "block.timestamp").Int()
	b.Block.ParentHash = removeHexPrefixAndToLower(json.Get(data, "block.parent_hash").String())
	b.Block.SHA3Uncles = removeHexPrefixAndToLower(json.Get(data, "block.sha_3_uncles").String())
	b.Block.Miner = removeHexPrefixAndToLower(json.Get(data, "block.miner").String())

	if poolName, ok := poolNameMap.Load(b.Block.Miner); ok {
		b.Block.PoolName = poolName.(string)
	} else {
		b.Block.PoolName = ""
	}
	b.Block.ExtraData = removeHexPrefixAndToLower(json.Get(data, "block.extra_data").String())
	b.Block.LogsBloom = removeHexPrefixAndToLower(json.Get(data, "block.logs_bloom").String())
	b.Block.TransactionRoot = removeHexPrefixAndToLower(json.Get(data, "block.transaction_root").String())
	b.Block.StateRoot = removeHexPrefixAndToLower(json.Get(data, "block.state_root").String())
	b.Block.ReceiptsRoot = removeHexPrefixAndToLower(json.Get(data, "block.receipts_root").String())
	b.Block.GasUsed = json.Get(data, "block.gas_used").Int()
	b.Block.GasLimit = json.Get(data, "block.gas_limit").Int()
	b.Block.Nonce = removeHexPrefixAndToLower(json.Get(data, "block.nonce").String())
	b.Block.MixHash = removeHexPrefixAndToLower(json.Get(data, "block.mix_hash").String())
	b.Block.Hash = removeHexPrefixAndToLower(json.Get(data, "block.hash").String())
	difficulty := json.Get(data, "block.difficulty").String()
	b.Block.Difficulty, err = parseBigInt(difficulty)
	if err != nil {
		log.Error("splitter eth: block %d difficulty '%s' parse error", b.Block.Height, difficulty)
		return nil, err
	}
	totalDifficulty := json.Get(data, "block.total_difficulty").String()
	b.Block.TotalDifficulty, err = parseBigInt(totalDifficulty)
	if err != nil {
		log.Error("splitter eth: block %d total difficulty '%s' parse error", b.Block.Height, totalDifficulty)
		return nil, err
	}
	b.Block.Size = json.Get(data, "block.size").Int()

	reward := json.Get(data, "block.block_reward").String()
	b.Block.Reward, err = parseBigInt(reward)
	log.Debug("splitter eth: block %d reward string is '%s'", b.Block.Height, reward)
	if err != nil {
		log.Error("splitter eth: block %d reward '%s' parse error", b.Block.Height, reward)
		return nil, err
	}

	referenceReward := json.Get(data, "block.reference_reward").String()
	b.Block.ReferenceReward, err = parseBigInt(referenceReward)
	if err != nil {
		log.Error("splitter eth: block %d reference reward '%s' parse error", b.Block.Height, referenceReward)
		return nil, err
	}
	b.Block.RealDifficulty = 0

	//parser miner
	accountMap := make(map[string]*model.Account, 0)
	minerAccount := new(model.Account)
	minerAccount.Address = b.Block.Miner
	minerBalance := json.Get(data, "block.miner_balance").String()
	minerAccount.Balance, err = parseBigInt(minerBalance)
	if err != nil {
		log.Error("splitter eth: block %d miner balance '%s' parse error", b.Block.Height, minerBalance)
		return nil, err
	}
	minerAccount.Type = AccountTypeMiner
	minerAccount.BirthTimestamp = b.Block.Timestamp
	minerAccount.LastActiveTimestamp = b.Block.Timestamp
	minerAccount.MinerCount = 1
	accountMap[minerAccount.Address] = minerAccount

	//parser uncle
	uncleList := json.Get(data, "uncles").Array()
	for _, uncleItem := range uncleList {
		uncle := new(model.Uncle)
		uncle.Height = json.Get(uncleItem.String(), "height").Int()
		uncle.Hash = removeHexPrefixAndToLower(json.Get(uncleItem.String(), "hash").String())
		uncle.BlockHeight = b.Block.Height
		uncle.ParentHash = removeHexPrefixAndToLower(json.Get(uncleItem.String(), "parent_hash").String())
		uncle.SHA3Uncles = removeHexPrefixAndToLower(json.Get(uncleItem.String(), "sha_3_uncles").String())
		uncle.Nonce = removeHexPrefixAndToLower(json.Get(uncleItem.String(), "nonce").String())
		uncle.MixHash = removeHexPrefixAndToLower(json.Get(uncleItem.String(), "mix_hash").String())
		uncle.Miner = strings.ToLower(removeHexPrefixAndToLower(json.Get(uncleItem.String(), "miner").String()))

		if poolName, ok := poolNameMap.Load(uncle.Miner); ok {
			uncle.PoolName = poolName.(string)
		} else {
			uncle.PoolName = ""
		}

		uncle.Timestamp = json.Get(uncleItem.String(), "timestamp").Int()
		uncle.ExtraData = removeHexPrefixAndToLower(json.Get(uncleItem.String(), "extra_data").String())
		uncle.LogsBloom = removeHexPrefixAndToLower(json.Get(uncleItem.String(), "logs_bloom").String())
		uncle.TransactionRoot = removeHexPrefixAndToLower(json.Get(uncleItem.String(), "transaction_root").String())
		uncle.StateRoot = removeHexPrefixAndToLower(json.Get(uncleItem.String(), "state_root").String())
		uncle.ReceiptsRoot = removeHexPrefixAndToLower(json.Get(uncleItem.String(), "receipts_root").String())
		uncle.GasUsed = json.Get(uncleItem.String(), "gas_used").Int()
		uncle.GasLimit = json.Get(uncleItem.String(), "gas_limit").Int()

		difficulty := json.Get(uncleItem.String(), "difficulty").String()
		uncle.Difficulty, err = parseBigInt(difficulty)
		if err != nil {
			log.Error("splitter eth: block %d uncle difficulty '%s' parse error", b.Block.Height, difficulty)
			return nil, err
		}

		totalDifficulty := json.Get(uncleItem.String(), "total_difficulty").String()
		uncle.TotalDifficulty, err = parseBigInt(totalDifficulty)
		if err != nil {
			log.Error("splitter eth: block %d uncle total difficulty '%s' parse error", b.Block.Height, totalDifficulty)
			return nil, err
		}

		uncle.Size = json.Get(uncleItem.String(), "size").Int()
		uncle.UncleLen = 0
		uncle.TxLen = 0

		uncleReward := json.Get(uncleItem.String(), "reward").String()
		uncle.Reward, err = parseBigInt(uncleReward)
		if err != nil {
			log.Error("splitter eth: block %d uncle reward '%s' parse error", b.Block.Height, uncleReward)
			return nil, err
		}

		b.Uncles = append(b.Uncles, uncle)

		// account
		minerAccount := new(model.Account)
		minerAccount.Address = uncle.Miner
		uncleMinerBalance := json.Get(uncleItem.String(), "miner_balance").String()
		minerAccount.Balance, err = parseBigInt(uncleMinerBalance)
		if err != nil {
			log.Error("splitter eth: block %d uncle miner balance '%s' parse error", b.Block.Height, uncleMinerBalance)
			return nil, err
		}
		minerAccount.Type = AccountTypeMiner
		minerAccount.BirthTimestamp = uncle.Timestamp
		minerAccount.LastActiveTimestamp = uncle.Timestamp
		minerAccount.MinerUncleCount = 1
		if _, ok := accountMap[minerAccount.Address]; ok {
			accountMap[minerAccount.Address].Balance = minerAccount.Balance
			accountMap[minerAccount.Address].LastActiveTimestamp = minerAccount.LastActiveTimestamp
			accountMap[minerAccount.Address].MinerUncleCount += minerAccount.MinerUncleCount
		} else {
			accountMap[minerAccount.Address] = minerAccount
		}
	}

	//parser transaction
	contractTransactCount := 0
	txList := json.Get(data, "transactions").Array()
	for _, txItem := range txList {
		transaction := new(model.Transaction)
		transaction.Hash = removeHexPrefixAndToLower(json.Get(txItem.String(), "hash").String())
		transaction.BlockHeight = json.Get(txItem.String(), "block_height").Int()
		transaction.From = removeHexPrefixAndToLower(json.Get(txItem.String(), "from").String())
		transaction.To = removeHexPrefixAndToLower(json.Get(txItem.String(), "to").String())
		value := json.Get(txItem.String(), "value").String()
		transaction.Value, err = parseBigInt(value)
		if err != nil {
			log.Error("splitter eth: block %d transaction %s value '%s' parse error", b.Block.Height, transaction.Hash, value)
			return nil, err
		}
		transaction.GasNumber = json.Get(txItem.String(), "gas_number").Int()
		transaction.GasPrice = json.Get(txItem.String(), "gas_price").Int()
		transaction.Nonce = int(json.Get(txItem.String(), "nonce").Int())
		transaction.V = removeHexPrefixAndToLower(json.Get(txItem.String(), "v").String())
		transaction.R = removeHexPrefixAndToLower(json.Get(txItem.String(), "r").String())
		transaction.S = removeHexPrefixAndToLower(json.Get(txItem.String(), "s").String())
		transaction.Timestamp = json.Get(txItem.String(), "timestamp").Int()
		transaction.TransactionBlockIndex = int(json.Get(txItem.String(), "tx_block_index").Int())
		transaction.Status = uint(json.Get(txItem.String(), "status").Int())
		transaction.Type = int(json.Get(txItem.String(), "type").Int())
		transaction.CumulativeGasUsed = json.Get(txItem.String(), "cumulative_gas_used").Int()
		transaction.GasUsed = json.Get(txItem.String(), "gas_used").Int()
		transaction.ContractAddress = removeHexPrefixAndToLower(json.Get(txItem.String(), "contract_address").String())
		transaction.LogsBloom = removeHexPrefixAndToLower(json.Get(txItem.String(), "logs_bloom").String())
		transaction.Root = removeHexPrefixAndToLower(json.Get(txItem.String(), "root").String())
		transaction.LogLen = int(json.Get(txItem.String(), "log_len").Int())
		transaction.TransactionSize = int(json.Get(txItem.String(), "tx_size").Int())

		fromAccount := new(model.Account)
		fromAccount.Address = transaction.From
		fromBalance := json.Get(txItem.String(), "from_balance").String()
		fromAccount.Balance, err = parseBigInt(fromBalance)
		if err != nil {
			log.Error("splitter eth: block %d transaction %s from balance '%s' parse error", b.Block.Height, transaction.Hash, fromBalance)
			return nil, err
		}
		fromAccount.Type = AccountTypeNormal
		fromAccount.BirthTimestamp = transaction.Timestamp
		fromAccount.LastActiveTimestamp = transaction.Timestamp
		if _, ok := accountMap[fromAccount.Address]; ok {
			accountMap[fromAccount.Address].Balance = fromAccount.Balance
			accountMap[fromAccount.Address].LastActiveTimestamp = fromAccount.LastActiveTimestamp
		} else {
			accountMap[fromAccount.Address] = fromAccount
		}

		toAccount := new(model.Account)
		if len(transaction.ContractAddress) > 0 {
			if !contractAddressFilter.Lookup([]byte(transaction.ContractAddress)) {
				contractAddressFilter.Insert([]byte(transaction.ContractAddress))
			}
			contractTransactCount += 1
			toAccount.Address = transaction.ContractAddress

			transaction.Type = TransactionTypeContract
		} else {
			if contractAddressFilter.Lookup([]byte(transaction.To)) {
				contractTransactCount += 1
				transaction.Type = TransactionTypeContract
			} else {
				transaction.Type = TransactionTypeNormal
			}
			toAccount.Address = transaction.To
		}

		toBalance := json.Get(txItem.String(), "to_balance").String()
		toAccount.Balance, err = parseBigInt(toBalance)
		if err != nil {
			log.Error("splitter eth: block %d transaction %s to balance '%s' parse error", b.Block.Height, transaction.Hash, toBalance)
			return nil, err
		}
		toAccount.Type = AccountTypeNormal
		toAccount.BirthTimestamp = transaction.Timestamp
		toAccount.LastActiveTimestamp = transaction.Timestamp
		if _, ok := accountMap[toAccount.Address]; ok {
			accountMap[toAccount.Address].Balance = toAccount.Balance
			accountMap[toAccount.Address].LastActiveTimestamp = toAccount.LastActiveTimestamp
		} else {
			accountMap[toAccount.Address] = toAccount
		}

		b.Transactions = append(b.Transactions, transaction)
	}

	//parser internal transaction
	internalTxList := json.Get(data, "internal_transactions").Array()
	for _, txItem := range internalTxList {
		transaction := new(model.InternalTransaction)
		transaction.Hash = removeHexPrefixAndToLower(json.Get(txItem.String(), "hash").String())
		transaction.BlockHeight = json.Get(txItem.String(), "block_height").Int()
		transaction.Timestamp = json.Get(txItem.String(), "timestamp").Int()
		transaction.Type = json.Get(txItem.String(), "type").String()
		transaction.From = removeHexPrefixAndToLower(json.Get(txItem.String(), "from").String())
		transaction.To = removeHexPrefixAndToLower(json.Get(txItem.String(), "to").String())
		transaction.TransactionIndex = json.Get(txItem.String(), "tx_index").Int()
		transaction.InternalTransactionIndex = json.Get(txItem.String(), "internal_tx_index").Int()

		switch transaction.Type {
		case "SELFDESTRUCT":
			transaction.Value = math.HexOrDecimal256(*big.NewInt(0))
			transaction.GasLimit = 0
			transaction.GasUsed = 0
		case "DELEGATECALL", "STATICCALL":
			transaction.Value = math.HexOrDecimal256(*big.NewInt(0))
			transaction.GasLimit = json.Get(txItem.String(), "gas").Int()
			transaction.GasUsed = json.Get(txItem.String(), "gas_used").Int()
		default:
			value := json.Get(txItem.String(), "value").String()
			transaction.Value, err = parseBigInt(value)
			if err != nil {
				log.Error("splitter eth: block %d internal transaction %s value '%s' parse error", b.Block.Height, transaction.Hash, value)
				return nil, err
			}
			transaction.GasLimit = json.Get(txItem.String(), "gas").Int()
			transaction.GasUsed = json.Get(txItem.String(), "gas_used").Int()
		}

		if transaction.Type == "CREATE" {
			if !contractAddressFilter.Lookup([]byte(transaction.To)) {
				contractAddressFilter.Insert([]byte(transaction.To))
			}
		}

		if transaction.GasUsed != 0 && transaction.GasLimit != 0 {
			fromAccount := new(model.Account)
			fromAccount.Address = transaction.From
			fromBalance := json.Get(txItem.String(), "from_balance").String()
			fromAccount.Balance, err = parseBigInt(fromBalance)
			if err != nil {
				log.Error("splitter eth: block %d internal transaction %s from balance '%s' parse error", b.Block.Height, transaction.Hash, fromBalance)
				return nil, err
			}
			fromAccount.Type = AccountTypeNormal
			fromAccount.HasInternalTransaction = 1
			fromAccount.BirthTimestamp = transaction.Timestamp
			fromAccount.LastActiveTimestamp = transaction.Timestamp
			if _, ok := accountMap[fromAccount.Address]; ok {
				accountMap[fromAccount.Address].Balance = fromAccount.Balance
				accountMap[fromAccount.Address].LastActiveTimestamp = fromAccount.LastActiveTimestamp
			} else {
				accountMap[fromAccount.Address] = fromAccount
			}

			if len(transaction.To) > 0 {
				toAccount := new(model.Account)
				toAccount.Address = transaction.To
				toBalance := json.Get(txItem.String(), "to_balance").String()
				toAccount.Balance, err = parseBigInt(toBalance)
				if err != nil {
					log.Error("splitter eth: block %d internal transaction %s to balance '%s' parse error", b.Block.Height, transaction.Hash, toBalance)
					return nil, err
				}
				toAccount.Type = AccountTypeNormal
				toAccount.HasInternalTransaction = 1
				toAccount.BirthTimestamp = transaction.Timestamp
				toAccount.LastActiveTimestamp = transaction.Timestamp
				if transaction.Type == "CREATE" {
					toAccount.Creator = transaction.From
					toAccount.Type = AccountTypeContract
				}
				toAccount.LastActiveTimestamp = transaction.Timestamp
				if _, ok := accountMap[toAccount.Address]; ok {
					accountMap[toAccount.Address].Balance = fromAccount.Balance
					accountMap[toAccount.Address].LastActiveTimestamp = fromAccount.LastActiveTimestamp
				} else {
					accountMap[toAccount.Address] = toAccount
				}
			}
		}

		b.InternalTransactions = append(b.InternalTransactions, transaction)
	}

	//parser token transaction
	tokenAccountMap := make(map[string]*model.TokenAccount, 0)
	tokenTxList := json.Get(data, "token_transactions").Array()
	for _, txItem := range tokenTxList {
		transaction := new(model.TokenTransaction)
		transaction.BlockHeight = json.Get(txItem.String(), "block_height").Int()
		transaction.Timestamp = json.Get(txItem.String(), "timestamp").Int()
		transaction.TokenAddress = removeHexPrefixAndToLower(json.Get(txItem.String(), "token_address").String())
		transaction.ParentTransactionHash = removeHexPrefixAndToLower(json.Get(txItem.String(), "parent_tx_hash").String())
		transaction.ParentTransactionIndex = json.Get(txItem.String(), "parent_tx_index").Int()
		transaction.From = removeHexPrefixAndToLower(json.Get(txItem.String(), "from").String())
		transaction.To = removeHexPrefixAndToLower(json.Get(txItem.String(), "to").String())
		value := json.Get(txItem.String(), "value").String()
		transaction.Value, err = parseBigInt(value)
		if err != nil {
			log.Error("splitter eth: block %d token transaction %s value '%s' parse error", b.Block.Height, transaction.ParentTransactionHash, value)
			return nil, err
		}

		transaction.TokenAddress = removeHexPrefixAndToLower(json.Get(txItem.String(), "token_address").String())
		transaction.LogIndex = json.Get(txItem.String(), "log_index").Int()
		transaction.IsRemoved = json.Get(txItem.String(), "is_removed").Bool()

		fromAccount := new(model.TokenAccount)
		fromAccount.TokenAddress = transaction.TokenAddress
		fromAccount.Address = transaction.From
		fromBalance := json.Get(txItem.String(), "from_balance").String()
		fromAccount.Balance, err = parseBigInt(fromBalance)
		if err != nil {
			log.Error("splitter eth: block %d token transaction %s from balance '%s' parse error", b.Block.Height, transaction.ParentTransactionHash, fromBalance)
			return nil, err
		}
		fromAccount.BirthTimestamp = transaction.Timestamp
		fromAccount.LastActiveTimestamp = transaction.Timestamp

		key := strings.ToLower(fmt.Sprintf("%s_%s", transaction.TokenAddress, transaction.From))
		if _, ok := tokenAccountMap[key]; ok {
			tokenAccountMap[key].Balance = fromAccount.Balance
			tokenAccountMap[key].LastActiveTimestamp = fromAccount.LastActiveTimestamp
		} else {
			tokenAccountMap[key] = fromAccount
		}

		toAccount := new(model.TokenAccount)
		toAccount.TokenAddress = transaction.TokenAddress
		toAccount.Address = transaction.To
		toBalance := json.Get(txItem.String(), "to_balance").String()
		toAccount.Balance, err = parseBigInt(toBalance)
		if err != nil {
			log.Error("splitter eth: block %d token transaction %s to balance '%s' parse error", b.Block.Height, transaction.ParentTransactionHash, toBalance)
			return nil, err
		}
		toAccount.BirthTimestamp = transaction.Timestamp
		toAccount.LastActiveTimestamp = transaction.Timestamp

		key = strings.ToLower(fmt.Sprintf("%s_%s", transaction.TokenAddress, transaction.To))
		if _, ok := tokenAccountMap[key]; ok {
			tokenAccountMap[key].Balance = toAccount.Balance
			tokenAccountMap[key].LastActiveTimestamp = toAccount.LastActiveTimestamp
		} else {
			tokenAccountMap[key] = toAccount
		}

		b.TokenTransactions = append(b.TokenTransactions, transaction)
	}

	//parser token
	tokenList := json.Get(data, "tokens").Array()
	for _, tokenItem := range tokenList {
		token := new(model.Token)
		token.TokenAddress = removeHexPrefixAndToLower(json.Get(tokenItem.String(), "token_address").String())
		token.DecimalLength = json.Get(tokenItem.String(), "decimal_len").Int()
		token.Name = strings.Replace(json.Get(tokenItem.String(), "name").String(), "'", "''", -1)
		token.Symbol = json.Get(tokenItem.String(), "symbol").String()

		token.TotalSupply = json.Get(tokenItem.String(), "total_supply").String()
		//totalSupply := json.Get(tokenItem.String(), "total_supply").String()
		//token.TotalSupply, err = parseBigInt(totalSupply)
		//if err != nil {
		//	log.Error("splitter eth: block %d token %s total supply '%s' parse error", b.Block.Height, token.TokenAddress, totalSupply)
		//	return nil, err
		//}

		token.Owner = removeHexPrefixAndToLower(json.Get(tokenItem.String(), "owner").String())
		//token.Timestamp = json.Get(tokenItem.String(), "timestamp").Int()
		token.Timestamp = b.Block.Timestamp

		b.Tokens = append(b.Tokens, token)
	}

	//parser ens
	ensList := json.Get(data, "enses").Array()
	for _, ensItem := range ensList {
		ens := new(model.ENS)
		ens.Timestamp = json.Get(ensItem.String(), "timestamp").Int()
		ens.Hash = removeHexPrefixAndToLower(json.Get(ensItem.String(), "hash").String())
		ens.BlockHeight = json.Get(ensItem.String(), "block_height").Int()
		ens.TransactionBlockIndex = int(json.Get(ensItem.String(), "tx_block_index").Int())
		ens.LabelHash = removeHexPrefixAndToLower(json.Get(ensItem.String(), "label_hash").String())
		ens.From = removeHexPrefixAndToLower(json.Get(ensItem.String(), "from").String())
		ens.To = removeHexPrefixAndToLower(json.Get(ensItem.String(), "to").String())
		ens.FunctionType = json.Get(ensItem.String(), "function_type").String()
		ens.RegistrationDate = json.Get(ensItem.String(), "registration_date").Int()
		ens.Bidder = json.Get(ensItem.String(), "bidder").String()
		deposit := json.Get(ensItem.String(), "deposit").String()
		ens.Deposit, err = parseBigInt(deposit)
		if err != nil {
			log.Error("splitter eth: block %d ens deposit '%s' parse error", b.Block.Height, deposit)
			return nil, err
		}

		value := json.Get(ensItem.String(), "value").String()
		ens.Value, err = parseBigInt(value)
		if err != nil {
			log.Error("splitter eth: block %d ens %s value '%s' parse error", b.Block.Height, ens.Hash, value)
			return nil, err
		}

		ens.RegistrationDate = json.Get(ensItem.String(), "registration_date").Int()
		ens.Owner = removeHexPrefixAndToLower(json.Get(ensItem.String(), "owner").String())

		b.ENSes = append(b.ENSes, ens)
	}

	for _, v := range accountMap {
		b.Accounts = append(b.Accounts, v)
	}

	for _, v := range tokenAccountMap {
		b.TokenAccounts = append(b.TokenAccounts, v)
	}

	b.Block.TransactionLength = len(b.Transactions)
	b.Block.UncleLength = len(b.Uncles)
	b.Block.ContractTransactionLen = contractTransactCount

	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eth: parse block %d, txs %d, elasped time %s", b.Block.Height, b.Block.TransactionLength, elaspedTime.String())

	return b, nil
}

//revert miner count by height
func revertMiner(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	index := "revert_miner"
	sql := fmt.Sprintf("UPDATE a SET a.miner_count = a.miner_count - 1 FROM eth_account a"+
		" JOIN (SELECT miner FROM eth_block WHERE height = '%d') b"+
		" ON a.address = b.miner ", height)
	affected1, err := tx.Execute(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	sql = fmt.Sprintf("UPDATE a SET a.miner_uncle_count = a.miner_uncle_count - 1 FROM eth_account a"+
		" JOIN (SELECT miner FROM eth_block WHERE height = '%d') b"+
		" ON a.address = b.miner ", height)
	affected2, err := tx.Execute(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eth index: %s affected %d %d elasped %s", index, affected1, affected2, elaspedTime.String())
	return nil
}

//revert account balance by height
func revertAccountBalance(height int64, tx *service.Transaction, handler *rpcHandler) error {
	startTime := time.Now()
	index := "revert_account"
	sql := fmt.Sprintf("SELECT t.address FROM"+
		" (SELECT DISTINCT(miner) AS address FROM eth_block WHERE height = '%d' "+
		" UNION SELECT DISTINCT(miner) AS address FROM eth_uncle WHERE block_height = '%d'"+
		" UNION SELECT DISTINCT([from]) AS adddress FROM eth_transaction WHERE block_height = '%d'"+
		" UNION SELECT DISTINCT([to]) AS adddress FROM eth_transaction WHERE block_height = '%d'"+
		" UNION SELECT DISTINCT([from]) AS adddress FROM eth_internal_transaction WHERE block_height = '%d'"+
		" UNION SELECT DISTINCT([to]) AS adddress FROM eth_internal_transaction WHERE [to] != '' AND block_height = '%d'"+
		" UNION SELECT contract_address AS adddress FROM eth_transaction WHERE block_height = '%d' AND contract_address != '') t ",
		height, height, height, height, height, height, height)
	result, err := tx.QueryString(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	accountBalance := make(map[string]string, 0)

	if len(result) > 0 {
		for _, v := range result {
			address := v["address"]
			if strings.TrimSpace(address) != "" {
				balance, err := handler.GetBalance(address, height)
				if err != nil {
					log.DetailError(err)
					return err
				}
				accountBalance[address] = balance.String()
			}
		}
	}
	updateSql := fmt.Sprintf("UPDATE eth_account SET")
	balanceSql := fmt.Sprintf(" balance = case address")
	updateSql1 := fmt.Sprintf(" end WHERE address IN (")

	var b1, in1 string
	batch := 0
	totalAffected1 := int64(0)
	if len(accountBalance) > 0 {
		for address, balance := range accountBalance {
			b := fmt.Sprintf(" WHEN '%s' THEN '%s'", address, balance)
			b1 = b1 + b
			in := fmt.Sprintf(" '%s',", address)
			in1 = in1 + in
			batch++
			if batch%100 == 0 {
				if len(in1) > 0 {
					lenI := len(in1) - 1
					sql := updateSql + balanceSql + b1 + updateSql1 + in1[0:lenI] + ")"
					affected, err := tx.Execute(sql)
					if err != nil {
						log.DetailError(err)
						return err
					}
					b1 = ""
					in1 = ""
					totalAffected1 += affected
				}
			}
		}
	}
	affected2 := int64(0)
	if len(accountBalance) > 0 {
		if len(in1) > 0 {
			lenI := len(in1) - 1
			sql := updateSql + balanceSql + b1 + updateSql1 + in1[0:lenI] + ")"
			affected2, err = tx.Execute(sql)
			if err != nil {
				log.DetailError(err)
				return err
			}
			b1 = ""
			in1 = ""
		}
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eth index: %s affected %d %d elasped %s", index, totalAffected1, affected2, elaspedTime.String())
	return nil
}

//revert token account balance by height
func revertTokenAccount(height int64, tx *service.Transaction, handler *rpcHandler) error {
	startTime := time.Now()
	index := "revert_token_account"
	notIN := fmt.Sprintf("SELECT t.address, t.token_address FROM"+
		" (SELECT [from] AS address, token_address FROM eth_token_transaction WHERE block_height = '%d'"+
		" UNION SELECT [to] AS address, token_address FROM eth_token_transaction WHERE block_height = '%d') t ",
		height, height)
	result, err := tx.QueryString(notIN)
	if err != nil {
		log.DetailError(err)
		return err
	}
	tokenAccounts := make([]*model.TokenAccount, 0)
	if len(result) > 0 {
		for _, v := range result {
			tokenAccount := new(model.TokenAccount)
			address := v["address"]
			tokenAddress := v["token_address"]
			value, err := handler.GetTokenBalance(tokenAddress, address, height)
			if err != nil {
				log.DetailError(err)
				return err
			}
			tokenAccount.Balance = math.HexOrDecimal256(*value)
			tokenAccount.Address = address
			tokenAccount.TokenAddress = tokenAddress
			tokenAccounts = append(tokenAccounts, tokenAccount)
		}
	}

	batchUpdateSQL0 := fmt.Sprintf("UPDATE a SET a.balance = b.balance,a.address=b.address,a.token_address = b.token_address FROM eth_token_account a INNER JOIN (")
	batchUpdateSQL3 := fmt.Sprintf("UPDATE a SET a.balance = b.balance,a.address=b.address,a.token_address = b.token_address FROM eth_token_account a INNER JOIN (")
	batchUpdateSQL1 := fmt.Sprintf(")b ON a.address = b.address and a.token_address = b.token_address")
	count := 0
	if len(tokenAccounts) > 0 {
		for i := 0; i < len(tokenAccounts); i++ {
			tmpB := big.Int(tokenAccounts[i].Balance)
			batchUpdateSql2 := fmt.Sprintf("SELECT '%s' AS balance, '%s' AS address, '%s' AS token_address", tmpB.String(), tokenAccounts[i].Address, tokenAccounts[i].TokenAddress)
			batchUpdateSQL0 = batchUpdateSQL0 + batchUpdateSql2
			count++
		}
	}
	var finalSql string
	if count > 1 {
		temp := strings.Split(batchUpdateSQL0, "SELECT")
		for i := 0; i < len(temp); i++ {
			if i > 0 && i < len(temp)-1 {
				finalSql = finalSql + " " + "SELECT " + temp[i] + " UNION ALL "
			}
			if i == 0 {
				finalSql = finalSql + " " + temp[i]
			}
			if i == len(temp)-1 {
				finalSql = finalSql + " " + "SELECT " + temp[i]
			}
		}
	} else {
		finalSql = batchUpdateSQL0
	}
	affected := int64(0)
	if batchUpdateSQL0 != batchUpdateSQL3 {
		sql := finalSql + batchUpdateSQL1
		affected, err = tx.Execute(sql)
		if err != nil {
			log.DetailError(err)
			return err
		}
		finalSql = ""
		batchUpdateSQL0 = batchUpdateSQL3
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eth index: %s affected %d elasped %s", index, affected, elaspedTime.String())
	return nil
}

//revert block by height
func revertBlock(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("DELETE from eth_block WHERE height = %d", height)
	affected, err := tx.Execute(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eth: revert block %d from eth_block table affected %d elasped %s", height, affected, elaspedTime.String())
	return nil
}

//revert uncle by height
func revertUncle(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("DELETE from eth_uncle WHERE block_height = %d", height)
	affected, err := tx.Execute(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eth: revert block %d from eth_uncle table affected %d elasped %s", height, affected, elaspedTime.String())
	return nil
}

//revert transaction
func revertTransaction(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("delete from eth_transaction where block_height = %d", height)
	affected, err := tx.Execute(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eth: revert block %d from eth_transaction table affected %d elasped %s", height, affected, elaspedTime.String())
	return nil
}

//revert token transaction
func revertTokenTransaction(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("DELETE FROM eth_token_transaction WHERE block_height = %d", height)
	affected, err := tx.Execute(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eth: revert block %d from eth_token_transaction table affected %d elasped %s", height, affected, elaspedTime.String())
	return nil
}

//revert ens
func revertENS(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("DELETE FROM eth_ens WHERE block_height = %d", height)
	affected, err := tx.Execute(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eth: revert block %d from eth_ens table affected %d elasped %s", height, affected, elaspedTime.String())
	return nil
}

//revert internal transaction
func revertInternalTransaction(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("DELETE FROM eth_internal_transaction WHERE block_height = %d", height)
	affected, err := tx.Execute(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eth: revert block %d from eth_internal_transaction table affected %d elasped %s", height, affected, elaspedTime.String())
	return nil
}

//update token and toke account
func updateToken(data *ETHBlockData, tx *service.Transaction) error {
	var totalInsertAffected, totalDeleteAffected int64
	for _, v := range data.Tokens {
		token := new(model.Token)
		token.TokenAddress = v.TokenAddress
		has, err := tx.Get(token)
		if err != nil {
			log.DetailError(err)
			return err
		}
		if !has {
			affected, err := tx.Insert(v)
			if err != nil {
				log.DetailError(err)
				return err
			}
			totalInsertAffected += affected
		}
	}
	log.Debug("splitter eth: block %d token affected %d", data.Block.Height, totalInsertAffected)

	totalInsertAffected = int64(0)
	for _, v := range data.TokenAccounts {
		tokenAccount := new(model.TokenAccount)
		tokenAccount.TokenAddress = v.TokenAddress
		tokenAccount.Address = v.Address
		has, err := tx.Get(tokenAccount)
		if err != nil {
			log.DetailError(err)
			return err
		}
		if has {
			affected, err := tx.Delete(tokenAccount)
			if err != nil {
				log.DetailError(err)
				return err
			}
			totalDeleteAffected += affected
		} else {
			tokenAccount.BirthTimestamp = v.BirthTimestamp
		}

		tokenAccount.ID = 0
		tokenAccount.Balance = v.Balance
		tokenAccount.LastActiveTimestamp = v.LastActiveTimestamp
		affected, err := tx.Insert(tokenAccount)
		if err != nil {
			log.DetailError(err)
			return err
		}
		totalInsertAffected += affected
	}

	log.Debug("splitter eth: block %d token account affected %d:%d", data.Block.Height, totalInsertAffected, totalDeleteAffected)
	return nil
}

//update account
func updateAccount(data *ETHBlockData, tx *service.Transaction) error {
	var totalInsertAffected, totalDeleteAffected int64
	for _, v := range data.Accounts {
		account := new(model.Account)
		account.Address = v.Address
		has, err := tx.Get(account)
		if err != nil {
			log.DetailError(err)
			return err
		}
		if has {
			affected, err := tx.Delete(account)
			if err != nil {
				log.DetailError(err)
				return err
			}
			totalDeleteAffected += affected
		} else {
			account.BirthTimestamp = v.BirthTimestamp
		}

		account.ID = 0
		account.HasInternalTransaction = v.HasInternalTransaction
		account.Balance = v.Balance
		account.MinerCount += v.MinerCount
		account.MinerUncleCount += v.MinerUncleCount
		if v.Creator != "" {
			account.Creator = v.Creator
		}
		account.LastActiveTimestamp = v.LastActiveTimestamp
		if account.MinerCount > 0 || account.MinerUncleCount > 0 {
			account.Type = AccountTypeMiner
		} else {
			account.Type = v.Type
		}

		affected, err := tx.Insert(account)
		if err != nil {
			log.DetailError(err)
			return err
		}
		totalInsertAffected += affected
	}
	log.Debug("splitter eth: block %d account update affected %d:%d", data.Block.Height, totalInsertAffected, totalDeleteAffected)

	return nil
}

//update real difficulty
func updateRealDifficulty(data *ETHBlockData, tx *service.Transaction) error {
	sql := fmt.Sprintf("UPDATE c SET real_difficulty=d.realdif"+
		" FROM eth_block c"+
		" JOIN (SELECT a.height AS height, (CAST(a.difficulty AS decimal(30,0)))*(a.timestamp-b.timestamp)/15 AS realdif"+
		" FROM eth_block AS a"+
		" JOIN eth_block AS b ON b.height+1=a.height WHERE a.height = '%d') d"+
		" ON c.height = d.height", data.Block.Height)
	affected, err := tx.Execute(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	log.Debug("splitter eth: block %d real difficulty affected %d", data.Block.Height, affected)
	return nil
}

func removeHexPrefixAndToLower(s string) string {
	return strings.ToLower(strings.TrimPrefix(s, "0x"))
}

func parseBigInt(s string) (math.HexOrDecimal256, error) {
	var n math.HexOrDecimal256
	if s == "0x" {
		s = "0x0"
	}

	v, ok := math.ParseBig256(s)
	if !ok {
		n = math.HexOrDecimal256(*defaultBigNumber)
	} else {
		if v.Cmp(maxBigNumber) >= 0 {
			n = math.HexOrDecimal256(*defaultBigNumber)
		} else {
			n = math.HexOrDecimal256(*v)
		}
	}
	return n, nil
}
