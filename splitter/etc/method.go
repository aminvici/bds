package etc

import (
	"errors"
	"fmt"
	"github.com/aminvici/bds/common/json"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/common/math"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/etc"
	"math/big"
	"strings"
	"time"
)

func ParseBlock(data string) (*ETCBlockData, error) {
	startTime := time.Now()
	var err error
	b := new(ETCBlockData)
	b.Block = new(model.Block)
	b.Uncles = make([]*model.Uncle, 0)
	b.Transactions = make([]*model.Transaction, 0)
	b.Tokens = make([]*model.Token, 0)
	b.TokenTransactions = make([]*model.TokenTransaction, 0)
	b.Accounts = make([]*model.Account, 0)
	b.TokenAccounts = make([]*model.TokenAccount, 0)

	b.Block.Height = json.Get(data, "block.height").Int()
	b.Block.Hash = removeHexPrefix(json.Get(data, "block.hash").String())
	b.Block.ParentHash = removeHexPrefix(json.Get(data, "block.parent_hash").String())
	b.Block.SHA3Uncles = removeHexPrefix(json.Get(data, "block.sha_3_uncles").String())
	b.Block.Nonce = removeHexPrefix(json.Get(data, "block.nonce").String())
	b.Block.MixHash = removeHexPrefix(json.Get(data, "block.mix_hash").String())
	b.Block.Miner = removeHexPrefix(json.Get(data, "block.miner").String())

	if poolName, ok := poolNameMap.Load(b.Block.Miner); ok {
		b.Block.PoolName = poolName.(string)
	} else {
		b.Block.PoolName = ""
	}

	b.Block.Timestamp = json.Get(data, "block.timestamp").Int()
	b.Block.ExtraData = removeHexPrefix(json.Get(data, "block.extra_data").String())
	b.Block.LogsBloom = removeHexPrefix(json.Get(data, "block.logs_bloom").String())
	b.Block.TransactionRoot = removeHexPrefix(json.Get(data, "block.transaction_root").String())
	b.Block.StateRoot = removeHexPrefix(json.Get(data, "block.state_root").String())
	b.Block.ReceiptsRoot = removeHexPrefix(json.Get(data, "block.receipts_root").String())
	b.Block.GasUsed = json.Get(data, "block.gas_used").Int()
	b.Block.GasLimit = json.Get(data, "block.gas_limit").Int()
	difficulty := json.Get(data, "block.difficulty").String()
	b.Block.Difficulty, err = parseBigInt(difficulty)
	if err != nil {
		log.Error("splitter etc: block %d difficulty '%s' parse error", b.Block.Height, difficulty)
		return nil, err
	}
	totalDifficulty := json.Get(data, "block.total_difficulty").String()
	b.Block.TotalDifficulty, err = parseBigInt(totalDifficulty)
	if err != nil {
		log.Error("splitter etc: block %d total difficulty '%s' parse error", b.Block.Height, totalDifficulty)
		return nil, err
	}
	b.Block.RealDifficulty = 0
	b.Block.Size = json.Get(data, "block.size").Int()
	b.Block.BlockReward = math.HexOrDecimal256(*big.NewInt(json.Get(data, "block.block_reward").Int()))
	b.Block.BlockUncleReward = uint64(json.Get(data, "block.block_uncle_reward").Int())

	minerAccount := new(model.Account)
	minerAccount.Address = b.Block.Miner
	minerAccount.Balance = math.HexOrDecimal256(*big.NewInt(json.Get(data, "block.miner_balance").Int()))
	b.Accounts = append(b.Accounts, minerAccount)

	uncleList := json.Get(data, "uncles").Array()
	for _, uncleItem := range uncleList {
		uncle := new(model.Uncle)
		uncle.Height = json.Get(uncleItem.String(), "height").Int()
		uncle.Hash = removeHexPrefix(json.Get(uncleItem.String(), "hash").String())
		uncle.BlockHeight = b.Block.Height
		uncle.ParentHash = removeHexPrefix(json.Get(uncleItem.String(), "parent_hash").String())
		uncle.Sha3uncles = removeHexPrefix(json.Get(uncleItem.String(), "sha_3_uncles").String())
		uncle.Nonce = removeHexPrefix(json.Get(uncleItem.String(), "nonce").String())
		uncle.MixHash = removeHexPrefix(json.Get(uncleItem.String(), "mix_hash").String())
		uncle.Miner = removeHexPrefix(json.Get(uncleItem.String(), "miner").String())

		if poolName, ok := poolNameMap.Load(uncle.Miner); ok {
			uncle.PoolName = poolName.(string)
		} else {
			uncle.PoolName = ""
		}

		uncle.Timestamp = json.Get(uncleItem.String(), "timestamp").Int()
		uncle.ExtraData = removeHexPrefix(json.Get(uncleItem.String(), "extra_data").String())
		uncle.LogsBloom = removeHexPrefix(json.Get(uncleItem.String(), "logs_bloom").String())
		uncle.TransactionRoot = removeHexPrefix(json.Get(uncleItem.String(), "transaction_root").String())
		uncle.StateRoot = removeHexPrefix(json.Get(uncleItem.String(), "state_root").String())
		uncle.ReceiptsRoot = removeHexPrefix(json.Get(uncleItem.String(), "receipts_root").String())
		uncle.GasUsed = json.Get(uncleItem.String(), "gas_used").Int()
		uncle.GasLimit = json.Get(uncleItem.String(), "gas_limit").Int()
		// TODO: uncle index ?
		uncle.Difficulty = json.Get(uncleItem.String(), "difficulty").Int()
		if uncle.Difficulty > 5703497331004136 || uncle.Difficulty < 0 {
			log.Warn("splitter etc: block %d incorrent uncle difficulty %d", b.Block.Height, uncle.Difficulty)
			continue
		}

		totalDifficulty := json.Get(uncleItem.String(), "total_difficulty").String()
		t, err := math.ParseInt256(totalDifficulty)
		if err != nil {
			log.Warn("splitter etc: block %d incorrent uncle total difficulty %s", b.Block.Height, totalDifficulty)
			continue
		}

		uncle.TotalDifficulty = math.HexOrDecimal256(*t)
		uncle.Size = json.Get(uncleItem.String(), "size").Int()
		uncle.UncleLen = 0
		uncle.Reward = uint64(json.Get(uncleItem.String(), "reward").Int())

		b.Uncles = append(b.Uncles, uncle)

		// account
		minerAccount := new(model.Account)
		minerAccount.Address = uncle.Miner
		minerAccount.Balance = math.HexOrDecimal256(*big.NewInt(json.Get(uncleItem.String(), "miner_balance").Int()))
		b.Accounts = append(b.Accounts, minerAccount)
	}

	contractTransactCount := 0
	txList := json.Get(data, "transactions").Array()
	for _, txItem := range txList {
		transaction := new(model.Transaction)
		transaction.Hash = removeHexPrefix(json.Get(txItem.String(), "hash").String())
		transaction.BlockHeight = json.Get(txItem.String(), "block_height").Int()
		transaction.From = removeHexPrefix(json.Get(txItem.String(), "from").String())
		transaction.To = removeHexPrefix(json.Get(txItem.String(), "to").String())
		transaction.ContractAddress = removeHexPrefix(json.Get(txItem.String(), "contract_address").String())
		transaction.Value = math.HexOrDecimal256(*big.NewInt(json.Get(txItem.String(), "block_height").Int()))
		transaction.Timestamp = json.Get(txItem.String(), "timestamp").Int()
		transaction.Gas = json.Get(txItem.String(), "gas").Int()
		transaction.GasPrice = json.Get(txItem.String(), "gas_price").Int()
		transaction.GasUsed = json.Get(txItem.String(), "gas_used").Int()
		transaction.CumulativeGasUsed = json.Get(txItem.String(), "cumulative_gas_used").Int()
		transaction.Nonce = int(json.Get(txItem.String(), "nonce").Int())
		transaction.TransactionBlockIndex = int(json.Get(txItem.String(), "tx_block_index").Int())
		transaction.Status = uint(json.Get(txItem.String(), "status").Int())
		transaction.Type = int(json.Get(txItem.String(), "type").Int())
		transaction.Root = removeHexPrefix(json.Get(txItem.String(), "root").String())
		// TODO: chain id
		transaction.LogLen = int(json.Get(txItem.String(), "log_len").Int())
		// TODO: replay protected

		fromAccount := new(model.Account)
		fromAccount.Address = transaction.From
		fromAccount.Balance = math.HexOrDecimal256(*big.NewInt(json.Get(txItem.String(), "from_balance").Int()))
		b.Accounts = append(b.Accounts, fromAccount)

		toAccount := new(model.Account)
		if len(transaction.ContractAddress) > 0 {
			if !contractAddressFilter.Lookup([]byte(transaction.ContractAddress)) {
				contractAddressFilter.Insert([]byte(transaction.ContractAddress))
			}
			contractTransactCount += 1
			toAccount.Address = transaction.ContractAddress
		} else {
			if contractAddressFilter.Lookup([]byte(transaction.To)) {
				contractTransactCount += 1
			}
			toAccount.Address = transaction.To
		}

		toAccount.Balance = math.HexOrDecimal256(*big.NewInt(json.Get(txItem.String(), "to_balance").Int()))
		b.Accounts = append(b.Accounts, toAccount)

		b.Transactions = append(b.Transactions, transaction)
	}

	tokenAccountMap := make(map[string]*model.TokenAccount, 0)
	tokenTxList := json.Get(data, "token_transactions").Array()
	for _, txItem := range tokenTxList {
		tokenTransaction := new(model.TokenTransaction)
		tokenTransaction.BlockHeight = json.Get(txItem.String(), "block_height").Int()
		tokenTransaction.ParentTransactionHash = removeHexPrefix(json.Get(txItem.String(), "parent_tx_hash").String())
		tokenTransaction.ParentTransactionIndex = json.Get(txItem.String(), "parent_tx_index").Int()
		tokenTransaction.From = removeHexPrefix(json.Get(txItem.String(), "from").String())
		tokenTransaction.To = removeHexPrefix(json.Get(txItem.String(), "to").String())
		value := json.Get(txItem.String(), "value").String()
		tokenTransaction.Value, err = parseBigInt(value)
		if err != nil {
			log.Error("splitter etc: block %d token transaction %s value '%s' parse error", b.Block.Height, tokenTransaction.ParentTransactionHash, value)
			return nil, err
		}
		tokenTransaction.Timestamp = json.Get(txItem.String(), "timestamp").Int()
		tokenTransaction.TokenAddress = removeHexPrefix(json.Get(txItem.String(), "token_address").String())
		tokenTransaction.LogIndex = json.Get(txItem.String(), "log_index").Int()

		fromAccount := new(model.TokenAccount)
		fromAccount.TokenAddress = tokenTransaction.TokenAddress
		fromAccount.Address = tokenTransaction.From
		fromBalance := json.Get(txItem.String(), "from_balance").String()
		fromAccount.Balance, err = parseBigInt(fromBalance)
		if err != nil {
			log.Error("splitter etc: block %d token transaction %s from balance '%s' parse error", b.Block.Height, tokenTransaction.ParentTransactionHash, fromBalance)
			return nil, err
		}
		fromAccount.BirthTimestamp = tokenTransaction.Timestamp
		fromAccount.LastActiveTimestamp = tokenTransaction.Timestamp

		key := strings.ToLower(fmt.Sprintf("%s_%s", tokenTransaction.TokenAddress, tokenTransaction.From))
		if _, ok := tokenAccountMap[key]; ok {
			tokenAccountMap[key].Balance = fromAccount.Balance
			tokenAccountMap[key].LastActiveTimestamp = fromAccount.LastActiveTimestamp
		} else {
			tokenAccountMap[key] = fromAccount
		}

		toAccount := new(model.TokenAccount)
		toAccount.TokenAddress = tokenTransaction.TokenAddress
		toAccount.Address = tokenTransaction.To
		toBalance := json.Get(txItem.String(), "to_balance").String()
		toAccount.Balance, err = parseBigInt(toBalance)
		if err != nil {
			log.Error("splitter etc: block %d token transaction %s to balance '%s' parse error", b.Block.Height, tokenTransaction.ParentTransactionHash, toBalance)
			return nil, err
		}
		toAccount.BirthTimestamp = tokenTransaction.Timestamp
		toAccount.LastActiveTimestamp = tokenTransaction.Timestamp

		key = strings.ToLower(fmt.Sprintf("%s_%s", tokenTransaction.TokenAddress, tokenTransaction.To))
		if _, ok := tokenAccountMap[key]; ok {
			tokenAccountMap[key].Balance = toAccount.Balance
			tokenAccountMap[key].LastActiveTimestamp = toAccount.LastActiveTimestamp
		} else {
			tokenAccountMap[key] = toAccount
		}

		// TODO: token balance
		b.TokenTransactions = append(b.TokenTransactions, tokenTransaction)
	}

	tokenList := json.Get(data, "tokens").Array()
	for _, tokenItem := range tokenList {
		token := new(model.Token)
		token.TokenAddress = removeHexPrefixAndToLower(json.Get(tokenItem.String(), "token_address").String())
		token.DecimalLength = json.Get(tokenItem.String(), "decimal_len").Int()
		token.Name = strings.Replace(json.Get(tokenItem.String(), "name").String(), "'", "''", -1)
		token.Symbol = json.Get(tokenItem.String(), "symbol").String()

		totalSupply := json.Get(tokenItem.String(), "total_supply").String()
		token.TotalSupply = totalSupply
		//if err != nil {
		//	log.Error("splitter etc: block %d token %s total supply '%s' parse error", b.Block.Height, token.TokenAddress, totalSupply)
		//	return nil, err
		//}

		token.Owner = removeHexPrefixAndToLower(json.Get(tokenItem.String(), "owner").String())
		//token.Timestamp = json.Get(tokenItem.String(), "timestamp").Int()
		token.Timestamp = b.Block.Timestamp

		b.Tokens = append(b.Tokens, token)
	}

	// count ?
	for _, v := range tokenAccountMap {
		b.TokenAccounts = append(b.TokenAccounts, v)
	}

	b.Block.TransactionLen = len(b.Transactions)
	b.Block.UncleLen = len(b.Uncles)
	b.Block.ContractTransactionLen = contractTransactCount

	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter etc: parse block %d, txs %d, elasped time %s", b.Block.Height, b.Block.TransactionLen, elaspedTime.String())

	return b, nil
}

func revertMiner(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	index := "revert_miner"
	sql := fmt.Sprintf("UPDATE a SET a.miner_count = a.miner_count - 1 FROM etc_account a"+
		" JOIN (SELECT miner FROM etc_block WHERE height = '%d') b"+
		" ON a.address = b.miner ", height)
	affected1, err := tx.Execute(sql)
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("UPDATE a SET a.miner_uncle_count = a.miner_uncle_count - 1 FROM etc_account a"+
		" JOIN (SELECT miner FROM etc_block WHERE height = '%d') b"+
		" ON a.address = b.miner ", height)
	affected2, err := tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter etc index: %s affected %d %d elasped %s", index, affected1, affected2, elaspedTime.String())
	return nil
}

func revertAccountBalance(height int64, tx *service.Transaction, handler *rpcHandler) error {
	startTime := time.Now()
	index := "revert_account"
	sql := fmt.Sprintf("SELECT t.address FROM"+
		" (SELECT DISTINCT(miner) AS address FROM etc_block WHERE height = '%d' "+
		" UNION SELECT DISTINCT(miner) AS address FROM etc_uncle WHERE block_height = '%d'"+
		" UNION SELECT DISTINCT([from]) AS adddress FROM etc_transaction WHERE block_height = '%d'"+
		" UNION SELECT DISTINCT([to]) AS adddress FROM etc_transaction WHERE block_height = '%d'"+
		" UNION SELECT contract_address AS adddress FROM etc_transaction WHERE block_height = '%d' AND contract_address != '') t ",
		height, height, height, height, height)
	result, err := tx.QueryString(sql)
	if err != nil {
		return err
	}
	accountBalance := make(map[string]string, 0)

	if len(result) > 0 {
		for _, v := range result {
			address := v["address"]
			if strings.TrimSpace(address) != "" {
				balance, err := handler.GetBalance(address, height)
				if err != nil {
					return err
				}
				accountBalance[address] = balance.String()
			}
		}
	}
	updateSql := fmt.Sprintf("UPDATE etc_account SET")
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
	log.Debug("splitter etc index: %s affected %d %d elasped %s", index, totalAffected1, affected2, elaspedTime.String())
	return nil
}

func revertBlock(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("DELETE from etc_block WHERE height = %d", height)
	affected, err := tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter etc: revert block %d from etc_block table affected %d elasped %s", height, affected, elaspedTime.String())
	return nil
}

func revertUncle(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("DELETE from etc_uncle WHERE block_height = %d", height)
	affected, err := tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter etc: revert block %d from etc_uncle table affected %d elasped %s", height, affected, elaspedTime.String())
	return nil
}

func revertTransaction(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("delete from etc_transaction where block_height = %d", height)
	affected, err := tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter etc: revert block %d from etc_transaction table affected %d elasped %s", height, affected, elaspedTime.String())
	return nil
}

func updatePoolName(data *ETCBlockData, tx *service.Transaction) error {
	poolNameMap := make(map[string]string, 0)
	sql := fmt.Sprintf("SELECT address, name FROM etc_pool_name")
	result, err := tx.QueryString(sql)
	if err != nil {
		return err
	}

	for _, v := range result {
		address := strings.ToLower(v["address"])
		name := v["name"]
		poolNameMap[address] = name
	}

	if _, ok := poolNameMap[data.Block.Miner]; ok {
		name := poolNameMap[data.Block.Miner]
		address := data.Block.Miner
		sql := fmt.Sprintf("UPDATE etc_block SET pool_name = '%s' WHERE miner = '%s' AND height = '%d'", name, address, data.Block.Height)
		affected, err := tx.Execute(sql)
		if err != nil {
			return err
		}
		log.Debug("splitter etc index: block %d pool_name update affected %d", data.Block.Height, affected)
	}

	for i := 0; i < len(data.Uncles); i++ {
		if _, ok := poolNameMap[data.Uncles[i].Miner]; ok {
			name := poolNameMap[data.Uncles[i].Miner]
			address := data.Uncles[i].Miner
			sql := fmt.Sprintf("UPDATE etc_uncle SET pool_name = '%s' WHERE miner = '%s' AND block_height = '%d'", name, address, data.Block.Height)
			affected, err := tx.Execute(sql)
			if err != nil {
				return err
			}
			log.Debug("splitter etc index: block %d uncle block pool_name update affected %d", data.Block.Height, affected)
		}
	}

	return nil
}

func updateToken(data *ETCBlockData, tx *service.Transaction) error {
	var totalInsertAffected, totalDeleteAffected int64
	tokenList := make([]*model.Token, 0)
	for _, v := range data.Tokens {
		token := new(model.Token)
		token.TokenAddress = v.TokenAddress
		has, err := tx.Get(token)
		if err != nil {
			log.DetailError(err)
			return err
		}
		if !has {
			tokenList = append(tokenList, v)
			totalInsertAffected += 1
		}
	}
	if len(tokenList) > 0 {
		_, err := tx.Insert(tokenList)
		if err != nil {
			log.DetailError(err)
			return err
		}
	}
	log.Debug("splitter etc: block %d token affected %d", data.Block.Height, totalInsertAffected)

	totalInsertAffected = int64(0)
	tokenAccountList := make([]*model.TokenAccount, 0)
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
		tokenAccountList = append(tokenAccountList, tokenAccount)
		totalInsertAffected += 1
	}
	if len(tokenAccountList) > 0 {
		_, err := tx.Insert(tokenAccountList)
		if err != nil {
			log.DetailError(err)
			return err
		}
	}
	log.Debug("splitter etc: block %d token account affected %d:%d", data.Block.Height, totalInsertAffected, totalDeleteAffected)
	return nil
}

func updateAccount(data *ETCBlockData, tx *service.Transaction) error {
	var totalInsertAffected, totalDeleteAffected int64
	accountList := make([]*model.Account, 0)
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
			account.BirthTimestamp = data.Block.Timestamp
		}

		account.ID = 0
		account.Balance = v.Balance
		account.MinerCount += v.MinerCount
		account.MinerUncleCount += v.MinerUncleCount
		if v.Creator != "" {
			account.Creator = v.Creator
		}
		account.LastActiveTimestamp = data.Block.Timestamp
		if account.MinerCount > 0 || account.MinerUncleCount > 0 {
			account.Type = AccountTypeMiner
		} else {
			account.Type = v.Type
		}
		if account.BirthTimestamp == 0 || account.LastActiveTimestamp == 0 {
			log.DetailError("BirthTimestamp = %d , LastActiveTimestamp = %d", account.BirthTimestamp, account.LastActiveTimestamp)
			return errors.New("BirthTimestamp or LastActiveTimestamp is 0")
		}
		accountList = append(accountList, account)
		totalInsertAffected += 1
	}
	if len(accountList) > 0 {
		_, err := tx.Insert(accountList)
		if err != nil {
			log.DetailError(err)
			return err
		}
	}
	log.Debug("splitter etc: block %d account update affected %d:%d", data.Block.Height, totalInsertAffected, totalDeleteAffected)

	return nil
}

func updateTransactionType(data *ETCBlockData, tx *service.Transaction) error {
	sql := fmt.Sprintf("UPDATE a SET type = 1 FROM etc_transaction a WHERE a.[to] != ''"+
		" AND (a.[to] IN (SELECT address FROM etc_account"+
		" WHERE type = 1 OR type = 11)) AND block_height = '%d'", data.Block.Height)
	affected, err := tx.Execute(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	log.Debug("splitter etc: block %d transaction type affected %d", data.Block.Height, affected)
	return nil
}
func updateRealDifficulty(data *ETCBlockData, tx *service.Transaction) error {
	sql := fmt.Sprintf("UPDATE c SET real_difficulty=d.realdif"+
		" FROM etc_block c"+
		" JOIN (SELECT a.height AS height, (CAST(a.difficulty AS decimal(30,0)))*(a.timestamp-b.timestamp)/15 AS realdif"+
		" FROM etc_block AS a"+
		" JOIN etc_block AS b ON b.height+1=a.height WHERE a.height = '%d') d"+
		" ON c.height = d.height", data.Block.Height)
	affected, err := tx.Execute(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	log.Debug("splitter etc: block %d real difficulty affected %d", data.Block.Height, affected)
	return nil
}

func removeHexPrefix(s string) string {
	return strings.TrimPrefix(s, "0x")
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
