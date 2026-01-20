package doge

import (
	"errors"
	"fmt"
	"github.com/aminvici/bds/common/json"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/doge"
	"strconv"
	"strings"
	"time"
)

func ParseBlock(data string) (*DOGEBlockData, error) {
	startTime := time.Now()
	b := new(DOGEBlockData)
	b.Block = new(model.Block)
	b.Transactions = make([]*model.Transaction, 0)
	b.VIns = make([]*model.VIn, 0)
	b.VOuts = make([]*model.VOut, 0)

	b.Block.Height = json.Get(data, "height").Int()
	b.Block.Size = json.Get(data, "size").Int()
	b.Block.Timestamp = json.Get(data, "time").Int()
	b.Block.Version = json.Get(data, "version").Int()
	b.Block.MerkleRoot = json.Get(data, "merkle_root").String()
	b.Block.Bits = json.Get(data, "bits").String()
	b.Block.Nonce = json.Get(data, "nonce").Int()
	b.Block.Hash = json.Get(data, "hash").String()
	b.Block.StrippedSize = json.Get(data, "stripped_size").Int()
	b.Block.Weight = json.Get(data, "weight").Int()
	b.Block.MedianTimestamp = json.Get(data, "median_time").Int()
	b.Block.Difficulty = json.Get(data, "difficulty").Float()
	b.Block.PreviousHash = json.Get(data, "prev_hash").String()
	b.Block.ChainWork = json.Get(data, "chain_work").String()

	txItemList := json.Get(data, "tx").Array()
	for txN, txItem := range txItemList {
		tx := new(model.Transaction)
		tx.TxID = json.Get(txItem.String(), "tx_id").String()
		tx.Version = json.Get(txItem.String(), "version").Int()
		tx.Size = json.Get(txItem.String(), "size").Int()
		tx.VSize = json.Get(txItem.String(), "vsize").Int()
		tx.LockTime = json.Get(txItem.String(), "lock_time").Int()
		tx.Hash = json.Get(txItem.String(), "hash").String()
		tx.Weight = json.Get(txItem.String(), "weight").Int()
		tx.Number = int64(txN)

		vInItemList := json.Get(txItem.String(), "vin").Array()
		for vInN, vInItem := range vInItemList {
			vIn := new(model.VIn)
			vIn.Sequence = json.Get(vInItem.String(), "sequence").Int()
			vIn.Coinbase = json.Get(vInItem.String(), "coinbase").String()
			if vIn.Coinbase == "" {
				vIn.TxIDOrigin = json.Get(vInItem.String(), "tx_id_origin").String()
				vIn.VOutNumberOrigin = json.Get(vInItem.String(), "vout_num_origin").Int()
				vIn.ScriptSignature = json.Get(vInItem.String(), "script_sig").String()
				txInWitnessList := make([]string, 0)
				txInWitnessItemList := json.Get(vInItem.String(), "tx_in_witness").Array()
				for _, witnessItem := range txInWitnessItemList {
					txInWitnessList = append(txInWitnessList, witnessItem.String())
				}

				txInWitness := strings.Join(txInWitnessList, ",")
				if len(txInWitness) > 2000 {
					vIn.TxInWitness = ""
				} else {
					vIn.TxInWitness = txInWitness
				}
			}

			vIn.TxID = tx.TxID
			vIn.Number = int64(vInN)
			vIn.BlockHeight = b.Block.Height
			vIn.Timestamp = b.Block.Timestamp

			b.VIns = append(b.VIns, vIn)
		}
		tx.VInCount = len(vInItemList)

		vOutItemList := json.Get(txItem.String(), "vout").Array()
		for vOutN, vOutItem := range vOutItemList {
			vOut := new(model.VOut)
			vOut.Value = json.Get(vOutItem.String(), "value").Uint()
			vOut.ScriptPublicKey = json.Get(vOutItem.String(), "script_pubkey").String()
			vOut.RequiredSignatures = json.Get(vOutItem.String(), "required_signatures").Int()
			vOut.Type = json.Get(vOutItem.String(), "type").String()
			addresses := json.Get(vOutItem.String(), "addresses").Array()
			if len(addresses) == 1 {
				vOut.Address = addresses[0].String()
			}
			vOut.Number = int64(vOutN)

			vOut.TxID = tx.TxID
			vOut.BlockHeight = b.Block.Height
			vOut.Timestamp = b.Block.Timestamp
			if txN > 0 {
				vOut.IsCoinbase = 0
			} else {
				vOut.IsCoinbase = 1
			}
			b.VOuts = append(b.VOuts, vOut)

			tx.VOutValue += vOut.Value
		}
		tx.VOutCount = len(vOutItemList)

		tx.BlockHeight = b.Block.Height
		tx.Timestamp = b.Block.Timestamp
		b.Transactions = append(b.Transactions, tx)
	}

	b.Block.TxCount = int64(len(b.Transactions))
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter doge: parse block %d, txs %d, elasped time %s", b.Block.Height, b.Block.TxCount, elaspedTime.String())
	return b, nil
}

func updateTransactionVersion(tx *service.Transaction, txVersion []int64, data *DOGEBlockData) error {
	count := 0
	var sql, sqlUpdate, sqlUpdate2 string
	for k, v := range txVersion {
		id := data.Transactions[k].TxID
		if count == 0 {
			sqlUpdate = fmt.Sprintf("UPDATE doge_transaction SET version=CASE tx_id WHEN '%s' THEN %d", id, v)
			sqlUpdate2 = fmt.Sprintf(" END WHERE tx_id IN ('%s'", id)
			count++
		} else {
			sqlUpdate += fmt.Sprintf(" WHEN '%s' THEN %d", id, v)
			sqlUpdate2 += fmt.Sprintf(",'%s'", id)
			count++
			if count >= 1000 {
				count = 0
				sql = sqlUpdate + sqlUpdate2 + ")"
				_, err := tx.Exec(sql)
				if err != nil {
					return err
				}
			}
		}
	}
	if count != 0 {
		sql = sqlUpdate + sqlUpdate2 + ")"
		_, err := tx.Exec(sql)
		if err != nil {
			return err
		}
	}
	return nil
}

func UpdateBlock(data *DOGEBlockData, tx *service.Transaction) error {
	height := data.Block.Height
	err := updateVOutIsUsed(height, tx)
	if err != nil {
		log.Error("splitter doge index: %s calculation error", "UpdateSelectVInValueAndAddress")
		return err
	}
	err = updateAddressTable(data, tx)
	if err != nil {
		log.Error("splitter doge index: %s calculation error", "UpdateAddressTable")
		return err
	}
	err = updateCoinbaseAddressCount(height, tx)
	if err != nil {
		log.Error("splitter doge index: %s calculation error", "UpdateCoinbaseAddressCount")
		return err
	}
	return nil
}

//更新矿工挖矿数量
func updateCoinbaseAddressCount(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	index := "coinbase_address_count"
	log.Debug("calulate index: %s", index)
	sql := fmt.Sprintf("SELECT address FROM doge_vout WHERE block_height=%d AND is_coinbase=1", height)
	result, err := tx.QueryString(sql)
	if err != nil {
		return err
	}
	for _, v := range result {
		if v["address"] == "" {
			continue
		}
		getMiner := new(model.Mining)
		getMiner.Address = v["address"]
		has, err := tx.Get(getMiner)
		if err != nil {
			return err
		}
		if has {
			deleteMiner := new(model.Mining)
			deleteMiner.Address = v["address"]
			_, err = tx.Delete(deleteMiner)
			if err != nil {
				return err
			}
		}
		getMiner.ID = 0
		getMiner.CoinbaseTimes++
		Miners := make([]*model.Mining, 0)
		Miners = append(Miners, getMiner)
		_, err = tx.Insert(Miners)
		if err != nil {
			return err
		}
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter doge index: %s elasped %s", index, elaspedTime.String())
	return nil
}

func revertBlock(height int64, tx *service.Transaction) error {
	err := revertSelectVInValueAndAddress(height, tx)
	if err != nil {
		log.DetailError(err)
		return err
	}
	err = revertAddressTable(height, tx)
	if err != nil {
		log.DetailError(err)
		return err
	}
	err = revertCoinbaseAddressCount(height, tx)
	if err != nil {
		log.DetailError(err)
		return err
	}
	return nil
}

//更新vin表中的value和address,vout表中的isused
func revertSelectVInValueAndAddress(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	index := "revert_select_vin_value_and_address"
	sql := fmt.Sprintf("UPDATE doge_vout SET is_used=0 where id in (select b.id from doge_vin a INNER JOIN doge_vout b ON a.tx_id_origin=b.tx_id AND a.vout_num_origin=b.number AND a.block_height=%d)", height)
	affected, err := tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter doge index: %s affected %d elasped %s", index, affected, elaspedTime.String())
	return nil
}

//插入新的账户地址，更新旧地址的最后交易时间，重新计算余额
func revertAddressTable(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	index := "revert_address_table"
	sql := fmt.Sprintf("DELETE FROM doge_address WHERE birth_timestamp=(SELECT timestamp FROM doge_block WHERE height=%d)", height)
	affected1, err := tx.Execute(sql)
	if err != nil {
		return err
	}
	sql = fmt.Sprintf("select a.address address,a.value+b.value value FROM doge_address a JOIN (SELECT address_origin address, SUM(value_origin) value FROM doge_vin WHERE block_height=%d GROUP BY address_origin) b ON a.address=b.address", height)
	result1, err := tx.QueryString(sql)
	if err != nil {
		return err
	}
	for _, v := range result1 {
		sql := fmt.Sprintf("update doge_address set value=%s where address='%s'", v["value"], v["address"])
		_, err := tx.Exec(sql)
		if err != nil {
			return err
		}
	}
	sql = fmt.Sprintf("select a.address address,a.value-b.value value FROM doge_address a JOIN (SELECT address, SUM(value) value FROM doge_vout WHERE block_height=%d GROUP BY address) b ON a.address=b.address", height)
	result2, err := tx.QueryString(sql)
	if err != nil {
		return err
	}
	for _, v := range result2 {
		sql := fmt.Sprintf("update doge_address set value=%s where address='%s'", v["value"], v["address"])
		_, err := tx.Exec(sql)
		if err != nil {
			return err
		}
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter doge index: %s affected %d %d %d elasped %s", index, affected1, len(result1), len(result2), elaspedTime.String())
	return nil
}

//更新矿工挖矿数量
func revertCoinbaseAddressCount(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	index := "revert_coinbase_address_count"
	sql := fmt.Sprintf("SELECT address FROM doge_vout WHERE block_height=%d AND is_coinbase=1", height)
	result, err := tx.QueryString(sql)
	if err != nil {
		return err
	}
	totalAffected := int64(0)
	for _, v := range result {
		address := v["address"]
		sql = fmt.Sprintf("UPDATE doge_mining SET coinbase_times=coinbase_times-1 WHERE address='%s'", address)
		affected, err := tx.Execute(sql)
		if err != nil {
			return err
		}
		totalAffected += affected
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter doge index: %s affected %d elasped %s", index, totalAffected, elaspedTime.String())
	return nil
}

func updateVInAddressAndValue(tx *service.Transaction, data *DOGEBlockData) error {
	valueMap := make(map[string]int64)
	for k, v := range data.VIns {
		sql := fmt.Sprintf("select address,value from doge_vout where tx_id='%s' and number=%d", v.TxIDOrigin, v.VOutNumberOrigin)
		result, err := tx.QueryString(sql)
		if err != nil {
			return err
		}
		for _, value := range result {
			data.VIns[k].AddressOrigin = value["address"]
			data.VIns[k].ValueOrigin, _ = strconv.ParseInt(value["value"], 10, 64)
			valueMap[data.VIns[k].TxID] += data.VIns[k].ValueOrigin
		}
	}
	for k, v := range data.Transactions {
		if v.Number != 0 {
			data.Transactions[k].VInValue = valueMap[v.TxID]
			data.Transactions[k].Fee = data.Transactions[k].VInValue - int64(data.Transactions[k].VOutValue)
		}
	}
	return nil
}

func updateVOutIsUsed(height int64, tx *service.Transaction) error {
	vOut := make([]*model.VOut, 0)
	sql := fmt.Sprintf("select a.* from doge_vout as a inner join (select tx_id_origin,vout_num_origin from doge_vin where block_height=%d) as b on a.tx_id=b.tx_id_origin and a.number=b.vout_num_origin", height)
	result, err := tx.QueryString(sql)
	if err != nil {
		return err
	}
	for _, v := range result {
		newVOut := new(model.VOut)
		newVOut.TxID = v["tx_id"]
		newVOut.BlockHeight, _ = strconv.ParseInt(v["block_height"], 10, 64)
		newVOut.Value, _ = strconv.ParseUint(v["value"], 10, 64)
		newVOut.Address = v["address"]
		newVOut.Timestamp, _ = strconv.ParseInt(v["timestamp"], 10, 64)
		newVOut.ScriptPublicKey = v["script_pubkey"]
		newVOut.Type = v["type"]
		newVOut.RequiredSignatures, _ = strconv.ParseInt(v["required_signatures"], 10, 64)
		newVOut.Number, _ = strconv.ParseInt(v["number"], 10, 64)
		newVOut.IsUsed = 1
		newVOut.IsCoinbase, _ = strconv.ParseInt(v["is_coinbase"], 10, 64)
		vOut = append(vOut, newVOut)
	}
	sql = fmt.Sprintf("delete from doge_vout where id in (select a.id from doge_vout as a inner join (select tx_id_origin,vout_num_origin from doge_vin where block_height=%d) as b on a.tx_id=b.tx_id_origin and a.number=b.vout_num_origin)", height)
	_, err = tx.Exec(sql)
	if err != nil {
		return err
	}
	affected, err := tx.BatchInsert(vOut)
	if err != nil {
		return err
	}
	log.Debug("doge calculate index: insert vOut affected %d done", affected)
	return nil
}

//插入新的账户地址，更新旧地址的最后交易时间，重新计算余额
func updateAddressTable(data *DOGEBlockData, tx *service.Transaction) error {
	height := data.Block.Height
	index := "update_address_table"
	log.Debug("calulate index: %s", index)
	addressValueMap := make(map[string]int64)
	addressHas := make(map[string]int64)
	addressList := make([]*model.Address, 0)
	for _, v := range data.VOuts {
		addressValueMap[v.Address] += int64(v.Value)
	}
	for _, v := range data.VIns {
		addressValueMap[v.AddressOrigin] -= int64(v.ValueOrigin)
	}
	sql := fmt.Sprintf("SELECT * FROM doge_address where address in (SELECT address FROM doge_vout WHERE block_height=%d UNION SELECT address_origin FROM doge_vin WHERE block_height=%d)", height, height)
	result, err := tx.QueryString(sql)
	if err != nil {
		return err
	}
	for _, v := range result {
		addressInfo := new(model.Address)
		addressInfo.Address = v["address"]
		addressInfo.BirthTimestamp, _ = strconv.ParseInt(v["birth_timestamp"], 10, 64)
		addressInfo.LatestTxTimestamp = data.Block.Timestamp
		addressInfo.Value, _ = strconv.ParseInt(v["value"], 10, 64)
		addressInfo.Value += addressValueMap[addressInfo.Address]
		addressHas[addressInfo.Address] = 1
		addressList = append(addressList, addressInfo)
	}
	for k, v := range addressValueMap {
		if _, has := addressHas[k]; !has {
			addressInfo := new(model.Address)
			addressInfo.Address = k
			addressInfo.BirthTimestamp = data.Block.Timestamp
			addressInfo.LatestTxTimestamp = data.Block.Timestamp
			addressInfo.Value = v
			addressList = append(addressList, addressInfo)
		}
	}
	sql = fmt.Sprintf("DELETE FROM doge_address where address in (SELECT address FROM doge_vout WHERE block_height=%d UNION SELECT address_origin FROM doge_vin WHERE block_height=%d)", height, height)
	_, err = tx.Exec(sql)
	if err != nil {
		return err
	}
	for _, v := range addressList {
		if v.Value < 0 {
			return errors.New("Value < 0 ")
		}
	}
	_, err = tx.BatchInsert(addressList)
	if err != nil {
		return err
	}
	return nil
}

//更新标记block所属矿池
func GetBlockMiner(data *DOGEBlockData, tx *service.Transaction) error {
	miner := new(model.Mining)
	if len(data.VOuts) == 0 {
		data.Block.PoolName = "UNKNOW"
		return nil
	}
	miner.Address = data.VOuts[0].Address
	_, err := tx.Get(miner)
	if err != nil {
		return err
	}
	data.Block.PoolName = miner.PoolName
	return nil
}
