package eos

import (
	"fmt"
	"github.com/aminvici/bds/common/json"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/eos"
	"time"
)

func ParseBlock(data string) (*EOSBlockData, error) {
	startTime := time.Now()

	b := new(EOSBlockData)
	b.Block = new(model.Block)
	b.Transactions = make([]*model.Transaction, 0)
	b.Actions = make([]*model.Action, 0)

	b.Block.BlockNum = json.Get(data, "block_num").Int()
	b.Block.Hash = json.Get(data, "id").String()
	b.Block.Previous = json.Get(data, "previous").String()
	b.Block.TimestampISO = json.Get(data, "timestamp").String()
	t, _ := time.ParseInLocation(TimeYmdHmssFormatISO, b.Block.TimestampISO, time.UTC)
	b.Block.Timestamp = t.Local().Unix()
	b.Block.ProducerSignature = json.Get(data, "producer_signature").String()
	b.Block.Producer = json.Get(data, "producer").String()

	b.Block.NewProducers = json.Get(data, "new_producers").Raw
	b.Block.TransactionMRoot = json.Get(data, "transaction_mroot").String()
	b.Block.ActionMRoot = json.Get(data, "action_mroot").String()
	b.Block.ScheduleVersion = json.Get(data, "schedule_version").Int()
	b.Block.RefBlockPrefix = json.Get(data, "ref_block_prefix").Int()
	b.Block.Confirmed = json.Get(data, "confirmed").Int()

	txList := json.Get(data, "transactions").Array()
	b.Block.TransactionLen = int64(len(txList))
	for _, txItem := range txList {
		transaction := new(model.Transaction)
		transaction.Status = json.Get(txItem.String(), "status").String()
		transaction.CpuUsageUs = json.Get(txItem.String(), "cpu_usage_us").Int()
		transaction.NetUsageWords = json.Get(txItem.String(), "net_usage_words").Int()
		transaction.BlockNum = b.Block.BlockNum
		if json.Get(txItem.String(), "trx").Type == json.String {
			transaction.Hash = json.Get(txItem.String(), "trx").String()
		} else {
			transaction.Hash = json.Get(txItem.String(), "trx.id").String()
			transaction.Compression = json.Get(txItem.String(), "trx.compression").String()
			transaction.Expiration = json.Get(txItem.String(), "trx.transaction.expiration").String()
			transaction.DelaySec = json.Get(txItem.String(), "trx.transaction.delay_sec").Int()
			transaction.MaxCpuUsageMs = json.Get(txItem.String(), "trx.transaction.max_cpu_usage_ms").Int()
			transaction.MaxNetUsageWords = json.Get(txItem.String(), "trx.transaction.max_net_usage_words").Int()
			transaction.PackedContextFreeData = json.Get(txItem.String(), "trx.packed_context_free_data").String()
			transaction.PackedTrx = json.Get(txItem.String(), "trx.packed_trx").String()
			transaction.RefBlockNum = json.Get(txItem.String(), "trx.transaction.ref_block_num").Int()
			transaction.RefBlockPrefix = json.Get(txItem.String(), "trx.transaction.ref_block_prefix").Int()
			actionList := json.Get(txItem.String(), "trx.transaction.actions").Array()
			for _, actionItem := range actionList {
				action := new(model.Action)
				action.TransactionHash = transaction.Hash
				action.BlockNum = b.Block.BlockNum
				action.Account = json.Get(actionItem.String(), "account").String()
				action.Name = json.Get(actionItem.String(), "name").String()
				action.Authorization = json.Get(actionItem.String(), "authorization").Raw
				action.Data = json.Get(actionItem.String(), "data").Raw
				action.HexData = json.Get(actionItem.String(), "hex_data").String()

				//log.Debug("action---%v", action)
				b.Actions = append(b.Actions, action)
			}
		}

		b.Transactions = append(b.Transactions, transaction)
	}

	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eos: parse block %d, txs %d, actions %d, elasped time %s", b.Block.BlockNum, b.Block.TransactionLen, len(b.Actions), elaspedTime.String())

	return b, nil
}

func revertBlock(num int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("DELETE from eos_block WHERE block_num = %d", num)
	affected, err := tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eos: revert block %d from eos_block table affected %d elasped %s", num, affected, elaspedTime.String())
	return nil
}

func revertTransaction(num int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("delete from eos_transaction where block_num = %d", num)
	affected, err := tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eos: revert block %d from eos_transaction table affected %d elasped %s", num, affected, elaspedTime.String())
	return nil
}

func revertAction(num int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("delete from eos_action where block_num = %d", num)
	affected, err := tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eos: revert block %d from eos_action table affected %d elasped %s", num, affected, elaspedTime.String())
	return nil
}
