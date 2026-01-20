package xlm

import (
	"github.com/aminvici/bds/common/json"
	"github.com/aminvici/bds/common/log"
	model "github.com/aminvici/bds/service/model/xlm"
	"time"
)

func ParseLedger(data string) ([]*XLMLedgerData, error) {
	b := make([]*XLMLedgerData, 0)
	if json.Get(data, "records").Exists() {
		valueItemList := json.Get(data, "records").Array()
		for _, value := range valueItemList {
			ledger := json.Get(value.String(), "value")
			return parse(ledger.String())
		}
	} else {
		return parse(data)

	}
	return b, nil
}

func parse(ledger string) ([]*XLMLedgerData, error) {
	startTime := time.Now()
	b := make([]*XLMLedgerData, 0)
	x := new(XLMLedgerData)
	x.Ledger = new(model.Ledger)
	x.Transactions = make([]*model.Transaction, 0)
	x.Operations = make([]*model.Operation, 0)
	x.Ledger.LedgerID = json.Get(ledger, "id").String()
	x.Ledger.PagingToken = json.Get(ledger, "paging_token").String()
	x.Ledger.LedgerHash = json.Get(ledger, "hash").String()
	x.Ledger.PreviousLedgerHash = json.Get(ledger, "prev_hash").String()
	x.Ledger.Sequence = json.Get(ledger, "sequence").Int()
	x.Ledger.TransactionCount = json.Get(ledger, "transaction_count").Int()
	x.Ledger.SuccessfulTransactionCount = json.Get(ledger, "successful_transaction_count").Int()
	x.Ledger.FailedTransactionCount = json.Get(ledger, "failed_transaction_count").Int()
	x.Ledger.OperationCount = json.Get(ledger, "operation_count").Int()
	t, _ := time.ParseInLocation("2006-01-02T15:04:05Z", json.Get(ledger, "closed_at").String(), time.UTC)
	x.Ledger.ClosedTime = t.Unix()
	x.Ledger.TotalCoins = json.Get(ledger, "total_coins").String()
	x.Ledger.FeePool = json.Get(ledger, "fee_pool").String()
	x.Ledger.BaseFeeInStroops = json.Get(ledger, "base_fee_in_stroops").Int()
	x.Ledger.BaseReserveInStroops = json.Get(ledger, "base_reserve_in_stroops").Int()
	x.Ledger.MaxTxSetSize = json.Get(ledger, "max_tx_set_size").Int()
	x.Ledger.ProtocolVersion = json.Get(ledger, "protocol_version").Int()

	txItemList := json.Get(ledger, "transactions").Array()
	for _, txItem := range txItemList {
		tx := new(model.Transaction)
		tx.TransactionID = json.Get(txItem.String(), "id").String()
		tx.PagingToken = json.Get(txItem.String(), "paging_token").String()
		tx.TransactionHash = json.Get(txItem.String(), "hash").String()
		tx.LedgerSequence = json.Get(txItem.String(), "ledger").Int()
		tx.SourceAccount = json.Get(txItem.String(), "source_account").String()
		tx.SourceAccountSequence = json.Get(txItem.String(), "source_account_sequence").String()
		tx.FeePaid = json.Get(txItem.String(), "fee_paid").Int()
		tx.OperationCount = json.Get(txItem.String(), "operation_count").Int()
		tx.MemoType = json.Get(txItem.String(), "memo_type").String()
		tx.Signatures = json.Get(txItem.String(), "signatures").String()
		x.Transactions = append(x.Transactions, tx)

		opItemList := json.Get(txItem.String(), "operations").Array()
		for _, opItem := range opItemList {
			op := new(model.Operation)
			op.TransactionID = json.Get(opItem.String(), "transaction_id").String()
			op.OperationID = json.Get(opItem.String(), "operation_id").String()
			op.ApplicationOrder = json.Get(opItem.String(), "application_order").Int()
			op.Type = json.Get(opItem.String(), "type").String()
			op.Detail = json.Get(opItem.String(), "detail").String()
			op.SourceAccount = json.Get(opItem.String(), "source_account").String()
			x.Operations = append(x.Operations, op)
		}
	}
	b = append(b, x)
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter xlm: parse ledger %d, txs %d, operations %d ,elasped time %s", x.Ledger.Sequence, len(x.Transactions), len(x.Operations), elaspedTime.String())
	return b, nil

}
