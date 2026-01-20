package xrp

import (
	"fmt"
	"github.com/aminvici/bds/common/json"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/common/math"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/xrp"
	"strings"
	"time"
)

func FormatAmount(transaction *model.Transaction, input string) *model.Amount {
	amount := new(model.Amount)
	amount.LedgerIndex = transaction.LedgerIndex
	amount.CloseTime = transaction.CloseTime
	amount.ParentHash = transaction.Hash
	amount.Currency = json.Get(input, "currency").String()
	amount.Issuer = json.Get(input, "issuer").String()
	amount.Value = json.Get(input, "value").String()
	return amount
}
func ParseBlock(data string) (*XRPBlockData, error) {
	startTime := time.Now()
	var err error

	b := new(XRPBlockData)
	b.Block = new(model.Block)
	b.Transactions = make([]*model.Transaction, 0)
	b.AffectedNodes = make([]*model.AffectedNodes, 0)
	b.Paths = make([]*model.Path, 0)
	b.Amounts = make([]*model.Amount, 0)

	b.Block.Accepted = int(json.Get(data, "accepted").Int())
	b.Block.AccountHash = json.Get(data, "account_hash").String()
	b.Block.CloseFlags = int(json.Get(data, "close_flags").Int())
	b.Block.CloseTime = json.Get(data, "close_time").Int()
	b.Block.CloseTimeHuman = json.Get(data, "close_time_human").String()
	b.Block.CloseTimeResolution = int(json.Get(data, "close_time_resolution").Int())
	b.Block.Closed = int(json.Get(data, "closed").Int())
	b.Block.Hash = json.Get(data, "hash").String()
	b.Block.LedgerHash = json.Get(data, "ledger_hash").String()
	b.Block.LedgerIndex = json.Get(data, "ledger_index").Int()
	b.Block.ParentCloseTime = json.Get(data, "parent_close_time").Int()
	b.Block.ParentHash = json.Get(data, "parent_hash").String()
	b.Block.SeqNum = json.Get(data, "seqNum").Int()
	totalCoins := json.Get(data, "total_coins").String()
	b.Block.TotalCoins, err = parseBigInt(totalCoins)
	if err != nil {
		log.Error("splitter xrp: block %d TotalCoins '%s' parse error", b.Block.LedgerIndex, totalCoins)
		return nil, err
	}
	b.Block.TransactionHash = json.Get(data, "transaction_hash").String()

	txList := json.Get(data, "transactions").Array()
	for _, txItem := range txList {
		transaction := new(model.Transaction)
		transaction.Account = json.Get(txItem.String(), "Account").String()
		transaction.TransactionType = json.Get(txItem.String(), "TransactionType").String()
		transaction.Fee = json.Get(txItem.String(), "Fee").Int()
		transaction.Sequence = json.Get(txItem.String(), "Sequence").Int()
		transaction.AccountTxnID = json.Get(txItem.String(), "AccountTxnID").String()
		transaction.Flags = json.Get(txItem.String(), "Flags").Int()
		transaction.LastLedgerSequence = json.Get(txItem.String(), "LastLedgerSequence").Int()
		transaction.Memos = json.Get(txItem.String(), "Memos").String()
		transaction.Signers = json.Get(txItem.String(), "Signers").String()
		transaction.SourceTag = json.Get(txItem.String(), "SourceTag").Int()
		transaction.SigningPubKey = json.Get(txItem.String(), "SigningPubKey").String()
		transaction.TxnSignature = json.Get(txItem.String(), "TxnSignature").String()

		transaction.Hash = json.Get(txItem.String(), "hash").String()
		transaction.LedgerIndex = b.Block.LedgerIndex
		transaction.TransactionResult = json.Get(txItem.String(), "metaData.TransactionResult").String()
		transaction.TransactionIndex = int(json.Get(txItem.String(), "metaData.TransactionIndex").Int())
		transaction.Validated = int(json.Get(txItem.String(), "validated").Int())
		transaction.Date = json.Get(txItem.String(), "date").Int()

		transaction.CloseTime = b.Block.CloseTime
		tempAmount := json.Get(txItem.String(), "Amount").String()
		if strings.Contains(tempAmount, "{") {
			amount := FormatAmount(transaction, tempAmount)
			amount.AmountType = 1
			b.Amounts = append(b.Amounts, amount)
			transaction.Amount = -1
		} else {
			transaction.Amount = json.Get(txItem.String(), "Amount").Int()
		}
		tempSendMax := json.Get(txItem.String(), "SendMax").String()
		if strings.Contains(tempSendMax, "{") {
			amount := FormatAmount(transaction, tempSendMax)
			amount.AmountType = 2
			b.Amounts = append(b.Amounts, amount)
			transaction.SendMax = -1
		} else {
			transaction.SendMax = json.Get(txItem.String(), "SendMax").Int()
		}

		tempDeliverMin := json.Get(txItem.String(), "DeliverMin").String()
		if strings.Contains(tempDeliverMin, "{") {
			amount := FormatAmount(transaction, tempDeliverMin)
			amount.AmountType = 2
			b.Amounts = append(b.Amounts, amount)
			transaction.DeliverMin = -1
		} else {
			transaction.DeliverMin = json.Get(txItem.String(), "DeliverMin").Int()
		}

		transaction.Destination = json.Get(txItem.String(), "Destination").String()
		transaction.DestinationTag = json.Get(txItem.String(), "DestinationTag").Int()
		transaction.InvoiceID = json.Get(txItem.String(), "InvoiceID").String()
		transaction.Expiration = json.Get(txItem.String(), "Expiration").Int()
		transaction.OfferSequence = int(json.Get(txItem.String(), "OfferSequence").Int())
		tempTakerGets := json.Get(txItem.String(), "TakerGets").String()
		if strings.Contains(tempTakerGets, "{") {
			amount := FormatAmount(transaction, tempTakerGets)
			amount.AmountType = 4
			b.Amounts = append(b.Amounts, amount)
			transaction.TakerGets = -1
		} else {
			transaction.TakerGets = json.Get(txItem.String(), "TakerGets").Int()
		}
		tempTakerPays := json.Get(txItem.String(), "TakerPays").String()
		if strings.Contains(tempTakerPays, "{") {
			amount := FormatAmount(transaction, tempTakerPays)
			amount.AmountType = 5
			b.Amounts = append(b.Amounts, amount)
			transaction.TakerPays = -1
		} else {
			transaction.TakerPays = json.Get(txItem.String(), "TakerPays").Int()
		}
		tempLimitAmount := json.Get(txItem.String(), "LimitAmount").String()
		if strings.Contains(tempLimitAmount, "{") {
			amount := FormatAmount(transaction, tempLimitAmount)
			amount.AmountType = 6
			b.Amounts = append(b.Amounts, amount)
			transaction.LimitAmount = -1
		} else {
			transaction.LimitAmount = json.Get(txItem.String(), "LimitAmount").Int()
		}

		transaction.QualityIn = json.Get(txItem.String(), "QualityIn").Int()
		transaction.QualityOut = json.Get(txItem.String(), "QualityOut").Int()
		transaction.ClearFlag = int(json.Get(txItem.String(), "ClearFlag").Int())
		transaction.Domain = json.Get(txItem.String(), "Domain").String()
		transaction.EmailHash = json.Get(txItem.String(), "EmailHash").String()
		transaction.MessageKey = json.Get(txItem.String(), "MessageKey").String()
		transaction.SetFlag = int(json.Get(txItem.String(), "SetFlag").Int())
		transaction.TransferRate = int(json.Get(txItem.String(), "TransferRate").Int())

		transaction.TickSize = int(json.Get(txItem.String(), "TickSize").Int())
		transaction.RegularKey = json.Get(txItem.String(), "RegularKey").String()
		transaction.SignerQuorum = int(json.Get(txItem.String(), "SignerQuorum").Int())
		transaction.CancelAfter = json.Get(txItem.String(), "CancelAfter").Int()
		transaction.FinishAfter = json.Get(txItem.String(), "FinishAfter").Int()
		transaction.Condition = json.Get(txItem.String(), "Condition").String()
		transaction.Owner = json.Get(txItem.String(), "Owner").String()

		transaction.Fulfillment = json.Get(txItem.String(), "Fulfillment").String()
		transaction.SettleDelay = json.Get(txItem.String(), "SettleDelay").Int()
		transaction.PublicKey = json.Get(txItem.String(), "PublicKey").String()
		transaction.Channel = json.Get(txItem.String(), "Channel").String()
		transaction.Balance = json.Get(txItem.String(), "Balance").Int()
		transaction.Authorize = json.Get(txItem.String(), "Authorize").String()
		transaction.UnAuthorize = json.Get(txItem.String(), "UnAuthorize").String()

		//paths
		txPaths := json.Get(txItem.String(), "Paths").Array()
		p := int64(0)
		for _, pathItem := range txPaths {
			inPaths := pathItem.Array()
			i := int64(0)
			for _, pathAgent := range inPaths {
				path := new(model.Path)
				path.CloseTime = b.Block.CloseTime
				path.LedgerIndex = b.Block.LedgerIndex
				path.Type = json.Get(pathAgent.String(), "type").Int()
				path.Currency = json.Get(pathAgent.String(), "currency").String()
				path.Issuer = json.Get(pathAgent.String(), "issuer").String()
				path.ParentHash = transaction.Hash
				path.InTxIndex = p
				path.InPathIndex = i

				b.Paths = append(b.Paths, path)
				i++
			}
			p++
		}
		//affectedNodes
		txNodes := json.Get(txItem.String(), "metaData.AffectedNodes").Array()
		for _, nodeItem := range txNodes {
			node := new(model.AffectedNodes)
			node.CloseTime = b.Block.CloseTime
			node.LedgerIndex = b.Block.LedgerIndex
			node.ParentHash = transaction.Hash
			for key, value := range nodeItem.Map() {
				node.NodeType = key
				node.LedgerEntryType = json.Get(value.String(), "LedgerEntryType").String()
				node.NodeLedgerIndex = json.Get(value.String(), "LedgerIndex").String()
				node.PreviousTxnID = json.Get(value.String(), "PreviousTxnID").String()
				node.PreviousTxnLgrSeq = json.Get(value.String(), "PreviousTxnLgrSeq").Int()
				node.FullJsonStr = value.String()
			}

			b.AffectedNodes = append(b.AffectedNodes, node)
		}
		transaction.PathesLen = len(txPaths)
		transaction.AffectedNodesLen = len(txNodes)

		b.Transactions = append(b.Transactions, transaction)
	}

	b.Block.TransactionLength = len(b.Transactions)

	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter xrp: parse block %d, txs %d, elasped time %s", b.Block.LedgerIndex, b.Block.TransactionLength, elaspedTime.String())

	return b, nil
}

func revertMiner(height int64, tx *service.Transaction) error {
	startTime := time.Now()
	index := "revert_miner"
	sql := fmt.Sprintf("UPDATE a SET a.miner_count = a.miner_count - 1 FROM xrp_account a"+
		" JOIN (SELECT miner FROM xrp_block WHERE height = '%d') b"+
		" ON a.address = b.miner ", height)
	affected1, err := tx.Execute(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	sql = fmt.Sprintf("UPDATE a SET a.miner_uncle_count = a.miner_uncle_count - 1 FROM xrp_account a"+
		" JOIN (SELECT miner FROM xrp_block WHERE height = '%d') b"+
		" ON a.address = b.miner ", height)
	affected2, err := tx.Execute(sql)
	if err != nil {
		log.DetailError(err)
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter xrp index: %s affected %d %d elasped %s", index, affected1, affected2, elaspedTime.String())
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
