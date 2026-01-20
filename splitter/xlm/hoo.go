package xlm

import (
	ej "encoding/json"
	"fmt"
	"github.com/aminvici/bds/common/json"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/xlm"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"
)

func (s *XLMSplitter) getMaxSequenceInDB() (int64, error) {
	var err error
	var maxSequence int64
	db := service.NewDatabase(s.cfg.Engine)
	ledgers := make([]*model.Ledger, 0)
	err = db.Desc("sequence").Limit(1).Find(&ledgers)
	if err != nil {
		log.Error("get max sequence in db error", err)
		log.DetailError(err)
		return maxSequence, err
	} else {
		if len(ledgers) > 0 {
			maxSequence = ledgers[0].Sequence
		} else {
			log.Warn("splitter xlm: database empty")
			maxSequence = 0
		}
	}
	return maxSequence, nil
}

func (s *XLMSplitter) getLedgerFromHorizon(maxSequence int64) {
	stopLedger := s.cfg.ConcurrentHeight
	startLedger := maxSequence + 1
	var wg sync.WaitGroup
	cuChan := make(chan bool, s.cfg.ConcurrentHTTP)
	for {
		log.Debug("splitter xlm :start ledger %d", startLedger, time.Now())
		endLedger := startLedger + 999
		if startLedger > stopLedger {
			break
		}
		if endLedger > stopLedger {
			endLedger = stopLedger
		}
		cuChan <- true
		wg.Add(1)

		go func(startHeight, endHeight int64) {
			urlLedgers := fmt.Sprintf("http://%s/send?ledger_start=%d&ledger_end=%d", s.cfg.Endpoint, startHeight, endHeight)
		STARTLEDGER:
			ledgers, err := httpLedgers(urlLedgers)
			if err != nil {
				log.Error("splitter xlm:get ledger from %d to %d error", startHeight, endHeight)
				time.Sleep(100 * time.Millisecond)
				goto STARTLEDGER
			}
			log.Debug("splitter xlm:finish get ledger from %d to %d", startHeight, endHeight)

			for _, ledger := range ledgers {
				stats.Add(MetricReceiveLedger, 1)
				s.databaseWorkerChan <- ledger
				stats.Add(MetricSendLedger, 1)
			}
			<-cuChan
			wg.Done()
		}(startLedger, endLedger)

		startLedger = endLedger + 1
	}
	wg.Wait()
}

func (s *XLMSplitter) getDataFromHorizonOrg() {
	db := service.NewDatabase(s.cfg.Engine)
	var heightInDB int64
	ledgers := make([]*model.Ledger, 0)
	err := db.Desc("sequence").Limit(1).Find(&ledgers)
	if err != nil {
		log.DetailError(err)
	} else {
		if len(ledgers) > 0 {
			heightInDB = ledgers[0].Sequence
		} else {
			log.Warn("splitter xlm: database empty")
			heightInDB = 0
		}
	}
	maxLedger := int64(23000000)
	startLedger := heightInDB + 1

	for i := startLedger; i <= maxLedger; i++ {
		urlLedger := fmt.Sprintf("https://horizon.stellar.org/ledgers/%d", i)
		urlTransaction := fmt.Sprintf("https://horizon.stellar.org/ledgers/%d/transactions?limit=200&order=asc", i)
		urlOperation := fmt.Sprintf("https://horizon.stellar.org/ledgers/%d/operations?limit=200&order=asc", i)
	STARTLEDGER:
		ledger, err := httpLedger(urlLedger)
		if err != nil {
			log.Error("get ledger %d error", i)
			time.Sleep(100 * time.Millisecond)
			goto STARTLEDGER
		}
	STARTTRANSACTION:
		transaction, tMap, err := httpTransaction(urlTransaction, ledger.TransactionCount)
		if err != nil {
			log.Error("get transaction %d error", i)
			time.Sleep(100 * time.Millisecond)
			goto STARTTRANSACTION
		}

	STARTOPERATION:
		operation, err := httpOperation(urlOperation, ledger.OperationCount, tMap)
		if err != nil {
			log.Error("get operation %d error", i)
			time.Sleep(100 * time.Millisecond)
			goto STARTOPERATION
		}

		xlm := new(XLMLedgerData)
		xlm.Ledger = ledger
		xlm.Operations = operation
		xlm.Transactions = transaction
		xlms := make([]*XLMLedgerData, 0)
		xlms = append(xlms, xlm)

		err = s.SaveLedger(xlms)
		if err != nil {
			log.Error("save xlm error %d error", i)
			time.Sleep(100 * time.Millisecond)
			goto STARTOPERATION
		}
		log.Info("save %d ledger successfully", i)
	}
}

func httpLedgers(url string) ([]*XLMLedgerData, error) {
	result := make([]*XLMLedgerData, 0)
	resp, err := http.Get(url)
	if err != nil {
		return result, err
	}
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return result, err
	}
	xRecords := json.Get(string(body), "records").Array()
	for _, xRecord := range xRecords {
		ledger := json.Get(xRecord.String(), "value").String()
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
		result = append(result, x)
	}
	resp.Body.Close()
	return result, nil
}

func httpLedger(url string) (*model.Ledger, error) {
	x := new(model.Ledger)
	resp, err := http.Get(url)
	if err != nil {
		return x, err
	}

	log.Debug("ledger url :%s", url)
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return x, err
	}
	ledger := string(body)

	x = new(model.Ledger)

	x.LedgerID = json.Get(ledger, "paging_token").String()

	x.PagingToken = json.Get(ledger, "paging_token").String()
	x.LedgerHash = json.Get(ledger, "hash").String()
	x.Sequence = json.Get(ledger, "sequence").Int()
	if x.Sequence != 1 {
		x.PreviousLedgerHash = json.Get(ledger, "prev_hash").String()
	} else {
		x.PreviousLedgerHash = ""
	}
	x.TransactionCount = json.Get(ledger, "successful_transaction_count").Int()
	x.SuccessfulTransactionCount = json.Get(ledger, "successful_transaction_count").Int()
	x.FailedTransactionCount = json.Get(ledger, "failed_transaction_count").Int()
	x.OperationCount = json.Get(ledger, "operation_count").Int()
	t, _ := time.ParseInLocation("2006-01-02T15:04:05Z", json.Get(ledger, "closed_at").String(), time.UTC)
	x.ClosedTime = t.Unix()
	x.TotalCoins = json.Get(ledger, "total_coins").String()
	x.FeePool = json.Get(ledger, "fee_pool").String()
	x.BaseFeeInStroops = json.Get(ledger, "base_fee_in_stroops").Int()
	x.BaseReserveInStroops = json.Get(ledger, "base_reserve_in_stroops").Int()
	x.MaxTxSetSize = json.Get(ledger, "max_tx_set_size").Int()
	x.ProtocolVersion = json.Get(ledger, "protocol_version").Int()
	resp.Body.Close()
	return x, nil
}

func httpTransaction(url string, count int64) ([]*model.Transaction, map[string]string, error) {
	tList := make([]*model.Transaction, 0)
	tMap := make(map[string]string, 0) //
	var cu int64
	if count%200 > 0 {
		cu = count/200 + 1
	} else {
		cu = count / 200
	}
	cursor := ""
	for i := int64(0); i < cu; i++ {
		u := url + "&cursor=" + cursor
		resp, err := http.Get(u)
		if err != nil {
			return tList, tMap, err
		}

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return tList, tMap, err
		}
		log.Debug("transaction url :%s", url)

		xx := json.Get(string(body), "_embedded").String()
		xxx := json.Get(xx, "records").Array()
		for _, txItem := range xxx {
			tx := new(model.Transaction)
			tx.TransactionID = json.Get(txItem.String(), "paging_token").String()
			tx.PagingToken = json.Get(txItem.String(), "paging_token").String()
			tx.TransactionHash = json.Get(txItem.String(), "hash").String()
			tx.LedgerSequence = json.Get(txItem.String(), "ledger").Int()
			tx.SourceAccount = json.Get(txItem.String(), "source_account").String()
			tx.SourceAccountSequence = json.Get(txItem.String(), "source_account_sequence").String()
			tx.FeePaid = json.Get(txItem.String(), "fee_paid").Int()
			tx.OperationCount = json.Get(txItem.String(), "operation_count").Int()
			tx.MemoType = json.Get(txItem.String(), "memo_type").String()
			sig := json.Get(txItem.String(), "signatures").Array()
			siArr := make([]string, 0)
			for _, sigItem := range sig {
				siArr = append(siArr, sigItem.String())
			}
			tx.Signatures = strings.Join(siArr, ",")
			tMap[tx.TransactionHash] = tx.TransactionID
			cursor = tx.PagingToken
			tList = append(tList, tx)
		}
		resp.Body.Close()
	}
	return tList, tMap, nil
}

func httpOperation(url string, count int64, tMap map[string]string) ([]*model.Operation, error) {
	opList := make([]*model.Operation, 0)
	var cu int64
	if count%200 > 0 {
		cu = count/200 + 1
	} else {
		cu = count / 200
	}
	cursor := ""
	order := int64(1)
	for i := int64(0); i < cu; i++ {
		u := url + "&cursor=" + cursor
		resp, err := http.Get(u)
		if err != nil {
			return opList, err
		}
		log.Debug("operation url :%s", url)

		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return opList, err
		}
		xx := json.Get(string(body), "_embedded").String()
		xxx := json.Get(xx, "records").Array()
		for _, opItem := range xxx {
			op := new(model.Operation)
			op.OperationID = json.Get(opItem.String(), "id").String()
			op.TransactionID = tMap[json.Get(opItem.String(), "transaction_hash").String()]
			op.ApplicationOrder = order
			order++
			op.Type = json.Get(opItem.String(), "type").String()
			op.SourceAccount = json.Get(opItem.String(), "source_account").String()

			switch op.Type {

			case "create_account":
				e := new(CreateAccount)
				err = ej.Unmarshal([]byte(opItem.String()), &e)
				dd, _ := ej.Marshal(e)
				op.Detail = string(dd)
			case "payment":
				e := new(Payment)
				err = ej.Unmarshal([]byte(opItem.String()), &e)
				dd, _ := ej.Marshal(e)
				op.Detail = string(dd)
			case "path_payment":
				e := new(PathPayment)
				err = ej.Unmarshal([]byte(opItem.String()), &e)
				dd, _ := ej.Marshal(e)
				op.Detail = string(dd)
			case "manage_offer":
				e := new(ManageOffer)
				err = ej.Unmarshal([]byte(opItem.String()), &e)
				dd, _ := ej.Marshal(e)
				op.Detail = string(dd)
			case "create_passive_offer":
				e := new(CreatePassiveOffer)
				err = ej.Unmarshal([]byte(opItem.String()), &e)
				dd, _ := ej.Marshal(e)
				op.Detail = string(dd)
			case "set_options":
				e := new(SetOptions)
				err = ej.Unmarshal([]byte(opItem.String()), &e)
				dd, _ := ej.Marshal(e)
				op.Detail = string(dd)
			case "change_trust":
				e := new(ChangeTrust)
				err = ej.Unmarshal([]byte(opItem.String()), &e)
				dd, _ := ej.Marshal(e)
				op.Detail = string(dd)
			case "allow_trust":
				e := new(AllowTrust)
				err = ej.Unmarshal([]byte(opItem.String()), &e)
				dd, _ := ej.Marshal(e)
				op.Detail = string(dd)
			case "account_merge":
				e := new(AccountMerge)
				err = ej.Unmarshal([]byte(opItem.String()), &e)
				dd, _ := ej.Marshal(e)
				op.Detail = string(dd)
			case "inflation":
				e := new(Inflation)
				err = ej.Unmarshal([]byte(opItem.String()), &e)
				dd, _ := ej.Marshal(e)
				op.Detail = string(dd)
			case "manage_data":
				e := new(ManageData)
				err = ej.Unmarshal([]byte(opItem.String()), &e)
				dd, _ := ej.Marshal(e)
				op.Detail = string(dd)
			case "bump_sequence":
				e := new(BumpSequence)
				err = ej.Unmarshal([]byte(opItem.String()), &e)
				dd, _ := ej.Marshal(e)
				op.Detail = string(dd)
			default:
				op.Detail = string("")

			}
			cursor = json.Get(opItem.String(), "paging_token").String()
			opList = append(opList, op)
		}
		resp.Body.Close()
	}
	return opList, nil
}

type BumpSequence struct {
	BumpTo string `json:"bump_to"`
}

// CreateAccount is the json resource representing a single operation whose type
// is CreateAccount.
type CreateAccount struct {
	StartingBalance string `json:"starting_balance"`
	Funder          string `json:"funder"`
	Account         string `json:"account"`
}

// Payment is the json resource representing a single operation whose type is
// Payment.
type Payment struct {
	Asset
	From   string `json:"from"`
	To     string `json:"to"`
	Amount string `json:"amount"`
}

// PathPayment is the json resource representing a single operation whose type
// is PathPayment.
type PathPayment struct {
	Payment
	Path              []Asset `json:"path"`
	SourceAmount      string  `json:"source_amount"`
	SourceMax         string  `json:"source_max"`
	SourceAssetType   string  `json:"source_asset_type"`
	SourceAssetCode   string  `json:"source_asset_code,omitempty"`
	SourceAssetIssuer string  `json:"source_asset_issuer,omitempty"`
}

// ManageData represents a ManageData operation as it is serialized into json
// for the horizon API.
type ManageData struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// CreatePassiveOffer is the json resource representing a single operation whose
// type is CreatePassiveOffer.
type CreatePassiveOffer struct {
	Amount             string `json:"amount"`
	Price              string `json:"price"`
	PriceR             Price  `json:"price_r"`
	BuyingAssetType    string `json:"buying_asset_type"`
	BuyingAssetCode    string `json:"buying_asset_code,omitempty"`
	BuyingAssetIssuer  string `json:"buying_asset_issuer,omitempty"`
	SellingAssetType   string `json:"selling_asset_type"`
	SellingAssetCode   string `json:"selling_asset_code,omitempty"`
	SellingAssetIssuer string `json:"selling_asset_issuer,omitempty"`
}

// ManageOffer is the json resource representing a single operation whose type
// is ManageOffer.
type ManageOffer struct {
	CreatePassiveOffer
	OfferID int64 `json:"offer_id"`
}

// SetOptions is the json resource representing a single operation whose type is
// SetOptions.
type SetOptions struct {
	HomeDomain    string `json:"home_domain,omitempty"`
	InflationDest string `json:"inflation_dest,omitempty"`

	MasterKeyWeight *int   `json:"master_key_weight,omitempty"`
	SignerKey       string `json:"signer_key,omitempty"`
	SignerWeight    *int   `json:"signer_weight,omitempty"`

	SetFlags    []int    `json:"set_flags,omitempty"`
	SetFlagsS   []string `json:"set_flags_s,omitempty"`
	ClearFlags  []int    `json:"clear_flags,omitempty"`
	ClearFlagsS []string `json:"clear_flags_s,omitempty"`

	LowThreshold  *int `json:"low_threshold,omitempty"`
	MedThreshold  *int `json:"med_threshold,omitempty"`
	HighThreshold *int `json:"high_threshold,omitempty"`
}

// ChangeTrust is the json resource representing a single operation whose type
// is ChangeTrust.
type ChangeTrust struct {
	Asset
	Limit   string `json:"limit"`
	Trustee string `json:"trustee"`
	Trustor string `json:"trustor"`
}

// AllowTrust is the json resource representing a single operation whose type is
// AllowTrust.
type AllowTrust struct {
	Asset
	Trustee   string `json:"trustee"`
	Trustor   string `json:"trustor"`
	Authorize bool   `json:"authorize"`
}

// AccountMerge is the json resource representing a single operation whose type
// is AccountMerge.
type AccountMerge struct {
	Account string `json:"account"`
	Into    string `json:"into"`
}

// Inflation is the json resource representing a single operation whose type is
// Inflation.
type Inflation struct {
}

type Price struct {
	N int32 `json:"n"`
	D int32 `json:"d"`
}

type Asset struct {
	Type   string `json:"asset_type"`
	Code   string `json:"asset_code,omitempty"`
	Issuer string `json:"asset_issuer,omitempty"`
}
