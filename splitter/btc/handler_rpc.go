package btc

import (
	"github.com/aminvici/bds/common/json"
	"github.com/aminvici/bds/common/jsonrpc"
	"github.com/aminvici/bds/common/math"
	model "github.com/aminvici/bds/service/model/btc"
	"strconv"
)

type rpcHandler struct {
	client *jsonrpc.Client
}

func newRPCHandler(c *jsonrpc.Client) (*rpcHandler, error) {
	h := new(rpcHandler)
	h.client = c
	return h, nil
}

//let BTC node send data by height
func (h *rpcHandler) SendBlock(height int64) error {
	defer stats.Add(MetricRPCCall, 1)
	_, err := h.client.Call("sendblock", height)
	if err != nil {
		return err
	}
	return nil
}

//let BTC node send data from startHeight to endHeight
func (h *rpcHandler) SendBatchBlock(startHeight, endHeight int64) error {
	defer stats.Add(MetricRPCCall, 1)
	_, err := h.client.Call("sendbatchblock", startHeight, endHeight)
	if err != nil {
		return err
	}
	return nil
}

//get omni block data
func (h *rpcHandler) GetOmniBlock(height int64) ([]*model.OmniTansaction, error) {
	defer stats.Add(MetricRPCCall, 1)
	var data []*model.OmniTansaction
	response, err := h.client.Call("omni_listblocktransactions", height)
	if err != nil {
		return data, err
	}
	result := json.Get(string(response), "result").Array()
	for _, v := range result {
		tx, err := h.GetOmniTx(v.String())
		if err != nil {
			return data, err
		}
		data = append(data, tx)
	}
	return data, nil
}

//get omni transaction details
func (h *rpcHandler) GetOmniTx(txId string) (*model.OmniTansaction, error) {
	defer stats.Add(MetricRPCCall, 1)
	data := new(model.OmniTansaction)
	response, err := h.client.Call("omni_gettransaction", txId)
	if err != nil {
		return data, err
	}
	txItem := json.Get(string(response), "result")
	data.TxID = json.Get(txItem.String(), "txid").String()
	data.Version = json.Get(txItem.String(), "version").Int()
	data.TypeInt = json.Get(txItem.String(), "type_int").Int()
	data.Type = json.Get(txItem.String(), "type").String()
	data.PropertyID = json.Get(txItem.String(), "propertyid").Int()
	data.Number = json.Get(txItem.String(), "positioninblock").Int()
	data.BlockHeight = json.Get(txItem.String(), "block").Int()
	data.SendingAddress = json.Get(txItem.String(), "sendingaddress").String()
	data.ReferenceAddress = json.Get(txItem.String(), "referenceaddress").String()
	data.Timestamp = json.Get(txItem.String(), "blocktime").Int()
	if json.Get(txItem.String(), "valid").Bool() {
		data.Valid = 1
	} else {
		data.Valid = 0
	}
	amountFloat, _ := strconv.ParseFloat(json.Get(txItem.String(), "amount").String(), 64)
	amount := math.Float64ToUint64(amountFloat * 100000000)
	data.Amount = amount
	feeFloat, _ := strconv.ParseFloat(json.Get(txItem.String(), "fee").String(), 64)
	fee := math.Float64ToUint64(feeFloat * 100000000)
	data.Fee = fee
	return data, nil
}

//get tether address balance
func (h *rpcHandler) GetTetherBalance(address string) (int64, error) {
	defer stats.Add(MetricRPCCall, 1)
	var data int64
	response, err := h.client.Call("omni_getbalance", address, 31)
	if err != nil {
		return data, err
	}
	dataFloat := json.Get(string(response), "result.balance").Float()
	data = math.Float64ToInt64(dataFloat * 100000000)
	return data, nil
}
