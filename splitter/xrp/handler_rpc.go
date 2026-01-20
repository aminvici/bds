package xrp

import (
	"errors"
	"fmt"
	"github.com/aminvici/bds/common/json"
	"github.com/aminvici/bds/common/jsonrpc"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/common/math"
	"math/big"
	"strconv"
	"strings"
)

type rpcHandler struct {
	client *jsonrpc.Client
}

func newRPCHandler(c *jsonrpc.Client) (*rpcHandler, error) {
	h := new(rpcHandler)
	h.client = c
	return h, nil
}

func (h *rpcHandler) GetBlockNumber() (int64, error) {
	defer stats.Add(MetricRPCCall, 1)
	data, err := h.client.Call("xrp_blockNumber")
	if err != nil {
		return 0, err
	}
	v := json.GetBytes(data, "result").String()
	if len(v) == 0 {
		return 0, errors.New("cannot get block number")
	}
	number, err := strconv.ParseInt(v, 0, 64)
	if err != nil {
		return 0, err
	}

	return number, nil
}

func (h *rpcHandler) GetBalance(address string, height int64) (*big.Int, error) {
	defer stats.Add(MetricRPCCall, 1)
	if !strings.HasPrefix(address, "0x") {
		address = fmt.Sprintf("0x%s", address)
	}

	var hexNumber string
	if height == int64(0) {
		hexNumber = "latest"
	} else {
		hexNumber = fmt.Sprintf("%#x", height)
	}
	data, err := h.client.Call("xrp_getBalance", address, ""+hexNumber+"")
	if err != nil {
		log.DetailError(err)
		return nil, err
	}
	tmp := json.GetBytes(data, "result").String()
	value, err := math.ParseInt256(tmp)
	if err != nil {
		log.DetailError(err)
		return nil, err
	}
	return value, nil
}

func (h *rpcHandler) SendBlock(number int64) error {
	defer stats.Add(MetricRPCCall, 1)
	hexNumber := fmt.Sprintf("%#x", number)
	_, err := h.client.Call("xrp_sendBlockByNumber", hexNumber, true)
	if err != nil {
		return err
	}
	return nil
}

type CompleteLedgers struct {
	startLedger int64
	endLedger   int64
}

func (h *rpcHandler) GetCompleteLedgers() (map[int]*CompleteLedgers, error) {
	totalCompleteLedgers := make(map[int]*CompleteLedgers, 0)

	res, err := h.client.CallXRP("server_info")
	if err != nil {
		return nil, err
	}
	data := string(res)
	cl_str := json.Get(data, "result.info.complete_ledgers").String()
	//cl_str demo:"47025320,47025422-47025425,47025527-47025528,47025629-47025661,47025763,47025864-47025877"
	log.Info("splitter xrp: get completed ledgers: %s\n\n", cl_str)
	if strings.ToLower(cl_str) == "empty" {
		return nil, nil
	}

	cl_arr := strings.Split(cl_str, ",")
	for i, v := range cl_arr {
		cl := new(CompleteLedgers)
		ind := strings.Index(v, "-")
		if ind > 0 {
			cl.startLedger, _ = strconv.ParseInt(v[:ind], 10, 64)
			cl.endLedger, _ = strconv.ParseInt(v[ind+1:], 10, 64)
		} else {
			cl.startLedger, _ = strconv.ParseInt(v, 10, 64)
			cl.endLedger, _ = strconv.ParseInt(v, 10, 64)
		}
		totalCompleteLedgers[i] = cl
	}
	return totalCompleteLedgers, nil
}
func (h *rpcHandler) SendBatchBlock(startNumber, endNumber int64) error {
	defer stats.Add(MetricRPCCall, 1)
	params := make(map[string]interface{}, 0)
	params["start_ledger_index"] = startNumber
	params["end_ledger_index"] = endNumber

	res, err := h.client.CallXRP("send_batch_ledger", params)
	log.Info("splitter xrp: send batch ledger res: %s", string(res))
	if err != nil {
		return err
	}
	return nil
}
