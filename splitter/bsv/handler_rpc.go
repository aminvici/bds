package bsv

import (
	"github.com/aminvici/bds/common/jsonrpc"
)

type rpcHandler struct {
	client *jsonrpc.Client
}

func newRPCHandler(c *jsonrpc.Client) (*rpcHandler, error) {
	h := new(rpcHandler)
	h.client = c
	return h, nil
}

//let BSV node send data by height
func (h *rpcHandler) SendBlock(height int64) error {
	defer stats.Add(MetricRPCCall, 1)
	_, err := h.client.Call("sendblock", height)
	if err != nil {
		return err
	}

	return nil
}

//let BSV node send data from startHeight to endHeight
func (h *rpcHandler) SendBatchBlock(startHeight, endHeight int64) error {
	defer stats.Add(MetricRPCCall, 1)
	_, err := h.client.Call("sendbatchblock", startHeight, endHeight)
	if err != nil {
		return err
	}

	return nil
}
