package tron

import (
	"encoding/json"
	"github.com/aminvici/bds/common/httputils"
)

type httpHandler struct {
	client   *httputils.RestClient
	endpoint string
}

func newHTTPHandler(c *httputils.RestClient, endpoint string) (*httpHandler, error) {
	h := new(httpHandler)
	h.client = c
	h.endpoint = endpoint
	return h, nil
}

func (h *httpHandler) SendBlock(number int64) error {
	params := make(map[string]interface{})
	params["num"] = number
	data, err := json.Marshal(params)
	if err != nil {
		return err
	}
	_, err = h.client.Post(h.endpoint+"/wallet/sendblockbynum", data)
	if err != nil {
		return err
	}
	return nil
}

func (h *httpHandler) SendBatchBlock(startNumber, endNumber int64) error {
	params := make(map[string]interface{})
	params["startNum"] = startNumber
	params["endNum"] = endNumber

	data, err := json.Marshal(params)
	if err != nil {
		return err
	}
	_, err = h.client.Post(h.endpoint+"/wallet/sendbatchblockbynum", data)
	if err != nil {
		return err
	}
	return nil
}
