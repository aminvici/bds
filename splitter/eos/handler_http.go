package eos

import (
	"encoding/json"
	"github.com/aminvici/bds/common/httputils"
)

type httpHandler struct {
	client         *httputils.RestClient
	endpoint       string
	topic          string
	kafkaProxyHost string
	kafkaProxyPort string
}

func newHTTPHandler(c *httputils.RestClient, endpoint, kafkaProxyHost, kafkaProxyPort, topic string) (*httpHandler, error) {
	h := new(httpHandler)
	h.client = c
	h.endpoint = endpoint
	h.topic = topic
	h.kafkaProxyHost = kafkaProxyHost
	h.kafkaProxyPort = kafkaProxyPort
	return h, nil
}

func (h *httpHandler) SendBlock(number int64) error {
	params := make(map[string]interface{})
	params["block_num_or_id"] = number
	params["kafka_proxy_host"] = h.kafkaProxyHost
	params["kafka_proxy_port"] = h.kafkaProxyPort
	params["kafka_topic"] = h.topic

	data, err := json.Marshal(params)
	if err != nil {
		return err
	}
	_, err = h.client.Post(h.endpoint+"/send_block", data)
	if err != nil {
		return err
	}
	return nil
}

func (h *httpHandler) SendBatchBlock(startNumber, endNumber int64) error {
	params := make(map[string]interface{})
	params["block_num_start"] = startNumber
	params["block_num_end"] = endNumber
	params["kafka_proxy_host"] = h.kafkaProxyHost
	params["kafka_proxy_port"] = h.kafkaProxyPort
	params["kafka_topic"] = h.topic

	data, err := json.Marshal(params)
	if err != nil {
		return err
	}
	_, err = h.client.Post(h.endpoint+"/send_batch_block", data)
	if err != nil {
		return err
	}
	return nil
}
