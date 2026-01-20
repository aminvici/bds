package eos

import (
	"github.com/aminvici/bds/common/httputils"
	"testing"
)

func TestSendBlock(t *testing.T) {
	httpClient := httputils.NewRestClientWithAuthentication(nil)
	remoteHandler, err := newHTTPHandler(httpClient, "http://127.0.0.1:8888/v1/chain", "10.226.138.107", "8082", "test")
	if err != nil {
		t.Fatal(err)
		return
	}
	err = remoteHandler.SendBlock(189)
	if err != nil {
		return
	}
	t.Log("over")
}

func TestSendBatchBlock(t *testing.T) {
	httpClient := httputils.NewRestClientWithAuthentication(nil)
	remoteHandler, err := newHTTPHandler(httpClient, "http://127.0.0.1:8888/v1/chain", "10.226.138.107", "8082", "test")
	if err != nil {
		t.Fatal(err)
		return
	}
	err = remoteHandler.SendBatchBlock(189, 191)
	if err != nil {
		return
	}
	t.Log("over")
}
