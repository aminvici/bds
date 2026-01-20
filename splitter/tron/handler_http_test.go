package tron

import (
	"github.com/aminvici/bds/common/httputils"
	"testing"
)

func TestSendBlock(t *testing.T) {
	t.Log("begin")
	t.Log(len("4eb9f0b5dba710ae0daf249ba3d3163b764f130450aef5dbff6d429f5803c884"))
	httpClient := httputils.NewRestClientWithAuthentication(nil)
	remoteHandler, err := newHTTPHandler(httpClient, "http://127.0.0.1:8090")
	if err != nil {
		t.Fatal(err)
		return
	}
	err = remoteHandler.SendBlock(174042)
	if err != nil {
		return
	}
	t.Log("over")
}

func TestSendBatchBlock(t *testing.T) {
	httpClient := httputils.NewRestClientWithAuthentication(nil)
	remoteHandler, err := newHTTPHandler(httpClient, "http://127.0.0.1:8090")
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
