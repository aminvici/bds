package jsonrpc

import (
	native_json "encoding/json"
	"errors"
	"fmt"
	"github.com/aminvici/bds/common/httputils"
	"github.com/aminvici/bds/common/json"
	"reflect"
	"strconv"
)

const (
	defaultVersion = "1.0"
	defaultID      = "bds-jsonrpc"
)

type RPCRequest struct {
	ID      string      `json:"id"`
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

type RPCResponse struct {
	ID      string      `json:"id"`
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *RPCError   `json:"error,omitempty"`
}

type RPCError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

func (e *RPCError) Error() string {
	return fmt.Sprintf("%s:%s", strconv.Itoa(e.Code), e.Message)
}

type Client struct {
	httpClient *httputils.RestClient
	endpoint   string
	version    string
	id         string
}

func New(c *httputils.RestClient, endpoint string) *Client {
	client := new(Client)
	client.httpClient = c
	client.endpoint = endpoint
	client.version = defaultVersion
	client.id = defaultID
	return client
}

func (c *Client) SetID(s string) {
	c.id = s
	return
}

func (c *Client) SetVersion(s string) {
	c.version = s
	return
}
func (c *Client) CallXRP(method string, params ...interface{}) ([]byte, error) {
	rpcRequest := &RPCRequest{
		ID:      c.id,
		Method:  method,
		Params:  params,
		JSONRPC: c.version,
	}

	data, err := native_json.Marshal(rpcRequest)
	if err != nil {
		return nil, err
	}
	response, err := c.httpClient.Post(c.endpoint, data)
	if err != nil {
		return nil, err
	}
	if len(response) == 0 {
		return nil, errors.New("response is empty")
	}

	errorMap := json.GetBytes(response, "error").Map()
	if len(errorMap) != 0 {
		code := json.GetBytes(response, "error.code").Int()
		msg := json.GetBytes(response, "error.message").String()
		return nil, errors.New(fmt.Sprintf("error: %d %s", code, msg))
	}

	return response, nil

}
func (c *Client) Call(method string, params ...interface{}) ([]byte, error) {
	rpcRequest := &RPCRequest{
		ID:      c.id,
		Method:  method,
		Params:  c.params(params...),
		JSONRPC: c.version,
	}

	data, err := native_json.Marshal(rpcRequest)
	if err != nil {
		return nil, err
	}
	response, err := c.httpClient.Post(c.endpoint, data)
	if err != nil {
		return nil, err
	}
	if len(response) == 0 {
		return nil, errors.New("response is empty")
	}

	errorMap := json.GetBytes(response, "error").Map()
	if len(errorMap) != 0 {
		code := json.GetBytes(response, "error.code").Int()
		msg := json.GetBytes(response, "error.message").String()
		return nil, errors.New(fmt.Sprintf("error: %d %s", code, msg))
	}

	return response, nil

}

func (c *Client) params(params ...interface{}) interface{} {
	var finalParams interface{}

	if params != nil {
		switch len(params) {
		case 0:
		case 1:
			if params[0] != nil {
				var typeOf reflect.Type

				for typeOf = reflect.TypeOf(params[0]); typeOf != nil && typeOf.Kind() == reflect.Ptr; typeOf = typeOf.Elem() {
				}

				if typeOf != nil {
					switch typeOf.Kind() {
					case reflect.Struct:
						finalParams = params[0]
					case reflect.Array:
						finalParams = params[0]
					case reflect.Slice:
						finalParams = params[0]
					case reflect.Interface:
						finalParams = params[0]
					case reflect.Map:
						finalParams = params[0]
					default:
						finalParams = params
					}
				}
			} else {
				finalParams = params
			}
		default:
			finalParams = params
		}
	}

	return finalParams
}
