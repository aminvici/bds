package etc

import (
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/aminvici/bds/common/json"
	"github.com/aminvici/bds/common/jsonrpc"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/common/math"
	model "github.com/aminvici/bds/service/model/etc"
	"math/big"
	"strconv"
	"strings"
	"unsafe"
)

type rpcHandler struct {
	client *jsonrpc.Client
}

func newRPCHandler(c *jsonrpc.Client) (*rpcHandler, error) {
	h := new(rpcHandler)
	h.client = c
	return h, nil
}

//get max height of block by rpc
func (h *rpcHandler) GetBlockNumber() (int64, error) {
	defer stats.Add(MetricRPCCall, 1)
	data, err := h.client.Call("eth_blockNumber")
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

//get address balance by rpc
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
	data, err := h.client.Call("eth_getBalance", address, ""+hexNumber+"")
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

//let node send block by height
func (h *rpcHandler) SendBlock(number int64) error {
	defer stats.Add(MetricRPCCall, 1)
	_, err := h.client.Call("eth_sendBlockByNumber", number)
	if err != nil {
		return err
	}
	return nil
}

//let node send block by height from startNumber to endNumber
func (h *rpcHandler) SendBatchBlock(startNumber, endNumber int64) error {
	defer stats.Add(MetricRPCCall, 1)
	_, err := h.client.Call("eth_sendBatchBlockByNumber", startNumber, endNumber)
	if err != nil {
		return err
	}
	return nil
}

//get token information by address
func (h *rpcHandler) GetToken(tokenAddress string) (*model.Token, error) {
	token := new(model.Token)
	name, err := h.GetTokenName(tokenAddress)
	if err != nil {
		return nil, err
	}
	token.Name = strings.Replace(name, "\u0000", "", -1)
	symbol, err := h.GetTokenSymbol(tokenAddress)
	if err != nil {
		return nil, err
	}
	token.Symbol = strings.Replace(symbol, "\u0000", "", -1)
	decimal, err := h.GetTokenDecimal(tokenAddress)
	if err != nil {
		return nil, err
	}
	token.DecimalLength = decimal
	totalSupply, err := h.GetTokenTotalSupply(tokenAddress)
	if err != nil {
		return nil, err
	}
	token.TotalSupply = totalSupply
	//token.TotalSupply,err = parseBigInt(totalSupply)
	//if err != nil {
	//	return nil, err
	//}
	owner, err := h.GetTokenOwner(tokenAddress)
	if err != nil {
		return nil, err
	}
	token.Owner = owner
	return token, nil
}

func byteString(p []byte) string {
	return *(*string)(unsafe.Pointer(&p))
}

//get token owner by address
func (h *rpcHandler) GetTokenOwner(tokenAddress string) (string, error) {
	params := make(map[string]interface{}, 0)
	params["to"] = fmt.Sprintf("0x%s", tokenAddress)
	params["data"] = fmt.Sprintf("0x8da5cb5b")
	var hexNumber string
	hexNumber = "latest"
	data, err := h.client.Call("eth_call", params, hexNumber)
	if err != nil {
		log.DetailError(err)
		return "", err
	}
	result := json.GetBytes(data, "result").String()
	temp := ""
	if len(result) > 26 {
		temp = result[26:]
		//tempByte,err := hex.DecodeString(temp)
		//if err!=nil{
		//	return "", err
		//}
		//temp = byteString(tempByte)
	} else {
		temp = ""
	}
	return temp, nil
}

//get token supply by address
func (h *rpcHandler) GetTokenTotalSupply(tokenAddress string) (string, error) {
	params := make(map[string]interface{}, 0)
	params["to"] = fmt.Sprintf("0x%s", tokenAddress)
	params["data"] = fmt.Sprintf("0x18160ddd")
	var hexNumber string
	hexNumber = "latest"
	data, err := h.client.Call("eth_call", params, hexNumber)
	if err != nil {
		log.DetailError(err)
		return "", err
	}
	result := json.GetBytes(data, "result").String()
	if len(result) > 2 {
		result = result[2:]
	}
	temp := big.NewInt(0)
	temp.SetString(result, 16)

	return temp.String(), nil
}

//get token decimal by address
func (h *rpcHandler) GetTokenDecimal(tokenAddress string) (int64, error) {
	params := make(map[string]interface{}, 0)
	params["to"] = fmt.Sprintf("0x%s", tokenAddress)
	params["data"] = fmt.Sprintf("0x313ce567")
	var hexNumber string
	hexNumber = "latest"
	data, err := h.client.Call("eth_call", params, hexNumber)
	if err != nil {
		log.DetailError(err)
		return -1, err
	}
	result := json.GetBytes(data, "result").String()
	if len(result) > 2 {
		result = result[2:]
	}
	temp := big.NewInt(0)
	temp.SetString(result, 16)

	return temp.Int64(), nil
}

//get token symbol by address
func (h *rpcHandler) GetTokenSymbol(tokenAddress string) (string, error) {
	params := make(map[string]interface{}, 0)
	params["to"] = fmt.Sprintf("0x%s", tokenAddress)
	params["data"] = fmt.Sprintf("0x95d89b41")
	var hexNumber string
	hexNumber = "latest"
	data, err := h.client.Call("eth_call", params, hexNumber)
	if err != nil {
		log.DetailError(err)
		return "", err
	}
	result := json.GetBytes(data, "result").String()
	temp := ""
	if len(result) > 130 {
		temp = result[130:]
		tempByte, err := hex.DecodeString(temp)
		if err != nil {
			return "", err
		}
		temp = byteString(tempByte)
	} else {
		temp = ""
	}
	return temp, nil
}

//get token name by address
func (h *rpcHandler) GetTokenName(tokenAddress string) (string, error) {
	params := make(map[string]interface{}, 0)
	params["to"] = fmt.Sprintf("0x%s", tokenAddress)
	params["data"] = fmt.Sprintf("0x06fdde03")
	var hexNumber string
	hexNumber = "latest"
	data, err := h.client.Call("eth_call", params, hexNumber)
	if err != nil {
		log.DetailError(err)
		return "", err
	}
	result := json.GetBytes(data, "result").String()
	temp := ""
	if len(result) > 130 {
		temp = result[130:]
		tempByte, err := hex.DecodeString(temp)
		if err != nil {
			return "", err
		}
		temp = byteString(tempByte)
	} else {
		temp = ""
	}
	return temp, nil
}

//get token balance by token address and account address
func (h *rpcHandler) GetTokenBalance(tokenAddress string, accountAddress string, height int64) (*big.Int, error) {
	defer stats.Add(MetricRPCCall, 1)
	params := make(map[string]interface{}, 0)
	params["to"] = fmt.Sprintf("0x%s", tokenAddress)
	params["data"] = fmt.Sprintf("0x70a08231000000000000000000000000%s", accountAddress)
	var hexNumber string
	if height == int64(0) {
		hexNumber = "latest"
	} else {
		hexNumber = fmt.Sprintf("%#x", height)
	}
	data, err := h.client.Call("eth_call", params, ""+hexNumber+"")
	if err != nil {
		log.DetailError(err)
		return nil, err
	}
	result := json.GetBytes(data, "result").String()
	var balance *big.Int
	if len(result) > 100 {
		result = result[0:10]
	}
	if len(result) > 2 {
		balance, err = math.ParseInt256(result)
		if err != nil {
			log.DetailError(err)
			return nil, err
		}
		if len(balance.String()) > 36 {
			balance = new(big.Int).SetInt64(-1)
		}
	} else {
		balance = new(big.Int).SetInt64(0)
	}
	return balance, nil
}
