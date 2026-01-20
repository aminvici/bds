package tron

import (
	"fmt"
	"github.com/aminvici/bds/common/json"
	"github.com/aminvici/bds/common/log"
	"github.com/aminvici/bds/service"
	model "github.com/aminvici/bds/service/model/tron"
	"time"
)

func ParseBlock(data string) (*TRONBlockData, error) {
	startTime := time.Now()
	b := new(TRONBlockData)
	b.Block = new(model.Block)
	b.Transactions = make([]*model.Transaction, 0)

	b.Block.BlockNumber = json.Get(data, "block_number").Int()
	b.Block.BlockHash = json.Get(data, "block_id").String()
	b.Block.ParentHash = json.Get(data, "parent_hash").String()
	b.Block.Size = json.Get(data, "size").Int()
	b.Block.Timestamp = json.Get(data, "timestamp").Int() / 1000
	b.Block.WitnessAddress = json.Get(data, "witness_address").String()
	b.Block.WitnessSignature = json.Get(data, "witness_signature").String()
	b.Block.TransactionRoot = json.Get(data, "transaction_root").String()

	transactionList := json.Get(data, "transactions").Array()
	for _, transaction := range transactionList {
		t := new(model.Transaction)
		t.BlockNumber = b.Block.BlockNumber
		t.Hash = json.Get(transaction.String(), "transaction_id").String()
		t.Contracts = make([]*model.Contract, 0)

		contractList := json.Get(transaction.String(), "raw_data.contract").Array()
		for i, contract := range contractList {
			c := new(model.Contract)
			c.BlockNumber = b.Block.BlockNumber
			c.TransactionHash = t.Hash
			c.ContractNumber = i
			c.Type = json.Get(contract.String(), "type").String()
			c.Value = json.Get(contract.String(), "parameter.value").String()

			t.Contracts = append(t.Contracts, c)
		}

		t.Timestamp = b.Block.Timestamp
		b.Transactions = append(b.Transactions, t)
	}

	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter tron: parse block %d, elasped time %s", b.Block.BlockNumber, elaspedTime.String())
	//log.Debug("splitter tron: block %s ", b.Block)
	return b, nil
}

func revertBlock(num int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("DELETE from tron_block WHERE block_number = %d", num)
	affected, err := tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_block table affected %d elasped %s", num, affected, elaspedTime.String())
	return nil
}

func revertTransaction(num int64, tx *service.Transaction) error {
	startTime := time.Now()
	sql := fmt.Sprintf("delete from tron_transaction where block_number = %d", num)
	affected, err := tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter eos: revert block %d from tron_transaction table affected %d elasped %s", num, affected, elaspedTime.String())
	return nil
}

func revertContract(num int64, tx *service.Transaction) error {
	startTime := time.Now()
	var affected int64
	var err error
	var sql string

	//case AccountCreateContract:
	sql = fmt.Sprintf("delete from tron_contract_account_create where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime := time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_account_create table affected %d elasped %s", num, affected, elaspedTime.String())
	//case TransferContract:
	sql = fmt.Sprintf("delete from tron_contract_transfer where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_transfer table affected %d elasped %s", num, affected, elaspedTime.String())
	//case TransferAssetContract:
	sql = fmt.Sprintf("delete from tron_contract_transfer_asset where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_transfer_asset table affected %d elasped %s", num, affected, elaspedTime.String())
	//case VoteAssetContract:
	sql = fmt.Sprintf("delete from tron_contract_vote_asset where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_vote_asset table affected %d elasped %s", num, affected, elaspedTime.String())
	//case VoteWitnessContract:
	sql = fmt.Sprintf("delete from tron_contract_vote_witness where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_vote_witness table affected %d elasped %s", num, affected, elaspedTime.String())
	//case WitnessCreateContract:
	sql = fmt.Sprintf("delete from tron_contract_witness_create where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_witness_create table affected %d elasped %s", num, affected, elaspedTime.String())
	//case AssetIssueContract:
	sql = fmt.Sprintf("delete from tron_contract_asset_issue where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_asset_issue table affected %d elasped %s", num, affected, elaspedTime.String())
	//case WitnessUpdateContract:
	sql = fmt.Sprintf("delete from tron_contract_witness_update where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_witness_update table affected %d elasped %s", num, affected, elaspedTime.String())
	//case ParticipateAssetIssueContract:
	sql = fmt.Sprintf("delete from tron_contract_participate_asset_issue where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_participate_asset_issue table affected %d elasped %s", num, affected, elaspedTime.String())
	//case AccountUpdateContract:
	sql = fmt.Sprintf("delete from tron_contract_account_update where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_account_update table affected %d elasped %s", num, affected, elaspedTime.String())
	//case FreezeBalanceContract:
	sql = fmt.Sprintf("delete from tron_contract_freeze_balance where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_freeze_balance table affected %d elasped %s", num, affected, elaspedTime.String())
	//case UnfreezeBalanceContract:
	sql = fmt.Sprintf("delete from tron_contract_unfreeze_balance where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_unfreeze_balance table affected %d elasped %s", num, affected, elaspedTime.String())
	//case UnfreezeAssetContract:
	sql = fmt.Sprintf("delete from tron_contract_unfreeze_asset where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_unfreeze_asset table affected %d elasped %s", num, affected, elaspedTime.String())
	//case WithdrawBalanceContract:
	sql = fmt.Sprintf("delete from tron_contract_withdraw_balance where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_withdraw_balance table affected %d elasped %s", num, affected, elaspedTime.String())
	//case UpdateAssetContract:
	sql = fmt.Sprintf("delete from tron_contract_update_asset where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_update_asset table affected %d elasped %s", num, affected, elaspedTime.String())
	//case CreateSmartContract:
	sql = fmt.Sprintf("delete from tron_contract_create_smart where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_create_smart table affected %d elasped %s", num, affected, elaspedTime.String())
	//case TriggerSmartContract:
	sql = fmt.Sprintf("delete from tron_contract_trigger_smart where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_trigger_smart table affected %d elasped %s", num, affected, elaspedTime.String())
	//case ProposalCreateContract:
	sql = fmt.Sprintf("delete from tron_contract_proposal_create where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_proposal_create table affected %d elasped %s", num, affected, elaspedTime.String())
	//case ProposalApproveContract:
	sql = fmt.Sprintf("delete from tron_contract_proposal_approve where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_proposal_approve table affected %d elasped %s", num, affected, elaspedTime.String())
	//case ProposalDeleteContract:
	sql = fmt.Sprintf("delete from tron_contract_proposal_delete where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_proposal_delete table affected %d elasped %s", num, affected, elaspedTime.String())
	//case ExchangeCreateContract:
	sql = fmt.Sprintf("delete from tron_contract_exchange_create where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_exchange_create table affected %d elasped %s", num, affected, elaspedTime.String())
	//case ExchangeInjectContract:
	sql = fmt.Sprintf("delete from tron_contract_exchange_inject where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_exchange_inject table affected %d elasped %s", num, affected, elaspedTime.String())
	//case ExchangeWithdrawContract:
	sql = fmt.Sprintf("delete from tron_contract_exchange_withdraw where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_exchange_withdraw table affected %d elasped %s", num, affected, elaspedTime.String())
	//case ExchangeTransactionContract:
	sql = fmt.Sprintf("delete from tron_contract_exchange_transaction where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_exchange_transaction table affected %d elasped %s", num, affected, elaspedTime.String())
	//case UpdateSettingContract:
	sql = fmt.Sprintf("delete from tron_contract_update_setting where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_update_setting table affected %d elasped %s", num, affected, elaspedTime.String())
	//case UpdateEnergyLimitContract:
	sql = fmt.Sprintf("delete from tron_contract_update_energy_limit where block_number = %d", num)
	affected, err = tx.Execute(sql)
	if err != nil {
		return err
	}
	elaspedTime = time.Now().Sub(startTime)
	log.Debug("splitter tron: revert block %d from tron_contract_update_energy_limit table affected %d elasped %s", num, affected, elaspedTime.String())

	return nil
}
