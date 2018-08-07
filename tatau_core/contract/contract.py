from web3.contract import ImplicitContract
from web3.utils.datastructures import AttributeDict

from tatau_core import settings, web3
from .abi import abi
import time
import hashlib


class Contract:
    def __init__(self):
        self._ccontract = web3.eth.contract(
            address=web3.toChecksumAddress(settings.CONTRACT_ADDRESS),
            abi=abi
        )
        self._icontract = ImplicitContract(classic_contract=self._ccontract)

    @classmethod
    def _wait_for_event(cls, event_filter, tx_hash, timeout=120):
        """
        Wait for event by transaction hash
        :param event_filter: event filter from contract
        :param tx_hash: transaction hash
        :param timeout: wait timeout
        :return: event

        Example usage:
            job_filter = self._ccontract.events.JobIssued.createFilter(
                fromBlock='latest',
                argument_filters={'issuer': web3.eth.defaultAccount}
            )
            self._wait_for_event(job_filter, tx_hash)
        """
        spent_time = 0
        while spent_time < timeout:
            for e in event_filter.get_new_entries():
                if e.transactionHash == tx_hash:
                    return e

            time.sleep(1)
            spent_time += 1

    @classmethod
    def _wait_for_transaction_mined(cls, tx_hash: bytearray, timeout=120):
        """
        Wait for transaction has been mined
        :param tx_hash: transaction hash
        :param timeout: timeout seconds
        :return: receipt
        """
        spent_time = 0
        while spent_time < timeout:
            receipt = web3.eth.waitForTransactionReceipt(tx_hash)
            if len(receipt.logs) and receipt.logs[0].type == 'mined':
                return receipt
            time.sleep(1)
            spent_time += 1
        raise TimeoutError("Transaction timed out {}".format(receipt))

    @classmethod
    def _asset_id_2_job_id(cls, asset_id: str):
        return hashlib.sha256(asset_id.encode()).digest()

    def issue_job(self, task_declaration_id: str, value: int):
        """
        Issue Job
        :param task_declaration_id: task declaration asset id
        :param value: deposit amount
        :return: job id
        """
        _id = self._asset_id_2_job_id(task_declaration_id)
        tx_hash = self._icontract.issueJob(_id, transact={'value': value})
        self._wait_for_transaction_mined(tx_hash=tx_hash)
        return _id

    def deposit(self, task_declaration_id: str, value: int):
        """
        Deposit Job
        :param task_declaration_id: task declaration id
        :param value: amount to deposit
        :return: receipt
        """
        _id = self._asset_id_2_job_id(task_declaration_id)
        tx_hash = self._icontract.deposit(_id, transact={'value': value})
        return self._wait_for_transaction_mined(tx_hash=tx_hash)

    def get_job_balance(self, task_declaration_id: str):
        """
        Get Job
        :param task_declaration_id: task declaration id
        :return: balance
        """
        _id = self._asset_id_2_job_id(task_declaration_id)
        return self._icontract.getJobBalance(_id)

    def distribute(self, task_declaration_id: str, workers: list, amounts: list):
        """
        Payout workers
        :role: validator
        :param task_declaration_id: task declaration id
        :param workers: workers address list
        :param amounts: amounts list for each worker
        :return: receipt
        """
        _id = self._asset_id_2_job_id(task_declaration_id)
        tx_hash = self._icontract.distribute(_id, workers, amounts)
        return self._wait_for_transaction_mined(tx_hash=tx_hash)

    def finish_job(self, task_declaration_id: str):
        """
        Finish Job
        :role: validator
        :param task_declaration_id: task declaration id
        :return: receipt
        """
        _id = self._asset_id_2_job_id(task_declaration_id)
        tx_hash = self._icontract.finishJob(_id)
        return self._wait_for_transaction_mined(tx_hash=tx_hash)
