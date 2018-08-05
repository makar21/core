from web3.contract import ImplicitContract
from web3.utils.datastructures import AttributeDict

from tatau_core import settings, web3
from .abi import abi
import time


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
        """
        spent_time = 0
        while spent_time < timeout:
            for e in event_filter.get_new_entries():
                if e.transactionHash == tx_hash:
                    return e

            time.sleep(1)
            spent_time += 1

    def issue_job(self, amount: int):
        """
        Issue Job
        :param amount: deposit amount
        :return: job index
        """
        tx_hash = self._icontract.issueJob(transact={'value': amount})

        job_filter = self._ccontract.events.JobIssued.createFilter(
            fromBlock='latest',
            argument_filters={'issuer': web3.eth.defaultAccount}
        )
        event = self._wait_for_event(job_filter, tx_hash)
        return event.args.index

    def deposit(self, index: int, amount: int):
        """
        Deposit Job
        :param index: job index
        :param amount: amount
        :return: receipt
        """
        tx_hash = self._icontract.deposit(index, transact={'value': amount})
        return web3.eth.waitForTransactionReceipt(tx_hash)

    def get_job(self, index: int):
        """
        Get Job
        :param index: job index
        :return: job
        """
        result = self._icontract.jobs(index)
        return AttributeDict(dict(issuer=result[0], balance=result[1], finished=result[2], index=index))

