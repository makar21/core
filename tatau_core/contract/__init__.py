from web3.personal import Personal

from tatau_core import web3
from .abi import abi
from .contract import Contract


class NodeContractInfo:
    _encrypted_key = None
    _password = None
    _account = None
    _personal = None
    _contract = Contract()

    @classmethod
    def configure(cls, encrypted_key, password):
        cls._encrypted_key = encrypted_key
        cls._password = password

        private_key = web3.eth.account.decrypt(encrypted_key, password)
        cls._account = web3.eth.account.privateKeyToAccount(private_key)
        cls._personal = Personal(web3=web3)

        cls._personal.unlockAccount(cls._account.address, password)

        web3.eth.defaultAccount = cls._account.address

    @classmethod
    def unlock_account(cls):
        cls._personal.unlockAccount(cls._account.address, cls._password)

    @classmethod
    def get_contract(cls):
        return cls._contract

    @classmethod
    def get_account_address(cls):
        return cls._account.address
