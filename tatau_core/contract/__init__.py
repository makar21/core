from web3.personal import Personal

from tatau_core import web3
from .abi import abi
from .contract import Contract


class NodeContractInfo:
    _encrypted_key = None
    _keyfile_pass = None
    _account = None
    _personal = None
    _contract = Contract()

    @classmethod
    def configure(cls, encrypted_key, keyfile_pass):
        cls._encrypted_key = encrypted_key
        cls._keyfile_pass = keyfile_pass

        private_key = web3.eth.account.decrypt(encrypted_key, keyfile_pass)
        cls._account = web3.eth.account.privateKeyToAccount(private_key)
        cls._personal = Personal(web3=web3)

        cls._personal.unlockAccount(cls._account.address, keyfile_pass)

        web3.eth.defaultAccount = cls._account.address

    @classmethod
    def unlock_account(cls):
        cls._personal.unlockAccount(cls._account.address, cls._keyfile_pass)

    @classmethod
    def get_contract(cls):
        return cls._contract

    @classmethod
    def get_account_address(cls):
        return cls._account.address

    @classmethod
    def init_poa(cls, key_name):

        with open('parity/sandbox/keys/{}.json'.format(key_name)) as keyfile:
            encrypted_key = keyfile.read()

        with open('parity/sandbox/wallet/{}.pass'.format(key_name)) as passfile:
            keyfile_pass = passfile.read()

        NodeContractInfo.configure(encrypted_key, keyfile_pass)
