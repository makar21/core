import os
import logging

from db import DB, TransactionListener
from encryption import Encryption

from settings import ROOT_DIR


logger = logging.getLogger()


class Node(TransactionListener):
    class NodeType:
        PRODUCER = 'producer'
        WORKER = 'worker'
        VERIFIER = 'verifier'

    # should be rename by child classes
    node_type = None
    asset_name = None

    def __init__(self, fs_keys_name=None, db_key=None, encryption_key=None):
        self.db = DB()
        self.bdb = self.db.bdb
        self.encryption = Encryption()

        if fs_keys_name:
            self.handle_fs_keys(fs_keys_name)
        else:
            self.db.import_key(db_key)
            self.encryption.import_key(encryption_key)

        self.asset_id = self.create_info_asset()

    def handle_fs_keys(self, name):
        path = os.path.join(ROOT_DIR, 'keys/bigchaindb/{}.json'.format(name))
        if os.path.isfile(path):
            with open(path, 'r') as f:
                self.db.import_key(f.read())
        else:
            os.makedirs(os.path.join(ROOT_DIR, 'keys/bigchaindb'), exist_ok=True)
            self.db.generate_keypair()
            with open(path, 'w') as f:
                f.write(self.db.export_key())

        path = os.path.join(ROOT_DIR, 'keys/encryption/{}.pem'.format(name))
        if os.path.isfile(path):
            with open(path, 'rb') as f:
                self.encryption.import_key(f.read())
        else:
            os.makedirs(os.path.join(ROOT_DIR, 'keys/encryption'), exist_ok=True)
            self.encryption.generate_key()
            with open(path, 'wb') as f:
                f.write(self.encryption.export_key())

    def create_info_asset(self):
        asset_id = self.db.create_asset(
            data={'name': self.asset_name},
            metadata=self.get_node_info(),
        )

        logging.info('{} created info asset: {}'.format(self.node_type, asset_id))
        return asset_id

    def get_node_info(self):
        raise NotImplemented

    def process_tx(self, data):
        """
        Accepts WS stream data dict and checks if the transaction
        needs to be processed.

        If this is one of task assignment or verification assignment
        transactions, runs a method that processes the transaction.
        """
        transaction = self.bdb.transactions.retrieve(data['transaction_id'])

        if self.ignore_operation(transaction['operation']):
            return

        asset_id = data['asset_id']
        asset_create_tx = self.db.retrieve_asset_create_tx(asset_id)

        name = asset_create_tx['asset']['data'].get('name')

        tx_methods = self.get_tx_methods()
        if name in tx_methods:
            tx_methods[name](asset_id, transaction)

    def get_tx_methods(self):
        raise NotImplemented

    def ignore_operation(self, operation):
        raise NotImplemented

    def encrypt_text(self, text):
        return self.encryption.encrypt(
            text.encode(),
            self.encryption.get_public_key()
        ).decode()

    def decrypt_text(self, encrypted_text):
        try:
            return self.encryption.decrypt(encrypted_text.encode()).decode()
        except ValueError:
            return encrypted_text
