import hashlib
import logging
import os

from tatau_core.db import DB, TransactionListener, NodeInfo
from tatau_core.utils.encryption import Encryption

from tatau_core.settings import ROOT_DIR


log = logging.getLogger()


class Node(TransactionListener):
    class NodeType:
        PRODUCER = 'producer'
        WORKER = 'worker'
        VERIFIER = 'verifier'

    # should be rename by child classes
    node_type = None
    asset_class = None

    def __init__(self, rsa_pk_fs_name=None, rsa_pk=None, *args, **kwargs):
        self.db = DB()
        self.bdb = self.db.bdb
        self.encryption = Encryption()
        NodeInfo.configure(self.db, self.encryption)

        if rsa_pk_fs_name:
            self.handle_fs_key(rsa_pk_fs_name)
        else:
            self.encryption.import_key(rsa_pk)
            seed = hashlib.sha256(rsa_pk).digest()
            self.db.generate_keypair(seed=seed)

        self.asset = self.create_info_asset()

    def __str__(self):
        return self.asset.__str__()

    @property
    def asset_id(self):
        return self.asset.asset_id

    def handle_fs_key(self, name):
        path = os.path.join(ROOT_DIR, 'keys/{}.pem'.format(name))
        if os.path.isfile(path):
            with open(path, 'rb') as f:
                rsa_pk = f.read()
            self.encryption.import_key(rsa_pk)
        else:
            os.makedirs(os.path.join(ROOT_DIR, 'keys'), exist_ok=True)
            self.encryption.generate_key()
            rsa_pk = self.encryption.export_key()
            with open(path, 'wb') as f:
                f.write(rsa_pk)
        seed = hashlib.sha256(rsa_pk).digest()
        self.db.generate_keypair(seed=seed)

    def create_info_asset(self):
        node_assets = list(self.asset_class.list())
        if len(node_assets) > 1:
            log.fatal('WTF?, this should not happen.')
        elif len(node_assets) == 1:
            return node_assets[0]
        else:
            return self.asset_class.create(enc_key=self.encryption.get_public_key().decode())

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

        name = asset_create_tx['asset']['data'].get('asset_name')
        log.debug('{} process tx of "{}": {}'.format(self, name, asset_id))

        tx_methods = self.get_tx_methods()
        if name in tx_methods:
            tx_methods[name](asset_id, transaction)
        else:
            log.debug('{} skip tx of "{}": {}'.format(self, name, asset_id))

    def get_tx_methods(self):
        raise NotImplemented

    def ignore_operation(self, operation):
        return False

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
