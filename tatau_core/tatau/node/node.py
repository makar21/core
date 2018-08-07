import hashlib
import os
import shutil
import tempfile
from logging import getLogger
from multiprocessing import Process

from tatau_core.db import DB, TransactionListener, NodeInfo
from tatau_core.settings import ROOT_DIR
from tatau_core.utils.encryption import Encryption
from tatau_core.utils.ipfs import IPFS

logger = getLogger()


class Node(TransactionListener):

    # should be rename by child classes
    asset_class = None

    def __init__(self, rsa_pk_fs_name=None, rsa_pk=None, *args, **kwargs):
        self.db = DB()
        self.bdb = self.db.bdb
        self.encryption = Encryption()
        NodeInfo.configure(self.db, self.encryption)

        if rsa_pk_fs_name:
            self._handle_fs_key(rsa_pk_fs_name)
        else:
            self.encryption.import_key(rsa_pk)
            seed = hashlib.sha256(rsa_pk).digest()
            self.db.generate_keypair(seed=seed)

        self.asset = self._create_info_asset()

    def __str__(self):
        return self.asset.__str__()

    @property
    def asset_id(self):
        return self.asset.asset_id

    def _handle_fs_key(self, name):
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

    def _create_info_asset(self):
        node_assets = self.asset_class.list()
        assert len(node_assets) <= 1

        if len(node_assets) == 1:
            return node_assets[0]
        else:
            return self.asset_class.create(enc_key=self.encryption.get_public_key().decode())

    def _process_tx(self, data):
        """
        Accepts WS stream data dict and checks if the transaction
        needs to be processed.

        If this is one of task assignment or verification assignment
        transactions, runs a method that processes the transaction.
        """
        transaction = self.bdb.transactions.retrieve(data['transaction_id'])

        if self._ignore_operation(transaction['operation']):
            return

        asset_id = data['asset_id']
        asset_create_tx = self.db.retrieve_asset_create_tx(asset_id)

        name = asset_create_tx['asset']['data'].get('asset_name')
        logger.debug('{} process tx of "{}": {}'.format(self, name, asset_id))

        tx_methods = self._get_tx_methods()
        if name in tx_methods:
            tx_methods[name](asset_id, transaction)
        else:
            logger.debug('{} skip tx of "{}": {}'.format(self, name, asset_id))

    def _get_tx_methods(self):
        raise NotImplemented

    def _ignore_operation(self, operation):
        return False

    def _download_file_from_ipfs_async(self, multihash):
        Process(
            target=self._download_file_from_ipfs,
            args=(multihash,)
        ).start()

    def _download_file_from_ipfs(self, multihash):
        ipfs = IPFS()
        target_dir = tempfile.mkdtemp()
        try:
            logger.info('Download {}'.format(multihash))
            ipfs.download(multihash, target_dir)
            logger.info('End download {}'.format(multihash))
        finally:
            shutil.rmtree(target_dir)