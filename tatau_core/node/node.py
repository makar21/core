import hashlib
import os
import shutil
import tempfile
from logging import getLogger
from multiprocessing import Process

from tatau_core import web3
from tatau_core.db import DB
from tatau_core.settings import ROOT_DIR
from tatau_core.utils.encryption import Encryption
from tatau_core.utils.ipfs import IPFS

logger = getLogger('tatau_core')


# noinspection PyMethodMayBeStatic
class Node:

    # should be rename by child classes
    asset_class = None

    def __init__(self, account_address, rsa_pk_fs_name=None, rsa_pk=None, *args, **kwargs):
        self.db = DB()
        self.bdb = self.db.bdb
        self.encryption = Encryption()

        if rsa_pk_fs_name:
            self._handle_fs_key(rsa_pk_fs_name)
        else:
            self.encryption.import_key(rsa_pk)
            seed = hashlib.sha256(rsa_pk).digest()
            self.db.generate_keypair(seed=seed)

        self.asset = self._create_info_asset(account_address=account_address)

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

    def _create_info_asset(self, account_address):
        normalized_account_address = web3.toChecksumAddress(account_address)
        node_assets = self.asset_class.list(db=self.db, encryption=self.encryption)
        assert len(node_assets) <= 1

        if len(node_assets) == 1:
            node_asset = node_assets[0]
            # update account_address
            if normalized_account_address != node_asset.account_address:
                node_asset.account_address = normalized_account_address
                node_asset.save()
            return node_asset
        else:
            return self.asset_class.create(
                enc_key=self.encryption.get_public_key().decode(),
                account_address=normalized_account_address,
                db=self.db,
                encryption=self.encryption
            )

    def _ipfs_prefetch_async(self, multihash):
        Process(
            target=self._ipfs_prefetch,
            args=(multihash,)
        ).start()

    def _ipfs_prefetch(self, multihash):
        ipfs = IPFS()
        target_dir = tempfile.mkdtemp()
        try:
            logger.info('Download {}'.format(multihash))
            ipfs.download(multihash, target_dir)
            logger.info('End download {}'.format(multihash))
        finally:
            shutil.rmtree(target_dir)

    def _run_session(self, assignment, session):
        failed = False

        try:
            session.process_assignment(assignment=assignment)
        except Exception as ex:
            self._dump_error(assignment, ex)
            failed = True
        finally:
            session.clean()

        return failed, session.get_tflops()

    def _parse_exception(self, ex: Exception):
        error_dict = {'exception': type(ex).__name__}
        msg = str(ex)
        if msg:
            error_dict['message'] = msg

        return error_dict

    def _dump_error(self, assignment, ex: Exception):
        raise NotImplemented
