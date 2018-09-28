import json
import time
from collections import deque
from logging import getLogger

import nacl.signing
from bigchaindb_driver import BigchainDB
from bigchaindb_driver.crypto import CryptoKeypair, generate_keypair
from cryptoconditions.crypto import Base58Encoder
from pymongo import MongoClient

from tatau_core import settings
from tatau_core.db import query
from tatau_core.utils.signleton import singleton

logger = getLogger('tatau_core')


@singleton
class async_commit:
    def __init__(self):
        self.async = False
        self.transaction_ids = deque()
        self._counter = 0

    def add_tx_id(self, tx_id):
        self.transaction_ids.append(tx_id)

    def __enter__(self):
        self._counter += 1
        self.async = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._counter -= 1
        if self._counter != 0:
            return

        self.async = False
        while len(self.transaction_ids):
            tx = self.transaction_ids[0]

            if not DB.bdb.blocks.get(txid=tx):
                logger.debug('Tx {} is not committed'.format(tx))
                time.sleep(1)
                continue

            logger.debug('Tx {} is committed'.format(tx))
            self.transaction_ids.popleft()

        self.transaction_ids.clear()


def use_async_commits(func):
    def wrapper(*args):
        with async_commit():
            return func(*args)
    return wrapper


class DB:
    bdb = BigchainDB(settings.BDB_ROOT_URL)

    def __init__(self):
        self.mongo_client = None
        self.mongo_db = None
        self.kp = None

    def connect_to_mongodb(self):
        if self.mongo_db is None or self.mongo_client is None:
            self.mongo_client = MongoClient(
                settings.MONGO_DB_HOST,
                settings.MONGO_DB_PORT
            )
            self.mongo_db = self.mongo_client.bigchain

    def generate_keypair(self, seed=None):
        if seed:
            sk = nacl.signing.SigningKey(seed=seed)
            self.kp = CryptoKeypair(
                sk.encode(encoder=Base58Encoder).decode(),
                sk.verify_key.encode(encoder=Base58Encoder).decode()
            )
        else:
            self.kp = generate_keypair()

    def export_key(self):
        return json.dumps({
            'private_key': self.kp.private_key,
            'public_key': self.kp.public_key,
        })

    def import_key(self, key):
        d = json.loads(key)
        self.kp = CryptoKeypair(d['private_key'], d['public_key'])

    def _get_transaction(self, transaction_id):
        transaction = query.get_transaction(self.mongo_db, transaction_id)

        if transaction:
            transaction['generation_time'] = transaction.pop('_id').generation_time
            asset = query.get_asset(self.mongo_db, transaction_id)
            metadata = query.get_metadata(self.mongo_db, [transaction_id])
            if asset:
                transaction['asset'] = asset

            if 'metadata' not in transaction:
                metadata = metadata[0] if metadata else None
                if metadata:
                    metadata = metadata.get('metadata')

                transaction.update({'metadata': metadata})

        return transaction

    def get_transactions(self, asset_id):
        self.connect_to_mongodb()
        transactions = []
        for transaction_id in query.get_txids_filtered(self.mongo_db, asset_id=asset_id):
            transactions.append(self._get_transaction(transaction_id))
        return transactions

    def retrieve_asset_ids(self, match, created_by_user=True, skip=None, limit=None):
        """
        Retrieves assets that match to a $match provided as match argument.

        If created_by_user is True, only retrieves
        the assets created by the user.

        Returns a generator object.
        """
        main_transaction_match = {
            'operation': 'CREATE',
        }
        if created_by_user:
            main_transaction_match['inputs.owners_before'] = self.kp.public_key

        pipeline = [
            {'$match': main_transaction_match},
            {'$lookup': {
                'from': 'assets',
                'localField': 'id',
                'foreignField': 'id',
                'as': 'assets',
            }},
            {'$match': match},
            # by default sort by -created_at
            {'$sort': {'_id': -1}}
        ]

        if skip:
            pipeline.append({'$skip': skip})

        if limit:
            pipeline.append({'$limit': limit})

        return (x['id'] for x in self.mongo_db.transactions.aggregate(pipeline))

    def retrieve_asset_count(self, match, created_by_user=True):
        """
        Retrieves count of assets that match to a $match provided as match argument.

        If created_by_user is True, only retrieves
        the assets created by the user.

        Returns a generator object.
        """
        main_transaction_match = {
            'operation': 'CREATE',
        }
        if created_by_user:
            main_transaction_match['inputs.owners_before'] = self.kp.public_key

        pipeline = [
            {'$match': main_transaction_match},
            {'$lookup': {
                'from': 'assets',
                'localField': 'id',
                'foreignField': 'id',
                'as': 'assets',
            }},
            {'$match': match},
            {'$count': 'count'}
        ]

        for cursor in self.mongo_db.transactions.aggregate(pipeline):
            return cursor['count']

        return 0
