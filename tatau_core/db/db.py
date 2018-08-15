import json

import bigchaindb_driver.exceptions
import nacl.signing
from bigchaindb_driver import BigchainDB
from bigchaindb_driver.crypto import CryptoKeypair, generate_keypair
from cryptoconditions.crypto import Base58Encoder
from pymongo import MongoClient

from tatau_core import settings
from tatau_core.db import query


class Asset:
    def __init__(self, asset_id, first_tx, last_tx):
        self.asset_id = asset_id
        self.last_tx = last_tx
        self.data = first_tx['asset']['data']
        self.metadata = last_tx['metadata']
        self.created_at = first_tx['generation_time']
        self.modified_at = last_tx['generation_time']


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

    def create_asset(self, data, metadata=None, recipients=None):
        """
        Makes a CREATE transaction in BigchainDB.

        The owner(s) of the asset can be changed
        using the recipients argument.

        Returns a tuple containing 2 elements:

        1. txid: the transaction ID
        2. created: was the asset created
        """
        asset = {
            'data': data,
        }

        prepared_create_tx = self.bdb.transactions.prepare(
            operation='CREATE',
            signers=self.kp.public_key,
            asset=asset,
            recipients=recipients,
            metadata=metadata
        )

        fulfilled_create_tx = self.bdb.transactions.fulfill(
            prepared_create_tx, private_keys=self.kp.private_key
        )

        # TODO: use send_commit and send_sync if commit is timeout
        # (while node is not synced, commit will be with timeout)
        created = True
        try:
            self.bdb.transactions.send_commit(fulfilled_create_tx)
        except bigchaindb_driver.exceptions.BadRequest as e:
            if isinstance(e, bigchaindb_driver.exceptions.BadRequest):
                if not 'already exists' in e.info['message']:
                    raise
                created = False
        except bigchaindb_driver.exceptions.TransportError as e:
            self.bdb.transactions.send_sync(fulfilled_create_tx)

        txid = fulfilled_create_tx['id']
        return (txid, created)

    def update_asset(self, asset_id, metadata, recipients=None):
        """
        Retrieves the list of transactions for the asset and makes
        a TRANSFER transaction in BigchainDB using the output
        of the previous transaction.

        The owner(s) of the asset can be changed
        using the recipients argument.

        Returns txid.
        """
        transactions = self.bdb.transactions.get(asset_id=asset_id)

        previous_tx = transactions[-1]

        transfer_asset = {
            'id': asset_id,
        }

        output_index = 0

        output = previous_tx['outputs'][output_index]

        transfer_input = {
            'fulfillment': output['condition']['details'],
            'fulfills': {
                'output_index': output_index,
                'transaction_id': previous_tx['id'],
            },
            'owners_before': output['public_keys'],
        }

        prepared_transfer_tx = self.bdb.transactions.prepare(
            operation='TRANSFER',
            asset=transfer_asset,
            inputs=transfer_input,
            recipients=(
                recipients or self.kp.public_key
            ),
            metadata=metadata,
        )

        fulfilled_transfer_tx = self.bdb.transactions.fulfill(
            prepared_transfer_tx,
            private_keys=self.kp.private_key,
        )

        self.bdb.transactions.send_commit(fulfilled_transfer_tx)

        txid = fulfilled_transfer_tx['id']

        return txid

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

    def _get_transactions(self, asset_id):
        self.connect_to_mongodb()
        transactions = []
        for transaction_id in query.get_txids_filtered(self.mongo_db, asset_id=asset_id):
            transactions.append(self._get_transaction(transaction_id))
        return transactions

    def retrieve_asset(self, asset_id):
        transactions = self._get_transactions(asset_id)
        latest_tx = transactions[-1]
        return Asset(asset_id=asset_id, first_tx=transactions[0], last_tx=latest_tx)

    def retrieve_asset_transactions(self, asset_id):
        """
        Retrieves transactions for an asset.
        """
        return self._get_transactions(asset_id=asset_id)

    def retrieve_asset_create_tx(self, asset_id):
        """
        Retrieves the CREATE transaction for an asset.

        Returns tx.
        """
        create_tx = self.bdb.transactions.get(
            asset_id=asset_id,
            operation='CREATE'
        )[0]
        return create_tx

    def retrieve_asset_ids(self, match, created_by_user=True, skip=None, limit=None):
        """
        Retreives assets that match to a $match provided as match argument.

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
        Retreives count of assets that match to a $match provided as match argument.

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
