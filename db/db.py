import json
import os
import time

from bigchaindb_driver import BigchainDB
from bigchaindb_driver.crypto import CryptoKeypair, generate_keypair

from pymongo import MongoClient

from const import update_asset_sleep_time


class Asset:
    def __init__(self, tx, asset_id):
        self.tx = tx
        self.metadata = tx['metadata']
        self.asset_id = asset_id


class DB:
    bdb_root_url = 'http://localhost:9984'
    bdb = BigchainDB(bdb_root_url)

    def __init__(self, name):
        d = os.path.dirname(os.path.abspath(__file__))
        path = os.path.join(d, 'keys/bigchaindb/{}.json'.format(name))
        if os.path.isfile(path):
            self.import_key(path)
        else:
            os.makedirs(os.path.join(d, 'keys/bigchaindb'), exist_ok=True)
            self.kp = generate_keypair()
            self.export_key(path)

    def connect_to_mongodb(self):
        self.mongo_client = MongoClient('localhost', 9986)
        self.mongo_db = self.mongo_client.bigchain

    def export_key(self, fn):
        with open(fn, 'w') as f:
            f.write(json.dumps({
                'private_key': self.kp.private_key,
                'public_key': self.kp.public_key,
            }))

    def import_key(self, fn):
        with open(fn, 'r') as f:
            d = json.loads(f.read())
        self.kp = CryptoKeypair(d['private_key'], d['public_key'])

    def create_asset(self, data, metadata, recipients=None):
        """
        Makes a CREATE transaction in BigchainDB.

        The owner(s) of the asset can be changed
        using the recipients argument.

        Returns txid.
        """
        asset = {
            'data': data,
        }

        prepared_create_tx = self.bdb.transactions.prepare(
            operation='CREATE',
            signers=self.kp.public_key,
            asset=asset,
            recipients=recipients,
            metadata=metadata,
        )

        fulfilled_create_tx = self.bdb.transactions.fulfill(
            prepared_create_tx, private_keys=self.kp.private_key
        )

        self.bdb.transactions.send(fulfilled_create_tx)

        txid = fulfilled_create_tx['id']

        return txid

    def update_asset(self, asset_id, metadata, recipients=None, sleep=False):
        """
        Retrieves the list of transactions for the asset and makes
        a TRANSFER transaction in BigchainDB using the output
        of the previous transaction.

        The owner(s) of the asset can be changed
        using the recipients argument.

        If sleep is True, sleep for update_asset_sleep_time seconds
        after submitting the transaction. This should be used
        when several update transactions on the same asset occur
        within a short time.

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

        self.bdb.transactions.send(fulfilled_transfer_tx)

        if sleep:
            time.sleep(update_asset_sleep_time)

        txid = fulfilled_transfer_tx['id']

        return txid

    def retrieve_asset(self, asset_id):
        """
        Retrieves transactions for an asset.

        Returns the latest transaction metadata.
        """
        transactions = self.bdb.transactions.get(asset_id=asset_id)

        latest_tx = transactions[-1]

        return Asset(tx=latest_tx, asset_id=asset_id)

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

    def retrieve_asset_ids(self, match, created_by_user=True):
        """
        Retreives assets that match to a $match provided as match argument.

        If created_by_user is True, only retrieves
        the assets created by the user.

        Returns a generator object.
        """
        main_transaction_match = {
            'block.transactions.operation': 'CREATE',
        }
        if created_by_user:
            main_transaction_match[
                'block.transactions.inputs.owners_before'
            ] = self.kp.public_key,
        pipeline = [
            {'$match': main_transaction_match},
            {'$unwind': '$block.transactions'},
            {'$match': main_transaction_match},
            {'$lookup': {
                'from': 'assets',
                'localField': 'block.transactions.id',
                'foreignField': 'id',
                'as': 'assets',
            }},
            {'$match': match},
        ]
        cursor = self.mongo_db.bigchain.aggregate(pipeline)

        return (x['block']['transactions']['id'] for x in cursor)
