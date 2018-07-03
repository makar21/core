import json

import bigchaindb_driver.exceptions

from bigchaindb_driver import BigchainDB
from bigchaindb_driver.crypto import CryptoKeypair, generate_keypair

from pymongo import MongoClient


class Asset:
    def __init__(self, tx, asset_id):
        self.tx = tx
        self.metadata = tx['metadata']
        self.asset_id = asset_id


class DB:
    bdb_root_url = 'http://localhost:9984'
    bdb = BigchainDB(bdb_root_url)

    def connect_to_mongodb(self):
        self.mongo_client = MongoClient('localhost', 27017)
        self.mongo_db = self.mongo_client.bigchain

    def generate_keypair(self):
        self.kp = generate_keypair()

    def export_key(self):
        return json.dumps({
            'private_key': self.kp.private_key,
            'public_key': self.kp.public_key,
        })

    def import_key(self, key):
        d = json.loads(key)
        self.kp = CryptoKeypair(d['private_key'], d['public_key'])

    def create_asset(self, data, metadata, recipients=None):
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
            metadata=metadata,
        )

        fulfilled_create_tx = self.bdb.transactions.fulfill(
            prepared_create_tx, private_keys=self.kp.private_key
        )

        try:
            self.bdb.transactions.send_commit(fulfilled_create_tx)
        except bigchaindb_driver.exceptions.BadRequest as e:
            if not 'already exists' in e.info['message']:
                raise
            created = False
        else:
            created = True

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

    def retrieve_asset(self, asset_id):
        """
        Retrieves transactions for an asset.

        Returns the latest transaction metadata.
        """
        transactions = self.bdb.transactions.get(asset_id=asset_id)

        latest_tx = transactions[-1]

        return Asset(tx=latest_tx, asset_id=asset_id)

    def retrieve_asset_metadata(self, asset_id):
        """
        Retrieves all metadata for an asset.

        Returns a list containing dictionaries
        with the asset’s metadata.
        """
        transactions = self.bdb.transactions.get(asset_id=asset_id)

        return [tx['metadata'] for tx in transactions]

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
            'operation': 'CREATE',
        }
        if created_by_user:
            main_transaction_match[
                'inputs.owners_before'
            ] = self.kp.public_key,
        pipeline = [
            {'$match': main_transaction_match},
            {'$lookup': {
                'from': 'assets',
                'localField': 'id',
                'foreignField': 'id',
                'as': 'assets',
            }},
            {'$match': match},
        ]
        cursor = self.mongo_db.transactions.aggregate(pipeline)

        return (x['id'] for x in cursor)
