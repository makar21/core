import sqlite3

from bigchaindb_driver import BigchainDB
from bigchaindb_driver.crypto import generate_keypair


class DB:
    bdb_root_url = 'http://localhost:9984'
    bdb = BigchainDB(bdb_root_url)

    sqlite_file = 'assets.db'
    sqlite_conn = sqlite3.connect(sqlite_file)
    sqlite_cursor = sqlite_conn.cursor()

    def __init__(self):
        self.create_database()

    def create_asset(self, name, data):
        """
        Generates a key pair, makes a CREATE transaction in BigchainDB
        and stores the key pair and the asset ID in the database.

        Saves the provided dict as metadata in BigchainDB.

        Returns txid.
        """
        kp = generate_keypair()

        asset = {
            'data': {
                'name': name,
            }
        }

        prepared_create_tx = self.bdb.transactions.prepare(
            operation='CREATE',
            signers=kp.public_key,
            asset=asset,
            metadata=data,
        )

        fulfilled_create_tx = self.bdb.transactions.fulfill(
            prepared_create_tx, private_keys=kp.private_key
        )

        self.bdb.transactions.send(fulfilled_create_tx)

        txid = fulfilled_create_tx['id']

        c = self.sqlite_cursor
        conn = self.sqlite_conn
        c.execute('insert into asset values (?,?,?)', (
            txid, kp.public_key, kp.private_key
        ))
        conn.commit()

        return txid

    def update_asset(self, asset_id, data):
        """
        Retrieves the key pair for the asset from the database,
        retrieves the list of transactions for the asset, and makes
        a TRANSFER transaction in BigchainDB using
        the output of the previous transaction.

        Saves the provided dict as metadata in BigchainDB.

        Returns txid.
        """
        c = self.sqlite_cursor
        c.execute('select public_key, private_key from asset where id=?', (
            asset_id,
        ))
        public_key, private_key = c.fetchone()

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
            recipients=public_key,
            metadata=data,
        )

        fulfilled_transfer_tx = self.bdb.transactions.fulfill(
            prepared_transfer_tx,
            private_keys=private_key,
        )

        self.bdb.transactions.send(fulfilled_transfer_tx)

        return fulfilled_transfer_tx['id']

    def retrieve_asset(self, asset_id):
        """
        Retrieves transactions for an asset.

        Returns the latest transaction metadata.
        """
        transactions = self.bdb.transactions.get(asset_id=asset_id)

        latest_tx = transactions[-1]

        return latest_tx['metadata']

    def create_database(self):
        """
        Creates a database.
        """
        c = self.sqlite_cursor
        conn = self.sqlite_conn
        c.execute(
            'create table if not exists asset ('
            'id int primary key, public_key text, private_key text'
            ')'
        )
        conn.commit()
