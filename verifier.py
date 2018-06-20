import json

import requests

from db import DB, TransactionListener
from encryption import Encryption


class Verifier(TransactionListener):
    def __init__(self):
        self.db = DB('verifier')
        self.bdb = self.db.bdb

        self.encryption = Encryption('verifier')

        verifier_info = {
            'enc_key': self.encryption.get_public_key().decode(),
        }

        self.verifier_id = self.db.create_asset(
            name='Verifier info',
            data=verifier_info,
        )

    def process_tx(self, data):
        """
        Accepts WS stream data dict and checks if the transaction
        needs to be processed.

        If this is one of verification declaration or verification assignment
        transactions, gets producer information and runs a method
        that processes the transaction.
        """
        transaction = self.bdb.transactions.retrieve(data['transaction_id'])

        if transaction['operation'] != 'CREATE':
            return

        name = transaction['asset']['data'].get('name')

        tx_methods = {
            'Verification declaration': self.process_verification_declaration,
            'Verification assignment': self.process_verification_assignment,
        }

        if name in tx_methods:
            producer_info = self.db.retrieve_asset(
                transaction['metadata']['producer_id']
            )
            tx_methods[name](transaction, producer_info)

    def process_verification_declaration(self, transaction, producer_info):
        print('Received verification declaration')
        producer_api_url = producer_info.data['producer_api_url']
        self.ping_producer(producer_api_url)

    def process_verification_assignment(self, transaction, producer_info):
        print('Received verification assignment')
        task = json.loads(
            self.encryption.decrypt(transaction['metadata']['task']).decode()
        )
        result = self.encryption.decrypt(transaction['metadata']['result']).decode()
        verified = self.verify(task, result)
        self.db.update_asset(
            asset_id=transaction['id'],
            data={'verified': verified},
        )
        print('Finished verification')

    def ping_producer(self, producer_api_url):
        print('Pinging producer')
        r = requests.post(
            '{}/verifier/ready/'.format(producer_api_url),
            json={'verifier': self.verifier_id},
        )

    def verify(self, task, result):
        return True


if __name__ == '__main__':
    v = Verifier()
    v.run_transaction_listener()
