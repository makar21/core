import re

import requests

from db import DB, TransactionListener
from encryption import Encryption


class Worker(TransactionListener):
    number_re = re.compile(r'^\d+$')

    def __init__(self):
        self.db = DB('worker')
        self.bdb = self.db.bdb

        self.e = Encryption('worker')

        worker_info = {
            'enc_key': self.e.get_public_key().decode(),
        }

        self.worker_id = self.db.create_asset(
            name='Worker info',
            data=worker_info,
        )

    def process_tx(self, data):
        """
        Accepts WS stream data dict and checks if the transaction
        needs to be processed.

        If this is one of task declaration or task assignment
        transactions, gets producer information and runs a method
        that processes the transaction.
        """
        transaction = self.bdb.transactions.retrieve(data['transaction_id'])

        if transaction['operation'] != 'CREATE':
            return

        name = transaction['asset']['data'].get('name')

        tx_methods = {
            'Task declaration': self.process_task_declaration,
            'Task assignment': self.process_task_assignment,
        }

        if name in tx_methods:
            producer_info = self.db.retrieve_asset(
                transaction['metadata']['producer_id']
            )
            tx_methods[name](transaction, producer_info)

    def process_task_declaration(self, transaction, producer_info):
        print('Received task declaration')
        producer_api_url = producer_info.data['producer_api_url']
        self.ping_producer(producer_api_url)

    def process_task_assignment(self, transaction, producer_info):
        print('Received task assignment')
        task = self.e.decrypt(transaction['metadata']['task']).decode()
        result = str(self.work(task))
        if result:
            self.db.update_asset(
                asset_id=transaction['id'],
                data={
                    'result': self.e.encrypt(
                        result.encode(),
                        producer_info.data['enc_key'],
                    ).decode()
                },
            )
            print('Finished task')

    def ping_producer(self, producer_api_url):
        print('Pinging producer')
        r = requests.post(
            '{}/worker/ready/'.format(producer_api_url),
            json={'worker': self.worker_id},
        )

    def work(self, task):
        values = task.split('+')
        # For simplicity: only whole positive numbers
        if all(self.number_re.match(i) for i in values):
            result = sum([int(i) for i in values])
            return result


if __name__ == '__main__':
    w = Worker()
    w.run_transaction_listener()
