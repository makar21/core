import signal
import sys

from multiprocessing import Process

from bottle import Bottle, request, run
from ipfs import IPFS

from db import DB, TransactionListener
from encryption import Encryption
from tasks import Task, TaskManager


class Producer(TransactionListener):
    producer_api_url = 'http://localhost:8080'

    def __init__(self):
        self.db = DB('producer')
        self.bdb = self.db.bdb

        self.encryption = Encryption('producer')

        self.ipfs = IPFS()

        self.tm = TaskManager(
            db=self.db,
            ipfs=self.ipfs,
            encryption=self.encryption,
        )

        producer_info = {
            'enc_key': self.encryption.get_public_key().decode(),
            'producer_api_url': self.producer_api_url,
        }

        self.producer_id = self.db.create_asset(
            name='Producer info',
            data=producer_info,
        )

    def add_task(self):
        task = Task(
            db=self.db,
            ipfs=self.ipfs,
            encryption=self.encryption,
            producer_id=self.producer_id,
            args=request.json['args'],
            workers_needed=1,
        )
        task.upload_to_ipfs(request.json['f'])
        task.save_task_declaration()
        print('Created task declaration {}'.format(
            task.td_asset_id
        ))

    def worker_ready(self):
        task = self.tm.pick_worker_task()

        if not task:
            return {'status': 'ok', 'msg': 'No task.'}

        worker_id = request.json['worker']

        asset_id = task.create_task_assignment(worker_id)

        print('Created task assignment {}'.format(asset_id))

        task.workers_needed -= 1
        task.save_task_declaration()

        print('Updated task declaration')

        return {'status': 'ok'}

    def verifier_ready(self):
        task = self.tm.pick_verifier_task()

        if not task:
            return {'status': 'ok', 'msg': 'No task.'}

        task_assignment_asset = self.db.retrieve_asset(task.ta_asset_id)
        result = task_assignment_asset.data.get('result')

        if not result:
            return {'status': 'ok', 'msg': 'No result.'}

        verifier_id = request.json['verifier']

        asset_id = task.create_verification_assignment(
            verifier_id,
            result,
        )

        print('Created verification assignment {}'.format(asset_id))

        task.verifiers_needed -= 1
        task.save_verification_declaration()

        print('Updated verification declaration')

        return {'status': 'ok'}

    def process_tx(self, data):
        """
        Accepts WS stream data dict and checks if the transaction
        needs to be processed.

        If this is one of task assignment or verification assignment
        transactions, runs a method that processes the transaction.
        """
        transaction = self.bdb.transactions.retrieve(data['transaction_id'])

        if transaction['operation'] != 'TRANSFER':
            return

        asset_create_tx = self.db.retrieve_asset_create_tx(data['asset_id'])

        name = asset_create_tx['asset']['data'].get('name')

        tx_methods = {
            'Task assignment': self.process_task_assignment,
            'Verification assignment': self.process_verification_assignment,
        }

        if name in tx_methods:
            tx_methods[name](transaction)

    def process_task_assignment(self, transaction):
        result = transaction['metadata'].get('result')

        if not result:
            return

        decrypted_result = self.encryption.decrypt(result).decode()
        print('Received task result: {}'.format(decrypted_result))

        ta_asset_id = transaction['asset']['id']

        task_assignment_create_tx = self.bdb.transactions.retrieve(
            ta_asset_id,
        )

        td_asset_id = task_assignment_create_tx['metadata']['td_asset_id']
        td_asset = self.db.retrieve_asset(td_asset_id)

        task = self.tm.get_task(td_asset)

        task.verifiers_needed = 1
        task.ta_asset_id = ta_asset_id

        task.save_verification_declaration()

        print('Created verification declaration {}'.format(
            task.vd_asset_id
        ))

    def process_verification_assignment(self, transaction):
        verified = transaction['metadata'].get('verified')

        if verified:
            print('Task result is verified')
        else:
            print('Task result is not verified')


def web_server(producer):
    producer.db.connect_to_mongodb()
    bottle = Bottle()
    bottle.post('/add_task/')(producer.add_task)
    bottle.post('/worker/ready/')(producer.worker_ready)
    bottle.post('/verifier/ready/')(producer.verifier_ready)
    run(bottle, host='localhost', port=8080)


def tx_stream_client(producer):
    producer.run_transaction_listener()


def sigint_handler(signal, frame):
    print('Exiting')
    sys.exit(0)


if __name__ == '__main__':
    p = Producer()

    web_server_process = Process(target=web_server, args=(p,))
    web_server_process.start()

    tx_stream_client_process = Process(target=tx_stream_client, args=(p,))
    tx_stream_client_process.start()

    signal.signal(signal.SIGINT, sigint_handler)
