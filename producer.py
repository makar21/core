import signal
import sys

from multiprocessing import Process

from bottle import Bottle, request, run
from ipfs import IPFS

from db import DB, TransactionListener
from encryption import Encryption
from task import Task
from task_manager import TaskManager


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

        self.task = Task(
            ipfs=self.ipfs,
            encryption=self.encryption,
            producer_id=self.producer_id,
            args=([2, 2],),
        )

        self.task.upload_to_ipfs('tasks_code/sum.py')

    def create_task_declaration(self):
        self.task.task_declaration_asset_id = self.db.create_asset(
            name='Task declaration',
            data=self.task.task_declaration,
        )
        print('Created task declaration {}'.format(
            self.task.task_declaration_asset_id
        ))

    def worker_ready(self):
        task = self.tm.pick_worker_task()

        worker_id = request.json['worker']
        worker_info = self.db.retrieve_asset(worker_id)

        task_assignment = {
            'worker': worker_id,
            'task': self.encryption.encrypt(
                task.json_str.encode(),
                worker_info.data['enc_key'],
            ).decode(),
            'producer_id': task.producer_id,
        }

        task_assignment_asset_id = self.db.create_asset(
            name='Task assignment',
            data=task_assignment,
            recipients=worker_info.tx['outputs'][0]['public_keys'][0],
        )

        print('Created task assignment {}'.format(
            task_assignment_asset_id
        ))

        return {'status': 'ok'}

    def verifier_ready(self):
        if not self.task.assigned:
            return {'status': 'ok', 'msg': 'Not assigned.'}

        task_assignment_asset = self.db.retrieve_asset(
            self.task.task_assignment_asset_id
        )
        result = task_assignment_asset.data.get('result')

        if not result:
            return {'status': 'ok', 'msg': 'No result.'}

        decrypted_result = self.encryption.decrypt(result).decode()

        self.task.verifiers_found += 1
        if self.task.verifiers_found == self.task.verifiers_needed:
            verifier_id = request.json['verifier']
            verifier_info = self.db.retrieve_asset(verifier_id)
            verification_assignment = {
                'verifier': verifier_id,
                'task': self.encryption.encrypt(
                    self.task.json_str.encode(),
                    verifier_info.data['enc_key'],
                ).decode(),
                'result': self.encryption.encrypt(
                    decrypted_result.encode(),
                    verifier_info.data['enc_key'],
                ).decode(),
                'producer_id': self.producer_id,
            }
            self.task.verification_assignment_asset_id = self.db.create_asset(
                name='Verification assignment',
                data=verification_assignment,
                recipients=verifier_info.tx['outputs'][0]['public_keys'][0],
            )
            print('Created verification assignment {}'.format(
                self.task.verification_assignment_asset_id
            ))
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

        self.task.verification_declaration_asset_id = self.db.create_asset(
            name='Verification declaration',
            data=self.task.verification_declaration,
        )
        print('Created verification declaration {}'.format(
            self.task.verification_declaration_asset_id
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
    p.create_task_declaration()

    web_server_process = Process(target=web_server, args=(p,))
    web_server_process.start()

    tx_stream_client_process = Process(target=tx_stream_client, args=(p,))
    tx_stream_client_process.start()

    signal.signal(signal.SIGINT, sigint_handler)
