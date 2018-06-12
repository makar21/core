import json
import signal
import sys

from multiprocessing import Process

import websocket

from bottle import Bottle, request, run

from db import DB
from encryption import Encryption
from task import Task
from const import valid_transactions_stream_url


class Producer:
    producer_api_url = 'http://localhost:8080'

    def __init__(self):
        self.db = DB('producer')
        self.bdb = self.db.bdb

        self.e = Encryption('producer')

        producer_info = {
            'enc_key': self.e.get_public_key().decode(),
            'producer_api_url': self.producer_api_url,
        }

        self.producer_id = self.db.create_asset(
            name='Producer info',
            data=producer_info,
        )

        self.task = Task(
            producer_id=self.producer_id,
            task='2+2',
        )

    def create_task_declaration(self):
        self.task_declaration_asset_id = self.db.create_asset(
            name='Task declaration',
            data=self.task.task_declaration,
        )
        print('Created task declaration {}'.format(
            self.task_declaration_asset_id
        ))

    def ready(self):
        if self.task.assigned:
            return {'status': 'ok', 'msg': 'Already assigned.'}
        self.task.workers_found += 1
        if self.task.workers_found == self.task.workers_needed:
            worker_id = request.json['worker']
            worker_info = self.db.retrieve_asset(worker_id)
            task_assignment = {
                'worker': worker_id,
                'task': self.e.encrypt(
                    self.task.task.encode(),
                    worker_info.data['enc_key'],
                ).decode(),
                'producer_id': self.producer_id,
            }
            self.task_assignment_asset_id = self.db.create_asset(
                name='Task assignment',
                data=task_assignment,
                recipients=worker_info.tx['outputs'][0]['public_keys'][0],
            )
            self.task.assigned = True
            print('Created task assignment {}'.format(
                self.task_assignment_asset_id
            ))
        return {'status': 'ok'}

    def on_message(self, ws, message):
        data = json.loads(message)
        self.process_tx(data)

    def on_error(self, ws, error):
        print(error)

    def on_close(self, ws):
        print('WS connection closed')

    def on_open(self, ws):
        print('WS connection opened')

    def process_tx(self, data):
        """
        Accepts WS stream data dict and checks if the transaction
        needs to be processed.

        If this is task result, print it.
        """
        transaction = self.bdb.transactions.retrieve(data['transaction_id'])

        if transaction['operation'] != 'TRANSFER':
            return

        asset_create_tx = self.db.retrieve_asset_create_tx(data['asset_id'])

        name = asset_create_tx['asset']['data'].get('name')

        if name != 'Task assignment':
            return

        result = transaction['metadata'].get('result')

        if result:
            print('Received task result: {}'.format(result))


def web_server(producer):
    bottle = Bottle()
    bottle.post('/ready/')(producer.ready)
    run(bottle, host='localhost', port=8080)


def tx_stream_client(producer):
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(
        valid_transactions_stream_url,
        on_message=producer.on_message,
        on_error=producer.on_error,
        on_close=producer.on_close,
    )
    ws.on_open = producer.on_open
    ws.run_forever()


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
