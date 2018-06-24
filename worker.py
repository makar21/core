import json
import os
import queue
import sys

from importlib import import_module
from multiprocessing import Process, Lock, Queue

import psutil
import requests

from db import DB, TransactionListener
from encryption import Encryption
from ipfs import IPFS

from const import tasks_code_tmp_dir, progress_report_interval


class Worker(TransactionListener):
    def __init__(self):
        self.db = DB('worker')
        self.bdb = self.db.bdb

        self.encryption = Encryption('worker')

        self.ipfs = IPFS()

        worker_info = {
            'enc_key': self.encryption.get_public_key().decode(),
        }

        self.worker_id = self.db.create_asset(
            name='Worker info',
            data=worker_info,
        )

        try:
            os.mkdir(tasks_code_tmp_dir)
        except FileExistsError:
            pass

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
        db_lock = Lock()
        task_queue = Queue()
        work_process = Process(
            target=self.work,
            args=(transaction, producer_info, db_lock, task_queue),
        )
        work_process.start()
        report_process = Process(
            target=self.report,
            args=(transaction, db_lock, task_queue),
        )
        report_process.start()

    def work(self, transaction, producer_info, db_lock, task_queue):
        task = json.loads(
            self.encryption.decrypt(transaction['metadata']['task']).decode()
        )

        task_code = self.ipfs.read(task['task'])

        path = os.path.join(
            tasks_code_tmp_dir,
            '{}.py'.format(transaction['id']),
        )

        with open(path, 'wb') as f:
            f.write(task_code)

        sys.path.append(tasks_code_tmp_dir)

        try:
            m = import_module(transaction['id'])
            result = str(m.run(*task['args']))
        except Exception as e:
            error_dict = {'exception': type(e).__name__}
            msg = str(e)
            if msg:
                error_dict['message'] = msg
            asset_data = {
                'error': self.encryption.encrypt(
                    json.dumps(error_dict).encode(),
                    producer_info.data['enc_key'],
                ).decode(),
            }
        else:
            asset_data = {
                'result': self.encryption.encrypt(
                    result.encode(),
                    producer_info.data['enc_key'],
                ).decode(),
            }

        db_lock.acquire()
        try:
            self.db.update_asset(
                asset_id=transaction['id'],
                data=asset_data,
                sleep=True,
            )
        finally:
            db_lock.release()

        task_queue.put('finished')

        print('Finished task')

    def report(self, transaction, db_lock, task_queue):
        continue_reporting = True

        while continue_reporting:
            cpu_load = psutil.cpu_percent(interval=progress_report_interval)

            print('Reporting CPU load {}'.format(cpu_load))

            db_lock.acquire()
            try:
                self.db.update_asset(
                    asset_id=transaction['id'],
                    data={
                        'cpu_load': cpu_load,
                    },
                    sleep=True,
                )
            finally:
                db_lock.release()

            try:
                if task_queue.get(block=False) == 'finished':
                    continue_reporting = False
                else:
                    continue_reporting = True
            except queue.Empty:
                continue_reporting = True

    def ping_producer(self, producer_api_url):
        print('Pinging producer')
        r = requests.post(
            '{}/worker/ready/'.format(producer_api_url),
            json={'worker': self.worker_id},
        )


if __name__ == '__main__':
    w = Worker()
    w.run_transaction_listener()
