import json
import os
import queue
import shutil
import sys
import tempfile
from importlib import import_module
from multiprocessing import Process, Lock, Queue

import psutil
import requests

from tatau_core import settings
from tatau_core.const import progress_report_interval
from tatau_core.ipfs import IPFS
from ..tasks import Task, TaskDeclaration, TaskAssignment
from .node import Node


class Worker(Node):
    node_type = Node.NodeType.WORKER

    key_name = 'worker'
    asset_name = 'Worker info'

    def get_node_info(self):
        return {
            'enc_key': self.encryption.get_public_key().decode(),
        }

    def get_tx_methods(self):
        return {
            Task.TaskType.TASK_DECLARATION: self.process_task_declaration,
            Task.TaskType.TASK_ASSIGNMENT: self.process_task_assignment,
        }

    def ignore_operation(self, operation):
        return operation in ['TRANSFER']

    def process_task_declaration(self, asset_id, transaction):
        task_declaration = TaskDeclaration.get(self, asset_id)
        print('Received task declaration asset:{}, producer:{}, workers_needed: {}'.format(
            asset_id, task_declaration.owner_producer_id, task_declaration.workers_needed))

        if task_declaration.workers_needed == 0:
            return

        producer_info = self.db.retrieve_asset(task_declaration.owner_producer_id).metadata
        producer_api_url = producer_info['producer_api_url']
        self.ping_producer(asset_id, producer_api_url)

    def process_task_assignment(self, asset_id, transaction):
        print('Received task assignment asset:{}'.format(asset_id))

        db_lock = Lock()
        task_queue = Queue()

        process_class = Process
        if settings.DEBUG:
            import threading
            process_class = threading.Thread

        work_process = process_class(
            target=self.work,
            args=(asset_id, db_lock, task_queue),
        )
        work_process.start()

        report_process = process_class(
            target=self.report,
            args=(asset_id, db_lock, task_queue),
        )
        report_process.start()

    def work(self, asset_id, db_lock, task_queue):
        task_assignment = TaskAssignment.get(self, asset_id)
        producer_info = self.db.retrieve_asset(task_assignment.owner_producer_id).metadata

        ipfs = IPFS()
        model_code = ipfs.read(task_assignment.train_data['model_code'])
        epochs = task_assignment.train_data['epochs']

        target_dir = tempfile.mkdtemp()

        train_x_paths = []
        for x_train in task_assignment.train_data['x_train_ipfs']:
            train_x_paths.append(ipfs.download(x_train, target_dir))

        train_y_paths = []
        for y_train in task_assignment.train_data['y_train_ipfs']:
            train_y_paths.append(ipfs.download(y_train, target_dir))

        test_x_path = ipfs.download(task_assignment.train_data['x_test_ipfs'], target_dir)
        test_y_path = ipfs.download(task_assignment.train_data['y_test_ipfs'], target_dir)

        try:
            model_code_path = os.path.join(target_dir, '{}.py'.format(asset_id))
            with open(model_code_path, 'wb') as f:
                f.write(model_code)
            sys.path.append(target_dir)

            try:
                m = import_module(asset_id)
                weights_file_path = str(m.run(
                    train_x_paths, train_y_paths, test_x_path, test_y_path, epochs, target_dir))
            except Exception as e:
                error_dict = {'exception': type(e).__name__}
                msg = str(e)
                if msg:
                    error_dict['message'] = msg

                task_assignment.error = self.encryption.encrypt(
                    json.dumps(error_dict).encode(),
                    producer_info['enc_key']
                ).decode()

            else:
                ipfs_file = ipfs.add_file(weights_file_path)
                task_assignment.result = self.encryption.encrypt(
                    ipfs_file.multihash.encode(),
                    producer_info['enc_key']
                ).decode()

            # TODO: check is it needed? now we load and save data directly to db
            db_lock.acquire()
            try:
                task_assignment.save(self.db)
            finally:
                db_lock.release()
                pass

            task_queue.put('finished')
            print('Finished task')
        finally:
            shutil.rmtree(target_dir)

    def report(self, asset_id, db_lock, task_queue):
        continue_reporting = True

        while continue_reporting:
            cpu_load = psutil.cpu_percent(interval=progress_report_interval)

            print('Reporting CPU load {}'.format(cpu_load))

            db_lock.acquire()
            try:
                # TODO: save spent TFLOPs, and update asset every 30 seconds or when work is finished
                task_assignment = TaskAssignment.get(self, asset_id)
                task_assignment.tflops += 1
                task_assignment.progress += 1
                task_assignment.save(self.db)
            finally:
                pass
                db_lock.release()

            try:
                if task_queue.get(block=False) == 'finished':
                    continue_reporting = False
                else:
                    continue_reporting = True
            except queue.Empty:
                continue_reporting = True

    def ping_producer(self, asset_id, producer_api_url):
        print('Pinging producer')
        requests.post(
            url='{}/worker/ready/'.format(producer_api_url),
            json={
                'worker_id': self.asset_id,
                'task_id': asset_id
            }
        )
