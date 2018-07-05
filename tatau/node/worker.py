import json
import logging
import os
import shutil
import sys
import tempfile
from importlib import import_module
from multiprocessing import Process, RLock, Event, Value

import requests

import settings
from ipfs import IPFS
from metrics import Snapshot
from tatau.tasks import Task, TaskDeclaration, TaskAssignment
from .node import Node

logger = logging.getLogger()


class TaskProgress:
    def __init__(self, worker, asset_id, interprocess):
        self.worker = worker
        self.asset_id = asset_id
        self.interprocess = interprocess

    def progress_callback(self, progress):
        ta = TaskAssignment.get(self.worker, self.asset_id)
        ta.progress = progress
        ta.tflops = self.interprocess.get_tflops()
        ta.save(self.worker.db)


class WorkerInterprocess:
    def __init__(self):
        self._event_start_collect_metrics = Event()
        self._event_stop = Event()
        self._tflops = Value('i', 0)
        self._tflops_lock = RLock()

    def get_tflops(self):
        with self._tflops_lock:
            return self._tflops.value

    def add_tflops(self, tflops):
        with self._tflops_lock:
            self._tflops.value += tflops

    def start_collect_metrics(self):
        self._event_start_collect_metrics.set()

    def wait_for_start_collect_metrics(self):
        self._event_start_collect_metrics.wait()

    def should_stop_collect_metrics(self, wait):
        return self._event_stop.wait(wait)

    def stop_collect_metrics(self):
        self._event_stop.set()


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
        logger.info('Received task declaration asset: {}, producer: {}, workers_needed: {}'.format(
            asset_id, task_declaration.owner_producer_id, task_declaration.workers_needed))

        if task_declaration.workers_needed == 0:
            return

        producer_info = self.db.retrieve_asset(task_declaration.owner_producer_id).metadata
        producer_api_url = producer_info['producer_api_url']
        self.ping_producer(asset_id, producer_api_url)

    def process_task_assignment(self, asset_id, transaction):
        # skip another assignment
        if TaskAssignment.get(self, asset_id).worker_id != self.asset_id:
            return

        logger.info('Received task assignment asset:{}'.format(asset_id))

        interprocess = WorkerInterprocess()

        process_class = Process
        if settings.DEBUG:
            import threading
            process_class = threading.Thread

        report_process = process_class(
            target=self.collect_metrics,
            args=[interprocess]
        )
        report_process.start()

        work_process = process_class(
            target=self.work,
            args=(asset_id, interprocess),
        )
        work_process.start()

    def work(self, asset_id, interprocess):
        logger.info('Start work process')
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

            progress = TaskProgress(self, asset_id, interprocess)
            interprocess.start_collect_metrics()
            try:
                m = import_module(asset_id)
                weights_file_path = str(m.run(
                    train_x_paths, train_y_paths, test_x_path, test_y_path, epochs, target_dir,
                    progress.progress_callback)
                )
            except Exception as e:
                error_dict = {'exception': type(e).__name__}
                msg = str(e)
                if msg:
                    error_dict['message'] = msg

                task_assignment.error = self.encryption.encrypt(
                    json.dumps(error_dict).encode(),
                    producer_info['enc_key']
                ).decode()

                logger.error('Train is failed: {}'.format(e))
            else:
                ipfs_file = ipfs.add_file(weights_file_path)
                task_assignment.result = self.encryption.encrypt(
                    ipfs_file.multihash.encode(),
                    producer_info['enc_key']
                ).decode()

            interprocess.stop_collect_metrics()
            task_assignment.tflops = interprocess.get_tflops()
            task_assignment.progress = 100
            task_assignment.save(self.db)

            logger.info('Finished task: {}, tflops:{}, result:{}, error:{}'.format(
                task_assignment.asset_id, task_assignment.tflops, task_assignment.result, task_assignment.error
            ))
        finally:
            shutil.rmtree(target_dir)

    def collect_metrics(self, interprocess):
        interprocess.wait_for_start_collect_metrics()
        logger.info('Start collect metrics')

        while not interprocess.should_stop_collect_metrics(1):
            snapshot = Snapshot()
            interprocess.add_tflops(snapshot.calc_tflops())

        logger.info('Stop collect metrics')

    def ping_producer(self, asset_id, producer_api_url):
        logger.info('Pinging producer')
        requests.post(
            url='{}/worker/ready/'.format(producer_api_url),
            json={
                'worker_id': self.asset_id,
                'task_id': asset_id
            }
        )
