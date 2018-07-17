import json
import logging
import os
import shutil
import sys
import tempfile
from importlib import import_module
from multiprocessing import Process, RLock, Event, Value

from tatau_core import settings
from tatau_core.ipfs import IPFS
from tatau_core.metrics import Snapshot
from tatau_core.tatau.models import WorkerNode, TaskDeclaration, TaskAssignment
from tatau_core.tatau.node import Node

log = logging.getLogger()


class TaskProgress:
    def __init__(self, worker, asset_id, interprocess):
        self.worker = worker
        self.asset_id = asset_id
        self.interprocess = interprocess

    def progress_callback(self, progress):
        ta = TaskAssignment.get(self.asset_id, self.worker.db, self.worker.encryption)
        ta.set_encryption_key(ta.producer.enc_key)
        ta.progress = progress
        ta.tflops = self.interprocess.get_tflops()
        ta.save()


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
    asset_class = WorkerNode

    def get_tx_methods(self):
        return {
            TaskDeclaration.get_asset_name(): self.process_task_declaration,
            TaskAssignment.get_asset_name(): self.process_task_assignment,
        }

    def ignore_operation(self, operation):
        return False

    def process_task_declaration(self, asset_id, transaction):
        if transaction['operation'] == 'TRANSFER':
            return

        task_declaration = TaskDeclaration.get(asset_id)
        log.info('Received task declaration asset: {}, producer: {}, workers_needed: {}'.format(
            asset_id, task_declaration.producer_id, task_declaration.workers_needed))

        if task_declaration.workers_needed == 0:
            return

        exists = TaskAssignment.exists(
            additional_match={
                'assets.data.worker_id': self.asset_id,
                'assets.data.task_declaration_id': task_declaration.asset_id,
            },
            created_by_user=False
        )

        if exists:
            log.info('Worker: {} already worked on task: {}', self.asset_id, task_declaration.asset_id)
            return

        self.add_task_assignment(task_declaration)

    def process_task_assignment(self, asset_id, transaction):
        if transaction['operation'] == 'CREATE':
            return

        task_assignment = TaskAssignment.get(asset_id)

        # skip another assignment
        if task_assignment.worker_id != self.asset_id:
            return

        # TODO: optimize
        # skip assignment that the worker has started working on
        if task_assignment.progress > 0 or task_assignment.tflops > 0:
            return

        # skip assignment that the worker has finished working on
        if task_assignment.result is not None:
            return

        log.info('Received task assignment asset: {}'.format(asset_id))

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
        log.info('Start work process')
        task_assignment = TaskAssignment.get(asset_id)
        task_assignment.set_encryption_key(task_assignment.producer.enc_key)

        ipfs = IPFS()
        model_code = ipfs.read(task_assignment.train_data['model_code'])
        epochs = task_assignment.train_data['epochs']
        batch_size = task_assignment.train_data['batch_size']

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
                    train_x_paths, train_y_paths, test_x_path, test_y_path, batch_size, epochs, target_dir,
                    progress.progress_callback)
                )
            except Exception as e:
                error_dict = {'exception': type(e).__name__}
                msg = str(e)
                if msg:
                    error_dict['message'] = msg

                task_assignment.error = json.dumps(error_dict)

                log.error('Train is failed: {}'.format(e))
            else:
                ipfs_file = ipfs.add_file(weights_file_path)
                task_assignment.result = ipfs_file.multihash

            interprocess.stop_collect_metrics()
            task_assignment.tflops = interprocess.get_tflops()
            task_assignment.progress = 100
            task_assignment.save()

            log.info('Finished task: {}, tflops: {}, result: {}, error: {}'.format(
                task_assignment.asset_id, task_assignment.tflops, task_assignment.result, task_assignment.error
            ))
        finally:
            shutil.rmtree(target_dir)

    def collect_metrics(self, interprocess):
        interprocess.wait_for_start_collect_metrics()
        log.info('Start collect metrics')

        while not interprocess.should_stop_collect_metrics(1):
            snapshot = Snapshot()
            interprocess.add_tflops(snapshot.calc_tflops())

        log.info('Stop collect metrics')

    def add_task_assignment(self, task_declaration):
        task_assignment = TaskAssignment.create(
            worker_id=self.asset_id,
            producer_id=task_declaration.producer_id,
            task_declaration_id=task_declaration.asset_id,
            recipients=task_declaration.producer.address
        )
        log.info('Added task assignment: {}'.format(
            task_assignment.asset_id
        ))

    def process_old_task_declarations(self):
        pass
        # for task_declaration in TaskDeclaration.list(self, created_by_user=False):
        #     if task_declaration.status == TaskDeclaration.Status.COMPLETED or task_declaration.workers_needed == 0:
        #         continue
        #
        #     exists = TaskAssignment.exists(
        #         node=self,
        #         additional_match={
        #             'assets.data.worker_id': self.asset_id,
        #             'assets.data.task_declaration_id': task_declaration.asset_id,
        #         },
        #         created_by_user=False
        #     )
        #
        #     if exists:
        #         continue
        #
        #     self.add_task_assignment(task_declaration)
        #     break
