import json
import os
import shutil
import tempfile
import time
from collections import deque
from logging import getLogger
from multiprocessing import Process

import numpy as np

from tatau_core import settings
from tatau_core.metrics import Snapshot
from tatau_core.nn.models.tatau import TatauModel
from tatau_core.tatau.models import WorkerNode, TaskDeclaration, TaskAssignment
from tatau_core.tatau.node import Node
from tatau_core.tatau.node.worker.task_progress import TaskProgress
from tatau_core.tatau.node.worker.worker_interprocess import WorkerInterprocess
from tatau_core.utils.ipfs import IPFS

logger = getLogger()


class Worker(Node):
    node_type = Node.NodeType.WORKER
    asset_class = WorkerNode

    def get_tx_methods(self):
        return {
            TaskDeclaration.get_asset_name(): self.process_task_declaration_transaction,
            TaskAssignment.get_asset_name(): self.process_task_assignment_transaction,
        }

    def process_task_declaration_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'TRANSFER':
            return

        task_declaration = TaskDeclaration.get(asset_id)
        logger.info('Received {}, workers_needed: {}'.format(task_declaration, task_declaration.workers_needed))
        if task_declaration.workers_needed == 0:
            return

        self.process_task_declaration(task_declaration)

    def process_task_declaration(self, task_declaration):
        logger.info('Process {}'.format(task_declaration))
        exists = TaskAssignment.exists(
            additional_match={
                'assets.data.worker_id': self.asset_id,
                'assets.data.task_declaration_id': task_declaration.asset_id,
            },
            created_by_user=False
        )

        if exists:
            logger.info('{} has already created task assignment for {}'.format(self, task_declaration))
            return

        task_assignment = TaskAssignment.create(
            worker_id=self.asset_id,
            producer_id=task_declaration.producer_id,
            task_declaration_id=task_declaration.asset_id,
            recipients=task_declaration.producer.address
        )

        logger.info('Added {}'.format(task_assignment))

    def process_task_assignment_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'CREATE':
            return

        task_assignment = TaskAssignment.get(asset_id)

        # skip another assignment
        if task_assignment.worker_id != self.asset_id:
            return

        self.process_task_assignment(task_assignment)

    def process_task_assignment(self, task_assignment):
        logger.info('{} proces {} state:{}'.format(self, task_assignment, task_assignment.state))
        # skip assignment that the worker has started working on
        if task_assignment.state == TaskAssignment.State.DATA_IS_READY:
            task_assignment.state = TaskAssignment.State.IN_PROGRESS
            task_assignment.save()

            interprocess = WorkerInterprocess()

            Process(
                target=self.collect_metrics,
                args=(interprocess,)
            ).start()

            work_process = Process(
                target=self.work,
                args=(task_assignment.asset_id, interprocess),
            )
            work_process.start()

            # work_process.join()
            #
            # # retry if failed work
            # task_assignment = TaskAssignment.get(task_assignment.asset_id)
            # if task_assignment.state == TaskAssignment.State.IN_PROGRESS:
            #     task_assignment.state = TaskAssignment.State.DATA_IS_READY
            #     task_assignment.save()

    # TODO: refactor to iterable
    @classmethod
    def load_dataset(cls, train_x_paths, train_y_paths, test_x_path, test_y_path):
        x_train = None
        for train_x_path in train_x_paths:
            f = np.load(train_x_path)
            if x_train is not None:
                x_train = np.concatenate((x_train, f))
            else:
                x_train = f

        y_train = None
        for train_y_path in train_y_paths:
            f = np.load(train_y_path)
            if y_train is not None:
                y_train = np.concatenate((y_train, f))
            else:
                y_train = f

        x_test = np.load(test_x_path)
        y_test = np.load(test_y_path)

        return x_train, y_train, x_test, y_test

    def work(self, asset_id, interprocess):
        logger.info('Start work process')
        task_assignment = TaskAssignment.get(asset_id)

        ipfs = IPFS()

        logger.info('Train data: {}'.format(task_assignment.train_data))

        model_code = ipfs.read(task_assignment.train_data['model_code'])
        batch_size = task_assignment.train_data['batch_size']

        target_dir = tempfile.mkdtemp()

        train_x_paths = deque()
        for x_train in task_assignment.train_data['x_train_ipfs']:
            train_x_paths.append(ipfs.download(x_train, target_dir))

        train_y_paths = deque()
        for y_train in task_assignment.train_data['y_train_ipfs']:
            train_y_paths.append(ipfs.download(y_train, target_dir))

        test_x_path = ipfs.download(task_assignment.train_data['x_test_ipfs'], target_dir)
        test_y_path = ipfs.download(task_assignment.train_data['y_test_ipfs'], target_dir)
        initial_weights_path = ipfs.download(task_assignment.train_data['initial_weights'], target_dir)

        try:
            model_code_path = os.path.join(target_dir, '{}.py'.format(asset_id))

            with open(model_code_path, 'wb') as f:
                f.write(model_code)

            progress = TaskProgress(self, asset_id, interprocess)
            interprocess.start_collect_metrics()

            # reset data from previous epoch
            task_assignment.result = None
            task_assignment.error = None

            try:
                x_train, y_train, x_test, y_test = self.load_dataset(
                    train_x_paths, train_y_paths, test_x_path, test_y_path)

                weights_file = np.load(initial_weights_path)

                initial_weights = [weights_file[r] for r in weights_file.files]

                model = TatauModel.load_model(path=model_code_path)
                model.set_weights(weights=initial_weights)
                train_history = model.train(
                    x=x_train, y=y_train, batch_size=batch_size, nb_epochs=1, train_progress=progress)

                task_assignment.train_history = {
                    'epoch': train_history.epoch,
                    'history': train_history.history,
                    'params': train_history.params
                }

                loss, accuracy = model.eval(x=x_test, y=y_test)
                task_assignment.loss = loss
                task_assignment.accuracy = accuracy

                weights = model.get_weights()
                weights_file_path = os.path.join(target_dir, 'train_weights.npz')
                np.savez(weights_file_path, *weights)

                ipfs_file = ipfs.add_file(weights_file_path)
                task_assignment.result = ipfs_file.multihash
            except Exception as e:
                error_dict = {'exception': type(e).__name__}
                msg = str(e)
                if msg:
                    error_dict['message'] = msg

                task_assignment.error = json.dumps(error_dict)
                logger.exception(e)

            interprocess.stop_collect_metrics()
            task_assignment.tflops = interprocess.get_tflops()
            task_assignment.progress = 100
            task_assignment.state = TaskAssignment.State.FINISHED
            task_assignment.set_encryption_key(task_assignment.producer.enc_key)
            task_assignment.save(recipients=task_assignment.producer.address)

            logger.info('Finished {}, tflops: {}, result: {}, error: {}'.format(
                task_assignment, task_assignment.tflops, task_assignment.result, task_assignment.error
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

    def process_task_declarations(self):
        for task_declaration in TaskDeclaration.list(created_by_user=False):
            if task_declaration.state == TaskDeclaration.State.DEPLOYMENT and task_declaration.workers_needed > 0:
                self.process_task_declaration(task_declaration)

    def process_task_assignments(self):
        for task_assignment in TaskAssignment.list():
            self.process_task_assignment(task_assignment)
            
    def search_tasks(self):
        while True:
            try:
                self.process_task_declarations()
                self.process_task_assignments()
                time.sleep(settings.WORKER_PROCESS_INTERVAL)
            except Exception as ex:
                logger.exception(ex)
