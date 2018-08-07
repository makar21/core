import json
import os
import shutil
import tempfile
import time
from logging import getLogger
from multiprocessing import Process

import numpy as np

from tatau_core import settings
from tatau_core.metrics import MetricsCollector
from tatau_core.nn.models.tatau import TatauModel, TrainProgress
from tatau_core.tatau.models import TaskDeclaration, EstimationAssignment
from tatau_core.tatau.node import Node
from tatau_core.utils.ipfs import IPFS

logger = getLogger()


class Estimator(Node):
    # estimator is not stand alone role
    asset_class = Node

    def _get_tx_methods(self):
        return {
            TaskDeclaration.get_asset_name(): self._process_task_declaration_transaction,
            EstimationAssignment.get_asset_name(): self._process_estimation_assignment_transaction,
        }

    def _process_task_declaration_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'TRANSFER':
            return

        task_declaration = TaskDeclaration.get(asset_id)
        logger.info('Received {}, estimators_needed: {}'.format(task_declaration, task_declaration.estimators_needed))
        if task_declaration.estimators_needed == 0:
            return

        self._process_task_declaration(task_declaration)

    def _process_task_declaration(self, task_declaration):
        if task_declaration.state == TaskDeclaration.State.ESTIMATE_IS_REQUIRED \
                and task_declaration.estimators_needed > 0:
            logger.info('Process {}'.format(task_declaration))
            exists = EstimationAssignment.exists(
                additional_match={
                    'assets.data.worker_id': self.asset_id,
                    'assets.data.task_declaration_id': task_declaration.asset_id,
                },
                created_by_user=False
            )

            if exists:
                logger.info('{} has already created estimation assignment for {}'.format(self, task_declaration))
                return

            estimation_assignment = EstimationAssignment.create(
                worker_id=self.asset_id,
                producer_id=task_declaration.producer_id,
                task_declaration_id=task_declaration.asset_id,
                recipients=task_declaration.producer.address
            )

            logger.info('Added {}'.format(estimation_assignment))

    def _process_estimation_assignment_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'CREATE':
            return

        estimation_assignment = EstimationAssignment.get(asset_id)

        # skip another assignment
        if estimation_assignment.worker_id != self.asset_id:
            return

        self._process_estimation_assignment(estimation_assignment)

    def _process_estimation_assignment(self, estimation_assignment):
        logger.debug('{} process {} state:{}'.format(self, estimation_assignment, estimation_assignment.state))

        if estimation_assignment.state == EstimationAssignment.State.IN_PROGRESS:
            if estimation_assignment.task_declaration.state == TaskDeclaration.State.ESTIMATE_IN_PROGRESS:
                estimation_assignment.state = EstimationAssignment.State.DATA_IS_READY

        if estimation_assignment.state == EstimationAssignment.State.RETRY:
            estimation_assignment.state = EstimationAssignment.State.INITIAL
            estimation_assignment.save(recipients=estimation_assignment.producer.address)
            return

        if estimation_assignment.state == EstimationAssignment.State.DATA_IS_READY:
            estimation_assignment.state = EstimationAssignment.State.IN_PROGRESS
            estimation_assignment.save()

            metrics_collector = MetricsCollector()
            metrics_collector.start_and_wait_signal()

            work_process = Process(
                target=self._estimate,
                args=(estimation_assignment.asset_id, metrics_collector),
            )
            work_process.start()
            work_process.join()

    # noinspection PyMethodMayBeStatic
    def _estimate(self, asset_id, metrics_collector):
        logger.info('Start estimate process')
        estimation_assignment = EstimationAssignment.get(asset_id)
        ipfs = IPFS()

        logger.info('Estimate data: {}'.format(estimation_assignment.estimation_data))

        model_code = ipfs.read(estimation_assignment.estimation_data['model_code'])
        logger.info('model code successfully downloaded')

        target_dir = tempfile.mkdtemp()

        train_x_path = ipfs.download(estimation_assignment.estimation_data['x_train'], target_dir)
        logger.info('x_train is downloaded')

        train_y_path = ipfs.download(estimation_assignment.estimation_data['y_train'], target_dir)
        logger.info('x_train is downloaded')

        initial_weights_path = ipfs.download(estimation_assignment.estimation_data['initial_weights'], target_dir)
        logger.info('initial weights are downloaded')

        batch_size = estimation_assignment.estimation_data['batch_size']

        metrics_collector.set_pid(os.getpid())
        iterations = 100
        try:
            model_code_path = os.path.join(target_dir, '{}.py'.format(asset_id))

            with open(model_code_path, 'wb') as f:
                f.write(model_code)

            # reset data from previous epoch
            estimation_assignment.error = None

            try:
                x_train = np.load(train_x_path)
                y_train = np.load(train_y_path)

                logger.info('Dataset is loaded')

                weights_file = np.load(initial_weights_path)
                initial_weights = [weights_file[r] for r in weights_file.files]

                logger.info('Initial weights are loaded')

                model = TatauModel.load_model(path=model_code_path)
                logger.info('Model is loaded')

                model.set_weights(weights=initial_weights)
                logger.info('Start training')
                progress = TrainProgress()
                with metrics_collector:
                    for i in range(iterations):
                        model.train(x=x_train, y=y_train, batch_size=batch_size, nb_epochs=1, train_progress=progress)
            except Exception as e:
                error_dict = {'exception': type(e).__name__}
                msg = str(e)
                if msg:
                    error_dict['message'] = msg

                estimation_assignment.error = json.dumps(error_dict)
                logger.exception(e)

            estimation_assignment.tflops = metrics_collector.get_tflops() / iterations
            estimation_assignment.state = EstimationAssignment.State.FINISHED
            estimation_assignment.set_encryption_key(estimation_assignment.producer.enc_key)
            estimation_assignment.save(recipients=estimation_assignment.producer.address)

            logger.info('Finished estimation {}, tflops: {}, error: {}'.format(
                estimation_assignment, estimation_assignment.tflops, estimation_assignment.error))
        finally:
            shutil.rmtree(target_dir)

    def _process_task_declarations(self):
        for task_declaration in TaskDeclaration.enumerate(created_by_user=False):
            self._process_task_declaration(task_declaration)

    def _process_estimation_assignments(self):
        for estimation_assignment in EstimationAssignment.enumerate():
            self._process_estimation_assignment(estimation_assignment)

    def search_tasks(self):
        while True:
            try:
                self._process_task_declarations()
                self._process_estimation_assignments()
                time.sleep(settings.WORKER_PROCESS_INTERVAL)
            except Exception as ex:
                logger.exception(ex)
