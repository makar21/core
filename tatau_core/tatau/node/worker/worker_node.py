import json
import os
import shutil
import tempfile
import time
from collections import deque
from logging import getLogger
from torch.multiprocessing import Process

import numpy as np

from tatau_core import settings
from tatau_core.metrics import Snapshot
from tatau_core.nn.tatau.model import Model
from tatau_core.tatau.models import WorkerNode, TaskDeclaration, TaskAssignment, EstimationAssignment
from tatau_core.tatau.node import Node
from tatau_core.tatau.node.worker.task_progress import TaskProgress
from tatau_core.tatau.node.worker.worker_interprocess import WorkerInterprocess
from tatau_core.utils.ipfs import IPFS
from tatau_core.nn.tatau.sessions.estimation import EstimationSession
from tatau_core.nn.tatau.sessions.train import TrainSession


logger = getLogger()


class Worker(Node):
    node_type = Node.NodeType.WORKER
    asset_class = WorkerNode

    def _get_tx_methods(self):
        return {
            TaskDeclaration.get_asset_name(): self._process_task_declaration_transaction,
            TaskAssignment.get_asset_name(): self._process_task_assignment_transaction,
            EstimationAssignment.get_asset_name(): self._process_estimation_assignment_transaction,
        }

    def _process_task_declaration_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'TRANSFER':
            return

        task_declaration = TaskDeclaration.get(asset_id)
        logger.info('Received {}, workers_needed: {}'.format(task_declaration, task_declaration.workers_needed))
        if task_declaration.workers_needed == 0:
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

        if task_declaration.state == TaskDeclaration.State.DEPLOYMENT \
                and task_declaration.workers_needed > 0:
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

    def _process_task_assignment_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'CREATE':
            return

        task_assignment = TaskAssignment.get(asset_id)

        # skip another assignment
        if task_assignment.worker_id != self.asset_id:
            return

        self._process_task_assignment(task_assignment)

    def _process_task_assignment(self, task_assignment):
        logger.debug('{} process {} state:{}'.format(self, task_assignment, task_assignment.state))

        if task_assignment.state == TaskAssignment.State.IN_PROGRESS:
            if task_assignment.task_declaration.state == TaskDeclaration.State.EPOCH_IN_PROGRESS:
                task_assignment.state = TaskAssignment.State.DATA_IS_READY

        if task_assignment.state == TaskAssignment.State.RETRY:
            task_assignment.state = TaskAssignment.State.INITIAL
            task_assignment.save(recipients=task_assignment.producer.address)
            return

        if task_assignment.state == TaskAssignment.State.DATA_IS_READY:
            task_assignment.state = TaskAssignment.State.IN_PROGRESS
            task_assignment.save()

            interprocess = WorkerInterprocess()

            Process(
                target=self._collect_metrics,
                args=(interprocess,)
            ).start()

            work_process = Process(
                target=self._train,
                args=(task_assignment.asset_id, interprocess),
            )
            work_process.start()
            work_process.join()

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
            if estimation_assignment.task_declaration.state == TaskDeclaration.State.EPOCH_IN_PROGRESS:
                estimation_assignment.state = TaskAssignment.State.DATA_IS_READY

        if estimation_assignment.state == EstimationAssignment.State.RETRY:
            estimation_assignment.state = EstimationAssignment.State.INITIAL
            estimation_assignment.save(recipients=estimation_assignment.producer.address)
            return

        if estimation_assignment.state == TaskAssignment.State.DATA_IS_READY:
            estimation_assignment.state = TaskAssignment.State.IN_PROGRESS
            estimation_assignment.save()

            interprocess = WorkerInterprocess()

            Process(
                target=self._collect_metrics,
                args=(interprocess,)
            ).start()

            work_process = Process(
                target=self._estimate,
                args=(estimation_assignment.asset_id, interprocess),
            )
            work_process.start()
            work_process.join()

    def _estimate(self, asset_id, collect_metrics=None):

        logger.info('Start estimate process')
        estimation_assignment = EstimationAssignment.get(asset_id)

        session = EstimationSession()

        try:
            try:
                with collect_metrics:
                    session.process_assignment(assignment=estimation_assignment)
            except Exception as e:
                error_dict = {'exception': type(e).__name__}
                msg = str(e)
                if msg:
                    error_dict['message'] = msg

                estimation_assignment.error = json.dumps(error_dict)
                logger.exception(e)

            estimation_assignment.tflops = collect_metrics.get_tflops()
            estimation_assignment.state = EstimationAssignment.State.FINISHED
            estimation_assignment.set_encryption_key(estimation_assignment.producer.enc_key)
            estimation_assignment.save(recipients=estimation_assignment.producer.address)

            logger.info('Finished estimation {}, tflops: {}, error: {}'.format(
                estimation_assignment, estimation_assignment.tflops, estimation_assignment.error))
        finally:
            session.clean()

    def _train(self, asset_id, collect_metrics):
        logger.info('Start work process'.format(asset_id))
        task_assignment = TaskAssignment.get(asset_id)
        logger.info("Train Task: {}".format(task_assignment))
        session = TrainSession()

        try:

            # progress = TaskProgress(self, asset_id, collect_metrics)

            # reset data from previous epoch
            task_assignment.result = None
            task_assignment.error = None

            try:
                with collect_metrics:
                    session.process_assignment(assignment=task_assignment)
            except Exception as e:
                error_dict = {'exception': type(e).__name__}
                msg = str(e)
                if msg:
                    error_dict['message'] = msg

                task_assignment.error = json.dumps(error_dict)
                logger.exception(e)

            task_assignment.tflops = collect_metrics.get_tflops()
            task_assignment.progress = 100
            task_assignment.state = TaskAssignment.State.FINISHED
            task_assignment.set_encryption_key(task_assignment.producer.enc_key)
            task_assignment.save(recipients=task_assignment.producer.address)

            logger.info('Finished {}, tflops: {}, result: {}, error: {}'.format(
                task_assignment, task_assignment.tflops, task_assignment.result, task_assignment.error
            ))
        finally:
            session.clean()

    def _collect_metrics(self, interprocess):
        interprocess.wait_for_start_collect_metrics()
        logger.info('Start collect metrics')

        while not interprocess.should_stop_collect_metrics(interprocess.interval):
            snapshot = Snapshot()
            interprocess.add_tflops(snapshot.calc_tflops() * interprocess.interval)

        logger.info('Stop collect metrics')

    def _process_task_declarations(self):
        for task_declaration in TaskDeclaration.enumerate(created_by_user=False):
            self._process_task_declaration(task_declaration)

    def _process_task_assignments(self):
        for task_assignment in TaskAssignment.enumerate():
            self._process_task_assignment(task_assignment)

    def _process_estimation_assignments(self):
        for estimation_assignment in EstimationAssignment.enumerate():
            self._process_estimation_assignment(estimation_assignment)
            
    def search_tasks(self):
        while True:
            try:
                self._process_task_declarations()
                self._process_estimation_assignments()
                self._process_task_assignments()
                time.sleep(settings.WORKER_PROCESS_INTERVAL)
            except Exception as ex:
                logger.exception(ex)
