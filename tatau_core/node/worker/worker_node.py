import json
import time
from logging import getLogger

import requests

from tatau_core import settings
from tatau_core.models import WorkerNode, TaskDeclaration, TaskAssignment, BenchmarkInfo
from tatau_core.models.train import TrainResult
from tatau_core.nn import benchmark
from tatau_core.nn.tatau.sessions.eval_train import TrainEvalSession
from tatau_core.nn.tatau.sessions.train import TrainSession
from tatau_core.node import Node
from tatau_core.utils.ipfs import Downloader

logger = getLogger()


class Worker(Node):

    asset_class = WorkerNode

    def _process_task_declaration(self, task_declaration):
        if task_declaration.in_finished_state:
            Downloader(task_declaration.asset_id).remove_storage()
            return

        if task_declaration.state in [TaskDeclaration.State.DEPLOYMENT, TaskDeclaration.State.DEPLOYMENT_TRAIN] \
                and task_declaration.workers_needed > 0:
            logger.info('Process {}'.format(task_declaration))
            exists = TaskAssignment.exists(
                additional_match={
                    'assets.data.worker_id': self.asset_id,
                    'assets.data.task_declaration_id': task_declaration.asset_id,
                },
                created_by_user=False,
                db=self.db
            )

            if exists:
                logger.debug('{} has already created task assignment for {}'.format(self, task_declaration))
                return

            task_assignment = TaskAssignment.create(
                worker_id=self.asset_id,
                producer_id=task_declaration.producer_id,
                task_declaration_id=task_declaration.asset_id,
                db=self.db,
                encryption=self.encryption
            )

            train_result = TrainResult.create(
                task_assignment_id=task_assignment.asset_id,
                public_key=task_assignment.producer.enc_key,
                db=self.db,
                encryption=self.encryption
            )

            task_assignment.train_result_id = train_result.asset_id
            task_assignment.state = TaskAssignment.State.READY
            task_assignment.save(recipients=task_declaration.producer.address)

            logger.info('Added {}'.format(task_assignment))

    def _process_task_assignment(self, task_assignment):
        if task_assignment.task_declaration.in_finished_state:
            return

        logger.debug('{} process {} state:{}'.format(self, task_assignment, task_assignment.state))

        if task_assignment.state == TaskAssignment.State.REASSIGN:
            task_assignment.state = TaskAssignment.State.READY
            # give ownership to producer
            task_assignment.save(recipients=task_assignment.producer.address)
            return

        if task_assignment.state == TaskAssignment.State.TRAINING:
            if not task_assignment.iteration_is_finished:
                self._train(task_assignment)

    def _dump_error(self, assignment, ex: Exception):
        assignment.train_result.error = json.dumps(self._parse_exception(ex))
        assignment.train_result.state = TrainResult.State.FINISHED
        assignment.train_result.save()
        logger.exception(ex)

    def _run_eval_session(self, task_assignment: TaskAssignment):
        # do not do eval on first iteration
        if task_assignment.train_data.current_iteration <= 1:
            return False, 0.0

        return self._run_session(task_assignment, TrainEvalSession())

    def _train(self, task_assignment: TaskAssignment):
        task_declaration = task_assignment.task_declaration
        if task_declaration.balance_in_wei < task_declaration.iteration_cost_in_wei:
            logger.info('Ignore {}, does not have enough balance'.format(task_declaration))
            return

        task_assignment.train_result.clean()
        task_assignment.train_result.state = TrainResult.State.IN_PROGRESS
        task_assignment.train_result.current_iteration = task_assignment.train_data.current_iteration
        task_assignment.train_result.save()

        failed, eval_tflops = self._run_eval_session(task_assignment)
        if failed:
            return

        failed, train_tflops = self._run_session(task_assignment, session=TrainSession())
        if failed:
            return

        task_assignment.train_result.tflops = eval_tflops + train_tflops
        task_assignment.train_result.progress = 100.0
        task_assignment.train_result.state = TrainResult.State.FINISHED
        task_assignment.train_result.save()

    def _process_task_declarations(self):
        task_declarations = TaskDeclaration.enumerate(created_by_user=False, db=self.db, encryption=self.encryption)
        for task_declaration in task_declarations:
            try:
                self._process_task_declaration(task_declaration)
            except Exception as ex:
                logger.exception(ex)

    def _process_task_assignments(self):
        for task_assignment in TaskAssignment.enumerate(db=self.db, encryption=self.encryption):
            try:
                self._process_task_assignment(task_assignment)
            except requests.exceptions.ConnectionError as ex:
                # hide from sentry connection errors to parity
                parity_ports = [settings.PARITY_JSONRPC_PORT, settings.PARITY_WEBSOCKET_PORT]
                if ex.args[0].pool.port in parity_ports and ex.args[0].pool.host == settings.PARITY_HOST:
                    logger.info(ex)
                else:
                    raise
            except Exception as ex:
                logger.exception(ex)

    def search_tasks(self):
        while True:
            try:
                self._process_task_declarations()
                self._process_task_assignments()
                time.sleep(settings.WORKER_PROCESS_INTERVAL)
            except Exception as ex:
                logger.exception(ex)

    def perform_benchmark(self):
        if not self.asset.benchmark_info:
            download_benchmark_info, train_benchmark_info = benchmark.run()
            benchmark_info_asset = BenchmarkInfo.create(
                worker_id=self.asset_id,
                info_ipfs=train_benchmark_info.info_ipfs,
                downloaded_size=download_benchmark_info.downloaded_size,
                download_time=int(download_benchmark_info.download_time),
                model_train_tflops=train_benchmark_info.model_train_tflops,
                train_time=int(train_benchmark_info.train_time),
                av_cpu_load=train_benchmark_info.av_cpu_load,
                av_gpu_load=train_benchmark_info.av_gpu_load,
                db=self.db,
                encryption=self.encryption
            )
            self.asset.benchmark_info_id = benchmark_info_asset.asset_id
            self.asset.save()
