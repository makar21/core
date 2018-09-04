import json
import time
from logging import getLogger

from tatau_core import settings
from tatau_core.nn import benchmark
from tatau_core.nn.tatau.sessions.train import TrainSession
from tatau_core.tatau.models import WorkerNode, TaskDeclaration, TaskAssignment, BenchmarkInfo
from tatau_core.tatau.node import Node

logger = getLogger()


class Worker(Node):

    asset_class = WorkerNode

    def _get_tx_methods(self):
        return {
            TaskDeclaration.get_asset_name(): self._process_task_declaration_transaction,
            TaskAssignment.get_asset_name(): self._process_task_assignment_transaction
        }

    def _process_task_declaration_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'TRANSFER':
            return

        task_declaration = TaskDeclaration.get(asset_id, db=self.db, encryption=self.encryption)
        logger.info('Received {}, workers_needed: {}'.format(task_declaration, task_declaration.workers_needed))
        if task_declaration.workers_needed == 0:
            return

        self._process_task_declaration(task_declaration)

    def _process_task_declaration(self, task_declaration):
        if task_declaration.state == TaskDeclaration.State.DEPLOYMENT \
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
                logger.info('{} has already created task assignment for {}'.format(self, task_declaration))
                return

            task_assignment = TaskAssignment.create(
                worker_id=self.asset_id,
                producer_id=task_declaration.producer_id,
                task_declaration_id=task_declaration.asset_id,
                recipients=task_declaration.producer.address,
                db=self.db,
                encryption=self.encryption
            )

            logger.info('Added {}'.format(task_assignment))

    def _process_task_assignment_transaction(self, asset_id, transaction):
        if transaction['operation'] == 'CREATE':
            return

        task_assignment = TaskAssignment.get(asset_id, db=self.db, encryption=self.encryption)

        # skip another assignment
        if task_assignment.worker_id != self.asset_id:
            return

        self._process_task_assignment(task_assignment)

    def _process_task_assignment(self, task_assignment):
        if task_assignment.task_declaration.is_in_finished_state():
            return

        logger.debug('{} process {} state:{}'.format(self, task_assignment, task_assignment.state))

        if task_assignment.state == TaskAssignment.State.IN_PROGRESS:
            if task_assignment.task_declaration.state == TaskDeclaration.State.EPOCH_IN_PROGRESS:
                task_assignment.state = TaskAssignment.State.DATA_IS_READY

        if task_assignment.state == TaskAssignment.State.RETRY:
            task_assignment.state = TaskAssignment.State.INITIAL
            task_assignment.save(recipients=task_assignment.producer.address)
            return

        if task_assignment.state == TaskAssignment.State.DATA_IS_READY:
            if not task_assignment.task_declaration.job_has_enough_balance():
                return

            task_assignment.state = TaskAssignment.State.IN_PROGRESS
            task_assignment.save()
            self._train(task_assignment.asset_id)

    def _train(self, asset_id):
        logger.info('Start work process'.format(asset_id))
        task_assignment = TaskAssignment.get(asset_id, db=self.db, encryption=self.encryption)
        logger.info("Train Task: {}".format(task_assignment))
        session = TrainSession()

        try:

            # progress = TaskProgress(self, asset_id, collect_metrics)

            # reset data from previous iteration
            task_assignment.result = None
            task_assignment.error = None

            try:
                session.process_assignment(assignment=task_assignment)
            except Exception as e:
                error_dict = {'exception': type(e).__name__}
                msg = str(e)
                if msg:
                    error_dict['message'] = msg

                task_assignment.error = json.dumps(error_dict)
                logger.exception(e)

            task_assignment.tflops = session.get_tflops()
            task_assignment.progress = 100
            task_assignment.state = TaskAssignment.State.FINISHED
            task_assignment.set_encryption_key(task_assignment.producer.enc_key)
            task_assignment.save(recipients=task_assignment.producer.address)

            logger.info('Finished {}, tflops: {}, result: {}, error: {}'.format(
                task_assignment, task_assignment.tflops, task_assignment.result, task_assignment.error
            ))
        finally:
            session.clean()

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
                av_gpu_load=train_benchmark_info.av_gpu_load
            )
            self.asset.benchmark_info_id = benchmark_info_asset.asset_id
            self.asset.save()
