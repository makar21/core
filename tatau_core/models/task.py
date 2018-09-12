from logging import getLogger
from typing import List

from tatau_core import settings, web3
from tatau_core.contract import poa_wrapper
from tatau_core.db import models, fields
from tatau_core.models.dataset import Dataset
from tatau_core.models.estimation import EstimationAssignment
from tatau_core.models.nodes import ProducerNode
from tatau_core.models.train import TaskAssignment
from tatau_core.models.train_model import TrainModel
from tatau_core.models.verification import VerificationAssignment
from tatau_core.utils import cached_property

logger = getLogger()

ListEstimationAssignments = List[EstimationAssignment]
ListTaskAssignments = List[TaskAssignment]
ListVerificationAssignments = List[VerificationAssignment]


class TaskDeclaration(models.Model):
    class State:
        ESTIMATE_IS_REQUIRED = 'estimate is required'
        ESTIMATE_IS_IN_PROGRESS = 'estimate is in progress'
        ESTIMATED = 'estimated'
        DEPLOYMENT = 'deployment'
        DEPLOYMENT_TRAIN = 'deployment train'
        DEPLOYMENT_VERIFICATION = 'deployment verification'
        EPOCH_IN_PROGRESS = 'training'
        VERIFY_IN_PROGRESS = 'verifying'
        COMPLETED = 'completed'
        FAILED = 'failed'
        CANCELED = 'canceled'

    producer_id = fields.CharField(immutable=True)
    dataset_id = fields.CharField(immutable=True)
    train_model_id = fields.CharField(immutable=True)

    weights = fields.EncryptedCharField(required=False)
    loss = fields.FloatField(required=False)
    accuracy = fields.FloatField(required=False)

    batch_size = fields.IntegerField(immutable=True)
    epochs = fields.IntegerField(immutable=True)
    epochs_in_iteration = fields.IntegerField(immutable=True, initial=1)

    workers_requested = fields.IntegerField(immutable=True)
    verifiers_requested = fields.IntegerField(immutable=True)
    estimators_requested = fields.IntegerField(immutable=True)

    workers_needed = fields.IntegerField()
    verifiers_needed = fields.IntegerField()
    estimators_needed = fields.IntegerField()

    state = fields.CharField(initial=State.ESTIMATE_IS_REQUIRED)
    current_iteration = fields.IntegerField(initial=0)
    current_iteration_retry = fields.IntegerField(initial=0)

    progress = fields.FloatField(initial=0.0)
    tflops = fields.FloatField(initial=0.0)
    estimated_tflops = fields.FloatField(initial=0.0)

    @cached_property
    def producer(self) -> ProducerNode:
        return ProducerNode.get(self.producer_id, db=self.db, encryption=self.encryption)

    @cached_property
    def dataset(self) -> Dataset:
        return Dataset.get(self.dataset_id, db=self.db, encryption=self.encryption)

    @cached_property
    def train_model(self) -> TrainModel:
        return TrainModel.get(self.train_model_id, db=self.db, encryption=self.encryption)

    @classmethod
    def create(cls, **kwargs):
        kwargs['workers_requested'] = kwargs['workers_needed']

        # Use only one verifier
        kwargs['verifiers_needed'] = 1
        kwargs['verifiers_requested'] = kwargs['verifiers_needed']

        # Use only one verifier
        kwargs['estimators_needed'] = 1
        kwargs['estimators_requested'] = kwargs['estimators_needed']
        return super(TaskDeclaration, cls).create(**kwargs)

    @property
    def in_finished_state(self):
        finish_states = (TaskDeclaration.State.FAILED, TaskDeclaration.State.COMPLETED, TaskDeclaration.State.CANCELED)
        return self.state in finish_states

    @property
    def last_iteration(self):
        return self.current_iteration * self.epochs_in_iteration >= self.epochs

    @property
    def epochs_in_current_iteration(self):
        return min(self.epochs_in_iteration, abs(self.epochs - self.epochs_in_iteration * (self.current_iteration - 1)))

    @property
    def epoch_cost(self):
        return self.estimated_tflops / self.epochs * settings.TFLOPS_COST

    @property
    def epoch_cost_in_wei(self):
        return web3.toWei(self.epoch_cost, 'ether')

    @property
    def iteration_cost(self):
        return self.epoch_cost * self.epochs_in_current_iteration

    @property
    def iteration_cost_in_wei(self):
        return web3.toWei(self.iteration_cost, 'ether')

    @property
    def train_cost(self):
        return self.estimated_tflops * settings.TFLOPS_COST

    @property
    def train_cost_in_wei(self):
        return web3.toWei(self.train_cost, 'ether')

    @property
    def balance(self):
        return web3.fromWei(self.balance_in_wei, 'ether')

    @property
    def balance_in_wei(self):
        return poa_wrapper.get_job_balance(self)

    @property
    def issued(self):
        return poa_wrapper.does_job_exist(self)

    def get_task_assignments(self, states=None) -> ListTaskAssignments:
        task_assignments = TaskAssignment.enumerate(
            additional_match={
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False,
            db=self.db,
            encryption=self.encryption
        )

        ret = []
        for task_assignment in task_assignments:
            if states is None or task_assignment.state in states:
                ret.append(task_assignment)
        return ret

    @property
    def task_assignments(self) -> ListTaskAssignments:
        return self.get_task_assignments()

    def get_estimation_assignments(self, states=None) -> ListEstimationAssignments:
        estimation_assignments = EstimationAssignment.enumerate(
            additional_match={
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False,
            db=self.db,
            encryption=self.encryption
        )

        ret = []
        for estimation_assignment in estimation_assignments:
            if states is None or estimation_assignment.state in states:
                ret.append(estimation_assignment)
        return ret

    @property
    def estimation_assignments(self) -> ListEstimationAssignments:
        return self.get_estimation_assignments()

    def get_verification_assignments(self, states=None) -> ListVerificationAssignments:
        verification_assignments = VerificationAssignment.enumerate(
            additional_match={
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False,
            db=self.db,
            encryption=self.encryption
        )

        ret = []
        for verification_assignment in verification_assignments:
            if states is None or verification_assignment.state in states:
                ret.append(verification_assignment)
        return ret

    @property
    def verification_assignments(self) -> ListVerificationAssignments:
        return self.get_verification_assignments()

    def get_progress_data(self):
        data = {
            'asset_id': self.asset_id,
            'dataset': self.dataset.name,
            'train_model': self.train_model.name,
            'state': self.state,
            'accepted_workers': self.workers_requested - self.workers_needed,
            'workers_requested': self.workers_requested,
            'accepted_verifiers': self.verifiers_requested - self.verifiers_needed,
            'verifiers_requested': self.verifiers_requested,
            'total_progress': self.progress,
            'current_epoch': min(
                self.current_iteration * self.epochs_in_iteration,
                self.epochs),
            'epochs': self.epochs,
            'epochs_in_iteration': self.epochs_in_iteration,
            'history': {},
            'spent_tflops': self.tflops,
            'cost': self.tflops * settings.TFLOPS_COST,
            'estimated_tflops': self.estimated_tflops,
            'estimated_cost': self.train_cost,
            'workers': {},
            'verifiers': {},
            'estimators': {},
            'start_time': self.created_at,
            'end_time': None,
            'duration': None,
            'balance': self.balance
        }

        if self.state == TaskDeclaration.State.COMPLETED:
            data['train_result'] = self.weights
        #
        # for td in TaskDeclaration.get_history(
        #         task_declaration.asset_id, db=task_declaration.db, encryption=task_declaration.encryption):
        #     if td.loss and td.accuracy and td.state in [TaskDeclaration.State.EPOCH_IN_PROGRESS,
        #                                                 TaskDeclaration.State.COMPLETED]:
        #         if td.state == TaskDeclaration.State.EPOCH_IN_PROGRESS:
        #             epoch = td.current_iteration - 1
        #         else:
        #             epoch = td.current_iteration
        #
        #         data['history'][epoch] = {
        #             'loss': td.loss,
        #             'accuracy': td.accuracy,
        #             'spent_tflops': 0.0,
        #             'cost': 0.0,
        #             'start_time': None,
        #             'end_time': None,
        #             'duration': None
        #         }
        #
        # estimation_assignments = task_declaration.get_estimation_assignments(
        #     states=(
        #         EstimationAssignment.State.IN_PROGRESS,
        #         EstimationAssignment.State.DATA_IS_READY,
        #         EstimationAssignment.State.FINISHED
        #     )
        # )
        #
        # estimator_start_epochs = {}
        # for estimation_assignment in estimation_assignments:
        #     estimator_id = estimation_assignment.estimator_id
        #     data['estimators'][estimator_id] = []
        #     history = EstimationAssignment.get_history(
        #         estimation_assignment.asset_id, db=task_declaration.db, encryption=task_declaration.encryption)
        #     for ea in history:
        #         if ea.state == EstimationAssignment.State.DATA_IS_READY:
        #             estimator_start_epochs[ea.estimator_id] = ea.modified_at
        #
        #         if ea.state == EstimationAssignment.State.FINISHED:
        #             data['estimators'][estimator_id].append({
        #                 'asset_id': ea.asset_id,
        #                 'state': ea.state,
        #                 'tflops': ea.tflops,
        #                 'cost': ea.tflops * TFLOPS_COST,
        #                 'start_time': estimator_start_epochs[ea.estimator_id],
        #                 'end_time': ea.modified_at,
        #                 'duration': (ea.modified_at - estimator_start_epochs[ea.estimator_id]).total_seconds()
        #             })
        #
        #     if estimation_assignment.state != EstimationAssignment.State.FINISHED:
        #         data['estimators'][estimator_id].append({
        #             'asset_id': estimation_assignment.asset_id,
        #             'state': estimation_assignment.state,
        #             'tflops': estimation_assignment.tflops,
        #             'cost': estimation_assignment.tflops * TFLOPS_COST,
        #             'start_time': estimation_assignment.modified_at,
        #             'end_time': None,
        #             'duration': (
        #                     datetime.datetime.utcnow().replace(
        #                         tzinfo=estimation_assignment.modified_at.tzinfo) - estimation_assignment.modified_at
        #             ).total_seconds()
        #         })
        #
        # task_assignments = task_declaration.get_task_assignments(
        #     states=(TaskAssignment.State.IN_PROGRESS, TaskAssignment.State.DATA_IS_READY, TaskAssignment.State.FINISHED)
        # )
        #
        # worker_start_epochs = {}
        # for task_assignment in task_assignments:
        #     history = TaskAssignment.get_history(
        #         task_assignment.asset_id, db=task_declaration.db, encryption=task_declaration.encryption)
        #
        #     for ta in history:
        #         if ta.state == TaskAssignment.State.DATA_IS_READY:
        #             if not worker_start_epochs.get(ta.worker_id):
        #                 worker_start_epochs[ta.worker_id] = {}
        #
        #             if not worker_start_epochs[ta.worker_id].get(ta.current_iteration):
        #                 worker_start_epochs[ta.worker_id][ta.current_iteration] = ta.modified_at
        #
        #             if data['history'].get(ta.current_iteration) is not None \
        #                     and (data['history'][ta.current_iteration]['start_time'] is None
        #                          or data['history'][ta.current_iteration]['start_time'] > ta.modified_at):
        #                 data['history'][ta.current_iteration]['start_time'] = ta.modified_at
        #
        #             if ta.current_iteration == 1 and (
        #                     data['start_time'] is None or data['start_time'] > ta.modified_at):
        #                 data['start_time'] = ta.modified_at
        #
        #         if ta.state == TaskAssignment.State.FINISHED:
        #             if not data['workers'].get(ta.current_iteration):
        #                 data['workers'][ta.current_iteration] = []
        #             data['workers'][ta.current_iteration].append({
        #                 'asset_id': ta.worker_id,
        #                 'state': ta.state,
        #                 'current_epoch': ta.current_iteration,
        #                 'progress': ta.progress,
        #                 'spent_tflops': ta.tflops,
        #                 'cost': ta.tflops * TFLOPS_COST,
        #                 'loss': ta.loss,
        #                 'accuracy': ta.accuracy,
        #                 'start_time': worker_start_epochs[ta.worker_id][ta.current_iteration],
        #                 'end_time': ta.modified_at,
        #                 'duration': (ta.modified_at - worker_start_epochs[ta.worker_id][
        #                     ta.current_iteration]).total_seconds()
        #             })
        #
        #             if data['history'].get(ta.current_iteration):
        #                 data['history'][ta.current_iteration]['spent_tflops'] += ta.tflops
        #                 data['history'][ta.current_iteration]['cost'] += ta.tflops * TFLOPS_COST
        #
        #     if task_assignment.state != TaskAssignment.State.FINISHED:
        #         if not data['workers'].get(task_assignment.current_iteration):
        #             data['workers'][task_assignment.current_iteration] = []
        #
        #         data['workers'][task_assignment.current_iteration].append({
        #             'asset_id': task_assignment.worker_id,
        #             'state': task_assignment.state,
        #             'current_epoch': task_assignment.current_iteration,
        #             'progress': task_assignment.progress,
        #             'spent_tflops': task_assignment.tflops,
        #             'cost': task_assignment.tflops * TFLOPS_COST,
        #             'loss': task_assignment.loss,
        #             'accuracy': task_assignment.accuracy,
        #             'start_time': task_assignment.modified_at,
        #             'end_time': None,
        #             'duration': (
        #                     datetime.datetime.utcnow().replace(
        #                         tzinfo=task_assignment.modified_at.tzinfo) - task_assignment.modified_at
        #             ).total_seconds()
        #         })
        #
        # verification_assignments = task_declaration.get_verification_assignments(
        #     states=(
        #         VerificationAssignment.State.IN_PROGRESS,
        #         VerificationAssignment.State.PARTIAL_DATA_IS_READY,
        #         VerificationAssignment.State.PARTIAL_DATA_IS_DOWNLOADED,
        #         VerificationAssignment.State.DATA_IS_READY,
        #         VerificationAssignment.State.VERIFICATION_FINISHED,
        #         VerificationAssignment.State.FINISHED
        #     )
        # )
        # verifier_start_epochs = {}
        # for verification_assignment in verification_assignments:
        #     verifier_id = verification_assignment.verifier_id
        #     data['verifiers'][verifier_id] = []
        #     history = VerificationAssignment.get_history(
        #         verification_assignment.asset_id, db=task_declaration.db, encryption=task_declaration.encryption)
        #     current_epoch = 0
        #     for va in history:
        #         if va.state == VerificationAssignment.State.DATA_IS_READY:
        #             if not verifier_start_epochs.get(va.verifier_id):
        #                 verifier_start_epochs[va.verifier_id] = {}
        #
        #             if not verifier_start_epochs[va.verifier_id].get(current_epoch):
        #                 verifier_start_epochs[va.verifier_id][current_epoch] = va.modified_at
        #
        #         if va.state == VerificationAssignment.State.FINISHED:
        #             data['verifiers'][verifier_id].append({
        #                 'asset_id': va.verifier_id,
        #                 'state': va.state,
        #                 'progress': va.progress,
        #                 'spent_tflops': va.tflops,
        #                 'result': va.result,
        #                 'cost': va.tflops * TFLOPS_COST,
        #                 'start_time': verifier_start_epochs[va.verifier_id][current_epoch],
        #                 'end_time': va.modified_at,
        #                 'duration': (va.modified_at - verifier_start_epochs[va.verifier_id][
        #                     current_epoch]).total_seconds()
        #
        #             })
        #             current_epoch += 1
        #             if data['history'].get(current_epoch):
        #                 data['history'][current_epoch]['spent_tflops'] += va.tflops
        #                 data['history'][current_epoch]['cost'] += va.tflops * TFLOPS_COST
        #
        #                 if data['history'][current_epoch]['end_time'] is None \
        #                         or data['history'][current_epoch]['end_time'] > va.modified_at:
        #                     data['history'][current_epoch]['end_time'] = va.modified_at
        #                     data['history'][current_epoch]['duration'] = (
        #                             data['history'][current_epoch]['end_time'] - data['history'][current_epoch][
        #                         'start_time']).total_seconds()
        #
        #     if verification_assignment.state != VerificationAssignment.State.FINISHED:
        #         data['verifiers'][verifier_id].append({
        #             'asset_id': verification_assignment.verifier_id,
        #             'state': verification_assignment.state,
        #             'progress': verification_assignment.progress,
        #             'spent_tflops': verification_assignment.tflops,
        #             'cost': verification_assignment.tflops * TFLOPS_COST,
        #             'result': None,
        #             'start_time': verification_assignment.modified_at,
        #             'end_time': None,
        #             'duration': (
        #                     datetime.datetime.utcnow().replace(
        #                         tzinfo=verification_assignment.modified_at.tzinfo) - verification_assignment.modified_at
        #             ).total_seconds()
        #         })
        #
        # if data['start_time']:
        #     data['duration'] = (
        #             datetime.datetime.utcnow().replace(tzinfo=data['start_time'].tzinfo) - data['start_time']
        #     ).total_seconds()
        #
        # if task_declaration.state in [TaskDeclaration.State.FAILED, TaskDeclaration.State.COMPLETED]:
        #     try:
        #         data['end_time'] = data['history'][task_declaration.epochs]['end_time']
        #     except KeyError:
        #         data['end_time'] = task_declaration.modified_at
        #     data['duration'] = (data['end_time'] - data['start_time']).total_seconds()

        return data

