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
