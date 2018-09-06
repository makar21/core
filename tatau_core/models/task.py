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


class TaskDeclaration(models.Model):
    class State:
        ESTIMATE_IS_REQUIRED = 'estimate is required'
        ESTIMATE_IS_IN_PROGRESS = 'estimate is in progress'
        ESTIMATED = 'estimated'
        DEPLOYMENT = 'deployment'
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
    progress = fields.FloatField(initial=0.0)
    tflops = fields.FloatField(initial=0.0)
    estimated_tflops = fields.FloatField(initial=0.0)

    @cached_property
    def producer(self):
        return ProducerNode.get(self.producer_id, db=self.db, encryption=self.encryption)

    @cached_property
    def dataset(self):
        return Dataset.get(self.dataset_id, db=self.db, encryption=self.encryption)

    @cached_property
    def train_model(self):
        return TrainModel.get(self.train_model_id, db=self.db, encryption=self.encryption)

    @classmethod
    def create(cls, **kwargs):
        kwargs['workers_requested'] = kwargs['workers_needed']

        # Use only one verifier
        kwargs['verifiers_needed'] = 1
        kwargs['verifiers_requested'] = kwargs['verifiers_needed']

        if 'estimators_needed' not in kwargs:
            kwargs['estimators_needed'] = 1

        kwargs['estimators_requested'] = kwargs['estimators_needed']
        return super(TaskDeclaration, cls).create(**kwargs)

    def is_in_finished_state(self):
        finish_states = (TaskDeclaration.State.FAILED, TaskDeclaration.State.COMPLETED, TaskDeclaration.State.CANCELED)
        return self.state in finish_states

    def get_current_cost_real(self):
        # calc real iteration cost
        if self.state == TaskDeclaration.State.VERIFY_IN_PROGRESS:
            spent_tflops = 0.0
            for task_assignments in self.get_task_assignments(states=(TaskAssignment.State.FINISHED,)):
                spent_tflops += task_assignments.train_result.tflops
            for verification_assignments in self.get_verification_assignments(
                    states=(VerificationAssignment.State.VERIFICATION_FINISHED,)):
                spent_tflops += verification_assignments.tflops

            iteration_cost = spent_tflops * settings.TFLOPS_COST
        else:
            if self.current_iteration == 0:
                # total cost for all epochs:
                iteration_cost = self.estimated_tflops * settings.TFLOPS_COST
            elif self.current_iteration == 1:
                # estimated cost for current_iteration
                iteration_cost = self.epoch_cost* self.epochs_in_current_iteration
            else:
                # average cost of epochs based on spend tflops and proceeded epochs
                proceeded_epochs = (self.current_iteration - 1) * self.epochs_in_iteration
                iteration_cost = self.tflops / proceeded_epochs * settings.TFLOPS_COST

        return web3.toWei(str(iteration_cost), 'ether')

    def get_current_cost(self):
        total_cost = self.estimated_tflops * settings.TFLOPS_COST
        if self.current_iteration == 0:
            # total cost for all epochs:
            iteration_cost = total_cost
        else:
            iteration_cost = self.epoch_cost * self.epochs_in_current_iteration

        return web3.toWei(str(iteration_cost), 'ether')

    @property
    def epoch_cost(self):
        return self.estimated_tflops / self.epochs * settings.TFLOPS_COST

    @property
    def epochs_in_current_iteration(self):
        return min(self.epochs_in_iteration, abs(self.epochs - self.epochs_in_iteration * (self.current_iteration - 1)))

    def job_has_enough_balance(self):
        balance = poa_wrapper.get_job_balance(self)
        if self.current_iteration == 0:
            cost_name = 'train'
        else:
            cost_name = 'iteration'

        epoch_cost = self.get_current_cost()

        balance_eth = web3.fromWei(balance, 'ether')
        epoch_cost_eth = web3.fromWei(epoch_cost, 'ether')
        if balance >= epoch_cost:
            logger.info('{} balance: {:.5f} ETH, {} cost: {:.5f} ETH'.format(
                self, balance_eth, cost_name, epoch_cost_eth))
            return True
        else:
            if poa_wrapper.does_job_exist(self):
                logger.info('{} balance: {:.5f} ETH, iteration cost: {:.5f} ETH. Deposit is required!!!'.format(
                    self, balance_eth, epoch_cost_eth))
            else:
                estimated_cost = self.estimated_tflops * settings.TFLOPS_COST
                logger.info('{} Issue job is required!!! Estimated cost: {:.5f} ETH'.format(self, estimated_cost))
            return False

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
    def estimation_assignments(self):
        return self.get_estimation_assignments()

    def is_last_epoch(self):
        return self.current_iteration * self.epochs_in_iteration >= self.epochs

    @property
    def task_assignments(self):
        return self.get_task_assignments()

    def get_verification_assignments(self, states=None):
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
    def verification_assignments(self):
        return self.get_verification_assignments()


