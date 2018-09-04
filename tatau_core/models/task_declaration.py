from logging import getLogger

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


class TaskDeclaration(models.Model):
    class State:
        ESTIMATE_IS_REQUIRED = 'estimate is required'
        ESTIMATE_IN_PROGRESS = 'estimate in progress'
        ESTIMATED = 'estimated'
        DEPLOYMENT = 'deployment'
        EPOCH_IN_PROGRESS = 'training'
        VERIFY_IN_PROGRESS = 'verifying'
        COMPLETED = 'completed'
        FAILED = 'failed'

    producer_id = fields.CharField(immutable=True)
    # TODO: make copy of dataset and train model data instead reference on assets
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
    results = fields.EncryptedJsonField(initial=[])

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

    def ready_for_start(self):
        ready = self.workers_needed == 0 and self.verifiers_needed == 0
        logger.info('{} ready: {} workers_needed: {} verifiers_needed: {}'.format(
            self, ready, self.workers_needed, self.verifiers_needed))
        return ready

    def get_current_cost_real(self):
        # calc real iteration cost
        if self.state == TaskDeclaration.State.VERIFY_IN_PROGRESS:
            spent_tflops = 0.0
            for task_assignments in self.get_task_assignments(states=(TaskAssignment.State.FINISHED,)):
                spent_tflops += task_assignments.tflops
            for verification_assignments in self.get_verification_assignments(
                    states=(VerificationAssignment.State.VERIFICATION_FINISHED,)):
                spent_tflops += verification_assignments.tflops

            iteration_cost = spent_tflops * settings.TFLOPS_COST
        else:
            if self.current_iteration == 0:
                # total cost for all epochs:
                iteration_cost = self.estimated_tflops * settings.TFLOPS_COST
            elif self.current_iteration == 1:
                # estimated cost for train_iteration
                iteration_cost = self.epoch_cost() * self.epochs_in_current_iteration()
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
            iteration_cost = self.epoch_cost() * self.epochs_in_current_iteration()

        return web3.toWei(str(iteration_cost), 'ether')

    def epoch_cost(self):
        return self.estimated_tflops / self.epochs * settings.TFLOPS_COST

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

    def get_task_assignments(self, states=None):
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

    def get_estimation_assignments(self, states=None):
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

    def is_task_assignment_allowed(self, task_assignment):
        if self.workers_needed == 0:
            return False

        if task_assignment.state != TaskAssignment.State.INITIAL:
            return False

        count = TaskAssignment.count(
            additional_match={
                'assets.data.worker_id': task_assignment.worker_id,
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False,
            db=self.db
        )

        if count == 1:
            logger.info('{} allowed for {}'.format(task_assignment, self))
            return True

        logger.info('{} not allowed for {}, worker created {} assignment for this task'.format(
            task_assignment, self, count))
        return False

    def is_verification_assignment_allowed(self, verification_assignment):
        if self.verifiers_needed == 0:
            return False

        if verification_assignment.state != VerificationAssignment.State.INITIAL:
            return False

        count = VerificationAssignment.count(
            additional_match={
                'assets.data.verifier_id': verification_assignment.verifier_id,
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False,
            db=self.db
        )

        if count == 1:
            logger.info('{} allowed for {}'.format(verification_assignment, self))
            return True

        logger.info('{} not allowed for {}, verifier created {} assignment for this task'.format(
            verification_assignment, self, count))
        return False

    def is_estimation_assignment_allowed(self, estimation_assignment):
        if self.estimators_needed == 0:
            return False

        if estimation_assignment.state != EstimationAssignment.State.INITIAL:
            return False

        count = EstimationAssignment.count(
            additional_match={
                'assets.data.estimator_id': estimation_assignment.estimator_id,
                'assets.data.task_declaration_id': self.asset_id
            },
            created_by_user=False,
            db=self.db
        )

        if count == 1:
            logger.info('{} allowed for {}'.format(estimation_assignment, self))
            return True

        logger.info('{} not allowed for {}, verifier created {} assignment for this task'.format(
            estimation_assignment, self, count))
        return False

    def verification_is_ready(self):
        verification_assignments = self.get_verification_assignments(
            states=(
                VerificationAssignment.State.DATA_IS_READY,
                VerificationAssignment.State.IN_PROGRESS,
                VerificationAssignment.State.VERIFICATION_FINISHED,
                VerificationAssignment.State.FINISHED
            )
        )

        for va in verification_assignments:
            if va.state != VerificationAssignment.State.FINISHED:
                return False
        return True

    def all_done(self):
        return self.is_last_epoch() and self.verification_is_ready()

    def is_in_finished_state(self):
        return self.state in (TaskDeclaration.State.FAILED, TaskDeclaration.State.COMPLETED)

