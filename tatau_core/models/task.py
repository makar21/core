import datetime
from logging import getLogger
from typing import List

from tatau_core import settings, web3
from tatau_core.contract import poa_wrapper
from tatau_core.db import models, fields
from tatau_core.models.dataset import Dataset
from tatau_core.models.estimation import EstimationAssignment, EstimationResult
from tatau_core.models.nodes import ProducerNode
from tatau_core.models.train import TaskAssignment, TrainResult
from tatau_core.models.train_model import TrainModel
from tatau_core.models.verification import VerificationAssignment, VerificationResult
from tatau_core.utils import cached_property

logger = getLogger('tatau_core')

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

    weights_ipfs = fields.CharField(required=True)
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

        cls._validate_data(**kwargs)
        return super(TaskDeclaration, cls).create(**kwargs)

    @classmethod
    def _validate_data(cls, **kwargs):
        pass

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

    @property
    def current_epoch(self):
        return min(self.current_iteration * self.epochs_in_iteration, self.epochs)

    @property
    def balance_info(self):
        balance = self.balance
        if self.in_finished_state:
            return {
                'state': self.state,
                'balance': balance,
                'required_balance': 0.0,
                'deposit_required': False,
                'issue_required': False
            }

        epoch_cost = self.epoch_cost

        issue_required = False
        if not self.issued:
            issue_required = True
            deposit_required = False
        else:
            deposit_required = epoch_cost > balance

        return {
            'state': self.state,
            'balance': balance,
            'required_balance': epoch_cost,
            'deposit_required': deposit_required,
            'issue_required': issue_required
        }

    def _add_history_info(self, data):
        for td in TaskDeclaration.get_history(self.asset_id, db=self.db, encryption=self.encryption):
            if td.loss and td.accuracy and td.state in (TaskDeclaration.State.VERIFY_IN_PROGRESS,
                                                        TaskDeclaration.State.COMPLETED):
                if td.state == TaskDeclaration.State.VERIFY_IN_PROGRESS:
                    iteration = td.current_iteration - 1
                else:
                    iteration = td.current_iteration

                data['history'][iteration] = {
                    'loss': td.loss,
                    'accuracy': td.accuracy,
                    'spent_tflops': 0.0,
                    'cost': 0.0,
                    'start_time': None,
                    'end_time': None,
                    'duration': None,
                    'weights_ipfs': td.weights_ipfs
                }

    def _add_estimation_info(self, data):
        estimation_assignments = self.get_estimation_assignments(
            states=(
                EstimationAssignment.State.ESTIMATING,
                EstimationAssignment.State.FINISHED
            )
        )

        for estimation_assignment in estimation_assignments:
            history = EstimationResult.get_history(
                estimation_assignment.estimation_result_id, db=self.db, encryption=self.encryption)

            estimator_data = {
                'start_time': None,
                'end_time': None,
                'duration': None,
            }

            for er in history:
                if er.state not in [EstimationResult.State.IN_PROGRESS, EstimationResult.State.FINISHED]:
                    continue

                estimator_data['estimator_id'] = estimation_assignment.estimator_id
                estimator_data['assignment_id'] = er.estimation_assignment_id
                estimator_data['state'] = er.state
                estimator_data['tflops'] = er.tflops
                estimator_data['cost'] = er.tflops * settings.TFLOPS_COST

                if er.state == EstimationResult.State.IN_PROGRESS and estimator_data['start_time'] is None:
                    estimator_data['start_time'] = er.modified_at

                if er.state == EstimationResult.State.FINISHED:
                    estimator_data['end_time'] = er.modified_at

                end_time = estimator_data['end_time'] or datetime.datetime.utcnow().replace(
                    tzinfo=er.modified_at.tzinfo)

                estimator_data['duration'] = (end_time - estimator_data['start_time']).total_seconds()

            data['estimators'].append(estimator_data)

    def _add_train_info(self, data):
        task_assignments = self.get_task_assignments(
            states=(TaskAssignment.State.TRAINING, TaskAssignment.State.FINISHED, TaskAssignment.State.TIMEOUT,
                    TaskAssignment.State.FAKE_RESULTS, TaskAssignment.State.FORGOTTEN)
        )

        train_info = data['workers']

        for task_assignment in task_assignments:
            history = TrainResult.get_history(task_assignment.train_result_id, db=self.db, encryption=self.encryption)

            train_data = {
                'worker_id': task_assignment.worker_id,
                'assignment_id': task_assignment.asset_id,
                'start_time': None,
                'end_time': None,
                'duration': None,
                'weights_ipfs': None,
                'error': None
            }

            for tr in history:
                if tr.state not in (TrainResult.State.IN_PROGRESS, TrainResult.State.FINISHED):
                    continue

                train_data['state'] = tr.state
                train_data['current_iteration'] = tr.current_iteration
                train_data['progress'] = tr.progress
                train_data['spent_tflops'] = tr.tflops
                train_data['cost'] = tr.tflops * settings.TFLOPS_COST
                train_data['loss'] = tr.loss
                train_data['accuracy'] = tr.accuracy

                if tr.state == TrainResult.State.IN_PROGRESS and train_data['start_time'] is None:
                    train_data['start_time'] = tr.modified_at
                    # update start time of iteration
                    if data['history'].get(tr.current_iteration) and (
                            data['history'][tr.current_iteration]['start_time'] is None or
                            data['history'][tr.current_iteration]['start_time'] > train_data['start_time']):

                        data['history'][tr.current_iteration]['start_time'] = train_data['start_time']

                if tr.state == TrainResult.State.FINISHED:
                    train_data['end_time'] = tr.modified_at
                    train_data['weights_ipfs'] = tr.weights_ipfs
                    train_data['error'] = tr.error

                    # update tflops
                    if data['history'].get(tr.current_iteration):
                        data['history'][tr.current_iteration]['spent_tflops'] += train_data['spent_tflops']
                        data['history'][tr.current_iteration]['cost'] += train_data['cost']

                end_time = train_data['end_time'] or datetime.datetime.utcnow().replace(
                    tzinfo=tr.modified_at.tzinfo)

                train_data['duration'] = (end_time - train_data['start_time']).total_seconds()

                if tr.state == TrainResult.State.FINISHED:
                    # save train data and refresh for next iteration
                    if train_info.get(train_data['current_iteration']) is None:
                        train_info[train_data['current_iteration']] = [train_data]
                    else:
                        train_info[train_data['current_iteration']].append(train_data)

                    train_data = {
                        'worker_id': task_assignment.worker_id,
                        'assignment_id': task_assignment.asset_id,
                        'start_time': None,
                        'end_time': None,
                        'duration': None,
                        'weights_ipfs': None,
                        'error': None
                    }

            if train_data.get('state') and train_data['state'] != TrainResult.State.FINISHED:
                if train_info.get(train_data['current_iteration']) is None:
                    train_info[train_data['current_iteration']] = [train_data]
                else:
                    train_info[train_data['current_iteration']].append(train_data)

    def _add_verification_info(self, data):
        verification_assignments = self.get_verification_assignments(
            states=(VerificationAssignment.State.VERIFYING, VerificationAssignment.State.FINISHED,
                    VerificationAssignment.State.TIMEOUT, VerificationAssignment.State.FORGOTTEN)
        )

        verification_info = data['verifiers']

        for verification_assignment in verification_assignments:
            history = VerificationResult.get_history(
                asset_id=verification_assignment.verification_result_id,
                db=self.db,
                encryption=self.encryption
            )

            verification_data = {
                'verifier_id': verification_assignment.verifier_id,
                'assignment_id': verification_assignment.asset_id,
                'start_time': None,
                'end_time': None,
                'duration': None,
                'results': [],
                'weights_ipfs': None,
                'error': None
            }

            for vr in history:
                if vr.state not in (VerificationResult.State.IN_PROGRESS,
                                    VerificationResult.State.VERIFICATION_FINISHED,
                                    VerificationResult.State.FINISHED):
                    continue

                verification_data['state'] = vr.state
                verification_data['current_iteration'] = vr.current_iteration
                verification_data['progress'] = vr.progress
                verification_data['spent_tflops'] = vr.tflops
                verification_data['cost'] = vr.tflops * settings.TFLOPS_COST

                if vr.state == VerificationResult.State.IN_PROGRESS and verification_data['start_time'] is None:
                    verification_data['start_time'] = vr.modified_at

                if vr.state == VerificationResult.State.VERIFICATION_FINISHED:
                    results = vr.result

                    # keep fake workers in result from previous iteration retry
                    for prev_result in verification_data['results']:
                        found = False
                        for current_result in results:
                            if prev_result['worker_id'] == current_result['worker_id']:
                                found = True
                                break
                        if not found:
                            results.append(prev_result)

                    verification_data['results'] = results
                    verification_data['weights_ipfs'] = vr.weights

                if vr.state == VerificationResult.State.FINISHED:
                    verification_data['end_time'] = vr.modified_at

                    # update end time of iteration
                    if data['history'].get(vr.current_iteration) and (
                            data['history'][vr.current_iteration]['end_time'] is None or
                            data['history'][vr.current_iteration]['end_time'] < verification_data['end_time']):

                        data['history'][vr.current_iteration]['end_time'] = verification_data['end_time']

                end_time = verification_data['end_time'] or datetime.datetime.utcnow().replace(
                    tzinfo=vr.modified_at.tzinfo)

                verification_data['duration'] = (end_time - verification_data['start_time']).total_seconds()

                if vr.state == VerificationResult.State.FINISHED:
                    # save train data and refresh for next iteration

                    if verification_info.get(verification_data['current_iteration']) is None:
                        verification_info[verification_data['current_iteration']] = [verification_data]
                    else:
                        verification_info[verification_data['current_iteration']].append(verification_data)

                    verification_data = {
                        'verifier_id': verification_assignment.verifier_id,
                        'assignment_id': verification_assignment.asset_id,
                        'start_time': None,
                        'end_time': None,
                        'duration': None,
                        'results': [],
                        'weights_ipfs': vr.weights,
                        'error': vr.error
                    }

            if verification_data.get('state') and verification_data['state'] != VerificationResult.State.FINISHED:
                if verification_info.get(verification_data['current_iteration']) is None:
                    verification_info[verification_data['current_iteration']] = [verification_data]
                else:
                    verification_info[verification_data['current_iteration']].append(verification_data)

    @property
    def progress_info(self):
        data = {
            'asset_id': self.asset_id,
            'dataset': self.dataset.name,
            'train_model': self.train_model.name,
            'state': self.state,
            'accepted_estimators': self.estimators_requested - self.estimators_needed,
            'estimators_requested': self.estimators_requested,
            'accepted_workers': self.workers_requested - self.workers_needed,
            'workers_requested': self.workers_requested,
            'accepted_verifiers': self.verifiers_requested - self.verifiers_needed,
            'verifiers_requested': self.verifiers_requested,
            'total_progress': self.progress,
            'spent_tflops': self.tflops,
            'epochs': self.epochs,
            'current_epoch': self.current_epoch,
            'epochs_in_iteration': self.epochs_in_iteration,
            'current_iteration': self.current_iteration,
            'current_iteration_retry': self.current_iteration_retry,
            'cost': self.tflops * settings.TFLOPS_COST,
            'estimated_tflops': self.estimated_tflops,
            'estimated_cost': self.train_cost,
            'estimators': [],
            'workers': {},
            'verifiers': {},
            'history': {},
            'start_time': self.created_at,
            'end_time': None,
            'duration': None,
            'balance': self.balance_info,
            'weights_ipfs': self.weights_ipfs
        }

        self._add_history_info(data)
        self._add_estimation_info(data)
        self._add_train_info(data)
        self._add_verification_info(data)

        # update duration of iterations
        for iteration, iteration_data in data['history'].items():
            if iteration_data['start_time'] is not None:
                end_time = iteration_data['end_time'] or datetime.datetime.utcnow().replace(
                        tzinfo=iteration_data['start_time'].tzinfo)

                iteration_data['duration'] = (end_time - iteration_data['start_time']).total_seconds()

        end_time = datetime.datetime.utcnow().replace(tzinfo=data['start_time'].tzinfo)
        if self.state == TaskDeclaration.State.COMPLETED:
            data['train_result'] = self.weights_ipfs
            data['end_time'] = data['history'][self.current_iteration]['end_time']
            end_time = data['end_time']

        data['duration'] = (end_time - data['start_time']).total_seconds()
        return data
