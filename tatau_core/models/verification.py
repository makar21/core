from logging import getLogger

from tatau_core.db import models, fields
from tatau_core.models.nodes import ProducerNode, VerifierNode
from tatau_core.utils import cached_property

logger = getLogger('tatau_core')


class VerificationData(models.Model):
    # owner only producer, share data with verifier
    test_dir_ipfs = fields.EncryptedCharField(immutable=True)
    model_code_ipfs = fields.EncryptedCharField(immutable=True)

    verification_assignment_id = fields.CharField()
    train_results = fields.EncryptedJsonField()

    @cached_property
    def verification_assignment(self):
        return VerificationAssignment.get(
            asset_id=self.verification_assignment_id, db=self.db, encryption=self.encryption)

    @cached_property
    def current_iteration(self):
        return self.verification_assignment.task_declaration.current_iteration

    @cached_property
    def current_iteration_retry(self):
        return self.verification_assignment.task_declaration.current_iteration_retry


class VerificationResult(models.Model):
    # owner only verifier, share data with producer
    class State:
        INITIAL = 'initial'
        IN_PROGRESS = 'in progress'
        VERIFICATION_FINISHED = 'verification is finished'
        FINISHED = 'finished'

    verification_assignment_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)
    progress = fields.FloatField(initial=0.0)
    tflops = fields.FloatField(initial=0.0)
    current_iteration = fields.IntegerField(initial=0)
    current_iteration_retry = fields.IntegerField(initial=0)

    # results should be public
    result = fields.JsonField(required=False)

    weights = fields.EncryptedCharField(required=False)
    loss = fields.FloatField(required=False)
    accuracy = fields.FloatField(required=False)

    error = fields.EncryptedCharField(required=False)

    def clean(self):
        self.progress = 0.0
        self.tflops = 0.0
        self.result = None
        self.weights = None
        self.loss = 0.0
        self.accuracy = 0.0

    @cached_property
    def verification_assignment(self):
        return VerificationAssignment.get(self.verification_assignment_id, db=self.db, encryption=self.encryption)


class DistributeHistory(models.Model):
    task_declaration_id = fields.CharField(immutable=True)
    verification_assignment_id = fields.CharField(immutable=True)
    distribute_transactions = fields.JsonField(initial={})

    @cached_property
    def task_declaration(self):
        from tatau_core.models import TaskDeclaration
        return TaskDeclaration.get(self.task_declaration_id, db=self.db, encryption=self.encryption)

    @cached_property
    def verification_assignment(self):
        return VerificationAssignment.get(self.verification_assignment_id, db=self.db, encryption=self.encryption)


class VerificationAssignment(models.Model):
    class State:
        INITIAL = 'initial'
        READY = 'ready'
        REASSIGN = 'reassign'
        REJECTED = 'rejected'
        ACCEPTED = 'accepted'
        VERIFYING = 'verifying'
        FINISHED = 'finished'
        TIMEOUT = 'timeout'
        FORGOTTEN = 'forgotten'

    producer_id = fields.CharField(immutable=True)
    verifier_id = fields.CharField(immutable=True)
    task_declaration_id = fields.CharField(immutable=True)

    state = fields.CharField(initial=State.INITIAL)

    verification_data_id = fields.CharField(null=True, initial=None)
    verification_result_id = fields.CharField(null=True, initial=None)
    distribute_history_id = fields.CharField(null=True, initial=None)

    @cached_property
    def producer(self) -> ProducerNode:
        return ProducerNode.get(self.producer_id, db=self.db, encryption=self.encryption)

    @cached_property
    def verifier(self) -> VerifierNode:
        return VerifierNode.get(self.verifier_id, db=self.db, encryption=self.encryption)

    @cached_property
    def task_declaration(self):
        from tatau_core.models import TaskDeclaration
        return TaskDeclaration.get(self.task_declaration_id, db=self.db, encryption=self.encryption)
    
    @cached_property
    def verification_data(self) -> VerificationData:
        vd = VerificationData.get(self.verification_data_id, db=self.db, encryption=self.encryption)
        # creator and owner must be producer, so share data to verifier
        vd.set_encryption_key(self.verifier.enc_key)
        return vd

    @cached_property
    def verification_result(self) -> VerificationResult:
        vr = VerificationResult.get(self.verification_result_id, db=self.db, encryption=self.encryption)
        # creator and owner must be verifier, so share data to producer
        vr.set_encryption_key(self.producer.enc_key)
        return vr

    @cached_property
    def distribute_history(self) -> DistributeHistory:
        return DistributeHistory.get(self.distribute_history_id, db=self.db, encryption=self.encryption)

    @property
    def iteration_is_finished(self):
        return self.verification_data.current_iteration == self.verification_result.current_iteration \
               and self.verification_data.current_iteration_retry == self.verification_result.current_iteration_retry \
               and self.verification_result.state == VerificationResult.State.FINISHED
