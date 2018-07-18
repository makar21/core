import logging

from tatau_core.tatau.models import VerifierNode, TaskDeclaration, VerificationAssignment
from .node import Node

log = logging.getLogger()


class Verifier(Node):
    node_type = Node.NodeType.VERIFIER
    asset_class = VerifierNode

    def get_tx_methods(self):
        return {
            TaskDeclaration.get_asset_name(): self.process_task_declaration,
            VerificationAssignment.get_asset_name(): self.process_verification_assignment,
        }

    def process_task_declaration(self, asset_id, transaction):
        if transaction['operation'] == 'TRANSFER':
            return

        task_declaration = TaskDeclaration.get(asset_id)
        if task_declaration.verifiers_needed == 0:
            return

        exists = VerificationAssignment.exists(
            additional_match={
                'assets.data.verifier_id': self.asset_id,
                'assets.data.task_declaration_id': task_declaration.asset_id,
            },
            created_by_user=False
        )

        if exists:
            log.info('{} has already created verification assignment to {}'.format(self, task_declaration))
            return

        verification_assignment = VerificationAssignment.create(
            producer_id=task_declaration.producer_id,
            verifier_id=self.asset_id,
            task_declaration_id=task_declaration.asset_id,
            recipients=task_declaration.producer.address
        )
        log.info('{} added {}'.format(self, verification_assignment))

    def process_verification_assignment(self, asset_id, transaction):
        if transaction['operation'] == 'CREATE':
            return

        # skip another assignment
        verification_assignment = VerificationAssignment.get(asset_id)
        if verification_assignment.verifier_id != self.asset_id:
            return

        if verification_assignment.state == VerificationAssignment.State.DATA_IS_READY:
            self.verify(verification_assignment, verification_assignment.train_results)
            verification_assignment.progress = 100
            verification_assignment.tflops = 99
            verification_assignment.state = VerificationAssignment.State.FINISHED
            verification_assignment.set_encryption_key(verification_assignment.producer.enc_key)
            verification_assignment.save(recipients=verification_assignment.producer.address)

    def verify(self, verification_assignment, result):
        log.info('Verified task: {}, results: {}'.format(verification_assignment.asset_id, result))
        verification_assignment.result = [{
            'worker_id': x['worker_id'],
            'result': True
        } for x in result]
