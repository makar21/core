import logging

from tatau_core.tatau.models import VerifierNode, VerificationDeclaration, VerificationAssignment
from .node import Node

log = logging.getLogger()


class Verifier(Node):
    node_type = Node.NodeType.VERIFIER
    asset_class = VerifierNode

    def get_tx_methods(self):
        return {
            VerificationDeclaration.get_asset_name(): self.process_verification_declaration,
            VerificationAssignment.get_asset_name(): self.process_verification_assignment,
        }

    def ignore_operation(self, operation):
        return False

    def process_verification_declaration(self, asset_id, transaction):
        if transaction['operation'] == 'TRANSFER':
            return

        verification_declaration = VerificationDeclaration.get(asset_id)
        log.info('Received task verification asset: {}, producer: {}, verifiers_needed: {}'.format(
            asset_id, verification_declaration.producer_id, verification_declaration.verifiers_needed))

        if verification_declaration.verifiers_needed == 0:
            return

        exists = VerificationAssignment.exists(
            additional_match={
                'assets.data.verifier_id': self.asset_id,
                'assets.data.task_declaration_id': verification_declaration.task_declaration_id,
            },
            created_by_user=False
        )

        if exists:
            log.info('Verifier: {} already worked on task: {}',
                     self.asset_id, verification_declaration.task_declaration_id)
            return

        self.add_verification_assignment(verification_declaration)

    def process_verification_assignment(self, asset_id, transaction):
        if transaction['operation'] == 'CREATE':
            return

        # skip another assignment
        verification_assignment = VerificationAssignment.get(asset_id)
        if verification_assignment.verifier_id != self.asset_id:
            return

        # skip finished
        if verification_assignment.result is not None:
            return

        log.info('Received verification assignment')
        # TODO: calc tflops and do real progress
        verification_assignment.result = '{}'.format(self.verify(verification_assignment, verification_assignment.train_results))
        verification_assignment.progress = 100
        verification_assignment.tflops = 99
        verification_assignment.save()
        log.info('Finished verification')

    def add_verification_assignment(self, verification_declaration):
        verification_assignment = VerificationAssignment.create(
            producer_id=verification_declaration.producer_id,
            verifier_id=self.asset_id,
            task_declaration_id=verification_declaration.task_declaration_id,
            verification_declaration_id=verification_declaration.asset_id,
            recipients=verification_declaration.producer.address
        )
        log.info('Added verification assignment: {}'.format(
            verification_assignment.asset_id
        ))

    def verify(self, verification_assignment, result):
        log.info('Verified task: {}, results: {}'.format(verification_assignment.asset_id, result))
        return True

    def process_old_verification_declarations(self):
        log.info('Process old verification declaration verifier: {}'.format(self.asset_id))
        # for verification_declaration in VerificationDeclaration.list(self, created_by_user=False):
        #     if verification_declaration.status == VerificationDeclaration.Status.COMPLETED \
        #             or verification_declaration.verifiers_needed == 0:
        #         logger.info('Skip verification Declaration: {}, status: {}, verifiers_needed: {}'.format(
        #             verification_declaration.asset_id,
        #             verification_declaration.status,
        #             verification_declaration.verifiers_needed
        #         ))
        #         continue
        #
        #     exists = VerificationAssignment.exists(
        #         node=self,
        #         additional_match={
        #             'assets.data.verifier_id': self.asset_id,
        #             'assets.data.task_declaration_id': verification_declaration.task_declaration_id,
        #         },
        #         created_by_user=False
        #     )
        #
        #     if exists:
        #         logger.info('Verifier: {} has already worked on task: {}'.format(
        #             self.asset_id, verification_declaration.asset_id)
        #         )
        #         continue
        #
        #     self.add_verification_assignment(verification_declaration)
        #     break
