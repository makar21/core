import logging

import requests

from ..tasks import Task, VerificationDeclaration, VerificationAssignment
from .node import Node

logger = logging.getLogger()


class Verifier(Node):
    node_type = Node.NodeType.VERIFIER

    key_name = 'verifier'
    asset_name = 'Verifier info'

    def get_node_info(self):
        return {
            'enc_key': self.encryption.get_public_key().decode(),
        }

    def get_tx_methods(self):
        return {
            Task.TaskType.VERIFICATION_DECLARATION: self.process_verification_declaration,
            Task.TaskType.VERIFICATION_ASSIGNMENT: self.process_verification_assignment,
        }

    def ignore_operation(self, operation):
        return operation in ['TRANSFER']

    def process_verification_declaration(self, asset_id, transaction):
        verification_declaration = VerificationDeclaration.get(self, asset_id)
        logger.info('Received task verification asset: {}, producer: {}, verifiers_needed: {}'.format(
            asset_id, verification_declaration.owner_producer_id, verification_declaration.verifiers_needed))

        if verification_declaration.verifiers_needed == 0:
            return

        exists = VerificationAssignment.exists(
            node=self,
            additional_match={
                'assets.data.verifier_id': self.asset_id,
                'assets.data.task_declaration_id': verification_declaration.task_declaration_id,
            },
            created_by_user=False
        )

        if exists:
            logger.info('Verifier: {} already worked on task: {}',
                        self.asset_id, verification_declaration.task_declaration_id)
            return

        self.ping_producer(asset_id, verification_declaration.owner_producer_id)

    def process_verification_assignment(self, asset_id, transaction):
        # skip another assignment
        verification_assignment = VerificationAssignment.get(self, asset_id)
        if verification_assignment.verifier_id != self.asset_id:
            return

        logger.info('Received verification assignment')
        # TODO: calc tflops and do real progress
        verification_assignment.verified = self.verify(verification_assignment, verification_assignment.train_results)
        verification_assignment.progress = 100
        verification_assignment.tflops = 99
        verification_assignment.save(self.db)
        logger.info('Finished verification')

    def ping_producer(self, verification_declaration_asset_id, producer_asset_id):
        logger.info('Pinging producer: {}'.format(producer_asset_id))
        producer_info = self.db.retrieve_asset(producer_asset_id).metadata
        producer_api_url = producer_info['producer_api_url']
        requests.post(
            url='{}/verifier/ready/'.format(producer_api_url),
            json={
                'verifier_id': self.asset_id,
                'task_id': verification_declaration_asset_id
            }
        )

    def verify(self, verification_assignment, result):
        logger.info('Verified task: {}, results: {}'.format(verification_assignment.asset_id, result))
        return True

    def process_old_verification_declarations(self):
        logger.info('Process old verification declaration verifier: {}'.format(self.asset_id))
        for verification_declaration in VerificationDeclaration.list(self, created_by_user=False):
            if verification_declaration.status == VerificationDeclaration.Status.COMPLETED \
                    or verification_declaration.verifiers_needed == 0:
                logger.info('Skip verification Declaration: {}, status: {}, verifiers_needed: {}'.format(
                    verification_declaration.asset_id,
                    verification_declaration.status,
                    verification_declaration.verifiers_needed
                ))
                continue

            exists = VerificationAssignment.exists(
                node=self,
                additional_match={
                    'assets.data.verifier_id': self.asset_id,
                    'assets.data.task_declaration_id': verification_declaration.task_declaration_id,
                },
                created_by_user=False
            )

            if exists:
                logger.info('Verifier: {} has already worked on task: {}'.format(
                    self.asset_id, verification_declaration.asset_id)
                )
                continue

            self.ping_producer(verification_declaration.asset_id, verification_declaration.owner_producer_id)
            break
