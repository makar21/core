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
        logger.info('Received task verification asset:{}, producer:{}, verifiers_needed: {}'.format(
            asset_id, verification_declaration.owner_producer_id, verification_declaration.verifiers_needed))

        if verification_declaration.verifiers_needed == 0:
            return

        producer_info = self.db.retrieve_asset(verification_declaration.owner_producer_id).metadata
        producer_api_url = producer_info['producer_api_url']
        self.ping_producer(asset_id, producer_api_url)

    def process_verification_assignment(self, asset_id, transaction):
        # skip another assignment
        verification_assignment = VerificationAssignment.get(self, asset_id)
        if verification_assignment.verifier_id != self.asset_id:
            return

        logger.info('Received verification assignment')
        verification_assignment.verified = self.verify(verification_assignment, verification_assignment.train_results)
        verification_assignment.save(self.db)
        logger.info('Finished verification')

    def ping_producer(self, asset_id, producer_api_url):
        logger.info('Pinging producer')
        requests.post(
            url='{}/verifier/ready/'.format(producer_api_url),
            json={
                'verifier_id': self.asset_id,
                'task_id': asset_id
            }
        )

    def verify(self, verification_assignment, result):
        logger.info('Verified task: {}, results: {}'.format(verification_assignment.asset_id, result))
        return True
