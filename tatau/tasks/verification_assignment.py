import json

from tatau.node.node import Node
from .task import Task


class VerificationAssignment(Task):
    task_type = Task.TaskType.VERIFICATION_ASSIGNMENT

    def __init__(self, owner_producer_id, verifier_id, train_results, task_declaration_id, asset_id, *args, **kwargs):
        super().__init__(asset_id, *args, **kwargs)
        self.owner_producer_id = owner_producer_id
        self.verifier_id = verifier_id
        self.train_results = train_results
        self.task_declaration_id = task_declaration_id
        self.progress = kwargs.get('progress', 0)
        self.result = kwargs.get('result', None)
        self.error = kwargs.get('error', None)
        self.tflops = kwargs.get('tflops', None)
        self.encrypted_text = kwargs.get('encrypted_text', None)
        self.verified = kwargs.get('verified', None)

    def get_data(self):
        return {
            'owner_producer_id': self.owner_producer_id,
            'verifier_id': self.verifier_id,
            'train_results': self.encrypted_text or self.train_results,
            'task_declaration_id': self.task_declaration_id,
            'progress': self.progress,
            'result': self.result,
            'error': self.error,
            'tflops': self.tflops,
            'verified': self.verified
        }

    # noinspection PyMethodOverriding
    @classmethod
    def add(cls, node, verifier_id, train_results, task_declaration_id, *args, **kwargs):
        if node.node_type != Node.NodeType.PRODUCER:
            raise ValueError('Only producer can create task assignment')

        producer = node
        verifier_asset = node.db.retrieve_asset(verifier_id)
        verifier_address = verifier_asset.tx['outputs'][0]['public_keys'][0]

        train_data_encrypted = node.encryption.encrypt(
            json.dumps(train_results).encode(),
            verifier_asset.metadata['enc_key'],
        ).decode()

        verifier_assignment = cls(
            owner_producer_id=producer.asset_id,
            verifier_id=verifier_id,
            train_results=train_data_encrypted,
            task_declaration_id=task_declaration_id,
            asset_id=None
        )

        asset_id = producer.db.create_asset(
            data={'name': cls.task_type},
            metadata=verifier_assignment.get_data(),
            recipients=verifier_address,
        )[0]

        verifier_assignment.asset_id = asset_id
        return verifier_assignment

    @classmethod
    def get(cls, node, asset_id):
        asset = node.db.retrieve_asset(asset_id)
        encrypted_text = asset.metadata['train_results']
        try:
            train_results = json.loads(node.decrypt_text(encrypted_text))
        except json.JSONDecodeError:
            train_results = encrypted_text

        return cls(
            asset_id=asset_id,
            owner_producer_id=asset.metadata['owner_producer_id'],
            verifier_id=asset.metadata['verifier_id'],
            train_results=train_results,
            task_declaration_id=asset.metadata['task_declaration_id'],
            progress=asset.metadata['progress'],
            result=asset.metadata['result'],
            error=asset.metadata['error'],
            tflops=asset.metadata['tflops'],
            verified=asset.metadata['verified'],
            encrypted_text=encrypted_text
        )
