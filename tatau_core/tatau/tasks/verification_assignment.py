import json

from ..node.node import Node
from .task import Task


class VerificationAssignment(Task):
    task_type = Task.TaskType.VERIFICATION_ASSIGNMENT

    def __init__(self, owner_producer_id, verifier_id,
                 verification_declaration_id, task_declaration_id,
                 asset_id, train_results=None, *args, **kwargs):
        super().__init__(asset_id, *args, **kwargs)
        self.owner_producer_id = owner_producer_id
        self.verifier_id = verifier_id
        self.train_results = train_results
        self.task_declaration_id = task_declaration_id
        self.verification_declaration_id = verification_declaration_id
        self.progress = kwargs.get('progress', 0)
        self.result = kwargs.get('result', None)
        self.error = kwargs.get('error', None)
        self.tflops = kwargs.get('tflops', None)
        self.verified = kwargs.get('verified', None)

    def get_data(self):
        data = super(VerificationAssignment, self).get_data()
        data.update({
            'owner_producer_id': self.owner_producer_id,
            'verifier_id': self.verifier_id,
            'task_declaration_id': self.task_declaration_id,
            'verification_declaration_id': self.verification_declaration_id
        })
        return data

    def get_metadata(self):
        return {
            'progress': self.progress,
            'result': self.result,
            'error': self.error,
            'tflops': self.tflops,
            'verified': self.verified,
            'train_results': self.train_results,
        }

    def assign(self, node, train_results):
        if node.node_type != Node.NodeType.PRODUCER:
            raise ValueError('Only producer can assign verification assignment')

        producer = node
        verifier_asset = node.db.retrieve_asset(self.verifier_id)
        verifier_address = verifier_asset.tx['outputs'][0]['public_keys'][0]

        self.train_results = node.encryption.encrypt(
            json.dumps(train_results).encode(),
            verifier_asset.metadata['enc_key'],
        ).decode()

        self.save(db=producer.db, recipients=verifier_address)

    # noinspection PyMethodOverriding
    @classmethod
    def add(cls, node, producer_id, verification_declaration_id, task_declaration_id, *args, **kwargs):
        if node.node_type != Node.NodeType.VERIFIER:
            raise ValueError('Only verifier can create verification assignment')

        verifier = node
        producer_asset = node.db.retrieve_asset(producer_id)
        producer_address = producer_asset.tx['outputs'][0]['public_keys'][0]

        task_assignment = cls(
            owner_producer_id=producer_id,
            verifier_id=verifier.asset_id,
            verification_declaration_id=verification_declaration_id,
            task_declaration_id=task_declaration_id,
            asset_id=None
        )

        asset_id, created = verifier.db.create_asset(
            data=task_assignment.get_data(),
            recipients=producer_address,
        )

        task_assignment.asset_id = asset_id
        return task_assignment

    @classmethod
    def get(cls, node, asset_id):
        asset = node.db.retrieve_asset(asset_id)

        kwargs = {
            'asset_id': asset_id,
            'owner_producer_id': asset.data['owner_producer_id'],
            'verifier_id': asset.data['verifier_id'],
            'task_declaration_id': asset.data['task_declaration_id'],
            'verification_declaration_id': asset.data['verification_declaration_id'],
        }

        if asset.metadata:
            kwargs.update({
                'train_results': node.decrypt_text(asset.metadata['train_results']),
                'progress': asset.metadata['progress'],
                'result': asset.metadata['result'],
                'error': asset.metadata['error'],
                'tflops': asset.metadata['tflops'],
                'verified': asset.metadata['verified'],
            })

        return cls(**kwargs)
