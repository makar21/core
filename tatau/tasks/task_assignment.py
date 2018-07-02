import json

from tatau.node.node import Node
from .task import Task


class TaskAssignment(Task):
    task_type = Task.TaskType.TASK_ASSIGNMENT

    def __init__(self, owner_producer_id, worker_id, train_data, task_declaration_id, asset_id, *args, **kwargs):
        super().__init__(asset_id, *args, **kwargs)
        self.owner_producer_id = owner_producer_id
        self.worker_id = worker_id
        self.train_data = train_data
        self.task_declaration_id = task_declaration_id
        self.progress = kwargs.get('progress', 0)
        self.result = kwargs.get('result', None)
        self.error = kwargs.get('error', None)
        self.tflops = kwargs.get('tflops', 0)
        self.encrypted_text = kwargs.get('encrypted_text', None)

    def get_data(self):
        return {
            'owner_producer_id': self.owner_producer_id,
            'worker_id': self.worker_id,
            'train_data': self.encrypted_text or self.train_data,
            'task_declaration_id': self.task_declaration_id,
            'progress': self.progress,
            'result': self.result,
            'error': self.error,
            'tflops': self.tflops
        }

    # noinspection PyMethodOverriding
    @classmethod
    def add(cls, node, worker_id, model_code, x_train_ipfs, y_train_ipfs, x_test_ipfs, y_test_ipfs, epochs,
            task_declaration_id, *args, **kwargs):
        if node.node_type != Node.NodeType.PRODUCER:
            raise ValueError('Only producer can create task assignment')

        producer = node
        worker_asset = node.db.retrieve_asset(worker_id)
        worker_address = worker_asset.tx['outputs'][0]['public_keys'][0]

        # TODO: use class
        train_data = dict(
            model_code=model_code,
            x_train_ipfs=x_train_ipfs,
            y_train_ipfs=y_train_ipfs,
            x_test_ipfs=x_test_ipfs,
            y_test_ipfs=y_test_ipfs,
            epochs=epochs,
        )

        train_data_encrypted = node.encryption.encrypt(
            json.dumps(train_data).encode(),
            worker_asset.data['enc_key'],
        ).decode()

        task_assignment = cls(
            owner_producer_id=producer.asset_id,
            worker_id=worker_id,
            train_data=train_data_encrypted,
            task_declaration_id=task_declaration_id,
            asset_id=None
        )

        asset_id = producer.db.create_asset(
            name=cls.task_type,
            data=task_assignment.get_data(),
            recipients=worker_address,
        )

        task_assignment.asset_id = asset_id
        return task_assignment

    @classmethod
    def get(cls, node, asset_id):
        asset = node.db.retrieve_asset(asset_id)
        encrypted_text = asset.data['train_data']
        try:
            train_data = json.loads(node.decrypt_text(encrypted_text))
        except json.JSONDecodeError:
            train_data = encrypted_text

        return cls(
            asset_id=asset_id,
            owner_producer_id=asset.data['owner_producer_id'],
            worker_id=asset.data['worker_id'],
            train_data=train_data,
            task_declaration_id=asset.data['task_declaration_id'],
            progress=asset.data['progress'],
            result=asset.data['result'],
            error=asset.data['error'],
            tflops=asset.data['tflops'],
            encrypted_text=encrypted_text
        )
