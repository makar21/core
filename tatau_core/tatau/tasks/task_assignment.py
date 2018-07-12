import json

from ..node.node import Node
from .task import Task


class TaskAssignment(Task):
    task_type = Task.TaskType.TASK_ASSIGNMENT

    def __init__(self, owner_producer_id, worker_id,
                 task_declaration_id, asset_id, train_data=None,
                 *args, **kwargs):
        super().__init__(asset_id, *args, **kwargs)
        self.owner_producer_id = owner_producer_id
        self.worker_id = worker_id
        self.train_data = train_data
        self.train_data_encrypted = None
        self.task_declaration_id = task_declaration_id
        self.progress = kwargs.get('progress', 0)
        self.result = kwargs.get('result', None)
        self.error = kwargs.get('error', None)
        self.tflops = kwargs.get('tflops', 0)

    def __str__(self):
        return 'Task Assignment: p:{} w:{} t:{} {}% {} result:{}'.format(
            self.owner_producer_id,
            self.worker_id,
            self.task_declaration_id,
            self.progress,
            self.tflops,
            self.result
        )

    def get_data(self):
        data = super(TaskAssignment, self).get_data()
        data.update({
            'owner_producer_id': self.owner_producer_id,
            'task_declaration_id': self.task_declaration_id,
            'worker_id': self.worker_id,
        })
        return data

    def get_metadata(self):
        metadata = {
            'progress': self.progress,
            'result': self.result,
            'error': self.error,
            'tflops': self.tflops,
        }

        if self.train_data_encrypted:
            metadata['train_data'] = self.train_data_encrypted

        return metadata

    def assign(self, node, model_code,
               x_train_ipfs, y_train_ipfs,
               x_test_ipfs, y_test_ipfs,
               batch_size, epochs):
        if node.node_type != Node.NodeType.PRODUCER:
            raise ValueError('Only producer can assign task assignment')

        producer = node
        worker_asset = node.db.retrieve_asset(self.worker_id)
        worker_address = worker_asset.tx['outputs'][0]['public_keys'][0]

        train_data = dict(
            model_code=model_code,
            x_train_ipfs=x_train_ipfs,
            y_train_ipfs=y_train_ipfs,
            x_test_ipfs=x_test_ipfs,
            y_test_ipfs=y_test_ipfs,
            batch_size=batch_size,
            epochs=epochs,
        )

        self.train_data_encrypted = node.encryption.encrypt(
            json.dumps(train_data).encode(),
            worker_asset.metadata['enc_key'],
        ).decode()

        self.save(db=producer.db, recipients=worker_address)

    # noinspection PyMethodOverriding
    @classmethod
    def add(cls, node, producer_id, task_declaration_id, *args, **kwargs):
        if node.node_type != Node.NodeType.WORKER:
            raise ValueError('Only worker can create task assignment')

        worker = node
        producer_asset = node.db.retrieve_asset(producer_id)
        producer_address = producer_asset.tx['outputs'][0]['public_keys'][0]

        task_assignment = cls(
            owner_producer_id=producer_id,
            worker_id=worker.asset_id,
            task_declaration_id=task_declaration_id,
            asset_id=None
        )

        asset_id, created = worker.db.create_asset(
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
            'worker_id': asset.data['worker_id'],
            'task_declaration_id': asset.data['task_declaration_id'],
        }
        if asset.metadata:
            if 'train_data' in asset.metadata:
                encrypted_text = asset.metadata['train_data']
                try:
                    kwargs['train_data'] = json.loads(
                        node.decrypt_text(encrypted_text)
                    )
                except json.JSONDecodeError:
                    kwargs['train_data_encrypted'] = encrypted_text
            kwargs.update({
                'progress': asset.metadata['progress'],
                'result': asset.metadata['result'],
                'error': asset.metadata['error'],
                'tflops': asset.metadata['tflops'],
            })

        return cls(**kwargs)
