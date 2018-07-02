import json
import logging
import os
import shutil
import tempfile

import numpy as np

from ipfs import IPFS

logger = logging.getLogger()


class DataSet:
    asset_name = 'Dataset'

    def __init__(self, owner_producer_id, name, train_dir_ipfs, x_test_ipfs, y_test_ipfs, encrypted_text=None,
                 asset_id=None):
        self.owner_producer_id = owner_producer_id
        self.name = name
        self.asset_id = asset_id
        self.train_dir_ipfs = train_dir_ipfs
        self.x_test_ipfs = x_test_ipfs
        self.y_test_ipfs = y_test_ipfs
        self.encrypted_text = encrypted_text

    def get_data(self):
        return {
            'owner_producer_id': self.owner_producer_id,
            'name': self.name,
            'train_dir_ipfs': self.train_dir_ipfs,
            'x_test_ipfs': self.x_test_ipfs,
            'y_test_ipfs': self.y_test_ipfs
        }

    def to_json(self):
        if self.encrypted_text is not None:
            return self.encrypted_text

        return json.dumps(self.get_data())

    @classmethod
    def add(cls, producer, name, x_train_path, y_train_path, x_test_path, y_test_path, files_count):
        from tatau.node import Node
        if producer.node_type != Node.NodeType.PRODUCER:
            raise ValueError('Only producer can add dataset')

        ipfs = IPFS()

        x_test_ipfs = ipfs.add_file(x_test_path).multihash
        y_test_ipfs = ipfs.add_file(y_test_path).multihash

        directory = tempfile.mkdtemp()
        try:
            # TODO: determine files_count
            # file_size = os.path.getsize(x_train_ds_path)
            # files_count = int(file_size / 4096)

            with np.load(x_train_path) as fx, np.load(y_train_path) as fy:
                split_x = np.split(fx[fx.files[0]], files_count)
                split_y = np.split(fy[fy.files[0]], files_count)
                for i in range(files_count):
                    np.savez(os.path.join(directory, 'x_{}'.format(i)), split_x[i])
                    np.savez(os.path.join(directory, 'y_{}'.format(i)), split_y[i])

                train_dir_ipfs = ipfs.add_dir(directory).multihash
        finally:
            shutil.rmtree(directory)

        dataset = cls(
            owner_producer_id=producer.asset_id,
            name=name,
            train_dir_ipfs=train_dir_ipfs,
            x_test_ipfs=x_test_ipfs,
            y_test_ipfs=y_test_ipfs
        )

        asset_id = producer.db.create_asset(
            data={'name': cls.asset_name},
            metadata={
                'producer_id': producer.asset_id,
                'name': dataset.name,
                'dataset': producer.encrypt_text(dataset.to_json())
            }
        )

        dataset.asset_id = asset_id

        logger.info('Producer "{}" added dataset, name: {}, asset_id: {}'.format(producer.asset_id, name, asset_id))
        return dataset

    @classmethod
    def get(cls, node, asset_id):
        asset = node.db.retrieve_asset(asset_id)
        encrypted_text = asset.metadata['dataset']
        try:
            dataset_data = json.loads(node.decrypt_text(encrypted_text))
        except json.JSONDecodeError:
            dataset_data = {
                'owner_producer_id': 'encrypted',
                'name': 'encrypted',
                'train_dir_ipfs': 'encrypted',
                'x_test_ipfs': 'encrypted',
                'y_test_ipfs': 'encrypted'
            }

        logger.info('{} {} load dataset, name:{}, asset_id: {}'.format(
            node.node_type, node.asset_id, asset.metadata['name'], asset_id)
        )

        return cls(
            owner_producer_id=asset.metadata['producer_id'],
            name=asset.metadata['name'],
            train_dir_ipfs=dataset_data['train_dir_ipfs'],
            x_test_ipfs=dataset_data['x_test_ipfs'],
            y_test_ipfs=dataset_data['y_test_ipfs'],
            asset_id=asset_id,
            encrypted_text=encrypted_text
        )

    @classmethod
    def list(cls, producer):
        # TODO: implement list of producer's datasets
        raise NotImplemented
