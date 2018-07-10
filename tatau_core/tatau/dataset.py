import logging
import os
import shutil
import tempfile

import numpy as np

from tatau_core.ipfs import IPFS
import logging
import os
import shutil
import tempfile

import numpy as np

logger = logging.getLogger()


class DataSet:
    asset_name = 'Dataset'

    def __init__(self, name, train_dir_ipfs, x_test_ipfs, y_test_ipfs, asset_id=None, **kwargs):
        self.asset_id = asset_id
        self.name = name
        self.train_dir_ipfs = train_dir_ipfs
        self.encrypted_train_dir_ipfs = kwargs.get('encrypted_train_dir_ipfs', None)

        self.x_test_ipfs = x_test_ipfs
        self.encrypted_x_test_ipfs = kwargs.get('encrypted_x_test_ipfs', None)

        self.y_test_ipfs = y_test_ipfs
        self.encrypted_y_test_ipfs = kwargs.get('encrypted_y_test_ipfs', None)

    def get_data(self):
        return {
            'name': self.asset_name,
        }

    def get_metadata(self):
        return {
            'name': self.name,
            'train_dir_ipfs': self.encrypted_train_dir_ipfs or self.train_dir_ipfs,
            'x_test_ipfs': self.encrypted_x_test_ipfs or self.x_test_ipfs,
            'y_test_ipfs': self.encrypted_y_test_ipfs or self.y_test_ipfs
        }

    def save(self, producer):
        from .node import Node
        if producer.node_type != Node.NodeType.PRODUCER:
            raise ValueError('Only producer can save dataset')

        producer.db.update_asset(
            asset_id=self.asset_id,
            metadata=self.get_metadata()
        )

    @classmethod
    def add(cls, producer, name, x_train_path, y_train_path, x_test_path, y_test_path, files_count):
        from .node import Node
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
            name=name,
            train_dir_ipfs=producer.encrypt_text(train_dir_ipfs),
            x_test_ipfs=producer.encrypt_text(x_test_ipfs),
            y_test_ipfs=producer.encrypt_text(y_test_ipfs)
        )

        asset_id, created = producer.db.create_asset(
            data=dataset.get_data(),
            metadata=dataset.get_metadata()
        )

        dataset.asset_id = asset_id

        logger.debug('Producer "{}" added dataset, name: {}, asset_id: {}'.format(producer.asset_id, name, asset_id))
        return dataset

    @classmethod
    def get(cls, node, asset_id):
        asset = node.db.retrieve_asset(asset_id)

        logger.debug('{} {} load dataset, name: {}, asset_id: {}'.format(
            node.node_type, node.asset_id, asset.metadata['name'], asset_id)
        )

        return cls(
            name=asset.metadata['name'],
            train_dir_ipfs=node.decrypt_text(asset.metadata['train_dir_ipfs']),
            encrypted_train_dir_ipfs=asset.metadata['train_dir_ipfs'],
            x_test_ipfs=node.decrypt_text(asset.metadata['x_test_ipfs']),
            encrypted_x_test_ipfs=asset.metadata['x_test_ipfs'],
            y_test_ipfs=node.decrypt_text(asset.metadata['y_test_ipfs']),
            encrypted_y_test_ipfs=asset.metadata['y_test_ipfs'],
            asset_id=asset_id
        )

    @classmethod
    def list(cls, producer):
        producer.db.connect_to_mongodb()
        match = {
            'assets.data.name': cls.asset_name,
        }

        return [cls.get(producer, x) for x in producer.db.retrieve_asset_ids(match=match)]
