import logging

from ipfs import IPFS
from tatau.node.node import Node

logger = logging.getLogger()


class TrainModel:
    asset_name = 'Train model'

    def __init__(self, name, code_ipfs, encrypted_text=None, asset_id=None):
        self.name = name
        self.asset_id = asset_id
        self.code_ipfs = code_ipfs
        self.encrypted_text = encrypted_text

    def get_data(self):
        return {
            'name': self.asset_name,
        }

    def get_metadata(self):
        return {
            'name': self.name,
            'code_ipfs': self.encrypted_text or self.code_ipfs
        }

    def save(self, producer):
        if producer.node_type != Node.NodeType.PRODUCER:
            raise ValueError('Only producer can save train model')

        producer.db.update_asset(
            asset_id=self.asset_id,
            metadata=self.get_metadata()
        )

    @classmethod
    def add(cls, producer, name, code_path):
        if producer.node_type != Node.NodeType.PRODUCER:
            raise ValueError('Only producer can add train model')

        ipfs = IPFS()
        code_ipfs = ipfs.add_file(code_path).multihash

        train_model = cls(
            name=name,
            code_ipfs=producer.encrypt_text(code_ipfs)
        )

        asset_id, created = producer.db.create_asset(
            data=train_model.get_data(),
            metadata=train_model.get_metadata()
        )

        train_model.asset_id = asset_id
        train_model.code_ipfs = code_ipfs

        logger.debug('Producer "{}" added train model, name: {}, asset_id: {}'.format(
            producer.asset_id, name, asset_id)
        )

        return train_model

    @classmethod
    def get(cls, node, asset_id):
        asset = node.db.retrieve_asset(asset_id)

        logger.debug('{} {} load train model, name:{}, asset_id: {}'.format(
            node.node_type, node.asset_id, asset.metadata['name'], asset_id)
        )

        return cls(
            name=asset.metadata['name'],
            code_ipfs=node.decrypt_text(asset.metadata['code_ipfs']),
            encrypted_text=asset.metadata['code_ipfs'],
            asset_id=asset_id,
        )

    @classmethod
    def list(cls, producer):
        producer.db.connect_to_mongodb()
        match = {
            'assets.data.name': cls.asset_name,
        }

        return [cls.get(producer, x) for x in producer.db.retrieve_asset_ids(match=match)]

