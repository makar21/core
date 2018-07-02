import json
import logging

from ipfs import IPFS
from tatau.node.node import Node

logger = logging.getLogger()


class TrainModel:
    asset_name = 'TRAIN MODEL'

    def __init__(self, owner_producer_id, name, code_ipfs, encrypted_text=None, asset_id=None):
        self.owner_producer_id = owner_producer_id
        self.name = name
        self.asset_id = asset_id
        self.code_ipfs = code_ipfs
        self.encrypted_text = encrypted_text

    def get_data(self):
        return {
            'owner_producer_id': self.owner_producer_id,
            'name': self.name,
            'code_ipfs': self.code_ipfs
        }

    def to_json(self):
        if self.encrypted_text is not None:
            return self.encrypted_text

        return json.dumps(self.get_data())

    @classmethod
    def add(cls, producer, name, code_path):
        if producer.node_type != Node.NodeType.PRODUCER:
            raise ValueError('Only producer can add train model')

        ipfs = IPFS()

        code_ipfs = ipfs.add_file(code_path).multihash

        train_model = cls(
            owner_producer_id=producer.asset_id,
            name=name,
            code_ipfs=code_ipfs
        )

        asset_id = producer.db.create_asset(
            name=cls.asset_name,
            data={
                'producer_id': producer.asset_id,
                'name': train_model.name,
                'train_model': producer.encrypt_text(train_model.to_json())
            }
        )

        train_model.asset_id = asset_id

        logger.info('Producer "{}" added train model, name: {}, asset_id: {}'.format(producer.asset_id, name, asset_id))
        return train_model

    @classmethod
    def get(cls, node, asset_id):
        asset = node.db.retrieve_asset(asset_id)
        encrypted_text = asset.data['train_model']
        try:
            train_model_data = json.loads(node.decrypt_text(encrypted_text))
        except json.JSONDecodeError:
            train_model_data = {
                'name': 'encrypted',
                'code_ipfs': 'encrypted'
            }

        logger.info('{} {} load train model, name:{}, asset_id: {}'.format(
            node.node_type, node.asset_id, asset.data['name'], asset_id)
        )

        return cls(
            owner_producer_id=asset.data['producer_id'],
            name=asset.data['name'],
            code_ipfs=train_model_data['code_ipfs'],
            asset_id=asset_id,
            encrypted_text=encrypted_text
        )

    @classmethod
    def list(cls, producer):
        # TODO: implement list of producer's train models
        raise NotImplemented
