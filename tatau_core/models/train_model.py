from logging import getLogger

from tatau_core.db import models, fields
from tatau_core.utils.ipfs import IPFS

logger = getLogger()


class TrainModel(models.Model):
    name = fields.CharField(immutable=True)
    code_ipfs = fields.EncryptedCharField(immutable=True)

    @classmethod
    def upload_and_create(cls, code_path, **kwargs):
        code_ipfs = IPFS().add_file(code_path).multihash
        return cls.create(code_ipfs=code_ipfs, **kwargs)
