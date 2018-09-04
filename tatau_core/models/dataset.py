import os
import shutil
import tempfile
from logging import getLogger

import numpy as np

from tatau_core.db import models, fields
from tatau_core.utils.ipfs import IPFS

logger = getLogger()


class Dataset(models.Model):
    name = fields.CharField()
    train_dir_ipfs = fields.EncryptedCharField()
    x_test_ipfs = fields.EncryptedCharField()
    y_test_ipfs = fields.EncryptedCharField()

    @classmethod
    def upload_and_create(cls, x_train_path, y_train_path, x_test_path, y_test_path, minibatch_size, **kwargs):
        logger.info('Creating dataset')
        ipfs = IPFS()

        kwargs['x_test_ipfs'] = ipfs.add_file(x_test_path).multihash
        kwargs['y_test_ipfs'] = ipfs.add_file(y_test_path).multihash

        directory = tempfile.mkdtemp()
        try:

            # TODO: determine files_count
            # file_size = os.path.getsize(x_train_ds_path)
            # files_count = int(file_size / 4096)
            x_train = np.load(x_train_path)
            y_train = np.load(y_train_path)
            batches = int(len(x_train) / minibatch_size)
            logger.info('Split dataset to {} batches'.format(batches))
            for batch_idx in range(0, batches):
                start_idx = batch_idx * minibatch_size
                end_idx = start_idx + minibatch_size
                x_batch = x_train[start_idx: end_idx]
                y_batch = y_train[start_idx: end_idx]
                x_path = os.path.join(directory, 'x_{:04d}'.format(batch_idx))
                np.save(x_path, x_batch)
                y_path = os.path.join(directory, 'y_{:04d}'.format(batch_idx))
                np.save(y_path, y_batch)
            logger.info('Upload dataset to IPFS')
            kwargs['train_dir_ipfs'] = ipfs.add_dir(directory).multihash
            logger.info('Dataset was uploaded')
        finally:
            logger.debug('Cleanup dataset tmp dir')
            shutil.rmtree(directory)

        return cls.create(**kwargs)
