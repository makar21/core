import csv
import os
import shutil
import tempfile
from io import StringIO
from logging import getLogger

import numpy as np

from tatau_core.db import models, fields
from tatau_core.utils.file_downloader import FileDownloader
from tatau_core.utils.ipfs import IPFS

logger = getLogger()


class Dataset(models.Model):
    name = fields.CharField(immutable=True)
    train_dir_ipfs = fields.EncryptedCharField(immutable=True)
    test_dir_ipfs = fields.EncryptedCharField(immutable=True)

    @classmethod
    def upload_and_create(cls, train_dir, test_dir, **kwargs):
        logger.info('Creating dataset')
        ipfs = IPFS()

        kwargs['test_dir_ipfs'] = ipfs.add_dir(test_dir).multihash
        kwargs['train_dir_ipfs'] = ipfs.add_dir(train_dir).multihash

        return cls.create(**kwargs)

    @staticmethod
    def _download_to_dir(csv_text, target_dir, train_part=True):
        urls = []
        for row in csv.reader(StringIO(csv_text), delimiter=',', quotechar='"'):
            urls.append({
                'x_url': row[0],
                'y_url': row[1]
            })

        if train_part:
            name_format = '_train_{{:0{}d}}'.format(len(urls) + 1)
        else:
            name_format = '_test_{{:0{}d}}'.format(len(urls) + 1)

        download_list = []
        for index, u in enumerate(urls):
            download_list += [
                FileDownloader.Params(
                    url=u['x_url'],
                    target_path=os.path.join(target_dir, 'x' + name_format.format(index))
                ),
                FileDownloader.Params(
                    url=u['y_url'],
                    target_path=os.path.join(target_dir, 'y' + name_format.format(index))
                )
            ]

        FileDownloader.download_all(download_list)

    @staticmethod
    def parse_csv_and_upload_to_ipfs(csv_text, train_part):
        target_dir = tempfile.mkdtemp()
        try:
            Dataset._download_to_dir(csv_text, target_dir, train_part)
            ipfs = IPFS()
            return ipfs.add_dir(target_dir).multihash
        finally:
            shutil.rmtree(target_dir)

    @classmethod
    def create_from_csv(cls, train_csv_text, test_csv_text, **kwargs):
        train_dir = tempfile.mkdtemp()
        test_dir = tempfile.mkdtemp()
        try:
            kwargs['test_dir_ipfs'] = Dataset.parse_csv_and_upload_to_ipfs(train_csv_text, train_part=False)
            logger.info('Test part is uploaded: {}'.format(kwargs['test_dir_ipfs']))

            kwargs['test_dir_ipfs'] = Dataset.parse_csv_and_upload_to_ipfs(test_csv_text, train_part=True)
            logger.info('Train part is uploaded: {}'.format(kwargs['train_dir_ipfs']))
        finally:
            shutil.rmtree(train_dir)
            shutil.rmtree(test_dir)

        return cls.create(**kwargs)

    @classmethod
    def upload_and_create_old(cls, x_train_path, y_train_path, x_test_path, y_test_path, minibatch_size, **kwargs):
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
