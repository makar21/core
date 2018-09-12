import os
import tempfile
from logging import getLogger
from multiprocessing import Pool

import ipfsapi

from tatau_core import settings

logger = getLogger()


class File:
    def __init__(self, multihash=None, ipfs_data=None):
        if multihash is None and ipfs_data is None:
            raise ValueError('"multihash" and "ipfs_data" cant be None')

        self._ipfs = IPFS()
        self._multihash = multihash if multihash is not None else ipfs_data['Hash']
        self._name = ipfs_data['Name'] if ipfs_data is not None else None
        self._size = ipfs_data['Size'] if ipfs_data is not None else None

    @property
    def multihash(self):
        return self._multihash

    @property
    def name(self):
        return self._name

    @property
    def size(self):
        return self._size

    def read(self):
        logger.debug('Reading {}'.format(self.multihash))
        return self._ipfs.api.cat(self.multihash)

    def download_to(self, target_dir):
        logger.debug('Downloading file {} to {}'.format(self.multihash, target_dir))
        return self._ipfs.download(self.multihash, target_dir)


class Directory(File):
    def ls(self):
        dirs = []
        files = []

        data = self._ipfs.api.ls(self.multihash)
        for obj in data['Objects']:
            if obj['Hash'] != self.multihash:
                continue

            for ipfs_obj in obj['Links']:
                if ipfs_obj['Type'] == 1:
                    dirs.append(Directory(ipfs_data=ipfs_obj))
                    continue

                if ipfs_obj['Type'] == 2:
                    files.append(File(ipfs_data=ipfs_obj))
                    continue

        return dirs, files


class IPFS:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not isinstance(cls._instance, cls):
            cls._instance = object.__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, host=settings.IPFS_HOST, port=settings.IPFS_PORT):
        self.api = ipfsapi.connect(host, port, chunk_size=1024*1024)

    @property
    def id(self):
        return self.api.id()['ID']

    @property
    def public_key(self):
        return self.api.id()['PublicKey']

    def download(self, multihash, target_dir):
        logger.info('Downloading {}'.format(multihash))
        self.api.get(multihash, filepath=target_dir, compress=False)
        target_path = os.path.join(target_dir, multihash)
        logger.info('Downloaded file size: {}Mb'.format(os.path.getsize(target_path) / 1024. / 1024.))
        return target_path

    def download_to(self, multihash, target_path):
        downloaded_path = self.download(multihash, tempfile.gettempdir())
        os.rename(downloaded_path, target_path)

    def read(self, multihash):
        logger.info('Reading {}'.format(multihash))
        return self.api.cat(multihash)

    def add_file(self, file_path):
        logger.info('Uploading file {} {}Mb'.format(file_path, os.path.getsize(file_path) / 1024. / 1024.))
        if os.path.isdir(file_path):
            raise ValueError('"{}" must be a path to file, not a to dir'.format(file_path))

        data = self.api.add(file_path)
        result = File(ipfs_data=data)
        logger.info("Upload complete: {}".format(file_path))
        return result

    def add_dir(self, dir_path, recursive=False):
        logger.debug('Uploading directory {}'.format(dir_path))
        if not os.path.isdir(dir_path):
            raise ValueError('"{}" must be a path to dir, not a to file'.format(dir_path))

        raw_data = self.api.add(dir_path, recursive=recursive)
        for file_data in raw_data:
            if os.path.basename(dir_path).lower() == os.path.basename(file_data['Name']).lower():
                return Directory(ipfs_data=file_data)

        raise Exception('WTF? Where is my dir?')

    def start_listen_messages(self, topic, callback_function):
        with self.api.pubsub_sub(topic=topic) as channel:
            # TODO: move to another thread and add stop condition
            # note daemon has to be started with --enable-pubsub-experiment
            while True:
                message = channel.read_message()
                callback_function(message)

    def send_message(self, topic, data):
        self.api.pubsub_pub(topic, data)


class Downloader:
    class DownloadParams:
        def __init__(self, multihash, target_path):
            self.multihash = multihash
            self.target_path = target_path

    @staticmethod
    def _download(download_params: DownloadParams):
        ipfs = IPFS()
        ipfs.download_to(download_params.multihash, download_params.target_path)

    @classmethod
    def download_all(cls, list_download_params, pool_size=settings.DOWNLOAD_POOL_SIZE):
        with Pool(pool_size) as p:
            return p.map(cls._download, list_download_params)
