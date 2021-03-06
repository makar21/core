import os
import shutil
import tempfile
import urllib.request
from logging import getLogger
from multiprocessing.pool import ThreadPool

import ipfsapi

from tatau_core import settings
from tatau_core.settings import IPFS_GATEWAY_HOST
from tatau_core.utils.signleton import singleton
from tatau_core.utils.misc import get_dir_size

logger = getLogger('tatau_core')


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


@singleton
class IPFS:
    def __init__(self, host=settings.IPFS_HOST, port=settings.IPFS_PORT):
        self.api = ipfsapi.connect(host, port, chunk_size=1024*1024)

    @property
    def id(self):
        return self.api.id()['ID']

    @property
    def public_key(self):
        return self.api.id()['PublicKey']

    def download(self, multihash, target_dir):
        logger.debug('Downloading {} to {}'.format(multihash, target_dir))
        self.api.get(multihash, filepath=target_dir, compress=False)
        target_path = os.path.join(target_dir, multihash)
        if not os.path.exists(target_path):
            logger.warning('IPFS download failed, try using gateway')
            result = urllib.request.urlretrieve(
                url='http://{}/ipfs/{}'.format(IPFS_GATEWAY_HOST, multihash), filename=target_path)
            logger.debug('URL Retrieve: {}'.format(result))

        if os.path.isfile(target_path):
            logger.debug(
                'Downloaded file {} size: {}Mb'.format(target_path, os.path.getsize(target_path) / 1024. / 1024.))
        else:
            logger.debug(
                'Downloaded dir {} size: {}Mb'.format(target_path, get_dir_size(target_path) / 1024. / 1024.))
        return target_path

    def download_to(self, multihash, target_path):
        with tempfile.TemporaryDirectory() as tmp_dir:
            downloaded_path = self.download(multihash, tmp_dir)
            os.rename(downloaded_path, target_path)
            logger.info('Moved {} -> {}'.format(downloaded_path, target_path))

    def read(self, multihash):
        logger.info('Reading {}'.format(multihash))
        return self.api.cat(multihash)

    def add_file(self, file_path):
        logger.info('Uploading file {} {}Mb'.format(file_path, os.path.getsize(file_path) / 1024. / 1024.))
        if os.path.isdir(file_path):
            raise ValueError('"{}" must be a path to file, not a to dir'.format(file_path))

        data = self.api.add(file_path)
        result = File(ipfs_data=data)
        logger.info('Upload complete: {}'.format(file_path))
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

    def ls(self, multihash, **kwargs):
        """Returns a list of objects linked to by the given hash."""
        return self.api.ls(multihash=multihash, **kwargs)

    def clean_repo(self):
        # remove all pined files
        for m, v in self.api.pin_ls('recursive')['Keys'].items():
            self.api.pin_rm(m)

        # clean repo
        self.api.repo_gc()

    def remove_from_storage(self, multihash):
        try:
            logger.debug('Removing from IPFS storage {}'.format(multihash))
            self.api.pin_rm(multihash)

            # blocks_rm ???
        except ipfsapi.exceptions.ErrorResponse as ex:
            logger.debug(ex)


class Downloader:

    def __init__(self, storage_name, base_dir=None, pool_size=None):
        self.base_dir = base_dir or settings.TATAU_STORAGE_BASE_DIR
        try:
            os.mkdir(self.base_dir)
        except FileExistsError:
            pass

        self.storage_dir_path = os.path.join(self.base_dir, storage_name)
        try:
            os.mkdir(self.storage_dir_path)
        except FileExistsError:
            pass
        self.pool_size = pool_size or settings.DOWNLOAD_POOL_SIZE
        self._ipfs_instance = None
        self._download_data = {}

    @property
    def _ipfs(self):
        if self._ipfs_instance:
            return self._ipfs_instance

        self._ipfs_instance = IPFS()
        return self._ipfs_instance

    def _download(self, multihash: str, file_names: list):
        logger.debug('Start download {}'.format(multihash))
        target_path = os.path.join(self.storage_dir_path, multihash)

        if os.path.exists(target_path):
            logger.debug('Already exist: {}'.format(target_path))
        else:
            self._ipfs.download(multihash, self.storage_dir_path)
            self._ipfs.remove_from_storage(multihash)

        for file_name in file_names:
            link_path = self.resolve_path(file_name)
            try:
                os.symlink(target_path, link_path)
            except FileExistsError as ex:
                if os.readlink(link_path) == target_path:
                    pass
                else:
                    logger.exception(ex)
                    raise

    def _download_wrapper(self, kwargs: dict):
        self._download(**kwargs)

    def add_to_download_list(self, multihash, file_name):
        try:
            self._download_data[multihash].append(file_name)
        except KeyError:
            self._download_data[multihash] = [file_name]

    def download_all(self):
        with ThreadPool(self.pool_size) as p:
            return p.map(self._download_wrapper, [
                {
                    'multihash': key,
                    'file_names': value
                } for key, value in self._download_data.items()
            ])

    def remove_storage(self):
        try:
            shutil.rmtree(self.storage_dir_path)
        except FileNotFoundError:
            pass

    def resolve_path(self, file_name):
        return os.path.join(self.storage_dir_path, file_name)

    def remove_from_storage(self, multihash):
        path_in_storage = self.resolve_path(multihash)
        logger.debug('Removing {}'.format(path_in_storage))

        try:
            shutil.rmtree(path_in_storage)
        except NotADirectoryError:
            os.remove(path_in_storage)
        except FileNotFoundError:
            pass
