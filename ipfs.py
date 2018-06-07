import ipfsapi
import os


class File:
    def __init__(self, multihash=None, ipfs_data=None, api=None):
        if multihash is None and ipfs_data is None:
            raise ValueError('"multihash" and "ipfs_data" cant be None')

        self._api = api if api is not None else IPFS().api
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
        return self._api.cat(self.multihash)

    def download_to(self, file_path):
        self._api.download(self.multihash, file_path)


class Directory(File):
    def ls(self):
        dirs = []
        files = []

        data = self._api.ls(self.multihash)
        for obj in data['Objects']:
            if obj['Hash'] != self.multihash:
                continue

            for ipfs_obj in obj['Links']:
                if ipfs_obj['Type'] == 1:
                    dirs.append(Directory(ipfs_data=ipfs_obj, api=self._api))
                    continue

                if ipfs_obj['Type'] == 2:
                    files.append(File(ipfs_data=ipfs_obj, api=self._api))
                    continue

        return dirs, files


class IPFS:
    def __init__(self, host='127.0.0.1', port=5001):
        self.api = ipfsapi.connect(host, port)

    @property
    def id(self):
        return self.api.id()['ID']

    @property
    def public_key(self):
        return self.api.id()['PublicKey']

    def download(self, multihash, target_dir):
        self.api.get(multihash, filepath=target_dir)
        return os.path.join(target_dir, multihash)

    def add_file(self, file_path):
        if os.path.isdir(file_path):
            raise ValueError('"{}" must be a path to file, not a to dir'.format(file_path))

        data = self.api.add(file_path)
        return File(ipfs_data=data, api=self.api)

    def add_dir(self, dir_path, recursive=False):
        if not os.path.isdir(dir_path):
            raise ValueError('"{}" must be a path to dir, not a to file'.format(dir_path))

        raw_data = self.api.add(dir_path, recursive=recursive)
        for file_data in raw_data:
            if os.path.realpath(dir_path).lower() == os.path.realpath(file_data['Name']).lower():
                return Directory(ipfs_data=file_data, api=self.api)

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

# tests
# ipfs = IPFS()
#
# print('id:', ipfs.id)
# print('public key:', ipfs.public_key)
#
#
# f = ipfs.add_file('uploads/test1.txt')
# print(f.read())
#
# d = ipfs.add_dir('/home/jeday/projects/tatau/core/uploads')
# print(d.ls())

#file_path = ipfs.download('QmPv7fpXPVV21q7Ee3qK759R9qdS6KYqnXcp2w86m7unpG', 'downloads')
#print(file_path)

#file_path = ipfs.download('QmdpdMQVcHb3oBWkCEH4bvYmVx6tzGTAqj4H9EUe7Kbq5w', 'downloads')
#print(file_path)
#
# data = ipfs.api.refs('QmPv7fpXPVV21q7Ee3qK759R9qdS6KYqnXcp2w86m7unpG')
# data = ipfs.api.refs('QmdpdMQVcHb3oBWkCEH4bvYmVx6tzGTAqj4H9EUe7Kbq5w')
#
# data = ipfs.api.object_links('QmPv7fpXPVV21q7Ee3qK759R9qdS6KYqnXcp2w86m7unpG')
# data = ipfs.api.object_links('QmdpdMQVcHb3oBWkCEH4bvYmVx6tzGTAqj4H9EUe7Kbq5w')
#
# print(ipfs.api.repo_stat())


