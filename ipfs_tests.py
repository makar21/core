import filecmp
import os
import shutil

from ipfs import IPFS, File, Directory

if __name__ == '__main__':

    ipfs = IPFS()

    print('id:', ipfs.id)
    print('public key:', ipfs.public_key)

    dir_path = 'test_upload'
    if not os.path.exists(dir_path):
        os.mkdir(dir_path)
    try:
        file_content = bytearray('This is test file content'.encode('UTF-8'))
        file_path = os.path.join(dir_path, 'test_file.txt')
        with open(file_path, 'wb') as f:
            f.write(file_content)

        ipfs_file = ipfs.add_file(file_path)
        uploaded_content = ipfs_file.read()
        print('TEST upload and read: {}'.format('SUCCEEDED' if uploaded_content == file_content else 'FAILED'))

        downloaded_file_path = File(ipfs_file.multihash).download_to(dir_path)
        print('TEST upload and download: {}'.format(
            'SUCCEEDED' if filecmp.cmp(file_path, downloaded_file_path) else 'FAILED'))

        ipfs_dir = ipfs.add_dir(dir_path)
        print('Uploaded dir:')
        print(ipfs_dir.ls())

        target_download_dir = 'test_dir'
        if os.path.exists(target_download_dir):
            shutil.rmtree(target_download_dir)

        # path = ipfs_dir.download_to('test_dir') # work too
        path = Directory(ipfs_dir.multihash).download_to(target_download_dir)
        print('Dir {} downloaded to {}'.format(ipfs_dir.multihash, path))
    finally:
        shutil.rmtree(dir_path)

