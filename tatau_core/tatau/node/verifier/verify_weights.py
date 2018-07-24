import shutil
import tempfile
from logging import getLogger

from tatau_core.utils.ipfs import IPFS

logger = getLogger()


def verify_train_results(train_results):
    logger.info('Verify results: {}'.format(train_results))
    target_dir = tempfile.mkdtemp()
    try:
        ipfs = IPFS()

        downloaded_results = []
        for worker_result in train_results:
            if worker_result['result'] is None:
                # TODO: handle this
                pass
            else:
                downloaded_results.append({
                    'worker_id': worker_result['worker_id'],
                    'file_path': ipfs.download(worker_result['result'], target_dir)
                })

        return verify(downloaded_results)
    finally:
        shutil.rmtree(target_dir)


def verify(results):
    return [{
        'worker_id': x['worker_id'],
        'is_fake': False
    } for x in results]
