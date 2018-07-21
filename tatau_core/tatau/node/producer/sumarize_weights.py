import logging
import shutil
import tempfile

from tatau_core.utils.ipfs import IPFS

log = logging.getLogger()


def summarize_weights(train_results):
    target_dir = tempfile.mkdtemp()
    try:
        ipfs = IPFS()

        downloaded_results = []
        for worker_result in train_results:
            if worker_result['result'] is None:
                # TODO: handle this
                pass
            else:
                downloaded_results.append(ipfs.download(worker_result['result'], target_dir))
        return summarize(downloaded_results)
    finally:
        shutil.rmtree(target_dir)


def summarize(downloaded_results):
    log.info('Summarize {}'.format(downloaded_results))
    if len(downloaded_results):
        ipfs = IPFS()
        return ipfs.add_file(downloaded_results[0]).multihash
    else:
        return 'error: list of weights is empty'
