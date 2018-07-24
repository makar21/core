from logging import getLogger
import shutil
import tempfile
from collections import deque
import numpy as np
from tatau_core.nn import summarizers
from tatau_core.utils.ipfs import IPFS
import os

logger = getLogger()


def summarize_weights(train_results):
    target_dir = tempfile.mkdtemp()
    try:
        ipfs = IPFS()

        downloaded_results = deque()
        for worker_result in train_results:
            if worker_result['result'] is None:
                # TODO: handle this
                pass
            else:
                downloaded_results.append(ipfs.download(worker_result['result'], target_dir))
        return summarize(downloaded_results)
    finally:
        shutil.rmtree(target_dir)


def summarize(downloaded_results: deque):
    if not len(downloaded_results):
        logger.error('list of weights is empty')
        raise ValueError('list of weights is empty')

    logger.info('Summarize {}'.format(downloaded_results))
    summarizer = summarizers.Median()

    for weights_path in downloaded_results:
        weights_file = np.load(weights_path)
        weights = [weights_file[r] for r in weights_file.files]
        summarizer.update(weights=weights)

    target_dir = tempfile.mkdtemp()
    result_weights_path = os.path.join(target_dir, "result_weights.npz")
    np.savez(result_weights_path, *summarizer.commit())

    try:
        file_hash = IPFS().add_file(result_weights_path).multihash
    finally:
        shutil.rmtree(target_dir)

    return file_hash
