from logging import getLogger
import shutil
import tempfile
from collections import deque
import numpy as np
from tatau_core.nn import summarizers
from tatau_core.utils.ipfs import IPFS
import os
from tatau_core.nn.models.tatau import TatauModel

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


def summarize(weights_updates: deque, x_test_path: str, y_test_path: str, model_code_path: str):
    if not len(weights_updates):
        logger.error('list of weights is empty')
        raise ValueError('list of weights is empty')

    target_dir = tempfile.mkdtemp()

    try:
        logger.info('Summarize {}'.format(weights_updates))
        summarizer = summarizers.Median()

        for weights_path in weights_updates:
            weights = np.load(weights_path)
            summarizer.update(weights=weights)

        result_weights_path = os.path.join(target_dir, "result_weights.npy")
        result_weights = summarizer.commit()
        np.save(result_weights_path, result_weights)

        x_test = np.load(x_test_path)
        y_test = np.load(y_test_path)
        model = TatauModel.load_model(model_code_path)
        model.set_weights(result_weights)
        eval_metrics = model.eval(x=x_test, y=y_test)
        file_hash = IPFS().add_file(result_weights_path).multihash

    finally:
        shutil.rmtree(target_dir)

    return file_hash, eval_metrics
