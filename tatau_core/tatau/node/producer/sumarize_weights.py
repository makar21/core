from logging import getLogger
import shutil
import tempfile
from collections import deque
import numpy as np
from tatau_core.utils.ipfs import IPFS
import os
from tatau_core.nn.tatau.model import Model

logger = getLogger()


def summarize_weights(train_results, x_test_ipfs: str, y_test_ipfs: str, model_code_ipfs: str):
    target_dir = tempfile.mkdtemp()
    try:
        ipfs = IPFS()
        x_test_path = ipfs.download(x_test_ipfs, target_dir)
        y_test_path = ipfs.download(y_test_ipfs, target_dir)

        model_code_path_tmp = ipfs.download(model_code_ipfs, target_dir)
        model_code_path = model_code_path_tmp + '.py'
        os.rename(model_code_path_tmp, model_code_path)

        downloaded_results = deque()
        for worker_result in train_results:
            if worker_result['result'] is None:
                # TODO: handle this
                pass
            else:
                downloaded_results.append(ipfs.download(worker_result['result'], target_dir))
        return summarize(downloaded_results, x_test_path, y_test_path, model_code_path)
    finally:
        shutil.rmtree(target_dir)


def summarize(weights_updates: deque, x_test_path: str, y_test_path: str, model_code_path: str):
    if not len(weights_updates):
        logger.error('list of weights is empty')
        raise ValueError('list of weights is empty')

    target_dir = tempfile.mkdtemp()

    try:
        logger.info('Summarize {}'.format(weights_updates))

        model = Model.load_model(model_code_path)

        summarizer = model.summarizer_class()
        serializer = model.weights_serializer_class()

        for weights_path in weights_updates:
            weights = serializer.load(weights_path)
            summarizer.update(weights=weights)

        weights_path = os.path.join(target_dir, 'result_weights')
        weights = summarizer.commit()
        serializer.save(weights=weights, path=weights_path)

        x_test = np.load(x_test_path)
        y_test = np.load(y_test_path)

        model.set_weights(weights)
        loss, acc = model.eval(x=x_test, y=y_test)
        file_hash = IPFS().add_file(weights_path).multihash

    finally:
        shutil.rmtree(target_dir)

    return file_hash, loss, acc
